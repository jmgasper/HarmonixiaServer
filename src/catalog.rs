use std::collections::{BTreeSet, HashMap};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::Utc;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sqlx::{postgres::PgRow, types::Json, Row};
use thiserror::Error;
use unicode_normalization::{char::is_combining_mark, UnicodeNormalization};
use uuid::Uuid;

use crate::{
    domain::{
        Album, AlbumKind, Artist, ArtworkAsset, ArtworkAssetDraft, ArtworkKind,
        CatalogEntityType, CatalogGrouping, CatalogImportDecision, CatalogImportOutcome,
        CatalogImportRequest, CatalogSearchProjection, Episode, MediaFile, MediaFileStatus,
        MediaKind, MediaProbeFacts, MetadataMatchKind, MetadataProviderLink,
        MetadataProviderLinkDraft, MetadataProvenance, MetadataProvenanceDraft,
        MusicCatalogGrouping, Podcast, PodcastCatalogGrouping, Playlist, PlaylistScope,
        ProviderKind, QuarantineItem, QuarantineReason, QuarantineStatus, Track,
    },
    storage::{
        upsert_playlist_search_projection_in_transaction, PgMaintenanceRepository,
        StorageError,
    },
};

const ARTIST_SELECT: &str = r#"
    id,
    name,
    normalized_name,
    sort_name,
    stable_grouping,
    published_at,
    created_at,
    updated_at
"#;

const ALBUM_SELECT: &str = r#"
    id,
    artist_id,
    title,
    normalized_title,
    album_kind::text AS album_kind,
    release_year,
    stable_grouping,
    published_at,
    created_at,
    updated_at
"#;

const TRACK_SELECT: &str = r#"
    id,
    album_id,
    artist_id,
    title,
    normalized_title,
    disc_number,
    track_number,
    duration_seconds,
    stable_grouping,
    published_at,
    created_at,
    updated_at
"#;

const PODCAST_SELECT: &str = r#"
    id,
    title,
    normalized_title,
    stable_grouping,
    published_at,
    created_at,
    updated_at
"#;

const EPISODE_SELECT: &str = r#"
    id,
    podcast_id,
    title,
    normalized_title,
    season_number,
    episode_number,
    duration_seconds,
    stable_grouping,
    published_at,
    created_at,
    updated_at
"#;

const MEDIA_FILE_SELECT: &str = r#"
    id,
    media_kind::text AS media_kind,
    status::text AS status,
    source_path,
    managed_path,
    file_hash,
    file_size,
    mime_type,
    container,
    audio_codec,
    duration_seconds,
    bitrate,
    sample_rate,
    channels,
    genres,
    format_keys,
    track_id,
    episode_id,
    duplicate_of_media_file_id,
    import_job_id,
    discovered_at,
    published_at,
    updated_at
"#;

const MEDIA_FILE_SELECT_MF: &str = r#"
    mf.id,
    mf.media_kind::text AS media_kind,
    mf.status::text AS status,
    mf.source_path,
    mf.managed_path,
    mf.file_hash,
    mf.file_size,
    mf.mime_type,
    mf.container,
    mf.audio_codec,
    mf.duration_seconds,
    mf.bitrate,
    mf.sample_rate,
    mf.channels,
    mf.genres,
    mf.format_keys,
    mf.track_id,
    mf.episode_id,
    mf.duplicate_of_media_file_id,
    mf.import_job_id,
    mf.discovered_at,
    mf.published_at,
    mf.updated_at
"#;

const QUARANTINE_ITEM_SELECT: &str = r#"
    id,
    media_file_id,
    source_path,
    reason::text AS reason,
    status::text AS status,
    retry_count,
    retry_eligible,
    last_import_job_id,
    admin_note,
    created_at,
    updated_at
"#;

const BROWSE_VISIBLE_MEDIA_FILE_PREDICATE: &str = r#"
    mf.status = 'published'
    AND mf.published_at IS NOT NULL
    AND mf.duplicate_of_media_file_id IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM quarantine_items qi
      WHERE qi.media_file_id = mf.id
        AND qi.status IN ('open', 'retrying')
    )
"#;

const SEARCH_MATCH_PREDICATE: &str = r#"
    (
      csp.normalized_display_title = $1
      OR csp.normalized_text = $1
      OR csp.normalized_display_title LIKE ($1 || '%')
      OR csp.normalized_text LIKE ($1 || '%')
      OR NOT EXISTS (
        SELECT 1
        FROM unnest($2::text[]) AS query_token(token)
        WHERE NOT EXISTS (
          SELECT 1
          FROM unnest(string_to_array(csp.normalized_text, ' ')) AS entity_token(token)
          WHERE entity_token.token = query_token.token
             OR entity_token.token LIKE (query_token.token || '%')
        )
      )
    )
"#;

const SEARCH_RANK_EXPRESSION: &str = r#"
    CASE
      WHEN csp.normalized_display_title = $1 OR csp.normalized_text = $1 THEN 0
      WHEN csp.normalized_display_title LIKE ($1 || '%')
        OR csp.normalized_text LIKE ($1 || '%') THEN 1
      ELSE 2
    END
"#;

const GENRE_FILTER_PREDICATE: &str =
    "($4::text IS NULL OR mf.genres @> ARRAY[$4::text])";
const FORMAT_FILTER_PREDICATE: &str =
    "($5::text IS NULL OR mf.format_keys @> ARRAY[$5::text])";

const METADATA_PROVIDER_LINK_SELECT: &str = r#"
    id,
    entity_type::text AS entity_type,
    entity_id,
    provider::text AS provider,
    provider_item_id,
    external_url,
    match_kind::text AS match_kind,
    confidence,
    auto_accepted,
    raw_metadata,
    created_at,
    updated_at
"#;

const METADATA_PROVENANCE_SELECT: &str = r#"
    id,
    entity_type::text AS entity_type,
    entity_id,
    field_name,
    provider::text AS provider,
    value,
    confidence,
    auto_accepted,
    import_job_id,
    source_path,
    created_at
"#;

const ARTWORK_ASSET_SELECT: &str = r#"
    id,
    entity_type::text AS entity_type,
    entity_id,
    provider::text AS provider,
    artwork_kind::text AS artwork_kind,
    source_uri,
    file_path,
    mime_type,
    width,
    height,
    confidence,
    created_at
"#;

const PLAYLIST_SELECT: &str = r#"
    id,
    name,
    description,
    scope::text AS scope,
    owner_account_id,
    created_by_account_id,
    updated_by_account_id,
    created_at,
    updated_at
"#;

const SEARCH_PROJECTION_SELECT: &str = r#"
    entity_type::text AS entity_type,
    entity_id,
    display_title,
    search_text,
    normalized_text,
    normalized_display_title,
    published,
    updated_at
"#;

#[derive(Debug, Clone)]
/// Represents catalog counts in the catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Functionality: Carries fields `artists`, `albums`, `tracks`, `podcasts`, `episodes`, `playlists`, `media_files`, `published_media_files`, `quarantined_media_files` for catalog persistence, browsing, search, import upsert, and normalization logic.
/// Dependencies: depends on `i64`, `i64`, `i64`, `i64`, `i64`, `i64`, and 3 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/catalog.rs`.
pub struct CatalogCounts {
    pub artists: i64,
    pub albums: i64,
    pub tracks: i64,
    pub podcasts: i64,
    pub episodes: i64,
    pub playlists: i64,
    pub media_files: i64,
    pub published_media_files: i64,
    pub quarantined_media_files: i64,
}

pub const DEFAULT_CATALOG_BROWSE_LIMIT: u32 = 50;
pub const MAX_CATALOG_BROWSE_LIMIT: u32 = 200;
pub const DEFAULT_CATALOG_SEARCH_LIMIT: u32 = 10;
pub const MAX_CATALOG_SEARCH_LIMIT: u32 = 50;

#[derive(Debug, Clone)]
/// Represents catalog browse page in the catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Functionality: Carries fields `items`, `limit`, `next_cursor`, `sort` for catalog persistence, browsing, search, import upsert, and normalization logic.
/// Dependencies: depends on `Vec<T>`, `u32`, `Option<String>`, `String` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/catalog.rs`, `src/state.rs`.
pub struct CatalogBrowsePage<T> {
    pub items: Vec<T>,
    pub limit: u32,
    pub next_cursor: Option<String>,
    pub sort: String,
}

#[derive(Debug, Clone)]
/// Represents catalog grouped search results in the catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Functionality: Carries fields `query`, `normalized_query`, `limit`, `artists`, `albums`, `tracks`, `podcasts`, `episodes`, and 1 more for catalog persistence, browsing, search, import upsert, and normalization logic.
/// Dependencies: depends on `String`, `String`, `u32`, `Vec<Artist>`, `Vec<Album>`, `Vec<Track>`, and 3 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/catalog.rs`, `src/state.rs`.
pub struct CatalogGroupedSearchResults {
    pub query: String,
    pub normalized_query: String,
    pub limit: u32,
    pub artists: Vec<Artist>,
    pub albums: Vec<Album>,
    pub tracks: Vec<Track>,
    pub podcasts: Vec<Podcast>,
    pub episodes: Vec<Episode>,
    pub playlists: Vec<Playlist>,
}

#[derive(Debug, Clone)]
/// Represents catalog podcast episode in the catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Functionality: Carries fields `podcast`, `episode` for catalog persistence, browsing, search, import upsert, and normalization logic.
/// Dependencies: depends on `Podcast`, `Episode` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/catalog.rs`, `src/state.rs`.
pub struct CatalogPodcastEpisode {
    pub podcast: Podcast,
    pub episode: Episode,
}

#[derive(Debug, Clone)]
/// Represents catalog search input in the catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Functionality: Carries fields `query`, `normalized_query`, `tokens` for catalog persistence, browsing, search, import upsert, and normalization logic.
/// Dependencies: depends on `String`, `String`, `Vec<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/catalog.rs`.
pub struct CatalogSearchInput {
    pub query: String,
    pub normalized_query: String,
    pub tokens: Vec<String>,
}

#[derive(Debug, Clone, Default)]
/// Represents catalog search filters in the catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Functionality: Carries fields `year`, `genre`, `format`, `media_type` for catalog persistence, browsing, search, import upsert, and normalization logic.
/// Dependencies: depends on `Option<i32>`, `Option<String>`, `Option<String>`, `Option<MediaKind>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/catalog.rs`.
pub struct CatalogSearchFilters {
    pub year: Option<i32>,
    pub genre: Option<String>,
    pub format: Option<String>,
    pub media_type: Option<MediaKind>,
}

impl CatalogSearchFilters {
    /// Handles has media filter for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn has_media_filter(&self) -> bool {
        self.year.is_some()
            || self.genre.is_some()
            || self.format.is_some()
            || self.media_type.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Represents catalog browse sort in the catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Functionality: Enumerates `ArtistName`, `AlbumArtistTitle`, `TrackAlbumPosition`, `PodcastTitle`, `EpisodePodcastPosition` states or choices for catalog persistence, browsing, search, import upsert, and normalization logic.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/catalog.rs`.
pub enum CatalogBrowseSort {
    ArtistName,
    AlbumArtistTitle,
    TrackAlbumPosition,
    PodcastTitle,
    EpisodePodcastPosition,
}

#[derive(Debug, Error)]
/// Represents catalog browse error in the catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Functionality: Enumerates `Storage`, `InvalidLimit`, `InvalidSort` states or choices for catalog persistence, browsing, search, import upsert, and normalization logic.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/catalog.rs`, `src/state.rs`.
pub enum CatalogBrowseError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("limit must be between 1 and {MAX_CATALOG_BROWSE_LIMIT}")]
    InvalidLimit,
    #[error("unsupported {resource} browse sort `{requested}`; supported values: {allowed}")]
    InvalidSort {
        resource: &'static str,
        requested: String,
        allowed: &'static str,
    },
    #[error("invalid browse cursor")]
    InvalidCursor,
    #[error("browse cursor was created for sort `{actual}`, not `{expected}`")]
    CursorSortMismatch { expected: String, actual: String },
}

#[derive(Debug, Error)]
/// Represents catalog search error in the catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Functionality: Enumerates `Storage`, `MissingQuery`, `EmptyQuery`, `InvalidLimit`, `EmptyFilter`, `InvalidMediaType` states or choices for catalog persistence, browsing, search, import upsert, and normalization logic.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/catalog.rs`, `src/state.rs`.
pub enum CatalogSearchError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("search query is required")]
    MissingQuery,
    #[error("search query must include at least one searchable term")]
    EmptyQuery,
    #[error("limit must be between 1 and {MAX_CATALOG_SEARCH_LIMIT}")]
    InvalidLimit,
    #[error("search filter `{field}` must include at least one searchable term")]
    EmptyFilter { field: &'static str },
    #[error("unsupported media_type `{requested}`; supported values: music, podcast")]
    InvalidMediaType { requested: String },
}

impl From<sqlx::Error> for CatalogBrowseError {
    /// Converts from the source domain type for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - `error`: `sqlx:Error`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn from(error: sqlx::Error) -> Self {
        StorageError::from(error).into()
    }
}

impl From<sqlx::Error> for CatalogSearchError {
    /// Converts from the source domain type for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - `error`: `sqlx:Error`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn from(error: sqlx::Error) -> Self {
        StorageError::from(error).into()
    }
}

impl CatalogBrowseSort {
    /// Verifies that api name.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&'static str` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn api_name(self) -> &'static str {
        match self {
            Self::ArtistName => "name",
            Self::AlbumArtistTitle => "artist_title",
            Self::TrackAlbumPosition => "album_position",
            Self::PodcastTitle => "title",
            Self::EpisodePodcastPosition => "podcast_position",
        }
    }
}

/// Normalizes caller-provided data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `limit`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
///
/// Output:
/// - Returns `u32` on success or `CatalogBrowseError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub fn normalize_browse_limit(limit: Option<u32>) -> Result<u32, CatalogBrowseError> {
    let limit = limit.unwrap_or(DEFAULT_CATALOG_BROWSE_LIMIT);
    match limit {
        1..=MAX_CATALOG_BROWSE_LIMIT => Ok(limit),
        _ => Err(CatalogBrowseError::InvalidLimit),
    }
}

/// Normalizes caller-provided data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `limit`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
///
/// Output:
/// - Returns `u32` on success or `CatalogSearchError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogSearchError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub fn normalize_search_limit(limit: Option<u32>) -> Result<u32, CatalogSearchError> {
    let limit = limit.unwrap_or(DEFAULT_CATALOG_SEARCH_LIMIT);
    match limit {
        1..=MAX_CATALOG_SEARCH_LIMIT => Ok(limit),
        _ => Err(CatalogSearchError::InvalidLimit),
    }
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `query`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `CatalogSearchInput` on success or `CatalogSearchError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogSearchError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub fn parse_catalog_search_query(
    query: Option<&str>,
) -> Result<CatalogSearchInput, CatalogSearchError> {
    let query = query
        .map(str::trim)
        .filter(|query| !query.is_empty())
        .ok_or(CatalogSearchError::MissingQuery)?;
    let tokens = normalize_catalog_tokens(query);
    if tokens.is_empty() {
        return Err(CatalogSearchError::EmptyQuery);
    }

    Ok(CatalogSearchInput {
        query: query.to_string(),
        normalized_query: tokens.join(" "),
        tokens,
    })
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `year`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
/// - `genre`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `format`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `media_type`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `CatalogSearchFilters` on success or `CatalogSearchError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogSearchError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub fn parse_catalog_search_filters(
    year: Option<i32>,
    genre: Option<&str>,
    format: Option<&str>,
    media_type: Option<&str>,
) -> Result<CatalogSearchFilters, CatalogSearchError> {
    Ok(CatalogSearchFilters {
        year,
        genre: normalize_optional_filter("genre", genre)?,
        format: normalize_optional_filter("format", format)?,
        media_type: parse_optional_media_type(media_type)?,
    })
}

/// Normalizes caller-provided data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `field`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `value`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Option<String>` on success or `CatalogSearchError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogSearchError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn normalize_optional_filter(
    field: &'static str,
    value: Option<&str>,
) -> Result<Option<String>, CatalogSearchError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let normalized = normalize_catalog_text(value);
    if normalized.is_empty() {
        return Err(CatalogSearchError::EmptyFilter { field });
    }
    Ok(Some(normalized))
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Option<MediaKind>` on success or `CatalogSearchError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogSearchError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_optional_media_type(
    value: Option<&str>,
) -> Result<Option<MediaKind>, CatalogSearchError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    match value.to_ascii_lowercase().replace('-', "_").as_str() {
        "music" => Ok(Some(MediaKind::Music)),
        "podcast" => Ok(Some(MediaKind::Podcast)),
        _ => Err(CatalogSearchError::InvalidMediaType {
            requested: value.to_string(),
        }),
    }
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `sort`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `CatalogBrowseSort` on success or `CatalogBrowseError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub fn parse_artist_browse_sort(
    sort: Option<&str>,
) -> Result<CatalogBrowseSort, CatalogBrowseError> {
    parse_browse_sort(sort, "artists", "name", CatalogBrowseSort::ArtistName)
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `sort`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `CatalogBrowseSort` on success or `CatalogBrowseError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub fn parse_album_browse_sort(
    sort: Option<&str>,
) -> Result<CatalogBrowseSort, CatalogBrowseError> {
    parse_browse_sort(
        sort,
        "albums",
        "artist_title",
        CatalogBrowseSort::AlbumArtistTitle,
    )
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `sort`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `CatalogBrowseSort` on success or `CatalogBrowseError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub fn parse_track_browse_sort(
    sort: Option<&str>,
) -> Result<CatalogBrowseSort, CatalogBrowseError> {
    parse_browse_sort(
        sort,
        "tracks",
        "album_position",
        CatalogBrowseSort::TrackAlbumPosition,
    )
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `sort`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `CatalogBrowseSort` on success or `CatalogBrowseError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub fn parse_podcast_browse_sort(
    sort: Option<&str>,
) -> Result<CatalogBrowseSort, CatalogBrowseError> {
    parse_browse_sort(sort, "podcasts", "title", CatalogBrowseSort::PodcastTitle)
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `sort`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `CatalogBrowseSort` on success or `CatalogBrowseError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub fn parse_episode_browse_sort(
    sort: Option<&str>,
) -> Result<CatalogBrowseSort, CatalogBrowseError> {
    parse_browse_sort(
        sort,
        "episodes",
        "podcast_position",
        CatalogBrowseSort::EpisodePodcastPosition,
    )
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `requested`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `resource`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `allowed`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `sort`: `CatalogBrowseSort`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `CatalogBrowseSort` on success or `CatalogBrowseError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_browse_sort(
    requested: Option<&str>,
    resource: &'static str,
    allowed: &'static str,
    sort: CatalogBrowseSort,
) -> Result<CatalogBrowseSort, CatalogBrowseError> {
    let Some(requested) = requested.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(sort);
    };

    if requested == allowed {
        Ok(sort)
    } else {
        Err(CatalogBrowseError::InvalidSort {
            resource,
            requested: requested.to_string(),
            allowed,
        })
    }
}

impl PgMaintenanceRepository {
    /// Handles backfill catalog search upgrade data for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `()` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn backfill_catalog_search_upgrade_data(&self) -> Result<(), StorageError> {
        let mut transaction = self.pool.begin().await?;
        backfill_playlist_search_projections_in_transaction(&mut transaction).await?;
        backfill_media_file_filter_keys_in_transaction(&mut transaction).await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Handles import catalog file for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `request`: `CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `CatalogImportOutcome` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn import_catalog_file(
        &self,
        request: CatalogImportRequest,
    ) -> Result<CatalogImportOutcome, StorageError> {
        if !request.grouping.is_stable() {
            return self
                .quarantine_catalog_file(
                    &request,
                    QuarantineReason::MetadataFailure,
                    CatalogImportDecision::QuarantinedUnstableGrouping,
                    None,
                    true,
                    Some("stable artist/album or podcast grouping could not be inferred"),
                )
                .await;
        }

        if request.allow_reuse_existing {
            if let Some(existing) = self.reusable_published_media_file(&request).await? {
                return Ok(reused_existing_outcome(existing));
            }
        }

        if let Some(existing) = self.find_duplicate_candidate(&request).await? {
            return self
                .quarantine_catalog_file(
                    &request,
                    QuarantineReason::Duplicate,
                    CatalogImportDecision::QuarantinedDuplicate,
                    Some(existing),
                    false,
                    Some("likely duplicate retained outside the visible catalog"),
                )
                .await;
        }

        match &request.grouping {
            CatalogGrouping::Music(grouping) => {
                self.publish_music_file(&request, grouping).await
            }
            CatalogGrouping::Podcast(grouping) => {
                self.publish_podcast_file(&request, grouping).await
            }
        }
    }

    /// Handles reusable published media file for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `request`: `&CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Option<MediaFile>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn reusable_published_media_file(
        &self,
        request: &CatalogImportRequest,
    ) -> Result<Option<MediaFile>, StorageError> {
        if let Some(existing) = self
            .published_media_file_by_managed_path(&request.source_path)
            .await?
            .filter(|existing| existing.file_hash == request.probe.file_hash)
        {
            return Ok(Some(existing));
        }

        if request.managed_path.as_deref() == Some(request.source_path.as_str()) {
            let existing = self
                .published_media_file_for_hash_any_path(&request.probe.file_hash)
                .await?;
            return Ok(existing.filter(|existing| {
                existing.file_hash == request.probe.file_hash && existing.managed_path.is_none()
            }));
        }

        Ok(None)
    }

    /// Handles quarantine file error for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `request`: `CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `message`: `impl Into<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `CatalogImportOutcome` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn quarantine_file_error(
        &self,
        request: CatalogImportRequest,
        message: impl Into<String>,
    ) -> Result<CatalogImportOutcome, StorageError> {
        let message = message.into();
        self.quarantine_catalog_file(
            &request,
            QuarantineReason::FileError,
            CatalogImportDecision::QuarantinedFileError,
            None,
            true,
            Some(message.as_str()),
        )
        .await
    }

    /// Handles quarantine metadata failure for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `request`: `CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `message`: `impl Into<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `CatalogImportOutcome` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn quarantine_metadata_failure(
        &self,
        request: CatalogImportRequest,
        message: impl Into<String>,
    ) -> Result<CatalogImportOutcome, StorageError> {
        let message = message.into();
        self.quarantine_catalog_file(
            &request,
            QuarantineReason::MetadataFailure,
            CatalogImportDecision::QuarantinedUnstableGrouping,
            None,
            true,
            Some(message.as_str()),
        )
        .await
    }

    /// Handles media file by source path for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `source_path`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Option<MediaFile>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn media_file_by_source_path(
        &self,
        source_path: &str,
    ) -> Result<Option<MediaFile>, StorageError> {
        let sql = format!(
            "SELECT {MEDIA_FILE_SELECT} FROM media_files WHERE source_path = $1 LIMIT 1"
        );
        let row = sqlx::query(&sql)
            .bind(source_path)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(media_file_from_row).transpose()
    }

    /// Handles published media file by managed path for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `managed_path`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Option<MediaFile>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn published_media_file_by_managed_path(
        &self,
        managed_path: &str,
    ) -> Result<Option<MediaFile>, StorageError> {
        let sql = format!(
            r#"
            SELECT {MEDIA_FILE_SELECT}
            FROM media_files
            WHERE managed_path = $1
              AND status = 'published'
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(managed_path)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(media_file_from_row).transpose()
    }

    /// Handles media file by id for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `media_file_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<MediaFile>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn media_file_by_id(
        &self,
        media_file_id: Uuid,
    ) -> Result<Option<MediaFile>, StorageError> {
        let sql = format!(
            "SELECT {MEDIA_FILE_SELECT} FROM media_files WHERE id = $1 LIMIT 1"
        );
        let row = sqlx::query(&sql)
            .bind(media_file_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(media_file_from_row).transpose()
    }

    /// Handles visible artist for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `artist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<Artist>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn visible_artist(
        &self,
        artist_id: Uuid,
    ) -> Result<Option<Artist>, StorageError> {
        let sql = format!(
            r#"
            SELECT {ARTIST_SELECT}
            FROM artists
            WHERE id = $1
              AND published_at IS NOT NULL
              AND stable_grouping
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(artist_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(artist_from_row).transpose()
    }

    /// Handles visible album for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `album_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<Album>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn visible_album(
        &self,
        album_id: Uuid,
    ) -> Result<Option<Album>, StorageError> {
        let sql = format!(
            r#"
            SELECT {ALBUM_SELECT}
            FROM albums
            WHERE id = $1
              AND published_at IS NOT NULL
              AND stable_grouping
              AND EXISTS (
                SELECT 1
                FROM artists album_artist
                WHERE album_artist.id = albums.artist_id
                  AND album_artist.published_at IS NOT NULL
                  AND album_artist.stable_grouping
              )
              AND EXISTS (
                SELECT 1
                FROM tracks t
                JOIN artists track_artist ON track_artist.id = t.artist_id
                JOIN media_files mf ON mf.id = t.canonical_media_file_id
                  AND mf.track_id = t.id
                WHERE t.album_id = albums.id
                  AND t.published_at IS NOT NULL
                  AND t.stable_grouping
                  AND track_artist.published_at IS NOT NULL
                  AND track_artist.stable_grouping
                  AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
              )
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(album_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(album_from_row).transpose()
    }

    /// Handles visible track for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `track_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<Track>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn visible_track(
        &self,
        track_id: Uuid,
    ) -> Result<Option<Track>, StorageError> {
        let sql = format!(
            r#"
            SELECT
              t.id,
              t.album_id,
              t.artist_id,
              t.title,
              t.normalized_title,
              t.disc_number,
              t.track_number,
              t.duration_seconds,
              t.stable_grouping,
              t.published_at,
              t.created_at,
              t.updated_at
            FROM tracks t
            JOIN albums al ON al.id = t.album_id
            JOIN artists album_artist ON album_artist.id = al.artist_id
            JOIN artists track_artist ON track_artist.id = t.artist_id
            JOIN media_files mf ON mf.id = t.canonical_media_file_id
              AND mf.track_id = t.id
            WHERE t.id = $1
              AND t.published_at IS NOT NULL
              AND t.stable_grouping
              AND al.published_at IS NOT NULL
              AND al.stable_grouping
              AND album_artist.published_at IS NOT NULL
              AND album_artist.stable_grouping
              AND track_artist.published_at IS NOT NULL
              AND track_artist.stable_grouping
              AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(track_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(track_from_row).transpose()
    }

    /// Handles visible tracks for album for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `album_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Vec<Track>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn visible_tracks_for_album(
        &self,
        album_id: Uuid,
    ) -> Result<Vec<Track>, StorageError> {
        let sql = format!(
            r#"
            SELECT
              t.id,
              t.album_id,
              t.artist_id,
              t.title,
              t.normalized_title,
              t.disc_number,
              t.track_number,
              t.duration_seconds,
              t.stable_grouping,
              t.published_at,
              t.created_at,
              t.updated_at
            FROM tracks t
            JOIN albums al ON al.id = t.album_id
            JOIN artists album_artist ON album_artist.id = al.artist_id
            JOIN artists track_artist ON track_artist.id = t.artist_id
            JOIN media_files mf ON mf.id = t.canonical_media_file_id
              AND mf.track_id = t.id
            WHERE t.album_id = $1
              AND t.published_at IS NOT NULL
              AND t.stable_grouping
              AND al.published_at IS NOT NULL
              AND al.stable_grouping
              AND album_artist.published_at IS NOT NULL
              AND album_artist.stable_grouping
              AND track_artist.published_at IS NOT NULL
              AND track_artist.stable_grouping
              AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
            ORDER BY
              COALESCE(t.disc_number, 0) ASC,
              COALESCE(t.track_number, 0) ASC,
              lower(t.title) ASC,
              t.id ASC
            "#
        );
        let rows = sqlx::query(&sql).bind(album_id).fetch_all(&self.pool).await?;
        rows.iter().map(track_from_row).collect()
    }

    /// Handles visible original media file for track for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `track_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<MediaFile>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn visible_original_media_file_for_track(
        &self,
        track_id: Uuid,
    ) -> Result<Option<MediaFile>, StorageError> {
        let sql = format!(
            r#"
            SELECT {MEDIA_FILE_SELECT_MF}
            FROM media_files mf
            WHERE mf.id = (
              SELECT t.canonical_media_file_id
              FROM tracks t
              JOIN albums al ON al.id = t.album_id
              JOIN artists album_artist ON album_artist.id = al.artist_id
              JOIN artists track_artist ON track_artist.id = t.artist_id
              WHERE t.id = $1
                AND t.published_at IS NOT NULL
                AND t.stable_grouping
                AND al.published_at IS NOT NULL
                AND al.stable_grouping
                AND album_artist.published_at IS NOT NULL
                AND album_artist.stable_grouping
                AND track_artist.published_at IS NOT NULL
                AND track_artist.stable_grouping
            )
              AND mf.track_id = $1
              AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
            "#
        );
        let row = sqlx::query(&sql)
            .bind(track_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(media_file_from_row).transpose()
    }

    /// Handles visible original media file for episode for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `episode_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<MediaFile>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn visible_original_media_file_for_episode(
        &self,
        episode_id: Uuid,
    ) -> Result<Option<MediaFile>, StorageError> {
        let sql = format!(
            r#"
            SELECT {MEDIA_FILE_SELECT_MF}
            FROM media_files mf
            WHERE mf.id = (
              SELECT e.canonical_media_file_id
              FROM episodes e
              JOIN podcasts p ON p.id = e.podcast_id
              WHERE e.id = $1
                AND e.published_at IS NOT NULL
                AND e.stable_grouping
                AND p.published_at IS NOT NULL
                AND p.stable_grouping
            )
              AND mf.episode_id = $1
              AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
            "#
        );
        let row = sqlx::query(&sql)
            .bind(episode_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(media_file_from_row).transpose()
    }

    pub async fn visible_artwork_assets(
        &self,
        entity_type: CatalogEntityType,
        entity_id: Uuid,
        artwork_kind: Option<ArtworkKind>,
    ) -> Result<Option<Vec<ArtworkAsset>>, StorageError> {
        if !self
            .visible_artwork_entity_exists(entity_type, entity_id)
            .await?
        {
            return Ok(None);
        }

        let sql = format!(
            r#"
            SELECT {ARTWORK_ASSET_SELECT}
            FROM artwork_assets aa
            WHERE aa.entity_type = $1::text::catalog_entity_type
              AND aa.entity_id = $2
              AND ($3::text IS NULL OR aa.artwork_kind = $3::text::artwork_kind)
              AND aa.file_path IS NOT NULL
            ORDER BY
              aa.confidence DESC,
              aa.created_at DESC,
              aa.id ASC
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(entity_type_name(entity_type))
            .bind(entity_id)
            .bind(artwork_kind.map(artwork_kind_name))
            .fetch_all(&self.pool)
            .await?;

        rows.iter().map(artwork_asset_from_row).collect::<Result<Vec<_>, _>>().map(Some)
    }

    pub async fn visible_artwork_asset(
        &self,
        artwork_asset_id: Uuid,
    ) -> Result<Option<ArtworkAsset>, StorageError> {
        let sql = format!(
            r#"
            SELECT {ARTWORK_ASSET_SELECT}
            FROM artwork_assets aa
            WHERE aa.id = $1
              AND aa.file_path IS NOT NULL
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(artwork_asset_id)
            .fetch_optional(&self.pool)
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let artwork = artwork_asset_from_row(&row)?;
        if self
            .visible_artwork_entity_exists(artwork.entity_type, artwork.entity_id)
            .await?
        {
            Ok(Some(artwork))
        } else {
            Ok(None)
        }
    }

    async fn visible_artwork_entity_exists(
        &self,
        entity_type: CatalogEntityType,
        entity_id: Uuid,
    ) -> Result<bool, StorageError> {
        let sql = match entity_type {
            CatalogEntityType::Artist => format!(
                r#"
                SELECT EXISTS (
                  SELECT 1
                  FROM artists ar
                  WHERE ar.id = $1
                    AND ar.published_at IS NOT NULL
                    AND ar.stable_grouping
                    AND (
                      EXISTS (
                        SELECT 1
                        FROM albums al
                        JOIN tracks t ON t.album_id = al.id
                        JOIN media_files mf ON mf.id = t.canonical_media_file_id
                        WHERE al.artist_id = ar.id
                          AND al.published_at IS NOT NULL
                          AND al.stable_grouping
                          AND t.published_at IS NOT NULL
                          AND t.stable_grouping
                          AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
                      )
                      OR EXISTS (
                        SELECT 1
                        FROM tracks t
                        JOIN albums al ON al.id = t.album_id
                        JOIN media_files mf ON mf.id = t.canonical_media_file_id
                        WHERE t.artist_id = ar.id
                          AND t.published_at IS NOT NULL
                          AND t.stable_grouping
                          AND al.published_at IS NOT NULL
                          AND al.stable_grouping
                          AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
                      )
                    )
                )
                "#
            ),
            CatalogEntityType::Album => format!(
                r#"
                SELECT EXISTS (
                  SELECT 1
                  FROM albums al
                  JOIN artists ar ON ar.id = al.artist_id
                  WHERE al.id = $1
                    AND al.published_at IS NOT NULL
                    AND al.stable_grouping
                    AND ar.published_at IS NOT NULL
                    AND ar.stable_grouping
                    AND EXISTS (
                      SELECT 1
                      FROM tracks t
                      JOIN media_files mf ON mf.id = t.canonical_media_file_id
                      WHERE t.album_id = al.id
                        AND t.published_at IS NOT NULL
                        AND t.stable_grouping
                        AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
                    )
                )
                "#
            ),
            CatalogEntityType::Track => format!(
                r#"
                SELECT EXISTS (
                  SELECT 1
                  FROM tracks t
                  JOIN albums al ON al.id = t.album_id
                  JOIN artists album_artist ON album_artist.id = al.artist_id
                  JOIN artists track_artist ON track_artist.id = t.artist_id
                  WHERE t.id = $1
                    AND t.published_at IS NOT NULL
                    AND t.stable_grouping
                    AND al.published_at IS NOT NULL
                    AND al.stable_grouping
                    AND album_artist.published_at IS NOT NULL
                    AND album_artist.stable_grouping
                    AND track_artist.published_at IS NOT NULL
                    AND track_artist.stable_grouping
                    AND EXISTS (
                      SELECT 1
                      FROM media_files mf
                      WHERE mf.id = t.canonical_media_file_id
                        AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
                    )
                )
                "#
            ),
            CatalogEntityType::Podcast => format!(
                r#"
                SELECT EXISTS (
                  SELECT 1
                  FROM podcasts p
                  WHERE p.id = $1
                    AND p.published_at IS NOT NULL
                    AND p.stable_grouping
                    AND EXISTS (
                      SELECT 1
                      FROM episodes e
                      JOIN media_files mf ON mf.id = e.canonical_media_file_id
                        AND mf.episode_id = e.id
                      WHERE e.podcast_id = p.id
                        AND e.published_at IS NOT NULL
                        AND e.stable_grouping
                        AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
                    )
                )
                "#
            ),
            CatalogEntityType::Episode => format!(
                r#"
                SELECT EXISTS (
                  SELECT 1
                  FROM episodes e
                  JOIN podcasts p ON p.id = e.podcast_id
                  JOIN media_files mf ON mf.id = e.canonical_media_file_id
                    AND mf.episode_id = e.id
                  WHERE e.id = $1
                    AND e.published_at IS NOT NULL
                    AND e.stable_grouping
                    AND p.published_at IS NOT NULL
                    AND p.stable_grouping
                    AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
                )
                "#
            ),
            CatalogEntityType::Playlist => {
                r#"
                SELECT EXISTS (
                  SELECT 1
                  FROM playlists p
                  WHERE p.id = $1
                )
                "#
                .to_string()
            }
            CatalogEntityType::MediaFile => {
                return Ok(false);
            }
        };

        let exists: bool = sqlx::query_scalar(&sql)
            .bind(entity_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(exists)
    }

    /// Handles published media file for hash any path for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `file_hash`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Option<MediaFile>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn published_media_file_for_hash_any_path(
        &self,
        file_hash: &str,
    ) -> Result<Option<MediaFile>, StorageError> {
        let sql = format!(
            r#"
            SELECT {MEDIA_FILE_SELECT}
            FROM media_files
            WHERE file_hash = $1
              AND status = 'published'
            ORDER BY published_at ASC NULLS LAST, discovered_at ASC, id ASC
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(file_hash)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(media_file_from_row).transpose()
    }

    /// Handles published media file for hash for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `file_hash`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `excluded_source_path`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Option<MediaFile>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn published_media_file_for_hash(
        &self,
        file_hash: &str,
        excluded_source_path: &str,
    ) -> Result<Option<MediaFile>, StorageError> {
        let sql = format!(
            r#"
            SELECT {MEDIA_FILE_SELECT}
            FROM media_files
            WHERE file_hash = $1
              AND source_path <> $2
              AND (managed_path IS NULL OR managed_path <> $2)
              AND status = 'published'
            ORDER BY published_at ASC NULLS LAST, discovered_at ASC, id ASC
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(file_hash)
            .bind(excluded_source_path)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(media_file_from_row).transpose()
    }

    /// Handles find duplicate candidate for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `request`: `&CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Option<MediaFile>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn find_duplicate_candidate(
        &self,
        request: &CatalogImportRequest,
    ) -> Result<Option<MediaFile>, StorageError> {
        if let Some(existing) = self
            .published_media_file_for_hash(&request.probe.file_hash, &request.source_path)
            .await?
        {
            return Ok(Some(existing));
        }

        match &request.grouping {
            CatalogGrouping::Music(grouping) => {
                self.find_similar_music_media_file(request, grouping).await
            }
            CatalogGrouping::Podcast(grouping) => {
                self.find_similar_podcast_media_file(request, grouping).await
            }
        }
    }

    /// Handles catalog counts for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `CatalogCounts` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn catalog_counts(&self) -> Result<CatalogCounts, StorageError> {
        let row = sqlx::query(
            r#"
            SELECT
              (SELECT count(*) FROM artists) AS artists,
              (SELECT count(*) FROM albums) AS albums,
              (SELECT count(*) FROM tracks) AS tracks,
              (SELECT count(*) FROM podcasts) AS podcasts,
              (SELECT count(*) FROM episodes) AS episodes,
              (SELECT count(*) FROM playlists) AS playlists,
              (SELECT count(*) FROM media_files) AS media_files,
              (SELECT count(*) FROM media_files WHERE status = 'published') AS published_media_files,
              (SELECT count(*) FROM media_files WHERE status IN ('duplicate', 'quarantined', 'failed')) AS quarantined_media_files
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(CatalogCounts {
            artists: row.try_get("artists")?,
            albums: row.try_get("albums")?,
            tracks: row.try_get("tracks")?,
            podcasts: row.try_get("podcasts")?,
            episodes: row.try_get("episodes")?,
            playlists: row.try_get("playlists")?,
            media_files: row.try_get("media_files")?,
            published_media_files: row.try_get("published_media_files")?,
            quarantined_media_files: row.try_get("quarantined_media_files")?,
        })
    }

    /// Handles published tracks for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Vec<Track>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn published_tracks(&self) -> Result<Vec<Track>, StorageError> {
        let sql = format!(
            r#"
            SELECT {TRACK_SELECT}
            FROM tracks
            WHERE published_at IS NOT NULL
            ORDER BY updated_at DESC, id ASC
            "#
        );
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        rows.iter().map(track_from_row).collect()
    }

    /// Returns a paginated browse view for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    /// - `cursor`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `sort`: `CatalogBrowseSort`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `CatalogBrowsePage<Artist>` on success or `CatalogBrowseError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn browse_artists(
        &self,
        limit: u32,
        cursor: Option<&str>,
        sort: CatalogBrowseSort,
    ) -> Result<CatalogBrowsePage<Artist>, CatalogBrowseError> {
        if sort != CatalogBrowseSort::ArtistName {
            return Err(CatalogBrowseError::InvalidSort {
                resource: "artists",
                requested: sort.api_name().to_string(),
                allowed: "name",
            });
        }
        let cursor = decode_cursor(cursor, sort)?;
        let artist_cursor = match cursor {
            Some(BrowseCursor::Name {
                key,
                name_key,
                id,
            }) => Some((key, name_key, id)),
            None => None,
            _ => return Err(CatalogBrowseError::InvalidCursor),
        };
        let sql = format!(
            r#"
            SELECT
              {ARTIST_SELECT},
              lower(COALESCE(ar.sort_name, ar.name)) AS browse_sort_key,
              lower(ar.name) AS browse_name_key
            FROM artists ar
            WHERE ar.published_at IS NOT NULL
              AND ar.stable_grouping
              AND (
                EXISTS (
                  SELECT 1
                  FROM albums al
                  JOIN tracks t ON t.album_id = al.id
                  JOIN media_files mf ON mf.id = t.canonical_media_file_id
                  WHERE al.artist_id = ar.id
                    AND al.published_at IS NOT NULL
                    AND al.stable_grouping
                    AND t.published_at IS NOT NULL
                    AND t.stable_grouping
                    AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
                )
                OR EXISTS (
                  SELECT 1
                  FROM tracks t
                  JOIN albums al ON al.id = t.album_id
                  JOIN media_files mf ON mf.id = t.canonical_media_file_id
                  WHERE t.artist_id = ar.id
                    AND t.published_at IS NOT NULL
                    AND t.stable_grouping
                    AND al.published_at IS NOT NULL
                    AND al.stable_grouping
                    AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
                )
              )
              AND (
                $1::text IS NULL
                OR (
                  lower(COALESCE(ar.sort_name, ar.name)),
                  lower(ar.name),
                  ar.id
                ) > ($1, $2, $3)
              )
            ORDER BY
              lower(COALESCE(ar.sort_name, ar.name)) ASC,
              lower(ar.name) ASC,
              ar.id ASC
            LIMIT $4
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(artist_cursor.as_ref().map(|cursor| cursor.0.as_str()))
            .bind(artist_cursor.as_ref().map(|cursor| cursor.1.as_str()))
            .bind(artist_cursor.as_ref().map(|cursor| cursor.2))
            .bind(limit_plus_one(limit)?)
            .fetch_all(&self.pool)
            .await?;

        let mut rows = rows;
        let has_next = truncate_to_limit(&mut rows, limit);
        let next_cursor = if has_next {
            rows.last()
                .map(artist_cursor_from_row)
                .transpose()?
                .map(encode_cursor)
                .transpose()?
        } else {
            None
        };
        let items = rows.iter().map(artist_from_row).collect::<Result<Vec<_>, _>>()?;

        Ok(CatalogBrowsePage {
            items,
            limit,
            next_cursor,
            sort: sort.api_name().to_string(),
        })
    }

    /// Returns a paginated browse view for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    /// - `cursor`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `sort`: `CatalogBrowseSort`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `CatalogBrowsePage<Album>` on success or `CatalogBrowseError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn browse_albums(
        &self,
        limit: u32,
        cursor: Option<&str>,
        sort: CatalogBrowseSort,
    ) -> Result<CatalogBrowsePage<Album>, CatalogBrowseError> {
        if sort != CatalogBrowseSort::AlbumArtistTitle {
            return Err(CatalogBrowseError::InvalidSort {
                resource: "albums",
                requested: sort.api_name().to_string(),
                allowed: "artist_title",
            });
        }
        let cursor = decode_cursor(cursor, sort)?;
        let album_cursor = match cursor {
            Some(BrowseCursor::ArtistTitle {
                artist_key,
                title_key,
                id,
            }) => Some((artist_key, title_key, id)),
            None => None,
            _ => return Err(CatalogBrowseError::InvalidCursor),
        };
        let sql = format!(
            r#"
            SELECT
              al.id,
              al.artist_id,
              al.title,
              al.normalized_title,
              al.album_kind::text AS album_kind,
              al.release_year,
              al.stable_grouping,
              al.published_at,
              al.created_at,
              al.updated_at,
              lower(COALESCE(ar.sort_name, ar.name)) AS browse_artist_key,
              lower(al.title) AS browse_title_key
            FROM albums al
            JOIN artists ar ON ar.id = al.artist_id
            WHERE al.published_at IS NOT NULL
              AND al.stable_grouping
              AND ar.published_at IS NOT NULL
              AND ar.stable_grouping
              AND EXISTS (
                SELECT 1
                FROM tracks t
                JOIN media_files mf ON mf.id = t.canonical_media_file_id
                WHERE t.album_id = al.id
                  AND t.published_at IS NOT NULL
                  AND t.stable_grouping
                  AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
              )
              AND (
                $1::text IS NULL
                OR (
                  lower(COALESCE(ar.sort_name, ar.name)),
                  lower(al.title),
                  al.id
                ) > ($1, $2, $3)
              )
            ORDER BY
              lower(COALESCE(ar.sort_name, ar.name)) ASC,
              lower(al.title) ASC,
              al.id ASC
            LIMIT $4
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(album_cursor.as_ref().map(|cursor| cursor.0.as_str()))
            .bind(album_cursor.as_ref().map(|cursor| cursor.1.as_str()))
            .bind(album_cursor.as_ref().map(|cursor| cursor.2))
            .bind(limit_plus_one(limit)?)
            .fetch_all(&self.pool)
            .await?;

        let mut rows = rows;
        let has_next = truncate_to_limit(&mut rows, limit);
        let next_cursor = if has_next {
            rows.last()
                .map(album_cursor_from_row)
                .transpose()?
                .map(encode_cursor)
                .transpose()?
        } else {
            None
        };
        let items = rows.iter().map(album_from_row).collect::<Result<Vec<_>, _>>()?;

        Ok(CatalogBrowsePage {
            items,
            limit,
            next_cursor,
            sort: sort.api_name().to_string(),
        })
    }

    /// Returns a paginated browse view for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    /// - `cursor`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `sort`: `CatalogBrowseSort`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `CatalogBrowsePage<Track>` on success or `CatalogBrowseError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn browse_tracks(
        &self,
        limit: u32,
        cursor: Option<&str>,
        sort: CatalogBrowseSort,
    ) -> Result<CatalogBrowsePage<Track>, CatalogBrowseError> {
        if sort != CatalogBrowseSort::TrackAlbumPosition {
            return Err(CatalogBrowseError::InvalidSort {
                resource: "tracks",
                requested: sort.api_name().to_string(),
                allowed: "album_position",
            });
        }
        let cursor = decode_cursor(cursor, sort)?;
        let track_cursor = match cursor {
            Some(BrowseCursor::AlbumPosition {
                album_artist_key,
                album_title_key,
                disc_key,
                track_key,
                title_key,
                id,
            }) => Some((
                album_artist_key,
                album_title_key,
                disc_key,
                track_key,
                title_key,
                id,
            )),
            None => None,
            _ => return Err(CatalogBrowseError::InvalidCursor),
        };
        let sql = format!(
            r#"
            SELECT
              t.id,
              t.album_id,
              t.artist_id,
              t.title,
              t.normalized_title,
              t.disc_number,
              t.track_number,
              t.duration_seconds,
              t.stable_grouping,
              t.published_at,
              t.created_at,
              t.updated_at,
              lower(COALESCE(album_artist.sort_name, album_artist.name)) AS browse_album_artist_key,
              lower(al.title) AS browse_album_title_key,
              COALESCE(t.disc_number, 0) AS browse_disc_key,
              COALESCE(t.track_number, 0) AS browse_track_key,
              lower(t.title) AS browse_title_key
            FROM tracks t
            JOIN albums al ON al.id = t.album_id
            JOIN artists album_artist ON album_artist.id = al.artist_id
            JOIN artists track_artist ON track_artist.id = t.artist_id
            WHERE t.published_at IS NOT NULL
              AND t.stable_grouping
              AND al.published_at IS NOT NULL
              AND al.stable_grouping
              AND album_artist.published_at IS NOT NULL
              AND album_artist.stable_grouping
              AND track_artist.published_at IS NOT NULL
              AND track_artist.stable_grouping
              AND EXISTS (
                SELECT 1
                FROM media_files mf
                WHERE mf.id = t.canonical_media_file_id
                  AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
              )
              AND (
                $1::text IS NULL
                OR (
                  lower(COALESCE(album_artist.sort_name, album_artist.name)),
                  lower(al.title),
                  COALESCE(t.disc_number, 0),
                  COALESCE(t.track_number, 0),
                  lower(t.title),
                  t.id
                ) > ($1, $2, $3, $4, $5, $6)
              )
            ORDER BY
              lower(COALESCE(album_artist.sort_name, album_artist.name)) ASC,
              lower(al.title) ASC,
              COALESCE(t.disc_number, 0) ASC,
              COALESCE(t.track_number, 0) ASC,
              lower(t.title) ASC,
              t.id ASC
            LIMIT $7
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(track_cursor.as_ref().map(|cursor| cursor.0.as_str()))
            .bind(track_cursor.as_ref().map(|cursor| cursor.1.as_str()))
            .bind(track_cursor.as_ref().map(|cursor| cursor.2))
            .bind(track_cursor.as_ref().map(|cursor| cursor.3))
            .bind(track_cursor.as_ref().map(|cursor| cursor.4.as_str()))
            .bind(track_cursor.as_ref().map(|cursor| cursor.5))
            .bind(limit_plus_one(limit)?)
            .fetch_all(&self.pool)
            .await?;

        let mut rows = rows;
        let has_next = truncate_to_limit(&mut rows, limit);
        let next_cursor = if has_next {
            rows.last()
                .map(track_cursor_from_row)
                .transpose()?
                .map(encode_cursor)
                .transpose()?
        } else {
            None
        };
        let items = rows.iter().map(track_from_row).collect::<Result<Vec<_>, _>>()?;

        Ok(CatalogBrowsePage {
            items,
            limit,
            next_cursor,
            sort: sort.api_name().to_string(),
        })
    }

    /// Returns a paginated browse view for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    /// - `cursor`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `sort`: `CatalogBrowseSort`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `CatalogBrowsePage<Podcast>` on success or `CatalogBrowseError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn browse_podcasts(
        &self,
        limit: u32,
        cursor: Option<&str>,
        sort: CatalogBrowseSort,
    ) -> Result<CatalogBrowsePage<Podcast>, CatalogBrowseError> {
        if sort != CatalogBrowseSort::PodcastTitle {
            return Err(CatalogBrowseError::InvalidSort {
                resource: "podcasts",
                requested: sort.api_name().to_string(),
                allowed: "title",
            });
        }
        let cursor = decode_cursor(cursor, sort)?;
        let podcast_cursor = match cursor {
            Some(BrowseCursor::Title { title_key, id }) => Some((title_key, id)),
            None => None,
            _ => return Err(CatalogBrowseError::InvalidCursor),
        };
        let sql = format!(
            r#"
            SELECT
              {PODCAST_SELECT},
              lower(p.title) AS browse_title_key
            FROM podcasts p
            WHERE p.published_at IS NOT NULL
              AND p.stable_grouping
              AND EXISTS (
                SELECT 1
                FROM episodes e
                JOIN media_files mf ON mf.id = e.canonical_media_file_id
                WHERE e.podcast_id = p.id
                  AND e.published_at IS NOT NULL
                  AND e.stable_grouping
                  AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
              )
              AND (
                $1::text IS NULL
                OR (lower(p.title), p.id) > ($1, $2)
              )
            ORDER BY lower(p.title) ASC, p.id ASC
            LIMIT $3
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(podcast_cursor.as_ref().map(|cursor| cursor.0.as_str()))
            .bind(podcast_cursor.as_ref().map(|cursor| cursor.1))
            .bind(limit_plus_one(limit)?)
            .fetch_all(&self.pool)
            .await?;

        let mut rows = rows;
        let has_next = truncate_to_limit(&mut rows, limit);
        let next_cursor = if has_next {
            rows.last()
                .map(podcast_cursor_from_row)
                .transpose()?
                .map(encode_cursor)
                .transpose()?
        } else {
            None
        };
        let items = rows.iter().map(podcast_from_row).collect::<Result<Vec<_>, _>>()?;

        Ok(CatalogBrowsePage {
            items,
            limit,
            next_cursor,
            sort: sort.api_name().to_string(),
        })
    }

    /// Handles visible podcast for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `podcast_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<Podcast>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn visible_podcast(
        &self,
        podcast_id: Uuid,
    ) -> Result<Option<Podcast>, StorageError> {
        let sql = format!(
            r#"
            SELECT {PODCAST_SELECT}
            FROM podcasts p
            WHERE p.id = $1
              AND p.published_at IS NOT NULL
              AND p.stable_grouping
              AND EXISTS (
                SELECT 1
                FROM episodes e
                JOIN media_files mf ON mf.id = e.canonical_media_file_id
                  AND mf.episode_id = e.id
                WHERE e.podcast_id = p.id
                  AND e.published_at IS NOT NULL
                  AND e.stable_grouping
                  AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
              )
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(podcast_id)
            .fetch_optional(&self.pool)
            .await?;

        row.as_ref().map(podcast_from_row).transpose()
    }

    /// Returns a paginated browse view for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    /// - `cursor`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `sort`: `CatalogBrowseSort`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `CatalogBrowsePage<Episode>` on success or `CatalogBrowseError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn browse_episodes(
        &self,
        limit: u32,
        cursor: Option<&str>,
        sort: CatalogBrowseSort,
    ) -> Result<CatalogBrowsePage<Episode>, CatalogBrowseError> {
        if sort != CatalogBrowseSort::EpisodePodcastPosition {
            return Err(CatalogBrowseError::InvalidSort {
                resource: "episodes",
                requested: sort.api_name().to_string(),
                allowed: "podcast_position",
            });
        }
        let cursor = decode_cursor(cursor, sort)?;
        let episode_cursor = match cursor {
            Some(BrowseCursor::PodcastPosition {
                podcast_key,
                season_key,
                episode_key,
                title_key,
                id,
            }) => Some((podcast_key, season_key, episode_key, title_key, id)),
            None => None,
            _ => return Err(CatalogBrowseError::InvalidCursor),
        };
        let sql = format!(
            r#"
            SELECT
              e.id,
              e.podcast_id,
              e.title,
              e.normalized_title,
              e.season_number,
              e.episode_number,
              e.duration_seconds,
              e.stable_grouping,
              e.published_at,
              e.created_at,
              e.updated_at,
              lower(p.title) AS browse_podcast_key,
              COALESCE(e.season_number, 0) AS browse_season_key,
              COALESCE(e.episode_number, 0) AS browse_episode_key,
              lower(e.title) AS browse_title_key
            FROM episodes e
            JOIN podcasts p ON p.id = e.podcast_id
            WHERE e.published_at IS NOT NULL
              AND e.stable_grouping
              AND p.published_at IS NOT NULL
              AND p.stable_grouping
              AND EXISTS (
                SELECT 1
                FROM media_files mf
                WHERE mf.id = e.canonical_media_file_id
                  AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
              )
              AND (
                $1::text IS NULL
                OR (
                  lower(p.title),
                  COALESCE(e.season_number, 0),
                  COALESCE(e.episode_number, 0),
                  lower(e.title),
                  e.id
                ) > ($1, $2, $3, $4, $5)
              )
            ORDER BY
              lower(p.title) ASC,
              COALESCE(e.season_number, 0) ASC,
              COALESCE(e.episode_number, 0) ASC,
              lower(e.title) ASC,
              e.id ASC
            LIMIT $6
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(episode_cursor.as_ref().map(|cursor| cursor.0.as_str()))
            .bind(episode_cursor.as_ref().map(|cursor| cursor.1))
            .bind(episode_cursor.as_ref().map(|cursor| cursor.2))
            .bind(episode_cursor.as_ref().map(|cursor| cursor.3.as_str()))
            .bind(episode_cursor.as_ref().map(|cursor| cursor.4))
            .bind(limit_plus_one(limit)?)
            .fetch_all(&self.pool)
            .await?;

        let mut rows = rows;
        let has_next = truncate_to_limit(&mut rows, limit);
        let next_cursor = if has_next {
            rows.last()
                .map(episode_cursor_from_row)
                .transpose()?
                .map(encode_cursor)
                .transpose()?
        } else {
            None
        };
        let items = rows.iter().map(episode_from_row).collect::<Result<Vec<_>, _>>()?;

        Ok(CatalogBrowsePage {
            items,
            limit,
            next_cursor,
            sort: sort.api_name().to_string(),
        })
    }

    /// Returns a paginated browse view for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `podcast_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    /// - `cursor`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `sort`: `CatalogBrowseSort`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `CatalogBrowsePage<Episode>` on success or `CatalogBrowseError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn browse_episodes_for_podcast(
        &self,
        podcast_id: Uuid,
        limit: u32,
        cursor: Option<&str>,
        sort: CatalogBrowseSort,
    ) -> Result<CatalogBrowsePage<Episode>, CatalogBrowseError> {
        if sort != CatalogBrowseSort::EpisodePodcastPosition {
            return Err(CatalogBrowseError::InvalidSort {
                resource: "episodes",
                requested: sort.api_name().to_string(),
                allowed: "podcast_position",
            });
        }
        let cursor = decode_cursor(cursor, sort)?;
        let episode_cursor = match cursor {
            Some(BrowseCursor::PodcastPosition {
                podcast_key,
                season_key,
                episode_key,
                title_key,
                id,
            }) => Some((podcast_key, season_key, episode_key, title_key, id)),
            None => None,
            _ => return Err(CatalogBrowseError::InvalidCursor),
        };
        let sql = format!(
            r#"
            SELECT
              e.id,
              e.podcast_id,
              e.title,
              e.normalized_title,
              e.season_number,
              e.episode_number,
              e.duration_seconds,
              e.stable_grouping,
              e.published_at,
              e.created_at,
              e.updated_at,
              lower(p.title) AS browse_podcast_key,
              COALESCE(e.season_number, 0) AS browse_season_key,
              COALESCE(e.episode_number, 0) AS browse_episode_key,
              lower(e.title) AS browse_title_key
            FROM episodes e
            JOIN podcasts p ON p.id = e.podcast_id
            WHERE e.podcast_id = $6
              AND e.published_at IS NOT NULL
              AND e.stable_grouping
              AND p.published_at IS NOT NULL
              AND p.stable_grouping
              AND EXISTS (
                SELECT 1
                FROM media_files mf
                WHERE mf.id = e.canonical_media_file_id
                  AND mf.episode_id = e.id
                  AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
              )
              AND (
                $1::text IS NULL
                OR (
                  lower(p.title),
                  COALESCE(e.season_number, 0),
                  COALESCE(e.episode_number, 0),
                  lower(e.title),
                  e.id
                ) > ($1, $2, $3, $4, $5)
              )
            ORDER BY
              lower(p.title) ASC,
              COALESCE(e.season_number, 0) ASC,
              COALESCE(e.episode_number, 0) ASC,
              lower(e.title) ASC,
              e.id ASC
            LIMIT $7
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(episode_cursor.as_ref().map(|cursor| cursor.0.as_str()))
            .bind(episode_cursor.as_ref().map(|cursor| cursor.1))
            .bind(episode_cursor.as_ref().map(|cursor| cursor.2))
            .bind(episode_cursor.as_ref().map(|cursor| cursor.3.as_str()))
            .bind(episode_cursor.as_ref().map(|cursor| cursor.4))
            .bind(podcast_id)
            .bind(limit_plus_one(limit)?)
            .fetch_all(&self.pool)
            .await?;

        let mut rows = rows;
        let has_next = truncate_to_limit(&mut rows, limit);
        let next_cursor = if has_next {
            rows.last()
                .map(episode_cursor_from_row)
                .transpose()?
                .map(encode_cursor)
                .transpose()?
        } else {
            None
        };
        let items = rows.iter().map(episode_from_row).collect::<Result<Vec<_>, _>>()?;

        Ok(CatalogBrowsePage {
            items,
            limit,
            next_cursor,
            sort: sort.api_name().to_string(),
        })
    }

    /// Handles visible episode for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `episode_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<CatalogPodcastEpisode>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn visible_episode(
        &self,
        episode_id: Uuid,
    ) -> Result<Option<CatalogPodcastEpisode>, StorageError> {
        let sql = format!(
            r#"
            SELECT
              e.id,
              e.podcast_id,
              e.title,
              e.normalized_title,
              e.season_number,
              e.episode_number,
              e.duration_seconds,
              e.stable_grouping,
              e.published_at,
              e.created_at,
              e.updated_at,
              p.id AS read_podcast_id,
              p.title AS read_podcast_title,
              p.normalized_title AS read_podcast_normalized_title,
              p.stable_grouping AS read_podcast_stable_grouping,
              p.published_at AS read_podcast_published_at,
              p.created_at AS read_podcast_created_at,
              p.updated_at AS read_podcast_updated_at
            FROM episodes e
            JOIN podcasts p ON p.id = e.podcast_id
            JOIN media_files mf ON mf.id = e.canonical_media_file_id
              AND mf.episode_id = e.id
            WHERE e.id = $1
              AND e.published_at IS NOT NULL
              AND e.stable_grouping
              AND p.published_at IS NOT NULL
              AND p.stable_grouping
              AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(episode_id)
            .fetch_optional(&self.pool)
            .await?;

        row.as_ref()
            .map(|row| {
                Ok(CatalogPodcastEpisode {
                    podcast: podcast_from_episode_read_row(row)?,
                    episode: episode_from_row(row)?,
                })
            })
            .transpose()
    }

    /// Searches resources for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `input`: `&CatalogSearchInput`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `filters`: `&CatalogSearchFilters`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `CatalogGroupedSearchResults` on success or `CatalogSearchError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `CatalogSearchError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn search_catalog(
        &self,
        account_id: Uuid,
        input: &CatalogSearchInput,
        filters: &CatalogSearchFilters,
        limit: u32,
    ) -> Result<CatalogGroupedSearchResults, CatalogSearchError> {
        Ok(CatalogGroupedSearchResults {
            query: input.query.clone(),
            normalized_query: input.normalized_query.clone(),
            limit,
            artists: self.search_artists(input, filters, limit).await?,
            albums: self.search_albums(input, filters, limit).await?,
            tracks: self.search_tracks(input, filters, limit).await?,
            podcasts: self.search_podcasts(input, filters, limit).await?,
            episodes: self.search_episodes(input, filters, limit).await?,
            playlists: self
                .search_playlists(account_id, input, filters, limit)
                .await?,
        })
    }

    /// Searches resources for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `input`: `&CatalogSearchInput`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `filters`: `&CatalogSearchFilters`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `Vec<Artist>` on success or `CatalogSearchError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `CatalogSearchError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn search_artists(
        &self,
        input: &CatalogSearchInput,
        filters: &CatalogSearchFilters,
        limit: u32,
    ) -> Result<Vec<Artist>, CatalogSearchError> {
        let sql = format!(
            r#"
            SELECT {ARTIST_SELECT}
            FROM catalog_search_projection csp
            JOIN artists ar ON ar.id = csp.entity_id
            WHERE csp.entity_type = 'artist'::catalog_entity_type
              AND csp.published
              AND ar.published_at IS NOT NULL
              AND ar.stable_grouping
              AND {SEARCH_MATCH_PREDICATE}
              AND (
                EXISTS (
                  SELECT 1
                  FROM albums al
                  JOIN tracks t ON t.album_id = al.id
                  JOIN media_files mf ON mf.id = t.canonical_media_file_id
                  WHERE al.artist_id = ar.id
                    AND al.published_at IS NOT NULL
                    AND al.stable_grouping
                    AND t.published_at IS NOT NULL
                    AND t.stable_grouping
                    AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
                    AND ($3::integer IS NULL OR al.release_year = $3)
                    AND {GENRE_FILTER_PREDICATE}
                    AND {FORMAT_FILTER_PREDICATE}
                    AND ($6::text IS NULL OR mf.media_kind = $6::text::media_kind)
                )
                OR EXISTS (
                  SELECT 1
                  FROM tracks t
                  JOIN albums al ON al.id = t.album_id
                  JOIN media_files mf ON mf.id = t.canonical_media_file_id
                  WHERE t.artist_id = ar.id
                    AND t.published_at IS NOT NULL
                    AND t.stable_grouping
                    AND al.published_at IS NOT NULL
                    AND al.stable_grouping
                    AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
                    AND ($3::integer IS NULL OR al.release_year = $3)
                    AND {GENRE_FILTER_PREDICATE}
                    AND {FORMAT_FILTER_PREDICATE}
                    AND ($6::text IS NULL OR mf.media_kind = $6::text::media_kind)
                )
              )
            ORDER BY
              {SEARCH_RANK_EXPRESSION} ASC,
              csp.normalized_display_title ASC,
              ar.id ASC
            LIMIT $7
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(input.normalized_query.as_str())
            .bind(input.tokens.clone())
            .bind(filters.year)
            .bind(filters.genre.as_deref())
            .bind(filters.format.as_deref())
            .bind(filters.media_type.map(media_kind_name))
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(artist_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(CatalogSearchError::from)
    }

    /// Searches resources for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `input`: `&CatalogSearchInput`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `filters`: `&CatalogSearchFilters`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `Vec<Album>` on success or `CatalogSearchError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `CatalogSearchError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn search_albums(
        &self,
        input: &CatalogSearchInput,
        filters: &CatalogSearchFilters,
        limit: u32,
    ) -> Result<Vec<Album>, CatalogSearchError> {
        let sql = format!(
            r#"
            SELECT
              al.id,
              al.artist_id,
              al.title,
              al.normalized_title,
              al.album_kind::text AS album_kind,
              al.release_year,
              al.stable_grouping,
              al.published_at,
              al.created_at,
              al.updated_at
            FROM catalog_search_projection csp
            JOIN albums al ON al.id = csp.entity_id
            JOIN artists ar ON ar.id = al.artist_id
            WHERE csp.entity_type = 'album'::catalog_entity_type
              AND csp.published
              AND al.published_at IS NOT NULL
              AND al.stable_grouping
              AND ar.published_at IS NOT NULL
              AND ar.stable_grouping
              AND {SEARCH_MATCH_PREDICATE}
              AND EXISTS (
                SELECT 1
                FROM tracks t
                JOIN media_files mf ON mf.id = t.canonical_media_file_id
                WHERE t.album_id = al.id
                  AND t.published_at IS NOT NULL
                  AND t.stable_grouping
                  AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
                  AND ($3::integer IS NULL OR al.release_year = $3)
                  AND {GENRE_FILTER_PREDICATE}
                  AND {FORMAT_FILTER_PREDICATE}
                  AND ($6::text IS NULL OR mf.media_kind = $6::text::media_kind)
              )
            ORDER BY
              {SEARCH_RANK_EXPRESSION} ASC,
              csp.normalized_display_title ASC,
              al.id ASC
            LIMIT $7
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(input.normalized_query.as_str())
            .bind(input.tokens.clone())
            .bind(filters.year)
            .bind(filters.genre.as_deref())
            .bind(filters.format.as_deref())
            .bind(filters.media_type.map(media_kind_name))
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(album_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(CatalogSearchError::from)
    }

    /// Searches resources for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `input`: `&CatalogSearchInput`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `filters`: `&CatalogSearchFilters`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `Vec<Track>` on success or `CatalogSearchError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `CatalogSearchError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn search_tracks(
        &self,
        input: &CatalogSearchInput,
        filters: &CatalogSearchFilters,
        limit: u32,
    ) -> Result<Vec<Track>, CatalogSearchError> {
        let sql = format!(
            r#"
            SELECT
              t.id,
              t.album_id,
              t.artist_id,
              t.title,
              t.normalized_title,
              t.disc_number,
              t.track_number,
              t.duration_seconds,
              t.stable_grouping,
              t.published_at,
              t.created_at,
              t.updated_at
            FROM catalog_search_projection csp
            JOIN tracks t ON t.id = csp.entity_id
            JOIN albums al ON al.id = t.album_id
            JOIN artists album_artist ON album_artist.id = al.artist_id
            JOIN artists track_artist ON track_artist.id = t.artist_id
            WHERE csp.entity_type = 'track'::catalog_entity_type
              AND csp.published
              AND t.published_at IS NOT NULL
              AND t.stable_grouping
              AND al.published_at IS NOT NULL
              AND al.stable_grouping
              AND album_artist.published_at IS NOT NULL
              AND album_artist.stable_grouping
              AND track_artist.published_at IS NOT NULL
              AND track_artist.stable_grouping
              AND {SEARCH_MATCH_PREDICATE}
              AND EXISTS (
                SELECT 1
                FROM media_files mf
                WHERE mf.id = t.canonical_media_file_id
                  AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
                  AND ($3::integer IS NULL OR al.release_year = $3)
                  AND {GENRE_FILTER_PREDICATE}
                  AND {FORMAT_FILTER_PREDICATE}
                  AND ($6::text IS NULL OR mf.media_kind = $6::text::media_kind)
              )
            ORDER BY
              {SEARCH_RANK_EXPRESSION} ASC,
              csp.normalized_display_title ASC,
              t.id ASC
            LIMIT $7
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(input.normalized_query.as_str())
            .bind(input.tokens.clone())
            .bind(filters.year)
            .bind(filters.genre.as_deref())
            .bind(filters.format.as_deref())
            .bind(filters.media_type.map(media_kind_name))
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(track_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(CatalogSearchError::from)
    }

    /// Searches resources for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `input`: `&CatalogSearchInput`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `filters`: `&CatalogSearchFilters`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `Vec<Podcast>` on success or `CatalogSearchError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `CatalogSearchError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn search_podcasts(
        &self,
        input: &CatalogSearchInput,
        filters: &CatalogSearchFilters,
        limit: u32,
    ) -> Result<Vec<Podcast>, CatalogSearchError> {
        let sql = format!(
            r#"
            SELECT {PODCAST_SELECT}
            FROM catalog_search_projection csp
            JOIN podcasts p ON p.id = csp.entity_id
            WHERE csp.entity_type = 'podcast'::catalog_entity_type
              AND csp.published
              AND p.published_at IS NOT NULL
              AND p.stable_grouping
              AND {SEARCH_MATCH_PREDICATE}
              AND EXISTS (
                SELECT 1
                FROM episodes e
                JOIN media_files mf ON mf.id = e.canonical_media_file_id
                WHERE e.podcast_id = p.id
                  AND e.published_at IS NOT NULL
                  AND e.stable_grouping
                  AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
                  AND $3::integer IS NULL
                  AND {GENRE_FILTER_PREDICATE}
                  AND {FORMAT_FILTER_PREDICATE}
                  AND ($6::text IS NULL OR mf.media_kind = $6::text::media_kind)
              )
            ORDER BY
              {SEARCH_RANK_EXPRESSION} ASC,
              csp.normalized_display_title ASC,
              p.id ASC
            LIMIT $7
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(input.normalized_query.as_str())
            .bind(input.tokens.clone())
            .bind(filters.year)
            .bind(filters.genre.as_deref())
            .bind(filters.format.as_deref())
            .bind(filters.media_type.map(media_kind_name))
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(podcast_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(CatalogSearchError::from)
    }

    /// Searches resources for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `input`: `&CatalogSearchInput`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `filters`: `&CatalogSearchFilters`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `Vec<Episode>` on success or `CatalogSearchError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `CatalogSearchError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn search_episodes(
        &self,
        input: &CatalogSearchInput,
        filters: &CatalogSearchFilters,
        limit: u32,
    ) -> Result<Vec<Episode>, CatalogSearchError> {
        let sql = format!(
            r#"
            SELECT
              e.id,
              e.podcast_id,
              e.title,
              e.normalized_title,
              e.season_number,
              e.episode_number,
              e.duration_seconds,
              e.stable_grouping,
              e.published_at,
              e.created_at,
              e.updated_at
            FROM catalog_search_projection csp
            JOIN episodes e ON e.id = csp.entity_id
            JOIN podcasts p ON p.id = e.podcast_id
            JOIN media_files mf ON mf.id = e.canonical_media_file_id
              AND mf.episode_id = e.id
            WHERE csp.entity_type = 'episode'::catalog_entity_type
              AND csp.published
              AND e.published_at IS NOT NULL
              AND e.stable_grouping
              AND p.published_at IS NOT NULL
              AND p.stable_grouping
              AND {SEARCH_MATCH_PREDICATE}
              AND {BROWSE_VISIBLE_MEDIA_FILE_PREDICATE}
              AND $3::integer IS NULL
              AND {GENRE_FILTER_PREDICATE}
              AND {FORMAT_FILTER_PREDICATE}
              AND ($6::text IS NULL OR mf.media_kind = $6::text::media_kind)
            ORDER BY
              {SEARCH_RANK_EXPRESSION} ASC,
              csp.normalized_display_title ASC,
              e.id ASC
            LIMIT $7
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(input.normalized_query.as_str())
            .bind(input.tokens.clone())
            .bind(filters.year)
            .bind(filters.genre.as_deref())
            .bind(filters.format.as_deref())
            .bind(filters.media_type.map(media_kind_name))
            .bind(i64::from(limit))
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(episode_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(CatalogSearchError::from)
    }

    /// Searches resources for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `input`: `&CatalogSearchInput`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `filters`: `&CatalogSearchFilters`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `Vec<Playlist>` on success or `CatalogSearchError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `CatalogSearchError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn search_playlists(
        &self,
        account_id: Uuid,
        input: &CatalogSearchInput,
        filters: &CatalogSearchFilters,
        limit: u32,
    ) -> Result<Vec<Playlist>, CatalogSearchError> {
        if filters.has_media_filter() {
            return Ok(Vec::new());
        }

        let sql = format!(
            r#"
            SELECT {PLAYLIST_SELECT}
            FROM catalog_search_projection csp
            JOIN playlists p ON p.id = csp.entity_id
            WHERE csp.entity_type = 'playlist'::catalog_entity_type
              AND csp.published
              AND (p.scope = 'shared' OR p.owner_account_id = $4)
              AND {SEARCH_MATCH_PREDICATE}
            ORDER BY
              {SEARCH_RANK_EXPRESSION} ASC,
              csp.normalized_display_title ASC,
              p.id ASC
            LIMIT $3
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(input.normalized_query.as_str())
            .bind(input.tokens.clone())
            .bind(i64::from(limit))
            .bind(account_id)
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(playlist_from_row)
            .collect::<Result<Vec<_>, _>>()
            .map_err(CatalogSearchError::from)
    }

    /// Handles publish music file for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `request`: `&CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `grouping`: `&MusicCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `CatalogImportOutcome` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn publish_music_file(
        &self,
        request: &CatalogImportRequest,
        grouping: &MusicCatalogGrouping,
    ) -> Result<CatalogImportOutcome, StorageError> {
        let mut transaction = self.pool.begin().await?;
        let existing = media_file_by_source_path_in_transaction(
            &mut transaction,
            &request.source_path,
        )
        .await?;
        let reused_existing = existing
            .as_ref()
            .map(|existing| {
                existing.status == MediaFileStatus::Published
                    && existing.file_hash == request.probe.file_hash
            })
            .unwrap_or(false);

        let album_artist = upsert_artist_in_transaction(
            &mut transaction,
            &grouping.album_artist,
            true,
        )
        .await?;
        let track_artist = upsert_artist_in_transaction(
            &mut transaction,
            &grouping.track_artist,
            true,
        )
        .await?;
        let album = upsert_album_in_transaction(
            &mut transaction,
            album_artist.id,
            &grouping.album_title,
            grouping.album_kind,
            grouping.release_year,
            true,
        )
        .await?;
        let track = upsert_track_in_transaction(
            &mut transaction,
            album.id,
            track_artist.id,
            &grouping.track_title,
            grouping.disc_number,
            grouping.track_number,
            request.probe.duration_seconds,
            true,
        )
        .await?;
        let media_file = upsert_media_file_in_transaction(
            &mut transaction,
            request,
            MediaFileStatus::Published,
            Some(track.id),
            None,
            None,
        )
        .await?;

        persist_metadata_in_transaction(
            &mut transaction,
            request,
            &CatalogEntityIds {
                album_artist_id: Some(album_artist.id),
                track_artist_id: Some(track_artist.id),
                album_id: Some(album.id),
                track_id: Some(track.id),
                podcast_id: None,
                episode_id: None,
                media_file_id: media_file.id,
            },
        )
        .await?;
        if request.rebuild_search_projections {
            upsert_music_search_projections_in_transaction(
                &mut transaction,
                &album_artist,
                &track_artist,
                &album,
                &track,
            )
            .await?;
        }

        transaction.commit().await?;

        Ok(CatalogImportOutcome {
            decision: if reused_existing {
                CatalogImportDecision::ReusedExisting
            } else {
                CatalogImportDecision::Published
            },
            media_file,
            artist: Some(track_artist),
            album: Some(album),
            track: Some(track),
            podcast: None,
            episode: None,
            duplicate_of: None,
            quarantine_item: None,
        })
    }

    /// Handles publish podcast file for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `request`: `&CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `grouping`: `&PodcastCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `CatalogImportOutcome` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn publish_podcast_file(
        &self,
        request: &CatalogImportRequest,
        grouping: &PodcastCatalogGrouping,
    ) -> Result<CatalogImportOutcome, StorageError> {
        let mut transaction = self.pool.begin().await?;
        let existing = media_file_by_source_path_in_transaction(
            &mut transaction,
            &request.source_path,
        )
        .await?;
        let reused_existing = existing
            .as_ref()
            .map(|existing| {
                existing.status == MediaFileStatus::Published
                    && existing.file_hash == request.probe.file_hash
            })
            .unwrap_or(false);

        let podcast = upsert_podcast_in_transaction(
            &mut transaction,
            &grouping.podcast_title,
            true,
        )
        .await?;
        let episode = upsert_episode_in_transaction(
            &mut transaction,
            podcast.id,
            &grouping.episode_title,
            grouping.season_number,
            grouping.episode_number,
            request.probe.duration_seconds,
            true,
        )
        .await?;
        let media_file = upsert_media_file_in_transaction(
            &mut transaction,
            request,
            MediaFileStatus::Published,
            None,
            Some(episode.id),
            None,
        )
        .await?;

        persist_metadata_in_transaction(
            &mut transaction,
            request,
            &CatalogEntityIds {
                album_artist_id: None,
                track_artist_id: None,
                album_id: None,
                track_id: None,
                podcast_id: Some(podcast.id),
                episode_id: Some(episode.id),
                media_file_id: media_file.id,
            },
        )
        .await?;
        if request.rebuild_search_projections {
            upsert_podcast_search_projections_in_transaction(
                &mut transaction,
                &podcast,
                &episode,
            )
            .await?;
        }

        transaction.commit().await?;

        Ok(CatalogImportOutcome {
            decision: if reused_existing {
                CatalogImportDecision::ReusedExisting
            } else {
                CatalogImportDecision::Published
            },
            media_file,
            artist: None,
            album: None,
            track: None,
            podcast: Some(podcast),
            episode: Some(episode),
            duplicate_of: None,
            quarantine_item: None,
        })
    }

    /// Handles quarantine catalog file for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `request`: `&CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `reason`: `QuarantineReason`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `decision`: `CatalogImportDecision`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `duplicate_of`: `Option<MediaFile>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `retry_eligible`: `bool`; expected to be a boolean flag controlling the documented branch.
    /// - `admin_note`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `CatalogImportOutcome` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn quarantine_catalog_file(
        &self,
        request: &CatalogImportRequest,
        reason: QuarantineReason,
        decision: CatalogImportDecision,
        duplicate_of: Option<MediaFile>,
        retry_eligible: bool,
        admin_note: Option<&str>,
    ) -> Result<CatalogImportOutcome, StorageError> {
        let mut transaction = self.pool.begin().await?;
        let status = match reason {
            QuarantineReason::Duplicate => MediaFileStatus::Duplicate,
            QuarantineReason::FileError => MediaFileStatus::Failed,
            _ => MediaFileStatus::Quarantined,
        };
        let media_file = upsert_media_file_in_transaction(
            &mut transaction,
            request,
            status,
            None,
            None,
            duplicate_of.as_ref().map(|media_file| media_file.id),
        )
        .await?;
        let quarantine_item = upsert_quarantine_item_in_transaction(
            &mut transaction,
            media_file.id,
            request.import_job_id,
            &request.source_path,
            reason,
            retry_eligible,
            admin_note,
        )
        .await?;

        persist_metadata_in_transaction(
            &mut transaction,
            request,
            &CatalogEntityIds {
                album_artist_id: None,
                track_artist_id: None,
                album_id: None,
                track_id: None,
                podcast_id: None,
                episode_id: None,
                media_file_id: media_file.id,
            },
        )
        .await?;

        transaction.commit().await?;

        Ok(CatalogImportOutcome {
            decision,
            media_file,
            artist: None,
            album: None,
            track: None,
            podcast: None,
            episode: None,
            duplicate_of,
            quarantine_item: Some(quarantine_item),
        })
    }

    /// Handles find similar music media file for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `request`: `&CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `grouping`: `&MusicCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Option<MediaFile>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn find_similar_music_media_file(
        &self,
        request: &CatalogImportRequest,
        grouping: &MusicCatalogGrouping,
    ) -> Result<Option<MediaFile>, StorageError> {
        let normalized_artist = normalize_catalog_text(&grouping.track_artist);
        let normalized_album = normalize_catalog_text(&grouping.album_title);
        let normalized_track = normalize_catalog_text(&grouping.track_title);
        let sql = format!(
            r#"
            SELECT {MEDIA_FILE_SELECT_MF}
            FROM media_files mf
            JOIN tracks t ON t.id = mf.track_id
            JOIN albums al ON al.id = t.album_id
            JOIN artists ar ON ar.id = t.artist_id
            WHERE mf.status = 'published'
              AND mf.source_path <> $1
              AND (mf.managed_path IS NULL OR mf.managed_path <> $1)
              AND ar.normalized_name = $2
              AND al.normalized_title = $3
              AND t.normalized_title = $4
              AND (
                $5::integer IS NULL
                OR mf.duration_seconds IS NULL
                OR abs(mf.duration_seconds - $5::integer) <= 3
              )
            ORDER BY mf.published_at ASC NULLS LAST, mf.discovered_at ASC, mf.id ASC
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(&request.source_path)
            .bind(normalized_artist)
            .bind(normalized_album)
            .bind(normalized_track)
            .bind(request.probe.duration_seconds)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(media_file_from_row).transpose()
    }

    /// Handles find similar podcast media file for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `request`: `&CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `grouping`: `&PodcastCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Option<MediaFile>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn find_similar_podcast_media_file(
        &self,
        request: &CatalogImportRequest,
        grouping: &PodcastCatalogGrouping,
    ) -> Result<Option<MediaFile>, StorageError> {
        let normalized_podcast = normalize_catalog_text(&grouping.podcast_title);
        let normalized_episode = normalize_catalog_text(&grouping.episode_title);
        let sql = format!(
            r#"
            SELECT {MEDIA_FILE_SELECT_MF}
            FROM media_files mf
            JOIN episodes e ON e.id = mf.episode_id
            JOIN podcasts p ON p.id = e.podcast_id
            WHERE mf.status = 'published'
              AND mf.source_path <> $1
              AND (mf.managed_path IS NULL OR mf.managed_path <> $1)
              AND p.normalized_title = $2
              AND e.normalized_title = $3
              AND (
                $4::integer IS NULL
                OR mf.duration_seconds IS NULL
                OR abs(mf.duration_seconds - $4::integer) <= 3
              )
              AND (
                ($5::integer IS NULL AND $6::integer IS NULL)
                OR (
                  e.season_number IS NOT DISTINCT FROM $5::integer
                  AND e.episode_number IS NOT DISTINCT FROM $6::integer
                )
              )
            ORDER BY mf.published_at ASC NULLS LAST, mf.discovered_at ASC, mf.id ASC
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(&request.source_path)
            .bind(normalized_podcast)
            .bind(normalized_episode)
            .bind(request.probe.duration_seconds)
            .bind(grouping.season_number)
            .bind(grouping.episode_number)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(media_file_from_row).transpose()
    }
}

/// Handles backfill playlist search projections in transaction for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn backfill_playlist_search_projections_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), StorageError> {
    let sql = format!("SELECT {PLAYLIST_SELECT} FROM playlists ORDER BY id ASC");
    let rows = sqlx::query(&sql).fetch_all(&mut **transaction).await?;

    for row in rows {
        let playlist = playlist_from_row(&row)?;
        upsert_playlist_search_projection_in_transaction(transaction, &playlist).await?;
    }

    Ok(())
}

/// Handles backfill media file filter keys in transaction for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn backfill_media_file_filter_keys_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), StorageError> {
    let provenance_rows = sqlx::query(
        r#"
        SELECT source_path, field_name, value
        FROM metadata_provenance
        WHERE source_path IS NOT NULL
        ORDER BY source_path ASC, created_at ASC, id ASC
        "#,
    )
    .fetch_all(&mut **transaction)
    .await?;
    let mut genres_by_source_path: HashMap<String, BTreeSet<String>> = HashMap::new();
    for row in provenance_rows {
        let field_name = normalize_catalog_text(&row.try_get::<String, _>("field_name")?);
        if !matches!(field_name.as_str(), "genre" | "genres") {
            continue;
        }

        let source_path: String = row.try_get("source_path")?;
        let value = row.try_get::<Json<Value>, _>("value")?.0;
        collect_json_filter_keys(
            &value,
            genres_by_source_path.entry(source_path).or_default(),
        );
    }

    let media_rows = sqlx::query(
        r#"
        SELECT
          id,
          source_path,
          mime_type,
          container,
          audio_codec,
          genres,
          format_keys
        FROM media_files
        ORDER BY id ASC
        "#,
    )
    .fetch_all(&mut **transaction)
    .await?;

    for row in media_rows {
        let source_path: String = row.try_get("source_path")?;
        let genres = genres_by_source_path
            .remove(&source_path)
            .map(|keys| keys.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let mime_type: Option<String> = row.try_get("mime_type")?;
        let container: Option<String> = row.try_get("container")?;
        let audio_codec: Option<String> = row.try_get("audio_codec")?;
        let format_keys = format_keys_for_probe_values(
            mime_type.as_deref(),
            container.as_deref(),
            audio_codec.as_deref(),
        );
        let current_genres: Vec<String> = row.try_get("genres")?;
        let current_format_keys: Vec<String> = row.try_get("format_keys")?;
        if current_genres == genres && current_format_keys == format_keys {
            continue;
        }

        sqlx::query(
            r#"
            UPDATE media_files
            SET genres = $2,
                format_keys = $3
            WHERE id = $1
            "#,
        )
        .bind(row.try_get::<Uuid, _>("id")?)
        .bind(genres)
        .bind(format_keys)
        .execute(&mut **transaction)
        .await?;
    }

    Ok(())
}

/// Represents catalog entity ids in the catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Functionality: Carries fields `album_artist_id`, `track_artist_id`, `album_id`, `track_id`, `podcast_id`, `episode_id`, `media_file_id` for catalog persistence, browsing, search, import upsert, and normalization logic.
/// Dependencies: depends on `Option<Uuid>`, `Option<Uuid>`, `Option<Uuid>`, `Option<Uuid>`, `Option<Uuid>`, `Option<Uuid>`, and 1 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/catalog.rs`.
struct CatalogEntityIds {
    album_artist_id: Option<Uuid>,
    track_artist_id: Option<Uuid>,
    album_id: Option<Uuid>,
    track_id: Option<Uuid>,
    podcast_id: Option<Uuid>,
    episode_id: Option<Uuid>,
    media_file_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "sort", rename_all = "snake_case")]
/// Represents browse cursor in the catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Functionality: Enumerates `Name` states or choices for catalog persistence, browsing, search, import upsert, and normalization logic.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/catalog.rs`.
enum BrowseCursor {
    Name {
        key: String,
        name_key: String,
        id: Uuid,
    },
    ArtistTitle {
        artist_key: String,
        title_key: String,
        id: Uuid,
    },
    AlbumPosition {
        album_artist_key: String,
        album_title_key: String,
        disc_key: i32,
        track_key: i32,
        title_key: String,
        id: Uuid,
    },
    Title {
        title_key: String,
        id: Uuid,
    },
    PodcastPosition {
        podcast_key: String,
        season_key: i32,
        episode_key: i32,
        title_key: String,
        id: Uuid,
    },
}

impl BrowseCursor {
    /// Handles sort for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `CatalogBrowseSort` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn sort(&self) -> CatalogBrowseSort {
        match self {
            BrowseCursor::Name { .. } => CatalogBrowseSort::ArtistName,
            BrowseCursor::ArtistTitle { .. } => CatalogBrowseSort::AlbumArtistTitle,
            BrowseCursor::AlbumPosition { .. } => CatalogBrowseSort::TrackAlbumPosition,
            BrowseCursor::Title { .. } => CatalogBrowseSort::PodcastTitle,
            BrowseCursor::PodcastPosition { .. } => CatalogBrowseSort::EpisodePodcastPosition,
        }
    }
}

/// Handles decode cursor for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `cursor`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `expected_sort`: `CatalogBrowseSort`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Option<BrowseCursor>` on success or `CatalogBrowseError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn decode_cursor(
    cursor: Option<&str>,
    expected_sort: CatalogBrowseSort,
) -> Result<Option<BrowseCursor>, CatalogBrowseError> {
    let Some(cursor) = cursor.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| CatalogBrowseError::InvalidCursor)?;
    let cursor = serde_json::from_slice::<BrowseCursor>(&bytes)
        .map_err(|_| CatalogBrowseError::InvalidCursor)?;
    if cursor.sort() != expected_sort {
        return Err(CatalogBrowseError::CursorSortMismatch {
            expected: expected_sort.api_name().to_string(),
            actual: cursor.sort().api_name().to_string(),
        });
    }

    Ok(Some(cursor))
}

/// Handles encode cursor for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `cursor`: `BrowseCursor`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `String` on success or `CatalogBrowseError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn encode_cursor(cursor: BrowseCursor) -> Result<String, CatalogBrowseError> {
    let bytes = serde_json::to_vec(&cursor).map_err(|_| CatalogBrowseError::InvalidCursor)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

/// Handles limit plus one for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `i64` on success or `CatalogBrowseError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `CatalogBrowseError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn limit_plus_one(limit: u32) -> Result<i64, CatalogBrowseError> {
    let limit = limit
        .checked_add(1)
        .ok_or(CatalogBrowseError::InvalidLimit)?;
    Ok(i64::from(limit))
}

/// Handles truncate to limit for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `rows`: `&mut Vec<PgRow>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn truncate_to_limit(rows: &mut Vec<PgRow>, limit: u32) -> bool {
    let limit = limit as usize;
    if rows.len() > limit {
        rows.truncate(limit);
        true
    } else {
        false
    }
}

/// Handles artist cursor from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `BrowseCursor` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn artist_cursor_from_row(row: &PgRow) -> Result<BrowseCursor, StorageError> {
    Ok(BrowseCursor::Name {
        key: row.try_get("browse_sort_key")?,
        name_key: row.try_get("browse_name_key")?,
        id: row.try_get("id")?,
    })
}

/// Handles album cursor from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `BrowseCursor` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn album_cursor_from_row(row: &PgRow) -> Result<BrowseCursor, StorageError> {
    Ok(BrowseCursor::ArtistTitle {
        artist_key: row.try_get("browse_artist_key")?,
        title_key: row.try_get("browse_title_key")?,
        id: row.try_get("id")?,
    })
}

/// Handles track cursor from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `BrowseCursor` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn track_cursor_from_row(row: &PgRow) -> Result<BrowseCursor, StorageError> {
    Ok(BrowseCursor::AlbumPosition {
        album_artist_key: row.try_get("browse_album_artist_key")?,
        album_title_key: row.try_get("browse_album_title_key")?,
        disc_key: row.try_get("browse_disc_key")?,
        track_key: row.try_get("browse_track_key")?,
        title_key: row.try_get("browse_title_key")?,
        id: row.try_get("id")?,
    })
}

/// Handles podcast cursor from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `BrowseCursor` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn podcast_cursor_from_row(row: &PgRow) -> Result<BrowseCursor, StorageError> {
    Ok(BrowseCursor::Title {
        title_key: row.try_get("browse_title_key")?,
        id: row.try_get("id")?,
    })
}

/// Handles episode cursor from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `BrowseCursor` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn episode_cursor_from_row(row: &PgRow) -> Result<BrowseCursor, StorageError> {
    Ok(BrowseCursor::PodcastPosition {
        podcast_key: row.try_get("browse_podcast_key")?,
        season_key: row.try_get("browse_season_key")?,
        episode_key: row.try_get("browse_episode_key")?,
        title_key: row.try_get("browse_title_key")?,
        id: row.try_get("id")?,
    })
}

/// Handles reused existing outcome for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `media_file`: `MediaFile`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `CatalogImportOutcome` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn reused_existing_outcome(media_file: MediaFile) -> CatalogImportOutcome {
    CatalogImportOutcome {
        decision: CatalogImportDecision::ReusedExisting,
        media_file,
        artist: None,
        album: None,
        track: None,
        podcast: None,
        episode: None,
        duplicate_of: None,
        quarantine_item: None,
    }
}

/// Handles media file by source path in transaction for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `source_path`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Option<MediaFile>` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn media_file_by_source_path_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    source_path: &str,
) -> Result<Option<MediaFile>, StorageError> {
    let sql = format!(
        "SELECT {MEDIA_FILE_SELECT} FROM media_files WHERE source_path = $1 LIMIT 1"
    );
    let row = sqlx::query(&sql)
        .bind(source_path)
        .fetch_optional(&mut **transaction)
        .await?;
    row.as_ref().map(media_file_from_row).transpose()
}

/// Inserts or updates data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `name`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `stable_grouping`: `bool`; expected to be a boolean flag controlling the documented branch.
///
/// Output:
/// - Returns `Artist` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn upsert_artist_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    name: &str,
    stable_grouping: bool,
) -> Result<Artist, StorageError> {
    let now = Utc::now();
    let normalized_name = normalize_catalog_text(name);
    let sort_name = sort_name(name);
    let sql = format!(
        r#"
        INSERT INTO artists (
            id,
            name,
            normalized_name,
            sort_name,
            stable_grouping,
            published_at,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, CASE WHEN $5 THEN $6 ELSE NULL END, $6, $6)
        ON CONFLICT (normalized_name) DO UPDATE SET
            name = EXCLUDED.name,
            sort_name = EXCLUDED.sort_name,
            stable_grouping = artists.stable_grouping OR EXCLUDED.stable_grouping,
            published_at = COALESCE(artists.published_at, EXCLUDED.published_at),
            updated_at = EXCLUDED.updated_at
        RETURNING {ARTIST_SELECT}
        "#
    );
    let row = sqlx::query(&sql)
        .bind(Uuid::new_v4())
        .bind(name.trim())
        .bind(normalized_name)
        .bind(sort_name)
        .bind(stable_grouping)
        .bind(now)
        .fetch_one(&mut **transaction)
        .await?;
    artist_from_row(&row)
}

/// Inserts or updates data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `artist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `title`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `album_kind`: `AlbumKind`; expected to be a value satisfying the type contract shown in the function signature.
/// - `release_year`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
/// - `stable_grouping`: `bool`; expected to be a boolean flag controlling the documented branch.
///
/// Output:
/// - Returns `Album` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn upsert_album_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    artist_id: Uuid,
    title: &str,
    album_kind: AlbumKind,
    release_year: Option<i32>,
    stable_grouping: bool,
) -> Result<Album, StorageError> {
    let now = Utc::now();
    let normalized_title = normalize_catalog_text(title);
    let sql = format!(
        r#"
        INSERT INTO albums (
            id,
            artist_id,
            title,
            normalized_title,
            album_kind,
            release_year,
            stable_grouping,
            published_at,
            created_at,
            updated_at
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5::text::album_kind,
            $6,
            $7,
            CASE WHEN $7 THEN $8 ELSE NULL END,
            $8,
            $8
        )
        ON CONFLICT (artist_id, normalized_title) DO UPDATE SET
            title = EXCLUDED.title,
            album_kind = CASE
                WHEN albums.album_kind = 'unknown' THEN EXCLUDED.album_kind
                ELSE albums.album_kind
            END,
            release_year = COALESCE(albums.release_year, EXCLUDED.release_year),
            stable_grouping = albums.stable_grouping OR EXCLUDED.stable_grouping,
            published_at = COALESCE(albums.published_at, EXCLUDED.published_at),
            updated_at = EXCLUDED.updated_at
        RETURNING {ALBUM_SELECT}
        "#
    );
    let row = sqlx::query(&sql)
        .bind(Uuid::new_v4())
        .bind(artist_id)
        .bind(title.trim())
        .bind(normalized_title)
        .bind(album_kind_name(album_kind))
        .bind(release_year)
        .bind(stable_grouping)
        .bind(now)
        .fetch_one(&mut **transaction)
        .await?;
    album_from_row(&row)
}

/// Inserts or updates data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `album_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `artist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `title`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `disc_number`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
/// - `track_number`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
/// - `duration_seconds`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
/// - `stable_grouping`: `bool`; expected to be a boolean flag controlling the documented branch.
///
/// Output:
/// - Returns `Track` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn upsert_track_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    album_id: Uuid,
    artist_id: Uuid,
    title: &str,
    disc_number: Option<i32>,
    track_number: Option<i32>,
    duration_seconds: Option<i32>,
    stable_grouping: bool,
) -> Result<Track, StorageError> {
    let now = Utc::now();
    let normalized_title = normalize_catalog_text(title);
    let sql = format!(
        r#"
        INSERT INTO tracks (
            id,
            album_id,
            artist_id,
            title,
            normalized_title,
            disc_number,
            track_number,
            duration_seconds,
            stable_grouping,
            published_at,
            created_at,
            updated_at
        )
        VALUES (
            $1,
            $2,
            $3,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            CASE WHEN $9 THEN $10 ELSE NULL END,
            $10,
            $10
        )
        ON CONFLICT (
            album_id,
            (COALESCE(disc_number, 0)),
            (COALESCE(track_number, 0)),
            normalized_title
        ) DO UPDATE SET
            artist_id = EXCLUDED.artist_id,
            title = EXCLUDED.title,
            duration_seconds = COALESCE(tracks.duration_seconds, EXCLUDED.duration_seconds),
            stable_grouping = tracks.stable_grouping OR EXCLUDED.stable_grouping,
            published_at = COALESCE(tracks.published_at, EXCLUDED.published_at),
            updated_at = EXCLUDED.updated_at
        RETURNING {TRACK_SELECT}
        "#
    );
    let row = sqlx::query(&sql)
        .bind(Uuid::new_v4())
        .bind(album_id)
        .bind(artist_id)
        .bind(title.trim())
        .bind(normalized_title)
        .bind(disc_number)
        .bind(track_number)
        .bind(duration_seconds)
        .bind(stable_grouping)
        .bind(now)
        .fetch_one(&mut **transaction)
        .await?;
    track_from_row(&row)
}

/// Inserts or updates data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `title`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `stable_grouping`: `bool`; expected to be a boolean flag controlling the documented branch.
///
/// Output:
/// - Returns `Podcast` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn upsert_podcast_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    title: &str,
    stable_grouping: bool,
) -> Result<Podcast, StorageError> {
    let now = Utc::now();
    let normalized_title = normalize_catalog_text(title);
    let sql = format!(
        r#"
        INSERT INTO podcasts (
            id,
            title,
            normalized_title,
            stable_grouping,
            published_at,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, CASE WHEN $4 THEN $5 ELSE NULL END, $5, $5)
        ON CONFLICT (normalized_title) DO UPDATE SET
            title = EXCLUDED.title,
            stable_grouping = podcasts.stable_grouping OR EXCLUDED.stable_grouping,
            published_at = COALESCE(podcasts.published_at, EXCLUDED.published_at),
            updated_at = EXCLUDED.updated_at
        RETURNING {PODCAST_SELECT}
        "#
    );
    let row = sqlx::query(&sql)
        .bind(Uuid::new_v4())
        .bind(title.trim())
        .bind(normalized_title)
        .bind(stable_grouping)
        .bind(now)
        .fetch_one(&mut **transaction)
        .await?;
    podcast_from_row(&row)
}

/// Inserts or updates data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `podcast_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `title`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `season_number`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
/// - `episode_number`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
/// - `duration_seconds`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
/// - `stable_grouping`: `bool`; expected to be a boolean flag controlling the documented branch.
///
/// Output:
/// - Returns `Episode` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn upsert_episode_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    podcast_id: Uuid,
    title: &str,
    season_number: Option<i32>,
    episode_number: Option<i32>,
    duration_seconds: Option<i32>,
    stable_grouping: bool,
) -> Result<Episode, StorageError> {
    let now = Utc::now();
    let normalized_title = normalize_catalog_text(title);
    let sql = format!(
        r#"
        INSERT INTO episodes (
            id,
            podcast_id,
            title,
            normalized_title,
            season_number,
            episode_number,
            duration_seconds,
            stable_grouping,
            published_at,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CASE WHEN $8 THEN $9 ELSE NULL END, $9, $9)
        ON CONFLICT (
            podcast_id,
            (COALESCE(season_number, 0)),
            (COALESCE(episode_number, 0)),
            normalized_title
        ) DO UPDATE SET
            title = EXCLUDED.title,
            duration_seconds = COALESCE(episodes.duration_seconds, EXCLUDED.duration_seconds),
            stable_grouping = episodes.stable_grouping OR EXCLUDED.stable_grouping,
            published_at = COALESCE(episodes.published_at, EXCLUDED.published_at),
            updated_at = EXCLUDED.updated_at
        RETURNING {EPISODE_SELECT}
        "#
    );
    let row = sqlx::query(&sql)
        .bind(Uuid::new_v4())
        .bind(podcast_id)
        .bind(title.trim())
        .bind(normalized_title)
        .bind(season_number)
        .bind(episode_number)
        .bind(duration_seconds)
        .bind(stable_grouping)
        .bind(now)
        .fetch_one(&mut **transaction)
        .await?;
    episode_from_row(&row)
}

/// Inserts or updates data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `request`: `&CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
/// - `status`: `MediaFileStatus`; expected to be a media domain value that has already passed upstream validation.
/// - `track_id`: `Option<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `episode_id`: `Option<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `duplicate_of_media_file_id`: `Option<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `MediaFile` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn upsert_media_file_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &CatalogImportRequest,
    status: MediaFileStatus,
    track_id: Option<Uuid>,
    episode_id: Option<Uuid>,
    duplicate_of_media_file_id: Option<Uuid>,
) -> Result<MediaFile, StorageError> {
    let now = Utc::now();
    let media_kind = request.grouping.media_kind();
    let published_at = if status == MediaFileStatus::Published {
        Some(now)
    } else {
        None
    };
    let managed_path = if status == MediaFileStatus::Published {
        request.managed_path.as_deref()
    } else {
        None
    };
    if status == MediaFileStatus::Published {
        if let Some(existing) =
            media_file_by_source_path_in_transaction(transaction, &request.source_path).await?
        {
            clear_reassigned_canonical_media_file_in_transaction(
                transaction,
                &existing,
                track_id,
                episode_id,
            )
            .await?;
        }
    }
    let genres = genre_keys_for_request(request);
    let format_keys = format_keys_for_probe(&request.probe);
    let sql = format!(
        r#"
        INSERT INTO media_files (
            id,
            media_kind,
            status,
            source_path,
            managed_path,
            file_hash,
            file_size,
            mime_type,
            container,
            audio_codec,
            duration_seconds,
            bitrate,
            sample_rate,
            channels,
            genres,
            format_keys,
            track_id,
            episode_id,
            duplicate_of_media_file_id,
            import_job_id,
            discovered_at,
            published_at,
            updated_at
        )
        VALUES (
            $1,
            $2::text::media_kind,
            $3::text::media_file_status,
            $4,
            $5,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14,
            $15,
            $16,
            $17,
            $18,
            $19,
            $20,
            $21,
            $22,
            $21
        )
        ON CONFLICT (source_path) DO UPDATE SET
            media_kind = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.media_kind
                ELSE EXCLUDED.media_kind
            END,
            status = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.status
                ELSE EXCLUDED.status
            END,
            managed_path = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.managed_path
                ELSE EXCLUDED.managed_path
            END,
            file_hash = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.file_hash
                ELSE EXCLUDED.file_hash
            END,
            file_size = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.file_size
                ELSE EXCLUDED.file_size
            END,
            mime_type = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.mime_type
                ELSE EXCLUDED.mime_type
            END,
            container = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.container
                ELSE EXCLUDED.container
            END,
            audio_codec = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.audio_codec
                ELSE EXCLUDED.audio_codec
            END,
            duration_seconds = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.duration_seconds
                ELSE EXCLUDED.duration_seconds
            END,
            bitrate = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.bitrate
                ELSE EXCLUDED.bitrate
            END,
            sample_rate = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.sample_rate
                ELSE EXCLUDED.sample_rate
            END,
            channels = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.channels
                ELSE EXCLUDED.channels
            END,
            genres = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.genres
                ELSE EXCLUDED.genres
            END,
            format_keys = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.format_keys
                ELSE EXCLUDED.format_keys
            END,
            track_id = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.track_id
                ELSE EXCLUDED.track_id
            END,
            episode_id = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.episode_id
                ELSE EXCLUDED.episode_id
            END,
            duplicate_of_media_file_id = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.duplicate_of_media_file_id
                ELSE EXCLUDED.duplicate_of_media_file_id
            END,
            import_job_id = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.import_job_id
                ELSE EXCLUDED.import_job_id
            END,
            published_at = CASE
                WHEN media_files.status = 'published' AND EXCLUDED.status <> 'published'
                    THEN media_files.published_at
                ELSE COALESCE(media_files.published_at, EXCLUDED.published_at)
            END,
            updated_at = EXCLUDED.updated_at
        RETURNING {MEDIA_FILE_SELECT}
        "#
    );
    let row = sqlx::query(&sql)
        .bind(Uuid::new_v4())
        .bind(media_kind_name(media_kind))
        .bind(media_file_status_name(status))
        .bind(request.source_path.as_str())
        .bind(managed_path)
        .bind(request.probe.file_hash.as_str())
        .bind(request.probe.file_size)
        .bind(request.probe.mime_type.as_deref())
        .bind(request.probe.container.as_deref())
        .bind(request.probe.audio_codec.as_deref())
        .bind(request.probe.duration_seconds)
        .bind(request.probe.bitrate)
        .bind(request.probe.sample_rate)
        .bind(request.probe.channels)
        .bind(genres)
        .bind(format_keys)
        .bind(track_id)
        .bind(episode_id)
        .bind(duplicate_of_media_file_id)
        .bind(request.import_job_id)
        .bind(now)
        .bind(published_at)
        .fetch_one(&mut **transaction)
        .await?;
    let media_file = media_file_from_row(&row)?;
    if media_file.status == MediaFileStatus::Published {
        if let Some(track_id) = media_file.track_id {
            set_track_canonical_media_file_in_transaction(
                transaction,
                track_id,
                media_file.id,
            )
            .await?;
        }
        if let Some(episode_id) = media_file.episode_id {
            set_episode_canonical_media_file_in_transaction(
                transaction,
                episode_id,
                media_file.id,
            )
            .await?;
        }
    }
    Ok(media_file)
}

/// Clears stored state for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `existing`: `&MediaFile`; expected to be a media domain value that has already passed upstream validation.
/// - `next_track_id`: `Option<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `next_episode_id`: `Option<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn clear_reassigned_canonical_media_file_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    existing: &MediaFile,
    next_track_id: Option<Uuid>,
    next_episode_id: Option<Uuid>,
) -> Result<(), StorageError> {
    if existing.track_id.is_some() && existing.track_id != next_track_id {
        sqlx::query(
            r#"
            UPDATE tracks
            SET canonical_media_file_id = NULL
            WHERE canonical_media_file_id = $1
            "#,
        )
        .bind(existing.id)
        .execute(&mut **transaction)
        .await?;
    }

    if existing.episode_id.is_some() && existing.episode_id != next_episode_id {
        sqlx::query(
            r#"
            UPDATE episodes
            SET canonical_media_file_id = NULL
            WHERE canonical_media_file_id = $1
            "#,
        )
        .bind(existing.id)
        .execute(&mut **transaction)
        .await?;
    }

    Ok(())
}

/// Sets stored state for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `track_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `media_file_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn set_track_canonical_media_file_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    track_id: Uuid,
    media_file_id: Uuid,
) -> Result<(), StorageError> {
    sqlx::query(
        r#"
        UPDATE tracks t
        SET canonical_media_file_id = $2
        WHERE t.id = $1
          AND EXISTS (
            SELECT 1
            FROM media_files new_mf
            WHERE new_mf.id = $2
              AND new_mf.track_id = t.id
              AND new_mf.status = 'published'::media_file_status
              AND new_mf.published_at IS NOT NULL
              AND new_mf.duplicate_of_media_file_id IS NULL
              AND NOT EXISTS (
                SELECT 1
                FROM quarantine_items qi
                WHERE qi.media_file_id = new_mf.id
                  AND qi.status IN ('open'::quarantine_status, 'retrying'::quarantine_status)
              )
          )
          AND (
            t.canonical_media_file_id IS NULL
            OR NOT EXISTS (
              SELECT 1
              FROM media_files current_mf
              WHERE current_mf.id = t.canonical_media_file_id
                AND current_mf.track_id = t.id
                AND current_mf.status = 'published'::media_file_status
                AND current_mf.published_at IS NOT NULL
                AND current_mf.duplicate_of_media_file_id IS NULL
                AND NOT EXISTS (
                  SELECT 1
                  FROM quarantine_items qi
                  WHERE qi.media_file_id = current_mf.id
                    AND qi.status IN ('open'::quarantine_status, 'retrying'::quarantine_status)
                )
            )
          )
        "#,
    )
    .bind(track_id)
    .bind(media_file_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

/// Sets stored state for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `episode_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `media_file_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn set_episode_canonical_media_file_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    episode_id: Uuid,
    media_file_id: Uuid,
) -> Result<(), StorageError> {
    sqlx::query(
        r#"
        UPDATE episodes e
        SET canonical_media_file_id = $2
        WHERE e.id = $1
          AND EXISTS (
            SELECT 1
            FROM media_files new_mf
            WHERE new_mf.id = $2
              AND new_mf.episode_id = e.id
              AND new_mf.status = 'published'::media_file_status
              AND new_mf.published_at IS NOT NULL
              AND new_mf.duplicate_of_media_file_id IS NULL
              AND NOT EXISTS (
                SELECT 1
                FROM quarantine_items qi
                WHERE qi.media_file_id = new_mf.id
                  AND qi.status IN ('open'::quarantine_status, 'retrying'::quarantine_status)
              )
          )
          AND (
            e.canonical_media_file_id IS NULL
            OR NOT EXISTS (
              SELECT 1
              FROM media_files current_mf
              WHERE current_mf.id = e.canonical_media_file_id
                AND current_mf.episode_id = e.id
                AND current_mf.status = 'published'::media_file_status
                AND current_mf.published_at IS NOT NULL
                AND current_mf.duplicate_of_media_file_id IS NULL
                AND NOT EXISTS (
                  SELECT 1
                  FROM quarantine_items qi
                  WHERE qi.media_file_id = current_mf.id
                    AND qi.status IN ('open'::quarantine_status, 'retrying'::quarantine_status)
                )
            )
          )
        "#,
    )
    .bind(episode_id)
    .bind(media_file_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

/// Handles genre keys for request for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `request`: `&CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Vec<String>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn genre_keys_for_request(request: &CatalogImportRequest) -> Vec<String> {
    let mut keys = BTreeSet::new();
    for provenance in &request.provenance {
        let field_name = normalize_catalog_text(&provenance.field_name);
        if matches!(field_name.as_str(), "genre" | "genres") {
            collect_json_filter_keys(&provenance.value, &mut keys);
        }
    }
    keys.into_iter().collect()
}

/// Formats display data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `probe`: `&MediaProbeFacts`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `Vec<String>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn format_keys_for_probe(probe: &MediaProbeFacts) -> Vec<String> {
    format_keys_for_probe_values(
        probe.mime_type.as_deref(),
        probe.container.as_deref(),
        probe.audio_codec.as_deref(),
    )
}

/// Formats display data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `mime_type`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `container`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `audio_codec`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Vec<String>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn format_keys_for_probe_values(
    mime_type: Option<&str>,
    container: Option<&str>,
    audio_codec: Option<&str>,
) -> Vec<String> {
    let mut keys = BTreeSet::new();
    for value in [mime_type, container, audio_codec]
    .into_iter()
    .flatten()
    {
        collect_filter_keys(value, &mut keys);
    }
    keys.into_iter().collect()
}

/// Handles collect json filter keys for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `&Value`; expected to be a value satisfying the type contract shown in the function signature.
/// - `keys`: `&mut BTreeSet<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn collect_json_filter_keys(value: &Value, keys: &mut BTreeSet<String>) {
    match value {
        Value::String(value) => collect_filter_keys(value, keys),
        Value::Array(values) => {
            for value in values {
                collect_json_filter_keys(value, keys);
            }
        }
        Value::Object(map) => {
            for key in ["name", "title", "value", "genre"] {
                if let Some(value) = map.get(key) {
                    collect_json_filter_keys(value, keys);
                }
            }
        }
        _ => {}
    }
}

/// Handles collect filter keys for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `keys`: `&mut BTreeSet<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn collect_filter_keys(value: &str, keys: &mut BTreeSet<String>) {
    let normalized = normalize_catalog_text(value);
    if !normalized.is_empty() {
        keys.insert(normalized);
    }

    for part in value.split([',', ';', '/', '|']) {
        let normalized = normalize_catalog_text(part);
        if !normalized.is_empty() {
            keys.insert(normalized);
        }
    }
}

/// Inserts or updates data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `media_file_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `import_job_id`: `Option<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `source_path`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `reason`: `QuarantineReason`; expected to be a value satisfying the type contract shown in the function signature.
/// - `retry_eligible`: `bool`; expected to be a boolean flag controlling the documented branch.
/// - `admin_note`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `QuarantineItem` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn upsert_quarantine_item_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    media_file_id: Uuid,
    import_job_id: Option<Uuid>,
    source_path: &str,
    reason: QuarantineReason,
    retry_eligible: bool,
    admin_note: Option<&str>,
) -> Result<QuarantineItem, StorageError> {
    let sql = format!(
        r#"
        SELECT {QUARANTINE_ITEM_SELECT}
        FROM quarantine_items
        WHERE media_file_id = $1
          AND reason = $2::text::quarantine_reason
          AND status IN ('open', 'retrying')
        ORDER BY created_at ASC, id ASC
        LIMIT 1
        "#
    );
    let existing = sqlx::query(&sql)
        .bind(media_file_id)
        .bind(quarantine_reason_name(reason))
        .fetch_optional(&mut **transaction)
        .await?;
    if let Some(row) = existing {
        let sql = format!(
            r#"
            UPDATE quarantine_items
            SET source_path = $2,
                status = 'open',
                retry_eligible = $3,
                last_import_job_id = COALESCE($4, last_import_job_id),
                admin_note = COALESCE($5, admin_note),
                updated_at = $6
            WHERE id = $1
            RETURNING {QUARANTINE_ITEM_SELECT}
            "#
        );
        let existing_item = quarantine_item_from_row(&row)?;
        let row = sqlx::query(&sql)
            .bind(existing_item.id)
            .bind(source_path)
            .bind(retry_eligible)
            .bind(import_job_id)
            .bind(admin_note)
            .bind(Utc::now())
            .fetch_one(&mut **transaction)
            .await?;
        return quarantine_item_from_row(&row);
    }

    let now = Utc::now();
    let sql = format!(
        r#"
        INSERT INTO quarantine_items (
            id,
            media_file_id,
            source_path,
            reason,
            status,
            retry_count,
            retry_eligible,
            last_import_job_id,
            admin_note,
            created_at,
            updated_at
        )
        VALUES ($1, $2, $3, $4::text::quarantine_reason, 'open', 0, $5, $6, $7, $8, $8)
        RETURNING {QUARANTINE_ITEM_SELECT}
        "#
    );
    let row = sqlx::query(&sql)
        .bind(Uuid::new_v4())
        .bind(media_file_id)
        .bind(source_path)
        .bind(quarantine_reason_name(reason))
        .bind(retry_eligible)
        .bind(import_job_id)
        .bind(admin_note)
        .bind(now)
        .fetch_one(&mut **transaction)
        .await?;
    quarantine_item_from_row(&row)
}

/// Handles persist metadata in transaction for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `request`: `&CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
/// - `ids`: `&CatalogEntityIds`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn persist_metadata_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    request: &CatalogImportRequest,
    ids: &CatalogEntityIds,
) -> Result<(), StorageError> {
    if !request.preserve_provenance_history {
        delete_metadata_provenance_in_transaction(transaction, ids).await?;
    }

    if request.refresh_artwork {
        delete_artwork_assets_in_transaction(transaction, ids).await?;
    }

    for link in &request.provider_links {
        if let Some(entity_id) = entity_id_for_link(link.entity_type, ids) {
            upsert_provider_link_in_transaction(
                transaction,
                link,
                entity_id,
                request.preserve_confidence_history,
            )
            .await?;
        }
    }

    for provenance in &request.provenance {
        if let Some(entity_id) = entity_id_for_link(provenance.entity_type, ids) {
            insert_provenance_in_transaction(
                transaction,
                provenance,
                entity_id,
                request.import_job_id,
                Some(request.source_path.as_str()),
            )
            .await?;
        }
    }

    for artwork in &request.artwork {
        if let Some(entity_id) = entity_id_for_link(artwork.entity_type, ids) {
            insert_artwork_asset_in_transaction(transaction, artwork, entity_id).await?;
        }
    }

    Ok(())
}

/// Deletes or removes a resource from catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `ids`: `&CatalogEntityIds`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn delete_metadata_provenance_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ids: &CatalogEntityIds,
) -> Result<(), StorageError> {
    for (entity_type, entity_id) in catalog_entity_pairs(ids) {
        sqlx::query(
            r#"
            DELETE FROM metadata_provenance
            WHERE entity_type = $1::text::catalog_entity_type
              AND entity_id = $2
            "#,
        )
        .bind(entity_type_name(entity_type))
        .bind(entity_id)
        .execute(&mut **transaction)
        .await?;
    }

    Ok(())
}

/// Deletes or removes a resource from catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `ids`: `&CatalogEntityIds`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn delete_artwork_assets_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    ids: &CatalogEntityIds,
) -> Result<(), StorageError> {
    for (entity_type, entity_id) in catalog_entity_pairs(ids) {
        sqlx::query(
            r#"
            DELETE FROM artwork_assets
            WHERE entity_type = $1::text::catalog_entity_type
              AND entity_id = $2
            "#,
        )
        .bind(entity_type_name(entity_type))
        .bind(entity_id)
        .execute(&mut **transaction)
        .await?;
    }

    Ok(())
}

/// Handles catalog entity pairs for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `ids`: `&CatalogEntityIds`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Vec<(CatalogEntityType, Uuid)>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn catalog_entity_pairs(ids: &CatalogEntityIds) -> Vec<(CatalogEntityType, Uuid)> {
    let mut pairs = Vec::new();
    if let Some(id) = ids.album_artist_id {
        pairs.push((CatalogEntityType::Artist, id));
    }
    if let Some(id) = ids.track_artist_id.filter(|id| Some(*id) != ids.album_artist_id) {
        pairs.push((CatalogEntityType::Artist, id));
    }
    if let Some(id) = ids.album_id {
        pairs.push((CatalogEntityType::Album, id));
    }
    if let Some(id) = ids.track_id {
        pairs.push((CatalogEntityType::Track, id));
    }
    if let Some(id) = ids.podcast_id {
        pairs.push((CatalogEntityType::Podcast, id));
    }
    if let Some(id) = ids.episode_id {
        pairs.push((CatalogEntityType::Episode, id));
    }
    pairs.push((CatalogEntityType::MediaFile, ids.media_file_id));
    pairs
}

/// Handles entity id for link for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `entity_type`: `CatalogEntityType`; expected to be a value satisfying the type contract shown in the function signature.
/// - `ids`: `&CatalogEntityIds`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Some(Uuid)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn entity_id_for_link(entity_type: CatalogEntityType, ids: &CatalogEntityIds) -> Option<Uuid> {
    match entity_type {
        CatalogEntityType::Artist => ids.track_artist_id.or(ids.album_artist_id),
        CatalogEntityType::Album => ids.album_id,
        CatalogEntityType::Track => ids.track_id,
        CatalogEntityType::Podcast => ids.podcast_id,
        CatalogEntityType::Episode => ids.episode_id,
        CatalogEntityType::MediaFile => Some(ids.media_file_id),
        CatalogEntityType::Playlist => None,
    }
}

/// Inserts or updates data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `link`: `&MetadataProviderLinkDraft`; expected to be a value satisfying the type contract shown in the function signature.
/// - `entity_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `preserve_confidence_history`: `bool`; expected to be a boolean flag controlling the documented branch.
///
/// Output:
/// - Returns `MetadataProviderLink` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn upsert_provider_link_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    link: &MetadataProviderLinkDraft,
    entity_id: Uuid,
    preserve_confidence_history: bool,
) -> Result<MetadataProviderLink, StorageError> {
    let now = Utc::now();
    let sql = format!(
        r#"
        INSERT INTO metadata_provider_links (
            id,
            entity_type,
            entity_id,
            provider,
            provider_item_id,
            external_url,
            match_kind,
            confidence,
            auto_accepted,
            raw_metadata,
            created_at,
            updated_at
        )
        VALUES (
            $1,
            $2::text::catalog_entity_type,
            $3,
            $4::text::provider_kind,
            $5,
            $6,
            $7::text::metadata_match_kind,
            $8,
            $9,
            $10,
            $11,
            $11
        )
        ON CONFLICT (entity_type, entity_id, provider, provider_item_id) DO UPDATE SET
            external_url = EXCLUDED.external_url,
            match_kind = EXCLUDED.match_kind,
            confidence = CASE
                WHEN $12 THEN metadata_provider_links.confidence
                ELSE EXCLUDED.confidence
            END,
            auto_accepted = CASE
                WHEN $12 THEN metadata_provider_links.auto_accepted
                ELSE EXCLUDED.auto_accepted
            END,
            raw_metadata = EXCLUDED.raw_metadata,
            updated_at = EXCLUDED.updated_at
        RETURNING {METADATA_PROVIDER_LINK_SELECT}
        "#
    );
    let row = sqlx::query(&sql)
        .bind(Uuid::new_v4())
        .bind(entity_type_name(link.entity_type))
        .bind(entity_id)
        .bind(link.provider.api_name())
        .bind(link.provider_item_id.as_str())
        .bind(link.external_url.as_deref())
        .bind(metadata_match_kind_name(link.match_kind))
        .bind(link.confidence)
        .bind(link.auto_accepted)
        .bind(Json(link.raw_metadata.clone()))
        .bind(now)
        .bind(preserve_confidence_history)
        .fetch_one(&mut **transaction)
        .await?;
    metadata_provider_link_from_row(&row)
}

/// Inserts data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provenance`: `&MetadataProvenanceDraft`; expected to be a value satisfying the type contract shown in the function signature.
/// - `entity_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `import_job_id`: `Option<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `source_path`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `MetadataProvenance` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn insert_provenance_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    provenance: &MetadataProvenanceDraft,
    entity_id: Uuid,
    import_job_id: Option<Uuid>,
    source_path: Option<&str>,
) -> Result<MetadataProvenance, StorageError> {
    let sql = format!(
        r#"
        INSERT INTO metadata_provenance (
            id,
            entity_type,
            entity_id,
            field_name,
            provider,
            value,
            confidence,
            auto_accepted,
            import_job_id,
            source_path,
            created_at
        )
        VALUES (
            $1,
            $2::text::catalog_entity_type,
            $3,
            $4,
            $5::text::provider_kind,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11
        )
        RETURNING {METADATA_PROVENANCE_SELECT}
        "#
    );
    let row = sqlx::query(&sql)
        .bind(Uuid::new_v4())
        .bind(entity_type_name(provenance.entity_type))
        .bind(entity_id)
        .bind(provenance.field_name.as_str())
        .bind(provenance.provider.api_name())
        .bind(Json(provenance.value.clone()))
        .bind(provenance.confidence)
        .bind(provenance.auto_accepted)
        .bind(import_job_id)
        .bind(source_path)
        .bind(Utc::now())
        .fetch_one(&mut **transaction)
        .await?;
    metadata_provenance_from_row(&row)
}

/// Inserts data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `artwork`: `&ArtworkAssetDraft`; expected to be a value satisfying the type contract shown in the function signature.
/// - `entity_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `ArtworkAsset` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn insert_artwork_asset_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    artwork: &ArtworkAssetDraft,
    entity_id: Uuid,
) -> Result<ArtworkAsset, StorageError> {
    let sql = format!(
        r#"
        INSERT INTO artwork_assets (
            id,
            entity_type,
            entity_id,
            provider,
            artwork_kind,
            source_uri,
            file_path,
            mime_type,
            width,
            height,
            confidence,
            created_at
        )
        VALUES (
            $1,
            $2::text::catalog_entity_type,
            $3,
            $4::text::provider_kind,
            $5::text::artwork_kind,
            $6,
            $7,
            $8,
            $9,
            $10,
            $11,
            $12
        )
        RETURNING {ARTWORK_ASSET_SELECT}
        "#
    );
    let row = sqlx::query(&sql)
        .bind(Uuid::new_v4())
        .bind(entity_type_name(artwork.entity_type))
        .bind(entity_id)
        .bind(artwork.provider.api_name())
        .bind(artwork_kind_name(artwork.artwork_kind))
        .bind(artwork.source_uri.as_deref())
        .bind(artwork.file_path.as_deref())
        .bind(artwork.mime_type.as_deref())
        .bind(artwork.width)
        .bind(artwork.height)
        .bind(artwork.confidence)
        .bind(Utc::now())
        .fetch_one(&mut **transaction)
        .await?;
    artwork_asset_from_row(&row)
}

/// Inserts or updates data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `album_artist`: `&Artist`; expected to be a value satisfying the type contract shown in the function signature.
/// - `track_artist`: `&Artist`; expected to be a value satisfying the type contract shown in the function signature.
/// - `album`: `&Album`; expected to be a value satisfying the type contract shown in the function signature.
/// - `track`: `&Track`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn upsert_music_search_projections_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    album_artist: &Artist,
    track_artist: &Artist,
    album: &Album,
    track: &Track,
) -> Result<(), StorageError> {
    upsert_search_projection_in_transaction(
        transaction,
        CatalogEntityType::Artist,
        album_artist.id,
        &album_artist.name,
        &album_artist.name,
        true,
    )
    .await?;
    if album_artist.id != track_artist.id {
        upsert_search_projection_in_transaction(
            transaction,
            CatalogEntityType::Artist,
            track_artist.id,
            &track_artist.name,
            &track_artist.name,
            true,
        )
        .await?;
    }
    upsert_search_projection_in_transaction(
        transaction,
        CatalogEntityType::Album,
        album.id,
        &album.title,
        &format!("{} {}", album.title, album_artist.name),
        true,
    )
    .await?;
    let track_search_text = if album_artist.name != track_artist.name {
        format!(
            "{} {} {} {}",
            track.title, album.title, album_artist.name, track_artist.name
        )
    } else {
        format!("{} {} {}", track.title, album.title, track_artist.name)
    };
    upsert_search_projection_in_transaction(
        transaction,
        CatalogEntityType::Track,
        track.id,
        &track.title,
        &track_search_text,
        true,
    )
    .await?;
    Ok(())
}

/// Inserts or updates data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `podcast`: `&Podcast`; expected to be a value satisfying the type contract shown in the function signature.
/// - `episode`: `&Episode`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn upsert_podcast_search_projections_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    podcast: &Podcast,
    episode: &Episode,
) -> Result<(), StorageError> {
    upsert_search_projection_in_transaction(
        transaction,
        CatalogEntityType::Podcast,
        podcast.id,
        &podcast.title,
        &podcast.title,
        true,
    )
    .await?;
    upsert_search_projection_in_transaction(
        transaction,
        CatalogEntityType::Episode,
        episode.id,
        &episode.title,
        &format!("{} {}", episode.title, podcast.title),
        true,
    )
    .await?;
    Ok(())
}

/// Inserts or updates data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `entity_type`: `CatalogEntityType`; expected to be a value satisfying the type contract shown in the function signature.
/// - `entity_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `display_title`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `search_text`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `published`: `bool`; expected to be a boolean flag controlling the documented branch.
///
/// Output:
/// - Returns `CatalogSearchProjection` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn upsert_search_projection_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    entity_type: CatalogEntityType,
    entity_id: Uuid,
    display_title: &str,
    search_text: &str,
    published: bool,
) -> Result<CatalogSearchProjection, StorageError> {
    let normalized_text = normalize_catalog_text(search_text);
    let normalized_display_title = normalize_catalog_text(display_title);
    let sql = format!(
        r#"
        INSERT INTO catalog_search_projection (
            entity_type,
            entity_id,
            display_title,
            search_text,
            normalized_text,
            normalized_display_title,
            published,
            updated_at
        )
        VALUES ($1::text::catalog_entity_type, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (entity_type, entity_id) DO UPDATE SET
            display_title = EXCLUDED.display_title,
            search_text = EXCLUDED.search_text,
            normalized_text = EXCLUDED.normalized_text,
            normalized_display_title = EXCLUDED.normalized_display_title,
            published = EXCLUDED.published,
            updated_at = EXCLUDED.updated_at
        RETURNING {SEARCH_PROJECTION_SELECT}
        "#
    );
    let row = sqlx::query(&sql)
        .bind(entity_type_name(entity_type))
        .bind(entity_id)
        .bind(display_title)
        .bind(search_text)
        .bind(normalized_text)
        .bind(normalized_display_title)
        .bind(published)
        .bind(Utc::now())
        .fetch_one(&mut **transaction)
        .await?;
    catalog_search_projection_from_row(&row)
}

/// Normalizes caller-provided data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
pub fn normalize_catalog_text(value: &str) -> String {
    normalize_catalog_tokens(value).join(" ")
}

/// Normalizes caller-provided data for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Vec<String>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn normalize_catalog_tokens(value: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();

    for character in value.nfkd() {
        if is_combining_mark(character) {
            continue;
        }
        for lower in character.to_lowercase() {
            if lower.is_alphanumeric() {
                current.push(lower);
            } else if !current.is_empty() {
                tokens.push(std::mem::take(&mut current));
            }
        }
    }

    if !current.is_empty() {
        tokens.push(current);
    }

    if matches!(tokens.first().map(String::as_str), Some("the" | "a" | "an")) {
        tokens.remove(0);
    }

    tokens
}

/// Handles sort name for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(String)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
pub fn sort_name(value: &str) -> Option<String> {
    let trimmed = value.trim();
    let lower = trimmed.to_ascii_lowercase();
    for article in ["the ", "a ", "an "] {
        if lower.starts_with(article) {
            let rest = trimmed[article.len()..].trim();
            if !rest.is_empty() {
                return Some(format!("{rest}, {}", article.trim_end()));
            }
        }
    }
    None
}

/// Handles sanitize path component for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
pub fn sanitize_path_component(value: &str) -> String {
    let mut sanitized = String::new();
    for character in value.trim().chars() {
        let replacement = match character {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => ' ',
            character if character.is_control() => ' ',
            character => character,
        };
        sanitized.push(replacement);
    }

    let collapsed = sanitized
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    let trimmed = collapsed.trim_matches('.');
    if trimmed.is_empty() {
        "Unknown".to_string()
    } else {
        trimmed.to_string()
    }
}

/// Handles likely compilation artist for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
pub fn likely_compilation_artist() -> &'static str {
    "Various Artists"
}

#[cfg(test)]
mod tests {
    use super::{
        normalize_catalog_text, parse_catalog_search_query, FORMAT_FILTER_PREDICATE,
        GENRE_FILTER_PREDICATE,
    };

    #[test]
    /// Handles catalog text normalization ignores articles punctuation and diacritics for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn catalog_text_normalization_ignores_articles_punctuation_and_diacritics() {
        assert_eq!(normalize_catalog_text("The Béatles"), "beatles");
        assert_eq!(normalize_catalog_text("A Tribe-Called Quest"), "tribe called quest");
        assert_eq!(normalize_catalog_text("Beyoncé / RENAISSANCE"), "beyonce renaissance");
    }

    #[test]
    /// Handles catalog text normalization retains non latin artist and title terms for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn catalog_text_normalization_retains_non_latin_artist_and_title_terms() {
        assert_eq!(normalize_catalog_text("Земфира"), "земфира");
        assert_eq!(normalize_catalog_text("東京事変"), "東京事変");
        assert_eq!(normalize_catalog_text("Весна / さくら"), "весна さくら");
        assert_eq!(
            parse_catalog_search_query(Some(" 東京事変 "))
                .unwrap()
                .normalized_query,
            "東京事変"
        );
    }

    #[test]
    /// Handles non latin catalog unique keys do not collapse to one sentinel for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn non_latin_catalog_unique_keys_do_not_collapse_to_one_sentinel() {
        let artist_key = normalize_catalog_text("東京事変");
        let other_artist_key = normalize_catalog_text("坂本龍一");
        let album_key = (artist_key.clone(), normalize_catalog_text("群青日和"));
        let other_album_key = (artist_key.clone(), normalize_catalog_text("透明人間"));
        let podcast_key = normalize_catalog_text("Новости");
        let other_podcast_key = normalize_catalog_text("東京ニュース");

        assert_ne!(artist_key, other_artist_key);
        assert_ne!(artist_key, "unknown");
        assert_ne!(album_key, other_album_key);
        assert_ne!(podcast_key, other_podcast_key);
        assert_ne!(podcast_key, "unknown");
    }

    #[test]
    /// Searches resources for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn search_query_rejects_empty_normalized_terms() {
        assert!(parse_catalog_search_query(None).is_err());
        assert!(parse_catalog_search_query(Some(" ... ")).is_err());
        assert!(parse_catalog_search_query(Some("the")).is_err());
        assert_eq!(
            parse_catalog_search_query(Some(" The Low-End Theory "))
                .unwrap()
                .normalized_query,
            "low end theory"
        );
    }

    #[test]
    /// Searches resources for catalog persistence, browsing, search, import upsert, and normalization logic.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn search_filter_predicates_use_gin_array_containment() {
        assert!(GENRE_FILTER_PREDICATE.contains("mf.genres @> ARRAY[$4::text]"));
        assert!(FORMAT_FILTER_PREDICATE.contains("mf.format_keys @> ARRAY[$5::text]"));
        assert!(!GENRE_FILTER_PREDICATE.contains("ANY"));
        assert!(!FORMAT_FILTER_PREDICATE.contains("ANY"));
    }
}

/// Handles artist from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Artist` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn artist_from_row(row: &PgRow) -> Result<Artist, StorageError> {
    Ok(Artist {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        normalized_name: row.try_get("normalized_name")?,
        sort_name: row.try_get("sort_name")?,
        stable_grouping: row.try_get("stable_grouping")?,
        published_at: row.try_get("published_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles album from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Album` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn album_from_row(row: &PgRow) -> Result<Album, StorageError> {
    Ok(Album {
        id: row.try_get("id")?,
        artist_id: row.try_get("artist_id")?,
        title: row.try_get("title")?,
        normalized_title: row.try_get("normalized_title")?,
        album_kind: parse_album_kind(row.try_get::<String, _>("album_kind")?)?,
        release_year: row.try_get("release_year")?,
        stable_grouping: row.try_get("stable_grouping")?,
        published_at: row.try_get("published_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles track from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Track` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn track_from_row(row: &PgRow) -> Result<Track, StorageError> {
    Ok(Track {
        id: row.try_get("id")?,
        album_id: row.try_get("album_id")?,
        artist_id: row.try_get("artist_id")?,
        title: row.try_get("title")?,
        normalized_title: row.try_get("normalized_title")?,
        disc_number: row.try_get("disc_number")?,
        track_number: row.try_get("track_number")?,
        duration_seconds: row.try_get("duration_seconds")?,
        stable_grouping: row.try_get("stable_grouping")?,
        published_at: row.try_get("published_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles podcast from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Podcast` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn podcast_from_row(row: &PgRow) -> Result<Podcast, StorageError> {
    Ok(Podcast {
        id: row.try_get("id")?,
        title: row.try_get("title")?,
        normalized_title: row.try_get("normalized_title")?,
        stable_grouping: row.try_get("stable_grouping")?,
        published_at: row.try_get("published_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles podcast from episode read row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Podcast` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn podcast_from_episode_read_row(row: &PgRow) -> Result<Podcast, StorageError> {
    Ok(Podcast {
        id: row.try_get("read_podcast_id")?,
        title: row.try_get("read_podcast_title")?,
        normalized_title: row.try_get("read_podcast_normalized_title")?,
        stable_grouping: row.try_get("read_podcast_stable_grouping")?,
        published_at: row.try_get("read_podcast_published_at")?,
        created_at: row.try_get("read_podcast_created_at")?,
        updated_at: row.try_get("read_podcast_updated_at")?,
    })
}

/// Handles episode from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Episode` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn episode_from_row(row: &PgRow) -> Result<Episode, StorageError> {
    Ok(Episode {
        id: row.try_get("id")?,
        podcast_id: row.try_get("podcast_id")?,
        title: row.try_get("title")?,
        normalized_title: row.try_get("normalized_title")?,
        season_number: row.try_get("season_number")?,
        episode_number: row.try_get("episode_number")?,
        duration_seconds: row.try_get("duration_seconds")?,
        stable_grouping: row.try_get("stable_grouping")?,
        published_at: row.try_get("published_at")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles media file from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `MediaFile` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn media_file_from_row(row: &PgRow) -> Result<MediaFile, StorageError> {
    Ok(MediaFile {
        id: row.try_get("id")?,
        media_kind: parse_media_kind(row.try_get::<String, _>("media_kind")?)?,
        status: parse_media_file_status(row.try_get::<String, _>("status")?)?,
        source_path: row.try_get("source_path")?,
        managed_path: row.try_get("managed_path")?,
        file_hash: row.try_get("file_hash")?,
        file_size: row.try_get("file_size")?,
        mime_type: row.try_get("mime_type")?,
        container: row.try_get("container")?,
        audio_codec: row.try_get("audio_codec")?,
        duration_seconds: row.try_get("duration_seconds")?,
        bitrate: row.try_get("bitrate")?,
        sample_rate: row.try_get("sample_rate")?,
        channels: row.try_get("channels")?,
        genres: row.try_get("genres")?,
        format_keys: row.try_get("format_keys")?,
        track_id: row.try_get("track_id")?,
        episode_id: row.try_get("episode_id")?,
        duplicate_of_media_file_id: row.try_get("duplicate_of_media_file_id")?,
        import_job_id: row.try_get("import_job_id")?,
        discovered_at: row.try_get("discovered_at")?,
        published_at: row.try_get("published_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles playlist from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Playlist` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn playlist_from_row(row: &PgRow) -> Result<Playlist, StorageError> {
    Ok(Playlist {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        description: row.try_get("description")?,
        scope: parse_playlist_scope(row.try_get::<String, _>("scope")?)?,
        owner_account_id: row.try_get("owner_account_id")?,
        created_by_account_id: row.try_get("created_by_account_id")?,
        updated_by_account_id: row.try_get("updated_by_account_id")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles quarantine item from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `QuarantineItem` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn quarantine_item_from_row(row: &PgRow) -> Result<QuarantineItem, StorageError> {
    Ok(QuarantineItem {
        id: row.try_get("id")?,
        media_file_id: row.try_get("media_file_id")?,
        source_path: row.try_get("source_path")?,
        reason: parse_quarantine_reason(row.try_get::<String, _>("reason")?)?,
        status: parse_quarantine_status(row.try_get::<String, _>("status")?)?,
        retry_count: i32_to_u32(row.try_get("retry_count")?, "quarantine_items.retry_count")?,
        retry_eligible: row.try_get("retry_eligible")?,
        last_import_job_id: row.try_get("last_import_job_id")?,
        admin_note: row.try_get("admin_note")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles metadata provider link from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `MetadataProviderLink` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn metadata_provider_link_from_row(
    row: &PgRow,
) -> Result<MetadataProviderLink, StorageError> {
    Ok(MetadataProviderLink {
        id: row.try_get("id")?,
        entity_type: parse_entity_type(row.try_get::<String, _>("entity_type")?)?,
        entity_id: row.try_get("entity_id")?,
        provider: parse_provider_kind(row.try_get::<String, _>("provider")?)?,
        provider_item_id: row.try_get("provider_item_id")?,
        external_url: row.try_get("external_url")?,
        match_kind: parse_metadata_match_kind(row.try_get::<String, _>("match_kind")?)?,
        confidence: row.try_get("confidence")?,
        auto_accepted: row.try_get("auto_accepted")?,
        raw_metadata: json_column(row, "raw_metadata")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles metadata provenance from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `MetadataProvenance` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn metadata_provenance_from_row(row: &PgRow) -> Result<MetadataProvenance, StorageError> {
    Ok(MetadataProvenance {
        id: row.try_get("id")?,
        entity_type: parse_entity_type(row.try_get::<String, _>("entity_type")?)?,
        entity_id: row.try_get("entity_id")?,
        field_name: row.try_get("field_name")?,
        provider: parse_provider_kind(row.try_get::<String, _>("provider")?)?,
        value: json_column(row, "value")?,
        confidence: row.try_get("confidence")?,
        auto_accepted: row.try_get("auto_accepted")?,
        import_job_id: row.try_get("import_job_id")?,
        source_path: row.try_get("source_path")?,
        created_at: row.try_get("created_at")?,
    })
}

/// Handles artwork asset from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ArtworkAsset` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn artwork_asset_from_row(row: &PgRow) -> Result<ArtworkAsset, StorageError> {
    Ok(ArtworkAsset {
        id: row.try_get("id")?,
        entity_type: parse_entity_type(row.try_get::<String, _>("entity_type")?)?,
        entity_id: row.try_get("entity_id")?,
        provider: parse_provider_kind(row.try_get::<String, _>("provider")?)?,
        artwork_kind: parse_artwork_kind(row.try_get::<String, _>("artwork_kind")?)?,
        source_uri: row.try_get("source_uri")?,
        file_path: row.try_get("file_path")?,
        mime_type: row.try_get("mime_type")?,
        width: row.try_get("width")?,
        height: row.try_get("height")?,
        confidence: row.try_get("confidence")?,
        created_at: row.try_get("created_at")?,
    })
}

/// Handles catalog search projection from row for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `CatalogSearchProjection` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn catalog_search_projection_from_row(
    row: &PgRow,
) -> Result<CatalogSearchProjection, StorageError> {
    Ok(CatalogSearchProjection {
        entity_type: parse_entity_type(row.try_get::<String, _>("entity_type")?)?,
        entity_id: row.try_get("entity_id")?,
        display_title: row.try_get("display_title")?,
        search_text: row.try_get("search_text")?,
        normalized_text: row.try_get("normalized_text")?,
        normalized_display_title: row.try_get("normalized_display_title")?,
        published: row.try_get("published")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles json column for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
/// - `column`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `T` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn json_column<T>(row: &PgRow, column: &'static str) -> Result<T, StorageError>
where
    T: DeserializeOwned,
{
    Ok(row.try_get::<Json<T>, _>(column)?.0)
}

/// Handles album kind name for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `kind`: `AlbumKind`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn album_kind_name(kind: AlbumKind) -> &'static str {
    match kind {
        AlbumKind::Album => "album",
        AlbumKind::Compilation => "compilation",
        AlbumKind::Single => "single",
        AlbumKind::Unknown => "unknown",
    }
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `AlbumKind` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_album_kind(value: String) -> Result<AlbumKind, StorageError> {
    match value.as_str() {
        "album" => Ok(AlbumKind::Album),
        "compilation" => Ok(AlbumKind::Compilation),
        "single" => Ok(AlbumKind::Single),
        "unknown" => Ok(AlbumKind::Unknown),
        _ => invalid_value("albums.album_kind", value),
    }
}

/// Handles media kind name for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `kind`: `MediaKind`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn media_kind_name(kind: MediaKind) -> &'static str {
    match kind {
        MediaKind::Music => "music",
        MediaKind::Podcast => "podcast",
    }
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `MediaKind` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_media_kind(value: String) -> Result<MediaKind, StorageError> {
    match value.as_str() {
        "music" => Ok(MediaKind::Music),
        "podcast" => Ok(MediaKind::Podcast),
        _ => invalid_value("media_files.media_kind", value),
    }
}

/// Handles media file status name for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `status`: `MediaFileStatus`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn media_file_status_name(status: MediaFileStatus) -> &'static str {
    match status {
        MediaFileStatus::Staged => "staged",
        MediaFileStatus::Published => "published",
        MediaFileStatus::Duplicate => "duplicate",
        MediaFileStatus::Quarantined => "quarantined",
        MediaFileStatus::Failed => "failed",
    }
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `MediaFileStatus` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_media_file_status(value: String) -> Result<MediaFileStatus, StorageError> {
    match value.as_str() {
        "staged" => Ok(MediaFileStatus::Staged),
        "published" => Ok(MediaFileStatus::Published),
        "duplicate" => Ok(MediaFileStatus::Duplicate),
        "quarantined" => Ok(MediaFileStatus::Quarantined),
        "failed" => Ok(MediaFileStatus::Failed),
        _ => invalid_value("media_files.status", value),
    }
}

/// Handles entity type name for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `entity_type`: `CatalogEntityType`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn entity_type_name(entity_type: CatalogEntityType) -> &'static str {
    match entity_type {
        CatalogEntityType::Artist => "artist",
        CatalogEntityType::Album => "album",
        CatalogEntityType::Track => "track",
        CatalogEntityType::Podcast => "podcast",
        CatalogEntityType::Episode => "episode",
        CatalogEntityType::MediaFile => "media_file",
        CatalogEntityType::Playlist => "playlist",
    }
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `CatalogEntityType` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_entity_type(value: String) -> Result<CatalogEntityType, StorageError> {
    match value.as_str() {
        "artist" => Ok(CatalogEntityType::Artist),
        "album" => Ok(CatalogEntityType::Album),
        "track" => Ok(CatalogEntityType::Track),
        "podcast" => Ok(CatalogEntityType::Podcast),
        "episode" => Ok(CatalogEntityType::Episode),
        "media_file" => Ok(CatalogEntityType::MediaFile),
        "playlist" => Ok(CatalogEntityType::Playlist),
        _ => invalid_value("catalog_entity_type", value),
    }
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `PlaylistScope` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_playlist_scope(value: String) -> Result<PlaylistScope, StorageError> {
    match value.as_str() {
        "personal" => Ok(PlaylistScope::Personal),
        "shared" => Ok(PlaylistScope::Shared),
        _ => invalid_value("playlists.scope", value),
    }
}

/// Handles artwork kind name for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `kind`: `ArtworkKind`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn artwork_kind_name(kind: ArtworkKind) -> &'static str {
    match kind {
        ArtworkKind::Cover => "cover",
        ArtworkKind::Artist => "artist",
        ArtworkKind::Fanart => "fanart",
        ArtworkKind::Thumbnail => "thumbnail",
        ArtworkKind::Other => "other",
    }
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `ArtworkKind` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_artwork_kind(value: String) -> Result<ArtworkKind, StorageError> {
    match value.as_str() {
        "cover" => Ok(ArtworkKind::Cover),
        "artist" => Ok(ArtworkKind::Artist),
        "fanart" => Ok(ArtworkKind::Fanart),
        "thumbnail" => Ok(ArtworkKind::Thumbnail),
        "other" => Ok(ArtworkKind::Other),
        _ => invalid_value("artwork_assets.artwork_kind", value),
    }
}

/// Handles metadata match kind name for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `kind`: `MetadataMatchKind`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn metadata_match_kind_name(kind: MetadataMatchKind) -> &'static str {
    match kind {
        MetadataMatchKind::ExactIdentifier => "exact_identifier",
        MetadataMatchKind::HighConfidence => "high_confidence",
        MetadataMatchKind::ModerateConfidence => "moderate_confidence",
        MetadataMatchKind::LocalOnly => "local_only",
    }
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `MetadataMatchKind` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_metadata_match_kind(value: String) -> Result<MetadataMatchKind, StorageError> {
    match value.as_str() {
        "exact_identifier" => Ok(MetadataMatchKind::ExactIdentifier),
        "high_confidence" => Ok(MetadataMatchKind::HighConfidence),
        "moderate_confidence" => Ok(MetadataMatchKind::ModerateConfidence),
        "local_only" => Ok(MetadataMatchKind::LocalOnly),
        _ => invalid_value("metadata_provider_links.match_kind", value),
    }
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `ProviderKind` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_provider_kind(value: String) -> Result<ProviderKind, StorageError> {
    value.parse().map_err(|_| StorageError::InvalidStoredValue {
        field: "provider_kind",
        value,
    })
}

/// Handles quarantine reason name for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `reason`: `QuarantineReason`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn quarantine_reason_name(reason: QuarantineReason) -> &'static str {
    match reason {
        QuarantineReason::Duplicate => "duplicate",
        QuarantineReason::MetadataFailure => "metadata_failure",
        QuarantineReason::FileError => "file_error",
        QuarantineReason::UnsupportedFormat => "unsupported_format",
        QuarantineReason::ConflictingMetadata => "conflicting_metadata",
    }
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `QuarantineReason` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_quarantine_reason(value: String) -> Result<QuarantineReason, StorageError> {
    match value.as_str() {
        "duplicate" => Ok(QuarantineReason::Duplicate),
        "metadata_failure" => Ok(QuarantineReason::MetadataFailure),
        "file_error" => Ok(QuarantineReason::FileError),
        "unsupported_format" => Ok(QuarantineReason::UnsupportedFormat),
        "conflicting_metadata" => Ok(QuarantineReason::ConflictingMetadata),
        _ => invalid_value("quarantine_items.reason", value),
    }
}

/// Parses and validates input for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `QuarantineStatus` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_quarantine_status(value: String) -> Result<QuarantineStatus, StorageError> {
    match value.as_str() {
        "open" => Ok(QuarantineStatus::Open),
        "retrying" => Ok(QuarantineStatus::Retrying),
        "resolved" => Ok(QuarantineStatus::Resolved),
        "deleted" => Ok(QuarantineStatus::Deleted),
        _ => invalid_value("quarantine_items.status", value),
    }
}

/// Handles invalid value for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `field`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `T` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn invalid_value<T>(field: &'static str, value: String) -> Result<T, StorageError> {
    Err(StorageError::InvalidStoredValue { field, value })
}

/// Handles i32 to u32 for catalog persistence, browsing, search, import upsert, and normalization logic.
///
/// Inputs:
/// - `value`: `i32`; expected to be a numeric value within the range accepted by the target domain or database column.
/// - `field`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `u32` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn i32_to_u32(value: i32, field: &'static str) -> Result<u32, StorageError> {
    u32::try_from(value).map_err(|_| StorageError::InvalidStoredValue {
        field,
        value: value.to_string(),
    })
}
