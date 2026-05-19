use std::collections::{BTreeMap, HashSet};

use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{
    api::home::{
        action_hint, primary_artwork, ScreenActionHint, ScreenArtwork, ScreenContextHint,
    },
    api::playback::{PlaybackProgressWriteRequest, PlaybackProgressWriteResponse},
    auth::AuthenticatedUser,
    catalog::CatalogBrowsePage,
    domain::{
        Album, AlbumKind, Artist, ArtworkKind, CatalogEntityType, Episode, PlaybackItemType,
        MetadataProvenance, PlaybackProgress, Playlist, Podcast, Track,
    },
    error::{ApiError, ErrorResponse},
    state::AppState,
};

/// Builds the Axum router for catalog browsing and search.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `Router<AppState>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/search", get(search_catalog))
        .route("/artists", get(browse_artists))
        .route("/artists/:artist_id/detail", get(get_artist_detail))
        .route("/albums", get(browse_albums))
        .route("/albums/:album_id/detail", get(get_album_detail))
        .route("/tracks", get(browse_tracks))
        .route("/podcasts", get(browse_podcasts))
        .route("/podcasts/:podcast_id", get(get_podcast))
        .route(
            "/podcasts/:podcast_id/episodes",
            get(browse_podcast_episodes),
        )
        .route("/episodes", get(browse_episodes))
        .route("/episodes/:episode_id", get(get_episode))
        .route(
            "/episodes/:episode_id/resume",
            get(get_episode_resume).put(write_episode_resume),
        )
}

#[derive(Debug, Clone, Deserialize, IntoParams, ToSchema)]
/// Represents catalog browse query in the authenticated catalog search, browse, and episode resume HTTP API.
///
/// Functionality: Carries fields `limit`, `cursor`, `sort` for authenticated catalog search, browse, and episode resume HTTP API.
/// Dependencies: depends on `Option<u32>`, `Option<String>`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`.
pub struct CatalogBrowseQuery {
    /// Maximum number of items to return. Defaults to 50; maximum is 200.
    pub limit: Option<u32>,
    /// Opaque cursor returned from the previous page.
    pub cursor: Option<String>,
    /// Stable resource-specific sort key.
    pub sort: Option<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams, ToSchema)]
/// Represents catalog search query in the authenticated catalog search, browse, and episode resume HTTP API.
///
/// Functionality: Carries fields `q`, `limit`, `year`, `genre`, `format`, `media_type` for authenticated catalog search, browse, and episode resume HTTP API.
/// Dependencies: depends on `Option<String>`, `Option<u32>`, `Option<i32>`, `Option<String>`, `Option<String>`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`.
pub struct CatalogSearchQuery {
    /// Search text. Matching ignores case, diacritics, punctuation, separators, and leading articles.
    pub q: Option<String>,
    /// Maximum number of items to return in each grouped section. Defaults to 10; maximum is 50.
    pub limit: Option<u32>,
    /// Restrict catalog media results to this release year.
    pub year: Option<i32>,
    /// Restrict catalog media results to this normalized genre.
    pub genre: Option<String>,
    /// Restrict catalog media results to this container, codec, or MIME format.
    pub format: Option<String>,
    /// Restrict catalog media results to music or podcast media.
    pub media_type: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents catalog browse page metadata in the authenticated catalog search, browse, and episode resume HTTP API.
///
/// Functionality: Carries fields `limit`, `next_cursor`, `sort` for authenticated catalog search, browse, and episode resume HTTP API.
/// Dependencies: depends on `u32`, `Option<String>`, `String` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`.
pub struct CatalogBrowsePageMetadata {
    pub limit: u32,
    pub next_cursor: Option<String>,
    pub sort: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents browse artists response in the authenticated catalog search, browse, and episode resume HTTP API.
///
/// Functionality: Carries fields `artists`, `page` for authenticated catalog search, browse, and episode resume HTTP API.
/// Dependencies: depends on `Vec<Artist>`, `CatalogBrowsePageMetadata` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`.
pub struct BrowseArtistsResponse {
    pub artists: Vec<ArtistBrowseItem>,
    pub page: CatalogBrowsePageMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArtistBrowseItem {
    pub id: Uuid,
    pub name: String,
    pub normalized_name: String,
    pub sort_name: Option<String>,
    pub stable_grouping: bool,
    pub published_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub primary_artwork: Option<ScreenArtwork>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents browse albums response in the authenticated catalog search, browse, and episode resume HTTP API.
///
/// Functionality: Carries fields `albums`, `page` for authenticated catalog search, browse, and episode resume HTTP API.
/// Dependencies: depends on `Vec<Album>`, `CatalogBrowsePageMetadata` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`.
pub struct BrowseAlbumsResponse {
    pub albums: Vec<Album>,
    pub page: CatalogBrowsePageMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents browse tracks response in the authenticated catalog search, browse, and episode resume HTTP API.
///
/// Functionality: Carries fields `tracks`, `page` for authenticated catalog search, browse, and episode resume HTTP API.
/// Dependencies: depends on `Vec<Track>`, `CatalogBrowsePageMetadata` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`.
pub struct BrowseTracksResponse {
    pub tracks: Vec<Track>,
    pub page: CatalogBrowsePageMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArtistDetailResponse {
    pub revision: u64,
    pub snapshot_at: DateTime<Utc>,
    pub artist: ArtistDetailHeader,
    pub primary_artwork: Option<ScreenArtwork>,
    pub metadata: ArtistDetailMetadata,
    pub summary: ArtistDetailSummary,
    pub album_groups: Vec<ArtistAlbumGroup>,
    pub track_groups: Vec<ArtistTrackGroup>,
    pub actions: Vec<ScreenActionHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AlbumDetailResponse {
    pub revision: u64,
    pub snapshot_at: DateTime<Utc>,
    pub album: AlbumDetailHeader,
    pub artist: ArtistDetailLink,
    pub primary_artwork: Option<ScreenArtwork>,
    pub summary: AlbumDetailSummary,
    pub track_groups: Vec<AlbumTrackGroup>,
    pub actions: Vec<ScreenActionHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArtistDetailHeader {
    pub id: Uuid,
    pub name: String,
    pub sort_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArtistDetailSummary {
    pub album_count: usize,
    pub track_count: usize,
    pub duration_seconds: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct ArtistDetailMetadata {
    pub description: Option<String>,
    pub genres: Vec<String>,
    pub style: Option<String>,
    pub mood: Option<String>,
    pub label: Option<String>,
    pub links: Vec<ArtistExternalLink>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArtistExternalLink {
    pub kind: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArtistAlbumGroup {
    pub id: Uuid,
    pub title: String,
    pub subtitle: Option<String>,
    pub release_year: Option<i32>,
    pub album_kind: AlbumKind,
    pub primary_artwork: Option<ScreenArtwork>,
    pub track_count: usize,
    pub duration_seconds: Option<i32>,
    pub tracks: Vec<DetailTrackItem>,
    pub actions: Vec<ScreenActionHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArtistTrackGroup {
    pub id: String,
    pub title: String,
    pub items: Vec<DetailTrackItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AlbumDetailHeader {
    pub id: Uuid,
    pub title: String,
    pub release_year: Option<i32>,
    pub album_kind: AlbumKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArtistDetailLink {
    pub id: Uuid,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AlbumDetailSummary {
    pub track_count: usize,
    pub duration_seconds: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AlbumTrackGroup {
    pub id: String,
    pub title: String,
    pub disc_number: Option<i32>,
    pub items: Vec<DetailTrackItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DetailTrackItem {
    pub id: Uuid,
    pub item_type: PlaybackItemType,
    pub title: String,
    pub subtitle: Option<String>,
    pub disc_number: Option<i32>,
    pub track_number: Option<i32>,
    pub duration_seconds: Option<i32>,
    pub artwork: Option<ScreenArtwork>,
    pub context: ScreenContextHint,
    pub is_favorite: bool,
    pub actions: Vec<ScreenActionHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents browse podcasts response in the authenticated catalog search, browse, and episode resume HTTP API.
///
/// Functionality: Carries fields `podcasts`, `page` for authenticated catalog search, browse, and episode resume HTTP API.
/// Dependencies: depends on `Vec<Podcast>`, `CatalogBrowsePageMetadata` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `tests/maintenance_api.rs`.
pub struct BrowsePodcastsResponse {
    pub podcasts: Vec<Podcast>,
    pub page: CatalogBrowsePageMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents podcast response in the authenticated catalog search, browse, and episode resume HTTP API.
///
/// Functionality: Carries fields `podcast` for authenticated catalog search, browse, and episode resume HTTP API.
/// Dependencies: depends on `Podcast` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `tests/maintenance_api.rs`.
pub struct PodcastResponse {
    pub podcast: Podcast,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents browse episodes response in the authenticated catalog search, browse, and episode resume HTTP API.
///
/// Functionality: Carries fields `episodes`, `page` for authenticated catalog search, browse, and episode resume HTTP API.
/// Dependencies: depends on `Vec<Episode>`, `CatalogBrowsePageMetadata` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `tests/maintenance_api.rs`.
pub struct BrowseEpisodesResponse {
    pub episodes: Vec<Episode>,
    pub page: CatalogBrowsePageMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents episode response in the authenticated catalog search, browse, and episode resume HTTP API.
///
/// Functionality: Carries fields `podcast`, `episode`, `resume` for authenticated catalog search, browse, and episode resume HTTP API.
/// Dependencies: depends on `Podcast`, `Episode`, `Option<PlaybackProgress>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `tests/maintenance_api.rs`.
pub struct EpisodeResponse {
    pub podcast: Podcast,
    pub episode: Episode,
    pub resume: Option<PlaybackProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents episode resume response in the authenticated catalog search, browse, and episode resume HTTP API.
///
/// Functionality: Carries fields `episode_id`, `resume` for authenticated catalog search, browse, and episode resume HTTP API.
/// Dependencies: depends on `Uuid`, `Option<PlaybackProgress>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `tests/maintenance_api.rs`.
pub struct EpisodeResumeResponse {
    pub episode_id: Uuid,
    pub resume: Option<PlaybackProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SearchTrackEntry {
    #[serde(flatten)]
    pub track: Track,
    pub is_favorite: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents catalog search response in the authenticated catalog search, browse, and episode resume HTTP API.
///
/// Functionality: Carries fields `query`, `normalized_query`, `limit`, `artists`, `albums`, `tracks`, `podcasts`, `episodes`, and 1 more for authenticated catalog search, browse, and episode resume HTTP API.
/// Dependencies: depends on `String`, `String`, `u32`, `Vec<Artist>`, `Vec<Album>`, `Vec<SearchTrackEntry>`, and 3 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `tests/maintenance_api.rs`.
pub struct CatalogSearchResponse {
    pub query: String,
    pub normalized_query: String,
    pub limit: u32,
    pub artists: Vec<Artist>,
    pub albums: Vec<Album>,
    pub tracks: Vec<SearchTrackEntry>,
    pub podcasts: Vec<Podcast>,
    pub episodes: Vec<Episode>,
    pub playlists: Vec<Playlist>,
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/search",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(CatalogSearchQuery),
    responses(
        (status = 200, description = "Grouped search results for published stable artists, albums, tracks, podcasts, and visible playlists. Catalog media results support year, genre, format, and media_type filters.", body = CatalogSearchResponse),
        (status = 400, description = "Invalid or empty search query or limit", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Searches resources for catalog browsing and search.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Query(query)`: `Query<CatalogSearchQuery>`; expected to be validated query-string parameters supplied by Axum.
///
/// Output:
/// - Returns `Json<CatalogSearchResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn search_catalog(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Query(query): Query<CatalogSearchQuery>,
) -> Result<Json<CatalogSearchResponse>, ApiError> {
    let results = state
        .search_catalog(
            account.id,
            query.q.as_deref(),
            query.limit,
            query.year,
            query.genre.as_deref(),
            query.format.as_deref(),
            query.media_type.as_deref(),
        )
        .await?;
    let favorite_ids = state.track_favorite_ids_for_account(account.id).await?;

    Ok(Json(CatalogSearchResponse {
        query: results.query,
        normalized_query: results.normalized_query,
        limit: results.limit,
        artists: results.artists,
        albums: results.albums,
        tracks: results
            .tracks
            .into_iter()
            .map(|track| SearchTrackEntry {
                is_favorite: favorite_ids.contains(&track.id),
                track,
            })
            .collect(),
        podcasts: results.podcasts,
        episodes: results.episodes,
        playlists: results.playlists,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/artists",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(CatalogBrowseQuery),
    responses(
        (status = 200, description = "Published artists with at least one published canonical track. Supported sort: name.", body = BrowseArtistsResponse),
        (status = 400, description = "Invalid pagination cursor, limit, or sort", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Returns a paginated browse view for catalog browsing and search.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(_account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Query(query)`: `Query<CatalogBrowseQuery>`; expected to be validated query-string parameters supplied by Axum.
///
/// Output:
/// - Returns `Json<BrowseArtistsResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn browse_artists(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Query(query): Query<CatalogBrowseQuery>,
) -> Result<Json<BrowseArtistsResponse>, ApiError> {
    let page = state
        .browse_artists(
            query.limit,
            query.cursor.as_deref(),
            query.sort.as_deref(),
        )
        .await?;

    Ok(Json(BrowseArtistsResponse {
        page: page_metadata(&page),
        artists: artist_browse_items(&state, account.id, page.items).await?,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/artists/{artist_id}/detail",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(("artist_id" = Uuid, Path, description = "Published artist id")),
    responses(
        (status = 200, description = "Screen-ready artist detail with primary artwork, album groupings, all-track grouping, and action/context hints", body = ArtistDetailResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Artist is not published, not visible, or not found", body = ErrorResponse)
    )
)]
/// Retrieves an artist detail read model for catalog clients.
pub async fn get_artist_detail(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(artist_id): Path<Uuid>,
) -> Result<Json<ArtistDetailResponse>, ApiError> {
    let artist = state.visible_artist(artist_id).await?;
    let albums = state.visible_albums_for_artist(artist.id).await?;
    let tracks = state.visible_tracks_for_artist(artist.id).await?;
    if albums.is_empty() && tracks.is_empty() {
        return Err(ApiError::NotFound(format!("artist {artist_id} was not found")));
    }

    let snapshot_at = Utc::now();
    let favorite_ids = state.track_favorite_ids_for_account(account.id).await?;
    let primary_artwork = primary_artwork(
        &state,
        account.id,
        CatalogEntityType::Artist,
        artist.id,
        ArtworkKind::Artist,
    )
    .await?;
    let metadata = artist_detail_metadata(
        &state
            .visible_metadata_provenance_for_entity(
                account.id,
                CatalogEntityType::Artist,
                artist.id,
            )
            .await?,
    );
    let album_groups =
        artist_album_groups(&state, account.id, &favorite_ids, &artist, albums).await?;
    let all_track_items = detail_track_items(
        &state,
        account.id,
        &favorite_ids,
        &artist,
        None,
        tracks.clone(),
    )
    .await?;

    Ok(Json(ArtistDetailResponse {
        revision: state.current_revision(),
        snapshot_at,
        artist: ArtistDetailHeader {
            id: artist.id,
            name: artist.name.clone(),
            sort_name: artist.sort_name,
        },
        primary_artwork,
        metadata,
        summary: ArtistDetailSummary {
            album_count: album_groups.len(),
            track_count: all_track_items.len(),
            duration_seconds: sum_track_duration(&tracks),
        },
        album_groups,
        track_groups: vec![ArtistTrackGroup {
            id: "all_tracks".to_string(),
            title: "Songs".to_string(),
            items: all_track_items,
        }],
        actions: vec![action_hint(
            "open",
            "GET",
            format!("/api/v1/catalog/artists/{artist_id}/detail"),
        )],
    }))
}

pub(crate) async fn artist_browse_items(
    state: &AppState,
    account_id: Uuid,
    artists: Vec<Artist>,
) -> Result<Vec<ArtistBrowseItem>, ApiError> {
    let mut items = Vec::with_capacity(artists.len());
    for artist in artists {
        let primary_artwork = primary_artwork(
            state,
            account_id,
            CatalogEntityType::Artist,
            artist.id,
            ArtworkKind::Artist,
        )
        .await?;
        items.push(ArtistBrowseItem {
            id: artist.id,
            name: artist.name,
            normalized_name: artist.normalized_name,
            sort_name: artist.sort_name,
            stable_grouping: artist.stable_grouping,
            published_at: artist.published_at,
            created_at: artist.created_at,
            updated_at: artist.updated_at,
            primary_artwork,
        });
    }
    Ok(items)
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/albums",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(CatalogBrowseQuery),
    responses(
        (status = 200, description = "Published albums with at least one published canonical track. Supported sort: artist_title.", body = BrowseAlbumsResponse),
        (status = 400, description = "Invalid pagination cursor, limit, or sort", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Returns a paginated browse view for catalog browsing and search.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(_account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Query(query)`: `Query<CatalogBrowseQuery>`; expected to be validated query-string parameters supplied by Axum.
///
/// Output:
/// - Returns `Json<BrowseAlbumsResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn browse_albums(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
    Query(query): Query<CatalogBrowseQuery>,
) -> Result<Json<BrowseAlbumsResponse>, ApiError> {
    let page = state
        .browse_albums(
            query.limit,
            query.cursor.as_deref(),
            query.sort.as_deref(),
        )
        .await?;

    Ok(Json(BrowseAlbumsResponse {
        page: page_metadata(&page),
        albums: page.items,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/albums/{album_id}/detail",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(("album_id" = Uuid, Path, description = "Published album id")),
    responses(
        (status = 200, description = "Screen-ready album detail with primary artwork, artist link, disc track groupings, and action/context hints", body = AlbumDetailResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Album is not published, not visible, or not found", body = ErrorResponse)
    )
)]
/// Retrieves an album detail read model for catalog clients.
pub async fn get_album_detail(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(album_id): Path<Uuid>,
) -> Result<Json<AlbumDetailResponse>, ApiError> {
    let album = state.visible_album(album_id).await?;
    let artist = state.visible_artist(album.artist_id).await?;
    let tracks = state.visible_tracks_for_album(album.id).await?;
    let snapshot_at = Utc::now();
    let favorite_ids = state.track_favorite_ids_for_account(account.id).await?;
    let primary_artwork = primary_artwork(
        &state,
        account.id,
        CatalogEntityType::Album,
        album.id,
        ArtworkKind::Cover,
    )
    .await?;
    let track_groups =
        album_track_groups(&state, account.id, &favorite_ids, &artist, &album, tracks.clone())
            .await?;

    Ok(Json(AlbumDetailResponse {
        revision: state.current_revision(),
        snapshot_at,
        album: AlbumDetailHeader {
            id: album.id,
            title: album.title.clone(),
            release_year: album.release_year,
            album_kind: album.album_kind,
        },
        artist: ArtistDetailLink {
            id: artist.id,
            name: artist.name,
        },
        primary_artwork,
        summary: AlbumDetailSummary {
            track_count: tracks.len(),
            duration_seconds: sum_track_duration(&tracks),
        },
        track_groups,
        actions: vec![action_hint(
            "open",
            "GET",
            format!("/api/v1/catalog/albums/{album_id}/detail"),
        )],
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/tracks",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(CatalogBrowseQuery),
    responses(
        (status = 200, description = "Published tracks backed by a published canonical media file. Supported sort: album_position.", body = BrowseTracksResponse),
        (status = 400, description = "Invalid pagination cursor, limit, or sort", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Returns a paginated browse view for catalog browsing and search.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(_account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Query(query)`: `Query<CatalogBrowseQuery>`; expected to be validated query-string parameters supplied by Axum.
///
/// Output:
/// - Returns `Json<BrowseTracksResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn browse_tracks(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
    Query(query): Query<CatalogBrowseQuery>,
) -> Result<Json<BrowseTracksResponse>, ApiError> {
    let page = state
        .browse_tracks(
            query.limit,
            query.cursor.as_deref(),
            query.sort.as_deref(),
        )
        .await?;

    Ok(Json(BrowseTracksResponse {
        page: page_metadata(&page),
        tracks: page.items,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/podcasts",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(CatalogBrowseQuery),
    responses(
        (status = 200, description = "Published podcasts with at least one published canonical episode. Supported sort: title.", body = BrowsePodcastsResponse),
        (status = 400, description = "Invalid pagination cursor, limit, or sort", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Returns a paginated browse view for catalog browsing and search.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(_account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Query(query)`: `Query<CatalogBrowseQuery>`; expected to be validated query-string parameters supplied by Axum.
///
/// Output:
/// - Returns `Json<BrowsePodcastsResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn browse_podcasts(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
    Query(query): Query<CatalogBrowseQuery>,
) -> Result<Json<BrowsePodcastsResponse>, ApiError> {
    let page = state
        .browse_podcasts(
            query.limit,
            query.cursor.as_deref(),
            query.sort.as_deref(),
        )
        .await?;

    Ok(Json(BrowsePodcastsResponse {
        page: page_metadata(&page),
        podcasts: page.items,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/podcasts/{podcast_id}",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(("podcast_id" = Uuid, Path, description = "Published podcast series id")),
    responses(
        (status = 200, description = "Published podcast series with at least one visible canonical episode", body = PodcastResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Podcast is not published, not visible, or not found", body = ErrorResponse)
    )
)]
/// Retrieves a resource for catalog browsing and search.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(_account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(podcast_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `Json<PodcastResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn get_podcast(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
    Path(podcast_id): Path<Uuid>,
) -> Result<Json<PodcastResponse>, ApiError> {
    Ok(Json(PodcastResponse {
        podcast: state.visible_podcast(podcast_id).await?,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/episodes",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(CatalogBrowseQuery),
    responses(
        (status = 200, description = "Published episodes backed by a published canonical media file. Supported sort: podcast_position.", body = BrowseEpisodesResponse),
        (status = 400, description = "Invalid pagination cursor, limit, or sort", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Returns a paginated browse view for catalog browsing and search.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(_account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Query(query)`: `Query<CatalogBrowseQuery>`; expected to be validated query-string parameters supplied by Axum.
///
/// Output:
/// - Returns `Json<BrowseEpisodesResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn browse_episodes(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
    Query(query): Query<CatalogBrowseQuery>,
) -> Result<Json<BrowseEpisodesResponse>, ApiError> {
    let page = state
        .browse_episodes(
            query.limit,
            query.cursor.as_deref(),
            query.sort.as_deref(),
        )
        .await?;

    Ok(Json(BrowseEpisodesResponse {
        page: page_metadata(&page),
        episodes: page.items,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/podcasts/{podcast_id}/episodes",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(
        ("podcast_id" = Uuid, Path, description = "Published podcast series id"),
        CatalogBrowseQuery
    ),
    responses(
        (status = 200, description = "Published episodes for one podcast series. Supported sort: podcast_position.", body = BrowseEpisodesResponse),
        (status = 400, description = "Invalid pagination cursor, limit, or sort", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Podcast is not published, not visible, or not found", body = ErrorResponse)
    )
)]
/// Returns a paginated browse view for catalog browsing and search.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(_account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(podcast_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `Query(query)`: `Query<CatalogBrowseQuery>`; expected to be validated query-string parameters supplied by Axum.
///
/// Output:
/// - Returns `Json<BrowseEpisodesResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn browse_podcast_episodes(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
    Path(podcast_id): Path<Uuid>,
    Query(query): Query<CatalogBrowseQuery>,
) -> Result<Json<BrowseEpisodesResponse>, ApiError> {
    let page = state
        .browse_episodes_for_podcast(
            podcast_id,
            query.limit,
            query.cursor.as_deref(),
            query.sort.as_deref(),
        )
        .await?;

    Ok(Json(BrowseEpisodesResponse {
        page: page_metadata(&page),
        episodes: page.items,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/episodes/{episode_id}",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(("episode_id" = Uuid, Path, description = "Published podcast episode id")),
    responses(
        (status = 200, description = "Published podcast episode, its series, and the authenticated user's current resume state when present", body = EpisodeResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Episode is not published, not visible, or not found", body = ErrorResponse)
    )
)]
/// Retrieves a resource for catalog browsing and search.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(episode_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `Json<EpisodeResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn get_episode(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(episode_id): Path<Uuid>,
) -> Result<Json<EpisodeResponse>, ApiError> {
    let item = state.visible_episode(episode_id).await?;
    let resume = state
        .optional_playback_progress_for_item(account.id, PlaybackItemType::Episode, episode_id)
        .await?;

    Ok(Json(EpisodeResponse {
        podcast: item.podcast,
        episode: item.episode,
        resume,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/episodes/{episode_id}/resume",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(("episode_id" = Uuid, Path, description = "Published podcast episode id")),
    responses(
        (status = 200, description = "Authenticated user's resume state for a published podcast episode. The resume field is null when no progress has been saved.", body = EpisodeResumeResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Episode is not published, not visible, or not found", body = ErrorResponse)
    )
)]
/// Retrieves a resource for catalog browsing and search.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(episode_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `Json<EpisodeResumeResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn get_episode_resume(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(episode_id): Path<Uuid>,
) -> Result<Json<EpisodeResumeResponse>, ApiError> {
    state.visible_episode(episode_id).await?;
    let resume = state
        .optional_playback_progress_for_item(account.id, PlaybackItemType::Episode, episode_id)
        .await?;

    Ok(Json(EpisodeResumeResponse { episode_id, resume }))
}

#[utoipa::path(
    put,
    path = "/api/v1/catalog/episodes/{episode_id}/resume",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(("episode_id" = Uuid, Path, description = "Published podcast episode id")),
    request_body = PlaybackProgressWriteRequest,
    responses(
        (status = 200, description = "Episode resume position upserted for the authenticated user and a playback history event recorded", body = PlaybackProgressWriteResponse),
        (status = 400, description = "Invalid progress data", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Episode is not published, not visible, or not found", body = ErrorResponse)
    )
)]
/// Writes data for catalog browsing and search.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(episode_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `Json(request)`: `Json<PlaybackProgressWriteRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `Json<PlaybackProgressWriteResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn write_episode_resume(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(episode_id): Path<Uuid>,
    Json(request): Json<PlaybackProgressWriteRequest>,
) -> Result<Json<PlaybackProgressWriteResponse>, ApiError> {
    state.visible_episode(episode_id).await?;
    let progress = state
        .upsert_playback_progress(
            account.id,
            PlaybackItemType::Episode,
            episode_id,
            request.context_type,
            request.context_id,
            request.position_seconds,
            request.duration_seconds,
            request.completed,
        )
        .await?;
    let history_event = state
        .insert_playback_history_event(
            account.id,
            PlaybackItemType::Episode,
            episode_id,
            request.context_type,
            request.context_id,
            request.position_seconds,
            request.duration_seconds,
            request.completed,
        )
        .await?;

    Ok(Json(PlaybackProgressWriteResponse {
        progress,
        history_event,
    }))
}

async fn artist_album_groups(
    state: &AppState,
    account_id: Uuid,
    favorite_ids: &HashSet<Uuid>,
    artist: &Artist,
    albums: Vec<Album>,
) -> Result<Vec<ArtistAlbumGroup>, ApiError> {
    let mut groups = Vec::new();
    for album in albums {
        let tracks = state.visible_tracks_for_album(album.id).await?;
        let primary_artwork = primary_artwork(
            state,
            account_id,
            CatalogEntityType::Album,
            album.id,
            ArtworkKind::Cover,
        )
        .await?;
        let track_items = detail_track_items(
            state,
            account_id,
            favorite_ids,
            artist,
            Some(&album),
            tracks.clone(),
        )
        .await?;
        groups.push(ArtistAlbumGroup {
            id: album.id,
            title: album.title.clone(),
            subtitle: Some(artist.name.clone()),
            release_year: album.release_year,
            album_kind: album.album_kind,
            primary_artwork,
            track_count: tracks.len(),
            duration_seconds: sum_track_duration(&tracks),
            tracks: track_items,
            actions: vec![action_hint(
                "open",
                "GET",
                format!("/api/v1/catalog/albums/{}/detail", album.id),
            )],
        });
    }
    Ok(groups)
}

async fn album_track_groups(
    state: &AppState,
    account_id: Uuid,
    favorite_ids: &HashSet<Uuid>,
    artist: &Artist,
    album: &Album,
    tracks: Vec<Track>,
) -> Result<Vec<AlbumTrackGroup>, ApiError> {
    let mut grouped: BTreeMap<Option<i32>, Vec<Track>> = BTreeMap::new();
    for track in tracks {
        grouped.entry(track.disc_number).or_default().push(track);
    }

    let multi_disc = grouped.len() > 1
        || grouped
            .keys()
            .any(|disc| matches!(disc, Some(n) if *n > 1));
    let mut groups = Vec::new();
    for (disc_number, tracks) in grouped {
        groups.push(AlbumTrackGroup {
            id: disc_number
                .map(|number| format!("disc_{number}"))
                .unwrap_or_else(|| "main".to_string()),
            title: match (multi_disc, disc_number) {
                (true, Some(number)) => format!("Disc {number}"),
                _ => "Tracks".to_string(),
            },
            disc_number,
            items: detail_track_items(
                state,
                account_id,
                favorite_ids,
                artist,
                Some(album),
                tracks,
            )
            .await?,
        });
    }
    Ok(groups)
}

async fn detail_track_items(
    state: &AppState,
    account_id: Uuid,
    favorite_ids: &HashSet<Uuid>,
    artist: &Artist,
    album: Option<&Album>,
    tracks: Vec<Track>,
) -> Result<Vec<DetailTrackItem>, ApiError> {
    let mut items = Vec::new();
    for track in tracks {
        let track_album = match album {
            Some(album) => album.clone(),
            None => match state.visible_album(track.album_id).await {
                Ok(album) => album,
                Err(ApiError::NotFound(_)) => continue,
                Err(error) => return Err(error),
            },
        };
        let artwork = primary_artwork(
            state,
            account_id,
            CatalogEntityType::Album,
            track_album.id,
            ArtworkKind::Cover,
        )
        .await?;
        items.push(DetailTrackItem {
            id: track.id,
            item_type: PlaybackItemType::Track,
            title: track.title,
            subtitle: Some(format!("{} - {}", artist.name, track_album.title)),
            disc_number: track.disc_number,
            track_number: track.track_number,
            duration_seconds: track.duration_seconds,
            artwork,
            context: ScreenContextHint {
                entity_type: CatalogEntityType::Album,
                entity_id: track_album.id,
                title: track_album.title,
            },
            is_favorite: favorite_ids.contains(&track.id),
            actions: vec![action_hint(
                "play",
                "GET",
                format!("/api/v1/media/track/{}/original", track.id),
            )],
        });
    }
    Ok(items)
}

fn sum_track_duration(tracks: &[Track]) -> Option<i32> {
    let total = tracks
        .iter()
        .filter_map(|track| track.duration_seconds)
        .filter(|duration| *duration > 0)
        .sum::<i32>();
    (total > 0).then_some(total)
}

fn artist_detail_metadata(provenance: &[MetadataProvenance]) -> ArtistDetailMetadata {
    let mut genres_by_key = BTreeMap::new();
    for field in ["genre", "genres"] {
        for row in provenance
            .iter()
            .filter(|row| row.field_name.eq_ignore_ascii_case(field))
        {
            for value in json_strings(&row.value) {
                genres_by_key
                    .entry(value.to_ascii_lowercase())
                    .or_insert(value);
            }
        }
    }

    let mut links = Vec::new();
    for (field, kind) in [
        ("website", "website"),
        ("facebook", "facebook"),
        ("twitter", "twitter"),
        ("lastfm", "lastfm"),
        ("last_fm", "lastfm"),
    ] {
        if let Some(url) = best_text_field(provenance, &[field]) {
            links.push(ArtistExternalLink {
                kind: kind.to_string(),
                url,
            });
        }
    }
    links = dedupe_links(links);

    ArtistDetailMetadata {
        description: best_text_field(provenance, &["description", "biography", "bio"]),
        genres: genres_by_key.into_values().collect(),
        style: best_text_field(provenance, &["style"]),
        mood: best_text_field(provenance, &["mood"]),
        label: best_text_field(provenance, &["label"]),
        links,
    }
}

fn best_text_field(provenance: &[MetadataProvenance], fields: &[&str]) -> Option<String> {
    let mut best: Option<(&MetadataProvenance, String)> = None;
    for row in provenance.iter().filter(|row| {
        fields
            .iter()
            .any(|field| row.field_name.eq_ignore_ascii_case(field))
    }) {
        let Some(value) = json_text(&row.value) else {
            continue;
        };
        if best
            .as_ref()
            .map(|(current, _)| metadata_candidate_is_better(row, current))
            .unwrap_or(true)
        {
            best = Some((row, value));
        }
    }
    best.map(|(_, value)| value)
}

fn metadata_candidate_is_better(
    candidate: &MetadataProvenance,
    current: &MetadataProvenance,
) -> bool {
    candidate.confidence > current.confidence
        || ((candidate.confidence - current.confidence).abs() < f32::EPSILON
            && candidate.created_at > current.created_at)
}

fn json_text(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(text) => non_empty_text(text),
        serde_json::Value::Number(number) => Some(number.to_string()),
        _ => None,
    }
}

fn json_strings(value: &serde_json::Value) -> Vec<String> {
    match value {
        serde_json::Value::String(text) => non_empty_text(text).into_iter().collect(),
        serde_json::Value::Array(values) => values.iter().filter_map(json_text).collect(),
        _ => Vec::new(),
    }
}

fn non_empty_text(value: &str) -> Option<String> {
    let text = value.trim();
    (!text.is_empty()).then(|| text.to_string())
}

fn dedupe_links(links: Vec<ArtistExternalLink>) -> Vec<ArtistExternalLink> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for link in links {
        let key = (link.kind.to_ascii_lowercase(), link.url.to_ascii_lowercase());
        if seen.insert(key) {
            deduped.push(link);
        }
    }
    deduped
}

/// Handles page metadata for catalog browsing and search.
///
/// Inputs:
/// - `page`: `&CatalogBrowsePage<T>`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `CatalogBrowsePageMetadata` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn page_metadata<T>(page: &CatalogBrowsePage<T>) -> CatalogBrowsePageMetadata {
    CatalogBrowsePageMetadata {
        limit: page.limit,
        next_cursor: page.next_cursor.clone(),
        sort: page.sort.clone(),
    }
}
