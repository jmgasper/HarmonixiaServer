use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{
    api::playback::{PlaybackProgressWriteRequest, PlaybackProgressWriteResponse},
    auth::AuthenticatedUser,
    catalog::CatalogBrowsePage,
    domain::{
        Album, Artist, Episode, PlaybackItemType, PlaybackProgress, Playlist, Podcast,
        Track,
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
        .route("/albums", get(browse_albums))
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
    pub artists: Vec<Artist>,
    pub page: CatalogBrowsePageMetadata,
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
/// Represents catalog search response in the authenticated catalog search, browse, and episode resume HTTP API.
///
/// Functionality: Carries fields `query`, `normalized_query`, `limit`, `artists`, `albums`, `tracks`, `podcasts`, `episodes`, and 1 more for authenticated catalog search, browse, and episode resume HTTP API.
/// Dependencies: depends on `String`, `String`, `u32`, `Vec<Artist>`, `Vec<Album>`, `Vec<Track>`, and 3 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `tests/maintenance_api.rs`.
pub struct CatalogSearchResponse {
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

    Ok(Json(CatalogSearchResponse {
        query: results.query,
        normalized_query: results.normalized_query,
        limit: results.limit,
        artists: results.artists,
        albums: results.albums,
        tracks: results.tracks,
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
    AuthenticatedUser(_account): AuthenticatedUser,
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
        artists: page.items,
    }))
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
