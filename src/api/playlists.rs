use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    auth::AuthenticatedUser,
    domain::{PlaybackItemType, Playlist, PlaylistItem, PlaylistScope},
    error::{ApiError, ErrorResponse},
    state::AppState,
};

/// Builds the Axum router for playlist management.
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
        .route("/", get(list_playlists).post(create_playlist))
        .route(
            "/:playlist_id",
            get(get_playlist).put(update_playlist).delete(delete_playlist),
        )
        .route(
            "/:playlist_id/items",
            get(list_playlist_items)
                .post(add_playlist_item)
                .put(reorder_playlist_items),
        )
        .route(
            "/:playlist_id/items/:playlist_item_id",
            delete(remove_playlist_item),
        )
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
/// Represents create playlist request in the authenticated playlist CRUD and playlist item HTTP API.
///
/// Functionality: Carries fields `name`, `description`, `scope` for authenticated playlist CRUD and playlist item HTTP API.
/// Dependencies: depends on `String`, `Option<String>`, `PlaylistScope` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/api/playlists.rs`.
pub struct CreatePlaylistRequest {
    pub name: String,
    pub description: Option<String>,
    pub scope: PlaylistScope,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
/// Represents update playlist request in the authenticated playlist CRUD and playlist item HTTP API.
///
/// Functionality: Carries fields `name`, `description` for authenticated playlist CRUD and playlist item HTTP API.
/// Dependencies: depends on `String`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/api/playlists.rs`.
pub struct UpdatePlaylistRequest {
    pub name: String,
    pub description: Option<String>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
/// Represents add playlist item request in the authenticated playlist CRUD and playlist item HTTP API.
///
/// Functionality: Carries fields `item_type`, `item_id`, `position` for authenticated playlist CRUD and playlist item HTTP API.
/// Dependencies: depends on `PlaybackItemType`, `Uuid`, `Option<u32>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/api/playlists.rs`.
pub struct AddPlaylistItemRequest {
    pub item_type: PlaybackItemType,
    pub item_id: Uuid,
    pub position: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
/// Represents reorder playlist items request in the authenticated playlist CRUD and playlist item HTTP API.
///
/// Functionality: Carries fields `item_ids` for authenticated playlist CRUD and playlist item HTTP API.
/// Dependencies: depends on `Vec<Uuid>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/api/playlists.rs`.
pub struct ReorderPlaylistItemsRequest {
    pub item_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents playlists response in the authenticated playlist CRUD and playlist item HTTP API.
///
/// Functionality: Carries fields `playlists` for authenticated playlist CRUD and playlist item HTTP API.
/// Dependencies: depends on `Vec<Playlist>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/api/playlists.rs`.
pub struct PlaylistsResponse {
    pub playlists: Vec<Playlist>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents playlist items response in the authenticated playlist CRUD and playlist item HTTP API.
///
/// Functionality: Carries fields `items` for authenticated playlist CRUD and playlist item HTTP API.
/// Dependencies: depends on `Vec<PlaylistItem>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/api/playlists.rs`.
pub struct PlaylistItemsResponse {
    pub items: Vec<PlaylistItem>,
}

#[utoipa::path(
    get,
    path = "/api/v1/playlists",
    tag = "playlists",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Personal and shared playlists visible to the authenticated account", body = PlaylistsResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Lists resources for playlist management.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Json<PlaylistsResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn list_playlists(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
) -> Result<Json<PlaylistsResponse>, ApiError> {
    Ok(Json(PlaylistsResponse {
        playlists: state.playlists_visible_to(account.id).await?,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/playlists",
    tag = "playlists",
    security(("basicAuth" = [])),
    request_body = CreatePlaylistRequest,
    responses(
        (status = 201, description = "Playlist created", body = Playlist),
        (status = 400, description = "Invalid playlist data", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Creates a new resource for playlist management.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Json(request)`: `Json<CreatePlaylistRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `(StatusCode, Json<Playlist>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn create_playlist(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Json(request): Json<CreatePlaylistRequest>,
) -> Result<(StatusCode, Json<Playlist>), ApiError> {
    let playlist = state
        .create_playlist(
            account.id,
            &request.name,
            request.description.as_deref(),
            request.scope,
        )
        .await?;

    Ok((StatusCode::CREATED, Json(playlist)))
}

#[utoipa::path(
    get,
    path = "/api/v1/playlists/{playlist_id}",
    tag = "playlists",
    security(("basicAuth" = [])),
    params(("playlist_id" = Uuid, Path, description = "Playlist id")),
    responses(
        (status = 200, description = "Visible playlist", body = Playlist),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Playlist not found or not visible to this account", body = ErrorResponse)
    )
)]
/// Retrieves a resource for playlist management.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(playlist_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `Json<Playlist>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn get_playlist(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(playlist_id): Path<Uuid>,
) -> Result<Json<Playlist>, ApiError> {
    Ok(Json(state.visible_playlist(account.id, playlist_id).await?))
}

#[utoipa::path(
    put,
    path = "/api/v1/playlists/{playlist_id}",
    tag = "playlists",
    security(("basicAuth" = [])),
    params(("playlist_id" = Uuid, Path, description = "Playlist id")),
    request_body = UpdatePlaylistRequest,
    responses(
        (status = 200, description = "Playlist updated", body = Playlist),
        (status = 400, description = "Invalid playlist data", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Playlist not found or not visible to this account", body = ErrorResponse)
    )
)]
/// Updates existing state for playlist management.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(playlist_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `Json(request)`: `Json<UpdatePlaylistRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `Json<Playlist>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn update_playlist(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(playlist_id): Path<Uuid>,
    Json(request): Json<UpdatePlaylistRequest>,
) -> Result<Json<Playlist>, ApiError> {
    Ok(Json(
        state
            .update_visible_playlist(
                account.id,
                playlist_id,
                &request.name,
                request.description.as_deref(),
            )
            .await?,
    ))
}

#[utoipa::path(
    delete,
    path = "/api/v1/playlists/{playlist_id}",
    tag = "playlists",
    security(("basicAuth" = [])),
    params(("playlist_id" = Uuid, Path, description = "Playlist id")),
    responses(
        (status = 204, description = "Playlist deleted"),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Playlist not found or not visible to this account", body = ErrorResponse)
    )
)]
/// Deletes or removes a resource from playlist management.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(playlist_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `StatusCode` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn delete_playlist(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(playlist_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state.delete_visible_playlist(account.id, playlist_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(
    get,
    path = "/api/v1/playlists/{playlist_id}/items",
    tag = "playlists",
    security(("basicAuth" = [])),
    params(("playlist_id" = Uuid, Path, description = "Playlist id")),
    responses(
        (status = 200, description = "Playlist membership ordered by zero-based position. Personal playlists are visible only to their owner; shared playlists are household-visible.", body = PlaylistItemsResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Playlist not found or not visible to this account", body = ErrorResponse)
    )
)]
/// Lists resources for playlist management.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(playlist_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `Json<PlaylistItemsResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn list_playlist_items(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(playlist_id): Path<Uuid>,
) -> Result<Json<PlaylistItemsResponse>, ApiError> {
    Ok(Json(PlaylistItemsResponse {
        items: state
            .list_visible_playlist_items(account.id, playlist_id)
            .await?,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/playlists/{playlist_id}/items",
    tag = "playlists",
    security(("basicAuth" = [])),
    params(("playlist_id" = Uuid, Path, description = "Playlist id")),
    request_body = AddPlaylistItemRequest,
    responses(
        (status = 201, description = "Playlist item appended or inserted. Omit position to append; provide a zero-based position from 0 through the current length to insert before that position.", body = PlaylistItem),
        (status = 400, description = "Invalid position or item is not a published playlist-eligible track or episode", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Playlist not found or not visible to this account", body = ErrorResponse)
    )
)]
/// Handles add playlist item for playlist management.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(playlist_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `Json(request)`: `Json<AddPlaylistItemRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `(StatusCode, Json<PlaylistItem>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn add_playlist_item(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(playlist_id): Path<Uuid>,
    Json(request): Json<AddPlaylistItemRequest>,
) -> Result<(StatusCode, Json<PlaylistItem>), ApiError> {
    let item = state
        .add_visible_playlist_item(
            account.id,
            playlist_id,
            request.item_type,
            request.item_id,
            request.position,
        )
        .await?;

    Ok((StatusCode::CREATED, Json(item)))
}

#[utoipa::path(
    put,
    path = "/api/v1/playlists/{playlist_id}/items",
    tag = "playlists",
    security(("basicAuth" = [])),
    params(("playlist_id" = Uuid, Path, description = "Playlist id")),
    request_body = ReorderPlaylistItemsRequest,
    responses(
        (status = 200, description = "Playlist items reordered to match item_ids exactly. The array must contain every current playlist item id exactly once.", body = PlaylistItemsResponse),
        (status = 400, description = "Reorder body is not an exact permutation of current playlist items", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Playlist not found or not visible to this account", body = ErrorResponse)
    )
)]
/// Handles reorder playlist items for playlist management.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(playlist_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `Json(request)`: `Json<ReorderPlaylistItemsRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `Json<PlaylistItemsResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn reorder_playlist_items(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(playlist_id): Path<Uuid>,
    Json(request): Json<ReorderPlaylistItemsRequest>,
) -> Result<Json<PlaylistItemsResponse>, ApiError> {
    Ok(Json(PlaylistItemsResponse {
        items: state
            .reorder_visible_playlist_items(account.id, playlist_id, request.item_ids)
            .await?,
    }))
}

#[utoipa::path(
    delete,
    path = "/api/v1/playlists/{playlist_id}/items/{playlist_item_id}",
    tag = "playlists",
    security(("basicAuth" = [])),
    params(
        ("playlist_id" = Uuid, Path, description = "Playlist id"),
        ("playlist_item_id" = Uuid, Path, description = "Playlist item id")
    ),
    responses(
        (status = 204, description = "Playlist item removed and remaining items resequenced contiguously"),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Playlist or playlist item not found or not visible to this account", body = ErrorResponse)
    )
)]
/// Handles remove playlist item for playlist management.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path((playlist_id, playlist_item_id))`: `Path<(Uuid, Uuid)>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `StatusCode` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn remove_playlist_item(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path((playlist_id, playlist_item_id)): Path<(Uuid, Uuid)>,
) -> Result<StatusCode, ApiError> {
    state
        .remove_visible_playlist_item(account.id, playlist_id, playlist_item_id)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}
