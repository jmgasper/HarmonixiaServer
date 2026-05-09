use std::str::FromStr;

use axum::{
    extract::{Path, Query, State},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{
    auth::AuthenticatedUser,
    domain::{PlaybackHistoryEvent, PlaybackItemType, PlaybackProgress},
    error::{ApiError, ErrorResponse},
    state::AppState,
};

/// Builds the Axum router for playback progress and history.
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
        .route("/progress", get(list_progress))
        .route(
            "/progress/:item_type/:item_id",
            get(get_progress).put(write_progress),
        )
        .route("/history", get(list_history).post(write_history))
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
/// Represents playback progress write request in the authenticated playback progress and history HTTP API.
///
/// Functionality: Carries fields `position_seconds`, `duration_seconds`, `completed` for authenticated playback progress and history HTTP API.
/// Dependencies: depends on `u32`, `Option<u32>`, `bool` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `src/api/playback.rs`, `tests/maintenance_api.rs`.
pub struct PlaybackProgressWriteRequest {
    pub position_seconds: u32,
    pub duration_seconds: Option<u32>,
    #[serde(default)]
    pub completed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents playback progress write response in the authenticated playback progress and history HTTP API.
///
/// Functionality: Carries fields `progress`, `history_event` for authenticated playback progress and history HTTP API.
/// Dependencies: depends on `PlaybackProgress`, `PlaybackHistoryEvent` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `src/api/playback.rs`, `tests/maintenance_api.rs`.
pub struct PlaybackProgressWriteResponse {
    pub progress: PlaybackProgress,
    pub history_event: PlaybackHistoryEvent,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
/// Represents playback history write request in the authenticated playback progress and history HTTP API.
///
/// Functionality: Carries fields `item_type`, `item_id`, `position_seconds`, `duration_seconds`, `completed` for authenticated playback progress and history HTTP API.
/// Dependencies: depends on `PlaybackItemType`, `Uuid`, `u32`, `Option<u32>`, `bool` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/api/playback.rs`.
pub struct PlaybackHistoryWriteRequest {
    pub item_type: PlaybackItemType,
    pub item_id: Uuid,
    pub position_seconds: u32,
    pub duration_seconds: Option<u32>,
    #[serde(default)]
    pub completed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents playback progress response in the authenticated playback progress and history HTTP API.
///
/// Functionality: Carries fields `progress` for authenticated playback progress and history HTTP API.
/// Dependencies: depends on `Vec<PlaybackProgress>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/api/playback.rs`.
pub struct PlaybackProgressResponse {
    pub progress: Vec<PlaybackProgress>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents playback history response in the authenticated playback progress and history HTTP API.
///
/// Functionality: Carries fields `history` for authenticated playback progress and history HTTP API.
/// Dependencies: depends on `Vec<PlaybackHistoryEvent>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/api/playback.rs`.
pub struct PlaybackHistoryResponse {
    pub history: Vec<PlaybackHistoryEvent>,
}

#[derive(Debug, Clone, Deserialize, IntoParams, ToSchema)]
/// Represents playback history query in the authenticated playback progress and history HTTP API.
///
/// Functionality: Carries fields `limit` for authenticated playback progress and history HTTP API.
/// Dependencies: depends on `Option<u32>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/api/playback.rs`.
pub struct PlaybackHistoryQuery {
    pub limit: Option<u32>,
}

#[utoipa::path(
    put,
    path = "/api/v1/me/playback/progress/{item_type}/{item_id}",
    tag = "playback",
    security(("basicAuth" = [])),
    params(
        ("item_type" = String, Path, description = "Playback item type: track or episode"),
        ("item_id" = Uuid, Path, description = "Catalog item id")
    ),
    request_body = PlaybackProgressWriteRequest,
    responses(
        (status = 200, description = "Progress upserted and a history event recorded", body = PlaybackProgressWriteResponse),
        (status = 400, description = "Invalid progress data", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Writes data for playback progress and history.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path((item_type, item_id))`: `Path<(String, Uuid)>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `Json(request)`: `Json<PlaybackProgressWriteRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `Json<PlaybackProgressWriteResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn write_progress(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path((item_type, item_id)): Path<(String, Uuid)>,
    Json(request): Json<PlaybackProgressWriteRequest>,
) -> Result<Json<PlaybackProgressWriteResponse>, ApiError> {
    let item_type = parse_playback_item_type(&item_type)?;
    let progress = state
        .upsert_playback_progress(
            account.id,
            item_type,
            item_id,
            request.position_seconds,
            request.duration_seconds,
            request.completed,
        )
        .await?;
    let history_event = state
        .insert_playback_history_event(
            account.id,
            item_type,
            item_id,
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

#[utoipa::path(
    get,
    path = "/api/v1/me/playback/progress",
    tag = "playback",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Progress records for the authenticated account", body = PlaybackProgressResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Lists resources for playback progress and history.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Json<PlaybackProgressResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn list_progress(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
) -> Result<Json<PlaybackProgressResponse>, ApiError> {
    Ok(Json(PlaybackProgressResponse {
        progress: state.playback_progress_for_account(account.id).await?,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/me/playback/progress/{item_type}/{item_id}",
    tag = "playback",
    security(("basicAuth" = [])),
    params(
        ("item_type" = String, Path, description = "Playback item type: track or episode"),
        ("item_id" = Uuid, Path, description = "Catalog item id")
    ),
    responses(
        (status = 200, description = "Progress record for one catalog item", body = PlaybackProgress),
        (status = 400, description = "Invalid item type", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Progress record not found", body = ErrorResponse)
    )
)]
/// Retrieves a resource for playback progress and history.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path((item_type, item_id))`: `Path<(String, Uuid)>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `Json<PlaybackProgress>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn get_progress(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path((item_type, item_id)): Path<(String, Uuid)>,
) -> Result<Json<PlaybackProgress>, ApiError> {
    let item_type = parse_playback_item_type(&item_type)?;
    Ok(Json(
        state
            .playback_progress_for_item(account.id, item_type, item_id)
            .await?,
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/me/playback/history",
    tag = "playback",
    security(("basicAuth" = [])),
    request_body = PlaybackHistoryWriteRequest,
    responses(
        (status = 200, description = "Playback history event recorded", body = PlaybackHistoryEvent),
        (status = 400, description = "Invalid history data", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Writes data for playback progress and history.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Json(request)`: `Json<PlaybackHistoryWriteRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `Json<PlaybackHistoryEvent>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn write_history(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Json(request): Json<PlaybackHistoryWriteRequest>,
) -> Result<Json<PlaybackHistoryEvent>, ApiError> {
    Ok(Json(
        state
            .insert_playback_history_event(
                account.id,
                request.item_type,
                request.item_id,
                request.position_seconds,
                request.duration_seconds,
                request.completed,
            )
            .await?,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/me/playback/history",
    tag = "playback",
    security(("basicAuth" = [])),
    params(PlaybackHistoryQuery),
    responses(
        (status = 200, description = "Playback history for the authenticated account", body = PlaybackHistoryResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Lists resources for playback progress and history.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Query(query)`: `Query<PlaybackHistoryQuery>`; expected to be validated query-string parameters supplied by Axum.
///
/// Output:
/// - Returns `Json<PlaybackHistoryResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn list_history(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Query(query): Query<PlaybackHistoryQuery>,
) -> Result<Json<PlaybackHistoryResponse>, ApiError> {
    Ok(Json(PlaybackHistoryResponse {
        history: state
            .playback_history_for_account(account.id, query.limit.unwrap_or(50))
            .await?,
    }))
}

/// Parses and validates input for playback progress and history.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `PlaybackItemType` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_playback_item_type(value: &str) -> Result<PlaybackItemType, ApiError> {
    PlaybackItemType::from_str(value)
        .map_err(|_| ApiError::BadRequest(format!("unknown playback item type: {value}")))
}
