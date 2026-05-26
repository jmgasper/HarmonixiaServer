use std::str::FromStr;

use axum::{
    extract::{Path, Query, State},
    response::Response,
    routing::{get, patch, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{
    api::sonos::map_sonos_operation_error,
    auth::AuthenticatedUser,
    domain::{
        AuthenticatedAccount, PlaybackContextType, PlaybackHistoryEvent, PlaybackItemType,
        PlaybackProgress, PlaybackRepeatMode, PlaybackSessionState,
    },
    error::{ApiError, ErrorResponse, SonosErrorReason},
    sonos::SonosOperationError,
    state::{
        AppState, PlaybackSessionQueueItem, PlaybackSessionQueueReplacement,
        PlaybackSessionReadModel, PlaybackSessionStateMutation, PlaybackSessionTarget,
    },
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
        .route("/session", get(get_session))
        .route("/session/attach", post(attach_session))
        .route("/session/detach", post(detach_session))
        .route("/session/queue", put(replace_session_queue))
        .route("/session/state", patch(update_session_state))
        .route("/session/transfer", post(request_session_transfer))
        .route("/session/transfer/confirm", post(confirm_session_transfer))
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
    pub context_type: Option<PlaybackContextType>,
    pub context_id: Option<Uuid>,
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
    pub context_type: Option<PlaybackContextType>,
    pub context_id: Option<Uuid>,
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

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PlaybackSessionAttachRequest {
    pub attachment_id: Option<Uuid>,
    pub device_id: Option<String>,
    pub device_name: Option<String>,
    pub app_instance_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PlaybackSessionQueueReplaceRequest {
    pub queue: Vec<PlaybackSessionQueueItem>,
    pub context_type: Option<PlaybackContextType>,
    pub context_id: Option<Uuid>,
    pub current_index: Option<u32>,
    pub playback_state: Option<PlaybackSessionState>,
    pub position_seconds: Option<u32>,
    pub duration_seconds: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PlaybackSessionStatePatchRequest {
    pub playback_state: Option<PlaybackSessionState>,
    pub current_index: Option<u32>,
    pub position_seconds: Option<u32>,
    pub duration_seconds: Option<u32>,
    pub repeat_mode: Option<PlaybackRepeatMode>,
    pub shuffle: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PlaybackSessionTransferRequest {
    pub target: PlaybackSessionTarget,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct PlaybackSessionTransferConfirmRequest {
    pub transfer_id: Uuid,
}

#[utoipa::path(
    get,
    path = "/api/v1/me/playback/session",
    tag = "playback",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Authoritative shared playback session snapshot", body = PlaybackSessionReadModel),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
pub async fn get_session(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
) -> Result<Json<PlaybackSessionReadModel>, ApiError> {
    Ok(Json(state.playback_session_for_account(account.id)))
}

#[utoipa::path(
    post,
    path = "/api/v1/me/playback/session/attach",
    tag = "playback",
    security(("basicAuth" = [])),
    request_body = PlaybackSessionAttachRequest,
    responses(
        (status = 200, description = "Local Android controller attached to the shared session", body = PlaybackSessionReadModel),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
pub async fn attach_session(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Json(request): Json<PlaybackSessionAttachRequest>,
) -> Result<Json<PlaybackSessionReadModel>, ApiError> {
    Ok(Json(state.attach_playback_session(
        account.id,
        request.attachment_id,
        request.device_id,
        request.device_name,
        request.app_instance_id,
    )))
}

#[utoipa::path(
    post,
    path = "/api/v1/me/playback/session/detach",
    tag = "playback",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Local Android controller detached without stopping Sonos playback", body = PlaybackSessionReadModel),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
pub async fn detach_session(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
) -> Result<Json<PlaybackSessionReadModel>, ApiError> {
    Ok(Json(state.detach_playback_session(account.id)))
}

#[utoipa::path(
    put,
    path = "/api/v1/me/playback/session/queue",
    tag = "playback",
    security(("basicAuth" = [])),
    request_body = PlaybackSessionQueueReplaceRequest,
    responses(
        (status = 200, description = "Playback session queue replaced", body = PlaybackSessionReadModel),
        (status = 400, description = "Invalid queue or current index", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
pub async fn replace_session_queue(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Json(request): Json<PlaybackSessionQueueReplaceRequest>,
) -> Result<Json<PlaybackSessionReadModel>, Response> {
    let replacement = PlaybackSessionQueueReplacement {
        queue: request.queue,
        context_type: request.context_type,
        context_id: request.context_id,
        current_index: request.current_index,
        playback_state: request.playback_state,
        position_seconds: request.position_seconds,
        duration_seconds: request.duration_seconds,
    };
    let preview = state
        .preview_playback_session_queue_replacement(account.id, &replacement)
        .map_err(axum::response::IntoResponse::into_response)?;
    if active_sonos_target_id(&preview).is_some() {
        apply_active_sonos_full_snapshot(&state, &account, &preview).await?;
        return state
            .commit_playback_session_snapshot(account.id, preview, "queue_replaced")
            .map(Json)
            .map_err(axum::response::IntoResponse::into_response);
    }
    state
        .replace_playback_session_queue(account.id, replacement)
        .map(Json)
        .map_err(axum::response::IntoResponse::into_response)
}

#[utoipa::path(
    patch,
    path = "/api/v1/me/playback/session/state",
    tag = "playback",
    security(("basicAuth" = [])),
    request_body = PlaybackSessionStatePatchRequest,
    responses(
        (status = 200, description = "Playback session state updated", body = PlaybackSessionReadModel),
        (status = 400, description = "Invalid playback state data", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
pub async fn update_session_state(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Json(request): Json<PlaybackSessionStatePatchRequest>,
) -> Result<Json<PlaybackSessionReadModel>, Response> {
    let should_persist_progress =
        request.position_seconds.is_some() || request.duration_seconds.is_some();
    let mutation = PlaybackSessionStateMutation {
        playback_state: request.playback_state,
        current_index: request.current_index,
        position_seconds: request.position_seconds,
        duration_seconds: request.duration_seconds,
        repeat_mode: request.repeat_mode,
        shuffle: request.shuffle,
    };
    let snapshot = if should_apply_sonos_state_mutation(&request) {
        let preview = state
            .preview_playback_session_state_update(account.id, &mutation)
            .map_err(axum::response::IntoResponse::into_response)?;
        if active_sonos_target_id(&preview).is_some() {
            apply_active_sonos_state_mutation(&state, &account, &preview, &request).await?;
            state
                .commit_playback_session_snapshot(account.id, preview, "state_updated")
                .map_err(axum::response::IntoResponse::into_response)?
        } else {
            state
                .update_playback_session_state(account.id, mutation)
                .map_err(axum::response::IntoResponse::into_response)?
        }
    } else {
        state
            .update_playback_session_state(account.id, mutation)
            .map_err(axum::response::IntoResponse::into_response)?
    };
    if should_persist_progress {
        persist_session_progress(&state, account.id, &snapshot)
            .await
            .map_err(axum::response::IntoResponse::into_response)?;
    }
    Ok(Json(snapshot))
}

#[utoipa::path(
    post,
    path = "/api/v1/me/playback/session/transfer",
    tag = "playback",
    security(("basicAuth" = [])),
    request_body = PlaybackSessionTransferRequest,
    responses(
        (status = 200, description = "Playback transfer requested or confirmed after destination startup", body = PlaybackSessionReadModel),
        (status = 400, description = "Invalid transfer request", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 409, description = "Transfer cannot be applied to the current session", body = ErrorResponse),
        (status = 503, description = "Sonos destination could not be started", body = ErrorResponse)
    )
)]
pub async fn request_session_transfer(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Json(request): Json<PlaybackSessionTransferRequest>,
) -> Result<Json<PlaybackSessionReadModel>, Response> {
    let account = user.0;
    let (transfer_id, pending_snapshot) = state
        .request_playback_session_transfer(account.id, request.target.clone())
        .map_err(axum::response::IntoResponse::into_response)?;

    match request.target {
        PlaybackSessionTarget::LocalAndroid { .. } => Ok(Json(pending_snapshot)),
        PlaybackSessionTarget::Sonos { target_id } => {
            let result = state
                .sonos_play_session_snapshot(target_id.clone(), account.clone(), &pending_snapshot)
                .await;
            match result {
                Ok(_) => state
                    .confirm_playback_session_transfer_to_sonos(
                        account.id,
                        transfer_id,
                        &target_id,
                    )
                    .map(Json)
                    .map_err(axum::response::IntoResponse::into_response),
                Err(error) => {
                    let _ = state.fail_playback_session_transfer(
                        account.id,
                        transfer_id,
                        error.to_string(),
                    );
                    Err(map_sonos_operation_error(error))
                }
            }
        }
    }
}

#[utoipa::path(
    post,
    path = "/api/v1/me/playback/session/transfer/confirm",
    tag = "playback",
    security(("basicAuth" = [])),
    request_body = PlaybackSessionTransferConfirmRequest,
    responses(
        (status = 200, description = "Pending playback transfer confirmed", body = PlaybackSessionReadModel),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 409, description = "Transfer id is stale or no longer pending", body = ErrorResponse)
    )
)]
pub async fn confirm_session_transfer(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Json(request): Json<PlaybackSessionTransferConfirmRequest>,
) -> Result<Json<PlaybackSessionReadModel>, Response> {
    let confirmation = state
        .playback_session_transfer_confirmation(account.id, request.transfer_id)
        .map_err(axum::response::IntoResponse::into_response)?;
    if let Some(target_id) = confirmation.sonos_target_to_stop {
        match state.sonos_stop_target_for_transfer(target_id).await {
            Ok(_) | Err(SonosOperationError::Reason(SonosErrorReason::SessionNotManaged)) => {}
            Err(error) => return Err(map_sonos_operation_error(error)),
        }
    }
    let snapshot = state
        .confirm_playback_session_transfer(account.id, request.transfer_id)
        .map_err(axum::response::IntoResponse::into_response)?;
    Ok(Json(snapshot))
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
            item_type,
            item_id,
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
                request.context_type,
                request.context_id,
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

async fn persist_session_progress(
    state: &AppState,
    account_id: Uuid,
    snapshot: &PlaybackSessionReadModel,
) -> Result<(), ApiError> {
    let Some(current_index) = snapshot.current_index else {
        return Ok(());
    };
    let Some(current) = snapshot.queue.get(current_index as usize) else {
        return Ok(());
    };
    state
        .upsert_playback_progress(
            account_id,
            current.item_type,
            current.item_id,
            snapshot.context_type,
            snapshot.context_id,
            snapshot.position_seconds,
            snapshot.duration_seconds,
            false,
        )
        .await?;
    Ok(())
}

fn active_sonos_target_id(snapshot: &PlaybackSessionReadModel) -> Option<String> {
    match &snapshot.active_target {
        PlaybackSessionTarget::Sonos { target_id } => {
            target_id.trim().to_string().into_non_empty()
        }
        PlaybackSessionTarget::LocalAndroid { .. } => None,
    }
}

fn should_apply_sonos_state_mutation(request: &PlaybackSessionStatePatchRequest) -> bool {
    request.playback_state.is_some()
        || request.current_index.is_some()
        || request.position_seconds.is_some()
        || request.duration_seconds.is_some()
}

async fn apply_active_sonos_full_snapshot(
    state: &AppState,
    account: &AuthenticatedAccount,
    snapshot: &PlaybackSessionReadModel,
) -> Result<(), Response> {
    let Some(target_id) = active_sonos_target_id(snapshot) else {
        return Ok(());
    };
    if snapshot.queue.is_empty()
        || snapshot.current_index.is_none()
        || snapshot.playback_state == PlaybackSessionState::Stopped
    {
        stop_active_sonos_session(state, target_id).await?;
        return Ok(());
    }

    state
        .sonos_play_session_snapshot(target_id.clone(), account.clone(), snapshot)
        .await
        .map_err(map_sonos_operation_error)?;
    if snapshot.playback_state == PlaybackSessionState::Paused {
        state
            .sonos_pause_target(target_id)
            .await
            .map_err(map_sonos_operation_error)?;
    }
    Ok(())
}

async fn apply_active_sonos_state_mutation(
    state: &AppState,
    account: &AuthenticatedAccount,
    snapshot: &PlaybackSessionReadModel,
    request: &PlaybackSessionStatePatchRequest,
) -> Result<(), Response> {
    let Some(target_id) = active_sonos_target_id(snapshot) else {
        return Ok(());
    };
    let requires_snapshot_refresh = request.current_index.is_some()
        || request.position_seconds.is_some()
        || request.duration_seconds.is_some();

    if snapshot.queue.is_empty()
        || snapshot.current_index.is_none()
        || request.playback_state == Some(PlaybackSessionState::Stopped)
    {
        stop_active_sonos_session(state, target_id).await?;
        return Ok(());
    }

    if requires_snapshot_refresh {
        apply_active_sonos_full_snapshot(state, account, snapshot).await?;
        return Ok(());
    }

    match request.playback_state {
        Some(PlaybackSessionState::Paused) => {
            state
                .sonos_pause_target(target_id)
                .await
                .map_err(map_sonos_operation_error)?;
        }
        Some(PlaybackSessionState::Playing | PlaybackSessionState::Buffering) => {
            match state.sonos_resume_target(target_id.clone()).await {
                Ok(_) => {}
                Err(SonosOperationError::Reason(SonosErrorReason::SessionNotManaged)) => {
                    state
                        .sonos_play_session_snapshot(target_id, account.clone(), snapshot)
                        .await
                        .map_err(map_sonos_operation_error)?;
                }
                Err(error) => return Err(map_sonos_operation_error(error)),
            }
        }
        Some(PlaybackSessionState::Stopped) => {
            stop_active_sonos_session(state, target_id).await?;
        }
        None => {}
    }
    Ok(())
}

async fn stop_active_sonos_session(
    state: &AppState,
    target_id: String,
) -> Result<(), Response> {
    match state.sonos_stop_target_for_transfer(target_id).await {
        Ok(_) | Err(SonosOperationError::Reason(SonosErrorReason::SessionNotManaged)) => Ok(()),
        Err(error) => Err(map_sonos_operation_error(error)),
    }
}

trait IntoNonEmptyString {
    fn into_non_empty(self) -> Option<String>;
}

impl IntoNonEmptyString for String {
    fn into_non_empty(self) -> Option<String> {
        if self.is_empty() {
            None
        } else {
            Some(self)
        }
    }
}
