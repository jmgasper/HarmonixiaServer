use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio_util::io::ReaderStream;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    auth::AuthenticatedUser,
    api::media::{
        filename_for_transcode, resolve_original_file, serve_original_media_file,
        transcode_response, ContentDisposition,
    },
    domain::{PlaybackItemType, SonosSessionStatus, SonosTransportState},
    error::{ApiError, ErrorResponse, ErrorResponseDetails, SonosErrorReason},
    sonos::SonosOperationError,
    state::{sonos_aac_profile_for_delivery, AppState, SonosSignedMediaValidationError},
    transcode::spawn_direct_aac_transcode,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/targets", get(list_targets))
        .route("/targets/:target_id/play", post(play_target))
        .route("/targets/:target_id/pause", post(pause_target))
        .route("/targets/:target_id/resume", post(resume_target))
        .route("/targets/:target_id/stop", post(stop_target))
        .route("/targets/:target_id/seek", post(seek_target))
        .route("/targets/:target_id/next", post(next_target))
        .route("/targets/:target_id/previous", post(previous_target))
        .route("/media/:token", get(fetch_signed_media))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SonosTargetsResponse {
    pub speakers: Vec<SonosSpeakerTarget>,
    pub groups: Vec<SonosGroupTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SonosSpeakerTarget {
    pub id: String,
    pub display_name: String,
    #[schema(required = true, nullable = true)]
    pub room_name: Option<String>,
    pub available: bool,
    #[schema(required = true, nullable = true)]
    pub volume_percent: Option<u8>,
    #[schema(required = true, nullable = true)]
    pub muted: Option<bool>,
    #[schema(required = true, nullable = true)]
    pub transport_state: Option<SonosTransportState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SonosGroupTarget {
    pub id: String,
    pub display_name: String,
    pub available: bool,
    #[schema(required = true, nullable = true)]
    pub volume_percent: Option<u8>,
    #[schema(required = true, nullable = true)]
    pub muted: Option<bool>,
    #[schema(required = true, nullable = true)]
    pub transport_state: Option<SonosTransportState>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum SonosPlaybackTarget {
    Speaker(SonosSpeakerTarget),
    Group(SonosGroupTarget),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SonosPlaybackResponse {
    pub target: SonosPlaybackTarget,
    #[schema(required = true, nullable = true)]
    pub session: Option<SonosSessionSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SonosSessionSummary {
    pub status: SonosSessionStatus,
    pub owner_username: String,
    pub current_item_type: PlaybackItemType,
    pub current_item_id: Uuid,
    pub queue_index: u32,
    pub queue_position: u32,
    pub queue_length: u32,
    pub current_position_seconds: u32,
    #[schema(required = true, nullable = true)]
    pub current_duration_seconds: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reconnect_seconds_remaining: Option<u32>,
    #[schema(required = true, nullable = true)]
    pub next_item: Option<SonosNextItemSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SonosNextItemSummary {
    pub item_type: PlaybackItemType,
    pub item_id: Uuid,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SonosPlaySourceType {
    Track,
    Episode,
    Playlist,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "source_type", rename_all = "snake_case")]
pub enum SonosPlayRequest {
    Track { source_id: Uuid },
    Episode { source_id: Uuid },
    Playlist { source_id: Uuid },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SonosSeekRequest {
    pub position_seconds: u32,
}

impl SonosPlayRequest {
    pub fn source_type(&self) -> SonosPlaySourceType {
        match self {
            Self::Track { .. } => SonosPlaySourceType::Track,
            Self::Episode { .. } => SonosPlaySourceType::Episode,
            Self::Playlist { .. } => SonosPlaySourceType::Playlist,
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/sonos/targets",
    tag = "sonos",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Live Sonos speakers and groups currently reachable by discovery", body = SonosTargetsResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
pub async fn list_targets(
    State(state): State<AppState>,
    _user: AuthenticatedUser,
) -> Json<SonosTargetsResponse> {
    Json(state.sonos_snapshot().to_targets_response())
}

#[utoipa::path(
    post,
    path = "/api/v1/sonos/targets/{target_id}/play",
    tag = "sonos",
    security(("basicAuth" = [])),
    params(("target_id" = String, Path, description = "Sonos speaker or group target id")),
    request_body = SonosPlayRequest,
    responses(
        (status = 200, description = "Managed Sonos playback started or queue replaced", body = SonosPlaybackResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Requested source is not visible", body = ErrorResponse),
        (status = 409, description = "Target is reconnecting", body = ErrorResponse),
        (status = 503, description = "Sonos target or media delivery is unavailable", body = ErrorResponse)
    )
)]
pub async fn play_target(
    State(state): State<AppState>,
    Path(target_id): Path<String>,
    user: AuthenticatedUser,
    Json(request): Json<SonosPlayRequest>,
) -> Result<Json<SonosPlaybackResponse>, Response> {
    state
        .sonos_play_target(target_id, user.0, request)
        .await
        .map(Json)
        .map_err(map_sonos_operation_error)
}

#[utoipa::path(
    post,
    path = "/api/v1/sonos/targets/{target_id}/pause",
    tag = "sonos",
    security(("basicAuth" = [])),
    params(("target_id" = String, Path, description = "Managed Sonos target id")),
    responses(
        (status = 200, description = "Managed Sonos session paused", body = SonosPlaybackResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 409, description = "Target is reconnecting or not managed", body = ErrorResponse),
        (status = 503, description = "Sonos target is unreachable", body = ErrorResponse)
    )
)]
pub async fn pause_target(
    State(state): State<AppState>,
    Path(target_id): Path<String>,
    _user: AuthenticatedUser,
) -> Result<Json<SonosPlaybackResponse>, Response> {
    state
        .sonos_pause_target(target_id)
        .await
        .map(Json)
        .map_err(map_sonos_operation_error)
}

#[utoipa::path(
    post,
    path = "/api/v1/sonos/targets/{target_id}/resume",
    tag = "sonos",
    security(("basicAuth" = [])),
    params(("target_id" = String, Path, description = "Managed Sonos target id")),
    responses(
        (status = 200, description = "Managed Sonos session resumed", body = SonosPlaybackResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 409, description = "Target is reconnecting or not managed", body = ErrorResponse),
        (status = 503, description = "Sonos target is unreachable", body = ErrorResponse)
    )
)]
pub async fn resume_target(
    State(state): State<AppState>,
    Path(target_id): Path<String>,
    _user: AuthenticatedUser,
) -> Result<Json<SonosPlaybackResponse>, Response> {
    state
        .sonos_resume_target(target_id)
        .await
        .map(Json)
        .map_err(map_sonos_operation_error)
}

#[utoipa::path(
    post,
    path = "/api/v1/sonos/targets/{target_id}/stop",
    tag = "sonos",
    security(("basicAuth" = [])),
    params(("target_id" = String, Path, description = "Managed Sonos target id")),
    responses(
        (status = 200, description = "Managed Sonos ownership ended; session is null", body = SonosPlaybackResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 409, description = "Target is not managed", body = ErrorResponse)
    )
)]
pub async fn stop_target(
    State(state): State<AppState>,
    Path(target_id): Path<String>,
    _user: AuthenticatedUser,
) -> Result<Json<SonosPlaybackResponse>, Response> {
    state
        .sonos_stop_target(target_id)
        .await
        .map(Json)
        .map_err(map_sonos_operation_error)
}

#[utoipa::path(
    post,
    path = "/api/v1/sonos/targets/{target_id}/seek",
    tag = "sonos",
    security(("basicAuth" = [])),
    params(("target_id" = String, Path, description = "Managed Sonos target id")),
    request_body = SonosSeekRequest,
    responses(
        (status = 200, description = "Managed Sonos session seeked", body = SonosPlaybackResponse),
        (status = 400, description = "Seek position is invalid", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 409, description = "Target is reconnecting or not managed", body = ErrorResponse),
        (status = 503, description = "Sonos target is unreachable", body = ErrorResponse)
    )
)]
pub async fn seek_target(
    State(state): State<AppState>,
    Path(target_id): Path<String>,
    _user: AuthenticatedUser,
    Json(request): Json<SonosSeekRequest>,
) -> Result<Json<SonosPlaybackResponse>, Response> {
    state
        .sonos_seek_target(target_id, request)
        .await
        .map(Json)
        .map_err(map_sonos_operation_error)
}

#[utoipa::path(
    post,
    path = "/api/v1/sonos/targets/{target_id}/next",
    tag = "sonos",
    security(("basicAuth" = [])),
    params(("target_id" = String, Path, description = "Managed Sonos target id")),
    responses(
        (status = 200, description = "Managed Sonos session advanced to next queue item", body = SonosPlaybackResponse),
        (status = 400, description = "No next queue item exists", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 409, description = "Target is reconnecting or not managed", body = ErrorResponse),
        (status = 503, description = "Sonos target or media delivery is unavailable", body = ErrorResponse)
    )
)]
pub async fn next_target(
    State(state): State<AppState>,
    Path(target_id): Path<String>,
    _user: AuthenticatedUser,
) -> Result<Json<SonosPlaybackResponse>, Response> {
    state
        .sonos_next_target(target_id)
        .await
        .map(Json)
        .map_err(map_sonos_operation_error)
}

#[utoipa::path(
    post,
    path = "/api/v1/sonos/targets/{target_id}/previous",
    tag = "sonos",
    security(("basicAuth" = [])),
    params(("target_id" = String, Path, description = "Managed Sonos target id")),
    responses(
        (status = 200, description = "Managed Sonos session moved to previous queue item", body = SonosPlaybackResponse),
        (status = 400, description = "No previous queue item exists", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 409, description = "Target is reconnecting or not managed", body = ErrorResponse),
        (status = 503, description = "Sonos target or media delivery is unavailable", body = ErrorResponse)
    )
)]
pub async fn previous_target(
    State(state): State<AppState>,
    Path(target_id): Path<String>,
    _user: AuthenticatedUser,
) -> Result<Json<SonosPlaybackResponse>, Response> {
    state
        .sonos_previous_target(target_id)
        .await
        .map(Json)
        .map_err(map_sonos_operation_error)
}

#[utoipa::path(
    get,
    path = "/api/v1/sonos/media/{token}",
    tag = "sonos",
    params(
        ("token" = String, Path, description = "Opaque URL-safe Sonos signed media token"),
        ("Range" = Option<String>, Header, description = "Optional byte range for original media delivery")
    ),
    responses(
        (status = 200, description = "Signed Sonos original or AAC-high fallback stream. Original streams include range-capable media headers; fallback streams are ADTS AAC.", content_type = "application/octet-stream"),
        (status = 206, description = "Signed Sonos partial original media stream.", content_type = "application/octet-stream"),
        (status = 403, description = "Signed media token is invalid or no longer matches the current Sonos session/item generation", body = ErrorResponse),
        (status = 404, description = "Catalog item is not visible or the original source is unavailable", body = ErrorResponse),
        (status = 416, description = "Requested byte range is not satisfiable"),
        (status = 503, description = "Sonos fallback transcode could not be admitted or started", body = ErrorResponse)
    )
)]
pub async fn fetch_signed_media(
    State(state): State<AppState>,
    Path(token): Path<String>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let claim = state
        .validate_sonos_signed_media_token(&token)
        .map_err(map_sonos_signed_media_validation_error)?;
    let media_file = state
        .visible_original_media_file(claim.item_type, claim.item_id)
        .await?;

    match sonos_aac_profile_for_delivery(claim.delivery_kind) {
        None => {
            serve_original_media_file(&state, media_file, headers, ContentDisposition::Inline)
                .await
        }
        Some(profile) => {
            let original = match resolve_original_file(&state, &media_file) {
                Ok(original) => original,
                Err(_) => {
                    return Ok(sonos_error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "service_unavailable",
                        "Sonos source fallback could not access the original media",
                        SonosErrorReason::SourceIncompatibleFallbackFailed,
                    ));
                }
            };
            let slot = match state.take_sonos_reserved_transcode_slot(&claim) {
                Some(slot) => slot,
                None => match state.try_acquire_transcode_slot() {
                    Ok(slot) => slot,
                    Err(_) => {
                        return Ok(sonos_error_response(
                            StatusCode::SERVICE_UNAVAILABLE,
                            "service_unavailable",
                            "transcode capacity is exhausted; retry later",
                            SonosErrorReason::TranscodeCapacityExhausted,
                        ));
                    }
                },
            };
            let transcode = match spawn_direct_aac_transcode(
                &state.config().ffmpeg_path,
                &original,
                profile,
                slot,
            )
            .await
            {
                Ok(transcode) => transcode,
                Err(error) => {
                    tracing::error!(%error, "failed to start Sonos AAC fallback transcode");
                    return Ok(sonos_error_response(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "service_unavailable",
                        "Sonos source fallback transcode failed",
                        SonosErrorReason::SourceIncompatibleFallbackFailed,
                    ));
                }
            };

            Ok(transcode_response(
                ReaderStream::new(transcode),
                &filename_for_transcode(&original, profile),
            ))
        }
    }
}

fn map_sonos_signed_media_validation_error(
    error: SonosSignedMediaValidationError,
) -> ApiError {
    match error {
        SonosSignedMediaValidationError::InvalidToken => {
            ApiError::Forbidden("invalid Sonos signed media token".into())
        }
        SonosSignedMediaValidationError::StaleClaim => {
            ApiError::Forbidden("Sonos signed media URL is no longer valid".into())
        }
    }
}

fn map_sonos_operation_error(error: SonosOperationError) -> Response {
    match error {
        SonosOperationError::Api(error) => error.into_response(),
        SonosOperationError::Reason(reason) => {
            let (status, code, message) = sonos_reason_status(reason);
            sonos_error_response(status, code, message, reason)
        }
    }
}

fn sonos_reason_status(reason: SonosErrorReason) -> (StatusCode, &'static str, &'static str) {
    match reason {
        SonosErrorReason::TargetReconnecting => (
            StatusCode::CONFLICT,
            "conflict",
            "Sonos target is reconnecting",
        ),
        SonosErrorReason::SessionNotManaged => (
            StatusCode::CONFLICT,
            "conflict",
            "Sonos target is not managed by Harmonixia",
        ),
        SonosErrorReason::TargetUnreachable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            "Sonos target is unreachable",
        ),
        SonosErrorReason::PublicBaseUrlUnusable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            "public_base_url is not usable for Sonos playback",
        ),
        SonosErrorReason::TranscodeCapacityExhausted => (
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            "transcode capacity is exhausted; retry later",
        ),
        SonosErrorReason::SourceIncompatibleFallbackFailed => (
            StatusCode::SERVICE_UNAVAILABLE,
            "service_unavailable",
            "Sonos source fallback failed",
        ),
    }
}

fn sonos_error_response(
    status: StatusCode,
    code: &str,
    message: &str,
    reason: SonosErrorReason,
) -> Response {
    (
        status,
        Json(ErrorResponse {
            code: code.to_string(),
            message: message.to_string(),
            details: Some(ErrorResponseDetails {
                reason: Some(reason),
            }),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::{SonosDeliveryKind, SonosSignedClaim};
    use serde_json::json;

    #[test]
    fn play_request_serializes_flat_discriminator_shape() {
        let source_id = Uuid::parse_str("018f26c0-0000-7000-8000-000000000001").unwrap();
        let request = SonosPlayRequest::Playlist { source_id };

        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({
                "source_type": "playlist",
                "source_id": "018f26c0-0000-7000-8000-000000000001"
            })
        );
    }

    #[test]
    fn signed_claim_serializes_agreed_field_names() {
        let claim = SonosSignedClaim {
            session_id: Uuid::parse_str("018f26c0-0000-7000-8000-000000000010").unwrap(),
            session_generation: 2,
            item_generation: 7,
            target_id: "sonos-room-1".into(),
            item_type: PlaybackItemType::Track,
            item_id: Uuid::parse_str("018f26c0-0000-7000-8000-000000000011").unwrap(),
            delivery_kind: SonosDeliveryKind::TranscodeAacHigh,
            exp: 1_800_000_000,
        };

        assert_eq!(
            serde_json::to_value(claim).unwrap(),
            json!({
                "session_id": "018f26c0-0000-7000-8000-000000000010",
                "session_generation": 2,
                "item_generation": 7,
                "target_id": "sonos-room-1",
                "item_type": "track",
                "item_id": "018f26c0-0000-7000-8000-000000000011",
                "delivery_kind": "transcode_aac_high",
                "exp": 1800000000
            })
        );
    }

    #[test]
    fn delivery_kind_accepts_only_sonos_v1_values() {
        assert_eq!(
            serde_json::from_value::<SonosDeliveryKind>(json!("original")).unwrap(),
            SonosDeliveryKind::Original
        );
        assert_eq!(
            serde_json::from_value::<SonosDeliveryKind>(json!("transcode_aac_high")).unwrap(),
            SonosDeliveryKind::TranscodeAacHigh
        );
        for value in ["mobile", "standard", "high", "transcode_aac_standard"] {
            assert!(serde_json::from_value::<SonosDeliveryKind>(json!(value)).is_err());
        }
    }

    #[test]
    fn session_summary_serializes_required_nulls_and_omits_reconnect_when_absent() {
        let summary = SonosSessionSummary {
            status: SonosSessionStatus::Active,
            owner_username: "alice".into(),
            current_item_type: PlaybackItemType::Episode,
            current_item_id: Uuid::parse_str("018f26c0-0000-7000-8000-000000000020").unwrap(),
            queue_index: 0,
            queue_position: 1,
            queue_length: 3,
            current_position_seconds: 42,
            current_duration_seconds: None,
            reconnect_seconds_remaining: None,
            next_item: None,
        };
        let value = serde_json::to_value(summary).unwrap();

        assert_eq!(value["current_duration_seconds"], serde_json::Value::Null);
        assert_eq!(value["next_item"], serde_json::Value::Null);
        assert!(!value
            .as_object()
            .unwrap()
            .contains_key("reconnect_seconds_remaining"));
    }

    #[test]
    fn target_unknown_state_serializes_as_explicit_nulls() {
        let target = SonosSpeakerTarget {
            id: "speaker-1".into(),
            display_name: "Kitchen".into(),
            room_name: None,
            available: true,
            volume_percent: None,
            muted: None,
            transport_state: None,
        };
        let value = serde_json::to_value(target).unwrap();

        assert_eq!(value["room_name"], serde_json::Value::Null);
        assert_eq!(value["volume_percent"], serde_json::Value::Null);
        assert_eq!(value["muted"], serde_json::Value::Null);
        assert_eq!(value["transport_state"], serde_json::Value::Null);
    }
}
