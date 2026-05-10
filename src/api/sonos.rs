use axum::{extract::State, routing::get, Json, Router};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    auth::AuthenticatedUser,
    domain::{PlaybackItemType, SonosSessionStatus, SonosTransportState},
    error::{ApiError, ErrorResponse},
    state::AppState,
};

pub fn router() -> Router<AppState> {
    Router::new().route("/targets", get(list_targets))
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

#[utoipa::path(
    get,
    path = "/api/v1/sonos/targets",
    tag = "sonos",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Current live Sonos speaker and group targets", body = SonosTargetsResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
pub async fn list_targets(
    State(state): State<AppState>,
    _user: AuthenticatedUser,
) -> Result<Json<SonosTargetsResponse>, ApiError> {
    Ok(Json(state.sonos_snapshot().to_targets_response()))
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

impl SonosPlayRequest {
    pub fn source_type(&self) -> SonosPlaySourceType {
        match self {
            Self::Track { .. } => SonosPlaySourceType::Track,
            Self::Episode { .. } => SonosPlaySourceType::Episode,
            Self::Playlist { .. } => SonosPlaySourceType::Playlist,
        }
    }
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
