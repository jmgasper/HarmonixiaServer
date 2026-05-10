use axum::{
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Debug, Error)]
/// Represents api error in the common API error model used by handlers and state validation.
///
/// Functionality: Enumerates `Unauthorized`, `Forbidden`, `BadRequest`, `NotFound`, `Conflict`, `ServiceUnavailable`, `Internal` states or choices for common API error model used by handlers and state validation.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/catalog.rs`, `src/api/config.rs`, `src/api/maintenance.rs`, and 7 more.
pub enum ApiError {
    #[error("{0}")]
    Unauthorized(String),
    #[error("{0}")]
    Forbidden(String),
    #[error("{0}")]
    BadRequest(String),
    #[error("{0}")]
    NotFound(String),
    #[error("{0}")]
    Conflict(String),
    #[error("{0}")]
    ServiceUnavailable(String),
    #[error("internal server error")]
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ErrorResponseDetails {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<SonosErrorReason>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SonosErrorReason {
    TargetReconnecting,
    TargetUnreachable,
    SessionNotManaged,
    PublicBaseUrlUnusable,
    TranscodeCapacityExhausted,
    SourceIncompatibleFallbackFailed,
}

#[derive(Debug, Serialize, ToSchema)]
/// Represents error response in the common API error model used by handlers and state validation.
///
/// Functionality: Carries fields `code`, `message`, `details` for common API error model used by handlers and state validation.
/// Dependencies: depends on `String`, `String`, `Option<ErrorResponseDetails>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/catalog.rs`, `src/api/config.rs`, `src/api/maintenance.rs`, and 5 more.
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<ErrorResponseDetails>,
}

impl IntoResponse for ApiError {
    /// Converts the API error into an HTTP response.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Response` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn into_response(self) -> Response {
        let message = self.to_string();
        let (status, code) = match &self {
            ApiError::Unauthorized(_) => (StatusCode::UNAUTHORIZED, "unauthorized"),
            ApiError::Forbidden(_) => (StatusCode::FORBIDDEN, "forbidden"),
            ApiError::BadRequest(_) => (StatusCode::BAD_REQUEST, "bad_request"),
            ApiError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
            ApiError::Conflict(_) => (StatusCode::CONFLICT, "conflict"),
            ApiError::ServiceUnavailable(_) => {
                (StatusCode::SERVICE_UNAVAILABLE, "service_unavailable")
            }
            ApiError::Internal => (StatusCode::INTERNAL_SERVER_ERROR, "internal"),
        };

        let body = ErrorResponse {
            code: code.to_string(),
            message,
            details: None,
        };

        let mut response = (status, Json(body)).into_response();
        if matches!(self, ApiError::Unauthorized(_)) {
            response.headers_mut().insert(
                header::WWW_AUTHENTICATE,
                HeaderValue::from_static("Basic realm=\"Harmonixia API\", charset=\"UTF-8\""),
            );
        }

        response
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn error_details_are_omitted_when_absent() {
        let response = ErrorResponse {
            code: "bad_request".into(),
            message: "invalid request".into(),
            details: None,
        };

        let value = serde_json::to_value(response).unwrap();
        assert!(!value.as_object().unwrap().contains_key("details"));
    }

    #[test]
    fn error_details_reason_serializes_when_present() {
        let response = ErrorResponse {
            code: "service_unavailable".into(),
            message: "target is reconnecting".into(),
            details: Some(ErrorResponseDetails {
                reason: Some(SonosErrorReason::TargetReconnecting),
            }),
        };

        assert_eq!(
            serde_json::to_value(response).unwrap(),
            json!({
                "code": "service_unavailable",
                "message": "target is reconnecting",
                "details": {
                    "reason": "target_reconnecting"
                }
            })
        );
    }

    #[test]
    fn error_details_reason_is_omitted_when_absent() {
        let details = ErrorResponseDetails { reason: None };

        let value = serde_json::to_value(details).unwrap();
        assert!(!value.as_object().unwrap().contains_key("reason"));
    }
}
