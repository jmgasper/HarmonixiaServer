use axum::{
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
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

#[derive(Debug, Serialize, ToSchema)]
/// Represents error response in the common API error model used by handlers and state validation.
///
/// Functionality: Carries fields `code`, `message` for common API error model used by handlers and state validation.
/// Dependencies: depends on `String`, `String` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/catalog.rs`, `src/api/config.rs`, `src/api/maintenance.rs`, and 5 more.
pub struct ErrorResponse {
    pub code: String,
    pub message: String,
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
