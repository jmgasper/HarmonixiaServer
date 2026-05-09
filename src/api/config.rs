use std::str::FromStr;

use axum::{
    extract::{Path, State},
    routing::{get, patch},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{
    auth::AdminAccount,
    domain::{ProviderKind, ProviderSetting, SystemConfig},
    error::{ApiError, ErrorResponse},
    state::AppState,
};

/// Builds the Axum router for system and provider configuration.
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
        .route(
            "/system/config",
            get(get_system_config).put(update_system_config),
        )
        .route("/providers/settings", get(list_provider_settings))
        .route(
            "/providers/:provider/settings",
            patch(update_provider_setting),
        )
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
/// Represents system config update request in the admin system and provider configuration HTTP API.
///
/// Functionality: Carries fields `library_root`, `dropbox_root`, `podcast_subtree`, `transcode_concurrency_limit`, and `scan_thread_count` for admin system and provider configuration HTTP API.
/// Dependencies: depends on `String`, `String`, `Option<String>`, `Option<i32>`, `Option<i32>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/config.rs`, `src/api/openapi.rs`.
pub struct SystemConfigUpdateRequest {
    pub library_root: String,
    pub dropbox_root: String,
    pub podcast_subtree: Option<String>,
    pub transcode_concurrency_limit: Option<i32>,
    pub scan_thread_count: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents provider settings response in the admin system and provider configuration HTTP API.
///
/// Functionality: Carries fields `providers` for admin system and provider configuration HTTP API.
/// Dependencies: depends on `Vec<ProviderSetting>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/config.rs`, `src/api/openapi.rs`.
pub struct ProviderSettingsResponse {
    pub providers: Vec<ProviderSetting>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
/// Represents provider setting update request in the admin system and provider configuration HTTP API.
///
/// Functionality: Carries fields `enabled`, `api_key`, `clear_api_key` for admin system and provider configuration HTTP API.
/// Dependencies: depends on `Option<bool>`, `Option<String>`, `Option<bool>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/config.rs`, `src/api/openapi.rs`.
pub struct ProviderSettingUpdateRequest {
    pub enabled: Option<bool>,
    pub api_key: Option<String>,
    pub clear_api_key: Option<bool>,
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/system/config",
    tag = "settings",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Durable system configuration", body = SystemConfig),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse)
    )
)]
/// Retrieves a resource for system and provider configuration.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Json<SystemConfig>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
pub async fn get_system_config(
    State(state): State<AppState>,
    _admin: AdminAccount,
) -> Json<SystemConfig> {
    Json(state.system_config())
}

#[utoipa::path(
    put,
    path = "/api/v1/admin/system/config",
    tag = "settings",
    security(("basicAuth" = [])),
    request_body = SystemConfigUpdateRequest,
    responses(
        (status = 200, description = "Durable system configuration updated", body = SystemConfig),
        (status = 400, description = "Invalid system configuration", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse)
    )
)]
/// Updates existing state for system and provider configuration.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Json(request)`: `Json<SystemConfigUpdateRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `Json<SystemConfig>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn update_system_config(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Json(request): Json<SystemConfigUpdateRequest>,
) -> Result<Json<SystemConfig>, ApiError> {
    Ok(Json(
        state
            .update_system_config(
                &request.library_root,
                &request.dropbox_root,
                request.podcast_subtree.as_deref(),
                request.transcode_concurrency_limit,
                request.scan_thread_count,
            )
            .await?,
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/providers/settings",
    tag = "settings",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Durable provider settings", body = ProviderSettingsResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse)
    )
)]
/// Lists resources for system and provider configuration.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Json<ProviderSettingsResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn list_provider_settings(
    State(state): State<AppState>,
    _admin: AdminAccount,
) -> Result<Json<ProviderSettingsResponse>, ApiError> {
    Ok(Json(ProviderSettingsResponse {
        providers: state.provider_settings().await?,
    }))
}

#[utoipa::path(
    patch,
    path = "/api/v1/admin/providers/{provider}/settings",
    tag = "settings",
    security(("basicAuth" = [])),
    params(("provider" = String, Path, description = "Provider identifier, for example music_brainz or discogs")),
    request_body = ProviderSettingUpdateRequest,
    responses(
        (status = 200, description = "Provider setting updated", body = ProviderSetting),
        (status = 400, description = "Invalid provider setting", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse),
        (status = 404, description = "Provider not found", body = ErrorResponse)
    )
)]
/// Updates existing state for system and provider configuration.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(provider)`: `Path<String>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `Json(request)`: `Json<ProviderSettingUpdateRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `Json<ProviderSetting>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn update_provider_setting(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Path(provider): Path<String>,
    Json(request): Json<ProviderSettingUpdateRequest>,
) -> Result<Json<ProviderSetting>, ApiError> {
    let provider = ProviderKind::from_str(&provider)
        .map_err(|_| ApiError::BadRequest(format!("unknown provider: {provider}")))?;

    Ok(Json(
        state
            .update_provider_setting(
                provider,
                request.enabled,
                request.api_key.as_deref(),
                request.clear_api_key.unwrap_or(false),
            )
            .await?,
    ))
}
