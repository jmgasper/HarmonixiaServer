use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{delete, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    auth::{AdminAccount, AuthenticatedUser},
    domain::{AccountRole, AuthenticatedAccount, UserAccount},
    error::{ApiError, ErrorResponse},
    state::AppState,
};

/// Builds the bootstrap Axum router for account bootstrap and user administration.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `Router<AppState>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
pub fn bootstrap_router() -> Router<AppState> {
    Router::new()
        .route("/status", get(bootstrap_status))
        .route("/first-admin", post(create_first_admin))
}

/// Builds the auth Axum router for account bootstrap and user administration.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `Router<AppState>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
pub fn auth_router() -> Router<AppState> {
    Router::new().route("/me", get(auth_me))
}

/// Builds the admin Axum router for account bootstrap and user administration.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `Router<AppState>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
pub fn admin_router() -> Router<AppState> {
    Router::new()
        .route("/users", get(list_users).post(create_user))
        .route("/users/:user_id", delete(delete_user))
        .route(
            "/users/:user_id/password-reset",
            post(reset_user_password),
        )
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents bootstrap status response in the account bootstrap, authentication, and admin user-management HTTP API.
///
/// Functionality: Carries fields `users_exist`, `first_admin_required`, `initial_scan_started` for account bootstrap, authentication, and admin user-management HTTP API.
/// Dependencies: depends on `bool`, `bool`, `bool` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/openapi.rs`.
pub struct BootstrapStatusResponse {
    pub users_exist: bool,
    pub first_admin_required: bool,
    pub initial_scan_started: bool,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
/// Represents create first admin request in the account bootstrap, authentication, and admin user-management HTTP API.
///
/// Functionality: Carries fields `username`, `password` for account bootstrap, authentication, and admin user-management HTTP API.
/// Dependencies: depends on `String`, `String` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/openapi.rs`.
pub struct CreateFirstAdminRequest {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
/// Represents create user request in the account bootstrap, authentication, and admin user-management HTTP API.
///
/// Functionality: Carries fields `username`, `password`, `role` for account bootstrap, authentication, and admin user-management HTTP API.
/// Dependencies: depends on `String`, `String`, `AccountRole` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/openapi.rs`.
pub struct CreateUserRequest {
    pub username: String,
    pub password: String,
    pub role: AccountRole,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
/// Represents reset password request in the account bootstrap, authentication, and admin user-management HTTP API.
///
/// Functionality: Carries fields `password` for account bootstrap, authentication, and admin user-management HTTP API.
/// Dependencies: depends on `String` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/openapi.rs`.
pub struct ResetPasswordRequest {
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents users response in the account bootstrap, authentication, and admin user-management HTTP API.
///
/// Functionality: Carries fields `users` for account bootstrap, authentication, and admin user-management HTTP API.
/// Dependencies: depends on `Vec<UserAccount>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/openapi.rs`.
pub struct UsersResponse {
    pub users: Vec<UserAccount>,
}

#[utoipa::path(
    get,
    path = "/api/v1/bootstrap/status",
    tag = "bootstrap",
    responses(
        (status = 200, description = "First-run bootstrap status", body = BootstrapStatusResponse)
    )
)]
/// Reports or performs first-run bootstrap work for account bootstrap and user administration.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
///
/// Output:
/// - Returns `Json<BootstrapStatusResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn bootstrap_status(
    State(state): State<AppState>,
) -> Result<Json<BootstrapStatusResponse>, ApiError> {
    let users_exist = state.has_local_accounts().await?;
    let initial_scan_started = state.initial_scan_started().await?;
    Ok(Json(BootstrapStatusResponse {
        users_exist,
        first_admin_required: !users_exist,
        initial_scan_started,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/bootstrap/first-admin",
    tag = "bootstrap",
    request_body = CreateFirstAdminRequest,
    responses(
        (status = 201, description = "First admin account created", body = UserAccount),
        (status = 400, description = "Invalid account data", body = ErrorResponse),
        (status = 409, description = "Users already exist", body = ErrorResponse)
    )
)]
/// Creates a new resource for account bootstrap and user administration.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `Json(request)`: `Json<CreateFirstAdminRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `(StatusCode, Json<UserAccount>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn create_first_admin(
    State(state): State<AppState>,
    Json(request): Json<CreateFirstAdminRequest>,
) -> Result<(StatusCode, Json<UserAccount>), ApiError> {
    let account = state
        .create_first_admin(&request.username, &request.password)
        .await?;

    Ok((StatusCode::CREATED, Json(account)))
}

#[utoipa::path(
    get,
    path = "/api/v1/auth/me",
    tag = "auth",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Authenticated account", body = AuthenticatedAccount),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Handles auth me for account bootstrap and user administration.
///
/// Inputs:
/// - `AuthenticatedUser(account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Json<AuthenticatedAccount>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
pub async fn auth_me(
    AuthenticatedUser(account): AuthenticatedUser,
) -> Json<AuthenticatedAccount> {
    Json(account)
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/users",
    tag = "users",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Local accounts", body = UsersResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse)
    )
)]
/// Lists resources for account bootstrap and user administration.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Json<UsersResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn list_users(
    State(state): State<AppState>,
    _admin: AdminAccount,
) -> Result<Json<UsersResponse>, ApiError> {
    Ok(Json(UsersResponse {
        users: state.user_accounts().await?,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/users",
    tag = "users",
    security(("basicAuth" = [])),
    request_body = CreateUserRequest,
    responses(
        (status = 201, description = "Local account created", body = UserAccount),
        (status = 400, description = "Invalid account data", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse),
        (status = 409, description = "Username already exists", body = ErrorResponse)
    )
)]
/// Creates a new resource for account bootstrap and user administration.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Json(request)`: `Json<CreateUserRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `(StatusCode, Json<UserAccount>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn create_user(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Json(request): Json<CreateUserRequest>,
) -> Result<(StatusCode, Json<UserAccount>), ApiError> {
    let account = state
        .create_user_account(&request.username, &request.password, request.role)
        .await?;

    Ok((StatusCode::CREATED, Json(account)))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/users/{user_id}/password-reset",
    tag = "users",
    security(("basicAuth" = [])),
    params(("user_id" = Uuid, Path, description = "Local account id")),
    request_body = ResetPasswordRequest,
    responses(
        (status = 200, description = "Password reset", body = UserAccount),
        (status = 400, description = "Invalid password", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse),
        (status = 404, description = "User not found", body = ErrorResponse)
    )
)]
/// Resets stored state for account bootstrap and user administration.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(user_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `Json(request)`: `Json<ResetPasswordRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `Json<UserAccount>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn reset_user_password(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Path(user_id): Path<Uuid>,
    Json(request): Json<ResetPasswordRequest>,
) -> Result<Json<UserAccount>, ApiError> {
    Ok(Json(
        state.reset_user_password(user_id, &request.password).await?,
    ))
}

#[utoipa::path(
    delete,
    path = "/api/v1/admin/users/{user_id}",
    tag = "users",
    security(("basicAuth" = [])),
    params(("user_id" = Uuid, Path, description = "Local account id")),
    responses(
        (status = 204, description = "User deleted"),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse),
        (status = 404, description = "User not found", body = ErrorResponse),
        (status = 409, description = "Deleting the user would violate account invariants", body = ErrorResponse)
    )
)]
/// Deletes or removes a resource from account bootstrap and user administration.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(user_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `StatusCode` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn delete_user(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Path(user_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state.delete_user_account(user_id).await?;
    Ok(StatusCode::NO_CONTENT)
}
