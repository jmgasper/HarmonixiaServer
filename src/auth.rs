use axum::{
    extract::FromRequestParts,
    http::{header, request::Parts},
};
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};

use crate::{
    domain::AuthenticatedAccount,
    error::ApiError,
    state::AppState,
};

#[derive(Debug, Clone)]
/// Represents authenticated user in the Basic-auth extractors and password hashing helpers used by the HTTP layer.
///
/// Functionality: Wraps a single domain value for Basic-auth extractors and password hashing helpers used by the HTTP layer.
/// Dependencies: depends on `AuthenticatedAccount` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/catalog.rs`, `src/api/media.rs`, `src/api/playback.rs`, and 2 more.
pub struct AuthenticatedUser(pub AuthenticatedAccount);

#[derive(Debug, Clone)]
/// Represents admin account in the Basic-auth extractors and password hashing helpers used by the HTTP layer.
///
/// Functionality: Wraps a single domain value for Basic-auth extractors and password hashing helpers used by the HTTP layer.
/// Dependencies: depends on `AuthenticatedAccount` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/config.rs`, `src/api/maintenance.rs`, `src/api/media.rs`, and 1 more.
pub struct AdminAccount(pub AuthenticatedAccount);

#[async_trait]
impl FromRequestParts<AppState> for AuthenticatedUser {
    type Rejection = ApiError;

    /// Handles from request parts for Basic-auth extractors and password hashing helpers used by the HTTP layer.
    ///
    /// Inputs:
    /// - `parts`: `&mut Parts`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `state`: `&AppState`; expected to be Axum application state with a live repository and runtime configuration.
    ///
    /// Output:
    /// - Returns `Self` on success or `Self::Rejection` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `Self::Rejection` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let Some((username, password)) = basic_credentials(parts) else {
            return Err(ApiError::Unauthorized("authentication required".into()));
        };

        let Some(account) = state.authenticate_local_account(&username, &password).await? else {
            return Err(ApiError::Unauthorized("invalid credentials".into()));
        };

        Ok(Self(account))
    }
}

#[async_trait]
impl FromRequestParts<AppState> for AdminAccount {
    type Rejection = ApiError;

    /// Handles from request parts for Basic-auth extractors and password hashing helpers used by the HTTP layer.
    ///
    /// Inputs:
    /// - `parts`: `&mut Parts`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `state`: `&AppState`; expected to be Axum application state with a live repository and runtime configuration.
    ///
    /// Output:
    /// - Returns `Self` on success or `Self::Rejection` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `Self::Rejection` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        let AuthenticatedUser(account) =
            AuthenticatedUser::from_request_parts(parts, state).await?;

        if !account.role.is_admin() {
            return Err(ApiError::Forbidden("admin role required".into()));
        }

        Ok(Self(account))
    }
}

/// Hashes security-sensitive data for Basic-auth extractors and password hashing helpers used by the HTTP layer.
///
/// Inputs:
/// - `password`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` on success or `argon2::password_hash::Error` when the operation cannot be completed.
///
/// Errors:
/// - Returns `argon2::password_hash::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
    use argon2::{
        password_hash::{PasswordHasher, SaltString},
        Argon2,
    };

    let salt = SaltString::encode_b64(uuid::Uuid::new_v4().as_bytes())?;
    Ok(Argon2::default()
        .hash_password(password.as_bytes(), salt.as_salt())?
        .to_string())
}

/// Verifies security-sensitive data for Basic-auth extractors and password hashing helpers used by the HTTP layer.
///
/// Inputs:
/// - `password`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `password_hash`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
pub fn verify_password(password: &str, password_hash: &str) -> bool {
    use argon2::{
        password_hash::{PasswordHash, PasswordVerifier},
        Argon2,
    };

    let Ok(parsed_hash) = PasswordHash::new(password_hash) else {
        return false;
    };

    Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok()
}

/// Handles basic credentials for Basic-auth extractors and password hashing helpers used by the HTTP layer.
///
/// Inputs:
/// - `parts`: `&Parts`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Some((String, String))` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn basic_credentials(parts: &Parts) -> Option<(String, String)> {
    let value = parts.headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, encoded) = value.trim().split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("basic") {
        return None;
    }

    let decoded = general_purpose::STANDARD.decode(encoded.trim()).ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    let (username, password) = decoded.split_once(':')?;
    if username.trim().is_empty() {
        return None;
    }

    Some((username.to_string(), password.to_string()))
}
