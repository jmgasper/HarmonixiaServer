use axum::{
    routing::{delete, get},
    Router,
};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::state::AppState;

const API_DOCS_PATH: &str = "/api/v1/api-docs";
const API_DOCS_OPENAPI_JSON_PATH: &str = "/api/v1/api-docs/openapi.json";

pub mod accounts;
pub mod admin_ui;
pub mod artwork;
pub mod catalog;
pub mod config;
pub mod maintenance;
pub mod media;
pub mod openapi;
pub mod playback;
pub mod playlists;

/// Builds the Axum router for top-level API routing.
///
/// Inputs:
/// - `state`: `AppState`; expected to be Axum application state with a live repository and runtime configuration.
///
/// Output:
/// - Returns `Router` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
pub fn router(state: AppState) -> Router {
    let admin_routes = maintenance::router()
        .merge(accounts::admin_router())
        .merge(config::router())
        .merge(media::admin_router());

    Router::new()
        .merge(admin_ui::router())
        .merge(api_docs())
        .route("/health", get(health_check))
        .route("/openapi.json", get(openapi_json))
        .nest("/api/v1/admin", admin_routes.clone())
        .nest("/api/admin", admin_routes)
        .nest("/api/v1/bootstrap", accounts::bootstrap_router())
        .nest("/api/v1/auth", accounts::auth_router())
        .nest("/api/v1/catalog", catalog::router().merge(artwork::catalog_router()))
        .nest("/api/v1/artwork", artwork::router())
        .nest("/api/v1/media", media::router())
        .route(
            "/api/v1/playlists",
            get(playlists::list_playlists).post(playlists::create_playlist),
        )
        .route(
            "/api/v1/playlists/:playlist_id",
            get(playlists::get_playlist)
                .put(playlists::update_playlist)
                .delete(playlists::delete_playlist),
        )
        .route(
            "/api/v1/playlists/:playlist_id/items",
            get(playlists::list_playlist_items)
                .post(playlists::add_playlist_item)
                .put(playlists::reorder_playlist_items),
        )
        .route(
            "/api/v1/playlists/:playlist_id/items/:playlist_item_id",
            delete(playlists::remove_playlist_item),
        )
        .nest("/api/v1/me/playback", playback::router())
        .with_state(state)
}

/// Verifies that api docs.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `SwaggerUi` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn api_docs() -> SwaggerUi {
    SwaggerUi::new(API_DOCS_PATH)
        .url(API_DOCS_OPENAPI_JSON_PATH, openapi::ApiDoc::openapi())
}

/// Handles health check for top-level API routing.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn health_check() -> &'static str {
    "ok"
}

/// Handles openapi json for top-level API routing.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `axum::Json<utoipa::openapi::OpenApi>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn openapi_json() -> axum::Json<utoipa::openapi::OpenApi> {
    axum::Json(openapi::ApiDoc::openapi())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use http_body_util::BodyExt;
    use serde_json::Value;
    use tower::ServiceExt;

    #[tokio::test]
    /// Verifies that api docs are served under api v1.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns a future that resolves to `()` after the operation completes.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    async fn api_docs_are_served_under_api_v1() {
        let app = Router::<()>::from(api_docs());

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(API_DOCS_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert_eq!(
            response
                .headers()
                .get(axum::http::header::LOCATION)
                .and_then(|header| header.to_str().ok()),
            Some("/api/v1/api-docs/")
        );

        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/api-docs/")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = app
            .oneshot(
                Request::builder()
                    .uri(API_DOCS_OPENAPI_JSON_PATH)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let document: Value = serde_json::from_slice(&body).unwrap();
        assert!(document.get("openapi").is_some());
        assert!(document.get("paths").is_some());
    }
}
