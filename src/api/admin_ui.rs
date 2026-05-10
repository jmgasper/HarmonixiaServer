use axum::{
    http::{header, HeaderValue},
    response::{Html, IntoResponse, Response},
    routing::get,
    Router,
};

use crate::state::AppState;

const INDEX_HTML: &str = include_str!("../admin_ui/index.html");
const APP_JS: &str = include_str!("../admin_ui/app.js");
const STYLES_CSS: &str = include_str!("../admin_ui/styles.css");

/// Builds the Axum router for admin UI static assets.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `Router<AppState>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/admin", get(admin_index))
        .route("/admin/", get(admin_index))
        .route("/admin/app.js", get(admin_script))
        .route("/admin/styles.css", get(admin_styles))
}

/// Handles admin index for admin UI static assets.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `Html<&'static str>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn admin_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

/// Handles admin script for admin UI static assets.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `Response` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn admin_script() -> Response {
    static_response("application/javascript; charset=utf-8", APP_JS)
}

/// Handles admin styles for admin UI static assets.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `Response` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn admin_styles() -> Response {
    static_response("text/css; charset=utf-8", STYLES_CSS)
}

/// Handles static response for admin UI static assets.
///
/// Inputs:
/// - `content_type`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `body`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Response` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn static_response(content_type: &'static str, body: &'static str) -> Response {
    let mut response = body.into_response();
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
}

#[cfg(test)]
mod tests {
    use axum::{body::Body, http::header, response::IntoResponse};
    use http_body_util::BodyExt;

    use super::*;

    /// Handles body text for admin UI static assets.
    ///
    /// Inputs:
    /// - `body`: `Body`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `String` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    async fn body_text(body: Body) -> String {
        let bytes = body.collect().await.unwrap().to_bytes();
        std::str::from_utf8(&bytes).unwrap().to_string()
    }

    #[tokio::test]
    /// Verifies that index exposes first run wizard hooks.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns a future that resolves to `()` after the operation completes.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    async fn index_exposes_first_run_wizard_hooks() {
        let response = admin_index().await.into_response();
        let html = body_text(response.into_body()).await;

        assert!(html.contains("Harmonixia First-Run Wizard"));
        assert!(html.contains("data-element-id=\"admin-username\""));
        assert!(html.contains("data-element-id=\"admin-password\""));
        assert!(html.contains("data-element-id=\"library-root\""));
        assert!(html.contains("data-element-id=\"dropbox-root\""));
        assert!(html.contains("data-element-id=\"provider-musicbrainz\""));
        assert!(html.contains("data-element-id=\"provider-discogs\""));
        assert!(html.contains("data-element-id=\"discogs-api-key\""));
        assert!(html.contains("data-element-id=\"provider-fanart\""));
        assert!(html.contains("data-element-id=\"fanart-api-key\""));
        assert!(html.contains("data-element-id=\"provider-audiodb\""));
        assert!(html.contains("data-element-id=\"audiodb-api-key\""));
        assert!(html.contains("data-element-id=\"start-scan-button\""));
    }

    #[tokio::test]
    /// Verifies that index exposes batch two admin dashboard hooks.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns a future that resolves to `()` after the operation completes.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    async fn index_exposes_batch_two_admin_dashboard_hooks() {
        let response = admin_index().await.into_response();
        let html = body_text(response.into_body()).await;

        assert!(html.contains("data-element-id=\"nav-dashboard\""));
        assert!(html.contains("data-element-id=\"nav-library\""));
        assert!(html.contains("data-element-id=\"nav-providers\""));
        assert!(html.contains("data-element-id=\"nav-users\""));
        assert!(!html.contains("data-element-id=\"nav-providers\" aria-disabled=\"true\""));
        assert!(!html.contains("data-element-id=\"nav-users\" aria-disabled=\"true\""));
        assert!(html.contains("data-element-id=\"summary-scanning\""));
        assert!(html.contains("data-element-id=\"summary-imported\""));
        assert!(html.contains("data-element-id=\"summary-quarantined\""));
        assert!(html.contains("data-element-id=\"summary-failed\""));
        assert!(html.contains("data-element-id=\"summary-artists\""));
        assert!(html.contains("data-element-id=\"summary-albums\""));
        assert!(html.contains("data-element-id=\"summary-tracks\""));
        assert!(html.contains("data-element-id=\"summary-playlists\""));
        assert!(html.contains("data-element-id=\"scan-progress-list\""));
        assert!(html.contains("data-element-id=\"transcode-in-use\""));
        assert!(html.contains("data-element-id=\"full-rescan-button\""));
        assert!(html.contains("data-element-id=\"subtree-rescan-path\""));
        assert!(html.contains("data-element-id=\"providers-table-body\""));
        assert!(html.contains("data-element-id=\"refresh-library-button\""));
        assert!(html.contains("data-element-id=\"library-list\""));
        assert!(html.contains("data-element-id=\"library-detail\""));
        assert!(html.contains("data-element-id=\"library-tab-artists\""));
        assert!(html.contains("data-element-id=\"library-tab-albums\""));
        assert!(html.contains("data-element-id=\"library-tab-tracks\""));
        assert!(html.contains("data-element-id=\"library-tab-playlists\""));
        assert!(html.contains("data-element-id=\"player-start-button\""));
        assert!(html.contains("data-element-id=\"player-stop-button\""));
        assert!(html.contains("data-element-id=\"player-next-button\""));
        assert!(html.contains("data-element-id=\"player-prev-button\""));
        assert!(html.contains("data-element-id=\"create-user-username\""));
        assert!(html.contains("data-element-id=\"create-user-password\""));
        assert!(html.contains("data-element-id=\"create-user-role\""));
        assert!(html.contains("data-element-id=\"users-table-body\""));
    }

    #[tokio::test]
    /// Handles static assets have browser content types for admin UI static assets.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns a future that resolves to `()` after the operation completes.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    async fn static_assets_have_browser_content_types() {
        let script = admin_script().await;
        assert_eq!(
            script.headers().get(header::CONTENT_TYPE).unwrap(),
            "application/javascript; charset=utf-8"
        );

        let styles = admin_styles().await;
        assert_eq!(
            styles.headers().get(header::CONTENT_TYPE).unwrap(),
            "text/css; charset=utf-8"
        );
    }

    #[test]
    /// Verifies that script uses server bootstrap status for setup landing.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn script_uses_server_bootstrap_status_for_setup_landing() {
        let compact_script = APP_JS.split_whitespace().collect::<Vec<_>>().join(" ");

        assert!(APP_JS.contains("loadBootstrapStatus()"));
        assert!(APP_JS.contains("status.initial_scan_started"));
        assert!(APP_JS.contains("openDashboard();") || APP_JS.contains("await openDashboard();"));
        assert!(APP_JS.contains("openResumableSetup("));
        assert!(compact_script.contains(
            "const status = await loadBootstrapStatus(); if (status.initial_scan_started) { await openDashboard(); return; } await loadSetupState(); openResumableSetup(resumeMessage);"
        ));
        assert!(compact_script.contains(
            "await openSetupOrDashboard(\"Review saved setup settings, then start the initial scan.\");"
        ));
        assert!(APP_JS
            .contains("Admin account created. Review saved settings, then start the initial scan."));
        assert!(compact_script.contains(
            "const status = await loadBootstrapStatus(); if (!status.initial_scan_started) { throw new Error(\"Initial scan was accepted, but setup completion was not recorded.\"); }"
        ));
        assert!(!APP_JS.contains("localStorage"));
        assert!(!APP_JS.contains("setupCompletionMatchesCurrentState"));
        assert!(!APP_JS.contains("rememberSetupComplete"));
        assert!(!APP_JS.contains("harmonixia.adminSetupComplete"));
    }

    #[test]
    /// Verifies that script hydrates setup state before wizard saves.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn script_hydrates_setup_state_before_wizard_saves() {
        let compact_script = APP_JS.split_whitespace().collect::<Vec<_>>().join(" ");

        assert!(compact_script.contains(
            "async function loadSetupState() { const [systemConfig, providerSettings] = await Promise.all([ api(\"/api/v1/admin/system/config\"), api(\"/api/v1/admin/providers/settings\"), ]);"
        ));
        assert!(compact_script.contains("hydrateSystemConfig(systemConfig);"));
        assert!(compact_script.contains("hydrateProviderSettings();"));
        assert!(compact_script.contains("if (!state.setupLoaded) { await loadSetupState(); }"));
        assert!(compact_script.contains(
            "const originalEnabled = setting ? Boolean(setting.enabled) : false;"
        ));
        assert!(compact_script.contains("if (!setting || enabled !== originalEnabled) {"));
        assert!(compact_script.contains("if (apiKey) { body.api_key = apiKey; }"));
    }

    #[test]
    /// Verifies that script wires batch two admin rest surfaces.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn script_wires_batch_two_admin_rest_surfaces() {
        for endpoint in [
            "/api/v1/admin/maintenance/summary",
            "/api/v1/admin/media/transcode-slots",
            "/api/v1/admin/providers/health",
            "/api/v1/admin/users",
            "/api/v1/admin/maintenance/rescans/full",
            "/api/v1/admin/maintenance/rescans/subtree",
            "/api/v1/admin/users/${userId}/password-reset",
            "/api/v1/admin/users/${userId}",
            "/api/v1/catalog/artists",
            "/api/v1/catalog/albums",
            "/api/v1/catalog/tracks",
            "/api/v1/catalog/${encodeURIComponent(entityType)}/${encodeURIComponent(entityId)}/artwork",
            "/api/v1/playlists",
            "/api/v1/media/track/${encodeURIComponent(track.id)}/transcode/standard",
            "/api/v1/me/playback/progress/track/${encodeURIComponent(state.currentTrackId)}",
        ] {
            assert!(APP_JS.contains(endpoint), "missing endpoint {endpoint}");
        }

        assert!(APP_JS.contains("openDashboardSection"));
        assert!(APP_JS.contains("renderProviderHealth"));
        assert!(APP_JS.contains("renderUsers"));
        assert!(APP_JS.contains("renderLibrary"));
        assert!(APP_JS.contains("playTrack("));
        assert!(!APP_JS.contains("/api/v1/admin/quarantine"));
    }
}
