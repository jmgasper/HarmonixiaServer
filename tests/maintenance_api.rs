use axum::{
    body::Body,
    http::{header, HeaderMap, Request, StatusCode},
};
use base64::{engine::general_purpose, Engine as _};
use chrono::{Duration as ChronoDuration, Utc};
use harmonixia_server::{
    domain::{
        AacTranscodeProfile, AccountRole, AlbumKind, ArtworkAssetDraft, ArtworkKind,
        CatalogEntityType, CatalogGrouping, CatalogImportDecision, CatalogImportRequest,
        ImportJobKind, ImportJobSource, ImportJobStatus, MaintenanceScope, MediaFileStatus,
        MediaProbeFacts, MetadataProvenanceDraft, MusicCatalogGrouping, PodcastCatalogGrouping,
        PlaybackItemType, ProviderHealth, ProviderKind, ProviderStatus, QuarantineItem,
        QuarantineStatus, RepairPlan, SonosDeliveryKind, SonosSessionStatus,
    },
    pipeline::ImportWorkRequest,
    providers::{ProviderCredential, ProviderRegistry},
    router, AppState, ServerConfig,
    services::{
        BackgroundServiceConfig, BackgroundServices, DropboxWatcherConfig, ImportWorkerConfig,
    },
    sonos::{SonosGroupSnapshot, SonosLiveState, SonosSnapshot, SonosSpeakerSnapshot},
    state::{ProviderConfig, ServerConfigError, SonosMediaAuthorizationRequest},
    storage::{DatabaseConfig, StorageError},
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    fs,
    future::Future,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
    task::JoinHandle,
};
use tower::ServiceExt;
use uuid::Uuid;

const ADMIN_USERNAME: &str = "admin";
const ADMIN_PASSWORD: &str = "admin-password";
const USER_USERNAME: &str = "listener";
const USER_PASSWORD: &str = "listener-password";

#[derive(Clone, Copy)]
/// Represents test auth in the integration-test support and end-to-end API behavior coverage.
///
/// Functionality: Enumerates `Admin`, `User` states or choices for integration-test support and end-to-end API behavior coverage.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `tests/maintenance_api.rs`.
enum TestAuth {
    Admin,
    User,
}

/// Verifies that test config.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `Some(ServerConfig)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn test_config() -> Option<ServerConfig> {
    let database_url = std::env::var("HARMONIXIA_TEST_DATABASE_URL")
        .or_else(|_| std::env::var("DATABASE_URL"))
        .ok()?;

    let mut config = ServerConfig::default();
    config.database = DatabaseConfig {
        url: database_url,
        max_connections: 2,
        connect_timeout: Duration::from_secs(5),
        schema: Some(format!("test_{}", Uuid::new_v4().simple())),
    };

    Some(config)
}

/// Verifies that disable external providers.
///
/// Inputs:
/// - `config`: `&mut ServerConfig`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn disable_external_providers(config: &mut ServerConfig) {
    for provider in [
        ProviderKind::MusicBrainz,
        ProviderKind::CoverArtArchive,
        ProviderKind::Discogs,
        ProviderKind::FanartTv,
        ProviderKind::TheAudioDb,
    ] {
        config.providers.insert(
            provider,
            ProviderConfig {
                enabled: false,
                api_key: None,
                api_key_configured: false,
                requires_api_key: matches!(
                    provider,
                    ProviderKind::Discogs | ProviderKind::FanartTv | ProviderKind::TheAudioDb
                ),
            },
        );
    }
}

/// Verifies that enable external provider.
///
/// Inputs:
/// - `config`: `&mut ServerConfig`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn enable_external_provider(config: &mut ServerConfig, provider: ProviderKind) {
    config.providers.insert(
        provider,
        ProviderConfig {
            enabled: true,
            api_key: None,
            api_key_configured: false,
            requires_api_key: matches!(
                provider,
                ProviderKind::Discogs | ProviderKind::FanartTv | ProviderKind::TheAudioDb
            ),
        },
    );
}

/// Represents env var guard in the integration-test support and end-to-end API behavior coverage.
///
/// Functionality: Carries fields `key`, `previous` for integration-test support and end-to-end API behavior coverage.
/// Dependencies: depends on `&'static str`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `tests/maintenance_api.rs`.
struct EnvVarGuard {
    key: &'static str,
    previous: Option<String>,
}

impl EnvVarGuard {
    /// Verifies that set.
    ///
    /// Inputs:
    /// - `key`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var(key).ok();
        std::env::set_var(key, value);
        Self { key, previous }
    }
}

impl Drop for EnvVarGuard {
    /// Releases owned resources and performs cleanup.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn drop(&mut self) {
        if let Some(previous) = self.previous.as_deref() {
            std::env::set_var(self.key, previous);
        } else {
            std::env::remove_var(self.key);
        }
    }
}

/// Represents mock provider server in the integration-test support and end-to-end API behavior coverage.
///
/// Functionality: Carries fields `base_url`, `requests`, `handle` for integration-test support and end-to-end API behavior coverage.
/// Dependencies: depends on `String`, `Arc<AtomicU32>`, `JoinHandle<()>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `tests/maintenance_api.rs`.
struct MockProviderServer {
    base_url: String,
    requests: Arc<AtomicU32>,
    handle: JoinHandle<()>,
}

impl MockProviderServer {
    /// Verifies that failing.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    async fn failing() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("mock provider server should bind");
        let base_url = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("mock provider server should have local address")
        );
        let requests = Arc::new(AtomicU32::new(0));
        let request_counter = requests.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let request_counter = request_counter.clone();
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 2048];
                    let _ = socket.read(&mut buffer).await;
                    request_counter.fetch_add(1, Ordering::SeqCst);
                    let response = concat!(
                        "HTTP/1.1 500 Internal Server Error\r\n",
                        "content-type: application/json\r\n",
                        "content-length: 2\r\n",
                        "connection: close\r\n",
                        "\r\n",
                        "{}"
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });

        Self {
            base_url,
            requests,
            handle,
        }
    }
}

impl Drop for MockProviderServer {
    /// Releases owned resources and performs cleanup.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Verifies that test state.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `Some(AppState)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn test_state() -> Option<AppState> {
    let Some(config) = test_config() else {
        eprintln!(
            "skipping Postgres-backed maintenance API test; set HARMONIXIA_TEST_DATABASE_URL"
        );
        return None;
    };

    let state = AppState::connect(config)
        .await
        .expect("test database should connect and migrate");
    seed_test_accounts(&state).await;
    Some(state)
}

/// Verifies that test state with roots.
///
/// Inputs:
/// - `library_root`: `PathBuf`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `dropbox_root`: `PathBuf`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `Some(AppState)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn test_state_with_roots(
    library_root: PathBuf,
    dropbox_root: PathBuf,
) -> Option<AppState> {
    let Some(mut config) = test_config() else {
        eprintln!(
            "skipping Postgres-backed maintenance API test; set HARMONIXIA_TEST_DATABASE_URL"
        );
        return None;
    };

    config.library_root = library_root;
    config.dropbox_root = dropbox_root;
    let state = AppState::connect(config)
        .await
        .expect("test database should connect and migrate");
    seed_test_accounts(&state).await;
    Some(state)
}

/// Verifies that test state with transcode runtime.
///
/// Inputs:
/// - `library_root`: `PathBuf`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `dropbox_root`: `PathBuf`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `ffmpeg_path`: `PathBuf`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `transcode_concurrency_limit`: `i32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `Some(AppState)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn test_state_with_transcode_runtime(
    library_root: PathBuf,
    dropbox_root: PathBuf,
    ffmpeg_path: PathBuf,
    transcode_concurrency_limit: i32,
) -> Option<AppState> {
    let Some(mut config) = test_config() else {
        eprintln!(
            "skipping Postgres-backed maintenance API test; set HARMONIXIA_TEST_DATABASE_URL"
        );
        return None;
    };

    config.library_root = library_root;
    config.dropbox_root = dropbox_root;
    config.ffmpeg_path = ffmpeg_path;
    config.transcode_concurrency_limit = transcode_concurrency_limit;
    let state = AppState::connect(config)
        .await
        .expect("test database should connect and migrate");
    seed_test_accounts(&state).await;
    Some(state)
}

/// Verifies that test state without accounts.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `Some(AppState)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn test_state_without_accounts() -> Option<AppState> {
    let Some(config) = test_config() else {
        eprintln!(
            "skipping Postgres-backed foundation API test; set HARMONIXIA_TEST_DATABASE_URL"
        );
        return None;
    };

    Some(
        AppState::connect(config)
            .await
            .expect("test database should connect and migrate"),
    )
}

/// Verifies that seed test accounts.
///
/// Inputs:
/// - `state`: `&AppState`; expected to be Axum application state with a live repository and runtime configuration.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn seed_test_accounts(state: &AppState) {
    state
        .create_local_account(ADMIN_USERNAME, ADMIN_PASSWORD, AccountRole::Admin)
        .await
        .expect("admin account should be created");
    state
        .create_local_account(USER_USERNAME, USER_PASSWORD, AccountRole::User)
        .await
        .expect("user account should be created");
}

/// Verifies that auth header.
///
/// Inputs:
/// - `auth`: `TestAuth`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn auth_header(auth: TestAuth) -> String {
    let (username, password) = match auth {
        TestAuth::Admin => (ADMIN_USERNAME, ADMIN_PASSWORD),
        TestAuth::User => (USER_USERNAME, USER_PASSWORD),
    };
    auth_header_for(username, password)
}

/// Verifies that auth header for.
///
/// Inputs:
/// - `username`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `password`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn auth_header_for(username: &str, password: &str) -> String {
    let credentials = general_purpose::STANDARD.encode(format!("{username}:{password}"));
    format!("Basic {credentials}")
}

/// Verifies that request json.
///
/// Inputs:
/// - `app`: `axum:Router`; expected to be a value satisfying the type contract shown in the function signature.
/// - `method`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `uri`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `body`: `Value`; expected to be a value satisfying the type contract shown in the function signature.
/// - `auth`: `Option<TestAuth>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
///
/// Output:
/// - Returns `(StatusCode, Value)` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn request_json(
    app: axum::Router,
    method: &str,
    uri: &str,
    body: Value,
    auth: Option<TestAuth>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder()
        .method(method)
        .uri(uri)
        .header("content-type", "application/json");
    if let Some(auth) = auth {
        builder = builder.header("authorization", auth_header(auth));
    }

    let response = app
        .oneshot(builder.body(Body::from(body.to_string())).unwrap())
        .await
        .unwrap();

    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// Verifies that get json.
///
/// Inputs:
/// - `app`: `axum:Router`; expected to be a value satisfying the type contract shown in the function signature.
/// - `uri`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `auth`: `Option<TestAuth>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
///
/// Output:
/// - Returns `(StatusCode, Value)` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn get_json(
    app: axum::Router,
    uri: &str,
    auth: Option<TestAuth>,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().method("GET").uri(uri);
    if let Some(auth) = auth {
        builder = builder.header("authorization", auth_header(auth));
    }

    let response = app
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// Verifies that get json with credentials.
///
/// Inputs:
/// - `app`: `axum:Router`; expected to be a value satisfying the type contract shown in the function signature.
/// - `uri`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `username`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `password`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `(StatusCode, Value)` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn get_json_with_credentials(
    app: axum::Router,
    uri: &str,
    username: &str,
    password: &str,
) -> (StatusCode, Value) {
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(uri)
                .header("authorization", auth_header_for(username, password))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

/// Verifies that get bytes.
///
/// Inputs:
/// - `app`: `axum:Router`; expected to be a value satisfying the type contract shown in the function signature.
/// - `uri`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `auth`: `Option<TestAuth>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
/// - `extra_headers`: `&[(&str, &str)]`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `(StatusCode, HeaderMap, Vec<u8>)` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn get_bytes(
    app: axum::Router,
    uri: &str,
    auth: Option<TestAuth>,
    extra_headers: &[(&str, &str)],
) -> (StatusCode, HeaderMap, Vec<u8>) {
    let mut builder = Request::builder().method("GET").uri(uri);
    if let Some(auth) = auth {
        builder = builder.header("authorization", auth_header(auth));
    }
    for (name, value) in extra_headers {
        builder = builder.header(*name, *value);
    }

    let response = app
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let headers = response.headers().clone();
    let bytes = response.into_body().collect().await.unwrap().to_bytes().to_vec();
    (status, headers, bytes)
}

async fn configure_public_base_url(
    state: &AppState,
    library_root: &Path,
    dropbox_root: &Path,
    public_base_url: &str,
) {
    state
        .update_system_config(
            &library_root.to_string_lossy(),
            &dropbox_root.to_string_lossy(),
            Some("Podcasts"),
            Some(Some(public_base_url)),
            None,
            None,
        )
        .await
        .unwrap();
}

fn sonos_media_request(
    item_type: PlaybackItemType,
    item_id: Uuid,
    session_generation: u64,
    item_generation: u64,
    target_id: &str,
) -> SonosMediaAuthorizationRequest {
    SonosMediaAuthorizationRequest {
        session_id: Uuid::parse_str("018f26c0-0000-7000-8000-000000000100").unwrap(),
        session_generation,
        item_generation,
        target_id: target_id.into(),
        item_type,
        item_id,
    }
}

fn path_and_query_from_url(url: &str) -> String {
    let url = reqwest::Url::parse(url).unwrap();
    match url.query() {
        Some(query) => format!("{}?{query}", url.path()),
        None => url.path().to_string(),
    }
}

fn latest_sonos_media_uri(raw_requests: &[String]) -> String {
    raw_requests
        .iter()
        .rev()
        .find_map(|request| {
            let start = request.find("<CurrentURI>")? + "<CurrentURI>".len();
            let end = request[start..].find("</CurrentURI>")? + start;
            Some(decode_test_xml_entities(&request[start..end]))
        })
        .expect("expected a Sonos SetAVTransportURI request")
}

fn decode_test_xml_entities(value: &str) -> String {
    value
        .replace("&apos;", "'")
        .replace("&quot;", "\"")
        .replace("&gt;", ">")
        .replace("&lt;", "<")
        .replace("&amp;", "&")
}

struct MockSonosSoapServer {
    base_url: String,
    requests: Arc<Mutex<Vec<String>>>,
    raw_requests: Arc<Mutex<Vec<String>>>,
    fail_next_actions: Arc<Mutex<Vec<String>>>,
    block_next_action: Arc<Mutex<Option<String>>>,
    blocked_action_seen: Arc<Notify>,
    release_blocked_action: Arc<Notify>,
    handle: JoinHandle<()>,
}

impl MockSonosSoapServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let raw_requests = Arc::new(Mutex::new(Vec::new()));
        let fail_next_actions = Arc::new(Mutex::new(Vec::new()));
        let block_next_action = Arc::new(Mutex::new(None));
        let blocked_action_seen = Arc::new(Notify::new());
        let release_blocked_action = Arc::new(Notify::new());
        let transport_state = Arc::new(Mutex::new("PLAYING".to_string()));
        let server_requests = requests.clone();
        let server_raw_requests = raw_requests.clone();
        let server_fail_next_actions = fail_next_actions.clone();
        let server_block_next_action = block_next_action.clone();
        let server_blocked_action_seen = blocked_action_seen.clone();
        let server_release_blocked_action = release_blocked_action.clone();
        let server_transport_state = transport_state.clone();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let server_requests = server_requests.clone();
                let server_raw_requests = server_raw_requests.clone();
                let server_fail_next_actions = server_fail_next_actions.clone();
                let server_block_next_action = server_block_next_action.clone();
                let server_blocked_action_seen = server_blocked_action_seen.clone();
                let server_release_blocked_action = server_release_blocked_action.clone();
                let server_transport_state = server_transport_state.clone();
                tokio::spawn(async move {
                    let mut buffer = [0_u8; 8192];
                    let Ok(len) = socket.read(&mut buffer).await else {
                        return;
                    };
                    let request = String::from_utf8_lossy(&buffer[..len]).to_string();
                    server_raw_requests.lock().unwrap().push(request.clone());
                    let first_line = request.lines().next().unwrap_or_default().to_string();
                    let action = sonos_action_from_request(&request);
                    let action_suffix = action
                        .map(|action| format!(" {action}"))
                        .unwrap_or_default();
                    server_requests
                        .lock()
                        .unwrap()
                        .push(format!("{first_line}{action_suffix}"));

                    if let Some(action) = action {
                        let should_block = {
                            let mut block_next_action = server_block_next_action.lock().unwrap();
                            if block_next_action.as_deref() == Some(action) {
                                *block_next_action = None;
                                true
                            } else {
                                false
                            }
                        };
                        if should_block {
                            server_blocked_action_seen.notify_one();
                            server_release_blocked_action.notified().await;
                        }

                        let should_fail = {
                            let mut fail_next_actions = server_fail_next_actions.lock().unwrap();
                            if let Some(index) = fail_next_actions
                                .iter()
                                .position(|candidate| candidate == action)
                            {
                                fail_next_actions.remove(index);
                                true
                            } else {
                                false
                            }
                        };
                        if should_fail {
                            let body = soap_response("");
                            let response = format!(
                                "HTTP/1.1 500 Internal Server Error\r\ncontent-type: text/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                                body.len(),
                                body
                            );
                            let _ = socket.write_all(response.as_bytes()).await;
                            return;
                        }
                    }

                    match action {
                        Some("Play") => {
                            *server_transport_state.lock().unwrap() = "PLAYING".to_string();
                        }
                        Some("Pause") => {
                            *server_transport_state.lock().unwrap() =
                                "PAUSED_PLAYBACK".to_string();
                        }
                        Some("Stop") => {
                            *server_transport_state.lock().unwrap() = "STOPPED".to_string();
                        }
                        _ => {}
                    }

                    let body = if request.contains("GetVolume") {
                        soap_response("<CurrentVolume>23</CurrentVolume>")
                    } else if request.contains("GetMute") {
                        soap_response("<CurrentMute>0</CurrentMute>")
                    } else if request.contains("GetTransportInfo") {
                        let state = server_transport_state.lock().unwrap().clone();
                        soap_response(&format!(
                            "<CurrentTransportState>{state}</CurrentTransportState>"
                        ))
                    } else {
                        soap_response("")
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\ncontent-type: text/xml\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });

        Self {
            base_url,
            requests,
            raw_requests,
            fail_next_actions,
            block_next_action,
            blocked_action_seen,
            release_blocked_action,
            handle,
        }
    }

    fn fail_next_action(&self, action: &str) {
        self.fail_next_actions
            .lock()
            .unwrap()
            .push(action.to_string());
    }

    fn block_next_action(&self, action: &str) {
        *self.block_next_action.lock().unwrap() = Some(action.to_string());
    }

    async fn wait_for_blocked_action(&self) {
        self.blocked_action_seen.notified().await;
    }

    fn release_blocked_action(&self) {
        self.release_blocked_action.notify_waiters();
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }

    fn raw_requests(&self) -> Vec<String> {
        self.raw_requests.lock().unwrap().clone()
    }
}

impl Drop for MockSonosSoapServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

fn sonos_action_from_request(request: &str) -> Option<&'static str> {
    for action in [
        "SetAVTransportURI",
        "Play",
        "Pause",
        "Seek",
        "Stop",
        "BecomeCoordinatorOfStandaloneGroup",
        "GetTransportInfo",
        "GetVolume",
        "GetMute",
    ] {
        if request.contains(action) {
            return Some(action);
        }
    }
    None
}

fn soap_response(body: &str) -> String {
    format!("<?xml version=\"1.0\"?><s:Envelope><s:Body>{body}</s:Body></s:Envelope>")
}

fn sonos_snapshot_for_speaker(
    target_id: &str,
    display_name: &str,
    base_url: &str,
    raw_transport_state: &str,
) -> SonosSnapshot {
    let mut locations = BTreeMap::new();
    locations.insert(
        target_id.to_string(),
        format!("{base_url}/xml/device.xml"),
    );
    SonosSnapshot::from_targets_with_control_locations(
        vec![SonosSpeakerSnapshot {
            id: target_id.into(),
            display_name: display_name.into(),
            room_name: Some(display_name.into()),
            available: true,
            live: SonosLiveState {
                volume_percent: Some(20),
                muted: Some(false),
                raw_transport_state: Some(raw_transport_state.into()),
            },
        }],
        Vec::new(),
        locations,
    )
}

async fn import_sonos_test_track(
    state: &AppState,
    dropbox_root: &Path,
    managed_path: &Path,
    hash: &str,
    title: &str,
    duration_seconds: i32,
) -> Uuid {
    let mut request = music_import_request(
        &dropbox_root.join(format!("{hash}-source.mp3")).to_string_lossy(),
        hash,
        "Sonos Test Artist",
        "Sonos Test Album",
        title,
        Some(1),
    );
    request.probe.mime_type = Some("audio/mpeg".into());
    request.probe.container = Some("mp3".into());
    request.probe.audio_codec = Some("mp3".into());
    request.probe.duration_seconds = Some(duration_seconds);
    state
        .repository()
        .import_catalog_file(with_managed_path(
            request,
            managed_path,
            i64::from(duration_seconds.max(1)),
        ))
        .await
        .unwrap()
        .track
        .unwrap()
        .id
}

/// Verifies that json array contains id.
///
/// Inputs:
/// - `values`: `&Value`; expected to be a value satisfying the type contract shown in the function signature.
/// - `id`: `&Value`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn json_array_contains_id(values: &Value, id: &Value) -> bool {
    values
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value["id"] == *id)
}

/// Verifies that provider setting json.
///
/// Inputs:
/// - `settings`: `&'a Value`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `&'a Value` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn provider_setting_json<'a>(settings: &'a Value, provider: &str) -> &'a Value {
    settings["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|setting| setting["provider"] == provider)
        .unwrap_or_else(|| panic!("provider setting {provider} should be present"))
}

/// Verifies that music import request.
///
/// Inputs:
/// - `source_path`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `file_hash`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `artist`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `album`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `title`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `track_number`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
///
/// Output:
/// - Returns `CatalogImportRequest` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn music_import_request(
    source_path: &str,
    file_hash: &str,
    artist: &str,
    album: &str,
    title: &str,
    track_number: Option<i32>,
) -> CatalogImportRequest {
    CatalogImportRequest {
        source_path: source_path.to_string(),
        managed_path: Some(format!("/srv/harmonixia/library/{file_hash}.flac")),
        grouping: CatalogGrouping::Music(MusicCatalogGrouping {
            album_artist: artist.to_string(),
            track_artist: artist.to_string(),
            album_title: album.to_string(),
            track_title: title.to_string(),
            album_kind: AlbumKind::Album,
            release_year: Some(1969),
            disc_number: Some(1),
            track_number,
        }),
        probe: MediaProbeFacts {
            file_hash: file_hash.to_string(),
            file_size: 1024,
            mime_type: Some("audio/flac".into()),
            container: Some("flac".into()),
            audio_codec: Some("flac".into()),
            duration_seconds: Some(180),
            bitrate: Some(900_000),
            sample_rate: Some(44_100),
            channels: Some(2),
        },
        import_job_id: None,
        provider_links: Vec::new(),
        provenance: Vec::new(),
        artwork: Vec::new(),
        allow_reuse_existing: false,
        refresh_artwork: true,
        rebuild_search_projections: true,
        preserve_provenance_history: true,
        preserve_confidence_history: true,
    }
}

/// Verifies that music import request with artists.
///
/// Inputs:
/// - `source_path`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `file_hash`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `album_artist`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `track_artist`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `album`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `title`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `track_number`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
///
/// Output:
/// - Returns `CatalogImportRequest` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn music_import_request_with_artists(
    source_path: &str,
    file_hash: &str,
    album_artist: &str,
    track_artist: &str,
    album: &str,
    title: &str,
    track_number: Option<i32>,
) -> CatalogImportRequest {
    let mut request =
        music_import_request(source_path, file_hash, album_artist, album, title, track_number);
    let CatalogGrouping::Music(grouping) = &mut request.grouping else {
        unreachable!("music_import_request always builds music grouping");
    };
    grouping.track_artist = track_artist.to_string();
    grouping.album_kind = AlbumKind::Compilation;
    request
}

/// Verifies that with music release year.
///
/// Inputs:
/// - `request`: `CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
/// - `year`: `i32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `CatalogImportRequest` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn with_music_release_year(mut request: CatalogImportRequest, year: i32) -> CatalogImportRequest {
    let CatalogGrouping::Music(grouping) = &mut request.grouping else {
        unreachable!("with_music_release_year is only used with music requests");
    };
    grouping.release_year = Some(year);
    request
}

/// Verifies that with genre.
///
/// Inputs:
/// - `request`: `CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
/// - `genre`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `CatalogImportRequest` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn with_genre(mut request: CatalogImportRequest, genre: &str) -> CatalogImportRequest {
    request.provenance.push(MetadataProvenanceDraft {
        entity_type: CatalogEntityType::MediaFile,
        field_name: "genre".into(),
        provider: ProviderKind::LocalSidecars,
        value: json!(genre),
        confidence: 1.0,
        auto_accepted: true,
    });
    request
}

/// Verifies that with probe format.
///
/// Inputs:
/// - `request`: `CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
/// - `mime_type`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `container`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `audio_codec`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `CatalogImportRequest` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn with_probe_format(
    mut request: CatalogImportRequest,
    mime_type: &str,
    container: &str,
    audio_codec: &str,
) -> CatalogImportRequest {
    request.probe.mime_type = Some(mime_type.into());
    request.probe.container = Some(container.into());
    request.probe.audio_codec = Some(audio_codec.into());
    request
}

/// Verifies that with managed path.
///
/// Inputs:
/// - `request`: `CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
/// - `managed_path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `file_size`: `i64`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `CatalogImportRequest` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn with_managed_path(
    mut request: CatalogImportRequest,
    managed_path: &Path,
    file_size: i64,
) -> CatalogImportRequest {
    request.managed_path = Some(managed_path.to_string_lossy().to_string());
    request.probe.file_size = file_size;
    request
}

/// Verifies that test png bytes.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `Vec<u8>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn test_png_bytes() -> Vec<u8> {
    let image = image::DynamicImage::ImageRgba8(image::RgbaImage::from_pixel(
        1,
        1,
        image::Rgba([0, 128, 255, 255]),
    ));
    let mut cursor = std::io::Cursor::new(Vec::new());
    image
        .write_to(&mut cursor, image::ImageOutputFormat::Png)
        .unwrap();
    cursor.into_inner()
}

/// Verifies that fake ffmpeg script.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `args_log`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `started_marker`: `Option<&Path>`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `sleep_seconds`: `u64`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn fake_ffmpeg_script(
    path: &Path,
    args_log: &Path,
    started_marker: Option<&Path>,
    sleep_seconds: u64,
) {
    let marker = started_marker
        .map(|path| format!("touch '{}'\n", path.display()))
        .unwrap_or_default();
    let sleep = if sleep_seconds > 0 {
        format!("sleep {sleep_seconds}\n")
    } else {
        String::new()
    };
    let script = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\n{}{}printf 'fake-aac-output'\n",
        args_log.display(),
        marker,
        sleep
    );
    fs::write(path, script).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

/// Verifies that fake hls ffmpeg script.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `args_log`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `started_marker`: `Option<&Path>`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `sleep_seconds`: `u64`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn fake_hls_ffmpeg_script(
    path: &Path,
    args_log: &Path,
    started_marker: Option<&Path>,
    sleep_seconds: u64,
) {
    let marker = started_marker
        .map(|path| format!("touch '{}'\n", path.display()))
        .unwrap_or_default();
    let sleep = if sleep_seconds > 0 {
        format!("sleep {sleep_seconds}\n")
    } else {
        String::new()
    };
    let script = format!(
        concat!(
            "#!/bin/sh\n",
            "printf '%s\\n' \"$@\" > '{}'\n",
            "{}{}",
            "mkdir -p segments\n",
            "cat > manifest.m3u8 <<'EOF'\n",
            "#EXTM3U\n",
            "#EXT-X-VERSION:3\n",
            "#EXT-X-TARGETDURATION:6\n",
            "#EXTINF:6.000,\n",
            "segments/segment-00000.ts\n",
            "#EXT-X-ENDLIST\n",
            "EOF\n",
            "printf 'fake-hls-segment' > segments/segment-00000.ts\n"
        ),
        args_log.display(),
        marker,
        sleep
    );
    fs::write(path, script).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

/// Verifies that fake hls ffmpeg script prompt manifest.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `args_log`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `started_marker`: `Option<&Path>`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `sleep_seconds`: `u64`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn fake_hls_ffmpeg_script_prompt_manifest(
    path: &Path,
    args_log: &Path,
    started_marker: Option<&Path>,
    sleep_seconds: u64,
) {
    let marker = started_marker
        .map(|path| format!("touch '{}'\n", path.display()))
        .unwrap_or_default();
    let sleep = if sleep_seconds > 0 {
        format!("sleep {sleep_seconds}\n")
    } else {
        String::new()
    };
    let script = format!(
        concat!(
            "#!/bin/sh\n",
            "printf '%s\\n' \"$@\" > '{}'\n",
            "{}",
            "mkdir -p segments\n",
            "cat > manifest.m3u8 <<'EOF'\n",
            "#EXTM3U\n",
            "#EXT-X-VERSION:3\n",
            "#EXT-X-TARGETDURATION:6\n",
            "#EXTINF:6.000,\n",
            "segments/segment-00000.ts\n",
            "EOF\n",
            "printf 'fake-hls-segment' > segments/segment-00000.ts\n",
            "{}"
        ),
        args_log.display(),
        marker,
        sleep
    );
    fs::write(path, script).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

/// Verifies that fake hls ffmpeg script with gate.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `args_log`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `launches_log`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `started_marker`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `release_marker`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `sleep_seconds`: `u64`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn fake_hls_ffmpeg_script_with_gate(
    path: &Path,
    args_log: &Path,
    launches_log: &Path,
    started_marker: &Path,
    release_marker: &Path,
    sleep_seconds: u64,
) {
    let sleep = if sleep_seconds > 0 {
        format!("sleep {sleep_seconds}\n")
    } else {
        String::new()
    };
    let script = format!(
        concat!(
            "#!/bin/sh\n",
            "printf 'launch\\n' >> '{}'\n",
            "printf '%s\\n' \"$@\" > '{}'\n",
            "touch '{}'\n",
            "while [ ! -f '{}' ]; do sleep 0.05; done\n",
            "mkdir -p segments\n",
            "cat > manifest.m3u8 <<'EOF'\n",
            "#EXTM3U\n",
            "#EXT-X-VERSION:3\n",
            "#EXT-X-TARGETDURATION:6\n",
            "#EXTINF:6.000,\n",
            "segments/segment-00000.ts\n",
            "EOF\n",
            "printf 'fake-hls-segment' > segments/segment-00000.ts\n",
            "{}"
        ),
        launches_log.display(),
        args_log.display(),
        started_marker.display(),
        release_marker.display(),
        sleep
    );
    fs::write(path, script).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

/// Verifies that podcast import request.
///
/// Inputs:
/// - `source_path`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `file_hash`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `podcast`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `episode`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `episode_number`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
///
/// Output:
/// - Returns `CatalogImportRequest` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn podcast_import_request(
    source_path: &str,
    file_hash: &str,
    podcast: &str,
    episode: &str,
    episode_number: Option<i32>,
) -> CatalogImportRequest {
    CatalogImportRequest {
        source_path: source_path.to_string(),
        managed_path: Some(format!("/srv/harmonixia/library/podcasts/{file_hash}.mp3")),
        grouping: CatalogGrouping::Podcast(PodcastCatalogGrouping {
            podcast_title: podcast.to_string(),
            episode_title: episode.to_string(),
            season_number: Some(1),
            episode_number,
        }),
        probe: MediaProbeFacts {
            file_hash: file_hash.to_string(),
            file_size: 2048,
            mime_type: Some("audio/mpeg".into()),
            container: Some("mp3".into()),
            audio_codec: Some("mp3".into()),
            duration_seconds: Some(900),
            bitrate: Some(128_000),
            sample_rate: Some(44_100),
            channels: Some(2),
        },
        import_job_id: None,
        provider_links: Vec::new(),
        provenance: Vec::new(),
        artwork: Vec::new(),
        allow_reuse_existing: false,
        refresh_artwork: true,
        rebuild_search_projections: true,
        preserve_provenance_history: true,
        preserve_confidence_history: true,
    }
}

/// Verifies that eventually.
///
/// Inputs:
/// - `timeout`: `Duration`; expected to be a value satisfying the type contract shown in the function signature.
/// - `check`: `F`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
async fn eventually<F, Fut>(timeout: Duration, mut check: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = Instant::now() + timeout;
    loop {
        if check().await {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
/// Verifies that first admin bootstrap only works before users exist.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn first_admin_bootstrap_only_works_before_users_exist() {
    let Some(state) = test_state_without_accounts().await else {
        return;
    };
    let app = router(state);

    let (status, body) = get_json(app.clone(), "/api/v1/bootstrap/status", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["users_exist"], false);
    assert_eq!(body["first_admin_required"], true);
    assert_eq!(body["initial_scan_started"], false);

    let (created_status, created) = request_json(
        app.clone(),
        "POST",
        "/api/v1/bootstrap/first-admin",
        json!({ "username": ADMIN_USERNAME, "password": ADMIN_PASSWORD }),
        None,
    )
    .await;
    assert_eq!(created_status, StatusCode::CREATED);
    assert_eq!(created["username"], ADMIN_USERNAME);
    assert_eq!(created["role"], "admin");

    let (me_status, me) =
        get_json(app.clone(), "/api/v1/auth/me", Some(TestAuth::Admin)).await;
    assert_eq!(me_status, StatusCode::OK);
    assert_eq!(me["username"], ADMIN_USERNAME);

    let (second_status, second_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/bootstrap/first-admin",
        json!({ "username": "other-admin", "password": "password" }),
        None,
    )
    .await;
    assert_eq!(second_status, StatusCode::CONFLICT);
    assert_eq!(second_body["code"], "conflict");

    let (final_status, final_body) = get_json(app, "/api/v1/bootstrap/status", None).await;
    assert_eq!(final_status, StatusCode::OK);
    assert_eq!(final_body["users_exist"], true);
    assert_eq!(final_body["first_admin_required"], false);
    assert_eq!(final_body["initial_scan_started"], false);
}

#[tokio::test]
/// Verifies that bootstrap status reports initial scan started from persisted jobs.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn bootstrap_status_reports_initial_scan_started_from_persisted_jobs() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state.clone());

    let (pending_status, pending_body) =
        get_json(app.clone(), "/api/v1/bootstrap/status", None).await;
    assert_eq!(pending_status, StatusCode::OK);
    assert_eq!(pending_body["users_exist"], true);
    assert_eq!(pending_body["first_admin_required"], false);
    assert_eq!(pending_body["initial_scan_started"], false);

    let (scan_status, scan_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/admin/maintenance/scans/initial",
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(scan_status, StatusCode::ACCEPTED);
    assert_eq!(scan_body["job"]["kind"], "initial_scan");

    let job_id = scan_body["job"]["id"].as_str().unwrap();
    sqlx::query(
        r#"
        UPDATE import_jobs
        SET status = 'completed',
            updated_at = now()
        WHERE id = $1
        "#,
    )
    .bind(Uuid::parse_str(job_id).unwrap())
    .execute(state.repository().pool())
    .await
    .unwrap();

    let (complete_status, complete_body) =
        get_json(app, "/api/v1/bootstrap/status", None).await;
    assert_eq!(complete_status, StatusCode::OK);
    assert_eq!(complete_body["users_exist"], true);
    assert_eq!(complete_body["first_admin_required"], false);
    assert_eq!(complete_body["initial_scan_started"], true);
}

#[tokio::test]
/// Verifies that admin dashboard summary reports coarse import counts.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn admin_dashboard_summary_reports_coarse_import_counts() {
    let Some(state) = test_state().await else {
        return;
    };

    state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/dashboard-summary-imported.flac",
            "dashboard-summary-imported-hash",
            "Dashboard Artist",
            "Dashboard Album",
            "Dashboard Song",
            Some(1),
        ))
        .await
        .unwrap();

    let mut quarantined_request = music_import_request(
        "/dropbox/dashboard-summary-quarantined.flac",
        "dashboard-summary-quarantined-hash",
        "Dashboard Artist",
        "Dashboard Album",
        "Quarantined Dashboard Song",
        Some(2),
    );
    let CatalogGrouping::Music(grouping) = &mut quarantined_request.grouping else {
        unreachable!("music_import_request always builds music grouping");
    };
    grouping.album_title.clear();
    state
        .repository()
        .import_catalog_file(quarantined_request)
        .await
        .unwrap();

    state
        .repository()
        .quarantine_file_error(
            music_import_request(
                "/dropbox/dashboard-summary-failed.flac",
                "dashboard-summary-failed-hash",
                "Dashboard Artist",
                "Dashboard Album",
                "Failed Dashboard Song",
                Some(3),
            ),
            "probe failed",
        )
        .await
        .unwrap();

    let queued = state
        .enqueue_import_work(ImportWorkRequest {
            kind: ImportJobKind::FullRescan,
            scope: MaintenanceScope::FullLibrary,
            repair_plan: RepairPlan::default(),
            provider_filter: Vec::new(),
            source: ImportJobSource::AdminFullRescan,
            reason: Some(format!("dashboard summary {}", Uuid::new_v4())),
            related_quarantine_item_id: None,
        })
        .await
        .unwrap();
    state
        .repository()
        .upsert_catalog_import_work_item(
            queued.job.id,
            "/dropbox/dashboard-summary-progress.flac",
            None,
            MediaFileStatus::Published,
            1,
            None,
        )
        .await
        .unwrap();
    sqlx::query(
        r#"
        INSERT INTO playlists (
            id,
            name,
            scope,
            created_at,
            updated_at
        )
        VALUES ($1, 'Dashboard Summary Playlist', 'shared', now(), now())
        "#,
    )
    .bind(Uuid::new_v4())
    .execute(state.repository().pool())
    .await
    .unwrap();

    let app = router(state);

    let (unauthorized_status, unauthorized_body) =
        get_json(app.clone(), "/api/v1/admin/maintenance/summary", None).await;
    assert_eq!(unauthorized_status, StatusCode::UNAUTHORIZED);
    assert_eq!(unauthorized_body["code"], "unauthorized");

    let (forbidden_status, forbidden_body) = get_json(
        app.clone(),
        "/api/v1/admin/maintenance/summary",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(forbidden_status, StatusCode::FORBIDDEN);
    assert_eq!(forbidden_body["code"], "forbidden");

    let (status, body) = get_json(
        app,
        "/api/v1/admin/maintenance/summary",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_object().unwrap().len(), 9);
    assert_eq!(body["scanning"], 1);
    assert_eq!(body["imported"], 1);
    assert_eq!(body["quarantined"], 1);
    assert_eq!(body["failed"], 1);
    assert_eq!(body["artists"], 1);
    assert_eq!(body["albums"], 1);
    assert_eq!(body["tracks"], 1);
    assert_eq!(body["playlists"], 1);
    assert_eq!(body["active_jobs"].as_array().unwrap().len(), 1);
    assert_eq!(body["active_jobs"][0]["id"], queued.job.id.to_string());
    assert_eq!(body["active_jobs"][0]["kind"], "full_rescan");
    assert_eq!(body["active_jobs"][0]["processed_files"], 1);
    assert_eq!(body["active_jobs"][0]["published_files"], 1);
    assert_eq!(body["active_jobs"][0]["quarantined_files"], 0);
    assert_eq!(body["active_jobs"][0]["failed_files"], 0);
    assert!(body["active_jobs"][0]["last_progress_at"].is_string());
}

#[tokio::test]
/// Verifies that admin import failures endpoint returns persisted work item errors.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn admin_import_failures_reports_failed_work_items() {
    let Some(state) = test_state().await else {
        return;
    };

    let failed_job = state
        .enqueue_import_work(ImportWorkRequest {
            kind: ImportJobKind::FullRescan,
            scope: MaintenanceScope::FullLibrary,
            repair_plan: RepairPlan::default(),
            provider_filter: Vec::new(),
            source: ImportJobSource::AdminFullRescan,
            reason: Some(format!("failure list {}", Uuid::new_v4())),
            related_quarantine_item_id: None,
        })
        .await
        .unwrap()
        .job;
    state
        .repository()
        .upsert_catalog_import_work_item(
            failed_job.id,
            "/dropbox/failure-list-failed.flac",
            None,
            MediaFileStatus::Failed,
            2,
            Some("database operation failed: column reference \"id\" is ambiguous"),
        )
        .await
        .unwrap();

    let other_job = state
        .enqueue_import_work(ImportWorkRequest {
            kind: ImportJobKind::SubtreeRescan,
            scope: MaintenanceScope::Path {
                path: "/dropbox/other".to_string(),
            },
            repair_plan: RepairPlan::default(),
            provider_filter: Vec::new(),
            source: ImportJobSource::AdminSubtreeRescan,
            reason: Some(format!("other failure list {}", Uuid::new_v4())),
            related_quarantine_item_id: None,
        })
        .await
        .unwrap()
        .job;
    state
        .repository()
        .upsert_catalog_import_work_item(
            other_job.id,
            "/dropbox/failure-list-other.flac",
            None,
            MediaFileStatus::Failed,
            1,
            Some("other error"),
        )
        .await
        .unwrap();

    let app = router(state);

    let (unauthorized_status, unauthorized_body) =
        get_json(app.clone(), "/api/v1/admin/maintenance/failures", None).await;
    assert_eq!(unauthorized_status, StatusCode::UNAUTHORIZED);
    assert_eq!(unauthorized_body["code"], "unauthorized");

    let (forbidden_status, forbidden_body) = get_json(
        app.clone(),
        "/api/v1/admin/maintenance/failures",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(forbidden_status, StatusCode::FORBIDDEN);
    assert_eq!(forbidden_body["code"], "forbidden");

    let (status, body) = get_json(
        app.clone(),
        &format!(
            "/api/v1/admin/maintenance/failures?import_job_id={}",
            failed_job.id
        ),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let failures = body["failures"].as_array().unwrap();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0]["import_job_id"], failed_job.id.to_string());
    assert_eq!(failures[0]["import_job_kind"], "full_rescan");
    assert_eq!(failures[0]["import_job_status"], "queued");
    assert_eq!(failures[0]["source_path"], "/dropbox/failure-list-failed.flac");
    assert_eq!(failures[0]["status"], "failed");
    assert_eq!(failures[0]["attempts"], 2);
    assert_eq!(
        failures[0]["last_error"],
        "database operation failed: column reference \"id\" is ambiguous"
    );
    assert!(failures[0]["updated_at"].is_string());

    let (all_status, all_body) = get_json(
        app,
        "/api/v1/admin/maintenance/failures?limit=1",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(all_status, StatusCode::OK);
    assert_eq!(all_body["failures"].as_array().unwrap().len(), 1);
}

#[tokio::test]
/// Verifies that admin dashboard summary excludes retrying quarantine items from problem counts.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn admin_dashboard_summary_excludes_retrying_quarantine_items_from_problem_counts() {
    let Some(state) = test_state().await else {
        return;
    };

    let mut quarantined_request = music_import_request(
        "/srv/harmonixia/dropbox/dashboard-summary-retrying-quarantined.flac",
        "dashboard-summary-retrying-quarantined-hash",
        "Dashboard Retry Artist",
        "Dashboard Retry Album",
        "Retrying Quarantined Dashboard Song",
        Some(1),
    );
    let CatalogGrouping::Music(grouping) = &mut quarantined_request.grouping else {
        unreachable!("music_import_request always builds music grouping");
    };
    grouping.album_title.clear();
    let quarantined_outcome = state
        .repository()
        .import_catalog_file(quarantined_request)
        .await
        .unwrap();
    let quarantined_item_id = quarantined_outcome
        .quarantine_item
        .as_ref()
        .expect("unstable grouping should create a quarantine item")
        .id;

    let failed_outcome = state
        .repository()
        .quarantine_file_error(
            music_import_request(
                "/srv/harmonixia/dropbox/dashboard-summary-retrying-failed.flac",
                "dashboard-summary-retrying-failed-hash",
                "Dashboard Retry Artist",
                "Dashboard Retry Album",
                "Retrying Failed Dashboard Song",
                Some(2),
            ),
            "probe failed",
        )
        .await
        .unwrap();
    let failed_item_id = failed_outcome
        .quarantine_item
        .as_ref()
        .expect("file errors should create a quarantine item")
        .id;

    let app = router(state.clone());

    let (initial_status, initial_body) = get_json(
        app.clone(),
        "/api/v1/admin/maintenance/summary",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(initial_status, StatusCode::OK);
    assert_eq!(initial_body["scanning"], 0);
    assert_eq!(initial_body["quarantined"], 1);
    assert_eq!(initial_body["failed"], 1);

    let (retry_status, retry_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/admin/quarantine/retry",
        json!({ "item_ids": [quarantined_item_id, failed_item_id] }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(retry_status, StatusCode::ACCEPTED);
    assert_eq!(
        retry_body["retried_item_ids"],
        json!([quarantined_item_id, failed_item_id])
    );

    for item_id in [quarantined_item_id, failed_item_id] {
        let item = state.quarantine_item(item_id).await.unwrap().unwrap();
        assert_eq!(item.status, QuarantineStatus::Retrying);
        assert!(item.last_import_job_id.is_some());
    }

    let (summary_status, summary_body) = get_json(
        app,
        "/api/v1/admin/maintenance/summary",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(summary_status, StatusCode::OK);
    assert_eq!(summary_body["scanning"], 2);
    assert_eq!(summary_body["quarantined"], 0);
    assert_eq!(summary_body["failed"], 0);
}

#[tokio::test]
/// Verifies that first admin creation can resume setup and complete from server state.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn first_admin_creation_can_resume_setup_and_complete_from_server_state() {
    let Some(state) = test_state_without_accounts().await else {
        return;
    };
    let app = router(state);

    let (created_status, created) = request_json(
        app.clone(),
        "POST",
        "/api/v1/bootstrap/first-admin",
        json!({ "username": ADMIN_USERNAME, "password": ADMIN_PASSWORD }),
        None,
    )
    .await;
    assert_eq!(created_status, StatusCode::CREATED);
    assert_eq!(created["role"], "admin");

    let (pending_status, pending_body) =
        get_json(app.clone(), "/api/v1/bootstrap/status", None).await;
    assert_eq!(pending_status, StatusCode::OK);
    assert_eq!(pending_body["first_admin_required"], false);
    assert_eq!(pending_body["initial_scan_started"], false);

    let (config_status, config_body) = get_json(
        app.clone(),
        "/api/v1/admin/system/config",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(config_status, StatusCode::OK);
    assert_eq!(config_body["library_root"], "/srv/harmonixia/library");
    assert_eq!(config_body["public_base_url"], Value::Null);
    assert_eq!(config_body["scan_thread_count"], 8);

    let (settings_status, settings_body) = get_json(
        app.clone(),
        "/api/v1/admin/providers/settings",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(settings_status, StatusCode::OK);
    assert!(settings_body["providers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|provider| provider["provider"] == "discogs"));

    let (config_update_status, config_update_body) = request_json(
        app.clone(),
        "PUT",
        "/api/v1/admin/system/config",
        json!({
            "library_root": "/data/harmonixia/library",
            "dropbox_root": "/data/harmonixia/dropbox"
        }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(config_update_status, StatusCode::OK);
    assert_eq!(
        config_update_body["library_root"],
        "/data/harmonixia/library"
    );

    let (provider_update_status, provider_update_body) = request_json(
        app.clone(),
        "PATCH",
        "/api/v1/admin/providers/discogs/settings",
        json!({ "enabled": true, "api_key": "setup-discogs-key" }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(provider_update_status, StatusCode::OK);
    assert_eq!(provider_update_body["api_key_configured"], true);

    let (scan_status, scan_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/admin/maintenance/scans/initial",
        json!({ "reason": "setup completion test" }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(scan_status, StatusCode::ACCEPTED);
    assert_eq!(scan_body["job"]["kind"], "initial_scan");

    let (complete_status, complete_body) =
        get_json(app, "/api/v1/bootstrap/status", None).await;
    assert_eq!(complete_status, StatusCode::OK);
    assert_eq!(complete_body["first_admin_required"], false);
    assert_eq!(complete_body["initial_scan_started"], true);
}

#[tokio::test]
/// Verifies that resumed setup save preserves persisted config and provider state.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn resumed_setup_save_preserves_persisted_config_and_provider_state() {
    let Some(state) = test_state_without_accounts().await else {
        return;
    };

    state
        .update_system_config(
            "/persisted/harmonixia/library",
            "/persisted/harmonixia/dropbox",
            Some("Shows/Podcasts"),
            Some(Some("https://speaker-lan.example.test:8443")),
            Some(7),
            Some(4),
        )
        .await
        .unwrap();
    state
        .update_provider_setting(
            ProviderKind::Discogs,
            Some(false),
            Some("persisted-discogs-key"),
            false,
        )
        .await
        .unwrap();
    state
        .update_provider_setting(
            ProviderKind::FanartTv,
            Some(true),
            Some("persisted-fanart-key"),
            false,
        )
        .await
        .unwrap();
    state
        .update_provider_setting(
            ProviderKind::TheAudioDb,
            Some(true),
            Some("persisted-audiodb-key"),
            false,
        )
        .await
        .unwrap();

    let app = router(state.clone());

    let (created_status, created) = request_json(
        app.clone(),
        "POST",
        "/api/v1/bootstrap/first-admin",
        json!({ "username": ADMIN_USERNAME, "password": ADMIN_PASSWORD }),
        None,
    )
    .await;
    assert_eq!(created_status, StatusCode::CREATED);
    assert_eq!(created["role"], "admin");

    let (pending_status, pending_body) =
        get_json(app.clone(), "/api/v1/bootstrap/status", None).await;
    assert_eq!(pending_status, StatusCode::OK);
    assert_eq!(pending_body["users_exist"], true);
    assert_eq!(pending_body["first_admin_required"], false);
    assert_eq!(pending_body["initial_scan_started"], false);

    let (config_status, config_body) = get_json(
        app.clone(),
        "/api/v1/admin/system/config",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(config_status, StatusCode::OK);
    assert_eq!(config_body["library_root"], "/persisted/harmonixia/library");
    assert_eq!(config_body["dropbox_root"], "/persisted/harmonixia/dropbox");
    assert_eq!(config_body["podcast_subtree"], "Shows/Podcasts");
    assert_eq!(
        config_body["public_base_url"],
        "https://speaker-lan.example.test:8443"
    );
    assert_eq!(config_body["transcode_concurrency_limit"], 7);
    assert_eq!(config_body["scan_thread_count"], 4);

    let (settings_status, settings_body) = get_json(
        app.clone(),
        "/api/v1/admin/providers/settings",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(settings_status, StatusCode::OK);
    let discogs = provider_setting_json(&settings_body, "discogs");
    assert_eq!(discogs["enabled"], false);
    assert_eq!(discogs["api_key_configured"], true);
    let fanart = provider_setting_json(&settings_body, "fanart_tv");
    assert_eq!(fanart["enabled"], true);
    assert_eq!(fanart["api_key_configured"], true);
    let audio_db = provider_setting_json(&settings_body, "the_audio_db");
    assert_eq!(audio_db["enabled"], true);
    assert_eq!(audio_db["api_key_configured"], true);

    let (config_update_status, updated_config) = request_json(
        app.clone(),
        "PUT",
        "/api/v1/admin/system/config",
        json!({
            "library_root": "/setup/harmonixia/library",
            "dropbox_root": "/setup/harmonixia/dropbox"
        }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(config_update_status, StatusCode::OK);
    assert_eq!(updated_config["library_root"], "/setup/harmonixia/library");
    assert_eq!(updated_config["dropbox_root"], "/setup/harmonixia/dropbox");
    assert_eq!(updated_config["podcast_subtree"], "Shows/Podcasts");
    assert_eq!(
        updated_config["public_base_url"],
        "https://speaker-lan.example.test:8443"
    );
    assert_eq!(updated_config["transcode_concurrency_limit"], 7);
    assert_eq!(updated_config["scan_thread_count"], 4);

    let (discogs_update_status, discogs_update) = request_json(
        app.clone(),
        "PATCH",
        "/api/v1/admin/providers/discogs/settings",
        json!({ "enabled": true }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(discogs_update_status, StatusCode::OK);
    assert_eq!(discogs_update["enabled"], true);
    assert_eq!(discogs_update["api_key_configured"], true);

    let (audio_update_status, audio_update) = request_json(
        app.clone(),
        "PATCH",
        "/api/v1/admin/providers/the_audio_db/settings",
        json!({ "api_key": "wizard-audiodb-key" }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(audio_update_status, StatusCode::OK);
    assert_eq!(audio_update["enabled"], true);
    assert_eq!(audio_update["api_key_configured"], true);

    let (scan_status, scan_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/admin/maintenance/scans/initial",
        json!({ "reason": "Initial scan started from the first-run admin wizard" }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(scan_status, StatusCode::ACCEPTED);
    assert_eq!(scan_body["job"]["kind"], "initial_scan");

    let (complete_status, complete_body) =
        get_json(app.clone(), "/api/v1/bootstrap/status", None).await;
    assert_eq!(complete_status, StatusCode::OK);
    assert_eq!(complete_body["first_admin_required"], false);
    assert_eq!(complete_body["initial_scan_started"], true);

    let (final_config_status, final_config) = get_json(
        app.clone(),
        "/api/v1/admin/system/config",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(final_config_status, StatusCode::OK);
    assert_eq!(final_config["library_root"], "/setup/harmonixia/library");
    assert_eq!(final_config["dropbox_root"], "/setup/harmonixia/dropbox");
    assert_eq!(final_config["podcast_subtree"], "Shows/Podcasts");
    assert_eq!(
        final_config["public_base_url"],
        "https://speaker-lan.example.test:8443"
    );
    assert_eq!(final_config["transcode_concurrency_limit"], 7);
    assert_eq!(final_config["scan_thread_count"], 4);
    assert_eq!(state.system_config().podcast_subtree, "Shows/Podcasts");
    assert_eq!(state.system_config().transcode_concurrency_limit, 7);
    assert_eq!(state.system_config().scan_thread_count, 4);

    let (final_settings_status, final_settings) = get_json(
        app,
        "/api/v1/admin/providers/settings",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(final_settings_status, StatusCode::OK);
    let final_discogs = provider_setting_json(&final_settings, "discogs");
    assert_eq!(final_discogs["enabled"], true);
    assert_eq!(final_discogs["api_key_configured"], true);
    let final_fanart = provider_setting_json(&final_settings, "fanart_tv");
    assert_eq!(final_fanart["enabled"], true);
    assert_eq!(final_fanart["api_key_configured"], true);
    let final_audio_db = provider_setting_json(&final_settings, "the_audio_db");
    assert_eq!(final_audio_db["enabled"], true);
    assert_eq!(final_audio_db["api_key_configured"], true);

    let credentials = state.repository().provider_credentials().await.unwrap();
    let credential_for = |provider| {
        credentials
            .iter()
            .find(|credential| credential.provider == provider)
            .and_then(|credential| credential.api_key.as_deref())
    };
    assert_eq!(
        credential_for(ProviderKind::Discogs),
        Some("persisted-discogs-key")
    );
    assert_eq!(
        credential_for(ProviderKind::FanartTv),
        Some("persisted-fanart-key")
    );
    assert_eq!(
        credential_for(ProviderKind::TheAudioDb),
        Some("wizard-audiodb-key")
    );
}

#[tokio::test]
/// Verifies that admin user management creates resets and deletes users.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn admin_user_management_creates_resets_and_deletes_users() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state);

    let (denied_status, denied_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/admin/users",
        json!({ "username": "denied", "password": "password", "role": "user" }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(denied_status, StatusCode::FORBIDDEN);
    assert_eq!(denied_body["code"], "forbidden");

    let (created_status, created) = request_json(
        app.clone(),
        "POST",
        "/api/v1/admin/users",
        json!({ "username": "temporary", "password": "initial-password", "role": "user" }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(created_status, StatusCode::CREATED);
    let user_id = created["id"].as_str().unwrap();

    let (duplicate_status, duplicate_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/admin/users",
        json!({ "username": "temporary", "password": "other-password", "role": "user" }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(duplicate_status, StatusCode::CONFLICT);
    assert_eq!(duplicate_body["code"], "conflict");

    let (reset_status, reset) = request_json(
        app.clone(),
        "POST",
        &format!("/api/v1/admin/users/{user_id}/password-reset"),
        json!({ "password": "changed-password" }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(reset_status, StatusCode::OK);
    assert_eq!(reset["id"], created["id"]);

    let (old_login_status, old_login_body) = get_json_with_credentials(
        app.clone(),
        "/api/v1/auth/me",
        "temporary",
        "initial-password",
    )
    .await;
    assert_eq!(old_login_status, StatusCode::UNAUTHORIZED);
    assert_eq!(old_login_body["code"], "unauthorized");

    let (new_login_status, new_login) = get_json_with_credentials(
        app.clone(),
        "/api/v1/auth/me",
        "temporary",
        "changed-password",
    )
    .await;
    assert_eq!(new_login_status, StatusCode::OK);
    assert_eq!(new_login["username"], "temporary");

    let (delete_status, _) = request_json(
        app.clone(),
        "DELETE",
        &format!("/api/v1/admin/users/{user_id}"),
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(delete_status, StatusCode::NO_CONTENT);

    let (deleted_login_status, deleted_login_body) = get_json_with_credentials(
        app.clone(),
        "/api/v1/auth/me",
        "temporary",
        "changed-password",
    )
    .await;
    assert_eq!(deleted_login_status, StatusCode::UNAUTHORIZED);
    assert_eq!(deleted_login_body["code"], "unauthorized");

    let (admin_me_status, admin_me) =
        get_json(app.clone(), "/api/v1/auth/me", Some(TestAuth::Admin)).await;
    assert_eq!(admin_me_status, StatusCode::OK);
    let admin_id = admin_me["id"].as_str().unwrap();

    let (last_admin_status, last_admin_body) = request_json(
        app,
        "DELETE",
        &format!("/api/v1/admin/users/{admin_id}"),
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(last_admin_status, StatusCode::CONFLICT);
    assert_eq!(last_admin_body["code"], "conflict");
}

#[tokio::test]
/// Verifies that playlist crud enforces personal and shared visibility.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn playlist_crud_enforces_personal_and_shared_visibility() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state);

    let (admin_personal_status, admin_personal) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Admin private", "scope": "personal" }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(admin_personal_status, StatusCode::CREATED);
    assert_eq!(admin_personal["scope"], "personal");
    assert!(admin_personal["owner_account_id"].is_string());

    let (shared_status, shared) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "House mix", "description": "shared", "scope": "shared" }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(shared_status, StatusCode::CREATED);
    assert_eq!(shared["scope"], "shared");
    assert!(shared["owner_account_id"].is_null());

    let (user_get_private_status, user_get_private_body) = get_json(
        app.clone(),
        &format!("/api/v1/playlists/{}", admin_personal["id"].as_str().unwrap()),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(user_get_private_status, StatusCode::NOT_FOUND);
    assert_eq!(user_get_private_body["code"], "not_found");

    let (user_list_status, user_list) =
        get_json(app.clone(), "/api/v1/playlists", Some(TestAuth::User)).await;
    assert_eq!(user_list_status, StatusCode::OK);
    assert!(json_array_contains_id(&user_list["playlists"], &shared["id"]));
    assert!(!json_array_contains_id(
        &user_list["playlists"],
        &admin_personal["id"]
    ));

    let (shared_update_status, shared_update) = request_json(
        app.clone(),
        "PUT",
        &format!("/api/v1/playlists/{}", shared["id"].as_str().unwrap()),
        json!({ "name": "Updated house mix", "description": null }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(shared_update_status, StatusCode::OK);
    assert_eq!(shared_update["name"], "Updated house mix");

    let (user_delete_private_status, user_delete_private_body) = request_json(
        app.clone(),
        "DELETE",
        &format!("/api/v1/playlists/{}", admin_personal["id"].as_str().unwrap()),
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(user_delete_private_status, StatusCode::NOT_FOUND);
    assert_eq!(user_delete_private_body["code"], "not_found");

    let (user_personal_status, user_personal) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Listener private", "scope": "personal" }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(user_personal_status, StatusCode::CREATED);

    let (user_shared_status, user_shared) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Listener shared", "scope": "shared" }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(user_shared_status, StatusCode::CREATED);

    let (user_me_status, user_me) =
        get_json(app.clone(), "/api/v1/auth/me", Some(TestAuth::User)).await;
    assert_eq!(user_me_status, StatusCode::OK);
    let user_id = user_me["id"].as_str().unwrap();
    let (delete_user_status, _) = request_json(
        app.clone(),
        "DELETE",
        &format!("/api/v1/admin/users/{user_id}"),
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(delete_user_status, StatusCode::NO_CONTENT);

    let (admin_list_status, admin_list) =
        get_json(app, "/api/v1/playlists", Some(TestAuth::Admin)).await;
    assert_eq!(admin_list_status, StatusCode::OK);
    assert!(json_array_contains_id(
        &admin_list["playlists"],
        &user_shared["id"]
    ));
    assert!(!json_array_contains_id(
        &admin_list["playlists"],
        &user_personal["id"]
    ));
}

#[tokio::test]
/// Verifies that playlist items support append insert reorder and remove.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn playlist_items_support_append_insert_reorder_and_remove() {
    let Some(state) = test_state().await else {
        return;
    };

    let track_one = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/playlist-track-one.flac",
            "playlist-track-one-hash",
            "Playlist Artist",
            "Playlist Album",
            "First Playlist Track",
            Some(1),
        ))
        .await
        .unwrap()
        .track
        .unwrap()
        .id;
    let track_two = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/playlist-track-two.flac",
            "playlist-track-two-hash",
            "Playlist Artist",
            "Playlist Album",
            "Second Playlist Track",
            Some(2),
        ))
        .await
        .unwrap()
        .track
        .unwrap()
        .id;
    let episode = state
        .repository()
        .import_catalog_file(podcast_import_request(
            "/dropbox/playlist-episode.mp3",
            "playlist-episode-hash",
            "Playlist Podcast",
            "Playlist Episode",
            Some(1),
        ))
        .await
        .unwrap()
        .episode
        .unwrap()
        .id;

    let app = router(state);
    let (playlist_status, playlist) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Ordered playlist", "scope": "personal" }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(playlist_status, StatusCode::CREATED);
    let playlist_id = playlist["id"].as_str().unwrap();

    let (first_status, first_item) = request_json(
        app.clone(),
        "POST",
        &format!("/api/v1/playlists/{playlist_id}/items"),
        json!({ "item_type": "track", "item_id": track_one }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(first_status, StatusCode::CREATED);
    assert_eq!(first_item["position"], 0);

    let (second_status, second_item) = request_json(
        app.clone(),
        "POST",
        &format!("/api/v1/playlists/{playlist_id}/items"),
        json!({ "item_type": "track", "item_id": track_two }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(second_status, StatusCode::CREATED);
    assert_eq!(second_item["position"], 1);

    let (episode_status, episode_item) = request_json(
        app.clone(),
        "POST",
        &format!("/api/v1/playlists/{playlist_id}/items"),
        json!({ "item_type": "episode", "item_id": episode, "position": 1 }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(episode_status, StatusCode::CREATED);
    assert_eq!(episode_item["position"], 1);

    let (list_status, list) = get_json(
        app.clone(),
        &format!("/api/v1/playlists/{playlist_id}/items"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(list_status, StatusCode::OK);
    let items = list["items"].as_array().unwrap();
    assert_eq!(items.len(), 3);
    assert_eq!(items[0]["item_id"], json!(track_one));
    assert_eq!(items[0]["position"], 0);
    assert_eq!(items[1]["item_id"], json!(episode));
    assert_eq!(items[1]["position"], 1);
    assert_eq!(items[2]["item_id"], json!(track_two));
    assert_eq!(items[2]["position"], 2);

    let (reorder_status, reordered) = request_json(
        app.clone(),
        "PUT",
        &format!("/api/v1/playlists/{playlist_id}/items"),
        json!({
            "item_ids": [
                second_item["id"].as_str().unwrap(),
                first_item["id"].as_str().unwrap(),
                episode_item["id"].as_str().unwrap()
            ]
        }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(reorder_status, StatusCode::OK);
    let reordered_items = reordered["items"].as_array().unwrap();
    assert_eq!(reordered_items[0]["item_id"], json!(track_two));
    assert_eq!(reordered_items[0]["position"], 0);
    assert_eq!(reordered_items[1]["item_id"], json!(track_one));
    assert_eq!(reordered_items[1]["position"], 1);
    assert_eq!(reordered_items[2]["item_id"], json!(episode));
    assert_eq!(reordered_items[2]["position"], 2);

    let (remove_status, _) = request_json(
        app.clone(),
        "DELETE",
        &format!(
            "/api/v1/playlists/{playlist_id}/items/{}",
            first_item["id"].as_str().unwrap()
        ),
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(remove_status, StatusCode::NO_CONTENT);

    let (after_remove_status, after_remove) = get_json(
        app,
        &format!("/api/v1/playlists/{playlist_id}/items"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(after_remove_status, StatusCode::OK);
    let remaining = after_remove["items"].as_array().unwrap();
    assert_eq!(remaining.len(), 2);
    assert_eq!(remaining[0]["item_id"], json!(track_two));
    assert_eq!(remaining[0]["position"], 0);
    assert_eq!(remaining[1]["item_id"], json!(episode));
    assert_eq!(remaining[1]["position"], 1);
}

#[tokio::test]
/// Verifies that playlist items enforce visibility and catalog eligibility.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn playlist_items_enforce_visibility_and_catalog_eligibility() {
    let Some(state) = test_state().await else {
        return;
    };

    let visible_track = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/playlist-visible.flac",
            "playlist-visible-hash",
            "Visible Playlist Artist",
            "Visible Playlist Album",
            "Visible Playlist Track",
            Some(1),
        ))
        .await
        .unwrap()
        .track
        .unwrap()
        .id;
    let unpublished_track = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/playlist-unpublished.flac",
            "playlist-unpublished-hash",
            "Unpublished Playlist Artist",
            "Unpublished Playlist Album",
            "Unpublished Playlist Track",
            Some(1),
        ))
        .await
        .unwrap()
        .track
        .unwrap()
        .id;
    sqlx::query("UPDATE tracks SET published_at = NULL WHERE id = $1")
        .bind(unpublished_track)
        .execute(state.repository().pool())
        .await
        .unwrap();

    let app = router(state);
    let (_, admin_private) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Admin item private", "scope": "personal" }),
        Some(TestAuth::Admin),
    )
    .await;
    let admin_private_id = admin_private["id"].as_str().unwrap();

    let (hidden_status, hidden_body) = request_json(
        app.clone(),
        "POST",
        &format!("/api/v1/playlists/{admin_private_id}/items"),
        json!({ "item_type": "track", "item_id": visible_track }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(hidden_status, StatusCode::NOT_FOUND);
    assert_eq!(hidden_body["code"], "not_found");

    let (_, user_playlist) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "User item private", "scope": "personal" }),
        Some(TestAuth::User),
    )
    .await;
    let user_playlist_id = user_playlist["id"].as_str().unwrap();

    let (missing_status, missing_body) = request_json(
        app.clone(),
        "POST",
        &format!("/api/v1/playlists/{user_playlist_id}/items"),
        json!({ "item_type": "track", "item_id": Uuid::new_v4() }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(missing_status, StatusCode::BAD_REQUEST);
    assert_eq!(missing_body["code"], "bad_request");

    let (unpublished_status, unpublished_body) = request_json(
        app.clone(),
        "POST",
        &format!("/api/v1/playlists/{user_playlist_id}/items"),
        json!({ "item_type": "track", "item_id": unpublished_track }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(unpublished_status, StatusCode::BAD_REQUEST);
    assert_eq!(unpublished_body["code"], "bad_request");

    let (bad_position_status, bad_position_body) = request_json(
        app,
        "POST",
        &format!("/api/v1/playlists/{user_playlist_id}/items"),
        json!({ "item_type": "track", "item_id": visible_track, "position": 1 }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(bad_position_status, StatusCode::BAD_REQUEST);
    assert_eq!(bad_position_body["code"], "bad_request");
}

#[tokio::test]
/// Verifies that playlist items prune ineligible members and resequence positions.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn playlist_items_prune_ineligible_members_and_resequence_positions() {
    let Some(state) = test_state().await else {
        return;
    };

    let first = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/playlist-prune-first.flac",
            "playlist-prune-first-hash",
            "Prune Playlist Artist",
            "Prune Playlist Album",
            "First Prune Track",
            Some(1),
        ))
        .await
        .unwrap();
    let stale = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/playlist-prune-stale.flac",
            "playlist-prune-stale-hash",
            "Prune Playlist Artist",
            "Prune Playlist Album",
            "Stale Prune Track",
            Some(2),
        ))
        .await
        .unwrap();
    let last = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/playlist-prune-last.flac",
            "playlist-prune-last-hash",
            "Prune Playlist Artist",
            "Prune Playlist Album",
            "Last Prune Track",
            Some(3),
        ))
        .await
        .unwrap();
    let first_track_id = first.track.as_ref().unwrap().id;
    let stale_track_id = stale.track.as_ref().unwrap().id;
    let last_track_id = last.track.as_ref().unwrap().id;

    let app = router(state.clone());
    let (playlist_status, playlist) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Pruned playlist", "scope": "personal" }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(playlist_status, StatusCode::CREATED);
    let playlist_id = playlist["id"].as_str().unwrap();

    for track_id in [first_track_id, stale_track_id, last_track_id] {
        let (add_status, _) = request_json(
            app.clone(),
            "POST",
            &format!("/api/v1/playlists/{playlist_id}/items"),
            json!({ "item_type": "track", "item_id": track_id }),
            Some(TestAuth::User),
        )
        .await;
        assert_eq!(add_status, StatusCode::CREATED);
    }

    sqlx::query(
        r#"
        INSERT INTO quarantine_items (
          id,
          media_file_id,
          source_path,
          reason,
          status,
          retry_count,
          retry_eligible,
          last_import_job_id,
          admin_note,
          created_at,
          updated_at
        )
        VALUES (
          $1,
          $2,
          $3,
          'metadata_failure'::quarantine_reason,
          'open'::quarantine_status,
          0,
          true,
          NULL,
          NULL,
          $4,
          $4
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(stale.media_file.id)
    .bind(stale.media_file.source_path.as_str())
    .bind(Utc::now())
    .execute(state.repository().pool())
    .await
    .unwrap();

    let (list_status, list) = get_json(
        app,
        &format!("/api/v1/playlists/{playlist_id}/items"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(list_status, StatusCode::OK);
    let items = list["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["item_id"], json!(first_track_id));
    assert_eq!(items[0]["position"], 0);
    assert_eq!(items[1]["item_id"], json!(last_track_id));
    assert_eq!(items[1]["position"], 1);
    assert!(!items.iter().any(|item| item["item_id"] == json!(stale_track_id)));

    let stored: Vec<(Uuid, i32)> = sqlx::query_as(
        r#"
        SELECT item_id, position
        FROM playlist_items
        WHERE playlist_id = $1
        ORDER BY position ASC
        "#,
    )
    .bind(Uuid::parse_str(playlist_id).unwrap())
    .fetch_all(state.repository().pool())
    .await
    .unwrap();
    assert_eq!(stored, vec![(first_track_id, 0), (last_track_id, 1)]);
}

#[tokio::test]
/// Verifies that user deletion cleans personal playlist data and preserves shared items.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn user_deletion_cleans_personal_playlist_data_and_preserves_shared_items() {
    let Some(state) = test_state().await else {
        return;
    };

    let track = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/playlist-cleanup.flac",
            "playlist-cleanup-hash",
            "Cleanup Playlist Artist",
            "Cleanup Playlist Album",
            "Cleanup Playlist Track",
            Some(1),
        ))
        .await
        .unwrap()
        .track
        .unwrap()
        .id;

    let app = router(state.clone());
    let (_, user_me) = get_json(app.clone(), "/api/v1/auth/me", Some(TestAuth::User)).await;
    let user_id = user_me["id"].as_str().unwrap();

    let (_, personal) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Cleanup personal", "scope": "personal" }),
        Some(TestAuth::User),
    )
    .await;
    let personal_id = personal["id"].as_str().unwrap();
    let (_, shared) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Cleanup shared", "scope": "shared" }),
        Some(TestAuth::User),
    )
    .await;
    let shared_id = shared["id"].as_str().unwrap();

    let (personal_item_status, _) = request_json(
        app.clone(),
        "POST",
        &format!("/api/v1/playlists/{personal_id}/items"),
        json!({ "item_type": "track", "item_id": track }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(personal_item_status, StatusCode::CREATED);
    let (shared_item_status, shared_item) = request_json(
        app.clone(),
        "POST",
        &format!("/api/v1/playlists/{shared_id}/items"),
        json!({ "item_type": "track", "item_id": track }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(shared_item_status, StatusCode::CREATED);

    let (delete_status, _) = request_json(
        app.clone(),
        "DELETE",
        &format!("/api/v1/admin/users/{user_id}"),
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(delete_status, StatusCode::NO_CONTENT);

    let personal_uuid = Uuid::parse_str(personal_id).unwrap();
    let personal_playlist_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM playlists WHERE id = $1")
            .bind(personal_uuid)
            .fetch_one(state.repository().pool())
            .await
            .unwrap();
    assert_eq!(personal_playlist_count, 0);
    let personal_projection_count: i64 = sqlx::query_scalar(
        r#"
        SELECT count(*)
        FROM catalog_search_projection
        WHERE entity_type = 'playlist'::catalog_entity_type
          AND entity_id = $1
        "#,
    )
    .bind(personal_uuid)
    .fetch_one(state.repository().pool())
    .await
    .unwrap();
    assert_eq!(personal_projection_count, 0);

    let (shared_items_status, shared_items) = get_json(
        app,
        &format!("/api/v1/playlists/{shared_id}/items"),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(shared_items_status, StatusCode::OK);
    let items = shared_items["items"].as_array().unwrap();
    assert_eq!(items.len(), 1);
    assert_eq!(items[0]["id"], shared_item["id"]);
    assert!(items[0]["added_by_account_id"].is_null());
}

#[tokio::test]
/// Verifies that catalog search returns only playlists visible to requesting user.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn catalog_search_returns_only_playlists_visible_to_requesting_user() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state);

    let (_, admin_private) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Search Admin Private", "scope": "personal" }),
        Some(TestAuth::Admin),
    )
    .await;
    let (_, shared) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Search Shared Mix", "scope": "shared" }),
        Some(TestAuth::Admin),
    )
    .await;
    let (_, user_private) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Search Listener Private", "scope": "personal" }),
        Some(TestAuth::User),
    )
    .await;

    let (user_search_status, user_search) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=search",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(user_search_status, StatusCode::OK);
    assert!(json_array_contains_id(
        &user_search["playlists"],
        &shared["id"]
    ));
    assert!(json_array_contains_id(
        &user_search["playlists"],
        &user_private["id"]
    ));
    assert!(!json_array_contains_id(
        &user_search["playlists"],
        &admin_private["id"]
    ));

    let (admin_search_status, admin_search) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=search",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(admin_search_status, StatusCode::OK);
    assert!(json_array_contains_id(
        &admin_search["playlists"],
        &shared["id"]
    ));
    assert!(json_array_contains_id(
        &admin_search["playlists"],
        &admin_private["id"]
    ));
    assert!(!json_array_contains_id(
        &admin_search["playlists"],
        &user_private["id"]
    ));

    let (filtered_status, filtered_search) = get_json(
        app,
        "/api/v1/catalog/search?q=search&media_type=music",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(filtered_status, StatusCode::OK);
    assert!(filtered_search["playlists"].as_array().unwrap().is_empty());
}

#[tokio::test]
/// Verifies that catalog search backfills legacy playlist projections.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn catalog_search_backfills_legacy_playlist_projections() {
    let Some(state) = test_state().await else {
        return;
    };

    let admin_id: Uuid =
        sqlx::query_scalar("SELECT id FROM local_accounts WHERE username = $1")
            .bind(ADMIN_USERNAME)
            .fetch_one(state.repository().pool())
            .await
            .unwrap();
    let now = Utc::now();
    let private_id = Uuid::new_v4();
    let shared_id = Uuid::new_v4();
    sqlx::query(
        r#"
        INSERT INTO playlists (
          id,
          name,
          description,
          scope,
          owner_account_id,
          created_by_account_id,
          updated_by_account_id,
          created_at,
          updated_at
        )
        VALUES (
          $1,
          'Legacy Upgrade Private',
          NULL,
          'personal'::playlist_scope,
          $2,
          $2,
          $2,
          $3,
          $3
        ),
        (
          $4,
          'Legacy Upgrade Shared',
          NULL,
          'shared'::playlist_scope,
          NULL,
          $2,
          $2,
          $3,
          $3
        )
        "#,
    )
    .bind(private_id)
    .bind(admin_id)
    .bind(now)
    .bind(shared_id)
    .execute(state.repository().pool())
    .await
    .unwrap();

    state
        .repository()
        .backfill_catalog_search_upgrade_data()
        .await
        .unwrap();

    let app = router(state);
    let (user_status, user_search) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=legacy%20upgrade",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(user_status, StatusCode::OK);
    assert!(json_array_contains_id(
        &user_search["playlists"],
        &json!(shared_id)
    ));
    assert!(!json_array_contains_id(
        &user_search["playlists"],
        &json!(private_id)
    ));

    let (admin_status, admin_search) = get_json(
        app,
        "/api/v1/catalog/search?q=legacy%20upgrade",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(admin_status, StatusCode::OK);
    assert!(json_array_contains_id(
        &admin_search["playlists"],
        &json!(private_id)
    ));
    assert!(json_array_contains_id(
        &admin_search["playlists"],
        &json!(shared_id)
    ));
}

#[tokio::test]
/// Verifies that catalog browse returns only published stable items with cursor paging.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn catalog_browse_returns_only_published_stable_items_with_cursor_paging() {
    let Some(state) = test_state().await else {
        return;
    };

    state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/come-together.flac",
            "music-hash-1",
            "The Beatles",
            "Abbey Road",
            "Come Together",
            Some(1),
        ))
        .await
        .unwrap();
    state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/something.flac",
            "music-hash-2",
            "The Beatles",
            "Abbey Road",
            "Something",
            Some(2),
        ))
        .await
        .unwrap();
    let duplicate = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/duplicate.flac",
            "music-hash-1",
            "Other Artist",
            "Other Album",
            "Other Track",
            Some(1),
        ))
        .await
        .unwrap();
    assert!(matches!(
        duplicate.decision,
        CatalogImportDecision::QuarantinedDuplicate
    ));
    let unstable = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/unstable.flac",
            "music-hash-3",
            "Still Grouping",
            "",
            "Unpublished Track",
            Some(1),
        ))
        .await
        .unwrap();
    assert!(matches!(
        unstable.decision,
        CatalogImportDecision::QuarantinedUnstableGrouping
    ));
    state
        .repository()
        .import_catalog_file(podcast_import_request(
            "/dropbox/podcast-1.mp3",
            "podcast-hash-1",
            "History Daily",
            "Moon Landing",
            Some(1),
        ))
        .await
        .unwrap();

    let app = router(state);

    let (unauth_status, unauth_body) =
        get_json(app.clone(), "/api/v1/catalog/tracks", None).await;
    assert_eq!(unauth_status, StatusCode::UNAUTHORIZED);
    assert_eq!(unauth_body["code"], "unauthorized");

    let (bad_limit_status, bad_limit_body) = get_json(
        app.clone(),
        "/api/v1/catalog/artists?limit=0",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(bad_limit_status, StatusCode::BAD_REQUEST);
    assert_eq!(bad_limit_body["code"], "bad_request");

    let (bad_sort_status, bad_sort_body) = get_json(
        app.clone(),
        "/api/v1/catalog/tracks?sort=title",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(bad_sort_status, StatusCode::BAD_REQUEST);
    assert_eq!(bad_sort_body["code"], "bad_request");

    let (artists_status, artists) =
        get_json(app.clone(), "/api/v1/catalog/artists", Some(TestAuth::User)).await;
    assert_eq!(artists_status, StatusCode::OK);
    assert_eq!(artists["page"]["sort"], "name");
    let artist_names = artists["artists"]
        .as_array()
        .unwrap()
        .iter()
        .map(|artist| artist["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(artist_names, vec!["The Beatles"]);

    let (albums_status, albums) =
        get_json(app.clone(), "/api/v1/catalog/albums", Some(TestAuth::User)).await;
    assert_eq!(albums_status, StatusCode::OK);
    assert_eq!(albums["albums"].as_array().unwrap().len(), 1);
    assert_eq!(albums["albums"][0]["title"], "Abbey Road");

    let (tracks_status, first_tracks_page) = get_json(
        app.clone(),
        "/api/v1/catalog/tracks?limit=1",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(tracks_status, StatusCode::OK);
    assert_eq!(first_tracks_page["page"]["sort"], "album_position");
    assert_eq!(
        first_tracks_page["tracks"][0]["title"],
        json!("Come Together")
    );
    assert!(first_tracks_page["page"]["next_cursor"].is_string());

    let next_cursor = first_tracks_page["page"]["next_cursor"].as_str().unwrap();
    let (next_tracks_status, second_tracks_page) = get_json(
        app.clone(),
        &format!("/api/v1/catalog/tracks?limit=1&cursor={next_cursor}"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(next_tracks_status, StatusCode::OK);
    assert_eq!(second_tracks_page["tracks"][0]["title"], json!("Something"));
    assert!(!second_tracks_page["tracks"]
        .as_array()
        .unwrap()
        .iter()
        .any(|track| track["title"] == "Other Track"));
    assert!(!second_tracks_page["tracks"]
        .as_array()
        .unwrap()
        .iter()
        .any(|track| track["title"] == "Unpublished Track"));

    let (podcasts_status, podcasts) =
        get_json(app.clone(), "/api/v1/catalog/podcasts", Some(TestAuth::User)).await;
    assert_eq!(podcasts_status, StatusCode::OK);
    assert_eq!(podcasts["podcasts"].as_array().unwrap().len(), 1);
    assert_eq!(podcasts["podcasts"][0]["title"], "History Daily");

    let (episodes_status, episodes) =
        get_json(app, "/api/v1/catalog/episodes", Some(TestAuth::User)).await;
    assert_eq!(episodes_status, StatusCode::OK);
    assert_eq!(episodes["page"]["sort"], "podcast_position");
    assert_eq!(episodes["episodes"].as_array().unwrap().len(), 1);
    assert_eq!(episodes["episodes"][0]["title"], "Moon Landing");
}

#[tokio::test]
/// Verifies that podcast duplicate detection keeps same title different seasons visible.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn podcast_duplicate_detection_keeps_same_title_different_seasons_visible() {
    let Some(state) = test_state().await else {
        return;
    };

    let first = state
        .repository()
        .import_catalog_file(podcast_import_request(
            "/dropbox/season-one-trailer.mp3",
            "same-title-season-one-trailer-hash",
            "Seasoned Show",
            "Trailer",
            Some(1),
        ))
        .await
        .unwrap();
    assert!(matches!(first.decision, CatalogImportDecision::Published));

    let mut second_request = podcast_import_request(
        "/dropbox/season-two-trailer.mp3",
        "same-title-season-two-trailer-hash",
        "Seasoned Show",
        "Trailer",
        Some(1),
    );
    match &mut second_request.grouping {
        CatalogGrouping::Podcast(grouping) => {
            grouping.season_number = Some(2);
        }
        CatalogGrouping::Music(_) => {
            unreachable!("podcast_import_request builds podcast grouping")
        }
    }

    let second = state
        .repository()
        .import_catalog_file(second_request)
        .await
        .unwrap();
    assert!(matches!(second.decision, CatalogImportDecision::Published));

    let first_episode = first.episode.as_ref().unwrap();
    let second_episode = second.episode.as_ref().unwrap();
    assert_ne!(first_episode.id, second_episode.id);

    let first_episode_id = json!(first_episode.id);
    let second_episode_id = json!(second_episode.id);
    let app = router(state);

    let (episodes_status, episodes) =
        get_json(app.clone(), "/api/v1/catalog/episodes", Some(TestAuth::User)).await;
    assert_eq!(episodes_status, StatusCode::OK);
    assert_eq!(episodes["episodes"].as_array().unwrap().len(), 2);
    assert!(episodes["episodes"].as_array().unwrap().iter().any(|episode| {
        episode["id"] == first_episode_id && episode["season_number"] == json!(1)
    }));
    assert!(episodes["episodes"].as_array().unwrap().iter().any(|episode| {
        episode["id"] == second_episode_id && episode["season_number"] == json!(2)
    }));

    let (search_status, search) =
        get_json(app, "/api/v1/catalog/search?q=trailer", Some(TestAuth::User)).await;
    assert_eq!(search_status, StatusCode::OK);
    assert!(json_array_contains_id(
        &search["episodes"],
        &first_episode_id
    ));
    assert!(json_array_contains_id(
        &search["episodes"],
        &second_episode_id
    ));
}

#[tokio::test]
/// Verifies that podcast read and resume apis are user scoped and visible only.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn podcast_read_and_resume_apis_are_user_scoped_and_visible_only() {
    let Some(state) = test_state().await else {
        return;
    };

    let imported = state
        .repository()
        .import_catalog_file(podcast_import_request(
            "/dropbox/read-podcast-1.mp3",
            "read-podcast-hash-1",
            "Read Podcast",
            "Read Episode",
            Some(7),
        ))
        .await
        .unwrap();
    let podcast_id = imported.podcast.as_ref().unwrap().id;
    let episode_id = imported.episode.as_ref().unwrap().id;

    let app = router(state);

    let (podcast_status, podcast_body) = get_json(
        app.clone(),
        &format!("/api/v1/catalog/podcasts/{podcast_id}"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(podcast_status, StatusCode::OK);
    assert_eq!(podcast_body["podcast"]["title"], "Read Podcast");

    let (episodes_status, episodes_body) = get_json(
        app.clone(),
        &format!("/api/v1/catalog/podcasts/{podcast_id}/episodes"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(episodes_status, StatusCode::OK);
    assert_eq!(episodes_body["page"]["sort"], "podcast_position");
    assert_eq!(episodes_body["episodes"][0]["id"], json!(episode_id));
    assert_eq!(episodes_body["episodes"][0]["title"], "Read Episode");

    let (episode_status, episode_body) = get_json(
        app.clone(),
        &format!("/api/v1/catalog/episodes/{episode_id}"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(episode_status, StatusCode::OK);
    assert_eq!(episode_body["podcast"]["id"], json!(podcast_id));
    assert_eq!(episode_body["episode"]["title"], "Read Episode");
    assert!(episode_body["resume"].is_null());

    let (empty_resume_status, empty_resume_body) = get_json(
        app.clone(),
        &format!("/api/v1/catalog/episodes/{episode_id}/resume"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(empty_resume_status, StatusCode::OK);
    assert!(empty_resume_body["resume"].is_null());

    let (write_status, written) = request_json(
        app.clone(),
        "PUT",
        &format!("/api/v1/catalog/episodes/{episode_id}/resume"),
        json!({
            "position_seconds": 120,
            "duration_seconds": 900,
            "completed": false
        }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(write_status, StatusCode::OK);
    assert_eq!(written["progress"]["item_type"], "episode");
    assert_eq!(written["progress"]["item_id"], json!(episode_id));
    assert_eq!(written["progress"]["position_seconds"], 120);
    assert_eq!(written["history_event"]["item_type"], "episode");

    let (resume_status, resume_body) = get_json(
        app.clone(),
        &format!("/api/v1/catalog/episodes/{episode_id}/resume"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(resume_status, StatusCode::OK);
    assert_eq!(resume_body["resume"]["position_seconds"], 120);

    let (admin_resume_status, admin_resume_body) = get_json(
        app.clone(),
        &format!("/api/v1/catalog/episodes/{episode_id}/resume"),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(admin_resume_status, StatusCode::OK);
    assert!(admin_resume_body["resume"].is_null());

    let missing_id = Uuid::new_v4();
    let (missing_status, missing_body) = get_json(
        app,
        &format!("/api/v1/catalog/episodes/{missing_id}"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(missing_status, StatusCode::NOT_FOUND);
    assert_eq!(missing_body["code"], "not_found");
}

#[tokio::test]
/// Verifies that catalog search indexes compilation album and track artists separately.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn catalog_search_indexes_compilation_album_and_track_artists_separately() {
    let Some(state) = test_state().await else {
        return;
    };

    state
        .repository()
        .import_catalog_file(music_import_request_with_artists(
            "/dropbox/search-compilation-guest.flac",
            "search-hash-compilation-guest",
            "Various Artists",
            "Guest Vocalist",
            "Shared Stage",
            "Spotlight",
            Some(1),
        ))
        .await
        .unwrap();

    let app = router(state);

    let (album_artist_status, album_artist_body) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=various%20artists",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(album_artist_status, StatusCode::OK);
    let album_artist_names = album_artist_body["artists"]
        .as_array()
        .unwrap()
        .iter()
        .map(|artist| artist["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    let album_artist_album_titles = album_artist_body["albums"]
        .as_array()
        .unwrap()
        .iter()
        .map(|album| album["title"].as_str().unwrap())
        .collect::<Vec<_>>();
    let album_artist_track_titles = album_artist_body["tracks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|track| track["title"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(album_artist_names.contains(&"Various Artists"));
    assert!(album_artist_album_titles.contains(&"Shared Stage"));
    assert!(album_artist_track_titles.contains(&"Spotlight"));

    let (track_artist_status, track_artist_body) = get_json(
        app,
        "/api/v1/catalog/search?q=guest%20vocalist",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(track_artist_status, StatusCode::OK);
    let track_artist_names = track_artist_body["artists"]
        .as_array()
        .unwrap()
        .iter()
        .map(|artist| artist["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    let track_artist_album_titles = track_artist_body["albums"]
        .as_array()
        .unwrap()
        .iter()
        .map(|album| album["title"].as_str().unwrap())
        .collect::<Vec<_>>();
    let track_artist_track_titles = track_artist_body["tracks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|track| track["title"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert!(track_artist_names.contains(&"Guest Vocalist"));
    assert!(!track_artist_album_titles.contains(&"Shared Stage"));
    assert!(track_artist_track_titles.contains(&"Spotlight"));
}

#[tokio::test]
/// Verifies that catalog search returns grouped normalized ranked visible results.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn catalog_search_returns_grouped_normalized_ranked_visible_results() {
    let Some(state) = test_state().await else {
        return;
    };

    state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/search-love.flac",
            "search-hash-love-1",
            "Ranking Artist",
            "Ranking Album",
            "Love",
            Some(1),
        ))
        .await
        .unwrap();
    state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/search-love-song.flac",
            "search-hash-love-2",
            "Ranking Artist",
            "Ranking Album",
            "Love Song",
            Some(2),
        ))
        .await
        .unwrap();
    state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/search-endless-love.flac",
            "search-hash-love-3",
            "Ranking Artist",
            "Ranking Album",
            "Endless Love",
            Some(3),
        ))
        .await
        .unwrap();
    state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/search-beatles.flac",
            "search-hash-beatles-1",
            "The Béatles",
            "Abbey Road",
            "Come Together",
            Some(1),
        ))
        .await
        .unwrap();
    state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/search-beyonce.flac",
            "search-hash-beyonce-1",
            "Beyoncé",
            "Renaissance",
            "Cuff It",
            Some(1),
        ))
        .await
        .unwrap();
    state
        .repository()
        .import_catalog_file(podcast_import_request(
            "/dropbox/search-daily-show.mp3",
            "search-hash-podcast-1",
            "The Daily-Show",
            "Headlines",
            Some(1),
        ))
        .await
        .unwrap();

    let duplicate = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/search-hidden-duplicate.flac",
            "search-hash-love-1",
            "Hidden Artist",
            "Hidden Album",
            "Hidden Love",
            Some(1),
        ))
        .await
        .unwrap();
    assert!(matches!(
        duplicate.decision,
        CatalogImportDecision::QuarantinedDuplicate
    ));
    let unstable = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/search-still-grouping.flac",
            "search-hash-unstable-1",
            "Still Grouping",
            "",
            "Grouping Love",
            Some(1),
        ))
        .await
        .unwrap();
    assert!(matches!(
        unstable.decision,
        CatalogImportDecision::QuarantinedUnstableGrouping
    ));

    let app = router(state);

    let (unauth_status, unauth_body) =
        get_json(app.clone(), "/api/v1/catalog/search?q=love", None).await;
    assert_eq!(unauth_status, StatusCode::UNAUTHORIZED);
    assert_eq!(unauth_body["code"], "unauthorized");

    let (empty_status, empty_body) =
        get_json(app.clone(), "/api/v1/catalog/search?q=the", Some(TestAuth::User)).await;
    assert_eq!(empty_status, StatusCode::BAD_REQUEST);
    assert_eq!(empty_body["code"], "bad_request");

    let (bad_limit_status, bad_limit_body) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=love&limit=0",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(bad_limit_status, StatusCode::BAD_REQUEST);
    assert_eq!(bad_limit_body["code"], "bad_request");

    let (rank_status, rank_body) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=love&limit=10",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(rank_status, StatusCode::OK);
    assert_eq!(rank_body["normalized_query"], "love");
    let track_titles = rank_body["tracks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|track| track["title"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(track_titles, vec!["Love", "Love Song", "Endless Love"]);
    assert!(!track_titles.contains(&"Hidden Love"));
    assert!(!track_titles.contains(&"Grouping Love"));

    let (article_status, article_body) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=BEATLES",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(article_status, StatusCode::OK);
    assert_eq!(article_body["artists"][0]["name"], "The Béatles");

    let (diacritic_status, diacritic_body) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=beyonce",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(diacritic_status, StatusCode::OK);
    assert_eq!(diacritic_body["artists"][0]["name"], "Beyoncé");

    let (podcast_status, podcast_body) = get_json(
        app,
        "/api/v1/catalog/search?q=daily%20show",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(podcast_status, StatusCode::OK);
    assert_eq!(podcast_body["podcasts"][0]["title"], "The Daily-Show");
    assert_eq!(podcast_body["episodes"][0]["title"], "Headlines");
}

#[tokio::test]
/// Verifies that catalog search applies year genre format and media type filters.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn catalog_search_applies_year_genre_format_and_media_type_filters() {
    let Some(state) = test_state().await else {
        return;
    };

    state
        .repository()
        .import_catalog_file(with_genre(
            with_music_release_year(
                music_import_request(
                    "/dropbox/filter-rock.flac",
                    "filter-hash-rock-1",
                    "Filter Rock Artist",
                    "Filter Rock Album",
                    "Filter Rock Song",
                    Some(1),
                ),
                1969,
            ),
            "Rock",
        ))
        .await
        .unwrap();
    state
        .repository()
        .import_catalog_file(with_genre(
            with_music_release_year(
                music_import_request(
                    "/dropbox/filter-jazz.flac",
                    "filter-hash-jazz-1",
                    "Filter Jazz Artist",
                    "Filter Jazz Album",
                    "Filter Jazz Song",
                    Some(1),
                ),
                1971,
            ),
            "Jazz",
        ))
        .await
        .unwrap();
    state
        .repository()
        .import_catalog_file(with_genre(
            podcast_import_request(
                "/dropbox/filter-podcast.mp3",
                "filter-hash-podcast-1",
                "Filter Podcast",
                "Filter Headlines",
                Some(1),
            ),
            "Talk",
        ))
        .await
        .unwrap();

    let app = router(state);

    let (year_status, year_body) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=filter&year=1969",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(year_status, StatusCode::OK);
    let year_track_titles = year_body["tracks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|track| track["title"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(year_track_titles, vec!["Filter Rock Song"]);
    assert!(year_body["podcasts"].as_array().unwrap().is_empty());
    assert!(year_body["episodes"].as_array().unwrap().is_empty());

    let (genre_status, genre_body) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=filter&genre=rock",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(genre_status, StatusCode::OK);
    let genre_track_titles = genre_body["tracks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|track| track["title"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(genre_track_titles, vec!["Filter Rock Song"]);

    let (format_status, format_body) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=filter&format=mp3",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(format_status, StatusCode::OK);
    assert!(format_body["tracks"].as_array().unwrap().is_empty());
    assert_eq!(format_body["podcasts"][0]["title"], "Filter Podcast");
    assert_eq!(format_body["episodes"][0]["title"], "Filter Headlines");

    let (media_type_status, media_type_body) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=filter&media_type=podcast&genre=talk",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(media_type_status, StatusCode::OK);
    assert!(media_type_body["tracks"].as_array().unwrap().is_empty());
    assert_eq!(media_type_body["podcasts"][0]["title"], "Filter Podcast");
    assert_eq!(media_type_body["episodes"][0]["title"], "Filter Headlines");

    let (bad_media_type_status, bad_media_type_body) = get_json(
        app,
        "/api/v1/catalog/search?q=filter&media_type=video",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(bad_media_type_status, StatusCode::BAD_REQUEST);
    assert_eq!(bad_media_type_body["code"], "bad_request");
}

#[tokio::test]
/// Verifies that catalog search backfills legacy media genre and format filters.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn catalog_search_backfills_legacy_media_genre_and_format_filters() {
    let Some(state) = test_state().await else {
        return;
    };

    let source_path = "/dropbox/legacy-filter.m4a";
    state
        .repository()
        .import_catalog_file(with_probe_format(
            with_genre(
                music_import_request(
                    source_path,
                    "legacy-filter-hash-1",
                    "Legacy Filter Artist",
                    "Legacy Filter Album",
                    "Legacy Filter Song",
                    Some(1),
                ),
                "Synth-Pop / New Wave",
            ),
            "audio/mp4",
            "mov,mp4,m4a,3gp,3g2,mj2",
            "AAC-LC",
        ))
        .await
        .unwrap();

    sqlx::query(
        r#"
        UPDATE media_files
        SET genres = '{}'::text[],
            format_keys = ARRAY[
              lower(mime_type),
              lower(container),
              lower(audio_codec)
            ]::text[]
        WHERE source_path = $1
        "#,
    )
    .bind(source_path)
    .execute(state.repository().pool())
    .await
    .unwrap();

    state
        .repository()
        .backfill_catalog_search_upgrade_data()
        .await
        .unwrap();

    let app = router(state);
    let (genre_status, genre_body) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=legacy%20filter&genre=synth-pop",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(genre_status, StatusCode::OK);
    let genre_track_titles = genre_body["tracks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|track| track["title"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(genre_track_titles, vec!["Legacy Filter Song"]);

    let (mime_status, mime_body) = get_json(
        app.clone(),
        "/api/v1/catalog/search?q=legacy%20filter&format=audio%2Fmp4",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(mime_status, StatusCode::OK);
    let mime_track_titles = mime_body["tracks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|track| track["title"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(mime_track_titles, vec!["Legacy Filter Song"]);

    let (container_status, container_body) = get_json(
        app,
        "/api/v1/catalog/search?q=legacy%20filter&format=mp4",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(container_status, StatusCode::OK);
    let container_track_titles = container_body["tracks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|track| track["title"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(container_track_titles, vec!["Legacy Filter Song"]);
}

#[tokio::test]
/// Verifies that catalog browse excludes published source paths with active quarantine.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn catalog_browse_excludes_published_source_paths_with_active_quarantine() {
    let Some(state) = test_state().await else {
        return;
    };

    let music_source_path = "/dropbox/requarantine-track.flac";
    let published_music = state
        .repository()
        .import_catalog_file(music_import_request(
            music_source_path,
            "requarantine-music-hash-1",
            "Quarantine Visible Artist",
            "Quarantine Visible Album",
            "Quarantine Visible Track",
            Some(1),
        ))
        .await
        .unwrap();
    assert!(matches!(
        published_music.decision,
        CatalogImportDecision::Published
    ));

    let podcast_source_path = "/dropbox/requarantine-episode.mp3";
    let published_podcast = state
        .repository()
        .import_catalog_file(podcast_import_request(
            podcast_source_path,
            "requarantine-podcast-hash-1",
            "Quarantine Visible Podcast",
            "Quarantine Visible Episode",
            Some(1),
        ))
        .await
        .unwrap();
    assert!(matches!(
        published_podcast.decision,
        CatalogImportDecision::Published
    ));

    let requarantined_music = state
        .repository()
        .import_catalog_file(music_import_request(
            music_source_path,
            "requarantine-music-hash-2",
            "Quarantine Visible Artist",
            "",
            "Quarantine Visible Track",
            Some(1),
        ))
        .await
        .unwrap();
    assert!(matches!(
        requarantined_music.decision,
        CatalogImportDecision::QuarantinedUnstableGrouping
    ));
    assert_eq!(
        requarantined_music.media_file.id,
        published_music.media_file.id
    );

    let requarantined_podcast = state
        .repository()
        .import_catalog_file(podcast_import_request(
            podcast_source_path,
            "requarantine-podcast-hash-2",
            "",
            "Quarantine Visible Episode",
            Some(1),
        ))
        .await
        .unwrap();
    assert!(matches!(
        requarantined_podcast.decision,
        CatalogImportDecision::QuarantinedUnstableGrouping
    ));
    assert_eq!(
        requarantined_podcast.media_file.id,
        published_podcast.media_file.id
    );

    let app = router(state);

    let (artists_status, artists) =
        get_json(app.clone(), "/api/v1/catalog/artists", Some(TestAuth::User)).await;
    assert_eq!(artists_status, StatusCode::OK);
    assert!(artists["artists"].as_array().unwrap().is_empty());

    let (albums_status, albums) =
        get_json(app.clone(), "/api/v1/catalog/albums", Some(TestAuth::User)).await;
    assert_eq!(albums_status, StatusCode::OK);
    assert!(albums["albums"].as_array().unwrap().is_empty());

    let (tracks_status, tracks) =
        get_json(app.clone(), "/api/v1/catalog/tracks", Some(TestAuth::User)).await;
    assert_eq!(tracks_status, StatusCode::OK);
    assert!(tracks["tracks"].as_array().unwrap().is_empty());

    let (podcasts_status, podcasts) =
        get_json(app.clone(), "/api/v1/catalog/podcasts", Some(TestAuth::User)).await;
    assert_eq!(podcasts_status, StatusCode::OK);
    assert!(podcasts["podcasts"].as_array().unwrap().is_empty());

    let (episodes_status, episodes) =
        get_json(app.clone(), "/api/v1/catalog/episodes", Some(TestAuth::User)).await;
    assert_eq!(episodes_status, StatusCode::OK);
    assert!(episodes["episodes"].as_array().unwrap().is_empty());

    let (search_status, search) = get_json(
        app,
        "/api/v1/catalog/search?q=quarantine%20visible",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(search_status, StatusCode::OK);
    assert!(search["artists"].as_array().unwrap().is_empty());
    assert!(search["albums"].as_array().unwrap().is_empty());
    assert!(search["tracks"].as_array().unwrap().is_empty());
    assert!(search["podcasts"].as_array().unwrap().is_empty());
    assert!(search["episodes"].as_array().unwrap().is_empty());
}

#[tokio::test]
/// Verifies that media original routes require authentication.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn media_original_routes_require_authentication() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state);
    let track_id = Uuid::new_v4();

    let (status, body) =
        get_json(app, &format!("/api/v1/media/track/{track_id}/original"), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "unauthorized");
}

#[tokio::test]
/// Verifies that media original routes hide not found non visible and non published items.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn media_original_routes_hide_not_found_non_visible_and_non_published_items() {
    let Some(state) = test_state().await else {
        return;
    };

    let hidden = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/media-hidden.flac",
            "media-hidden-hash",
            "Media Hidden Artist",
            "Media Hidden Album",
            "Media Hidden Track",
            Some(1),
        ))
        .await
        .unwrap();
    let hidden_track_id = hidden.track.as_ref().unwrap().id;
    sqlx::query(
        r#"
        INSERT INTO quarantine_items (
          id,
          media_file_id,
          source_path,
          reason,
          status,
          retry_count,
          retry_eligible,
          last_import_job_id,
          admin_note,
          created_at,
          updated_at
        )
        VALUES (
          $1,
          $2,
          $3,
          'metadata_failure'::quarantine_reason,
          'open'::quarantine_status,
          0,
          true,
          NULL,
          NULL,
          $4,
          $4
        )
        "#,
    )
    .bind(Uuid::new_v4())
    .bind(hidden.media_file.id)
    .bind(hidden.media_file.source_path.as_str())
    .bind(Utc::now())
    .execute(state.repository().pool())
    .await
    .unwrap();

    let unpublished = state
        .repository()
        .import_catalog_file(music_import_request(
            "/dropbox/media-unpublished.flac",
            "media-unpublished-hash",
            "Media Unpublished Artist",
            "Media Unpublished Album",
            "Media Unpublished Track",
            Some(1),
        ))
        .await
        .unwrap();
    let unpublished_track_id = unpublished.track.as_ref().unwrap().id;
    sqlx::query(
        r#"
        UPDATE media_files
        SET status = 'staged'::media_file_status,
            published_at = NULL
        WHERE id = $1
        "#,
    )
    .bind(unpublished.media_file.id)
    .execute(state.repository().pool())
    .await
    .unwrap();

    let app = router(state);
    let missing_id = Uuid::new_v4();
    for track_id in [missing_id, hidden_track_id, unpublished_track_id] {
        let (status, body) = get_json(
            app.clone(),
            &format!("/api/v1/media/track/{track_id}/original"),
            Some(TestAuth::User),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], "not_found");
    }
}

#[tokio::test]
/// Verifies that media original download serves published track original.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn media_original_download_serves_published_track_original() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-media-download-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artist/Album/original-download.flac");
    let body = b"original download bytes";
    fs::write(&managed_path, body).unwrap();

    let Some(state) = test_state_with_roots(library_root, dropbox_root.clone()).await else {
        return;
    };
    let source_path = dropbox_root.join("source-download.flac");
    let imported = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &source_path.to_string_lossy(),
                "media-download-hash",
                "Download Artist",
                "Download Album",
                "Download Track",
                Some(1),
            ),
            &managed_path,
            body.len() as i64,
        ))
        .await
        .unwrap();
    let track_id = imported.track.as_ref().unwrap().id;
    let app = router(state);

    let (status, headers, bytes) = get_bytes(
        app,
        &format!("/api/v1/media/track/{track_id}/original/download"),
        Some(TestAuth::User),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, body);
    assert_eq!(headers[header::ACCEPT_RANGES], "bytes");
    assert_eq!(headers[header::CONTENT_LENGTH], body.len().to_string());
    assert_eq!(headers[header::CONTENT_TYPE], "audio/flac");
    assert_eq!(
        headers[header::CONTENT_DISPOSITION],
        "attachment; filename=\"original-download.flac\""
    );
}

#[tokio::test]
/// Verifies that artwork routes expose metadata and resized images for visible catalog entities.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn artwork_routes_expose_metadata_and_resized_images_for_visible_entities() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-artwork-api-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artwork Artist/Artwork Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artwork Artist/Artwork Album/artwork-track.flac");
    let audio_body = b"artwork route audio bytes";
    fs::write(&managed_path, audio_body).unwrap();
    let cover_path = library_root.join("Artwork Artist/Artwork Album/cover.png");
    let artist_path = library_root.join("Artwork Artist/artist.png");
    let image_body = test_png_bytes();
    fs::write(&cover_path, &image_body).unwrap();
    fs::write(&artist_path, &image_body).unwrap();

    let Some(state) = test_state_with_roots(library_root, dropbox_root.clone()).await else {
        return;
    };
    let source_path = dropbox_root.join("artwork-track.flac");
    let mut request = with_managed_path(
        music_import_request(
            &source_path.to_string_lossy(),
            "artwork-api-hash",
            "Artwork Artist",
            "Artwork Album",
            "Artwork Track",
            Some(1),
        ),
        &managed_path,
        audio_body.len() as i64,
    );
    request.artwork.push(ArtworkAssetDraft {
        entity_type: CatalogEntityType::Album,
        provider: ProviderKind::LocalSidecars,
        artwork_kind: ArtworkKind::Cover,
        source_uri: Some("file:///private/source/cover.png".to_string()),
        file_path: Some(cover_path.to_string_lossy().to_string()),
        mime_type: Some("image/png".to_string()),
        width: Some(1),
        height: Some(1),
        confidence: 0.98,
    });
    request.artwork.push(ArtworkAssetDraft {
        entity_type: CatalogEntityType::Artist,
        provider: ProviderKind::LocalSidecars,
        artwork_kind: ArtworkKind::Artist,
        source_uri: Some("file:///private/source/artist.png".to_string()),
        file_path: Some(artist_path.to_string_lossy().to_string()),
        mime_type: Some("image/png".to_string()),
        width: Some(1),
        height: Some(1),
        confidence: 0.97,
    });
    let imported = state.repository().import_catalog_file(request).await.unwrap();
    let album_id = imported.album.as_ref().unwrap().id;
    let artist_id = imported.artist.as_ref().unwrap().id;
    let app = router(state);

    let (status, body) = get_json(
        app.clone(),
        &format!("/api/v1/catalog/album/{album_id}/artwork?kind=cover"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let artwork = body["artwork"].as_array().unwrap();
    assert_eq!(artwork.len(), 1);
    let cover = &artwork[0];
    assert_eq!(cover["entity_type"], "album");
    assert_eq!(cover["entity_id"], album_id.to_string());
    assert_eq!(cover["artwork_kind"], "cover");
    assert_eq!(cover["mime_type"], "image/png");
    assert_eq!(cover["width"], 1);
    assert_eq!(cover["height"], 1);
    assert!(cover.get("file_path").is_none());
    assert!(cover.get("source_uri").is_none());

    let cover_url = cover["url"].as_str().unwrap();
    let (status, headers, bytes) =
        get_bytes(app.clone(), cover_url, Some(TestAuth::User), &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CONTENT_TYPE], "image/png");
    assert_eq!(bytes, image_body);

    let (status, headers, resized_bytes) = get_bytes(
        app.clone(),
        &format!("{cover_url}?width=2"),
        Some(TestAuth::User),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CONTENT_TYPE], "image/png");
    let resized_image = image::load_from_memory(&resized_bytes).unwrap();
    assert_eq!(resized_image.width(), 2);
    assert_eq!(resized_image.height(), 2);

    let (status, body) = get_json(
        app,
        &format!("/api/v1/catalog/band/{artist_id}/artwork?kind=artist"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let artwork = body["artwork"].as_array().unwrap();
    assert_eq!(artwork.len(), 1);
    assert_eq!(artwork[0]["entity_type"], "artist");
    assert_eq!(artwork[0]["artwork_kind"], "artist");

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that media original download uses track canonical file when multiple published files exist.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn media_original_download_uses_track_canonical_file_when_multiple_published_files_exist() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-media-canonical-track-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let canonical_path = library_root.join("Artist/Album/canonical-track.flac");
    let noncanonical_path = library_root.join("Artist/Album/noncanonical-track.flac");
    let canonical_body = b"canonical track original";
    let noncanonical_body = b"older noncanonical track original";
    fs::write(&canonical_path, canonical_body).unwrap();
    fs::write(&noncanonical_path, noncanonical_body).unwrap();

    let Some(state) = test_state_with_roots(library_root, dropbox_root.clone()).await else {
        return;
    };
    let canonical_source_path = dropbox_root.join("canonical-track-source.flac");
    let canonical = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &canonical_source_path.to_string_lossy(),
                "canonical-track-hash",
                "Canonical Artist",
                "Canonical Album",
                "Canonical Track",
                Some(1),
            ),
            &canonical_path,
            canonical_body.len() as i64,
        ))
        .await
        .unwrap();
    let track_id = canonical.track.as_ref().unwrap().id;

    let noncanonical_source_path = dropbox_root.join("noncanonical-track-source.flac");
    let mut noncanonical_request = with_managed_path(
        music_import_request(
            &noncanonical_source_path.to_string_lossy(),
            "noncanonical-track-hash",
            "Canonical Artist",
            "Canonical Album",
            "Canonical Track",
            Some(1),
        ),
        &noncanonical_path,
        noncanonical_body.len() as i64,
    );
    noncanonical_request.probe.duration_seconds = Some(999);
    let noncanonical = state
        .repository()
        .import_catalog_file(noncanonical_request)
        .await
        .unwrap();
    assert!(matches!(
        noncanonical.decision,
        CatalogImportDecision::Published
    ));
    assert_eq!(noncanonical.track.as_ref().unwrap().id, track_id);

    let older_than_canonical = Utc::now() - ChronoDuration::days(1);
    sqlx::query(
        r#"
        UPDATE media_files
        SET discovered_at = $2,
            published_at = $2
        WHERE id = $1
        "#,
    )
    .bind(noncanonical.media_file.id)
    .bind(older_than_canonical)
    .execute(state.repository().pool())
    .await
    .unwrap();

    let app = router(state);
    let (status, headers, bytes) = get_bytes(
        app,
        &format!("/api/v1/media/track/{track_id}/original/download"),
        Some(TestAuth::User),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, canonical_body);
    assert_eq!(
        headers[header::CONTENT_DISPOSITION],
        "attachment; filename=\"canonical-track.flac\""
    );

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that media original stream supports range requests for episode originals.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn media_original_stream_supports_range_requests_for_episode_originals() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-media-range-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Podcasts/Show")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Podcasts/Show/range-episode.mp3");
    let body = b"abcdefghij";
    fs::write(&managed_path, body).unwrap();

    let Some(state) = test_state_with_roots(library_root, dropbox_root.clone()).await else {
        return;
    };
    let source_path = dropbox_root.join("source-range.mp3");
    let imported = state
        .repository()
        .import_catalog_file(with_managed_path(
            podcast_import_request(
                &source_path.to_string_lossy(),
                "media-range-hash",
                "Range Podcast",
                "Range Episode",
                Some(1),
            ),
            &managed_path,
            body.len() as i64,
        ))
        .await
        .unwrap();
    let episode_id = imported.episode.as_ref().unwrap().id;
    let app = router(state);

    let (status, headers, bytes) = get_bytes(
        app.clone(),
        &format!("/api/v1/media/episode/{episode_id}/original"),
        Some(TestAuth::User),
        &[("range", "bytes=2-5")],
    )
    .await;
    assert_eq!(status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(bytes, b"cdef");
    assert_eq!(headers[header::ACCEPT_RANGES], "bytes");
    assert_eq!(headers[header::CONTENT_LENGTH], "4");
    assert_eq!(headers[header::CONTENT_RANGE], "bytes 2-5/10");
    assert_eq!(headers[header::CONTENT_TYPE], "audio/mpeg");
    assert_eq!(
        headers[header::CONTENT_DISPOSITION],
        "inline; filename=\"range-episode.mp3\""
    );

    let (unsat_status, unsat_headers, _) = get_bytes(
        app,
        &format!("/api/v1/media/episode/{episode_id}/original"),
        Some(TestAuth::User),
        &[("range", "bytes=99-100")],
    )
    .await;
    assert_eq!(unsat_status, StatusCode::RANGE_NOT_SATISFIABLE);
    assert_eq!(unsat_headers[header::ACCEPT_RANGES], "bytes");
    assert_eq!(unsat_headers[header::CONTENT_RANGE], "bytes */10");
}

#[tokio::test]
/// Verifies that media original stream uses episode canonical file when multiple published files exist.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn media_original_stream_uses_episode_canonical_file_when_multiple_published_files_exist() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-media-canonical-episode-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Podcasts/Show")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let canonical_path = library_root.join("Podcasts/Show/canonical-episode.mp3");
    let noncanonical_path = library_root.join("Podcasts/Show/noncanonical-episode.mp3");
    let canonical_body = b"canonical episode original";
    let noncanonical_body = b"older noncanonical episode original";
    fs::write(&canonical_path, canonical_body).unwrap();
    fs::write(&noncanonical_path, noncanonical_body).unwrap();

    let Some(state) = test_state_with_roots(library_root, dropbox_root.clone()).await else {
        return;
    };
    let canonical_source_path = dropbox_root.join("canonical-episode-source.mp3");
    let canonical = state
        .repository()
        .import_catalog_file(with_managed_path(
            podcast_import_request(
                &canonical_source_path.to_string_lossy(),
                "canonical-episode-hash",
                "Canonical Podcast",
                "Canonical Episode",
                Some(1),
            ),
            &canonical_path,
            canonical_body.len() as i64,
        ))
        .await
        .unwrap();
    let episode_id = canonical.episode.as_ref().unwrap().id;

    let noncanonical_source_path = dropbox_root.join("noncanonical-episode-source.mp3");
    let mut noncanonical_request = with_managed_path(
        podcast_import_request(
            &noncanonical_source_path.to_string_lossy(),
            "noncanonical-episode-hash",
            "Canonical Podcast",
            "Canonical Episode",
            Some(1),
        ),
        &noncanonical_path,
        noncanonical_body.len() as i64,
    );
    noncanonical_request.probe.duration_seconds = Some(1_800);
    let noncanonical = state
        .repository()
        .import_catalog_file(noncanonical_request)
        .await
        .unwrap();
    assert!(matches!(
        noncanonical.decision,
        CatalogImportDecision::Published
    ));
    assert_eq!(noncanonical.episode.as_ref().unwrap().id, episode_id);

    let older_than_canonical = Utc::now() - ChronoDuration::days(1);
    sqlx::query(
        r#"
        UPDATE media_files
        SET discovered_at = $2,
            published_at = $2
        WHERE id = $1
        "#,
    )
    .bind(noncanonical.media_file.id)
    .bind(older_than_canonical)
    .execute(state.repository().pool())
    .await
    .unwrap();

    let app = router(state);
    let (status, headers, bytes) = get_bytes(
        app,
        &format!("/api/v1/media/episode/{episode_id}/original"),
        Some(TestAuth::User),
        &[],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, canonical_body);
    assert_eq!(
        headers[header::CONTENT_DISPOSITION],
        "inline; filename=\"canonical-episode.mp3\""
    );

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_targets_requires_authentication() {
    let Some(state) = test_state().await else {
        return;
    };

    let (status, body) = get_json(router(state), "/api/v1/sonos/targets", None).await;

    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "unauthorized");
}

#[tokio::test]
async fn sonos_targets_returns_live_snapshot_for_authenticated_user_and_replaces_targets() {
    let Some(state) = test_state().await else {
        return;
    };

    state.replace_sonos_snapshot(SonosSnapshot::from_targets(
        vec![
            SonosSpeakerSnapshot {
                id: "speaker-kitchen".into(),
                display_name: "Kitchen".into(),
                room_name: Some("Kitchen".into()),
                available: true,
                live: SonosLiveState {
                    volume_percent: Some(18),
                    muted: Some(false),
                    raw_transport_state: Some("idle".into()),
                },
            },
            SonosSpeakerSnapshot {
                id: "speaker-office".into(),
                display_name: "Office".into(),
                room_name: None,
                available: true,
                live: SonosLiveState {
                    volume_percent: None,
                    muted: None,
                    raw_transport_state: None,
                },
            },
        ],
        vec![SonosGroupSnapshot {
            id: "group-downstairs".into(),
            display_name: "Downstairs".into(),
            available: true,
            live: SonosLiveState {
                volume_percent: Some(71),
                muted: Some(true),
                raw_transport_state: Some("playing".into()),
            },
        }],
    ));

    let (status, body) = get_json(
        router(state.clone()),
        "/api/v1/sonos/targets",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let speakers = body["speakers"].as_array().unwrap();
    let groups = body["groups"].as_array().unwrap();
    assert_eq!(speakers.len(), 2);
    assert_eq!(groups.len(), 1);

    let kitchen = speakers
        .iter()
        .find(|speaker| speaker["id"] == "speaker-kitchen")
        .unwrap();
    let office = speakers
        .iter()
        .find(|speaker| speaker["id"] == "speaker-office")
        .unwrap();
    let group = &groups[0];

    assert_eq!(kitchen["room_name"], "Kitchen");
    assert_eq!(kitchen["volume_percent"], json!(18));
    assert_eq!(kitchen["muted"], json!(false));
    assert_eq!(kitchen["transport_state"], "stopped");
    assert!(kitchen.as_object().unwrap().contains_key("room_name"));

    for field in ["room_name", "volume_percent", "muted", "transport_state"] {
        assert!(office.as_object().unwrap().contains_key(field));
        assert_eq!(office[field], Value::Null);
    }

    assert!(!group.as_object().unwrap().contains_key("room_name"));
    assert_eq!(group["volume_percent"], json!(71));
    assert_eq!(group["muted"], json!(true));
    assert_eq!(group["transport_state"], "playing");

    state.replace_sonos_snapshot(SonosSnapshot::empty());
    let (status, body) = get_json(router(state), "/api/v1/sonos/targets", Some(TestAuth::User))
        .await;

    assert_eq!(status, StatusCode::OK);
    assert!(body["speakers"].as_array().unwrap().is_empty());
    assert!(body["groups"].as_array().unwrap().is_empty());
}

#[tokio::test]
async fn sonos_managed_playback_controls_queue_replacement_and_owner_attribution() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-playback-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(library_root.join("Podcasts/Show")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();

    let Some(state) = test_state_with_roots(library_root.clone(), dropbox_root.clone()).await
    else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;

    let track_one_path = library_root.join("Artist/Album/sonos-one.mp3");
    let track_two_path = library_root.join("Artist/Album/sonos-two.mp3");
    let episode_path = library_root.join("Podcasts/Show/sonos-episode.mp3");
    fs::write(&track_one_path, b"one").unwrap();
    fs::write(&track_two_path, b"two").unwrap();
    fs::write(&episode_path, b"episode").unwrap();

    let mut first_request = music_import_request(
        &dropbox_root.join("sonos-one-source.mp3").to_string_lossy(),
        "sonos-managed-one",
        "Sonos Artist",
        "Sonos Album",
        "First Managed Track",
        Some(1),
    );
    first_request.probe.mime_type = Some("audio/mpeg".into());
    first_request.probe.container = Some("mp3".into());
    first_request.probe.audio_codec = Some("mp3".into());
    let track_one = state
        .repository()
        .import_catalog_file(with_managed_path(first_request, &track_one_path, 3))
        .await
        .unwrap()
        .track
        .unwrap()
        .id;

    let mut second_request = music_import_request(
        &dropbox_root.join("sonos-two-source.mp3").to_string_lossy(),
        "sonos-managed-two",
        "Sonos Artist",
        "Sonos Album",
        "Second Managed Track",
        Some(2),
    );
    second_request.probe.mime_type = Some("audio/mpeg".into());
    second_request.probe.container = Some("mp3".into());
    second_request.probe.audio_codec = Some("mp3".into());
    let track_two = state
        .repository()
        .import_catalog_file(with_managed_path(second_request, &track_two_path, 3))
        .await
        .unwrap()
        .track
        .unwrap()
        .id;

    let episode = state
        .repository()
        .import_catalog_file(with_managed_path(
            podcast_import_request(
                &dropbox_root.join("sonos-episode-source.mp3").to_string_lossy(),
                "sonos-managed-episode",
                "Sonos Podcast",
                "Managed Episode",
                Some(1),
            ),
            &episode_path,
            7,
        ))
        .await
        .unwrap()
        .episode
        .unwrap()
        .id;

    let sonos = MockSonosSoapServer::start().await;
    let mut locations = BTreeMap::new();
    locations.insert(
        "speaker-kitchen".to_string(),
        format!("{}/xml/device.xml", sonos.base_url),
    );
    state.replace_sonos_snapshot(SonosSnapshot::from_targets_with_control_locations(
        vec![SonosSpeakerSnapshot {
            id: "speaker-kitchen".into(),
            display_name: "Kitchen".into(),
            room_name: Some("Kitchen".into()),
            available: true,
            live: SonosLiveState {
                volume_percent: Some(18),
                muted: Some(false),
                raw_transport_state: Some("PLAYING".into()),
            },
        }],
        Vec::new(),
        locations,
    ));

    let app = router(state.clone());
    let (track_status, track_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-kitchen/play",
        json!({ "source_type": "track", "source_id": track_one }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(track_status, StatusCode::OK);
    assert_eq!(track_body["target"]["id"], "speaker-kitchen");
    assert_eq!(track_body["session"]["owner_username"], USER_USERNAME);
    assert_eq!(track_body["session"]["queue_index"], 0);
    assert_eq!(track_body["session"]["queue_position"], 1);
    assert_eq!(track_body["session"]["queue_length"], 1);
    assert_eq!(track_body["session"]["current_duration_seconds"], 180);
    assert!(track_body["session"]["next_item"].is_null());

    let (pause_status, pause_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-kitchen/pause",
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(pause_status, StatusCode::OK);
    assert_eq!(pause_body["session"]["status"], "active");

    let (seek_status, seek_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-kitchen/seek",
        json!({ "position_seconds": 42 }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(seek_status, StatusCode::OK);
    assert_eq!(seek_body["session"]["current_position_seconds"], 42);

    let (user_progress_status, user_progress) = get_json(
        app.clone(),
        "/api/v1/me/playback/progress",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(user_progress_status, StatusCode::OK);
    assert!(user_progress["progress"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["item_id"] == json!(track_one)));
    let (admin_progress_status, admin_progress) = get_json(
        app.clone(),
        "/api/v1/me/playback/progress",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(admin_progress_status, StatusCode::OK);
    assert!(!admin_progress["progress"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["item_id"] == json!(track_one)));

    let (playlist_status, playlist) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Sonos Queue", "scope": "personal" }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(playlist_status, StatusCode::CREATED);
    let playlist_id = playlist["id"].as_str().unwrap();
    for (item_type, item_id) in [
        ("track", track_one),
        ("episode", episode),
        ("track", track_two),
    ] {
        let (add_status, _) = request_json(
            app.clone(),
            "POST",
            &format!("/api/v1/playlists/{playlist_id}/items"),
            json!({ "item_type": item_type, "item_id": item_id }),
            Some(TestAuth::User),
        )
        .await;
        assert_eq!(add_status, StatusCode::CREATED);
    }

    let (playlist_play_status, playlist_play) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-kitchen/play",
        json!({ "source_type": "playlist", "source_id": playlist_id }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(playlist_play_status, StatusCode::OK);
    assert_eq!(playlist_play["session"]["queue_length"], 3);
    assert_eq!(playlist_play["session"]["next_item"]["item_type"], "episode");
    assert_eq!(playlist_play["session"]["next_item"]["item_id"], json!(episode));

    let (next_status, next_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-kitchen/next",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(next_status, StatusCode::OK);
    assert_eq!(next_body["session"]["queue_index"], 1);
    assert_eq!(next_body["session"]["queue_position"], 2);
    assert_eq!(next_body["session"]["current_item_type"], "episode");
    assert_eq!(next_body["session"]["next_item"]["item_id"], json!(track_two));

    let (previous_status, previous_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-kitchen/previous",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(previous_status, StatusCode::OK);
    assert_eq!(previous_body["session"]["queue_index"], 0);

    let (episode_play_status, episode_play) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-kitchen/play",
        json!({ "source_type": "episode", "source_id": episode }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(episode_play_status, StatusCode::OK);
    assert_eq!(episode_play["session"]["owner_username"], ADMIN_USERNAME);
    assert_eq!(episode_play["session"]["queue_length"], 1);
    assert_eq!(episode_play["session"]["current_item_type"], "episode");

    let (stop_status, stop_body) = request_json(
        app,
        "POST",
        "/api/v1/sonos/targets/speaker-kitchen/stop",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(stop_status, StatusCode::OK);
    assert_eq!(stop_body["target"]["id"], "speaker-kitchen");
    assert!(stop_body["session"].is_null());

    let requests = sonos.requests();
    assert!(requests
        .iter()
        .any(|request| request.contains("SetAVTransportURI")));
    assert!(requests.iter().any(|request| request.contains("Pause")));
    assert!(requests.iter().any(|request| request.contains("Seek")));
    assert!(requests.iter().any(|request| request.contains("Stop")));

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_skip_writes_outgoing_item_progress_and_history_for_session_owner() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-skip-attribution-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();

    let Some(state) = test_state_with_roots(library_root.clone(), dropbox_root.clone()).await
    else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;

    let first_path = library_root.join("Artist/Album/sonos-skip-one.mp3");
    let second_path = library_root.join("Artist/Album/sonos-skip-two.mp3");
    fs::write(&first_path, b"skip-one").unwrap();
    fs::write(&second_path, b"skip-two").unwrap();
    let first_track = import_sonos_test_track(
        &state,
        &dropbox_root,
        &first_path,
        "sonos-skip-one",
        "Skip One",
        120,
    )
    .await;
    let second_track = import_sonos_test_track(
        &state,
        &dropbox_root,
        &second_path,
        "sonos-skip-two",
        "Skip Two",
        120,
    )
    .await;

    let sonos = MockSonosSoapServer::start().await;
    state.replace_sonos_snapshot(sonos_snapshot_for_speaker(
        "speaker-skip",
        "Skip Room",
        &sonos.base_url,
        "PLAYING",
    ));

    let app = router(state.clone());
    let (playlist_status, playlist) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Sonos Skip Queue", "scope": "personal" }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(playlist_status, StatusCode::CREATED);
    let playlist_id = playlist["id"].as_str().unwrap();
    for track in [first_track, second_track] {
        let (add_status, _) = request_json(
            app.clone(),
            "POST",
            &format!("/api/v1/playlists/{playlist_id}/items"),
            json!({ "item_type": "track", "item_id": track }),
            Some(TestAuth::User),
        )
        .await;
        assert_eq!(add_status, StatusCode::CREATED);
    }

    let (play_status, play_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-skip/play",
        json!({ "source_type": "playlist", "source_id": playlist_id }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(play_status, StatusCode::OK);
    assert_eq!(play_body["session"]["current_item_id"], json!(first_track));
    assert_eq!(play_body["session"]["next_item"]["item_id"], json!(second_track));

    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let (next_status, next_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-skip/next",
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(next_status, StatusCode::OK);
    assert_eq!(next_body["session"]["current_item_id"], json!(second_track));

    let (first_progress_status, first_progress) = get_json(
        app.clone(),
        &format!("/api/v1/me/playback/progress/track/{first_track}"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(first_progress_status, StatusCode::OK);
    assert!(first_progress["position_seconds"].as_u64().unwrap() >= 2);

    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let (previous_status, previous_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-skip/previous",
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(previous_status, StatusCode::OK);
    assert_eq!(previous_body["session"]["current_item_id"], json!(first_track));

    let (second_progress_status, second_progress) = get_json(
        app.clone(),
        &format!("/api/v1/me/playback/progress/track/{second_track}"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(second_progress_status, StatusCode::OK);
    assert!(second_progress["position_seconds"].as_u64().unwrap() >= 2);

    let (admin_progress_status, admin_progress) = get_json(
        app.clone(),
        "/api/v1/me/playback/progress",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(admin_progress_status, StatusCode::OK);
    assert!(admin_progress["progress"].as_array().unwrap().is_empty());

    let (history_status, history) = get_json(
        app,
        "/api/v1/me/playback/history?limit=20",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(history_status, StatusCode::OK);
    let history_items = history["history"].as_array().unwrap();
    assert!(history_items.iter().any(|event| {
        event["item_id"] == json!(first_track)
            && event["position_seconds"].as_u64().unwrap_or_default() >= 2
    }));
    assert!(history_items.iter().any(|event| {
        event["item_id"] == json!(second_track)
            && event["position_seconds"].as_u64().unwrap_or_default() >= 2
    }));

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_failed_skip_handoff_does_not_write_outgoing_final_history() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-skip-failed-handoff-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();

    let Some(state) = test_state_with_roots(library_root.clone(), dropbox_root.clone()).await
    else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;

    let first_path = library_root.join("Artist/Album/sonos-skip-fail-one.mp3");
    let second_path = library_root.join("Artist/Album/sonos-skip-fail-two.mp3");
    fs::write(&first_path, b"skip-fail-one").unwrap();
    fs::write(&second_path, b"skip-fail-two").unwrap();
    let first_track = import_sonos_test_track(
        &state,
        &dropbox_root,
        &first_path,
        "sonos-skip-fail-one",
        "Skip Fail One",
        120,
    )
    .await;
    let second_track = import_sonos_test_track(
        &state,
        &dropbox_root,
        &second_path,
        "sonos-skip-fail-two",
        "Skip Fail Two",
        120,
    )
    .await;

    let sonos = MockSonosSoapServer::start().await;
    state.replace_sonos_snapshot(sonos_snapshot_for_speaker(
        "speaker-skip-fail",
        "Skip Fail Room",
        &sonos.base_url,
        "PLAYING",
    ));

    let app = router(state.clone());
    let (playlist_status, playlist) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Sonos Failed Skip Queue", "scope": "personal" }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(playlist_status, StatusCode::CREATED);
    let playlist_id = playlist["id"].as_str().unwrap();
    for track in [first_track, second_track] {
        let (add_status, _) = request_json(
            app.clone(),
            "POST",
            &format!("/api/v1/playlists/{playlist_id}/items"),
            json!({ "item_type": "track", "item_id": track }),
            Some(TestAuth::User),
        )
        .await;
        assert_eq!(add_status, StatusCode::CREATED);
    }

    let (play_status, _) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-skip-fail/play",
        json!({ "source_type": "playlist", "source_id": playlist_id }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(play_status, StatusCode::OK);

    let (_, history_before) = get_json(
        app.clone(),
        "/api/v1/me/playback/history?limit=20",
        Some(TestAuth::User),
    )
    .await;
    let first_history_before = history_before["history"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["item_id"] == json!(first_track))
        .count();

    sonos.fail_next_action("SetAVTransportURI");
    tokio::time::sleep(Duration::from_millis(2_100)).await;
    let (next_status, next_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-skip-fail/next",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(next_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(next_body["details"]["reason"], "target_unreachable");

    let summary = state
        .sonos_managed_sessions()
        .session_summary("speaker-skip-fail", Instant::now())
        .unwrap();
    assert_eq!(summary.current_item_id, first_track);

    let (history_status, history_after) = get_json(
        app,
        "/api/v1/me/playback/history?limit=20",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(history_status, StatusCode::OK);
    let history_items = history_after["history"].as_array().unwrap();
    let first_history_after = history_items
        .iter()
        .filter(|event| event["item_id"] == json!(first_track))
        .count();
    assert_eq!(first_history_after, first_history_before);
    assert!(!history_items.iter().any(|event| {
        event["item_id"] == json!(first_track)
            && event["position_seconds"].as_u64().unwrap_or_default() >= 2
    }));
    assert!(!history_items
        .iter()
        .any(|event| event["item_id"] == json!(second_track)));

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_pause_freezes_progress_across_active_session_ticks() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-pause-freeze-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();

    let Some(state) = test_state_with_roots(library_root.clone(), dropbox_root.clone()).await else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;
    let track_path = library_root.join("Artist/Album/sonos-paused.mp3");
    fs::write(&track_path, b"paused").unwrap();
    let track_id = import_sonos_test_track(
        &state,
        &dropbox_root,
        &track_path,
        "sonos-paused-track",
        "Paused Track",
        90,
    )
    .await;

    let sonos = MockSonosSoapServer::start().await;
    state.replace_sonos_snapshot(sonos_snapshot_for_speaker(
        "speaker-pause",
        "Pause Room",
        &sonos.base_url,
        "PLAYING",
    ));

    let app = router(state.clone());
    let (play_status, _) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-pause/play",
        json!({ "source_type": "track", "source_id": track_id }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(play_status, StatusCode::OK);

    let (seek_status, _) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-pause/seek",
        json!({ "position_seconds": 9 }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(seek_status, StatusCode::OK);

    let (pause_status, pause_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-pause/pause",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(pause_status, StatusCode::OK);
    assert_eq!(pause_body["session"]["current_position_seconds"], 9);

    tokio::time::sleep(Duration::from_millis(2_200)).await;
    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(50))
        .await;

    let (resume_status, resume_body) = request_json(
        app,
        "POST",
        "/api/v1/sonos/targets/speaker-pause/resume",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(resume_status, StatusCode::OK);
    assert_eq!(resume_body["session"]["current_position_seconds"], 9);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_reconnect_window_reports_countdown_and_expires_session() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-reconnect-expiry-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();

    let Some(state) = test_state_with_roots(library_root.clone(), dropbox_root.clone()).await
    else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;
    let track_path = library_root.join("Artist/Album/sonos-reconnect-expiry.mp3");
    fs::write(&track_path, b"reconnect").unwrap();
    let track_id = import_sonos_test_track(
        &state,
        &dropbox_root,
        &track_path,
        "sonos-reconnect-expiry",
        "Reconnect Expiry Track",
        90,
    )
    .await;

    let sonos = MockSonosSoapServer::start().await;
    state.replace_sonos_snapshot(sonos_snapshot_for_speaker(
        "speaker-reconnect",
        "Reconnect Room",
        &sonos.base_url,
        "PLAYING",
    ));

    let app = router(state.clone());
    let (play_status, _) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-reconnect/play",
        json!({ "source_type": "track", "source_id": track_id }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(play_status, StatusCode::OK);

    drop(sonos);
    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(25))
        .await;
    let transient_summary = state
        .sonos_managed_sessions()
        .session_summary("speaker-reconnect", Instant::now())
        .unwrap();
    assert_eq!(transient_summary.status, SonosSessionStatus::Active);
    assert!(transient_summary.reconnect_seconds_remaining.is_none());

    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(25))
        .await;
    let reconnect_summary = state
        .sonos_managed_sessions()
        .session_summary("speaker-reconnect", Instant::now())
        .unwrap();
    assert_eq!(reconnect_summary.status, SonosSessionStatus::Reconnecting);
    let remaining = reconnect_summary.reconnect_seconds_remaining.unwrap();
    assert!((1..=15).contains(&remaining));

    let (resume_status, resume_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-reconnect/resume",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(resume_status, StatusCode::CONFLICT);
    assert_eq!(resume_body["details"]["reason"], "target_reconnecting");

    tokio::time::sleep(Duration::from_secs(16)).await;
    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(25))
        .await;
    assert!(state
        .sonos_managed_sessions()
        .session_summary("speaker-reconnect", Instant::now())
        .is_none());

    let (stop_status, stop_body) = request_json(
        app,
        "POST",
        "/api/v1/sonos/targets/speaker-reconnect/stop",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(stop_status, StatusCode::CONFLICT);
    assert_eq!(stop_body["details"]["reason"], "session_not_managed");

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_reconnect_resume_seeks_to_saved_position() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-reconnect-seek-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();

    let Some(state) = test_state_with_roots(library_root.clone(), dropbox_root.clone()).await
    else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;
    let track_path = library_root.join("Artist/Album/sonos-reconnect-seek.mp3");
    fs::write(&track_path, b"seek").unwrap();
    let track_id = import_sonos_test_track(
        &state,
        &dropbox_root,
        &track_path,
        "sonos-reconnect-seek",
        "Reconnect Seek Track",
        120,
    )
    .await;

    let first_sonos = MockSonosSoapServer::start().await;
    state.replace_sonos_snapshot(sonos_snapshot_for_speaker(
        "speaker-reconnect-seek",
        "Reconnect Seek Room",
        &first_sonos.base_url,
        "PLAYING",
    ));

    let app = router(state.clone());
    let (play_status, _) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-reconnect-seek/play",
        json!({ "source_type": "track", "source_id": track_id }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(play_status, StatusCode::OK);
    let (seek_status, _) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-reconnect-seek/seek",
        json!({ "position_seconds": 12 }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(seek_status, StatusCode::OK);

    drop(first_sonos);
    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(25))
        .await;
    assert_eq!(
        state
            .sonos_managed_sessions()
            .session_summary("speaker-reconnect-seek", Instant::now())
            .unwrap()
            .status,
        SonosSessionStatus::Active
    );

    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(25))
        .await;
    assert_eq!(
        state
            .sonos_managed_sessions()
            .session_summary("speaker-reconnect-seek", Instant::now())
            .unwrap()
            .status,
        SonosSessionStatus::Reconnecting
    );

    let second_sonos = MockSonosSoapServer::start().await;
    state.replace_sonos_snapshot(sonos_snapshot_for_speaker(
        "speaker-reconnect-seek",
        "Reconnect Seek Room",
        &second_sonos.base_url,
        "PLAYING",
    ));
    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(100))
        .await;

    let active_summary = state
        .sonos_managed_sessions()
        .session_summary("speaker-reconnect-seek", Instant::now())
        .unwrap();
    assert_eq!(active_summary.status, SonosSessionStatus::Active);
    assert_eq!(active_summary.current_position_seconds, 12);
    assert!(second_sonos
        .raw_requests()
        .iter()
        .any(|request| {
            request.contains("Seek") && request.contains("<Target>00:00:12</Target>")
        }));

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_stale_reconnect_resume_does_not_replace_new_play_after_stop() {
    sonos_stale_reconnect_resume_does_not_replace_new_play_after_stop_for_blocked_action(
        "SetAVTransportURI",
        "set-uri",
    )
    .await;
}

#[tokio::test]
async fn sonos_stale_reconnect_play_does_not_restart_after_stop() {
    sonos_stale_reconnect_resume_does_not_replace_new_play_after_stop_for_blocked_action(
        "Play",
        "play",
    )
    .await;
}

async fn sonos_stale_reconnect_resume_does_not_replace_new_play_after_stop_for_blocked_action(
    blocked_action: &'static str,
    suffix: &str,
) {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-stale-reconnect-{suffix}-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();

    let Some(state) = test_state_with_roots(library_root.clone(), dropbox_root.clone()).await
    else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;

    let old_body = b"stale reconnect old";
    let new_body = b"stale reconnect new";
    let old_path = library_root.join(format!("Artist/Album/sonos-stale-old-{suffix}.mp3"));
    let new_path = library_root.join(format!("Artist/Album/sonos-stale-new-{suffix}.mp3"));
    fs::write(&old_path, old_body).unwrap();
    fs::write(&new_path, new_body).unwrap();
    let old_track = import_sonos_test_track(
        &state,
        &dropbox_root,
        &old_path,
        &format!("sonos-stale-old-{suffix}"),
        "Stale Old",
        120,
    )
    .await;
    let new_track = import_sonos_test_track(
        &state,
        &dropbox_root,
        &new_path,
        &format!("sonos-stale-new-{suffix}"),
        "Stale New",
        120,
    )
    .await;

    let target_id = format!("speaker-stale-reconnect-{suffix}");
    let play_uri = format!("/api/v1/sonos/targets/{target_id}/play");
    let stop_uri = format!("/api/v1/sonos/targets/{target_id}/stop");
    let first_sonos = MockSonosSoapServer::start().await;
    state.replace_sonos_snapshot(sonos_snapshot_for_speaker(
        &target_id,
        "Stale Reconnect Room",
        &first_sonos.base_url,
        "PLAYING",
    ));

    let app = router(state.clone());
    let (old_play_status, _) = request_json(
        app.clone(),
        "POST",
        &play_uri,
        json!({ "source_type": "track", "source_id": old_track }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(old_play_status, StatusCode::OK);

    drop(first_sonos);
    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(25))
        .await;
    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(25))
        .await;
    assert_eq!(
        state
            .sonos_managed_sessions()
            .session_summary(&target_id, Instant::now())
            .unwrap()
            .status,
        SonosSessionStatus::Reconnecting
    );

    let replacement_sonos = MockSonosSoapServer::start().await;
    replacement_sonos.block_next_action(blocked_action);
    state.replace_sonos_snapshot(sonos_snapshot_for_speaker(
        &target_id,
        "Stale Reconnect Room",
        &replacement_sonos.base_url,
        "PLAYING",
    ));

    let reconcile_state = state.clone();
    let reconcile = tokio::spawn(async move {
        harmonixia_server::sonos::reconcile_active_sessions(
            &reconcile_state,
            Duration::from_secs(5),
        )
        .await;
    });
    tokio::time::timeout(
        Duration::from_secs(2),
        replacement_sonos.wait_for_blocked_action(),
    )
    .await
    .unwrap_or_else(|_| panic!("reconnect resume should block on {blocked_action}"));

    let stop_app = app.clone();
    let stop_uri_for_task = stop_uri.clone();
    let stop = tokio::spawn(async move {
        request_json(
            stop_app,
            "POST",
            &stop_uri_for_task,
            json!({}),
            Some(TestAuth::User),
        )
        .await
    });
    tokio::task::yield_now().await;

    let play_app = app.clone();
    let play_uri_for_task = play_uri.clone();
    let replacement_play = tokio::spawn(async move {
        request_json(
            play_app,
            "POST",
            &play_uri_for_task,
            json!({ "source_type": "track", "source_id": new_track }),
            Some(TestAuth::Admin),
        )
        .await
    });

    let removed_while_blocked = eventually(Duration::from_millis(250), || {
        let state = state.clone();
        let target_id = target_id.clone();
        async move {
            state
                .sonos_managed_sessions()
                .session_summary(&target_id, Instant::now())
                .is_none()
        }
    })
    .await;
    assert!(
        !removed_while_blocked,
        "stop must wait for the target transport guard before invalidating the stale session"
    );
    assert!(
        !stop.is_finished(),
        "stop should remain pending while the reconnect {blocked_action} request is blocked"
    );
    assert!(
        !replacement_play.is_finished(),
        "replacement play should wait behind stop while the reconnect {blocked_action} request is blocked"
    );

    replacement_sonos.release_blocked_action();

    let (stop_status, _) = stop.await.unwrap();
    assert_eq!(stop_status, StatusCode::OK);

    let (new_play_status, new_play_body) = replacement_play.await.unwrap();
    assert_eq!(new_play_status, StatusCode::OK);
    assert_eq!(new_play_body["session"]["current_item_id"], json!(new_track));
    let new_play_uri = latest_sonos_media_uri(&replacement_sonos.raw_requests());

    reconcile.await.unwrap();

    let summary = state
        .sonos_managed_sessions()
        .session_summary(&target_id, Instant::now())
        .unwrap();
    assert_eq!(summary.status, SonosSessionStatus::Active);
    assert_eq!(summary.current_item_id, new_track);
    let requests = replacement_sonos.requests();
    let last_stop = requests
        .iter()
        .rposition(|request| request.ends_with(" Stop"))
        .expect("stop should be sent after the stale reconnect transport action finishes");
    let last_play = requests
        .iter()
        .rposition(|request| request.ends_with(" Play"))
        .expect("replacement play should send Play");
    assert!(
        last_stop < last_play,
        "replacement Play should be the playback start after Stop"
    );
    let latest_uri = latest_sonos_media_uri(&replacement_sonos.raw_requests());
    assert_eq!(latest_uri, new_play_uri);
    let (media_status, _, media_bytes) = get_bytes(
        app,
        &path_and_query_from_url(&latest_uri),
        None,
        &[],
    )
    .await;
    assert_eq!(media_status, StatusCode::OK);
    assert_eq!(media_bytes, new_body);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_handoff_signed_media_urls_validate_for_play_skip_and_reconnect_resume() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-handoff-signed-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();

    let Some(state) = test_state_with_roots(library_root.clone(), dropbox_root.clone()).await
    else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;

    let first_body = b"handoff first sonos bytes";
    let second_body = b"handoff second sonos bytes";
    let first_path = library_root.join("Artist/Album/sonos-handoff-one.mp3");
    let second_path = library_root.join("Artist/Album/sonos-handoff-two.mp3");
    fs::write(&first_path, first_body).unwrap();
    fs::write(&second_path, second_body).unwrap();
    let first_track = import_sonos_test_track(
        &state,
        &dropbox_root,
        &first_path,
        "sonos-handoff-one",
        "Handoff One",
        120,
    )
    .await;
    let second_track = import_sonos_test_track(
        &state,
        &dropbox_root,
        &second_path,
        "sonos-handoff-two",
        "Handoff Two",
        120,
    )
    .await;

    let first_sonos = MockSonosSoapServer::start().await;
    state.replace_sonos_snapshot(sonos_snapshot_for_speaker(
        "speaker-handoff",
        "Handoff Room",
        &first_sonos.base_url,
        "PLAYING",
    ));

    let app = router(state.clone());
    let (playlist_status, playlist) = request_json(
        app.clone(),
        "POST",
        "/api/v1/playlists",
        json!({ "name": "Sonos Handoff Queue", "scope": "personal" }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(playlist_status, StatusCode::CREATED);
    let playlist_id = playlist["id"].as_str().unwrap();
    for track in [first_track, second_track] {
        let (add_status, _) = request_json(
            app.clone(),
            "POST",
            &format!("/api/v1/playlists/{playlist_id}/items"),
            json!({ "item_type": "track", "item_id": track }),
            Some(TestAuth::User),
        )
        .await;
        assert_eq!(add_status, StatusCode::CREATED);
    }

    let (play_status, _) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-handoff/play",
        json!({ "source_type": "playlist", "source_id": playlist_id }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(play_status, StatusCode::OK);
    let play_uri = latest_sonos_media_uri(&first_sonos.raw_requests());
    let (play_media_status, _, play_bytes) = get_bytes(
        app.clone(),
        &path_and_query_from_url(&play_uri),
        None,
        &[],
    )
    .await;
    assert_eq!(play_media_status, StatusCode::OK);
    assert_eq!(play_bytes, first_body);

    let (next_status, _) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-handoff/next",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(next_status, StatusCode::OK);
    let next_uri = latest_sonos_media_uri(&first_sonos.raw_requests());
    let (next_media_status, _, next_bytes) = get_bytes(
        app.clone(),
        &path_and_query_from_url(&next_uri),
        None,
        &[],
    )
    .await;
    assert_eq!(next_media_status, StatusCode::OK);
    assert_eq!(next_bytes, second_body);
    let (stale_play_status, _, _) = get_bytes(
        app.clone(),
        &path_and_query_from_url(&play_uri),
        None,
        &[],
    )
    .await;
    assert_eq!(stale_play_status, StatusCode::FORBIDDEN);

    drop(first_sonos);
    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(25))
        .await;
    assert_eq!(
        state
            .sonos_managed_sessions()
            .session_summary("speaker-handoff", Instant::now())
            .unwrap()
            .status,
        SonosSessionStatus::Active
    );

    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(25))
        .await;
    assert_eq!(
        state
            .sonos_managed_sessions()
            .session_summary("speaker-handoff", Instant::now())
            .unwrap()
            .status,
        SonosSessionStatus::Reconnecting
    );

    let second_sonos = MockSonosSoapServer::start().await;
    state.replace_sonos_snapshot(sonos_snapshot_for_speaker(
        "speaker-handoff",
        "Handoff Room",
        &second_sonos.base_url,
        "PLAYING",
    ));
    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(100))
        .await;
    let resume_uri = latest_sonos_media_uri(&second_sonos.raw_requests());
    let (resume_media_status, _, resume_bytes) = get_bytes(
        app.clone(),
        &path_and_query_from_url(&resume_uri),
        None,
        &[],
    )
    .await;
    assert_eq!(resume_media_status, StatusCode::OK);
    assert_eq!(resume_bytes, second_body);
    let (stale_next_status, _, _) = get_bytes(
        app,
        &path_and_query_from_url(&next_uri),
        None,
        &[],
    )
    .await;
    assert_eq!(stale_next_status, StatusCode::FORBIDDEN);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_replacement_writes_previous_owner_final_snapshot() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-replacement-final-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();

    let Some(state) = test_state_with_roots(library_root.clone(), dropbox_root.clone()).await
    else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;
    let first_path = library_root.join("Artist/Album/sonos-replace-one.mp3");
    let second_path = library_root.join("Artist/Album/sonos-replace-two.mp3");
    fs::write(&first_path, b"replace-one").unwrap();
    fs::write(&second_path, b"replace-two").unwrap();
    let first_track = import_sonos_test_track(
        &state,
        &dropbox_root,
        &first_path,
        "sonos-replace-one",
        "Replace One",
        120,
    )
    .await;
    let second_track = import_sonos_test_track(
        &state,
        &dropbox_root,
        &second_path,
        "sonos-replace-two",
        "Replace Two",
        120,
    )
    .await;

    let sonos = MockSonosSoapServer::start().await;
    state.replace_sonos_snapshot(sonos_snapshot_for_speaker(
        "speaker-replace",
        "Replace Room",
        &sonos.base_url,
        "PLAYING",
    ));

    let app = router(state.clone());
    let (play_status, _) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-replace/play",
        json!({ "source_type": "track", "source_id": first_track }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(play_status, StatusCode::OK);

    tokio::time::sleep(Duration::from_millis(2_100)).await;
    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(100))
        .await;

    let (replace_status, _) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-replace/play",
        json!({ "source_type": "track", "source_id": second_track }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(replace_status, StatusCode::OK);

    let (progress_status, progress) = get_json(
        app.clone(),
        &format!("/api/v1/me/playback/progress/track/{first_track}"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(progress_status, StatusCode::OK);
    assert!(progress["position_seconds"].as_u64().unwrap() >= 2);

    let (history_status, history) = get_json(
        app,
        "/api/v1/me/playback/history?limit=10",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(history_status, StatusCode::OK);
    assert!(history["history"].as_array().unwrap().iter().any(|event| {
        event["item_id"] == json!(first_track)
            && event["position_seconds"].as_u64().unwrap_or_default() >= 2
    }));

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_control_errors_and_reconnect_window_use_planned_reasons() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-errors-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artist/Album/sonos-error-track.mp3");
    fs::write(&managed_path, b"track").unwrap();

    let Some(state) = test_state_with_roots(library_root.clone(), dropbox_root.clone()).await else {
        return;
    };

    let mut track_request = music_import_request(
        &dropbox_root.join("sonos-error-source.mp3").to_string_lossy(),
        "sonos-error-track",
        "Sonos Artist",
        "Sonos Album",
        "Error Track",
        Some(1),
    );
    track_request.probe.mime_type = Some("audio/mpeg".into());
    track_request.probe.container = Some("mp3".into());
    track_request.probe.audio_codec = Some("mp3".into());
    let track_id = state
        .repository()
        .import_catalog_file(with_managed_path(track_request, &managed_path, 5))
        .await
        .unwrap()
        .track
        .unwrap()
        .id;

    let sonos = MockSonosSoapServer::start().await;
    let mut locations = BTreeMap::new();
    locations.insert(
        "speaker-office".to_string(),
        format!("{}/xml/device.xml", sonos.base_url),
    );
    state.replace_sonos_snapshot(SonosSnapshot::from_targets_with_control_locations(
        vec![SonosSpeakerSnapshot {
            id: "speaker-office".into(),
            display_name: "Office".into(),
            room_name: Some("Office".into()),
            available: true,
            live: SonosLiveState {
                volume_percent: Some(10),
                muted: Some(false),
                raw_transport_state: Some("PLAYING".into()),
            },
        }],
        Vec::new(),
        locations,
    ));

    let app = router(state.clone());
    let (unmanaged_status, unmanaged_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-office/pause",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(unmanaged_status, StatusCode::CONFLICT);
    assert_eq!(unmanaged_body["details"]["reason"], "session_not_managed");

    let (no_base_status, no_base_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-office/play",
        json!({ "source_type": "track", "source_id": track_id }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(no_base_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        no_base_body["details"]["reason"],
        "public_base_url_unusable"
    );

    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;
    let (missing_status, missing_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/missing-speaker/play",
        json!({ "source_type": "track", "source_id": track_id }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(missing_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(missing_body["details"]["reason"], "target_unreachable");

    let (play_status, _) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-office/play",
        json!({ "source_type": "track", "source_id": track_id }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(play_status, StatusCode::OK);

    state.replace_sonos_snapshot(SonosSnapshot::empty());
    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(25)).await;
    let active_summary = state
        .sonos_managed_sessions()
        .session_summary("speaker-office", Instant::now())
        .unwrap();
    assert_eq!(active_summary.status, SonosSessionStatus::Active);

    let (resume_status, resume_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-office/resume",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(resume_status, StatusCode::OK);
    assert_eq!(resume_body["session"]["status"], "active");

    drop(sonos);
    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(25)).await;
    let transient_failed_summary = state
        .sonos_managed_sessions()
        .session_summary("speaker-office", Instant::now())
        .unwrap();
    assert_eq!(
        transient_failed_summary.status,
        SonosSessionStatus::Active
    );
    assert!(transient_failed_summary.reconnect_seconds_remaining.is_none());

    harmonixia_server::sonos::reconcile_active_sessions(&state, Duration::from_millis(25)).await;
    let sustained_failed_summary = state
        .sonos_managed_sessions()
        .session_summary("speaker-office", Instant::now())
        .unwrap();
    assert_eq!(
        sustained_failed_summary.status,
        SonosSessionStatus::Reconnecting
    );
    let reconnect_remaining = sustained_failed_summary
        .reconnect_seconds_remaining
        .unwrap();
    assert!((1..=15).contains(&reconnect_remaining));

    let (reconnecting_resume_status, reconnecting_resume_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-office/resume",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(reconnecting_resume_status, StatusCode::CONFLICT);
    assert_eq!(
        reconnecting_resume_body["details"]["reason"],
        "target_reconnecting"
    );

    let (stop_status, stop_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-office/stop",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(stop_status, StatusCode::OK);
    assert!(stop_body["session"].is_null());

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_play_fails_fast_for_fallback_capacity_and_source_access() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-fallback-errors-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artist/Album/sonos-fallback.flac");
    fs::write(&managed_path, b"flac").unwrap();
    let fake_ffmpeg = root.join("fake-ffmpeg.sh");
    let args_log = root.join("ffmpeg-args.log");
    fake_ffmpeg_script(&fake_ffmpeg, &args_log, None, 0);

    let Some(state) =
        test_state_with_transcode_runtime(library_root.clone(), dropbox_root.clone(), fake_ffmpeg, 0)
            .await
    else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;

    let track_id = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &dropbox_root.join("sonos-fallback-source.flac").to_string_lossy(),
                "sonos-fallback-capacity",
                "Sonos Artist",
                "Sonos Album",
                "Fallback Capacity",
                Some(1),
            ),
            &managed_path,
            4,
        ))
        .await
        .unwrap()
        .track
        .unwrap()
        .id;

    let sonos = MockSonosSoapServer::start().await;
    let mut locations = BTreeMap::new();
    locations.insert(
        "speaker-den".to_string(),
        format!("{}/xml/device.xml", sonos.base_url),
    );
    state.replace_sonos_snapshot(SonosSnapshot::from_targets_with_control_locations(
        vec![SonosSpeakerSnapshot {
            id: "speaker-den".into(),
            display_name: "Den".into(),
            room_name: Some("Den".into()),
            available: true,
            live: SonosLiveState {
                volume_percent: Some(10),
                muted: Some(false),
                raw_transport_state: Some("STOPPED".into()),
            },
        }],
        Vec::new(),
        locations,
    ));

    let app = router(state.clone());
    let (capacity_status, capacity_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/sonos/targets/speaker-den/play",
        json!({ "source_type": "track", "source_id": track_id }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(capacity_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        capacity_body["details"]["reason"],
        "transcode_capacity_exhausted"
    );
    assert!(sonos.requests().is_empty());

    let missing_path = library_root.join("Artist/Album/missing-fallback.flac");
    let missing_track = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &dropbox_root.join("missing-fallback-source.flac").to_string_lossy(),
                "sonos-fallback-missing",
                "Sonos Artist",
                "Sonos Album",
                "Missing Fallback",
                Some(2),
            ),
            &missing_path,
            4,
        ))
        .await
        .unwrap()
        .track
        .unwrap()
        .id;
    state
        .update_system_config(
            &library_root.to_string_lossy(),
            &dropbox_root.to_string_lossy(),
            Some("Podcasts"),
            None,
            Some(1),
            None,
        )
        .await
        .unwrap();

    let (source_status, source_body) = request_json(
        app,
        "POST",
        "/api/v1/sonos/targets/speaker-den/play",
        json!({ "source_type": "track", "source_id": missing_track }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(source_status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        source_body["details"]["reason"],
        "source_incompatible_fallback_failed"
    );

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_signed_original_fetch_uses_sonos_path_without_basic_auth() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-original-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Podcasts/Show")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Podcasts/Show/sonos-original.mp3");
    let body = b"sonos original bytes";
    fs::write(&managed_path, body).unwrap();

    let Some(state) = test_state_with_roots(library_root.clone(), dropbox_root.clone()).await else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test/prefix",
    )
    .await;
    let source_path = dropbox_root.join("sonos-original-source.mp3");
    let imported = state
        .repository()
        .import_catalog_file(with_managed_path(
            podcast_import_request(
                &source_path.to_string_lossy(),
                "sonos-original-hash",
                "Sonos Podcast",
                "Original Episode",
                Some(1),
            ),
            &managed_path,
            body.len() as i64,
        ))
        .await
        .unwrap();
    let episode_id = imported.episode.as_ref().unwrap().id;
    let signed = state
        .register_sonos_media_authorization(sonos_media_request(
            PlaybackItemType::Episode,
            episode_id,
            1,
            1,
            "speaker-1",
        ))
        .await
        .unwrap();
    assert_eq!(signed.claim.delivery_kind, SonosDeliveryKind::Original);
    assert!(signed
        .url
        .contains("/prefix/api/v1/sonos/media/"));
    assert!(!signed.url.contains("/api/v1/media/"));

    let app = router(state);
    let (status, headers, bytes) =
        get_bytes(app.clone(), &path_and_query_from_url(&signed.url), None, &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, body);
    assert_eq!(headers[header::CONTENT_TYPE], "audio/mpeg");

    let (range_status, _, range_bytes) = get_bytes(
        app,
        &path_and_query_from_url(&signed.url),
        None,
        &[("range", "bytes=0-4")],
    )
    .await;
    assert_eq!(range_status, StatusCode::PARTIAL_CONTENT);
    assert_eq!(range_bytes, b"sonos");

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_unsafe_media_uses_only_aac_high_fallback() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-transcode-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artist/Album/sonos-source.flac");
    fs::write(&managed_path, b"source media bytes").unwrap();
    let fake_ffmpeg = root.join("fake-ffmpeg.sh");
    let args_log = root.join("ffmpeg-args.log");
    fake_ffmpeg_script(&fake_ffmpeg, &args_log, None, 0);

    let Some(state) = test_state_with_transcode_runtime(
        library_root.clone(),
        dropbox_root.clone(),
        fake_ffmpeg,
        2,
    )
    .await
    else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;
    let source_path = dropbox_root.join("sonos-source.flac");
    let imported = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &source_path.to_string_lossy(),
                "sonos-transcode-hash",
                "Sonos Artist",
                "Sonos Album",
                "Fallback Track",
                Some(1),
            ),
            &managed_path,
            18,
        ))
        .await
        .unwrap();
    let track_id = imported.track.as_ref().unwrap().id;
    let signed = state
        .register_sonos_media_authorization(sonos_media_request(
            PlaybackItemType::Track,
            track_id,
            1,
            1,
            "speaker-1",
        ))
        .await
        .unwrap();
    assert_eq!(
        signed.claim.delivery_kind,
        SonosDeliveryKind::TranscodeAacHigh
    );

    let (status, headers, bytes) =
        get_bytes(router(state), &path_and_query_from_url(&signed.url), None, &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, b"fake-aac-output");
    assert_eq!(headers[header::CONTENT_TYPE], "audio/aac");

    let args = fs::read_to_string(args_log).unwrap();
    assert!(args.lines().any(|arg| arg == "256k"));
    assert!(!args.lines().any(|arg| arg == "64k"));
    assert!(!args.lines().any(|arg| arg == "128k"));

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_transcode_slot_exhaustion_returns_planned_reason() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-saturation-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artist/Album/sonos-saturation.flac");
    fs::write(&managed_path, b"source media bytes").unwrap();
    let fake_ffmpeg = root.join("fake-ffmpeg.sh");
    let args_log = root.join("ffmpeg-args.log");
    let started_marker = root.join("ffmpeg-started");
    fake_ffmpeg_script(&fake_ffmpeg, &args_log, Some(&started_marker), 4);

    let Some(state) = test_state_with_transcode_runtime(
        library_root.clone(),
        dropbox_root.clone(),
        fake_ffmpeg,
        1,
    )
    .await
    else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;
    let source_path = dropbox_root.join("sonos-saturation.flac");
    let imported = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &source_path.to_string_lossy(),
                "sonos-saturation-hash",
                "Sonos Artist",
                "Sonos Album",
                "Saturation Track",
                Some(1),
            ),
            &managed_path,
            18,
        ))
        .await
        .unwrap();
    let track_id = imported.track.as_ref().unwrap().id;
    let signed = state
        .register_sonos_media_authorization(sonos_media_request(
            PlaybackItemType::Track,
            track_id,
            1,
            1,
            "speaker-1",
        ))
        .await
        .unwrap();
    let uri = path_and_query_from_url(&signed.url);
    let app = router(state);
    let first_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_response.status(), StatusCode::OK);
    assert!(eventually(Duration::from_secs(1), || {
        let started_marker = started_marker.clone();
        async move { started_marker.exists() }
    })
    .await);

    let started = Instant::now();
    let (status, _, body) = get_bytes(app, &uri, None, &[]).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(started.elapsed() < Duration::from_secs(2));
    let body = serde_json::from_slice::<Value>(&body).unwrap();
    assert_eq!(body["code"], "service_unavailable");
    assert_eq!(body["details"]["reason"], "transcode_capacity_exhausted");

    drop(first_response);
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_signed_urls_tolerate_expiry_and_invalidate_on_context_changes() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-invalidation-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let first_path = library_root.join("Artist/Album/first.mp3");
    let second_path = library_root.join("Artist/Album/second.mp3");
    fs::write(&first_path, b"first sonos bytes").unwrap();
    fs::write(&second_path, b"second sonos bytes").unwrap();

    let Some(state) = test_state_with_roots(library_root.clone(), dropbox_root.clone()).await else {
        return;
    };
    configure_public_base_url(
        &state,
        &library_root,
        &dropbox_root,
        "https://speaker-lan.example.test",
    )
    .await;
    let first_source = dropbox_root.join("first-source.mp3");
    let first = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &first_source.to_string_lossy(),
                "sonos-first-hash",
                "Sonos Artist",
                "Sonos Album",
                "First Track",
                Some(1),
            ),
            &first_path,
            17,
        ))
        .await
        .unwrap();
    let second_source = dropbox_root.join("second-source.mp3");
    let second = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &second_source.to_string_lossy(),
                "sonos-second-hash",
                "Sonos Artist",
                "Sonos Album",
                "Second Track",
                Some(2),
            ),
            &second_path,
            18,
        ))
        .await
        .unwrap();
    let first_id = first.track.as_ref().unwrap().id;
    let second_id = second.track.as_ref().unwrap().id;

    let old = state
        .register_sonos_media_authorization(sonos_media_request(
            PlaybackItemType::Track,
            first_id,
            1,
            1,
            "speaker-1",
        ))
        .await
        .unwrap();
    let expired = state
        .issue_sonos_signed_media_url_with_exp(Utc::now().timestamp() - 60)
        .unwrap();
    let app = router(state.clone());
    let (expired_status, _, expired_bytes) =
        get_bytes(app.clone(), &path_and_query_from_url(&expired.url), None, &[]).await;
    assert_eq!(expired_status, StatusCode::OK);
    assert_eq!(expired_bytes, b"first sonos bytes");

    state
        .register_sonos_media_authorization(sonos_media_request(
            PlaybackItemType::Track,
            first_id,
            2,
            1,
            "speaker-1",
        ))
        .await
        .unwrap();
    let (session_status, _, _) =
        get_bytes(app.clone(), &path_and_query_from_url(&old.url), None, &[]).await;
    assert_eq!(session_status, StatusCode::FORBIDDEN);

    let first_current = state
        .register_sonos_media_authorization(sonos_media_request(
            PlaybackItemType::Track,
            first_id,
            3,
            1,
            "speaker-1",
        ))
        .await
        .unwrap();
    state
        .register_sonos_media_authorization(sonos_media_request(
            PlaybackItemType::Track,
            second_id,
            3,
            2,
            "speaker-1",
        ))
        .await
        .unwrap();
    let (item_status, _, _) =
        get_bytes(app, &path_and_query_from_url(&first_current.url), None, &[]).await;
    assert_eq!(item_status, StatusCode::FORBIDDEN);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
async fn sonos_signed_urls_are_invalidated_by_recreated_app_state() {
    let Some(mut config) = test_config() else {
        eprintln!(
            "skipping Postgres-backed Sonos restart test; set HARMONIXIA_TEST_DATABASE_URL"
        );
        return;
    };
    let root = std::env::temp_dir().join(format!(
        "harmonixia-sonos-restart-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artist/Album/restart.mp3");
    fs::write(&managed_path, b"restart sonos bytes").unwrap();
    config.library_root = library_root.clone();
    config.dropbox_root = dropbox_root.clone();
    config.public_base_url = Some("https://speaker-lan.example.test".into());

    let state = AppState::connect(config.clone()).await.unwrap();
    seed_test_accounts(&state).await;
    let source_path = dropbox_root.join("restart-source.mp3");
    let imported = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &source_path.to_string_lossy(),
                "sonos-restart-hash",
                "Sonos Artist",
                "Sonos Album",
                "Restart Track",
                Some(1),
            ),
            &managed_path,
            19,
        ))
        .await
        .unwrap();
    let track_id = imported.track.as_ref().unwrap().id;
    let signed = state
        .register_sonos_media_authorization(sonos_media_request(
            PlaybackItemType::Track,
            track_id,
            1,
            1,
            "speaker-1",
        ))
        .await
        .unwrap();
    let uri = path_and_query_from_url(&signed.url);
    let app = router(state);
    let (ok_status, _, _) = get_bytes(app, &uri, None, &[]).await;
    assert_eq!(ok_status, StatusCode::OK);

    let restarted = AppState::connect(config).await.unwrap();
    let (restart_status, _, _) = get_bytes(router(restarted), &uri, None, &[]).await;
    assert_eq!(restart_status, StatusCode::FORBIDDEN);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that media direct transcode requires auth and uses selected aac profile.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn media_direct_transcode_requires_auth_and_uses_selected_aac_profile() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-media-transcode-profile-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artist/Album/transcode-source.flac");
    fs::write(&managed_path, b"source media bytes").unwrap();
    let fake_ffmpeg = root.join("fake-ffmpeg.sh");
    let args_log = root.join("ffmpeg-args.log");
    fake_ffmpeg_script(&fake_ffmpeg, &args_log, None, 0);

    let Some(state) = test_state_with_transcode_runtime(
        library_root,
        dropbox_root.clone(),
        fake_ffmpeg,
        2,
    )
    .await
    else {
        return;
    };
    let source_path = dropbox_root.join("source-transcode.flac");
    let imported = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &source_path.to_string_lossy(),
                "media-transcode-profile-hash",
                "Transcode Artist",
                "Transcode Album",
                "Transcode Track",
                Some(1),
            ),
            &managed_path,
            18,
        ))
        .await
        .unwrap();
    let track_id = imported.track.as_ref().unwrap().id;
    let app = router(state);
    let uri = format!("/api/v1/media/track/{track_id}/transcode/standard");

    let (unauth_status, _, unauth_body) = get_bytes(app.clone(), &uri, None, &[]).await;
    assert_eq!(unauth_status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        serde_json::from_slice::<Value>(&unauth_body).unwrap()["code"],
        "unauthorized"
    );
    assert!(!args_log.exists());

    let (status, headers, bytes) =
        get_bytes(app, &uri, Some(TestAuth::User), &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(bytes, b"fake-aac-output");
    assert_eq!(headers[header::CONTENT_TYPE], "audio/aac");
    assert_eq!(
        headers[header::CONTENT_DISPOSITION],
        "inline; filename=\"transcode-source-standard.aac\""
    );

    let args = fs::read_to_string(args_log).unwrap();
    assert!(args
        .lines()
        .any(|arg| arg == managed_path.to_string_lossy().as_ref()));
    assert!(args.lines().any(|arg| arg == "-b:a"));
    assert!(args.lines().any(|arg| arg == "128k"));
    assert!(args.lines().any(|arg| arg == "-f"));
    assert!(args.lines().any(|arg| arg == "adts"));
    assert!(args.lines().any(|arg| arg == "pipe:1"));

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that media direct transcode reports slots and fails fast when saturated.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn media_direct_transcode_reports_slots_and_fails_fast_when_saturated() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-media-transcode-saturation-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artist/Album/saturation-source.flac");
    fs::write(&managed_path, b"source media bytes").unwrap();
    let fake_ffmpeg = root.join("fake-ffmpeg.sh");
    let args_log = root.join("ffmpeg-args.log");
    let started_marker = root.join("ffmpeg-started");
    fake_ffmpeg_script(&fake_ffmpeg, &args_log, Some(&started_marker), 4);

    let Some(state) = test_state_with_transcode_runtime(
        library_root,
        dropbox_root.clone(),
        fake_ffmpeg,
        1,
    )
    .await
    else {
        return;
    };
    let source_path = dropbox_root.join("source-saturation.flac");
    let imported = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &source_path.to_string_lossy(),
                "media-transcode-saturation-hash",
                "Saturation Artist",
                "Saturation Album",
                "Saturation Track",
                Some(1),
            ),
            &managed_path,
            18,
        ))
        .await
        .unwrap();
    let track_id = imported.track.as_ref().unwrap().id;
    let app = router(state);
    let uri = format!("/api/v1/media/track/{track_id}/transcode/high");
    let first_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .header("authorization", auth_header(TestAuth::User))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(first_response.status(), StatusCode::OK);
    assert!(eventually(Duration::from_secs(1), || {
        let started_marker = started_marker.clone();
        async move { started_marker.exists() }
    })
    .await);

    let slots_visible = eventually(Duration::from_secs(1), || {
        let app = app.clone();
        async move {
            let (status, body) =
                get_json(app, "/api/v1/admin/media/transcode-slots", Some(TestAuth::Admin))
                    .await;
            status == StatusCode::OK
                && body["limit"] == json!(1)
                && body["in_use"] == json!(1)
                && body["available"] == json!(0)
        }
    })
    .await;
    assert!(slots_visible);

    let started = Instant::now();
    let (status, _, body) = get_bytes(app, &uri, Some(TestAuth::User), &[]).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "saturated transcode admission should fail without waiting for a slot"
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["code"],
        "service_unavailable"
    );

    drop(first_response);
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that media direct transcode drop releases slot quickly.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn media_direct_transcode_drop_releases_slot_quickly() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-media-transcode-drop-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artist/Album/drop-source.flac");
    fs::write(&managed_path, b"source media bytes").unwrap();
    let fake_ffmpeg = root.join("fake-ffmpeg.sh");
    let args_log = root.join("ffmpeg-args.log");
    let started_marker = root.join("ffmpeg-started");
    fake_ffmpeg_script(&fake_ffmpeg, &args_log, Some(&started_marker), 4);

    let Some(state) = test_state_with_transcode_runtime(
        library_root,
        dropbox_root.clone(),
        fake_ffmpeg,
        1,
    )
    .await
    else {
        return;
    };
    let source_path = dropbox_root.join("source-drop.flac");
    let imported = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &source_path.to_string_lossy(),
                "media-transcode-drop-hash",
                "Drop Artist",
                "Drop Album",
                "Drop Track",
                Some(1),
            ),
            &managed_path,
            18,
        ))
        .await
        .unwrap();
    let track_id = imported.track.as_ref().unwrap().id;
    let app = router(state);
    let uri = format!("/api/v1/media/track/{track_id}/transcode/mobile");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri(&uri)
                .header("authorization", auth_header(TestAuth::User))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(eventually(Duration::from_secs(1), || {
        let started_marker = started_marker.clone();
        async move { started_marker.exists() }
    })
    .await);

    drop(response);

    assert!(
        eventually(Duration::from_secs(1), || {
            let app = app.clone();
            async move {
                let (status, body) =
                    get_json(app, "/api/v1/admin/media/transcode-slots", Some(TestAuth::Admin))
                        .await;
                status == StatusCode::OK
                    && body["limit"] == json!(1)
                    && body["in_use"] == json!(0)
                    && body["available"] == json!(1)
            }
        })
        .await,
        "dropping an unconsumed transcode response should release its slot promptly"
    );

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that media hls manifest and segments require auth and use selected profile.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn media_hls_manifest_and_segments_require_auth_and_use_selected_profile() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-media-hls-profile-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artist/Album/hls-source.flac");
    fs::write(&managed_path, b"source media bytes").unwrap();
    let fake_ffmpeg = root.join("fake-ffmpeg.sh");
    let args_log = root.join("ffmpeg-args.log");
    let started_marker = root.join("ffmpeg-started");
    fake_hls_ffmpeg_script_prompt_manifest(&fake_ffmpeg, &args_log, Some(&started_marker), 2);

    let Some(state) = test_state_with_transcode_runtime(
        library_root,
        dropbox_root.clone(),
        fake_ffmpeg,
        2,
    )
    .await
    else {
        return;
    };
    let source_path = dropbox_root.join("source-hls.flac");
    let imported = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &source_path.to_string_lossy(),
                "media-hls-profile-hash",
                "HLS Artist",
                "HLS Album",
                "HLS Track",
                Some(1),
            ),
            &managed_path,
            18,
        ))
        .await
        .unwrap();
    let track_id = imported.track.as_ref().unwrap().id;
    let app = router(state);
    let manifest_uri = format!("/api/v1/media/track/{track_id}/hls/standard/manifest.m3u8");

    let (unauth_status, _, unauth_body) =
        get_bytes(app.clone(), &manifest_uri, None, &[]).await;
    assert_eq!(unauth_status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        serde_json::from_slice::<Value>(&unauth_body).unwrap()["code"],
        "unauthorized"
    );
    assert!(!args_log.exists());

    let started = Instant::now();
    let (status, headers, body) =
        get_bytes(app.clone(), &manifest_uri, Some(TestAuth::User), &[]).await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "cold HLS manifest should be served after initial playlist output, not after FFmpeg exits"
    );
    assert_eq!(
        headers[header::CONTENT_TYPE],
        "application/vnd.apple.mpegurl"
    );
    let manifest = String::from_utf8(body).unwrap();
    assert!(manifest.contains("#EXTM3U"));
    assert!(manifest.contains("segments/segment-00000.ts"));

    let args = fs::read_to_string(&args_log).unwrap();
    assert!(args
        .lines()
        .any(|arg| arg == managed_path.to_string_lossy().as_ref()));
    assert!(args.lines().any(|arg| arg == "-b:a"));
    assert!(args.lines().any(|arg| arg == "128k"));
    assert!(args.lines().any(|arg| arg == "-f"));
    assert!(args.lines().any(|arg| arg == "hls"));
    assert!(args.lines().any(|arg| arg == "-hls_segment_filename"));
    assert!(args.lines().any(|arg| arg == "segments/segment-%05d.ts"));

    let (slot_status, slots) =
        get_json(app.clone(), "/api/v1/admin/media/transcode-slots", Some(TestAuth::Admin))
            .await;
    assert_eq!(slot_status, StatusCode::OK);
    assert_eq!(slots["in_use"], json!(1));

    let segment_uri =
        format!("/api/v1/media/track/{track_id}/hls/standard/segments/segment-00000.ts");
    let (unauth_segment_status, _, unauth_segment_body) =
        get_bytes(app.clone(), &segment_uri, None, &[]).await;
    assert_eq!(unauth_segment_status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        serde_json::from_slice::<Value>(&unauth_segment_body).unwrap()["code"],
        "unauthorized"
    );

    let (segment_status, segment_headers, segment) =
        get_bytes(app.clone(), &segment_uri, Some(TestAuth::User), &[]).await;
    assert_eq!(segment_status, StatusCode::OK);
    assert_eq!(segment_headers[header::CONTENT_TYPE], "video/mp2t");
    assert_eq!(segment, b"fake-hls-segment");

    assert!(eventually(Duration::from_secs(3), || {
        let app = app.clone();
        async move {
            let (status, body) =
                get_json(app, "/api/v1/admin/media/transcode-slots", Some(TestAuth::Admin))
                    .await;
            status == StatusCode::OK && body["in_use"] == json!(0)
        }
    })
    .await);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that media hls accepts same profile names as direct transcode.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn media_hls_accepts_same_profile_names_as_direct_transcode() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-media-hls-profiles-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artist/Album/hls-profiles-source.flac");
    fs::write(&managed_path, b"source media bytes").unwrap();
    let fake_ffmpeg = root.join("fake-ffmpeg.sh");
    let args_log = root.join("ffmpeg-args.log");
    fake_hls_ffmpeg_script(&fake_ffmpeg, &args_log, None, 0);

    let Some(state) = test_state_with_transcode_runtime(
        library_root,
        dropbox_root.clone(),
        fake_ffmpeg,
        2,
    )
    .await
    else {
        return;
    };
    let source_path = dropbox_root.join("source-hls-profiles.flac");
    let imported = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &source_path.to_string_lossy(),
                "media-hls-profiles-hash",
                "HLS Profiles Artist",
                "HLS Profiles Album",
                "HLS Profiles Track",
                Some(1),
            ),
            &managed_path,
            18,
        ))
        .await
        .unwrap();
    let track_id = imported.track.as_ref().unwrap().id;
    let app = router(state);

    for profile in AacTranscodeProfile::all() {
        let uri = format!(
            "/api/v1/media/track/{track_id}/hls/{}/manifest.m3u8",
            profile.api_name()
        );
        let (status, _, _) = get_bytes(app.clone(), &uri, Some(TestAuth::User), &[]).await;
        assert_eq!(status, StatusCode::OK, "{uri}");
        let args = fs::read_to_string(&args_log).unwrap();
        assert!(
            args.lines().any(|arg| arg == profile.bitrate()),
            "{uri} did not use {}",
            profile.bitrate()
        );
    }

    let invalid_uri = format!("/api/v1/media/track/{track_id}/hls/lossless/manifest.m3u8");
    let (status, body) = get_json(app, &invalid_uri, Some(TestAuth::User)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "bad_request");

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that media hls manifest generation fails fast when transcode slots are saturated.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn media_hls_manifest_generation_fails_fast_when_transcode_slots_are_saturated() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-media-hls-saturation-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artist/Album/hls-saturation-source.flac");
    fs::write(&managed_path, b"source media bytes").unwrap();
    let fake_ffmpeg = root.join("fake-ffmpeg.sh");
    let args_log = root.join("ffmpeg-args.log");
    let started_marker = root.join("ffmpeg-started");
    fake_hls_ffmpeg_script(&fake_ffmpeg, &args_log, Some(&started_marker), 2);

    let Some(state) = test_state_with_transcode_runtime(
        library_root,
        dropbox_root.clone(),
        fake_ffmpeg,
        1,
    )
    .await
    else {
        return;
    };
    let source_path = dropbox_root.join("source-hls-saturation.flac");
    let imported = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &source_path.to_string_lossy(),
                "media-hls-saturation-hash",
                "HLS Saturation Artist",
                "HLS Saturation Album",
                "HLS Saturation Track",
                Some(1),
            ),
            &managed_path,
            18,
        ))
        .await
        .unwrap();
    let track_id = imported.track.as_ref().unwrap().id;
    let app = router(state);
    let uri = format!("/api/v1/media/track/{track_id}/hls/high/manifest.m3u8");
    let saturated_uri =
        format!("/api/v1/media/track/{track_id}/hls/standard/manifest.m3u8");
    let first_app = app.clone();
    let first_uri = uri.clone();
    let first_response = tokio::spawn(async move {
        first_app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(&first_uri)
                    .header("authorization", auth_header(TestAuth::User))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap()
    });
    assert!(eventually(Duration::from_secs(1), || {
        let started_marker = started_marker.clone();
        async move { started_marker.exists() }
    })
    .await);

    let slots_visible = eventually(Duration::from_secs(1), || {
        let app = app.clone();
        async move {
            let (status, body) =
                get_json(app, "/api/v1/admin/media/transcode-slots", Some(TestAuth::Admin))
                    .await;
            status == StatusCode::OK
                && body["limit"] == json!(1)
                && body["in_use"] == json!(1)
                && body["available"] == json!(0)
        }
    })
    .await;
    assert!(slots_visible);

    let started = Instant::now();
    let (status, _, body) =
        get_bytes(app, &saturated_uri, Some(TestAuth::User), &[]).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "saturated HLS admission should fail without waiting for a slot"
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["code"],
        "service_unavailable"
    );

    let response = first_response.await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that media hls duplicate cold manifest requests share one generation.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn media_hls_duplicate_cold_manifest_requests_share_one_generation() {
    let root = std::env::temp_dir().join(format!(
        "harmonixia-media-hls-duplicate-{}",
        Uuid::new_v4().simple()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(library_root.join("Artist/Album")).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    let managed_path = library_root.join("Artist/Album/hls-duplicate-source.flac");
    fs::write(&managed_path, b"source media bytes").unwrap();
    let fake_ffmpeg = root.join("fake-ffmpeg.sh");
    let args_log = root.join("ffmpeg-args.log");
    let launches_log = root.join("ffmpeg-launches.log");
    let started_marker = root.join("ffmpeg-started");
    let release_marker = root.join("ffmpeg-release");
    fake_hls_ffmpeg_script_with_gate(
        &fake_ffmpeg,
        &args_log,
        &launches_log,
        &started_marker,
        &release_marker,
        1,
    );

    let Some(state) = test_state_with_transcode_runtime(
        library_root,
        dropbox_root.clone(),
        fake_ffmpeg,
        1,
    )
    .await
    else {
        return;
    };
    let source_path = dropbox_root.join("source-hls-duplicate.flac");
    let imported = state
        .repository()
        .import_catalog_file(with_managed_path(
            music_import_request(
                &source_path.to_string_lossy(),
                "media-hls-duplicate-hash",
                "HLS Duplicate Artist",
                "HLS Duplicate Album",
                "HLS Duplicate Track",
                Some(1),
            ),
            &managed_path,
            18,
        ))
        .await
        .unwrap();
    let track_id = imported.track.as_ref().unwrap().id;
    let app = router(state);
    let uri = format!("/api/v1/media/track/{track_id}/hls/high/manifest.m3u8");

    let first_app = app.clone();
    let first_uri = uri.clone();
    let first_response = tokio::spawn(async move {
        get_bytes(first_app, &first_uri, Some(TestAuth::User), &[]).await
    });
    assert!(eventually(Duration::from_secs(1), || {
        let started_marker = started_marker.clone();
        async move { started_marker.exists() }
    })
    .await);

    let second_app = app.clone();
    let second_uri = uri.clone();
    let second_response = tokio::spawn(async move {
        get_bytes(second_app, &second_uri, Some(TestAuth::User), &[]).await
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    let slots_visible = eventually(Duration::from_secs(1), || {
        let app = app.clone();
        async move {
            let (status, body) =
                get_json(app, "/api/v1/admin/media/transcode-slots", Some(TestAuth::Admin))
                    .await;
            status == StatusCode::OK
                && body["limit"] == json!(1)
                && body["in_use"] == json!(1)
                && body["available"] == json!(0)
        }
    })
    .await;
    assert!(slots_visible);
    assert_eq!(
        fs::read_to_string(&launches_log).unwrap().lines().count(),
        1,
        "duplicate cold HLS requests should not spawn duplicate FFmpeg jobs"
    );
    assert!(
        !second_response.is_finished(),
        "duplicate cold HLS request should wait for the in-flight generation"
    );

    fs::write(&release_marker, b"release").unwrap();

    let (first_status, _, first_body) =
        tokio::time::timeout(Duration::from_secs(2), first_response)
            .await
            .unwrap()
            .unwrap();
    let (second_status, _, second_body) =
        tokio::time::timeout(Duration::from_secs(2), second_response)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(first_status, StatusCode::OK);
    assert_eq!(second_status, StatusCode::OK);
    assert!(String::from_utf8(first_body)
        .unwrap()
        .contains("segments/segment-00000.ts"));
    assert!(String::from_utf8(second_body)
        .unwrap()
        .contains("segments/segment-00000.ts"));
    assert_eq!(
        fs::read_to_string(&launches_log).unwrap().lines().count(),
        1,
        "only one FFmpeg process should be needed for the shared rendition"
    );

    assert!(eventually(Duration::from_secs(3), || {
        let app = app.clone();
        async move {
            let (status, body) =
                get_json(app, "/api/v1/admin/media/transcode-slots", Some(TestAuth::Admin))
                    .await;
            status == StatusCode::OK && body["in_use"] == json!(0)
        }
    })
    .await);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that playback progress and history are scoped per user.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn playback_progress_and_history_are_scoped_per_user() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state);
    let track_id = Uuid::new_v4();
    let album_id = Uuid::new_v4();
    let episode_id = Uuid::new_v4();

    let (unauth_status, unauth_body) =
        get_json(app.clone(), "/api/v1/me/playback/progress", None).await;
    assert_eq!(unauth_status, StatusCode::UNAUTHORIZED);
    assert_eq!(unauth_body["code"], "unauthorized");

    let (write_status, written) = request_json(
        app.clone(),
        "PUT",
        &format!("/api/v1/me/playback/progress/track/{track_id}"),
        json!({
            "context_type": "album",
            "context_id": album_id,
            "position_seconds": 42,
            "duration_seconds": 180,
            "completed": false
        }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(write_status, StatusCode::OK);
    assert_eq!(written["progress"]["item_type"], "track");
    assert_eq!(written["progress"]["context_type"], "album");
    assert_eq!(written["progress"]["context_id"], json!(album_id));
    assert_eq!(written["progress"]["position_seconds"], 42);
    assert_eq!(written["history_event"]["item_id"], json!(track_id));
    assert_eq!(written["history_event"]["context_type"], "album");
    assert_eq!(written["history_event"]["context_id"], json!(album_id));

    let (get_status, progress) = get_json(
        app.clone(),
        &format!("/api/v1/me/playback/progress/track/{track_id}"),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(progress["item_id"], json!(track_id));
    assert_eq!(progress["context_type"], "album");
    assert_eq!(progress["context_id"], json!(album_id));

    let (invalid_context_status, invalid_context_body) = request_json(
        app.clone(),
        "PUT",
        &format!("/api/v1/me/playback/progress/track/{track_id}"),
        json!({ "context_type": "album", "position_seconds": 4, "duration_seconds": 180 }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(invalid_context_status, StatusCode::BAD_REQUEST);
    assert_eq!(invalid_context_body["code"], "bad_request");

    let (admin_get_status, admin_get_body) = get_json(
        app.clone(),
        &format!("/api/v1/me/playback/progress/track/{track_id}"),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(admin_get_status, StatusCode::NOT_FOUND);
    assert_eq!(admin_get_body["code"], "not_found");

    let (admin_list_status, admin_list) =
        get_json(app.clone(), "/api/v1/me/playback/progress", Some(TestAuth::Admin)).await;
    assert_eq!(admin_list_status, StatusCode::OK);
    assert!(admin_list["progress"].as_array().unwrap().is_empty());

    let (history_write_status, history_event) = request_json(
        app.clone(),
        "POST",
        "/api/v1/me/playback/history",
        json!({
            "item_type": "episode",
            "item_id": episode_id,
            "position_seconds": 12,
            "duration_seconds": 600,
            "completed": false
        }),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(history_write_status, StatusCode::OK);
    assert_eq!(history_event["item_type"], "episode");

    let (history_status, history) = get_json(
        app.clone(),
        "/api/v1/me/playback/history?limit=10",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(history_status, StatusCode::OK);
    let history_items = history["history"].as_array().unwrap();
    assert_eq!(history_items.len(), 2);
    assert!(history_items.iter().any(|event| event["item_id"] == json!(track_id)));
    assert!(history_items
        .iter()
        .any(|event| event["item_id"] == json!(episode_id)));

    let (admin_history_status, admin_history) = get_json(
        app,
        "/api/v1/me/playback/history?limit=10",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(admin_history_status, StatusCode::OK);
    assert!(admin_history["history"].as_array().unwrap().is_empty());
}

#[tokio::test]
/// Verifies that admin maintenance routes reject unauthenticated requests.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors.
async fn admin_maintenance_routes_reject_unauthenticated_requests() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state);

    let (full_status, full_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/admin/maintenance/rescans/full",
        json!({}),
        None,
    )
    .await;
    assert_eq!(full_status, StatusCode::UNAUTHORIZED);
    assert_eq!(full_body["code"], "unauthorized");

    let (health_status, health_body) =
        get_json(app.clone(), "/api/v1/admin/providers/health", None).await;
    assert_eq!(health_status, StatusCode::UNAUTHORIZED);
    assert_eq!(health_body["code"], "unauthorized");

    let (repair_status, repair_body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/admin/providers/music_brainz/repair",
        json!({}),
        None,
    )
    .await;
    assert_eq!(repair_status, StatusCode::UNAUTHORIZED);
    assert_eq!(repair_body["code"], "unauthorized");

    let (quarantine_status, quarantine_body) = request_json(
        app,
        "POST",
        "/api/v1/admin/quarantine/retry",
        json!({ "item_ids": [] }),
        None,
    )
    .await;
    assert_eq!(quarantine_status, StatusCode::UNAUTHORIZED);
    assert_eq!(quarantine_body["code"], "unauthorized");
}

#[tokio::test]
/// Verifies that admin maintenance routes reject authenticated non admin users.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors.
async fn admin_maintenance_routes_reject_authenticated_non_admin_users() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state);

    let (status, body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/admin/maintenance/rescans/full",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["code"], "forbidden");

    let (health_status, health_body) = get_json(
        app.clone(),
        "/api/v1/admin/providers/health",
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(health_status, StatusCode::FORBIDDEN);
    assert_eq!(health_body["code"], "forbidden");

    let (repair_status, repair_body) = request_json(
        app,
        "POST",
        "/api/v1/admin/providers/music_brainz/repair",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(repair_status, StatusCode::FORBIDDEN);
    assert_eq!(repair_body["code"], "forbidden");
}

#[tokio::test]
/// Verifies that admin alias uses same admin authorization.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors.
async fn admin_alias_uses_same_admin_authorization() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state);

    let (unauth_status, unauth_body) = request_json(
        app.clone(),
        "POST",
        "/api/admin/maintenance/rescans/full",
        json!({}),
        None,
    )
    .await;
    assert_eq!(unauth_status, StatusCode::UNAUTHORIZED);
    assert_eq!(unauth_body["code"], "unauthorized");

    let (non_admin_status, non_admin_body) = request_json(
        app.clone(),
        "POST",
        "/api/admin/maintenance/rescans/full",
        json!({}),
        Some(TestAuth::User),
    )
    .await;
    assert_eq!(non_admin_status, StatusCode::FORBIDDEN);
    assert_eq!(non_admin_body["code"], "forbidden");

    let (admin_status, admin_body) = request_json(
        app,
        "POST",
        "/api/admin/maintenance/rescans/full",
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(admin_status, StatusCode::ACCEPTED);
    assert_eq!(admin_body["job"]["kind"], "full_rescan");
}

#[tokio::test]
/// Verifies that full rescan uses import pipeline defaults and idempotency.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn full_rescan_uses_import_pipeline_defaults_and_idempotency() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state.clone());

    let (status, body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/admin/maintenance/rescans/full",
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body["job"]["kind"], "full_rescan");
    assert_eq!(body["job"]["pipeline"], "import_pipeline");
    assert_eq!(body["job"]["scope"]["type"], "full_library");
    assert_eq!(body["job"]["repair_plan"]["refresh_provider_metadata"], true);
    assert_eq!(body["job"]["repair_plan"]["refresh_artwork"], true);
    assert_eq!(body["job"]["repair_plan"]["rewrite_sidecars"], true);
    assert_eq!(body["job"]["repair_plan"]["rebuild_search_projections"], true);
    assert_eq!(body["job"]["repair_plan"]["preserve_provenance_history"], true);
    assert_eq!(body["job"]["repair_plan"]["preserve_confidence_history"], true);
    assert_eq!(
        body["job"]["catalog_mutation_policy"],
        "preserve_visible_until_stable_grouping"
    );

    let first_id = body["job"]["id"].clone();
    let (second_status, second_body) = request_json(
        app,
        "POST",
        "/api/v1/admin/maintenance/rescans/full",
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;

    assert_eq!(second_status, StatusCode::ACCEPTED);
    assert_eq!(second_body["job"]["id"], first_id);
    assert_eq!(second_body["reused_existing"], true);
    assert_eq!(state.import_jobs().await.unwrap().len(), 1);
}

#[tokio::test]
/// Verifies that subtree rescan rejects parent directory traversal.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn subtree_rescan_rejects_parent_directory_traversal() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state);

    let (status, body) = request_json(
        app,
        "POST",
        "/api/v1/admin/maintenance/rescans/subtree",
        json!({ "path": "../outside" }),
        Some(TestAuth::Admin),
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["code"], "bad_request");
}

#[tokio::test]
/// Verifies that subtree rescan accepts managed relative path.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn subtree_rescan_accepts_managed_relative_path() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state);

    let (status, body) = request_json(
        app,
        "POST",
        "/api/v1/admin/maintenance/rescans/subtree",
        json!({ "path": "Artists/Album" }),
        Some(TestAuth::Admin),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body["job"]["kind"], "subtree_rescan");
    assert_eq!(body["job"]["scope"]["type"], "path");
    assert_eq!(
        body["job"]["scope"]["path"],
        "/srv/harmonixia/library/Artists/Album"
    );
}

#[tokio::test]
/// Verifies that provider health exposes backoff and readiness.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn provider_health_exposes_backoff_and_readiness() {
    let Some(state) = test_state().await else {
        return;
    };
    state
        .set_provider_backoff_for_tests(ProviderKind::MusicBrainz, 600)
        .await
        .unwrap();
    let app = router(state);

    let (status, health) = get_json(
        app.clone(),
        "/api/v1/admin/providers/health",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let music_brainz = health["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|provider| provider["provider"] == "music_brainz")
        .unwrap();
    assert_eq!(music_brainz["status"], "backing_off");
    assert_eq!(music_brainz["maintenance_ready"], false);
    assert!(music_brainz["retry_after"].is_string());

    let (readiness_status, readiness) = get_json(
        app,
        "/api/v1/admin/maintenance/readiness",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(readiness_status, StatusCode::OK);
    assert_eq!(readiness["can_start_rescan"], true);
    assert!(readiness["backing_off_providers"]
        .as_array()
        .unwrap()
        .contains(&json!("music_brainz")));
}

#[tokio::test]
/// Verifies that expired provider backoff is refreshable in health readiness and registry.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn expired_provider_backoff_is_refreshable_in_health_readiness_and_registry() {
    let Some(state) = test_state().await else {
        return;
    };
    state
        .set_provider_backoff_for_tests(ProviderKind::MusicBrainz, -60)
        .await
        .unwrap();
    let app = router(state.clone());

    let (status, health) = get_json(
        app.clone(),
        "/api/v1/admin/providers/health",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let music_brainz = health["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|provider| provider["provider"] == "music_brainz")
        .unwrap();
    assert_eq!(music_brainz["status"], "degraded");
    assert_eq!(music_brainz["maintenance_ready"], true);
    assert!(music_brainz["retry_after"].is_null());

    let (readiness_status, readiness) = get_json(
        app,
        "/api/v1/admin/maintenance/readiness",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(readiness_status, StatusCode::OK);
    assert_eq!(readiness["can_refresh_provider_metadata"], true);
    assert!(!readiness["backing_off_providers"]
        .as_array()
        .unwrap()
        .contains(&json!("music_brainz")));

    let provider_health = state.provider_health().await.unwrap();
    let registry =
        ProviderRegistry::from_health(&provider_health, &[ProviderKind::MusicBrainz]);
    assert_eq!(registry.providers(), &[ProviderKind::MusicBrainz]);

    let stored = state
        .repository()
        .provider(ProviderKind::MusicBrainz)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.status, ProviderStatus::Degraded);
    assert!(stored.maintenance_ready);
    assert!(stored.retry_after.is_none());
}

#[tokio::test]
/// Verifies that provider health is seeded from server config.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn provider_health_is_seeded_from_server_config() {
    let Some(mut config) = test_config() else {
        return;
    };
    config.providers.insert(
        ProviderKind::Discogs,
        ProviderConfig {
            enabled: true,
            api_key: Some("discogs-secret".into()),
            api_key_configured: true,
            requires_api_key: true,
        },
    );
    config.providers.insert(
        ProviderKind::FanartTv,
        ProviderConfig {
            enabled: false,
            api_key: None,
            api_key_configured: false,
            requires_api_key: true,
        },
    );
    let state = AppState::connect(config).await.unwrap();

    let discogs = state.provider(ProviderKind::Discogs).await.unwrap().unwrap();
    assert_eq!(discogs.status, ProviderStatus::Healthy);
    assert!(discogs.api_key_configured);
    assert!(discogs.maintenance_ready);

    let fanart = state.provider(ProviderKind::FanartTv).await.unwrap().unwrap();
    assert_eq!(fanart.status, ProviderStatus::Disabled);
    assert!(!fanart.maintenance_ready);

    let audio_db = state
        .provider(ProviderKind::TheAudioDb)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(audio_db.status, ProviderStatus::Unconfigured);
    assert!(!audio_db.maintenance_ready);
}

#[test]
/// Verifies that provider registry honors disabled unconfigured and missing runtime keys.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn provider_registry_honors_disabled_unconfigured_and_missing_runtime_keys() {
    let now = Utc::now();
    let music_brainz = ProviderHealth::healthy(ProviderKind::MusicBrainz, now);
    let mut discogs = ProviderHealth::healthy(ProviderKind::Discogs, now);
    discogs.api_key_configured = true;

    let mut fanart = ProviderHealth::healthy(ProviderKind::FanartTv, now);
    fanart.enabled = false;
    fanart.status = ProviderStatus::Disabled;
    fanart.maintenance_ready = false;

    let mut audio_db = ProviderHealth::healthy(ProviderKind::TheAudioDb, now);
    audio_db.status = ProviderStatus::Unconfigured;
    audio_db.api_key_configured = false;
    audio_db.maintenance_ready = false;

    let local_sidecars = ProviderHealth::healthy(ProviderKind::LocalSidecars, now);
    let health = vec![music_brainz, discogs.clone(), fanart, audio_db, local_sidecars];
    let credentials = vec![ProviderCredential::new(
        ProviderKind::Discogs,
        Some("discogs-token".into()),
        None,
    )];

    let registry = ProviderRegistry::from_health_and_credentials(&health, &credentials, &[]);
    assert_eq!(
        registry.providers(),
        &[
            ProviderKind::MusicBrainz,
            ProviderKind::Discogs,
            ProviderKind::LocalSidecars,
        ]
    );

    let filtered = ProviderRegistry::from_health_and_credentials(
        &health,
        &credentials,
        &[ProviderKind::Discogs],
    );
    assert_eq!(filtered.providers(), &[ProviderKind::Discogs]);

    let missing_secret =
        ProviderRegistry::from_health_and_credentials(&[discogs], &[], &[ProviderKind::Discogs]);
    assert!(missing_secret.providers().is_empty());
}

#[tokio::test]
/// Verifies that persisted system config overrides restart bootstrap defaults.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn persisted_system_config_overrides_restart_bootstrap_defaults() {
    let Some(mut config) = test_config() else {
        return;
    };
    config.library_root = PathBuf::from("/bootstrap/library");
    config.dropbox_root = PathBuf::from("/bootstrap/dropbox");
    config.public_base_url = Some("https://bootstrap.example.test/".into());
    let state = AppState::connect(config.clone()).await.unwrap();
    assert_eq!(state.system_config().library_root, "/bootstrap/library");
    assert_eq!(state.system_config().dropbox_root, "/bootstrap/dropbox");
    assert_eq!(
        state.system_config().public_base_url.as_deref(),
        Some("https://bootstrap.example.test/")
    );

    state
        .update_system_config(
            "/persisted/library",
            "/persisted/dropbox",
            Some("Podcasts"),
            Some(Some("https://persisted.example.test")),
            None,
            Some(3),
        )
        .await
        .unwrap();

    let mut restarted_config = config.clone();
    restarted_config.library_root = PathBuf::from("/changed/library");
    restarted_config.dropbox_root = PathBuf::from("/changed/dropbox");
    restarted_config.public_base_url = Some("https://changed.example.test/".into());
    let restarted = AppState::connect(restarted_config).await.unwrap();
    assert_eq!(restarted.system_config().library_root, "/persisted/library");
    assert_eq!(restarted.system_config().dropbox_root, "/persisted/dropbox");
    assert_eq!(
        restarted.system_config().public_base_url.as_deref(),
        Some("https://persisted.example.test")
    );
    assert_eq!(restarted.system_config().scan_thread_count, 3);

    seed_test_accounts(&restarted).await;
    let app = router(restarted);
    let (status, body) = request_json(
        app,
        "POST",
        "/api/v1/admin/maintenance/rescans/subtree",
        json!({ "path": "Artists/Album" }),
        Some(TestAuth::Admin),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(
        body["job"]["scope"]["path"],
        "/persisted/library/Artists/Album"
    );
}

#[tokio::test]
/// Verifies that previously persisted unspecified public base URLs are rejected during restart.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn persisted_unspecified_public_base_url_is_rejected_on_restart() {
    let Some(config) = test_config() else {
        eprintln!(
            "skipping Postgres-backed maintenance API test; set HARMONIXIA_TEST_DATABASE_URL"
        );
        return;
    };

    let state = AppState::connect(config.clone())
        .await
        .expect("test database should connect and migrate");
    let mut stored = state.system_config();
    stored.public_base_url = Some("https://0.0.0.0:1400".into());
    stored.updated_at = Utc::now();
    state
        .repository()
        .save_system_config(&stored)
        .await
        .expect("legacy invalid public base URL should be persisted for regression setup");
    drop(state);

    let restart = AppState::connect(config).await;
    assert!(matches!(
        restart,
        Err(StorageError::InvalidStoredValue {
            field: "system_config.public_base_url",
            value
        }) if value.contains("0.0.0.0") && value.contains("unspecified")
    ));
}

#[test]
/// Verifies that bootstrap public base URL environment validation rejects unspecified hosts and accepts valid schemes.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `()` after completing the validation checks.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn bootstrap_public_base_url_env_validation_rejects_unspecified_and_accepts_valid_schemes() {
    let Some(config) = test_config() else {
        eprintln!(
            "skipping Postgres-backed config env test; set HARMONIXIA_TEST_DATABASE_URL"
        );
        return;
    };

    let _database_url = EnvVarGuard::set("HARMONIXIA_DATABASE_URL", &config.database.url);
    let _database_max_connections = EnvVarGuard::set("HARMONIXIA_DATABASE_MAX_CONNECTIONS", "2");
    let _database_connect_timeout =
        EnvVarGuard::set("HARMONIXIA_DATABASE_CONNECT_TIMEOUT_SECONDS", "5");
    let _database_schema = EnvVarGuard::set(
        "HARMONIXIA_DATABASE_SCHEMA",
        config.database.schema.as_deref().unwrap_or("public"),
    );
    let _transcode_limit = EnvVarGuard::set("HARMONIXIA_TRANSCODE_CONCURRENCY_LIMIT", "2");
    let _scan_thread_count = EnvVarGuard::set("HARMONIXIA_SCAN_THREAD_COUNT", "4");

    for public_base_url in [
        "https://speaker-lan.example.test",
        "http://speaker-lan.example.test:1400",
        "http://192.168.1.50:1400",
        "http://[2001:db8::1]:1400",
    ] {
        let _public_base_url = EnvVarGuard::set("HARMONIXIA_PUBLIC_BASE_URL", public_base_url);
        let from_env = ServerConfig::from_env().expect("valid public base URL should load");
        assert_eq!(from_env.public_base_url.as_deref(), Some(public_base_url));
    }

    for public_base_url in [
        "https://0.0.0.0:1400",
        "http://[::]:1400",
        "http://[0:0:0:0:0:0:0:0]:1400",
    ] {
        let _public_base_url = EnvVarGuard::set("HARMONIXIA_PUBLIC_BASE_URL", public_base_url);
        assert!(matches!(
            ServerConfig::from_env(),
            Err(ServerConfigError::InvalidPublicBaseUrl)
        ));
    }
}

#[tokio::test]
/// Verifies that provider settings persist and drive health across restart.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn provider_settings_persist_and_drive_health_across_restart() {
    let Some(mut config) = test_config() else {
        return;
    };
    config.providers.insert(
        ProviderKind::Discogs,
        ProviderConfig {
            enabled: false,
            api_key: None,
            api_key_configured: false,
            requires_api_key: true,
        },
    );
    let state = AppState::connect(config.clone()).await.unwrap();
    state
        .update_provider_setting(
            ProviderKind::Discogs,
            Some(true),
            Some("persisted-discogs-key"),
            false,
        )
        .await
        .unwrap();

    let mut restarted_config = config.clone();
    restarted_config.providers.insert(
        ProviderKind::Discogs,
        ProviderConfig {
            enabled: false,
            api_key: None,
            api_key_configured: false,
            requires_api_key: true,
        },
    );
    let restarted = AppState::connect(restarted_config).await.unwrap();

    let setting = restarted
        .provider_setting(ProviderKind::Discogs)
        .await
        .unwrap()
        .unwrap();
    assert!(setting.enabled);
    assert!(setting.api_key_configured);

    let health = restarted
        .provider(ProviderKind::Discogs)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(health.status, ProviderStatus::Healthy);
    assert!(health.maintenance_ready);
}

#[tokio::test]
/// Verifies that provider runtime credentials are loaded from persisted settings.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn provider_runtime_credentials_are_loaded_from_persisted_settings() {
    let Some(state) = test_state_without_accounts().await else {
        return;
    };
    state
        .update_provider_setting(
            ProviderKind::Discogs,
            Some(true),
            Some("runtime-discogs-token"),
            false,
        )
        .await
        .unwrap();

    let credentials = state.repository().provider_credentials().await.unwrap();
    let discogs = credentials
        .iter()
        .find(|credential| credential.provider == ProviderKind::Discogs)
        .unwrap();
    assert_eq!(discogs.api_key.as_deref(), Some("runtime-discogs-token"));

    let public_setting = state
        .provider_setting(ProviderKind::Discogs)
        .await
        .unwrap()
        .unwrap();
    assert!(public_setting.api_key_configured);
}

#[tokio::test]
/// Verifies that admin settings endpoints update system and provider settings.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn admin_settings_endpoints_update_system_and_provider_settings() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state.clone());

    let (config_status, config_body) = get_json(
        app.clone(),
        "/api/v1/admin/system/config",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(config_status, StatusCode::OK);
    assert_eq!(config_body["library_root"], "/srv/harmonixia/library");
    assert_eq!(config_body["public_base_url"], Value::Null);

    let (update_status, updated_config) = request_json(
        app.clone(),
        "PUT",
        "/api/v1/admin/system/config",
        json!({
            "library_root": "/data/harmonixia/library",
            "dropbox_root": "/data/harmonixia/dropbox",
            "podcast_subtree": "Podcasts",
            "public_base_url": "https://speaker-lan.example.test"
        }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(update_status, StatusCode::OK);
    assert_eq!(updated_config["library_root"], "/data/harmonixia/library");
    assert_eq!(
        updated_config["public_base_url"],
        "https://speaker-lan.example.test"
    );
    assert_eq!(state.system_config().library_root, "/data/harmonixia/library");
    assert_eq!(
        state.system_config().public_base_url.as_deref(),
        Some("https://speaker-lan.example.test")
    );

    let (http_update_status, http_updated_config) = request_json(
        app.clone(),
        "PUT",
        "/api/v1/admin/system/config",
        json!({
            "library_root": "/data/harmonixia/library",
            "dropbox_root": "/data/harmonixia/dropbox",
            "podcast_subtree": "Podcasts",
            "public_base_url": "http://speaker-lan.example.test:1400"
        }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(http_update_status, StatusCode::OK);
    assert_eq!(
        http_updated_config["public_base_url"],
        "http://speaker-lan.example.test:1400"
    );
    assert_eq!(
        state.system_config().public_base_url.as_deref(),
        Some("http://speaker-lan.example.test:1400")
    );

    let (ip_update_status, ip_updated_config) = request_json(
        app.clone(),
        "PUT",
        "/api/v1/admin/system/config",
        json!({
            "library_root": "/data/harmonixia/library",
            "dropbox_root": "/data/harmonixia/dropbox",
            "podcast_subtree": "Podcasts",
            "public_base_url": "http://192.168.1.51:1400"
        }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(ip_update_status, StatusCode::OK);
    assert_eq!(
        ip_updated_config["public_base_url"],
        "http://192.168.1.51:1400"
    );
    assert_eq!(
        state.system_config().public_base_url.as_deref(),
        Some("http://192.168.1.51:1400")
    );

    let (settings_status, settings) = get_json(
        app.clone(),
        "/api/v1/admin/providers/settings",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(settings_status, StatusCode::OK);
    assert!(settings["providers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|provider| provider["provider"] == "discogs"));

    let (provider_status, provider_setting) = request_json(
        app,
        "PATCH",
        "/api/v1/admin/providers/discogs/settings",
        json!({ "enabled": true, "api_key": "discogs-api-key" }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(provider_status, StatusCode::OK);
    assert_eq!(provider_setting["provider"], "discogs");
    assert_eq!(provider_setting["api_key_configured"], true);
    assert!(!provider_setting.as_object().unwrap().contains_key("api_key"));

    let discogs = state.provider(ProviderKind::Discogs).await.unwrap().unwrap();
    assert_eq!(discogs.status, ProviderStatus::Healthy);
    assert!(discogs.maintenance_ready);
}

#[tokio::test]
/// Verifies that admin system config update rejects unspecified public base URL hosts.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn admin_system_config_update_rejects_unspecified_public_base_url_hosts() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state.clone());

    let (seed_status, seed_config) = request_json(
        app.clone(),
        "PUT",
        "/api/v1/admin/system/config",
        json!({
            "library_root": "/srv/harmonixia/library",
            "dropbox_root": "/srv/harmonixia/dropbox",
            "podcast_subtree": "Shows/Podcasts",
            "public_base_url": "https://setup-speakers.example.test:9443",
            "transcode_concurrency_limit": 7,
            "scan_thread_count": 4
        }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(seed_status, StatusCode::OK);
    assert_eq!(
        seed_config["public_base_url"],
        "https://setup-speakers.example.test:9443"
    );

    for public_base_url in [
        "https://0.0.0.0:1400",
        "http://[::]:1400",
        "https://[0:0:0:0:0:0:0:0]/music",
    ] {
        let (reject_status, reject_body) = request_json(
            app.clone(),
            "PUT",
            "/api/v1/admin/system/config",
            json!({
                "library_root": "/should/not/save/library",
                "dropbox_root": "/should/not/save/dropbox",
                "podcast_subtree": "Rejected/Podcasts",
                "public_base_url": public_base_url,
                "transcode_concurrency_limit": 9,
                "scan_thread_count": 8
            }),
            Some(TestAuth::Admin),
        )
        .await;
        assert_eq!(reject_status, StatusCode::BAD_REQUEST);
        assert_eq!(reject_body["code"], "bad_request");
        assert!(reject_body["message"]
            .as_str()
            .unwrap()
            .contains("public_base_url"));
        let envelope = reject_body.as_object().unwrap();
        assert!(envelope.contains_key("code"));
        assert!(envelope.contains_key("message"));
        assert!(!envelope.contains_key("details"));

        let saved = state.system_config();
        assert_eq!(saved.library_root, "/srv/harmonixia/library");
        assert_eq!(saved.dropbox_root, "/srv/harmonixia/dropbox");
        assert_eq!(saved.podcast_subtree, "Shows/Podcasts");
        assert_eq!(
            saved.public_base_url.as_deref(),
            Some("https://setup-speakers.example.test:9443")
        );
        assert_eq!(saved.transcode_concurrency_limit, 7);
        assert_eq!(saved.scan_thread_count, 4);
    }

    let (get_status, saved_config) = get_json(
        app,
        "/api/v1/admin/system/config",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(saved_config["library_root"], "/srv/harmonixia/library");
    assert_eq!(saved_config["dropbox_root"], "/srv/harmonixia/dropbox");
    assert_eq!(saved_config["podcast_subtree"], "Shows/Podcasts");
    assert_eq!(
        saved_config["public_base_url"],
        "https://setup-speakers.example.test:9443"
    );
    assert_eq!(saved_config["transcode_concurrency_limit"], 7);
    assert_eq!(saved_config["scan_thread_count"], 4);
}

#[tokio::test]
/// Verifies that admin system config update preserves omitted podcast subtree.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn admin_system_config_update_preserves_omitted_podcast_subtree() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state.clone());

    let (initial_status, initial_config) = request_json(
        app.clone(),
        "PUT",
        "/api/v1/admin/system/config",
        json!({
            "library_root": "/srv/harmonixia/library",
            "dropbox_root": "/srv/harmonixia/dropbox",
            "podcast_subtree": "Shows/Podcasts",
            "public_base_url": "https://setup-speakers.example.test:9443",
            "transcode_concurrency_limit": 7,
            "scan_thread_count": 4
        }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(initial_status, StatusCode::OK);
    assert_eq!(initial_config["podcast_subtree"], "Shows/Podcasts");
    assert_eq!(
        initial_config["public_base_url"],
        "https://setup-speakers.example.test:9443"
    );
    assert_eq!(initial_config["transcode_concurrency_limit"], 7);
    assert_eq!(initial_config["scan_thread_count"], 4);

    let (update_status, updated_config) = request_json(
        app.clone(),
        "PUT",
        "/api/v1/admin/system/config",
        json!({
            "library_root": "/data/harmonixia/library",
            "dropbox_root": "/data/harmonixia/dropbox"
        }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(update_status, StatusCode::OK);
    assert_eq!(updated_config["library_root"], "/data/harmonixia/library");
    assert_eq!(updated_config["podcast_subtree"], "Shows/Podcasts");
    assert_eq!(
        updated_config["public_base_url"],
        "https://setup-speakers.example.test:9443"
    );
    assert_eq!(updated_config["transcode_concurrency_limit"], 7);
    assert_eq!(updated_config["scan_thread_count"], 4);
    assert_eq!(state.system_config().podcast_subtree, "Shows/Podcasts");
    assert_eq!(
        state.system_config().public_base_url.as_deref(),
        Some("https://setup-speakers.example.test:9443")
    );
    assert_eq!(state.system_config().transcode_concurrency_limit, 7);
    assert_eq!(state.system_config().scan_thread_count, 4);

    let (get_status, saved_config) = get_json(
        app,
        "/api/v1/admin/system/config",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(get_status, StatusCode::OK);
    assert_eq!(saved_config["podcast_subtree"], "Shows/Podcasts");
    assert_eq!(
        saved_config["public_base_url"],
        "https://setup-speakers.example.test:9443"
    );
    assert_eq!(saved_config["transcode_concurrency_limit"], 7);
    assert_eq!(saved_config["scan_thread_count"], 4);
}

#[tokio::test]
/// Verifies that admin provider update preserves existing api key when omitted.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn admin_provider_update_preserves_existing_api_key_when_omitted() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state.clone());

    state
        .update_provider_setting(
            ProviderKind::Discogs,
            Some(true),
            Some("persisted-discogs-key"),
            false,
        )
        .await
        .unwrap();

    let (update_status, updated_setting) = request_json(
        app.clone(),
        "PATCH",
        "/api/v1/admin/providers/discogs/settings",
        json!({ "enabled": true }),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(update_status, StatusCode::OK);
    assert_eq!(updated_setting["provider"], "discogs");
    assert_eq!(updated_setting["enabled"], true);
    assert_eq!(updated_setting["api_key_configured"], true);
    assert!(!updated_setting.as_object().unwrap().contains_key("api_key"));

    let credentials = state.repository().provider_credentials().await.unwrap();
    let discogs = credentials
        .iter()
        .find(|credential| credential.provider == ProviderKind::Discogs)
        .unwrap();
    assert_eq!(discogs.api_key.as_deref(), Some("persisted-discogs-key"));

    let (noop_status, noop_setting) = request_json(
        app,
        "PATCH",
        "/api/v1/admin/providers/discogs/settings",
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(noop_status, StatusCode::OK);
    assert_eq!(noop_setting["api_key_configured"], true);
}

#[tokio::test]
/// Verifies that maintenance state persists across recreated app state.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn maintenance_state_persists_across_recreated_app_state() {
    let Some(config) = test_config() else {
        return;
    };
    let state = AppState::connect(config.clone()).await.unwrap();

    let outcome = state
        .enqueue_import_work(ImportWorkRequest {
            kind: ImportJobKind::FullRescan,
            scope: MaintenanceScope::FullLibrary,
            repair_plan: RepairPlan::default(),
            provider_filter: Vec::new(),
            source: ImportJobSource::AdminFullRescan,
            reason: Some("persistence regression test".into()),
            related_quarantine_item_id: None,
        })
        .await
        .unwrap();
    state
        .set_provider_backoff_for_tests(ProviderKind::MusicBrainz, 600)
        .await
        .unwrap();
    let item = state
        .insert_quarantine_item_for_tests(QuarantineItem::metadata_failure(
            "/srv/harmonixia/library/Persisted/Track.mp3",
        ))
        .await
        .unwrap();

    let restarted = AppState::connect(config).await.unwrap();
    let jobs = restarted.import_jobs().await.unwrap();
    assert!(jobs.iter().any(|job| job.id == outcome.job.id));

    let music_brainz = restarted
        .provider(ProviderKind::MusicBrainz)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(music_brainz.status, ProviderStatus::BackingOff);

    let persisted_item = restarted.quarantine_item(item.id).await.unwrap().unwrap();
    assert_eq!(persisted_item.source_path, item.source_path);
    assert_eq!(persisted_item.status, QuarantineStatus::Open);
}

#[tokio::test]
/// Verifies that readiness blocks rescan when import job is active.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn readiness_blocks_rescan_when_import_job_is_active() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state);

    let (status, _) = request_json(
        app.clone(),
        "POST",
        "/api/v1/admin/maintenance/rescans/full",
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(status, StatusCode::ACCEPTED);

    let (readiness_status, readiness) = get_json(
        app,
        "/api/v1/admin/maintenance/readiness",
        Some(TestAuth::Admin),
    )
    .await;
    assert_eq!(readiness_status, StatusCode::OK);

    assert_eq!(readiness["can_start_rescan"], false);
    assert!(readiness["warnings"]
        .as_array()
        .unwrap()
        .iter()
        .any(|warning| warning.as_str().unwrap().contains("active import job")));
}

#[tokio::test]
/// Verifies that provider repair clears backoff and enqueues provider filtered job.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn provider_repair_clears_backoff_and_enqueues_provider_filtered_job() {
    let Some(state) = test_state().await else {
        return;
    };
    state
        .set_provider_backoff_for_tests(ProviderKind::Discogs, 600)
        .await
        .unwrap();
    let app = router(state.clone());

    let (status, body) = request_json(
        app,
        "POST",
        "/api/v1/admin/providers/discogs/repair",
        json!({ "path": "Artists" }),
        Some(TestAuth::Admin),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body["job"]["kind"], "provider_repair");
    assert_eq!(body["job"]["provider_filter"], json!(["discogs"]));
    assert_eq!(body["job"]["scope"]["path"], "/srv/harmonixia/library/Artists");

    let discogs = state.provider(ProviderKind::Discogs).await.unwrap().unwrap();
    assert_eq!(discogs.status, ProviderStatus::Degraded);
    assert!(discogs.retry_after.is_none());
    assert!(discogs.maintenance_ready);
}

#[tokio::test]
/// Verifies that provider repair rejects unconfigured provider.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn provider_repair_rejects_unconfigured_provider() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state);

    let (status, body) = request_json(
        app,
        "POST",
        "/api/v1/admin/providers/discogs/repair",
        json!({}),
        Some(TestAuth::Admin),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "conflict");
}

#[tokio::test]
/// Verifies that quarantine retry enqueues metadata repair job.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn quarantine_retry_enqueues_metadata_repair_job() {
    let Some(state) = test_state().await else {
        return;
    };
    let item = state
        .insert_quarantine_item_for_tests(QuarantineItem::metadata_failure(
            "/srv/harmonixia/library/Unknown Artist/Episode.mp3",
        ))
        .await
        .unwrap();
    let app = router(state.clone());

    let (status, body) = request_json(
        app,
        "POST",
        "/api/v1/admin/quarantine/retry",
        json!({ "item_ids": [item.id] }),
        Some(TestAuth::Admin),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body["jobs"][0]["kind"], "quarantine_retry");
    assert_eq!(body["jobs"][0]["pipeline"], "import_pipeline");
    assert_eq!(body["jobs"][0]["related_quarantine_item_id"], json!(item.id));
    assert_eq!(
        body["jobs"][0]["repair_plan"]["refresh_provider_metadata"],
        true
    );
    assert_eq!(
        body["jobs"][0]["repair_plan"]["rebuild_search_projections"],
        true
    );
    assert_eq!(
        body["jobs"][0]["catalog_mutation_policy"],
        "preserve_visible_until_stable_grouping"
    );

    let updated = state.quarantine_item(item.id).await.unwrap().unwrap();
    assert_eq!(updated.status, QuarantineStatus::Retrying);
    assert_eq!(updated.retry_count, 1);
    assert!(updated.last_import_job_id.is_some());
}

#[tokio::test]
/// Verifies that bulk quarantine retry rejects mixed validity without partial updates.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn bulk_quarantine_retry_rejects_mixed_validity_without_partial_updates() {
    let Some(state) = test_state().await else {
        return;
    };
    let valid = state
        .insert_quarantine_item_for_tests(QuarantineItem::metadata_failure(
            "/srv/harmonixia/library/Unknown Artist/Retryable.mp3",
        ))
        .await
        .unwrap();
    let mut ineligible = QuarantineItem::metadata_failure(
        "/srv/harmonixia/library/Unknown Artist/Ineligible.mp3",
    );
    ineligible.retry_eligible = false;
    let ineligible = state
        .insert_quarantine_item_for_tests(ineligible)
        .await
        .unwrap();
    let app = router(state.clone());

    let (status, body) = request_json(
        app,
        "POST",
        "/api/v1/admin/quarantine/retry",
        json!({ "item_ids": [valid.id, ineligible.id] }),
        Some(TestAuth::Admin),
    )
    .await;

    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(body["code"], "conflict");

    let valid_after = state.quarantine_item(valid.id).await.unwrap().unwrap();
    assert_eq!(valid_after.status, QuarantineStatus::Open);
    assert_eq!(valid_after.retry_count, 0);
    assert!(valid_after.last_import_job_id.is_none());

    let ineligible_after = state
        .quarantine_item(ineligible.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ineligible_after.status, QuarantineStatus::Open);
    assert_eq!(ineligible_after.retry_count, 0);
    assert!(ineligible_after.last_import_job_id.is_none());

    let retry_job_count = state
        .import_jobs()
        .await
        .unwrap()
        .iter()
        .filter(|job| job.kind == ImportJobKind::QuarantineRetry)
        .count();
    assert_eq!(retry_job_count, 0);
}

#[tokio::test]
/// Verifies that bulk quarantine retry rejects missing items without partial updates.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn bulk_quarantine_retry_rejects_missing_items_without_partial_updates() {
    let Some(state) = test_state().await else {
        return;
    };
    let valid = state
        .insert_quarantine_item_for_tests(QuarantineItem::metadata_failure(
            "/srv/harmonixia/library/Unknown Artist/Present.mp3",
        ))
        .await
        .unwrap();
    let missing_id = Uuid::new_v4();
    let app = router(state.clone());

    let (status, body) = request_json(
        app,
        "POST",
        "/api/v1/admin/quarantine/retry",
        json!({ "item_ids": [valid.id, missing_id] }),
        Some(TestAuth::Admin),
    )
    .await;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "not_found");

    let valid_after = state.quarantine_item(valid.id).await.unwrap().unwrap();
    assert_eq!(valid_after.status, QuarantineStatus::Open);
    assert_eq!(valid_after.retry_count, 0);
    assert!(valid_after.last_import_job_id.is_none());

    let retry_job_count = state
        .import_jobs()
        .await
        .unwrap()
        .iter()
        .filter(|job| job.kind == ImportJobKind::QuarantineRetry)
        .count();
    assert_eq!(retry_job_count, 0);
}

#[tokio::test]
/// Verifies that bulk quarantine retry transitions all items and reuses active jobs.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn bulk_quarantine_retry_transitions_all_items_and_reuses_active_jobs() {
    let Some(state) = test_state().await else {
        return;
    };
    let first = state
        .insert_quarantine_item_for_tests(QuarantineItem::metadata_failure(
            "/srv/harmonixia/library/Unknown Artist/First.mp3",
        ))
        .await
        .unwrap();
    let second = state
        .insert_quarantine_item_for_tests(QuarantineItem::metadata_failure(
            "/srv/harmonixia/library/Unknown Artist/Second.mp3",
        ))
        .await
        .unwrap();
    let app = router(state.clone());

    let (status, body) = request_json(
        app.clone(),
        "POST",
        "/api/v1/admin/quarantine/retry",
        json!({ "item_ids": [first.id, second.id] }),
        Some(TestAuth::Admin),
    )
    .await;

    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body["retried_item_ids"], json!([first.id, second.id]));
    assert_eq!(body["jobs"].as_array().unwrap().len(), 2);
    let first_job_ids = body["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|job| job["id"].clone())
        .collect::<Vec<_>>();

    for item in [first.id, second.id] {
        let updated = state.quarantine_item(item).await.unwrap().unwrap();
        assert_eq!(updated.status, QuarantineStatus::Retrying);
        assert_eq!(updated.retry_count, 1);
        assert!(updated.last_import_job_id.is_some());
    }

    let (second_status, second_body) = request_json(
        app,
        "POST",
        "/api/v1/admin/quarantine/retry",
        json!({ "item_ids": [first.id, second.id] }),
        Some(TestAuth::Admin),
    )
    .await;

    assert_eq!(second_status, StatusCode::ACCEPTED);
    let second_job_ids = second_body["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .map(|job| job["id"].clone())
        .collect::<Vec<_>>();
    assert_eq!(second_job_ids, first_job_ids);

    for item in [first.id, second.id] {
        let updated = state.quarantine_item(item).await.unwrap().unwrap();
        assert_eq!(updated.status, QuarantineStatus::Retrying);
        assert_eq!(updated.retry_count, 1);
    }

    let retry_job_count = state
        .import_jobs()
        .await
        .unwrap()
        .iter()
        .filter(|job| job.kind == ImportJobKind::QuarantineRetry)
        .count();
    assert_eq!(retry_job_count, 2);
}

#[tokio::test]
/// Verifies that catalog pipeline publishes stable dropbox file and quarantines duplicate.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn catalog_pipeline_publishes_stable_dropbox_file_and_quarantines_duplicate() {
    let Some(mut config) = test_config() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("harmonixia-catalog-{}", Uuid::new_v4()));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(&library_root).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    disable_external_providers(&mut config);
    config.library_root = library_root.clone();
    config.dropbox_root = dropbox_root.clone();

    let state = AppState::connect(config).await.unwrap();
    let source = dropbox_root.join("incoming-track.mp3");
    fs::write(&source, b"same audio bytes").unwrap();
    fs::write(
        source.with_extension("json"),
        json!({
            "artist": "The Example Artist",
            "album": "Example Album",
            "title": "Example Song",
            "track_number": 1
        })
        .to_string(),
    )
    .unwrap();

    let first = state.enqueue_dropbox_ingest(None, None).await.unwrap();
    let first_summary = state.run_import_job(first.job.id).await.unwrap();
    assert_eq!(first_summary.scanned_files, 1);
    assert_eq!(first_summary.published_files, 1);
    let managed = library_root
        .join("The Example Artist")
        .join("Example Album")
        .join("01 - Example Song.mp3");
    assert!(managed.exists());
    assert!(managed
        .parent()
        .unwrap()
        .join("harmonixia.metadata.json")
        .exists());

    let duplicate = dropbox_root.join("duplicate-track.mp3");
    fs::write(&duplicate, b"same audio bytes").unwrap();
    fs::write(
        duplicate.with_extension("json"),
        json!({
            "artist": "The Example Artist",
            "album": "Example Album",
            "title": "Example Song",
            "track_number": 1
        })
        .to_string(),
    )
    .unwrap();

    let second = state.enqueue_dropbox_ingest(None, None).await.unwrap();
    let second_summary = state.run_import_job(second.job.id).await.unwrap();
    assert_eq!(second_summary.scanned_files, 1);
    assert_eq!(second_summary.duplicate_files, 1);
    assert!(duplicate.exists());

    let counts = state.repository().catalog_counts().await.unwrap();
    assert_eq!(counts.published_media_files, 1);
    assert_eq!(counts.quarantined_media_files, 1);
    assert_eq!(counts.tracks, 1);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that catalog pipeline enforces live provider backoff for later dropbox files.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn catalog_pipeline_enforces_live_provider_backoff_for_later_dropbox_files() {
    let Some(mut config) = test_config() else {
        return;
    };
    let mock_provider = MockProviderServer::failing().await;
    let _endpoint = EnvVarGuard::set(
        "HARMONIXIA_PROVIDER_COVER_ART_ARCHIVE_BASE_URL",
        &mock_provider.base_url,
    );
    let root = std::env::temp_dir().join(format!(
        "harmonixia-live-backoff-ingest-{}",
        Uuid::new_v4()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(&library_root).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    disable_external_providers(&mut config);
    enable_external_provider(&mut config, ProviderKind::CoverArtArchive);
    config.library_root = library_root.clone();
    config.dropbox_root = dropbox_root.clone();

    let state = AppState::connect(config).await.unwrap();
    for (index, title) in [(1, "Backoff First"), (2, "Backoff Second")] {
        let source = dropbox_root.join(format!("{index:02}-{title}.mp3"));
        fs::write(&source, format!("provider backoff audio bytes {index}")).unwrap();
        fs::write(
            source.with_extension("json"),
            json!({
                "artist": "Backoff Artist",
                "album": "Backoff Album",
                "title": title,
                "track_number": index,
                "musicbrainz_albumid": "a1d2f70d-24b2-4a07-bf2b-6d2f746b710c"
            })
            .to_string(),
        )
        .unwrap();
    }

    let queued = state.enqueue_dropbox_ingest(None, None).await.unwrap();
    let summary = state.run_import_job(queued.job.id).await.unwrap();

    assert_eq!(summary.scanned_files, 2);
    assert_eq!(summary.published_files, 2);
    assert_eq!(summary.quarantined_files, 0);
    assert_eq!(mock_provider.requests.load(Ordering::SeqCst), 3);

    let cover_art = state
        .provider(ProviderKind::CoverArtArchive)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(cover_art.status, ProviderStatus::BackingOff);
    assert!(!cover_art.maintenance_ready);
    assert!(cover_art.retry_after.unwrap() > Utc::now());

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that provider repair quarantines later files skipped by live backoff.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn provider_repair_quarantines_later_files_skipped_by_live_backoff() {
    let Some(mut config) = test_config() else {
        return;
    };
    let mock_provider = MockProviderServer::failing().await;
    let _endpoint = EnvVarGuard::set(
        "HARMONIXIA_PROVIDER_COVER_ART_ARCHIVE_BASE_URL",
        &mock_provider.base_url,
    );
    let root = std::env::temp_dir().join(format!(
        "harmonixia-live-backoff-repair-{}",
        Uuid::new_v4()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(&library_root).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    disable_external_providers(&mut config);
    enable_external_provider(&mut config, ProviderKind::CoverArtArchive);
    config.library_root = library_root.clone();
    config.dropbox_root = dropbox_root.clone();

    let state = AppState::connect(config).await.unwrap();
    let artist_root = library_root.join("Repair Artist").join("Repair Album");
    fs::create_dir_all(&artist_root).unwrap();
    for (index, title) in [(1, "Repair First"), (2, "Repair Second")] {
        let source = artist_root.join(format!("{index:02}-{title}.mp3"));
        fs::write(&source, format!("repair provider backoff audio bytes {index}")).unwrap();
        fs::write(
            source.with_extension("json"),
            json!({
                "artist": "Repair Artist",
                "album": "Repair Album",
                "title": title,
                "track_number": index,
                "musicbrainz_albumid": "a1d2f70d-24b2-4a07-bf2b-6d2f746b710c"
            })
            .to_string(),
        )
        .unwrap();
    }

    let queued = state
        .enqueue_import_work(ImportWorkRequest {
            kind: ImportJobKind::ProviderRepair,
            scope: MaintenanceScope::FullLibrary,
            repair_plan: RepairPlan::default(),
            provider_filter: vec![ProviderKind::CoverArtArchive],
            source: ImportJobSource::AdminProviderRepair,
            reason: Some("verify live backoff keeps unresolved repair files quarantined".into()),
            related_quarantine_item_id: None,
        })
        .await
        .unwrap();
    let summary = state.run_import_job(queued.job.id).await.unwrap();

    assert_eq!(summary.scanned_files, 2);
    assert_eq!(summary.published_files, 0);
    assert_eq!(summary.quarantined_files, 2);
    assert_eq!(mock_provider.requests.load(Ordering::SeqCst), 3);

    let counts = state.repository().catalog_counts().await.unwrap();
    assert_eq!(counts.published_media_files, 0);
    assert_eq!(counts.quarantined_media_files, 2);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that catalog pipeline reuses managed file after dropbox import on rescans.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn catalog_pipeline_reuses_managed_file_after_dropbox_import_on_rescans() {
    let Some(mut config) = test_config() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("harmonixia-rescan-{}", Uuid::new_v4()));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(&library_root).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    disable_external_providers(&mut config);
    config.library_root = library_root.clone();
    config.dropbox_root = dropbox_root.clone();

    let state = AppState::connect(config).await.unwrap();
    let source = dropbox_root.join("incoming-track.mp3");
    fs::write(&source, b"same audio bytes").unwrap();
    fs::write(
        source.with_extension("json"),
        json!({
            "artist": "The Example Artist",
            "album": "Example Album",
            "title": "Example Song",
            "track_number": 1
        })
        .to_string(),
    )
    .unwrap();

    let first = state.enqueue_dropbox_ingest(None, None).await.unwrap();
    let first_summary = state.run_import_job(first.job.id).await.unwrap();
    assert_eq!(first_summary.scanned_files, 1);
    assert_eq!(first_summary.published_files, 1);

    let managed = library_root
        .join("The Example Artist")
        .join("Example Album")
        .join("01 - Example Song.mp3");
    let source_path = source.to_string_lossy().to_string();
    let managed_path = managed.to_string_lossy().to_string();
    assert!(!source.exists());
    assert!(managed.exists());

    let media_file = state
        .repository()
        .media_file_by_source_path(&source_path)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(media_file.managed_path.as_deref(), Some(managed_path.as_str()));

    let full_rescan = state
        .enqueue_import_work(ImportWorkRequest {
            kind: ImportJobKind::FullRescan,
            scope: MaintenanceScope::FullLibrary,
            repair_plan: RepairPlan::default(),
            provider_filter: Vec::new(),
            source: ImportJobSource::AdminFullRescan,
            reason: Some("verify managed identity reuse".to_string()),
            related_quarantine_item_id: None,
        })
        .await
        .unwrap();
    let full_summary = state.run_import_job(full_rescan.job.id).await.unwrap();
    assert_eq!(full_summary.scanned_files, 1);
    assert_eq!(full_summary.published_files, 0);
    assert_eq!(full_summary.reused_files, 1);
    assert_eq!(full_summary.duplicate_files, 0);
    assert_eq!(full_summary.quarantined_files, 0);

    let subtree_rescan = state
        .enqueue_import_work(ImportWorkRequest {
            kind: ImportJobKind::SubtreeRescan,
            scope: MaintenanceScope::Path {
                path: managed_path.clone(),
            },
            repair_plan: RepairPlan::default(),
            provider_filter: Vec::new(),
            source: ImportJobSource::AdminSubtreeRescan,
            reason: Some("verify managed subtree identity reuse".to_string()),
            related_quarantine_item_id: None,
        })
        .await
        .unwrap();
    let subtree_summary = state.run_import_job(subtree_rescan.job.id).await.unwrap();
    assert_eq!(subtree_summary.scanned_files, 1);
    assert_eq!(subtree_summary.published_files, 0);
    assert_eq!(subtree_summary.reused_files, 1);
    assert_eq!(subtree_summary.duplicate_files, 0);
    assert_eq!(subtree_summary.quarantined_files, 0);

    let counts = state.repository().catalog_counts().await.unwrap();
    assert_eq!(counts.media_files, 1);
    assert_eq!(counts.published_media_files, 1);
    assert_eq!(counts.quarantined_media_files, 0);
    assert_eq!(counts.tracks, 1);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that catalog pipeline honors repair plan refresh flags.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn catalog_pipeline_honors_repair_plan_refresh_flags() {
    let Some(mut config) = test_config() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("harmonixia-repair-plan-{}", Uuid::new_v4()));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(&library_root).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    disable_external_providers(&mut config);
    config.library_root = library_root.clone();
    config.dropbox_root = dropbox_root.clone();

    let state = AppState::connect(config).await.unwrap();
    let source = dropbox_root.join("repair-plan-track.mp3");
    fs::write(&source, b"repair plan audio bytes").unwrap();
    fs::write(
        source.with_extension("json"),
        json!({
            "artist": "Plan Artist",
            "album": "Plan Album",
            "title": "Plan Song",
            "track_number": 3
        })
        .to_string(),
    )
    .unwrap();
    fs::write(dropbox_root.join("cover.jpg"), b"fake image bytes").unwrap();

    let minimal_plan = RepairPlan {
        refresh_provider_metadata: false,
        refresh_artwork: false,
        rewrite_sidecars: false,
        rebuild_search_projections: false,
        preserve_provenance_history: true,
        preserve_confidence_history: true,
    };
    let queued = state
        .enqueue_import_work(ImportWorkRequest {
            kind: ImportJobKind::DropboxIngest,
            scope: MaintenanceScope::FullLibrary,
            repair_plan: minimal_plan,
            provider_filter: Vec::new(),
            source: ImportJobSource::AdminDropboxIngest,
            reason: Some("verify repair plan skips refresh work".to_string()),
            related_quarantine_item_id: None,
        })
        .await
        .unwrap();
    let summary = state.run_import_job(queued.job.id).await.unwrap();
    assert_eq!(summary.published_files, 1);

    let managed = library_root
        .join("Plan Artist")
        .join("Plan Album")
        .join("03 - Plan Song.mp3");
    let managed_parent = managed.parent().unwrap();
    assert!(managed.exists());
    assert!(!managed_parent.join("harmonixia.metadata.json").exists());
    assert!(!managed_parent.join("cover.jpg").exists());

    let provider_links: i64 = sqlx::query_scalar("SELECT count(*) FROM metadata_provider_links")
        .fetch_one(state.repository().pool())
        .await
        .unwrap();
    let artwork_assets: i64 = sqlx::query_scalar("SELECT count(*) FROM artwork_assets")
        .fetch_one(state.repository().pool())
        .await
        .unwrap();
    let search_projections: i64 =
        sqlx::query_scalar("SELECT count(*) FROM catalog_search_projection")
            .fetch_one(state.repository().pool())
            .await
            .unwrap();
    assert_eq!(provider_links, 0);
    assert_eq!(artwork_assets, 0);
    assert_eq!(search_projections, 0);

    fs::write(managed_parent.join("cover.jpg"), b"managed image bytes").unwrap();
    let refresh_plan = RepairPlan {
        refresh_provider_metadata: true,
        refresh_artwork: true,
        rewrite_sidecars: true,
        rebuild_search_projections: true,
        preserve_provenance_history: false,
        preserve_confidence_history: false,
    };
    let refresh = state
        .enqueue_import_work(ImportWorkRequest {
            kind: ImportJobKind::SubtreeRescan,
            scope: MaintenanceScope::Path {
                path: managed.to_string_lossy().to_string(),
            },
            repair_plan: refresh_plan,
            provider_filter: Vec::new(),
            source: ImportJobSource::AdminSubtreeRescan,
            reason: Some("verify repair plan performs refresh work".to_string()),
            related_quarantine_item_id: None,
        })
        .await
        .unwrap();
    let refresh_summary = state.run_import_job(refresh.job.id).await.unwrap();
    assert_eq!(refresh_summary.reused_files, 1);
    assert!(managed_parent.join("harmonixia.metadata.json").exists());

    let provider_links: i64 = sqlx::query_scalar("SELECT count(*) FROM metadata_provider_links")
        .fetch_one(state.repository().pool())
        .await
        .unwrap();
    let artwork_assets: i64 = sqlx::query_scalar("SELECT count(*) FROM artwork_assets")
        .fetch_one(state.repository().pool())
        .await
        .unwrap();
    let search_projections: i64 =
        sqlx::query_scalar("SELECT count(*) FROM catalog_search_projection")
            .fetch_one(state.repository().pool())
            .await
            .unwrap();
    assert!(provider_links > 0);
    assert!(artwork_assets > 0);
    assert!(search_projections > 0);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that catalog pipeline quarantines unresolved file operation failures.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn catalog_pipeline_quarantines_unresolved_file_operation_failures() {
    let Some(mut config) = test_config() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("harmonixia-file-failure-{}", Uuid::new_v4()));
    let library_root = root.join("library-is-a-file");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(&dropbox_root).unwrap();
    fs::write(&library_root, b"not a directory").unwrap();
    disable_external_providers(&mut config);
    config.library_root = library_root.clone();
    config.dropbox_root = dropbox_root.clone();

    let state = AppState::connect(config).await.unwrap();
    let source = dropbox_root.join("file-error-track.mp3");
    fs::write(&source, b"file operation failure bytes").unwrap();
    fs::write(
        source.with_extension("json"),
        json!({
            "artist": "File Error Artist",
            "album": "File Error Album",
            "title": "File Error Song"
        })
        .to_string(),
    )
    .unwrap();

    let queued = state.enqueue_dropbox_ingest(None, None).await.unwrap();
    let summary = state.run_import_job(queued.job.id).await.unwrap();
    assert_eq!(summary.scanned_files, 1);
    assert_eq!(summary.published_files, 0);
    assert_eq!(summary.quarantined_files, 1);
    assert!(source.exists());

    let counts = state.repository().catalog_counts().await.unwrap();
    assert_eq!(counts.published_media_files, 0);
    assert_eq!(counts.quarantined_media_files, 1);
    assert_eq!(counts.tracks, 0);

    let open_file_errors: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM quarantine_items WHERE reason = 'file_error' AND status = 'open' AND retry_eligible = true",
    )
    .fetch_one(state.repository().pool())
    .await
    .unwrap();
    assert_eq!(open_file_errors, 1);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that quarantine retry reenters import pipeline and resolves on success.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn quarantine_retry_reenters_import_pipeline_and_resolves_on_success() {
    let Some(mut config) = test_config() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("harmonixia-quarantine-retry-{}", Uuid::new_v4()));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(&library_root).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    disable_external_providers(&mut config);
    config.library_root = library_root.clone();
    config.dropbox_root = dropbox_root.clone();

    let state = AppState::connect(config).await.unwrap();
    let source = dropbox_root.join("retry-track.mp3");
    fs::write(&source, b"retry audio bytes").unwrap();
    fs::write(
        source.with_extension("json"),
        json!({
            "artist": "Retry Artist",
            "album": "Retry Album",
            "title": "Retry Song"
        })
        .to_string(),
    )
    .unwrap();
    let item = state
        .insert_quarantine_item_for_tests(QuarantineItem::metadata_failure(
            source.to_string_lossy().to_string(),
        ))
        .await
        .unwrap();

    let retried = state
        .enqueue_quarantine_retries(vec![item.id], RepairPlan::default())
        .await
        .unwrap();
    let job = &retried[0].1;
    assert_eq!(job.kind, ImportJobKind::QuarantineRetry);
    assert_eq!(job.pipeline, "import_pipeline");
    assert_eq!(job.related_quarantine_item_id, Some(item.id));

    let summary = state.run_import_job(job.id).await.unwrap();
    assert_eq!(summary.published_files, 1);

    let updated = state.quarantine_item(item.id).await.unwrap().unwrap();
    assert_eq!(updated.status, QuarantineStatus::Resolved);
    assert!(!updated.retry_eligible);
    assert!(updated.media_file_id.is_some());

    let counts = state.repository().catalog_counts().await.unwrap();
    assert_eq!(counts.published_media_files, 1);
    assert_eq!(counts.quarantined_media_files, 0);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that quarantine retry stays quarantined when provider already backing off.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn quarantine_retry_stays_quarantined_when_provider_already_backing_off() {
    let Some(mut config) = test_config() else {
        return;
    };
    let root = std::env::temp_dir().join(format!(
        "harmonixia-quarantine-retry-backoff-{}",
        Uuid::new_v4()
    ));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(&library_root).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    disable_external_providers(&mut config);
    enable_external_provider(&mut config, ProviderKind::CoverArtArchive);
    config.library_root = library_root.clone();
    config.dropbox_root = dropbox_root.clone();

    let state = AppState::connect(config).await.unwrap();
    state
        .set_provider_backoff_for_tests(ProviderKind::CoverArtArchive, 600)
        .await
        .unwrap();
    let source = dropbox_root.join("retry-backoff-track.mp3");
    fs::write(&source, b"retry backoff audio bytes").unwrap();
    fs::write(
        source.with_extension("json"),
        json!({
            "artist": "Retry Backoff Artist",
            "album": "Retry Backoff Album",
            "title": "Retry Backoff Song",
            "musicbrainz_albumid": "a1d2f70d-24b2-4a07-bf2b-6d2f746b710c"
        })
        .to_string(),
    )
    .unwrap();
    let item = state
        .insert_quarantine_item_for_tests(QuarantineItem::metadata_failure(
            source.to_string_lossy().to_string(),
        ))
        .await
        .unwrap();

    let retried = state
        .enqueue_quarantine_retries(vec![item.id], RepairPlan::default())
        .await
        .unwrap();
    let job = &retried[0].1;
    assert_eq!(job.kind, ImportJobKind::QuarantineRetry);

    let summary = state.run_import_job(job.id).await.unwrap();
    assert_eq!(summary.scanned_files, 1);
    assert_eq!(summary.published_files, 0);
    assert_eq!(summary.quarantined_files, 1);
    assert!(source.exists());

    let updated = state.quarantine_item(item.id).await.unwrap().unwrap();
    assert_eq!(updated.status, QuarantineStatus::Open);
    assert!(updated.retry_eligible);
    assert!(updated.media_file_id.is_some());

    let counts = state.repository().catalog_counts().await.unwrap();
    assert_eq!(counts.published_media_files, 0);
    assert_eq!(counts.quarantined_media_files, 1);

    let last_error: Option<String> = sqlx::query_scalar(
        "SELECT last_error FROM catalog_import_work_items WHERE import_job_id = $1",
    )
    .bind(job.id)
    .fetch_one(state.repository().pool())
    .await
    .unwrap();
    assert!(last_error
        .unwrap()
        .contains("Cover Art Archive skipped: provider is in retry backoff"));

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that provider health is updated from successful provider execution.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn provider_health_is_updated_from_successful_provider_execution() {
    let Some(mut config) = test_config() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("harmonixia-provider-health-{}", Uuid::new_v4()));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(&library_root).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    disable_external_providers(&mut config);
    config.library_root = library_root.clone();
    config.dropbox_root = dropbox_root.clone();

    let state = AppState::connect(config).await.unwrap();
    let mut local_sidecars = state
        .provider(ProviderKind::LocalSidecars)
        .await
        .unwrap()
        .unwrap();
    local_sidecars.status = ProviderStatus::Degraded;
    local_sidecars.maintenance_ready = true;
    local_sidecars.failure_count = 2;
    local_sidecars.message = Some("previous local sidecar failure".to_string());
    state
        .repository()
        .save_provider_health(&local_sidecars)
        .await
        .unwrap();

    let source = dropbox_root.join("provider-health-track.mp3");
    fs::write(&source, b"provider health audio bytes").unwrap();
    fs::write(
        source.with_extension("json"),
        json!({
            "artist": "Health Artist",
            "album": "Health Album",
            "title": "Health Song"
        })
        .to_string(),
    )
    .unwrap();

    let queued = state.enqueue_dropbox_ingest(None, None).await.unwrap();
    state.run_import_job(queued.job.id).await.unwrap();

    let local_sidecars = state
        .provider(ProviderKind::LocalSidecars)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(local_sidecars.status, ProviderStatus::Healthy);
    assert!(local_sidecars.maintenance_ready);
    assert_eq!(local_sidecars.failure_count, 0);
    assert!(local_sidecars.retry_after.is_none());
    assert!(local_sidecars.message.is_none());

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that background import worker runs queued dropbox ingest job.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn background_import_worker_runs_queued_dropbox_ingest_job() {
    let Some(mut config) = test_config() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("harmonixia-worker-{}", Uuid::new_v4()));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(&library_root).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    disable_external_providers(&mut config);
    config.library_root = library_root.clone();
    config.dropbox_root = dropbox_root.clone();

    let state = AppState::connect(config).await.unwrap();
    let source = dropbox_root.join("worker-track.mp3");
    fs::write(&source, b"background worker audio bytes").unwrap();
    fs::write(
        source.with_extension("json"),
        json!({
            "artist": "Worker Artist",
            "album": "Worker Album",
            "title": "Worker Song",
            "track_number": 2
        })
        .to_string(),
    )
    .unwrap();

    let queued = state.enqueue_dropbox_ingest(None, None).await.unwrap();
    assert_eq!(queued.job.status, ImportJobStatus::Queued);

    let _services = BackgroundServices::spawn_import_worker(
        state.clone(),
        ImportWorkerConfig {
            poll_interval: Duration::from_millis(50),
            error_backoff: Duration::from_millis(50),
        },
    );
    let managed = library_root
        .join("Worker Artist")
        .join("Worker Album")
        .join("02 - Worker Song.mp3");
    let job_id = queued.job.id;

    assert!(
        eventually(Duration::from_secs(5), || {
            let state = state.clone();
            let managed = managed.clone();
            async move {
                let job = state
                    .repository()
                    .import_job(job_id)
                    .await
                    .unwrap()
                    .unwrap();
                job.status == ImportJobStatus::Completed && managed.exists()
            }
        })
        .await,
        "background worker did not complete queued dropbox ingest"
    );

    let job = state.repository().import_job(job_id).await.unwrap().unwrap();
    assert_eq!(job.status, ImportJobStatus::Completed);
    let counts = state.repository().catalog_counts().await.unwrap();
    assert_eq!(counts.published_media_files, 1);

    let _ = fs::remove_dir_all(root);
}

#[tokio::test]
/// Verifies that dropbox watcher enqueues stable file and worker publishes it.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn dropbox_watcher_enqueues_stable_file_and_worker_publishes_it() {
    let Some(mut config) = test_config() else {
        return;
    };
    let root = std::env::temp_dir().join(format!("harmonixia-watcher-{}", Uuid::new_v4()));
    let library_root = root.join("library");
    let dropbox_root = root.join("dropbox");
    fs::create_dir_all(&library_root).unwrap();
    fs::create_dir_all(&dropbox_root).unwrap();
    disable_external_providers(&mut config);
    config.library_root = library_root.clone();
    config.dropbox_root = dropbox_root.clone();

    let state = AppState::connect(config).await.unwrap();
    let _services = BackgroundServices::spawn(
        state.clone(),
        BackgroundServiceConfig {
            import_worker: ImportWorkerConfig {
                poll_interval: Duration::from_millis(50),
                error_backoff: Duration::from_millis(50),
            },
            dropbox_watcher: DropboxWatcherConfig {
                poll_interval: Duration::from_millis(50),
                stable_for: Duration::from_millis(100),
                error_backoff: Duration::from_millis(50),
            },
            sonos: Default::default(),
        },
    );

    let source = dropbox_root.join("watcher-track.mp3");
    fs::write(&source, b"partial").unwrap();
    tokio::time::sleep(Duration::from_millis(60)).await;
    fs::write(&source, b"finished watcher audio bytes").unwrap();
    fs::write(
        source.with_extension("json"),
        json!({
            "artist": "Watcher Artist",
            "album": "Watcher Album",
            "title": "Watcher Song",
            "track_number": 3
        })
        .to_string(),
    )
    .unwrap();

    let managed = library_root
        .join("Watcher Artist")
        .join("Watcher Album")
        .join("03 - Watcher Song.mp3");

    assert!(
        eventually(Duration::from_secs(5), || {
            let state = state.clone();
            let managed = managed.clone();
            async move {
                let counts = state.repository().catalog_counts().await.unwrap();
                let jobs = state.import_jobs().await.unwrap();
                counts.published_media_files == 1
                    && managed.exists()
                    && jobs.iter().any(|job| {
                        job.source == ImportJobSource::DropboxWatcher
                            && job.status == ImportJobStatus::Completed
                    })
            }
        })
        .await,
        "dropbox watcher did not enqueue and publish stable media file"
    );

    let jobs = state.import_jobs().await.unwrap();
    let watcher_jobs = jobs
        .iter()
        .filter(|job| job.source == ImportJobSource::DropboxWatcher)
        .collect::<Vec<_>>();
    assert_eq!(watcher_jobs.len(), 1);
    assert_eq!(watcher_jobs[0].kind, ImportJobKind::DropboxIngest);
    assert_eq!(watcher_jobs[0].status, ImportJobStatus::Completed);

    let _ = fs::remove_dir_all(root);
}

fn schema_requires_field(schema: &Value, field: &str) -> bool {
    schema["required"]
        .as_array()
        .map(|required| required.iter().any(|value| value.as_str() == Some(field)))
        .unwrap_or(false)
}

fn schema_property_is_nullable(schema: &Value, field: &str) -> bool {
    schema["properties"]
        .get(field)
        .map(schema_is_nullable)
        .unwrap_or(false)
}

fn schema_is_nullable(schema: &Value) -> bool {
    if let Some(schema_type) = schema.get("type") {
        match schema_type {
            Value::String(value) => return value == "null",
            Value::Array(values) => {
                return values.iter().any(|value| value.as_str() == Some("null"));
            }
            _ => {}
        }
    }

    for composite in ["anyOf", "oneOf", "allOf"] {
        if schema
            .get(composite)
            .and_then(Value::as_array)
            .map(|schemas| schemas.iter().any(schema_is_nullable))
            .unwrap_or(false)
        {
            return true;
        }
    }

    false
}

#[tokio::test]
/// Verifies that openapi documents maintenance and provider repair endpoints.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn openapi_documents_maintenance_and_provider_repair_endpoints() {
    let Some(state) = test_state().await else {
        return;
    };
    let app = router(state);
    let response = app
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
            .unwrap();
    let paths = body["paths"].as_object().unwrap();

    assert!(paths.contains_key("/api/v1/bootstrap/status"));
    assert!(paths.contains_key("/api/v1/bootstrap/first-admin"));
    assert!(paths.contains_key("/api/v1/auth/me"));
    assert!(paths.contains_key("/api/v1/admin/users"));
    assert!(paths.contains_key("/api/v1/admin/users/{user_id}"));
    assert!(paths.contains_key(
        "/api/v1/admin/users/{user_id}/password-reset"
    ));
    assert!(paths.contains_key("/api/v1/admin/system/config"));
    assert!(paths.contains_key("/api/v1/admin/providers/settings"));
    assert!(paths.contains_key(
        "/api/v1/admin/providers/{provider}/settings"
    ));
    assert!(paths.contains_key("/api/v1/admin/maintenance/rescans/full"));
    assert!(paths.contains_key("/api/v1/admin/maintenance/rescans/subtree"));
    assert!(paths.contains_key("/api/v1/admin/maintenance/summary"));
    assert!(paths.contains_key("/api/v1/admin/providers/{provider}/repair"));
    assert!(paths.contains_key("/api/v1/admin/quarantine/retry"));
    assert!(paths.contains_key("/api/v1/catalog/search"));
    assert!(paths.contains_key("/api/v1/catalog/artists"));
    assert!(paths.contains_key("/api/v1/catalog/albums"));
    assert!(paths.contains_key("/api/v1/catalog/tracks"));
    assert!(paths.contains_key("/api/v1/catalog/podcasts"));
    assert!(paths.contains_key("/api/v1/catalog/podcasts/{podcast_id}"));
    assert!(paths.contains_key(
        "/api/v1/catalog/podcasts/{podcast_id}/episodes"
    ));
    assert!(paths.contains_key("/api/v1/catalog/episodes"));
    assert!(paths.contains_key("/api/v1/catalog/episodes/{episode_id}"));
    assert!(paths.contains_key(
        "/api/v1/catalog/episodes/{episode_id}/resume"
    ));
    assert!(paths.contains_key(
        "/api/v1/media/{item_type}/{item_id}/original"
    ));
    assert!(paths.contains_key(
        "/api/v1/media/{item_type}/{item_id}/original/download"
    ));
    assert!(paths.contains_key(
        "/api/v1/media/{item_type}/{item_id}/transcode/{profile}"
    ));
    assert!(paths.contains_key(
        "/api/v1/media/{item_type}/{item_id}/hls/{profile}/manifest.m3u8"
    ));
    assert!(paths.contains_key(
        "/api/v1/media/{item_type}/{item_id}/hls/{profile}/segments/{segment}"
    ));
    assert!(paths.contains_key("/api/v1/admin/media/transcode-slots"));
    assert!(paths.contains_key("/api/v1/playlists"));
    assert!(paths.contains_key("/api/v1/playlists/{playlist_id}"));
    assert!(paths.contains_key("/api/v1/playlists/{playlist_id}/items"));
    assert!(paths.contains_key(
        "/api/v1/playlists/{playlist_id}/items/{playlist_item_id}"
    ));
    assert!(paths.contains_key(
        "/api/v1/me/playback/progress/{item_type}/{item_id}"
    ));
    assert!(paths.contains_key("/api/v1/me/playback/progress"));
    assert!(paths.contains_key("/api/v1/me/playback/history"));
    assert!(paths.contains_key("/api/v1/sonos/targets"));
    for route in ["play", "pause", "resume", "stop", "seek", "next", "previous"] {
        assert!(paths.contains_key(&format!(
            "/api/v1/sonos/targets/{{target_id}}/{route}"
        )));
    }
    for route in ["play", "pause", "resume", "seek", "next", "previous"] {
        let operation =
            &paths[&format!("/api/v1/sonos/targets/{{target_id}}/{route}")]["post"];
        assert!(operation["responses"].get("409").is_some());
        assert!(operation["responses"]["409"]["description"]
            .as_str()
            .unwrap_or_default()
            .contains("reconnecting"));
    }
    assert!(paths.contains_key("/api/v1/sonos/media/{token}"));
    let sonos_targets = &paths["/api/v1/sonos/targets"]["get"];
    assert!(sonos_targets["tags"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tag| tag.as_str() == Some("sonos")));
    assert!(sonos_targets
        .get("security")
        .and_then(Value::as_array)
        .map(|entries| entries.iter().any(|entry| entry.get("basicAuth").is_some()))
        .unwrap_or(false));
    assert!(sonos_targets["responses"].get("401").is_some());
    assert_eq!(
        sonos_targets["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
            .as_str(),
        Some("#/components/schemas/SonosTargetsResponse")
    );

    let sonos_media = &paths["/api/v1/sonos/media/{token}"]["get"];
    assert!(sonos_media["tags"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tag| tag.as_str() == Some("sonos")));
    assert!(!sonos_media
        .get("security")
        .and_then(Value::as_array)
        .map(|entries| entries.iter().any(|entry| entry.get("basicAuth").is_some()))
        .unwrap_or(false));
    assert!(sonos_media["responses"].get("401").is_none());

    let sonos_play = &paths["/api/v1/sonos/targets/{target_id}/play"]["post"];
    assert!(sonos_play["tags"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tag| tag.as_str() == Some("sonos")));
    assert!(sonos_play
        .get("security")
        .and_then(Value::as_array)
        .map(|entries| entries.iter().any(|entry| entry.get("basicAuth").is_some()))
        .unwrap_or(false));
    assert_eq!(
        sonos_play["responses"]["200"]["content"]["application/json"]["schema"]["$ref"]
            .as_str(),
        Some("#/components/schemas/SonosPlaybackResponse")
    );
    for route in ["play", "pause", "resume", "seek", "next", "previous"] {
        assert!(
            paths[&format!("/api/v1/sonos/targets/{{target_id}}/{route}")]["post"]["responses"]
                .get("409")
                .is_some(),
            "Sonos {route} should document a 409 response"
        );
    }
    assert_eq!(
        paths["/api/v1/sonos/targets/{target_id}/seek"]["post"]["requestBody"]["content"]
            ["application/json"]["schema"]["$ref"]
            .as_str(),
        Some("#/components/schemas/SonosSeekRequest")
    );

    let schemes = body["components"]["securitySchemes"]
        .as_object()
        .unwrap();
    assert_eq!(schemes["basicAuth"]["type"], "http");
    assert_eq!(schemes["basicAuth"]["scheme"], "basic");
    assert!(body["tags"]
        .as_array()
        .unwrap()
        .iter()
        .any(|tag| tag["name"].as_str() == Some("sonos")));

    let search_parameters = paths["/api/v1/catalog/search"]["get"]["parameters"]
        .as_array()
        .unwrap()
        .iter()
        .map(|parameter| parameter["name"].as_str().unwrap())
        .collect::<Vec<_>>();
    for parameter in ["q", "limit", "year", "genre", "format", "media_type"] {
        assert!(search_parameters.contains(&parameter));
    }
    let search_schema =
        &body["components"]["schemas"]["CatalogSearchResponse"]["properties"];
    assert!(search_schema.get("artists").is_some());
    assert!(search_schema.get("albums").is_some());
    assert!(search_schema.get("tracks").is_some());
    assert!(search_schema.get("podcasts").is_some());
    assert!(search_schema.get("episodes").is_some());
    assert!(search_schema.get("playlists").is_some());

    let schemas = body["components"]["schemas"].as_object().unwrap();
    for schema in [
        "Podcast",
        "PodcastResponse",
        "BrowsePodcastsResponse",
        "Episode",
        "EpisodeResponse",
        "BrowseEpisodesResponse",
        "EpisodeResumeResponse",
        "PlaybackProgress",
        "PlaybackProgressWriteRequest",
        "PlaybackProgressWriteResponse",
        "DashboardSummaryResponse",
        "ErrorResponseDetails",
        "SonosDeliveryKind",
        "SonosErrorReason",
        "SonosGroupTarget",
        "SonosNextItemSummary",
        "SonosPlaybackResponse",
        "SonosPlaybackTarget",
        "SonosPlayRequest",
        "SonosPlaySourceType",
        "SonosSeekRequest",
        "SonosSessionStatus",
        "SonosSessionSummary",
        "SonosSignedClaim",
        "SonosSpeakerTarget",
        "SonosTargetsResponse",
        "SonosTransportState",
    ] {
        assert!(schemas.contains_key(schema), "missing schema {schema}");
    }

    let system_config_schema = &schemas["SystemConfig"];
    assert!(system_config_schema["properties"]
        .as_object()
        .unwrap()
        .contains_key("public_base_url"));
    assert!(schema_requires_field(
        system_config_schema,
        "public_base_url"
    ));
    assert!(schema_property_is_nullable(
        system_config_schema,
        "public_base_url"
    ));

    let system_config_update_schema = &schemas["SystemConfigUpdateRequest"];
    assert!(system_config_update_schema["properties"]
        .as_object()
        .unwrap()
        .contains_key("public_base_url"));
    assert!(!schema_requires_field(
        system_config_update_schema,
        "public_base_url"
    ));
    assert!(schema_property_is_nullable(
        system_config_update_schema,
        "public_base_url"
    ));

    let error_response_schema = &schemas["ErrorResponse"];
    assert!(error_response_schema["properties"]
        .as_object()
        .unwrap()
        .contains_key("details"));
    assert!(!schema_requires_field(error_response_schema, "details"));
    assert!(!schema_property_is_nullable(error_response_schema, "details"));
    assert!(schemas["ErrorResponseDetails"]["properties"]
        .as_object()
        .unwrap()
        .contains_key("reason"));
    assert!(!schema_requires_field(
        &schemas["ErrorResponseDetails"],
        "reason"
    ));
    assert!(!schema_property_is_nullable(
        &schemas["ErrorResponseDetails"],
        "reason"
    ));

    for field in [
        "room_name",
        "volume_percent",
        "muted",
        "transport_state",
    ] {
        assert!(schema_requires_field(&schemas["SonosSpeakerTarget"], field));
        assert!(schema_property_is_nullable(
            &schemas["SonosSpeakerTarget"],
            field
        ));
    }
    for field in ["volume_percent", "muted", "transport_state"] {
        assert!(schema_requires_field(&schemas["SonosGroupTarget"], field));
        assert!(schema_property_is_nullable(
            &schemas["SonosGroupTarget"],
            field
        ));
    }
    for field in ["current_duration_seconds", "next_item"] {
        assert!(schema_requires_field(&schemas["SonosSessionSummary"], field));
        assert!(schema_property_is_nullable(
            &schemas["SonosSessionSummary"],
            field
        ));
    }
    assert!(schema_requires_field(
        &schemas["SonosPlaybackResponse"],
        "session"
    ));
    assert!(schema_property_is_nullable(
        &schemas["SonosPlaybackResponse"],
        "session"
    ));
    let playback_target_schema =
        &schemas["SonosPlaybackResponse"]["properties"]["target"]["anyOf"];
    assert!(playback_target_schema
        .as_array()
        .unwrap()
        .iter()
        .any(|schema| schema["$ref"] == "#/components/schemas/SonosSpeakerTarget"));
    assert!(playback_target_schema
        .as_array()
        .unwrap()
        .iter()
        .any(|schema| schema["$ref"] == "#/components/schemas/SonosGroupTarget"));
    assert!(!schema_requires_field(
        &schemas["SonosSessionSummary"],
        "reconnect_seconds_remaining"
    ));
    assert!(!schema_property_is_nullable(
        &schemas["SonosSessionSummary"],
        "reconnect_seconds_remaining"
    ));

    assert_eq!(
        paths["/api/v1/catalog/podcasts"]["get"]["responses"]["200"]["content"]
            ["application/json"]["schema"]["$ref"]
            .as_str(),
        Some("#/components/schemas/BrowsePodcastsResponse")
    );
    assert_eq!(
        paths["/api/v1/catalog/podcasts/{podcast_id}"]["get"]["responses"]["200"]["content"]
            ["application/json"]["schema"]["$ref"]
            .as_str(),
        Some("#/components/schemas/PodcastResponse")
    );
    assert_eq!(
        paths["/api/v1/catalog/podcasts/{podcast_id}/episodes"]["get"]["responses"]["200"]
            ["content"]["application/json"]["schema"]["$ref"]
            .as_str(),
        Some("#/components/schemas/BrowseEpisodesResponse")
    );
    assert_eq!(
        paths["/api/v1/catalog/episodes/{episode_id}"]["get"]["responses"]["200"]["content"]
            ["application/json"]["schema"]["$ref"]
            .as_str(),
        Some("#/components/schemas/EpisodeResponse")
    );
    assert_eq!(
        paths["/api/v1/catalog/episodes/{episode_id}/resume"]["get"]["responses"]["200"]
            ["content"]["application/json"]["schema"]["$ref"]
            .as_str(),
        Some("#/components/schemas/EpisodeResumeResponse")
    );
    assert_eq!(
        paths["/api/v1/catalog/episodes/{episode_id}/resume"]["put"]["requestBody"]["content"]
            ["application/json"]["schema"]["$ref"]
            .as_str(),
        Some("#/components/schemas/PlaybackProgressWriteRequest")
    );
    assert_eq!(
        paths["/api/v1/catalog/episodes/{episode_id}/resume"]["put"]["responses"]["200"]
            ["content"]["application/json"]["schema"]["$ref"]
            .as_str(),
        Some("#/components/schemas/PlaybackProgressWriteResponse")
    );
    let podcast_response_schema = &schemas["PodcastResponse"]["properties"];
    assert_eq!(
        podcast_response_schema["podcast"]["$ref"].as_str(),
        Some("#/components/schemas/Podcast")
    );
    let episode_response_schema = &schemas["EpisodeResponse"]["properties"];
    assert_eq!(
        episode_response_schema["podcast"]["$ref"].as_str(),
        Some("#/components/schemas/Podcast")
    );
    assert_eq!(
        episode_response_schema["episode"]["$ref"].as_str(),
        Some("#/components/schemas/Episode")
    );
    assert!(episode_response_schema.get("resume").is_some());
    let episode_resume_schema = &schemas["EpisodeResumeResponse"]["properties"];
    assert!(episode_resume_schema.get("episode_id").is_some());
    assert!(episode_resume_schema.get("resume").is_some());

    let protected_operations = [
        ("/api/v1/auth/me", "get"),
        ("/api/v1/admin/users", "get"),
        ("/api/v1/admin/users", "post"),
        ("/api/v1/admin/users/{user_id}", "delete"),
        (
            "/api/v1/admin/users/{user_id}/password-reset",
            "post",
        ),
        ("/api/v1/admin/system/config", "get"),
        ("/api/v1/admin/system/config", "put"),
        ("/api/v1/admin/providers/settings", "get"),
        ("/api/v1/admin/providers/{provider}/settings", "patch"),
        ("/api/v1/admin/maintenance/rescans/full", "post"),
        ("/api/v1/admin/maintenance/rescans/subtree", "post"),
        ("/api/v1/admin/maintenance/provider-refreshes", "post"),
        ("/api/v1/admin/maintenance/summary", "get"),
        ("/api/v1/admin/maintenance/readiness", "get"),
        ("/api/v1/admin/providers/health", "get"),
        ("/api/v1/admin/providers/{provider}/repair", "post"),
        ("/api/v1/admin/quarantine/retry", "post"),
        ("/api/v1/admin/quarantine/{item_id}/retry", "post"),
        ("/api/v1/catalog/search", "get"),
        ("/api/v1/catalog/artists", "get"),
        ("/api/v1/catalog/albums", "get"),
        ("/api/v1/catalog/tracks", "get"),
        ("/api/v1/catalog/podcasts", "get"),
        ("/api/v1/catalog/podcasts/{podcast_id}", "get"),
        (
            "/api/v1/catalog/podcasts/{podcast_id}/episodes",
            "get",
        ),
        ("/api/v1/catalog/episodes", "get"),
        ("/api/v1/catalog/episodes/{episode_id}", "get"),
        ("/api/v1/catalog/episodes/{episode_id}/resume", "get"),
        ("/api/v1/catalog/episodes/{episode_id}/resume", "put"),
        ("/api/v1/media/{item_type}/{item_id}/original", "get"),
        (
            "/api/v1/media/{item_type}/{item_id}/original/download",
            "get",
        ),
        (
            "/api/v1/media/{item_type}/{item_id}/transcode/{profile}",
            "get",
        ),
        (
            "/api/v1/media/{item_type}/{item_id}/hls/{profile}/manifest.m3u8",
            "get",
        ),
        (
            "/api/v1/media/{item_type}/{item_id}/hls/{profile}/segments/{segment}",
            "get",
        ),
        ("/api/v1/admin/media/transcode-slots", "get"),
        ("/api/v1/playlists", "get"),
        ("/api/v1/playlists", "post"),
        ("/api/v1/playlists/{playlist_id}", "get"),
        ("/api/v1/playlists/{playlist_id}", "put"),
        ("/api/v1/playlists/{playlist_id}", "delete"),
        ("/api/v1/playlists/{playlist_id}/items", "get"),
        ("/api/v1/playlists/{playlist_id}/items", "post"),
        ("/api/v1/playlists/{playlist_id}/items", "put"),
        (
            "/api/v1/playlists/{playlist_id}/items/{playlist_item_id}",
            "delete",
        ),
        (
            "/api/v1/me/playback/progress/{item_type}/{item_id}",
            "put",
        ),
        (
            "/api/v1/me/playback/progress/{item_type}/{item_id}",
            "get",
        ),
        ("/api/v1/me/playback/progress", "get"),
        ("/api/v1/me/playback/history", "get"),
        ("/api/v1/me/playback/history", "post"),
    ];

    for (path, method) in protected_operations.iter().copied() {
        let operation = &paths[path][method];
        assert!(operation["security"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry.get("basicAuth").is_some()));
        assert!(operation["responses"].get("401").is_some());
    }

    for (path, method) in protected_operations
        .iter()
        .copied()
        .filter(|(path, _)| path.starts_with("/api/v1/admin/"))
    {
        let operation = &paths[path][method];
        assert!(operation["responses"].get("403").is_some());
    }
}

#[test]
/// Verifies that import job kinds cover requested maintenance flows.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn import_job_kinds_cover_requested_maintenance_flows() {
    assert_eq!(ImportJobKind::FullRescan.api_name(), "full_rescan");
    assert_eq!(ImportJobKind::SubtreeRescan.api_name(), "subtree_rescan");
    assert_eq!(ImportJobKind::ProviderRepair.api_name(), "provider_repair");
    assert_eq!(ImportJobKind::QuarantineRetry.api_name(), "quarantine_retry");
}

#[test]
/// Verifies that maintenance scope idempotency distinguishes full and path repairs.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn maintenance_scope_idempotency_distinguishes_full_and_path_repairs() {
    assert_ne!(
        MaintenanceScope::FullLibrary.idempotency_fragment(),
        MaintenanceScope::Path {
            path: "/srv/harmonixia/library".into()
        }
        .idempotency_fragment()
    );
}
