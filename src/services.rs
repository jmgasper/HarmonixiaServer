use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant, SystemTime},
};

use tokio::{task::JoinHandle, time::sleep};
use walkdir::WalkDir;

use crate::{
    media::is_supported_media_path,
    sonos::{self, SonosRuntimeConfig},
    state::AppState,
};

#[derive(Debug, Clone)]
/// Represents background service config in the background import worker and dropbox watcher runtime services.
///
/// Functionality: Carries fields `import_worker`, `dropbox_watcher` for background import worker and dropbox watcher runtime services.
/// Dependencies: depends on `ImportWorkerConfig`, `DropboxWatcherConfig` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/lib.rs`, `src/main.rs`, `src/services.rs`, `tests/maintenance_api.rs`.
pub struct BackgroundServiceConfig {
    pub import_worker: ImportWorkerConfig,
    pub dropbox_watcher: DropboxWatcherConfig,
    pub sonos: SonosRuntimeConfig,
}

impl Default for BackgroundServiceConfig {
    /// Builds the default configuration for background import worker and dropbox watcher runtime services.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn default() -> Self {
        Self {
            import_worker: ImportWorkerConfig::default(),
            dropbox_watcher: DropboxWatcherConfig::default(),
            sonos: SonosRuntimeConfig::default(),
        }
    }
}

#[derive(Debug, Clone)]
/// Represents import worker config in the background import worker and dropbox watcher runtime services.
///
/// Functionality: Carries fields `poll_interval`, `error_backoff` for background import worker and dropbox watcher runtime services.
/// Dependencies: depends on `Duration`, `Duration` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/services.rs`, `tests/maintenance_api.rs`.
pub struct ImportWorkerConfig {
    pub poll_interval: Duration,
    pub error_backoff: Duration,
}

impl Default for ImportWorkerConfig {
    /// Builds the default configuration for background import worker and dropbox watcher runtime services.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(2),
            error_backoff: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone)]
/// Represents dropbox watcher config in the background import worker and dropbox watcher runtime services.
///
/// Functionality: Carries fields `poll_interval`, `stable_for`, `error_backoff` for background import worker and dropbox watcher runtime services.
/// Dependencies: depends on `Duration`, `Duration`, `Duration` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/services.rs`, `tests/maintenance_api.rs`.
pub struct DropboxWatcherConfig {
    pub poll_interval: Duration,
    pub stable_for: Duration,
    pub error_backoff: Duration,
}

impl Default for DropboxWatcherConfig {
    /// Builds the default configuration for background import worker and dropbox watcher runtime services.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(5),
            stable_for: Duration::from_secs(10),
            error_backoff: Duration::from_secs(10),
        }
    }
}

#[derive(Debug)]
/// Represents background services in the background import worker and dropbox watcher runtime services.
///
/// Functionality: Carries fields `handles` for background import worker and dropbox watcher runtime services.
/// Dependencies: depends on `Vec<JoinHandle<()>>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/lib.rs`, `src/main.rs`, `src/services.rs`, `tests/maintenance_api.rs`.
pub struct BackgroundServices {
    handles: Vec<JoinHandle<()>>,
}

impl BackgroundServices {
    /// Spawns asynchronous work for background import worker and dropbox watcher runtime services.
    ///
    /// Inputs:
    /// - `state`: `AppState`; expected to be Axum application state with a live repository and runtime configuration.
    /// - `config`: `BackgroundServiceConfig`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn spawn(state: AppState, config: BackgroundServiceConfig) -> Self {
        let mut handles = Vec::with_capacity(4);
        handles.push(tokio::spawn(import_worker_loop(
            state.clone(),
            config.import_worker,
        )));
        handles.push(tokio::spawn(dropbox_watcher_loop(
            state.clone(),
            config.dropbox_watcher,
        )));
        let sonos_config = config.sonos;
        handles.push(tokio::spawn(sonos::runtime_loop(
            state.clone(),
            sonos_config.clone(),
        )));
        handles.push(tokio::spawn(sonos::active_session_loop(
            state,
            sonos_config.request_timeout,
        )));
        Self { handles }
    }

    /// Spawns asynchronous work for background import worker and dropbox watcher runtime services.
    ///
    /// Inputs:
    /// - `state`: `AppState`; expected to be Axum application state with a live repository and runtime configuration.
    /// - `config`: `ImportWorkerConfig`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn spawn_import_worker(state: AppState, config: ImportWorkerConfig) -> Self {
        Self {
            handles: vec![tokio::spawn(import_worker_loop(state, config))],
        }
    }

    /// Spawns asynchronous work for background import worker and dropbox watcher runtime services.
    ///
    /// Inputs:
    /// - `state`: `AppState`; expected to be Axum application state with a live repository and runtime configuration.
    /// - `config`: `DropboxWatcherConfig`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn spawn_dropbox_watcher(state: AppState, config: DropboxWatcherConfig) -> Self {
        Self {
            handles: vec![tokio::spawn(dropbox_watcher_loop(state, config))],
        }
    }
}

impl Drop for BackgroundServices {
    /// Releases owned resources and performs cleanup.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

/// Handles import worker loop for background import worker and dropbox watcher runtime services.
///
/// Inputs:
/// - `state`: `AppState`; expected to be Axum application state with a live repository and runtime configuration.
/// - `config`: `ImportWorkerConfig`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors.
async fn import_worker_loop(state: AppState, config: ImportWorkerConfig) {
    loop {
        match state.run_next_import_job().await {
            Ok(Some(summary)) => {
                tracing::info!(
                    job_id = %summary.job.id,
                    kind = ?summary.job.kind,
                    status = ?summary.job.status,
                    scanned_files = summary.scanned_files,
                    published_files = summary.published_files,
                    reused_files = summary.reused_files,
                    quarantined_files = summary.quarantined_files,
                    failed_files = summary.failed_files,
                    "background import job finished"
                );
            }
            Ok(None) => sleep(config.poll_interval).await,
            Err(error) => {
                tracing::error!(%error, "background import worker failed");
                sleep(config.error_backoff).await;
            }
        }
    }
}

/// Handles dropbox watcher loop for background import worker and dropbox watcher runtime services.
///
/// Inputs:
/// - `state`: `AppState`; expected to be Axum application state with a live repository and runtime configuration.
/// - `config`: `DropboxWatcherConfig`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors.
async fn dropbox_watcher_loop(state: AppState, config: DropboxWatcherConfig) {
    let mut observed = HashMap::new();

    loop {
        match scan_dropbox_once(&state, &config, &mut observed).await {
            Ok(()) => sleep(config.poll_interval).await,
            Err(error) => {
                tracing::error!(%error, "dropbox watcher failed");
                sleep(config.error_backoff).await;
            }
        }
    }
}

/// Handles scan dropbox once for background import worker and dropbox watcher runtime services.
///
/// Inputs:
/// - `state`: `&AppState`; expected to be Axum application state with a live repository and runtime configuration.
/// - `config`: `&DropboxWatcherConfig`; expected to be a value satisfying the type contract shown in the function signature.
/// - `observed`: `&mut HashMap<PathBuf, ObservedFile>`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `()` on success or `crate::error::ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `crate::error::ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn scan_dropbox_once(
    state: &AppState,
    config: &DropboxWatcherConfig,
    observed: &mut HashMap<PathBuf, ObservedFile>,
) -> Result<(), crate::error::ApiError> {
    let root = PathBuf::from(state.system_config().dropbox_root);
    if root.as_os_str().is_empty() || !root.exists() {
        observed.clear();
        return Ok(());
    }

    let now = Instant::now();
    let mut present = HashSet::new();

    for (path, fingerprint) in supported_media_files(&root) {
        present.insert(path.clone());
        let stable_fingerprint = {
            let entry = observed
                .entry(path.clone())
                .and_modify(|entry| {
                    if entry.fingerprint != fingerprint {
                        entry.fingerprint = fingerprint.clone();
                        entry.changed_at = now;
                        entry.enqueued_fingerprint = None;
                    }
                })
                .or_insert_with(|| ObservedFile {
                    fingerprint: fingerprint.clone(),
                    changed_at: now,
                    enqueued_fingerprint: None,
                });

            if now.duration_since(entry.changed_at) < config.stable_for {
                None
            } else if entry.enqueued_fingerprint.as_ref() == Some(&entry.fingerprint) {
                None
            } else {
                Some(entry.fingerprint.clone())
            }
        };

        let Some(stable_fingerprint) = stable_fingerprint else {
            continue;
        };
        let outcome = state.enqueue_dropbox_watcher_ingest(&path).await?;
        if let Some(entry) = observed.get_mut(&path) {
            if entry.fingerprint == stable_fingerprint {
                entry.enqueued_fingerprint = Some(stable_fingerprint);
            }
        }
        tracing::info!(
            path = %path.display(),
            job_id = %outcome.job.id,
            reused_existing = outcome.reused_existing,
            "dropbox watcher enqueued stable media file"
        );
    }

    observed.retain(|path, _| present.contains(path));
    Ok(())
}

/// Handles supported media files for background import worker and dropbox watcher runtime services.
///
/// Inputs:
/// - `root`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `Vec<(PathBuf, FileFingerprint)>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn supported_media_files(root: &Path) -> Vec<(PathBuf, FileFingerprint)> {
    if root.is_file() {
        return fingerprint(root)
            .filter(|_| is_supported_media_path(root))
            .map(|fingerprint| vec![(root.to_path_buf(), fingerprint)])
            .unwrap_or_default();
    }

    let mut files = WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| entry.into_path())
        .filter(|path| is_supported_media_path(path))
        .filter_map(|path| fingerprint(&path).map(|fingerprint| (path, fingerprint)))
        .collect::<Vec<_>>();
    files.sort_by(|left, right| left.0.cmp(&right.0));
    files
}

/// Handles fingerprint for background import worker and dropbox watcher runtime services.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `Some(FileFingerprint)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn fingerprint(path: &Path) -> Option<FileFingerprint> {
    let metadata = fs::metadata(path).ok()?;
    Some(FileFingerprint {
        len: metadata.len(),
        modified: metadata.modified().ok(),
    })
}

#[derive(Debug, Clone)]
/// Represents observed file in the background import worker and dropbox watcher runtime services.
///
/// Functionality: Carries fields `fingerprint`, `changed_at`, `enqueued_fingerprint` for background import worker and dropbox watcher runtime services.
/// Dependencies: depends on `FileFingerprint`, `Instant`, `Option<FileFingerprint>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/services.rs`.
struct ObservedFile {
    fingerprint: FileFingerprint,
    changed_at: Instant,
    enqueued_fingerprint: Option<FileFingerprint>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Represents file fingerprint in the background import worker and dropbox watcher runtime services.
///
/// Functionality: Carries fields `len`, `modified` for background import worker and dropbox watcher runtime services.
/// Dependencies: depends on `u64`, `Option<SystemTime>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/services.rs`.
struct FileFingerprint {
    len: u64,
    modified: Option<SystemTime>,
}
