use std::{
    collections::HashMap,
    fmt,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    process::{ExitStatus, Stdio},
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::Duration,
};

use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncRead, ReadBuf},
    process::{Child, ChildStdout, Command},
    sync::watch,
};
use uuid::Uuid;

use crate::domain::{AacTranscodeProfile, TranscodeSlotUsage};

#[derive(Debug, Clone)]
/// Represents transcode admission in the ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Functionality: Carries fields `inner` for ffmpeg-backed direct AAC and HLS transcoding runtime.
/// Dependencies: depends on `Arc<TranscodeAdmissionInner>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/transcode.rs`.
pub struct TranscodeAdmission {
    inner: Arc<TranscodeAdmissionInner>,
}

#[derive(Debug)]
/// Represents transcode admission inner in the ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Functionality: Carries fields `state` for ffmpeg-backed direct AAC and HLS transcoding runtime.
/// Dependencies: depends on `Mutex<TranscodeAdmissionState>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/transcode.rs`.
struct TranscodeAdmissionInner {
    state: Mutex<TranscodeAdmissionState>,
}

#[derive(Debug)]
/// Represents transcode admission state in the ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Functionality: Carries fields `limit`, `in_use` for ffmpeg-backed direct AAC and HLS transcoding runtime.
/// Dependencies: depends on `u32`, `u32` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/transcode.rs`.
struct TranscodeAdmissionState {
    limit: u32,
    in_use: u32,
}

#[derive(Debug)]
/// Represents transcode slot in the ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Functionality: Carries fields `admission` for ffmpeg-backed direct AAC and HLS transcoding runtime.
/// Dependencies: depends on `Arc<TranscodeAdmissionInner>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/transcode.rs`.
pub struct TranscodeSlot {
    admission: Arc<TranscodeAdmissionInner>,
}

#[derive(Clone)]
/// Represents hls generation coordinator in the ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Functionality: Carries fields `inner` for ffmpeg-backed direct AAC and HLS transcoding runtime.
/// Dependencies: depends on `Arc<Mutex<HashMap<PathBuf` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/transcode.rs`.
pub struct HlsGenerationCoordinator {
    inner: Arc<Mutex<HashMap<PathBuf, watch::Sender<bool>>>>,
}

/// Represents hls generation lease in the ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Functionality: Enumerates `Start`, `Wait` states or choices for ffmpeg-backed direct AAC and HLS transcoding runtime.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/media.rs`, `src/state.rs`, `src/transcode.rs`.
pub enum HlsGenerationLease {
    Start(HlsGenerationGuard),
    Wait(HlsGenerationWaiter),
}

/// Represents hls generation guard in the ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Functionality: Carries fields `key`, `inner`, `done` for ffmpeg-backed direct AAC and HLS transcoding runtime.
/// Dependencies: depends on `PathBuf`, `Arc<Mutex<HashMap<PathBuf`, `watch::Sender<bool>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/transcode.rs`.
pub struct HlsGenerationGuard {
    key: PathBuf,
    inner: Arc<Mutex<HashMap<PathBuf, watch::Sender<bool>>>>,
    done: watch::Sender<bool>,
}

/// Represents hls generation waiter in the ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Functionality: Carries fields `done` for ffmpeg-backed direct AAC and HLS transcoding runtime.
/// Dependencies: depends on `watch::Receiver<bool>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/transcode.rs`.
pub struct HlsGenerationWaiter {
    done: watch::Receiver<bool>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Represents transcode capacity exhausted in the ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Functionality: Acts as a marker or zero-field value for ffmpeg-backed direct AAC and HLS transcoding runtime.
/// Dependencies: has no direct field dependencies beyond derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/transcode.rs`.
pub struct TranscodeCapacityExhausted;

#[derive(Debug, Error)]
/// Represents direct transcode error in the ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Functionality: Enumerates `Spawn`, `MissingStdout` states or choices for ffmpeg-backed direct AAC and HLS transcoding runtime.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/media.rs`, `src/transcode.rs`.
pub enum DirectTranscodeError {
    #[error("failed to start ffmpeg: {0}")]
    Spawn(io::Error),
    #[error("ffmpeg process did not provide stdout")]
    MissingStdout,
}

#[derive(Debug, Error)]
/// Represents hls transcode error in the ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Functionality: Enumerates `Prepare`, `Spawn`, `Wait`, `Unsuccessful`, `MissingManifest` states or choices for ffmpeg-backed direct AAC and HLS transcoding runtime.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/media.rs`, `src/transcode.rs`.
pub enum HlsTranscodeError {
    #[error("failed to prepare HLS output directory: {0}")]
    Prepare(io::Error),
    #[error("failed to start ffmpeg: {0}")]
    Spawn(io::Error),
    #[error("failed to wait for ffmpeg: {0}")]
    Wait(io::Error),
    #[error("ffmpeg HLS transcode exited unsuccessfully: {0}")]
    Unsuccessful(ExitStatus),
    #[error("ffmpeg HLS transcode did not write a manifest")]
    MissingManifest,
}

#[derive(Debug)]
/// Represents direct aac transcode stream in the ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Functionality: Carries fields `stdout`, `child`, `slot`, `stdout_eof` for ffmpeg-backed direct AAC and HLS transcoding runtime.
/// Dependencies: depends on `ChildStdout`, `Option<Child>`, `Option<TranscodeSlot>`, `bool` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/transcode.rs`.
pub struct DirectAacTranscodeStream {
    stdout: ChildStdout,
    child: Option<Child>,
    slot: Option<TranscodeSlot>,
    stdout_eof: bool,
}

impl DirectAacTranscodeStream {
    /// Constructs a new instance for ffmpeg-backed direct AAC and HLS transcoding runtime.
    ///
    /// Inputs:
    /// - `stdout`: `ChildStdout`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `child`: `Child`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `slot`: `TranscodeSlot`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn new(stdout: ChildStdout, child: Child, slot: TranscodeSlot) -> Self {
        Self {
            stdout,
            child: Some(child),
            slot: Some(slot),
            stdout_eof: false,
        }
    }
}

impl AsyncRead for DirectAacTranscodeStream {
    /// Polls the direct transcode stream for more bytes.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `cx`: `&mut Context<'_>`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `buf`: `&mut ReadBuf<'_>`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Poll<io::Result<()>>` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let requested = buf.remaining();
        let previous_len = buf.filled().len();
        match Pin::new(&mut self.stdout).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if requested > 0 && buf.filled().len() == previous_len {
                    self.stdout_eof = true;
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl Drop for DirectAacTranscodeStream {
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
        let slot = self.slot.take();
        let Some(mut child) = self.child.take() else {
            drop(slot);
            return;
        };

        match child.try_wait() {
            Ok(Some(status)) => {
                drop(slot);
                log_direct_transcode_status(Ok(status));
            }
            Ok(None) if self.stdout_eof => {
                wait_for_direct_transcode_child(child, slot);
            }
            Ok(None) => {
                if let Err(error) = child.start_kill() {
                    tracing::warn!(%error, "failed to kill abandoned direct AAC transcode process");
                }
                drop(slot);
                wait_for_direct_transcode_child(child, None);
            }
            Err(error) => {
                tracing::warn!(%error, "failed to check direct AAC transcode process status");
                if !self.stdout_eof {
                    if let Err(error) = child.start_kill() {
                        tracing::warn!(%error, "failed to kill abandoned direct AAC transcode process");
                    }
                    drop(slot);
                    wait_for_direct_transcode_child(child, None);
                } else {
                    wait_for_direct_transcode_child(child, slot);
                }
            }
        }
    }
}

impl TranscodeAdmission {
    /// Constructs a new instance for ffmpeg-backed direct AAC and HLS transcoding runtime.
    ///
    /// Inputs:
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    pub fn new(limit: u32) -> Self {
        Self {
            inner: Arc::new(TranscodeAdmissionInner {
                state: Mutex::new(TranscodeAdmissionState { limit, in_use: 0 }),
            }),
        }
    }

    /// Sets stored state for ffmpeg-backed direct AAC and HLS transcoding runtime.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    pub fn set_limit(&self, limit: u32) {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("transcode admission lock poisoned");
        state.limit = limit;
    }

    /// Handles usage for ffmpeg-backed direct AAC and HLS transcoding runtime.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `TranscodeSlotUsage` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    pub fn usage(&self) -> TranscodeSlotUsage {
        let state = self
            .inner
            .state
            .lock()
            .expect("transcode admission lock poisoned");
        TranscodeSlotUsage {
            limit: state.limit,
            in_use: state.in_use,
            available: state.limit.saturating_sub(state.in_use),
        }
    }

    /// Handles try acquire for ffmpeg-backed direct AAC and HLS transcoding runtime.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `TranscodeSlot` on success or `TranscodeCapacityExhausted` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `TranscodeCapacityExhausted` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    pub fn try_acquire(&self) -> Result<TranscodeSlot, TranscodeCapacityExhausted> {
        let mut state = self
            .inner
            .state
            .lock()
            .expect("transcode admission lock poisoned");
        if state.in_use >= state.limit {
            return Err(TranscodeCapacityExhausted);
        }
        state.in_use += 1;
        Ok(TranscodeSlot {
            admission: self.inner.clone(),
        })
    }
}

impl HlsGenerationCoordinator {
    /// Constructs a new instance for ffmpeg-backed direct AAC and HLS transcoding runtime.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Handles join or start for ffmpeg-backed direct AAC and HLS transcoding runtime.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `key`: `PathBuf`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
    ///
    /// Output:
    /// - Returns `HlsGenerationLease` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    pub fn join_or_start(&self, key: PathBuf) -> HlsGenerationLease {
        let mut generations = self
            .inner
            .lock()
            .expect("HLS generation lock poisoned");
        if let Some(done) = generations.get(&key) {
            return HlsGenerationLease::Wait(HlsGenerationWaiter {
                done: done.subscribe(),
            });
        }

        let (done, _receiver) = watch::channel(false);
        generations.insert(key.clone(), done.clone());
        HlsGenerationLease::Start(HlsGenerationGuard {
            key,
            inner: self.inner.clone(),
            done,
        })
    }
}

impl fmt::Debug for HlsGenerationCoordinator {
    /// Formats the value for display or serialization.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `f`: `&mut fmt:Formatter<'_>`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `()` on success or `fmt::Error` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `fmt::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HlsGenerationCoordinator")
            .finish_non_exhaustive()
    }
}

impl HlsGenerationWaiter {
    /// Waits for asynchronous completion for ffmpeg-backed direct AAC and HLS transcoding runtime.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns a future that resolves to `()` after the operation completes.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    pub async fn wait(mut self) {
        while !*self.done.borrow() {
            if self.done.changed().await.is_err() {
                break;
            }
        }
    }
}

impl Drop for HlsGenerationGuard {
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
        let mut generations = self
            .inner
            .lock()
            .expect("HLS generation lock poisoned");
        let should_remove = generations
            .get(&self.key)
            .map(|done| done.same_channel(&self.done))
            .unwrap_or(false);
        let _ = self.done.send(true);
        if should_remove {
            generations.remove(&self.key);
        }
    }
}

impl Drop for TranscodeSlot {
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
        let mut state = self
            .admission
            .state
            .lock()
            .expect("transcode admission lock poisoned");
        debug_assert!(state.in_use > 0, "transcode slot counter underflow");
        state.in_use = state.in_use.saturating_sub(1);
    }
}

/// Spawns asynchronous work for ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Inputs:
/// - `ffmpeg_path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `input_path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `profile`: `AacTranscodeProfile`; expected to be a value satisfying the type contract shown in the function signature.
/// - `slot`: `TranscodeSlot`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `DirectAacTranscodeStream` on success or `DirectTranscodeError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `DirectTranscodeError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn spawn_direct_aac_transcode(
    ffmpeg_path: &Path,
    input_path: &Path,
    profile: AacTranscodeProfile,
    slot: TranscodeSlot,
) -> Result<DirectAacTranscodeStream, DirectTranscodeError> {
    let mut child = Command::new(ffmpeg_path)
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-i",
        ])
        .arg(input_path)
        .args([
            "-vn",
            "-map",
            "0:a:0",
            "-c:a",
            "aac",
            "-b:a",
            profile.bitrate(),
            "-f",
            "adts",
            "pipe:1",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(DirectTranscodeError::Spawn)?;

    let Some(stdout) = child.stdout.take() else {
        let _ = child.kill().await;
        return Err(DirectTranscodeError::MissingStdout);
    };

    Ok(DirectAacTranscodeStream::new(stdout, child, slot))
}

/// Generates derived output for ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Inputs:
/// - `ffmpeg_path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `input_path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `profile`: `AacTranscodeProfile`; expected to be a value satisfying the type contract shown in the function signature.
/// - `output_dir`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `slot`: `TranscodeSlot`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Vec<u8>` on success or `HlsTranscodeError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `HlsTranscodeError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn generate_hls_aac_transcode(
    ffmpeg_path: &Path,
    input_path: &Path,
    profile: AacTranscodeProfile,
    output_dir: &Path,
    slot: TranscodeSlot,
) -> Result<Vec<u8>, HlsTranscodeError> {
    let parent = output_dir.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .await
        .map_err(HlsTranscodeError::Prepare)?;

    let output_name = output_dir
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.trim().is_empty())
        .unwrap_or("hls");
    let temp_dir = parent.join(format!(".{output_name}-{}", Uuid::new_v4().simple()));
    fs::create_dir_all(temp_dir.join("segments"))
        .await
        .map_err(HlsTranscodeError::Prepare)?;

    let mut child = Command::new(ffmpeg_path)
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-nostdin",
            "-i",
        ])
        .arg(input_path)
        .args([
            "-vn",
            "-map",
            "0:a:0",
            "-c:a",
            "aac",
            "-b:a",
            profile.bitrate(),
            "-f",
            "hls",
            "-hls_time",
            "6",
            "-hls_playlist_type",
            "vod",
            "-hls_list_size",
            "0",
            "-hls_segment_type",
            "mpegts",
            "-hls_segment_filename",
            "segments/segment-%05d.ts",
            "manifest.m3u8",
        ])
        .current_dir(&temp_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(HlsTranscodeError::Spawn)?;

    let initial_manifest = match wait_for_initial_hls_manifest(&temp_dir, &mut child).await {
        Ok(manifest) => manifest,
        Err(error) => {
            cleanup_hls_temp_dir(&temp_dir).await;
            return Err(error);
        }
    };

    let output_manifest = output_dir.join("manifest.m3u8");
    if path_is_file(&output_manifest)
        .await
        .map_err(HlsTranscodeError::Prepare)?
    {
        if let Err(error) = child.start_kill() {
            tracing::warn!(%error, "failed to kill duplicate HLS AAC transcode process");
        }
        wait_for_hls_transcode_child(child, slot, temp_dir);
        return Ok(initial_manifest);
    }

    if let Err(error) = fs::rename(&temp_dir, output_dir).await {
        if path_is_file(&output_manifest).await.unwrap_or(false) {
            if let Err(error) = child.start_kill() {
                tracing::warn!(%error, "failed to kill duplicate HLS AAC transcode process");
            }
            wait_for_hls_transcode_child(child, slot, temp_dir);
            return Ok(initial_manifest);
        }
        remove_dir_all_if_exists(output_dir)
            .await
            .map_err(HlsTranscodeError::Prepare)?;
        if let Err(rename_error) = fs::rename(&temp_dir, output_dir).await {
            tracing::warn!(
                %error,
                "initial HLS output directory publish failed before stale output removal"
            );
            return Err(HlsTranscodeError::Prepare(rename_error));
        }
    }

    wait_for_hls_transcode_child(child, slot, output_dir.to_path_buf());
    Ok(initial_manifest)
}

/// Waits for asynchronous completion for ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Inputs:
/// - `output_dir`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `child`: `&mut Child`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Vec<u8>` on success or `HlsTranscodeError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `HlsTranscodeError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn wait_for_initial_hls_manifest(
    output_dir: &Path,
    child: &mut Child,
) -> Result<Vec<u8>, HlsTranscodeError> {
    loop {
        if let Some(manifest) = read_usable_hls_manifest(output_dir)
            .await
            .map_err(HlsTranscodeError::Prepare)?
        {
            return Ok(manifest);
        }

        if let Some(status) = child.try_wait().map_err(HlsTranscodeError::Wait)? {
            if !status.success() {
                return Err(HlsTranscodeError::Unsuccessful(status));
            }

            return read_usable_hls_manifest(output_dir)
                .await
                .map_err(HlsTranscodeError::Prepare)?
                .ok_or(HlsTranscodeError::MissingManifest);
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Reads data for ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Inputs:
/// - `output_dir`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `Option<Vec<u8>>` on success or `io::Error` when the operation cannot be completed.
///
/// Errors:
/// - Returns `io::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn read_usable_hls_manifest(output_dir: &Path) -> io::Result<Option<Vec<u8>>> {
    let manifest = output_dir.join("manifest.m3u8");
    let body = match fs::read(&manifest).await {
        Ok(body) => body,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    let Ok(text) = std::str::from_utf8(&body) else {
        return Ok(None);
    };
    if !text.lines().any(|line| line.trim() == "#EXTM3U") {
        return Ok(None);
    }

    for line in text.lines().map(str::trim) {
        if line.is_empty()
            || line.starts_with('#')
            || line.starts_with('/')
            || line.contains("..")
        {
            continue;
        }
        if path_is_file(&output_dir.join(line)).await? {
            return Ok(Some(body));
        }
    }

    Ok(None)
}

/// Handles path is file for ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `bool` on success or `io::Error` when the operation cannot be completed.
///
/// Errors:
/// - Returns `io::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn path_is_file(path: &Path) -> io::Result<bool> {
    match fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.is_file()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// Handles remove dir all if exists for ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `()` on success or `io::Error` when the operation cannot be completed.
///
/// Errors:
/// - Returns `io::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn remove_dir_all_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Handles cleanup hls temp dir for ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors.
async fn cleanup_hls_temp_dir(path: &Path) {
    if let Err(error) = fs::remove_dir_all(path).await {
        if error.kind() != io::ErrorKind::NotFound {
            tracing::warn!(
                %error,
                path = %path.display(),
                "failed to clean up temporary HLS output directory"
            );
        }
    }
}

/// Waits for asynchronous completion for ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Inputs:
/// - `child`: `Child`; expected to be a value satisfying the type contract shown in the function signature.
/// - `slot`: `Option<TranscodeSlot>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn wait_for_direct_transcode_child(mut child: Child, slot: Option<TranscodeSlot>) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        drop(slot);
        return;
    };

    handle.spawn(async move {
        let status = child.wait().await;
        drop(slot);
        log_direct_transcode_status(status);
    });
}

/// Waits for asynchronous completion for ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Inputs:
/// - `child`: `Child`; expected to be a value satisfying the type contract shown in the function signature.
/// - `slot`: `TranscodeSlot`; expected to be a value satisfying the type contract shown in the function signature.
/// - `output_dir`: `PathBuf`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn wait_for_hls_transcode_child(mut child: Child, slot: TranscodeSlot, output_dir: PathBuf) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        drop(slot);
        return;
    };

    handle.spawn(async move {
        let status = child.wait().await;
        drop(slot);
        log_hls_transcode_status(status, &output_dir).await;
    });
}

/// Handles log direct transcode status for ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Inputs:
/// - `status`: `io:Result<ExitStatus>`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn log_direct_transcode_status(status: io::Result<ExitStatus>) {
    match status {
        Ok(status) if status.success() => {
            tracing::debug!(%status, "direct AAC transcode process completed");
        }
        Ok(status) => {
            tracing::warn!(%status, "direct AAC transcode process exited unsuccessfully");
        }
        Err(error) => {
            tracing::warn!(%error, "failed to wait for direct AAC transcode process");
        }
    }
}

/// Handles log hls transcode status for ffmpeg-backed direct AAC and HLS transcoding runtime.
///
/// Inputs:
/// - `status`: `io:Result<ExitStatus>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `output_dir`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns a future that resolves to `()` after the operation completes.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn log_hls_transcode_status(status: io::Result<ExitStatus>, output_dir: &Path) {
    match status {
        Ok(status) if status.success() => {
            tracing::debug!(%status, "HLS AAC transcode process completed");
        }
        Ok(status) => {
            tracing::warn!(%status, "HLS AAC transcode process exited unsuccessfully");
            cleanup_hls_temp_dir(output_dir).await;
        }
        Err(error) => {
            tracing::warn!(%error, "failed to wait for HLS AAC transcode process");
            cleanup_hls_temp_dir(output_dir).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        str::FromStr,
        sync::{
            atomic::{AtomicBool, AtomicU32, Ordering},
            Arc, Barrier,
        },
        thread,
        time::Duration,
    };

    use super::*;

    #[test]
    /// Handles fixed aac profiles have expected names and bitrates for ffmpeg-backed direct AAC and HLS transcoding runtime.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn fixed_aac_profiles_have_expected_names_and_bitrates() {
        let profiles = AacTranscodeProfile::all()
            .iter()
            .map(|profile| (profile.api_name(), profile.bitrate()))
            .collect::<Vec<_>>();

        assert_eq!(
            profiles,
            vec![("mobile", "64k"), ("standard", "128k"), ("high", "256k")]
        );
        assert_eq!(
            AacTranscodeProfile::from_str("mobile"),
            Ok(AacTranscodeProfile::Mobile)
        );
        assert_eq!(
            AacTranscodeProfile::from_str("standard"),
            Ok(AacTranscodeProfile::Standard)
        );
        assert_eq!(
            AacTranscodeProfile::from_str("high"),
            Ok(AacTranscodeProfile::High)
        );
        assert!(AacTranscodeProfile::from_str("lossless").is_err());
    }

    #[test]
    /// Handles lowered limit blocks new slot acquisition for ffmpeg-backed direct AAC and HLS transcoding runtime.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn lowered_limit_blocks_new_slot_acquisition() {
        let admission = TranscodeAdmission::new(2);
        let first = admission.try_acquire().unwrap();
        let second = admission.try_acquire().unwrap();

        admission.set_limit(1);
        assert!(admission.try_acquire().is_err());

        drop(first);
        assert!(admission.try_acquire().is_err());

        drop(second);
        let slot = admission.try_acquire().unwrap();
        let usage = admission.usage();
        assert_eq!(usage.limit, 1);
        assert_eq!(usage.in_use, 1);
        assert_eq!(usage.available, 0);
        drop(slot);
    }

    #[test]
    /// Handles lowered limit blocks concurrent new slot acquisition for ffmpeg-backed direct AAC and HLS transcoding runtime.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn lowered_limit_blocks_concurrent_new_slot_acquisition() {
        let admission = TranscodeAdmission::new(8);
        let start = Arc::new(Barrier::new(9));
        let lowered = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let violations = Arc::new(AtomicU32::new(0));

        let workers = (0..8)
            .map(|_| {
                let admission = admission.clone();
                let start = start.clone();
                let lowered = lowered.clone();
                let stop = stop.clone();
                let violations = violations.clone();
                thread::spawn(move || {
                    start.wait();
                    while !stop.load(Ordering::Acquire) {
                        if lowered.load(Ordering::Acquire) {
                            if admission.try_acquire().is_ok() {
                                violations.fetch_add(1, Ordering::AcqRel);
                            }
                        } else if let Ok(slot) = admission.try_acquire() {
                            thread::yield_now();
                            drop(slot);
                        } else {
                            thread::yield_now();
                        }
                    }
                })
            })
            .collect::<Vec<_>>();

        start.wait();
        thread::sleep(Duration::from_millis(20));
        admission.set_limit(0);
        lowered.store(true, Ordering::Release);
        thread::sleep(Duration::from_millis(20));
        stop.store(true, Ordering::Release);

        for worker in workers {
            worker.join().unwrap();
        }

        assert_eq!(violations.load(Ordering::Acquire), 0);
        let usage = admission.usage();
        assert_eq!(usage.limit, 0);
        assert_eq!(usage.in_use, 0);
        assert_eq!(usage.available, 0);
    }
}
