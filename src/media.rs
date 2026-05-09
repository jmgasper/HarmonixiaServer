use std::{
    collections::BTreeMap,
    fs,
    io::{self, Read},
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::domain::MediaProbeFacts;

#[derive(Debug, Error)]
/// Represents media probe error in the local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Functionality: Enumerates `Io`, `FileTooLarge` states or choices for local media probing, file metadata, tag extraction, and sidecar discovery logic.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/media.rs`, `src/pipeline.rs`.
pub enum MediaProbeError {
    #[error("failed to read media file: {0}")]
    Io(#[from] io::Error),
    #[error("media file is too large to store file size in Postgres: {0}")]
    FileTooLarge(u64),
}

#[derive(Debug, Clone)]
/// Represents probed media file in the local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Functionality: Carries fields `source_path`, `facts`, `tags`, `sidecar_paths`, `folder_images` for local media probing, file metadata, tag extraction, and sidecar discovery logic.
/// Dependencies: depends on `PathBuf`, `MediaProbeFacts`, `LocalMediaTags`, `Vec<PathBuf>`, `Vec<PathBuf>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/media.rs`, `src/pipeline.rs`, `src/providers.rs`.
pub struct ProbedMediaFile {
    pub source_path: PathBuf,
    pub facts: MediaProbeFacts,
    pub tags: LocalMediaTags,
    pub sidecar_paths: Vec<PathBuf>,
    pub folder_images: Vec<PathBuf>,
}

#[derive(Debug, Clone, Default)]
/// Represents local media tags in the local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Functionality: Carries fields `values` for local media probing, file metadata, tag extraction, and sidecar discovery logic.
/// Dependencies: depends on `BTreeMap<String` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/media.rs`, `src/pipeline.rs`, `src/providers.rs`.
pub struct LocalMediaTags {
    values: BTreeMap<String, String>,
}

impl LocalMediaTags {
    /// Inserts data for local media probing, file metadata, tag extraction, and sidecar discovery logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `key`: `impl AsRef<str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `value`: `impl Into<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn insert(&mut self, key: impl AsRef<str>, value: impl Into<String>) {
        let key = normalize_tag_key(key.as_ref());
        let value = value.into().trim().to_string();
        if key.is_empty() || value.is_empty() {
            return;
        }
        self.values.insert(key, value);
    }

    /// Retrieves a resource for local media probing, file metadata, tag extraction, and sidecar discovery logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `keys`: `&[&str]`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Some(&str)` when a value is available; otherwise returns `None`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn get(&self, keys: &[&str]) -> Option<&str> {
        keys.iter()
            .filter_map(|key| self.values.get(&normalize_tag_key(key)))
            .map(String::as_str)
            .find(|value| !value.trim().is_empty())
    }

    /// Handles values for local media probing, file metadata, tag extraction, and sidecar discovery logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&BTreeMap<String, String>` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn values(&self) -> &BTreeMap<String, String> {
        &self.values
    }

    /// Handles number for local media probing, file metadata, tag extraction, and sidecar discovery logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `keys`: `&[&str]`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Some(i32)` when a value is available; otherwise returns `None`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn number(&self, keys: &[&str]) -> Option<i32> {
        self.get(keys).and_then(parse_number)
    }

    /// Handles bool for local media probing, file metadata, tag extraction, and sidecar discovery logic.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `keys`: `&[&str]`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn bool(&self, keys: &[&str]) -> bool {
        self.get(keys)
            .map(|value| {
                matches!(
                    value.trim().to_ascii_lowercase().as_str(),
                    "1" | "true" | "yes" | "y" | "on" | "compilation"
                )
            })
            .unwrap_or(false)
    }
}

/// Probes media data for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `path`: `impl AsRef<Path>`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `ProbedMediaFile` on success or `MediaProbeError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `MediaProbeError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub fn probe_media_file(path: impl AsRef<Path>) -> Result<ProbedMediaFile, MediaProbeError> {
    let path = path.as_ref();
    let metadata = fs::metadata(path)?;
    let file_size = i64::try_from(metadata.len())
        .map_err(|_| MediaProbeError::FileTooLarge(metadata.len()))?;
    let file_hash = sha256_file(path)?;
    let mut facts = MediaProbeFacts {
        file_hash,
        file_size,
        mime_type: mime_type_for_path(path).map(str::to_string),
        container: path
            .extension()
            .and_then(|extension| extension.to_str())
            .map(|extension| extension.to_ascii_lowercase()),
        audio_codec: None,
        duration_seconds: None,
        bitrate: None,
        sample_rate: None,
        channels: None,
    };
    let mut tags = LocalMediaTags::default();

    if let Some(ffprobe) = run_ffprobe(path) {
        merge_ffprobe(&ffprobe, &mut facts, &mut tags);
    }

    let sidecar_paths = read_sidecars(path, &mut tags);
    let folder_images = find_folder_images(path);

    Ok(ProbedMediaFile {
        source_path: path.to_path_buf(),
        facts,
        tags,
        sidecar_paths,
        folder_images,
    })
}

/// Handles is supported media path for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
pub fn is_supported_media_path(path: &Path) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| {
            matches!(
                extension.to_ascii_lowercase().as_str(),
                "aac"
                    | "aiff"
                    | "alac"
                    | "dff"
                    | "dsf"
                    | "flac"
                    | "m4a"
                    | "m4b"
                    | "mp4"
                    | "mp3"
                    | "ogg"
                    | "opus"
                    | "wav"
                    | "webm"
                    | "wma"
            )
        })
        .unwrap_or(false)
}

/// Handles mime type for path for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `Some(&'static str)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
pub fn mime_type_for_path(path: &Path) -> Option<&'static str> {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_ascii_lowercase())
        .as_deref()
    {
        Some("aac") => Some("audio/aac"),
        Some("aiff") => Some("audio/aiff"),
        Some("alac") => Some("audio/mp4"),
        Some("dff") => Some("audio/dsd"),
        Some("dsf") => Some("audio/x-dsf"),
        Some("flac") => Some("audio/flac"),
        Some("m4a") | Some("m4b") => Some("audio/mp4"),
        Some("mp4") => Some("audio/mp4"),
        Some("mp3") => Some("audio/mpeg"),
        Some("ogg") => Some("audio/ogg"),
        Some("opus") => Some("audio/opus"),
        Some("wav") => Some("audio/wav"),
        Some("webm") => Some("audio/webm"),
        Some("wma") => Some("audio/x-ms-wma"),
        _ => None,
    }
}

/// Handles sha256 file for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `String` on success or `io::Error` when the operation cannot be completed.
///
/// Errors:
/// - Returns `io::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn sha256_file(path: &Path) -> Result<String, io::Error> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];

    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }

    Ok(hex_encode(&hasher.finalize()))
}

/// Handles hex encode for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `bytes`: `&[u8]`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

/// Runs the operation for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `Some(Value)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn run_ffprobe(path: &Path) -> Option<Value> {
    let output = Command::new("ffprobe")
        .args([
            "-v",
            "quiet",
            "-print_format",
            "json",
            "-show_format",
            "-show_streams",
        ])
        .arg(path)
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    serde_json::from_slice(&output.stdout).ok()
}

/// Handles merge ffprobe for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `value`: `&Value`; expected to be a value satisfying the type contract shown in the function signature.
/// - `facts`: `&mut MediaProbeFacts`; expected to be a media domain value that has already passed upstream validation.
/// - `tags`: `&mut LocalMediaTags`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn merge_ffprobe(value: &Value, facts: &mut MediaProbeFacts, tags: &mut LocalMediaTags) {
    if let Some(format) = value.get("format").and_then(Value::as_object) {
        if let Some(format_name) = format.get("format_name").and_then(Value::as_str) {
            facts.container = Some(format_name.to_string());
        }
        facts.duration_seconds = format
            .get("duration")
            .and_then(parse_json_seconds)
            .or(facts.duration_seconds);
        facts.bitrate = format
            .get("bit_rate")
            .and_then(parse_json_i32)
            .or(facts.bitrate);
        if let Some(format_tags) = format.get("tags").and_then(Value::as_object) {
            merge_json_tags(format_tags, tags);
        }
    }

    if let Some(streams) = value.get("streams").and_then(Value::as_array) {
        for stream in streams {
            let Some(stream) = stream.as_object() else {
                continue;
            };
            if stream.get("codec_type").and_then(Value::as_str) != Some("audio") {
                continue;
            }
            facts.audio_codec = stream
                .get("codec_name")
                .and_then(Value::as_str)
                .map(str::to_string)
                .or_else(|| facts.audio_codec.clone());
            facts.sample_rate = stream
                .get("sample_rate")
                .and_then(parse_json_i32)
                .or(facts.sample_rate);
            facts.channels = stream
                .get("channels")
                .and_then(parse_json_i32)
                .or(facts.channels);
            if let Some(stream_tags) = stream.get("tags").and_then(Value::as_object) {
                merge_json_tags(stream_tags, tags);
            }
            break;
        }
    }
}

/// Handles merge json tags for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `object`: `&serde_json:Map<String, Value>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `tags`: `&mut LocalMediaTags`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn merge_json_tags(
    object: &serde_json::Map<String, Value>,
    tags: &mut LocalMediaTags,
) {
    for (key, value) in object {
        match value {
            Value::String(value) => tags.insert(key, value.clone()),
            Value::Number(value) => tags.insert(key, value.to_string()),
            Value::Bool(value) => tags.insert(key, value.to_string()),
            _ => {}
        }
    }
}

/// Reads data for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `tags`: `&mut LocalMediaTags`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `Vec<PathBuf>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn read_sidecars(path: &Path, tags: &mut LocalMediaTags) -> Vec<PathBuf> {
    let mut sidecars = Vec::new();
    let mut candidates = Vec::new();

    candidates.push(path.with_extension("json"));
    if let Some(parent) = path.parent() {
        candidates.push(parent.join("metadata.json"));
        candidates.push(parent.join("album.json"));
        candidates.push(parent.join("podcast.json"));
    }

    candidates.sort();
    candidates.dedup();

    for candidate in candidates {
        let Ok(contents) = fs::read_to_string(&candidate) else {
            continue;
        };
        let Ok(value) = serde_json::from_str::<Value>(&contents) else {
            continue;
        };
        merge_sidecar_value(&value, tags);
        sidecars.push(candidate);
    }

    sidecars
}

/// Handles merge sidecar value for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `value`: `&Value`; expected to be a value satisfying the type contract shown in the function signature.
/// - `tags`: `&mut LocalMediaTags`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn merge_sidecar_value(value: &Value, tags: &mut LocalMediaTags) {
    let Some(object) = value.as_object() else {
        return;
    };
    for (key, value) in object {
        match value {
            Value::String(value) => tags.insert(key, value.clone()),
            Value::Number(value) => tags.insert(key, value.to_string()),
            Value::Bool(value) => tags.insert(key, value.to_string()),
            Value::Object(_) if key == "tags" || key == "metadata" => {
                if let Some(nested) = value.as_object() {
                    merge_json_tags(nested, tags);
                }
            }
            _ => {}
        }
    }
}

/// Handles find folder images for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `Vec<PathBuf>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn find_folder_images(path: &Path) -> Vec<PathBuf> {
    let Some(parent) = path.parent() else {
        return Vec::new();
    };
    let mut images = Vec::new();
    for basename in ["cover", "folder", "front", "albumart", "artwork"] {
        for extension in ["jpg", "jpeg", "png", "webp"] {
            let candidate = parent.join(format!("{basename}.{extension}"));
            if candidate.is_file() {
                images.push(candidate);
            }
        }
    }
    images
}

/// Parses and validates input for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `value`: `&Value`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Some(i32)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn parse_json_seconds(value: &Value) -> Option<i32> {
    match value {
        Value::String(value) => value.parse::<f64>().ok(),
        Value::Number(value) => value.as_f64(),
        _ => None,
    }
    .and_then(|value| {
        if value.is_finite() && value >= 0.0 && value <= i32::MAX as f64 {
            Some(value.round() as i32)
        } else {
            None
        }
    })
}

/// Parses and validates input for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `value`: `&Value`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Some(i32)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn parse_json_i32(value: &Value) -> Option<i32> {
    match value {
        Value::String(value) => value.parse::<i32>().ok(),
        Value::Number(value) => value.as_i64().and_then(|value| i32::try_from(value).ok()),
        _ => None,
    }
}

/// Parses and validates input for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(i32)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn parse_number(value: &str) -> Option<i32> {
    let first = value
        .split(['/', '-', ' '])
        .find(|fragment| !fragment.trim().is_empty())?;
    first.trim().parse::<i32>().ok()
}

/// Normalizes caller-provided data for local media probing, file metadata, tag extraction, and sidecar discovery logic.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn normalize_tag_key(value: &str) -> String {
    value
        .chars()
        .filter(|character| character.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{
        is_supported_media_path, merge_ffprobe, mime_type_for_path, LocalMediaTags,
    };
    use crate::domain::MediaProbeFacts;
    use serde_json::json;
    use std::path::Path;

    #[test]
    /// Handles supports mp4 and dsd audio extensions for local media probing, file metadata, tag extraction, and sidecar discovery logic.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn supports_mp4_and_dsd_audio_extensions() {
        for path in ["track.mp4", "track.dsf", "track.dff"] {
            assert!(is_supported_media_path(Path::new(path)), "{path}");
        }
    }

    #[test]
    /// Handles returns mime types for mp4 and dsd audio extensions for local media probing, file metadata, tag extraction, and sidecar discovery logic.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn returns_mime_types_for_mp4_and_dsd_audio_extensions() {
        assert_eq!(mime_type_for_path(Path::new("track.mp4")), Some("audio/mp4"));
        assert_eq!(mime_type_for_path(Path::new("track.dsf")), Some("audio/x-dsf"));
        assert_eq!(mime_type_for_path(Path::new("track.dff")), Some("audio/dsd"));
    }

    #[test]
    /// Handles rejects unsupported media extensions for local media probing, file metadata, tag extraction, and sidecar discovery logic.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn rejects_unsupported_media_extensions() {
        let path = Path::new("track.txt");

        assert!(!is_supported_media_path(path));
        assert_eq!(mime_type_for_path(path), None);
    }

    #[test]
    /// Handles ffprobe merge populates container and codec for expanded formats for local media probing, file metadata, tag extraction, and sidecar discovery logic.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn ffprobe_merge_populates_container_and_codec_for_expanded_formats() {
        for (extension, mime_type, format_name, codec_name) in [
            ("mp4", "audio/mp4", "mov,mp4,m4a,3gp,3g2,mj2", "aac"),
            ("dsf", "audio/x-dsf", "dsf", "dsd_lsbf"),
            ("dff", "audio/dsd", "iff", "dsd_msbf"),
        ] {
            let ffprobe = json!({
                "format": {
                    "format_name": format_name,
                    "duration": "1.5",
                    "bit_rate": "128000"
                },
                "streams": [{
                    "codec_type": "audio",
                    "codec_name": codec_name,
                    "sample_rate": "44100",
                    "channels": 2
                }]
            });
            let mut facts = MediaProbeFacts {
                file_hash: String::new(),
                file_size: 0,
                mime_type: Some(mime_type.to_string()),
                container: Some(extension.to_string()),
                audio_codec: None,
                duration_seconds: None,
                bitrate: None,
                sample_rate: None,
                channels: None,
            };
            let mut tags = LocalMediaTags::default();

            merge_ffprobe(&ffprobe, &mut facts, &mut tags);

            assert_eq!(facts.mime_type.as_deref(), Some(mime_type));
            assert_eq!(facts.container.as_deref(), Some(format_name));
            assert_eq!(facts.audio_codec.as_deref(), Some(codec_name));
            assert_eq!(facts.duration_seconds, Some(2));
            assert_eq!(facts.bitrate, Some(128000));
            assert_eq!(facts.sample_rate, Some(44100));
            assert_eq!(facts.channels, Some(2));
        }
    }
}
