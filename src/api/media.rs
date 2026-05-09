use std::{
    io,
    path::{Path as FsPath, PathBuf},
    str::FromStr,
};

use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    Json,
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, SeekFrom},
};
use tokio_util::io::ReaderStream;
use uuid::Uuid;

use crate::{
    auth::{AdminAccount, AuthenticatedUser},
    domain::{AacTranscodeProfile, MediaFile, PlaybackItemType, TranscodeSlotUsage},
    error::{ApiError, ErrorResponse},
    state::AppState,
    transcode::{
        generate_hls_aac_transcode, spawn_direct_aac_transcode, DirectTranscodeError,
        HlsGenerationLease, HlsTranscodeError,
    },
};

/// Builds the Axum router for media streaming and transcoding.
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
        .route("/:item_type/:item_id/original", get(stream_original))
        .route(
            "/:item_type/:item_id/original/download",
            get(download_original),
        )
        .route(
            "/:item_type/:item_id/transcode/:profile",
            get(stream_direct_transcode),
        )
        .route(
            "/:item_type/:item_id/hls/:profile/manifest.m3u8",
            get(hls_manifest),
        )
        .route(
            "/:item_type/:item_id/hls/:profile/playlist.m3u8",
            get(hls_manifest),
        )
        .route(
            "/:item_type/:item_id/hls/:profile/segments/:segment",
            get(hls_segment),
        )
}

/// Builds the admin Axum router for media streaming and transcoding.
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
    Router::new().route("/media/transcode-slots", get(transcode_slot_usage))
}

#[utoipa::path(
    get,
    path = "/api/v1/media/{item_type}/{item_id}/original",
    tag = "media",
    security(("basicAuth" = [])),
    params(
        ("item_type" = String, Path, description = "Catalog item type: track or episode"),
        ("item_id" = Uuid, Path, description = "Published catalog item id"),
        ("Range" = Option<String>, Header, description = "Optional byte range, for example: bytes=0-1048575")
    ),
    responses(
        (status = 200, description = "Authenticated original media stream. Includes Accept-Ranges, Content-Length, Content-Type, and inline Content-Disposition headers.", content_type = "application/octet-stream"),
        (status = 206, description = "Authenticated partial original media stream. Includes Accept-Ranges, Content-Length, Content-Range, Content-Type, and inline Content-Disposition headers.", content_type = "application/octet-stream"),
        (status = 400, description = "Invalid item type", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Catalog item is not published, not visible, not backed by a published canonical media file, or the original file is unavailable", body = ErrorResponse),
        (status = 416, description = "Requested byte range is not satisfiable")
    )
)]
/// Streams media for media streaming and transcoding.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(_account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path((item_type, item_id))`: `Path<(String, Uuid)>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `headers`: `HeaderMap`; expected to be HTTP headers supplied by the caller.
///
/// Output:
/// - Returns `Response` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn stream_original(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
    Path((item_type, item_id)): Path<(String, Uuid)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    serve_original(state, item_type, item_id, headers, ContentDisposition::Inline).await
}

#[utoipa::path(
    get,
    path = "/api/v1/media/{item_type}/{item_id}/original/download",
    tag = "media",
    security(("basicAuth" = [])),
    params(
        ("item_type" = String, Path, description = "Catalog item type: track or episode"),
        ("item_id" = Uuid, Path, description = "Published catalog item id"),
        ("Range" = Option<String>, Header, description = "Optional byte range, for resumable downloads")
    ),
    responses(
        (status = 200, description = "Authenticated original file download. Includes Accept-Ranges, Content-Length, Content-Type, and attachment Content-Disposition headers.", content_type = "application/octet-stream"),
        (status = 206, description = "Authenticated partial original file download. Includes Accept-Ranges, Content-Length, Content-Range, Content-Type, and attachment Content-Disposition headers.", content_type = "application/octet-stream"),
        (status = 400, description = "Invalid item type", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Catalog item is not published, not visible, not backed by a published canonical media file, or the original file is unavailable", body = ErrorResponse),
        (status = 416, description = "Requested byte range is not satisfiable")
    )
)]
/// Builds a download response for media streaming and transcoding.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(_account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path((item_type, item_id))`: `Path<(String, Uuid)>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `headers`: `HeaderMap`; expected to be HTTP headers supplied by the caller.
///
/// Output:
/// - Returns `Response` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn download_original(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
    Path((item_type, item_id)): Path<(String, Uuid)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    serve_original(
        state,
        item_type,
        item_id,
        headers,
        ContentDisposition::Attachment,
    )
    .await
}

#[utoipa::path(
    get,
    path = "/api/v1/media/{item_type}/{item_id}/transcode/{profile}",
    tag = "media",
    security(("basicAuth" = [])),
    params(
        ("item_type" = String, Path, description = "Catalog item type: track or episode"),
        ("item_id" = Uuid, Path, description = "Published catalog item id"),
        ("profile" = AacTranscodeProfile, Path, description = "Server-owned AAC profile: mobile, standard, or high")
    ),
    responses(
        (status = 200, description = "Authenticated direct AAC transcode stream for the selected profile. Output is ADTS AAC and does not support byte ranges.", content_type = "audio/aac"),
        (status = 400, description = "Invalid item type or AAC profile", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Catalog item is not published, not visible, not backed by a published canonical media file, or the original file is unavailable", body = ErrorResponse),
        (status = 503, description = "Transcode capacity is exhausted; request was rejected immediately without queueing or falling back to original media", body = ErrorResponse)
    )
)]
/// Streams media for media streaming and transcoding.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(_account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path((item_type, item_id, profile))`: `Path<(String, Uuid, String)>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `Response` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn stream_direct_transcode(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
    Path((item_type, item_id, profile)): Path<(String, Uuid, String)>,
) -> Result<Response, ApiError> {
    let item_type = parse_media_item_type(&item_type)?;
    let profile = parse_aac_transcode_profile(&profile)?;
    let media_file = state.visible_original_media_file(item_type, item_id).await?;
    let original = resolve_original_file(&state, &media_file)?;
    let slot = state.try_acquire_transcode_slot().map_err(|_| {
        ApiError::ServiceUnavailable(
            "transcode capacity is exhausted; retry later or request original media".into(),
        )
    })?;

    let transcode =
        spawn_direct_aac_transcode(&state.config().ffmpeg_path, &original, profile, slot)
            .await
            .map_err(map_direct_transcode_error)?;

    Ok(transcode_response(
        ReaderStream::new(transcode),
        &filename_for_transcode(&original, profile),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/media/{item_type}/{item_id}/hls/{profile}/manifest.m3u8",
    tag = "media",
    security(("basicAuth" = [])),
    params(
        ("item_type" = String, Path, description = "Catalog item type: track or episode"),
        ("item_id" = Uuid, Path, description = "Published catalog item id"),
        ("profile" = AacTranscodeProfile, Path, description = "Server-owned AAC HLS profile: mobile, standard, or high")
    ),
    responses(
        (status = 200, description = "Authenticated HLS media playlist for the selected AAC profile. Segment URIs are relative and require the same Basic authentication.", content_type = "application/vnd.apple.mpegurl"),
        (status = 400, description = "Invalid item type or AAC profile", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Catalog item is not published, not visible, not backed by a published canonical media file, or the original file is unavailable", body = ErrorResponse),
        (status = 503, description = "Transcode capacity is exhausted; HLS generation was rejected immediately without queueing or falling back to original media", body = ErrorResponse)
    )
)]
/// Handles hls manifest for media streaming and transcoding.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(_account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path((item_type, item_id, profile))`: `Path<(String, Uuid, String)>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `Response` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn hls_manifest(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
    Path((item_type, item_id, profile)): Path<(String, Uuid, String)>,
) -> Result<Response, ApiError> {
    let item_type = parse_media_item_type(&item_type)?;
    let profile = parse_aac_transcode_profile(&profile)?;
    let media_file = state.visible_original_media_file(item_type, item_id).await?;
    let original = resolve_original_file(&state, &media_file)?;
    let output_dir = hls_output_dir(&media_file, profile);
    let manifest = hls_manifest_path(&media_file, profile);

    loop {
        if path_is_file(&manifest).await? {
            let body = tokio::fs::read(&manifest)
                .await
                .map_err(map_hls_file_error)?;
            return Ok(hls_manifest_response(body));
        }

        match state.join_or_start_hls_generation(output_dir.clone()) {
            HlsGenerationLease::Start(generation) => {
                if path_is_file(&manifest).await? {
                    drop(generation);
                    let body = tokio::fs::read(&manifest)
                        .await
                        .map_err(map_hls_file_error)?;
                    return Ok(hls_manifest_response(body));
                }

                let slot = state.try_acquire_transcode_slot().map_err(|_| {
                    ApiError::ServiceUnavailable(
                        "transcode capacity is exhausted; retry later or request original media"
                            .into(),
                    )
                })?;
                let body = generate_hls_aac_transcode(
                    &state.config().ffmpeg_path,
                    &original,
                    profile,
                    &output_dir,
                    slot,
                )
                .await
                .map_err(map_hls_transcode_error)?;
                drop(generation);
                return Ok(hls_manifest_response(body));
            }
            HlsGenerationLease::Wait(generation) => {
                generation.wait().await;
            }
        }
    }
}

#[utoipa::path(
    get,
    path = "/api/v1/media/{item_type}/{item_id}/hls/{profile}/segments/{segment}",
    tag = "media",
    security(("basicAuth" = [])),
    params(
        ("item_type" = String, Path, description = "Catalog item type: track or episode"),
        ("item_id" = Uuid, Path, description = "Published catalog item id"),
        ("profile" = AacTranscodeProfile, Path, description = "Server-owned AAC HLS profile: mobile, standard, or high"),
        ("segment" = String, Path, description = "Server-generated HLS media segment filename from the authenticated manifest")
    ),
    responses(
        (status = 200, description = "Authenticated HLS MPEG-TS media segment for the selected AAC profile.", content_type = "video/mp2t"),
        (status = 400, description = "Invalid item type, AAC profile, or segment filename", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Catalog item, original media, or generated HLS segment was not found", body = ErrorResponse)
    )
)]
/// Handles hls segment for media streaming and transcoding.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `AuthenticatedUser(_account)`: `AuthenticatedUser`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path((item_type, item_id, profile, segment))`: `Path<(String, Uuid, String, String)>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `Response` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn hls_segment(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
    Path((item_type, item_id, profile, segment)): Path<(String, Uuid, String, String)>,
) -> Result<Response, ApiError> {
    let item_type = parse_media_item_type(&item_type)?;
    let profile = parse_aac_transcode_profile(&profile)?;
    let segment = validate_hls_segment_name(&segment)?;
    let media_file = state.visible_original_media_file(item_type, item_id).await?;
    let _original = resolve_original_file(&state, &media_file)?;
    let path = hls_output_dir(&media_file, profile)
        .join("segments")
        .join(segment);
    let file = File::open(&path).await.map_err(map_hls_file_error)?;
    let metadata = file.metadata().await.map_err(map_hls_file_error)?;
    if !metadata.is_file() {
        return Err(hls_file_not_found());
    }

    Ok(hls_segment_response(
        ReaderStream::new(file.take(metadata.len())),
        metadata.len(),
    ))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/media/transcode-slots",
    tag = "media",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Current direct and HLS transcode slot usage and configured limit", body = TranscodeSlotUsage),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse)
    )
)]
/// Handles transcode slot usage for media streaming and transcoding.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Json<TranscodeSlotUsage>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
pub async fn transcode_slot_usage(
    State(state): State<AppState>,
    _admin: AdminAccount,
) -> Json<TranscodeSlotUsage> {
    Json(state.transcode_slot_usage())
}

/// Serves content for media streaming and transcoding.
///
/// Inputs:
/// - `state`: `AppState`; expected to be Axum application state with a live repository and runtime configuration.
/// - `item_type`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `headers`: `HeaderMap`; expected to be HTTP headers supplied by the caller.
/// - `disposition`: `ContentDisposition`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Response` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn serve_original(
    state: AppState,
    item_type: String,
    item_id: Uuid,
    headers: HeaderMap,
    disposition: ContentDisposition,
) -> Result<Response, ApiError> {
    let item_type = parse_media_item_type(&item_type)?;
    let media_file = state.visible_original_media_file(item_type, item_id).await?;
    let original = resolve_original_file(&state, &media_file)?;
    let filename = filename_for_path(&original);
    let mut file = open_original_file(&original).await?;
    let metadata = file.metadata().await.map_err(map_file_error)?;
    if !metadata.is_file() {
        return Err(media_file_not_found());
    }
    let file_size = metadata.len();

    let selection = match headers.get(header::RANGE) {
        Some(value) => {
            let Ok(value) = value.to_str() else {
                return Ok(range_not_satisfiable_response(file_size));
            };
            match parse_range_header(value, file_size) {
                Ok(selection) => selection,
                Err(RangeNotSatisfiable) => {
                    return Ok(range_not_satisfiable_response(file_size));
                }
            }
        }
        None => RangeSelection::Full,
    };

    match selection {
        RangeSelection::Full => Ok(media_response(
            StatusCode::OK,
            ReaderStream::new(file),
            &media_file,
            &filename,
            disposition,
            file_size,
            None,
        )),
        RangeSelection::Partial { start, end } => {
            file.seek(SeekFrom::Start(start))
                .await
                .map_err(map_file_error)?;
            let content_length = end - start + 1;
            let content_range = format!("bytes {start}-{end}/{file_size}");
            Ok(media_response(
                StatusCode::PARTIAL_CONTENT,
                ReaderStream::new(file.take(content_length)),
                &media_file,
                &filename,
                disposition,
                content_length,
                Some(content_range),
            ))
        }
    }
}

/// Handles media response for media streaming and transcoding.
///
/// Inputs:
/// - `status`: `StatusCode`; expected to be a value satisfying the type contract shown in the function signature.
/// - `stream`: `ReaderStream<R>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `media_file`: `&MediaFile`; expected to be a media domain value that has already passed upstream validation.
/// - `filename`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `disposition`: `ContentDisposition`; expected to be a value satisfying the type contract shown in the function signature.
/// - `content_length`: `u64`; expected to be a numeric value within the range accepted by the target domain or database column.
/// - `content_range`: `Option<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Response` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn media_response<R>(
    status: StatusCode,
    stream: ReaderStream<R>,
    media_file: &MediaFile,
    filename: &str,
    disposition: ContentDisposition,
    content_length: u64,
    content_range: Option<String>,
) -> Response
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .expect("numeric content length should be a valid header"),
    );
    headers.insert(
        header::CONTENT_TYPE,
        media_file
            .mime_type
            .as_deref()
            .and_then(|mime_type| HeaderValue::from_str(mime_type).ok())
            .unwrap_or_else(|| HeaderValue::from_static("application/octet-stream")),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&content_disposition(disposition, filename))
            .expect("sanitized content disposition should be a valid header"),
    );
    if let Some(content_range) = content_range {
        headers.insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&content_range)
                .expect("content range should be a valid header"),
        );
    }

    response
}

/// Handles transcode response for media streaming and transcoding.
///
/// Inputs:
/// - `stream`: `ReaderStream<R>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `filename`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Response` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn transcode_response<R>(stream: ReaderStream<R>, filename: &str) -> Response
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("audio/aac"));
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&content_disposition(ContentDisposition::Inline, filename))
            .expect("sanitized content disposition should be a valid header"),
    );

    response
}

/// Handles hls manifest response for media streaming and transcoding.
///
/// Inputs:
/// - `body`: `Vec<u8>`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Response` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn hls_manifest_response(body: Vec<u8>) -> Response {
    let content_length = body.len();
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/vnd.apple.mpegurl"),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .expect("numeric content length should be a valid header"),
    );

    response
}

/// Handles hls segment response for media streaming and transcoding.
///
/// Inputs:
/// - `stream`: `ReaderStream<R>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `content_length`: `u64`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `Response` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn hls_segment_response<R>(stream: ReaderStream<R>, content_length: u64) -> Response
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("video/mp2t"));
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .expect("numeric content length should be a valid header"),
    );

    response
}

/// Handles range not satisfiable response for media streaming and transcoding.
///
/// Inputs:
/// - `file_size`: `u64`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `Response` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn range_not_satisfiable_response(file_size: u64) -> Response {
    let mut response = StatusCode::RANGE_NOT_SATISFIABLE.into_response();
    response
        .headers_mut()
        .insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    response.headers_mut().insert(
        header::CONTENT_RANGE,
        HeaderValue::from_str(&format!("bytes */{file_size}"))
            .expect("content range should be a valid header"),
    );
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    response
}

/// Opens a browser view or route for media streaming and transcoding.
///
/// Inputs:
/// - `path`: `&FsPath`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `File` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn open_original_file(path: &FsPath) -> Result<File, ApiError> {
    File::open(path).await.map_err(map_file_error)
}

/// Resolves configured or derived state for media streaming and transcoding.
///
/// Inputs:
/// - `state`: `&AppState`; expected to be Axum application state with a live repository and runtime configuration.
/// - `media_file`: `&MediaFile`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `PathBuf` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn resolve_original_file(state: &AppState, media_file: &MediaFile) -> Result<PathBuf, ApiError> {
    let raw_path = media_file
        .managed_path
        .as_deref()
        .unwrap_or(media_file.source_path.as_str());
    let original = PathBuf::from(raw_path);
    if !original.is_absolute() {
        tracing::warn!(
            media_file_id = %media_file.id,
            path = raw_path,
            "published media file has non-absolute original path"
        );
        return Err(media_file_not_found());
    }

    let library_root = PathBuf::from(state.system_config().library_root);
    let library_root = library_root.canonicalize().map_err(map_file_error)?;
    let original = original.canonicalize().map_err(map_file_error)?;
    if !original.starts_with(&library_root) {
        tracing::warn!(
            media_file_id = %media_file.id,
            path = %original.display(),
            library_root = %library_root.display(),
            "published media file resolved outside the managed library root"
        );
        return Err(media_file_not_found());
    }

    Ok(original)
}

/// Maps an internal value for media streaming and transcoding.
///
/// Inputs:
/// - `error`: `io:Error`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ApiError` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn map_file_error(error: io::Error) -> ApiError {
    if error.kind() == io::ErrorKind::NotFound {
        media_file_not_found()
    } else {
        tracing::error!(%error, "failed to access original media file");
        ApiError::Internal
    }
}

/// Handles media file not found for media streaming and transcoding.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `ApiError` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn media_file_not_found() -> ApiError {
    ApiError::NotFound("media original was not found".into())
}

/// Handles hls file not found for media streaming and transcoding.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `ApiError` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn hls_file_not_found() -> ApiError {
    ApiError::NotFound("HLS output was not found".into())
}

/// Parses and validates input for media streaming and transcoding.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `PlaybackItemType` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_media_item_type(value: &str) -> Result<PlaybackItemType, ApiError> {
    let normalized = value.to_ascii_lowercase().replace('-', "_");
    match normalized.as_str() {
        "tracks" => Ok(PlaybackItemType::Track),
        "episodes" => Ok(PlaybackItemType::Episode),
        _ => PlaybackItemType::from_str(value).map_err(|_| {
            ApiError::BadRequest(format!(
                "unknown media item type: {value}; expected track or episode"
            ))
        }),
    }
}

/// Parses and validates input for media streaming and transcoding.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `AacTranscodeProfile` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_aac_transcode_profile(value: &str) -> Result<AacTranscodeProfile, ApiError> {
    AacTranscodeProfile::from_str(value).map_err(|_| {
        let profiles = AacTranscodeProfile::all()
            .iter()
            .map(|profile| profile.api_name())
            .collect::<Vec<_>>()
            .join(", ");
        ApiError::BadRequest(format!(
            "unknown AAC transcode profile: {value}; expected one of {profiles}"
        ))
    })
}

#[derive(Debug, Clone, Copy)]
/// Represents content disposition in the authenticated original media, direct transcode, and HLS HTTP API.
///
/// Functionality: Enumerates `Inline`, `Attachment` states or choices for authenticated original media, direct transcode, and HLS HTTP API.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/media.rs`.
enum ContentDisposition {
    Inline,
    Attachment,
}

/// Handles content disposition for media streaming and transcoding.
///
/// Inputs:
/// - `disposition`: `ContentDisposition`; expected to be a value satisfying the type contract shown in the function signature.
/// - `filename`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn content_disposition(disposition: ContentDisposition, filename: &str) -> String {
    let disposition = match disposition {
        ContentDisposition::Inline => "inline",
        ContentDisposition::Attachment => "attachment",
    };
    format!(r#"{disposition}; filename="{}""#, quoted_filename(filename))
}

/// Handles filename for path for media streaming and transcoding.
///
/// Inputs:
/// - `path`: `&FsPath`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn filename_for_path(path: &FsPath) -> String {
    path.file_name()
        .and_then(|filename| filename.to_str())
        .filter(|filename| !filename.trim().is_empty())
        .unwrap_or("original")
        .to_string()
}

/// Handles filename for transcode for media streaming and transcoding.
///
/// Inputs:
/// - `path`: `&FsPath`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `profile`: `AacTranscodeProfile`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn filename_for_transcode(path: &FsPath, profile: AacTranscodeProfile) -> String {
    let stem = path
        .file_stem()
        .and_then(|filename| filename.to_str())
        .filter(|filename| !filename.trim().is_empty())
        .unwrap_or("transcode");
    format!("{stem}-{}.aac", profile.api_name())
}

/// Handles hls manifest path for media streaming and transcoding.
///
/// Inputs:
/// - `media_file`: `&MediaFile`; expected to be a media domain value that has already passed upstream validation.
/// - `profile`: `AacTranscodeProfile`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `PathBuf` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn hls_manifest_path(media_file: &MediaFile, profile: AacTranscodeProfile) -> PathBuf {
    hls_output_dir(media_file, profile).join("manifest.m3u8")
}

/// Handles hls output dir for media streaming and transcoding.
///
/// Inputs:
/// - `media_file`: `&MediaFile`; expected to be a media domain value that has already passed upstream validation.
/// - `profile`: `AacTranscodeProfile`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `PathBuf` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn hls_output_dir(media_file: &MediaFile, profile: AacTranscodeProfile) -> PathBuf {
    std::env::temp_dir()
        .join("harmonixia-hls")
        .join(media_file.id.to_string())
        .join(safe_hls_path_fragment(&media_file.file_hash))
        .join(profile.api_name())
}

/// Handles safe hls path fragment for media streaming and transcoding.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn safe_hls_path_fragment(value: &str) -> String {
    let safe = value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_') {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
    if safe.is_empty() {
        "media".to_string()
    } else {
        safe
    }
}

/// Validates data for media streaming and transcoding.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `&str` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn validate_hls_segment_name(value: &str) -> Result<&str, ApiError> {
    if value.is_empty()
        || value.starts_with('.')
        || value.contains("..")
        || !value.ends_with(".ts")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(ApiError::BadRequest(
            "invalid HLS segment filename".to_string(),
        ));
    }
    Ok(value)
}

/// Handles quoted filename for media streaming and transcoding.
///
/// Inputs:
/// - `filename`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn quoted_filename(filename: &str) -> String {
    filename
        .chars()
        .map(|ch| match ch {
            '"' | '\\' => '_',
            ch if ch.is_control() => '_',
            ch => ch,
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Represents range selection in the authenticated original media, direct transcode, and HLS HTTP API.
///
/// Functionality: Enumerates `Full`, `Partial` states or choices for authenticated original media, direct transcode, and HLS HTTP API.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/media.rs`.
enum RangeSelection {
    Full,
    Partial { start: u64, end: u64 },
}

#[derive(Debug, Clone, Copy)]
/// Represents range not satisfiable in the authenticated original media, direct transcode, and HLS HTTP API.
///
/// Functionality: Acts as a marker or zero-field value for authenticated original media, direct transcode, and HLS HTTP API.
/// Dependencies: has no direct field dependencies beyond derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/media.rs`.
struct RangeNotSatisfiable;

/// Parses and validates input for media streaming and transcoding.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `file_size`: `u64`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `RangeSelection` on success or `RangeNotSatisfiable` when the operation cannot be completed.
///
/// Errors:
/// - Returns `RangeNotSatisfiable` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_range_header(
    value: &str,
    file_size: u64,
) -> Result<RangeSelection, RangeNotSatisfiable> {
    let value = value.trim();
    let Some((unit, range_set)) = value.split_once('=') else {
        return Err(RangeNotSatisfiable);
    };
    if !unit.trim().eq_ignore_ascii_case("bytes") {
        return Ok(RangeSelection::Full);
    }
    let range_set = range_set.trim();
    if range_set.is_empty() || range_set.contains(',') || file_size == 0 {
        return Err(RangeNotSatisfiable);
    }

    let Some((start, end)) = range_set.split_once('-') else {
        return Err(RangeNotSatisfiable);
    };
    let start = start.trim();
    let end = end.trim();

    if start.is_empty() {
        let suffix_length = parse_u64(end)?;
        if suffix_length == 0 {
            return Err(RangeNotSatisfiable);
        }
        let start = file_size.saturating_sub(suffix_length);
        return Ok(RangeSelection::Partial {
            start,
            end: file_size - 1,
        });
    }

    let start = parse_u64(start)?;
    if start >= file_size {
        return Err(RangeNotSatisfiable);
    }

    let end = if end.is_empty() {
        file_size - 1
    } else {
        let end = parse_u64(end)?;
        if end < start {
            return Err(RangeNotSatisfiable);
        }
        end.min(file_size - 1)
    };

    Ok(RangeSelection::Partial { start, end })
}

/// Parses and validates input for media streaming and transcoding.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `u64` on success or `RangeNotSatisfiable` when the operation cannot be completed.
///
/// Errors:
/// - Returns `RangeNotSatisfiable` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn parse_u64(value: &str) -> Result<u64, RangeNotSatisfiable> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(RangeNotSatisfiable);
    }
    value.parse().map_err(|_| RangeNotSatisfiable)
}

/// Maps an internal value for media streaming and transcoding.
///
/// Inputs:
/// - `error`: `DirectTranscodeError`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ApiError` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn map_direct_transcode_error(error: DirectTranscodeError) -> ApiError {
    tracing::error!(%error, "failed to start direct AAC transcode");
    ApiError::Internal
}

/// Handles path is file for media streaming and transcoding.
///
/// Inputs:
/// - `path`: `&FsPath`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `bool` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
async fn path_is_file(path: &FsPath) -> Result<bool, ApiError> {
    match tokio::fs::metadata(path).await {
        Ok(metadata) => Ok(metadata.is_file()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(map_hls_file_error(error)),
    }
}

/// Maps an internal value for media streaming and transcoding.
///
/// Inputs:
/// - `error`: `io:Error`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ApiError` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn map_hls_file_error(error: io::Error) -> ApiError {
    if error.kind() == io::ErrorKind::NotFound {
        hls_file_not_found()
    } else {
        tracing::error!(%error, "failed to access HLS output file");
        ApiError::Internal
    }
}

/// Maps an internal value for media streaming and transcoding.
///
/// Inputs:
/// - `error`: `HlsTranscodeError`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `ApiError` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn map_hls_transcode_error(error: HlsTranscodeError) -> ApiError {
    tracing::error!(%error, "failed to generate HLS AAC output");
    ApiError::Internal
}

#[cfg(test)]
mod tests {
    use super::{parse_range_header, RangeSelection};

    #[test]
    /// Handles parses byte ranges for media streaming and transcoding.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn parses_byte_ranges() {
        assert_eq!(
            parse_range_header("bytes=2-5", 10).unwrap(),
            RangeSelection::Partial { start: 2, end: 5 }
        );
        assert_eq!(
            parse_range_header("bytes=8-", 10).unwrap(),
            RangeSelection::Partial { start: 8, end: 9 }
        );
        assert_eq!(
            parse_range_header("bytes=-4", 10).unwrap(),
            RangeSelection::Partial { start: 6, end: 9 }
        );
        assert_eq!(
            parse_range_header("items=0-1", 10).unwrap(),
            RangeSelection::Full
        );
        assert!(parse_range_header("bytes=20-21", 10).is_err());
        assert!(parse_range_header("bytes=5-2", 10).is_err());
        assert!(parse_range_header("bytes=0-0,2-2", 10).is_err());
    }
}
