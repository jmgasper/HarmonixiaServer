use std::{
    io::{self, Cursor},
    path::{Path as FsPath, PathBuf},
};

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header, HeaderValue, StatusCode},
    response::Response,
    routing::get,
    Json, Router,
};
use image::imageops::FilterType;
use serde::{Deserialize, Serialize};
use tokio::{fs::File, task};
use tokio_util::io::ReaderStream;
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{
    auth::AuthenticatedUser,
    domain::{ArtworkAsset, ArtworkKind, CatalogEntityType},
    error::{ApiError, ErrorResponse},
    state::AppState,
};

const MAX_RESIZE_DIMENSION: u32 = 4096;

pub fn router() -> Router<AppState> {
    Router::new().route("/:artwork_asset_id", get(get_artwork_image))
}

pub fn catalog_router() -> Router<AppState> {
    Router::new().route(
        "/:entity_type/:entity_id/artwork",
        get(get_catalog_entity_artwork),
    )
}

#[derive(Debug, Clone, Deserialize, IntoParams, ToSchema)]
pub struct ArtworkLookupQuery {
    /// Optional artwork kind filter: cover, artist, fanart, thumbnail, or other.
    pub kind: Option<String>,
}

#[derive(Debug, Clone, Deserialize, IntoParams, ToSchema)]
pub struct ArtworkImageQuery {
    /// Optional target width in pixels. When only width is supplied, height is derived from the original aspect ratio.
    pub width: Option<u32>,
    /// Optional target height in pixels. When only height is supplied, width is derived from the original aspect ratio.
    pub height: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArtworkAssetResponse {
    pub id: Uuid,
    pub entity_type: CatalogEntityType,
    pub entity_id: Uuid,
    pub artwork_kind: ArtworkKind,
    pub mime_type: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub confidence: f32,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArtworkAssetsResponse {
    pub artwork: Vec<ArtworkAssetResponse>,
}

#[utoipa::path(
    get,
    path = "/api/v1/catalog/{entity_type}/{entity_id}/artwork",
    tag = "catalog",
    security(("basicAuth" = [])),
    params(
        ("entity_type" = String, Path, description = "Catalog entity type with artwork: artist, band, album, track, podcast, or episode"),
        ("entity_id" = Uuid, Path, description = "Published visible catalog entity id"),
        ArtworkLookupQuery
    ),
    responses(
        (status = 200, description = "Artwork metadata for a published visible catalog entity. Internal file paths and source URIs are not exposed.", body = ArtworkAssetsResponse),
        (status = 400, description = "Invalid entity type or artwork kind", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Catalog entity is not published, not visible, or not found", body = ErrorResponse)
    )
)]
pub async fn get_catalog_entity_artwork(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
    Path((entity_type, entity_id)): Path<(String, Uuid)>,
    Query(query): Query<ArtworkLookupQuery>,
) -> Result<Json<ArtworkAssetsResponse>, ApiError> {
    let entity_type = parse_artwork_entity_type(&entity_type)?;
    let artwork_kind = parse_optional_artwork_kind(query.kind.as_deref())?;
    let artwork = state
        .visible_artwork_assets(entity_type, entity_id, artwork_kind)
        .await?;

    Ok(Json(ArtworkAssetsResponse {
        artwork: artwork.iter().map(artwork_response).collect(),
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/artwork/{artwork_asset_id}",
    tag = "artwork",
    security(("basicAuth" = [])),
    params(
        ("artwork_asset_id" = Uuid, Path, description = "Artwork asset id from catalog artwork metadata"),
        ArtworkImageQuery
    ),
    responses(
        (status = 200, description = "Authenticated artwork image. Without width or height query parameters, the original full-size image is returned.", content_type = "image/*"),
        (status = 400, description = "Invalid resize parameters or unsupported image data", body = ErrorResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Artwork asset, backing file, or visible catalog entity was not found", body = ErrorResponse)
    )
)]
pub async fn get_artwork_image(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
    Path(artwork_asset_id): Path<Uuid>,
    Query(query): Query<ArtworkImageQuery>,
) -> Result<Response, ApiError> {
    let resize = validate_resize_request(&query)?;
    let artwork = state.visible_artwork_asset(artwork_asset_id).await?;
    let path = resolve_artwork_file(&state, &artwork)?;
    let file = File::open(&path).await.map_err(map_artwork_file_error)?;
    let metadata = file.metadata().await.map_err(map_artwork_file_error)?;
    if !metadata.is_file() {
        return Err(artwork_not_found());
    }

    match resize {
        None => Ok(original_artwork_response(
            ReaderStream::new(file),
            &artwork,
            &path,
            metadata.len(),
        )),
        Some(resize) => {
            let bytes = tokio::fs::read(&path)
                .await
                .map_err(map_artwork_file_error)?;
            let resized = resize_artwork(bytes, artwork.mime_type.as_deref(), resize).await?;
            Ok(resized_artwork_response(resized, &path))
        }
    }
}

fn artwork_response(artwork: &ArtworkAsset) -> ArtworkAssetResponse {
    ArtworkAssetResponse {
        id: artwork.id,
        entity_type: artwork.entity_type,
        entity_id: artwork.entity_id,
        artwork_kind: artwork.artwork_kind,
        mime_type: artwork.mime_type.clone(),
        width: artwork.width,
        height: artwork.height,
        confidence: artwork.confidence,
        url: format!("/api/v1/artwork/{}", artwork.id),
    }
}

fn original_artwork_response<R>(
    stream: ReaderStream<R>,
    artwork: &ArtworkAsset,
    path: &FsPath,
    content_length: u64,
) -> Response
where
    R: tokio::io::AsyncRead + Unpin + Send + 'static,
{
    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        artwork_content_type(artwork.mime_type.as_deref(), path),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .expect("numeric content length should be a valid header"),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&content_disposition(path))
            .expect("sanitized content disposition should be a valid header"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=3600"),
    );
    response
}

fn resized_artwork_response(resized: ResizedArtwork, path: &FsPath) -> Response {
    let content_length = resized.bytes.len();
    let mut response = Response::new(Body::from(resized.bytes));
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(resized.mime_type),
    );
    headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from_str(&content_length.to_string())
            .expect("numeric content length should be a valid header"),
    );
    headers.insert(
        header::CONTENT_DISPOSITION,
        HeaderValue::from_str(&content_disposition(path))
            .expect("sanitized content disposition should be a valid header"),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=3600"),
    );
    response
}

fn resolve_artwork_file(state: &AppState, artwork: &ArtworkAsset) -> Result<PathBuf, ApiError> {
    let Some(raw_path) = artwork.file_path.as_deref() else {
        return Err(artwork_not_found());
    };
    let path = PathBuf::from(raw_path);
    if !path.is_absolute() {
        tracing::warn!(
            artwork_asset_id = %artwork.id,
            path = raw_path,
            "published artwork asset has non-absolute file path"
        );
        return Err(artwork_not_found());
    }

    let path = path.canonicalize().map_err(map_artwork_file_error)?;
    let library_root = PathBuf::from(state.system_config().library_root)
        .canonicalize()
        .map_err(map_artwork_file_error)?;
    if path.starts_with(&library_root) {
        return Ok(path);
    }

    let dropbox_root = PathBuf::from(state.system_config().dropbox_root)
        .canonicalize()
        .map_err(map_artwork_file_error)?;
    if path.starts_with(&dropbox_root) {
        return Ok(path);
    }

    tracing::warn!(
        artwork_asset_id = %artwork.id,
        path = %path.display(),
        library_root = %library_root.display(),
        dropbox_root = %dropbox_root.display(),
        "published artwork asset resolved outside configured media roots"
    );
    Err(artwork_not_found())
}

fn parse_artwork_entity_type(value: &str) -> Result<CatalogEntityType, ApiError> {
    match value.to_ascii_lowercase().replace('-', "_").as_str() {
        "artist" | "artists" | "band" | "bands" => Ok(CatalogEntityType::Artist),
        "album" | "albums" => Ok(CatalogEntityType::Album),
        "track" | "tracks" => Ok(CatalogEntityType::Track),
        "podcast" | "podcasts" => Ok(CatalogEntityType::Podcast),
        "episode" | "episodes" => Ok(CatalogEntityType::Episode),
        _ => Err(ApiError::BadRequest(format!(
            "unknown artwork entity type: {value}; expected artist, band, album, track, podcast, or episode"
        ))),
    }
}

fn parse_optional_artwork_kind(value: Option<&str>) -> Result<Option<ArtworkKind>, ApiError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    parse_artwork_kind(value).map(Some)
}

fn parse_artwork_kind(value: &str) -> Result<ArtworkKind, ApiError> {
    match value.to_ascii_lowercase().replace('-', "_").as_str() {
        "cover" => Ok(ArtworkKind::Cover),
        "artist" | "band" => Ok(ArtworkKind::Artist),
        "fanart" | "fan_art" => Ok(ArtworkKind::Fanart),
        "thumbnail" | "thumb" => Ok(ArtworkKind::Thumbnail),
        "other" => Ok(ArtworkKind::Other),
        _ => Err(ApiError::BadRequest(format!(
            "unknown artwork kind: {value}; expected cover, artist, fanart, thumbnail, or other"
        ))),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ResizeRequest {
    width: Option<u32>,
    height: Option<u32>,
}

fn validate_resize_request(query: &ArtworkImageQuery) -> Result<Option<ResizeRequest>, ApiError> {
    let resize = ResizeRequest {
        width: query.width,
        height: query.height,
    };
    if resize.width.is_none() && resize.height.is_none() {
        return Ok(None);
    }
    for (name, value) in [("width", resize.width), ("height", resize.height)] {
        if matches!(value, Some(0)) {
            return Err(ApiError::BadRequest(format!("{name} must be greater than zero")));
        }
        if value.is_some_and(|value| value > MAX_RESIZE_DIMENSION) {
            return Err(ApiError::BadRequest(format!(
                "{name} must be less than or equal to {MAX_RESIZE_DIMENSION}"
            )));
        }
    }
    Ok(Some(resize))
}

struct ResizedArtwork {
    bytes: Vec<u8>,
    mime_type: &'static str,
}

async fn resize_artwork(
    bytes: Vec<u8>,
    source_mime_type: Option<&str>,
    resize: ResizeRequest,
) -> Result<ResizedArtwork, ApiError> {
    let source_mime_type = source_mime_type.map(str::to_string);
    task::spawn_blocking(move || resize_artwork_blocking(&bytes, source_mime_type.as_deref(), resize))
        .await
        .map_err(|error| {
            tracing::error!(%error, "artwork resize task failed");
            ApiError::Internal
        })?
}

fn resize_artwork_blocking(
    bytes: &[u8],
    source_mime_type: Option<&str>,
    resize: ResizeRequest,
) -> Result<ResizedArtwork, ApiError> {
    let image = image::load_from_memory(bytes).map_err(|error| {
        tracing::warn!(%error, "failed to decode artwork image");
        ApiError::BadRequest("artwork image could not be decoded for resizing".into())
    })?;
    let source_width = image.width();
    let source_height = image.height();
    if source_width == 0 || source_height == 0 {
        return Err(ApiError::BadRequest(
            "artwork image has invalid dimensions".into(),
        ));
    }

    let (width, height) = resized_dimensions(source_width, source_height, resize);
    let resized = image.resize(width, height, FilterType::Lanczos3);
    let (format, mime_type) = output_format(source_mime_type);
    let mut cursor = Cursor::new(Vec::new());
    resized.write_to(&mut cursor, format).map_err(|error| {
        tracing::warn!(%error, "failed to encode resized artwork image");
        ApiError::BadRequest("artwork image could not be encoded after resizing".into())
    })?;

    Ok(ResizedArtwork {
        bytes: cursor.into_inner(),
        mime_type,
    })
}

fn resized_dimensions(source_width: u32, source_height: u32, resize: ResizeRequest) -> (u32, u32) {
    match (resize.width, resize.height) {
        (Some(width), Some(height)) => (width, height),
        (Some(width), None) => {
            let height = ((source_height as u64 * width as u64) / source_width as u64)
                .max(1)
                .min(MAX_RESIZE_DIMENSION as u64) as u32;
            (width, height)
        }
        (None, Some(height)) => {
            let width = ((source_width as u64 * height as u64) / source_height as u64)
                .max(1)
                .min(MAX_RESIZE_DIMENSION as u64) as u32;
            (width, height)
        }
        (None, None) => (source_width, source_height),
    }
}

fn output_format(source_mime_type: Option<&str>) -> (image::ImageOutputFormat, &'static str) {
    match source_mime_type
        .map(|mime_type| mime_type.to_ascii_lowercase())
        .as_deref()
    {
        Some("image/jpeg") | Some("image/jpg") => (image::ImageOutputFormat::Jpeg(85), "image/jpeg"),
        Some("image/png") => (image::ImageOutputFormat::Png, "image/png"),
        _ => (image::ImageOutputFormat::Png, "image/png"),
    }
}

fn artwork_content_type(mime_type: Option<&str>, path: &FsPath) -> HeaderValue {
    mime_type
        .and_then(|mime_type| HeaderValue::from_str(mime_type).ok())
        .unwrap_or_else(|| HeaderValue::from_static(mime_type_for_path(path)))
}

fn mime_type_for_path(path: &FsPath) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_ascii_lowercase())
        .as_deref()
    {
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("png") => "image/png",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("bmp") => "image/bmp",
        Some("tif") | Some("tiff") => "image/tiff",
        _ => "application/octet-stream",
    }
}

fn content_disposition(path: &FsPath) -> String {
    let filename = path
        .file_name()
        .and_then(|filename| filename.to_str())
        .filter(|filename| !filename.trim().is_empty())
        .unwrap_or("artwork");
    format!(r#"inline; filename="{}""#, quoted_filename(filename))
}

fn quoted_filename(filename: &str) -> String {
    filename
        .chars()
        .map(|ch| match ch {
            '"' | '\\' | '\r' | '\n' => '_',
            _ => ch,
        })
        .collect()
}

fn map_artwork_file_error(error: io::Error) -> ApiError {
    if error.kind() == io::ErrorKind::NotFound {
        artwork_not_found()
    } else {
        tracing::error!(%error, "failed to access artwork file");
        ApiError::Internal
    }
}

fn artwork_not_found() -> ApiError {
    ApiError::NotFound("artwork asset was not found".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_missing_resize_dimension_from_aspect_ratio() {
        assert_eq!(
            resized_dimensions(
                1200,
                600,
                ResizeRequest {
                    width: Some(300),
                    height: None,
                },
            ),
            (300, 150)
        );
        assert_eq!(
            resized_dimensions(
                1200,
                600,
                ResizeRequest {
                    width: None,
                    height: Some(300),
                },
            ),
            (600, 300)
        );
    }

    #[test]
    fn validates_resize_bounds() {
        assert!(validate_resize_request(&ArtworkImageQuery {
            width: None,
            height: None,
        })
        .unwrap()
        .is_none());
        assert!(validate_resize_request(&ArtworkImageQuery {
            width: Some(0),
            height: None,
        })
        .is_err());
        assert!(validate_resize_request(&ArtworkImageQuery {
            width: Some(MAX_RESIZE_DIMENSION + 1),
            height: None,
        })
        .is_err());
    }

    #[test]
    fn parses_band_alias_as_artist_artwork_entity() {
        assert_eq!(
            parse_artwork_entity_type("band").unwrap(),
            CatalogEntityType::Artist
        );
    }
}
