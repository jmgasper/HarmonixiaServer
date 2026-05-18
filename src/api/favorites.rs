use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post, put},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    api::home::{action_hint, primary_artwork, ScreenActionHint, ScreenArtwork},
    auth::AuthenticatedUser,
    domain::{
        Album, Artist, ArtworkKind, CatalogEntityType, FavoriteToggleOutcome, Track,
        TrackFavorite,
    },
    error::{ApiError, ErrorResponse},
    state::AppState,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/", get(get_favorites))
        .route("/:track_id/toggle", post(toggle_track_favorite))
        .route("/:track_id", put(add_track_favorite).delete(remove_track_favorite))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FavoritesReadModel {
    pub revision: u64,
    pub snapshot_at: DateTime<Utc>,
    pub tracks: Vec<FavoriteTrackEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FavoriteTrackEntry {
    pub track: Track,
    pub album: Album,
    pub artist: Artist,
    pub artwork: Option<ScreenArtwork>,
    pub favorited_at: DateTime<Utc>,
    pub is_favorite: bool,
    pub actions: Vec<ScreenActionHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FavoriteToggleResponse {
    pub track_id: Uuid,
    pub is_favorite: bool,
    pub favorited_at: Option<DateTime<Utc>>,
}

#[utoipa::path(
    get,
    path = "/api/v1/me/favorites/tracks",
    tag = "favorites",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Account track favorites read model with track, album, artist, artwork, and action hints", body = FavoritesReadModel),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
pub async fn get_favorites(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
) -> Result<Json<FavoritesReadModel>, ApiError> {
    let tracks = favorite_track_entries(&state, account.id).await?;

    Ok(Json(FavoritesReadModel {
        revision: state.current_revision(),
        snapshot_at: Utc::now(),
        tracks,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/me/favorites/tracks/{track_id}/toggle",
    tag = "favorites",
    security(("basicAuth" = [])),
    params(("track_id" = Uuid, Path, description = "Published track id")),
    responses(
        (status = 200, description = "Track favorite toggle result", body = FavoriteToggleResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Track not found or not visible", body = ErrorResponse)
    )
)]
pub async fn toggle_track_favorite(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(track_id): Path<Uuid>,
) -> Result<Json<FavoriteToggleResponse>, ApiError> {
    state.visible_track(track_id).await?;
    let outcome = state.toggle_track_favorite(account.id, track_id).await?;
    let is_favorite = matches!(outcome, FavoriteToggleOutcome::Added);

    Ok(Json(FavoriteToggleResponse {
        track_id,
        is_favorite,
        favorited_at: is_favorite.then(Utc::now),
    }))
}

#[utoipa::path(
    put,
    path = "/api/v1/me/favorites/tracks/{track_id}",
    tag = "favorites",
    security(("basicAuth" = [])),
    params(("track_id" = Uuid, Path, description = "Published track id")),
    responses(
        (status = 201, description = "Track favorite added", body = FavoriteToggleResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Track not found or not visible", body = ErrorResponse)
    )
)]
pub async fn add_track_favorite(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(track_id): Path<Uuid>,
) -> Result<(StatusCode, Json<FavoriteToggleResponse>), ApiError> {
    state.visible_track(track_id).await?;
    let favorite = state.add_track_favorite(account.id, track_id).await?;

    Ok((
        StatusCode::CREATED,
        Json(FavoriteToggleResponse {
            track_id,
            is_favorite: true,
            favorited_at: Some(favorite.favorited_at),
        }),
    ))
}

#[utoipa::path(
    delete,
    path = "/api/v1/me/favorites/tracks/{track_id}",
    tag = "favorites",
    security(("basicAuth" = [])),
    params(("track_id" = Uuid, Path, description = "Published track id")),
    responses(
        (status = 204, description = "Track favorite removed"),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Track or favorite not found", body = ErrorResponse)
    )
)]
pub async fn remove_track_favorite(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(track_id): Path<Uuid>,
) -> Result<StatusCode, ApiError> {
    state.remove_track_favorite(account.id, track_id).await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn favorite_track_entries(
    state: &AppState,
    account_id: Uuid,
) -> Result<Vec<FavoriteTrackEntry>, ApiError> {
    let favorites = state.list_track_favorites(account_id).await?;
    let mut tracks = Vec::with_capacity(favorites.len());

    for favorite in favorites {
        if let Some(entry) = favorite_track_entry(state, account_id, favorite).await? {
            tracks.push(entry);
        }
    }

    Ok(tracks)
}

async fn favorite_track_entry(
    state: &AppState,
    account_id: Uuid,
    favorite: TrackFavorite,
) -> Result<Option<FavoriteTrackEntry>, ApiError> {
    let track = match state.visible_track(favorite.track_id).await {
        Ok(track) => track,
        Err(ApiError::NotFound(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    let album = match state.visible_album(track.album_id).await {
        Ok(album) => album,
        Err(ApiError::NotFound(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    let artist = match state.visible_artist(track.artist_id).await {
        Ok(artist) => artist,
        Err(ApiError::NotFound(_)) => return Ok(None),
        Err(error) => return Err(error),
    };
    let artwork = primary_artwork(
        state,
        account_id,
        CatalogEntityType::Album,
        album.id,
        ArtworkKind::Cover,
    )
    .await?;
    let actions = favorite_track_actions(track.id, album.id);

    Ok(Some(FavoriteTrackEntry {
        actions,
        track,
        album,
        artist,
        artwork,
        favorited_at: favorite.favorited_at,
        is_favorite: true,
    }))
}

fn favorite_track_actions(track_id: Uuid, album_id: Uuid) -> Vec<ScreenActionHint> {
    vec![
        action_hint(
            "play",
            "GET",
            format!("/api/v1/media/track/{track_id}/original"),
        ),
        action_hint(
            "open",
            "GET",
            format!("/api/v1/catalog/albums/{album_id}/detail"),
        ),
    ]
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::Value;

    use super::*;

    #[test]
    fn favorite_toggle_response_serializes_added() {
        let favorited_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let response = FavoriteToggleResponse {
            track_id: Uuid::new_v4(),
            is_favorite: true,
            favorited_at: Some(favorited_at),
        };

        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["is_favorite"], Value::Bool(true));
        assert!(value["favorited_at"].is_string());
    }

    #[test]
    fn favorite_toggle_response_serializes_removed() {
        let response = FavoriteToggleResponse {
            track_id: Uuid::new_v4(),
            is_favorite: false,
            favorited_at: None,
        };

        let value = serde_json::to_value(response).unwrap();
        assert_eq!(value["is_favorite"], Value::Bool(false));
        assert!(value["favorited_at"].is_null());
    }
}
