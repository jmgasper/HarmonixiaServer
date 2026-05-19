use axum::{
    extract::{Path, State},
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    api::catalog::{artist_browse_items, ArtistBrowseItem},
    api::home::{
        action_hint, primary_artwork, progress_hint, PlaybackPositionHint, ScreenActionHint,
        ScreenArtwork,
    },
    auth::AuthenticatedUser,
    domain::{
        Album, ArtworkKind, CatalogEntityType, Episode, PlaybackItemType, Playlist, Podcast,
    },
    error::{ApiError, ErrorResponse},
    state::AppState,
};

const STARTUP_BROWSE_LIMIT: u32 = 30;

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/playlists/snapshot", get(playlist_list_snapshot))
        .route("/artists/snapshot", get(artists_browse_snapshot))
        .route("/albums/snapshot", get(albums_browse_snapshot))
        .route("/podcasts/snapshot", get(podcasts_browse_snapshot))
        .route("/podcasts/:podcast_id/detail", get(podcast_detail))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PlaylistListSnapshot {
    pub revision: u64,
    pub snapshot_at: DateTime<Utc>,
    pub playlists: Vec<Playlist>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ArtistsBrowseSnapshot {
    pub revision: u64,
    pub snapshot_at: DateTime<Utc>,
    pub artists: Vec<ArtistBrowseItem>,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AlbumsBrowseSnapshot {
    pub revision: u64,
    pub snapshot_at: DateTime<Utc>,
    pub albums: Vec<Album>,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PodcastsBrowseSnapshot {
    pub revision: u64,
    pub snapshot_at: DateTime<Utc>,
    pub podcasts: Vec<Podcast>,
    pub limit: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PodcastDetailReadModel {
    pub revision: u64,
    pub snapshot_at: DateTime<Utc>,
    pub podcast: Podcast,
    pub primary_artwork: Option<ScreenArtwork>,
    pub episodes: Vec<PodcastDetailEpisode>,
    pub actions: Vec<ScreenActionHint>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PodcastDetailEpisode {
    pub episode: Episode,
    pub resume: Option<PlaybackPositionHint>,
    pub actions: Vec<ScreenActionHint>,
}

#[utoipa::path(
    get,
    path = "/api/v1/startup/playlists/snapshot",
    tag = "startup",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Startup playlist list snapshot", body = PlaylistListSnapshot),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
pub async fn playlist_list_snapshot(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
) -> Result<Json<PlaylistListSnapshot>, ApiError> {
    Ok(Json(PlaylistListSnapshot {
        revision: state.current_revision(),
        snapshot_at: Utc::now(),
        playlists: state.playlists_for_startup_snapshot(account.id).await?,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/startup/artists/snapshot",
    tag = "startup",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Startup artist browse snapshot", body = ArtistsBrowseSnapshot),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
pub async fn artists_browse_snapshot(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
) -> Result<Json<ArtistsBrowseSnapshot>, ApiError> {
    let artists = state.startup_browse_artists(STARTUP_BROWSE_LIMIT).await?;
    Ok(Json(ArtistsBrowseSnapshot {
        revision: state.current_revision(),
        snapshot_at: Utc::now(),
        artists: artist_browse_items(&state, account.id, artists).await?,
        limit: STARTUP_BROWSE_LIMIT,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/startup/albums/snapshot",
    tag = "startup",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Startup album browse snapshot", body = AlbumsBrowseSnapshot),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
pub async fn albums_browse_snapshot(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
) -> Result<Json<AlbumsBrowseSnapshot>, ApiError> {
    Ok(Json(AlbumsBrowseSnapshot {
        revision: state.current_revision(),
        snapshot_at: Utc::now(),
        albums: state.startup_browse_albums(STARTUP_BROWSE_LIMIT).await?,
        limit: STARTUP_BROWSE_LIMIT,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/startup/podcasts/snapshot",
    tag = "startup",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Startup podcast browse snapshot", body = PodcastsBrowseSnapshot),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
pub async fn podcasts_browse_snapshot(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
) -> Result<Json<PodcastsBrowseSnapshot>, ApiError> {
    Ok(Json(PodcastsBrowseSnapshot {
        revision: state.current_revision(),
        snapshot_at: Utc::now(),
        podcasts: state.startup_browse_podcasts(STARTUP_BROWSE_LIMIT).await?,
        limit: STARTUP_BROWSE_LIMIT,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/startup/podcasts/{podcast_id}/detail",
    tag = "startup",
    security(("basicAuth" = [])),
    params(("podcast_id" = Uuid, Path, description = "Published podcast id")),
    responses(
        (status = 200, description = "Startup podcast detail read model with episodes and resume hints", body = PodcastDetailReadModel),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Podcast not found or not visible", body = ErrorResponse)
    )
)]
pub async fn podcast_detail(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(podcast_id): Path<Uuid>,
) -> Result<Json<PodcastDetailReadModel>, ApiError> {
    let (podcast, episodes) = state.podcast_detail(podcast_id).await?;
    let primary_artwork = primary_artwork(
        &state,
        account.id,
        CatalogEntityType::Podcast,
        podcast.id,
        ArtworkKind::Cover,
    )
    .await?;

    let mut episode_entries = Vec::with_capacity(episodes.len());
    for episode in episodes {
        let resume = state
            .optional_playback_progress_for_item(account.id, PlaybackItemType::Episode, episode.id)
            .await?
            .map(|progress| progress_hint(&progress));
        let action = if resume.is_some() { "resume" } else { "play" };
        episode_entries.push(PodcastDetailEpisode {
            actions: vec![action_hint(
                action,
                "GET",
                format!("/api/v1/media/episode/{}/original", episode.id),
            )],
            episode,
            resume,
        });
    }

    Ok(Json(PodcastDetailReadModel {
        revision: state.current_revision(),
        snapshot_at: Utc::now(),
        actions: vec![action_hint(
            "open",
            "GET",
            format!("/api/v1/startup/podcasts/{podcast_id}/detail"),
        )],
        podcast,
        primary_artwork,
        episodes: episode_entries,
    }))
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use serde_json::Value;

    use super::*;

    #[test]
    fn startup_browse_limit_is_thirty() {
        assert_eq!(STARTUP_BROWSE_LIMIT, 30);
    }

    #[test]
    fn podcast_detail_episode_serializes_resume_hint() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let entry = PodcastDetailEpisode {
            episode: Episode {
                id: Uuid::new_v4(),
                podcast_id: Uuid::new_v4(),
                title: "Episode".to_string(),
                normalized_title: "episode".to_string(),
                season_number: Some(1),
                episode_number: Some(2),
                duration_seconds: Some(1800),
                stable_grouping: true,
                published_at: Some(now),
                created_at: now,
                updated_at: now,
            },
            resume: Some(PlaybackPositionHint {
                position_seconds: 120,
                duration_seconds: Some(1800),
                completed: false,
                updated_at: now,
            }),
            actions: vec![action_hint(
                "resume",
                "GET",
                "/api/v1/media/episode/test/original",
            )],
        };

        let value = serde_json::to_value(&entry).unwrap();
        let decoded: PodcastDetailEpisode = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(value["resume"]["position_seconds"], Value::from(120));
        assert_eq!(decoded.resume.unwrap().position_seconds, 120);
    }
}
