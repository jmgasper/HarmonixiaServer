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
    api::{
        artwork::ArtworkAssetResponse,
        media::{filename_for_downloadable_transcode, filename_for_original_media_file},
    },
    auth::AuthenticatedUser,
    domain::{
        AacTranscodeProfile, Album, Artist, ArtworkAsset, CatalogEntityType, Episode,
        MediaFile, PlaybackItemType, Playlist, PlaylistItem, Track,
    },
    error::{ApiError, ErrorResponse},
    state::AppState,
};

pub fn router() -> Router<AppState> {
    Router::new()
        .route("/albums/:album_id/snapshot", get(album_sync_snapshot))
        .route(
            "/playlists/:playlist_id/snapshot",
            get(playlist_sync_snapshot),
        )
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AlbumSyncSnapshot {
    pub album: Album,
    pub artist: Artist,
    pub tracks: Vec<SyncTrackEntry>,
    pub artwork: Vec<ArtworkAssetResponse>,
    pub revision: String,
    pub snapshot_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SyncTrackEntry {
    pub track: Track,
    pub download_variants: Vec<DownloadVariantEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq, Eq)]
pub struct DownloadVariantEntry {
    pub profile: String,
    pub url: String,
    pub filename: String,
    pub mime_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PlaylistSyncSnapshot {
    pub playlist: Playlist,
    pub items: Vec<SyncPlaylistItemEntry>,
    pub artwork: Vec<ArtworkAssetResponse>,
    pub revision: String,
    pub snapshot_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SyncPlaylistItemEntry {
    pub item: PlaylistItem,
    pub track: Option<Track>,
    pub episode: Option<Episode>,
    pub download_variants: Vec<DownloadVariantEntry>,
}

#[utoipa::path(
    get,
    path = "/api/v1/sync/albums/{album_id}/snapshot",
    tag = "sync",
    security(("basicAuth" = [])),
    params(
        ("album_id" = Uuid, Path, description = "Published album id to snapshot for offline sync")
    ),
    responses(
        (status = 200, description = "Album offline-sync snapshot with denormalized artist, ordered tracks, artwork references, revision, and stable download URLs.", body = AlbumSyncSnapshot),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Album, artist, artwork, or canonical media was not found", body = ErrorResponse)
    )
)]
pub async fn album_sync_snapshot(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(album_id): Path<Uuid>,
) -> Result<Json<AlbumSyncSnapshot>, ApiError> {
    let album = state.visible_album(album_id).await?;
    let artist = state.visible_artist(album.artist_id).await?;
    let tracks = state.visible_tracks_for_album(album.id).await?;
    let artwork = state
        .visible_artwork_assets(account.id, CatalogEntityType::Album, album.id, None)
        .await?;

    let mut entries = Vec::with_capacity(tracks.len());
    for track in &tracks {
        let media_file = state
            .visible_original_media_file(PlaybackItemType::Track, track.id)
            .await?;
        entries.push(SyncTrackEntry {
            track: track.clone(),
            download_variants: download_variant_entries(
                PlaybackItemType::Track,
                track.id,
                &media_file,
            ),
        });
    }

    Ok(Json(AlbumSyncSnapshot {
        revision: album_revision(&album, &tracks),
        album,
        artist,
        tracks: entries,
        artwork: artwork.iter().map(artwork_asset_response).collect(),
        snapshot_at: Utc::now(),
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/sync/playlists/{playlist_id}/snapshot",
    tag = "sync",
    security(("basicAuth" = [])),
    params(
        ("playlist_id" = Uuid, Path, description = "Visible playlist id to snapshot for offline sync")
    ),
    responses(
        (status = 200, description = "Playlist offline-sync snapshot with ordered items, resolved media entries, artwork references, revision, and stable download URLs.", body = PlaylistSyncSnapshot),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 404, description = "Playlist, playlist item media, artwork, or canonical media was not found", body = ErrorResponse)
    )
)]
pub async fn playlist_sync_snapshot(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
    Path(playlist_id): Path<Uuid>,
) -> Result<Json<PlaylistSyncSnapshot>, ApiError> {
    let playlist = state.visible_playlist(account.id, playlist_id).await?;
    let items = state
        .list_visible_playlist_items(account.id, playlist_id)
        .await?;
    let artwork = state
        .visible_artwork_assets(account.id, CatalogEntityType::Playlist, playlist.id, None)
        .await?;

    let mut entries = Vec::with_capacity(items.len());
    for item in items {
        let media_file = state
            .visible_original_media_file(item.item_type, item.item_id)
            .await?;
        let (track, episode) = match item.item_type {
            PlaybackItemType::Track => (Some(state.visible_track(item.item_id).await?), None),
            PlaybackItemType::Episode => {
                let episode = state.visible_episode(item.item_id).await?.episode;
                (None, Some(episode))
            }
        };
        entries.push(SyncPlaylistItemEntry {
            download_variants: download_variant_entries(
                item.item_type,
                item.item_id,
                &media_file,
            ),
            item,
            track,
            episode,
        });
    }

    Ok(Json(PlaylistSyncSnapshot {
        revision: playlist_revision(&playlist),
        playlist,
        items: entries,
        artwork: artwork.iter().map(artwork_asset_response).collect(),
        snapshot_at: Utc::now(),
    }))
}

fn album_revision(album: &Album, tracks: &[Track]) -> String {
    tracks
        .iter()
        .map(|track| track.updated_at)
        .max()
        .unwrap_or(album.updated_at)
        .max(album.updated_at)
        .to_rfc3339()
}

fn playlist_revision(playlist: &Playlist) -> String {
    playlist.updated_at.to_rfc3339()
}

fn download_variant_entries(
    item_type: PlaybackItemType,
    item_id: Uuid,
    media_file: &MediaFile,
) -> Vec<DownloadVariantEntry> {
    let mut variants = Vec::with_capacity(AacTranscodeProfile::all().len() + 1);
    variants.push(DownloadVariantEntry {
        profile: "original".to_string(),
        url: format!(
            "/api/v1/media/{}/{}/original/download",
            item_type.api_name(),
            item_id
        ),
        filename: filename_for_original_media_file(media_file),
        mime_type: media_file
            .mime_type
            .clone()
            .unwrap_or_else(|| "application/octet-stream".to_string()),
    });

    variants.extend(AacTranscodeProfile::all().iter().map(|profile| {
        DownloadVariantEntry {
            profile: profile.api_name().to_string(),
            url: format!(
                "/api/v1/media/{}/{}/transcode/{}/download",
                item_type.api_name(),
                item_id,
                profile.api_name()
            ),
            filename: filename_for_downloadable_transcode(media_file, *profile),
            mime_type: "audio/mp4".to_string(),
        }
    }));

    variants
}

fn artwork_asset_response(artwork: &ArtworkAsset) -> ArtworkAssetResponse {
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

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use crate::domain::{AlbumKind, MediaFileStatus, MediaKind};

    use super::*;

    #[test]
    fn album_revision_uses_latest_album_or_track_update() {
        let album_id = Uuid::new_v4();
        let artist_id = Uuid::new_v4();
        let album_updated = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let older_track_updated = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap();
        let newest_track_updated = Utc.with_ymd_and_hms(2026, 1, 3, 0, 0, 0).unwrap();

        let album = Album {
            id: album_id,
            artist_id,
            title: "Album".to_string(),
            normalized_title: "album".to_string(),
            album_kind: AlbumKind::Album,
            release_year: None,
            stable_grouping: true,
            published_at: Some(album_updated),
            created_at: album_updated,
            updated_at: album_updated,
        };
        let tracks = vec![
            test_track(album_id, artist_id, older_track_updated),
            test_track(album_id, artist_id, newest_track_updated),
        ];

        assert_eq!(album_revision(&album, &tracks), newest_track_updated.to_rfc3339());
    }

    #[test]
    fn download_variant_urls_cover_original_and_all_transcode_profiles() {
        let item_id = Uuid::new_v4();
        let media_file = test_media_file(item_id);
        let variants = download_variant_entries(PlaybackItemType::Track, item_id, &media_file);

        assert_eq!(
            variants
                .iter()
                .map(|variant| (variant.profile.clone(), variant.url.clone()))
                .collect::<Vec<_>>(),
            vec![
                (
                    "original".to_string(),
                    format!("/api/v1/media/track/{item_id}/original/download")
                ),
                (
                    "mobile".to_string(),
                    format!("/api/v1/media/track/{item_id}/transcode/mobile/download")
                ),
                (
                    "standard".to_string(),
                    format!("/api/v1/media/track/{item_id}/transcode/standard/download")
                ),
                (
                    "high".to_string(),
                    format!("/api/v1/media/track/{item_id}/transcode/high/download")
                ),
            ]
        );
    }

    fn test_track(album_id: Uuid, artist_id: Uuid, updated_at: DateTime<Utc>) -> Track {
        Track {
            id: Uuid::new_v4(),
            album_id,
            artist_id,
            title: "Track".to_string(),
            normalized_title: "track".to_string(),
            disc_number: Some(1),
            track_number: Some(1),
            duration_seconds: Some(180),
            stable_grouping: true,
            published_at: Some(updated_at),
            created_at: updated_at,
            updated_at,
        }
    }

    fn test_media_file(track_id: Uuid) -> MediaFile {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        MediaFile {
            id: Uuid::new_v4(),
            media_kind: MediaKind::Music,
            status: MediaFileStatus::Published,
            source_path: "/library/Track.flac".to_string(),
            managed_path: Some("/library/Track.flac".to_string()),
            file_hash: "hash".to_string(),
            file_size: 1024,
            mime_type: Some("audio/flac".to_string()),
            container: Some("flac".to_string()),
            audio_codec: Some("flac".to_string()),
            duration_seconds: Some(180),
            bitrate: None,
            sample_rate: Some(44_100),
            channels: Some(2),
            genres: Vec::new(),
            format_keys: Vec::new(),
            track_id: Some(track_id),
            episode_id: None,
            duplicate_of_media_file_id: None,
            import_job_id: None,
            discovered_at: now,
            published_at: Some(now),
            updated_at: now,
        }
    }
}
