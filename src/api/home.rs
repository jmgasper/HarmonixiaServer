use axum::{extract::State, routing::get, Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::{
    auth::AuthenticatedUser,
    catalog::CatalogPodcastEpisode,
    domain::{
        Album, Artist, ArtworkAsset, ArtworkKind, CatalogEntityType, Episode,
        PlaybackContextType, PlaybackHistoryEvent, PlaybackItemType, PlaybackProgress, Podcast,
        Track,
    },
    error::{ApiError, ErrorResponse},
    state::AppState,
};

pub fn router() -> Router<AppState> {
    Router::new().route("/", get(get_home))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HomeResponse {
    pub revision: u64,
    pub snapshot_at: DateTime<Utc>,
    pub sections: Vec<HomeSection>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HomeSection {
    pub id: HomeSectionId,
    pub title: String,
    pub position: u32,
    pub items: Vec<HomeCard>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HomeSectionId {
    ContinueListening,
    RecentlyPlayed,
    NewReleases,
    LatestPodcasts,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HomeCard {
    pub id: String,
    pub item_type: HomeCardItemType,
    pub item_id: Uuid,
    pub title: String,
    pub subtitle: Option<String>,
    pub detail: Option<String>,
    pub artwork: Option<ScreenArtwork>,
    pub context: Option<ScreenContextHint>,
    pub progress: Option<PlaybackPositionHint>,
    pub played_at: Option<DateTime<Utc>>,
    pub released_at: Option<DateTime<Utc>>,
    pub actions: Vec<ScreenActionHint>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum HomeCardItemType {
    Track,
    Episode,
    Album,
    Podcast,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ScreenArtwork {
    pub id: Uuid,
    pub entity_type: CatalogEntityType,
    pub entity_id: Uuid,
    pub artwork_kind: ArtworkKind,
    pub mime_type: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ScreenContextHint {
    pub entity_type: CatalogEntityType,
    pub entity_id: Uuid,
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ScreenActionHint {
    pub action: String,
    pub method: String,
    pub href: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PlaybackPositionHint {
    pub position_seconds: u32,
    pub duration_seconds: Option<u32>,
    pub completed: bool,
    pub updated_at: DateTime<Utc>,
}

#[utoipa::path(
    get,
    path = "/api/v1/me/home",
    tag = "home",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Ordered authenticated Home screen read model with stable v1 sections, including latest podcast episode cards with artwork references, playback/open actions, and context hints.", body = HomeResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
pub async fn get_home(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
) -> Result<Json<HomeResponse>, ApiError> {
    Ok(Json(home_response(&state, account.id).await?))
}

pub async fn home_response(
    state: &AppState,
    account_id: Uuid,
) -> Result<HomeResponse, ApiError> {
    let snapshot_at = Utc::now();
    let revision = state.current_revision();

    Ok(HomeResponse {
        revision,
        snapshot_at,
        sections: home_sections(state, account_id, None).await?,
    })
}

pub async fn home_sections(
    state: &AppState,
    account_id: Uuid,
    only: Option<&[HomeSectionId]>,
) -> Result<Vec<HomeSection>, ApiError> {
    let include = |id: HomeSectionId| only.map(|ids| ids.contains(&id)).unwrap_or(true);
    let mut sections = Vec::new();

    if include(HomeSectionId::ContinueListening) {
        sections.push(continue_listening_section(state, account_id).await?);
    }
    if include(HomeSectionId::RecentlyPlayed) {
        sections.push(recently_played_section(state, account_id).await?);
    }
    if include(HomeSectionId::NewReleases) {
        sections.push(new_releases_section(state, account_id).await?);
    }
    if include(HomeSectionId::LatestPodcasts) {
        sections.push(latest_podcasts_section(state, account_id).await?);
    }

    Ok(sections)
}

pub async fn continue_listening_section(
    state: &AppState,
    account_id: Uuid,
) -> Result<HomeSection, ApiError> {
    let progress = state.playback_progress_for_account(account_id).await?;
    Ok(HomeSection {
        id: HomeSectionId::ContinueListening,
        title: "Continue listening".to_string(),
        position: 0,
        items: continue_listening_cards(state, account_id, progress).await?,
    })
}

pub async fn recently_played_section(
    state: &AppState,
    account_id: Uuid,
) -> Result<HomeSection, ApiError> {
    let history = state.playback_history_for_account(account_id, 20).await?;
    Ok(HomeSection {
        id: HomeSectionId::RecentlyPlayed,
        title: "Recently played".to_string(),
        position: 1,
        items: recently_played_cards(state, account_id, history).await?,
    })
}

pub async fn new_releases_section(
    state: &AppState,
    account_id: Uuid,
) -> Result<HomeSection, ApiError> {
    let albums = state.latest_albums(Some(12)).await?;
    Ok(HomeSection {
        id: HomeSectionId::NewReleases,
        title: "New releases".to_string(),
        position: 2,
        items: album_cards(state, account_id, albums).await?,
    })
}

pub async fn latest_podcasts_section(
    state: &AppState,
    account_id: Uuid,
) -> Result<HomeSection, ApiError> {
    let latest_podcast_episodes = state.latest_podcast_episodes(Some(12)).await?;
    Ok(HomeSection {
        id: HomeSectionId::LatestPodcasts,
        title: "Latest podcast episodes".to_string(),
        position: 3,
        items: latest_podcast_episode_cards(state, account_id, latest_podcast_episodes).await?,
    })
}

async fn continue_listening_cards(
    state: &AppState,
    account_id: Uuid,
    progress: Vec<PlaybackProgress>,
) -> Result<Vec<HomeCard>, ApiError> {
    let mut cards = Vec::new();
    for item in progress.into_iter().filter(|item| !item.completed).take(20) {
        if let Some(card) = playback_card(
            state,
            account_id,
            "continue_listening",
            item.item_type,
            item.item_id,
            Some(&item),
            None,
        )
        .await?
        {
            cards.push(card);
        }
    }
    Ok(cards)
}

async fn recently_played_cards(
    state: &AppState,
    account_id: Uuid,
    history: Vec<PlaybackHistoryEvent>,
) -> Result<Vec<HomeCard>, ApiError> {
    let mut cards = Vec::new();
    for item in history.into_iter().take(20) {
        if let Some(card) = playback_card(
            state,
            account_id,
            "recently_played",
            item.item_type,
            item.item_id,
            None,
            Some(&item),
        )
        .await?
        {
            cards.push(card);
        }
    }
    Ok(cards)
}

async fn album_cards(
    state: &AppState,
    account_id: Uuid,
    albums: Vec<Album>,
) -> Result<Vec<HomeCard>, ApiError> {
    let mut cards = Vec::new();
    for album in albums {
        let artist = match state.visible_artist(album.artist_id).await {
            Ok(artist) => artist,
            Err(ApiError::NotFound(_)) => continue,
            Err(error) => return Err(error),
        };
        cards.push(album_card(state, account_id, album, artist).await?);
    }
    Ok(cards)
}

async fn latest_podcast_episode_cards(
    state: &AppState,
    account_id: Uuid,
    episodes: Vec<CatalogPodcastEpisode>,
) -> Result<Vec<HomeCard>, ApiError> {
    let mut cards = Vec::new();
    for item in episodes {
        cards.push(
            episode_home_card(
                state,
                account_id,
                "latest_podcasts",
                item.episode,
                item.podcast,
                None,
                None,
            )
            .await?,
        );
    }
    Ok(cards)
}

async fn playback_card(
    state: &AppState,
    account_id: Uuid,
    section_id: &str,
    item_type: PlaybackItemType,
    item_id: Uuid,
    progress: Option<&PlaybackProgress>,
    history: Option<&PlaybackHistoryEvent>,
) -> Result<Option<HomeCard>, ApiError> {
    match item_type {
        PlaybackItemType::Track => {
            let track = match state.visible_track(item_id).await {
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
            Ok(Some(
                track_home_card(
                    state, account_id, section_id, track, album, artist, progress, history,
                )
                .await?,
            ))
        }
        PlaybackItemType::Episode => {
            let item = match state.visible_episode(item_id).await {
                Ok(item) => item,
                Err(ApiError::NotFound(_)) => return Ok(None),
                Err(error) => return Err(error),
            };
            Ok(Some(
                episode_home_card(
                    state,
                    account_id,
                    section_id,
                    item.episode,
                    item.podcast,
                    progress,
                    history,
                )
                .await?,
            ))
        }
    }
}

async fn track_home_card(
    state: &AppState,
    account_id: Uuid,
    section_id: &str,
    track: Track,
    album: Album,
    artist: Artist,
    progress: Option<&PlaybackProgress>,
    history: Option<&PlaybackHistoryEvent>,
) -> Result<HomeCard, ApiError> {
    let artwork = primary_artwork(
        state,
        account_id,
        CatalogEntityType::Album,
        album.id,
        ArtworkKind::Cover,
    )
    .await?;
    Ok(HomeCard {
        id: format!("{section_id}:track:{}", track.id),
        item_type: HomeCardItemType::Track,
        item_id: track.id,
        title: track.title,
        subtitle: Some(artist.name),
        detail: Some(album.title.clone()),
        artwork,
        context: Some(ScreenContextHint {
            entity_type: CatalogEntityType::Album,
            entity_id: album.id,
            title: album.title,
        }),
        progress: progress.map(progress_hint),
        played_at: history.map(|event| event.played_at),
        released_at: album.published_at,
        actions: vec![action_hint(
            if progress.is_some() { "resume" } else { "play" },
            "GET",
            format!("/api/v1/media/track/{}/original", track.id),
        )],
    })
}

async fn episode_home_card(
    state: &AppState,
    account_id: Uuid,
    section_id: &str,
    episode: Episode,
    podcast: Podcast,
    progress: Option<&PlaybackProgress>,
    history: Option<&PlaybackHistoryEvent>,
) -> Result<HomeCard, ApiError> {
    let artwork = primary_artwork(
        state,
        account_id,
        CatalogEntityType::Podcast,
        podcast.id,
        ArtworkKind::Cover,
    )
    .await?;
    Ok(HomeCard {
        id: format!("{section_id}:episode:{}", episode.id),
        item_type: HomeCardItemType::Episode,
        item_id: episode.id,
        title: episode.title,
        subtitle: Some(podcast.title.clone()),
        detail: episode
            .episode_number
            .map(|number| format!("Episode {number}")),
        artwork,
        context: Some(ScreenContextHint {
            entity_type: CatalogEntityType::Podcast,
            entity_id: podcast.id,
            title: podcast.title,
        }),
        progress: progress.map(progress_hint),
        played_at: history.map(|event| event.played_at),
        released_at: episode.published_at,
        actions: vec![
            action_hint(
                if progress.is_some() { "resume" } else { "play" },
                "GET",
                format!("/api/v1/media/episode/{}/original", episode.id),
            ),
            action_hint(
                "open",
                "GET",
                format!("/api/v1/catalog/episodes/{}", episode.id),
            ),
        ],
    })
}

async fn album_card(
    state: &AppState,
    account_id: Uuid,
    album: Album,
    artist: Artist,
) -> Result<HomeCard, ApiError> {
    let artwork = primary_artwork(
        state,
        account_id,
        CatalogEntityType::Album,
        album.id,
        ArtworkKind::Cover,
    )
    .await?;
    Ok(HomeCard {
        id: format!("new_releases:album:{}", album.id),
        item_type: HomeCardItemType::Album,
        item_id: album.id,
        title: album.title.clone(),
        subtitle: Some(artist.name),
        detail: album.release_year.map(|year| year.to_string()),
        artwork,
        context: Some(ScreenContextHint {
            entity_type: CatalogEntityType::Album,
            entity_id: album.id,
            title: album.title,
        }),
        progress: None,
        played_at: None,
        released_at: album.published_at,
        actions: vec![action_hint(
            "open",
            "GET",
            format!("/api/v1/catalog/albums/{}/detail", album.id),
        )],
    })
}

pub async fn primary_artwork(
    state: &AppState,
    account_id: Uuid,
    entity_type: CatalogEntityType,
    entity_id: Uuid,
    artwork_kind: ArtworkKind,
) -> Result<Option<ScreenArtwork>, ApiError> {
    match state
        .visible_artwork_assets(account_id, entity_type, entity_id, Some(artwork_kind))
        .await
    {
        Ok(artwork) => Ok(artwork.into_iter().next().map(screen_artwork)),
        Err(ApiError::NotFound(_)) => Ok(None),
        Err(error) => Err(error),
    }
}

fn screen_artwork(artwork: ArtworkAsset) -> ScreenArtwork {
    ScreenArtwork {
        id: artwork.id,
        entity_type: artwork.entity_type,
        entity_id: artwork.entity_id,
        artwork_kind: artwork.artwork_kind,
        mime_type: artwork.mime_type,
        width: artwork.width,
        height: artwork.height,
        url: format!("/api/v1/artwork/{}", artwork.id),
    }
}

fn progress_hint(progress: &PlaybackProgress) -> PlaybackPositionHint {
    PlaybackPositionHint {
        position_seconds: progress.position_seconds,
        duration_seconds: progress.duration_seconds,
        completed: progress.completed,
        updated_at: progress.updated_at,
    }
}

pub fn action_hint(
    action: impl Into<String>,
    method: impl Into<String>,
    href: impl Into<String>,
) -> ScreenActionHint {
    ScreenActionHint {
        action: action.into(),
        method: method.into(),
        href: href.into(),
    }
}

pub fn playback_context_hint(
    context_type: Option<PlaybackContextType>,
    context_id: Option<Uuid>,
    fallback_entity_type: CatalogEntityType,
    fallback_entity_id: Uuid,
    fallback_title: String,
) -> ScreenContextHint {
    let (entity_type, entity_id) = match (context_type, context_id) {
        (Some(PlaybackContextType::Album), Some(id)) => (CatalogEntityType::Album, id),
        (Some(PlaybackContextType::Playlist), Some(id)) => (CatalogEntityType::Playlist, id),
        (Some(PlaybackContextType::Podcast), Some(id)) => (CatalogEntityType::Podcast, id),
        _ => (fallback_entity_type, fallback_entity_id),
    };

    ScreenContextHint {
        entity_type,
        entity_id,
        title: fallback_title,
    }
}
