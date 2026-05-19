use std::{fmt, str::FromStr};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

pub const DEFAULT_SCAN_THREAD_COUNT: i32 = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents account role in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Admin`, `User` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/openapi.rs`, `src/domain.rs`, `src/state.rs`, and 2 more.
pub enum AccountRole {
    Admin,
    User,
}

impl AccountRole {
    /// Checks whether this account role grants administrator access for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn is_admin(self) -> bool {
        matches!(self, Self::Admin)
    }
}

#[derive(Debug, Clone)]
/// Represents local account in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `username`, `password_hash`, `role`, `disabled`, `created_at`, `updated_at` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `String`, `String`, `AccountRole`, `bool`, `DateTime<Utc>`, and 1 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/domain.rs`, `src/storage.rs`.
pub struct LocalAccount {
    pub id: Uuid,
    pub username: String,
    pub password_hash: String,
    pub role: AccountRole,
    pub disabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents authenticated account in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `username`, `role` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `String`, `AccountRole` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/openapi.rs`, `src/auth.rs`, `src/domain.rs`, and 1 more.
pub struct AuthenticatedAccount {
    pub id: Uuid,
    pub username: String,
    pub role: AccountRole,
}

impl From<LocalAccount> for AuthenticatedAccount {
    /// Converts from the source domain type for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - `account`: `LocalAccount`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn from(account: LocalAccount) -> Self {
        Self {
            id: account.id,
            username: account.username,
            role: account.role,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents user account in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `username`, `role`, `disabled`, `created_at`, `updated_at` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `String`, `AccountRole`, `bool`, `DateTime<Utc>`, `DateTime<Utc>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/openapi.rs`, `src/domain.rs`, `src/state.rs`.
pub struct UserAccount {
    pub id: Uuid,
    pub username: String,
    pub role: AccountRole,
    pub disabled: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<LocalAccount> for UserAccount {
    /// Converts from the source domain type for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - `account`: `LocalAccount`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn from(account: LocalAccount) -> Self {
        Self {
            id: account.id,
            username: account.username,
            role: account.role,
            disabled: account.disabled,
            created_at: account.created_at,
            updated_at: account.updated_at,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents playlist scope in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Personal`, `Shared` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/api/playlists.rs`, `src/catalog.rs`, `src/domain.rs`, and 2 more.
pub enum PlaylistScope {
    Personal,
    Shared,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents playlist in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `name`, `description`, `scope`, `owner_account_id`, `created_by_account_id`, `updated_by_account_id`, `created_at`, and 1 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `String`, `Option<String>`, `PlaylistScope`, `Option<Uuid>`, `Option<Uuid>`, and 3 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `src/api/playlists.rs`, `src/catalog.rs`, and 4 more.
pub struct Playlist {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub scope: PlaylistScope,
    pub owner_account_id: Option<Uuid>,
    pub created_by_account_id: Option<Uuid>,
    pub updated_by_account_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents playlist item in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `playlist_id`, `item_type`, `item_id`, `position`, `added_by_account_id`, `created_at` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `Uuid`, `PlaybackItemType`, `Uuid`, `u32`, `Option<Uuid>`, and 1 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/api/playlists.rs`, `src/domain.rs`, `src/state.rs`, and 1 more.
pub struct PlaylistItem {
    pub id: Uuid,
    pub playlist_id: Uuid,
    pub item_type: PlaybackItemType,
    pub item_id: Uuid,
    pub position: u32,
    pub added_by_account_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents playback item type in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Track`, `Episode` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/media.rs`, `src/api/openapi.rs`, `src/api/playback.rs`, and 4 more.
pub enum PlaybackItemType {
    Track,
    Episode,
}

impl PlaybackItemType {
    /// Verifies that api name.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&'static str` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn api_name(self) -> &'static str {
        match self {
            PlaybackItemType::Track => "track",
            PlaybackItemType::Episode => "episode",
        }
    }
}

impl fmt::Display for PlaybackItemType {
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
    /// - Returns `fmt::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.api_name())
    }
}

impl FromStr for PlaybackItemType {
    type Err = ();

    /// Parses a text representation into the domain value.
    ///
    /// Inputs:
    /// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Self` on success or `Self::Err` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `Self::Err` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().replace('-', "_").as_str() {
            "track" => Ok(PlaybackItemType::Track),
            "episode" => Ok(PlaybackItemType::Episode),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents the collection context that produced a playback event.
///
/// Functionality: Enumerates `Album`, `Playlist`, and `Podcast` contexts for server-side recently played grouping and resume hints.
/// Dependencies: depends on serde, utoipa schema derivation, and text parsing helpers.
/// Used by: referenced from `src/api/playback.rs`, `src/domain.rs`, `src/state.rs`, and `src/storage.rs`.
pub enum PlaybackContextType {
    Album,
    Playlist,
    Podcast,
}

impl PlaybackContextType {
    pub fn api_name(self) -> &'static str {
        match self {
            PlaybackContextType::Album => "album",
            PlaybackContextType::Playlist => "playlist",
            PlaybackContextType::Podcast => "podcast",
        }
    }
}

impl fmt::Display for PlaybackContextType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.api_name())
    }
}

impl FromStr for PlaybackContextType {
    type Err = ();

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().replace('-', "_").as_str() {
            "album" => Ok(PlaybackContextType::Album),
            "playlist" => Ok(PlaybackContextType::Playlist),
            "podcast" => Ok(PlaybackContextType::Podcast),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents playback progress in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `item_type`, `item_id`, `position_seconds`, `duration_seconds`, `completed`, `updated_at` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `PlaybackItemType`, `Uuid`, `u32`, `Option<u32>`, `bool`, `DateTime<Utc>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `src/api/playback.rs`, `src/domain.rs`, and 3 more.
pub struct PlaybackProgress {
    pub item_type: PlaybackItemType,
    pub item_id: Uuid,
    pub context_type: Option<PlaybackContextType>,
    pub context_id: Option<Uuid>,
    pub position_seconds: u32,
    pub duration_seconds: Option<u32>,
    pub completed: bool,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TrackFavorite {
    pub account_id: Uuid,
    pub track_id: Uuid,
    pub favorited_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum FavoriteToggleOutcome {
    Added,
    Removed,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents playback history event in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `item_type`, `item_id`, `position_seconds`, `duration_seconds`, `completed`, `played_at` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `PlaybackItemType`, `Uuid`, `u32`, `Option<u32>`, `bool`, and 1 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/api/playback.rs`, `src/domain.rs`, `src/state.rs`, and 1 more.
pub struct PlaybackHistoryEvent {
    pub id: Uuid,
    pub item_type: PlaybackItemType,
    pub item_id: Uuid,
    pub context_type: Option<PlaybackContextType>,
    pub context_id: Option<Uuid>,
    pub position_seconds: u32,
    pub duration_seconds: Option<u32>,
    pub completed: bool,
    pub played_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents system config in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `library_root`, `dropbox_root`, `podcast_subtree`, `transcode_concurrency_limit`, `scan_thread_count`, and `updated_at` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `String`, `String`, `String`, `i32`, `i32`, and `DateTime<Utc>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/config.rs`, `src/api/openapi.rs`, `src/domain.rs`, `src/pipeline.rs`, and 2 more.
pub struct SystemConfig {
    pub library_root: String,
    pub dropbox_root: String,
    pub podcast_subtree: String,
    pub public_base_url: Option<String>,
    pub transcode_concurrency_limit: i32,
    pub scan_thread_count: i32,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SonosTransportState {
    Stopped,
    Buffering,
    Paused,
    Playing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SonosSessionStatus {
    Active,
    Reconnecting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum SonosDeliveryKind {
    Original,
    TranscodeAacHigh,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct SonosSignedClaim {
    pub session_id: Uuid,
    pub session_generation: u64,
    pub item_generation: u64,
    pub target_id: String,
    pub item_type: PlaybackItemType,
    pub item_id: Uuid,
    pub delivery_kind: SonosDeliveryKind,
    pub exp: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents aac transcode profile in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Mobile`, `Standard`, `High` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/media.rs`, `src/api/openapi.rs`, `src/domain.rs`, `src/transcode.rs`, and 1 more.
pub enum AacTranscodeProfile {
    Mobile,
    Standard,
    High,
}

impl AacTranscodeProfile {
    /// Verifies that api name.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&'static str` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn api_name(self) -> &'static str {
        match self {
            AacTranscodeProfile::Mobile => "mobile",
            AacTranscodeProfile::Standard => "standard",
            AacTranscodeProfile::High => "high",
        }
    }

    /// Handles bitrate for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&'static str` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn bitrate(self) -> &'static str {
        match self {
            AacTranscodeProfile::Mobile => "64k",
            AacTranscodeProfile::Standard => "128k",
            AacTranscodeProfile::High => "256k",
        }
    }

    /// Handles all for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `&'static [AacTranscodeProfile]` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn all() -> &'static [AacTranscodeProfile] {
        &[
            AacTranscodeProfile::Mobile,
            AacTranscodeProfile::Standard,
            AacTranscodeProfile::High,
        ]
    }
}

impl fmt::Display for AacTranscodeProfile {
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
    /// - Returns `fmt::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.api_name())
    }
}

impl FromStr for AacTranscodeProfile {
    type Err = ();

    /// Parses a text representation into the domain value.
    ///
    /// Inputs:
    /// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Self` on success or `Self::Err` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `Self::Err` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().replace('-', "_").as_str() {
            "mobile" => Ok(AacTranscodeProfile::Mobile),
            "standard" => Ok(AacTranscodeProfile::Standard),
            "high" => Ok(AacTranscodeProfile::High),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, ToSchema)]
/// Represents transcode slot usage in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `limit`, `in_use`, `available` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `u32`, `u32`, `u32` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/media.rs`, `src/api/openapi.rs`, `src/domain.rs`, `src/state.rs`, and 1 more.
pub struct TranscodeSlotUsage {
    pub limit: u32,
    pub in_use: u32,
    pub available: u32,
}

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, ToSchema
)]
#[serde(rename_all = "snake_case")]
/// Represents provider kind in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `MusicBrainz`, `CoverArtArchive`, `Discogs`, `FanartTv`, `TheAudioDb`, `LocalSidecars` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/config.rs`, `src/api/maintenance.rs`, `src/api/openapi.rs`, `src/catalog.rs`, and 6 more.
pub enum ProviderKind {
    MusicBrainz,
    CoverArtArchive,
    Discogs,
    FanartTv,
    TheAudioDb,
    LocalSidecars,
}

impl ProviderKind {
    /// Handles display name for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&'static str` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn display_name(self) -> &'static str {
        match self {
            ProviderKind::MusicBrainz => "MusicBrainz",
            ProviderKind::CoverArtArchive => "Cover Art Archive",
            ProviderKind::Discogs => "Discogs",
            ProviderKind::FanartTv => "Fanart.tv",
            ProviderKind::TheAudioDb => "TheAudioDB",
            ProviderKind::LocalSidecars => "Local sidecars",
        }
    }

    /// Handles all for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `&'static [ProviderKind]` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn all() -> &'static [ProviderKind] {
        &[
            ProviderKind::MusicBrainz,
            ProviderKind::CoverArtArchive,
            ProviderKind::Discogs,
            ProviderKind::FanartTv,
            ProviderKind::TheAudioDb,
            ProviderKind::LocalSidecars,
        ]
    }

    /// Verifies that api name.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&'static str` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn api_name(self) -> &'static str {
        match self {
            ProviderKind::MusicBrainz => "music_brainz",
            ProviderKind::CoverArtArchive => "cover_art_archive",
            ProviderKind::Discogs => "discogs",
            ProviderKind::FanartTv => "fanart_tv",
            ProviderKind::TheAudioDb => "the_audio_db",
            ProviderKind::LocalSidecars => "local_sidecars",
        }
    }
}

impl fmt::Display for ProviderKind {
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
    /// - Returns `fmt::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.api_name())
    }
}

impl FromStr for ProviderKind {
    type Err = ();

    /// Parses a text representation into the domain value.
    ///
    /// Inputs:
    /// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Self` on success or `Self::Err` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `Self::Err` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().replace('-', "_").as_str() {
            "music_brainz" | "musicbrainz" | "mb" => Ok(ProviderKind::MusicBrainz),
            "cover_art_archive" | "coverartarchive" | "caa" => Ok(ProviderKind::CoverArtArchive),
            "discogs" => Ok(ProviderKind::Discogs),
            "fanart_tv" | "fanart" => Ok(ProviderKind::FanartTv),
            "the_audio_db" | "theaudiodb" | "audiodb" => Ok(ProviderKind::TheAudioDb),
            "local_sidecars" | "sidecars" | "local" => Ok(ProviderKind::LocalSidecars),
            _ => Err(()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents provider setting in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `provider`, `display_name`, `enabled`, `requires_api_key`, `api_key_configured`, `updated_at` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `ProviderKind`, `String`, `bool`, `bool`, `bool`, `DateTime<Utc>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/config.rs`, `src/api/openapi.rs`, `src/domain.rs`, `src/state.rs`, and 1 more.
pub struct ProviderSetting {
    pub provider: ProviderKind,
    pub display_name: String,
    pub enabled: bool,
    pub requires_api_key: bool,
    pub api_key_configured: bool,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents provider status in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Healthy`, `Degraded`, `BackingOff`, `Disabled`, `Unconfigured` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`, `src/domain.rs`, `src/pipeline.rs`, and 4 more.
pub enum ProviderStatus {
    Healthy,
    Degraded,
    BackingOff,
    Disabled,
    Unconfigured,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents provider health in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `provider`, `display_name`, `enabled`, `status`, `api_key_configured`, `maintenance_ready`, `failure_count`, `retry_after`, and 4 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `ProviderKind`, `String`, `bool`, `ProviderStatus`, `bool`, `bool`, and 6 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`, `src/domain.rs`, `src/pipeline.rs`, and 4 more.
pub struct ProviderHealth {
    pub provider: ProviderKind,
    pub display_name: String,
    pub enabled: bool,
    pub status: ProviderStatus,
    pub api_key_configured: bool,
    pub maintenance_ready: bool,
    pub failure_count: u32,
    pub retry_after: Option<DateTime<Utc>>,
    pub last_success_at: Option<DateTime<Utc>>,
    pub last_failure_at: Option<DateTime<Utc>>,
    pub message: Option<String>,
    pub updated_at: DateTime<Utc>,
}

impl ProviderHealth {
    /// Handles healthy for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    /// - `now`: `DateTime<Utc>`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn healthy(provider: ProviderKind, now: DateTime<Utc>) -> Self {
        Self {
            provider,
            display_name: provider.display_name().to_string(),
            enabled: true,
            status: ProviderStatus::Healthy,
            api_key_configured: false,
            maintenance_ready: true,
            failure_count: 0,
            retry_after: None,
            last_success_at: Some(now),
            last_failure_at: None,
            message: None,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents import job kind in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `InitialScan`, `DropboxIngest`, `FullRescan`, `SubtreeRescan`, `ProviderRepair`, `QuarantineRetry` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`, `src/domain.rs`, `src/pipeline.rs`, and 3 more.
pub enum ImportJobKind {
    InitialScan,
    DropboxIngest,
    FullRescan,
    SubtreeRescan,
    ProviderRepair,
    QuarantineRetry,
}

impl ImportJobKind {
    /// Verifies that api name.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&'static str` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn api_name(self) -> &'static str {
        match self {
            ImportJobKind::InitialScan => "initial_scan",
            ImportJobKind::DropboxIngest => "dropbox_ingest",
            ImportJobKind::FullRescan => "full_rescan",
            ImportJobKind::SubtreeRescan => "subtree_rescan",
            ImportJobKind::ProviderRepair => "provider_repair",
            ImportJobKind::QuarantineRetry => "quarantine_retry",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents import job status in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Queued`, `Running`, `Completed`, `Failed`, `Quarantined`, `Retrying` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/domain.rs`, `src/pipeline.rs`, `src/storage.rs`, and 1 more.
pub enum ImportJobStatus {
    Queued,
    Running,
    Completed,
    Failed,
    Quarantined,
    Retrying,
}

impl ImportJobStatus {
    /// Handles is active for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn is_active(self) -> bool {
        matches!(
            self,
            ImportJobStatus::Queued | ImportJobStatus::Running | ImportJobStatus::Retrying
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Represents maintenance scope in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `FullLibrary`, `Path` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`, `src/domain.rs`, `src/pipeline.rs`, and 3 more.
pub enum MaintenanceScope {
    FullLibrary,
    Path { path: String },
}

impl MaintenanceScope {
    /// Handles idempotency fragment for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `String` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn idempotency_fragment(&self) -> String {
        match self {
            MaintenanceScope::FullLibrary => "full_library".to_string(),
            MaintenanceScope::Path { path } => format!("path:{path}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
/// Represents repair plan in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `refresh_provider_metadata`, `refresh_artwork`, `rewrite_sidecars`, `rebuild_search_projections`, `preserve_provenance_history`, `preserve_confidence_history` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `bool`, `bool`, `bool`, `bool`, `bool`, `bool` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`, `src/domain.rs`, `src/pipeline.rs`, and 3 more.
pub struct RepairPlan {
    pub refresh_provider_metadata: bool,
    pub refresh_artwork: bool,
    pub rewrite_sidecars: bool,
    pub rebuild_search_projections: bool,
    pub preserve_provenance_history: bool,
    pub preserve_confidence_history: bool,
}

impl Default for RepairPlan {
    /// Builds the default configuration for shared domain model used by storage, state, API, pipeline, and provider layers.
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
            refresh_provider_metadata: true,
            refresh_artwork: true,
            rewrite_sidecars: true,
            rebuild_search_projections: true,
            preserve_provenance_history: true,
            preserve_confidence_history: true,
        }
    }
}

impl RepairPlan {
    /// Handles idempotency fragment for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `String` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn idempotency_fragment(&self) -> String {
        format!(
            "provider:{}|artwork:{}|sidecars:{}|search:{}|provenance:{}|confidence:{}",
            self.refresh_provider_metadata,
            self.refresh_artwork,
            self.rewrite_sidecars,
            self.rebuild_search_projections,
            self.preserve_provenance_history,
            self.preserve_confidence_history
        )
    }

    /// Handles can reuse existing without refresh for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn can_reuse_existing_without_refresh(&self) -> bool {
        !self.refresh_provider_metadata
            && !self.refresh_artwork
            && !self.rewrite_sidecars
            && !self.rebuild_search_projections
            && self.preserve_provenance_history
            && self.preserve_confidence_history
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents catalog mutation policy in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `PreserveVisibleUntilStableGrouping` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/domain.rs`, `src/pipeline.rs`, `src/storage.rs`.
pub enum CatalogMutationPolicy {
    PreserveVisibleUntilStableGrouping,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents import job in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `kind`, `status`, `scope`, `repair_plan`, `catalog_mutation_policy`, `provider_filter`, `pipeline`, and 7 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `ImportJobKind`, `ImportJobStatus`, `MaintenanceScope`, `RepairPlan`, `CatalogMutationPolicy`, and 9 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`, `src/domain.rs`, `src/pipeline.rs`, and 2 more.
pub struct ImportJob {
    pub id: Uuid,
    pub kind: ImportJobKind,
    pub status: ImportJobStatus,
    pub scope: MaintenanceScope,
    pub repair_plan: RepairPlan,
    pub catalog_mutation_policy: CatalogMutationPolicy,
    pub provider_filter: Vec<ProviderKind>,
    pub pipeline: String,
    pub source: ImportJobSource,
    pub reason: Option<String>,
    pub related_quarantine_item_id: Option<Uuid>,
    pub idempotency_key: String,
    pub attempts: u32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents import job source in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `StartupInitialScan`, `DropboxWatcher`, `AdminInitialScan`, `AdminDropboxIngest`, `AdminFullRescan`, `AdminSubtreeRescan`, `AdminProviderRepair`, `QuarantineRetry` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`, `src/domain.rs`, `src/pipeline.rs`, and 3 more.
pub enum ImportJobSource {
    StartupInitialScan,
    DropboxWatcher,
    AdminInitialScan,
    AdminDropboxIngest,
    AdminFullRescan,
    AdminSubtreeRescan,
    AdminProviderRepair,
    QuarantineRetry,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents media kind in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Music`, `Podcast` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 1 more.
pub enum MediaKind {
    Music,
    Podcast,
}

impl MediaKind {
    /// Verifies that api name.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&'static str` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn api_name(self) -> &'static str {
        match self {
            MediaKind::Music => "music",
            MediaKind::Podcast => "podcast",
        }
    }
}

impl fmt::Display for MediaKind {
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
    /// - Returns `fmt::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.api_name())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents album kind in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Album`, `Compilation`, `Single`, `Unknown` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 2 more.
pub enum AlbumKind {
    Album,
    Compilation,
    Single,
    Unknown,
}

impl AlbumKind {
    /// Verifies that api name.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&'static str` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn api_name(self) -> &'static str {
        match self {
            AlbumKind::Album => "album",
            AlbumKind::Compilation => "compilation",
            AlbumKind::Single => "single",
            AlbumKind::Unknown => "unknown",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents media file status in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Staged`, `Published`, `Duplicate`, `Quarantined`, `Failed` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 1 more.
pub enum MediaFileStatus {
    Staged,
    Published,
    Duplicate,
    Quarantined,
    Failed,
}

impl MediaFileStatus {
    /// Verifies that api name.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&'static str` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn api_name(self) -> &'static str {
        match self {
            MediaFileStatus::Staged => "staged",
            MediaFileStatus::Published => "published",
            MediaFileStatus::Duplicate => "duplicate",
            MediaFileStatus::Quarantined => "quarantined",
            MediaFileStatus::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents catalog entity type in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Artist`, `Album`, `Track`, `Podcast`, `Episode`, `MediaFile`, `Playlist` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 2 more.
pub enum CatalogEntityType {
    Artist,
    Album,
    Track,
    Podcast,
    Episode,
    MediaFile,
    Playlist,
}

impl CatalogEntityType {
    /// Verifies that api name.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&'static str` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn api_name(self) -> &'static str {
        match self {
            CatalogEntityType::Artist => "artist",
            CatalogEntityType::Album => "album",
            CatalogEntityType::Track => "track",
            CatalogEntityType::Podcast => "podcast",
            CatalogEntityType::Episode => "episode",
            CatalogEntityType::MediaFile => "media_file",
            CatalogEntityType::Playlist => "playlist",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents artwork kind in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Cover`, `Artist`, `Fanart`, `Thumbnail`, `Other` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 1 more.
pub enum ArtworkKind {
    Cover,
    Artist,
    Fanart,
    Thumbnail,
    Other,
}

impl ArtworkKind {
    /// Verifies that api name.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&'static str` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn api_name(self) -> &'static str {
        match self {
            ArtworkKind::Cover => "cover",
            ArtworkKind::Artist => "artist",
            ArtworkKind::Fanart => "fanart",
            ArtworkKind::Thumbnail => "thumbnail",
            ArtworkKind::Other => "other",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents metadata match kind in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `ExactIdentifier`, `HighConfidence`, `ModerateConfidence`, `LocalOnly` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 1 more.
pub enum MetadataMatchKind {
    ExactIdentifier,
    HighConfidence,
    ModerateConfidence,
    LocalOnly,
}

impl MetadataMatchKind {
    /// Verifies that api name.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&'static str` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn api_name(self) -> &'static str {
        match self {
            MetadataMatchKind::ExactIdentifier => "exact_identifier",
            MetadataMatchKind::HighConfidence => "high_confidence",
            MetadataMatchKind::ModerateConfidence => "moderate_confidence",
            MetadataMatchKind::LocalOnly => "local_only",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents artist in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `name`, `normalized_name`, `sort_name`, `stable_grouping`, `published_at`, `created_at`, `updated_at` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `String`, `String`, `Option<String>`, `bool`, `Option<DateTime<Utc>>`, and 2 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, and 4 more.
pub struct Artist {
    pub id: Uuid,
    pub name: String,
    pub normalized_name: String,
    pub sort_name: Option<String>,
    pub stable_grouping: bool,
    pub published_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents album in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `artist_id`, `title`, `normalized_title`, `album_kind`, `release_year`, `stable_grouping`, `published_at`, and 2 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `Uuid`, `String`, `String`, `AlbumKind`, `Option<i32>`, and 4 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, and 4 more.
pub struct Album {
    pub id: Uuid,
    pub artist_id: Uuid,
    pub title: String,
    pub normalized_title: String,
    pub album_kind: AlbumKind,
    pub release_year: Option<i32>,
    pub stable_grouping: bool,
    pub published_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents track in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `album_id`, `artist_id`, `title`, `normalized_title`, `disc_number`, `track_number`, `duration_seconds`, and 4 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `Uuid`, `Uuid`, `String`, `String`, `Option<i32>`, and 6 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/media.rs`, `src/api/openapi.rs`, `src/catalog.rs`, and 6 more.
pub struct Track {
    pub id: Uuid,
    pub album_id: Uuid,
    pub artist_id: Uuid,
    pub title: String,
    pub normalized_title: String,
    pub disc_number: Option<i32>,
    pub track_number: Option<i32>,
    pub duration_seconds: Option<i32>,
    pub quality: Option<String>,
    pub stable_grouping: bool,
    pub published_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents podcast in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `title`, `normalized_title`, `stable_grouping`, `published_at`, `created_at`, `updated_at` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `String`, `String`, `bool`, `Option<DateTime<Utc>>`, `DateTime<Utc>`, and 1 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, and 4 more.
pub struct Podcast {
    pub id: Uuid,
    pub title: String,
    pub normalized_title: String,
    pub stable_grouping: bool,
    pub published_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents episode in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `podcast_id`, `title`, `normalized_title`, `season_number`, `episode_number`, `duration_seconds`, `stable_grouping`, and 3 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `Uuid`, `String`, `String`, `Option<i32>`, `Option<i32>`, and 5 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/catalog.rs`, `src/api/media.rs`, `src/api/openapi.rs`, `src/catalog.rs`, and 6 more.
pub struct Episode {
    pub id: Uuid,
    pub podcast_id: Uuid,
    pub title: String,
    pub normalized_title: String,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
    pub duration_seconds: Option<i32>,
    pub stable_grouping: bool,
    pub published_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents media file in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `media_kind`, `status`, `source_path`, `managed_path`, `file_hash`, `file_size`, `mime_type`, and 15 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `MediaKind`, `MediaFileStatus`, `String`, `Option<String>`, `String`, and 17 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/media.rs`, `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, and 3 more.
pub struct MediaFile {
    pub id: Uuid,
    pub media_kind: MediaKind,
    pub status: MediaFileStatus,
    pub source_path: String,
    pub managed_path: Option<String>,
    pub file_hash: String,
    pub file_size: i64,
    pub mime_type: Option<String>,
    pub container: Option<String>,
    pub audio_codec: Option<String>,
    pub duration_seconds: Option<i32>,
    pub bitrate: Option<i32>,
    pub sample_rate: Option<i32>,
    pub channels: Option<i32>,
    pub genres: Vec<String>,
    pub format_keys: Vec<String>,
    pub track_id: Option<Uuid>,
    pub episode_id: Option<Uuid>,
    pub duplicate_of_media_file_id: Option<Uuid>,
    pub import_job_id: Option<Uuid>,
    pub discovered_at: DateTime<Utc>,
    pub published_at: Option<DateTime<Utc>>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents artwork asset in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `entity_type`, `entity_id`, `provider`, `artwork_kind`, `source_uri`, `file_path`, `mime_type`, and 4 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `CatalogEntityType`, `Uuid`, `ProviderKind`, `ArtworkKind`, `Option<String>`, and 6 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`.
pub struct ArtworkAsset {
    pub id: Uuid,
    pub entity_type: CatalogEntityType,
    pub entity_id: Uuid,
    pub provider: ProviderKind,
    pub artwork_kind: ArtworkKind,
    pub source_uri: Option<String>,
    pub file_path: Option<String>,
    pub mime_type: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub confidence: f32,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents metadata provider link in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `entity_type`, `entity_id`, `provider`, `provider_item_id`, `external_url`, `match_kind`, `confidence`, and 4 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `CatalogEntityType`, `Uuid`, `ProviderKind`, `String`, `Option<String>`, and 6 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`.
pub struct MetadataProviderLink {
    pub id: Uuid,
    pub entity_type: CatalogEntityType,
    pub entity_id: Uuid,
    pub provider: ProviderKind,
    pub provider_item_id: String,
    pub external_url: Option<String>,
    pub match_kind: MetadataMatchKind,
    pub confidence: f32,
    pub auto_accepted: bool,
    pub raw_metadata: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents metadata provenance in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `entity_type`, `entity_id`, `field_name`, `provider`, `value`, `confidence`, `auto_accepted`, and 3 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `CatalogEntityType`, `Uuid`, `String`, `ProviderKind`, `serde_json::Value`, and 5 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`.
pub struct MetadataProvenance {
    pub id: Uuid,
    pub entity_type: CatalogEntityType,
    pub entity_id: Uuid,
    pub field_name: String,
    pub provider: ProviderKind,
    pub value: serde_json::Value,
    pub confidence: f32,
    pub auto_accepted: bool,
    pub import_job_id: Option<Uuid>,
    pub source_path: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents catalog search projection in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `entity_type`, `entity_id`, `display_title`, `search_text`, `normalized_text`, `normalized_display_title`, `published`, `updated_at` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `CatalogEntityType`, `Uuid`, `String`, `String`, `String`, `String`, and 2 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`.
pub struct CatalogSearchProjection {
    pub entity_type: CatalogEntityType,
    pub entity_id: Uuid,
    pub display_title: String,
    pub search_text: String,
    pub normalized_text: String,
    pub normalized_display_title: String,
    pub published: bool,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents music catalog grouping in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `album_artist`, `track_artist`, `album_title`, `track_title`, `album_kind`, `release_year`, `disc_number`, `track_number` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `String`, `String`, `String`, `String`, `AlbumKind`, `Option<i32>`, and 2 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 2 more.
pub struct MusicCatalogGrouping {
    pub album_artist: String,
    pub track_artist: String,
    pub album_title: String,
    pub track_title: String,
    pub album_kind: AlbumKind,
    pub release_year: Option<i32>,
    pub disc_number: Option<i32>,
    pub track_number: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents podcast catalog grouping in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `podcast_title`, `episode_title`, `season_number`, `episode_number` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `String`, `String`, `Option<i32>`, `Option<i32>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 2 more.
pub struct PodcastCatalogGrouping {
    pub podcast_title: String,
    pub episode_title: String,
    pub season_number: Option<i32>,
    pub episode_number: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Represents catalog grouping in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Music`, `Podcast` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 2 more.
pub enum CatalogGrouping {
    Music(MusicCatalogGrouping),
    Podcast(PodcastCatalogGrouping),
}

impl CatalogGrouping {
    /// Handles is stable for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn is_stable(&self) -> bool {
        match self {
            CatalogGrouping::Music(grouping) => {
                !grouping.album_artist.trim().is_empty()
                    && !grouping.track_artist.trim().is_empty()
                    && !grouping.album_title.trim().is_empty()
                    && !grouping.track_title.trim().is_empty()
            }
            CatalogGrouping::Podcast(grouping) => {
                !grouping.podcast_title.trim().is_empty()
                    && !grouping.episode_title.trim().is_empty()
            }
        }
    }

    /// Handles media kind for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `MediaKind` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn media_kind(&self) -> MediaKind {
        match self {
            CatalogGrouping::Music(_) => MediaKind::Music,
            CatalogGrouping::Podcast(_) => MediaKind::Podcast,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents media probe facts in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `file_hash`, `file_size`, `mime_type`, `container`, `audio_codec`, `duration_seconds`, `bitrate`, `sample_rate`, and 1 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `String`, `i64`, `Option<String>`, `Option<String>`, `Option<String>`, `Option<i32>`, and 3 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/media.rs`, and 3 more.
pub struct MediaProbeFacts {
    pub file_hash: String,
    pub file_size: i64,
    pub mime_type: Option<String>,
    pub container: Option<String>,
    pub audio_codec: Option<String>,
    pub duration_seconds: Option<i32>,
    pub bitrate: Option<i32>,
    pub sample_rate: Option<i32>,
    pub channels: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents metadata provider link draft in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `entity_type`, `provider`, `provider_item_id`, `external_url`, `match_kind`, `confidence`, `auto_accepted`, `raw_metadata` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `CatalogEntityType`, `ProviderKind`, `String`, `Option<String>`, `MetadataMatchKind`, `f32`, and 2 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 1 more.
pub struct MetadataProviderLinkDraft {
    pub entity_type: CatalogEntityType,
    pub provider: ProviderKind,
    pub provider_item_id: String,
    pub external_url: Option<String>,
    pub match_kind: MetadataMatchKind,
    pub confidence: f32,
    pub auto_accepted: bool,
    pub raw_metadata: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents metadata provenance draft in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `entity_type`, `field_name`, `provider`, `value`, `confidence`, `auto_accepted` for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `CatalogEntityType`, `String`, `ProviderKind`, `serde_json::Value`, `f32`, `bool` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 2 more.
pub struct MetadataProvenanceDraft {
    pub entity_type: CatalogEntityType,
    pub field_name: String,
    pub provider: ProviderKind,
    pub value: serde_json::Value,
    pub confidence: f32,
    pub auto_accepted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents artwork asset draft in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `entity_type`, `provider`, `artwork_kind`, `source_uri`, `file_path`, `mime_type`, `width`, `height`, and 1 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `CatalogEntityType`, `ProviderKind`, `ArtworkKind`, `Option<String>`, `Option<String>`, `Option<String>`, and 3 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 1 more.
pub struct ArtworkAssetDraft {
    pub entity_type: CatalogEntityType,
    pub provider: ProviderKind,
    pub artwork_kind: ArtworkKind,
    pub source_uri: Option<String>,
    pub file_path: Option<String>,
    pub mime_type: Option<String>,
    pub width: Option<i32>,
    pub height: Option<i32>,
    pub confidence: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents catalog import request in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `source_path`, `managed_path`, `grouping`, `probe`, `import_job_id`, `provider_links`, `provenance`, `artwork`, and 5 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `String`, `Option<String>`, `CatalogGrouping`, `MediaProbeFacts`, `Option<Uuid>`, `Vec<MetadataProviderLinkDraft>`, and 7 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 1 more.
pub struct CatalogImportRequest {
    pub source_path: String,
    pub managed_path: Option<String>,
    pub grouping: CatalogGrouping,
    pub probe: MediaProbeFacts,
    pub import_job_id: Option<Uuid>,
    pub provider_links: Vec<MetadataProviderLinkDraft>,
    pub provenance: Vec<MetadataProvenanceDraft>,
    pub artwork: Vec<ArtworkAssetDraft>,
    pub allow_reuse_existing: bool,
    pub refresh_artwork: bool,
    pub rebuild_search_projections: bool,
    pub preserve_provenance_history: bool,
    pub preserve_confidence_history: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents catalog import decision in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Published`, `ReusedExisting`, `QuarantinedUnstableGrouping`, `QuarantinedDuplicate`, `QuarantinedFileError` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/pipeline.rs`, and 1 more.
pub enum CatalogImportDecision {
    Published,
    ReusedExisting,
    QuarantinedUnstableGrouping,
    QuarantinedDuplicate,
    QuarantinedFileError,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents catalog import outcome in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `decision`, `media_file`, `artist`, `album`, `track`, `podcast`, `episode`, `duplicate_of`, and 1 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `CatalogImportDecision`, `MediaFile`, `Option<Artist>`, `Option<Album>`, `Option<Track>`, `Option<Podcast>`, and 3 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`.
pub struct CatalogImportOutcome {
    pub decision: CatalogImportDecision,
    pub media_file: MediaFile,
    pub artist: Option<Artist>,
    pub album: Option<Album>,
    pub track: Option<Track>,
    pub podcast: Option<Podcast>,
    pub episode: Option<Episode>,
    pub duplicate_of: Option<MediaFile>,
    pub quarantine_item: Option<QuarantineItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents quarantine reason in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Duplicate`, `MetadataFailure`, `FileError`, `UnsupportedFormat`, `ConflictingMetadata` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/storage.rs`.
pub enum QuarantineReason {
    Duplicate,
    MetadataFailure,
    FileError,
    UnsupportedFormat,
    ConflictingMetadata,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
/// Represents quarantine status in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Enumerates `Open`, `Retrying`, `Resolved`, `Deleted` states or choices for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/state.rs`, and 2 more.
pub enum QuarantineStatus {
    Open,
    Retrying,
    Resolved,
    Deleted,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents quarantine item in the shared domain model used by storage, state, API, pipeline, and provider layers.
///
/// Functionality: Carries fields `id`, `media_file_id`, `source_path`, `reason`, `status`, `retry_count`, `retry_eligible`, `last_import_job_id`, and 3 more for shared domain model used by storage, state, API, pipeline, and provider layers.
/// Dependencies: depends on `Uuid`, `Option<Uuid>`, `String`, `QuarantineReason`, `QuarantineStatus`, `u32`, and 5 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`, `src/catalog.rs`, `src/domain.rs`, `src/state.rs`, and 2 more.
pub struct QuarantineItem {
    pub id: Uuid,
    pub media_file_id: Option<Uuid>,
    pub source_path: String,
    pub reason: QuarantineReason,
    pub status: QuarantineStatus,
    pub retry_count: u32,
    pub retry_eligible: bool,
    pub last_import_job_id: Option<Uuid>,
    pub admin_note: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl QuarantineItem {
    /// Handles metadata failure for shared domain model used by storage, state, API, pipeline, and provider layers.
    ///
    /// Inputs:
    /// - `source_path`: `impl Into<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn metadata_failure(source_path: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            media_file_id: None,
            source_path: source_path.into(),
            reason: QuarantineReason::MetadataFailure,
            status: QuarantineStatus::Open,
            retry_count: 0,
            retry_eligible: true,
            last_import_job_id: None,
            admin_note: None,
            created_at: now,
            updated_at: now,
        }
    }
}
