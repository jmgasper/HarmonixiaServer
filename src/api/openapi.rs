use utoipa::{
    openapi::security::{Http, HttpAuthScheme, SecurityScheme},
    Modify, OpenApi,
};

use crate::{
    api::accounts::{
        BootstrapStatusResponse, CreateFirstAdminRequest, CreateUserRequest,
        ResetPasswordRequest, UsersResponse,
    },
    api::artwork::{
        ArtworkAssetResponse, ArtworkAssetsResponse, ArtworkImageQuery, ArtworkLookupQuery,
    },
    api::catalog::{
        BrowseAlbumsResponse, BrowseArtistsResponse, BrowseEpisodesResponse,
        BrowsePodcastsResponse, BrowseTracksResponse, CatalogBrowsePageMetadata,
        CatalogBrowseQuery, CatalogSearchQuery, CatalogSearchResponse,
        EpisodeResponse, EpisodeResumeResponse, PodcastResponse,
    },
    api::config::{
        ProviderSettingUpdateRequest, ProviderSettingsResponse, SystemConfigUpdateRequest,
    },
    api::maintenance::{
        DashboardActiveImportJobResponse, DashboardSummaryResponse, DropboxIngestRequest,
        FullRescanRequest, ImportFailureResponse, ImportFailuresQuery,
        ImportFailuresResponse, InitialScanRequest, MaintenanceOperationResponse,
        MaintenanceOptionsRequest, MaintenanceReadinessResponse, ProviderHealthResponse,
        ProviderRefreshRequest, QuarantineRetryRequest, QuarantineRetryResponse,
        SubtreeRescanRequest,
    },
    api::playback::{
        PlaybackHistoryQuery, PlaybackHistoryResponse, PlaybackHistoryWriteRequest,
        PlaybackProgressResponse, PlaybackProgressWriteRequest, PlaybackProgressWriteResponse,
    },
    api::playlists::{
        AddPlaylistItemRequest, CreatePlaylistRequest, PlaylistItemsResponse,
        PlaylistsResponse, ReorderPlaylistItemsRequest, UpdatePlaylistRequest,
    },
    domain::{
        AacTranscodeProfile, AccountRole, Album, AlbumKind, Artist, ArtworkAsset, ArtworkAssetDraft,
        ArtworkKind, AuthenticatedAccount, CatalogEntityType, CatalogGrouping,
        CatalogImportDecision, CatalogImportOutcome, CatalogImportRequest,
        CatalogMutationPolicy, CatalogSearchProjection, Episode, ImportJob,
        ImportJobKind, ImportJobSource, ImportJobStatus, MaintenanceScope, MediaFile,
        MediaFileStatus, MediaKind, MediaProbeFacts, MetadataMatchKind,
        MetadataProviderLink, MetadataProviderLinkDraft, MetadataProvenance,
        MetadataProvenanceDraft, MusicCatalogGrouping, PlaybackHistoryEvent,
        PlaybackItemType, PlaybackProgress, Playlist, PlaylistItem, PlaylistScope, Podcast,
        PodcastCatalogGrouping, ProviderHealth, ProviderKind, ProviderSetting,
        ProviderStatus, QuarantineItem, QuarantineReason, QuarantineStatus, RepairPlan,
        SystemConfig, Track, TranscodeSlotUsage, UserAccount,
    },
    error::ErrorResponse,
};

#[derive(OpenApi)]
#[openapi(
    paths(
        crate::api::accounts::bootstrap_status,
        crate::api::accounts::create_first_admin,
        crate::api::accounts::auth_me,
        crate::api::accounts::list_users,
        crate::api::accounts::create_user,
        crate::api::accounts::reset_user_password,
        crate::api::accounts::delete_user,
        crate::api::config::get_system_config,
        crate::api::config::update_system_config,
        crate::api::config::list_provider_settings,
        crate::api::config::update_provider_setting,
        crate::api::maintenance::trigger_initial_scan,
        crate::api::maintenance::trigger_dropbox_ingest,
        crate::api::maintenance::trigger_full_rescan,
        crate::api::maintenance::trigger_subtree_rescan,
        crate::api::maintenance::trigger_provider_refresh,
        crate::api::maintenance::dashboard_summary,
        crate::api::maintenance::list_import_failures,
        crate::api::maintenance::provider_repair,
        crate::api::maintenance::list_provider_health,
        crate::api::maintenance::maintenance_readiness,
        crate::api::maintenance::retry_quarantine_items,
        crate::api::maintenance::retry_quarantine_item,
        crate::api::catalog::search_catalog,
        crate::api::catalog::browse_artists,
        crate::api::catalog::browse_albums,
        crate::api::catalog::browse_tracks,
        crate::api::catalog::browse_podcasts,
        crate::api::catalog::get_podcast,
        crate::api::catalog::browse_podcast_episodes,
        crate::api::catalog::browse_episodes,
        crate::api::catalog::get_episode,
        crate::api::catalog::get_episode_resume,
        crate::api::catalog::write_episode_resume,
        crate::api::artwork::get_catalog_entity_artwork,
        crate::api::artwork::get_artwork_image,
        crate::api::media::stream_original,
        crate::api::media::download_original,
        crate::api::media::stream_direct_transcode,
        crate::api::media::hls_manifest,
        crate::api::media::hls_segment,
        crate::api::media::transcode_slot_usage,
        crate::api::playlists::list_playlists,
        crate::api::playlists::create_playlist,
        crate::api::playlists::get_playlist,
        crate::api::playlists::update_playlist,
        crate::api::playlists::delete_playlist,
        crate::api::playlists::list_playlist_items,
        crate::api::playlists::add_playlist_item,
        crate::api::playlists::reorder_playlist_items,
        crate::api::playlists::remove_playlist_item,
        crate::api::playback::write_progress,
        crate::api::playback::list_progress,
        crate::api::playback::get_progress,
        crate::api::playback::write_history,
        crate::api::playback::list_history,
    ),
    components(
        schemas(
            AacTranscodeProfile,
            ErrorResponse,
            AccountRole,
            AddPlaylistItemRequest,
            Album,
            AlbumKind,
            Artist,
            ArtworkAsset,
            ArtworkAssetDraft,
            ArtworkAssetResponse,
            ArtworkAssetsResponse,
            ArtworkImageQuery,
            ArtworkLookupQuery,
            ArtworkKind,
            AuthenticatedAccount,
            BootstrapStatusResponse,
            BrowseAlbumsResponse,
            BrowseArtistsResponse,
            BrowseEpisodesResponse,
            BrowsePodcastsResponse,
            BrowseTracksResponse,
            CatalogBrowsePageMetadata,
            CatalogBrowseQuery,
            CatalogEntityType,
            CatalogGrouping,
            CatalogImportDecision,
            CatalogImportOutcome,
            CatalogImportRequest,
            CatalogMutationPolicy,
            CatalogSearchQuery,
            CatalogSearchProjection,
            CatalogSearchResponse,
            CreateFirstAdminRequest,
            CreatePlaylistRequest,
            CreateUserRequest,
            DashboardActiveImportJobResponse,
            DashboardSummaryResponse,
            DropboxIngestRequest,
            Episode,
            EpisodeResponse,
            EpisodeResumeResponse,
            FullRescanRequest,
            ImportFailureResponse,
            ImportFailuresQuery,
            ImportFailuresResponse,
            InitialScanRequest,
            ImportJob,
            ImportJobKind,
            ImportJobSource,
            ImportJobStatus,
            MaintenanceOperationResponse,
            MaintenanceOptionsRequest,
            MaintenanceReadinessResponse,
            MaintenanceScope,
            MediaFile,
            MediaFileStatus,
            MediaKind,
            MediaProbeFacts,
            MetadataMatchKind,
            MetadataProviderLink,
            MetadataProviderLinkDraft,
            MetadataProvenance,
            MetadataProvenanceDraft,
            MusicCatalogGrouping,
            PlaybackHistoryEvent,
            PlaybackHistoryQuery,
            PlaybackHistoryResponse,
            PlaybackHistoryWriteRequest,
            PlaybackItemType,
            PlaybackProgress,
            PlaybackProgressResponse,
            PlaybackProgressWriteRequest,
            PlaybackProgressWriteResponse,
            Playlist,
            PlaylistItem,
            PlaylistItemsResponse,
            PlaylistScope,
            PlaylistsResponse,
            Podcast,
            PodcastResponse,
            PodcastCatalogGrouping,
            ProviderHealth,
            ProviderHealthResponse,
            ProviderKind,
            ProviderSetting,
            ProviderSettingUpdateRequest,
            ProviderSettingsResponse,
            ProviderRefreshRequest,
            ProviderStatus,
            QuarantineItem,
            QuarantineReason,
            QuarantineRetryRequest,
            QuarantineRetryResponse,
            QuarantineStatus,
            ReorderPlaylistItemsRequest,
            ResetPasswordRequest,
            RepairPlan,
            SubtreeRescanRequest,
            SystemConfig,
            Track,
            TranscodeSlotUsage,
            SystemConfigUpdateRequest,
            UpdatePlaylistRequest,
            UserAccount,
            UsersResponse
        )
    ),
    tags(
        (name = "bootstrap", description = "First-run local admin bootstrap"),
        (name = "auth", description = "Authenticated account inspection"),
        (name = "catalog", description = "Published catalog browse and grouped search APIs for external clients"),
        (name = "artwork", description = "Authenticated catalog artwork image delivery"),
        (name = "users", description = "Admin local account management"),
        (name = "maintenance", description = "Admin metadata maintenance and import repair operations"),
        (name = "media", description = "Authenticated original media streaming, downloads, direct AAC transcodes, and HLS delivery"),
        (name = "playback", description = "User-scoped playback progress and history"),
        (name = "playlists", description = "Personal and household-shared playlists with ordered track/episode membership"),
        (name = "providers", description = "Metadata provider health and repair operations"),
        (name = "quarantine", description = "Quarantine retry handoff into the import pipeline"),
        (name = "settings", description = "Admin system and provider configuration")
    ),
    modifiers(&SecurityAddon)
)]
/// Represents api doc in the OpenAPI document assembly and security metadata.
///
/// Functionality: Acts as a marker or zero-field value for OpenAPI document assembly and security metadata.
/// Dependencies: has no direct field dependencies beyond derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/mod.rs`, `src/api/openapi.rs`.
pub struct ApiDoc;

/// Represents security addon in the OpenAPI document assembly and security metadata.
///
/// Functionality: Acts as a marker or zero-field value for OpenAPI document assembly and security metadata.
/// Dependencies: has no direct field dependencies beyond derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/api/openapi.rs`.
struct SecurityAddon;

impl Modify for SecurityAddon {
    /// Handles modify for OpenAPI documentation.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `openapi`: `&mut utoipa:openapi:OpenApi`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "basicAuth",
                SecurityScheme::Http(Http::new(HttpAuthScheme::Basic)),
            );
        }
    }
}
