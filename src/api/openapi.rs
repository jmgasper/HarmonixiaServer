use utoipa::{
    openapi::{
        schema::{AnyOf, Object, Ref, Schema, SchemaType, Type},
        security::{Http, HttpAuthScheme, SecurityScheme},
        RefOr,
    },
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
        AlbumDetailHeader, AlbumDetailResponse, AlbumDetailSummary, AlbumTrackGroup,
        ArtistAlbumGroup, ArtistDetailHeader, ArtistDetailLink, ArtistDetailResponse,
        ArtistDetailSummary, ArtistTrackGroup, BrowseAlbumsResponse, BrowseArtistsResponse,
        BrowseEpisodesResponse, BrowsePodcastsResponse, BrowseTracksResponse,
        CatalogBrowsePageMetadata, CatalogBrowseQuery, CatalogSearchQuery,
        CatalogSearchResponse, DetailTrackItem, EpisodeResponse, EpisodeResumeResponse,
        PodcastResponse,
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
    api::home::{
        HomeCard, HomeCardItemType, HomeResponse, HomeSection, HomeSectionId,
        PlaybackPositionHint, ScreenActionHint, ScreenArtwork, ScreenContextHint,
    },
    api::playback::{
        PlaybackHistoryQuery, PlaybackHistoryResponse, PlaybackHistoryWriteRequest,
        PlaybackProgressResponse, PlaybackProgressWriteRequest, PlaybackProgressWriteResponse,
    },
    api::playlists::{
        AddPlaylistItemRequest, CreatePlaylistRequest, PlaylistItemsResponse,
        PlaylistsResponse, ReorderPlaylistItemsRequest, UpdatePlaylistRequest,
    },
    api::sonos::{
        SonosGroupTarget, SonosNextItemSummary, SonosPlaybackResponse, SonosPlaybackTarget,
        SonosPlayRequest, SonosPlaySourceType, SonosSeekRequest, SonosSessionSummary,
        SonosSpeakerTarget, SonosTargetsResponse,
    },
    api::sync::{
        AlbumSyncSnapshot, DownloadVariantEntry, PlaylistSyncSnapshot,
        SyncPlaylistItemEntry, SyncTrackEntry,
    },
    domain::{
        AacTranscodeProfile, AccountRole, Album, AlbumKind, Artist, ArtworkAsset, ArtworkAssetDraft,
        ArtworkKind, AuthenticatedAccount, CatalogEntityType, CatalogGrouping,
        CatalogImportDecision, CatalogImportOutcome, CatalogImportRequest,
        CatalogMutationPolicy, CatalogSearchProjection, Episode, ImportJob,
        ImportJobKind, ImportJobSource, ImportJobStatus, MaintenanceScope, MediaFile,
        MediaFileStatus, MediaKind, MediaProbeFacts, MetadataMatchKind,
        MetadataProviderLink, MetadataProviderLinkDraft, MetadataProvenance,
        MetadataProvenanceDraft, MusicCatalogGrouping, PlaybackContextType,
        PlaybackHistoryEvent, PlaybackItemType, PlaybackProgress, Playlist, PlaylistItem,
        PlaylistScope, Podcast, PodcastCatalogGrouping, ProviderHealth, ProviderKind, ProviderSetting,
        ProviderStatus, QuarantineItem, QuarantineReason, QuarantineStatus, RepairPlan,
        SonosDeliveryKind, SonosSessionStatus, SonosSignedClaim, SonosTransportState,
        SystemConfig, Track, TranscodeSlotUsage, UserAccount,
    },
    error::{ErrorResponse, ErrorResponseDetails, SonosErrorReason},
    state::{
        AppEvent, AppEventAudience, HomeSectionsPatch, PlaybackHistoryScreenPatch,
        PlaybackProgressScreenPatch, PlaylistDetailRemovePatch, PlaylistDetailReplacePatch,
        PlaylistListRemovePatch, PlaylistListUpsertPatch, RecoveryScreenPatch, ScreenPatch,
        ScreenSurface,
    },
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
        crate::api::catalog::get_artist_detail,
        crate::api::catalog::browse_albums,
        crate::api::catalog::get_album_detail,
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
        crate::api::media::download_transcode,
        crate::api::media::hls_manifest,
        crate::api::media::hls_segment,
        crate::api::media::transcode_slot_usage,
        crate::api::sync::album_sync_snapshot,
        crate::api::sync::playlist_sync_snapshot,
        crate::api::home::get_home,
        crate::api::events::stream_events,
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
        crate::api::sonos::list_targets,
        crate::api::sonos::play_target,
        crate::api::sonos::pause_target,
        crate::api::sonos::resume_target,
        crate::api::sonos::stop_target,
        crate::api::sonos::seek_target,
        crate::api::sonos::next_target,
        crate::api::sonos::previous_target,
        crate::api::sonos::fetch_signed_media,
    ),
    components(
        schemas(
            AacTranscodeProfile,
            ErrorResponse,
            ErrorResponseDetails,
            AccountRole,
            AddPlaylistItemRequest,
            Album,
            AlbumDetailHeader,
            AlbumDetailResponse,
            AlbumDetailSummary,
            AlbumSyncSnapshot,
            AlbumKind,
            AlbumTrackGroup,
            Artist,
            ArtistAlbumGroup,
            ArtistDetailHeader,
            ArtistDetailLink,
            ArtistDetailResponse,
            ArtistDetailSummary,
            ArtistTrackGroup,
            AppEvent,
            AppEventAudience,
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
            DetailTrackItem,
            DropboxIngestRequest,
            DownloadVariantEntry,
            Episode,
            EpisodeResponse,
            EpisodeResumeResponse,
            FullRescanRequest,
            HomeCard,
            HomeCardItemType,
            HomeResponse,
            HomeSection,
            HomeSectionId,
            HomeSectionsPatch,
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
            PlaybackContextType,
            PlaybackHistoryEvent,
            PlaybackHistoryScreenPatch,
            PlaybackHistoryQuery,
            PlaybackHistoryResponse,
            PlaybackHistoryWriteRequest,
            PlaybackItemType,
            PlaybackPositionHint,
            PlaybackProgress,
            PlaybackProgressScreenPatch,
            PlaybackProgressResponse,
            PlaybackProgressWriteRequest,
            PlaybackProgressWriteResponse,
            Playlist,
            PlaylistItem,
            PlaylistItemsResponse,
            PlaylistScope,
            PlaylistSyncSnapshot,
            PlaylistDetailRemovePatch,
            PlaylistDetailReplacePatch,
            PlaylistListRemovePatch,
            PlaylistListUpsertPatch,
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
            RecoveryScreenPatch,
            SonosDeliveryKind,
            SonosErrorReason,
            SonosGroupTarget,
            SonosNextItemSummary,
            SonosPlaybackResponse,
            SonosPlaybackTarget,
            SonosPlayRequest,
            SonosPlaySourceType,
            SonosSeekRequest,
            SonosSessionStatus,
            SonosSessionSummary,
            SonosSignedClaim,
            SonosSpeakerTarget,
            SonosTargetsResponse,
            SonosTransportState,
            SubtreeRescanRequest,
            SystemConfig,
            Track,
            ScreenActionHint,
            ScreenArtwork,
            ScreenContextHint,
            ScreenPatch,
            ScreenSurface,
            SyncPlaylistItemEntry,
            SyncTrackEntry,
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
        (name = "home", description = "Account-scoped Home screen read model with fixed sections and latest podcast episode cards"),
        (name = "events", description = "Account-scoped Server-Sent Events with typed surface patches"),
        (name = "artwork", description = "Authenticated catalog artwork image delivery"),
        (name = "users", description = "Admin local account management"),
        (name = "maintenance", description = "Admin metadata maintenance and import repair operations"),
        (name = "media", description = "Authenticated original media streaming, downloads, direct AAC transcodes, downloadable transcode variants, and HLS delivery"),
        (name = "playback", description = "User-scoped playback progress and history"),
        (name = "playlists", description = "Personal and household-shared playlists with ordered track/episode membership"),
        (name = "providers", description = "Metadata provider health and repair operations"),
        (name = "quarantine", description = "Quarantine retry handoff into the import pipeline"),
        (name = "sonos", description = "Live Sonos discovery, signed-media delivery, managed playback, and target control"),
        (name = "sync", description = "Album and playlist offline-sync snapshot metadata"),
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
        apply_contract_schema_overrides(openapi);
    }
}

fn apply_contract_schema_overrides(openapi: &mut utoipa::openapi::OpenApi) {
    let Some(components) = openapi.components.as_mut() else {
        return;
    };
    let schemas = &mut components.schemas;

    for (schema_name, field_name) in [
        ("SystemConfig", "public_base_url"),
        ("SonosSpeakerTarget", "room_name"),
        ("SonosSpeakerTarget", "volume_percent"),
        ("SonosSpeakerTarget", "muted"),
        ("SonosSpeakerTarget", "transport_state"),
        ("SonosGroupTarget", "volume_percent"),
        ("SonosGroupTarget", "muted"),
        ("SonosGroupTarget", "transport_state"),
        ("SonosPlaybackResponse", "session"),
        ("SonosSessionSummary", "current_duration_seconds"),
        ("SonosSessionSummary", "next_item"),
    ] {
        require_nullable_property(schemas, schema_name, field_name);
    }
    sonos_playback_response_target_schema(schemas);
    nullable_property(schemas, "SystemConfigUpdateRequest", "public_base_url");
    for (schema_name, field_name) in [
        ("ErrorResponse", "details"),
        ("ErrorResponseDetails", "reason"),
        ("SonosSessionSummary", "reconnect_seconds_remaining"),
    ] {
        non_nullable_property(schemas, schema_name, field_name);
    }
}

fn sonos_playback_response_target_schema(
    schemas: &mut std::collections::BTreeMap<String, RefOr<Schema>>,
) {
    let Some(schema) = schema_object_mut(schemas, "SonosPlaybackResponse") else {
        return;
    };
    if !schema.required.iter().any(|field| field == "target") {
        schema.required.push("target".to_string());
    }
    schema.properties.insert(
        "target".to_string(),
        RefOr::T(Schema::AnyOf(AnyOf {
            items: vec![
                RefOr::Ref(Ref::from_schema_name("SonosSpeakerTarget")),
                RefOr::Ref(Ref::from_schema_name("SonosGroupTarget")),
            ],
            ..Default::default()
        })),
    );
}

fn require_nullable_property(
    schemas: &mut std::collections::BTreeMap<String, RefOr<Schema>>,
    schema_name: &str,
    field_name: &str,
) {
    if let Some(schema) = schema_object_mut(schemas, schema_name) {
        if !schema.required.iter().any(|field| field == field_name) {
            schema.required.push(field_name.to_string());
        }
    }
    nullable_property(schemas, schema_name, field_name);
}

fn nullable_property(
    schemas: &mut std::collections::BTreeMap<String, RefOr<Schema>>,
    schema_name: &str,
    field_name: &str,
) {
    if let Some(property_schema) = schema_object_mut(schemas, schema_name)
        .and_then(|schema| schema.properties.get_mut(field_name))
    {
        make_nullable(property_schema);
    }
}

fn non_nullable_property(
    schemas: &mut std::collections::BTreeMap<String, RefOr<Schema>>,
    schema_name: &str,
    field_name: &str,
) {
    if let Some(property_schema) = schema_object_mut(schemas, schema_name)
        .and_then(|schema| schema.properties.get_mut(field_name))
    {
        make_non_nullable(property_schema);
    }
}

fn schema_object_mut<'a>(
    schemas: &'a mut std::collections::BTreeMap<String, RefOr<Schema>>,
    schema_name: &str,
) -> Option<&'a mut Object> {
    match schemas.get_mut(schema_name) {
        Some(RefOr::T(Schema::Object(schema))) => Some(schema),
        _ => None,
    }
}

fn make_nullable(schema: &mut RefOr<Schema>) {
    match schema {
        RefOr::Ref(_) => {
            let original = schema.clone();
            *schema = RefOr::T(Schema::AnyOf(AnyOf {
                items: vec![original, null_schema()],
                ..Default::default()
            }));
        }
        RefOr::T(schema) => make_schema_nullable(schema),
    }
}

fn make_schema_nullable(schema: &mut Schema) {
    match schema {
        Schema::Object(schema) => add_null_type(&mut schema.schema_type),
        Schema::Array(schema) => add_null_type(&mut schema.schema_type),
        Schema::AnyOf(schema) => {
            if !schema.items.iter().any(ref_or_is_null_schema) {
                schema.items.push(null_schema());
            }
        }
        Schema::OneOf(schema) => {
            if !schema.items.iter().any(ref_or_is_null_schema) {
                schema.items.push(null_schema());
            }
        }
        _ => {}
    }
}

fn make_non_nullable(schema: &mut RefOr<Schema>) {
    if let RefOr::T(schema) = schema {
        make_schema_non_nullable(schema);
    }
}

fn make_schema_non_nullable(schema: &mut Schema) {
    match schema {
        Schema::Object(schema) => remove_null_type(&mut schema.schema_type),
        Schema::Array(schema) => remove_null_type(&mut schema.schema_type),
        Schema::AnyOf(schema) => {
            schema.items.retain(|item| !ref_or_is_null_schema(item));
        }
        Schema::OneOf(schema) => {
            schema.items.retain(|item| !ref_or_is_null_schema(item));
        }
        _ => {}
    }
}

fn add_null_type(schema_type: &mut SchemaType) {
    match schema_type {
        SchemaType::Type(schema_type_value) => {
            if *schema_type_value != Type::Null {
                *schema_type =
                    [schema_type_value.clone(), Type::Null].into_iter().collect();
            }
        }
        SchemaType::Array(schema_types) => {
            if !schema_types.contains(&Type::Null) {
                schema_types.push(Type::Null);
            }
        }
        SchemaType::AnyValue => {
            *schema_type = [Type::Object, Type::Null].into_iter().collect();
        }
    }
}

fn remove_null_type(schema_type: &mut SchemaType) {
    if let SchemaType::Array(schema_types) = schema_type {
        schema_types.retain(|schema_type| schema_type != &Type::Null);
        if schema_types.len() == 1 {
            *schema_type = SchemaType::Type(schema_types[0].clone());
        }
    }
}

fn null_schema() -> RefOr<Schema> {
    RefOr::T(Schema::Object(Object::with_type(Type::Null)))
}

fn ref_or_is_null_schema(schema: &RefOr<Schema>) -> bool {
    matches!(
        schema,
        RefOr::T(Schema::Object(Object {
            schema_type: SchemaType::Type(Type::Null),
            ..
        }))
    )
}
