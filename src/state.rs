use std::{
    collections::{BTreeMap, HashSet},
    env,
    path::{Component, Path, PathBuf},
    sync::{Arc, RwLock},
};

use chrono::{DateTime, Duration, Utc};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    auth::{hash_password, verify_password},
    catalog::{
        normalize_browse_limit, normalize_search_limit, parse_album_browse_sort,
        parse_artist_browse_sort, parse_catalog_search_filters, parse_catalog_search_query,
        parse_episode_browse_sort, parse_podcast_browse_sort, parse_track_browse_sort,
        CatalogBrowseError, CatalogBrowsePage, CatalogGroupedSearchResults,
        CatalogPodcastEpisode, CatalogSearchError,
    },
    domain::{
        AccountRole, Album, Artist, ArtworkAsset, ArtworkKind, AuthenticatedAccount,
        CatalogEntityType, Episode, ImportJob, ImportJobKind, ImportJobSource, MaintenanceScope,
        MediaFile, PlaybackHistoryEvent, PlaybackItemType, PlaybackProgress, Playlist,
        PlaylistItem, PlaylistScope, Podcast, ProviderHealth, ProviderKind, ProviderSetting,
        ProviderStatus, QuarantineItem, QuarantineStatus, RepairPlan, SystemConfig, Track,
        TranscodeSlotUsage, UserAccount, DEFAULT_SCAN_THREAD_COUNT,
    },
    error::ApiError,
    pipeline::{
        EnqueueOutcome, ImportPipeline, ImportPipelineError, ImportRunSummary,
        ImportWorkRequest,
    },
    providers::reconcile_provider_readiness,
    storage::{
        CatalogImportFailure, ConfigError as DatabaseConfigError, DatabaseConfig,
        PgMaintenanceRepository, PlaylistItemAddResult, PlaylistItemListResult,
        PlaylistItemRemoveResult, PlaylistItemReorderResult, ProviderSettingSeed,
        QuarantineRetryError, QuarantineRetryWork, StorageError,
    },
    transcode::{
        HlsGenerationCoordinator, HlsGenerationLease, TranscodeAdmission,
        TranscodeCapacityExhausted, TranscodeSlot,
    },
};

#[derive(Debug, Clone)]
/// Represents server config in the application state facade used by HTTP handlers and background workers.
///
/// Functionality: Carries fields `database`, `library_root`, `dropbox_root`, `ffmpeg_path`, `transcode_concurrency_limit`, `scan_thread_count`, and `providers` for application state facade used by HTTP handlers and background workers.
/// Dependencies: depends on `DatabaseConfig`, `PathBuf`, `PathBuf`, `PathBuf`, `i32`, `i32`, and `BTreeMap<ProviderKind` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/lib.rs`, `src/main.rs`, `src/state.rs`, `tests/maintenance_api.rs`.
pub struct ServerConfig {
    pub database: DatabaseConfig,
    pub library_root: PathBuf,
    pub dropbox_root: PathBuf,
    pub ffmpeg_path: PathBuf,
    pub transcode_concurrency_limit: i32,
    pub scan_thread_count: i32,
    pub providers: BTreeMap<ProviderKind, ProviderConfig>,
}

#[derive(Debug, Clone)]
/// Represents provider config in the application state facade used by HTTP handlers and background workers.
///
/// Functionality: Carries fields `enabled`, `api_key`, `api_key_configured`, `requires_api_key` for application state facade used by HTTP handlers and background workers.
/// Dependencies: depends on `bool`, `Option<String>`, `bool`, `bool` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `tests/maintenance_api.rs`.
pub struct ProviderConfig {
    pub enabled: bool,
    pub api_key: Option<String>,
    pub api_key_configured: bool,
    pub requires_api_key: bool,
}

#[derive(Debug, Clone)]
/// Represents admin dashboard summary counts in the application state facade used by HTTP handlers and background workers.
///
/// Functionality: Carries fields `scanning`, `imported`, `quarantined`, `failed`, `artists`, `albums`, `tracks`, `playlists`, `active_jobs` for application state facade used by HTTP handlers and background workers.
/// Dependencies: depends on `i64`, `i64`, `i64`, `i64`, `i64`, and 4 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`.
pub struct AdminDashboardSummaryCounts {
    pub scanning: i64,
    pub imported: i64,
    pub quarantined: i64,
    pub failed: i64,
    pub artists: i64,
    pub albums: i64,
    pub tracks: i64,
    pub playlists: i64,
    pub active_jobs: Vec<AdminDashboardActiveImportJob>,
}

#[derive(Debug, Clone)]
/// Represents active import job progress for the admin dashboard in the application state facade.
///
/// Functionality: Carries fields `job`, `processed_files`, `published_files`, `quarantined_files`, `failed_files`, `last_progress_at` for application state facade used by HTTP handlers and background workers.
/// Dependencies: depends on `ImportJob`, `i64`, `i64`, `i64`, `i64`, `Option<DateTime<Utc>>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/state.rs`.
pub struct AdminDashboardActiveImportJob {
    pub job: ImportJob,
    pub processed_files: i64,
    pub published_files: i64,
    pub quarantined_files: i64,
    pub failed_files: i64,
    pub last_progress_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Error)]
/// Represents server config error in the application state facade used by HTTP handlers and background workers.
///
/// Functionality: Enumerates `Database`, `InvalidTranscodeConcurrencyLimit` states or choices for application state facade used by HTTP handlers and background workers.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`.
pub enum ServerConfigError {
    #[error(transparent)]
    Database(#[from] DatabaseConfigError),
    #[error("HARMONIXIA_TRANSCODE_CONCURRENCY_LIMIT must be a non-negative integer")]
    InvalidTranscodeConcurrencyLimit,
    #[error("HARMONIXIA_SCAN_THREAD_COUNT must be a positive integer")]
    InvalidScanThreadCount,
}

impl ServerConfig {
    /// Builds configuration from environment variables for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `Self` on success or `ServerConfigError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ServerConfigError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub fn from_env() -> Result<Self, ServerConfigError> {
        Ok(Self {
            database: DatabaseConfig::from_env()?,
            library_root: env::var("HARMONIXIA_LIBRARY_ROOT")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("/srv/harmonixia/library")),
            dropbox_root: env::var("HARMONIXIA_DROPBOX_ROOT")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("/srv/harmonixia/dropbox")),
            ffmpeg_path: env::var("HARMONIXIA_FFMPEG_PATH")
                .map(PathBuf::from)
                .unwrap_or_else(|_| PathBuf::from("ffmpeg")),
            transcode_concurrency_limit: env_nonnegative_i32(
                "HARMONIXIA_TRANSCODE_CONCURRENCY_LIMIT",
                2,
            )?,
            scan_thread_count: env_positive_i32(
                "HARMONIXIA_SCAN_THREAD_COUNT",
                DEFAULT_SCAN_THREAD_COUNT,
            )?,
            providers: ProviderKind::all()
                .iter()
                .copied()
                .map(|provider| (provider, ProviderConfig::from_env(provider)))
                .collect(),
        })
    }

    /// Handles managed roots configured for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn managed_roots_configured(&self) -> bool {
        !self.library_root.as_os_str().is_empty() && !self.dropbox_root.as_os_str().is_empty()
    }
}

impl Default for ServerConfig {
    /// Builds the default configuration for application state facade used by HTTP handlers and background workers.
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
            database: DatabaseConfig {
                url: "postgres://localhost/harmonixia".to_string(),
                max_connections: 5,
                connect_timeout: std::time::Duration::from_secs(5),
                schema: None,
            },
            library_root: PathBuf::from("/srv/harmonixia/library"),
            dropbox_root: PathBuf::from("/srv/harmonixia/dropbox"),
            ffmpeg_path: PathBuf::from("ffmpeg"),
            transcode_concurrency_limit: 2,
            scan_thread_count: DEFAULT_SCAN_THREAD_COUNT,
            providers: ProviderKind::all()
                .iter()
                .copied()
                .map(|provider| (provider, ProviderConfig::default_for(provider)))
                .collect(),
        }
    }
}

impl ProviderConfig {
    /// Builds provider-specific default configuration for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn default_for(provider: ProviderKind) -> Self {
        Self {
            enabled: true,
            api_key: None,
            api_key_configured: false,
            requires_api_key: provider_requires_api_key(provider),
        }
    }

    /// Builds configuration from environment variables for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn from_env(provider: ProviderKind) -> Self {
        let defaults = Self::default_for(provider);
        let env_fragment = provider_env_fragment(provider);
        let enabled_var = format!("HARMONIXIA_PROVIDER_{env_fragment}_ENABLED");
        let api_key_var = format!("HARMONIXIA_PROVIDER_{env_fragment}_API_KEY");
        let token_var = format!("HARMONIXIA_PROVIDER_{env_fragment}_TOKEN");
        let api_key = env_value(&api_key_var).or_else(|| env_value(&token_var));

        Self {
            enabled: env_bool(&enabled_var).unwrap_or(defaults.enabled),
            api_key_configured: api_key.is_some(),
            api_key,
            requires_api_key: defaults.requires_api_key,
        }
    }
}

#[derive(Debug, Clone)]
/// Represents app state in the application state facade used by HTTP handlers and background workers.
///
/// Functionality: Carries fields `inner` for application state facade used by HTTP handlers and background workers.
/// Dependencies: depends on `Arc<AppStateInner>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/accounts.rs`, `src/api/admin_ui.rs`, `src/api/catalog.rs`, `src/api/config.rs`, and 11 more.
pub struct AppState {
    inner: Arc<AppStateInner>,
}

#[derive(Debug)]
/// Represents app state inner in the application state facade used by HTTP handlers and background workers.
///
/// Functionality: Carries fields `config`, `system_config`, `transcode_admission`, `hls_generation_coordinator`, `repository` for application state facade used by HTTP handlers and background workers.
/// Dependencies: depends on `ServerConfig`, `RwLock<SystemConfig>`, `TranscodeAdmission`, `HlsGenerationCoordinator`, `PgMaintenanceRepository` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`.
struct AppStateInner {
    config: ServerConfig,
    system_config: RwLock<SystemConfig>,
    transcode_admission: TranscodeAdmission,
    hls_generation_coordinator: HlsGenerationCoordinator,
    repository: PgMaintenanceRepository,
}

impl AppState {
    /// Connects to persistence and initializes runtime state for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - `config`: `ServerConfig`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    pub async fn connect(config: ServerConfig) -> Result<Self, StorageError> {
        let repository = PgMaintenanceRepository::connect(&config.database).await?;
        Self::from_repository(config, repository).await
    }

    /// Builds application state from an existing repository for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - `config`: `ServerConfig`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `repository`: `PgMaintenanceRepository`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    pub async fn from_repository(
        config: ServerConfig,
        repository: PgMaintenanceRepository,
    ) -> Result<Self, StorageError> {
        let system_config = repository
            .load_or_initialize_system_config(&system_config_from_bootstrap(&config))
            .await?;
        let provider_settings = repository
            .load_or_initialize_provider_settings(
                &provider_setting_seeds_from_bootstrap(&config),
            )
            .await?;
        seed_provider_health(&repository, &provider_settings).await?;
        repository.backfill_catalog_search_upgrade_data().await?;

        Ok(Self {
            inner: Arc::new(AppStateInner {
                config,
                transcode_admission: TranscodeAdmission::new(
                    system_config.transcode_concurrency_limit as u32,
                ),
                hls_generation_coordinator: HlsGenerationCoordinator::new(),
                system_config: RwLock::new(system_config),
                repository,
            }),
        })
    }

    /// Handles config for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&ServerConfig` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    pub fn config(&self) -> &ServerConfig {
        &self.inner.config
    }

    /// Handles repository for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&PgMaintenanceRepository` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    pub fn repository(&self) -> &PgMaintenanceRepository {
        &self.inner.repository
    }

    /// Handles system config for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `SystemConfig` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    pub fn system_config(&self) -> SystemConfig {
        self.inner
            .system_config
            .read()
            .expect("system config lock poisoned")
            .clone()
    }

    /// Updates existing state for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `library_root`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `dropbox_root`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `podcast_subtree`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `transcode_concurrency_limit`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `scan_thread_count`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    ///
    /// Output:
    /// - Returns `SystemConfig` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    pub async fn update_system_config(
        &self,
        library_root: &str,
        dropbox_root: &str,
        podcast_subtree: Option<&str>,
        transcode_concurrency_limit: Option<i32>,
        scan_thread_count: Option<i32>,
    ) -> Result<SystemConfig, ApiError> {
        let current = self.system_config();
        let config = SystemConfig {
            library_root: normalize_root_path(library_root, "library_root")?,
            dropbox_root: normalize_root_path(dropbox_root, "dropbox_root")?,
            podcast_subtree: normalize_podcast_subtree(
                podcast_subtree.unwrap_or(current.podcast_subtree.as_str()),
            )?,
            transcode_concurrency_limit: normalize_transcode_concurrency_limit(
                transcode_concurrency_limit.unwrap_or(current.transcode_concurrency_limit),
            )?,
            scan_thread_count: normalize_scan_thread_count(
                scan_thread_count.unwrap_or(current.scan_thread_count),
            )?,
            updated_at: Utc::now(),
        };
        let config = self
            .inner
            .repository
            .save_system_config(&config)
            .await
            .map_err(api_storage_error)?;

        *self
            .inner
            .system_config
            .write()
            .expect("system config lock poisoned") = config.clone();
        self.inner
            .transcode_admission
            .set_limit(config.transcode_concurrency_limit as u32);

        Ok(config)
    }

    /// Handles transcode slot usage for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `TranscodeSlotUsage` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn transcode_slot_usage(&self) -> TranscodeSlotUsage {
        self.inner.transcode_admission.usage()
    }

    /// Handles try acquire transcode slot for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `TranscodeSlot` on success or `TranscodeCapacityExhausted` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `TranscodeCapacityExhausted` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub fn try_acquire_transcode_slot(
        &self,
    ) -> Result<TranscodeSlot, TranscodeCapacityExhausted> {
        self.inner.transcode_admission.try_acquire()
    }

    /// Handles join or start hls generation for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `key`: `PathBuf`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
    ///
    /// Output:
    /// - Returns `HlsGenerationLease` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn join_or_start_hls_generation(&self, key: PathBuf) -> HlsGenerationLease {
        self.inner.hls_generation_coordinator.join_or_start(key)
    }

    /// Handles provider settings for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Vec<ProviderSetting>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn provider_settings(&self) -> Result<Vec<ProviderSetting>, ApiError> {
        self.inner
            .repository
            .provider_settings()
            .await
            .map_err(api_storage_error)
    }

    /// Handles provider setting for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    ///
    /// Output:
    /// - Returns `Option<ProviderSetting>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn provider_setting(
        &self,
        provider: ProviderKind,
    ) -> Result<Option<ProviderSetting>, ApiError> {
        self.inner
            .repository
            .provider_setting(provider)
            .await
            .map_err(api_storage_error)
    }

    /// Updates existing state for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    /// - `enabled`: `Option<bool>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `api_key`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `clear_api_key`: `bool`; expected to be a boolean flag controlling the documented branch.
    ///
    /// Output:
    /// - Returns `ProviderSetting` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn update_provider_setting(
        &self,
        provider: ProviderKind,
        enabled: Option<bool>,
        api_key: Option<&str>,
        clear_api_key: bool,
    ) -> Result<ProviderSetting, ApiError> {
        if clear_api_key && api_key.is_some() {
            return Err(ApiError::BadRequest(
                "api_key and clear_api_key cannot be used together".into(),
            ));
        }
        let api_key = api_key
            .map(|api_key| normalize_secret(api_key, "api_key"))
            .transpose()?;

        let setting = self
            .inner
            .repository
            .update_provider_setting(provider, enabled, api_key.as_deref(), clear_api_key)
            .await
            .map_err(api_storage_error)?
            .ok_or_else(|| ApiError::NotFound(format!("unknown provider: {provider}")))?;
        self.reconcile_provider_health(setting.clone()).await?;

        Ok(setting)
    }

    /// Reconciles state for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `setting`: `ProviderSetting`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `()` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn reconcile_provider_health(
        &self,
        setting: ProviderSetting,
    ) -> Result<(), ApiError> {
        let now = Utc::now();
        let config_health = provider_setting_health(&setting, now);
        let mut reconciled = match self
            .inner
            .repository
            .provider(setting.provider)
            .await
            .map_err(api_storage_error)?
        {
            None => config_health,
            Some(existing) => reconcile_provider_health_record(existing, config_health, now),
        };
        reconcile_provider_readiness(&mut reconciled, &now);

        self.inner
            .repository
            .save_provider_health(&reconciled)
            .await
            .map_err(api_storage_error)
    }

    /// Creates a new resource for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `username`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `password`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `role`: `AccountRole`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `AuthenticatedAccount` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn create_local_account(
        &self,
        username: &str,
        password: &str,
        role: AccountRole,
    ) -> Result<AuthenticatedAccount, ApiError> {
        let username = normalize_username(username)?;
        validate_password(password)?;
        let password_hash = hash_password(password).map_err(|error| {
            tracing::error!(%error, "password hashing failed");
            ApiError::Internal
        })?;

        self.inner
            .repository
            .create_local_account(&username, &password_hash, role)
            .await
            .map(AuthenticatedAccount::from)
            .map_err(api_storage_error)
    }

    /// Creates a new resource for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `username`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `password`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `UserAccount` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn create_first_admin(
        &self,
        username: &str,
        password: &str,
    ) -> Result<UserAccount, ApiError> {
        let username = normalize_username(username)?;
        validate_password(password)?;
        let password_hash = hash_password(password).map_err(|error| {
            tracing::error!(%error, "password hashing failed");
            ApiError::Internal
        })?;

        match self
            .inner
            .repository
            .create_first_admin_if_no_accounts(&username, &password_hash)
            .await
        {
            Ok(Some(account)) => Ok(account.into()),
            Ok(None) => Err(ApiError::Conflict(
                "first admin bootstrap is only allowed when no users exist".into(),
            )),
            Err(error) if error.is_unique_violation() => Err(ApiError::Conflict(
                "username is already in use".into(),
            )),
            Err(error) => Err(api_storage_error(error)),
        }
    }

    /// Handles has local accounts for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `bool` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn has_local_accounts(&self) -> Result<bool, ApiError> {
        self.inner
            .repository
            .local_account_count()
            .await
            .map(|count| count > 0)
            .map_err(api_storage_error)
    }

    /// Handles user accounts for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Vec<UserAccount>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn user_accounts(&self) -> Result<Vec<UserAccount>, ApiError> {
        self.inner
            .repository
            .local_accounts()
            .await
            .map(|accounts| accounts.into_iter().map(UserAccount::from).collect())
            .map_err(api_storage_error)
    }

    /// Creates a new resource for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `username`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `password`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `role`: `AccountRole`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `UserAccount` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn create_user_account(
        &self,
        username: &str,
        password: &str,
        role: AccountRole,
    ) -> Result<UserAccount, ApiError> {
        let username = normalize_username(username)?;
        validate_password(password)?;
        let password_hash = hash_password(password).map_err(|error| {
            tracing::error!(%error, "password hashing failed");
            ApiError::Internal
        })?;

        match self
            .inner
            .repository
            .create_local_account(&username, &password_hash, role)
            .await
        {
            Ok(account) => Ok(account.into()),
            Err(error) if error.is_unique_violation() => Err(ApiError::Conflict(
                "username is already in use".into(),
            )),
            Err(error) => Err(api_storage_error(error)),
        }
    }

    /// Resets stored state for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `password`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `UserAccount` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn reset_user_password(
        &self,
        account_id: Uuid,
        password: &str,
    ) -> Result<UserAccount, ApiError> {
        validate_password(password)?;
        let password_hash = hash_password(password).map_err(|error| {
            tracing::error!(%error, "password hashing failed");
            ApiError::Internal
        })?;

        self.inner
            .repository
            .update_local_account_password(account_id, &password_hash)
            .await
            .map_err(api_storage_error)?
            .map(UserAccount::from)
            .ok_or_else(|| ApiError::NotFound(format!("user {account_id} was not found")))
    }

    /// Deletes or removes a resource from application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `UserAccount` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn delete_user_account(
        &self,
        account_id: Uuid,
    ) -> Result<UserAccount, ApiError> {
        match self.inner.repository.delete_local_account(account_id).await {
            Ok(Some(account)) => Ok(account.into()),
            Ok(None) => Err(ApiError::NotFound(format!("user {account_id} was not found"))),
            Err(StorageError::LastEnabledAdmin) => Err(ApiError::Conflict(
                "cannot delete the last enabled admin account".into(),
            )),
            Err(error) => Err(api_storage_error(error)),
        }
    }

    /// Handles authenticate local account for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `username`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `password`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Option<AuthenticatedAccount>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn authenticate_local_account(
        &self,
        username: &str,
        password: &str,
    ) -> Result<Option<AuthenticatedAccount>, ApiError> {
        let username = username.trim();
        if username.is_empty() {
            return Ok(None);
        }

        let Some(account) = self
            .inner
            .repository
            .local_account_by_username(username)
            .await
            .map_err(api_storage_error)?
        else {
            return Ok(None);
        };

        if verify_password(password, &account.password_hash) {
            Ok(Some(account.into()))
        } else {
            Ok(None)
        }
    }

    /// Creates a new resource for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `name`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `description`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `scope`: `PlaylistScope`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Playlist` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn create_playlist(
        &self,
        account_id: Uuid,
        name: &str,
        description: Option<&str>,
        scope: PlaylistScope,
    ) -> Result<Playlist, ApiError> {
        let name = normalize_name(name, "playlist name")?;
        let description = normalize_optional_text(description);

        self.inner
            .repository
            .create_playlist(account_id, &name, description.as_deref(), scope)
            .await
            .map_err(api_storage_error)
    }

    /// Handles playlists visible to for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Vec<Playlist>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn playlists_visible_to(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<Playlist>, ApiError> {
        self.inner
            .repository
            .playlists_visible_to(account_id)
            .await
            .map_err(api_storage_error)
    }

    /// Handles visible playlist for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Playlist` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn visible_playlist(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
    ) -> Result<Playlist, ApiError> {
        self.inner
            .repository
            .visible_playlist(account_id, playlist_id)
            .await
            .map_err(api_storage_error)?
            .ok_or_else(|| ApiError::NotFound(format!("playlist {playlist_id} was not found")))
    }

    /// Updates existing state for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `name`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `description`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Playlist` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn update_visible_playlist(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
        name: &str,
        description: Option<&str>,
    ) -> Result<Playlist, ApiError> {
        let name = normalize_name(name, "playlist name")?;
        let description = normalize_optional_text(description);

        self.inner
            .repository
            .update_visible_playlist(account_id, playlist_id, &name, description.as_deref())
            .await
            .map_err(api_storage_error)?
            .ok_or_else(|| ApiError::NotFound(format!("playlist {playlist_id} was not found")))
    }

    /// Deletes or removes a resource from application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Playlist` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn delete_visible_playlist(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
    ) -> Result<Playlist, ApiError> {
        self.inner
            .repository
            .delete_visible_playlist(account_id, playlist_id)
            .await
            .map_err(api_storage_error)?
            .ok_or_else(|| ApiError::NotFound(format!("playlist {playlist_id} was not found")))
    }

    /// Lists resources for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Vec<PlaylistItem>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn list_visible_playlist_items(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
    ) -> Result<Vec<PlaylistItem>, ApiError> {
        match self
            .inner
            .repository
            .list_visible_playlist_items(account_id, playlist_id)
            .await
            .map_err(api_storage_error)?
        {
            PlaylistItemListResult::Items(items) => Ok(items),
            PlaylistItemListResult::PlaylistNotFound => {
                Err(ApiError::NotFound(format!("playlist {playlist_id} was not found")))
            }
        }
    }

    /// Handles add visible playlist item for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `item_type`: `PlaybackItemType`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `position`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    ///
    /// Output:
    /// - Returns `PlaylistItem` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn add_visible_playlist_item(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
        item_type: PlaybackItemType,
        item_id: Uuid,
        position: Option<u32>,
    ) -> Result<PlaylistItem, ApiError> {
        match self
            .inner
            .repository
            .add_visible_playlist_item(account_id, playlist_id, item_type, item_id, position)
            .await
            .map_err(api_storage_error)?
        {
            PlaylistItemAddResult::Added(item) => Ok(item),
            PlaylistItemAddResult::PlaylistNotFound => {
                Err(ApiError::NotFound(format!("playlist {playlist_id} was not found")))
            }
            PlaylistItemAddResult::ItemNotEligible => Err(ApiError::BadRequest(format!(
                "{item_type} {item_id} is not a published playlist-eligible catalog item"
            ))),
            PlaylistItemAddResult::InvalidPosition => Err(ApiError::BadRequest(
                "position must be between 0 and the current playlist length".into(),
            )),
        }
    }

    /// Handles remove visible playlist item for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `()` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn remove_visible_playlist_item(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
        playlist_item_id: Uuid,
    ) -> Result<(), ApiError> {
        match self
            .inner
            .repository
            .remove_visible_playlist_item(account_id, playlist_id, playlist_item_id)
            .await
            .map_err(api_storage_error)?
        {
            PlaylistItemRemoveResult::Removed => Ok(()),
            PlaylistItemRemoveResult::PlaylistNotFound => Err(ApiError::NotFound(format!(
                "playlist item {playlist_item_id} was not found"
            ))),
        }
    }

    /// Handles reorder visible playlist items for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_item_ids`: `Vec<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Vec<PlaylistItem>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn reorder_visible_playlist_items(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
        playlist_item_ids: Vec<Uuid>,
    ) -> Result<Vec<PlaylistItem>, ApiError> {
        let unique_ids = playlist_item_ids.iter().copied().collect::<HashSet<_>>();
        if unique_ids.len() != playlist_item_ids.len() {
            return Err(ApiError::BadRequest(
                "item_ids must not contain duplicate playlist item ids".into(),
            ));
        }

        match self
            .inner
            .repository
            .reorder_visible_playlist_items(account_id, playlist_id, playlist_item_ids)
            .await
            .map_err(api_storage_error)?
        {
            PlaylistItemReorderResult::Reordered(items) => Ok(items),
            PlaylistItemReorderResult::PlaylistNotFound => {
                Err(ApiError::NotFound(format!("playlist {playlist_id} was not found")))
            }
            PlaylistItemReorderResult::ItemSetMismatch => Err(ApiError::BadRequest(
                "item_ids must contain every current playlist item exactly once".into(),
            )),
        }
    }

    /// Returns a paginated browse view for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `limit`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `cursor`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `sort`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `CatalogBrowsePage<Artist>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn browse_artists(
        &self,
        limit: Option<u32>,
        cursor: Option<&str>,
        sort: Option<&str>,
    ) -> Result<CatalogBrowsePage<Artist>, ApiError> {
        let limit = normalize_browse_limit(limit).map_err(api_catalog_browse_error)?;
        let sort = parse_artist_browse_sort(sort).map_err(api_catalog_browse_error)?;

        self.inner
            .repository
            .browse_artists(limit, cursor, sort)
            .await
            .map_err(api_catalog_browse_error)
    }

    /// Returns a paginated browse view for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `limit`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `cursor`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `sort`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `CatalogBrowsePage<Album>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn browse_albums(
        &self,
        limit: Option<u32>,
        cursor: Option<&str>,
        sort: Option<&str>,
    ) -> Result<CatalogBrowsePage<Album>, ApiError> {
        let limit = normalize_browse_limit(limit).map_err(api_catalog_browse_error)?;
        let sort = parse_album_browse_sort(sort).map_err(api_catalog_browse_error)?;

        self.inner
            .repository
            .browse_albums(limit, cursor, sort)
            .await
            .map_err(api_catalog_browse_error)
    }

    /// Returns a paginated browse view for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `limit`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `cursor`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `sort`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `CatalogBrowsePage<Track>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn browse_tracks(
        &self,
        limit: Option<u32>,
        cursor: Option<&str>,
        sort: Option<&str>,
    ) -> Result<CatalogBrowsePage<Track>, ApiError> {
        let limit = normalize_browse_limit(limit).map_err(api_catalog_browse_error)?;
        let sort = parse_track_browse_sort(sort).map_err(api_catalog_browse_error)?;

        self.inner
            .repository
            .browse_tracks(limit, cursor, sort)
            .await
            .map_err(api_catalog_browse_error)
    }

    /// Returns a paginated browse view for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `limit`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `cursor`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `sort`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `CatalogBrowsePage<Podcast>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn browse_podcasts(
        &self,
        limit: Option<u32>,
        cursor: Option<&str>,
        sort: Option<&str>,
    ) -> Result<CatalogBrowsePage<Podcast>, ApiError> {
        let limit = normalize_browse_limit(limit).map_err(api_catalog_browse_error)?;
        let sort = parse_podcast_browse_sort(sort).map_err(api_catalog_browse_error)?;

        self.inner
            .repository
            .browse_podcasts(limit, cursor, sort)
            .await
            .map_err(api_catalog_browse_error)
    }

    /// Handles visible podcast for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `podcast_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Podcast` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn visible_podcast(&self, podcast_id: Uuid) -> Result<Podcast, ApiError> {
        self.inner
            .repository
            .visible_podcast(podcast_id)
            .await
            .map_err(api_storage_error)?
            .ok_or_else(|| ApiError::NotFound(format!("podcast {podcast_id} was not found")))
    }

    /// Returns a paginated browse view for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `limit`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `cursor`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `sort`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `CatalogBrowsePage<Episode>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn browse_episodes(
        &self,
        limit: Option<u32>,
        cursor: Option<&str>,
        sort: Option<&str>,
    ) -> Result<CatalogBrowsePage<Episode>, ApiError> {
        let limit = normalize_browse_limit(limit).map_err(api_catalog_browse_error)?;
        let sort = parse_episode_browse_sort(sort).map_err(api_catalog_browse_error)?;

        self.inner
            .repository
            .browse_episodes(limit, cursor, sort)
            .await
            .map_err(api_catalog_browse_error)
    }

    /// Returns a paginated browse view for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `podcast_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `limit`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `cursor`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `sort`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `CatalogBrowsePage<Episode>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn browse_episodes_for_podcast(
        &self,
        podcast_id: Uuid,
        limit: Option<u32>,
        cursor: Option<&str>,
        sort: Option<&str>,
    ) -> Result<CatalogBrowsePage<Episode>, ApiError> {
        self.visible_podcast(podcast_id).await?;
        let limit = normalize_browse_limit(limit).map_err(api_catalog_browse_error)?;
        let sort = parse_episode_browse_sort(sort).map_err(api_catalog_browse_error)?;

        self.inner
            .repository
            .browse_episodes_for_podcast(podcast_id, limit, cursor, sort)
            .await
            .map_err(api_catalog_browse_error)
    }

    /// Handles visible episode for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `episode_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `CatalogPodcastEpisode` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn visible_episode(
        &self,
        episode_id: Uuid,
    ) -> Result<CatalogPodcastEpisode, ApiError> {
        self.inner
            .repository
            .visible_episode(episode_id)
            .await
            .map_err(api_storage_error)?
            .ok_or_else(|| ApiError::NotFound(format!("episode {episode_id} was not found")))
    }

    /// Searches resources for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `query`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `limit`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `year`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `genre`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `format`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `media_type`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `CatalogGroupedSearchResults` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn search_catalog(
        &self,
        account_id: Uuid,
        query: Option<&str>,
        limit: Option<u32>,
        year: Option<i32>,
        genre: Option<&str>,
        format: Option<&str>,
        media_type: Option<&str>,
    ) -> Result<CatalogGroupedSearchResults, ApiError> {
        let input = parse_catalog_search_query(query).map_err(api_catalog_search_error)?;
        let filters = parse_catalog_search_filters(year, genre, format, media_type)
            .map_err(api_catalog_search_error)?;
        let limit = normalize_search_limit(limit).map_err(api_catalog_search_error)?;

        self.inner
            .repository
            .search_catalog(account_id, &input, &filters, limit)
            .await
            .map_err(api_catalog_search_error)
    }

    /// Handles visible original media file for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `item_type`: `PlaybackItemType`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `MediaFile` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn visible_original_media_file(
        &self,
        item_type: PlaybackItemType,
        item_id: Uuid,
    ) -> Result<MediaFile, ApiError> {
        let media_file = match item_type {
            PlaybackItemType::Track => self
                .inner
                .repository
                .visible_original_media_file_for_track(item_id)
                .await
                .map_err(api_storage_error)?,
            PlaybackItemType::Episode => self
                .inner
                .repository
                .visible_original_media_file_for_episode(item_id)
                .await
                .map_err(api_storage_error)?,
        };

        media_file.ok_or_else(|| {
            ApiError::NotFound(format!("{item_type} {item_id} was not found"))
        })
    }

    pub async fn visible_artwork_assets(
        &self,
        entity_type: CatalogEntityType,
        entity_id: Uuid,
        artwork_kind: Option<ArtworkKind>,
    ) -> Result<Vec<ArtworkAsset>, ApiError> {
        self.inner
            .repository
            .visible_artwork_assets(entity_type, entity_id, artwork_kind)
            .await
            .map_err(api_storage_error)?
            .ok_or_else(|| {
                ApiError::NotFound(format!(
                    "{} {entity_id} was not found",
                    entity_type.api_name()
                ))
            })
    }

    pub async fn visible_artwork_asset(
        &self,
        artwork_asset_id: Uuid,
    ) -> Result<ArtworkAsset, ApiError> {
        self.inner
            .repository
            .visible_artwork_asset(artwork_asset_id)
            .await
            .map_err(api_storage_error)?
            .ok_or_else(|| {
                ApiError::NotFound(format!("artwork asset {artwork_asset_id} was not found"))
            })
    }

    /// Inserts or updates data for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `item_type`: `PlaybackItemType`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `position_seconds`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    /// - `duration_seconds`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `completed`: `bool`; expected to be a boolean flag controlling the documented branch.
    ///
    /// Output:
    /// - Returns `PlaybackProgress` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn upsert_playback_progress(
        &self,
        account_id: Uuid,
        item_type: PlaybackItemType,
        item_id: Uuid,
        position_seconds: u32,
        duration_seconds: Option<u32>,
        completed: bool,
    ) -> Result<PlaybackProgress, ApiError> {
        validate_progress_seconds(position_seconds, duration_seconds)?;

        self.inner
            .repository
            .upsert_playback_progress(
                account_id,
                item_type,
                item_id,
                position_seconds,
                duration_seconds,
                completed,
            )
            .await
            .map_err(api_storage_error)
    }

    /// Handles playback progress for account for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Vec<PlaybackProgress>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn playback_progress_for_account(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<PlaybackProgress>, ApiError> {
        self.inner
            .repository
            .playback_progress_for_account(account_id)
            .await
            .map_err(api_storage_error)
    }

    /// Handles optional playback progress for item for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `item_type`: `PlaybackItemType`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<PlaybackProgress>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn optional_playback_progress_for_item(
        &self,
        account_id: Uuid,
        item_type: PlaybackItemType,
        item_id: Uuid,
    ) -> Result<Option<PlaybackProgress>, ApiError> {
        self.inner
            .repository
            .playback_progress_for_item(account_id, item_type, item_id)
            .await
            .map_err(api_storage_error)
    }

    /// Handles playback progress for item for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `item_type`: `PlaybackItemType`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `PlaybackProgress` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn playback_progress_for_item(
        &self,
        account_id: Uuid,
        item_type: PlaybackItemType,
        item_id: Uuid,
    ) -> Result<PlaybackProgress, ApiError> {
        self.inner
            .repository
            .playback_progress_for_item(account_id, item_type, item_id)
            .await
            .map_err(api_storage_error)?
            .ok_or_else(|| {
                ApiError::NotFound(format!("{item_type} progress {item_id} was not found"))
            })
    }

    /// Inserts data for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `item_type`: `PlaybackItemType`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `position_seconds`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    /// - `duration_seconds`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `completed`: `bool`; expected to be a boolean flag controlling the documented branch.
    ///
    /// Output:
    /// - Returns `PlaybackHistoryEvent` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn insert_playback_history_event(
        &self,
        account_id: Uuid,
        item_type: PlaybackItemType,
        item_id: Uuid,
        position_seconds: u32,
        duration_seconds: Option<u32>,
        completed: bool,
    ) -> Result<PlaybackHistoryEvent, ApiError> {
        validate_progress_seconds(position_seconds, duration_seconds)?;

        self.inner
            .repository
            .insert_playback_history_event(
                account_id,
                item_type,
                item_id,
                position_seconds,
                duration_seconds,
                completed,
            )
            .await
            .map_err(api_storage_error)
    }

    /// Handles playback history for account for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `Vec<PlaybackHistoryEvent>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn playback_history_for_account(
        &self,
        account_id: Uuid,
        limit: u32,
    ) -> Result<Vec<PlaybackHistoryEvent>, ApiError> {
        let limit = limit.clamp(1, 200);

        self.inner
            .repository
            .playback_history_for_account(account_id, limit)
            .await
            .map_err(api_storage_error)
    }

    /// Enqueues background work for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `work`: `ImportWorkRequest`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `EnqueueOutcome` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn enqueue_import_work(
        &self,
        work: ImportWorkRequest,
    ) -> Result<EnqueueOutcome, ApiError> {
        let (job, reused_existing) = self
            .inner
            .repository
            .enqueue_import_work(work)
            .await
            .map_err(api_storage_error)?;

        Ok(EnqueueOutcome {
            job,
            reused_existing,
        })
    }

    /// Enqueues background work for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `reason`: `Option<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `EnqueueOutcome` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn enqueue_initial_scan(
        &self,
        reason: Option<String>,
    ) -> Result<EnqueueOutcome, ApiError> {
        self.enqueue_import_work(ImportWorkRequest {
            kind: ImportJobKind::InitialScan,
            scope: MaintenanceScope::FullLibrary,
            repair_plan: RepairPlan::default(),
            provider_filter: Vec::new(),
            source: ImportJobSource::AdminInitialScan,
            reason,
            related_quarantine_item_id: None,
        })
        .await
    }

    /// Enqueues background work for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `path`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `reason`: `Option<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `EnqueueOutcome` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn enqueue_dropbox_ingest(
        &self,
        path: Option<&str>,
        reason: Option<String>,
    ) -> Result<EnqueueOutcome, ApiError> {
        let scope = match path {
            Some(raw_path) => self.normalize_dropbox_scope(Some(raw_path))?,
            None => MaintenanceScope::FullLibrary,
        };
        self.enqueue_import_work(ImportWorkRequest {
            kind: ImportJobKind::DropboxIngest,
            scope,
            repair_plan: RepairPlan::default(),
            provider_filter: Vec::new(),
            source: ImportJobSource::AdminDropboxIngest,
            reason,
            related_quarantine_item_id: None,
        })
        .await
    }

    /// Enqueues background work for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
    ///
    /// Output:
    /// - Returns `EnqueueOutcome` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn enqueue_dropbox_watcher_ingest(
        &self,
        path: &Path,
    ) -> Result<EnqueueOutcome, ApiError> {
        let raw_path = path.to_string_lossy();
        let scope = self.normalize_dropbox_scope(Some(raw_path.as_ref()))?;
        self.enqueue_import_work(ImportWorkRequest {
            kind: ImportJobKind::DropboxIngest,
            scope,
            repair_plan: RepairPlan::default(),
            provider_filter: Vec::new(),
            source: ImportJobSource::DropboxWatcher,
            reason: Some("dropbox watcher detected a stable media file".into()),
            related_quarantine_item_id: None,
        })
        .await
    }

    /// Normalizes caller-provided data for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `path`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `MaintenanceScope` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub fn normalize_dropbox_scope(
        &self,
        path: Option<&str>,
    ) -> Result<MaintenanceScope, ApiError> {
        let Some(raw_path) = path else {
            return Ok(MaintenanceScope::FullLibrary);
        };

        let trimmed = raw_path.trim();
        if trimmed.is_empty() {
            return Err(ApiError::BadRequest("path cannot be empty".into()));
        }
        if trimmed.contains('\0') {
            return Err(ApiError::BadRequest(
                "path cannot contain NUL bytes".into(),
            ));
        }
        let path = Path::new(trimmed);
        if contains_parent_dir(path) {
            return Err(ApiError::BadRequest(
                "path cannot contain parent-directory traversal".into(),
            ));
        }
        let dropbox_root = PathBuf::from(&self.system_config().dropbox_root);
        let normalized = if path.is_absolute() {
            path.to_path_buf()
        } else {
            dropbox_root.join(path)
        };
        if !normalized.starts_with(&dropbox_root) {
            return Err(ApiError::BadRequest(format!(
                "dropbox ingest path must be under the dropbox root ({})",
                dropbox_root.display()
            )));
        }
        Ok(MaintenanceScope::Path {
            path: normalized.to_string_lossy().to_string(),
        })
    }

    /// Runs the operation for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `ImportRunSummary` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn run_import_job(
        &self,
        job_id: Uuid,
    ) -> Result<ImportRunSummary, ApiError> {
        let provider_health = self.provider_health().await?;
        let pipeline = ImportPipeline::new(
            self.inner.repository.clone(),
            self.system_config(),
            provider_health,
        );
        pipeline
            .run_job(job_id)
            .await
            .map_err(api_import_pipeline_error)
    }

    /// Runs the operation for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Option<ImportRunSummary>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn run_next_import_job(&self) -> Result<Option<ImportRunSummary>, ApiError> {
        let provider_health = self.provider_health().await?;
        let pipeline = ImportPipeline::new(
            self.inner.repository.clone(),
            self.system_config(),
            provider_health,
        );
        let Some(job) = self
            .inner
            .repository
            .claim_next_import_job()
            .await
            .map_err(api_storage_error)?
        else {
            return Ok(None);
        };

        pipeline
            .run_claimed(job)
            .await
            .map(Some)
            .map_err(api_import_pipeline_error)
    }

    /// Handles import jobs for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Vec<ImportJob>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn import_jobs(&self) -> Result<Vec<ImportJob>, ApiError> {
        self.inner
            .repository
            .import_jobs()
            .await
            .map_err(api_storage_error)
    }

    /// Handles active import jobs for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Vec<ImportJob>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn active_import_jobs(&self) -> Result<Vec<ImportJob>, ApiError> {
        self.inner
            .repository
            .active_import_jobs()
            .await
            .map_err(api_storage_error)
    }

    /// Handles admin dashboard summary counts for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `AdminDashboardSummaryCounts` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn admin_dashboard_summary_counts(
        &self,
    ) -> Result<AdminDashboardSummaryCounts, ApiError> {
        let catalog_counts = self
            .inner
            .repository
            .catalog_counts()
            .await
            .map_err(api_storage_error)?;
        let operational_counts = self
            .inner
            .repository
            .admin_dashboard_operational_counts()
            .await
            .map_err(api_storage_error)?;
        let active_jobs = self.active_import_jobs().await?;
        let mut active_job_progress = Vec::with_capacity(active_jobs.len());
        for job in active_jobs {
            let progress = self
                .inner
                .repository
                .import_job_progress_counts(job.id)
                .await
                .map_err(api_storage_error)?;
            active_job_progress.push(AdminDashboardActiveImportJob {
                job,
                processed_files: progress.processed_files,
                published_files: progress.published_files,
                quarantined_files: progress.quarantined_files,
                failed_files: progress.failed_files,
                last_progress_at: progress.last_progress_at,
            });
        }

        Ok(AdminDashboardSummaryCounts {
            scanning: operational_counts.scanning,
            imported: catalog_counts.published_media_files,
            quarantined: operational_counts.quarantined,
            failed: operational_counts.failed,
            artists: catalog_counts.artists,
            albums: catalog_counts.albums,
            tracks: catalog_counts.tracks,
            playlists: catalog_counts.playlists,
            active_jobs: active_job_progress,
        })
    }

    /// Lists recent failed import work items for admin diagnostics.
    ///
    /// Inputs:
    /// - `import_job_id`: optional active or historical job filter.
    /// - `limit`: maximum number of rows to return.
    ///
    /// Output:
    /// - Returns failed work items with source paths and stored errors.
    ///
    /// Errors:
    /// - Returns `ApiError` when persistence fails.
    pub async fn admin_import_failures(
        &self,
        import_job_id: Option<Uuid>,
        limit: u32,
    ) -> Result<Vec<CatalogImportFailure>, ApiError> {
        self.inner
            .repository
            .catalog_import_failures(import_job_id, limit)
            .await
            .map_err(api_storage_error)
    }

    /// Handles initial scan started for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `bool` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn initial_scan_started(&self) -> Result<bool, ApiError> {
        self.inner
            .repository
            .import_job_kind_exists(ImportJobKind::InitialScan)
            .await
            .map_err(api_storage_error)
    }

    /// Handles provider health for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Vec<ProviderHealth>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn provider_health(&self) -> Result<Vec<ProviderHealth>, ApiError> {
        let mut providers = self
            .inner
            .repository
            .provider_health()
            .await
            .map_err(api_storage_error)?;
        let now = Utc::now();
        for health in &mut providers {
            if reconcile_provider_readiness(health, &now) {
                self.inner
                    .repository
                    .save_provider_health(health)
                    .await
                    .map_err(api_storage_error)?;
            }
        }
        Ok(providers)
    }

    /// Handles provider for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    ///
    /// Output:
    /// - Returns `Option<ProviderHealth>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn provider(
        &self,
        provider: ProviderKind,
    ) -> Result<Option<ProviderHealth>, ApiError> {
        let Some(mut health) = self
            .inner
            .repository
            .provider(provider)
            .await
            .map_err(api_storage_error)?
        else {
            return Ok(None);
        };
        let now = Utc::now();
        if reconcile_provider_readiness(&mut health, &now) {
            self.inner
                .repository
                .save_provider_health(&health)
                .await
                .map_err(api_storage_error)?;
        }
        Ok(Some(health))
    }

    /// Handles prepare provider admin retry for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    ///
    /// Output:
    /// - Returns `ProviderHealth` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn prepare_provider_admin_retry(
        &self,
        provider: ProviderKind,
    ) -> Result<ProviderHealth, ApiError> {
        let Some(mut health) = self.provider(provider).await? else {
            return Err(ApiError::NotFound(format!("unknown provider: {provider}")));
        };

        if !health.enabled {
            return Err(ApiError::Conflict(format!(
                "{} is disabled",
                provider.display_name()
            )));
        }
        if health.status == ProviderStatus::Unconfigured {
            return Err(ApiError::Conflict(format!(
                "{} is not configured for provider repair",
                provider.display_name()
            )));
        }

        health.retry_after = None;
        health.maintenance_ready = true;
        if health.status == ProviderStatus::BackingOff {
            health.status = ProviderStatus::Degraded;
        }
        health.message =
            Some("Admin retry requested; next repair job will re-check provider.".into());
        health.updated_at = Utc::now();

        self.inner
            .repository
            .save_provider_health(&health)
            .await
            .map_err(api_storage_error)?;

        Ok(health)
    }

    /// Sets stored state for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    /// - `retry_after_seconds`: `i64`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `()` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn set_provider_backoff_for_tests(
        &self,
        provider: ProviderKind,
        retry_after_seconds: i64,
    ) -> Result<(), ApiError> {
        if let Some(mut health) = self.provider(provider).await? {
            let now = Utc::now();
            health.status = ProviderStatus::BackingOff;
            health.maintenance_ready = false;
            health.failure_count += 1;
            health.last_failure_at = Some(now);
            health.retry_after = Some(now + Duration::seconds(retry_after_seconds));
            health.message = Some("Provider is in retry backoff after repeated failures.".into());
            health.updated_at = now;
            self.inner
                .repository
                .save_provider_health(&health)
                .await
                .map_err(api_storage_error)?;
        }

        Ok(())
    }

    /// Inserts data for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `item`: `QuarantineItem`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `QuarantineItem` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn insert_quarantine_item_for_tests(
        &self,
        item: QuarantineItem,
    ) -> Result<QuarantineItem, ApiError> {
        self.inner
            .repository
            .insert_quarantine_item(&item)
            .await
            .map_err(api_storage_error)?;
        Ok(item)
    }

    /// Handles quarantine item for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<QuarantineItem>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn quarantine_item(
        &self,
        item_id: Uuid,
    ) -> Result<Option<QuarantineItem>, ApiError> {
        self.inner
            .repository
            .quarantine_item(item_id)
            .await
            .map_err(api_storage_error)
    }

    /// Marks UI or workflow state for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `job_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `QuarantineItem` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn mark_quarantine_retrying(
        &self,
        item_id: Uuid,
        job_id: Uuid,
    ) -> Result<QuarantineItem, ApiError> {
        self.inner
            .repository
            .mark_quarantine_retrying(item_id, job_id)
            .await
            .map_err(api_storage_error)?
            .ok_or_else(|| {
                ApiError::NotFound(format!("quarantine item {item_id} was not found"))
            })
    }

    /// Handles active job for quarantine item for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<ImportJob>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn active_job_for_quarantine_item(
        &self,
        item_id: Uuid,
    ) -> Result<Option<ImportJob>, ApiError> {
        self.inner
            .repository
            .active_job_for_quarantine_item(item_id)
            .await
            .map_err(api_storage_error)
    }

    /// Enqueues background work for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `item_ids`: `Vec<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `repair_plan`: `RepairPlan`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Vec<(Uuid, ImportJob)>` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn enqueue_quarantine_retries(
        &self,
        item_ids: Vec<Uuid>,
        repair_plan: RepairPlan,
    ) -> Result<Vec<(Uuid, ImportJob)>, ApiError> {
        let mut prepared = Vec::with_capacity(item_ids.len());

        for item_id in item_ids {
            let item = self.quarantine_item(item_id).await?.ok_or_else(|| {
                ApiError::NotFound(format!("quarantine item {item_id} was not found"))
            })?;
            validate_quarantine_retry_item(&item)?;

            let scope = self.normalize_maintenance_scope(Some(&item.source_path))?;
            let work = ImportWorkRequest {
                kind: crate::domain::ImportJobKind::QuarantineRetry,
                scope,
                repair_plan: repair_plan.clone(),
                provider_filter: Vec::new(),
                source: crate::domain::ImportJobSource::QuarantineRetry,
                reason: Some(format!(
                    "retry quarantined item {item_id}: {:?}",
                    item.reason
                )),
                related_quarantine_item_id: Some(item_id),
            };

            prepared.push(QuarantineRetryWork { item_id, work });
        }

        self.inner
            .repository
            .enqueue_quarantine_retries(prepared)
            .await
            .map_err(api_quarantine_retry_error)
    }

    /// Normalizes caller-provided data for application state facade used by HTTP handlers and background workers.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `path`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `MaintenanceScope` on success or `ApiError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub fn normalize_maintenance_scope(
        &self,
        path: Option<&str>,
    ) -> Result<MaintenanceScope, ApiError> {
        let Some(raw_path) = path else {
            return Ok(MaintenanceScope::FullLibrary);
        };

        let trimmed = raw_path.trim();
        if trimmed.is_empty() {
            return Err(ApiError::BadRequest("path cannot be empty".into()));
        }
        if trimmed.contains('\0') {
            return Err(ApiError::BadRequest("path cannot contain NUL bytes".into()));
        }

        let path = Path::new(trimmed);
        if contains_parent_dir(path) {
            return Err(ApiError::BadRequest(
                "path cannot contain parent-directory traversal".into(),
            ));
        }

        let config = self.system_config();
        let library_root = PathBuf::from(&config.library_root);
        let dropbox_root = PathBuf::from(&config.dropbox_root);

        let normalized = if path.is_absolute() {
            path.to_path_buf()
        } else {
            library_root.join(path)
        };

        if !normalized.starts_with(&library_root) && !normalized.starts_with(&dropbox_root)
        {
            return Err(ApiError::BadRequest(format!(
                "path must be under the managed library root ({}) or dropbox root ({})",
                library_root.display(),
                dropbox_root.display()
            )));
        }

        Ok(MaintenanceScope::Path {
            path: normalized.to_string_lossy().to_string(),
        })
    }
}

/// Validates data for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `item`: `&QuarantineItem`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn validate_quarantine_retry_item(item: &QuarantineItem) -> Result<(), ApiError> {
    if !item.retry_eligible {
        return Err(ApiError::Conflict(format!(
            "quarantine item {} is not retry eligible",
            item.id
        )));
    }
    if matches!(
        item.status,
        QuarantineStatus::Deleted | QuarantineStatus::Resolved
    ) {
        return Err(ApiError::Conflict(format!(
            "quarantine item {} is already {:?}",
            item.id, item.status
        )));
    }

    Ok(())
}

/// Handles seed provider health for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `repository`: `&PgMaintenanceRepository`; expected to be a value satisfying the type contract shown in the function signature.
/// - `settings`: `&[ProviderSetting]`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn seed_provider_health(
    repository: &PgMaintenanceRepository,
    settings: &[ProviderSetting],
) -> Result<(), StorageError> {
    let now = Utc::now();

    for setting in settings {
        let config_health = provider_setting_health(setting, now);

        let mut reconciled = match repository.provider(setting.provider).await? {
            None => config_health,
            Some(existing) => reconcile_provider_health_record(existing, config_health, now),
        };
        reconcile_provider_readiness(&mut reconciled, &now);

        repository.save_provider_health(&reconciled).await?;
    }

    Ok(())
}

/// Handles system config from bootstrap for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `config`: `&ServerConfig`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `SystemConfig` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn system_config_from_bootstrap(config: &ServerConfig) -> SystemConfig {
    SystemConfig {
        library_root: config.library_root.to_string_lossy().to_string(),
        dropbox_root: config.dropbox_root.to_string_lossy().to_string(),
        podcast_subtree: "Podcasts".to_string(),
        transcode_concurrency_limit: config.transcode_concurrency_limit,
        scan_thread_count: config.scan_thread_count,
        updated_at: Utc::now(),
    }
}

/// Handles provider setting seeds from bootstrap for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `config`: `&ServerConfig`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Vec<ProviderSettingSeed>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn provider_setting_seeds_from_bootstrap(config: &ServerConfig) -> Vec<ProviderSettingSeed> {
    ProviderKind::all()
        .iter()
        .copied()
        .map(|provider| {
            let provider_config = config
                .providers
                .get(&provider)
                .cloned()
                .unwrap_or_else(|| ProviderConfig::default_for(provider));
            ProviderSettingSeed {
                provider,
                enabled: provider_config.enabled,
                requires_api_key: provider_config.requires_api_key,
                api_key_configured: provider_config.api_key_configured
                    || provider_config.api_key.is_some(),
                api_key_secret: provider_config.api_key,
            }
        })
        .collect()
}

/// Handles provider setting health for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `setting`: `&ProviderSetting`; expected to be a value satisfying the type contract shown in the function signature.
/// - `now`: `chrono:DateTime<Utc>`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ProviderHealth` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn provider_setting_health(
    setting: &ProviderSetting,
    now: chrono::DateTime<Utc>,
) -> ProviderHealth {
    let mut health = ProviderHealth::healthy(setting.provider, now);
    health.enabled = setting.enabled;
    health.api_key_configured = setting.api_key_configured;
    health.updated_at = now;

    if !setting.enabled {
        health.status = ProviderStatus::Disabled;
        health.maintenance_ready = false;
        health.last_success_at = None;
        health.message = Some("Provider is disabled in provider settings.".into());
        return health;
    }

    if setting.requires_api_key && !setting.api_key_configured {
        health.status = ProviderStatus::Unconfigured;
        health.maintenance_ready = false;
        health.last_success_at = None;
        health.message =
            Some("Configure an API key in provider settings to enable provider repairs.".into());
        return health;
    }

    health.status = ProviderStatus::Healthy;
    health.maintenance_ready = true;
    health
}

/// Reconciles state for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `existing`: `ProviderHealth`; expected to be a value satisfying the type contract shown in the function signature.
/// - `config_health`: `ProviderHealth`; expected to be a value satisfying the type contract shown in the function signature.
/// - `now`: `chrono:DateTime<Utc>`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ProviderHealth` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn reconcile_provider_health_record(
    mut existing: ProviderHealth,
    config_health: ProviderHealth,
    now: chrono::DateTime<Utc>,
) -> ProviderHealth {
    if should_config_override_health(&config_health) {
        let mut config_health = config_health;
        config_health.failure_count = existing.failure_count;
        config_health.last_failure_at = existing.last_failure_at;
        return config_health;
    }

    existing.enabled = config_health.enabled;
    existing.api_key_configured = config_health.api_key_configured;
    existing.updated_at = now;

    if matches!(
        existing.status,
        ProviderStatus::Disabled | ProviderStatus::Unconfigured
    ) {
        existing.status = ProviderStatus::Healthy;
        existing.maintenance_ready = true;
        existing.retry_after = None;
        existing.message = None;
        existing.last_success_at.get_or_insert(now);
    }

    existing
}

/// Handles should config override health for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `health`: `&ProviderHealth`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn should_config_override_health(health: &ProviderHealth) -> bool {
    matches!(
        health.status,
        ProviderStatus::Disabled | ProviderStatus::Unconfigured
    )
}

/// Verifies that api storage error.
///
/// Inputs:
/// - `error`: `StorageError`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ApiError` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn api_storage_error(error: StorageError) -> ApiError {
    tracing::error!(%error, "maintenance persistence operation failed");
    ApiError::Internal
}

/// Verifies that api catalog browse error.
///
/// Inputs:
/// - `error`: `CatalogBrowseError`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ApiError` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn api_catalog_browse_error(error: CatalogBrowseError) -> ApiError {
    match error {
        CatalogBrowseError::Storage(error) => api_storage_error(error),
        CatalogBrowseError::InvalidLimit
        | CatalogBrowseError::InvalidSort { .. }
        | CatalogBrowseError::InvalidCursor
        | CatalogBrowseError::CursorSortMismatch { .. } => {
            ApiError::BadRequest(error.to_string())
        }
    }
}

/// Verifies that api catalog search error.
///
/// Inputs:
/// - `error`: `CatalogSearchError`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ApiError` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn api_catalog_search_error(error: CatalogSearchError) -> ApiError {
    match error {
        CatalogSearchError::Storage(error) => api_storage_error(error),
        CatalogSearchError::MissingQuery
        | CatalogSearchError::EmptyQuery
        | CatalogSearchError::InvalidLimit
        | CatalogSearchError::EmptyFilter { .. }
        | CatalogSearchError::InvalidMediaType { .. } => ApiError::BadRequest(error.to_string()),
    }
}

/// Verifies that api quarantine retry error.
///
/// Inputs:
/// - `error`: `QuarantineRetryError`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ApiError` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn api_quarantine_retry_error(error: QuarantineRetryError) -> ApiError {
    match error {
        QuarantineRetryError::Storage(error) => api_storage_error(error),
        QuarantineRetryError::NotFound(item_id) => {
            ApiError::NotFound(format!("quarantine item {item_id} was not found"))
        }
        QuarantineRetryError::NotRetryEligible(item_id) => {
            ApiError::Conflict(format!("quarantine item {item_id} is not retry eligible"))
        }
        QuarantineRetryError::TerminalStatus { item_id, status } => {
            ApiError::Conflict(format!("quarantine item {item_id} is already {status:?}"))
        }
        QuarantineRetryError::MissingPreparedWork(item_id) => {
            tracing::error!(%item_id, "quarantine retry transaction missing prepared work");
            ApiError::Internal
        }
    }
}

/// Verifies that api import pipeline error.
///
/// Inputs:
/// - `error`: `ImportPipelineError`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ApiError` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn api_import_pipeline_error(error: ImportPipelineError) -> ApiError {
    match error {
        ImportPipelineError::Storage(error) => api_storage_error(error),
        ImportPipelineError::JobNotFound(job_id) => {
            ApiError::NotFound(format!("import job {job_id} was not found"))
        }
        ImportPipelineError::JobNotRunnable { job_id, status } => {
            ApiError::Conflict(format!("import job {job_id} is already {status:?}"))
        }
        ImportPipelineError::InvalidScope(message) => ApiError::BadRequest(message),
        ImportPipelineError::MediaProbe(error) => {
            tracing::error!(%error, "media probing failed before quarantine could be created");
            ApiError::Internal
        }
        ImportPipelineError::FileOperation(error) => {
            tracing::error!(%error, "file operation failed before quarantine could be created");
            ApiError::Internal
        }
        ImportPipelineError::ScanTaskJoin(error) => {
            tracing::error!(%error, "import scan task failed");
            ApiError::Internal
        }
    }
}

/// Handles contains parent dir for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn contains_parent_dir(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::ParentDir))
}

/// Normalizes caller-provided data for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `field`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn normalize_root_path(value: &str, field: &str) -> Result<String, ApiError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ApiError::BadRequest(format!("{field} cannot be empty")));
    }
    if value.chars().any(char::is_control) {
        return Err(ApiError::BadRequest(format!(
            "{field} cannot contain control characters"
        )));
    }

    let path = Path::new(value);
    if !path.is_absolute() {
        return Err(ApiError::BadRequest(format!("{field} must be an absolute path")));
    }
    if contains_parent_dir(path) {
        return Err(ApiError::BadRequest(format!(
            "{field} cannot contain parent-directory traversal"
        )));
    }

    Ok(value.to_string())
}

/// Normalizes caller-provided data for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn normalize_podcast_subtree(value: &str) -> Result<String, ApiError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ApiError::BadRequest(
            "podcast_subtree cannot be empty".into(),
        ));
    }
    if value.chars().any(char::is_control) {
        return Err(ApiError::BadRequest(
            "podcast_subtree cannot contain control characters".into(),
        ));
    }

    let path = Path::new(value);
    if path.is_absolute() {
        return Err(ApiError::BadRequest(
            "podcast_subtree must be relative to library_root".into(),
        ));
    }
    if contains_parent_dir(path) {
        return Err(ApiError::BadRequest(
            "podcast_subtree cannot contain parent-directory traversal".into(),
        ));
    }

    Ok(value.to_string())
}

/// Normalizes caller-provided data for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `value`: `i32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `i32` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn normalize_transcode_concurrency_limit(value: i32) -> Result<i32, ApiError> {
    if value < 0 {
        return Err(ApiError::BadRequest(
            "transcode_concurrency_limit must be non-negative".into(),
        ));
    }

    Ok(value)
}

/// Normalizes caller-provided data for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `value`: `i32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `i32` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn normalize_scan_thread_count(value: i32) -> Result<i32, ApiError> {
    if value <= 0 {
        return Err(ApiError::BadRequest(
            "scan_thread_count must be positive".into(),
        ));
    }

    Ok(value)
}

/// Normalizes caller-provided data for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `field`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn normalize_secret(value: &str, field: &str) -> Result<String, ApiError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ApiError::BadRequest(format!("{field} cannot be empty")));
    }
    if value.chars().any(char::is_control) {
        return Err(ApiError::BadRequest(format!(
            "{field} cannot contain control characters"
        )));
    }

    Ok(value.to_string())
}

/// Handles provider requires api key for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn provider_requires_api_key(provider: ProviderKind) -> bool {
    matches!(
        provider,
        ProviderKind::Discogs | ProviderKind::FanartTv | ProviderKind::TheAudioDb
    )
}

/// Handles provider env fragment for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn provider_env_fragment(provider: ProviderKind) -> String {
    provider.api_name().to_ascii_uppercase()
}

/// Handles env value for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `name`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(String)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn env_value(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Handles env bool for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `name`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(bool)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn env_bool(name: &str) -> Option<bool> {
    let value = env::var(name).ok()?;
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" | "enabled" => Some(true),
        "0" | "false" | "no" | "off" | "disabled" => Some(false),
        _ => None,
    }
}

/// Handles env nonnegative i32 for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `name`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `default`: `i32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `i32` on success or `ServerConfigError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ServerConfigError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn env_nonnegative_i32(name: &str, default: i32) -> Result<i32, ServerConfigError> {
    match env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<i32>()
            .ok()
            .filter(|value| *value >= 0)
            .ok_or(ServerConfigError::InvalidTranscodeConcurrencyLimit),
        Err(_) => Ok(default),
    }
}

/// Handles env positive i32 for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `name`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `default`: `i32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `i32` on success or `ServerConfigError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ServerConfigError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn env_positive_i32(name: &str, default: i32) -> Result<i32, ServerConfigError> {
    match env::var(name) {
        Ok(value) => value
            .trim()
            .parse::<i32>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or(ServerConfigError::InvalidScanThreadCount),
        Err(_) => Ok(default),
    }
}

/// Normalizes caller-provided data for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `username`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn normalize_username(username: &str) -> Result<String, ApiError> {
    let username = username.trim();
    if username.is_empty() {
        return Err(ApiError::BadRequest("username cannot be empty".into()));
    }
    if username.contains(':') {
        return Err(ApiError::BadRequest(
            "username cannot contain colon characters".into(),
        ));
    }
    if username.chars().any(char::is_control) {
        return Err(ApiError::BadRequest(
            "username cannot contain control characters".into(),
        ));
    }

    Ok(username.to_string())
}

/// Normalizes caller-provided data for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `field`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn normalize_name(value: &str, field: &str) -> Result<String, ApiError> {
    let value = value.trim();
    if value.is_empty() {
        return Err(ApiError::BadRequest(format!("{field} cannot be empty")));
    }
    if value.chars().any(char::is_control) {
        return Err(ApiError::BadRequest(format!(
            "{field} cannot contain control characters"
        )));
    }

    Ok(value.to_string())
}

/// Normalizes caller-provided data for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `value`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(String)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Validates data for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `password`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `()` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn validate_password(password: &str) -> Result<(), ApiError> {
    if password.is_empty() {
        return Err(ApiError::BadRequest("password cannot be empty".into()));
    }

    Ok(())
}

/// Validates data for application state facade used by HTTP handlers and background workers.
///
/// Inputs:
/// - `position_seconds`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
/// - `duration_seconds`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
///
/// Output:
/// - Returns `()` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn validate_progress_seconds(
    position_seconds: u32,
    duration_seconds: Option<u32>,
) -> Result<(), ApiError> {
    if let Some(duration_seconds) = duration_seconds {
        if position_seconds > duration_seconds {
            return Err(ApiError::BadRequest(
                "position_seconds cannot exceed duration_seconds".into(),
            ));
        }
    }

    Ok(())
}
