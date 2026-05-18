use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
    time::Duration,
};

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use sqlx::{
    postgres::{PgConnectOptions, PgConnection, PgPoolOptions, PgRow},
    types::Json,
    ConnectOptions, Connection, Executor, PgPool, Row,
};
use thiserror::Error;
use uuid::Uuid;

use crate::{
    catalog::normalize_catalog_text,
    domain::{
        AccountRole, CatalogMutationPolicy, ImportJob, ImportJobKind, ImportJobSource,
        ImportJobStatus, LocalAccount, MaintenanceScope, MediaFileStatus,
        PlaybackContextType, PlaybackHistoryEvent, PlaybackItemType, PlaybackProgress, Playlist,
        PlaylistItem, PlaylistScope, ProviderHealth, ProviderKind, ProviderSetting, ProviderStatus,
        QuarantineItem, QuarantineReason, QuarantineStatus, RepairPlan, SystemConfig,
    },
    pipeline::ImportWorkRequest,
    providers::ProviderCredential,
};

const SYSTEM_CONFIG_SELECT: &str = r#"
    library_root,
    dropbox_root,
    podcast_subtree,
    public_base_url,
    transcode_concurrency_limit,
    scan_thread_count,
    updated_at
"#;

const LOCAL_ACCOUNT_SELECT: &str = r#"
    id,
    username,
    password_hash,
    role::text AS role,
    disabled,
    created_at,
    updated_at
"#;

const IMPORT_JOB_SELECT: &str = r#"
    id,
    kind::text AS kind,
    status::text AS status,
    scope,
    repair_plan,
    catalog_mutation_policy,
    provider_filter::text[] AS provider_filter,
    pipeline,
    source,
    reason,
    related_quarantine_item_id,
    idempotency_key,
    attempts,
    created_at,
    updated_at
"#;

const IMPORT_JOB_SELECT_FROM_JOIN: &str = r#"
    j.id,
    j.kind::text AS kind,
    j.status::text AS status,
    j.scope,
    j.repair_plan,
    j.catalog_mutation_policy,
    j.provider_filter::text[] AS provider_filter,
    j.pipeline,
    j.source,
    j.reason,
    j.related_quarantine_item_id,
    j.idempotency_key,
    j.attempts,
    j.created_at,
    j.updated_at
"#;

const PROVIDER_HEALTH_SELECT: &str = r#"
    provider::text AS provider,
    enabled,
    status::text AS status,
    api_key_configured,
    maintenance_ready,
    failure_count,
    retry_after,
    last_success_at,
    last_failure_at,
    message,
    updated_at
"#;

const PROVIDER_SETTING_SELECT: &str = r#"
    provider::text AS provider,
    enabled,
    requires_api_key,
    (api_key_configured OR NULLIF(btrim(api_key_secret), '') IS NOT NULL) AS api_key_configured,
    updated_at
"#;

const QUARANTINE_ITEM_SELECT: &str = r#"
    id,
    media_file_id,
    source_path,
    reason::text AS reason,
    status::text AS status,
    retry_count,
    retry_eligible,
    last_import_job_id,
    admin_note,
    created_at,
    updated_at
"#;

const PLAYLIST_SELECT: &str = r#"
    id,
    name,
    description,
    scope::text AS scope,
    owner_account_id,
    created_by_account_id,
    updated_by_account_id,
    created_at,
    updated_at
"#;

const PLAYLIST_ITEM_SELECT: &str = r#"
    id,
    playlist_id,
    item_type::text AS item_type,
    item_id,
    position,
    added_by_account_id,
    created_at
"#;

const PLAYLIST_ELIGIBLE_MEDIA_FILE_PREDICATE: &str = r#"
    mf.status = 'published'
    AND mf.published_at IS NOT NULL
    AND mf.duplicate_of_media_file_id IS NULL
    AND NOT EXISTS (
      SELECT 1
      FROM quarantine_items qi
      WHERE qi.media_file_id = mf.id
        AND qi.status IN ('open', 'retrying')
    )
"#;

const PLAYBACK_PROGRESS_SELECT: &str = r#"
    item_type::text AS item_type,
    item_id,
    context_type::text AS context_type,
    context_id,
    position_seconds,
    duration_seconds,
    completed,
    updated_at
"#;

const PLAYBACK_HISTORY_SELECT: &str = r#"
    id,
    item_type::text AS item_type,
    item_id,
    context_type::text AS context_type,
    context_id,
    position_seconds,
    duration_seconds,
    completed,
    played_at
"#;

#[derive(Debug, Clone)]
/// Represents database config in the Postgres configuration, migrations, and maintenance repository persistence.
///
/// Functionality: Carries fields `url`, `max_connections`, `connect_timeout`, `schema` for Postgres configuration, migrations, and maintenance repository persistence.
/// Dependencies: depends on `String`, `u32`, `Duration`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/storage.rs`, `tests/maintenance_api.rs`.
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub connect_timeout: Duration,
    pub schema: Option<String>,
}

#[derive(Debug, Error)]
/// Represents config error in the Postgres configuration, migrations, and maintenance repository persistence.
///
/// Functionality: Enumerates `MissingDatabaseUrl`, `InvalidMaxConnections`, `InvalidConnectTimeout`, `InvalidSchema` states or choices for Postgres configuration, migrations, and maintenance repository persistence.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/storage.rs`.
pub enum ConfigError {
    #[error("Postgres is required: set HARMONIXIA_DATABASE_URL or DATABASE_URL")]
    MissingDatabaseUrl,
    #[error("HARMONIXIA_DATABASE_MAX_CONNECTIONS must be a positive integer")]
    InvalidMaxConnections,
    #[error("HARMONIXIA_DATABASE_CONNECT_TIMEOUT_SECONDS must be a positive integer")]
    InvalidConnectTimeout,
    #[error("invalid Postgres schema `{0}`; use letters, numbers, and underscores")]
    InvalidSchema(String),
}

#[derive(Debug, Error)]
/// Represents storage error in the Postgres configuration, migrations, and maintenance repository persistence.
///
/// Functionality: Enumerates `Sqlx`, `Migration`, `SchemaVerification`, `InvalidStoredValue` states or choices for Postgres configuration, migrations, and maintenance repository persistence.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/catalog.rs`, `src/pipeline.rs`, `src/state.rs`, `src/storage.rs`.
pub enum StorageError {
    #[error("database operation failed: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("database migration failed: {0}")]
    Migration(String),
    #[error("database schema verification failed: {0}")]
    SchemaVerification(String),
    #[error("database row contains invalid {field}: {value}")]
    InvalidStoredValue {
        field: &'static str,
        value: String,
    },
    #[error("cannot delete the last enabled admin account")]
    LastEnabledAdmin,
}

#[derive(Debug, Error)]
/// Represents quarantine retry error in the Postgres configuration, migrations, and maintenance repository persistence.
///
/// Functionality: Enumerates `Storage`, `NotFound`, `NotRetryEligible`, `TerminalStatus` states or choices for Postgres configuration, migrations, and maintenance repository persistence.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/storage.rs`.
pub enum QuarantineRetryError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("quarantine item {0} was not found")]
    NotFound(Uuid),
    #[error("quarantine item {0} is not retry eligible")]
    NotRetryEligible(Uuid),
    #[error("quarantine item {item_id} is already {status:?}")]
    TerminalStatus {
        item_id: Uuid,
        status: QuarantineStatus,
    },
    #[error("retry work was not prepared for quarantine item {0}")]
    MissingPreparedWork(Uuid),
}

#[derive(Debug, Clone)]
/// Represents quarantine retry work in the Postgres configuration, migrations, and maintenance repository persistence.
///
/// Functionality: Carries fields `item_id`, `work` for Postgres configuration, migrations, and maintenance repository persistence.
/// Dependencies: depends on `Uuid`, `ImportWorkRequest` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/storage.rs`.
pub struct QuarantineRetryWork {
    pub item_id: Uuid,
    pub work: ImportWorkRequest,
}

#[derive(Debug, Clone)]
/// Represents provider setting seed in the Postgres configuration, migrations, and maintenance repository persistence.
///
/// Functionality: Carries fields `provider`, `enabled`, `requires_api_key`, `api_key_configured`, `api_key_secret` for Postgres configuration, migrations, and maintenance repository persistence.
/// Dependencies: depends on `ProviderKind`, `bool`, `bool`, `bool`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/storage.rs`.
pub struct ProviderSettingSeed {
    pub provider: ProviderKind,
    pub enabled: bool,
    pub requires_api_key: bool,
    pub api_key_configured: bool,
    pub api_key_secret: Option<String>,
}

#[derive(Debug, Clone, Copy)]
/// Represents admin dashboard operational counts in the Postgres configuration, migrations, and maintenance repository persistence.
///
/// Functionality: Carries fields `scanning`, `quarantined`, `failed` for Postgres configuration, migrations, and maintenance repository persistence.
/// Dependencies: depends on `i64`, `i64`, `i64` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/storage.rs`.
pub struct AdminDashboardOperationalCounts {
    pub scanning: i64,
    pub quarantined: i64,
    pub failed: i64,
}

#[derive(Debug, Clone, Copy)]
/// Represents import job progress counts in the Postgres configuration, migrations, and maintenance repository persistence.
///
/// Functionality: Carries fields `processed_files`, `published_files`, `quarantined_files`, `failed_files`, `last_progress_at` for Postgres configuration, migrations, and maintenance repository persistence.
/// Dependencies: depends on `i64`, `i64`, `i64`, `i64`, `Option<DateTime<Utc>>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/storage.rs`.
pub struct ImportJobProgressCounts {
    pub processed_files: i64,
    pub published_files: i64,
    pub quarantined_files: i64,
    pub failed_files: i64,
    pub last_progress_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
/// Represents one failed import work item for admin diagnostics.
///
/// Functionality: Carries fields from `catalog_import_work_items` plus import job context for Postgres-backed maintenance reporting.
/// Dependencies: depends on `Uuid`, `ImportJobKind`, `ImportJobStatus`, `MediaFileStatus`, `String`, `Option<String>`, `DateTime<Utc>`.
/// Used by: referenced from `src/storage.rs`, `src/state.rs`, `src/api/maintenance.rs`.
pub struct CatalogImportFailure {
    pub id: Uuid,
    pub import_job_id: Uuid,
    pub import_job_kind: ImportJobKind,
    pub import_job_status: ImportJobStatus,
    pub source_path: String,
    pub media_file_id: Option<Uuid>,
    pub status: MediaFileStatus,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
/// Represents playlist item add result in the Postgres configuration, migrations, and maintenance repository persistence.
///
/// Functionality: Enumerates `Added`, `PlaylistNotFound`, `ItemNotEligible`, `InvalidPosition` states or choices for Postgres configuration, migrations, and maintenance repository persistence.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/storage.rs`.
pub enum PlaylistItemAddResult {
    Added(PlaylistItem),
    PlaylistNotFound,
    ItemNotEligible,
    InvalidPosition,
}

#[derive(Debug, Clone)]
/// Represents playlist item list result in the Postgres configuration, migrations, and maintenance repository persistence.
///
/// Functionality: Enumerates `Items`, `PlaylistNotFound` states or choices for Postgres configuration, migrations, and maintenance repository persistence.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/storage.rs`.
pub enum PlaylistItemListResult {
    Items(Vec<PlaylistItem>),
    PlaylistNotFound,
}

#[derive(Debug, Clone)]
/// Represents playlist item remove result in the Postgres configuration, migrations, and maintenance repository persistence.
///
/// Functionality: Enumerates `Removed`, `PlaylistNotFound` states or choices for Postgres configuration, migrations, and maintenance repository persistence.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/storage.rs`.
pub enum PlaylistItemRemoveResult {
    Removed,
    PlaylistNotFound,
}

#[derive(Debug, Clone)]
/// Represents playlist item reorder result in the Postgres configuration, migrations, and maintenance repository persistence.
///
/// Functionality: Enumerates `Reordered`, `PlaylistNotFound`, `ItemSetMismatch` states or choices for Postgres configuration, migrations, and maintenance repository persistence.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/state.rs`, `src/storage.rs`.
pub enum PlaylistItemReorderResult {
    Reordered(Vec<PlaylistItem>),
    PlaylistNotFound,
    ItemSetMismatch,
}

#[derive(Debug, Clone)]
/// Represents pg maintenance repository in the Postgres configuration, migrations, and maintenance repository persistence.
///
/// Functionality: Carries fields `pool` for Postgres configuration, migrations, and maintenance repository persistence.
/// Dependencies: depends on `PgPool` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/catalog.rs`, `src/pipeline.rs`, `src/state.rs`, `src/storage.rs`.
pub struct PgMaintenanceRepository {
    pub(crate) pool: PgPool,
}

impl DatabaseConfig {
    /// Builds configuration from environment variables for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `Self` on success or `ConfigError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ConfigError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub fn from_env() -> Result<Self, ConfigError> {
        let url = std::env::var("HARMONIXIA_DATABASE_URL")
            .or_else(|_| std::env::var("DATABASE_URL"))
            .map_err(|_| ConfigError::MissingDatabaseUrl)?;

        let max_connections = match std::env::var("HARMONIXIA_DATABASE_MAX_CONNECTIONS") {
            Ok(value) => value
                .parse::<u32>()
                .ok()
                .filter(|value| *value > 0)
                .ok_or(ConfigError::InvalidMaxConnections)?,
            Err(_) => 15,
        };

        let connect_timeout = match std::env::var("HARMONIXIA_DATABASE_CONNECT_TIMEOUT_SECONDS") {
            Ok(value) => Duration::from_secs(
                value
                    .parse::<u64>()
                    .ok()
                    .filter(|value| *value > 0)
                    .ok_or(ConfigError::InvalidConnectTimeout)?,
            ),
            Err(_) => Duration::from_secs(5),
        };

        let schema = std::env::var("HARMONIXIA_DATABASE_SCHEMA")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .map(|value| {
                let schema = value.trim().to_string();
                validate_schema_name(&schema)?;
                Ok(schema)
            })
            .transpose()?;

        Ok(Self {
            url,
            max_connections,
            connect_timeout,
            schema,
        })
    }
}

impl PgMaintenanceRepository {
    /// Connects to persistence and initializes runtime state for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - `config`: `&DatabaseConfig`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn connect(config: &DatabaseConfig) -> Result<Self, StorageError> {
        if let Some(schema) = &config.schema {
            validate_schema_name(schema).map_err(|error| {
                StorageError::SchemaVerification(format!(
                    "invalid configured database schema: {error}"
                ))
            })?;
        }

        let options = PgConnectOptions::from_str(&config.url)?
            .application_name("harmonixia-server")
            .log_statements(tracing::log::LevelFilter::Debug);

        if let Some(schema) = &config.schema {
            let mut connection = PgConnection::connect_with(&options).await?;
            let create_schema = format!("CREATE SCHEMA IF NOT EXISTS {}", quote_identifier(schema));
            connection.execute(create_schema.as_str()).await?;
        }

        let schema = config.schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(config.connect_timeout)
            .after_connect(move |connection, _meta| {
                let schema = schema.clone();
                Box::pin(async move {
                    if let Some(schema) = schema {
                        let set_path = format!(
                            "SET search_path TO {}, public",
                            quote_identifier(&schema)
                        );
                        connection.execute(set_path.as_str()).await?;
                    }
                    Ok(())
                })
            })
            .connect_with(options)
            .await?;

        let repository = Self { pool };
        repository.apply_migrations().await?;
        repository.verify_schema().await?;
        Ok(repository)
    }

    /// Constructs a new instance for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - `pool`: `PgPool`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Handles pool for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&PgPool` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Applies derived state for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `()` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn apply_migrations(&self) -> Result<(), StorageError> {
        sqlx::migrate!("./migrations")
            .run(&self.pool)
            .await
            .map_err(|error| StorageError::Migration(error.to_string()))?;
        Ok(())
    }

    /// Verifies security-sensitive data for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `()` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn verify_schema(&self) -> Result<(), StorageError> {
        for table in [
            "system_config",
            "provider_settings",
            "local_accounts",
            "import_jobs",
            "provider_health",
            "artists",
            "albums",
            "tracks",
            "podcasts",
            "episodes",
            "media_files",
            "artwork_assets",
            "metadata_provider_links",
            "metadata_provenance",
            "catalog_search_projection",
            "catalog_import_work_items",
            "playlists",
            "playlist_items",
            "playback_progress",
            "playback_history_events",
            "quarantine_items",
        ] {
            let exists: bool = sqlx::query_scalar(
                r#"
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                      AND table_name = $1
                )
                "#,
            )
            .bind(table)
            .fetch_one(&self.pool)
            .await?;

            if !exists {
                return Err(StorageError::SchemaVerification(format!(
                    "required table `{table}` is missing after migrations"
                )));
            }
        }

        Ok(())
    }

    /// Loads persisted state for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `defaults`: `&SystemConfig`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `SystemConfig` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn load_or_initialize_system_config(
        &self,
        defaults: &SystemConfig,
    ) -> Result<SystemConfig, StorageError> {
        sqlx::query(
            r#"
            INSERT INTO system_config (
                id,
                library_root,
                dropbox_root,
                podcast_subtree,
                public_base_url,
                transcode_concurrency_limit,
                scan_thread_count,
                updated_at
            )
            VALUES (1, $1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO NOTHING
            "#,
        )
        .bind(defaults.library_root.as_str())
        .bind(defaults.dropbox_root.as_str())
        .bind(defaults.podcast_subtree.as_str())
        .bind(defaults.public_base_url.as_deref())
        .bind(defaults.transcode_concurrency_limit)
        .bind(defaults.scan_thread_count)
        .bind(defaults.updated_at)
        .execute(&self.pool)
        .await?;

        self.system_config().await?.ok_or_else(|| {
            StorageError::SchemaVerification(
                "system_config row was not initialized after migration".into(),
            )
        })
    }

    /// Handles system config for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Option<SystemConfig>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn system_config(&self) -> Result<Option<SystemConfig>, StorageError> {
        let sql = format!("SELECT {SYSTEM_CONFIG_SELECT} FROM system_config WHERE id = 1");
        let row = sqlx::query(&sql).fetch_optional(&self.pool).await?;
        row.as_ref().map(system_config_from_row).transpose()
    }

    /// Persists state for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `config`: `&SystemConfig`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `SystemConfig` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn save_system_config(
        &self,
        config: &SystemConfig,
    ) -> Result<SystemConfig, StorageError> {
        let sql = format!(
            r#"
            INSERT INTO system_config (
                id,
                library_root,
                dropbox_root,
                podcast_subtree,
                public_base_url,
                transcode_concurrency_limit,
                scan_thread_count,
                updated_at
            )
            VALUES (1, $1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO UPDATE SET
                library_root = EXCLUDED.library_root,
                dropbox_root = EXCLUDED.dropbox_root,
                podcast_subtree = EXCLUDED.podcast_subtree,
                public_base_url = EXCLUDED.public_base_url,
                transcode_concurrency_limit = EXCLUDED.transcode_concurrency_limit,
                scan_thread_count = EXCLUDED.scan_thread_count,
                updated_at = EXCLUDED.updated_at
            RETURNING {SYSTEM_CONFIG_SELECT}
            "#
        );
        let row = sqlx::query(&sql)
            .bind(config.library_root.as_str())
            .bind(config.dropbox_root.as_str())
            .bind(config.podcast_subtree.as_str())
            .bind(config.public_base_url.as_deref())
            .bind(config.transcode_concurrency_limit)
            .bind(config.scan_thread_count)
            .bind(config.updated_at)
            .fetch_one(&self.pool)
            .await?;

        system_config_from_row(&row)
    }

    /// Loads persisted state for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `defaults`: `&[ProviderSettingSeed]`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Vec<ProviderSetting>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn load_or_initialize_provider_settings(
        &self,
        defaults: &[ProviderSettingSeed],
    ) -> Result<Vec<ProviderSetting>, StorageError> {
        for default in defaults {
            sqlx::query(
                r#"
                INSERT INTO provider_settings (
                    provider,
                    enabled,
                    requires_api_key,
                    api_key_configured,
                    api_key_secret,
                    updated_at
                )
                VALUES (
                    $1::text::provider_kind,
                    $2,
                    $3,
                    $4,
                    $5,
                    $6
                )
                ON CONFLICT (provider) DO NOTHING
                "#,
            )
            .bind(default.provider.api_name())
            .bind(default.enabled)
            .bind(default.requires_api_key)
            .bind(default.api_key_configured)
            .bind(default.api_key_secret.as_deref())
            .bind(Utc::now())
            .execute(&self.pool)
            .await?;
        }

        self.provider_settings().await
    }

    /// Handles provider settings for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Vec<ProviderSetting>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn provider_settings(&self) -> Result<Vec<ProviderSetting>, StorageError> {
        let sql = format!(
            "SELECT {PROVIDER_SETTING_SELECT} FROM provider_settings ORDER BY provider ASC"
        );
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        rows.iter().map(provider_setting_from_row).collect()
    }

    /// Handles provider setting for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    ///
    /// Output:
    /// - Returns `Option<ProviderSetting>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn provider_setting(
        &self,
        provider: ProviderKind,
    ) -> Result<Option<ProviderSetting>, StorageError> {
        let sql = format!(
            "SELECT {PROVIDER_SETTING_SELECT} FROM provider_settings WHERE provider = $1::text::provider_kind"
        );
        let row = sqlx::query(&sql)
            .bind(provider.api_name())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(provider_setting_from_row).transpose()
    }

    /// Handles provider credentials for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Vec<ProviderCredential>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn provider_credentials(&self) -> Result<Vec<ProviderCredential>, StorageError> {
        let rows = sqlx::query(
            r#"
            SELECT
                provider::text AS provider,
                NULLIF(btrim(api_key_secret), '') AS api_key_secret
            FROM provider_settings
            ORDER BY provider ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(|row| {
                let provider = parse_provider_kind(row.try_get::<String, _>("provider")?)?;
                Ok(ProviderCredential::new(
                    provider,
                    row.try_get::<Option<String>, _>("api_key_secret")?,
                    None,
                ))
            })
            .collect()
    }

    /// Updates existing state for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    /// - `enabled`: `Option<bool>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
    /// - `api_key_secret`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `clear_api_key`: `bool`; expected to be a boolean flag controlling the documented branch.
    ///
    /// Output:
    /// - Returns `Option<ProviderSetting>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn update_provider_setting(
        &self,
        provider: ProviderKind,
        enabled: Option<bool>,
        api_key_secret: Option<&str>,
        clear_api_key: bool,
    ) -> Result<Option<ProviderSetting>, StorageError> {
        let sql = format!(
            r#"
            UPDATE provider_settings
            SET enabled = COALESCE($2, enabled),
                api_key_secret = CASE
                    WHEN $4 THEN NULL
                    WHEN $3::text IS NOT NULL THEN NULLIF(btrim($3), '')
                    ELSE api_key_secret
                END,
                api_key_configured = CASE
                    WHEN $4 THEN false
                    WHEN $3::text IS NOT NULL THEN NULLIF(btrim($3), '') IS NOT NULL
                    ELSE api_key_configured
                END,
                updated_at = $5
            WHERE provider = $1::text::provider_kind
            RETURNING {PROVIDER_SETTING_SELECT}
            "#
        );
        let row = sqlx::query(&sql)
            .bind(provider.api_name())
            .bind(enabled)
            .bind(api_key_secret)
            .bind(clear_api_key)
            .bind(Utc::now())
            .fetch_optional(&self.pool)
            .await?;

        row.as_ref().map(provider_setting_from_row).transpose()
    }

    /// Creates a new resource for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `username`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `password_hash`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `role`: `AccountRole`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `LocalAccount` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn create_local_account(
        &self,
        username: &str,
        password_hash: &str,
        role: AccountRole,
    ) -> Result<LocalAccount, StorageError> {
        let now = Utc::now();
        let id = Uuid::new_v4();
        let sql = format!(
            r#"
            INSERT INTO local_accounts (
                id,
                username,
                password_hash,
                role,
                disabled,
                created_at,
                updated_at
            )
            VALUES (
                $1,
                $2,
                $3,
                $4::text::account_role,
                false,
                $5,
                $5
            )
            RETURNING {LOCAL_ACCOUNT_SELECT}
            "#
        );

        let row = sqlx::query(&sql)
            .bind(id)
            .bind(username)
            .bind(password_hash)
            .bind(account_role_name(role))
            .bind(now)
            .fetch_one(&self.pool)
            .await?;

        local_account_from_row(&row)
    }

    /// Creates a new resource for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `username`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `password_hash`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Option<LocalAccount>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn create_first_admin_if_no_accounts(
        &self,
        username: &str,
        password_hash: &str,
    ) -> Result<Option<LocalAccount>, StorageError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("LOCK TABLE local_accounts IN EXCLUSIVE MODE")
            .execute(&mut *transaction)
            .await?;

        let account_count: i64 = sqlx::query_scalar("SELECT count(*) FROM local_accounts")
            .fetch_one(&mut *transaction)
            .await?;
        if account_count > 0 {
            return Ok(None);
        }

        let now = Utc::now();
        let id = Uuid::new_v4();
        let sql = format!(
            r#"
            INSERT INTO local_accounts (
                id,
                username,
                password_hash,
                role,
                disabled,
                created_at,
                updated_at
            )
            VALUES (
                $1,
                $2,
                $3,
                'admin',
                false,
                $4,
                $4
            )
            RETURNING {LOCAL_ACCOUNT_SELECT}
            "#
        );

        let row = sqlx::query(&sql)
            .bind(id)
            .bind(username)
            .bind(password_hash)
            .bind(now)
            .fetch_one(&mut *transaction)
            .await?;
        let account = local_account_from_row(&row)?;

        transaction.commit().await?;
        Ok(Some(account))
    }

    /// Handles local account count for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `i64` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn local_account_count(&self) -> Result<i64, StorageError> {
        let count = sqlx::query_scalar("SELECT count(*) FROM local_accounts")
            .fetch_one(&self.pool)
            .await?;
        Ok(count)
    }

    /// Handles local account by id for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<LocalAccount>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn local_account_by_id(
        &self,
        account_id: Uuid,
    ) -> Result<Option<LocalAccount>, StorageError> {
        let sql = format!(
            r#"
            SELECT {LOCAL_ACCOUNT_SELECT}
            FROM local_accounts
            WHERE id = $1
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(account_id)
            .fetch_optional(&self.pool)
            .await?;

        row.as_ref().map(local_account_from_row).transpose()
    }

    /// Handles local accounts for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Vec<LocalAccount>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn local_accounts(&self) -> Result<Vec<LocalAccount>, StorageError> {
        let sql = format!(
            r#"
            SELECT {LOCAL_ACCOUNT_SELECT}
            FROM local_accounts
            ORDER BY lower(username), username, id
            "#
        );
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        rows.iter().map(local_account_from_row).collect()
    }

    /// Handles local account by username for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `username`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Option<LocalAccount>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn local_account_by_username(
        &self,
        username: &str,
    ) -> Result<Option<LocalAccount>, StorageError> {
        let sql = format!(
            r#"
            SELECT {LOCAL_ACCOUNT_SELECT}
            FROM local_accounts
            WHERE lower(username) = lower($1)
              AND disabled = false
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(username)
            .fetch_optional(&self.pool)
            .await?;

        row.as_ref().map(local_account_from_row).transpose()
    }

    /// Updates existing state for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `password_hash`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Option<LocalAccount>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn update_local_account_password(
        &self,
        account_id: Uuid,
        password_hash: &str,
    ) -> Result<Option<LocalAccount>, StorageError> {
        let sql = format!(
            r#"
            UPDATE local_accounts
            SET password_hash = $2,
                updated_at = $3
            WHERE id = $1
            RETURNING {LOCAL_ACCOUNT_SELECT}
            "#
        );
        let row = sqlx::query(&sql)
            .bind(account_id)
            .bind(password_hash)
            .bind(Utc::now())
            .fetch_optional(&self.pool)
            .await?;

        row.as_ref().map(local_account_from_row).transpose()
    }

    /// Deletes or removes a resource from Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<LocalAccount>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn delete_local_account(
        &self,
        account_id: Uuid,
    ) -> Result<Option<LocalAccount>, StorageError> {
        let mut transaction = self.pool.begin().await?;
        sqlx::query("LOCK TABLE local_accounts IN EXCLUSIVE MODE")
            .execute(&mut *transaction)
            .await?;

        let sql = format!(
            r#"
            SELECT {LOCAL_ACCOUNT_SELECT}
            FROM local_accounts
            WHERE id = $1
            FOR UPDATE
            "#
        );
        let Some(row) = sqlx::query(&sql)
            .bind(account_id)
            .fetch_optional(&mut *transaction)
            .await?
        else {
            return Ok(None);
        };
        let account = local_account_from_row(&row)?;

        if account.role.is_admin() && !account.disabled {
            let enabled_admin_count: i64 = sqlx::query_scalar(
                r#"
                SELECT count(*)
                FROM local_accounts
                WHERE role = 'admin'
                  AND disabled = false
                "#,
            )
            .fetch_one(&mut *transaction)
            .await?;
            if enabled_admin_count <= 1 {
                return Err(StorageError::LastEnabledAdmin);
            }
        }

        let personal_playlist_ids: Vec<Uuid> = sqlx::query_scalar(
            r#"
            SELECT id
            FROM playlists
            WHERE owner_account_id = $1
            FOR UPDATE
            "#,
        )
        .bind(account_id)
        .fetch_all(&mut *transaction)
        .await?;
        if !personal_playlist_ids.is_empty() {
            sqlx::query(
                r#"
                DELETE FROM catalog_search_projection
                WHERE entity_type = 'playlist'::catalog_entity_type
                  AND entity_id = ANY($1)
                "#,
            )
            .bind(&personal_playlist_ids)
            .execute(&mut *transaction)
            .await?;
        }

        sqlx::query("DELETE FROM local_accounts WHERE id = $1")
            .bind(account_id)
            .execute(&mut *transaction)
            .await?;
        transaction.commit().await?;

        Ok(Some(account))
    }

    /// Creates a new resource for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `name`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `description`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `scope`: `PlaylistScope`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Playlist` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn create_playlist(
        &self,
        account_id: Uuid,
        name: &str,
        description: Option<&str>,
        scope: PlaylistScope,
    ) -> Result<Playlist, StorageError> {
        let now = Utc::now();
        let id = Uuid::new_v4();
        let owner_account_id = match scope {
            PlaylistScope::Personal => Some(account_id),
            PlaylistScope::Shared => None,
        };
        let mut transaction = self.pool.begin().await?;
        let sql = format!(
            r#"
            INSERT INTO playlists (
                id,
                name,
                description,
                scope,
                owner_account_id,
                created_by_account_id,
                updated_by_account_id,
                created_at,
                updated_at
            )
            VALUES (
                $1,
                $2,
                $3,
                $4::text::playlist_scope,
                $5,
                $6,
                $6,
                $7,
                $7
            )
            RETURNING {PLAYLIST_SELECT}
            "#
        );

        let row = sqlx::query(&sql)
            .bind(id)
            .bind(name)
            .bind(description)
            .bind(playlist_scope_name(scope))
            .bind(owner_account_id)
            .bind(account_id)
            .bind(now)
            .fetch_one(&mut *transaction)
            .await?;

        let playlist = playlist_from_row(&row)?;
        upsert_playlist_search_projection_in_transaction(&mut transaction, &playlist).await?;
        transaction.commit().await?;

        Ok(playlist)
    }

    /// Handles playlists visible to for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Vec<Playlist>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn playlists_visible_to(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<Playlist>, StorageError> {
        let sql = format!(
            r#"
            SELECT {PLAYLIST_SELECT}
            FROM playlists
            WHERE scope = 'shared'
               OR owner_account_id = $1
            ORDER BY updated_at DESC, name ASC, id ASC
            "#
        );
        let rows = sqlx::query(&sql).bind(account_id).fetch_all(&self.pool).await?;
        rows.iter().map(playlist_from_row).collect()
    }

    /// Handles visible playlist for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<Playlist>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn visible_playlist(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
    ) -> Result<Option<Playlist>, StorageError> {
        let sql = format!(
            r#"
            SELECT {PLAYLIST_SELECT}
            FROM playlists
            WHERE id = $1
              AND (scope = 'shared' OR owner_account_id = $2)
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(playlist_id)
            .bind(account_id)
            .fetch_optional(&self.pool)
            .await?;

        row.as_ref().map(playlist_from_row).transpose()
    }

    /// Updates existing state for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `name`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `description`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Option<Playlist>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn update_visible_playlist(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
        name: &str,
        description: Option<&str>,
    ) -> Result<Option<Playlist>, StorageError> {
        let sql = format!(
            r#"
            UPDATE playlists
            SET name = $3,
                description = $4,
                updated_by_account_id = $2,
                updated_at = $5
            WHERE id = $1
              AND (scope = 'shared' OR owner_account_id = $2)
            RETURNING {PLAYLIST_SELECT}
            "#
        );
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(&sql)
            .bind(playlist_id)
            .bind(account_id)
            .bind(name)
            .bind(description)
            .bind(Utc::now())
            .fetch_optional(&mut *transaction)
            .await?;

        let playlist = row.as_ref().map(playlist_from_row).transpose()?;
        if let Some(playlist) = &playlist {
            upsert_playlist_search_projection_in_transaction(&mut transaction, playlist).await?;
        }
        transaction.commit().await?;

        Ok(playlist)
    }

    /// Deletes or removes a resource from Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<Playlist>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn delete_visible_playlist(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
    ) -> Result<Option<Playlist>, StorageError> {
        let sql = format!(
            r#"
            DELETE FROM playlists
            WHERE id = $1
              AND (scope = 'shared' OR owner_account_id = $2)
            RETURNING {PLAYLIST_SELECT}
            "#
        );
        let mut transaction = self.pool.begin().await?;
        let row = sqlx::query(&sql)
            .bind(playlist_id)
            .bind(account_id)
            .fetch_optional(&mut *transaction)
            .await?;

        let playlist = row.as_ref().map(playlist_from_row).transpose()?;
        if playlist.is_some() {
            delete_playlist_search_projection_in_transaction(&mut transaction, playlist_id).await?;
        }
        transaction.commit().await?;

        Ok(playlist)
    }

    /// Lists resources for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `PlaylistItemListResult` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn list_visible_playlist_items(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
    ) -> Result<PlaylistItemListResult, StorageError> {
        let mut transaction = self.pool.begin().await?;
        let Some(_) =
            visible_playlist_for_update_in_transaction(&mut transaction, account_id, playlist_id)
                .await?
        else {
            return Ok(PlaylistItemListResult::PlaylistNotFound);
        };

        cleanup_playlist_items_in_transaction(&mut transaction, playlist_id).await?;
        let items =
            playlist_items_for_playlist_in_transaction(&mut transaction, playlist_id).await?;

        transaction.commit().await?;
        Ok(PlaylistItemListResult::Items(items))
    }

    /// Handles add visible playlist item for Postgres configuration, migrations, and maintenance repository persistence.
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
    /// - Returns `PlaylistItemAddResult` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn add_visible_playlist_item(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
        item_type: PlaybackItemType,
        item_id: Uuid,
        position: Option<u32>,
    ) -> Result<PlaylistItemAddResult, StorageError> {
        let mut transaction = self.pool.begin().await?;
        let Some(_) =
            visible_playlist_for_update_in_transaction(&mut transaction, account_id, playlist_id)
                .await?
        else {
            return Ok(PlaylistItemAddResult::PlaylistNotFound);
        };
        cleanup_playlist_items_in_transaction(&mut transaction, playlist_id).await?;

        if !playlist_catalog_item_is_eligible_in_transaction(
            &mut transaction,
            item_type,
            item_id,
        )
        .await?
        {
            transaction.commit().await?;
            return Ok(PlaylistItemAddResult::ItemNotEligible);
        }

        let item_count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM playlist_items WHERE playlist_id = $1")
                .bind(playlist_id)
                .fetch_one(&mut *transaction)
                .await?;
        let requested_position = position.map(i64::from).unwrap_or(item_count);
        if requested_position > item_count {
            transaction.commit().await?;
            return Ok(PlaylistItemAddResult::InvalidPosition);
        }
        let target_position =
            i32::try_from(requested_position).map_err(|_| StorageError::InvalidStoredValue {
                field: "playlist_items.position",
                value: requested_position.to_string(),
            })?;

        sqlx::query(
            r#"
            UPDATE playlist_items
            SET position = position + 1
            WHERE playlist_id = $1
              AND position >= $2
            "#,
        )
        .bind(playlist_id)
        .bind(target_position)
        .execute(&mut *transaction)
        .await?;

        let now = Utc::now();
        let sql = format!(
            r#"
            INSERT INTO playlist_items (
                id,
                playlist_id,
                item_type,
                item_id,
                position,
                added_by_account_id,
                created_at
            )
            VALUES (
                $1,
                $2,
                $3::text::playback_item_type,
                $4,
                $5,
                $6,
                $7
            )
            RETURNING {PLAYLIST_ITEM_SELECT}
            "#
        );
        let row = sqlx::query(&sql)
            .bind(Uuid::new_v4())
            .bind(playlist_id)
            .bind(playback_item_type_name(item_type))
            .bind(item_id)
            .bind(target_position)
            .bind(account_id)
            .bind(now)
            .fetch_one(&mut *transaction)
            .await?;
        touch_playlist_in_transaction(&mut transaction, playlist_id, account_id).await?;
        let item = playlist_item_from_row(&row)?;

        transaction.commit().await?;
        Ok(PlaylistItemAddResult::Added(item))
    }

    /// Handles remove visible playlist item for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `PlaylistItemRemoveResult` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn remove_visible_playlist_item(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
        playlist_item_id: Uuid,
    ) -> Result<PlaylistItemRemoveResult, StorageError> {
        let mut transaction = self.pool.begin().await?;
        let Some(_) =
            visible_playlist_for_update_in_transaction(&mut transaction, account_id, playlist_id)
                .await?
        else {
            return Ok(PlaylistItemRemoveResult::PlaylistNotFound);
        };

        cleanup_playlist_items_in_transaction(&mut transaction, playlist_id).await?;

        let Some(position) = sqlx::query_scalar::<_, i32>(
            r#"
            SELECT position
            FROM playlist_items
            WHERE playlist_id = $1
              AND id = $2
            FOR UPDATE
            "#,
        )
        .bind(playlist_id)
        .bind(playlist_item_id)
        .fetch_optional(&mut *transaction)
        .await?
        else {
            transaction.commit().await?;
            return Ok(PlaylistItemRemoveResult::PlaylistNotFound);
        };

        sqlx::query("DELETE FROM playlist_items WHERE id = $1")
            .bind(playlist_item_id)
            .execute(&mut *transaction)
            .await?;
        sqlx::query(
            r#"
            UPDATE playlist_items
            SET position = position - 1
            WHERE playlist_id = $1
              AND position > $2
            "#,
        )
        .bind(playlist_id)
        .bind(position)
        .execute(&mut *transaction)
        .await?;
        touch_playlist_in_transaction(&mut transaction, playlist_id, account_id).await?;

        transaction.commit().await?;
        Ok(PlaylistItemRemoveResult::Removed)
    }

    /// Handles reorder visible playlist items for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `playlist_item_ids`: `Vec<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `PlaylistItemReorderResult` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn reorder_visible_playlist_items(
        &self,
        account_id: Uuid,
        playlist_id: Uuid,
        playlist_item_ids: Vec<Uuid>,
    ) -> Result<PlaylistItemReorderResult, StorageError> {
        let mut transaction = self.pool.begin().await?;
        let Some(_) =
            visible_playlist_for_update_in_transaction(&mut transaction, account_id, playlist_id)
                .await?
        else {
            return Ok(PlaylistItemReorderResult::PlaylistNotFound);
        };

        cleanup_playlist_items_in_transaction(&mut transaction, playlist_id).await?;

        let current_ids: Vec<Uuid> = sqlx::query_scalar(
            r#"
            SELECT id
            FROM playlist_items
            WHERE playlist_id = $1
            ORDER BY position ASC, id ASC
            FOR UPDATE
            "#,
        )
        .bind(playlist_id)
        .fetch_all(&mut *transaction)
        .await?;

        let current_set = current_ids.iter().copied().collect::<HashSet<_>>();
        let requested_set = playlist_item_ids.iter().copied().collect::<HashSet<_>>();
        if current_ids.len() != playlist_item_ids.len()
            || current_set.len() != playlist_item_ids.len()
            || current_set != requested_set
        {
            transaction.commit().await?;
            return Ok(PlaylistItemReorderResult::ItemSetMismatch);
        }

        if !playlist_item_ids.is_empty() {
            sqlx::query(
                r#"
                WITH requested AS (
                    SELECT item_id, (ordinality - 1)::integer AS new_position
                    FROM unnest($2::uuid[]) WITH ORDINALITY AS requested(item_id, ordinality)
                )
                UPDATE playlist_items pi
                SET position = requested.new_position
                FROM requested
                WHERE pi.playlist_id = $1
                  AND pi.id = requested.item_id
                "#,
            )
            .bind(playlist_id)
            .bind(&playlist_item_ids)
            .execute(&mut *transaction)
            .await?;
        }
        touch_playlist_in_transaction(&mut transaction, playlist_id, account_id).await?;
        let items =
            playlist_items_for_playlist_in_transaction(&mut transaction, playlist_id).await?;

        transaction.commit().await?;
        Ok(PlaylistItemReorderResult::Reordered(items))
    }

    /// Inserts or updates data for Postgres configuration, migrations, and maintenance repository persistence.
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
    /// - Returns `PlaybackProgress` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn upsert_playback_progress(
        &self,
        account_id: Uuid,
        item_type: PlaybackItemType,
        item_id: Uuid,
        context_type: Option<PlaybackContextType>,
        context_id: Option<Uuid>,
        position_seconds: u32,
        duration_seconds: Option<u32>,
        completed: bool,
    ) -> Result<PlaybackProgress, StorageError> {
        let sql = format!(
            r#"
            INSERT INTO playback_progress (
                account_id,
                item_type,
                item_id,
                context_type,
                context_id,
                position_seconds,
                duration_seconds,
                completed,
                updated_at
            )
            VALUES (
                $1,
                $2::text::playback_item_type,
                $3,
                $4::text::playback_context_type,
                $5,
                $6,
                $7,
                $8,
                $9
            )
            ON CONFLICT (account_id, item_type, item_id) DO UPDATE SET
                context_type = EXCLUDED.context_type,
                context_id = EXCLUDED.context_id,
                position_seconds = EXCLUDED.position_seconds,
                duration_seconds = EXCLUDED.duration_seconds,
                completed = EXCLUDED.completed,
                updated_at = EXCLUDED.updated_at
            RETURNING {PLAYBACK_PROGRESS_SELECT}
            "#
        );
        let row = sqlx::query(&sql)
            .bind(account_id)
            .bind(playback_item_type_name(item_type))
            .bind(item_id)
            .bind(context_type.map(playback_context_type_name))
            .bind(context_id)
            .bind(u32_to_i32(position_seconds, "playback_progress.position_seconds")?)
            .bind(optional_u32_to_i32(
                duration_seconds,
                "playback_progress.duration_seconds",
            )?)
            .bind(completed)
            .bind(Utc::now())
            .fetch_one(&self.pool)
            .await?;

        playback_progress_from_row(&row)
    }

    /// Handles playback progress for account for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Vec<PlaybackProgress>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn playback_progress_for_account(
        &self,
        account_id: Uuid,
    ) -> Result<Vec<PlaybackProgress>, StorageError> {
        let sql = format!(
            r#"
            SELECT {PLAYBACK_PROGRESS_SELECT}
            FROM playback_progress
            WHERE account_id = $1
            ORDER BY updated_at DESC, item_type ASC, item_id ASC
            "#
        );
        let rows = sqlx::query(&sql).bind(account_id).fetch_all(&self.pool).await?;
        rows.iter().map(playback_progress_from_row).collect()
    }

    /// Handles playback progress for item for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `item_type`: `PlaybackItemType`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<PlaybackProgress>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn playback_progress_for_item(
        &self,
        account_id: Uuid,
        item_type: PlaybackItemType,
        item_id: Uuid,
    ) -> Result<Option<PlaybackProgress>, StorageError> {
        let sql = format!(
            r#"
            SELECT {PLAYBACK_PROGRESS_SELECT}
            FROM playback_progress
            WHERE account_id = $1
              AND item_type = $2::text::playback_item_type
              AND item_id = $3
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(account_id)
            .bind(playback_item_type_name(item_type))
            .bind(item_id)
            .fetch_optional(&self.pool)
            .await?;

        row.as_ref().map(playback_progress_from_row).transpose()
    }

    /// Inserts data for Postgres configuration, migrations, and maintenance repository persistence.
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
    /// - Returns `PlaybackHistoryEvent` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn insert_playback_history_event(
        &self,
        account_id: Uuid,
        item_type: PlaybackItemType,
        item_id: Uuid,
        context_type: Option<PlaybackContextType>,
        context_id: Option<Uuid>,
        position_seconds: u32,
        duration_seconds: Option<u32>,
        completed: bool,
    ) -> Result<PlaybackHistoryEvent, StorageError> {
        let sql = format!(
            r#"
            INSERT INTO playback_history_events (
                id,
                account_id,
                item_type,
                item_id,
                context_type,
                context_id,
                position_seconds,
                duration_seconds,
                completed,
                played_at
            )
            VALUES (
                $1,
                $2,
                $3::text::playback_item_type,
                $4,
                $5::text::playback_context_type,
                $6,
                $7,
                $8,
                $9,
                $10
            )
            RETURNING {PLAYBACK_HISTORY_SELECT}
            "#
        );
        let row = sqlx::query(&sql)
            .bind(Uuid::new_v4())
            .bind(account_id)
            .bind(playback_item_type_name(item_type))
            .bind(item_id)
            .bind(context_type.map(playback_context_type_name))
            .bind(context_id)
            .bind(u32_to_i32(
                position_seconds,
                "playback_history_events.position_seconds",
            )?)
            .bind(optional_u32_to_i32(
                duration_seconds,
                "playback_history_events.duration_seconds",
            )?)
            .bind(completed)
            .bind(Utc::now())
            .fetch_one(&self.pool)
            .await?;

        playback_history_event_from_row(&row)
    }

    /// Handles playback history for account for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `limit`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `Vec<PlaybackHistoryEvent>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn playback_history_for_account(
        &self,
        account_id: Uuid,
        limit: u32,
    ) -> Result<Vec<PlaybackHistoryEvent>, StorageError> {
        let sql = format!(
            r#"
            SELECT {PLAYBACK_HISTORY_SELECT}
            FROM playback_history_events
            WHERE account_id = $1
            ORDER BY played_at DESC, id DESC
            LIMIT $2
            "#
        );
        let rows = sqlx::query(&sql)
            .bind(account_id)
            .bind(u32_to_i32(limit, "playback_history_events.limit")?)
            .fetch_all(&self.pool)
            .await?;

        rows.iter().map(playback_history_event_from_row).collect()
    }

    /// Enqueues background work for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `work`: `ImportWorkRequest`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `(ImportJob, bool)` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn enqueue_import_work(
        &self,
        work: ImportWorkRequest,
    ) -> Result<(ImportJob, bool), StorageError> {
        let idempotency_key = work.idempotency_key();
        if let Some(existing) = self.active_import_job_by_idempotency(&idempotency_key).await? {
            return Ok((existing, true));
        }

        let job = work.into_job();
        match self.insert_import_job(&job).await {
            Ok(()) => Ok((job, false)),
            Err(error) if error.is_unique_violation() => {
                if let Some(existing) = self
                    .active_import_job_by_idempotency(&job.idempotency_key)
                    .await?
                {
                    Ok((existing, true))
                } else {
                    Err(error)
                }
            }
            Err(error) => Err(error),
        }
    }

    /// Handles import jobs for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Vec<ImportJob>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn import_jobs(&self) -> Result<Vec<ImportJob>, StorageError> {
        let sql = format!(
            "SELECT {IMPORT_JOB_SELECT} FROM import_jobs ORDER BY created_at ASC, id ASC"
        );
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        rows.iter().map(import_job_from_row).collect()
    }

    /// Handles import job for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<ImportJob>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn import_job(
        &self,
        job_id: Uuid,
    ) -> Result<Option<ImportJob>, StorageError> {
        let sql = format!(
            "SELECT {IMPORT_JOB_SELECT} FROM import_jobs WHERE id = $1 LIMIT 1"
        );
        let row = sqlx::query(&sql)
            .bind(job_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(import_job_from_row).transpose()
    }

    /// Handles claim import job for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<ImportJob>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn claim_import_job(
        &self,
        job_id: Uuid,
    ) -> Result<Option<ImportJob>, StorageError> {
        let sql = format!(
            r#"
            WITH claimed AS (
                UPDATE import_jobs
                SET status = 'running',
                    attempts = attempts + 1,
                    updated_at = $2
                WHERE id = $1
                  AND status IN ('queued', 'retrying')
                RETURNING *
            )
            SELECT {IMPORT_JOB_SELECT}
            FROM claimed
            "#
        );
        let row = sqlx::query(&sql)
            .bind(job_id)
            .bind(Utc::now())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(import_job_from_row).transpose()
    }

    /// Handles claim next import job for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Option<ImportJob>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn claim_next_import_job(&self) -> Result<Option<ImportJob>, StorageError> {
        let sql = format!(
            r#"
            WITH next_job AS (
                SELECT id
                FROM import_jobs
                WHERE status IN ('queued', 'retrying')
                ORDER BY created_at ASC, id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            ),
            claimed AS (
                UPDATE import_jobs AS jobs
                SET status = 'running',
                    attempts = attempts + 1,
                    updated_at = $1
                FROM next_job
                WHERE jobs.id = next_job.id
                RETURNING jobs.*
            )
            SELECT {IMPORT_JOB_SELECT}
            FROM claimed
            "#
        );
        let row = sqlx::query(&sql)
            .bind(Utc::now())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(import_job_from_row).transpose()
    }

    /// Handles active import jobs for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Vec<ImportJob>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn active_import_jobs(&self) -> Result<Vec<ImportJob>, StorageError> {
        let sql = format!(
            r#"
            SELECT {IMPORT_JOB_SELECT}
            FROM import_jobs
            WHERE status IN ('queued', 'running', 'retrying')
            ORDER BY created_at ASC, id ASC
            "#
        );
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        rows.iter().map(import_job_from_row).collect()
    }

    /// Handles import job progress counts for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `ImportJobProgressCounts` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn import_job_progress_counts(
        &self,
        job_id: Uuid,
    ) -> Result<ImportJobProgressCounts, StorageError> {
        let row = sqlx::query(
            r#"
            SELECT
              count(*) AS processed_files,
              count(*) FILTER (WHERE status = 'published') AS published_files,
              count(*) FILTER (WHERE status IN ('duplicate', 'quarantined')) AS quarantined_files,
              count(*) FILTER (WHERE status = 'failed') AS failed_files,
              max(updated_at) AS last_progress_at
            FROM catalog_import_work_items
            WHERE import_job_id = $1
            "#,
        )
        .bind(job_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(ImportJobProgressCounts {
            processed_files: row.try_get("processed_files")?,
            published_files: row.try_get("published_files")?,
            quarantined_files: row.try_get("quarantined_files")?,
            failed_files: row.try_get("failed_files")?,
            last_progress_at: row.try_get("last_progress_at")?,
        })
    }

    /// Lists recent failed catalog import work items for admin diagnostics.
    ///
    /// Inputs:
    /// - `import_job_id`: optional import job filter.
    /// - `limit`: maximum number of failed rows to return.
    ///
    /// Output:
    /// - Returns recent failed work items with their persisted failure details.
    ///
    /// Errors:
    /// - Returns `StorageError` when persistence fails or stored enum values are invalid.
    pub async fn catalog_import_failures(
        &self,
        import_job_id: Option<Uuid>,
        limit: u32,
    ) -> Result<Vec<CatalogImportFailure>, StorageError> {
        let limit = limit.clamp(1, 500);
        let rows = sqlx::query(
            r#"
            SELECT
              wi.id,
              wi.import_job_id,
              j.kind::text AS import_job_kind,
              j.status::text AS import_job_status,
              wi.source_path,
              wi.media_file_id,
              wi.status::text AS status,
              wi.attempts,
              wi.last_error,
              wi.created_at,
              wi.updated_at
            FROM catalog_import_work_items wi
            JOIN import_jobs j ON j.id = wi.import_job_id
            WHERE wi.status = 'failed'
              AND ($1::uuid IS NULL OR wi.import_job_id = $1)
            ORDER BY wi.updated_at DESC, wi.id ASC
            LIMIT $2
            "#,
        )
        .bind(import_job_id)
        .bind(u32_to_i32(limit, "catalog_import_work_items.limit")?)
        .fetch_all(&self.pool)
        .await?;

        rows.iter()
            .map(catalog_import_failure_from_row)
            .collect()
    }

    /// Handles admin dashboard operational counts for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `AdminDashboardOperationalCounts` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn admin_dashboard_operational_counts(
        &self,
    ) -> Result<AdminDashboardOperationalCounts, StorageError> {
        let row = sqlx::query(
            r#"
            SELECT
              (
                SELECT count(*)
                FROM import_jobs
                WHERE status IN ('queued', 'running', 'retrying')
              ) AS scanning,
              (
                SELECT count(*)
                FROM quarantine_items
                WHERE status = 'open'
                  AND reason <> 'file_error'
              ) AS quarantined,
              (
                SELECT count(*)
                FROM quarantine_items
                WHERE status = 'open'
                  AND reason = 'file_error'
              ) AS failed
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(AdminDashboardOperationalCounts {
            scanning: row.try_get("scanning")?,
            quarantined: row.try_get("quarantined")?,
            failed: row.try_get("failed")?,
        })
    }

    /// Handles import job kind exists for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `kind`: `ImportJobKind`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `bool` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn import_job_kind_exists(
        &self,
        kind: ImportJobKind,
    ) -> Result<bool, StorageError> {
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM import_jobs
                WHERE kind = $1::text::import_job_kind
            )
            "#,
        )
        .bind(import_job_kind_name(kind))
        .fetch_one(&self.pool)
        .await?;

        Ok(exists)
    }

    /// Handles active import job by idempotency for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `idempotency_key`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Option<ImportJob>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn active_import_job_by_idempotency(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<ImportJob>, StorageError> {
        let sql = format!(
            r#"
            SELECT {IMPORT_JOB_SELECT}
            FROM import_jobs
            WHERE idempotency_key = $1
              AND status IN ('queued', 'running', 'retrying')
            ORDER BY created_at ASC, id ASC
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(idempotency_key)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(import_job_from_row).transpose()
    }

    /// Inserts data for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job`: `&ImportJob`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `()` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn insert_import_job(&self, job: &ImportJob) -> Result<(), StorageError> {
        let provider_filter = job
            .provider_filter
            .iter()
            .map(|provider| provider.api_name().to_string())
            .collect::<Vec<_>>();

        sqlx::query(
            r#"
            INSERT INTO import_jobs (
                id,
                kind,
                status,
                scope,
                repair_plan,
                catalog_mutation_policy,
                provider_filter,
                pipeline,
                source,
                reason,
                related_quarantine_item_id,
                idempotency_key,
                attempts,
                created_at,
                updated_at
            )
            VALUES (
                $1,
                $2::text::import_job_kind,
                $3::text::import_job_status,
                $4,
                $5,
                $6,
                ARRAY(SELECT unnest($7::text[])::provider_kind),
                $8,
                $9,
                $10,
                $11,
                $12,
                $13,
                $14,
                $15
            )
            "#,
        )
        .bind(job.id)
        .bind(import_job_kind_name(job.kind))
        .bind(import_job_status_name(job.status))
        .bind(Json(job.scope.clone()))
        .bind(Json(job.repair_plan.clone()))
        .bind(catalog_mutation_policy_name(job.catalog_mutation_policy))
        .bind(provider_filter)
        .bind(job.pipeline.as_str())
        .bind(import_job_source_name(job.source))
        .bind(job.reason.as_deref())
        .bind(job.related_quarantine_item_id)
        .bind(job.idempotency_key.as_str())
        .bind(u32_to_i32(job.attempts, "import_jobs.attempts")?)
        .bind(job.created_at)
        .bind(job.updated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Updates existing state for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `status`: `ImportJobStatus`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Option<ImportJob>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn update_import_job_status(
        &self,
        job_id: Uuid,
        status: ImportJobStatus,
    ) -> Result<Option<ImportJob>, StorageError> {
        let sql = format!(
            r#"
            UPDATE import_jobs
            SET status = $2::text::import_job_status,
                attempts = CASE
                    WHEN $2::text::import_job_status = 'running' THEN attempts + 1
                    ELSE attempts
                END,
                updated_at = $3
            WHERE id = $1
            RETURNING {IMPORT_JOB_SELECT}
            "#
        );
        let row = sqlx::query(&sql)
            .bind(job_id)
            .bind(import_job_status_name(status))
            .bind(Utc::now())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(import_job_from_row).transpose()
    }

    /// Inserts or updates data for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `import_job_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `source_path`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `media_file_id`: `Option<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `status`: `crate:domain:MediaFileStatus`; expected to be a media domain value that has already passed upstream validation.
    /// - `attempts`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
    /// - `last_error`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `()` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn upsert_catalog_import_work_item(
        &self,
        import_job_id: Uuid,
        source_path: &str,
        media_file_id: Option<Uuid>,
        status: crate::domain::MediaFileStatus,
        attempts: u32,
        last_error: Option<&str>,
    ) -> Result<(), StorageError> {
        let status = match status {
            crate::domain::MediaFileStatus::Staged => "staged",
            crate::domain::MediaFileStatus::Published => "published",
            crate::domain::MediaFileStatus::Duplicate => "duplicate",
            crate::domain::MediaFileStatus::Quarantined => "quarantined",
            crate::domain::MediaFileStatus::Failed => "failed",
        };
        sqlx::query(
            r#"
            INSERT INTO catalog_import_work_items (
                id,
                import_job_id,
                source_path,
                media_file_id,
                status,
                attempts,
                last_error,
                created_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5::text::media_file_status, $6, $7, $8, $8)
            ON CONFLICT (import_job_id, source_path) DO UPDATE SET
                media_file_id = EXCLUDED.media_file_id,
                status = EXCLUDED.status,
                attempts = EXCLUDED.attempts,
                last_error = EXCLUDED.last_error,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(import_job_id)
        .bind(source_path)
        .bind(media_file_id)
        .bind(status)
        .bind(u32_to_i32(attempts, "catalog_import_work_items.attempts")?)
        .bind(last_error)
        .bind(Utc::now())
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Handles provider health for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `Vec<ProviderHealth>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn provider_health(&self) -> Result<Vec<ProviderHealth>, StorageError> {
        let sql = format!(
            "SELECT {PROVIDER_HEALTH_SELECT} FROM provider_health ORDER BY provider ASC"
        );
        let rows = sqlx::query(&sql).fetch_all(&self.pool).await?;
        rows.iter().map(provider_health_from_row).collect()
    }

    /// Handles provider for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    ///
    /// Output:
    /// - Returns `Option<ProviderHealth>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn provider(
        &self,
        provider: ProviderKind,
    ) -> Result<Option<ProviderHealth>, StorageError> {
        let sql = format!(
            "SELECT {PROVIDER_HEALTH_SELECT} FROM provider_health WHERE provider = $1::text::provider_kind"
        );
        let row = sqlx::query(&sql)
            .bind(provider.api_name())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(provider_health_from_row).transpose()
    }

    /// Persists state for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `health`: `&ProviderHealth`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `()` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn save_provider_health(
        &self,
        health: &ProviderHealth,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO provider_health (
                provider,
                enabled,
                status,
                api_key_configured,
                maintenance_ready,
                failure_count,
                retry_after,
                last_success_at,
                last_failure_at,
                message,
                updated_at
            )
            VALUES (
                $1::text::provider_kind,
                $2,
                $3::text::provider_status,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                $10,
                $11
            )
            ON CONFLICT (provider) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                status = EXCLUDED.status,
                api_key_configured = EXCLUDED.api_key_configured,
                maintenance_ready = EXCLUDED.maintenance_ready,
                failure_count = EXCLUDED.failure_count,
                retry_after = EXCLUDED.retry_after,
                last_success_at = EXCLUDED.last_success_at,
                last_failure_at = EXCLUDED.last_failure_at,
                message = EXCLUDED.message,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(health.provider.api_name())
        .bind(health.enabled)
        .bind(provider_status_name(health.status))
        .bind(health.api_key_configured)
        .bind(health.maintenance_ready)
        .bind(u32_to_i32(health.failure_count, "provider_health.failure_count")?)
        .bind(health.retry_after)
        .bind(health.last_success_at)
        .bind(health.last_failure_at)
        .bind(health.message.as_deref())
        .bind(health.updated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Inserts data for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `item`: `&QuarantineItem`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `()` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn insert_quarantine_item(
        &self,
        item: &QuarantineItem,
    ) -> Result<(), StorageError> {
        sqlx::query(
            r#"
            INSERT INTO quarantine_items (
                id,
                media_file_id,
                source_path,
                reason,
                status,
                retry_count,
                retry_eligible,
                last_import_job_id,
                admin_note,
                created_at,
                updated_at
            )
            VALUES (
                $1,
                $2,
                $3,
                $4::text::quarantine_reason,
                $5::text::quarantine_status,
                $6,
                $7,
                $8,
                $9,
                $10,
                $11
            )
            "#,
        )
        .bind(item.id)
        .bind(item.media_file_id)
        .bind(item.source_path.as_str())
        .bind(quarantine_reason_name(item.reason))
        .bind(quarantine_status_name(item.status))
        .bind(u32_to_i32(item.retry_count, "quarantine_items.retry_count")?)
        .bind(item.retry_eligible)
        .bind(item.last_import_job_id)
        .bind(item.admin_note.as_deref())
        .bind(item.created_at)
        .bind(item.updated_at)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Handles quarantine item for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<QuarantineItem>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn quarantine_item(
        &self,
        item_id: Uuid,
    ) -> Result<Option<QuarantineItem>, StorageError> {
        let sql = format!(
            "SELECT {QUARANTINE_ITEM_SELECT} FROM quarantine_items WHERE id = $1"
        );
        let row = sqlx::query(&sql)
            .bind(item_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(quarantine_item_from_row).transpose()
    }

    /// Marks UI or workflow state for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `job_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<QuarantineItem>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn mark_quarantine_retrying(
        &self,
        item_id: Uuid,
        job_id: Uuid,
    ) -> Result<Option<QuarantineItem>, StorageError> {
        let sql = format!(
            r#"
            UPDATE quarantine_items
            SET status = 'retrying',
                retry_count = retry_count + 1,
                last_import_job_id = $2,
                updated_at = $3
            WHERE id = $1
            RETURNING {QUARANTINE_ITEM_SELECT}
            "#
        );
        let row = sqlx::query(&sql)
            .bind(item_id)
            .bind(job_id)
            .bind(Utc::now())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(quarantine_item_from_row).transpose()
    }

    /// Marks UI or workflow state for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `media_file_id`: `Option<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `admin_note`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Option<QuarantineItem>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn mark_quarantine_resolved(
        &self,
        item_id: Uuid,
        media_file_id: Option<Uuid>,
        admin_note: Option<&str>,
    ) -> Result<Option<QuarantineItem>, StorageError> {
        let sql = format!(
            r#"
            UPDATE quarantine_items
            SET status = 'resolved',
                media_file_id = COALESCE($2, media_file_id),
                retry_eligible = false,
                admin_note = COALESCE($3, admin_note),
                updated_at = $4
            WHERE id = $1
            RETURNING {QUARANTINE_ITEM_SELECT}
            "#
        );
        let row = sqlx::query(&sql)
            .bind(item_id)
            .bind(media_file_id)
            .bind(admin_note)
            .bind(Utc::now())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(quarantine_item_from_row).transpose()
    }

    /// Marks UI or workflow state for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `media_file_id`: `Option<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `retry_eligible`: `bool`; expected to be a boolean flag controlling the documented branch.
    /// - `admin_note`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Option<QuarantineItem>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn mark_quarantine_open(
        &self,
        item_id: Uuid,
        media_file_id: Option<Uuid>,
        retry_eligible: bool,
        admin_note: Option<&str>,
    ) -> Result<Option<QuarantineItem>, StorageError> {
        let sql = format!(
            r#"
            UPDATE quarantine_items
            SET status = 'open',
                media_file_id = COALESCE($2, media_file_id),
                retry_eligible = $3,
                admin_note = COALESCE($4, admin_note),
                updated_at = $5
            WHERE id = $1
            RETURNING {QUARANTINE_ITEM_SELECT}
            "#
        );
        let row = sqlx::query(&sql)
            .bind(item_id)
            .bind(media_file_id)
            .bind(retry_eligible)
            .bind(admin_note)
            .bind(Utc::now())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(quarantine_item_from_row).transpose()
    }

    /// Handles active job for quarantine item for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `Option<ImportJob>` on success or `StorageError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn active_job_for_quarantine_item(
        &self,
        item_id: Uuid,
    ) -> Result<Option<ImportJob>, StorageError> {
        let sql = format!(
            r#"
            SELECT {IMPORT_JOB_SELECT_FROM_JOIN}
            FROM quarantine_items q
            JOIN import_jobs j ON j.id = q.last_import_job_id
            WHERE q.id = $1
              AND j.status IN ('queued', 'running', 'retrying')
            LIMIT 1
            "#
        );
        let row = sqlx::query(&sql)
            .bind(item_id)
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(import_job_from_row).transpose()
    }

    /// Enqueues background work for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `prepared`: `Vec<QuarantineRetryWork>`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Vec<(Uuid, ImportJob)>` on success or `QuarantineRetryError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `QuarantineRetryError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn enqueue_quarantine_retries(
        &self,
        prepared: Vec<QuarantineRetryWork>,
    ) -> Result<Vec<(Uuid, ImportJob)>, QuarantineRetryError> {
        let requested_order = prepared
            .iter()
            .map(|prepared| prepared.item_id)
            .collect::<Vec<_>>();
        let mut unique_item_ids = Vec::new();
        let mut seen_item_ids = HashSet::new();
        for item_id in &requested_order {
            if seen_item_ids.insert(*item_id) {
                unique_item_ids.push(*item_id);
            }
        }

        let mut work_by_item_id = prepared
            .into_iter()
            .map(|prepared| (prepared.item_id, prepared.work))
            .collect::<HashMap<_, _>>();
        let mut transaction = self.pool.begin().await.map_err(StorageError::from)?;

        for item_id in &unique_item_ids {
            let sql = format!(
                r#"
                SELECT {QUARANTINE_ITEM_SELECT}
                FROM quarantine_items
                WHERE id = $1
                FOR UPDATE
                "#
            );
            let row = sqlx::query(&sql)
                .bind(item_id)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(StorageError::from)?;
            let item = row
                .as_ref()
                .map(quarantine_item_from_row)
                .transpose()?
                .ok_or(QuarantineRetryError::NotFound(*item_id))?;

            if !item.retry_eligible {
                return Err(QuarantineRetryError::NotRetryEligible(*item_id));
            }
            if matches!(item.status, QuarantineStatus::Deleted | QuarantineStatus::Resolved) {
                return Err(QuarantineRetryError::TerminalStatus {
                    item_id: *item_id,
                    status: item.status,
                });
            }
        }

        let mut job_by_item_id = HashMap::with_capacity(unique_item_ids.len());
        for item_id in &unique_item_ids {
            if let Some(active_job) =
                active_job_for_quarantine_item_in_transaction(&mut transaction, *item_id).await?
            {
                job_by_item_id.insert(*item_id, active_job);
                continue;
            }

            let work = work_by_item_id
                .remove(item_id)
                .ok_or(QuarantineRetryError::MissingPreparedWork(*item_id))?;
            let job = enqueue_import_work_in_transaction(&mut transaction, work).await?;
            mark_quarantine_retrying_in_transaction(&mut transaction, *item_id, job.id).await?;
            job_by_item_id.insert(*item_id, job);
        }

        let retried = requested_order
            .into_iter()
            .map(|item_id| {
                job_by_item_id
                    .get(&item_id)
                    .cloned()
                    .map(|job| (item_id, job))
                    .ok_or(QuarantineRetryError::MissingPreparedWork(item_id))
            })
            .collect::<Result<Vec<_>, _>>()?;

        transaction.commit().await.map_err(StorageError::from)?;
        Ok(retried)
    }
}

/// Handles active job for quarantine item in transaction for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `Option<ImportJob>` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn active_job_for_quarantine_item_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    item_id: Uuid,
) -> Result<Option<ImportJob>, StorageError> {
    let sql = format!(
        r#"
        SELECT {IMPORT_JOB_SELECT_FROM_JOIN}
        FROM quarantine_items q
        JOIN import_jobs j ON j.id = q.last_import_job_id
        WHERE q.id = $1
          AND j.status IN ('queued', 'running', 'retrying')
        LIMIT 1
        "#
    );
    let row = sqlx::query(&sql)
        .bind(item_id)
        .fetch_optional(&mut **transaction)
        .await?;
    row.as_ref().map(import_job_from_row).transpose()
}

/// Enqueues background work for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `work`: `ImportWorkRequest`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ImportJob` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn enqueue_import_work_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    work: ImportWorkRequest,
) -> Result<ImportJob, StorageError> {
    let job = work.into_job();
    let provider_filter = job
        .provider_filter
        .iter()
        .map(|provider| provider.api_name().to_string())
        .collect::<Vec<_>>();

    let result = sqlx::query(
        r#"
        INSERT INTO import_jobs (
            id,
            kind,
            status,
            scope,
            repair_plan,
            catalog_mutation_policy,
            provider_filter,
            pipeline,
            source,
            reason,
            related_quarantine_item_id,
            idempotency_key,
            attempts,
            created_at,
            updated_at
        )
        VALUES (
            $1,
            $2::text::import_job_kind,
            $3::text::import_job_status,
            $4,
            $5,
            $6,
            ARRAY(SELECT unnest($7::text[])::provider_kind),
            $8,
            $9,
            $10,
            $11,
            $12,
            $13,
            $14,
            $15
        )
        ON CONFLICT (idempotency_key)
            WHERE status IN ('queued', 'running', 'retrying')
            DO NOTHING
        "#,
    )
    .bind(job.id)
    .bind(import_job_kind_name(job.kind))
    .bind(import_job_status_name(job.status))
    .bind(Json(job.scope.clone()))
    .bind(Json(job.repair_plan.clone()))
    .bind(catalog_mutation_policy_name(job.catalog_mutation_policy))
    .bind(provider_filter)
    .bind(job.pipeline.as_str())
    .bind(import_job_source_name(job.source))
    .bind(job.reason.as_deref())
    .bind(job.related_quarantine_item_id)
    .bind(job.idempotency_key.as_str())
    .bind(u32_to_i32(job.attempts, "import_jobs.attempts")?)
    .bind(job.created_at)
    .bind(job.updated_at)
    .execute(&mut **transaction)
    .await?;

    if result.rows_affected() > 0 {
        return Ok(job);
    }

    let existing = active_import_job_by_idempotency_in_transaction(
        transaction,
        &job.idempotency_key,
    )
    .await?;
    existing.ok_or_else(|| {
        StorageError::SchemaVerification(format!(
            "active import job disappeared for idempotency key {}",
            job.idempotency_key
        ))
    })
}

/// Handles active import job by idempotency in transaction for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `idempotency_key`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Option<ImportJob>` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn active_import_job_by_idempotency_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    idempotency_key: &str,
) -> Result<Option<ImportJob>, StorageError> {
    let sql = format!(
        r#"
        SELECT {IMPORT_JOB_SELECT}
        FROM import_jobs
        WHERE idempotency_key = $1
          AND status IN ('queued', 'running', 'retrying')
        ORDER BY created_at ASC, id ASC
        LIMIT 1
        "#
    );
    let row = sqlx::query(&sql)
        .bind(idempotency_key)
        .fetch_optional(&mut **transaction)
        .await?;
    row.as_ref().map(import_job_from_row).transpose()
}

/// Marks UI or workflow state for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `job_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `QuarantineItem` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn mark_quarantine_retrying_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    item_id: Uuid,
    job_id: Uuid,
) -> Result<QuarantineItem, StorageError> {
    let sql = format!(
        r#"
        UPDATE quarantine_items
        SET status = 'retrying',
            retry_count = retry_count + 1,
            last_import_job_id = $2,
            updated_at = $3
        WHERE id = $1
        RETURNING {QUARANTINE_ITEM_SELECT}
        "#
    );
    let row = sqlx::query(&sql)
        .bind(item_id)
        .bind(job_id)
        .bind(Utc::now())
        .fetch_one(&mut **transaction)
        .await?;
    quarantine_item_from_row(&row)
}

impl StorageError {
    /// Handles is unique violation for Postgres configuration, migrations, and maintenance repository persistence.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub(crate) fn is_unique_violation(&self) -> bool {
        match self {
            StorageError::Sqlx(sqlx::Error::Database(error)) => {
                error.code().as_deref() == Some("23505")
            }
            _ => false,
        }
    }
}

/// Handles system config from row for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `SystemConfig` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn system_config_from_row(row: &PgRow) -> Result<SystemConfig, StorageError> {
    Ok(SystemConfig {
        library_root: row.try_get("library_root")?,
        dropbox_root: row.try_get("dropbox_root")?,
        podcast_subtree: row.try_get("podcast_subtree")?,
        public_base_url: row.try_get("public_base_url")?,
        transcode_concurrency_limit: row.try_get("transcode_concurrency_limit")?,
        scan_thread_count: row.try_get("scan_thread_count")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles provider setting from row for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ProviderSetting` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn provider_setting_from_row(row: &PgRow) -> Result<ProviderSetting, StorageError> {
    let provider = parse_provider_kind(row.try_get::<String, _>("provider")?)?;

    Ok(ProviderSetting {
        provider,
        display_name: provider.display_name().to_string(),
        enabled: row.try_get("enabled")?,
        requires_api_key: row.try_get("requires_api_key")?,
        api_key_configured: row.try_get("api_key_configured")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles local account from row for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `LocalAccount` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn local_account_from_row(row: &PgRow) -> Result<LocalAccount, StorageError> {
    Ok(LocalAccount {
        id: row.try_get("id")?,
        username: row.try_get("username")?,
        password_hash: row.try_get("password_hash")?,
        role: parse_account_role(row.try_get::<String, _>("role")?)?,
        disabled: row.try_get("disabled")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Inserts or updates data for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `playlist`: `&Playlist`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub(crate) async fn upsert_playlist_search_projection_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    playlist: &Playlist,
) -> Result<(), StorageError> {
    let normalized_name = normalize_catalog_text(&playlist.name);
    if normalized_name.is_empty() {
        delete_playlist_search_projection_in_transaction(transaction, playlist.id).await?;
        return Ok(());
    }

    sqlx::query(
        r#"
        INSERT INTO catalog_search_projection (
            entity_type,
            entity_id,
            display_title,
            search_text,
            normalized_text,
            normalized_display_title,
            published,
            updated_at
        )
        VALUES (
            'playlist'::catalog_entity_type,
            $1,
            $2,
            $2,
            $3,
            $3,
            true,
            $4
        )
        ON CONFLICT (entity_type, entity_id) DO UPDATE SET
            display_title = EXCLUDED.display_title,
            search_text = EXCLUDED.search_text,
            normalized_text = EXCLUDED.normalized_text,
            normalized_display_title = EXCLUDED.normalized_display_title,
            published = EXCLUDED.published,
            updated_at = EXCLUDED.updated_at
        "#,
    )
    .bind(playlist.id)
    .bind(playlist.name.as_str())
    .bind(normalized_name)
    .bind(playlist.updated_at)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

/// Deletes or removes a resource from Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn delete_playlist_search_projection_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    playlist_id: Uuid,
) -> Result<(), StorageError> {
    sqlx::query(
        r#"
        DELETE FROM catalog_search_projection
        WHERE entity_type = 'playlist'::catalog_entity_type
          AND entity_id = $1
        "#,
    )
    .bind(playlist_id)
    .execute(&mut **transaction)
    .await?;

    Ok(())
}

/// Handles visible playlist for update in transaction for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `Option<Playlist>` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn visible_playlist_for_update_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    playlist_id: Uuid,
) -> Result<Option<Playlist>, StorageError> {
    let sql = format!(
        r#"
        SELECT {PLAYLIST_SELECT}
        FROM playlists
        WHERE id = $1
          AND (scope = 'shared' OR owner_account_id = $2)
        FOR UPDATE
        "#
    );
    let row = sqlx::query(&sql)
        .bind(playlist_id)
        .bind(account_id)
        .fetch_optional(&mut **transaction)
        .await?;

    row.as_ref().map(playlist_from_row).transpose()
}

/// Handles playlist items for playlist in transaction for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `Vec<PlaylistItem>` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn playlist_items_for_playlist_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    playlist_id: Uuid,
) -> Result<Vec<PlaylistItem>, StorageError> {
    let sql = format!(
        r#"
        SELECT {PLAYLIST_ITEM_SELECT}
        FROM playlist_items
        WHERE playlist_id = $1
        ORDER BY position ASC, id ASC
        "#
    );
    let rows = sqlx::query(&sql)
        .bind(playlist_id)
        .fetch_all(&mut **transaction)
        .await?;
    rows.iter().map(playlist_item_from_row).collect()
}

/// Handles defer playlist item position constraint for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn defer_playlist_item_position_constraint(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
) -> Result<(), StorageError> {
    sqlx::query("SET CONSTRAINTS playlist_items_playlist_position_key DEFERRED")
        .execute(&mut **transaction)
        .await?;
    Ok(())
}

/// Normalizes caller-provided data for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn normalize_playlist_item_positions_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    playlist_id: Uuid,
) -> Result<(), StorageError> {
    sqlx::query(
        r#"
        WITH ordered AS (
            SELECT
                id,
                (row_number() OVER (ORDER BY position ASC, id ASC) - 1)::integer AS new_position
            FROM playlist_items
            WHERE playlist_id = $1
        )
        UPDATE playlist_items pi
        SET position = ordered.new_position
        FROM ordered
        WHERE pi.id = ordered.id
          AND pi.position <> ordered.new_position
        "#,
    )
    .bind(playlist_id)
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

/// Handles cleanup playlist items in transaction for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn cleanup_playlist_items_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    playlist_id: Uuid,
) -> Result<(), StorageError> {
    defer_playlist_item_position_constraint(transaction).await?;

    let track_exists =
        playlist_catalog_item_eligible_exists_sql(PlaybackItemType::Track, "pi.item_id");
    let episode_exists =
        playlist_catalog_item_eligible_exists_sql(PlaybackItemType::Episode, "pi.item_id");
    let sql = format!(
        r#"
        DELETE FROM playlist_items pi
        WHERE pi.playlist_id = $1
          AND NOT (
            (
              pi.item_type = 'track'::playback_item_type
              AND {track_exists}
            )
            OR
            (
              pi.item_type = 'episode'::playback_item_type
              AND {episode_exists}
            )
          )
        "#
    );
    sqlx::query(&sql)
        .bind(playlist_id)
        .execute(&mut **transaction)
        .await?;

    normalize_playlist_item_positions_in_transaction(transaction, playlist_id).await?;
    Ok(())
}

/// Handles touch playlist in transaction for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `playlist_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
/// - `account_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `()` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn touch_playlist_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    playlist_id: Uuid,
    account_id: Uuid,
) -> Result<(), StorageError> {
    sqlx::query(
        r#"
        UPDATE playlists
        SET updated_by_account_id = $2,
            updated_at = $3
        WHERE id = $1
        "#,
    )
    .bind(playlist_id)
    .bind(account_id)
    .bind(Utc::now())
    .execute(&mut **transaction)
    .await?;
    Ok(())
}

/// Handles playlist catalog item eligible exists sql for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `item_type`: `PlaybackItemType`; expected to be a value satisfying the type contract shown in the function signature.
/// - `item_id_expr`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn playlist_catalog_item_eligible_exists_sql(
    item_type: PlaybackItemType,
    item_id_expr: &str,
) -> String {
    match item_type {
        PlaybackItemType::Track => format!(
            r#"
            EXISTS (
                SELECT 1
                FROM tracks t
                JOIN albums al ON al.id = t.album_id
                JOIN artists album_artist ON album_artist.id = al.artist_id
                JOIN artists track_artist ON track_artist.id = t.artist_id
                JOIN media_files mf ON mf.id = t.canonical_media_file_id
                  AND mf.track_id = t.id
                WHERE t.id = {item_id_expr}
                  AND t.published_at IS NOT NULL
                  AND t.stable_grouping
                  AND al.published_at IS NOT NULL
                  AND al.stable_grouping
                  AND album_artist.published_at IS NOT NULL
                  AND album_artist.stable_grouping
                  AND track_artist.published_at IS NOT NULL
                  AND track_artist.stable_grouping
                  AND {PLAYLIST_ELIGIBLE_MEDIA_FILE_PREDICATE}
            )
            "#
        ),
        PlaybackItemType::Episode => format!(
            r#"
            EXISTS (
                SELECT 1
                FROM episodes e
                JOIN podcasts p ON p.id = e.podcast_id
                JOIN media_files mf ON mf.id = e.canonical_media_file_id
                  AND mf.episode_id = e.id
                WHERE e.id = {item_id_expr}
                  AND e.published_at IS NOT NULL
                  AND e.stable_grouping
                  AND p.published_at IS NOT NULL
                  AND p.stable_grouping
                  AND {PLAYLIST_ELIGIBLE_MEDIA_FILE_PREDICATE}
            )
            "#
        ),
    }
}

/// Handles playlist catalog item is eligible in transaction for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `transaction`: `&mut sqlx:Transaction<'_, sqlx::Postgres>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `item_type`: `PlaybackItemType`; expected to be a value satisfying the type contract shown in the function signature.
/// - `item_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `bool` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn playlist_catalog_item_is_eligible_in_transaction(
    transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    item_type: PlaybackItemType,
    item_id: Uuid,
) -> Result<bool, StorageError> {
    let exists = playlist_catalog_item_eligible_exists_sql(item_type, "$1");
    let sql = format!("SELECT {exists}");
    let eligible = sqlx::query_scalar(&sql)
        .bind(item_id)
        .fetch_one(&mut **transaction)
        .await?;
    Ok(eligible)
}

/// Handles playlist from row for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Playlist` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn playlist_from_row(row: &PgRow) -> Result<Playlist, StorageError> {
    Ok(Playlist {
        id: row.try_get("id")?,
        name: row.try_get("name")?,
        description: row.try_get("description")?,
        scope: parse_playlist_scope(row.try_get::<String, _>("scope")?)?,
        owner_account_id: row.try_get("owner_account_id")?,
        created_by_account_id: row.try_get("created_by_account_id")?,
        updated_by_account_id: row.try_get("updated_by_account_id")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles playlist item from row for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `PlaylistItem` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn playlist_item_from_row(row: &PgRow) -> Result<PlaylistItem, StorageError> {
    let position = i32_to_u32(
        row.try_get::<i32, _>("position")?,
        "playlist_items.position",
    )?;

    Ok(PlaylistItem {
        id: row.try_get("id")?,
        playlist_id: row.try_get("playlist_id")?,
        item_type: parse_playback_item_type(row.try_get::<String, _>("item_type")?)?,
        item_id: row.try_get("item_id")?,
        position,
        added_by_account_id: row.try_get("added_by_account_id")?,
        created_at: row.try_get("created_at")?,
    })
}

/// Handles playback progress from row for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `PlaybackProgress` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn playback_progress_from_row(row: &PgRow) -> Result<PlaybackProgress, StorageError> {
    let position_seconds = i32_to_u32(
        row.try_get::<i32, _>("position_seconds")?,
        "playback_progress.position_seconds",
    )?;
    let duration_seconds = optional_i32_to_u32(
        row.try_get::<Option<i32>, _>("duration_seconds")?,
        "playback_progress.duration_seconds",
    )?;

    Ok(PlaybackProgress {
        item_type: parse_playback_item_type(row.try_get::<String, _>("item_type")?)?,
        item_id: row.try_get("item_id")?,
        context_type: row
            .try_get::<Option<String>, _>("context_type")?
            .map(parse_playback_context_type)
            .transpose()?,
        context_id: row.try_get("context_id")?,
        position_seconds,
        duration_seconds,
        completed: row.try_get("completed")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles playback history event from row for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `PlaybackHistoryEvent` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn playback_history_event_from_row(
    row: &PgRow,
) -> Result<PlaybackHistoryEvent, StorageError> {
    let position_seconds = i32_to_u32(
        row.try_get::<i32, _>("position_seconds")?,
        "playback_history_events.position_seconds",
    )?;
    let duration_seconds = optional_i32_to_u32(
        row.try_get::<Option<i32>, _>("duration_seconds")?,
        "playback_history_events.duration_seconds",
    )?;

    Ok(PlaybackHistoryEvent {
        id: row.try_get("id")?,
        item_type: parse_playback_item_type(row.try_get::<String, _>("item_type")?)?,
        item_id: row.try_get("item_id")?,
        context_type: row
            .try_get::<Option<String>, _>("context_type")?
            .map(parse_playback_context_type)
            .transpose()?,
        context_id: row.try_get("context_id")?,
        position_seconds,
        duration_seconds,
        completed: row.try_get("completed")?,
        played_at: row.try_get("played_at")?,
    })
}

/// Handles import job from row for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ImportJob` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn import_job_from_row(row: &PgRow) -> Result<ImportJob, StorageError> {
    let kind = parse_import_job_kind(row.try_get::<String, _>("kind")?)?;
    let status = parse_import_job_status(row.try_get::<String, _>("status")?)?;
    let scope = json_column::<MaintenanceScope>(row, "scope")?;
    let repair_plan = json_column::<RepairPlan>(row, "repair_plan")?;
    let catalog_mutation_policy =
        parse_catalog_mutation_policy(row.try_get::<String, _>("catalog_mutation_policy")?)?;
    let provider_filter = row
        .try_get::<Vec<String>, _>("provider_filter")?
        .into_iter()
        .map(parse_provider_kind)
        .collect::<Result<Vec<_>, _>>()?;
    let source = parse_import_job_source(row.try_get::<String, _>("source")?)?;
    let attempts = i32_to_u32(row.try_get::<i32, _>("attempts")?, "import_jobs.attempts")?;

    Ok(ImportJob {
        id: row.try_get("id")?,
        kind,
        status,
        scope,
        repair_plan,
        catalog_mutation_policy,
        provider_filter,
        pipeline: row.try_get("pipeline")?,
        source,
        reason: row.try_get("reason")?,
        related_quarantine_item_id: row.try_get("related_quarantine_item_id")?,
        idempotency_key: row.try_get("idempotency_key")?,
        attempts,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles catalog import failure from row for Postgres maintenance diagnostics.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a row from `catalog_import_failures`.
///
/// Output:
/// - Returns `CatalogImportFailure` on success.
///
/// Errors:
/// - Returns `StorageError` when stored enum or numeric values are invalid.
fn catalog_import_failure_from_row(
    row: &PgRow,
) -> Result<CatalogImportFailure, StorageError> {
    let attempts = i32_to_u32(
        row.try_get::<i32, _>("attempts")?,
        "catalog_import_work_items.attempts",
    )?;

    Ok(CatalogImportFailure {
        id: row.try_get("id")?,
        import_job_id: row.try_get("import_job_id")?,
        import_job_kind: parse_import_job_kind(
            row.try_get::<String, _>("import_job_kind")?,
        )?,
        import_job_status: parse_import_job_status(
            row.try_get::<String, _>("import_job_status")?,
        )?,
        source_path: row.try_get("source_path")?,
        media_file_id: row.try_get("media_file_id")?,
        status: parse_media_file_status(row.try_get::<String, _>("status")?)?,
        attempts,
        last_error: row.try_get("last_error")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles provider health from row for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `ProviderHealth` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn provider_health_from_row(row: &PgRow) -> Result<ProviderHealth, StorageError> {
    let provider = parse_provider_kind(row.try_get::<String, _>("provider")?)?;
    let failure_count = i32_to_u32(
        row.try_get::<i32, _>("failure_count")?,
        "provider_health.failure_count",
    )?;

    Ok(ProviderHealth {
        provider,
        display_name: provider.display_name().to_string(),
        enabled: row.try_get("enabled")?,
        status: parse_provider_status(row.try_get::<String, _>("status")?)?,
        api_key_configured: row.try_get("api_key_configured")?,
        maintenance_ready: row.try_get("maintenance_ready")?,
        failure_count,
        retry_after: row.try_get::<Option<DateTime<Utc>>, _>("retry_after")?,
        last_success_at: row.try_get::<Option<DateTime<Utc>>, _>("last_success_at")?,
        last_failure_at: row.try_get::<Option<DateTime<Utc>>, _>("last_failure_at")?,
        message: row.try_get("message")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles quarantine item from row for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `QuarantineItem` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn quarantine_item_from_row(row: &PgRow) -> Result<QuarantineItem, StorageError> {
    let retry_count = i32_to_u32(
        row.try_get::<i32, _>("retry_count")?,
        "quarantine_items.retry_count",
    )?;

    Ok(QuarantineItem {
        id: row.try_get("id")?,
        media_file_id: row.try_get("media_file_id")?,
        source_path: row.try_get("source_path")?,
        reason: parse_quarantine_reason(row.try_get::<String, _>("reason")?)?,
        status: parse_quarantine_status(row.try_get::<String, _>("status")?)?,
        retry_count,
        retry_eligible: row.try_get("retry_eligible")?,
        last_import_job_id: row.try_get("last_import_job_id")?,
        admin_note: row.try_get("admin_note")?,
        created_at: row.try_get("created_at")?,
        updated_at: row.try_get("updated_at")?,
    })
}

/// Handles json column for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `row`: `&PgRow`; expected to be a value satisfying the type contract shown in the function signature.
/// - `column`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `T` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn json_column<T>(row: &PgRow, column: &'static str) -> Result<T, StorageError>
where
    T: DeserializeOwned,
{
    let value = row.try_get::<Json<T>, _>(column)?.0;
    Ok(value)
}

/// Handles account role name for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `role`: `AccountRole`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn account_role_name(role: AccountRole) -> &'static str {
    match role {
        AccountRole::Admin => "admin",
        AccountRole::User => "user",
    }
}

/// Parses and validates input for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `AccountRole` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_account_role(value: String) -> Result<AccountRole, StorageError> {
    match value.as_str() {
        "admin" => Ok(AccountRole::Admin),
        "user" => Ok(AccountRole::User),
        _ => invalid_value("local_accounts.role", value),
    }
}

/// Handles playlist scope name for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `scope`: `PlaylistScope`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn playlist_scope_name(scope: PlaylistScope) -> &'static str {
    match scope {
        PlaylistScope::Personal => "personal",
        PlaylistScope::Shared => "shared",
    }
}

/// Parses and validates input for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `PlaylistScope` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_playlist_scope(value: String) -> Result<PlaylistScope, StorageError> {
    match value.as_str() {
        "personal" => Ok(PlaylistScope::Personal),
        "shared" => Ok(PlaylistScope::Shared),
        _ => invalid_value("playlists.scope", value),
    }
}

/// Handles playback item type name for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `item_type`: `PlaybackItemType`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn playback_item_type_name(item_type: PlaybackItemType) -> &'static str {
    match item_type {
        PlaybackItemType::Track => "track",
        PlaybackItemType::Episode => "episode",
    }
}

/// Parses and validates input for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `PlaybackItemType` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_playback_item_type(value: String) -> Result<PlaybackItemType, StorageError> {
    match value.as_str() {
        "track" => Ok(PlaybackItemType::Track),
        "episode" => Ok(PlaybackItemType::Episode),
        _ => invalid_value("playback_item_type", value),
    }
}

fn playback_context_type_name(context_type: PlaybackContextType) -> &'static str {
    match context_type {
        PlaybackContextType::Album => "album",
        PlaybackContextType::Playlist => "playlist",
        PlaybackContextType::Podcast => "podcast",
    }
}

fn parse_playback_context_type(value: String) -> Result<PlaybackContextType, StorageError> {
    match value.as_str() {
        "album" => Ok(PlaybackContextType::Album),
        "playlist" => Ok(PlaybackContextType::Playlist),
        "podcast" => Ok(PlaybackContextType::Podcast),
        _ => invalid_value("playback_context_type", value),
    }
}

/// Handles import job kind name for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `kind`: `ImportJobKind`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn import_job_kind_name(kind: ImportJobKind) -> &'static str {
    match kind {
        ImportJobKind::InitialScan => "initial_scan",
        ImportJobKind::DropboxIngest => "dropbox_ingest",
        ImportJobKind::FullRescan => "full_rescan",
        ImportJobKind::SubtreeRescan => "subtree_rescan",
        ImportJobKind::ProviderRepair => "provider_repair",
        ImportJobKind::QuarantineRetry => "quarantine_retry",
    }
}

/// Parses and validates input for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `ImportJobKind` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_import_job_kind(value: String) -> Result<ImportJobKind, StorageError> {
    match value.as_str() {
        "initial_scan" => Ok(ImportJobKind::InitialScan),
        "dropbox_ingest" => Ok(ImportJobKind::DropboxIngest),
        "full_rescan" => Ok(ImportJobKind::FullRescan),
        "subtree_rescan" => Ok(ImportJobKind::SubtreeRescan),
        "provider_repair" => Ok(ImportJobKind::ProviderRepair),
        "quarantine_retry" => Ok(ImportJobKind::QuarantineRetry),
        _ => invalid_value("import_jobs.kind", value),
    }
}

/// Handles import job status name for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `status`: `ImportJobStatus`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn import_job_status_name(status: ImportJobStatus) -> &'static str {
    match status {
        ImportJobStatus::Queued => "queued",
        ImportJobStatus::Running => "running",
        ImportJobStatus::Completed => "completed",
        ImportJobStatus::Failed => "failed",
        ImportJobStatus::Quarantined => "quarantined",
        ImportJobStatus::Retrying => "retrying",
    }
}

/// Parses and validates input for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `ImportJobStatus` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_import_job_status(value: String) -> Result<ImportJobStatus, StorageError> {
    match value.as_str() {
        "queued" => Ok(ImportJobStatus::Queued),
        "running" => Ok(ImportJobStatus::Running),
        "completed" => Ok(ImportJobStatus::Completed),
        "failed" => Ok(ImportJobStatus::Failed),
        "quarantined" => Ok(ImportJobStatus::Quarantined),
        "retrying" => Ok(ImportJobStatus::Retrying),
        _ => invalid_value("import_jobs.status", value),
    }
}

/// Parses and validates input for Postgres maintenance reporting.
///
/// Inputs:
/// - `value`: stored `media_file_status` text.
///
/// Output:
/// - Returns `MediaFileStatus` on success.
///
/// Errors:
/// - Returns `StorageError` when the stored value is invalid.
fn parse_media_file_status(value: String) -> Result<MediaFileStatus, StorageError> {
    match value.as_str() {
        "staged" => Ok(MediaFileStatus::Staged),
        "published" => Ok(MediaFileStatus::Published),
        "duplicate" => Ok(MediaFileStatus::Duplicate),
        "quarantined" => Ok(MediaFileStatus::Quarantined),
        "failed" => Ok(MediaFileStatus::Failed),
        _ => invalid_value("catalog_import_work_items.status", value),
    }
}

/// Handles provider status name for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `status`: `ProviderStatus`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn provider_status_name(status: ProviderStatus) -> &'static str {
    match status {
        ProviderStatus::Healthy => "healthy",
        ProviderStatus::Degraded => "degraded",
        ProviderStatus::BackingOff => "backing_off",
        ProviderStatus::Disabled => "disabled",
        ProviderStatus::Unconfigured => "unconfigured",
    }
}

/// Parses and validates input for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `ProviderStatus` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_provider_status(value: String) -> Result<ProviderStatus, StorageError> {
    match value.as_str() {
        "healthy" => Ok(ProviderStatus::Healthy),
        "degraded" => Ok(ProviderStatus::Degraded),
        "backing_off" => Ok(ProviderStatus::BackingOff),
        "disabled" => Ok(ProviderStatus::Disabled),
        "unconfigured" => Ok(ProviderStatus::Unconfigured),
        _ => invalid_value("provider_health.status", value),
    }
}

/// Parses and validates input for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `ProviderKind` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_provider_kind(value: String) -> Result<ProviderKind, StorageError> {
    ProviderKind::from_str(&value).map_err(|_| StorageError::InvalidStoredValue {
        field: "provider_kind",
        value,
    })
}

/// Handles catalog mutation policy name for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `policy`: `CatalogMutationPolicy`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn catalog_mutation_policy_name(policy: CatalogMutationPolicy) -> &'static str {
    match policy {
        CatalogMutationPolicy::PreserveVisibleUntilStableGrouping => {
            "preserve_visible_until_stable_grouping"
        }
    }
}

/// Parses and validates input for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `CatalogMutationPolicy` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_catalog_mutation_policy(value: String) -> Result<CatalogMutationPolicy, StorageError> {
    match value.as_str() {
        "preserve_visible_until_stable_grouping" => {
            Ok(CatalogMutationPolicy::PreserveVisibleUntilStableGrouping)
        }
        _ => invalid_value("import_jobs.catalog_mutation_policy", value),
    }
}

/// Handles import job source name for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `source`: `ImportJobSource`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn import_job_source_name(source: ImportJobSource) -> &'static str {
    match source {
        ImportJobSource::StartupInitialScan => "startup_initial_scan",
        ImportJobSource::DropboxWatcher => "dropbox_watcher",
        ImportJobSource::AdminInitialScan => "admin_initial_scan",
        ImportJobSource::AdminDropboxIngest => "admin_dropbox_ingest",
        ImportJobSource::AdminFullRescan => "admin_full_rescan",
        ImportJobSource::AdminSubtreeRescan => "admin_subtree_rescan",
        ImportJobSource::AdminProviderRepair => "admin_provider_repair",
        ImportJobSource::QuarantineRetry => "quarantine_retry",
    }
}

/// Parses and validates input for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `ImportJobSource` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_import_job_source(value: String) -> Result<ImportJobSource, StorageError> {
    match value.as_str() {
        "startup_initial_scan" => Ok(ImportJobSource::StartupInitialScan),
        "dropbox_watcher" => Ok(ImportJobSource::DropboxWatcher),
        "admin_initial_scan" => Ok(ImportJobSource::AdminInitialScan),
        "admin_dropbox_ingest" => Ok(ImportJobSource::AdminDropboxIngest),
        "admin_full_rescan" => Ok(ImportJobSource::AdminFullRescan),
        "admin_subtree_rescan" => Ok(ImportJobSource::AdminSubtreeRescan),
        "admin_provider_repair" => Ok(ImportJobSource::AdminProviderRepair),
        "quarantine_retry" => Ok(ImportJobSource::QuarantineRetry),
        _ => invalid_value("import_jobs.source", value),
    }
}

/// Handles quarantine reason name for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `reason`: `QuarantineReason`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn quarantine_reason_name(reason: QuarantineReason) -> &'static str {
    match reason {
        QuarantineReason::Duplicate => "duplicate",
        QuarantineReason::MetadataFailure => "metadata_failure",
        QuarantineReason::FileError => "file_error",
        QuarantineReason::UnsupportedFormat => "unsupported_format",
        QuarantineReason::ConflictingMetadata => "conflicting_metadata",
    }
}

/// Parses and validates input for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `QuarantineReason` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_quarantine_reason(value: String) -> Result<QuarantineReason, StorageError> {
    match value.as_str() {
        "duplicate" => Ok(QuarantineReason::Duplicate),
        "metadata_failure" => Ok(QuarantineReason::MetadataFailure),
        "file_error" => Ok(QuarantineReason::FileError),
        "unsupported_format" => Ok(QuarantineReason::UnsupportedFormat),
        "conflicting_metadata" => Ok(QuarantineReason::ConflictingMetadata),
        _ => invalid_value("quarantine_items.reason", value),
    }
}

/// Handles quarantine status name for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `status`: `QuarantineStatus`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `&'static str` borrowed or static text owned by the documented domain.
///
/// Errors:
/// - Does not return recoverable errors.
fn quarantine_status_name(status: QuarantineStatus) -> &'static str {
    match status {
        QuarantineStatus::Open => "open",
        QuarantineStatus::Retrying => "retrying",
        QuarantineStatus::Resolved => "resolved",
        QuarantineStatus::Deleted => "deleted",
    }
}

/// Parses and validates input for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `QuarantineStatus` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn parse_quarantine_status(value: String) -> Result<QuarantineStatus, StorageError> {
    match value.as_str() {
        "open" => Ok(QuarantineStatus::Open),
        "retrying" => Ok(QuarantineStatus::Retrying),
        "resolved" => Ok(QuarantineStatus::Resolved),
        "deleted" => Ok(QuarantineStatus::Deleted),
        _ => invalid_value("quarantine_items.status", value),
    }
}

/// Handles invalid value for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `field`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `value`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `T` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn invalid_value<T>(field: &'static str, value: String) -> Result<T, StorageError> {
    Err(StorageError::InvalidStoredValue { field, value })
}

/// Handles i32 to u32 for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `i32`; expected to be a numeric value within the range accepted by the target domain or database column.
/// - `field`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `u32` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn i32_to_u32(value: i32, field: &'static str) -> Result<u32, StorageError> {
    u32::try_from(value).map_err(|_| StorageError::InvalidStoredValue {
        field,
        value: value.to_string(),
    })
}

/// Handles optional i32 to u32 for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `Option<i32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
/// - `field`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Option<u32>` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn optional_i32_to_u32(
    value: Option<i32>,
    field: &'static str,
) -> Result<Option<u32>, StorageError> {
    value.map(|value| i32_to_u32(value, field)).transpose()
}

/// Handles u32 to i32 for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
/// - `field`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `i32` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn u32_to_i32(value: u32, field: &'static str) -> Result<i32, StorageError> {
    i32::try_from(value).map_err(|_| StorageError::InvalidStoredValue {
        field,
        value: value.to_string(),
    })
}

/// Handles optional u32 to i32 for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `value`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
/// - `field`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Option<i32>` on success or `StorageError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `StorageError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn optional_u32_to_i32(
    value: Option<u32>,
    field: &'static str,
) -> Result<Option<i32>, StorageError> {
    value.map(|value| u32_to_i32(value, field)).transpose()
}

/// Validates data for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `schema`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `()` on success or `ConfigError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ConfigError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn validate_schema_name(schema: &str) -> Result<(), ConfigError> {
    let is_valid = !schema.is_empty()
        && schema
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_');

    if is_valid {
        Ok(())
    } else {
        Err(ConfigError::InvalidSchema(schema.to_string()))
    }
}

/// Handles quote identifier for Postgres configuration, migrations, and maintenance repository persistence.
///
/// Inputs:
/// - `identifier`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}
