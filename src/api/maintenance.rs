use std::str::FromStr;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};
use uuid::Uuid;

use crate::{
    auth::AdminAccount,
    domain::{
        ImportJob, ImportJobKind, ImportJobSource, ImportJobStatus, MaintenanceScope,
        ProviderHealth, ProviderKind, ProviderStatus, RepairPlan,
    },
    error::{ApiError, ErrorResponse},
    pipeline::{EnqueueOutcome, ImportWorkRequest},
    providers::provider_refresh_ready_at,
    state::{AdminDashboardActiveImportJob, AppState},
    storage::CatalogImportFailure,
};

/// Builds the Axum router for maintenance operations and provider health.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `Router<AppState>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/maintenance/scans/initial", post(trigger_initial_scan))
        .route("/maintenance/ingests/dropbox", post(trigger_dropbox_ingest))
        .route("/maintenance/rescans/full", post(trigger_full_rescan))
        .route("/maintenance/rescans/subtree", post(trigger_subtree_rescan))
        .route(
            "/maintenance/provider-refreshes",
            post(trigger_provider_refresh),
        )
        .route("/maintenance/summary", get(dashboard_summary))
        .route("/maintenance/failures", get(list_import_failures))
        .route("/maintenance/readiness", get(maintenance_readiness))
        .route("/providers/health", get(list_provider_health))
        .route("/providers/:provider/repair", post(provider_repair))
        .route("/quarantine/retry", post(retry_quarantine_items))
        .route(
            "/quarantine/:item_id/retry",
            post(retry_quarantine_item),
        )
}

#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
/// Represents maintenance options request in the admin maintenance, provider health, and quarantine retry HTTP API.
///
/// Functionality: Carries fields `reason`, `refresh_provider_metadata`, `refresh_artwork`, `rewrite_sidecars`, `rebuild_search_projections` for admin maintenance, provider health, and quarantine retry HTTP API.
/// Dependencies: depends on `Option<String>`, `Option<bool>`, `Option<bool>`, `Option<bool>`, `Option<bool>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct MaintenanceOptionsRequest {
    pub reason: Option<String>,
    pub refresh_provider_metadata: Option<bool>,
    pub refresh_artwork: Option<bool>,
    pub rewrite_sidecars: Option<bool>,
    pub rebuild_search_projections: Option<bool>,
}

impl MaintenanceOptionsRequest {
    /// Handles into repair plan for maintenance operations and provider health.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `RepairPlan` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn into_repair_plan(self) -> RepairPlan {
        let defaults = RepairPlan::default();
        RepairPlan {
            refresh_provider_metadata: self
                .refresh_provider_metadata
                .unwrap_or(defaults.refresh_provider_metadata),
            refresh_artwork: self.refresh_artwork.unwrap_or(defaults.refresh_artwork),
            rewrite_sidecars: self.rewrite_sidecars.unwrap_or(defaults.rewrite_sidecars),
            rebuild_search_projections: self
                .rebuild_search_projections
                .unwrap_or(defaults.rebuild_search_projections),
            preserve_provenance_history: true,
            preserve_confidence_history: true,
        }
    }
}

#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
/// Represents initial scan request in the admin maintenance, provider health, and quarantine retry HTTP API.
///
/// Functionality: Carries fields `options` for admin maintenance, provider health, and quarantine retry HTTP API.
/// Dependencies: depends on `MaintenanceOptionsRequest` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct InitialScanRequest {
    #[serde(flatten)]
    pub options: MaintenanceOptionsRequest,
}

#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
/// Represents dropbox ingest request in the admin maintenance, provider health, and quarantine retry HTTP API.
///
/// Functionality: Carries fields `path`, `options` for admin maintenance, provider health, and quarantine retry HTTP API.
/// Dependencies: depends on `Option<String>`, `MaintenanceOptionsRequest` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct DropboxIngestRequest {
    pub path: Option<String>,
    #[serde(flatten)]
    pub options: MaintenanceOptionsRequest,
}

#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
/// Represents full rescan request in the admin maintenance, provider health, and quarantine retry HTTP API.
///
/// Functionality: Carries fields `options` for admin maintenance, provider health, and quarantine retry HTTP API.
/// Dependencies: depends on `MaintenanceOptionsRequest` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct FullRescanRequest {
    #[serde(flatten)]
    pub options: MaintenanceOptionsRequest,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
/// Represents subtree rescan request in the admin maintenance, provider health, and quarantine retry HTTP API.
///
/// Functionality: Carries fields `path`, `options` for admin maintenance, provider health, and quarantine retry HTTP API.
/// Dependencies: depends on `String`, `MaintenanceOptionsRequest` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct SubtreeRescanRequest {
    pub path: String,
    #[serde(flatten)]
    pub options: MaintenanceOptionsRequest,
}

#[derive(Debug, Clone, Default, Deserialize, ToSchema)]
/// Represents provider refresh request in the admin maintenance, provider health, and quarantine retry HTTP API.
///
/// Functionality: Carries fields `providers`, `path`, `options` for admin maintenance, provider health, and quarantine retry HTTP API.
/// Dependencies: depends on `Vec<ProviderKind>`, `Option<String>`, `MaintenanceOptionsRequest` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct ProviderRefreshRequest {
    #[serde(default)]
    pub providers: Vec<ProviderKind>,
    pub path: Option<String>,
    #[serde(flatten)]
    pub options: MaintenanceOptionsRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents maintenance operation response in the admin maintenance, provider health, and quarantine retry HTTP API.
///
/// Functionality: Carries fields `job`, `reused_existing` for admin maintenance, provider health, and quarantine retry HTTP API.
/// Dependencies: depends on `ImportJob`, `bool` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct MaintenanceOperationResponse {
    pub job: ImportJob,
    pub reused_existing: bool,
}

impl From<EnqueueOutcome> for MaintenanceOperationResponse {
    /// Converts from the source domain type for maintenance operations and provider health.
    ///
    /// Inputs:
    /// - `outcome`: `EnqueueOutcome`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn from(outcome: EnqueueOutcome) -> Self {
        Self {
            job: outcome.job,
            reused_existing: outcome.reused_existing,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents active import job progress in the admin maintenance, provider health, and quarantine retry HTTP API.
///
/// Functionality: Carries fields `id`, `kind`, `status`, `scope`, `reason`, `attempts`, `processed_files`, `published_files`, `quarantined_files`, `failed_files`, `created_at`, `updated_at`, `last_progress_at` for admin maintenance, provider health, and quarantine retry HTTP API.
/// Dependencies: depends on `Uuid`, `ImportJobKind`, `ImportJobStatus`, `MaintenanceScope`, `Option<String>`, and 8 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct DashboardActiveImportJobResponse {
    pub id: Uuid,
    pub kind: ImportJobKind,
    pub status: ImportJobStatus,
    pub scope: MaintenanceScope,
    pub reason: Option<String>,
    pub attempts: u32,
    pub processed_files: i64,
    pub published_files: i64,
    pub quarantined_files: i64,
    pub failed_files: i64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub last_progress_at: Option<DateTime<Utc>>,
}

impl From<AdminDashboardActiveImportJob> for DashboardActiveImportJobResponse {
    /// Builds an API response from application state import progress.
    ///
    /// Inputs:
    /// - `progress`: `AdminDashboardActiveImportJob`; expected to carry an active import job plus aggregate work item counts.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn from(progress: AdminDashboardActiveImportJob) -> Self {
        let job = progress.job;
        Self {
            id: job.id,
            kind: job.kind,
            status: job.status,
            scope: job.scope,
            reason: job.reason,
            attempts: job.attempts,
            processed_files: progress.processed_files,
            published_files: progress.published_files,
            quarantined_files: progress.quarantined_files,
            failed_files: progress.failed_files,
            created_at: job.created_at,
            updated_at: job.updated_at,
            last_progress_at: progress.last_progress_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents dashboard summary response in the admin maintenance, provider health, and quarantine retry HTTP API.
///
/// Functionality: Carries fields `scanning`, `imported`, `quarantined`, `failed`, `artists`, `albums`, `tracks`, `playlists`, `active_jobs` for admin maintenance, provider health, and quarantine retry HTTP API.
/// Dependencies: depends on `i64`, `i64`, `i64`, `i64`, `i64`, and 4 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`, `tests/maintenance_api.rs`.
pub struct DashboardSummaryResponse {
    pub scanning: i64,
    pub imported: i64,
    pub quarantined: i64,
    pub failed: i64,
    pub artists: i64,
    pub albums: i64,
    pub tracks: i64,
    pub playlists: i64,
    pub active_jobs: Vec<DashboardActiveImportJobResponse>,
}

#[derive(Debug, Clone, Default, Deserialize, IntoParams, ToSchema)]
/// Represents import failure list query parameters in the admin maintenance HTTP API.
///
/// Functionality: Carries optional `import_job_id` and `limit` filters for admin failure diagnostics.
/// Dependencies: depends on `Option<Uuid>` and `Option<u32>`.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct ImportFailuresQuery {
    pub import_job_id: Option<Uuid>,
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents one failed import work item in the admin maintenance HTTP API.
///
/// Functionality: Carries the source path, stored failure detail, job context, attempts, and timestamps for admin failure diagnostics.
/// Dependencies: depends on `Uuid`, `ImportJobKind`, `ImportJobStatus`, `DateTime<Utc>`, and scalar fields.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct ImportFailureResponse {
    pub id: Uuid,
    pub import_job_id: Uuid,
    pub import_job_kind: ImportJobKind,
    pub import_job_status: ImportJobStatus,
    pub source_path: String,
    pub media_file_id: Option<Uuid>,
    pub status: crate::domain::MediaFileStatus,
    pub attempts: u32,
    pub last_error: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl From<CatalogImportFailure> for ImportFailureResponse {
    /// Builds an API response from a persisted import failure row.
    ///
    /// Inputs:
    /// - `failure`: persisted failure diagnostic row.
    ///
    /// Output:
    /// - Returns `Self`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn from(failure: CatalogImportFailure) -> Self {
        Self {
            id: failure.id,
            import_job_id: failure.import_job_id,
            import_job_kind: failure.import_job_kind,
            import_job_status: failure.import_job_status,
            source_path: failure.source_path,
            media_file_id: failure.media_file_id,
            status: failure.status,
            attempts: failure.attempts,
            last_error: failure.last_error,
            created_at: failure.created_at,
            updated_at: failure.updated_at,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents import failure list response in the admin maintenance HTTP API.
///
/// Functionality: Carries recent failed import work items for admin failure diagnostics.
/// Dependencies: depends on `Vec<ImportFailureResponse>`.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct ImportFailuresResponse {
    pub failures: Vec<ImportFailureResponse>,
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/maintenance/summary",
    tag = "maintenance",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Coarse admin dashboard import summary", body = DashboardSummaryResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse)
    )
)]
/// Handles dashboard summary for maintenance operations and provider health.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Json<DashboardSummaryResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn dashboard_summary(
    State(state): State<AppState>,
    _admin: AdminAccount,
) -> Result<Json<DashboardSummaryResponse>, ApiError> {
    let summary = state.admin_dashboard_summary_counts().await?;
    Ok(Json(DashboardSummaryResponse {
        scanning: summary.scanning,
        imported: summary.imported,
        quarantined: summary.quarantined,
        failed: summary.failed,
        artists: summary.artists,
        albums: summary.albums,
        tracks: summary.tracks,
        playlists: summary.playlists,
        active_jobs: summary.active_jobs.into_iter().map(Into::into).collect(),
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/maintenance/failures",
    tag = "maintenance",
    security(("basicAuth" = [])),
    params(ImportFailuresQuery),
    responses(
        (status = 200, description = "Recent failed import work items", body = ImportFailuresResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse)
    )
)]
/// Lists failed import work items for maintenance diagnostics.
///
/// Inputs:
/// - `State(state)`: Axum application state with a live repository.
/// - `_admin`: authenticated admin account.
/// - `Query(query)`: optional import job id and limit filters.
///
/// Output:
/// - Returns recent failed import work item details.
///
/// Errors:
/// - Returns `ApiError` when persistence fails.
pub async fn list_import_failures(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Query(query): Query<ImportFailuresQuery>,
) -> Result<Json<ImportFailuresResponse>, ApiError> {
    let failures = state
        .admin_import_failures(query.import_job_id, query.limit.unwrap_or(100))
        .await?;
    Ok(Json(ImportFailuresResponse {
        failures: failures.into_iter().map(Into::into).collect(),
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/maintenance/scans/initial",
    tag = "maintenance",
    security(("basicAuth" = [])),
    request_body = InitialScanRequest,
    responses(
        (status = 202, description = "Initial library and dropbox scan accepted", body = MaintenanceOperationResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse)
    )
)]
/// Starts an asynchronous operation for maintenance operations and provider health.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Json(request)`: `Json<InitialScanRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `(StatusCode, Json<MaintenanceOperationResponse>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn trigger_initial_scan(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Json(request): Json<InitialScanRequest>,
) -> Result<(StatusCode, Json<MaintenanceOperationResponse>), ApiError> {
    let reason = request.options.reason.clone();
    let repair_plan = request.options.into_repair_plan();
    let work = ImportWorkRequest {
        kind: ImportJobKind::InitialScan,
        scope: MaintenanceScope::FullLibrary,
        repair_plan,
        provider_filter: Vec::new(),
        source: ImportJobSource::AdminInitialScan,
        reason,
        related_quarantine_item_id: None,
    };

    Ok((
        StatusCode::ACCEPTED,
        Json(state.enqueue_import_work(work).await?.into()),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/maintenance/ingests/dropbox",
    tag = "maintenance",
    security(("basicAuth" = [])),
    request_body = DropboxIngestRequest,
    responses(
        (status = 202, description = "Dropbox ingest accepted", body = MaintenanceOperationResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse),
        (status = 400, description = "Invalid dropbox path", body = ErrorResponse)
    )
)]
/// Starts an asynchronous operation for maintenance operations and provider health.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Json(request)`: `Json<DropboxIngestRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `(StatusCode, Json<MaintenanceOperationResponse>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn trigger_dropbox_ingest(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Json(request): Json<DropboxIngestRequest>,
) -> Result<(StatusCode, Json<MaintenanceOperationResponse>), ApiError> {
    let reason = request.options.reason.clone();
    let outcome = if request.options.refresh_provider_metadata.is_none()
        && request.options.refresh_artwork.is_none()
        && request.options.rewrite_sidecars.is_none()
        && request.options.rebuild_search_projections.is_none()
    {
        state
            .enqueue_dropbox_ingest(request.path.as_deref(), reason)
            .await?
    } else {
        let repair_plan = request.options.into_repair_plan();
        let scope = state.normalize_dropbox_scope(request.path.as_deref())?;
        state
            .enqueue_import_work(ImportWorkRequest {
                kind: ImportJobKind::DropboxIngest,
                scope,
                repair_plan,
                provider_filter: Vec::new(),
                source: ImportJobSource::AdminDropboxIngest,
                reason,
                related_quarantine_item_id: None,
            })
            .await?
    };

    Ok((StatusCode::ACCEPTED, Json(outcome.into())))
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents provider health response in the admin maintenance, provider health, and quarantine retry HTTP API.
///
/// Functionality: Carries fields `providers` for admin maintenance, provider health, and quarantine retry HTTP API.
/// Dependencies: depends on `Vec<ProviderHealth>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct ProviderHealthResponse {
    pub providers: Vec<ProviderHealth>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents maintenance readiness response in the admin maintenance, provider health, and quarantine retry HTTP API.
///
/// Functionality: Carries fields `can_start_rescan`, `can_refresh_provider_metadata`, `degraded_providers`, `backing_off_providers`, `warnings` for admin maintenance, provider health, and quarantine retry HTTP API.
/// Dependencies: depends on `bool`, `bool`, `Vec<ProviderKind>`, `Vec<ProviderKind>`, `Vec<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct MaintenanceReadinessResponse {
    pub can_start_rescan: bool,
    pub can_refresh_provider_metadata: bool,
    pub degraded_providers: Vec<ProviderKind>,
    pub backing_off_providers: Vec<ProviderKind>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
/// Represents quarantine retry request in the admin maintenance, provider health, and quarantine retry HTTP API.
///
/// Functionality: Carries fields `item_ids`, `options` for admin maintenance, provider health, and quarantine retry HTTP API.
/// Dependencies: depends on `Vec<Uuid>`, `MaintenanceOptionsRequest` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct QuarantineRetryRequest {
    pub item_ids: Vec<Uuid>,
    #[serde(flatten)]
    pub options: MaintenanceOptionsRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents quarantine retry response in the admin maintenance, provider health, and quarantine retry HTTP API.
///
/// Functionality: Carries fields `jobs`, `retried_item_ids` for admin maintenance, provider health, and quarantine retry HTTP API.
/// Dependencies: depends on `Vec<ImportJob>`, `Vec<Uuid>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/api/openapi.rs`.
pub struct QuarantineRetryResponse {
    pub jobs: Vec<ImportJob>,
    pub retried_item_ids: Vec<Uuid>,
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/maintenance/rescans/full",
    tag = "maintenance",
    security(("basicAuth" = [])),
    request_body = FullRescanRequest,
    responses(
        (status = 202, description = "Full library rescan accepted", body = MaintenanceOperationResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse)
    )
)]
/// Starts an asynchronous operation for maintenance operations and provider health.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Json(request)`: `Json<FullRescanRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `(StatusCode, Json<MaintenanceOperationResponse>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn trigger_full_rescan(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Json(request): Json<FullRescanRequest>,
) -> Result<(StatusCode, Json<MaintenanceOperationResponse>), ApiError> {
    let reason = request.options.reason.clone();
    let repair_plan = request.options.into_repair_plan();
    let work = ImportWorkRequest {
        kind: ImportJobKind::FullRescan,
        scope: MaintenanceScope::FullLibrary,
        repair_plan,
        provider_filter: Vec::new(),
        source: ImportJobSource::AdminFullRescan,
        reason,
        related_quarantine_item_id: None,
    };

    Ok((
        StatusCode::ACCEPTED,
        Json(state.enqueue_import_work(work).await?.into()),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/maintenance/rescans/subtree",
    tag = "maintenance",
    security(("basicAuth" = [])),
    request_body = SubtreeRescanRequest,
    responses(
        (status = 202, description = "Path or subtree rescan accepted", body = MaintenanceOperationResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse),
        (status = 400, description = "Invalid path", body = ErrorResponse)
    )
)]
/// Starts an asynchronous operation for maintenance operations and provider health.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Json(request)`: `Json<SubtreeRescanRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `(StatusCode, Json<MaintenanceOperationResponse>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn trigger_subtree_rescan(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Json(request): Json<SubtreeRescanRequest>,
) -> Result<(StatusCode, Json<MaintenanceOperationResponse>), ApiError> {
    let scope = state.normalize_maintenance_scope(Some(&request.path))?;
    let reason = request.options.reason.clone();
    let repair_plan = request.options.into_repair_plan();
    let work = ImportWorkRequest {
        kind: ImportJobKind::SubtreeRescan,
        scope,
        repair_plan,
        provider_filter: Vec::new(),
        source: ImportJobSource::AdminSubtreeRescan,
        reason,
        related_quarantine_item_id: None,
    };

    Ok((
        StatusCode::ACCEPTED,
        Json(state.enqueue_import_work(work).await?.into()),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/maintenance/provider-refreshes",
    tag = "maintenance",
    security(("basicAuth" = [])),
    request_body = ProviderRefreshRequest,
    responses(
        (status = 202, description = "Provider metadata refresh accepted", body = MaintenanceOperationResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 409, description = "Provider cannot be repaired", body = ErrorResponse)
    )
)]
/// Starts an asynchronous operation for maintenance operations and provider health.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Json(request)`: `Json<ProviderRefreshRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `(StatusCode, Json<MaintenanceOperationResponse>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn trigger_provider_refresh(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Json(request): Json<ProviderRefreshRequest>,
) -> Result<(StatusCode, Json<MaintenanceOperationResponse>), ApiError> {
    enqueue_provider_refresh(state, request).await
}

/// Enqueues background work for maintenance operations and provider health.
///
/// Inputs:
/// - `state`: `AppState`; expected to be Axum application state with a live repository and runtime configuration.
/// - `request`: `ProviderRefreshRequest`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `(StatusCode, Json<MaintenanceOperationResponse>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn enqueue_provider_refresh(
    state: AppState,
    request: ProviderRefreshRequest,
) -> Result<(StatusCode, Json<MaintenanceOperationResponse>), ApiError> {
    let providers = provider_filter_or_enabled(&state, request.providers).await?;
    for provider in &providers {
        state.prepare_provider_admin_retry(*provider).await?;
    }

    let scope = state.normalize_maintenance_scope(request.path.as_deref())?;
    let reason = request.options.reason.clone();
    let repair_plan = request.options.into_repair_plan();
    let work = ImportWorkRequest {
        kind: ImportJobKind::ProviderRepair,
        scope,
        repair_plan,
        provider_filter: providers,
        source: ImportJobSource::AdminProviderRepair,
        reason,
        related_quarantine_item_id: None,
    };

    Ok((
        StatusCode::ACCEPTED,
        Json(state.enqueue_import_work(work).await?.into()),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/providers/{provider}/repair",
    tag = "providers",
    security(("basicAuth" = [])),
    params(("provider" = String, Path, description = "Provider identifier, for example music_brainz or discogs")),
    request_body = ProviderRefreshRequest,
    responses(
        (status = 202, description = "Provider repair accepted", body = MaintenanceOperationResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse),
        (status = 400, description = "Invalid provider or path", body = ErrorResponse),
        (status = 409, description = "Provider cannot be repaired", body = ErrorResponse)
    )
)]
/// Handles provider repair for maintenance operations and provider health.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(provider)`: `Path<String>`; expected to be a route or domain identifier that must parse to the expected type.
/// - `Json(mut request)`: `Json<ProviderRefreshRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `(StatusCode, Json<MaintenanceOperationResponse>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn provider_repair(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Path(provider): Path<String>,
    Json(mut request): Json<ProviderRefreshRequest>,
) -> Result<(StatusCode, Json<MaintenanceOperationResponse>), ApiError> {
    let provider = ProviderKind::from_str(&provider)
        .map_err(|_| ApiError::BadRequest(format!("unknown provider: {provider}")))?;
    request.providers = vec![provider];
    enqueue_provider_refresh(state, request).await
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/providers/health",
    tag = "providers",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Provider health", body = ProviderHealthResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse)
    )
)]
/// Lists resources for maintenance operations and provider health.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Json<ProviderHealthResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn list_provider_health(
    State(state): State<AppState>,
    _admin: AdminAccount,
) -> Result<Json<ProviderHealthResponse>, ApiError> {
    Ok(Json(ProviderHealthResponse {
        providers: state.provider_health().await?,
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/admin/maintenance/readiness",
    tag = "maintenance",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Metadata maintenance readiness", body = MaintenanceReadinessResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse)
    )
)]
/// Handles maintenance readiness for maintenance operations and provider health.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Json<MaintenanceReadinessResponse>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn maintenance_readiness(
    State(state): State<AppState>,
    _admin: AdminAccount,
) -> Result<Json<MaintenanceReadinessResponse>, ApiError> {
    let providers = state.provider_health().await?;
    let active_jobs = state.active_import_jobs().await?;
    let system_config = state.system_config();
    let now = chrono::Utc::now();
    let managed_roots_configured = !system_config.library_root.is_empty()
        && !system_config.dropbox_root.is_empty();
    let mut degraded_providers = Vec::new();
    let mut backing_off_providers = Vec::new();
    let mut warnings = Vec::new();
    let can_refresh_provider_metadata = providers
        .iter()
        .any(|provider| provider_refresh_ready_at(provider, &now));

    for provider in &providers {
        if provider.status == ProviderStatus::BackingOff
            && !provider_refresh_ready_at(provider, &now)
        {
            backing_off_providers.push(provider.provider);
            warnings.push(format!(
                "{} is backing off until {}",
                provider.display_name.as_str(),
                provider
                    .retry_after
                    .as_ref()
                    .map(|value| value.to_rfc3339())
                    .unwrap_or_else(|| "a later retry window".to_string())
            ));
            continue;
        }

        match provider.status {
            ProviderStatus::Degraded | ProviderStatus::Unconfigured => {
                degraded_providers.push(provider.provider);
                warnings.push(format!(
                    "{} is {:?}: {}",
                    provider.display_name.as_str(),
                    provider.status,
                    provider
                        .message
                        .as_deref()
                        .unwrap_or("metadata quality may be reduced")
                ));
            }
            ProviderStatus::Healthy
            | ProviderStatus::BackingOff
            | ProviderStatus::Disabled => {}
        }
    }

    if !managed_roots_configured {
        warnings.push("Managed library and dropbox roots must be configured.".into());
    }
    if !active_jobs.is_empty() {
        warnings.push(format!(
            "{} active import job(s) are already queued or running.",
            active_jobs.len()
        ));
    }
    if !can_refresh_provider_metadata {
        warnings.push("No configured metadata providers are available for refresh.".into());
    }

    Ok(Json(MaintenanceReadinessResponse {
        can_start_rescan: managed_roots_configured
            && active_jobs.is_empty()
            && can_refresh_provider_metadata,
        can_refresh_provider_metadata,
        degraded_providers,
        backing_off_providers,
        warnings,
    }))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/quarantine/retry",
    tag = "quarantine",
    security(("basicAuth" = [])),
    request_body = QuarantineRetryRequest,
    responses(
        (status = 202, description = "Quarantine items handed back to import pipeline", body = QuarantineRetryResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse),
        (status = 400, description = "Invalid request", body = ErrorResponse),
        (status = 404, description = "Missing quarantine item", body = ErrorResponse),
        (status = 409, description = "Item cannot be retried", body = ErrorResponse)
    )
)]
/// Handles retry quarantine items for maintenance operations and provider health.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Json(request)`: `Json<QuarantineRetryRequest>`; expected to be a deserialized JSON request body that matches the API schema.
///
/// Output:
/// - Returns `(StatusCode, Json<QuarantineRetryResponse>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn retry_quarantine_items(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Json(request): Json<QuarantineRetryRequest>,
) -> Result<(StatusCode, Json<QuarantineRetryResponse>), ApiError> {
    enqueue_quarantine_retries(state, request).await
}

/// Enqueues background work for maintenance operations and provider health.
///
/// Inputs:
/// - `state`: `AppState`; expected to be Axum application state with a live repository and runtime configuration.
/// - `request`: `QuarantineRetryRequest`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `(StatusCode, Json<QuarantineRetryResponse>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn enqueue_quarantine_retries(
    state: AppState,
    request: QuarantineRetryRequest,
) -> Result<(StatusCode, Json<QuarantineRetryResponse>), ApiError> {
    if request.item_ids.is_empty() {
        return Err(ApiError::BadRequest(
            "at least one quarantine item id is required".into(),
        ));
    }

    let repair_plan = request.options.into_repair_plan();
    let retried = state
        .enqueue_quarantine_retries(request.item_ids, repair_plan)
        .await?;
    let mut jobs = Vec::with_capacity(retried.len());
    let mut retried_item_ids = Vec::with_capacity(retried.len());
    for (item_id, job) in retried {
        retried_item_ids.push(item_id);
        jobs.push(job);
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(QuarantineRetryResponse {
            jobs,
            retried_item_ids,
        }),
    ))
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/quarantine/{item_id}/retry",
    tag = "quarantine",
    security(("basicAuth" = [])),
    params(("item_id" = Uuid, Path, description = "Quarantine item id")),
    responses(
        (status = 202, description = "Quarantine item handed back to import pipeline", body = QuarantineRetryResponse),
        (status = 401, description = "Authentication required", body = ErrorResponse),
        (status = 403, description = "Admin role required", body = ErrorResponse),
        (status = 404, description = "Missing quarantine item", body = ErrorResponse),
        (status = 409, description = "Item cannot be retried", body = ErrorResponse)
    )
)]
/// Handles retry quarantine item for maintenance operations and provider health.
///
/// Inputs:
/// - `State(state)`: `State<AppState>`; expected to be Axum application state with a live repository and runtime configuration.
/// - `_admin`: `AdminAccount`; expected to be a value satisfying the type contract shown in the function signature.
/// - `Path(item_id)`: `Path<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
///
/// Output:
/// - Returns `(StatusCode, Json<QuarantineRetryResponse>)` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
pub async fn retry_quarantine_item(
    State(state): State<AppState>,
    _admin: AdminAccount,
    Path(item_id): Path<Uuid>,
) -> Result<(StatusCode, Json<QuarantineRetryResponse>), ApiError> {
    enqueue_quarantine_retries(
        state,
        QuarantineRetryRequest {
            item_ids: vec![item_id],
            options: MaintenanceOptionsRequest::default(),
        },
    )
    .await
}

/// Handles provider filter or enabled for maintenance operations and provider health.
///
/// Inputs:
/// - `state`: `&AppState`; expected to be Axum application state with a live repository and runtime configuration.
/// - `requested`: `Vec<ProviderKind>`; expected to be one of the supported metadata provider identifiers.
///
/// Output:
/// - Returns `Vec<ProviderKind>` on success or `ApiError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ApiError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn provider_filter_or_enabled(
    state: &AppState,
    mut requested: Vec<ProviderKind>,
) -> Result<Vec<ProviderKind>, ApiError> {
    if !requested.is_empty() {
        requested.sort();
        requested.dedup();

        for provider in &requested {
            let health = state
                .provider(*provider)
                .await?
                .ok_or_else(|| ApiError::NotFound(format!("unknown provider: {provider}")))?;
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
        }

        return Ok(requested);
    }

    let mut providers = state
        .provider_health()
        .await?
        .into_iter()
        .filter(|provider| {
            provider.enabled && provider.status != ProviderStatus::Unconfigured
        })
        .map(|provider| provider.provider)
        .collect::<Vec<_>>();
    providers.sort();
    providers.dedup();

    if providers.is_empty() {
        return Err(ApiError::Conflict(
            "no configured metadata providers are available".into(),
        ));
    }

    Ok(providers)
}
