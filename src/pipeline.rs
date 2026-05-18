use std::{
    collections::{BTreeSet, HashMap},
    fs,
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use chrono::{Duration as ChronoDuration, Utc};
use reqwest::header;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use thiserror::Error;
use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    task::{self, JoinSet},
};
use utoipa::ToSchema;
use uuid::Uuid;
use walkdir::WalkDir;
use tracing::warn;

use crate::{
    catalog::{likely_compilation_artist, sanitize_path_component},
    domain::{
        AlbumKind, ArtworkAssetDraft, ArtworkKind, CatalogEntityType, CatalogGrouping,
        CatalogImportDecision, CatalogImportRequest, CatalogMutationPolicy, ImportJob,
        ImportJobKind, ImportJobSource, ImportJobStatus, MaintenanceScope, MediaFile,
        MediaFileStatus, MediaKind, MetadataProvenanceDraft, MusicCatalogGrouping,
        PodcastCatalogGrouping,
        ProviderHealth, ProviderKind, RepairPlan, SystemConfig,
        DEFAULT_SCAN_THREAD_COUNT,
    },
    media::{is_supported_media_path, probe_media_file, MediaProbeError, ProbedMediaFile},
    providers::{
        provider_backoff_active_at, provider_supports_media_kind,
        reconcile_provider_readiness, ProviderCredential, ProviderExecutionOutcome,
        ProviderMetadataBundle, ProviderRegistry,
        PROVIDER_AUTO_ACCEPT_CONFIDENCE_THRESHOLD,
    },
    storage::{PgMaintenanceRepository, StorageError},
};

pub const IMPORT_PIPELINE_NAME: &str = "import_pipeline";
const IMPORT_JOB_MAX_ATTEMPTS: u32 = 3;
const FILE_OPERATION_MAX_ATTEMPTS: u32 = 3;
const FILE_OPERATION_RETRY_BACKOFF: Duration = Duration::from_millis(50);
const REMOTE_ARTWORK_TIMEOUT: Duration = Duration::from_secs(10);
const REMOTE_ARTWORK_MAX_BYTES: u64 = 10 * 1024 * 1024;
type SharedProviderBackoff = Arc<Mutex<BTreeSet<ProviderKind>>>;

#[derive(Debug, Clone)]
/// Represents import work request in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Carries fields `kind`, `scope`, `repair_plan`, `provider_filter`, `source`, `reason`, `related_quarantine_item_id` for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: depends on `ImportJobKind`, `MaintenanceScope`, `RepairPlan`, `Vec<ProviderKind>`, `ImportJobSource`, `Option<String>`, and 1 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/pipeline.rs`, `src/state.rs`, `src/storage.rs`, and 1 more.
pub struct ImportWorkRequest {
    pub kind: ImportJobKind,
    pub scope: MaintenanceScope,
    pub repair_plan: RepairPlan,
    pub provider_filter: Vec<ProviderKind>,
    pub source: ImportJobSource,
    pub reason: Option<String>,
    pub related_quarantine_item_id: Option<Uuid>,
}

impl ImportWorkRequest {
    /// Handles idempotency key for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `String` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn idempotency_key(&self) -> String {
        let mut providers = self.provider_filter.clone();
        providers.sort();
        let provider_fragment = providers
            .into_iter()
            .map(|provider| provider.api_name())
            .collect::<Vec<_>>()
            .join(",");

        let quarantine_fragment = self
            .related_quarantine_item_id
            .map(|id| format!("|quarantine:{id}"))
            .unwrap_or_default();

        format!(
            "{}|{}|{}|providers:{}{}",
            self.kind.api_name(),
            self.scope.idempotency_fragment(),
            self.repair_plan.idempotency_fragment(),
            provider_fragment,
            quarantine_fragment
        )
    }

    /// Handles into job for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `ImportJob` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn into_job(self) -> ImportJob {
        let now = Utc::now();
        let idempotency_key = self.idempotency_key();

        ImportJob {
            id: Uuid::new_v4(),
            kind: self.kind,
            status: ImportJobStatus::Queued,
            scope: self.scope,
            repair_plan: self.repair_plan,
            catalog_mutation_policy: CatalogMutationPolicy::PreserveVisibleUntilStableGrouping,
            provider_filter: self.provider_filter,
            pipeline: IMPORT_PIPELINE_NAME.to_string(),
            source: self.source,
            reason: self.reason,
            related_quarantine_item_id: self.related_quarantine_item_id,
            idempotency_key,
            attempts: 0,
            created_at: now,
            updated_at: now,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents enqueue outcome in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Carries fields `job`, `reused_existing` for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: depends on `ImportJob`, `bool` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/api/maintenance.rs`, `src/pipeline.rs`, `src/state.rs`.
pub struct EnqueueOutcome {
    pub job: ImportJob,
    pub reused_existing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
/// Represents import run summary in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Carries fields `job`, `scanned_files`, `published_files`, `reused_files`, `quarantined_files`, `duplicate_files`, `failed_files` for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: depends on `ImportJob`, `u32`, `u32`, `u32`, `u32`, `u32`, and 1 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`, `src/state.rs`.
pub struct ImportRunSummary {
    pub job: ImportJob,
    pub scanned_files: u32,
    pub published_files: u32,
    pub reused_files: u32,
    pub quarantined_files: u32,
    pub duplicate_files: u32,
    pub failed_files: u32,
}

impl ImportRunSummary {
    /// Constructs a new instance for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - `job`: `ImportJob`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn new(job: ImportJob) -> Self {
        Self {
            job,
            scanned_files: 0,
            published_files: 0,
            reused_files: 0,
            quarantined_files: 0,
            duplicate_files: 0,
            failed_files: 0,
        }
    }
}

#[derive(Debug, Error)]
/// Represents import pipeline error in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Enumerates `Storage`, `MediaProbe`, `FileOperation`, `JobNotFound`, `JobNotRunnable` states or choices for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`, `src/state.rs`.
pub enum ImportPipelineError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    MediaProbe(#[from] MediaProbeError),
    #[error("file operation failed: {0}")]
    FileOperation(#[from] io::Error),
    #[error("import job {0} was not found")]
    JobNotFound(Uuid),
    #[error("import job {job_id} is not runnable because it is {status:?}")]
    JobNotRunnable {
        job_id: Uuid,
        status: ImportJobStatus,
    },
    #[error("maintenance path is not importable: {0}")]
    InvalidScope(String),
    #[error("import scan task failed: {0}")]
    ScanTaskJoin(#[from] tokio::task::JoinError),
}

#[derive(Debug, Clone)]
/// Represents import pipeline in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Carries fields `repository`, `system_config`, `provider_health` for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: depends on `PgMaintenanceRepository`, `SystemConfig`, `Vec<ProviderHealth>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`, `src/state.rs`.
pub struct ImportPipeline {
    repository: PgMaintenanceRepository,
    system_config: SystemConfig,
    provider_health: Vec<ProviderHealth>,
    run_locks: Arc<ImportRunLocks>,
}

/// Represents provider registry for job in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Carries fields `registry`, `providers_backing_off` for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: depends on `ProviderRegistry`, `BTreeSet<ProviderKind>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`.
struct ProviderRegistryForJob {
    registry: ProviderRegistry,
    providers_backing_off: BTreeSet<ProviderKind>,
}

#[derive(Debug, Default)]
/// Coordinates per-run locks for import paths that may touch the same target file, folder sidecar, or catalog duplicate key.
struct ImportRunLocks {
    locks: Mutex<HashMap<String, Arc<Semaphore>>>,
}

/// Represents a completed per-path import attempt.
enum ImportTaskOutcome {
    Imported(CatalogImportDecision),
    Failed,
}

impl ImportRunLocks {
    /// Acquires deterministic per-key locks for one import path.
    async fn acquire(&self, mut keys: Vec<String>) -> Vec<OwnedSemaphorePermit> {
        keys.retain(|key| !key.trim().is_empty());
        keys.sort();
        keys.dedup();

        let semaphores = {
            let mut locks = self.locks.lock().await;
            keys.into_iter()
                .map(|key| {
                    locks
                        .entry(key)
                        .or_insert_with(|| Arc::new(Semaphore::new(1)))
                        .clone()
                })
                .collect::<Vec<_>>()
        };

        let mut permits = Vec::with_capacity(semaphores.len());
        for semaphore in semaphores {
            let permit = semaphore
                .acquire_owned()
                .await
                .expect("import run lock semaphore closed");
            permits.push(permit);
        }
        permits
    }
}

impl ImportPipeline {
    /// Constructs a new instance for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - `repository`: `PgMaintenanceRepository`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `system_config`: `SystemConfig`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `provider_health`: `Vec<ProviderHealth>`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn new(
        repository: PgMaintenanceRepository,
        system_config: SystemConfig,
        provider_health: Vec<ProviderHealth>,
    ) -> Self {
        Self {
            repository,
            system_config,
            provider_health,
            run_locks: Arc::new(ImportRunLocks::default()),
        }
    }

    /// Runs the operation for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job_id`: `Uuid`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `ImportRunSummary` on success or `ImportPipelineError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ImportPipelineError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn run_job(&self, job_id: Uuid) -> Result<ImportRunSummary, ImportPipelineError> {
        let Some(job) = self.repository.claim_import_job(job_id).await? else {
            let job = self
                .repository
                .import_job(job_id)
                .await?
                .ok_or(ImportPipelineError::JobNotFound(job_id))?;
            return Err(ImportPipelineError::JobNotRunnable {
                job_id,
                status: job.status,
            });
        };
        self.run_claimed(job).await
    }

    /// Runs the operation for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job`: `ImportJob`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `ImportRunSummary` on success or `ImportPipelineError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ImportPipelineError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn run(&self, job: ImportJob) -> Result<ImportRunSummary, ImportPipelineError> {
        let Some(running_job) = self.repository.claim_import_job(job.id).await? else {
            let current = self
                .repository
                .import_job(job.id)
                .await?
                .ok_or(ImportPipelineError::JobNotFound(job.id))?;
            return Err(ImportPipelineError::JobNotRunnable {
                job_id: current.id,
                status: current.status,
            });
        };
        self.run_claimed(running_job).await
    }

    /// Runs the operation for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `running_job`: `ImportJob`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `ImportRunSummary` on success or `ImportPipelineError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ImportPipelineError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    pub async fn run_claimed(
        &self,
        running_job: ImportJob,
    ) -> Result<ImportRunSummary, ImportPipelineError> {
        if running_job.status != ImportJobStatus::Running {
            return Err(ImportPipelineError::JobNotRunnable {
                job_id: running_job.id,
                status: running_job.status,
            });
        }

        let running_job = self
            .repository
            .import_job(running_job.id)
            .await?
            .unwrap_or(running_job);
        let mut summary = ImportRunSummary::new(running_job.clone());
        let provider_credentials = self.repository.provider_credentials().await?;
        let import_paths = self.resolve_import_paths(&running_job)?;

        let run_result = async {
            let paths = collect_media_paths(import_paths).await?;
            summary.scanned_files = u32::try_from(paths.len()).unwrap_or(u32::MAX);

            let provider_credentials = Arc::new(provider_credentials);
            let providers_backing_off_in_run = Arc::new(Mutex::new(BTreeSet::new()));
            self.run_import_paths(
                &running_job,
                provider_credentials,
                providers_backing_off_in_run,
                paths,
                &mut summary,
            )
            .await?;

            Ok::<(), ImportPipelineError>(())
        }
        .await;

        let terminal_status = if run_result.is_ok() {
            ImportJobStatus::Completed
        } else if running_job.attempts >= IMPORT_JOB_MAX_ATTEMPTS {
            ImportJobStatus::Failed
        } else {
            ImportJobStatus::Retrying
        };
        let final_job = self
            .repository
            .update_import_job_status(running_job.id, terminal_status)
            .await?
            .unwrap_or(running_job);
        summary.job = final_job;

        run_result?;
        Ok(summary)
    }

    /// Runs import path work with bounded concurrency.
    async fn run_import_paths(
        &self,
        running_job: &ImportJob,
        provider_credentials: Arc<Vec<ProviderCredential>>,
        providers_backing_off_in_run: SharedProviderBackoff,
        paths: Vec<PathBuf>,
        summary: &mut ImportRunSummary,
    ) -> Result<(), ImportPipelineError> {
        let scan_thread_count = self.scan_thread_count().max(1);
        let mut paths = paths.into_iter();
        let mut join_set = JoinSet::new();

        for _ in 0..scan_thread_count {
            let Some(path) = paths.next() else {
                break;
            };
            self.spawn_import_task(
                &mut join_set,
                running_job,
                provider_credentials.clone(),
                providers_backing_off_in_run.clone(),
                path,
            );
        }

        while let Some(result) = join_set.join_next().await {
            let outcome = result??;
            match outcome {
                ImportTaskOutcome::Imported(decision) => {
                    apply_import_decision_to_summary(summary, decision);
                }
                ImportTaskOutcome::Failed => {
                    summary.failed_files += 1;
                }
            }

            if let Some(path) = paths.next() {
                self.spawn_import_task(
                    &mut join_set,
                    running_job,
                    provider_credentials.clone(),
                    providers_backing_off_in_run.clone(),
                    path,
                );
            }
        }

        Ok(())
    }

    /// Spawns one bounded import task for a media path.
    fn spawn_import_task(
        &self,
        join_set: &mut JoinSet<Result<ImportTaskOutcome, ImportPipelineError>>,
        running_job: &ImportJob,
        provider_credentials: Arc<Vec<ProviderCredential>>,
        providers_backing_off_in_run: SharedProviderBackoff,
        path: PathBuf,
    ) {
        let pipeline = self.clone();
        let running_job = running_job.clone();
        join_set.spawn(async move {
            pipeline
                .process_import_path(
                    &running_job,
                    provider_credentials.as_slice(),
                    providers_backing_off_in_run,
                    path,
                )
                .await
        });
    }

    /// Processes one import path and records per-file failures without failing the whole job.
    async fn process_import_path(
        &self,
        running_job: &ImportJob,
        provider_credentials: &[ProviderCredential],
        providers_backing_off_in_run: SharedProviderBackoff,
        path: PathBuf,
    ) -> Result<ImportTaskOutcome, ImportPipelineError> {
        match self
            .import_one(
                running_job,
                provider_credentials,
                providers_backing_off_in_run,
                &path,
            )
            .await
        {
            Ok(decision) => Ok(ImportTaskOutcome::Imported(decision)),
            Err(error) => {
                let error_message = error.to_string();
                self.settle_related_quarantine_failure(
                    running_job,
                    None,
                    Some(error_message.as_str()),
                )
                .await?;
                self.repository
                    .upsert_catalog_import_work_item(
                        running_job.id,
                        &path.to_string_lossy(),
                        None,
                        MediaFileStatus::Failed,
                        running_job.attempts + 1,
                        Some(error_message.as_str()),
                    )
                    .await?;
                Ok(ImportTaskOutcome::Failed)
            }
        }
    }

    /// Resolves the configured import scan worker count.
    fn scan_thread_count(&self) -> usize {
        usize::try_from(self.system_config.scan_thread_count)
            .ok()
            .filter(|count| *count > 0)
            .unwrap_or(DEFAULT_SCAN_THREAD_COUNT as usize)
    }

    /// Acquires locks for path/hash targets that could race during catalog mutation.
    async fn acquire_import_locks(
        &self,
        request: &CatalogImportRequest,
        managed_path: Option<&Path>,
    ) -> Vec<OwnedSemaphorePermit> {
        let mut keys = vec![format!("hash:{}", request.probe.file_hash)];
        if let Some(managed_path) = managed_path {
            keys.push(format!("managed:{}", managed_path.to_string_lossy()));
            if let Some(parent) = managed_path.parent() {
                keys.push(format!("folder:{}", parent.to_string_lossy()));
            }
        }

        self.run_locks.acquire(keys).await
    }

    /// Resolves configured or derived state for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job`: `&ImportJob`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Vec<PathBuf>` on success or `ImportPipelineError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ImportPipelineError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    fn resolve_import_paths(
        &self,
        job: &ImportJob,
    ) -> Result<Vec<PathBuf>, ImportPipelineError> {
        match (&job.kind, &job.scope) {
            (ImportJobKind::InitialScan, MaintenanceScope::FullLibrary) => Ok(vec![
                PathBuf::from(&self.system_config.library_root),
                PathBuf::from(&self.system_config.dropbox_root),
            ]),
            (ImportJobKind::DropboxIngest, MaintenanceScope::FullLibrary) => {
                Ok(vec![PathBuf::from(&self.system_config.dropbox_root)])
            }
            (_, MaintenanceScope::FullLibrary) => Ok(vec![PathBuf::from(
                &self.system_config.library_root,
            )]),
            (_, MaintenanceScope::Path { path }) => Ok(vec![PathBuf::from(path)]),
        }
    }

    /// Handles import one for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job`: `&ImportJob`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `provider_credentials`: `&[ProviderCredential]`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `providers_backing_off_in_run`: `SharedProviderBackoff`; expected to be shared run state for live provider backoff.
    /// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
    ///
    /// Output:
    /// - Returns `CatalogImportDecision` on success or `ImportPipelineError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ImportPipelineError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn import_one(
        &self,
        job: &ImportJob,
        provider_credentials: &[ProviderCredential],
        providers_backing_off_in_run: SharedProviderBackoff,
        path: &Path,
    ) -> Result<CatalogImportDecision, ImportPipelineError> {
        let probed = probe_media_file_blocking(path.to_path_buf()).await?;
        let source_path = path.to_string_lossy().to_string();
        let existing_managed_file = self
            .repository
            .published_media_file_by_managed_path(&source_path)
            .await?
            .filter(|existing| existing.file_hash == probed.facts.file_hash);
        if job.repair_plan.can_reuse_existing_without_refresh() {
            if let Some(existing) = existing_managed_file.as_ref() {
                return self.record_reused_existing(job, path, existing).await;
            }
        }

        let local_grouping = infer_catalog_grouping(&self.system_config, &probed);
        let provider_registry = self
            .provider_registry_for_job(job, provider_credentials)
            .await?;
        let provider_backoff_snapshot = {
            let mut providers_backing_off_in_run =
                providers_backing_off_in_run.lock().await;
            providers_backing_off_in_run
                .extend(provider_registry.providers_backing_off.iter().copied());
            providers_backing_off_in_run.clone()
        };
        let mut provider_report = if job.repair_plan.refresh_provider_metadata {
            provider_registry
                .registry
                .enrich(&local_grouping, &probed)
                .await
        } else {
            Default::default()
        };
        provider_report.outcomes.extend(live_backoff_outcomes(
            job,
            &provider_backoff_snapshot,
            provider_registry.registry.providers(),
        ));
        self.record_provider_outcomes(&provider_report.outcomes)
            .await?;
        {
            let mut providers_backing_off_in_run =
                providers_backing_off_in_run.lock().await;
            update_live_provider_backoff(
                &mut providers_backing_off_in_run,
                &provider_report.outcomes,
            );
        }
        let mut provider_bundle = provider_report
            .bundle
            .filter_for_media_kind(local_grouping.media_kind());
        if !job.repair_plan.refresh_artwork {
            provider_bundle.artwork.clear();
        }
        let grouping_decision =
            choose_catalog_grouping(&local_grouping, &provider_bundle, &probed);
        let managed_path =
            managed_path_for_grouping(&self.system_config, &grouping_decision.grouping, path);
        let mut request = CatalogImportRequest {
            source_path,
            managed_path: managed_path
                .as_ref()
                .map(|path| path.to_string_lossy().to_string()),
            grouping: grouping_decision.grouping.clone(),
            probe: probed.facts.clone(),
            import_job_id: Some(job.id),
            provider_links: provider_bundle.provider_links,
            provenance: provider_bundle.provenance,
            artwork: provider_bundle.artwork,
            allow_reuse_existing: job.repair_plan.can_reuse_existing_without_refresh(),
            refresh_artwork: job.repair_plan.refresh_artwork,
            rebuild_search_projections: job.repair_plan.rebuild_search_projections,
            preserve_provenance_history: job.repair_plan.preserve_provenance_history,
            preserve_confidence_history: job.repair_plan.preserve_confidence_history,
        };

        add_probe_provenance(&mut request, &probed);
        add_grouping_decision_provenance(&mut request, &local_grouping, &grouping_decision);
        let _import_locks = self.acquire_import_locks(&request, managed_path.as_deref()).await;

        if let Some(existing) = existing_managed_file.as_ref() {
            if request
                .managed_path
                .as_deref()
                .map(|managed_path| paths_equal(Path::new(managed_path), path))
                .unwrap_or(false)
            {
                request.source_path = existing.source_path.clone();
            }
        }

        if provider_failure_requires_quarantine(job, &provider_report.outcomes) {
            let error_message = provider_failure_message(&provider_report.outcomes);
            let outcome = self
                .repository
                .quarantine_metadata_failure(request, error_message.clone())
                .await?;
            self.repository
                .upsert_catalog_import_work_item(
                    job.id,
                    &path.to_string_lossy(),
                    Some(outcome.media_file.id),
                    outcome.media_file.status,
                    job.attempts + 1,
                    Some(error_message.as_str()),
                )
                .await?;
            self.settle_related_quarantine(
                job,
                &outcome.decision,
                Some(outcome.media_file.id),
            )
            .await?;
            return Ok(outcome.decision);
        }

        if request.grouping.is_stable()
            && managed_path
                .as_ref()
                .map(|managed_path| paths_equal(path, managed_path))
                .unwrap_or(false)
        {
            if job.repair_plan.can_reuse_existing_without_refresh() {
                if let Some(existing) = self
                    .repository
                    .published_media_file_for_hash_any_path(&request.probe.file_hash)
                    .await?
                    .filter(|existing| {
                        existing.file_hash == request.probe.file_hash
                            && self.hash_fallback_matches_current_managed_file(path, existing)
                    })
                {
                    return self.record_reused_existing(job, path, &existing).await;
                }
            }
        }

        if request.grouping.is_stable()
            && self
                .repository
                .find_duplicate_candidate(&request)
                .await?
                .is_none()
        {
            if let Some(target) = &managed_path {
                match materialize_managed_local_files_blocking(
                    path.to_path_buf(),
                    target.clone(),
                    request.probe.file_hash.clone(),
                    job.repair_plan.rewrite_sidecars,
                    job.repair_plan.refresh_artwork,
                    request.clone(),
                    probed.clone(),
                )
                .await
                {
                    Ok(materialized) => {
                        request.managed_path =
                            Some(materialized.final_path.to_string_lossy().to_string());
                        if let Some(copied_artwork) = materialized.copied_artwork {
                            request.artwork.push(copied_artwork);
                        }
                        if job.repair_plan.refresh_artwork {
                            materialize_remote_artwork(&mut request, &materialized.final_path)
                                .await;
                        }
                    }
                    Err(error) => {
                        let error_message = error.to_string();
                        let outcome = self
                            .repository
                            .quarantine_file_error(request, error_message.clone())
                            .await?;
                        self.repository
                            .upsert_catalog_import_work_item(
                                job.id,
                                &path.to_string_lossy(),
                                Some(outcome.media_file.id),
                                outcome.media_file.status,
                                job.attempts + 1,
                                Some(error_message.as_str()),
                            )
                            .await?;
                        self.settle_related_quarantine(
                            job,
                            &outcome.decision,
                            Some(outcome.media_file.id),
                        )
                        .await?;
                        return Ok(outcome.decision);
                    }
                }
            }
        }

        let outcome = self.repository.import_catalog_file(request).await?;
        self.repository
            .upsert_catalog_import_work_item(
                job.id,
                &path.to_string_lossy(),
                Some(outcome.media_file.id),
                outcome.media_file.status,
                job.attempts + 1,
                None,
            )
            .await?;
        self.settle_related_quarantine(
            job,
            &outcome.decision,
            Some(outcome.media_file.id),
        )
        .await?;
        Ok(outcome.decision)
    }

    /// Handles provider registry for job for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job`: `&ImportJob`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `provider_credentials`: `&[ProviderCredential]`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `ProviderRegistryForJob` on success or `ImportPipelineError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ImportPipelineError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn provider_registry_for_job(
        &self,
        job: &ImportJob,
        provider_credentials: &[ProviderCredential],
    ) -> Result<ProviderRegistryForJob, ImportPipelineError> {
        let mut provider_health = self.repository.provider_health().await?;
        let now = Utc::now();
        let mut providers_backing_off = BTreeSet::new();
        for health in &mut provider_health {
            if reconcile_provider_readiness(health, &now) {
                self.repository.save_provider_health(health).await?;
            }
            if provider_relevant_to_live_backoff(job, health.provider)
                && provider_backoff_active_at(health, &now)
            {
                providers_backing_off.insert(health.provider);
            }
        }

        Ok(ProviderRegistryForJob {
            registry: ProviderRegistry::from_health_and_credentials(
                &provider_health,
                provider_credentials,
                &job.provider_filter,
            ),
            providers_backing_off,
        })
    }

    /// Handles record reused existing for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job`: `&ImportJob`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
    /// - `existing`: `&MediaFile`; expected to be a media domain value that has already passed upstream validation.
    ///
    /// Output:
    /// - Returns `CatalogImportDecision` on success or `ImportPipelineError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ImportPipelineError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn record_reused_existing(
        &self,
        job: &ImportJob,
        path: &Path,
        existing: &MediaFile,
    ) -> Result<CatalogImportDecision, ImportPipelineError> {
        self.repository
            .upsert_catalog_import_work_item(
                job.id,
                &path.to_string_lossy(),
                Some(existing.id),
                existing.status,
                job.attempts + 1,
                None,
            )
            .await?;
        self.settle_related_quarantine(
            job,
            &CatalogImportDecision::ReusedExisting,
            Some(existing.id),
        )
        .await?;
        Ok(CatalogImportDecision::ReusedExisting)
    }

    /// Handles settle related quarantine for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job`: `&ImportJob`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `decision`: `&CatalogImportDecision`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `media_file_id`: `Option<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
    ///
    /// Output:
    /// - Returns `()` on success or `ImportPipelineError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ImportPipelineError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn settle_related_quarantine(
        &self,
        job: &ImportJob,
        decision: &CatalogImportDecision,
        media_file_id: Option<Uuid>,
    ) -> Result<(), ImportPipelineError> {
        let Some(item_id) = job.related_quarantine_item_id else {
            return Ok(());
        };

        match decision {
            CatalogImportDecision::Published | CatalogImportDecision::ReusedExisting => {
                self.repository
                    .mark_quarantine_resolved(
                        item_id,
                        media_file_id,
                        Some("retry resolved through the import pipeline"),
                    )
                    .await?;
            }
            CatalogImportDecision::QuarantinedUnstableGrouping
            | CatalogImportDecision::QuarantinedDuplicate
            | CatalogImportDecision::QuarantinedFileError => {
                self.settle_related_quarantine_failure(
                    job,
                    media_file_id,
                    Some("retry completed but the item remains quarantined"),
                )
                .await?;
            }
        }

        Ok(())
    }

    /// Handles settle related quarantine failure for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `job`: `&ImportJob`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `media_file_id`: `Option<Uuid>`; expected to be a route or domain identifier that must parse to the expected type.
    /// - `message`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `()` on success or `ImportPipelineError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ImportPipelineError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn settle_related_quarantine_failure(
        &self,
        job: &ImportJob,
        media_file_id: Option<Uuid>,
        message: Option<&str>,
    ) -> Result<(), ImportPipelineError> {
        let Some(item_id) = job.related_quarantine_item_id else {
            return Ok(());
        };

        self.repository
            .mark_quarantine_open(item_id, media_file_id, true, message)
            .await?;
        Ok(())
    }

    /// Handles record provider outcomes for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `outcomes`: `&[ProviderExecutionOutcome]`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `()` on success or `ImportPipelineError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ImportPipelineError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn record_provider_outcomes(
        &self,
        outcomes: &[ProviderExecutionOutcome],
    ) -> Result<(), ImportPipelineError> {
        for outcome in outcomes.iter().filter(|outcome| outcome.attempted) {
            let Some(mut health) = self
                .repository
                .provider(outcome.provider)
                .await?
                .or_else(|| {
                    self.provider_health
                        .iter()
                        .find(|health| health.provider == outcome.provider)
                        .cloned()
                })
            else {
                continue;
            };

            apply_provider_execution_outcome(&mut health, outcome);
            self.repository.save_provider_health(&health).await?;
        }

        Ok(())
    }

    /// Hashes security-sensitive data for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
    /// - `existing`: `&MediaFile`; expected to be a media domain value that has already passed upstream validation.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn hash_fallback_matches_current_managed_file(
        &self,
        path: &Path,
        existing: &MediaFile,
    ) -> bool {
        if existing
            .managed_path
            .as_deref()
            .map(|managed_path| paths_equal(Path::new(managed_path), path))
            .unwrap_or(false)
        {
            return true;
        }

        existing.managed_path.is_none()
            && Path::new(&existing.source_path)
                .starts_with(Path::new(&self.system_config.dropbox_root))
    }
}

/// Applies one path decision to the run summary counters.
fn apply_import_decision_to_summary(
    summary: &mut ImportRunSummary,
    decision: CatalogImportDecision,
) {
    match decision {
        CatalogImportDecision::Published => summary.published_files += 1,
        CatalogImportDecision::ReusedExisting => summary.reused_files += 1,
        CatalogImportDecision::QuarantinedDuplicate => {
            summary.duplicate_files += 1;
            summary.quarantined_files += 1;
        }
        CatalogImportDecision::QuarantinedUnstableGrouping
        | CatalogImportDecision::QuarantinedFileError => {
            summary.quarantined_files += 1;
        }
    }
}

/// Handles provider failure requires quarantine for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `job`: `&ImportJob`; expected to be a value satisfying the type contract shown in the function signature.
/// - `outcomes`: `&[ProviderExecutionOutcome]`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn provider_failure_requires_quarantine(
    job: &ImportJob,
    outcomes: &[ProviderExecutionOutcome],
) -> bool {
    if !matches!(
        job.kind,
        ImportJobKind::ProviderRepair | ImportJobKind::QuarantineRetry
    ) {
        return false;
    }

    outcomes.iter().any(ProviderExecutionOutcome::has_failures)
}

/// Handles provider relevant to live backoff for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `job`: `&ImportJob`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn provider_relevant_to_live_backoff(job: &ImportJob, provider: ProviderKind) -> bool {
    matches!(
        job.kind,
        ImportJobKind::ProviderRepair | ImportJobKind::QuarantineRetry
    ) && (job.provider_filter.is_empty() || job.provider_filter.contains(&provider))
}

/// Handles live backoff outcomes for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `job`: `&ImportJob`; expected to be a value satisfying the type contract shown in the function signature.
/// - `providers_backing_off_in_run`: `&BTreeSet<ProviderKind>`; expected to be one of the supported metadata provider identifiers.
/// - `active_providers`: `&[ProviderKind]`; expected to be one of the supported metadata provider identifiers.
///
/// Output:
/// - Returns `Vec<ProviderExecutionOutcome>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn live_backoff_outcomes(
    job: &ImportJob,
    providers_backing_off_in_run: &BTreeSet<ProviderKind>,
    active_providers: &[ProviderKind],
) -> Vec<ProviderExecutionOutcome> {
    if !matches!(
        job.kind,
        ImportJobKind::ProviderRepair | ImportJobKind::QuarantineRetry
    ) {
        return Vec::new();
    }

    providers_backing_off_in_run
        .iter()
        .copied()
        .filter(|provider| provider_relevant_to_live_backoff(job, *provider))
        .filter(|provider| !active_providers.contains(provider))
        .map(|provider| ProviderExecutionOutcome {
            provider,
            attempted: false,
            attempts: 0,
            successful_requests: 0,
            failures: vec![
                "provider is in retry backoff; retry window has not elapsed"
                    .to_string(),
            ],
        })
        .collect()
}

/// Updates existing state for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `providers_backing_off_in_run`: `&mut BTreeSet<ProviderKind>`; expected to be one of the supported metadata provider identifiers.
/// - `outcomes`: `&[ProviderExecutionOutcome]`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn update_live_provider_backoff(
    providers_backing_off_in_run: &mut BTreeSet<ProviderKind>,
    outcomes: &[ProviderExecutionOutcome],
) {
    for outcome in outcomes.iter().filter(|outcome| outcome.attempted) {
        if outcome.has_failures() {
            providers_backing_off_in_run.insert(outcome.provider);
        } else if outcome.successful_requests > 0 {
            providers_backing_off_in_run.remove(&outcome.provider);
        }
    }
}

/// Handles provider failure message for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `outcomes`: `&[ProviderExecutionOutcome]`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn provider_failure_message(outcomes: &[ProviderExecutionOutcome]) -> String {
    let failures = outcomes
        .iter()
        .filter(|outcome| outcome.has_failures())
        .map(|outcome| {
            let message = outcome
                .failures
                .first()
                .map(String::as_str)
                .unwrap_or("provider request failed");
            if !outcome.attempted {
                return format!("{} skipped: {}", outcome.provider.display_name(), message);
            }
            format!(
                "{} failed after {} attempt(s): {}",
                outcome.provider.display_name(),
                outcome.attempts,
                message
            )
        })
        .collect::<Vec<_>>();

    if failures.is_empty() {
        "provider metadata refresh failed".to_string()
    } else {
        failures.join("; ")
    }
}

/// Applies derived state for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `health`: `&mut ProviderHealth`; expected to be a value satisfying the type contract shown in the function signature.
/// - `outcome`: `&ProviderExecutionOutcome`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn apply_provider_execution_outcome(
    health: &mut ProviderHealth,
    outcome: &ProviderExecutionOutcome,
) {
    let now = Utc::now();
    health.updated_at = now;

    if outcome.has_failures() {
        health.failure_count = health.failure_count.saturating_add(1);
        health.status = crate::domain::ProviderStatus::BackingOff;
        health.maintenance_ready = false;
        health.last_failure_at = Some(now);
        health.retry_after = Some(now + provider_backoff_duration(health.failure_count));
        health.message = Some(provider_failure_message(std::slice::from_ref(outcome)));
        return;
    }

    if outcome.successful_requests > 0 {
        health.failure_count = 0;
        health.status = crate::domain::ProviderStatus::Healthy;
        health.maintenance_ready = true;
        health.retry_after = None;
        health.last_success_at = Some(now);
        health.message = None;
    }
}

/// Handles provider backoff duration for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `failure_count`: `u32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `ChronoDuration` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn provider_backoff_duration(failure_count: u32) -> ChronoDuration {
    match failure_count {
        0 | 1 => ChronoDuration::seconds(60),
        2 => ChronoDuration::seconds(300),
        3 => ChronoDuration::seconds(900),
        _ => ChronoDuration::seconds(3600),
    }
}

/// Handles retry file operation for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `operation`: `F`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `T` on success or `io::Error` when the operation cannot be completed.
///
/// Errors:
/// - Returns `io::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn retry_file_operation<T, F>(mut operation: F) -> Result<T, io::Error>
where
    F: FnMut() -> Result<T, io::Error>,
{
    let mut attempt = 1;
    loop {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if attempt >= FILE_OPERATION_MAX_ATTEMPTS => return Err(error),
            Err(_) => {
                attempt += 1;
                std::thread::sleep(FILE_OPERATION_RETRY_BACKOFF);
            }
        }
    }
}

/// Represents provider bundle filter in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Defines required behavior through methods `filter_for_media_kind` for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: depends on `MediaKind` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`.
trait ProviderBundleFilter {
    /// Handles filter for media kind for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `media_kind`: `MediaKind`; expected to be a media domain value that has already passed upstream validation.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn filter_for_media_kind(self, media_kind: MediaKind) -> Self;
}

impl ProviderBundleFilter for crate::providers::ProviderMetadataBundle {
    /// Handles filter for media kind for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `media_kind`: `MediaKind`; expected to be a media domain value that has already passed upstream validation.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn filter_for_media_kind(mut self, media_kind: MediaKind) -> Self {
        self.provider_links
            .retain(|link| provider_supports_media_kind(link.provider, media_kind));
        self.provenance
            .retain(|link| provider_supports_media_kind(link.provider, media_kind));
        self.artwork
            .retain(|link| provider_supports_media_kind(link.provider, media_kind));
        self
    }
}

#[derive(Debug, Clone)]
/// Represents chosen grouping decision in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Carries fields `grouping`, `provider_influenced`, `confidence`, `source_provider` for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: depends on `CatalogGrouping`, `bool`, `f32`, `Option<ProviderKind>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`.
struct ChosenGroupingDecision {
    grouping: CatalogGrouping,
    provider_influenced: bool,
    confidence: f32,
    source_provider: Option<ProviderKind>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Represents local field strength in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Enumerates `Missing`, `Weak`, `Strong` states or choices for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`.
enum LocalFieldStrength {
    Missing,
    Weak,
    Strong,
}

impl LocalFieldStrength {
    /// Handles allows provider choice for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn allows_provider_choice(self) -> bool {
        !matches!(self, Self::Strong)
    }
}

#[derive(Debug, Clone)]
/// Represents field candidate in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Carries fields `value`, `provider`, `confidence` for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: depends on `T`, `ProviderKind`, `f32` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`.
struct FieldCandidate<T> {
    value: T,
    provider: ProviderKind,
    confidence: f32,
}

#[derive(Debug, Clone)]
/// Represents music grouping evidence in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Carries fields `album_artist`, `track_artist`, `album_title`, `track_title`, `release_year` for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: depends on `LocalFieldStrength`, `LocalFieldStrength`, `LocalFieldStrength`, `LocalFieldStrength`, `LocalFieldStrength` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`.
struct MusicGroupingEvidence {
    album_artist: LocalFieldStrength,
    track_artist: LocalFieldStrength,
    album_title: LocalFieldStrength,
    track_title: LocalFieldStrength,
    release_year: LocalFieldStrength,
}

#[derive(Debug, Clone)]
/// Represents podcast grouping evidence in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Carries fields `podcast_title`, `episode_title` for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: depends on `LocalFieldStrength`, `LocalFieldStrength` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`.
struct PodcastGroupingEvidence {
    podcast_title: LocalFieldStrength,
    episode_title: LocalFieldStrength,
}

/// Handles choose catalog grouping for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `local_grouping`: `&CatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider_bundle`: `&ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `media`: `&ProbedMediaFile`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `ChosenGroupingDecision` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn choose_catalog_grouping(
    local_grouping: &CatalogGrouping,
    provider_bundle: &ProviderMetadataBundle,
    media: &ProbedMediaFile,
) -> ChosenGroupingDecision {
    match local_grouping {
        CatalogGrouping::Music(grouping) => {
            choose_music_grouping(grouping, provider_bundle, media)
        }
        CatalogGrouping::Podcast(grouping) => {
            choose_podcast_grouping(grouping, provider_bundle, media)
        }
    }
}

/// Handles choose music grouping for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `local_grouping`: `&MusicCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider_bundle`: `&ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `media`: `&ProbedMediaFile`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `ChosenGroupingDecision` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn choose_music_grouping(
    local_grouping: &MusicCatalogGrouping,
    provider_bundle: &ProviderMetadataBundle,
    media: &ProbedMediaFile,
) -> ChosenGroupingDecision {
    let evidence = music_grouping_evidence(local_grouping, media);
    let mut grouping = local_grouping.clone();
    let mut influence = ProviderInfluence::default();

    if grouping.album_kind != AlbumKind::Compilation {
        let album_artist = best_external_string_candidate(
            provider_bundle,
            &[
                (CatalogEntityType::Album, "artist_name"),
                (CatalogEntityType::Artist, "name"),
            ],
        );
        apply_string_candidate(
            &mut grouping.album_artist,
            evidence.album_artist,
            album_artist,
            &mut influence,
        );
    }

    let track_artist = best_external_string_candidate(
        provider_bundle,
        &[
            (CatalogEntityType::Track, "artist_name"),
            (CatalogEntityType::Artist, "name"),
        ],
    );
    apply_string_candidate(
        &mut grouping.track_artist,
        evidence.track_artist,
        track_artist,
        &mut influence,
    );

    let album_title =
        best_external_string_candidate(provider_bundle, &[(CatalogEntityType::Album, "title")]);
    apply_string_candidate(
        &mut grouping.album_title,
        evidence.album_title,
        album_title,
        &mut influence,
    );

    let track_title =
        best_external_string_candidate(provider_bundle, &[(CatalogEntityType::Track, "title")]);
    apply_string_candidate(
        &mut grouping.track_title,
        evidence.track_title,
        track_title,
        &mut influence,
    );

    let release_year = best_external_i32_candidate(
        provider_bundle,
        CatalogEntityType::Album,
        "release_year",
    );
    if grouping.release_year.is_none() || evidence.release_year.allows_provider_choice() {
        if let Some(candidate) = release_year {
            grouping.release_year = Some(candidate.value);
            influence.record(candidate.provider, candidate.confidence);
        }
    }

    if grouping.album_kind == AlbumKind::Compilation
        || grouping
            .album_artist
            .trim()
            .eq_ignore_ascii_case(likely_compilation_artist())
    {
        grouping.album_artist = likely_compilation_artist().to_string();
        grouping.album_kind = AlbumKind::Compilation;
    }

    ChosenGroupingDecision {
        grouping: CatalogGrouping::Music(grouping),
        provider_influenced: influence.provider_influenced,
        confidence: influence.confidence.unwrap_or(1.0),
        source_provider: influence.source_provider,
    }
}

/// Handles choose podcast grouping for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `local_grouping`: `&PodcastCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider_bundle`: `&ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `media`: `&ProbedMediaFile`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `ChosenGroupingDecision` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn choose_podcast_grouping(
    local_grouping: &PodcastCatalogGrouping,
    provider_bundle: &ProviderMetadataBundle,
    media: &ProbedMediaFile,
) -> ChosenGroupingDecision {
    let evidence = podcast_grouping_evidence(local_grouping, media);
    let mut grouping = local_grouping.clone();
    let mut influence = ProviderInfluence::default();

    let podcast_title = best_external_string_candidate(
        provider_bundle,
        &[(CatalogEntityType::Podcast, "title")],
    );
    apply_string_candidate(
        &mut grouping.podcast_title,
        evidence.podcast_title,
        podcast_title,
        &mut influence,
    );

    let episode_title = best_external_string_candidate(
        provider_bundle,
        &[(CatalogEntityType::Episode, "title")],
    );
    apply_string_candidate(
        &mut grouping.episode_title,
        evidence.episode_title,
        episode_title,
        &mut influence,
    );

    ChosenGroupingDecision {
        grouping: CatalogGrouping::Podcast(grouping),
        provider_influenced: influence.provider_influenced,
        confidence: influence.confidence.unwrap_or(1.0),
        source_provider: influence.source_provider,
    }
}

#[derive(Debug, Default)]
/// Represents provider influence in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Carries fields `provider_influenced`, `confidence`, `source_provider` for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: depends on `bool`, `Option<f32>`, `Option<ProviderKind>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`.
struct ProviderInfluence {
    provider_influenced: bool,
    confidence: Option<f32>,
    source_provider: Option<ProviderKind>,
}

impl ProviderInfluence {
    /// Handles record for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    /// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn record(&mut self, provider: ProviderKind, confidence: f32) {
        self.provider_influenced = true;
        if self
            .confidence
            .map(|current| confidence > current)
            .unwrap_or(true)
        {
            self.confidence = Some(confidence);
            self.source_provider = Some(provider);
        }
    }
}

/// Applies derived state for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `target`: `&mut String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `strength`: `LocalFieldStrength`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `candidate`: `Option<FieldCandidate<String>>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `influence`: `&mut ProviderInfluence`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn apply_string_candidate(
    target: &mut String,
    strength: LocalFieldStrength,
    candidate: Option<FieldCandidate<String>>,
    influence: &mut ProviderInfluence,
) {
    if !strength.allows_provider_choice() {
        return;
    }
    let Some(candidate) = candidate else {
        return;
    };
    if candidate.value.trim().is_empty() || candidate.value.trim() == target.trim() {
        return;
    }

    *target = candidate.value;
    influence.record(candidate.provider, candidate.confidence);
}

/// Handles best external string candidate for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `provider_bundle`: `&ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `fields`: `&[(CatalogEntityType, &str)]`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(FieldCandidate<String>)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn best_external_string_candidate(
    provider_bundle: &ProviderMetadataBundle,
    fields: &[(CatalogEntityType, &str)],
) -> Option<FieldCandidate<String>> {
    let mut best = None;

    for (entity_type, field_name) in fields {
        for provenance in &provider_bundle.provenance {
            if provenance.provider == ProviderKind::LocalSidecars
                || provenance.entity_type != *entity_type
                || provenance.field_name != *field_name
                || !is_auto_accepted_provider_value(
                    provenance.confidence,
                    provenance.auto_accepted,
                )
            {
                continue;
            }
            let Some(value) = string_value(&provenance.value) else {
                continue;
            };
            let candidate = FieldCandidate {
                value,
                provider: provenance.provider,
                confidence: provenance.confidence,
            };
            if candidate_is_better(&candidate, best.as_ref()) {
                best = Some(candidate);
            }
        }
        if best.is_some() {
            break;
        }
    }

    best
}

/// Handles best external i32 candidate for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `provider_bundle`: `&ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `entity_type`: `CatalogEntityType`; expected to be a value satisfying the type contract shown in the function signature.
/// - `field_name`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(FieldCandidate<i32>)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn best_external_i32_candidate(
    provider_bundle: &ProviderMetadataBundle,
    entity_type: CatalogEntityType,
    field_name: &str,
) -> Option<FieldCandidate<i32>> {
    let mut best = None;

    for provenance in &provider_bundle.provenance {
        if provenance.provider == ProviderKind::LocalSidecars
            || provenance.entity_type != entity_type
            || provenance.field_name != field_name
                || !is_auto_accepted_provider_value(
                    provenance.confidence,
                    provenance.auto_accepted,
                )
        {
            continue;
        }
        let Some(value) = i32_value(&provenance.value) else {
            continue;
        };
        let candidate = FieldCandidate {
            value,
            provider: provenance.provider,
            confidence: provenance.confidence,
        };
        if candidate_is_better(&candidate, best.as_ref()) {
            best = Some(candidate);
        }
    }

    best
}

/// Handles candidate is better for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `candidate`: `&FieldCandidate<T>`; expected to be a value satisfying the type contract shown in the function signature.
/// - `best`: `Option<&FieldCandidate<T>>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn candidate_is_better<T>(
    candidate: &FieldCandidate<T>,
    best: Option<&FieldCandidate<T>>,
) -> bool {
    best.map(|best| candidate.confidence > best.confidence)
        .unwrap_or(true)
}

/// Handles is auto accepted provider value for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
/// - `auto_accepted`: `bool`; expected to be a boolean flag controlling the documented branch.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn is_auto_accepted_provider_value(confidence: f32, auto_accepted: bool) -> bool {
    auto_accepted && confidence >= PROVIDER_AUTO_ACCEPT_CONFIDENCE_THRESHOLD
}

/// Handles string value for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `value`: `&Value`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Some(String)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn string_value(value: &Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// Handles i32 value for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `value`: `&Value`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Some(i32)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn i32_value(value: &Value) -> Option<i32> {
    value
        .as_i64()
        .and_then(|value| i32::try_from(value).ok())
        .or_else(|| value.as_str().and_then(|value| value.trim().parse().ok()))
}

/// Handles music grouping evidence for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `grouping`: `&MusicCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
/// - `media`: `&ProbedMediaFile`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `MusicGroupingEvidence` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn music_grouping_evidence(
    grouping: &MusicCatalogGrouping,
    media: &ProbedMediaFile,
) -> MusicGroupingEvidence {
    let compilation = media.tags.bool(&[
        "compilation",
        "part_of_compilation",
        "partofcompilation",
        "itunescompilation",
    ]);
    MusicGroupingEvidence {
        album_artist: if grouping.album_artist.trim().is_empty() {
            LocalFieldStrength::Missing
        } else if compilation
            || first_tag(
                media,
                &[
                    "album_artist",
                    "albumartist",
                    "albumartistsort",
                    "album artist",
                ],
            )
            .is_some()
        {
            LocalFieldStrength::Strong
        } else {
            LocalFieldStrength::Weak
        },
        track_artist: strength_for_string(
            &grouping.track_artist,
            first_tag(media, &["artist", "artists", "track_artist", "performer"]).is_some(),
        ),
        album_title: strength_for_string(
            &grouping.album_title,
            first_tag(media, &["album", "release", "release_title"]).is_some(),
        ),
        track_title: strength_for_string(
            &grouping.track_title,
            first_tag(media, &["title", "track_title"]).is_some(),
        ),
        release_year: if grouping.release_year.is_some() {
            LocalFieldStrength::Strong
        } else {
            LocalFieldStrength::Missing
        },
    }
}

/// Handles podcast grouping evidence for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `grouping`: `&PodcastCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
/// - `media`: `&ProbedMediaFile`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `PodcastGroupingEvidence` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn podcast_grouping_evidence(
    grouping: &PodcastCatalogGrouping,
    media: &ProbedMediaFile,
) -> PodcastGroupingEvidence {
    PodcastGroupingEvidence {
        podcast_title: strength_for_string(
            &grouping.podcast_title,
            first_tag(
                media,
                &[
                    "podcast",
                    "podcast_title",
                    "show",
                    "showtitle",
                    "series",
                    "album",
                ],
            )
            .is_some(),
        ),
        episode_title: strength_for_string(
            &grouping.episode_title,
            first_tag(media, &["episode_title", "episodetitle", "title"]).is_some(),
        ),
    }
}

/// Handles strength for string for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `has_direct_local_tag`: `bool`; expected to be a boolean flag controlling the documented branch.
///
/// Output:
/// - Returns `LocalFieldStrength` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn strength_for_string(value: &str, has_direct_local_tag: bool) -> LocalFieldStrength {
    if value.trim().is_empty() {
        LocalFieldStrength::Missing
    } else if has_direct_local_tag {
        LocalFieldStrength::Strong
    } else {
        LocalFieldStrength::Weak
    }
}

async fn collect_media_paths(
    import_paths: Vec<PathBuf>,
) -> Result<Vec<PathBuf>, ImportPipelineError> {
    task::spawn_blocking(move || {
        let mut paths = Vec::new();
        for root in import_paths {
            paths.extend(media_paths(root)?);
        }
        Ok(paths)
    })
    .await?
}

async fn probe_media_file_blocking(path: PathBuf) -> Result<ProbedMediaFile, ImportPipelineError> {
    task::spawn_blocking(move || probe_media_file(path))
        .await?
        .map_err(ImportPipelineError::from)
}

/// Handles media paths for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `root`: `PathBuf`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `Vec<PathBuf>` on success or `ImportPipelineError` when the operation cannot be completed.
///
/// Errors:
/// - Returns `ImportPipelineError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn media_paths(root: PathBuf) -> Result<Vec<PathBuf>, ImportPipelineError> {
    if root.is_file() {
        return if is_supported_media_path(&root) {
            Ok(vec![root])
        } else {
            Err(ImportPipelineError::InvalidScope(
                root.to_string_lossy().to_string(),
            ))
        };
    }

    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut paths = WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| entry.into_path())
        .filter(|path| is_supported_media_path(path))
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
}

struct ManagedLocalFileMaterialization {
    final_path: PathBuf,
    copied_artwork: Option<ArtworkAssetDraft>,
}

async fn materialize_managed_local_files_blocking(
    source_path: PathBuf,
    target_path: PathBuf,
    file_hash: String,
    rewrite_sidecars: bool,
    refresh_artwork: bool,
    request: CatalogImportRequest,
    probed: ProbedMediaFile,
) -> Result<ManagedLocalFileMaterialization, io::Error> {
    task::spawn_blocking(move || {
        let final_path = retry_file_operation(|| {
            ensure_managed_file(&source_path, &target_path, &file_hash)
        })?;
        if rewrite_sidecars {
            retry_file_operation(|| write_managed_sidecar(&final_path, &request))?;
        }
        let copied_artwork = if refresh_artwork {
            retry_file_operation(|| copy_folder_image(&probed, &request.grouping, &final_path))?
        } else {
            None
        };
        Ok(ManagedLocalFileMaterialization {
            final_path,
            copied_artwork,
        })
    })
    .await
    .map_err(blocking_join_error_to_io_error)?
}

fn blocking_join_error_to_io_error(error: task::JoinError) -> io::Error {
    io::Error::new(
        io::ErrorKind::Other,
        format!("blocking import task failed: {error}"),
    )
}

/// Handles infer catalog grouping for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `config`: `&SystemConfig`; expected to be a value satisfying the type contract shown in the function signature.
/// - `media`: `&ProbedMediaFile`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `CatalogGrouping` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn infer_catalog_grouping(config: &SystemConfig, media: &ProbedMediaFile) -> CatalogGrouping {
    if is_podcast_media(config, media) {
        CatalogGrouping::Podcast(PodcastCatalogGrouping {
            podcast_title: first_tag(
                media,
                &[
                    "podcast",
                    "podcast_title",
                    "show",
                    "showtitle",
                    "series",
                    "album",
                ],
            )
            .or_else(|| parent_component(&media.source_path, 0))
            .unwrap_or_default(),
            episode_title: first_tag(media, &["episode_title", "episodetitle", "title"])
                .or_else(|| file_stem(&media.source_path))
                .unwrap_or_default(),
            season_number: media.tags.number(&["season", "season_number", "seasonnumber"]),
            episode_number: media
                .tags
                .number(&["episode", "episode_number", "episodenumber", "track", "tracknumber"]),
        })
    } else {
        let mut album_artist = first_tag(
            media,
            &["album_artist", "albumartist", "albumartistsort", "album artist"],
        );
        let track_artist =
            first_tag(media, &["artist", "artists", "track_artist", "performer"])
                .or_else(|| album_artist.clone())
                .or_else(|| parent_component(&media.source_path, 1))
                .unwrap_or_default();
        let compilation = media.tags.bool(&[
            "compilation",
            "part_of_compilation",
            "partofcompilation",
            "itunescompilation",
        ]) || album_artist
            .as_deref()
            .map(|artist| artist.eq_ignore_ascii_case(likely_compilation_artist()))
            .unwrap_or(false);

        if compilation {
            album_artist = Some(likely_compilation_artist().to_string());
        }

        CatalogGrouping::Music(MusicCatalogGrouping {
            album_artist: album_artist
                .or_else(|| first_tag(media, &["artist"]))
                .or_else(|| parent_component(&media.source_path, 1))
                .unwrap_or_default(),
            track_artist,
            album_title: first_tag(media, &["album", "release", "release_title"])
                .or_else(|| parent_component(&media.source_path, 0))
                .unwrap_or_default(),
            track_title: first_tag(media, &["title", "track_title"])
                .or_else(|| file_stem(&media.source_path))
                .unwrap_or_default(),
            album_kind: if compilation {
                AlbumKind::Compilation
            } else {
                AlbumKind::Album
            },
            release_year: release_year(media),
            disc_number: media.tags.number(&["disc", "discnumber", "disc_number"]),
            track_number: media.tags.number(&["track", "tracknumber", "track_number"]),
        })
    }
}

/// Handles is podcast media for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `config`: `&SystemConfig`; expected to be a value satisfying the type contract shown in the function signature.
/// - `media`: `&ProbedMediaFile`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn is_podcast_media(config: &SystemConfig, media: &ProbedMediaFile) -> bool {
    if media
        .tags
        .get(&["media_kind", "mediakind", "type"])
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "podcast" | "episode" | "podcasts"
            )
        })
        .unwrap_or(false)
    {
        return true;
    }

    if media.tags.get(&["podcast", "show", "series"]).is_some() {
        return true;
    }

    PathBuf::from(&config.library_root)
        .join(&config.podcast_subtree)
        .canonicalize()
        .ok()
        .zip(media.source_path.canonicalize().ok())
        .map(|(podcasts_root, media_path)| media_path.starts_with(podcasts_root))
        .unwrap_or_else(|| {
            media.source_path
                .components()
                .any(|component| {
                    component.as_os_str().to_string_lossy() == config.podcast_subtree.as_str()
                })
        })
}

/// Handles managed path for grouping for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `config`: `&SystemConfig`; expected to be a value satisfying the type contract shown in the function signature.
/// - `grouping`: `&CatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
/// - `source_path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `Some(PathBuf)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn managed_path_for_grouping(
    config: &SystemConfig,
    grouping: &CatalogGrouping,
    source_path: &Path,
) -> Option<PathBuf> {
    let extension = source_path
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("media");
    match grouping {
        CatalogGrouping::Music(grouping) if grouping.is_stable() => {
            let filename = music_filename(grouping, extension);
            Some(
                PathBuf::from(&config.library_root)
                    .join(sanitize_path_component(&grouping.album_artist))
                    .join(sanitize_path_component(&grouping.album_title))
                    .join(filename),
            )
        }
        CatalogGrouping::Podcast(grouping) if grouping.is_stable() => {
            let filename = podcast_filename(grouping, extension);
            Some(
                PathBuf::from(&config.library_root)
                    .join(&config.podcast_subtree)
                    .join(sanitize_path_component(&grouping.podcast_title))
                    .join(filename),
            )
        }
        _ => None,
    }
}

/// Represents stable grouping in the maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Functionality: Defines required behavior through methods `is_stable` for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
/// Dependencies: has no direct field dependencies beyond derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`.
trait StableGrouping {
    /// Handles is stable for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn is_stable(&self) -> bool;
}

impl StableGrouping for MusicCatalogGrouping {
    /// Handles is stable for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn is_stable(&self) -> bool {
        !self.album_artist.trim().is_empty()
            && !self.track_artist.trim().is_empty()
            && !self.album_title.trim().is_empty()
            && !self.track_title.trim().is_empty()
    }
}

impl StableGrouping for PodcastCatalogGrouping {
    /// Handles is stable for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn is_stable(&self) -> bool {
        !self.podcast_title.trim().is_empty() && !self.episode_title.trim().is_empty()
    }
}

/// Handles music filename for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `grouping`: `&MusicCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
/// - `extension`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn music_filename(grouping: &MusicCatalogGrouping, extension: &str) -> String {
    let title = sanitize_path_component(&grouping.track_title);
    match grouping.track_number {
        Some(track_number) if track_number > 0 => {
            format!("{track_number:02} - {title}.{extension}")
        }
        _ => format!("{title}.{extension}"),
    }
}

/// Handles podcast filename for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `grouping`: `&PodcastCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
/// - `extension`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn podcast_filename(grouping: &PodcastCatalogGrouping, extension: &str) -> String {
    let title = sanitize_path_component(&grouping.episode_title);
    match (grouping.season_number, grouping.episode_number) {
        (Some(season), Some(episode)) if season > 0 && episode > 0 => {
            format!("S{season:02}E{episode:02} - {title}.{extension}")
        }
        (_, Some(episode)) if episode > 0 => format!("{episode:03} - {title}.{extension}"),
        _ => format!("{title}.{extension}"),
    }
}

/// Handles ensure managed file for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `source_path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `target_path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `file_hash`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `PathBuf` on success or `io::Error` when the operation cannot be completed.
///
/// Errors:
/// - Returns `io::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn ensure_managed_file(
    source_path: &Path,
    target_path: &Path,
    file_hash: &str,
) -> Result<PathBuf, io::Error> {
    let final_path = unique_target_path(source_path, target_path, file_hash)?;
    if paths_equal(source_path, &final_path) {
        return Ok(final_path);
    }

    if let Some(parent) = final_path.parent() {
        fs::create_dir_all(parent)?;
    }

    if fs::rename(source_path, &final_path).is_err() {
        fs::copy(source_path, &final_path)?;
        fs::remove_file(source_path)?;
    }
    Ok(final_path)
}

/// Handles unique target path for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `source_path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `target_path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `file_hash`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `PathBuf` on success or `io::Error` when the operation cannot be completed.
///
/// Errors:
/// - Returns `io::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn unique_target_path(
    source_path: &Path,
    target_path: &Path,
    file_hash: &str,
) -> Result<PathBuf, io::Error> {
    if !target_path.exists() || paths_equal(source_path, target_path) {
        return Ok(target_path.to_path_buf());
    }

    if probe_media_file(target_path)
        .map(|existing| existing.facts.file_hash == file_hash)
        .unwrap_or(false)
    {
        return Ok(target_path.to_path_buf());
    }

    let parent = target_path.parent().unwrap_or_else(|| Path::new(""));
    let stem = target_path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("media");
    let extension = target_path.extension().and_then(|extension| extension.to_str());

    for index in 2..10_000 {
        let filename = match extension {
            Some(extension) => format!("{stem} ({index}).{extension}"),
            None => format!("{stem} ({index})"),
        };
        let candidate = parent.join(filename);
        if !candidate.exists() {
            return Ok(candidate);
        }
    }

    Ok(target_path.to_path_buf())
}

/// Writes data for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `managed_path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `request`: `&CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` on success or `io::Error` when the operation cannot be completed.
///
/// Errors:
/// - Returns `io::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn write_managed_sidecar(
    managed_path: &Path,
    request: &CatalogImportRequest,
) -> Result<(), io::Error> {
    let Some(parent) = managed_path.parent() else {
        return Ok(());
    };
    let sidecar = parent.join("harmonixia.metadata.json");
    let contents = serde_json::to_vec_pretty(&json!({
        "managed_path": managed_path.to_string_lossy().to_string(),
        "source_path": &request.source_path,
        "grouping": &request.grouping,
        "probe": &request.probe,
        "provider_links": &request.provider_links,
        "provenance": &request.provenance,
        "embedded_tags_rewritten": false
    }))
    .map_err(|error| io::Error::new(io::ErrorKind::Other, error))?;
    fs::write(sidecar, contents)
}

/// Handles copy folder image for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `media`: `&ProbedMediaFile`; expected to be a media domain value that has already passed upstream validation.
/// - `grouping`: `&CatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
/// - `managed_path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `Option<ArtworkAssetDraft>` on success or `io::Error` when the operation cannot be completed.
///
/// Errors:
/// - Returns `io::Error` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
fn copy_folder_image(
    media: &ProbedMediaFile,
    grouping: &CatalogGrouping,
    managed_path: &Path,
) -> Result<Option<ArtworkAssetDraft>, io::Error> {
    let Some(source_image) = media.folder_images.first() else {
        return Ok(None);
    };
    let Some(parent) = managed_path.parent() else {
        return Ok(None);
    };
    let extension = source_image
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("jpg");
    let target = parent.join(format!("cover.{extension}"));
    if !paths_equal(source_image, &target) {
        fs::copy(source_image, &target)?;
    }

    Ok(Some(ArtworkAssetDraft {
        entity_type: match grouping {
            CatalogGrouping::Music(_) => CatalogEntityType::Album,
            CatalogGrouping::Podcast(_) => CatalogEntityType::Podcast,
        },
        provider: ProviderKind::LocalSidecars,
        artwork_kind: ArtworkKind::Cover,
        source_uri: None,
        file_path: Some(target.to_string_lossy().to_string()),
        mime_type: crate::media::mime_type_for_path(&target).map(str::to_string),
        width: None,
        height: None,
        confidence: 0.98,
    }))
}

/// Downloads provider-sourced artwork into the managed library so the authenticated artwork API can serve it later.
async fn materialize_remote_artwork(request: &mut CatalogImportRequest, managed_path: &Path) {
    let client = remote_artwork_client();
    let grouping = request.grouping.clone();

    for artwork in &mut request.artwork {
        if artwork.file_path.is_some() {
            continue;
        }
        let Some(source_uri) = artwork
            .source_uri
            .as_deref()
            .map(str::trim)
            .filter(|value| value.starts_with("https://") || value.starts_with("http://"))
            .map(str::to_string)
        else {
            continue;
        };
        let Some(target) = remote_artwork_target_path(&grouping, artwork, managed_path, &source_uri)
        else {
            continue;
        };

        match download_remote_artwork(&client, &source_uri, &target).await {
            Ok(mime_type) => {
                artwork.file_path = Some(target.to_string_lossy().to_string());
                if artwork.mime_type.is_none() {
                    artwork.mime_type = mime_type;
                }
            }
            Err(error) => {
                warn!(
                    source_uri,
                    target = %target.display(),
                    error = %error,
                    "failed to materialize provider artwork"
                );
            }
        }
    }
}

fn remote_artwork_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(REMOTE_ARTWORK_TIMEOUT)
        .user_agent(format!(
            "HarmonixiaServer/{} (artwork materialization)",
            env!("CARGO_PKG_VERSION")
        ))
        .build()
        .unwrap_or_else(|error| {
            warn!(error = %error, "failed to build artwork HTTP client; falling back to defaults");
            reqwest::Client::new()
        })
}

async fn download_remote_artwork(
    client: &reqwest::Client,
    source_uri: &str,
    target: &Path,
) -> Result<Option<String>, String> {
    let response = client
        .get(source_uri)
        .header(header::ACCEPT, "image/*")
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status = response.status();
    if !status.is_success() {
        return Err(format!("provider returned HTTP {status}"));
    }
    if response
        .content_length()
        .is_some_and(|length| length > REMOTE_ARTWORK_MAX_BYTES)
    {
        return Err("provider artwork is larger than the configured limit".into());
    }

    let mime_type = response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(normalize_image_mime_type)
        .map(str::to_string);
    let bytes = response.bytes().await.map_err(|error| error.to_string())?;
    if bytes.len() as u64 > REMOTE_ARTWORK_MAX_BYTES {
        return Err("provider artwork is larger than the configured limit".into());
    }
    if bytes.is_empty() {
        return Err("provider artwork response was empty".into());
    }

    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| error.to_string())?;
    }
    tokio::fs::write(target, bytes)
        .await
        .map_err(|error| error.to_string())?;
    Ok(mime_type)
}

fn remote_artwork_target_path(
    grouping: &CatalogGrouping,
    artwork: &ArtworkAssetDraft,
    managed_path: &Path,
    source_uri: &str,
) -> Option<PathBuf> {
    let directory = match artwork.entity_type {
        CatalogEntityType::Artist => artist_artwork_directory(grouping, managed_path),
        CatalogEntityType::Album
        | CatalogEntityType::Track
        | CatalogEntityType::Podcast
        | CatalogEntityType::Episode => managed_path.parent().map(Path::to_path_buf),
        CatalogEntityType::MediaFile | CatalogEntityType::Playlist => None,
    }?;
    let extension = artwork_extension(artwork.mime_type.as_deref(), source_uri);
    let filename = format!(
        "{}-{}-{}.{}",
        artwork.entity_type.api_name(),
        artwork.artwork_kind.api_name(),
        artwork.provider.api_name(),
        extension
    );
    Some(directory.join("artwork").join(filename))
}

fn artist_artwork_directory(
    grouping: &CatalogGrouping,
    managed_path: &Path,
) -> Option<PathBuf> {
    match grouping {
        CatalogGrouping::Music(_) => managed_path
            .parent()
            .and_then(Path::parent)
            .map(Path::to_path_buf),
        CatalogGrouping::Podcast(_) => managed_path.parent().map(Path::to_path_buf),
    }
}

fn artwork_extension(mime_type: Option<&str>, source_uri: &str) -> &'static str {
    mime_type
        .and_then(normalize_image_mime_type)
        .and_then(extension_for_image_mime_type)
        .or_else(|| extension_from_uri(source_uri))
        .unwrap_or("jpg")
}

fn normalize_image_mime_type(value: &str) -> Option<&'static str> {
    match value.split(';').next()?.trim().to_ascii_lowercase().as_str() {
        "image/jpeg" | "image/jpg" => Some("image/jpeg"),
        "image/png" => Some("image/png"),
        "image/webp" => Some("image/webp"),
        "image/gif" => Some("image/gif"),
        _ => None,
    }
}

fn extension_for_image_mime_type(mime_type: &str) -> Option<&'static str> {
    match mime_type {
        "image/jpeg" => Some("jpg"),
        "image/png" => Some("png"),
        "image/webp" => Some("webp"),
        "image/gif" => Some("gif"),
        _ => None,
    }
}

fn extension_from_uri(source_uri: &str) -> Option<&'static str> {
    let path = source_uri.split(['?', '#']).next()?;
    let extension = Path::new(path)
        .extension()
        .and_then(|extension| extension.to_str())?;
    match extension.to_ascii_lowercase().as_str() {
        "jpg" | "jpeg" => Some("jpg"),
        "png" => Some("png"),
        "webp" => Some("webp"),
        "gif" => Some("gif"),
        _ => None,
    }
}

/// Handles add probe provenance for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `request`: `&mut CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
/// - `media`: `&ProbedMediaFile`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn add_probe_provenance(request: &mut CatalogImportRequest, media: &ProbedMediaFile) {
    request.provenance.push(MetadataProvenanceDraft {
        entity_type: CatalogEntityType::MediaFile,
        field_name: "file_hash".to_string(),
        provider: ProviderKind::LocalSidecars,
        value: json!(&media.facts.file_hash),
        confidence: 1.0,
        auto_accepted: true,
    });
    request.provenance.push(MetadataProvenanceDraft {
        entity_type: CatalogEntityType::MediaFile,
        field_name: "duration_seconds".to_string(),
        provider: ProviderKind::LocalSidecars,
        value: json!(media.facts.duration_seconds),
        confidence: if media.facts.duration_seconds.is_some() {
            0.9
        } else {
            0.4
        },
        auto_accepted: true,
    });
}

/// Handles add grouping decision provenance for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `request`: `&mut CatalogImportRequest`; expected to be a value satisfying the type contract shown in the function signature.
/// - `local_grouping`: `&CatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
/// - `decision`: `&ChosenGroupingDecision`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn add_grouping_decision_provenance(
    request: &mut CatalogImportRequest,
    local_grouping: &CatalogGrouping,
    decision: &ChosenGroupingDecision,
) {
    request.provenance.push(MetadataProvenanceDraft {
        entity_type: CatalogEntityType::MediaFile,
        field_name: "local_grouping".to_string(),
        provider: ProviderKind::LocalSidecars,
        value: json!(local_grouping),
        confidence: if local_grouping.is_stable() {
            0.7
        } else {
            0.45
        },
        auto_accepted: false,
    });
    request.provenance.push(MetadataProvenanceDraft {
        entity_type: CatalogEntityType::MediaFile,
        field_name: "chosen_grouping".to_string(),
        provider: decision
            .source_provider
            .unwrap_or(ProviderKind::LocalSidecars),
        value: json!({
            "grouping": &decision.grouping,
            "provider_influenced": decision.provider_influenced,
            "source_provider": decision.source_provider.map(|provider| provider.api_name()),
            "confidence": decision.confidence,
            "auto_accept_threshold": PROVIDER_AUTO_ACCEPT_CONFIDENCE_THRESHOLD,
            "embedded_tags_rewritten": false
        }),
        confidence: decision.confidence,
        auto_accepted: true,
    });
}

/// Handles first tag for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `media`: `&ProbedMediaFile`; expected to be a media domain value that has already passed upstream validation.
/// - `keys`: `&[&str]`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(String)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn first_tag(media: &ProbedMediaFile, keys: &[&str]) -> Option<String> {
    media.tags.get(keys).map(str::trim).map(str::to_string)
}

/// Handles parent component for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `levels_up`: `usize`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `Some(String)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn parent_component(path: &Path, levels_up: usize) -> Option<String> {
    let mut current = path.parent()?;
    for _ in 0..levels_up {
        current = current.parent()?;
    }
    current
        .file_name()
        .and_then(|value| value.to_str())
        .map(str::to_string)
}

/// Handles file stem for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `path`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `Some(String)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn file_stem(path: &Path) -> Option<String> {
    path.file_stem()
        .and_then(|value| value.to_str())
        .map(str::to_string)
}

/// Handles release year for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `media`: `&ProbedMediaFile`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `Some(i32)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn release_year(media: &ProbedMediaFile) -> Option<i32> {
    media.tags.get(&["date", "year", "releasedate"]).and_then(|value| {
        value
            .chars()
            .collect::<Vec<_>>()
            .windows(4)
            .find_map(|window| {
                let value = window.iter().collect::<String>();
                value.parse::<i32>().ok().filter(|year| (1800..=2200).contains(year))
            })
    })
}

/// Handles paths equal for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
///
/// Inputs:
/// - `left`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
/// - `right`: `&Path`; expected to be a filesystem path; callers should pass a path inside configured managed roots when required.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn paths_equal(left: &Path, right: &Path) -> bool {
    match (left.canonicalize(), right.canonicalize()) {
        (Ok(left), Ok(right)) => left == right,
        _ => left == right,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{MediaProbeFacts, MetadataMatchKind, MetadataProviderLinkDraft, ProviderStatus},
        media::LocalMediaTags,
    };

    /// Verifies that test config.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `SystemConfig` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn test_config() -> SystemConfig {
        SystemConfig {
            library_root: "/library".to_string(),
            dropbox_root: "/dropbox".to_string(),
            podcast_subtree: "Podcasts".to_string(),
            public_base_url: None,
            transcode_concurrency_limit: 2,
            scan_thread_count: DEFAULT_SCAN_THREAD_COUNT,
            updated_at: Utc::now(),
        }
    }

    /// Handles probed music file for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - `path`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `tags`: `LocalMediaTags`; expected to be a media domain value that has already passed upstream validation.
    ///
    /// Output:
    /// - Returns `ProbedMediaFile` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn probed_music_file(path: &str, tags: LocalMediaTags) -> ProbedMediaFile {
        ProbedMediaFile {
            source_path: PathBuf::from(path),
            facts: MediaProbeFacts {
                file_hash: "hash".to_string(),
                file_size: 12,
                mime_type: Some("audio/mpeg".to_string()),
                container: Some("mp3".to_string()),
                audio_codec: None,
                duration_seconds: Some(180),
                bitrate: None,
                sample_rate: None,
                channels: None,
            },
            tags,
            sidecar_paths: Vec::new(),
            folder_images: Vec::new(),
        }
    }

    /// Handles provider bundle for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
    /// - `auto_accepted`: `bool`; expected to be a boolean flag controlling the documented branch.
    ///
    /// Output:
    /// - Returns `ProviderMetadataBundle` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn provider_bundle(confidence: f32, auto_accepted: bool) -> ProviderMetadataBundle {
        ProviderMetadataBundle {
            provider_links: vec![MetadataProviderLinkDraft {
                entity_type: CatalogEntityType::Album,
                provider: ProviderKind::MusicBrainz,
                provider_item_id: "musicbrainz-release".to_string(),
                external_url: Some(
                    "https://musicbrainz.org/release/musicbrainz-release".to_string(),
                ),
                match_kind: MetadataMatchKind::ModerateConfidence,
                confidence,
                auto_accepted,
                raw_metadata: json!({ "fixture": true }),
            }],
            provenance: vec![
                provider_provenance(
                    CatalogEntityType::Album,
                    "artist_name",
                    json!("Provider Artist"),
                    confidence,
                    auto_accepted,
                ),
                provider_provenance(
                    CatalogEntityType::Album,
                    "title",
                    json!("Provider Album"),
                    confidence,
                    auto_accepted,
                ),
                provider_provenance(
                    CatalogEntityType::Track,
                    "artist_name",
                    json!("Provider Artist"),
                    confidence,
                    auto_accepted,
                ),
                provider_provenance(
                    CatalogEntityType::Track,
                    "title",
                    json!("Provider Song"),
                    confidence,
                    auto_accepted,
                ),
                provider_provenance(
                    CatalogEntityType::Album,
                    "release_year",
                    json!(2001),
                    confidence,
                    auto_accepted,
                ),
            ],
            artwork: Vec::new(),
        }
    }

    /// Handles provider provenance for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - `entity_type`: `CatalogEntityType`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `field_name`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `value`: `Value`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
    /// - `auto_accepted`: `bool`; expected to be a boolean flag controlling the documented branch.
    ///
    /// Output:
    /// - Returns `MetadataProvenanceDraft` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn provider_provenance(
        entity_type: CatalogEntityType,
        field_name: &str,
        value: Value,
        confidence: f32,
        auto_accepted: bool,
    ) -> MetadataProvenanceDraft {
        MetadataProvenanceDraft {
            entity_type,
            field_name: field_name.to_string(),
            provider: ProviderKind::MusicBrainz,
            value,
            confidence,
            auto_accepted,
        }
    }

    #[test]
    /// Handles provider enriched grouping drives managed path for weak local guess for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn provider_enriched_grouping_drives_managed_path_for_weak_local_guess() {
        let config = test_config();
        let media =
            probed_music_file("/dropbox/Incoming/track01.mp3", LocalMediaTags::default());
        let local_grouping = infer_catalog_grouping(&config, &media);

        let decision = choose_catalog_grouping(
            &local_grouping,
            &provider_bundle(0.76, true),
            &media,
        );

        let CatalogGrouping::Music(grouping) = &decision.grouping else {
            panic!("expected music grouping");
        };
        assert!(decision.provider_influenced);
        assert_eq!(grouping.album_artist, "Provider Artist");
        assert_eq!(grouping.track_artist, "Provider Artist");
        assert_eq!(grouping.album_title, "Provider Album");
        assert_eq!(grouping.track_title, "Provider Song");
        assert_eq!(grouping.release_year, Some(2001));

        let managed_path = managed_path_for_grouping(
            &config,
            &decision.grouping,
            Path::new("/dropbox/Incoming/track01.mp3"),
        )
        .expect("provider-corrected grouping should be stable");
        assert_eq!(
            managed_path,
            PathBuf::from("/library/Provider Artist/Provider Album/Provider Song.mp3")
        );
    }

    #[test]
    /// Handles provider grouping candidate below threshold keeps local guess for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn provider_grouping_candidate_below_threshold_keeps_local_guess() {
        let config = test_config();
        let media =
            probed_music_file("/dropbox/Incoming/track01.mp3", LocalMediaTags::default());
        let local_grouping = infer_catalog_grouping(&config, &media);

        let decision = choose_catalog_grouping(
            &local_grouping,
            &provider_bundle(
                PROVIDER_AUTO_ACCEPT_CONFIDENCE_THRESHOLD - 0.01,
                false,
            ),
            &media,
        );

        let CatalogGrouping::Music(grouping) = &decision.grouping else {
            panic!("expected music grouping");
        };
        assert!(!decision.provider_influenced);
        assert_eq!(grouping.album_artist, "dropbox");
        assert_eq!(grouping.album_title, "Incoming");
        assert_eq!(grouping.track_title, "track01");

        let managed_path = managed_path_for_grouping(
            &config,
            &decision.grouping,
            Path::new("/dropbox/Incoming/track01.mp3"),
        )
        .expect("local path-derived grouping is stable even when weak");
        assert_eq!(
            managed_path,
            PathBuf::from("/library/dropbox/Incoming/track01.mp3")
        );
    }

    #[test]
    /// Handles provider execution failure sets bounded backoff and success clears it for maintenance import pipeline that scans files, enriches metadata, and mutates the catalog.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn provider_execution_failure_sets_bounded_backoff_and_success_clears_it() {
        let now = Utc::now();
        let mut health = ProviderHealth::healthy(ProviderKind::MusicBrainz, now);
        let failure = ProviderExecutionOutcome {
            provider: ProviderKind::MusicBrainz,
            attempted: true,
            attempts: 3,
            successful_requests: 0,
            failures: vec!["provider returned HTTP 500".to_string()],
        };

        apply_provider_execution_outcome(&mut health, &failure);

        assert_eq!(health.status, ProviderStatus::BackingOff);
        assert!(!health.maintenance_ready);
        assert_eq!(health.failure_count, 1);
        assert!(health.retry_after.is_some());
        assert!(health.message.as_deref().unwrap().contains("MusicBrainz"));

        let success = ProviderExecutionOutcome {
            provider: ProviderKind::MusicBrainz,
            attempted: true,
            attempts: 1,
            successful_requests: 1,
            failures: Vec::new(),
        };
        apply_provider_execution_outcome(&mut health, &success);

        assert_eq!(health.status, ProviderStatus::Healthy);
        assert!(health.maintenance_ready);
        assert_eq!(health.failure_count, 0);
        assert!(health.retry_after.is_none());
        assert!(health.message.is_none());
    }
}
