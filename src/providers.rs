use std::{
    collections::BTreeMap,
    env,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use reqwest::StatusCode;
use serde::{de::DeserializeOwned, Deserialize, Deserializer, Serialize};
use serde_json::{json, Value};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::{
    catalog::normalize_catalog_text,
    domain::{
        ArtworkAssetDraft, ArtworkKind, CatalogEntityType, CatalogGrouping, MediaKind,
        MetadataMatchKind, MetadataProviderLinkDraft, MetadataProvenanceDraft,
        MusicCatalogGrouping, PodcastCatalogGrouping, ProviderHealth, ProviderKind,
        ProviderStatus,
    },
    media::ProbedMediaFile,
};

pub const PROVIDER_AUTO_ACCEPT_CONFIDENCE_THRESHOLD: f32 = 0.66;
const PROVIDER_HTTP_TIMEOUT: Duration = Duration::from_secs(5);
const PROVIDER_ENRICHMENT_MAX_ATTEMPTS: u32 = 3;
const PROVIDER_ENRICHMENT_RETRY_BACKOFF: Duration = Duration::from_millis(50);
const USER_AGENT: &str = concat!(
    "HarmonixiaServer/",
    env!("CARGO_PKG_VERSION"),
    " (metadata enrichment)"
);

#[derive(Debug, Clone, Default)]
/// Represents provider metadata bundle in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `provider_links`, `provenance`, `artwork` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Vec<MetadataProviderLinkDraft>`, `Vec<MetadataProvenanceDraft>`, `Vec<ArtworkAssetDraft>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`, `src/providers.rs`.
pub struct ProviderMetadataBundle {
    pub provider_links: Vec<MetadataProviderLinkDraft>,
    pub provenance: Vec<MetadataProvenanceDraft>,
    pub artwork: Vec<ArtworkAssetDraft>,
}

impl ProviderMetadataBundle {
    /// Handles extend for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `other`: `ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn extend(&mut self, other: ProviderMetadataBundle) {
        self.provider_links.extend(other.provider_links);
        self.provenance.extend(other.provenance);
        self.artwork.extend(other.artwork);
    }
}

#[derive(Debug, Clone, Default)]
/// Represents provider enrichment report in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `bundle`, `outcomes` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `ProviderMetadataBundle`, `Vec<ProviderExecutionOutcome>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
pub struct ProviderEnrichmentReport {
    pub bundle: ProviderMetadataBundle,
    pub outcomes: Vec<ProviderExecutionOutcome>,
}

#[derive(Debug, Clone)]
/// Represents provider enrichment result in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `bundle`, `outcome` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `ProviderMetadataBundle`, `ProviderExecutionOutcome` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
pub struct ProviderEnrichmentResult {
    pub bundle: ProviderMetadataBundle,
    pub outcome: ProviderExecutionOutcome,
}

impl ProviderEnrichmentResult {
    /// Constructs a new instance for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn new(provider: ProviderKind) -> Self {
        Self {
            bundle: ProviderMetadataBundle::default(),
            outcome: ProviderExecutionOutcome::new(provider),
        }
    }
}

#[derive(Debug, Clone)]
/// Represents provider execution outcome in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `provider`, `attempted`, `attempts`, `successful_requests`, `failures` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `ProviderKind`, `bool`, `u32`, `u32`, `Vec<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`, `src/providers.rs`.
pub struct ProviderExecutionOutcome {
    pub provider: ProviderKind,
    pub attempted: bool,
    pub attempts: u32,
    pub successful_requests: u32,
    pub failures: Vec<String>,
}

impl ProviderExecutionOutcome {
    /// Constructs a new instance for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn new(provider: ProviderKind) -> Self {
        Self {
            provider,
            attempted: false,
            attempts: 1,
            successful_requests: 0,
            failures: Vec::new(),
        }
    }

    /// Handles local success for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn local_success(provider: ProviderKind) -> Self {
        let mut outcome = Self::new(provider);
        outcome.attempted = true;
        outcome.successful_requests = 1;
        outcome
    }

    /// Handles has failures for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn has_failures(&self) -> bool {
        !self.failures.is_empty()
    }

    /// Handles observe http for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `result`: `&Result<T, ProviderHttpError>`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn observe_http<T>(&mut self, result: &Result<T, ProviderHttpError>) {
        self.attempted = true;
        match result {
            Ok(_) | Err(ProviderHttpError::NotFound) => {
                self.successful_requests += 1;
            }
            Err(error) => {
                self.failures.push(error.to_string());
            }
        }
    }
}

#[derive(Debug, Clone)]
/// Represents provider credential in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `provider`, `api_key`, `api_secret` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `ProviderKind`, `Option<String>`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`, `src/providers.rs`, `src/storage.rs`, `tests/maintenance_api.rs`.
pub struct ProviderCredential {
    pub provider: ProviderKind,
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
}

impl ProviderCredential {
    /// Constructs a new instance for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    /// - `api_key`: `Option<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `api_secret`: `Option<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn new(
        provider: ProviderKind,
        api_key: Option<String>,
        api_secret: Option<String>,
    ) -> Self {
        Self {
            provider,
            api_key: normalize_optional_secret(api_key),
            api_secret: normalize_optional_secret(api_secret),
        }
    }

    /// Handles empty for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn empty(provider: ProviderKind) -> Self {
        Self {
            provider,
            api_key: None,
            api_secret: None,
        }
    }

    /// Handles has api key for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn has_api_key(&self) -> bool {
        self.api_key
            .as_deref()
            .map(|value| !value.trim().is_empty())
            .unwrap_or(false)
    }
}

#[derive(Clone, Copy)]
/// Represents provider enrichment context in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `grouping`, `media`, `prior_links` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `&'a CatalogGrouping`, `&'a ProbedMediaFile`, `&'a [MetadataProviderLinkDraft]` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
pub struct ProviderEnrichmentContext<'a> {
    pub grouping: &'a CatalogGrouping,
    pub media: &'a ProbedMediaFile,
    pub prior_links: &'a [MetadataProviderLinkDraft],
}

#[async_trait]
/// Represents metadata provider in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Defines required behavior through methods `kind`, `enrich` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `ProviderKind`, `ProviderEnrichmentContext`, `ProviderEnrichmentResult` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
pub trait MetadataProvider {
    /// Handles kind for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `ProviderKind` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn kind(&self) -> ProviderKind;

    /// Handles enrich for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `context`: `ProviderEnrichmentContext<'_>`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `ProviderEnrichmentResult` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    async fn enrich(
        &self,
        context: ProviderEnrichmentContext<'_>,
    ) -> ProviderEnrichmentResult;
}

/// Represents provider registry in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `providers`, `provider_kinds` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Vec<Box<dyn MetadataProvider + Send + Sync>>`, `Vec<ProviderKind>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/pipeline.rs`, `src/providers.rs`, `tests/maintenance_api.rs`.
pub struct ProviderRegistry {
    providers: Vec<Box<dyn MetadataProvider + Send + Sync>>,
    provider_kinds: Vec<ProviderKind>,
}

impl ProviderRegistry {
    /// Handles from health for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `provider_health`: `&[ProviderHealth]`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `provider_filter`: `&[ProviderKind]`; expected to be one of the supported metadata provider identifiers.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn from_health(
        provider_health: &[ProviderHealth],
        provider_filter: &[ProviderKind],
    ) -> Self {
        Self::from_health_and_credentials(provider_health, &[], provider_filter)
    }

    /// Handles from health and credentials for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `provider_health`: `&[ProviderHealth]`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `credentials`: `&[ProviderCredential]`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `provider_filter`: `&[ProviderKind]`; expected to be one of the supported metadata provider identifiers.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn from_health_and_credentials(
        provider_health: &[ProviderHealth],
        credentials: &[ProviderCredential],
        provider_filter: &[ProviderKind],
    ) -> Self {
        let credential_by_provider = credentials
            .iter()
            .cloned()
            .map(|credential| (credential.provider, credential))
            .collect::<BTreeMap<_, _>>();

        let mut provider_configs = provider_health
            .iter()
            .filter_map(|health| {
                if !provider_is_ready(health) {
                    return None;
                }
                if !provider_filter.is_empty() && !provider_filter.contains(&health.provider) {
                    return None;
                }

                let credential = credential_by_provider
                    .get(&health.provider)
                    .cloned()
                    .unwrap_or_else(|| ProviderCredential::empty(health.provider));

                if provider_requires_runtime_key(health.provider) && !credential.has_api_key() {
                    debug!(
                        provider = health.provider.api_name(),
                        "skipping provider because no runtime API key was loaded"
                    );
                    return None;
                }

                Some((health.provider, credential))
            })
            .collect::<Vec<_>>();

        provider_configs.sort_by_key(|(provider, _)| *provider);
        provider_configs.dedup_by_key(|(provider, _)| *provider);

        let client = build_http_client();
        let provider_kinds = provider_configs
            .iter()
            .map(|(provider, _)| *provider)
            .collect::<Vec<_>>();
        let providers = provider_configs
            .into_iter()
            .map(|(provider, credential)| provider_adapter(provider, credential, client.clone()))
            .collect();

        Self {
            providers,
            provider_kinds,
        }
    }

    /// Handles enrich for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `grouping`: `&CatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `media`: `&ProbedMediaFile`; expected to be a media domain value that has already passed upstream validation.
    ///
    /// Output:
    /// - Returns `ProviderEnrichmentReport` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub async fn enrich(
        &self,
        grouping: &CatalogGrouping,
        media: &ProbedMediaFile,
    ) -> ProviderEnrichmentReport {
        let mut report = ProviderEnrichmentReport::default();
        for provider in &self.providers {
            let mut attempt = 1;
            let result = loop {
                let context = ProviderEnrichmentContext {
                    grouping,
                    media,
                    prior_links: &report.bundle.provider_links,
                };
                let mut result = provider.enrich(context).await;
                result.outcome.attempts = attempt;
                if !result.outcome.has_failures()
                    || attempt >= PROVIDER_ENRICHMENT_MAX_ATTEMPTS
                {
                    break result;
                }
                attempt += 1;
                tokio::time::sleep(PROVIDER_ENRICHMENT_RETRY_BACKOFF).await;
            };
            report.bundle.extend(result.bundle);
            report.outcomes.push(result.outcome);
        }
        report
    }

    /// Handles providers for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `&[ProviderKind]` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    pub fn providers(&self) -> &[ProviderKind] {
        &self.provider_kinds
    }
}

/// Handles provider is ready for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `health`: `&ProviderHealth`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn provider_is_ready(health: &ProviderHealth) -> bool {
    let now = Utc::now();
    provider_refresh_ready_at(health, &now)
}

/// Handles provider refresh ready at for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `health`: `&ProviderHealth`; expected to be a value satisfying the type contract shown in the function signature.
/// - `now`: `&DateTime<Utc>`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
pub fn provider_refresh_ready_at(
    health: &ProviderHealth,
    now: &DateTime<Utc>,
) -> bool {
    if !health.enabled
        || matches!(
            health.status,
            ProviderStatus::Disabled | ProviderStatus::Unconfigured
        )
    {
        return false;
    }

    if health.status == ProviderStatus::BackingOff {
        return health
            .retry_after
            .as_ref()
            .map(|retry_after| retry_after <= now)
            .unwrap_or(false);
    }

    health.maintenance_ready
}

/// Handles provider backoff active at for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `health`: `&ProviderHealth`; expected to be a value satisfying the type contract shown in the function signature.
/// - `now`: `&DateTime<Utc>`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
pub fn provider_backoff_active_at(
    health: &ProviderHealth,
    now: &DateTime<Utc>,
) -> bool {
    health.enabled
        && health.status == ProviderStatus::BackingOff
        && health
            .retry_after
            .as_ref()
            .map(|retry_after| retry_after > now)
            .unwrap_or(false)
}

/// Reconciles state for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `health`: `&mut ProviderHealth`; expected to be a value satisfying the type contract shown in the function signature.
/// - `now`: `&DateTime<Utc>`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
pub fn reconcile_provider_readiness(
    health: &mut ProviderHealth,
    now: &DateTime<Utc>,
) -> bool {
    let original_status = health.status;
    let original_ready = health.maintenance_ready;
    let original_retry_after = health.retry_after.clone();
    let original_message = health.message.clone();

    if !health.enabled {
        health.maintenance_ready = false;
    } else if matches!(
        health.status,
        ProviderStatus::Disabled | ProviderStatus::Unconfigured
    ) {
        health.maintenance_ready = false;
    } else if health.status == ProviderStatus::BackingOff {
        if provider_backoff_active_at(health, now) {
            health.maintenance_ready = false;
        } else if health.retry_after.is_some() {
            health.status = ProviderStatus::Degraded;
            health.maintenance_ready = true;
            health.retry_after = None;
            health.message = Some(
                "Provider retry backoff has elapsed; maintenance can retry this provider."
                    .to_string(),
            );
        } else {
            health.maintenance_ready = false;
        }
    }

    let changed = health.status != original_status
        || health.maintenance_ready != original_ready
        || health.retry_after != original_retry_after
        || health.message != original_message;
    if changed {
        health.updated_at = now.clone();
    }
    changed
}

/// Handles provider requires runtime key for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn provider_requires_runtime_key(provider: ProviderKind) -> bool {
    matches!(
        provider,
        ProviderKind::Discogs | ProviderKind::FanartTv | ProviderKind::TheAudioDb
    )
}

/// Handles provider adapter for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `credential`: `ProviderCredential`; expected to be a value satisfying the type contract shown in the function signature.
/// - `client`: `reqwest:Client`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Box<dyn MetadataProvider + Send + Sync>` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn provider_adapter(
    provider: ProviderKind,
    credential: ProviderCredential,
    client: reqwest::Client,
) -> Box<dyn MetadataProvider + Send + Sync> {
    match provider {
        ProviderKind::MusicBrainz => Box::new(MusicBrainzProvider::new(credential, client)),
        ProviderKind::CoverArtArchive => {
            Box::new(CoverArtArchiveProvider::new(credential, client))
        }
        ProviderKind::Discogs => Box::new(DiscogsProvider::new(credential, client)),
        ProviderKind::FanartTv => Box::new(FanartTvProvider::new(credential, client)),
        ProviderKind::TheAudioDb => Box::new(TheAudioDbProvider::new(credential, client)),
        ProviderKind::LocalSidecars => Box::new(LocalSidecarProvider),
    }
}

/// Handles build http client for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `reqwest::Client` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn build_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(PROVIDER_HTTP_TIMEOUT)
        .user_agent(USER_AGENT)
        .build()
        .unwrap_or_else(|error| {
            warn!(error = %error, "failed to build provider HTTP client; falling back to defaults");
            reqwest::Client::new()
        })
}

/// Represents provider http in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `client`, `base_url`, `min_interval`, `last_request_at` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `reqwest::Client`, `String`, `Duration`, `Mutex<Option<Instant>>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct ProviderHttp {
    client: reqwest::Client,
    base_url: String,
    min_interval: Duration,
    last_request_at: Mutex<Option<Instant>>,
}

impl ProviderHttp {
    /// Constructs a new instance for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `client`: `reqwest:Client`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `base_url`: `String`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `min_interval`: `Duration`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn new(client: reqwest::Client, base_url: String, min_interval: Duration) -> Self {
        Self {
            client,
            base_url,
            min_interval,
            last_request_at: Mutex::new(None),
        }
    }

    /// Retrieves a resource for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `path`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `query`: `&[(&str, String)]`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `T` on success or `ProviderHttpError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ProviderHttpError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn get_json<T>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T, ProviderHttpError>
    where
        T: DeserializeOwned,
    {
        self.wait_for_rate_limit().await;

        let url = format!("{}{}", self.base_url, path);
        let response = self
            .client
            .get(url)
            .header("accept", "application/json")
            .query(query)
            .send()
            .await?;

        let status = response.status();
        if status == StatusCode::NOT_FOUND || status == StatusCode::NO_CONTENT {
            return Err(ProviderHttpError::NotFound);
        }
        if !status.is_success() {
            return Err(ProviderHttpError::Status(status));
        }

        Ok(response.json::<T>().await?)
    }

    /// Waits for asynchronous completion for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns a future that resolves to `()` after the operation completes.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    async fn wait_for_rate_limit(&self) {
        if self.min_interval.is_zero() {
            return;
        }

        let mut last_request_at = self.last_request_at.lock().await;
        if let Some(last_request_at) = *last_request_at {
            let elapsed = last_request_at.elapsed();
            if elapsed < self.min_interval {
                tokio::time::sleep(self.min_interval - elapsed).await;
            }
        }
        *last_request_at = Some(Instant::now());
    }
}

#[derive(Debug, Error)]
/// Represents provider http error in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Enumerates `Request`, `Status`, `NotFound` states or choices for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on the enum variants plus any derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
enum ProviderHttpError {
    #[error("request failed: {0}")]
    Request(#[from] reqwest::Error),
    #[error("provider returned HTTP {0}")]
    Status(StatusCode),
    #[error("provider result was not found")]
    NotFound,
}

impl ProviderHttpError {
    /// Handles is not found for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn is_not_found(&self) -> bool {
        matches!(self, Self::NotFound)
    }
}

/// Represents local sidecar provider in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Acts as a marker or zero-field value for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: has no direct field dependencies beyond derive macros or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct LocalSidecarProvider;

/// Represents music brainz provider in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `http`, `_api_key`, `_api_secret` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `ProviderHttp`, `Option<String>`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct MusicBrainzProvider {
    http: ProviderHttp,
    _api_key: Option<String>,
    _api_secret: Option<String>,
}

/// Represents cover art archive provider in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `http`, `_api_key`, `_api_secret` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `ProviderHttp`, `Option<String>`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct CoverArtArchiveProvider {
    http: ProviderHttp,
    _api_key: Option<String>,
    _api_secret: Option<String>,
}

/// Represents discogs provider in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `http`, `api_key`, `_api_secret` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `ProviderHttp`, `Option<String>`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct DiscogsProvider {
    http: ProviderHttp,
    api_key: Option<String>,
    _api_secret: Option<String>,
}

/// Represents fanart tv provider in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `http`, `api_key`, `_api_secret` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `ProviderHttp`, `Option<String>`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct FanartTvProvider {
    http: ProviderHttp,
    api_key: Option<String>,
    _api_secret: Option<String>,
}

/// Represents the audio db provider in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `http`, `api_key`, `_api_secret` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `ProviderHttp`, `Option<String>`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct TheAudioDbProvider {
    http: ProviderHttp,
    api_key: Option<String>,
    _api_secret: Option<String>,
}

impl MusicBrainzProvider {
    /// Constructs a new instance for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `credential`: `ProviderCredential`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `client`: `reqwest:Client`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn new(credential: ProviderCredential, client: reqwest::Client) -> Self {
        Self {
            http: ProviderHttp::new(
                client,
                provider_base_url(
                    ProviderKind::MusicBrainz,
                    "https://musicbrainz.org",
                ),
                Duration::from_secs(1),
            ),
            _api_key: credential.api_key,
            _api_secret: credential.api_secret,
        }
    }

    /// Searches resources for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `grouping`: `&MusicCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Option<MusicBrainzRecording>` on success or `ProviderHttpError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ProviderHttpError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn search_recording(
        &self,
        grouping: &MusicCatalogGrouping,
    ) -> Result<Option<MusicBrainzRecording>, ProviderHttpError> {
        let query = format!(
            "artist:{} AND release:{} AND recording:{}",
            lucene_quoted(&grouping.track_artist),
            lucene_quoted(&grouping.album_title),
            lucene_quoted(&grouping.track_title)
        );
        let response = self
            .http
            .get_json::<MusicBrainzRecordingSearch>(
                "/ws/2/recording",
                &[
                    ("query", query),
                    ("fmt", "json".to_string()),
                    ("limit", "1".to_string()),
                ],
            )
            .await?;

        Ok(response.recordings.into_iter().next())
    }

    /// Searches resources for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `grouping`: `&MusicCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Option<MusicBrainzRelease>` on success or `ProviderHttpError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ProviderHttpError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn search_release(
        &self,
        grouping: &MusicCatalogGrouping,
    ) -> Result<Option<MusicBrainzRelease>, ProviderHttpError> {
        let query = format!(
            "artist:{} AND release:{}",
            lucene_quoted(&grouping.album_artist),
            lucene_quoted(&grouping.album_title)
        );
        let response = self
            .http
            .get_json::<MusicBrainzReleaseSearch>(
                "/ws/2/release",
                &[
                    ("query", query),
                    ("fmt", "json".to_string()),
                    ("limit", "1".to_string()),
                ],
            )
            .await?;

        Ok(response.releases.into_iter().next())
    }

    /// Handles lookup recording for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `recording_id`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `MusicBrainzRecording` on success or `ProviderHttpError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ProviderHttpError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn lookup_recording(
        &self,
        recording_id: &str,
    ) -> Result<MusicBrainzRecording, ProviderHttpError> {
        self.http
            .get_json::<MusicBrainzRecording>(
                &format!("/ws/2/recording/{recording_id}"),
                &[
                    ("fmt", "json".to_string()),
                    ("inc", "artists+releases".to_string()),
                ],
            )
            .await
    }

    /// Handles lookup release for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `release_id`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `MusicBrainzRelease` on success or `ProviderHttpError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ProviderHttpError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn lookup_release(
        &self,
        release_id: &str,
    ) -> Result<MusicBrainzRelease, ProviderHttpError> {
        self.http
            .get_json::<MusicBrainzRelease>(
                &format!("/ws/2/release/{release_id}"),
                &[
                    ("fmt", "json".to_string()),
                    ("inc", "artist-credits".to_string()),
                ],
            )
            .await
    }
}

impl CoverArtArchiveProvider {
    /// Constructs a new instance for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `credential`: `ProviderCredential`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `client`: `reqwest:Client`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn new(credential: ProviderCredential, client: reqwest::Client) -> Self {
        Self {
            http: ProviderHttp::new(
                client,
                provider_base_url(
                    ProviderKind::CoverArtArchive,
                    "https://coverartarchive.org",
                ),
                Duration::from_millis(250),
            ),
            _api_key: credential.api_key,
            _api_secret: credential.api_secret,
        }
    }
}

impl DiscogsProvider {
    /// Constructs a new instance for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `credential`: `ProviderCredential`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `client`: `reqwest:Client`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn new(credential: ProviderCredential, client: reqwest::Client) -> Self {
        Self {
            http: ProviderHttp::new(
                client,
                provider_base_url(ProviderKind::Discogs, "https://api.discogs.com"),
                Duration::from_secs(1),
            ),
            api_key: credential.api_key,
            _api_secret: credential.api_secret,
        }
    }

    /// Searches resources for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `grouping`: `&MusicCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Option<DiscogsSearchResult>` on success or `ProviderHttpError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ProviderHttpError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn search_release(
        &self,
        grouping: &MusicCatalogGrouping,
    ) -> Result<Option<DiscogsSearchResult>, ProviderHttpError> {
        let Some(token) = self.api_key.as_deref() else {
            return Ok(None);
        };
        let response = self
            .http
            .get_json::<DiscogsSearchResponse>(
                "/database/search",
                &[
                    ("type", "release".to_string()),
                    ("artist", grouping.album_artist.clone()),
                    ("release_title", grouping.album_title.clone()),
                    ("track", grouping.track_title.clone()),
                    ("token", token.to_string()),
                ],
            )
            .await?;

        Ok(response.results.into_iter().next())
    }
}

impl FanartTvProvider {
    /// Constructs a new instance for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `credential`: `ProviderCredential`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `client`: `reqwest:Client`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn new(credential: ProviderCredential, client: reqwest::Client) -> Self {
        Self {
            http: ProviderHttp::new(
                client,
                provider_base_url(
                    ProviderKind::FanartTv,
                    "https://webservice.fanart.tv",
                ),
                Duration::from_millis(500),
            ),
            api_key: credential.api_key,
            _api_secret: credential.api_secret,
        }
    }

    /// Handles artist images for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `artist_mbid`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Value` on success or `ProviderHttpError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ProviderHttpError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn artist_images(&self, artist_mbid: &str) -> Result<Value, ProviderHttpError> {
        let Some(api_key) = self.api_key.as_deref() else {
            return Ok(Value::Null);
        };
        self.http
            .get_json::<Value>(
                &format!("/v3/music/{artist_mbid}"),
                &[("api_key", api_key.to_string())],
            )
            .await
    }
}

impl TheAudioDbProvider {
    /// Constructs a new instance for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `credential`: `ProviderCredential`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `client`: `reqwest:Client`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Self` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn new(credential: ProviderCredential, client: reqwest::Client) -> Self {
        Self {
            http: ProviderHttp::new(
                client,
                provider_base_url(
                    ProviderKind::TheAudioDb,
                    "https://www.theaudiodb.com",
                ),
                Duration::from_millis(500),
            ),
            api_key: credential.api_key,
            _api_secret: credential.api_secret,
        }
    }

    /// Searches resources for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `grouping`: `&MusicCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `Option<TheAudioDbAlbum>` on success or `ProviderHttpError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ProviderHttpError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn search_album(
        &self,
        grouping: &MusicCatalogGrouping,
    ) -> Result<Option<TheAudioDbAlbum>, ProviderHttpError> {
        let Some(api_key) = self.api_key.as_deref() else {
            return Ok(None);
        };
        let response = self
            .http
            .get_json::<TheAudioDbAlbumResponse>(
                &format!("/api/v1/json/{api_key}/searchalbum.php"),
                &[
                    ("s", grouping.album_artist.clone()),
                    ("a", grouping.album_title.clone()),
                ],
            )
            .await?;

        Ok(response.album.into_iter().flatten().next())
    }

    /// Searches resources for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `artist`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `Option<TheAudioDbArtist>` on success or `ProviderHttpError` when the operation cannot be completed.
    ///
    /// Errors:
    /// - Returns `ProviderHttpError` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
    async fn search_artist(
        &self,
        artist: &str,
    ) -> Result<Option<TheAudioDbArtist>, ProviderHttpError> {
        let Some(api_key) = self.api_key.as_deref() else {
            return Ok(None);
        };
        let response = self
            .http
            .get_json::<TheAudioDbArtistResponse>(
                &format!("/api/v1/json/{api_key}/search.php"),
                &[("s", artist.to_string())],
            )
            .await?;

        Ok(response.artists.into_iter().flatten().next())
    }
}

/// Handles provider base url for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `default`: `&'static str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn provider_base_url(provider: ProviderKind, default: &'static str) -> String {
    let env_name = format!(
        "HARMONIXIA_PROVIDER_{}_BASE_URL",
        provider.api_name().to_ascii_uppercase()
    );
    env::var(env_name)
        .ok()
        .map(|value| value.trim().trim_end_matches('/').to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| default.to_string())
}

#[async_trait]
impl MetadataProvider for LocalSidecarProvider {
    /// Handles kind for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `ProviderKind` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn kind(&self) -> ProviderKind {
        ProviderKind::LocalSidecars
    }

    /// Handles enrich for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `context`: `ProviderEnrichmentContext<'_>`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `ProviderEnrichmentResult` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    async fn enrich(
        &self,
        context: ProviderEnrichmentContext<'_>,
    ) -> ProviderEnrichmentResult {
        let mut result = ProviderEnrichmentResult::new(self.kind());
        match context.grouping {
            CatalogGrouping::Music(grouping) => {
                add_music_provenance(&mut result.bundle, self.kind(), grouping, 0.92, true);
                result.bundle.provider_links.push(local_link(
                    CatalogEntityType::Track,
                    self.kind(),
                    &format!(
                        "{}:{}:{}",
                        grouping.track_artist, grouping.album_title, grouping.track_title
                    ),
                    0.92,
                ));
                for image in &context.media.folder_images {
                    result.bundle.artwork.push(ArtworkAssetDraft {
                        entity_type: CatalogEntityType::Album,
                        provider: self.kind(),
                        artwork_kind: ArtworkKind::Cover,
                        source_uri: None,
                        file_path: Some(image.to_string_lossy().to_string()),
                        mime_type: crate::media::mime_type_for_path(image).map(str::to_string),
                        width: None,
                        height: None,
                        confidence: 0.95,
                    });
                }
            }
            CatalogGrouping::Podcast(grouping) => {
                add_podcast_provenance(&mut result.bundle, self.kind(), grouping, 0.92, true);
                result.bundle.provider_links.push(local_link(
                    CatalogEntityType::Episode,
                    self.kind(),
                    &format!("{}:{}", grouping.podcast_title, grouping.episode_title),
                    0.92,
                ));
                for image in &context.media.folder_images {
                    result.bundle.artwork.push(ArtworkAssetDraft {
                        entity_type: CatalogEntityType::Podcast,
                        provider: self.kind(),
                        artwork_kind: ArtworkKind::Cover,
                        source_uri: None,
                        file_path: Some(image.to_string_lossy().to_string()),
                        mime_type: crate::media::mime_type_for_path(image).map(str::to_string),
                        width: None,
                        height: None,
                        confidence: 0.95,
                    });
                }
            }
        }
        result.outcome = ProviderExecutionOutcome::local_success(self.kind());
        result
    }
}

#[async_trait]
impl MetadataProvider for MusicBrainzProvider {
    /// Handles kind for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `ProviderKind` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn kind(&self) -> ProviderKind {
        ProviderKind::MusicBrainz
    }

    /// Handles enrich for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `context`: `ProviderEnrichmentContext<'_>`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `ProviderEnrichmentResult` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    async fn enrich(
        &self,
        context: ProviderEnrichmentContext<'_>,
    ) -> ProviderEnrichmentResult {
        let mut result = ProviderEnrichmentResult::new(self.kind());
        let CatalogGrouping::Music(grouping) = context.grouping else {
            return result;
        };

        if let Some(track_id) = context
            .media
            .tags
            .get(&["musicbrainz_trackid", "musicbrainzreleasetrackid"])
        {
            let lookup = self.lookup_recording(track_id).await;
            result.outcome.observe_http(&lookup);
            match lookup {
                Ok(recording) => {
                    add_musicbrainz_recording_with_confidence(
                        &mut result.bundle,
                        self.kind(),
                        recording,
                        0.99,
                        MetadataMatchKind::ExactIdentifier,
                    );
                }
                Err(error) => {
                    log_provider_error(self.kind(), &error);
                    result.bundle.provider_links.push(external_link_with_raw(
                        CatalogEntityType::Track,
                        self.kind(),
                        track_id,
                        Some(format!("https://musicbrainz.org/recording/{track_id}")),
                        MetadataMatchKind::ExactIdentifier,
                        0.99,
                        json!({ "source": "local_tags" }),
                    ));
                }
            }
        }

        if let Some(release_id) = context
            .media
            .tags
            .get(&["musicbrainz_albumid", "musicbrainz_releaseid"])
        {
            let lookup = self.lookup_release(release_id).await;
            result.outcome.observe_http(&lookup);
            match lookup {
                Ok(release) => {
                    add_musicbrainz_release_with_confidence(
                        &mut result.bundle,
                        self.kind(),
                        release,
                        0.99,
                        MetadataMatchKind::ExactIdentifier,
                    );
                }
                Err(error) => {
                    log_provider_error(self.kind(), &error);
                    result.bundle.provider_links.push(external_link_with_raw(
                        CatalogEntityType::Album,
                        self.kind(),
                        release_id,
                        Some(format!("https://musicbrainz.org/release/{release_id}")),
                        MetadataMatchKind::ExactIdentifier,
                        0.99,
                        json!({ "source": "local_tags" }),
                    ));
                }
            }
        }

        if !stable_music_grouping(grouping) {
            return result;
        }

        let recording_search = self.search_recording(grouping).await;
        result.outcome.observe_http(&recording_search);
        match recording_search {
            Ok(Some(recording)) => {
                add_musicbrainz_recording(&mut result.bundle, self.kind(), recording);
            }
            Ok(None) => {}
            Err(error) if error.is_not_found() => {}
            Err(error) => log_provider_error(self.kind(), &error),
        }

        if !has_provider_link(
            &result.bundle.provider_links,
            ProviderKind::MusicBrainz,
            CatalogEntityType::Album,
        ) {
            let release_search = self.search_release(grouping).await;
            result.outcome.observe_http(&release_search);
            match release_search {
                Ok(Some(release)) => {
                    add_musicbrainz_release(&mut result.bundle, self.kind(), release);
                }
                Ok(None) => {}
                Err(error) if error.is_not_found() => {}
                Err(error) => log_provider_error(self.kind(), &error),
            }
        }

        result
    }
}

#[async_trait]
impl MetadataProvider for CoverArtArchiveProvider {
    /// Handles kind for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `ProviderKind` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn kind(&self) -> ProviderKind {
        ProviderKind::CoverArtArchive
    }

    /// Handles enrich for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `context`: `ProviderEnrichmentContext<'_>`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `ProviderEnrichmentResult` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    async fn enrich(
        &self,
        context: ProviderEnrichmentContext<'_>,
    ) -> ProviderEnrichmentResult {
        let mut result = ProviderEnrichmentResult::new(self.kind());
        if !matches!(context.grouping, CatalogGrouping::Music(_)) {
            return result;
        };

        let Some(release_id) = musicbrainz_release_id(context) else {
            return result;
        };

        let path = format!("/release/{release_id}");
        let lookup = self.http.get_json::<CoverArtArchiveRelease>(&path, &[]).await;
        result.outcome.observe_http(&lookup);
        match lookup {
            Ok(release) => {
                add_cover_art_archive_release(
                    &mut result.bundle,
                    self.kind(),
                    &release_id,
                    release,
                );
            }
            Err(error) if error.is_not_found() => {}
            Err(error) => log_provider_error(self.kind(), &error),
        }

        result
    }
}

#[async_trait]
impl MetadataProvider for DiscogsProvider {
    /// Handles kind for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `ProviderKind` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn kind(&self) -> ProviderKind {
        ProviderKind::Discogs
    }

    /// Handles enrich for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `context`: `ProviderEnrichmentContext<'_>`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `ProviderEnrichmentResult` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    async fn enrich(
        &self,
        context: ProviderEnrichmentContext<'_>,
    ) -> ProviderEnrichmentResult {
        let mut result = ProviderEnrichmentResult::new(self.kind());
        let CatalogGrouping::Music(grouping) = context.grouping else {
            return result;
        };

        if self.api_key.is_none() {
            return result;
        }

        if let Some(release_id) = context
            .media
            .tags
            .get(&["discogs_release_id", "discogsreleaseid"])
        {
            result.bundle.provider_links.push(external_link_with_raw(
                CatalogEntityType::Album,
                self.kind(),
                release_id,
                Some(format!("https://www.discogs.com/release/{release_id}")),
                MetadataMatchKind::ExactIdentifier,
                0.96,
                json!({ "source": "local_tags" }),
            ));
        }

        if !stable_music_grouping(grouping) {
            return result;
        }

        let search = self.search_release(grouping).await;
        result.outcome.observe_http(&search);
        match search {
            Ok(Some(release)) => add_discogs_release(&mut result.bundle, self.kind(), release),
            Ok(None) => {}
            Err(error) if error.is_not_found() => {}
            Err(error) => log_provider_error(self.kind(), &error),
        }

        result
    }
}

#[async_trait]
impl MetadataProvider for FanartTvProvider {
    /// Handles kind for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `ProviderKind` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn kind(&self) -> ProviderKind {
        ProviderKind::FanartTv
    }

    /// Handles enrich for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `context`: `ProviderEnrichmentContext<'_>`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `ProviderEnrichmentResult` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    async fn enrich(
        &self,
        context: ProviderEnrichmentContext<'_>,
    ) -> ProviderEnrichmentResult {
        let mut result = ProviderEnrichmentResult::new(self.kind());
        if !matches!(context.grouping, CatalogGrouping::Music(_)) {
            return result;
        }
        if self.api_key.is_none() {
            return result;
        }

        let Some(artist_mbid) = musicbrainz_artist_id(context) else {
            return result;
        };

        let lookup = self.artist_images(&artist_mbid).await;
        result.outcome.observe_http(&lookup);
        match lookup {
            Ok(value) if value.is_object() => {
                add_fanart_artist_images(&mut result.bundle, self.kind(), &artist_mbid, value);
            }
            Ok(_) => {}
            Err(error) if error.is_not_found() => {}
            Err(error) => log_provider_error(self.kind(), &error),
        }

        result
    }
}

#[async_trait]
impl MetadataProvider for TheAudioDbProvider {
    /// Handles kind for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    ///
    /// Output:
    /// - Returns `ProviderKind` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn kind(&self) -> ProviderKind {
        ProviderKind::TheAudioDb
    }

    /// Handles enrich for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - the current instance; expected to have been initialized with its documented invariants.
    /// - `context`: `ProviderEnrichmentContext<'_>`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `ProviderEnrichmentResult` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    async fn enrich(
        &self,
        context: ProviderEnrichmentContext<'_>,
    ) -> ProviderEnrichmentResult {
        let mut result = ProviderEnrichmentResult::new(self.kind());
        let CatalogGrouping::Music(grouping) = context.grouping else {
            return result;
        };
        if self.api_key.is_none() || !stable_music_grouping(grouping) {
            return result;
        }

        let album_search = self.search_album(grouping).await;
        result.outcome.observe_http(&album_search);
        match album_search {
            Ok(Some(album)) => add_the_audio_db_album(&mut result.bundle, self.kind(), album),
            Ok(None) => {}
            Err(error) if error.is_not_found() => {}
            Err(error) => log_provider_error(self.kind(), &error),
        }

        let artist_search = self.search_artist(&grouping.album_artist).await;
        result.outcome.observe_http(&artist_search);
        match artist_search {
            Ok(Some(artist)) => add_the_audio_db_artist(&mut result.bundle, self.kind(), artist),
            Ok(None) => {}
            Err(error) if error.is_not_found() => {}
            Err(error) => log_provider_error(self.kind(), &error),
        }

        result
    }
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents music brainz recording search in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `recordings` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Vec<MusicBrainzRecording>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct MusicBrainzRecordingSearch {
    #[serde(default)]
    recordings: Vec<MusicBrainzRecording>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents music brainz release search in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `releases` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Vec<MusicBrainzRelease>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct MusicBrainzReleaseSearch {
    #[serde(default)]
    releases: Vec<MusicBrainzRelease>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents music brainz recording in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `id`, `title`, `score`, `artist_credit`, `releases` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Option<String>`, `Option<String>`, `Option<u32>`, `Vec<MusicBrainzArtistCredit>`, `Vec<MusicBrainzRelease>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct MusicBrainzRecording {
    id: Option<String>,
    title: Option<String>,
    score: Option<u32>,
    #[serde(default, rename = "artist-credit")]
    artist_credit: Vec<MusicBrainzArtistCredit>,
    #[serde(default)]
    releases: Vec<MusicBrainzRelease>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents music brainz artist credit in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `name`, `artist` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Option<String>`, `Option<MusicBrainzArtist>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct MusicBrainzArtistCredit {
    name: Option<String>,
    artist: Option<MusicBrainzArtist>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents music brainz artist in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `id`, `name`, `sort_name` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Option<String>`, `Option<String>`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct MusicBrainzArtist {
    id: Option<String>,
    name: Option<String>,
    #[serde(rename = "sort-name")]
    sort_name: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents music brainz release in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `id`, `title`, `score`, `date`, `status`, `artist_credit` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Option<String>`, `Option<String>`, `Option<u32>`, `Option<String>`, `Option<String>`, `Vec<MusicBrainzArtistCredit>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct MusicBrainzRelease {
    id: Option<String>,
    title: Option<String>,
    score: Option<u32>,
    date: Option<String>,
    status: Option<String>,
    #[serde(default, rename = "artist-credit")]
    artist_credit: Vec<MusicBrainzArtistCredit>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents cover art archive release in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `images` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Vec<CoverArtArchiveImage>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct CoverArtArchiveRelease {
    #[serde(default)]
    images: Vec<CoverArtArchiveImage>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents cover art archive image in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `image`, `front`, `approved`, `types`, `thumbnails` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Option<String>`, `Option<bool>`, `Option<bool>`, `Vec<String>`, `Option<BTreeMap<String` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct CoverArtArchiveImage {
    image: Option<String>,
    front: Option<bool>,
    approved: Option<bool>,
    #[serde(default)]
    types: Vec<String>,
    thumbnails: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents discogs search response in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `results` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Vec<DiscogsSearchResult>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct DiscogsSearchResponse {
    #[serde(default)]
    results: Vec<DiscogsSearchResult>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents discogs search result in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `id`, `title`, `uri`, `resource_url`, `cover_image`, `year`, `format` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Option<u64>`, `Option<String>`, `Option<String>`, `Option<String>`, `Option<String>`, `Option<i32>`, and 1 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct DiscogsSearchResult {
    id: Option<u64>,
    title: Option<String>,
    uri: Option<String>,
    resource_url: Option<String>,
    cover_image: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_i32")]
    year: Option<i32>,
    #[serde(default)]
    format: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents the audio db album response in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `album` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Option<Vec<TheAudioDbAlbum>>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct TheAudioDbAlbumResponse {
    album: Option<Vec<TheAudioDbAlbum>>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents the audio db album in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `id_album`, `id_artist`, `album`, `artist`, `year_released`, `album_thumb`, `album_cd_art` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Option<String>`, `Option<String>`, `Option<String>`, `Option<String>`, `Option<String>`, `Option<String>`, and 1 more and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct TheAudioDbAlbum {
    #[serde(rename = "idAlbum")]
    id_album: Option<String>,
    #[serde(rename = "idArtist")]
    id_artist: Option<String>,
    #[serde(rename = "strAlbum")]
    album: Option<String>,
    #[serde(rename = "strArtist")]
    artist: Option<String>,
    #[serde(rename = "intYearReleased")]
    year_released: Option<String>,
    #[serde(rename = "strAlbumThumb")]
    album_thumb: Option<String>,
    #[serde(rename = "strAlbumCDart")]
    album_cd_art: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents the audio db artist response in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `artists` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Option<Vec<TheAudioDbArtist>>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct TheAudioDbArtistResponse {
    artists: Option<Vec<TheAudioDbArtist>>,
}

#[derive(Debug, Deserialize, Serialize)]
/// Represents the audio db artist in the metadata provider registry and provider adapters used by the import pipeline.
///
/// Functionality: Carries fields `id_artist`, `artist`, `artist_thumb`, `artist_fanart`, `artist_fanart_2` for metadata provider registry and provider adapters used by the import pipeline.
/// Dependencies: depends on `Option<String>`, `Option<String>`, `Option<String>`, `Option<String>`, `Option<String>` and any derives or trait bounds declared on the type.
/// Used by: referenced from `src/providers.rs`.
struct TheAudioDbArtist {
    #[serde(rename = "idArtist")]
    id_artist: Option<String>,
    #[serde(rename = "strArtist")]
    artist: Option<String>,
    #[serde(rename = "strArtistThumb")]
    artist_thumb: Option<String>,
    #[serde(rename = "strArtistFanart")]
    artist_fanart: Option<String>,
    #[serde(rename = "strArtistFanart2")]
    artist_fanart_2: Option<String>,
}

/// Handles add musicbrainz recording for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `bundle`: `&mut ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `recording`: `MusicBrainzRecording`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn add_musicbrainz_recording(
    bundle: &mut ProviderMetadataBundle,
    provider: ProviderKind,
    recording: MusicBrainzRecording,
) {
    let confidence = confidence_from_score(recording.score, 0.7, 0.95);
    let match_kind = match_kind_for_confidence(confidence);
    add_musicbrainz_recording_with_confidence(
        bundle,
        provider,
        recording,
        confidence,
        match_kind,
    );
}

/// Handles add musicbrainz recording with confidence for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `bundle`: `&mut ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `recording`: `MusicBrainzRecording`; expected to be a value satisfying the type contract shown in the function signature.
/// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
/// - `match_kind`: `MetadataMatchKind`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn add_musicbrainz_recording_with_confidence(
    bundle: &mut ProviderMetadataBundle,
    provider: ProviderKind,
    recording: MusicBrainzRecording,
    confidence: f32,
    match_kind: MetadataMatchKind,
) {
    let raw_metadata = json_value(&recording);

    if let Some(recording_id) = non_empty_owned(recording.id.as_deref()) {
        bundle.provider_links.push(external_link_with_raw(
            CatalogEntityType::Track,
            provider,
            &recording_id,
            Some(format!("https://musicbrainz.org/recording/{recording_id}")),
            match_kind,
            confidence,
            raw_metadata.clone(),
        ));
    }

    if let Some(title) = non_empty_owned(recording.title.as_deref()) {
        push_provenance(
            bundle,
            CatalogEntityType::Track,
            provider,
            "title",
            json!(title),
            confidence,
        );
    }

    if let Some(artist_credit) = recording.artist_credit.first() {
        if let Some(artist) = artist_credit.artist.as_ref() {
            if let Some(artist_id) = non_empty_owned(artist.id.as_deref()) {
                bundle.provider_links.push(external_link_with_raw(
                    CatalogEntityType::Artist,
                    provider,
                    &artist_id,
                    Some(format!("https://musicbrainz.org/artist/{artist_id}")),
                    match_kind,
                    confidence,
                    json_value(artist),
                ));
            }
            if let Some(name) = non_empty_owned(
                artist
                    .name
                    .as_deref()
                    .or(artist_credit.name.as_deref()),
            ) {
                push_provenance(
                    bundle,
                    CatalogEntityType::Artist,
                    provider,
                    "name",
                    json!(name),
                    confidence,
                );
                push_provenance(
                    bundle,
                    CatalogEntityType::Track,
                    provider,
                    "artist_name",
                    json!(name),
                    confidence,
                );
            }
        }
    }

    if let Some(release) = recording.releases.into_iter().next() {
        add_musicbrainz_release_with_confidence(
            bundle,
            provider,
            release,
            confidence,
            match_kind_for_confidence(confidence),
        );
    }
}

/// Handles add musicbrainz release for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `bundle`: `&mut ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `release`: `MusicBrainzRelease`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn add_musicbrainz_release(
    bundle: &mut ProviderMetadataBundle,
    provider: ProviderKind,
    release: MusicBrainzRelease,
) {
    let confidence = confidence_from_score(release.score, 0.68, 0.92);
    add_musicbrainz_release_with_confidence(
        bundle,
        provider,
        release,
        confidence,
        match_kind_for_confidence(confidence),
    );
}

/// Handles add musicbrainz release with confidence for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `bundle`: `&mut ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `release`: `MusicBrainzRelease`; expected to be a value satisfying the type contract shown in the function signature.
/// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
/// - `match_kind`: `MetadataMatchKind`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn add_musicbrainz_release_with_confidence(
    bundle: &mut ProviderMetadataBundle,
    provider: ProviderKind,
    release: MusicBrainzRelease,
    confidence: f32,
    match_kind: MetadataMatchKind,
) {
    if let Some(release_id) = non_empty_owned(release.id.as_deref()) {
        bundle.provider_links.push(external_link_with_raw(
            CatalogEntityType::Album,
            provider,
            &release_id,
            Some(format!("https://musicbrainz.org/release/{release_id}")),
            match_kind,
            confidence,
            json_value(&release),
        ));
    }

    if let Some(artist_credit) = release.artist_credit.first() {
        if let Some(artist) = artist_credit.artist.as_ref() {
            if let Some(artist_id) = non_empty_owned(artist.id.as_deref()) {
                bundle.provider_links.push(external_link_with_raw(
                    CatalogEntityType::Artist,
                    provider,
                    &artist_id,
                    Some(format!("https://musicbrainz.org/artist/{artist_id}")),
                    match_kind,
                    confidence,
                    json_value(artist),
                ));
            }
            if let Some(name) = non_empty_owned(
                artist
                    .name
                    .as_deref()
                    .or(artist_credit.name.as_deref()),
            ) {
                push_provenance(
                    bundle,
                    CatalogEntityType::Artist,
                    provider,
                    "name",
                    json!(name),
                    confidence,
                );
                push_provenance(
                    bundle,
                    CatalogEntityType::Album,
                    provider,
                    "artist_name",
                    json!(name),
                    confidence,
                );
            }
        }
    }

    if let Some(title) = non_empty_owned(release.title.as_deref()) {
        push_provenance(
            bundle,
            CatalogEntityType::Album,
            provider,
            "title",
            json!(title),
            confidence,
        );
    }
    if let Some(year) = release_year_from_date(release.date.as_deref()) {
        push_provenance(
            bundle,
            CatalogEntityType::Album,
            provider,
            "release_year",
            json!(year),
            confidence,
        );
    }
}

/// Handles add cover art archive release for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `bundle`: `&mut ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `release_id`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `release`: `CoverArtArchiveRelease`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn add_cover_art_archive_release(
    bundle: &mut ProviderMetadataBundle,
    provider: ProviderKind,
    release_id: &str,
    release: CoverArtArchiveRelease,
) {
    let raw_metadata = json_value(&release);
    let Some(image) = release.images.iter().find(|image| is_front_image(image)) else {
        return;
    };
    let Some(source_uri) = image_url(image) else {
        return;
    };
    let confidence = if image.approved.unwrap_or(false) { 0.92 } else { 0.86 };

    bundle.provider_links.push(external_link_with_raw(
        CatalogEntityType::Album,
        provider,
        release_id,
        Some(format!("https://coverartarchive.org/release/{release_id}")),
        MetadataMatchKind::ExactIdentifier,
        confidence,
        raw_metadata,
    ));
    bundle.artwork.push(ArtworkAssetDraft {
        entity_type: CatalogEntityType::Album,
        provider,
        artwork_kind: ArtworkKind::Cover,
        mime_type: mime_type_for_uri(&source_uri).map(str::to_string),
        source_uri: Some(source_uri),
        file_path: None,
        width: None,
        height: None,
        confidence,
    });
}

/// Handles add discogs release for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `bundle`: `&mut ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `result`: `DiscogsSearchResult`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn add_discogs_release(
    bundle: &mut ProviderMetadataBundle,
    provider: ProviderKind,
    result: DiscogsSearchResult,
) {
    let Some(release_id) = result.id.map(|id| id.to_string()) else {
        return;
    };
    let confidence = 0.76;
    let external_url = result
        .uri
        .as_deref()
        .map(discogs_uri)
        .or_else(|| Some(format!("https://www.discogs.com/release/{release_id}")));

    bundle.provider_links.push(external_link_with_raw(
        CatalogEntityType::Album,
        provider,
        &release_id,
        external_url,
        match_kind_for_confidence(confidence),
        confidence,
        json_value(&result),
    ));

    if let Some(title) = non_empty_owned(result.title.as_deref()) {
        let (artist_name, album_title) = split_discogs_title(&title);
        if let Some(artist_name) = artist_name {
            push_provenance(
                bundle,
                CatalogEntityType::Artist,
                provider,
                "name",
                json!(artist_name),
                confidence,
            );
            push_provenance(
                bundle,
                CatalogEntityType::Album,
                provider,
                "artist_name",
                json!(artist_name),
                confidence,
            );
        }
        push_provenance(
            bundle,
            CatalogEntityType::Album,
            provider,
            "title",
            json!(album_title),
            confidence,
        );
    }
    if let Some(year) = result.year {
        push_provenance(
            bundle,
            CatalogEntityType::Album,
            provider,
            "release_year",
            json!(year),
            confidence,
        );
    }
    if let Some(cover_image) = result
        .cover_image
        .as_deref()
        .and_then(non_placeholder_image_url)
    {
        bundle.artwork.push(ArtworkAssetDraft {
            entity_type: CatalogEntityType::Album,
            provider,
            artwork_kind: ArtworkKind::Cover,
            source_uri: Some(cover_image.to_string()),
            file_path: None,
            mime_type: mime_type_for_uri(cover_image).map(str::to_string),
            width: None,
            height: None,
            confidence: 0.72,
        });
    }
}

/// Handles add fanart artist images for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `bundle`: `&mut ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `artist_mbid`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `value`: `Value`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn add_fanart_artist_images(
    bundle: &mut ProviderMetadataBundle,
    provider: ProviderKind,
    artist_mbid: &str,
    value: Value,
) {
    let confidence = 0.78;
    bundle.provider_links.push(external_link_with_raw(
        CatalogEntityType::Artist,
        provider,
        artist_mbid,
        Some(format!("https://fanart.tv/artist/{artist_mbid}")),
        MetadataMatchKind::ExactIdentifier,
        confidence,
        value.clone(),
    ));

    if let Some(name) = value.get("name").and_then(Value::as_str).and_then(non_empty) {
        push_provenance(
            bundle,
            CatalogEntityType::Artist,
            provider,
            "name",
            json!(name),
            confidence,
        );
    }

    if let Some(url) = fanart_image_url(&value, "artistthumb") {
        bundle.artwork.push(ArtworkAssetDraft {
            entity_type: CatalogEntityType::Artist,
            provider,
            artwork_kind: ArtworkKind::Artist,
            source_uri: Some(url.to_string()),
            file_path: None,
            mime_type: mime_type_for_uri(url).map(str::to_string),
            width: None,
            height: None,
            confidence: 0.78,
        });
    }

    if let Some(url) = fanart_image_url(&value, "artistbackground") {
        bundle.artwork.push(ArtworkAssetDraft {
            entity_type: CatalogEntityType::Artist,
            provider,
            artwork_kind: ArtworkKind::Fanart,
            source_uri: Some(url.to_string()),
            file_path: None,
            mime_type: mime_type_for_uri(url).map(str::to_string),
            width: None,
            height: None,
            confidence: 0.74,
        });
    }
}

/// Handles add the audio db album for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `bundle`: `&mut ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `album`: `TheAudioDbAlbum`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn add_the_audio_db_album(
    bundle: &mut ProviderMetadataBundle,
    provider: ProviderKind,
    album: TheAudioDbAlbum,
) {
    let confidence = 0.8;
    if let Some(album_id) = non_empty_owned(album.id_album.as_deref()) {
        bundle.provider_links.push(external_link_with_raw(
            CatalogEntityType::Album,
            provider,
            &album_id,
            Some(format!("https://www.theaudiodb.com/album/{album_id}")),
            match_kind_for_confidence(confidence),
            confidence,
            json_value(&album),
        ));
    }
    if let Some(artist_id) = non_empty_owned(album.id_artist.as_deref()) {
        bundle.provider_links.push(external_link_with_raw(
            CatalogEntityType::Artist,
            provider,
            &artist_id,
            Some(format!("https://www.theaudiodb.com/artist/{artist_id}")),
            match_kind_for_confidence(confidence),
            confidence,
            json_value(&album),
        ));
    }
    if let Some(title) = non_empty_owned(album.album.as_deref()) {
        push_provenance(
            bundle,
            CatalogEntityType::Album,
            provider,
            "title",
            json!(title),
            confidence,
        );
    }
    if let Some(artist) = non_empty_owned(album.artist.as_deref()) {
        push_provenance(
            bundle,
            CatalogEntityType::Artist,
            provider,
            "name",
            json!(artist),
            confidence,
        );
        push_provenance(
            bundle,
            CatalogEntityType::Album,
            provider,
            "artist_name",
            json!(artist),
            confidence,
        );
    }
    if let Some(year) = album.year_released.as_deref().and_then(parse_i32) {
        push_provenance(
            bundle,
            CatalogEntityType::Album,
            provider,
            "release_year",
            json!(year),
            confidence,
        );
    }
    if let Some(url) = album.album_thumb.as_deref().and_then(non_empty) {
        bundle.artwork.push(ArtworkAssetDraft {
            entity_type: CatalogEntityType::Album,
            provider,
            artwork_kind: ArtworkKind::Cover,
            source_uri: Some(url.to_string()),
            file_path: None,
            mime_type: mime_type_for_uri(url).map(str::to_string),
            width: None,
            height: None,
            confidence: 0.78,
        });
    }
}

/// Handles add the audio db artist for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `bundle`: `&mut ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `artist`: `TheAudioDbArtist`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn add_the_audio_db_artist(
    bundle: &mut ProviderMetadataBundle,
    provider: ProviderKind,
    artist: TheAudioDbArtist,
) {
    let confidence = 0.78;
    if let Some(artist_id) = non_empty_owned(artist.id_artist.as_deref()) {
        bundle.provider_links.push(external_link_with_raw(
            CatalogEntityType::Artist,
            provider,
            &artist_id,
            Some(format!("https://www.theaudiodb.com/artist/{artist_id}")),
            match_kind_for_confidence(confidence),
            confidence,
            json_value(&artist),
        ));
    }
    if let Some(name) = non_empty_owned(artist.artist.as_deref()) {
        push_provenance(
            bundle,
            CatalogEntityType::Artist,
            provider,
            "name",
            json!(name),
            confidence,
        );
    }
    if let Some(url) = artist.artist_thumb.as_deref().and_then(non_empty) {
        bundle.artwork.push(ArtworkAssetDraft {
            entity_type: CatalogEntityType::Artist,
            provider,
            artwork_kind: ArtworkKind::Artist,
            source_uri: Some(url.to_string()),
            file_path: None,
            mime_type: mime_type_for_uri(url).map(str::to_string),
            width: None,
            height: None,
            confidence: 0.76,
        });
    }
    if let Some(url) = artist
        .artist_fanart
        .as_deref()
        .or(artist.artist_fanart_2.as_deref())
        .and_then(non_empty)
    {
        bundle.artwork.push(ArtworkAssetDraft {
            entity_type: CatalogEntityType::Artist,
            provider,
            artwork_kind: ArtworkKind::Fanart,
            source_uri: Some(url.to_string()),
            file_path: None,
            mime_type: mime_type_for_uri(url).map(str::to_string),
            width: None,
            height: None,
            confidence: 0.74,
        });
    }
}

/// Handles add music provenance for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `bundle`: `&mut ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `grouping`: `&MusicCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
/// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
/// - `auto_accepted`: `bool`; expected to be a boolean flag controlling the documented branch.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn add_music_provenance(
    bundle: &mut ProviderMetadataBundle,
    provider: ProviderKind,
    grouping: &MusicCatalogGrouping,
    confidence: f32,
    auto_accepted: bool,
) {
    for (entity_type, field_name, value) in [
        (
            CatalogEntityType::Artist,
            "name",
            grouping.track_artist.as_str(),
        ),
        (
            CatalogEntityType::Album,
            "title",
            grouping.album_title.as_str(),
        ),
        (
            CatalogEntityType::Track,
            "title",
            grouping.track_title.as_str(),
        ),
    ] {
        bundle.provenance.push(MetadataProvenanceDraft {
            entity_type,
            field_name: field_name.to_string(),
            provider,
            value: json!(value),
            confidence,
            auto_accepted,
        });
    }
}

/// Handles add podcast provenance for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `bundle`: `&mut ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `grouping`: `&PodcastCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
/// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
/// - `auto_accepted`: `bool`; expected to be a boolean flag controlling the documented branch.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn add_podcast_provenance(
    bundle: &mut ProviderMetadataBundle,
    provider: ProviderKind,
    grouping: &PodcastCatalogGrouping,
    confidence: f32,
    auto_accepted: bool,
) {
    for (entity_type, field_name, value) in [
        (
            CatalogEntityType::Podcast,
            "title",
            grouping.podcast_title.as_str(),
        ),
        (
            CatalogEntityType::Episode,
            "title",
            grouping.episode_title.as_str(),
        ),
    ] {
        bundle.provenance.push(MetadataProvenanceDraft {
            entity_type,
            field_name: field_name.to_string(),
            provider,
            value: json!(value),
            confidence,
            auto_accepted,
        });
    }
}

/// Handles push provenance for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `bundle`: `&mut ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
/// - `entity_type`: `CatalogEntityType`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `field_name`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `value`: `Value`; expected to be a value satisfying the type contract shown in the function signature.
/// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors.
fn push_provenance(
    bundle: &mut ProviderMetadataBundle,
    entity_type: CatalogEntityType,
    provider: ProviderKind,
    field_name: &str,
    value: Value,
    confidence: f32,
) {
    if value.as_str().map(str::trim).is_some_and(str::is_empty) {
        return;
    }
    bundle.provenance.push(MetadataProvenanceDraft {
        entity_type,
        field_name: field_name.to_string(),
        provider,
        value,
        confidence,
        auto_accepted: auto_accepted(confidence),
    });
}

/// Handles local link for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `entity_type`: `CatalogEntityType`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `fingerprint`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `MetadataProviderLinkDraft` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn local_link(
    entity_type: CatalogEntityType,
    provider: ProviderKind,
    fingerprint: &str,
    confidence: f32,
) -> MetadataProviderLinkDraft {
    external_link(
        entity_type,
        provider,
        &format!("local:{}", normalize_catalog_text(fingerprint).replace(' ', ":")),
        None,
        MetadataMatchKind::LocalOnly,
        confidence,
        true,
    )
}

/// Handles external link for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `entity_type`: `CatalogEntityType`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `provider_item_id`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `external_url`: `Option<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `match_kind`: `MetadataMatchKind`; expected to be a value satisfying the type contract shown in the function signature.
/// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
/// - `auto_accepted`: `bool`; expected to be a boolean flag controlling the documented branch.
///
/// Output:
/// - Returns `MetadataProviderLinkDraft` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn external_link(
    entity_type: CatalogEntityType,
    provider: ProviderKind,
    provider_item_id: &str,
    external_url: Option<String>,
    match_kind: MetadataMatchKind,
    confidence: f32,
    auto_accepted: bool,
) -> MetadataProviderLinkDraft {
    MetadataProviderLinkDraft {
        entity_type,
        provider,
        provider_item_id: provider_item_id.to_string(),
        external_url,
        match_kind,
        confidence,
        auto_accepted,
        raw_metadata: json!({
            "provider": provider.api_name(),
            "match_kind": match_kind.api_name(),
            "confidence": confidence,
            "auto_accepted": auto_accepted
        }),
    }
}

/// Handles external link with raw for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `entity_type`: `CatalogEntityType`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `provider_item_id`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `external_url`: `Option<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
/// - `match_kind`: `MetadataMatchKind`; expected to be a value satisfying the type contract shown in the function signature.
/// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
/// - `raw_metadata`: `Value`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `MetadataProviderLinkDraft` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn external_link_with_raw(
    entity_type: CatalogEntityType,
    provider: ProviderKind,
    provider_item_id: &str,
    external_url: Option<String>,
    match_kind: MetadataMatchKind,
    confidence: f32,
    raw_metadata: Value,
) -> MetadataProviderLinkDraft {
    MetadataProviderLinkDraft {
        entity_type,
        provider,
        provider_item_id: provider_item_id.to_string(),
        external_url,
        match_kind,
        confidence,
        auto_accepted: auto_accepted(confidence),
        raw_metadata: json!({
            "provider": provider.api_name(),
            "match_kind": match_kind.api_name(),
            "confidence": confidence,
            "auto_accepted": auto_accepted(confidence),
            "metadata": raw_metadata
        }),
    }
}

/// Handles stable music grouping for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `grouping`: `&MusicCatalogGrouping`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn stable_music_grouping(grouping: &MusicCatalogGrouping) -> bool {
    !grouping.album_artist.trim().is_empty()
        && !grouping.album_title.trim().is_empty()
        && !grouping.track_artist.trim().is_empty()
        && !grouping.track_title.trim().is_empty()
}

/// Handles confidence from score for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `score`: `Option<u32>`; expected to be an optional value; `None` asks the function to use its default or omit that filter.
/// - `floor`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
/// - `ceiling`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `f32` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn confidence_from_score(score: Option<u32>, floor: f32, ceiling: f32) -> f32 {
    let score = score.unwrap_or(80).min(100) as f32 / 100.0;
    (floor + ((ceiling - floor) * score)).clamp(0.0, 1.0)
}

/// Handles match kind for confidence for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `MetadataMatchKind` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn match_kind_for_confidence(confidence: f32) -> MetadataMatchKind {
    if confidence >= 0.85 {
        MetadataMatchKind::HighConfidence
    } else {
        MetadataMatchKind::ModerateConfidence
    }
}

/// Handles auto accepted for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `confidence`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn auto_accepted(confidence: f32) -> bool {
    confidence >= PROVIDER_AUTO_ACCEPT_CONFIDENCE_THRESHOLD
}

/// Handles has provider link for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `links`: `&[MetadataProviderLinkDraft]`; expected to be a value satisfying the type contract shown in the function signature.
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `entity_type`: `CatalogEntityType`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn has_provider_link(
    links: &[MetadataProviderLinkDraft],
    provider: ProviderKind,
    entity_type: CatalogEntityType,
) -> bool {
    links
        .iter()
        .any(|link| link.provider == provider && link.entity_type == entity_type)
}

/// Handles musicbrainz release id for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `context`: `ProviderEnrichmentContext<'_>`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Some(String)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn musicbrainz_release_id(context: ProviderEnrichmentContext<'_>) -> Option<String> {
    context
        .media
        .tags
        .get(&["musicbrainz_albumid", "musicbrainz_releaseid"])
        .and_then(non_empty)
        .map(str::to_string)
        .or_else(|| {
            context
                .prior_links
                .iter()
                .find(|link| {
                    link.provider == ProviderKind::MusicBrainz
                        && link.entity_type == CatalogEntityType::Album
                        && !link.provider_item_id.starts_with("candidate:")
                })
                .map(|link| link.provider_item_id.clone())
        })
}

/// Handles musicbrainz artist id for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `context`: `ProviderEnrichmentContext<'_>`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Some(String)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn musicbrainz_artist_id(context: ProviderEnrichmentContext<'_>) -> Option<String> {
    context
        .media
        .tags
        .get(&[
            "musicbrainz_albumartistid",
            "musicbrainz_artistid",
            "musicbrainzartistid",
        ])
        .and_then(non_empty)
        .map(str::to_string)
        .or_else(|| {
            context
                .prior_links
                .iter()
                .find(|link| {
                    link.provider == ProviderKind::MusicBrainz
                        && link.entity_type == CatalogEntityType::Artist
                        && !link.provider_item_id.starts_with("candidate:")
                })
                .map(|link| link.provider_item_id.clone())
        })
}

/// Handles image url for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `image`: `&CoverArtArchiveImage`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Some(String)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn image_url(image: &CoverArtArchiveImage) -> Option<String> {
    image
        .thumbnails
        .as_ref()
        .and_then(|thumbnails| thumbnails.get("large").or_else(|| thumbnails.get("small")))
        .and_then(|url| non_empty(url))
        .or_else(|| image.image.as_deref().and_then(non_empty))
        .map(str::to_string)
}

/// Handles is front image for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `image`: `&CoverArtArchiveImage`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors.
fn is_front_image(image: &CoverArtArchiveImage) -> bool {
    image.front.unwrap_or(false)
        || image
            .types
            .iter()
            .any(|image_type| image_type.eq_ignore_ascii_case("front"))
}

/// Handles fanart image url for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `value`: `&'a Value`; expected to be a value satisfying the type contract shown in the function signature.
/// - `key`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(&'a str)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn fanart_image_url<'a>(value: &'a Value, key: &str) -> Option<&'a str> {
    value
        .get(key)
        .and_then(Value::as_array)
        .and_then(|images| images.first())
        .and_then(|image| image.get("url"))
        .and_then(Value::as_str)
        .and_then(non_empty)
}

/// Handles split discogs title for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `title`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `(Option<String>, String)` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn split_discogs_title(title: &str) -> (Option<String>, String) {
    if let Some((artist, album)) = title.split_once(" - ") {
        (
            non_empty(artist).map(str::to_string),
            non_empty(album).unwrap_or(title).to_string(),
        )
    } else {
        (None, title.to_string())
    }
}

/// Handles discogs uri for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `uri`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors.
fn discogs_uri(uri: &str) -> String {
    if uri.starts_with("http://") || uri.starts_with("https://") {
        uri.to_string()
    } else {
        format!("https://www.discogs.com{uri}")
    }
}

/// Handles non placeholder image url for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(&str)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn non_placeholder_image_url(value: &str) -> Option<&str> {
    let value = non_empty(value)?;
    if value.ends_with("/spacer.gif") {
        None
    } else {
        Some(value)
    }
}

/// Handles mime type for uri for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `uri`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(&'static str)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn mime_type_for_uri(uri: &str) -> Option<&'static str> {
    let path = uri.split('?').next().unwrap_or(uri).to_ascii_lowercase();
    if path.ends_with(".jpg") || path.ends_with(".jpeg") {
        Some("image/jpeg")
    } else if path.ends_with(".png") {
        Some("image/png")
    } else if path.ends_with(".webp") {
        Some("image/webp")
    } else if path.ends_with(".gif") {
        Some("image/gif")
    } else {
        None
    }
}

/// Handles release year from date for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `value`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(i32)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn release_year_from_date(value: Option<&str>) -> Option<i32> {
    value
        .and_then(|value| value.split('-').next())
        .and_then(parse_i32)
}

fn deserialize_optional_i32<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    Ok(value.and_then(|value| match value {
        Value::Number(number) => number.as_i64().and_then(|value| i32::try_from(value).ok()),
        Value::String(value) => parse_i32(&value),
        _ => None,
    }))
}

/// Parses and validates input for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(i32)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn parse_i32(value: &str) -> Option<i32> {
    value.trim().parse::<i32>().ok()
}

/// Handles non empty for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(&str)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors.
fn non_empty(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

/// Handles non empty owned for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `value`: `Option<&str>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(String)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn non_empty_owned(value: Option<&str>) -> Option<String> {
    value.and_then(non_empty).map(str::to_string)
}

/// Normalizes caller-provided data for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `value`: `Option<String>`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `Some(String)` when a value is available; otherwise returns `None`.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn normalize_optional_secret(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

/// Handles json value for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `value`: `&T`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `Value` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn json_value<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or_else(|_| Value::Null)
}

/// Handles lucene quoted for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `value`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
///
/// Output:
/// - Returns `String` as produced by the operation.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn lucene_quoted(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

/// Handles log provider error for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `error`: `&ProviderHttpError`; expected to be a value satisfying the type contract shown in the function signature.
///
/// Output:
/// - Returns `()` after completing the side effects described above.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
fn log_provider_error(provider: ProviderKind, error: &ProviderHttpError) {
    if error.is_not_found() {
        debug!(
            provider = provider.api_name(),
            error = %error,
            "metadata provider did not find a matching result"
        );
    } else {
        warn!(
            provider = provider.api_name(),
            error = %error,
            "metadata provider request failed"
        );
    }
}

/// Handles provider supports media kind for metadata provider registry and provider adapters used by the import pipeline.
///
/// Inputs:
/// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
/// - `media_kind`: `MediaKind`; expected to be a media domain value that has already passed upstream validation.
///
/// Output:
/// - Returns `true` when the documented condition is satisfied; otherwise returns `false`.
///
/// Errors:
/// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
pub fn provider_supports_media_kind(provider: ProviderKind, media_kind: MediaKind) -> bool {
    !matches!(
        (provider, media_kind),
        (ProviderKind::CoverArtArchive, MediaKind::Podcast)
            | (ProviderKind::Discogs, MediaKind::Podcast)
            | (ProviderKind::FanartTv, MediaKind::Podcast)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::{AlbumKind, MediaProbeFacts},
        media::LocalMediaTags,
    };
    use async_trait::async_trait;
    use std::sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    };

    /// Handles assert approx eq for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `actual`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
    /// - `expected`: `f32`; expected to be a numeric value within the range accepted by the target domain or database column.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn assert_approx_eq(actual: f32, expected: f32) {
        assert!(
            (actual - expected).abs() < f32::EPSILON,
            "expected {actual} to equal {expected}"
        );
    }

    /// Handles provider link for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `bundle`: `&'a ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    /// - `entity_type`: `CatalogEntityType`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `provider_item_id`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `&'a MetadataProviderLinkDraft` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn provider_link<'a>(
        bundle: &'a ProviderMetadataBundle,
        provider: ProviderKind,
        entity_type: CatalogEntityType,
        provider_item_id: &str,
    ) -> &'a MetadataProviderLinkDraft {
        bundle
            .provider_links
            .iter()
            .find(|link| {
                link.provider == provider
                    && link.entity_type == entity_type
                    && link.provider_item_id == provider_item_id
            })
            .expect("expected provider link")
    }

    /// Handles provenance for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `bundle`: `&'a ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    /// - `entity_type`: `CatalogEntityType`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `field_name`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    /// - `value`: `serde_json:Value`; expected to be a value satisfying the type contract shown in the function signature.
    ///
    /// Output:
    /// - Returns `&'a MetadataProvenanceDraft` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn provenance<'a>(
        bundle: &'a ProviderMetadataBundle,
        provider: ProviderKind,
        entity_type: CatalogEntityType,
        field_name: &str,
        value: serde_json::Value,
    ) -> &'a MetadataProvenanceDraft {
        bundle
            .provenance
            .iter()
            .find(|entry| {
                entry.provider == provider
                    && entry.entity_type == entity_type
                    && entry.field_name == field_name
                    && entry.value == value
            })
            .expect("expected provenance entry")
    }

    /// Handles artwork for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - `bundle`: `&'a ProviderMetadataBundle`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `provider`: `ProviderKind`; expected to be one of the supported metadata provider identifiers.
    /// - `entity_type`: `CatalogEntityType`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `artwork_kind`: `ArtworkKind`; expected to be a value satisfying the type contract shown in the function signature.
    /// - `source_uri`: `&str`; expected to be text input; empty strings, unsupported names, or malformed values are rejected where this function validates them.
    ///
    /// Output:
    /// - Returns `&'a ArtworkAssetDraft` borrowed or static text owned by the documented domain.
    ///
    /// Errors:
    /// - Does not return recoverable errors. May panic if an internal invariant documented by the implementation is violated, such as a poisoned lock or intentionally failing test setup.
    fn artwork<'a>(
        bundle: &'a ProviderMetadataBundle,
        provider: ProviderKind,
        entity_type: CatalogEntityType,
        artwork_kind: ArtworkKind,
        source_uri: &str,
    ) -> &'a ArtworkAssetDraft {
        bundle
            .artwork
            .iter()
            .find(|artwork| {
                artwork.provider == provider
                    && artwork.entity_type == entity_type
                    && artwork.artwork_kind == artwork_kind
                    && artwork.source_uri.as_deref() == Some(source_uri)
            })
            .expect("expected artwork draft")
    }

    /// Handles registry test grouping for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `CatalogGrouping` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn registry_test_grouping() -> CatalogGrouping {
        CatalogGrouping::Music(MusicCatalogGrouping {
            album_artist: "Retry Artist".to_string(),
            track_artist: "Retry Artist".to_string(),
            album_title: "Retry Album".to_string(),
            track_title: "Retry Track".to_string(),
            album_kind: AlbumKind::Album,
            release_year: None,
            disc_number: None,
            track_number: None,
        })
    }

    /// Handles registry test media for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `ProbedMediaFile` as produced by the operation.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn registry_test_media() -> ProbedMediaFile {
        ProbedMediaFile {
            source_path: "/library/Retry Artist/Retry Album/Retry Track.mp3".into(),
            facts: MediaProbeFacts {
                file_hash: "retry-hash".to_string(),
                file_size: 10,
                mime_type: Some("audio/mpeg".to_string()),
                container: Some("mp3".to_string()),
                audio_codec: None,
                duration_seconds: Some(120),
                bitrate: None,
                sample_rate: None,
                channels: None,
            },
            tags: LocalMediaTags::default(),
            sidecar_paths: Vec::new(),
            folder_images: Vec::new(),
        }
    }

    /// Represents flaky provider in the metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Functionality: Carries fields `calls`, `failures_before_success` for metadata provider registry and provider adapters used by the import pipeline.
    /// Dependencies: depends on `Arc<AtomicU32>`, `u32` and any derives or trait bounds declared on the type.
    /// Used by: referenced from `src/providers.rs`.
    struct FlakyProvider {
        calls: Arc<AtomicU32>,
        failures_before_success: u32,
    }

    #[async_trait]
    impl MetadataProvider for FlakyProvider {
        /// Handles kind for metadata provider registry and provider adapters used by the import pipeline.
        ///
        /// Inputs:
        /// - the current instance; expected to have been initialized with its documented invariants.
        ///
        /// Output:
        /// - Returns `ProviderKind` as produced by the operation.
        ///
        /// Errors:
        /// - Does not return recoverable errors.
        fn kind(&self) -> ProviderKind {
            ProviderKind::MusicBrainz
        }

        /// Handles enrich for metadata provider registry and provider adapters used by the import pipeline.
        ///
        /// Inputs:
        /// - the current instance; expected to have been initialized with its documented invariants.
        /// - `_context`: `ProviderEnrichmentContext<'_>`; expected to be a value satisfying the type contract shown in the function signature.
        ///
        /// Output:
        /// - Returns `ProviderEnrichmentResult` as produced by the operation.
        ///
        /// Errors:
        /// - Does not return recoverable errors.
        async fn enrich(
            &self,
            _context: ProviderEnrichmentContext<'_>,
        ) -> ProviderEnrichmentResult {
            let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let mut result = ProviderEnrichmentResult::new(self.kind());
            result.outcome.attempted = true;
            if call <= self.failures_before_success {
                result
                    .outcome
                    .failures
                    .push("synthetic provider failure".to_string());
            } else {
                result.outcome.successful_requests = 1;
            }
            result
        }
    }

    #[tokio::test]
    /// Handles provider registry retries transient failures until success for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns a future that resolves to `()` after the operation completes.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    async fn provider_registry_retries_transient_failures_until_success() {
        let calls = Arc::new(AtomicU32::new(0));
        let registry = ProviderRegistry {
            providers: vec![Box::new(FlakyProvider {
                calls: calls.clone(),
                failures_before_success: 2,
            })],
            provider_kinds: vec![ProviderKind::MusicBrainz],
        };

        let report = registry
            .enrich(&registry_test_grouping(), &registry_test_media())
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(report.outcomes[0].attempts, 3);
        assert!(!report.outcomes[0].has_failures());
        assert_eq!(report.outcomes[0].successful_requests, 1);
    }

    #[tokio::test]
    /// Handles provider registry stops after bounded retry failures for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns a future that resolves to `()` after the operation completes.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    async fn provider_registry_stops_after_bounded_retry_failures() {
        let calls = Arc::new(AtomicU32::new(0));
        let registry = ProviderRegistry {
            providers: vec![Box::new(FlakyProvider {
                calls: calls.clone(),
                failures_before_success: 10,
            })],
            provider_kinds: vec![ProviderKind::MusicBrainz],
        };

        let report = registry
            .enrich(&registry_test_grouping(), &registry_test_media())
            .await;

        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(report.outcomes[0].attempts, 3);
        assert!(report.outcomes[0].has_failures());
    }

    #[test]
    /// Handles parses musicbrainz recording response for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn parses_musicbrainz_recording_response() {
        let mut bundle = ProviderMetadataBundle::default();
        let recording_id = "fcbcdc39-8851-4efc-a02a-ab0e13be224f";
        let artist_id = "8ca01f46-53ac-4af2-8516-55a909c0905e";

        add_musicbrainz_recording(
            &mut bundle,
            ProviderKind::MusicBrainz,
            MusicBrainzRecording {
                id: Some(recording_id.to_string()),
                title: Some("Teardrop".to_string()),
                score: Some(80),
                artist_credit: vec![MusicBrainzArtistCredit {
                    name: Some("Massive Attack".to_string()),
                    artist: Some(MusicBrainzArtist {
                        id: Some(artist_id.to_string()),
                        name: Some("Massive Attack".to_string()),
                        sort_name: Some("Massive Attack".to_string()),
                    }),
                }],
                releases: vec![],
            },
        );

        let track_link = provider_link(
            &bundle,
            ProviderKind::MusicBrainz,
            CatalogEntityType::Track,
            recording_id,
        );
        assert_eq!(
            track_link.external_url.as_deref(),
            Some("https://musicbrainz.org/recording/fcbcdc39-8851-4efc-a02a-ab0e13be224f")
        );
        assert!(track_link.auto_accepted);
        assert!(track_link.confidence >= PROVIDER_AUTO_ACCEPT_CONFIDENCE_THRESHOLD);

        let artist_link = provider_link(
            &bundle,
            ProviderKind::MusicBrainz,
            CatalogEntityType::Artist,
            artist_id,
        );
        assert_eq!(
            artist_link.external_url.as_deref(),
            Some("https://musicbrainz.org/artist/8ca01f46-53ac-4af2-8516-55a909c0905e")
        );
        assert!(artist_link.auto_accepted);

        let title = provenance(
            &bundle,
            ProviderKind::MusicBrainz,
            CatalogEntityType::Track,
            "title",
            serde_json::json!("Teardrop"),
        );
        assert!(title.auto_accepted);

        let artist_name = provenance(
            &bundle,
            ProviderKind::MusicBrainz,
            CatalogEntityType::Artist,
            "name",
            serde_json::json!("Massive Attack"),
        );
        assert!(artist_name.auto_accepted);
    }

    #[test]
    /// Handles parses musicbrainz release response for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn parses_musicbrainz_release_response() {
        let mut bundle = ProviderMetadataBundle::default();
        let release_id = "a1d2f70d-24b2-4a07-bf2b-6d2f746b710c";

        add_musicbrainz_release(
            &mut bundle,
            ProviderKind::MusicBrainz,
            MusicBrainzRelease {
                id: Some(release_id.to_string()),
                title: Some("Mezzanine".to_string()),
                score: Some(150),
                date: Some("1998-04-20".to_string()),
                status: Some("Official".to_string()),
                artist_credit: vec![MusicBrainzArtistCredit {
                    name: Some("Massive Attack".to_string()),
                    artist: Some(MusicBrainzArtist {
                        id: Some("8ca01f46-53ac-4af2-8516-55a909c0905e".to_string()),
                        name: Some("Massive Attack".to_string()),
                        sort_name: Some("Massive Attack".to_string()),
                    }),
                }],
            },
        );

        let release_link = provider_link(
            &bundle,
            ProviderKind::MusicBrainz,
            CatalogEntityType::Album,
            release_id,
        );
        assert_eq!(
            release_link.external_url.as_deref(),
            Some("https://musicbrainz.org/release/a1d2f70d-24b2-4a07-bf2b-6d2f746b710c")
        );
        assert_approx_eq(release_link.confidence, 0.92);

        let title = provenance(
            &bundle,
            ProviderKind::MusicBrainz,
            CatalogEntityType::Album,
            "title",
            serde_json::json!("Mezzanine"),
        );
        assert_approx_eq(title.confidence, 0.92);

        let year = provenance(
            &bundle,
            ProviderKind::MusicBrainz,
            CatalogEntityType::Album,
            "release_year",
            serde_json::json!(1998),
        );
        assert_approx_eq(year.confidence, 0.92);
    }

    #[test]
    /// Handles parses cover art archive release response for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn parses_cover_art_archive_release_response() {
        let mut bundle = ProviderMetadataBundle::default();
        let release_id = "a1d2f70d-24b2-4a07-bf2b-6d2f746b710c";
        let mut thumbnails = BTreeMap::new();
        thumbnails.insert(
            "large".to_string(),
            "https://coverartarchive.org/release/a1d2f70d-24b2-4a07-bf2b-6d2f746b710c/123.jpg"
                .to_string(),
        );

        add_cover_art_archive_release(
            &mut bundle,
            ProviderKind::CoverArtArchive,
            release_id,
            CoverArtArchiveRelease {
                images: vec![CoverArtArchiveImage {
                    image: Some(
                        "https://coverartarchive.org/release/a1d2f70d-24b2-4a07-bf2b-6d2f746b710c/123-original.jpg"
                            .to_string(),
                    ),
                    front: Some(true),
                    approved: Some(true),
                    types: vec!["Front".to_string()],
                    thumbnails: Some(thumbnails),
                }],
            },
        );

        let link = provider_link(
            &bundle,
            ProviderKind::CoverArtArchive,
            CatalogEntityType::Album,
            release_id,
        );
        assert_eq!(
            link.external_url.as_deref(),
            Some("https://coverartarchive.org/release/a1d2f70d-24b2-4a07-bf2b-6d2f746b710c")
        );

        let cover = artwork(
            &bundle,
            ProviderKind::CoverArtArchive,
            CatalogEntityType::Album,
            ArtworkKind::Cover,
            "https://coverartarchive.org/release/a1d2f70d-24b2-4a07-bf2b-6d2f746b710c/123.jpg",
        );
        assert_eq!(cover.mime_type.as_deref(), Some("image/jpeg"));
    }

    #[test]
    /// Handles parses discogs release response for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn parses_discogs_release_response() {
        let mut bundle = ProviderMetadataBundle::default();

        add_discogs_release(
            &mut bundle,
            ProviderKind::Discogs,
            DiscogsSearchResult {
                id: Some(249504),
                title: Some("Massive Attack - Mezzanine".to_string()),
                uri: Some("/Massive-Attack-Mezzanine/release/249504".to_string()),
                resource_url: Some("https://api.discogs.com/releases/249504".to_string()),
                cover_image: Some("https://i.discogs.com/cover.jpg".to_string()),
                year: Some(1998),
                format: vec!["CD".to_string(), "Album".to_string()],
            },
        );

        let link = provider_link(
            &bundle,
            ProviderKind::Discogs,
            CatalogEntityType::Album,
            "249504",
        );
        assert_eq!(
            link.external_url.as_deref(),
            Some("https://www.discogs.com/Massive-Attack-Mezzanine/release/249504")
        );

        provenance(
            &bundle,
            ProviderKind::Discogs,
            CatalogEntityType::Artist,
            "name",
            serde_json::json!("Massive Attack"),
        );
        provenance(
            &bundle,
            ProviderKind::Discogs,
            CatalogEntityType::Album,
            "title",
            serde_json::json!("Mezzanine"),
        );
        provenance(
            &bundle,
            ProviderKind::Discogs,
            CatalogEntityType::Album,
            "release_year",
            serde_json::json!(1998),
        );
        artwork(
            &bundle,
            ProviderKind::Discogs,
            CatalogEntityType::Album,
            ArtworkKind::Cover,
            "https://i.discogs.com/cover.jpg",
        );
    }

    #[test]
    fn parses_discogs_search_year_from_string() {
        let response: DiscogsSearchResponse = serde_json::from_value(serde_json::json!({
            "results": [
                {
                    "id": 249504,
                    "title": "Massive Attack - Mezzanine",
                    "year": "2009"
                }
            ]
        }))
        .unwrap();

        assert_eq!(response.results[0].year, Some(2009));
    }

    #[test]
    /// Handles parses fanart artist images response for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn parses_fanart_artist_images_response() {
        let mut bundle = ProviderMetadataBundle::default();
        let artist_mbid = "8ca01f46-53ac-4af2-8516-55a909c0905e";

        add_fanart_artist_images(
            &mut bundle,
            ProviderKind::FanartTv,
            artist_mbid,
            serde_json::json!({
                "name": "Massive Attack",
                "mbid_id": artist_mbid,
                "artistthumb": [
                    { "id": "1", "url": "https://assets.fanart.tv/fanart/music/thumb.jpg" }
                ],
                "artistbackground": [
                    { "id": "2", "url": "https://assets.fanart.tv/fanart/music/background.png" }
                ]
            }),
        );

        let link = provider_link(
            &bundle,
            ProviderKind::FanartTv,
            CatalogEntityType::Artist,
            artist_mbid,
        );
        assert_eq!(
            link.external_url.as_deref(),
            Some("https://fanart.tv/artist/8ca01f46-53ac-4af2-8516-55a909c0905e")
        );

        provenance(
            &bundle,
            ProviderKind::FanartTv,
            CatalogEntityType::Artist,
            "name",
            serde_json::json!("Massive Attack"),
        );
        artwork(
            &bundle,
            ProviderKind::FanartTv,
            CatalogEntityType::Artist,
            ArtworkKind::Artist,
            "https://assets.fanart.tv/fanart/music/thumb.jpg",
        );
        artwork(
            &bundle,
            ProviderKind::FanartTv,
            CatalogEntityType::Artist,
            ArtworkKind::Fanart,
            "https://assets.fanart.tv/fanart/music/background.png",
        );
        assert_eq!(bundle.artwork.len(), 2);
    }

    #[test]
    /// Handles parses the audio db album response for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn parses_the_audio_db_album_response() {
        let mut bundle = ProviderMetadataBundle::default();

        add_the_audio_db_album(
            &mut bundle,
            ProviderKind::TheAudioDb,
            TheAudioDbAlbum {
                id_album: Some("2115888".to_string()),
                id_artist: Some("111239".to_string()),
                album: Some("Mezzanine".to_string()),
                artist: Some("Massive Attack".to_string()),
                year_released: Some("1998".to_string()),
                album_thumb: Some(
                    "https://www.theaudiodb.com/images/media/album/thumb.jpg".to_string(),
                ),
                album_cd_art: Some(
                    "https://www.theaudiodb.com/images/media/album/cdart.png".to_string(),
                ),
            },
        );

        let album_link = provider_link(
            &bundle,
            ProviderKind::TheAudioDb,
            CatalogEntityType::Album,
            "2115888",
        );
        assert_eq!(
            album_link.external_url.as_deref(),
            Some("https://www.theaudiodb.com/album/2115888")
        );

        let artist_link = provider_link(
            &bundle,
            ProviderKind::TheAudioDb,
            CatalogEntityType::Artist,
            "111239",
        );
        assert_eq!(
            artist_link.external_url.as_deref(),
            Some("https://www.theaudiodb.com/artist/111239")
        );

        provenance(
            &bundle,
            ProviderKind::TheAudioDb,
            CatalogEntityType::Album,
            "title",
            serde_json::json!("Mezzanine"),
        );
        provenance(
            &bundle,
            ProviderKind::TheAudioDb,
            CatalogEntityType::Artist,
            "name",
            serde_json::json!("Massive Attack"),
        );
        provenance(
            &bundle,
            ProviderKind::TheAudioDb,
            CatalogEntityType::Album,
            "release_year",
            serde_json::json!(1998),
        );
        artwork(
            &bundle,
            ProviderKind::TheAudioDb,
            CatalogEntityType::Album,
            ArtworkKind::Cover,
            "https://www.theaudiodb.com/images/media/album/thumb.jpg",
        );
    }

    #[test]
    /// Handles parses the audio db artist response for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn parses_the_audio_db_artist_response() {
        let mut bundle = ProviderMetadataBundle::default();

        add_the_audio_db_artist(
            &mut bundle,
            ProviderKind::TheAudioDb,
            TheAudioDbArtist {
                id_artist: Some("111239".to_string()),
                artist: Some("Massive Attack".to_string()),
                artist_thumb: Some(
                    "https://www.theaudiodb.com/images/media/artist/thumb.jpg".to_string(),
                ),
                artist_fanart: Some(
                    "https://www.theaudiodb.com/images/media/artist/fanart.jpg".to_string(),
                ),
                artist_fanart_2: None,
            },
        );

        let link = provider_link(
            &bundle,
            ProviderKind::TheAudioDb,
            CatalogEntityType::Artist,
            "111239",
        );
        assert_eq!(
            link.external_url.as_deref(),
            Some("https://www.theaudiodb.com/artist/111239")
        );

        provenance(
            &bundle,
            ProviderKind::TheAudioDb,
            CatalogEntityType::Artist,
            "name",
            serde_json::json!("Massive Attack"),
        );
        artwork(
            &bundle,
            ProviderKind::TheAudioDb,
            CatalogEntityType::Artist,
            ArtworkKind::Artist,
            "https://www.theaudiodb.com/images/media/artist/thumb.jpg",
        );
        artwork(
            &bundle,
            ProviderKind::TheAudioDb,
            CatalogEntityType::Artist,
            ArtworkKind::Fanart,
            "https://www.theaudiodb.com/images/media/artist/fanart.jpg",
        );
        assert_eq!(bundle.artwork.len(), 2);
    }

    #[test]
    /// Handles applies confidence and auto accept thresholds for metadata provider registry and provider adapters used by the import pipeline.
    ///
    /// Inputs:
    /// - None.
    ///
    /// Output:
    /// - Returns `()` after completing the side effects described above.
    ///
    /// Errors:
    /// - Does not return recoverable errors.
    fn applies_confidence_and_auto_accept_thresholds() {
        assert!(!auto_accepted(0.65));
        assert!(auto_accepted(0.66));
        assert_approx_eq(confidence_from_score(Some(100), 0.7, 0.95), 0.95);
        assert_approx_eq(confidence_from_score(Some(0), 0.7, 0.95), 0.7);
    }
}
