use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

use thiserror::Error;
use tokio::{
    net::UdpSocket,
    sync::{Mutex as AsyncMutex, OwnedMutexGuard},
    time::sleep,
};
use uuid::Uuid;

use crate::{
    api::{
        media::resolve_original_file,
        sonos::{
            SonosGroupTarget, SonosNextItemSummary, SonosPlaybackResponse,
            SonosPlaybackTarget, SonosPlayRequest, SonosSeekRequest,
            SonosSessionSummary, SonosSpeakerTarget, SonosTargetsResponse,
        },
    },
    domain::{
        AuthenticatedAccount, MediaFile, PlaybackContextType, PlaybackItemType,
        SonosDeliveryKind, SonosSessionStatus, SonosSignedClaim, SonosTransportState,
    },
    error::{ApiError, SonosErrorReason},
    state::{
        sonos_aac_profile_for_delivery, sonos_delivery_kind_for_media_file, AppState,
        SonosMediaAuthorizationContext, SonosSignedMediaIssueError,
    },
    transcode::TranscodeSlot,
};

const SSDP_DISCOVERY_ADDR: &str = "239.255.255.250:1900";
const ZONE_PLAYER_ST: &str = "urn:schemas-upnp-org:device:ZonePlayer:1";
const GROUP_RENDERING_CONTROL_PATH: &str = "/MediaRenderer/GroupRenderingControl/Control";
const GROUP_RENDERING_CONTROL_SERVICE: &str =
    "urn:schemas-upnp-org:service:GroupRenderingControl:1";
const AV_TRANSPORT_CONTROL_PATH: &str = "/MediaRenderer/AVTransport/Control";
const AV_TRANSPORT_SERVICE: &str = "urn:schemas-upnp-org:service:AVTransport:1";
const EMPTY_DISCOVERY_REFRESHES_BEFORE_EXPIRY: usize = 3;
const ACTIVE_SESSION_INTERVAL: Duration = Duration::from_secs(2);
const RECONNECT_WINDOW: Duration = Duration::from_secs(15);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Clone)]
pub struct SonosRuntimeConfig {
    pub poll_interval: Duration,
    pub error_backoff: Duration,
    pub discovery_timeout: Duration,
    pub request_timeout: Duration,
}

impl Default for SonosRuntimeConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(30),
            error_backoff: Duration::from_secs(10),
            discovery_timeout: Duration::from_secs(2),
            request_timeout: Duration::from_secs(3),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SonosTargetKind {
    Speaker,
    Group,
}

#[derive(Debug, Clone)]
pub struct SonosResolvedTarget {
    pub id: String,
    pub kind: SonosTargetKind,
    pub public_target: SonosPlaybackTarget,
    control_location: Option<String>,
    coordinator_location: Option<String>,
    grouped_coordinator_id: Option<String>,
}

#[derive(Debug, Clone)]
struct SonosControlTargetSnapshot {
    kind: SonosTargetKind,
    public_target: SonosPlaybackTarget,
    control_location: Option<String>,
    coordinator_location: Option<String>,
    grouped_coordinator_id: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SonosSnapshot {
    speakers: BTreeMap<String, SonosSpeakerSnapshot>,
    groups: BTreeMap<String, SonosGroupSnapshot>,
    controls: BTreeMap<String, SonosControlTargetSnapshot>,
}

impl SonosSnapshot {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_targets(
        speakers: Vec<SonosSpeakerSnapshot>,
        groups: Vec<SonosGroupSnapshot>,
    ) -> Self {
        Self::from_targets_with_control_locations(speakers, groups, BTreeMap::new())
    }

    pub fn from_targets_with_control_locations(
        speakers: Vec<SonosSpeakerSnapshot>,
        groups: Vec<SonosGroupSnapshot>,
        control_locations: BTreeMap<String, String>,
    ) -> Self {
        let speakers: BTreeMap<_, _> = speakers
            .into_iter()
            .map(|speaker| (speaker.id.clone(), speaker))
            .collect();
        let groups: BTreeMap<_, _> = groups
            .into_iter()
            .map(|group| (group.id.clone(), group))
            .collect();
        let controls = control_targets_from_snapshots(
            &speakers,
            &groups,
            &control_locations,
            &control_locations,
            &[],
        );

        Self {
            speakers,
            groups,
            controls,
        }
    }

    pub fn to_targets_response(&self) -> SonosTargetsResponse {
        SonosTargetsResponse {
            speakers: self
                .speakers
                .values()
                .filter(|speaker| speaker.available)
                .map(SonosSpeakerSnapshot::to_target)
                .collect(),
            groups: self
                .groups
                .values()
                .filter(|group| group.available)
                .map(SonosGroupSnapshot::to_target)
                .collect(),
        }
    }

    fn is_empty(&self) -> bool {
        self.speakers.is_empty() && self.groups.is_empty()
    }

    pub fn target(&self, target_id: &str) -> Option<SonosResolvedTarget> {
        self.controls
            .get(target_id)
            .map(|target| SonosResolvedTarget {
                id: target_id.to_string(),
                kind: target.kind,
                public_target: target.public_target.clone(),
                control_location: target.control_location.clone(),
                coordinator_location: target.coordinator_location.clone(),
                grouped_coordinator_id: target.grouped_coordinator_id.clone(),
            })
    }
}

#[derive(Debug, Clone)]
pub struct SonosSpeakerSnapshot {
    pub id: String,
    pub display_name: String,
    pub room_name: Option<String>,
    pub available: bool,
    pub live: SonosLiveState,
}

impl SonosSpeakerSnapshot {
    pub fn to_target(&self) -> SonosSpeakerTarget {
        SonosSpeakerTarget {
            id: self.id.clone(),
            display_name: display_name(&self.display_name, self.room_name.as_deref(), &self.id),
            room_name: self.room_name.clone(),
            available: self.available,
            volume_percent: self.live.volume_percent,
            muted: self.live.muted,
            transport_state: self.live.transport_state(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SonosGroupSnapshot {
    pub id: String,
    pub display_name: String,
    pub available: bool,
    pub live: SonosLiveState,
}

impl SonosGroupSnapshot {
    pub fn to_target(&self) -> SonosGroupTarget {
        SonosGroupTarget {
            id: self.id.clone(),
            display_name: display_name(&self.display_name, None, &self.id),
            available: self.available,
            volume_percent: self.live.volume_percent,
            muted: self.live.muted,
            transport_state: self.live.transport_state(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SonosLiveState {
    pub volume_percent: Option<u8>,
    pub muted: Option<bool>,
    pub raw_transport_state: Option<String>,
}

impl SonosLiveState {
    pub fn unknown() -> Self {
        Self::default()
    }

    pub fn transport_state(&self) -> Option<SonosTransportState> {
        self.raw_transport_state
            .as_deref()
            .and_then(map_raw_transport_state)
    }
}

#[derive(Debug, Default)]
pub struct ManagedSonosSessions {
    sessions: RwLock<HashMap<String, ManagedSonosSession>>,
    transport_guards: AsyncMutex<HashMap<String, Arc<AsyncMutex<()>>>>,
}

impl ManagedSonosSessions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn target_context(
        &self,
        target_id: &str,
    ) -> Option<SonosMediaAuthorizationContext> {
        let sessions = self
            .sessions
            .read()
            .expect("managed Sonos sessions lock poisoned");
        sessions
            .get(target_id)
            .and_then(ManagedSonosSession::current_authorization_context)
    }

    fn store_prepared_media_url(
        &self,
        target_id: &str,
        context: &SonosMediaAuthorizationContext,
        media_url: String,
    ) -> bool {
        let mut sessions = self
            .sessions
            .write()
            .expect("managed Sonos sessions lock poisoned");
        let Some(session) = sessions.get_mut(target_id) else {
            return false;
        };
        let Some(prepared) = session.prepared_item.as_mut() else {
            return false;
        };
        if prepared.context != *context {
            return false;
        }
        prepared.media_url = Some(media_url);
        true
    }

    pub fn validate_claim(&self, claim: &SonosSignedClaim) -> bool {
        self.validate_claim_for_current_session(claim)
            .unwrap_or(false)
    }

    pub fn validate_claim_for_current_session(
        &self,
        claim: &SonosSignedClaim,
    ) -> Option<bool> {
        let sessions = self
            .sessions
            .read()
            .expect("managed Sonos sessions lock poisoned");
        sessions
            .get(&claim.target_id)
            .map(|session| session.matches_claim(claim))
    }

    pub fn take_reserved_transcode_slot(
        &self,
        claim: &SonosSignedClaim,
    ) -> Option<TranscodeSlot> {
        let mut sessions = self
            .sessions
            .write()
            .expect("managed Sonos sessions lock poisoned");
        let session = sessions.get_mut(&claim.target_id)?;
        if !session.matches_claim(claim) {
            return None;
        }
        session
            .prepared_item
            .as_mut()
            .and_then(|prepared| prepared.reserved_transcode_slot.take())
    }

    fn next_generation(&self, target_id: &str) -> u64 {
        let sessions = self
            .sessions
            .read()
            .expect("managed Sonos sessions lock poisoned");
        sessions
            .get(target_id)
            .map(|session| session.session_generation.saturating_add(1))
            .unwrap_or(1)
    }

    fn insert(&self, session: ManagedSonosSession) -> Option<ManagedSonosSession> {
        self.sessions
            .write()
            .expect("managed Sonos sessions lock poisoned")
            .insert(session.target_id.clone(), session)
    }

    fn remove(&self, target_id: &str) -> Option<ManagedSonosSession> {
        self.sessions
            .write()
            .expect("managed Sonos sessions lock poisoned")
            .remove(target_id)
    }

    fn remove_snapshot(
        &self,
        snapshot: &ManagedSonosSessionSnapshot,
    ) -> Option<ManagedSonosSession> {
        let mut sessions = self
            .sessions
            .write()
            .expect("managed Sonos sessions lock poisoned");
        let current = sessions.get(&snapshot.target_id)?;
        if !session_matches_snapshot(current, snapshot) {
            return None;
        }
        sessions.remove(&snapshot.target_id)
    }

    fn snapshot(&self, target_id: &str) -> Option<ManagedSonosSessionSnapshot> {
        let sessions = self
            .sessions
            .read()
            .expect("managed Sonos sessions lock poisoned");
        sessions.get(target_id).map(ManagedSonosSession::snapshot)
    }

    pub fn session_summary(&self, target_id: &str, now: Instant) -> Option<SonosSessionSummary> {
        let sessions = self
            .sessions
            .read()
            .expect("managed Sonos sessions lock poisoned");
        sessions.get(target_id).and_then(|session| session.summary(now))
    }

    fn update<F, R>(&self, target_id: &str, update: F) -> Option<R>
    where
        F: FnOnce(&mut ManagedSonosSession) -> R,
    {
        let mut sessions = self
            .sessions
            .write()
            .expect("managed Sonos sessions lock poisoned");
        sessions.get_mut(target_id).map(update)
    }

    fn update_snapshot<F, R>(
        &self,
        snapshot: &ManagedSonosSessionSnapshot,
        update: F,
    ) -> Option<R>
    where
        F: FnOnce(&mut ManagedSonosSession) -> R,
    {
        let mut sessions = self
            .sessions
            .write()
            .expect("managed Sonos sessions lock poisoned");
        let session = sessions.get_mut(&snapshot.target_id)?;
        if !session_matches_snapshot(session, snapshot) {
            return None;
        }
        Some(update(session))
    }

    fn matches_snapshot(&self, snapshot: &ManagedSonosSessionSnapshot) -> bool {
        let sessions = self
            .sessions
            .read()
            .expect("managed Sonos sessions lock poisoned");
        sessions
            .get(&snapshot.target_id)
            .is_some_and(|session| session_matches_snapshot(session, snapshot))
    }

    async fn acquire_transport_guard(&self, target_id: &str) -> OwnedMutexGuard<()> {
        let guard = {
            let mut guards = self.transport_guards.lock().await;
            guards
                .entry(target_id.to_string())
                .or_insert_with(|| Arc::new(AsyncMutex::new(())))
                .clone()
        };
        guard.lock_owned().await
    }

    fn target_context_for_snapshot(
        &self,
        snapshot: &ManagedSonosSessionSnapshot,
    ) -> Option<SonosMediaAuthorizationContext> {
        let sessions = self
            .sessions
            .read()
            .expect("managed Sonos sessions lock poisoned");
        let session = sessions.get(&snapshot.target_id)?;
        if !session_matches_snapshot(session, snapshot) {
            return None;
        }
        session.current_authorization_context()
    }

    fn all_snapshots(&self) -> Vec<ManagedSonosSessionSnapshot> {
        let sessions = self
            .sessions
            .read()
            .expect("managed Sonos sessions lock poisoned");
        sessions
            .values()
            .map(ManagedSonosSession::snapshot)
            .collect()
    }
}

fn session_matches_snapshot(
    session: &ManagedSonosSession,
    snapshot: &ManagedSonosSessionSnapshot,
) -> bool {
    session.session_id == snapshot.session_id
        && session.session_generation == snapshot.session_generation
}

#[derive(Debug)]
struct ManagedSonosSession {
    target_id: String,
    target_kind: SonosTargetKind,
    control_location: Option<String>,
    coordinator_location: Option<String>,
    grouped_coordinator_id: Option<String>,
    owner_account_id: Uuid,
    owner_username: String,
    context_type: Option<PlaybackContextType>,
    context_id: Option<Uuid>,
    session_id: Uuid,
    session_generation: u64,
    item_generation: u64,
    queue: Vec<SonosQueueEntry>,
    queue_index: usize,
    current_position_seconds: u32,
    current_duration_seconds: Option<u32>,
    status: SonosSessionStatus,
    position_advancing: bool,
    reconnect_deadline: Option<Instant>,
    transient_loss_observed_at: Option<Instant>,
    latest_target: SonosPlaybackTarget,
    prepared_item: Option<SonosPreparedItem>,
    last_progress_write: Option<Instant>,
    last_position_tick: Instant,
}

#[derive(Debug, Clone)]
struct ManagedSonosSessionSnapshot {
    target_id: String,
    target_kind: SonosTargetKind,
    latest_target: SonosPlaybackTarget,
    control_location: Option<String>,
    coordinator_location: Option<String>,
    grouped_coordinator_id: Option<String>,
    owner_account_id: Uuid,
    context_type: Option<PlaybackContextType>,
    context_id: Option<Uuid>,
    session_id: Uuid,
    session_generation: u64,
    item_generation: u64,
    queue: Vec<SonosQueueEntry>,
    queue_index: usize,
    current_position_seconds: u32,
    current_duration_seconds: Option<u32>,
    status: SonosSessionStatus,
    position_advancing: bool,
    reconnect_deadline: Option<Instant>,
    transient_loss_observed_at: Option<Instant>,
}

#[derive(Debug, Clone)]
struct SonosQueueEntry {
    item_type: PlaybackItemType,
    item_id: Uuid,
    duration_seconds: Option<u32>,
}

#[derive(Debug)]
struct SonosPreparedItem {
    context: SonosMediaAuthorizationContext,
    media_url: Option<String>,
    reserved_transcode_slot: Option<TranscodeSlot>,
}

#[derive(Debug)]
struct ManagedSonosSessionRollback {
    target_kind: SonosTargetKind,
    control_location: Option<String>,
    coordinator_location: Option<String>,
    grouped_coordinator_id: Option<String>,
    queue_index: usize,
    item_generation: u64,
    current_position_seconds: u32,
    current_duration_seconds: Option<u32>,
    status: SonosSessionStatus,
    position_advancing: bool,
    reconnect_deadline: Option<Instant>,
    transient_loss_observed_at: Option<Instant>,
    latest_target: SonosPlaybackTarget,
    prepared_item: Option<SonosPreparedItem>,
    last_progress_write: Option<Instant>,
    last_position_tick: Instant,
}

#[derive(Debug, Error)]
pub enum SonosOperationError {
    #[error(transparent)]
    Api(#[from] ApiError),
    #[error("Sonos operation failed: {0:?}")]
    Reason(SonosErrorReason),
}

impl SonosOperationError {
    pub fn reason(reason: SonosErrorReason) -> Self {
        Self::Reason(reason)
    }
}

impl ManagedSonosSession {
    fn snapshot(&self) -> ManagedSonosSessionSnapshot {
        ManagedSonosSessionSnapshot {
            target_id: self.target_id.clone(),
            target_kind: self.target_kind,
            latest_target: self.latest_target.clone(),
            control_location: self.control_location.clone(),
            coordinator_location: self.coordinator_location.clone(),
            grouped_coordinator_id: self.grouped_coordinator_id.clone(),
            owner_account_id: self.owner_account_id,
            context_type: self.context_type,
            context_id: self.context_id,
            session_id: self.session_id,
            session_generation: self.session_generation,
            item_generation: self.item_generation,
            queue: self.queue.clone(),
            queue_index: self.queue_index,
            current_position_seconds: self.current_position_seconds,
            current_duration_seconds: self.current_duration_seconds,
            status: self.status,
            position_advancing: self.position_advancing,
            reconnect_deadline: self.reconnect_deadline,
            transient_loss_observed_at: self.transient_loss_observed_at,
        }
    }

    fn current_entry(&self) -> Option<&SonosQueueEntry> {
        self.queue.get(self.queue_index)
    }

    fn current_authorization_context(&self) -> Option<SonosMediaAuthorizationContext> {
        self.prepared_item
            .as_ref()
            .map(|prepared| prepared.context.clone())
            .or_else(|| {
                self.current_entry().map(|entry| SonosMediaAuthorizationContext {
                    session_id: self.session_id,
                    session_generation: self.session_generation,
                    item_generation: self.item_generation,
                    target_id: self.target_id.clone(),
                    item_type: entry.item_type,
                    item_id: entry.item_id,
                    delivery_kind: SonosDeliveryKind::Original,
                })
            })
    }

    fn matches_claim(&self, claim: &SonosSignedClaim) -> bool {
        let Some(prepared) = self.prepared_item.as_ref() else {
            return false;
        };
        prepared.context.matches_claim(claim)
    }

    fn summary(&self, now: Instant) -> Option<SonosSessionSummary> {
        session_summary_from_parts(
            &self.owner_username,
            self.status,
            &self.queue,
            self.queue_index,
            self.current_position_seconds,
            self.current_duration_seconds,
            self.reconnect_deadline,
            now,
        )
    }

    fn cached_resolved_target(&self) -> Option<SonosResolvedTarget> {
        cached_resolved_target(
            &self.target_id,
            self.target_kind,
            self.latest_target.clone(),
            self.control_location.clone(),
            self.coordinator_location.clone(),
            self.grouped_coordinator_id.clone(),
        )
    }

    fn cache_control_target(&mut self, target: &SonosResolvedTarget) {
        self.target_kind = target.kind;
        self.control_location = target.control_location.clone();
        self.coordinator_location = target.coordinator_location.clone();
        self.grouped_coordinator_id = target.grouped_coordinator_id.clone();
    }
}

impl ManagedSonosSessionSnapshot {
    fn current_entry(&self) -> Option<&SonosQueueEntry> {
        self.queue.get(self.queue_index)
    }

    fn cached_resolved_target(&self) -> Option<SonosResolvedTarget> {
        cached_resolved_target(
            &self.target_id,
            self.target_kind,
            self.latest_target.clone(),
            self.control_location.clone(),
            self.coordinator_location.clone(),
            self.grouped_coordinator_id.clone(),
        )
    }
}

impl ManagedSonosSessionRollback {
    fn capture(session: &mut ManagedSonosSession) -> Self {
        Self {
            target_kind: session.target_kind,
            control_location: session.control_location.clone(),
            coordinator_location: session.coordinator_location.clone(),
            grouped_coordinator_id: session.grouped_coordinator_id.clone(),
            queue_index: session.queue_index,
            item_generation: session.item_generation,
            current_position_seconds: session.current_position_seconds,
            current_duration_seconds: session.current_duration_seconds,
            status: session.status,
            position_advancing: session.position_advancing,
            reconnect_deadline: session.reconnect_deadline,
            transient_loss_observed_at: session.transient_loss_observed_at,
            latest_target: session.latest_target.clone(),
            prepared_item: session.prepared_item.take(),
            last_progress_write: session.last_progress_write,
            last_position_tick: session.last_position_tick,
        }
    }

    fn restore(self, session: &mut ManagedSonosSession) {
        session.target_kind = self.target_kind;
        session.control_location = self.control_location;
        session.coordinator_location = self.coordinator_location;
        session.grouped_coordinator_id = self.grouped_coordinator_id;
        session.queue_index = self.queue_index;
        session.item_generation = self.item_generation;
        session.current_position_seconds = self.current_position_seconds;
        session.current_duration_seconds = self.current_duration_seconds;
        session.status = self.status;
        session.position_advancing = self.position_advancing;
        session.reconnect_deadline = self.reconnect_deadline;
        session.transient_loss_observed_at = self.transient_loss_observed_at;
        session.latest_target = self.latest_target;
        session.prepared_item = self.prepared_item;
        session.last_progress_write = self.last_progress_write;
        session.last_position_tick = self.last_position_tick;
    }
}

fn cached_resolved_target(
    target_id: &str,
    target_kind: SonosTargetKind,
    latest_target: SonosPlaybackTarget,
    control_location: Option<String>,
    coordinator_location: Option<String>,
    grouped_coordinator_id: Option<String>,
) -> Option<SonosResolvedTarget> {
    if control_location.is_none() && coordinator_location.is_none() {
        return None;
    }

    Some(SonosResolvedTarget {
        id: target_id.to_string(),
        kind: target_kind,
        public_target: latest_target,
        control_location,
        coordinator_location,
        grouped_coordinator_id,
    })
}

fn session_summary_from_parts(
    owner_username: &str,
    status: SonosSessionStatus,
    queue: &[SonosQueueEntry],
    queue_index: usize,
    current_position_seconds: u32,
    current_duration_seconds: Option<u32>,
    reconnect_deadline: Option<Instant>,
    now: Instant,
) -> Option<SonosSessionSummary> {
    let current = queue.get(queue_index)?;
    let next_item = queue
        .get(queue_index.saturating_add(1))
        .map(|entry| SonosNextItemSummary {
            item_type: entry.item_type,
            item_id: entry.item_id,
        });
    let reconnect_seconds_remaining = if status == SonosSessionStatus::Reconnecting {
        Some(reconnect_seconds_remaining(reconnect_deadline, now))
    } else {
        None
    };

    Some(SonosSessionSummary {
        status,
        owner_username: owner_username.to_string(),
        current_item_type: current.item_type,
        current_item_id: current.item_id,
        queue_index: queue_index as u32,
        queue_position: queue_index.saturating_add(1) as u32,
        queue_length: queue.len() as u32,
        current_position_seconds,
        current_duration_seconds,
        reconnect_seconds_remaining,
        next_item,
    })
}

fn reconnect_seconds_remaining(deadline: Option<Instant>, now: Instant) -> u32 {
    let Some(deadline) = deadline else {
        return 0;
    };
    let remaining = deadline.saturating_duration_since(now);
    ((remaining.as_millis() + 999) / 1000) as u32
}

pub async fn play_target(
    state: AppState,
    target_id: String,
    owner: AuthenticatedAccount,
    request: SonosPlayRequest,
) -> Result<SonosPlaybackResponse, SonosOperationError> {
    expire_reconnecting_target_if_overdue(&state, &target_id, Instant::now()).await;
    if let Some(snapshot) = state.sonos_managed_sessions().snapshot(&target_id) {
        if snapshot.status == SonosSessionStatus::Reconnecting {
            return Err(SonosOperationError::reason(SonosErrorReason::TargetReconnecting));
        }
    }

    let resolved_target = resolve_live_target(&state, &target_id)?;
    let playback_context = playback_context_for_sonos_request(&request);
    let queue = resolve_play_queue(&state, owner.id, request).await?;
    if queue.is_empty() {
        return Err(
            ApiError::BadRequest("Sonos play source resolved to an empty queue".into()).into(),
        );
    }

    let session_generation = state.sonos_managed_sessions().next_generation(&target_id);
    let item_generation = 1;
    let session_id = Uuid::new_v4();
    let prepared_item = prepare_current_item(
        &state,
        &target_id,
        session_id,
        session_generation,
        item_generation,
        &queue[0],
    )
    .await?;

    let client = control_client()?;
    let transport_guard = state
        .sonos_managed_sessions()
        .acquire_transport_guard(&target_id)
        .await;
    expire_reconnecting_target_if_overdue(&state, &target_id, Instant::now()).await;
    if let Some(snapshot) = state.sonos_managed_sessions().snapshot(&target_id) {
        if snapshot.status == SonosSessionStatus::Reconnecting {
            return Err(SonosOperationError::reason(SonosErrorReason::TargetReconnecting));
        }
    }
    ungroup_if_needed(&client, &resolved_target).await?;
    let now = Instant::now();
    let current_duration_seconds = queue[0].duration_seconds;
    let session = ManagedSonosSession {
        target_id: target_id.clone(),
        target_kind: resolved_target.kind,
        control_location: resolved_target.control_location.clone(),
        coordinator_location: resolved_target.coordinator_location.clone(),
        grouped_coordinator_id: resolved_target.grouped_coordinator_id.clone(),
        owner_account_id: owner.id,
        owner_username: owner.username,
        context_type: playback_context.map(|context| context.0),
        context_id: playback_context.map(|context| context.1),
        session_id,
        session_generation,
        item_generation,
        queue,
        queue_index: 0,
        current_position_seconds: 0,
        current_duration_seconds,
        status: SonosSessionStatus::Active,
        position_advancing: true,
        reconnect_deadline: None,
        transient_loss_observed_at: None,
        latest_target: resolved_target.public_target.clone(),
        prepared_item: Some(prepared_item),
        last_progress_write: Some(now),
        last_position_tick: now,
    };
    let response_session = session.summary(now);
    let mut replaced_session = state.sonos_managed_sessions().insert(session);
    let replaced_snapshot = replaced_session.as_mut().map(|session| {
        advance_position(session, now);
        session.snapshot()
    });
    let media_url = match mint_committed_current_item_url(&state, &target_id) {
        Ok(media_url) => media_url,
        Err(error) => {
            if let Some(session) = replaced_session {
                state.sonos_managed_sessions().insert(session);
            } else {
                state.sonos_managed_sessions().remove(&target_id);
            }
            return Err(error);
        }
    };
    if let Err(error) = load_and_start_current_item(&client, &resolved_target, &media_url).await {
        if let Some(session) = replaced_session {
            state.sonos_managed_sessions().insert(session);
        } else {
            state.sonos_managed_sessions().remove(&target_id);
        }
        return Err(error);
    }
    let latest_target = refresh_target_after_command(&client, resolved_target.clone()).await;
    state.sonos_managed_sessions().update(&target_id, |session| {
        session.latest_target = latest_target.clone();
        session.cache_control_target(&resolved_target);
    });
    drop(transport_guard);
    if let Some(snapshot) = replaced_snapshot {
        write_session_snapshot_attribution(&state, &snapshot, false, true).await;
    }
    write_session_attribution(&state, &target_id, false, true).await;

    Ok(SonosPlaybackResponse {
        target: latest_target,
        session: response_session,
    })
}

pub async fn pause_target(
    state: AppState,
    target_id: String,
) -> Result<SonosPlaybackResponse, SonosOperationError> {
    command_current_session(state, target_id, SonosControlCommand::Pause).await
}

pub async fn resume_target(
    state: AppState,
    target_id: String,
) -> Result<SonosPlaybackResponse, SonosOperationError> {
    command_current_session(state, target_id, SonosControlCommand::Resume).await
}

pub async fn seek_target(
    state: AppState,
    target_id: String,
    request: SonosSeekRequest,
) -> Result<SonosPlaybackResponse, SonosOperationError> {
    command_current_session(
        state,
        target_id,
        SonosControlCommand::Seek(request.position_seconds),
    )
    .await
}

pub async fn next_target(
    state: AppState,
    target_id: String,
) -> Result<SonosPlaybackResponse, SonosOperationError> {
    item_change_current_session(state, target_id, 1).await
}

pub async fn previous_target(
    state: AppState,
    target_id: String,
) -> Result<SonosPlaybackResponse, SonosOperationError> {
    item_change_current_session(state, target_id, -1).await
}

pub async fn stop_target(
    state: AppState,
    target_id: String,
) -> Result<SonosPlaybackResponse, SonosOperationError> {
    let transport_guard = state
        .sonos_managed_sessions()
        .acquire_transport_guard(&target_id)
        .await;
    if expire_reconnecting_target_if_overdue(&state, &target_id, Instant::now()).await {
        return Err(SonosOperationError::reason(SonosErrorReason::SessionNotManaged));
    }
    let Some(mut session) = state.sonos_managed_sessions().remove(&target_id) else {
        return Err(SonosOperationError::reason(SonosErrorReason::SessionNotManaged));
    };

    let target = match state
        .sonos_snapshot()
        .target(&target_id)
        .or_else(|| session.cached_resolved_target())
    {
        Some(resolved_target) => {
            if let Ok(client) = control_client() {
                let _ = send_av_transport_action(
                    &client,
                    &resolved_target,
                    "Stop",
                    "<InstanceID>0</InstanceID>",
                )
                .await;
                let target = refresh_target_after_command(&client, resolved_target).await;
                target
            } else {
                session.latest_target.clone()
            }
        }
        None => session.latest_target.clone(),
    };

    advance_position(&mut session, Instant::now());
    let stopped_snapshot = session.snapshot();
    drop(transport_guard);
    write_session_snapshot_attribution(&state, &stopped_snapshot, false, true).await;
    Ok(SonosPlaybackResponse {
        target,
        session: None,
    })
}

pub async fn active_session_loop(state: AppState, request_timeout: Duration) {
    loop {
        reconcile_active_sessions(&state, request_timeout).await;
        sleep(ACTIVE_SESSION_INTERVAL).await;
    }
}

pub async fn reconcile_active_sessions(state: &AppState, request_timeout: Duration) {
    let snapshots = state.sonos_managed_sessions().all_snapshots();
    if snapshots.is_empty() {
        return;
    }
    let now = Instant::now();
    let client = match reqwest::Client::builder().timeout(request_timeout).build() {
        Ok(client) => client,
        Err(error) => {
            tracing::debug!(%error, "failed to create Sonos active-session client");
            return;
        }
    };
    for session in snapshots {
        if expire_reconnecting_snapshot_if_overdue(state, &session, now).await {
            continue;
        }
        let target = state
            .sonos_snapshot()
            .target(&session.target_id)
            .or_else(|| session.cached_resolved_target());
        let live_target = match target {
            Some(target) => verify_active_target(&client, target).await,
            None => None,
        };
        match live_target {
            None => {
                mark_session_reconnecting(state, &session, now);
            }
            Some(target) if session.status == SonosSessionStatus::Reconnecting => {
                if let Err(error) = resume_reconnected_session(
                    state,
                    session.clone(),
                    target,
                    request_timeout,
                )
                .await
                {
                    tracing::debug!(
                        %error,
                        target_id = %session.target_id,
                        "Sonos reconnect resume failed"
                    );
                    mark_session_reconnecting(state, &session, now);
                }
            }
            Some(target) => {
                let position_advancing =
                    position_advancing_for_transport(target_transport_state(&target.public_target))
                        .unwrap_or(session.position_advancing);
                let updated =
                    state
                        .sonos_managed_sessions()
                        .update_snapshot(&session, |session| {
                            update_position_advancement(session, now, position_advancing);
                            session.cache_control_target(&target);
                            session.latest_target = target.public_target;
                            session.status = SonosSessionStatus::Active;
                            session.reconnect_deadline = None;
                            session.transient_loss_observed_at = None;
                        });
                if updated.is_some() {
                    maybe_write_heartbeat(state, &session, now).await;
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum SonosControlCommand {
    Pause,
    Resume,
    Seek(u32),
}

async fn command_current_session(
    state: AppState,
    target_id: String,
    command: SonosControlCommand,
) -> Result<SonosPlaybackResponse, SonosOperationError> {
    expire_reconnecting_target_if_overdue(&state, &target_id, Instant::now()).await;
    let snapshot = managed_session_or_error(&state, &target_id)?;
    if snapshot.status == SonosSessionStatus::Reconnecting {
        return Err(SonosOperationError::reason(SonosErrorReason::TargetReconnecting));
    }
    if let SonosControlCommand::Seek(position_seconds) = command {
        if let Some(duration_seconds) = snapshot.current_duration_seconds {
            if position_seconds > duration_seconds {
                return Err(ApiError::BadRequest(format!(
                    "seek position {position_seconds} exceeds current item duration {duration_seconds}"
                ))
                .into());
            }
        }
    }

    let resolved_target = resolve_control_target_or_reconnecting(&state, &target_id)?;
    let client = control_client()?;
    let transport_guard = state
        .sonos_managed_sessions()
        .acquire_transport_guard(&target_id)
        .await;
    if !state.sonos_managed_sessions().matches_snapshot(&snapshot) {
        return Err(SonosOperationError::reason(SonosErrorReason::SessionNotManaged));
    }
    match command {
        SonosControlCommand::Pause => {
            send_av_transport_action(
                &client,
                &resolved_target,
                "Pause",
                "<InstanceID>0</InstanceID>",
            )
            .await?;
        }
        SonosControlCommand::Resume => {
            send_av_transport_action(
                &client,
                &resolved_target,
                "Play",
                "<InstanceID>0</InstanceID><Speed>1</Speed>",
            )
            .await?;
        }
        SonosControlCommand::Seek(position_seconds) => {
            let target = format_duration(position_seconds);
            let body = format!(
                "<InstanceID>0</InstanceID><Unit>REL_TIME</Unit><Target>{target}</Target>"
            );
            send_av_transport_action(&client, &resolved_target, "Seek", &body).await?;
        }
    }

    let latest_target = refresh_target_after_command(&client, resolved_target.clone()).await;
    let now = Instant::now();
    let (session_summary, current_snapshot) = state
        .sonos_managed_sessions()
        .update_snapshot(&snapshot, |session| {
            match command {
                SonosControlCommand::Pause => {
                    advance_position(session, now);
                    session.position_advancing = false;
                }
                SonosControlCommand::Resume => {
                    advance_position(session, now);
                    session.position_advancing = true;
                }
                SonosControlCommand::Seek(position_seconds) => {
                    advance_position(session, now);
                    session.current_position_seconds = position_seconds;
                }
            }
            session.status = SonosSessionStatus::Active;
            session.reconnect_deadline = None;
            session.transient_loss_observed_at = None;
            session.latest_target = latest_target.clone();
            session.cache_control_target(&resolved_target);
            session.last_progress_write = Some(now);
            session.last_position_tick = now;
            (session.summary(now), session.snapshot())
        })
        .and_then(|(summary, snapshot)| summary.map(|summary| (summary, snapshot)))
        .ok_or_else(|| SonosOperationError::reason(SonosErrorReason::SessionNotManaged))?;
    drop(transport_guard);

    write_session_snapshot_attribution(&state, &current_snapshot, false, true).await;

    Ok(SonosPlaybackResponse {
        target: latest_target,
        session: Some(session_summary),
    })
}

async fn item_change_current_session(
    state: AppState,
    target_id: String,
    delta: isize,
) -> Result<SonosPlaybackResponse, SonosOperationError> {
    expire_reconnecting_target_if_overdue(&state, &target_id, Instant::now()).await;
    let snapshot = managed_session_or_error(&state, &target_id)?;
    if snapshot.status == SonosSessionStatus::Reconnecting {
        return Err(SonosOperationError::reason(SonosErrorReason::TargetReconnecting));
    }

    let new_index = if delta < 0 {
        snapshot
            .queue_index
            .checked_sub(delta.unsigned_abs())
            .ok_or_else(|| ApiError::BadRequest("already at the first Sonos queue item".into()))?
    } else {
        snapshot.queue_index.saturating_add(delta as usize)
    };
    if new_index >= snapshot.queue.len() {
        return Err(ApiError::BadRequest("already at the last Sonos queue item".into()).into());
    }

    let resolved_target = resolve_control_target_or_reconnecting(&state, &target_id)?;
    let item_generation = snapshot.item_generation.saturating_add(1);
    let prepared_item = prepare_current_item(
        &state,
        &target_id,
        snapshot.session_id,
        snapshot.session_generation,
        item_generation,
        &snapshot.queue[new_index],
    )
    .await?;

    let client = control_client()?;
    let outgoing_now = Instant::now();
    let outgoing_snapshot = state
        .sonos_managed_sessions()
        .update_snapshot(&snapshot, |session| {
            advance_position(session, outgoing_now);
            session.snapshot()
        })
        .ok_or_else(|| SonosOperationError::reason(SonosErrorReason::SessionNotManaged))?;

    let transition_now = Instant::now();
    let mut rollback = Some(state
        .sonos_managed_sessions()
        .update_snapshot(&snapshot, |session| {
            let rollback = ManagedSonosSessionRollback::capture(session);
            session.queue_index = new_index;
            session.item_generation = item_generation;
            session.current_position_seconds = 0;
            session.current_duration_seconds = session.queue[new_index].duration_seconds;
            session.status = SonosSessionStatus::Active;
            session.position_advancing = true;
            session.reconnect_deadline = None;
            session.transient_loss_observed_at = None;
            session.latest_target = resolved_target.public_target.clone();
            session.cache_control_target(&resolved_target);
            session.prepared_item = Some(prepared_item);
            session.last_progress_write = Some(transition_now);
            session.last_position_tick = transition_now;
            rollback
        })
        .ok_or_else(|| SonosOperationError::reason(SonosErrorReason::SessionNotManaged))?);
    let media_url = match mint_committed_current_item_url_for_snapshot(&state, &snapshot) {
        Ok(media_url) => media_url,
        Err(error) => {
            if let Some(rollback) = rollback.take() {
                state
                    .sonos_managed_sessions()
                    .update_snapshot(&snapshot, |session| {
                        rollback.restore(session);
                    });
            }
            return Err(error);
        }
    };
    let transport_guard = state
        .sonos_managed_sessions()
        .acquire_transport_guard(&target_id)
        .await;
    if !state.sonos_managed_sessions().matches_snapshot(&snapshot) {
        if let Some(rollback) = rollback.take() {
            state
                .sonos_managed_sessions()
                .update_snapshot(&snapshot, |session| {
                    rollback.restore(session);
                });
        }
        return Err(SonosOperationError::reason(SonosErrorReason::SessionNotManaged));
    }
    if let Err(error) = set_current_item_uri(&client, &resolved_target, &media_url).await {
        if let Some(rollback) = rollback.take() {
            state
                .sonos_managed_sessions()
                .update_snapshot(&snapshot, |session| {
                    rollback.restore(session);
                });
        }
        return Err(error);
    }
    if !state.sonos_managed_sessions().matches_snapshot(&snapshot) {
        if let Some(rollback) = rollback.take() {
            state
                .sonos_managed_sessions()
                .update_snapshot(&snapshot, |session| {
                    rollback.restore(session);
                });
        }
        return Err(SonosOperationError::reason(SonosErrorReason::SessionNotManaged));
    }
    if let Err(error) = start_current_item(&client, &resolved_target).await {
        if let Some(rollback) = rollback.take() {
            state
                .sonos_managed_sessions()
                .update_snapshot(&snapshot, |session| {
                    rollback.restore(session);
                });
        }
        return Err(error);
    }

    let latest_target = refresh_target_after_command(&client, resolved_target.clone()).await;
    let now = Instant::now();
    let (session_summary, current_snapshot) = state
        .sonos_managed_sessions()
        .update_snapshot(&snapshot, |session| {
            session.status = SonosSessionStatus::Active;
            session.reconnect_deadline = None;
            session.transient_loss_observed_at = None;
            session.latest_target = latest_target.clone();
            session.cache_control_target(&resolved_target);
            session.last_position_tick = now;
            (session.summary(now), session.snapshot())
        })
        .and_then(|(summary, snapshot)| summary.map(|summary| (summary, snapshot)))
        .ok_or_else(|| SonosOperationError::reason(SonosErrorReason::SessionNotManaged))?;
    drop(transport_guard);

    write_session_snapshot_attribution(&state, &outgoing_snapshot, false, true).await;
    write_session_snapshot_attribution(&state, &current_snapshot, false, true).await;

    Ok(SonosPlaybackResponse {
        target: latest_target,
        session: Some(session_summary),
    })
}

fn managed_session_or_error(
    state: &AppState,
    target_id: &str,
) -> Result<ManagedSonosSessionSnapshot, SonosOperationError> {
    state
        .sonos_managed_sessions()
        .snapshot(target_id)
        .ok_or_else(|| SonosOperationError::reason(SonosErrorReason::SessionNotManaged))
}

fn resolve_live_target(
    state: &AppState,
    target_id: &str,
) -> Result<SonosResolvedTarget, SonosOperationError> {
    state
        .sonos_snapshot()
        .target(target_id)
        .ok_or_else(|| SonosOperationError::reason(SonosErrorReason::TargetUnreachable))
}

fn resolve_control_target_or_reconnecting(
    state: &AppState,
    target_id: &str,
) -> Result<SonosResolvedTarget, SonosOperationError> {
    match state.sonos_snapshot().target(target_id) {
        Some(target) => Ok(target),
        None => {
            let Some(snapshot) = state.sonos_managed_sessions().snapshot(target_id) else {
                return Err(SonosOperationError::reason(SonosErrorReason::TargetReconnecting));
            };
            if let Some(target) = snapshot.cached_resolved_target() {
                return Ok(target);
            }
            mark_session_reconnecting(state, &snapshot, Instant::now());
            Err(SonosOperationError::reason(SonosErrorReason::TargetReconnecting))
        }
    }
}

async fn resolve_play_queue(
    state: &AppState,
    owner_account_id: Uuid,
    request: SonosPlayRequest,
) -> Result<Vec<SonosQueueEntry>, ApiError> {
    match request {
        SonosPlayRequest::Track { source_id } => {
            let media_file = state
                .visible_original_media_file(PlaybackItemType::Track, source_id)
                .await?;
            Ok(vec![queue_entry_from_media_file(
                PlaybackItemType::Track,
                source_id,
                &media_file,
            )])
        }
        SonosPlayRequest::Episode { source_id } => {
            let media_file = state
                .visible_original_media_file(PlaybackItemType::Episode, source_id)
                .await?;
            Ok(vec![queue_entry_from_media_file(
                PlaybackItemType::Episode,
                source_id,
                &media_file,
            )])
        }
        SonosPlayRequest::Playlist { source_id } => {
            let mut items = state
                .list_visible_playlist_items(owner_account_id, source_id)
                .await?;
            items.sort_by_key(|item| item.position);
            let mut queue = Vec::with_capacity(items.len());
            for item in items {
                let media_file = state
                    .visible_original_media_file(item.item_type, item.item_id)
                    .await?;
                queue.push(queue_entry_from_media_file(
                    item.item_type,
                    item.item_id,
                    &media_file,
                ));
            }
            Ok(queue)
        }
    }
}

fn playback_context_for_sonos_request(
    request: &SonosPlayRequest,
) -> Option<(PlaybackContextType, Uuid)> {
    match request {
        SonosPlayRequest::Playlist { source_id } => {
            Some((PlaybackContextType::Playlist, *source_id))
        }
        SonosPlayRequest::Track { .. } | SonosPlayRequest::Episode { .. } => None,
    }
}

fn queue_entry_from_media_file(
    item_type: PlaybackItemType,
    item_id: Uuid,
    media_file: &MediaFile,
) -> SonosQueueEntry {
    SonosQueueEntry {
        item_type,
        item_id,
        duration_seconds: media_file
            .duration_seconds
            .and_then(|duration| u32::try_from(duration).ok()),
    }
}

async fn prepare_current_item(
    state: &AppState,
    target_id: &str,
    session_id: Uuid,
    session_generation: u64,
    item_generation: u64,
    entry: &SonosQueueEntry,
) -> Result<SonosPreparedItem, SonosOperationError> {
    let media_file = state
        .visible_original_media_file(entry.item_type, entry.item_id)
        .await?;
    let delivery_kind = sonos_delivery_kind_for_media_file(&media_file);
    let context = SonosMediaAuthorizationContext {
        session_id,
        session_generation,
        item_generation,
        target_id: target_id.to_string(),
        item_type: entry.item_type,
        item_id: entry.item_id,
        delivery_kind,
    };
    let mut reserved_transcode_slot = None;
    if sonos_aac_profile_for_delivery(delivery_kind).is_some() {
        let _original_path = resolve_original_file(state, &media_file).map_err(|_| {
            SonosOperationError::reason(SonosErrorReason::SourceIncompatibleFallbackFailed)
        })?;
        reserved_transcode_slot = Some(state.try_acquire_transcode_slot().map_err(|_| {
            SonosOperationError::reason(SonosErrorReason::TranscodeCapacityExhausted)
        })?);
    }

    Ok(SonosPreparedItem {
        context,
        media_url: None,
        reserved_transcode_slot,
    })
}

fn mint_committed_current_item_url(
    state: &AppState,
    target_id: &str,
) -> Result<String, SonosOperationError> {
    let context = state
        .sonos_managed_sessions()
        .target_context(target_id)
        .ok_or_else(|| SonosOperationError::Api(ApiError::Internal))?;
    let signed = state
        .issue_sonos_signed_media_url_for_context(context.clone())
        .map_err(map_signed_media_issue_error)?;
    let media_url = signed.url;
    if !state
        .sonos_managed_sessions()
        .store_prepared_media_url(target_id, &context, media_url.clone())
    {
        return Err(SonosOperationError::reason(SonosErrorReason::SessionNotManaged));
    }
    Ok(media_url)
}

fn mint_committed_current_item_url_for_snapshot(
    state: &AppState,
    snapshot: &ManagedSonosSessionSnapshot,
) -> Result<String, SonosOperationError> {
    let context = state
        .sonos_managed_sessions()
        .target_context_for_snapshot(snapshot)
        .ok_or_else(|| SonosOperationError::reason(SonosErrorReason::SessionNotManaged))?;
    let signed = state
        .issue_sonos_signed_media_url_for_context(context.clone())
        .map_err(map_signed_media_issue_error)?;
    let media_url = signed.url;
    if !state.sonos_managed_sessions().store_prepared_media_url(
        &snapshot.target_id,
        &context,
        media_url.clone(),
    ) {
        return Err(SonosOperationError::reason(SonosErrorReason::SessionNotManaged));
    }
    Ok(media_url)
}

fn map_signed_media_issue_error(error: SonosSignedMediaIssueError) -> SonosOperationError {
    match error.reason() {
        Some(reason) => SonosOperationError::reason(reason),
        None => SonosOperationError::Api(ApiError::Internal),
    }
}

fn control_client() -> Result<reqwest::Client, SonosOperationError> {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(3))
        .build()
        .map_err(|_| SonosOperationError::reason(SonosErrorReason::TargetUnreachable))
}

async fn ungroup_if_needed(
    client: &reqwest::Client,
    target: &SonosResolvedTarget,
) -> Result<(), SonosOperationError> {
    if target.kind != SonosTargetKind::Speaker || target.grouped_coordinator_id.is_none() {
        return Ok(());
    }
    send_av_transport_action(
        client,
        target,
        "BecomeCoordinatorOfStandaloneGroup",
        "<InstanceID>0</InstanceID>",
    )
    .await
    .map(|_| ())
}

async fn load_and_start_current_item(
    client: &reqwest::Client,
    target: &SonosResolvedTarget,
    media_url: &str,
) -> Result<(), SonosOperationError> {
    set_current_item_uri(client, target, media_url).await?;
    start_current_item(client, target).await
}

async fn set_current_item_uri(
    client: &reqwest::Client,
    target: &SonosResolvedTarget,
    media_url: &str,
) -> Result<(), SonosOperationError> {
    let body = format!(
        "<InstanceID>0</InstanceID><CurrentURI>{}</CurrentURI><CurrentURIMetaData></CurrentURIMetaData>",
        encode_xml_entities(media_url)
    );
    send_av_transport_action(client, target, "SetAVTransportURI", &body)
        .await
        .map(|_| ())
}

async fn start_current_item(
    client: &reqwest::Client,
    target: &SonosResolvedTarget,
) -> Result<(), SonosOperationError> {
    send_av_transport_action(
        client,
        target,
        "Play",
        "<InstanceID>0</InstanceID><Speed>1</Speed>",
    )
    .await
    .map(|_| ())
}

async fn send_av_transport_action(
    client: &reqwest::Client,
    target: &SonosResolvedTarget,
    action: &str,
    body: &str,
) -> Result<String, SonosOperationError> {
    let location = target
        .coordinator_location
        .as_deref()
        .or(target.control_location.as_deref())
        .ok_or_else(|| SonosOperationError::reason(SonosErrorReason::TargetUnreachable))?;
    soap_action(
        client,
        location,
        AV_TRANSPORT_CONTROL_PATH,
        AV_TRANSPORT_SERVICE,
        action,
        body,
    )
    .await
    .ok_or_else(|| SonosOperationError::reason(SonosErrorReason::TargetUnreachable))
}

async fn refresh_target_after_command(
    client: &reqwest::Client,
    target: SonosResolvedTarget,
) -> SonosPlaybackTarget {
    let Some(location) = target
        .coordinator_location
        .as_deref()
        .or(target.control_location.as_deref())
    else {
        return target.public_target;
    };
    let live = match target.kind {
        SonosTargetKind::Speaker => fetch_live_state(client, location).await,
        SonosTargetKind::Group => fetch_group_live_state(client, location).await,
    };
    match target.public_target {
        SonosPlaybackTarget::Speaker(mut speaker) => {
            speaker.volume_percent = live.volume_percent.or(speaker.volume_percent);
            speaker.muted = live.muted.or(speaker.muted);
            speaker.transport_state = live.transport_state().or(speaker.transport_state);
            SonosPlaybackTarget::Speaker(speaker)
        }
        SonosPlaybackTarget::Group(mut group) => {
            group.volume_percent = live.volume_percent.or(group.volume_percent);
            group.muted = live.muted.or(group.muted);
            group.transport_state = live.transport_state().or(group.transport_state);
            SonosPlaybackTarget::Group(group)
        }
    }
}

async fn verify_active_target(
    client: &reqwest::Client,
    target: SonosResolvedTarget,
) -> Option<SonosResolvedTarget> {
    let SonosResolvedTarget {
        id,
        kind,
        public_target,
        control_location,
        coordinator_location,
        grouped_coordinator_id,
    } = target;
    let location = coordinator_location.as_deref().or(control_location.as_deref())?;
    let raw_transport_state = fetch_transport_state(client, location).await?;
    let live = SonosLiveState {
        volume_percent: None,
        muted: None,
        raw_transport_state: Some(raw_transport_state),
    };
    let public_target = playback_target_with_live_state(public_target, &live);
    Some(SonosResolvedTarget {
        id,
        kind,
        public_target,
        control_location,
        coordinator_location,
        grouped_coordinator_id,
    })
}

fn playback_target_with_live_state(
    target: SonosPlaybackTarget,
    live: &SonosLiveState,
) -> SonosPlaybackTarget {
    match target {
        SonosPlaybackTarget::Speaker(mut speaker) => {
            speaker.volume_percent = live.volume_percent.or(speaker.volume_percent);
            speaker.muted = live.muted.or(speaker.muted);
            speaker.transport_state = live.transport_state().or(speaker.transport_state);
            SonosPlaybackTarget::Speaker(speaker)
        }
        SonosPlaybackTarget::Group(mut group) => {
            group.volume_percent = live.volume_percent.or(group.volume_percent);
            group.muted = live.muted.or(group.muted);
            group.transport_state = live.transport_state().or(group.transport_state);
            SonosPlaybackTarget::Group(group)
        }
    }
}

fn target_transport_state(target: &SonosPlaybackTarget) -> Option<SonosTransportState> {
    match target {
        SonosPlaybackTarget::Speaker(speaker) => speaker.transport_state,
        SonosPlaybackTarget::Group(group) => group.transport_state,
    }
}

fn position_advancing_for_transport(
    transport_state: Option<SonosTransportState>,
) -> Option<bool> {
    match transport_state {
        Some(SonosTransportState::Playing) => Some(true),
        Some(_) => Some(false),
        None => None,
    }
}

fn mark_session_reconnecting(
    state: &AppState,
    snapshot: &ManagedSonosSessionSnapshot,
    now: Instant,
) {
    state.sonos_managed_sessions().update_snapshot(snapshot, |session| {
        record_active_verification_miss(session, now);
    });
}

fn record_active_verification_miss(session: &mut ManagedSonosSession, now: Instant) {
    advance_position(session, now);
    let first_loss = session.transient_loss_observed_at.unwrap_or(now);
    if session.status != SonosSessionStatus::Reconnecting
        && session.transient_loss_observed_at.is_none()
    {
        session.transient_loss_observed_at = Some(first_loss);
        session.last_position_tick = now;
        return;
    }

    session.status = SonosSessionStatus::Reconnecting;
    session.position_advancing = false;
    if session.reconnect_deadline.is_none() {
        session.reconnect_deadline = Some(first_loss + RECONNECT_WINDOW);
    }
    session.transient_loss_observed_at = None;
    session.last_position_tick = now;
}

async fn expire_reconnecting_target_if_overdue(
    state: &AppState,
    target_id: &str,
    now: Instant,
) -> bool {
    let Some(snapshot) = state.sonos_managed_sessions().snapshot(target_id) else {
        return false;
    };
    expire_reconnecting_snapshot_if_overdue(state, &snapshot, now).await
}

async fn expire_reconnecting_snapshot_if_overdue(
    state: &AppState,
    snapshot: &ManagedSonosSessionSnapshot,
    now: Instant,
) -> bool {
    if !reconnect_deadline_elapsed(snapshot, now) {
        return false;
    }
    let Some(session) = state.sonos_managed_sessions().remove_snapshot(snapshot) else {
        return false;
    };
    write_session_snapshot_attribution(state, &session.snapshot(), false, true).await;
    true
}

fn reconnect_deadline_elapsed(snapshot: &ManagedSonosSessionSnapshot, now: Instant) -> bool {
    debug_assert!(
        snapshot.transient_loss_observed_at.is_none()
            || snapshot.status == SonosSessionStatus::Active
    );
    snapshot.status == SonosSessionStatus::Reconnecting
        && snapshot
            .reconnect_deadline
            .is_some_and(|deadline| now >= deadline)
}

async fn resume_reconnected_session(
    state: &AppState,
    snapshot: ManagedSonosSessionSnapshot,
    resolved_target: SonosResolvedTarget,
    request_timeout: Duration,
) -> Result<(), SonosOperationError> {
    let item_generation = snapshot.item_generation.saturating_add(1);
    let current_entry = snapshot
        .current_entry()
        .ok_or_else(|| SonosOperationError::Api(ApiError::Internal))?;
    let prepared_item = prepare_current_item(
        state,
        &snapshot.target_id,
        snapshot.session_id,
        snapshot.session_generation,
        item_generation,
        current_entry,
    )
    .await?;
    let client = reqwest::Client::builder()
        .timeout(request_timeout)
        .build()
        .map_err(|_| SonosOperationError::reason(SonosErrorReason::TargetUnreachable))?;
    let transition_now = Instant::now();
    let Some(captured_rollback) =
        state
            .sonos_managed_sessions()
            .update_snapshot(&snapshot, |session| {
                let rollback = ManagedSonosSessionRollback::capture(session);
                session.item_generation = item_generation;
                session.current_position_seconds = snapshot.current_position_seconds;
                session.status = SonosSessionStatus::Active;
                session.position_advancing = true;
                session.reconnect_deadline = None;
                session.transient_loss_observed_at = None;
                session.latest_target = resolved_target.public_target.clone();
                session.cache_control_target(&resolved_target);
                session.prepared_item = Some(prepared_item);
                session.last_position_tick = transition_now;
                session.last_progress_write = Some(transition_now);
                rollback
            })
    else {
        return Ok(());
    };
    let mut rollback = Some(captured_rollback);
    let media_url = match mint_committed_current_item_url_for_snapshot(state, &snapshot) {
        Ok(media_url) => media_url,
        Err(error) => {
            if let Some(rollback) = rollback.take() {
                state
                    .sonos_managed_sessions()
                    .update_snapshot(&snapshot, |session| {
                        rollback.restore(session);
                    });
            }
            return Err(error);
        }
    };
    let transport_guard = state
        .sonos_managed_sessions()
        .acquire_transport_guard(&snapshot.target_id)
        .await;
    if !state.sonos_managed_sessions().matches_snapshot(&snapshot) {
        if let Some(rollback) = rollback.take() {
            state
                .sonos_managed_sessions()
                .update_snapshot(&snapshot, |session| {
                    rollback.restore(session);
                });
        }
        return Ok(());
    }
    if let Err(error) = set_current_item_uri(&client, &resolved_target, &media_url).await {
        if let Some(rollback) = rollback.take() {
            state
                .sonos_managed_sessions()
                .update_snapshot(&snapshot, |session| {
                    rollback.restore(session);
                });
        }
        return Err(error);
    }
    if !state.sonos_managed_sessions().matches_snapshot(&snapshot) {
        if let Some(rollback) = rollback.take() {
            state
                .sonos_managed_sessions()
                .update_snapshot(&snapshot, |session| {
                    rollback.restore(session);
                });
        }
        return Ok(());
    }
    if let Err(error) = start_current_item(&client, &resolved_target).await {
        if let Some(rollback) = rollback.take() {
            state
                .sonos_managed_sessions()
                .update_snapshot(&snapshot, |session| {
                    rollback.restore(session);
                });
        }
        return Err(error);
    }
    if !state.sonos_managed_sessions().matches_snapshot(&snapshot) {
        if let Some(rollback) = rollback.take() {
            state
                .sonos_managed_sessions()
                .update_snapshot(&snapshot, |session| {
                    rollback.restore(session);
                });
        }
        return Ok(());
    }
    if snapshot.current_position_seconds > 0 {
        let target = format_duration(snapshot.current_position_seconds);
        let body = format!(
            "<InstanceID>0</InstanceID><Unit>REL_TIME</Unit><Target>{target}</Target>"
        );
        if !state.sonos_managed_sessions().matches_snapshot(&snapshot) {
            if let Some(rollback) = rollback.take() {
                state
                    .sonos_managed_sessions()
                    .update_snapshot(&snapshot, |session| {
                        rollback.restore(session);
                    });
            }
            return Ok(());
        }
        if let Err(error) =
            send_av_transport_action(&client, &resolved_target, "Seek", &body).await
        {
            if let Some(rollback) = rollback.take() {
                state
                    .sonos_managed_sessions()
                    .update_snapshot(&snapshot, |session| {
                        rollback.restore(session);
                    });
            }
            return Err(error);
        }
    }
    let latest_target = refresh_target_after_command(&client, resolved_target.clone()).await;
    let now = Instant::now();
    let Some(current_snapshot) =
        state
            .sonos_managed_sessions()
            .update_snapshot(&snapshot, |session| {
                session.current_position_seconds = snapshot.current_position_seconds;
                session.status = SonosSessionStatus::Active;
                session.position_advancing = true;
                session.reconnect_deadline = None;
                session.transient_loss_observed_at = None;
                session.latest_target = latest_target;
                session.cache_control_target(&resolved_target);
                session.last_position_tick = now;
                session.last_progress_write = Some(now);
                session.snapshot()
            })
    else {
        return Ok(());
    };
    drop(transport_guard);
    write_session_snapshot_attribution(state, &current_snapshot, false, true).await;
    Ok(())
}

fn advance_position(session: &mut ManagedSonosSession, now: Instant) {
    if session.status != SonosSessionStatus::Active || !session.position_advancing {
        session.last_position_tick = now;
        return;
    }
    let elapsed = now.saturating_duration_since(session.last_position_tick);
    if elapsed.as_secs() > 0 {
        let next_position = session
            .current_position_seconds
            .saturating_add(elapsed.as_secs() as u32);
        session.current_position_seconds = match session.current_duration_seconds {
            Some(duration) => next_position.min(duration),
            None => next_position,
        };
        session.last_position_tick = now;
    }
}

fn update_position_advancement(
    session: &mut ManagedSonosSession,
    now: Instant,
    position_advancing: bool,
) {
    advance_position(session, now);
    session.position_advancing = position_advancing;
    session.last_position_tick = now;
}

async fn maybe_write_heartbeat(
    state: &AppState,
    snapshot: &ManagedSonosSessionSnapshot,
    now: Instant,
) {
    let snapshot_to_write = state
        .sonos_managed_sessions()
        .update_snapshot(snapshot, |session| {
            if session.status != SonosSessionStatus::Active || !session.position_advancing {
                return None;
            }
            match session.last_progress_write {
                Some(last) if now.saturating_duration_since(last) < HEARTBEAT_INTERVAL => None,
                _ => {
                    session.last_progress_write = Some(now);
                    Some(session.snapshot())
                }
            }
        })
        .flatten();
    if let Some(snapshot) = snapshot_to_write {
        write_session_snapshot_attribution(state, &snapshot, false, false).await;
    }
}

async fn write_session_attribution(
    state: &AppState,
    target_id: &str,
    completed: bool,
    history: bool,
) {
    let Some(snapshot) = state.sonos_managed_sessions().snapshot(target_id) else {
        return;
    };
    write_session_snapshot_attribution(state, &snapshot, completed, history).await;
}

async fn write_session_snapshot_attribution(
    state: &AppState,
    snapshot: &ManagedSonosSessionSnapshot,
    completed: bool,
    history: bool,
) {
    let Some(current) = snapshot.current_entry() else {
        return;
    };
    if let Err(error) = state
        .upsert_playback_progress(
            snapshot.owner_account_id,
            current.item_type,
            current.item_id,
            snapshot.context_type,
            snapshot.context_id,
            snapshot.current_position_seconds,
            snapshot.current_duration_seconds,
            completed,
        )
        .await
    {
        tracing::debug!(%error, target_id = %snapshot.target_id, "failed to write Sonos playback progress");
    }
    if history {
        if let Err(error) = state
            .insert_playback_history_event(
                snapshot.owner_account_id,
                current.item_type,
                current.item_id,
                snapshot.context_type,
                snapshot.context_id,
                snapshot.current_position_seconds,
                snapshot.current_duration_seconds,
                completed,
            )
            .await
        {
            tracing::debug!(%error, target_id = %snapshot.target_id, "failed to write Sonos playback history");
        }
    }
}

fn format_duration(seconds: u32) -> String {
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let seconds = seconds % 60;
    format!("{hours:02}:{minutes:02}:{seconds:02}")
}

#[derive(Debug, Error)]
pub enum SonosDiscoveryError {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

#[derive(Debug, Default)]
struct SonosRefreshTracker {
    consecutive_empty_refreshes: usize,
}

impl SonosRefreshTracker {
    fn publishable_snapshot(&mut self, snapshot: SonosSnapshot) -> Option<SonosSnapshot> {
        if snapshot.is_empty() {
            self.consecutive_empty_refreshes += 1;
            if self.consecutive_empty_refreshes < EMPTY_DISCOVERY_REFRESHES_BEFORE_EXPIRY {
                return None;
            }
        } else {
            self.consecutive_empty_refreshes = 0;
        }

        Some(snapshot)
    }
}

pub async fn runtime_loop(state: AppState, config: SonosRuntimeConfig) {
    let mut refresh_tracker = SonosRefreshTracker::default();

    loop {
        match discover_snapshot(&config).await {
            Ok(snapshot) => {
                if let Some(snapshot) = refresh_tracker.publishable_snapshot(snapshot) {
                    if snapshot.is_empty() {
                        tracing::debug!(
                            "sonos discovery returned no targets persistently; publishing empty snapshot"
                        );
                    }
                    state.replace_sonos_snapshot(snapshot);
                } else {
                    tracing::debug!(
                        "sonos discovery returned no targets; retaining previous snapshot"
                    );
                }
                sleep(config.poll_interval).await;
            }
            Err(error) => {
                tracing::warn!(%error, "sonos discovery refresh failed");
                if let Some(snapshot) =
                    refresh_tracker.publishable_snapshot(SonosSnapshot::empty())
                {
                    state.replace_sonos_snapshot(snapshot);
                }
                sleep(config.error_backoff).await;
            }
        }
    }
}

async fn discover_snapshot(
    config: &SonosRuntimeConfig,
) -> Result<SonosSnapshot, SonosDiscoveryError> {
    let devices = discover_devices(config.discovery_timeout).await?;
    let client = reqwest::Client::builder()
        .timeout(config.request_timeout)
        .build()?;

    Ok(discover_snapshot_from_devices(&client, devices).await)
}

async fn discover_snapshot_from_devices(
    client: &reqwest::Client,
    devices: BTreeMap<String, DiscoveredDevice>,
) -> SonosSnapshot {
    let mut speakers = BTreeMap::new();
    let mut speaker_locations = device_locations_by_usn(&devices);

    for device in devices.values() {
        if let Some(speaker) = fetch_speaker(&client, device).await {
            speaker_locations.insert(speaker.id.clone(), device.location.clone());
            speakers.insert(speaker.id.clone(), speaker);
        }
    }

    let mut groups = BTreeMap::new();
    let mut group_topologies = Vec::new();
    for device in devices.values() {
        if let Some(topology) = fetch_topology(&client, &device.location).await {
            for group in fetch_topology_group_snapshots(
                client,
                &topology,
                &speakers,
                &speaker_locations,
            )
            .await
            {
                group_topologies.push(group.topology.clone());
                groups.insert(group.snapshot.id.clone(), group.snapshot);
            }
            if !groups.is_empty() {
                break;
            }
        }
    }

    let controls = control_targets_from_snapshots(
        &speakers,
        &groups,
        &speaker_locations,
        &speaker_locations,
        &group_topologies,
    );

    SonosSnapshot {
        speakers,
        groups,
        controls,
    }
}

async fn discover_devices(
    discovery_timeout: Duration,
) -> Result<BTreeMap<String, DiscoveredDevice>, std::io::Error> {
    let socket = UdpSocket::bind("0.0.0.0:0").await?;
    let message = format!(
        "M-SEARCH * HTTP/1.1\r\n\
         HOST: {SSDP_DISCOVERY_ADDR}\r\n\
         MAN: \"ssdp:discover\"\r\n\
         MX: 1\r\n\
         ST: {ZONE_PLAYER_ST}\r\n\
         USER-AGENT: Harmonixia/0.1 UPnP/1.1\r\n\r\n"
    );
    socket.send_to(message.as_bytes(), SSDP_DISCOVERY_ADDR).await?;

    let started = Instant::now();
    let mut buffer = [0_u8; 8192];
    let mut devices = BTreeMap::new();

    while started.elapsed() < discovery_timeout {
        let remaining = discovery_timeout
            .checked_sub(started.elapsed())
            .unwrap_or_else(|| Duration::from_millis(0));
        if remaining.is_zero() {
            break;
        }

        let received = tokio::time::timeout(remaining, socket.recv_from(&mut buffer)).await;
        let Ok(Ok((len, _addr))) = received else {
            break;
        };
        let response = String::from_utf8_lossy(&buffer[..len]);
        if let Some(device) = parse_ssdp_response(&response) {
            devices.insert(device.location.clone(), device);
        }
    }

    Ok(devices)
}

async fn fetch_speaker(
    client: &reqwest::Client,
    device: &DiscoveredDevice,
) -> Option<SonosSpeakerSnapshot> {
    let xml = client
        .get(&device.location)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .text()
        .await
        .ok()?;
    let id = xml_tag(&xml, "UDN")
        .or_else(|| device.usn.clone())
        .map(|value| normalize_sonos_id(&value))
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| device.location.clone());
    let room_name = xml_tag(&xml, "roomName").filter(|value| !value.trim().is_empty());
    let display_name = xml_tag(&xml, "friendlyName")
        .or_else(|| room_name.clone())
        .unwrap_or_else(|| id.clone());

    let live = fetch_live_state(client, &device.location).await;

    Some(SonosSpeakerSnapshot {
        id,
        display_name,
        room_name,
        available: true,
        live,
    })
}

async fn fetch_live_state(client: &reqwest::Client, location: &str) -> SonosLiveState {
    let volume_percent = fetch_volume(client, location).await;
    let muted = fetch_mute(client, location).await;
    let raw_transport_state = fetch_transport_state(client, location).await;

    SonosLiveState {
        volume_percent,
        muted,
        raw_transport_state,
    }
}

async fn fetch_group_live_state(client: &reqwest::Client, location: &str) -> SonosLiveState {
    let volume_percent = fetch_group_volume(client, location).await;
    let muted = fetch_group_mute(client, location).await;
    let raw_transport_state = fetch_transport_state(client, location).await;

    SonosLiveState {
        volume_percent,
        muted,
        raw_transport_state,
    }
}

async fn fetch_volume(client: &reqwest::Client, location: &str) -> Option<u8> {
    let response = soap_action(
        client,
        location,
        "/MediaRenderer/RenderingControl/Control",
        "urn:schemas-upnp-org:service:RenderingControl:1",
        "GetVolume",
        "<InstanceID>0</InstanceID><Channel>Master</Channel>",
    )
    .await?;
    xml_tag(&response, "CurrentVolume")?.parse().ok()
}

async fn fetch_group_volume(client: &reqwest::Client, location: &str) -> Option<u8> {
    let response = soap_action(
        client,
        location,
        GROUP_RENDERING_CONTROL_PATH,
        GROUP_RENDERING_CONTROL_SERVICE,
        "GetGroupVolume",
        "<InstanceID>0</InstanceID>",
    )
    .await?;
    xml_tag(&response, "CurrentVolume")?.parse().ok()
}

async fn fetch_mute(client: &reqwest::Client, location: &str) -> Option<bool> {
    let response = soap_action(
        client,
        location,
        "/MediaRenderer/RenderingControl/Control",
        "urn:schemas-upnp-org:service:RenderingControl:1",
        "GetMute",
        "<InstanceID>0</InstanceID><Channel>Master</Channel>",
    )
    .await?;
    parse_mute_value(&xml_tag(&response, "CurrentMute")?)
}

async fn fetch_group_mute(client: &reqwest::Client, location: &str) -> Option<bool> {
    let response = soap_action(
        client,
        location,
        GROUP_RENDERING_CONTROL_PATH,
        GROUP_RENDERING_CONTROL_SERVICE,
        "GetGroupMute",
        "<InstanceID>0</InstanceID>",
    )
    .await?;
    parse_mute_value(&xml_tag(&response, "CurrentMute")?)
}

fn parse_mute_value(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" => Some(true),
        "0" | "false" => Some(false),
        _ => None,
    }
}

async fn fetch_transport_state(client: &reqwest::Client, location: &str) -> Option<String> {
    let response = soap_action(
        client,
        location,
        "/MediaRenderer/AVTransport/Control",
        "urn:schemas-upnp-org:service:AVTransport:1",
        "GetTransportInfo",
        "<InstanceID>0</InstanceID>",
    )
    .await?;
    xml_tag(&response, "CurrentTransportState")
}

async fn soap_action(
    client: &reqwest::Client,
    location: &str,
    path: &str,
    service: &str,
    action: &str,
    body: &str,
) -> Option<String> {
    let url = control_url(location, path)?;
    let envelope = format!(
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\
         <s:Envelope xmlns:s=\"http://schemas.xmlsoap.org/soap/envelope/\" \
         s:encodingStyle=\"http://schemas.xmlsoap.org/soap/encoding/\">\
         <s:Body><u:{action} xmlns:u=\"{service}\">{body}</u:{action}></s:Body>\
         </s:Envelope>"
    );

    client
        .post(url)
        .header("SOAPACTION", format!("\"{service}#{action}\""))
        .header("CONTENT-TYPE", "text/xml; charset=\"utf-8\"")
        .body(envelope)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .text()
        .await
        .ok()
}

fn control_url(location: &str, path: &str) -> Option<reqwest::Url> {
    let mut url = reqwest::Url::parse(location).ok()?;
    url.set_path(path);
    url.set_query(None);
    url.set_fragment(None);
    Some(url)
}

async fn fetch_topology(client: &reqwest::Client, location: &str) -> Option<String> {
    let url = control_url(location, "/status/topology")?;
    client
        .get(url)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .text()
        .await
        .ok()
}

async fn fetch_topology_group_snapshots(
    client: &reqwest::Client,
    topology: &str,
    speakers: &BTreeMap<String, SonosSpeakerSnapshot>,
    speaker_locations: &BTreeMap<String, String>,
) -> Vec<SonosGroupDiscoverySnapshot> {
    let mut groups = Vec::new();

    for group in parse_topology_groups(topology, speakers) {
        let live = if let Some(location) = group
            .coordinator_id
            .as_deref()
            .and_then(|coordinator| speaker_locations.get(coordinator))
        {
            fetch_group_live_state(client, location).await
        } else {
            SonosLiveState::unknown()
        };

        groups.push(SonosGroupDiscoverySnapshot {
            snapshot: SonosGroupSnapshot {
                id: group.id.clone(),
                display_name: group.display_name.clone(),
                available: true,
                live,
            },
            topology: group,
        });
    }

    groups
}

#[derive(Debug, Clone)]
struct SonosGroupDiscoverySnapshot {
    snapshot: SonosGroupSnapshot,
    topology: SonosGroupTopology,
}

#[derive(Debug, Clone)]
struct SonosGroupTopology {
    id: String,
    display_name: String,
    coordinator_id: Option<String>,
    member_ids: Vec<String>,
}

fn parse_topology_groups(
    xml: &str,
    speakers: &BTreeMap<String, SonosSpeakerSnapshot>,
) -> Vec<SonosGroupTopology> {
    let mut groups = Vec::new();
    let mut remaining = xml;

    while let Some(start) = remaining.find("<ZoneGroup ") {
        let group_start = &remaining[start..];
        let Some(start_tag_end) = group_start.find('>') else {
            break;
        };
        let start_tag = &group_start[..=start_tag_end];
        let attrs = parse_xml_attributes(start_tag);
        let body_start = start_tag_end + 1;
        let Some(close_start) = group_start[body_start..].find("</ZoneGroup>") else {
            break;
        };
        let body = &group_start[body_start..body_start + close_start];
        let coordinator_id = attrs
            .get("Coordinator")
            .map(|value| normalize_sonos_id(value))
            .filter(|value| !value.is_empty());
        let group_id = attrs
            .get("ID")
            .map(|value| normalize_sonos_id(value))
            .filter(|value| !value.is_empty())
            .or_else(|| coordinator_id.clone());

        if let Some(id) = group_id {
            let members = parse_group_members(body, speakers);
            let member_names: Vec<_> = members
                .iter()
                .filter_map(|member| member.name.clone())
                .collect();
            let display_name = if member_names.is_empty() {
                coordinator_id
                    .as_deref()
                    .and_then(|coordinator| speakers.get(coordinator))
                    .map(|speaker| {
                        display_name(
                            &speaker.display_name,
                            speaker.room_name.as_deref(),
                            &speaker.id,
                        )
                    })
                    .unwrap_or_else(|| id.clone())
            } else {
                member_names.join(" + ")
            };

            groups.push(SonosGroupTopology {
                id,
                display_name,
                coordinator_id,
                member_ids: members
                    .into_iter()
                    .filter_map(|member| member.id)
                    .collect(),
            });
        }

        let consumed = body_start + close_start + "</ZoneGroup>".len();
        remaining = &group_start[consumed..];
    }

    groups
}

#[derive(Debug, Clone)]
struct SonosGroupMemberTopology {
    id: Option<String>,
    name: Option<String>,
}

fn parse_group_members(
    xml: &str,
    speakers: &BTreeMap<String, SonosSpeakerSnapshot>,
) -> Vec<SonosGroupMemberTopology> {
    let mut members = Vec::new();
    let mut remaining = xml;

    while let Some(start) = remaining.find("<ZoneGroupMember") {
        let member_start = &remaining[start..];
        let Some(tag_end) = member_start.find('>') else {
            break;
        };
        let tag = &member_start[..=tag_end];
        let attrs = parse_xml_attributes(tag);
        let id = attrs
            .get("UUID")
            .or_else(|| attrs.get("Uuid"))
            .map(|value| normalize_sonos_id(value));
        let name = attrs
            .get("ZoneName")
            .or_else(|| attrs.get("RoomName"))
            .cloned()
            .or_else(|| {
                id.as_deref().and_then(|id| {
                    speakers.get(id).map(|speaker| {
                        display_name(
                            &speaker.display_name,
                            speaker.room_name.as_deref(),
                            &speaker.id,
                        )
                    })
                })
            });

        let name = name.filter(|name| !name.trim().is_empty());
        members.push(SonosGroupMemberTopology { id, name });

        remaining = &member_start[tag_end + 1..];
    }

    members
}

fn device_locations_by_usn(
    devices: &BTreeMap<String, DiscoveredDevice>,
) -> BTreeMap<String, String> {
    devices
        .values()
        .filter_map(|device| {
            let id = device
                .usn
                .as_deref()
                .map(normalize_sonos_id)
                .filter(|id| !id.is_empty())?;
            Some((id, device.location.clone()))
        })
        .collect()
}

fn control_targets_from_snapshots(
    speakers: &BTreeMap<String, SonosSpeakerSnapshot>,
    groups: &BTreeMap<String, SonosGroupSnapshot>,
    speaker_locations: &BTreeMap<String, String>,
    coordinator_locations: &BTreeMap<String, String>,
    group_topologies: &[SonosGroupTopology],
) -> BTreeMap<String, SonosControlTargetSnapshot> {
    let mut controls = BTreeMap::new();
    let grouped_speakers = grouped_speaker_metadata(group_topologies, coordinator_locations);

    for speaker in speakers.values() {
        let grouped = grouped_speakers.get(&speaker.id);
        controls.insert(
            speaker.id.clone(),
            SonosControlTargetSnapshot {
                kind: SonosTargetKind::Speaker,
                public_target: SonosPlaybackTarget::Speaker(speaker.to_target()),
                control_location: speaker_locations.get(&speaker.id).cloned(),
                coordinator_location: speaker_locations.get(&speaker.id).cloned(),
                grouped_coordinator_id: grouped.and_then(|metadata| {
                    metadata.coordinator_id.as_ref().filter(|id| *id != &speaker.id).cloned()
                }),
            },
        );
    }

    for group in groups.values() {
        let topology = group_topologies
            .iter()
            .find(|topology| topology.id == group.id);
        let coordinator_location = topology
            .and_then(|topology| topology.coordinator_id.as_ref())
            .and_then(|coordinator_id| coordinator_locations.get(coordinator_id))
            .cloned();
        controls.insert(
            group.id.clone(),
            SonosControlTargetSnapshot {
                kind: SonosTargetKind::Group,
                public_target: SonosPlaybackTarget::Group(group.to_target()),
                control_location: coordinator_location.clone(),
                coordinator_location,
                grouped_coordinator_id: None,
            },
        );
    }

    controls
}

#[derive(Debug)]
struct GroupedSpeakerMetadata {
    coordinator_id: Option<String>,
}

fn grouped_speaker_metadata(
    group_topologies: &[SonosGroupTopology],
    coordinator_locations: &BTreeMap<String, String>,
) -> BTreeMap<String, GroupedSpeakerMetadata> {
    let mut metadata = BTreeMap::new();
    for topology in group_topologies {
        if topology.member_ids.len() <= 1 {
            continue;
        }
        if let Some(coordinator_id) = topology.coordinator_id.as_ref() {
            if !coordinator_locations.contains_key(coordinator_id) {
                continue;
            }
        }
        for member_id in &topology.member_ids {
            metadata.insert(
                member_id.clone(),
                GroupedSpeakerMetadata {
                    coordinator_id: topology.coordinator_id.clone(),
                },
            );
        }
    }
    metadata
}

#[derive(Debug, Clone)]
struct DiscoveredDevice {
    location: String,
    usn: Option<String>,
}

fn parse_ssdp_response(response: &str) -> Option<DiscoveredDevice> {
    let mut location = None;
    let mut usn = None;
    let mut is_zone_player = false;

    for line in response.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        match name.as_str() {
            "location" => location = Some(value.to_string()),
            "usn" => {
                is_zone_player |= value.contains("ZonePlayer");
                usn = Some(value.to_string());
            }
            "st" => is_zone_player |= value.contains("ZonePlayer"),
            _ => {}
        }
    }

    if !is_zone_player {
        return None;
    }

    Some(DiscoveredDevice {
        location: location?,
        usn,
    })
}

fn parse_xml_attributes(tag: &str) -> HashMap<String, String> {
    let mut attributes = HashMap::new();
    let mut rest = tag
        .trim()
        .trim_start_matches('<')
        .trim_end_matches('>')
        .trim_end_matches('/')
        .trim();

    if let Some(name_end) = rest.find(char::is_whitespace) {
        rest = rest[name_end..].trim();
    } else {
        return attributes;
    }

    while !rest.is_empty() {
        let Some(eq) = rest.find('=') else {
            break;
        };
        let name = rest[..eq].trim();
        let mut after_eq = rest[eq + 1..].trim_start();
        let Some(quote) = after_eq.chars().next() else {
            break;
        };
        if quote != '"' && quote != '\'' {
            break;
        }
        after_eq = &after_eq[quote.len_utf8()..];
        let Some(end) = after_eq.find(quote) else {
            break;
        };
        let value = decode_xml_entities(&after_eq[..end]);
        if !name.is_empty() {
            attributes.insert(name.to_string(), value);
        }
        rest = after_eq[end + quote.len_utf8()..].trim_start();
    }

    attributes
}

fn xml_tag(xml: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let start = xml.find(&open)? + open.len();
    let end = xml[start..].find(&close)? + start;
    Some(decode_xml_entities(xml[start..end].trim()))
}

fn normalize_sonos_id(value: &str) -> String {
    let value = value
        .trim()
        .split("::")
        .next()
        .unwrap_or(value)
        .trim()
        .trim_start_matches("uuid:")
        .trim_start_matches("UUID:");
    value.to_string()
}

fn display_name(display_name: &str, room_name: Option<&str>, id: &str) -> String {
    if !display_name.trim().is_empty() {
        return display_name.to_string();
    }
    if let Some(room_name) = room_name {
        if !room_name.trim().is_empty() {
            return room_name.to_string();
        }
    }
    id.to_string()
}

fn decode_xml_entities(value: &str) -> String {
    value
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
}

fn encode_xml_entities(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn map_raw_transport_state(value: &str) -> Option<SonosTransportState> {
    match value.trim().to_ascii_lowercase().as_str() {
        "idle" | "stopped" | "stop" | "no_media_present" => {
            Some(SonosTransportState::Stopped)
        }
        "buffering" | "transitioning" => Some(SonosTransportState::Buffering),
        "paused" | "paused_playback" => Some(SonosTransportState::Paused),
        "playing" => Some(SonosTransportState::Playing),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };

    #[test]
    fn raw_idle_transport_projects_as_stopped() {
        let snapshot = SonosSnapshot::from_targets(
            vec![SonosSpeakerSnapshot {
                id: "speaker-1".into(),
                display_name: "Kitchen".into(),
                room_name: Some("Kitchen".into()),
                available: true,
                live: SonosLiveState {
                    volume_percent: Some(20),
                    muted: Some(false),
                    raw_transport_state: Some("idle".into()),
                },
            }],
            Vec::new(),
        );

        let response = snapshot.to_targets_response();
        assert_eq!(
            response.speakers[0].transport_state,
            Some(SonosTransportState::Stopped)
        );
    }

    #[test]
    fn transient_empty_refreshes_keep_previous_targets_until_persistent_loss() {
        let mut refresh_tracker = SonosRefreshTracker::default();
        let mut published = refresh_tracker
            .publishable_snapshot(SonosSnapshot::from_targets(
                vec![SonosSpeakerSnapshot {
                    id: "speaker-1".into(),
                    display_name: "Kitchen".into(),
                    room_name: Some("Kitchen".into()),
                    available: true,
                    live: SonosLiveState {
                        volume_percent: Some(20),
                        muted: Some(false),
                        raw_transport_state: Some("playing".into()),
                    },
                }],
                Vec::new(),
            ))
            .expect("populated snapshot should publish immediately");
        assert_eq!(published.to_targets_response().speakers.len(), 1);

        for _ in 1..EMPTY_DISCOVERY_REFRESHES_BEFORE_EXPIRY {
            assert!(refresh_tracker
                .publishable_snapshot(SonosSnapshot::empty())
                .is_none());
            assert_eq!(published.to_targets_response().speakers.len(), 1);
        }

        published = refresh_tracker
            .publishable_snapshot(SonosSnapshot::empty())
            .expect("persistent empty refreshes should publish empty snapshot");

        assert!(published.to_targets_response().speakers.is_empty());
        assert!(published.to_targets_response().groups.is_empty());
    }

    #[test]
    fn active_session_verification_miss_requires_confirmation_before_reconnect() {
        let now = Instant::now();
        let item_id = Uuid::new_v4();
        let mut session = ManagedSonosSession {
            target_id: "speaker-1".into(),
            target_kind: SonosTargetKind::Speaker,
            control_location: Some("http://127.0.0.1/xml/device.xml".into()),
            coordinator_location: None,
            grouped_coordinator_id: None,
            owner_account_id: Uuid::new_v4(),
            owner_username: "owner".into(),
            context_type: None,
            context_id: None,
            session_id: Uuid::new_v4(),
            session_generation: 1,
            item_generation: 1,
            queue: vec![SonosQueueEntry {
                item_type: PlaybackItemType::Track,
                item_id,
                duration_seconds: Some(90),
            }],
            queue_index: 0,
            current_position_seconds: 0,
            current_duration_seconds: Some(90),
            status: SonosSessionStatus::Active,
            position_advancing: true,
            reconnect_deadline: None,
            transient_loss_observed_at: None,
            latest_target: SonosPlaybackTarget::Speaker(SonosSpeakerTarget {
                id: "speaker-1".into(),
                display_name: "Kitchen".into(),
                room_name: Some("Kitchen".into()),
                available: true,
                volume_percent: Some(20),
                muted: Some(false),
                transport_state: Some(SonosTransportState::Playing),
            }),
            prepared_item: None,
            last_progress_write: None,
            last_position_tick: now,
        };

        record_active_verification_miss(&mut session, now);
        assert_eq!(session.status, SonosSessionStatus::Active);
        assert_eq!(session.transient_loss_observed_at, Some(now));
        assert_eq!(session.reconnect_deadline, None);

        let confirmed_at = now + Duration::from_secs(2);
        record_active_verification_miss(&mut session, confirmed_at);
        assert_eq!(session.status, SonosSessionStatus::Reconnecting);
        assert_eq!(session.position_advancing, false);
        assert_eq!(session.transient_loss_observed_at, None);
        assert_eq!(
            session.reconnect_deadline,
            Some(now + RECONNECT_WINDOW)
        );
    }

    #[tokio::test]
    async fn topology_groups_use_group_live_state_from_coordinator_endpoint() {
        let server = MockSonosControlServer::start().await;
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let mut speakers = BTreeMap::new();
        speakers.insert(
            "speaker-1".into(),
            SonosSpeakerSnapshot {
                id: "speaker-1".into(),
                display_name: "Kitchen".into(),
                room_name: Some("Kitchen".into()),
                available: true,
                live: SonosLiveState {
                    volume_percent: Some(12),
                    muted: Some(false),
                    raw_transport_state: Some("playing".into()),
                },
            },
        );
        let mut speaker_locations = BTreeMap::new();
        speaker_locations.insert(
            "speaker-1".into(),
            format!("{}/xml/device.xml", server.base_url),
        );
        let topology = r#"
            <ZoneGroups>
                <ZoneGroup Coordinator="uuid:speaker-1" ID="uuid:group-1">
                    <ZoneGroupMember UUID="uuid:speaker-1" ZoneName="Kitchen"/>
                </ZoneGroup>
            </ZoneGroups>
        "#;

        let groups =
            fetch_topology_group_snapshots(&client, topology, &speakers, &speaker_locations).await;

        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].snapshot.live.volume_percent, Some(77));
        assert_eq!(groups[0].snapshot.live.muted, Some(true));
        assert_eq!(groups[0].snapshot.live.transport_state(), Some(SonosTransportState::Paused));
        assert_ne!(
            groups[0].snapshot.live.volume_percent,
            speakers["speaker-1"].live.volume_percent
        );
        assert_ne!(groups[0].snapshot.live.muted, speakers["speaker-1"].live.muted);

        let requests = server.requests();
        assert!(requests
            .iter()
            .any(|request| request
                .starts_with("POST /MediaRenderer/GroupRenderingControl/Control")));
        assert!(requests
            .iter()
            .any(|request| request.starts_with("POST /MediaRenderer/AVTransport/Control")));
        assert!(!requests
            .iter()
            .any(|request| request.starts_with("POST /MediaRenderer/RenderingControl/Control")));
    }

    struct MockSonosControlServer {
        base_url: String,
        requests: Arc<Mutex<Vec<String>>>,
        handle: JoinHandle<()>,
    }

    impl MockSonosControlServer {
        async fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base_url = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::new(Mutex::new(Vec::new()));
            let server_requests = requests.clone();
            let handle = tokio::spawn(async move {
                loop {
                    let Ok((mut socket, _)) = listener.accept().await else {
                        break;
                    };
                    let server_requests = server_requests.clone();
                    tokio::spawn(async move {
                        let mut buffer = [0_u8; 4096];
                        let Ok(len) = socket.read(&mut buffer).await else {
                            return;
                        };
                        let request = String::from_utf8_lossy(&buffer[..len]);
                        let first_line = request.lines().next().unwrap_or_default().to_string();
                        server_requests.lock().unwrap().push(first_line);

                        let body = if request.contains("GetGroupVolume") {
                            soap_response("<CurrentVolume>77</CurrentVolume>")
                        } else if request.contains("GetGroupMute") {
                            soap_response("<CurrentMute>1</CurrentMute>")
                        } else if request.contains("GetTransportInfo") {
                            soap_response(
                                "<CurrentTransportState>PAUSED_PLAYBACK</CurrentTransportState>",
                            )
                        } else {
                            soap_response("")
                        };
                        let response = format!(
                            "HTTP/1.1 200 OK\r\n\
                             content-type: text/xml\r\n\
                             content-length: {}\r\n\
                             connection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = socket.write_all(response.as_bytes()).await;
                    });
                }
            });

            Self {
                base_url,
                requests,
                handle,
            }
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl Drop for MockSonosControlServer {
        fn drop(&mut self) {
            self.handle.abort();
        }
    }

    fn soap_response(body: &str) -> String {
        format!(
            "<?xml version=\"1.0\"?><s:Envelope><s:Body>{body}</s:Body></s:Envelope>"
        )
    }
}
