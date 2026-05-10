use std::{
    collections::{BTreeMap, HashMap},
    time::{Duration, Instant},
};

use thiserror::Error;
use tokio::{net::UdpSocket, time::sleep};

use crate::{
    api::sonos::{SonosGroupTarget, SonosSpeakerTarget, SonosTargetsResponse},
    domain::SonosTransportState,
    state::AppState,
};

const SSDP_DISCOVERY_ADDR: &str = "239.255.255.250:1900";
const ZONE_PLAYER_ST: &str = "urn:schemas-upnp-org:device:ZonePlayer:1";
const EMPTY_DISCOVERY_REFRESHES_BEFORE_EXPIRY: usize = 3;
const GROUP_RENDERING_CONTROL_PATH: &str = "/MediaRenderer/GroupRenderingControl/Control";
const GROUP_RENDERING_CONTROL_SERVICE: &str =
    "urn:schemas-upnp-org:service:GroupRenderingControl:1";

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

#[derive(Debug, Clone, Default)]
pub struct SonosSnapshot {
    speakers: BTreeMap<String, SonosSpeakerSnapshot>,
    groups: BTreeMap<String, SonosGroupSnapshot>,
}

impl SonosSnapshot {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn from_targets(
        speakers: Vec<SonosSpeakerSnapshot>,
        groups: Vec<SonosGroupSnapshot>,
    ) -> Self {
        Self {
            speakers: speakers
                .into_iter()
                .map(|speaker| (speaker.id.clone(), speaker))
                .collect(),
            groups: groups
                .into_iter()
                .map(|group| (group.id.clone(), group))
                .collect(),
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
                groups.insert(group.id.clone(), group);
            }
            if !groups.is_empty() {
                break;
            }
        }
    }

    SonosSnapshot { speakers, groups }
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
) -> Vec<SonosGroupSnapshot> {
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

        groups.push(SonosGroupSnapshot {
            id: group.id,
            display_name: group.display_name,
            available: true,
            live,
        });
    }

    groups
}

#[derive(Debug, Clone)]
struct SonosGroupTopology {
    id: String,
    display_name: String,
    coordinator_id: Option<String>,
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
            let member_names = parse_group_member_names(body, speakers);
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
            });
        }

        let consumed = body_start + close_start + "</ZoneGroup>".len();
        remaining = &group_start[consumed..];
    }

    groups
}

fn parse_group_member_names(
    xml: &str,
    speakers: &BTreeMap<String, SonosSpeakerSnapshot>,
) -> Vec<String> {
    let mut names = Vec::new();
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

        if let Some(name) = name {
            if !name.trim().is_empty() {
                names.push(name);
            }
        }

        remaining = &member_start[tag_end + 1..];
    }

    names
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
    fn transient_empty_refresh_keeps_previous_targets_until_expiry() {
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
            .expect("populated snapshot should be published");

        assert!(refresh_tracker
            .publishable_snapshot(SonosSnapshot::empty())
            .is_none());
        assert_eq!(published.to_targets_response().speakers.len(), 1);

        for _ in 1..EMPTY_DISCOVERY_REFRESHES_BEFORE_EXPIRY {
            if let Some(snapshot) = refresh_tracker.publishable_snapshot(SonosSnapshot::empty()) {
                published = snapshot;
            }
        }

        assert!(published.to_targets_response().speakers.is_empty());
        assert!(published.to_targets_response().groups.is_empty());
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
        assert_eq!(groups[0].live.volume_percent, Some(77));
        assert_eq!(groups[0].live.muted, Some(true));
        assert_eq!(groups[0].live.transport_state(), Some(SonosTransportState::Paused));
        assert_ne!(
            groups[0].live.volume_percent,
            speakers["speaker-1"].live.volume_percent
        );
        assert_ne!(groups[0].live.muted, speakers["speaker-1"].live.muted);

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
