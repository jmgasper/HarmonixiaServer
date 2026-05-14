use std::{convert::Infallible, time::Duration};

use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
    routing::get,
    Router,
};
use chrono::Utc;
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    Stream, StreamExt,
};
use uuid::Uuid;

use crate::{
    auth::AuthenticatedUser,
    error::ErrorResponse,
    state::{AppEvent, AppEventAudience, AppState, RecoveryScreenPatch, ScreenPatch, ScreenSurface},
};

/// Builds the router for authenticated runtime event streaming.
pub fn router() -> Router<AppState> {
    Router::new().route("/", get(stream_events))
}

#[utoipa::path(
    get,
    path = "/api/v1/events",
    tag = "events",
    security(("basicAuth" = [])),
    responses(
        (status = 200, description = "Server-Sent Event stream. Each data frame is one flat screen patch envelope with surface, revision, snapshot_at, and an authoritative typed patch payload. Lagged streams receive a recovery marker and clients must refetch targeted surfaces.", content_type = "text/event-stream", body = AppEvent),
        (status = 401, description = "Authentication required", body = ErrorResponse)
    )
)]
/// Streams server events as Server-Sent Events for clients that need live refreshes.
pub async fn stream_events(
    State(state): State<AppState>,
    AuthenticatedUser(account): AuthenticatedUser,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let account_id = account.id;
    let stream_epoch = state.event_stream_epoch();
    let stream = BroadcastStream::new(state.subscribe_events()).filter_map(move |message| {
        let event = match message {
            Ok(event) if event.visible_to(account_id) => Some(sse_event(event)),
            Ok(_) => None,
            Err(BroadcastStreamRecvError::Lagged(_skipped)) => {
                Some(sse_event(lagged_event(stream_epoch)))
            }
        };
        event.map(Ok::<Event, Infallible>)
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    )
}

fn sse_event(event: AppEvent) -> Event {
    let event_name = event.event.clone();
    let stream_epoch = event.stream_epoch;
    let event_id = format!("{stream_epoch}:{}", event.sequence);
    let data = serde_json::to_string(&event).unwrap_or_else(|_| {
        serde_json::to_string(&lagged_event(stream_epoch)).unwrap_or_else(|_| "{}".to_string())
    });
    Event::default().event(event_name).id(event_id).data(data)
}

fn lagged_event(stream_epoch: Uuid) -> AppEvent {
    let timestamp = Utc::now();
    AppEvent {
        stream_epoch,
        sequence: 0,
        surface: ScreenSurface::Recovery,
        revision: 0,
        snapshot_at: timestamp,
        patch: ScreenPatch::RecoveryRequested(RecoveryScreenPatch {
            reason: "stream_lagged".to_string(),
        }),
        event: "library_updated".to_string(),
        resource: "library".to_string(),
        action: "updated".to_string(),
        entity_id: Option::<Uuid>::None,
        timestamp,
        audience: AppEventAudience::All,
    }
}
