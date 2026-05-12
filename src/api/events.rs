use std::{convert::Infallible, time::Duration};

use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
    routing::get,
    Router,
};
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    Stream, StreamExt,
};

use crate::{
    auth::AuthenticatedUser,
    state::{AppEvent, AppState},
};

/// Builds the router for authenticated runtime event streaming.
pub fn router() -> Router<AppState> {
    Router::new().route("/", get(stream_events))
}

/// Streams server events as Server-Sent Events for clients that need live refreshes.
pub async fn stream_events(
    State(state): State<AppState>,
    AuthenticatedUser(_account): AuthenticatedUser,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = BroadcastStream::new(state.subscribe_events()).map(|message| {
        let event = match message {
            Ok(event) => sse_event(event),
            Err(BroadcastStreamRecvError::Lagged(_skipped)) => Event::default()
                .event("library_updated")
                .data(r#"{"event":"library_updated","resource":"library","action":"updated"}"#),
        };
        Ok::<Event, Infallible>(event)
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    )
}

fn sse_event(event: AppEvent) -> Event {
    let event_name = event.event.clone();
    let event_id = event.sequence.to_string();
    let data = serde_json::to_string(&event).unwrap_or_else(|_| {
        r#"{"event":"library_updated","resource":"library","action":"updated"}"#.to_string()
    });
    Event::default().event(event_name).id(event_id).data(data)
}
