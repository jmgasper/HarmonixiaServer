use std::{env, net::SocketAddr};

use harmonixia_server::{
    router, AppState, BackgroundServiceConfig, BackgroundServices, ServerConfig,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
/// Starts the Harmonixia HTTP server, background services, logging, and database-backed application state.
///
/// Inputs:
/// - None.
///
/// Output:
/// - Returns `()` on success or `Box<dyn std::error::Error>` when the operation cannot be completed.
///
/// Errors:
/// - Returns `Box<dyn std::error::Error>` when validation fails, persistence or I/O fails, an external process/provider fails, or a downstream operation returns that error.
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "harmonixia_server=info,tower_http=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let bind_addr: SocketAddr = env::var("HARMONIXIA_BIND_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3000".to_string())
        .parse()?;

    let config = ServerConfig::from_env()?;
    let state = AppState::connect(config).await?;
    tracing::info!("connected to Postgres and verified maintenance migrations");
    let _background_services =
        BackgroundServices::spawn(state.clone(), BackgroundServiceConfig::default());
    let listener = tokio::net::TcpListener::bind(bind_addr).await?;

    tracing::info!(%bind_addr, "starting Harmonixia server");
    axum::serve(listener, router(state)).await?;

    Ok(())
}
