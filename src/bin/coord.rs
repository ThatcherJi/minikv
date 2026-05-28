use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use clap::Parser;
use minikv_lite::coord::{self, CoordState};
use tokio::{net::TcpListener, time};
use tracing::{info, warn};

#[derive(Debug, Clone, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7000")]
    listen: String,
    #[arg(long, default_value_t = 2)]
    replicas: usize,
    #[arg(long, default_value_t = 64)]
    vnodes: usize,
    #[arg(long, default_value_t = 6)]
    dead_after_secs: u64,
    #[arg(long, default_value = "./coord-meta.json")]
    meta: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    let listen: SocketAddr = args
        .listen
        .parse()
        .with_context(|| format!("invalid listen address: {}", args.listen))?;

    let mut state = CoordState::new(
        args.replicas,
        args.vnodes,
        Duration::from_secs(args.dead_after_secs),
    );
    for meta_volume in coord::load_meta(&args.meta)
        .await
        .with_context(|| format!("failed to load meta from {}", args.meta))?
    {
        state.load_volume(meta_volume.volume_id, meta_volume.addr);
    }

    let shared = coord::shared_state(state);
    tokio::spawn(meta_loop(shared.clone(), args.meta.clone()));

    let app = coord::router(shared);
    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("failed to bind {}", args.listen))?;
    info!(listen = %args.listen, "coordinator listening");
    axum::serve(listener, app)
        .await
        .context("coordinator server failed")?;
    Ok(())
}

async fn meta_loop(state: coord::SharedCoordState, meta_path: String) {
    let mut interval = time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        let snapshot = coord::reap_dead_and_snapshot(&state).await;
        if let Err(err) = coord::write_meta(&meta_path, &snapshot).await {
            warn!(%err, "failed to write coordinator meta");
        }
    }
}
