use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result};
use clap::Parser;
use minikv_lite::{store::Store, volume};
use reqwest::{Client, StatusCode};
use serde::Serialize;
use tokio::{net::TcpListener, time};
use tracing::{info, warn};

#[derive(Debug, Clone, Parser)]
struct Args {
    #[arg(long)]
    id: String,
    #[arg(long, default_value = "127.0.0.1:7001")]
    listen: String,
    #[arg(long, default_value = "http://127.0.0.1:7000")]
    coord: String,
    #[arg(long, default_value = "./data/v1")]
    data: String,
    #[arg(long, default_value_t = 2)]
    heartbeat_secs: u64,
}

#[derive(Debug, Serialize)]
struct RegisterPayload<'a> {
    volume_id: &'a str,
    addr: &'a str,
}

#[derive(Debug, Serialize)]
struct HeartbeatPayload<'a> {
    volume_id: &'a str,
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
    let store = Store::open(&args.data)
        .with_context(|| format!("failed to open store at {}", args.data))?;
    let app = volume::router(volume::AppState::new(store));
    let client = Client::new();

    if let Err(err) = register(&client, &args).await {
        warn!(%err, "initial register failed; will retry on heartbeat loop");
    }

    tokio::spawn(heartbeat_loop(client, args.clone()));

    let listener = TcpListener::bind(listen)
        .await
        .with_context(|| format!("failed to bind {}", args.listen))?;
    info!(listen = %args.listen, "volume listening");
    axum::serve(listener, app)
        .await
        .context("volume server failed")?;
    Ok(())
}

async fn heartbeat_loop(client: Client, args: Args) {
    let mut interval = time::interval(Duration::from_secs(args.heartbeat_secs));
    loop {
        interval.tick().await;
        match heartbeat(&client, &args).await {
            Ok(HeartbeatResult::Ok) => {}
            Ok(HeartbeatResult::RegisterNeeded) => {
                if let Err(err) = register(&client, &args).await {
                    warn!(%err, "register retry failed");
                }
            }
            Err(err) => warn!(%err, "heartbeat failed"),
        }
    }
}

async fn register(client: &Client, args: &Args) -> Result<()> {
    let url = format!("{}/register", args.coord.trim_end_matches('/'));
    let payload = RegisterPayload {
        volume_id: &args.id,
        addr: &args.listen,
    };
    let resp = client
        .post(url)
        .json(&payload)
        .send()
        .await
        .context("register request failed")?;
    if resp.status().is_success() {
        Ok(())
    } else {
        anyhow::bail!("register returned {}", resp.status())
    }
}

enum HeartbeatResult {
    Ok,
    RegisterNeeded,
}

async fn heartbeat(client: &Client, args: &Args) -> Result<HeartbeatResult> {
    let url = format!("{}/heartbeat", args.coord.trim_end_matches('/'));
    let payload = HeartbeatPayload {
        volume_id: &args.id,
    };
    let resp = client
        .post(url)
        .json(&payload)
        .send()
        .await
        .context("heartbeat request failed")?;

    if resp.status().is_success() {
        Ok(HeartbeatResult::Ok)
    } else if resp.status() == StatusCode::NOT_FOUND {
        Ok(HeartbeatResult::RegisterNeeded)
    } else {
        anyhow::bail!("heartbeat returned {}", resp.status())
    }
}
