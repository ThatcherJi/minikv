use std::{
    collections::HashMap,
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    body::Bytes,
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::ring::HashRing;

#[derive(Debug, Clone)]
pub struct VolumeInfo {
    pub addr: String,
    pub healthy: bool,
    pub last_seen: Instant,
}

#[derive(Debug)]
pub struct CoordState {
    pub registry: HashMap<String, VolumeInfo>,
    pub ring: HashRing,
    pub replicas: usize,
    pub write_quorum: usize,
    pub dead_after: Duration,
}

pub type SharedCoordState = Arc<Mutex<CoordState>>;

#[derive(Clone)]
struct AppState {
    inner: SharedCoordState,
    client: Client,
}

#[derive(Debug, Deserialize)]
pub struct RegisterRequest {
    pub volume_id: String,
    pub addr: String,
}

#[derive(Debug, Deserialize)]
pub struct HeartbeatRequest {
    pub volume_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetaVolume {
    pub volume_id: String,
    pub addr: String,
}

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub replicas: usize,
    pub vnodes: usize,
    pub volumes: Vec<StatusVolume>,
}

#[derive(Debug, Serialize)]
pub struct StatusVolume {
    pub volume_id: String,
    pub addr: String,
    pub healthy: bool,
    pub last_seen_secs_ago: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct RouteTarget {
    pub volume_id: String,
    pub addr: String,
    pub healthy: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct RouteSnapshot {
    pub key: String,
    pub replicas: usize,
    pub write_quorum: usize,
    pub targets: Vec<RouteTarget>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ClusterSummary {
    pub replicas: usize,
    pub write_quorum: usize,
    pub vnodes: usize,
    pub total_volumes: usize,
    pub healthy_volumes: usize,
    pub unhealthy_volumes: usize,
}

#[derive(Debug, Serialize)]
pub struct ClusterResponse {
    pub summary: ClusterSummary,
    pub volumes: Vec<StatusVolume>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VolumeStatsTarget {
    pub volume_id: String,
    pub addr: String,
}

#[derive(Debug, Serialize)]
pub struct VolumeStatsEntry {
    pub volume_id: String,
    pub addr: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stats: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct VolumeStatsResponse {
    pub volumes: Vec<VolumeStatsEntry>,
}

impl CoordState {
    pub fn new(replicas: usize, vnodes: usize, dead_after: Duration) -> Self {
        Self {
            registry: HashMap::new(),
            ring: HashRing::new(vnodes),
            replicas,
            write_quorum: replicas,
            dead_after,
        }
    }

    pub fn load_volume(&mut self, volume_id: String, addr: String) {
        self.registry.insert(
            volume_id.clone(),
            VolumeInfo {
                addr,
                healthy: true,
                last_seen: Instant::now(),
            },
        );
        self.ring.add_volume(&volume_id);
    }

    fn register(&mut self, req: RegisterRequest) {
        self.registry.insert(
            req.volume_id.clone(),
            VolumeInfo {
                addr: req.addr,
                healthy: true,
                last_seen: Instant::now(),
            },
        );
        self.ring.add_volume(&req.volume_id);
    }

    pub fn register_for_test(&mut self, req: RegisterRequest) {
        self.register(req);
    }

    fn heartbeat(&mut self, volume_id: &str) -> bool {
        let Some(info) = self.registry.get_mut(volume_id) else {
            return false;
        };

        info.last_seen = Instant::now();
        if !info.healthy {
            info.healthy = true;
            self.ring.add_volume(volume_id);
        }
        true
    }

    fn status(&self) -> StatusResponse {
        let mut volumes: Vec<_> = self
            .registry
            .iter()
            .map(|(volume_id, info)| StatusVolume {
                volume_id: volume_id.clone(),
                addr: info.addr.clone(),
                healthy: info.healthy,
                last_seen_secs_ago: info.last_seen.elapsed().as_secs(),
            })
            .collect();
        volumes.sort_by(|a, b| a.volume_id.cmp(&b.volume_id));

        StatusResponse {
            replicas: self.replicas,
            vnodes: self.ring.vnodes,
            volumes,
        }
    }

    pub fn cluster_summary(&self) -> ClusterSummary {
        let healthy_volumes = self.registry.values().filter(|info| info.healthy).count();
        let total_volumes = self.registry.len();

        ClusterSummary {
            replicas: self.replicas,
            write_quorum: self.write_quorum,
            vnodes: self.ring.vnodes,
            total_volumes,
            healthy_volumes,
            unhealthy_volumes: total_volumes.saturating_sub(healthy_volumes),
        }
    }

    pub fn cluster_response(&self) -> ClusterResponse {
        ClusterResponse {
            summary: self.cluster_summary(),
            volumes: self.status().volumes,
        }
    }

    fn meta_snapshot(&self) -> Vec<MetaVolume> {
        let mut volumes: Vec<_> = self
            .registry
            .iter()
            .map(|(volume_id, info)| MetaVolume {
                volume_id: volume_id.clone(),
                addr: info.addr.clone(),
            })
            .collect();
        volumes.sort_by(|a, b| a.volume_id.cmp(&b.volume_id));
        volumes
    }

    fn route_targets(&self, key: &str) -> (Vec<String>, usize) {
        let addrs = self
            .ring
            .replicas_for(key, self.replicas)
            .into_iter()
            .filter_map(|volume_id| {
                self.registry.get(&volume_id).and_then(|info| {
                    if info.healthy {
                        Some(info.addr.clone())
                    } else {
                        None
                    }
                })
            })
            .collect();
        (addrs, self.write_quorum)
    }

    pub fn route_snapshot(&self, key: &str) -> RouteSnapshot {
        let targets = self
            .ring
            .replicas_for(key, self.replicas)
            .into_iter()
            .filter_map(|volume_id| {
                self.registry.get(&volume_id).map(|info| RouteTarget {
                    volume_id,
                    addr: info.addr.clone(),
                    healthy: info.healthy,
                })
            })
            .collect();

        RouteSnapshot {
            key: key.to_string(),
            replicas: self.replicas,
            write_quorum: self.write_quorum,
            targets,
        }
    }

    pub fn healthy_volume_stats_targets(&self) -> Vec<VolumeStatsTarget> {
        let mut targets: Vec<_> = self
            .registry
            .iter()
            .filter_map(|(volume_id, info)| {
                if info.healthy {
                    Some(VolumeStatsTarget {
                        volume_id: volume_id.clone(),
                        addr: info.addr.clone(),
                    })
                } else {
                    None
                }
            })
            .collect();
        targets.sort_by(|a, b| a.volume_id.cmp(&b.volume_id));
        targets
    }

    fn reap_dead(&mut self) {
        let now = Instant::now();
        let dead_after = self.dead_after;
        let dead_ids: Vec<String> = self
            .registry
            .iter_mut()
            .filter_map(|(volume_id, info)| {
                if info.healthy && now.duration_since(info.last_seen) > dead_after {
                    info.healthy = false;
                    Some(volume_id.clone())
                } else {
                    None
                }
            })
            .collect();

        for volume_id in dead_ids {
            debug!(%volume_id, "marking volume unhealthy");
            self.ring.remove_volume(&volume_id);
        }
    }

    pub fn reap_dead_for_test(&mut self) {
        self.reap_dead();
    }
}

pub fn shared_state(state: CoordState) -> SharedCoordState {
    Arc::new(Mutex::new(state))
}

pub fn router(state: SharedCoordState) -> Router {
    let app_state = AppState {
        inner: state,
        client: Client::new(),
    };

    Router::new()
        .route("/register", post(register))
        .route("/heartbeat", post(heartbeat))
        .route("/status", get(status))
        .route("/admin/ring/:key", get(admin_ring))
        .route("/admin/cluster", get(admin_cluster))
        .route("/admin/volumes/stats", get(admin_volume_stats))
        .route("/kv/:key", put(put_kv).get(get_kv).delete(delete_kv))
        .with_state(app_state)
}

pub async fn load_meta(path: impl AsRef<Path>) -> crate::error::Result<Vec<MetaVolume>> {
    match tokio::fs::read(path).await {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes)?),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(err) => Err(err.into()),
    }
}

pub async fn write_meta(
    path: impl AsRef<Path>,
    volumes: &[MetaVolume],
) -> crate::error::Result<()> {
    let bytes = serde_json::to_vec_pretty(volumes)?;
    tokio::fs::write(path, bytes).await?;
    Ok(())
}

pub async fn reap_dead_and_snapshot(state: &SharedCoordState) -> Vec<MetaVolume> {
    let mut guard = state.lock().await;
    guard.reap_dead();
    guard.meta_snapshot()
}

async fn register(State(state): State<AppState>, Json(req): Json<RegisterRequest>) -> StatusCode {
    let mut guard = state.inner.lock().await;
    guard.register(req);
    StatusCode::OK
}

async fn heartbeat(State(state): State<AppState>, Json(req): Json<HeartbeatRequest>) -> StatusCode {
    let mut guard = state.inner.lock().await;
    if guard.heartbeat(&req.volume_id) {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    }
}

async fn status(State(state): State<AppState>) -> Json<StatusResponse> {
    let guard = state.inner.lock().await;
    Json(guard.status())
}

async fn admin_ring(
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
) -> Json<RouteSnapshot> {
    let guard = state.inner.lock().await;
    Json(guard.route_snapshot(&key))
}

async fn admin_cluster(State(state): State<AppState>) -> Json<ClusterResponse> {
    let guard = state.inner.lock().await;
    Json(guard.cluster_response())
}

async fn admin_volume_stats(State(state): State<AppState>) -> Json<VolumeStatsResponse> {
    let targets = {
        let guard = state.inner.lock().await;
        guard.healthy_volume_stats_targets()
    };

    let mut volumes = Vec::with_capacity(targets.len());
    for target in targets {
        let url = format!("http://{}/admin/stats", target.addr);
        let entry = match state.client.get(url).send().await {
            Ok(resp) if resp.status().is_success() => {
                match resp.json::<serde_json::Value>().await {
                    Ok(stats) => VolumeStatsEntry {
                        volume_id: target.volume_id,
                        addr: target.addr,
                        ok: true,
                        stats: Some(stats),
                        error: None,
                    },
                    Err(err) => VolumeStatsEntry {
                        volume_id: target.volume_id,
                        addr: target.addr,
                        ok: false,
                        stats: None,
                        error: Some(format!("invalid stats json: {err}")),
                    },
                }
            }
            Ok(resp) => VolumeStatsEntry {
                volume_id: target.volume_id,
                addr: target.addr,
                ok: false,
                stats: None,
                error: Some(format!("volume returned {}", resp.status())),
            },
            Err(err) => VolumeStatsEntry {
                volume_id: target.volume_id,
                addr: target.addr,
                ok: false,
                stats: None,
                error: Some(err.to_string()),
            },
        };
        volumes.push(entry);
    }

    Json(VolumeStatsResponse { volumes })
}

async fn put_kv(
    State(state): State<AppState>,
    AxumPath(key): AxumPath<String>,
    body: Bytes,
) -> StatusCode {
    let (targets, write_quorum) = {
        let guard = state.inner.lock().await;
        guard.route_targets(&key)
    };

    if targets.len() < write_quorum {
        return StatusCode::SERVICE_UNAVAILABLE;
    }

    let key_segment = encode_path_segment(&key);
    let mut successes = 0usize;
    for addr in targets {
        let url = format!("http://{addr}/local/{key_segment}");
        match state.client.put(url).body(body.clone()).send().await {
            Ok(resp) if resp.status().is_success() => successes += 1,
            Ok(resp) => warn!(status = %resp.status(), "replica put failed"),
            Err(err) => warn!(%err, "replica put request failed"),
        }
    }

    if successes >= write_quorum {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

async fn get_kv(State(state): State<AppState>, AxumPath(key): AxumPath<String>) -> Response {
    let targets = {
        let guard = state.inner.lock().await;
        let (targets, _) = guard.route_targets(&key);
        targets
    };

    let key_segment = encode_path_segment(&key);
    for addr in targets {
        let url = format!("http://{addr}/local/{key_segment}");
        match state.client.get(url).send().await {
            Ok(resp) if resp.status().is_success() => match resp.bytes().await {
                Ok(bytes) => return (StatusCode::OK, bytes).into_response(),
                Err(err) => warn!(%err, "failed to read replica get body"),
            },
            Ok(resp) => debug!(status = %resp.status(), "replica get did not return value"),
            Err(err) => warn!(%err, "replica get request failed"),
        }
    }

    StatusCode::NOT_FOUND.into_response()
}

async fn delete_kv(State(state): State<AppState>, AxumPath(key): AxumPath<String>) -> StatusCode {
    let (targets, write_quorum) = {
        let guard = state.inner.lock().await;
        guard.route_targets(&key)
    };

    if targets.len() < write_quorum {
        return StatusCode::SERVICE_UNAVAILABLE;
    }

    let key_segment = encode_path_segment(&key);
    let mut successes = 0usize;
    for addr in targets {
        let url = format!("http://{addr}/local/{key_segment}");
        match state.client.delete(url).send().await {
            Ok(resp) if resp.status().is_success() => successes += 1,
            Ok(resp) => warn!(status = %resp.status(), "replica delete failed"),
            Err(err) => warn!(%err, "replica delete request failed"),
        }
    }

    if successes >= write_quorum {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}

pub fn encode_path_segment(input: &str) -> String {
    let mut out = String::new();
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            _ => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                out.push('%');
                out.push(HEX[(byte >> 4) as usize] as char);
                out.push(HEX[(byte & 0x0F) as usize] as char);
            }
        }
    }
    out
}
