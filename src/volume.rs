use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tracing::error;

use crate::store::{Store, StoreStats};

#[derive(Clone)]
pub struct AppState {
    store: Arc<Mutex<Store>>,
}

impl AppState {
    pub fn new(store: Store) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route(
            "/local/:key",
            put(put_local).get(get_local).delete(delete_local),
        )
        .route("/healthz", get(healthz))
        .route("/admin/stats", get(admin_stats))
        .route("/admin/keys", get(admin_keys))
        .route("/admin/compact", post(admin_compact))
        .with_state(state)
}

#[derive(Debug, Serialize)]
struct KeysResponse {
    keys: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct KeysQuery {
    prefix: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct CompactResponse {
    before_bytes: u64,
    after_bytes: u64,
    keys: usize,
}

async fn put_local(
    State(state): State<AppState>,
    Path(key): Path<String>,
    body: Bytes,
) -> StatusCode {
    let mut store = state.store.lock().await;
    match store.put(&key, &body) {
        Ok(()) => StatusCode::OK,
        Err(err) => {
            error!(%err, "failed to put local key");
            StatusCode::INTERNAL_SERVER_ERROR
        }
    }
}

async fn get_local(State(state): State<AppState>, Path(key): Path<String>) -> Response {
    let store = state.store.lock().await;
    match store.get(&key) {
        Ok(Some(value)) => (StatusCode::OK, value).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(err) => {
            error!(%err, "failed to get local key");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn delete_local(State(state): State<AppState>, Path(key): Path<String>) -> StatusCode {
    let mut store = state.store.lock().await;
    if let Err(err) = store.delete(&key) {
        error!(%err, "failed to delete local key");
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    StatusCode::OK
}

async fn healthz() -> &'static str {
    "ok"
}

async fn admin_stats(State(state): State<AppState>) -> Response {
    let store = state.store.lock().await;
    match store.stats() {
        Ok(stats) => Json(stats).into_response(),
        Err(err) => {
            error!(%err, "failed to read store stats");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn admin_keys(
    State(state): State<AppState>,
    Query(query): Query<KeysQuery>,
) -> Json<KeysResponse> {
    let store = state.store.lock().await;
    Json(KeysResponse {
        keys: store.keys_with_prefix(query.prefix.as_deref(), query.limit),
    })
}

async fn admin_compact(State(state): State<AppState>) -> Response {
    let mut store = state.store.lock().await;
    match compact_store(&mut store) {
        Ok(response) => Json(response).into_response(),
        Err(err) => {
            error!(%err, "failed to compact store");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

fn compact_store(store: &mut Store) -> std::io::Result<CompactResponse> {
    let before = store.data_file_bytes()?;
    store.compact()?;
    let stats: StoreStats = store.stats()?;
    Ok(CompactResponse {
        before_bytes: before,
        after_bytes: stats.data_file_bytes,
        keys: stats.keys,
    })
}
