use std::time::Duration;

use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use minikv_lite::coord::{self, CoordState, RegisterRequest};
use serde_json::Value;
use tower::ServiceExt;

fn request(method: &str, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

async fn body_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn shared_registered_state() -> coord::SharedCoordState {
    let mut state = CoordState::new(2, 16, Duration::from_secs(30));
    state.register_for_test(RegisterRequest {
        volume_id: "v1".to_string(),
        addr: "127.0.0.1:7001".to_string(),
    });
    state.register_for_test(RegisterRequest {
        volume_id: "v2".to_string(),
        addr: "127.0.0.1:7002".to_string(),
    });
    coord::shared_state(state)
}

#[tokio::test]
async fn coord_admin_cluster_reports_summary_and_sorted_volumes() {
    let app = coord::router(shared_registered_state());

    let response = app.oneshot(request("GET", "/admin/cluster")).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = body_json(response).await;
    assert_eq!(json["summary"]["replicas"], 2);
    assert_eq!(json["summary"]["healthy_volumes"], 2);
    assert_eq!(json["summary"]["unhealthy_volumes"], 0);
    assert_eq!(json["volumes"][0]["volume_id"], "v1");
    assert_eq!(json["volumes"][1]["volume_id"], "v2");
}

#[tokio::test]
async fn coord_admin_ring_reports_targets_for_key() {
    let app = coord::router(shared_registered_state());

    let response = app
        .oneshot(request("GET", "/admin/ring/alpha"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = body_json(response).await;
    assert_eq!(json["key"], "alpha");
    assert_eq!(json["replicas"], 2);
    assert_eq!(json["write_quorum"], 2);
    assert_eq!(json["targets"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn coord_admin_volume_stats_returns_empty_list_without_volumes() {
    let state = coord::shared_state(CoordState::new(2, 16, Duration::from_secs(30)));
    let app = coord::router(state);

    let response = app
        .oneshot(request("GET", "/admin/volumes/stats"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = body_json(response).await;
    assert_eq!(json["volumes"], serde_json::json!([]));
}
