use axum::{
    body::{to_bytes, Body},
    http::{Request, StatusCode},
};
use minikv_lite::{store::Store, volume};
use serde_json::Value;
use tower::ServiceExt;

fn request(method: &str, uri: &str, body: impl Into<Body>) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(body.into())
        .unwrap()
}

async fn body_json(response: axum::response::Response) -> Value {
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn volume_admin_stats_reports_store_state() {
    let dir = tempfile::tempdir().unwrap();
    let app = volume::router(volume::AppState::new(Store::open(dir.path()).unwrap()));

    let response = app
        .clone()
        .oneshot(request("PUT", "/local/a", "one"))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(request("GET", "/admin/stats", Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;

    assert_eq!(json["keys"], 1);
    assert_eq!(json["puts"], 1);
    assert_eq!(
        json["data_file_bytes"].as_u64().unwrap(),
        json["write_offset"].as_u64().unwrap()
    );
}

#[tokio::test]
async fn volume_admin_keys_are_sorted_and_compact_preserves_values() {
    let dir = tempfile::tempdir().unwrap();
    let app = volume::router(volume::AppState::new(Store::open(dir.path()).unwrap()));

    for (key, value) in [("b", "old"), ("a", "first"), ("b", "new")] {
        let response = app
            .clone()
            .oneshot(request("PUT", &format!("/local/{key}"), value))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    let response = app
        .clone()
        .oneshot(request("GET", "/admin/keys", Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let json = body_json(response).await;
    assert_eq!(json["keys"], serde_json::json!(["a", "b"]));

    let response = app
        .clone()
        .oneshot(request("POST", "/admin/compact", Body::empty()))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(request("GET", "/local/b", Body::empty()))
        .await
        .unwrap();
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert_eq!(&bytes[..], b"new");
}

#[tokio::test]
async fn volume_admin_keys_support_prefix_and_limit() {
    let dir = tempfile::tempdir().unwrap();
    let app = volume::router(volume::AppState::new(Store::open(dir.path()).unwrap()));

    for key in ["app:3", "sys:1", "app:1", "app:2"] {
        let response = app
            .clone()
            .oneshot(request("PUT", &format!("/local/{key}"), key))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }

    let response = app
        .oneshot(request(
            "GET",
            "/admin/keys?prefix=app%3A&limit=2",
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let json = body_json(response).await;
    assert_eq!(json["keys"], serde_json::json!(["app:1", "app:2"]));
}
