use std::time::Duration;

use minikv_lite::coord::{CoordState, RegisterRequest};

#[test]
fn route_snapshot_lists_healthy_replica_addresses() {
    let mut state = CoordState::new(2, 16, Duration::from_secs(30));
    state.register_for_test(RegisterRequest {
        volume_id: "v1".to_string(),
        addr: "127.0.0.1:7001".to_string(),
    });
    state.register_for_test(RegisterRequest {
        volume_id: "v2".to_string(),
        addr: "127.0.0.1:7002".to_string(),
    });

    let route = state.route_snapshot("alpha");

    assert_eq!(route.key, "alpha");
    assert_eq!(route.replicas, 2);
    assert_eq!(route.targets.len(), 2);
    assert!(route.targets.iter().all(|target| target.healthy));
}

#[test]
fn cluster_summary_counts_healthy_and_unhealthy_volumes() {
    let mut state = CoordState::new(2, 16, Duration::from_millis(0));
    state.register_for_test(RegisterRequest {
        volume_id: "v1".to_string(),
        addr: "127.0.0.1:7001".to_string(),
    });
    state.register_for_test(RegisterRequest {
        volume_id: "v2".to_string(),
        addr: "127.0.0.1:7002".to_string(),
    });
    state.reap_dead_for_test();

    let summary = state.cluster_summary();

    assert_eq!(summary.total_volumes, 2);
    assert_eq!(summary.healthy_volumes, 0);
    assert_eq!(summary.unhealthy_volumes, 2);
    assert_eq!(summary.write_quorum, 2);
}
