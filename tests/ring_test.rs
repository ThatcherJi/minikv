use std::collections::HashSet;

use minikv_lite::ring::HashRing;

#[test]
fn replicas_for_same_key_is_stable() {
    let mut ring = HashRing::new(64);
    ring.add_volume("v1");
    ring.add_volume("v2");
    ring.add_volume("v3");

    let first = ring.replicas_for("alpha", 2);
    let second = ring.replicas_for("alpha", 2);

    assert_eq!(first, second);
}

#[test]
fn replicas_for_returns_distinct_volume_ids() {
    let mut ring = HashRing::new(64);
    ring.add_volume("v1");
    ring.add_volume("v2");
    ring.add_volume("v3");

    let replicas = ring.replicas_for("alpha", 3);
    let distinct: HashSet<_> = replicas.iter().collect();

    assert_eq!(replicas.len(), distinct.len());
}

#[test]
fn replicas_for_returns_requested_count_when_enough_nodes_exist() {
    let mut ring = HashRing::new(64);
    ring.add_volume("v1");
    ring.add_volume("v2");
    ring.add_volume("v3");

    let replicas = ring.replicas_for("alpha", 2);

    assert_eq!(replicas.len(), 2);
}
