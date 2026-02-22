use rustmemodb::{
    InMemoryRuntimeForwarder, PersistEntityRuntime, RuntimeClusterMembership, RuntimeClusterNode,
    RuntimeCommandEnvelope, RuntimeCommandPayloadSchema, RuntimeOperationalPolicy,
    RuntimePayloadType, RuntimeShardRoutingTable, stable_shard_for,
};
use serde_json::json;
use std::sync::Arc;
use tempfile::tempdir;
use tokio::sync::Mutex;

fn count_from_state(state: &rustmemodb::PersistState) -> i64 {
    state
        .fields
        .as_object()
        .and_then(|fields| fields.get("count"))
        .and_then(|v| v.as_i64())
        .unwrap_or_default()
}

fn register_counter_increment(runtime: &mut PersistEntityRuntime) {
    runtime.register_deterministic_command_with_schema(
        "Counter",
        "increment",
        RuntimeCommandPayloadSchema::object()
            .require_field("delta", RuntimePayloadType::Integer)
            .allow_extra_fields(false),
        Arc::new(|state, payload| {
            let delta = payload.get("delta").and_then(|v| v.as_i64()).unwrap_or(0);
            let fields = state.fields_object_mut()?;
            let current = fields
                .get("count")
                .and_then(|v| v.as_i64())
                .unwrap_or_default();
            fields.insert("count".to_string(), json!(current + delta));
            state.metadata.version = state.metadata.version.saturating_add(1);
            Ok(())
        }),
    );
}

async fn bootstrap_counter_replica_dirs() -> (
    tempfile::TempDir,
    tempfile::TempDir,
    tempfile::TempDir,
    String,
) {
    let dir_a = tempdir().unwrap();
    let dir_b = tempdir().unwrap();
    let dir_c = tempdir().unwrap();

    let mut bootstrap_policy = RuntimeOperationalPolicy::default();
    bootstrap_policy
        .replication
        .replica_roots
        .push(dir_b.path().to_path_buf());
    bootstrap_policy
        .replication
        .replica_roots
        .push(dir_c.path().to_path_buf());

    let id = {
        let mut bootstrap = PersistEntityRuntime::open(dir_a.path(), bootstrap_policy)
            .await
            .unwrap();
        let id = bootstrap
            .create_entity("Counter", "counter_state", json!({"count": 0}), 1)
            .await
            .unwrap();
        bootstrap.force_snapshot_and_compact().await.unwrap();
        id
    };

    (dir_a, dir_b, dir_c, id)
}

#[test]
fn shard_routing_is_stable_and_uses_leader_map() {
    let mut table = RuntimeShardRoutingTable::new(8, "node-a").unwrap();
    table.set_shard_leader(3, "node-b", 5).unwrap();

    let shard = stable_shard_for("User", "user-42", 8);
    let route_1 = table.route_for("User", "user-42", "node-a");
    let route_2 = table.route_for("User", "user-42", "node-a");
    assert_eq!(route_1.shard_id, shard);
    assert_eq!(route_1, route_2);

    let forced = table.route_for("Forced", "forced", "node-a");
    if forced.shard_id == 3 {
        assert_eq!(forced.leader_node_id, "node-b");
        assert_eq!(forced.leader_epoch, 5);
    } else {
        assert_eq!(forced.leader_node_id, "node-a");
        assert_eq!(forced.leader_epoch, 1);
    }
}

#[test]
fn shard_routing_tracks_followers_and_quorum() {
    let mut table = RuntimeShardRoutingTable::new(2, "node-a").unwrap();
    table.set_shard_leader(0, "node-a", 2).unwrap();
    table
        .set_shard_followers(0, vec!["node-b".to_string(), "node-c".to_string()])
        .unwrap();
    table.set_shard_quorum(0, 2).unwrap();
    table.validate().unwrap();

    assert_eq!(table.write_quorum_for_shard(0), 2);
    assert_eq!(
        table.replica_nodes_for_shard(0),
        vec![
            "node-a".to_string(),
            "node-b".to_string(),
            "node-c".to_string()
        ]
    );
}

#[test]
fn shard_membership_and_leader_movement_primitives_work() {
    let mut membership = RuntimeClusterMembership::new(vec!["node-a".to_string()]).unwrap();
    membership.add_node("node-b").unwrap();
    membership.add_node("node-c").unwrap();
    assert!(membership.contains("node-b"));
    assert!(membership.remove_node("node-c"));
    assert!(!membership.contains("node-c"));

    let mut table = RuntimeShardRoutingTable::new(1, "node-a").unwrap();
    table.set_shard_leader(0, "node-a", 5).unwrap();
    table
        .set_shard_followers(0, vec!["node-b".to_string()])
        .unwrap();
    table.set_shard_quorum(0, 2).unwrap();

    let movement = table
        .move_shard_leader(0, "node-b", Some(&membership))
        .unwrap();
    assert_eq!(movement.previous_leader.node_id, "node-a");
    assert_eq!(movement.next_leader.node_id, "node-b");
    assert_eq!(movement.next_leader.epoch, 6);
    assert!(movement.followers.contains(&"node-a".to_string()));

    let route = table.route_for("Counter", "id-1", "node-b");
    assert!(route.local_is_leader);
    assert_eq!(route.leader_epoch, 6);
}

#[test]
fn shard_leader_movement_rejects_unknown_members() {
    let membership = RuntimeClusterMembership::new(vec!["node-a".to_string()]).unwrap();
    let mut table = RuntimeShardRoutingTable::new(1, "node-a").unwrap();
    table.set_shard_leader(0, "node-a", 1).unwrap();

    let err = table
        .move_shard_leader(0, "node-z", Some(&membership))
        .unwrap_err();
    assert!(
        err.to_string().contains("not part of cluster membership"),
        "unexpected error: {}",
        err
    );
}

#[tokio::test]
async fn cluster_node_forwards_to_remote_leader() {
    let dir_local = tempdir().unwrap();
    let dir_remote = tempdir().unwrap();

    let policy = RuntimeOperationalPolicy::default();
    let local_runtime = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir_local.path(), policy.clone())
            .await
            .unwrap(),
    ));
    let remote_runtime = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir_remote.path(), policy)
            .await
            .unwrap(),
    ));

    for runtime in [&local_runtime, &remote_runtime] {
        let mut guard = runtime.lock().await;
        register_counter_increment(&mut guard);
    }

    let remote_id = {
        let mut guard = remote_runtime.lock().await;
        guard
            .create_entity("Counter", "counter_state", json!({"count": 1}), 1)
            .await
            .unwrap()
    };

    let mut routing = RuntimeShardRoutingTable::new(1, "node-local").unwrap();
    routing.set_shard_leader(0, "node-remote", 1).unwrap();

    let forwarder = InMemoryRuntimeForwarder::new();
    forwarder
        .register_peer("node-remote", remote_runtime.clone())
        .await
        .unwrap();

    let cluster_node = RuntimeClusterNode::new("node-local", routing, Arc::new(forwarder)).unwrap();

    let envelope =
        RuntimeCommandEnvelope::new("Counter", &remote_id, "increment", json!({ "delta": 3 }))
            .with_expected_version(1);

    let applied = {
        let mut local_guard = local_runtime.lock().await;
        cluster_node
            .apply_command_envelope(&mut local_guard, envelope)
            .await
            .unwrap()
    };
    assert_eq!(count_from_state(&applied.state), 4);

    let remote_state = {
        let mut remote_guard = remote_runtime.lock().await;
        remote_guard.get_state("Counter", &remote_id).unwrap()
    };
    assert_eq!(count_from_state(&remote_state), 4);

    let local_missing = {
        let mut local_guard = local_runtime.lock().await;
        local_guard.get_state("Counter", &remote_id)
    };
    assert!(local_missing.is_err());
}

#[tokio::test]
async fn cluster_leader_replication_reaches_quorum() {
    let (dir_a, dir_b, dir_c, entity_id) = bootstrap_counter_replica_dirs().await;

    let runtime_a = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir_a.path(), RuntimeOperationalPolicy::default())
            .await
            .unwrap(),
    ));
    let runtime_b = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir_b.path(), RuntimeOperationalPolicy::default())
            .await
            .unwrap(),
    ));
    let runtime_c = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir_c.path(), RuntimeOperationalPolicy::default())
            .await
            .unwrap(),
    ));

    for runtime in [&runtime_a, &runtime_b, &runtime_c] {
        let mut guard = runtime.lock().await;
        register_counter_increment(&mut guard);
    }

    let mut leader_routing = RuntimeShardRoutingTable::new(1, "node-a").unwrap();
    leader_routing.set_shard_leader(0, "node-a", 2).unwrap();
    leader_routing
        .set_shard_followers(0, vec!["node-b".to_string(), "node-c".to_string()])
        .unwrap();
    leader_routing.set_shard_quorum(0, 2).unwrap();

    let mut follower_routing = RuntimeShardRoutingTable::new(1, "node-a").unwrap();
    follower_routing.set_shard_leader(0, "node-a", 2).unwrap();

    let forwarder = InMemoryRuntimeForwarder::new();
    forwarder
        .register_peer_with_routing("node-b", runtime_b.clone(), Some(follower_routing.clone()))
        .await
        .unwrap();
    forwarder
        .register_peer_with_routing("node-c", runtime_c.clone(), Some(follower_routing))
        .await
        .unwrap();

    let cluster_node =
        RuntimeClusterNode::new("node-a", leader_routing, Arc::new(forwarder)).unwrap();

    let envelope =
        RuntimeCommandEnvelope::new("Counter", &entity_id, "increment", json!({ "delta": 5 }))
            .with_expected_version(1);

    let applied = {
        let mut leader_guard = runtime_a.lock().await;
        cluster_node
            .apply_command_envelope_with_cluster(&mut leader_guard, envelope)
            .await
            .unwrap()
    };
    assert!(!applied.forwarded);
    assert!(applied.quorum.quorum_met());
    assert!(applied.quorum.acknowledged_nodes.len() >= 2);
    assert_eq!(count_from_state(&applied.apply_result.state), 5);

    for runtime in [&runtime_a, &runtime_b, &runtime_c] {
        let state = {
            let mut guard = runtime.lock().await;
            guard.get_state("Counter", &entity_id).unwrap()
        };
        assert_eq!(count_from_state(&state), 5);
    }
}

#[tokio::test]
async fn cluster_stale_leader_is_fenced_by_new_epoch() {
    let (dir_a, dir_b, dir_c, entity_id) = bootstrap_counter_replica_dirs().await;

    let runtime_a = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir_a.path(), RuntimeOperationalPolicy::default())
            .await
            .unwrap(),
    ));
    let runtime_b = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir_b.path(), RuntimeOperationalPolicy::default())
            .await
            .unwrap(),
    ));
    let runtime_c = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir_c.path(), RuntimeOperationalPolicy::default())
            .await
            .unwrap(),
    ));

    for runtime in [&runtime_a, &runtime_b, &runtime_c] {
        let mut guard = runtime.lock().await;
        register_counter_increment(&mut guard);
    }

    let mut stale_routing = RuntimeShardRoutingTable::new(1, "node-a").unwrap();
    stale_routing.set_shard_leader(0, "node-a", 1).unwrap();
    stale_routing
        .set_shard_followers(0, vec!["node-b".to_string(), "node-c".to_string()])
        .unwrap();
    stale_routing.set_shard_quorum(0, 2).unwrap();

    let mut fresh_peer_routing = RuntimeShardRoutingTable::new(1, "node-b").unwrap();
    fresh_peer_routing.set_shard_leader(0, "node-b", 2).unwrap();

    let forwarder = InMemoryRuntimeForwarder::new();
    forwarder
        .register_peer_with_routing(
            "node-b",
            runtime_b.clone(),
            Some(fresh_peer_routing.clone()),
        )
        .await
        .unwrap();
    forwarder
        .register_peer_with_routing("node-c", runtime_c.clone(), Some(fresh_peer_routing))
        .await
        .unwrap();

    let stale_node = RuntimeClusterNode::new("node-a", stale_routing, Arc::new(forwarder)).unwrap();

    let rejected = {
        let mut stale_leader_guard = runtime_a.lock().await;
        stale_node
            .apply_command_envelope(
                &mut stale_leader_guard,
                RuntimeCommandEnvelope::new(
                    "Counter",
                    &entity_id,
                    "increment",
                    json!({ "delta": 1 }),
                )
                .with_expected_version(1),
            )
            .await
    };
    assert!(rejected.is_err());

    for runtime in [&runtime_a, &runtime_b, &runtime_c] {
        let state = {
            let mut guard = runtime.lock().await;
            guard.get_state("Counter", &entity_id).unwrap()
        };
        assert_eq!(count_from_state(&state), 0);
    }
}

#[tokio::test]
async fn cluster_quorum_preflight_rejects_when_insufficient_replicas_online() {
    let (dir_a, dir_b, _dir_c, entity_id) = bootstrap_counter_replica_dirs().await;

    let runtime_a = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir_a.path(), RuntimeOperationalPolicy::default())
            .await
            .unwrap(),
    ));
    let runtime_b = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir_b.path(), RuntimeOperationalPolicy::default())
            .await
            .unwrap(),
    ));

    for runtime in [&runtime_a, &runtime_b] {
        let mut guard = runtime.lock().await;
        register_counter_increment(&mut guard);
    }

    let mut routing = RuntimeShardRoutingTable::new(1, "node-a").unwrap();
    routing.set_shard_leader(0, "node-a", 3).unwrap();
    routing
        .set_shard_followers(0, vec!["node-b".to_string(), "node-c".to_string()])
        .unwrap();
    routing.set_shard_quorum(0, 3).unwrap();

    let mut peer_routing = RuntimeShardRoutingTable::new(1, "node-a").unwrap();
    peer_routing.set_shard_leader(0, "node-a", 3).unwrap();

    let forwarder = InMemoryRuntimeForwarder::new();
    forwarder
        .register_peer_with_routing("node-b", runtime_b.clone(), Some(peer_routing))
        .await
        .unwrap();
    // node-c intentionally not registered

    let cluster_node = RuntimeClusterNode::new("node-a", routing, Arc::new(forwarder)).unwrap();

    let rejected = {
        let mut leader_guard = runtime_a.lock().await;
        cluster_node
            .apply_command_envelope(
                &mut leader_guard,
                RuntimeCommandEnvelope::new(
                    "Counter",
                    &entity_id,
                    "increment",
                    json!({ "delta": 2 }),
                )
                .with_expected_version(1),
            )
            .await
    };
    assert!(rejected.is_err());

    let leader_state = {
        let mut guard = runtime_a.lock().await;
        guard.get_state("Counter", &entity_id).unwrap()
    };
    assert_eq!(count_from_state(&leader_state), 0);
}

#[tokio::test]
async fn cluster_failover_leader_movement_preserves_writes_and_fences_old_leader() {
    let (dir_a, dir_b, dir_c, entity_id) = bootstrap_counter_replica_dirs().await;

    let runtime_a = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir_a.path(), RuntimeOperationalPolicy::default())
            .await
            .unwrap(),
    ));
    let runtime_b = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir_b.path(), RuntimeOperationalPolicy::default())
            .await
            .unwrap(),
    ));
    let runtime_c = Arc::new(Mutex::new(
        PersistEntityRuntime::open(dir_c.path(), RuntimeOperationalPolicy::default())
            .await
            .unwrap(),
    ));

    for runtime in [&runtime_a, &runtime_b, &runtime_c] {
        let mut guard = runtime.lock().await;
        register_counter_increment(&mut guard);
    }

    let mut routing_epoch1 = RuntimeShardRoutingTable::new(1, "node-a").unwrap();
    routing_epoch1.set_shard_leader(0, "node-a", 1).unwrap();
    routing_epoch1
        .set_shard_followers(0, vec!["node-b".to_string(), "node-c".to_string()])
        .unwrap();
    routing_epoch1.set_shard_quorum(0, 2).unwrap();

    let forwarder_a = InMemoryRuntimeForwarder::new();
    forwarder_a
        .register_peer_with_routing("node-b", runtime_b.clone(), Some(routing_epoch1.clone()))
        .await
        .unwrap();
    forwarder_a
        .register_peer_with_routing("node-c", runtime_c.clone(), Some(routing_epoch1.clone()))
        .await
        .unwrap();

    let node_a = RuntimeClusterNode::new(
        "node-a",
        routing_epoch1.clone(),
        Arc::new(forwarder_a.clone()),
    )
    .unwrap();

    {
        let mut guard = runtime_a.lock().await;
        node_a
            .apply_command_envelope(
                &mut guard,
                RuntimeCommandEnvelope::new(
                    "Counter",
                    &entity_id,
                    "increment",
                    json!({ "delta": 2 }),
                )
                .with_expected_version(1),
            )
            .await
            .unwrap();
    }

    let mut routing_epoch2 = routing_epoch1.clone();
    let membership = RuntimeClusterMembership::new(vec![
        "node-a".to_string(),
        "node-b".to_string(),
        "node-c".to_string(),
    ])
    .unwrap();
    let movement = routing_epoch2
        .move_shard_leader(0, "node-b", Some(&membership))
        .unwrap();
    assert_eq!(movement.next_leader.node_id, "node-b");
    assert_eq!(movement.next_leader.epoch, 2);

    forwarder_a
        .update_peer_routing_table("node-b", Some(routing_epoch2.clone()))
        .await
        .unwrap();
    forwarder_a
        .update_peer_routing_table("node-c", Some(routing_epoch2.clone()))
        .await
        .unwrap();

    let forwarder_b = InMemoryRuntimeForwarder::new();
    forwarder_b
        .register_peer_with_routing("node-a", runtime_a.clone(), Some(routing_epoch2.clone()))
        .await
        .unwrap();
    forwarder_b
        .register_peer_with_routing("node-c", runtime_c.clone(), Some(routing_epoch2.clone()))
        .await
        .unwrap();

    let node_b =
        RuntimeClusterNode::new("node-b", routing_epoch2.clone(), Arc::new(forwarder_b)).unwrap();
    {
        let mut guard = runtime_b.lock().await;
        node_b
            .apply_command_envelope(
                &mut guard,
                RuntimeCommandEnvelope::new(
                    "Counter",
                    &entity_id,
                    "increment",
                    json!({ "delta": 5 }),
                )
                .with_expected_version(2),
            )
            .await
            .unwrap();
    }

    for runtime in [&runtime_a, &runtime_b, &runtime_c] {
        let state = {
            let mut guard = runtime.lock().await;
            guard.get_state("Counter", &entity_id).unwrap()
        };
        assert_eq!(count_from_state(&state), 7);
    }

    let stale_err = {
        let mut guard = runtime_a.lock().await;
        node_a
            .apply_command_envelope(
                &mut guard,
                RuntimeCommandEnvelope::new(
                    "Counter",
                    &entity_id,
                    "increment",
                    json!({ "delta": 1 }),
                )
                .with_expected_version(3),
            )
            .await
            .unwrap_err()
    };
    assert!(
        stale_err.to_string().contains("Epoch fence rejected"),
        "unexpected stale leader error: {}",
        stale_err
    );
}
