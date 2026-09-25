use std::sync::Arc;

use futures_util::StreamExt;
use lattice_config::store::{ConfigStore, LocalConfigStore};
use lattice_model::cluster::{ActorGroupId, CoordinatorScope};
use serde_json::json;

use super::{SequenceDiscovery, scope, target};
use crate::{
    aggregate::AggregateDiscovery,
    config_store::ConfigStoreDiscovery,
    provider::{CoordinatorDirectorySnapshot, CoordinatorDiscovery, validate_snapshot},
};

fn directory(hint: &str) -> CoordinatorDirectorySnapshot {
    CoordinatorDirectorySnapshot {
        scope: scope(),
        generation: 1,
        leader_hint: Some(target(hint, 7447, Some(hint), 0)),
        targets: vec![target("seed", 7447, Some("seed"), 1)],
    }
}

#[tokio::test]
async fn aggregate_preserves_hint_and_turns_conflicting_hints_into_candidates() {
    let one = Arc::new(SequenceDiscovery::new(vec![Ok(directory("one"))]));
    let aggregate = AggregateDiscovery::new(vec![one.clone()]).unwrap();
    let snapshot = aggregate.snapshots().next().await.unwrap().unwrap();
    assert_eq!(snapshot.leader_hint.unwrap().address.host(), "one");

    let two = Arc::new(SequenceDiscovery::new(vec![Ok(directory("two"))]));
    let aggregate = AggregateDiscovery::new(vec![one, two]).unwrap();
    let snapshot = aggregate.snapshots().next().await.unwrap().unwrap();
    assert!(snapshot.leader_hint.is_none());
    assert_eq!(snapshot.targets.len(), 3);
}

#[tokio::test]
async fn aggregate_emits_hint_changes_even_when_candidate_addresses_do_not_change() {
    let first = directory("one");
    let mut second = first.clone();
    second.generation = 2;
    second.targets.push(second.leader_hint.take().unwrap());
    let provider = Arc::new(SequenceDiscovery::new(vec![Ok(first), Ok(second)]));
    let aggregate = AggregateDiscovery::new(vec![provider]).unwrap();
    let snapshots = aggregate.snapshots().collect::<Vec<_>>().await;
    assert_eq!(snapshots.len(), 2);
    assert!(snapshots[0].as_ref().unwrap().leader_hint.is_some());
    assert!(snapshots[1].as_ref().unwrap().leader_hint.is_none());
}

#[test]
fn rejects_conflicting_hint_identity_or_tls_expectations() {
    let mut snapshot = directory("one");
    let mut duplicate = snapshot.leader_hint.clone().unwrap();
    snapshot.targets.push(duplicate.clone());
    validate_snapshot(&snapshot).unwrap();
    duplicate.expected_node_id = Some("different".into());
    snapshot.targets[1] = duplicate;
    assert!(validate_snapshot(&snapshot).is_err());
    snapshot.targets[1] = snapshot.leader_hint.clone().unwrap();
    snapshot.targets[1].source = snapshot.targets[1]
        .source
        .clone()
        .with_tls_server_name("other.example");
    assert!(validate_snapshot(&snapshot).is_err());
}

#[tokio::test]
async fn aggregate_rejects_cross_provider_tls_conflicts_and_wrong_scope() {
    let first = directory("one");
    let mut conflicting = first.clone();
    conflicting.leader_hint.as_mut().unwrap().source = conflicting
        .leader_hint
        .as_ref()
        .unwrap()
        .source
        .clone()
        .with_tls_server_name("other.example");
    let aggregate = AggregateDiscovery::new(vec![
        Arc::new(SequenceDiscovery::new(vec![Ok(first)])),
        Arc::new(SequenceDiscovery::new(vec![Ok(conflicting)])),
    ])
    .unwrap();
    assert!(aggregate.snapshots().next().await.unwrap().is_err());

    let mut wrong = directory("one");
    wrong.scope = CoordinatorScope::Group(ActorGroupId::new("other").unwrap());
    let aggregate =
        AggregateDiscovery::new(vec![Arc::new(SequenceDiscovery::new(vec![Ok(wrong)]))]).unwrap();
    assert!(aggregate.snapshots().next().await.unwrap().is_err());
}

#[tokio::test]
async fn config_store_publishes_leader_hint_and_can_withdraw_it() {
    let store = LocalConfigStore::default();
    store
        .put(
            "/directory".into(),
            json!({
                "schema_version": 1, "generation": 1,
                "leader": {"host": "leader", "port": 7447, "node_id": "leader"},
                "endpoints": [{"host": "seed", "port": 7447}]
            }),
        )
        .await
        .unwrap();
    let provider = ConfigStoreDiscovery::new(scope(), store.clone(), "/directory").unwrap();
    let mut events = provider.snapshots();
    let first = events.next().await.unwrap().unwrap();
    assert_eq!(first.leader_hint.unwrap().address.host(), "leader");
    assert_eq!(first.targets[0].address.host(), "seed");
    store
        .put(
            "/directory".into(),
            json!({
                "schema_version": 1, "generation": 2,
                "endpoints": [{"host": "seed", "port": 7447}]
            }),
        )
        .await
        .unwrap();
    assert!(events.next().await.unwrap().unwrap().leader_hint.is_none());
}
