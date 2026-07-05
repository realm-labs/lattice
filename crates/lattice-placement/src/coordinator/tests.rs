use std::collections::{BTreeMap, BTreeSet};

use lattice_core::instance::InstanceCapacity;
use lattice_core::{actor_kind, service_kind};

use super::*;
use crate::instance::InstanceState;
use crate::vshard::{GradualRebalanceShardAssigner, RoundRobinShardAssigner, VirtualShardId};
use crate::{InMemoryPlacementStore, PlacementPrefix};

#[derive(Debug, Clone, Default)]
struct CountingLogicControl {
    calls: Arc<AtomicU64>,
    delay: Duration,
}

#[async_trait]
impl LogicControl for CountingLogicControl {
    async fn activate_actor(
        &self,
        _instance: &InstanceRecord,
        _key: &ActorPlacementKey,
        _epoch: Epoch,
    ) -> Result<(), PlacementError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct SelectiveShardMigrationControl {
    eligible: Arc<std::sync::Mutex<BTreeSet<VirtualShardId>>>,
    prepared: Arc<std::sync::Mutex<Vec<VirtualShardId>>>,
}

#[async_trait]
impl LogicControl for SelectiveShardMigrationControl {
    async fn activate_actor(
        &self,
        _instance: &InstanceRecord,
        _key: &ActorPlacementKey,
        _epoch: Epoch,
    ) -> Result<(), PlacementError> {
        Ok(())
    }
}

#[async_trait]
impl VirtualShardMigrationControl for SelectiveShardMigrationControl {
    async fn prepare_virtual_shard_migration(
        &self,
        _instance: &InstanceRecord,
        request: PrepareVirtualShardMigrationRequest,
    ) -> Result<VirtualShardMigrationOutcome, PlacementError> {
        self.prepared
            .lock()
            .expect("prepared shard mutex poisoned")
            .push(request.shard_id);
        let eligible = self
            .eligible
            .lock()
            .expect("eligible shard mutex poisoned")
            .contains(&request.shard_id);
        Ok(VirtualShardMigrationOutcome {
            shard_id: request.shard_id,
            eligible,
            running_actors: usize::from(!eligible),
            passivated_actors: usize::from(eligible),
        })
    }
}

#[tokio::test]
async fn coordinator_activates_missing_actor_owner_without_prewritten_logic_key() {
    let store = ready_store().await;
    let logic = CountingLogicControl::default();
    let coordinator = PlacementCoordinator::new(store.clone(), logic.clone());

    let record = coordinator
        .activate_actor(ActivateActorRequest {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(7),
        })
        .await
        .unwrap();

    assert_eq!(record.owner, InstanceId::new("world-a"));
    assert_eq!(record.epoch, Epoch(1));
    assert_eq!(logic.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn concurrent_coordinator_activation_creates_one_owner() {
    let store = ready_store().await;
    let logic = CountingLogicControl {
        calls: Arc::new(AtomicU64::new(0)),
        delay: Duration::from_millis(10),
    };
    let coordinator = Arc::new(PlacementCoordinator::new(store, logic.clone()));

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let coordinator = coordinator.clone();
        tasks.push(tokio::spawn(async move {
            coordinator
                .activate_actor(ActivateActorRequest {
                    service_kind: service_kind!("World"),
                    actor_kind: actor_kind!("World"),
                    actor_id: ActorId::U64(7),
                })
                .await
        }));
    }

    for task in tasks {
        let record = task.await.unwrap().unwrap();
        assert_eq!(record.owner, InstanceId::new("world-a"));
    }
    assert_eq!(logic.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn explicit_route_resolver_activates_missing_owner_and_uses_cache() {
    let store = ready_store().await;
    let logic = CountingLogicControl::default();
    let coordinator = PlacementCoordinator::new(store.clone(), logic.clone());
    let resolver = ExplicitRouteResolver::new(
        service_kind!("World"),
        store,
        coordinator,
        RouteCacheConfig::default(),
    );
    let request = ResolveRequest {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        route_key: RouteKey::U64(7),
    };

    let first = resolver.resolve(request.clone()).await.unwrap();
    let second = resolver.resolve(request).await.unwrap();

    assert_eq!(first.instance_id, InstanceId::new("world-a"));
    assert_eq!(second.instance_id, InstanceId::new("world-a"));
    assert_eq!(first.owner_epoch, Some(Epoch(1)));
    assert_eq!(resolver.placement_lookups(), 1);
    assert_eq!(logic.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn placement_route_resolver_reads_existing_store_record_without_activation() {
    let store = ready_store().await;
    let key = ActorPlacementKey {
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(7),
    };
    store
        .compare_and_put_actor(
            key,
            None,
            ActorPlacementRecord {
                actor_kind: actor_kind!("World"),
                actor_id: ActorId::U64(7),
                owner: InstanceId::new("world-a"),
                epoch: Epoch(3),
                lease_id: LeaseId(10),
                state: PlacementState::Running,
            },
        )
        .await
        .unwrap();
    let logic = CountingLogicControl::default();
    let coordinator = PlacementCoordinator::new(store.clone(), logic.clone());
    let resolver = PlacementRouteResolver::new(
        service_kind!("World"),
        store,
        coordinator,
        RouteCacheConfig::default(),
    );

    let target = resolver
        .resolve(ResolveRequest {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            route_key: RouteKey::U64(7),
        })
        .await
        .unwrap();

    assert_eq!(target.instance_id, InstanceId::new("world-a"));
    assert_eq!(target.owner_epoch, Some(Epoch(3)));
    assert_eq!(resolver.placement_lookups(), 1);
    assert_eq!(logic.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn coordinator_owner_move_increments_epoch() {
    let store = ready_store().await;
    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
        .await
        .unwrap();
    let coordinator = PlacementCoordinator::new(store, NoopLogicControl);
    let key = ActorPlacementKey {
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(7),
    };
    coordinator
        .activate_actor(ActivateActorRequest {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(7),
        })
        .await
        .unwrap();

    let moved = coordinator
        .move_actor(key, InstanceId::new("world-b"))
        .await
        .unwrap();

    assert_eq!(moved.owner, InstanceId::new("world-b"));
    assert_eq!(moved.epoch, Epoch(2));
}

#[tokio::test]
async fn explicit_route_resolver_refreshes_cache_from_placement_watch() {
    let store = ready_store().await;
    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
        .await
        .unwrap();
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
    let resolver = ExplicitRouteResolver::new(
        service_kind!("World"),
        store,
        coordinator.clone(),
        RouteCacheConfig::default(),
    );
    let watch_task = resolver.watch_cache_updates().await.unwrap();
    let request = ResolveRequest {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        route_key: RouteKey::U64(7),
    };
    let key = ActorPlacementKey {
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(7),
    };

    let first = resolver.resolve(request.clone()).await.unwrap();
    let lookups_after_first_resolve = resolver.placement_lookups();
    coordinator
        .move_actor(key, InstanceId::new("world-b"))
        .await
        .unwrap();

    for _ in 0..50 {
        let refreshed = resolver.resolve(request.clone()).await.unwrap();
        if refreshed.instance_id == InstanceId::new("world-b") {
            assert_eq!(first.instance_id, InstanceId::new("world-a"));
            assert_eq!(refreshed.owner_epoch, Some(Epoch(2)));
            assert_eq!(resolver.placement_lookups(), lookups_after_first_resolve);
            watch_task.cancel();
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    watch_task.cancel();
    panic!("placement watch did not refresh route cache");
}

#[tokio::test]
async fn scale_out_ready_instance_participates_in_virtual_shard_assignment() {
    let store = ready_store().await;
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
    let request = RebalanceVirtualShardsRequest {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        shard_count: 4,
        eligible_shards: BTreeSet::new(),
        max_migrations: usize::MAX,
        movement_policy: VirtualShardMovementPolicy::EligibleOnly,
    };

    let initial = coordinator
        .rebalance_virtual_shards(request.clone(), &RoundRobinShardAssigner)
        .await
        .unwrap();
    assert_eq!(
        initial,
        RebalanceVirtualShardsReport {
            ready_instances: 1,
            assignments_written: 4,
            moved_shards: 0,
        }
    );

    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
        .await
        .unwrap();
    let after_scale_out = coordinator
        .rebalance_virtual_shards(
            RebalanceVirtualShardsRequest {
                eligible_shards: BTreeSet::from([VirtualShardId(1), VirtualShardId(3)]),
                max_migrations: 2,
                movement_policy: VirtualShardMovementPolicy::EligibleOnly,
                ..request
            },
            &GradualRebalanceShardAssigner,
        )
        .await
        .unwrap();
    let assignments = store
        .list_virtual_shards(&service_kind!("World"), &actor_kind!("World"))
        .await
        .unwrap()
        .into_iter()
        .map(|(_, record)| (record.shard_id, (record.owner, record.epoch)))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        after_scale_out,
        RebalanceVirtualShardsReport {
            ready_instances: 2,
            assignments_written: 2,
            moved_shards: 2,
        }
    );
    assert_eq!(
        assignments.get(&VirtualShardId(1)),
        Some(&(InstanceId::new("world-b"), Epoch(2)))
    );
    assert_eq!(
        assignments.get(&VirtualShardId(3)),
        Some(&(InstanceId::new("world-b"), Epoch(2)))
    );
}

#[tokio::test]
async fn ready_instance_watch_triggers_virtual_shard_assignment() {
    let store = ready_store().await;
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
    let watch_task = coordinator
        .start_virtual_shard_scale_out_watch(
            vec![RebalanceVirtualShardsRequest {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                shard_count: 4,
                eligible_shards: BTreeSet::new(),
                max_migrations: usize::MAX,
                movement_policy: VirtualShardMovementPolicy::EligibleOnly,
            }],
            GradualRebalanceShardAssigner,
        )
        .await
        .unwrap();

    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
        .await
        .unwrap();

    for _ in 0..50 {
        let assignments = store
            .list_virtual_shards(&service_kind!("World"), &actor_kind!("World"))
            .await
            .unwrap()
            .into_iter()
            .map(|(_, record)| (record.shard_id, (record.owner, record.epoch)))
            .collect::<BTreeMap<_, _>>();
        if assignments.len() == 4
            && assignments.get(&VirtualShardId(1)) == Some(&(InstanceId::new("world-b"), Epoch(1)))
            && assignments.get(&VirtualShardId(3)) == Some(&(InstanceId::new("world-b"), Epoch(1)))
        {
            watch_task.cancel();
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    watch_task.cancel();
    panic!("ready instance watch did not assign virtual shards");
}

#[tokio::test]
async fn virtual_shard_rebalance_respects_running_actor_movement_policy() {
    let store = ready_store().await;
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
    let initial = RebalanceVirtualShardsRequest {
        service_kind: service_kind!("World"),
        actor_kind: actor_kind!("World"),
        shard_count: 4,
        eligible_shards: BTreeSet::new(),
        max_migrations: usize::MAX,
        movement_policy: VirtualShardMovementPolicy::AllowRunningMigration,
    };
    coordinator
        .rebalance_virtual_shards(initial, &RoundRobinShardAssigner)
        .await
        .unwrap();
    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
        .await
        .unwrap();

    let running_guarded = coordinator
        .rebalance_virtual_shards(
            RebalanceVirtualShardsRequest {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                shard_count: 4,
                eligible_shards: BTreeSet::new(),
                max_migrations: usize::MAX,
                movement_policy: VirtualShardMovementPolicy::EligibleOnly,
            },
            &GradualRebalanceShardAssigner,
        )
        .await
        .unwrap();
    let running_allowed = coordinator
        .rebalance_virtual_shards(
            RebalanceVirtualShardsRequest {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                shard_count: 4,
                eligible_shards: BTreeSet::new(),
                max_migrations: usize::MAX,
                movement_policy: VirtualShardMovementPolicy::AllowRunningMigration,
            },
            &GradualRebalanceShardAssigner,
        )
        .await
        .unwrap();

    assert_eq!(running_guarded.moved_shards, 0);
    assert_eq!(running_allowed.moved_shards, 2);
}

#[tokio::test]
async fn prepared_virtual_shard_rebalance_moves_only_policy_eligible_shards() {
    let store = ready_store().await;
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
    coordinator
        .rebalance_virtual_shards(
            RebalanceVirtualShardsRequest {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                shard_count: 4,
                eligible_shards: BTreeSet::new(),
                max_migrations: usize::MAX,
                movement_policy: VirtualShardMovementPolicy::AllowRunningMigration,
            },
            &RoundRobinShardAssigner,
        )
        .await
        .unwrap();
    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
        .await
        .unwrap();
    let logic = SelectiveShardMigrationControl::default();
    logic
        .eligible
        .lock()
        .expect("eligible shard mutex poisoned")
        .insert(VirtualShardId(1));
    let coordinator = PlacementCoordinator::new(store.clone(), logic.clone());

    let report = coordinator
        .prepare_and_rebalance_virtual_shards(
            RebalanceVirtualShardsRequest {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                shard_count: 4,
                eligible_shards: BTreeSet::new(),
                max_migrations: usize::MAX,
                movement_policy: VirtualShardMovementPolicy::EligibleOnly,
            },
            &GradualRebalanceShardAssigner,
        )
        .await
        .unwrap();
    let assignments = store
        .list_virtual_shards(&service_kind!("World"), &actor_kind!("World"))
        .await
        .unwrap()
        .into_iter()
        .map(|(_, record)| (record.shard_id, (record.owner, record.epoch)))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(report.moved_shards, 1);
    assert_eq!(
        *logic
            .prepared
            .lock()
            .expect("prepared shard mutex poisoned"),
        vec![VirtualShardId(1), VirtualShardId(3)]
    );
    assert_eq!(
        assignments.get(&VirtualShardId(1)),
        Some(&(InstanceId::new("world-b"), Epoch(2)))
    );
    assert_eq!(
        assignments.get(&VirtualShardId(3)),
        Some(&(InstanceId::new("world-a"), Epoch(1)))
    );
}

#[tokio::test]
async fn coordinator_drain_marks_instance_draining_and_migrates_owned_actors() {
    let store = ready_store().await;
    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
        .await
        .unwrap();
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
    let key = ActorPlacementKey {
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(7),
    };
    coordinator
        .activate_actor(ActivateActorRequest {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(7),
        })
        .await
        .unwrap();
    store
        .compare_and_put_virtual_shard(
            VirtualShardPlacementKey {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                shard_id: VirtualShardId(3),
            },
            None,
            VirtualShardPlacementRecord {
                service_kind: service_kind!("World"),
                actor_kind: actor_kind!("World"),
                shard_id: VirtualShardId(3),
                owner: InstanceId::new("world-a"),
                epoch: Epoch(1),
            },
        )
        .await
        .unwrap();

    let report = coordinator
        .drain_instance(service_kind!("World"), InstanceId::new("world-a"))
        .await
        .unwrap();
    let drained = store
        .get_instance(&InstanceId::new("world-a"))
        .await
        .unwrap()
        .unwrap();
    let migrated = store.get_actor(&key).await.unwrap().unwrap().1;
    let migrated_shard = store
        .get_virtual_shard(&VirtualShardPlacementKey {
            service_kind: service_kind!("World"),
            actor_kind: actor_kind!("World"),
            shard_id: VirtualShardId(3),
        })
        .await
        .unwrap()
        .unwrap()
        .1;

    assert_eq!(
        report,
        DrainReport {
            drained_instance: InstanceId::new("world-a"),
            migrated_actors: 1,
            migrated_virtual_shards: 1,
        }
    );
    assert_eq!(drained.state, InstanceState::Draining);
    assert_eq!(migrated.owner, InstanceId::new("world-b"));
    assert_eq!(migrated.epoch, Epoch(2));
    assert_eq!(migrated_shard.owner, InstanceId::new("world-b"));
    assert_eq!(migrated_shard.epoch, Epoch(2));
}

#[tokio::test]
async fn lease_expiry_reconciler_observes_missing_instance_and_fails_over() {
    let store = ready_store().await;
    store
        .upsert_instance(instance_record("world-b", InstanceState::Ready))
        .await
        .unwrap();
    let actor_key = ActorPlacementKey {
        actor_kind: actor_kind!("World"),
        actor_id: ActorId::U64(7),
    };
    store
        .compare_and_put_actor(
            actor_key.clone(),
            None,
            ActorPlacementRecord {
                actor_kind: actor_kind!("World"),
                actor_id: ActorId::U64(7),
                owner: InstanceId::new("world-a"),
                epoch: Epoch(3),
                lease_id: LeaseId(9),
                state: PlacementState::Running,
            },
        )
        .await
        .unwrap();
    let singleton_key = SingletonKey {
        service_kind: service_kind!("World"),
        singleton_kind: actor_kind!("SeasonManager"),
        scope: "global".to_string(),
    };
    store
        .compare_and_put_singleton(
            singleton_key.clone(),
            None,
            SingletonPlacementRecord {
                service_kind: service_kind!("World"),
                singleton_kind: actor_kind!("SeasonManager"),
                scope: "global".to_string(),
                owner: InstanceId::new("world-a"),
                epoch: Epoch(5),
                lease_id: LeaseId(11),
                state: PlacementState::Running,
            },
        )
        .await
        .unwrap();
    store.remove_instance_for_test(&InstanceId::new("world-a"));
    let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
    let task = coordinator.start_all_service_lease_expiry_reconciler(Duration::from_millis(5));

    for _ in 0..50 {
        let actor = store.get_actor(&actor_key).await.unwrap().unwrap().1;
        let singleton = store
            .get_singleton(&singleton_key)
            .await
            .unwrap()
            .unwrap()
            .1;
        if actor.owner == InstanceId::new("world-b")
            && singleton.owner == InstanceId::new("world-b")
        {
            task.cancel();
            assert_eq!(actor.epoch, Epoch(4));
            assert_eq!(singleton.epoch, Epoch(6));
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    task.cancel();
    panic!("lease-expiry reconciler did not fail over expired owner");
}

async fn ready_store() -> InMemoryPlacementStore {
    let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
    store
        .upsert_instance(instance_record("world-a", InstanceState::Ready))
        .await
        .unwrap();
    store
}

fn instance_record(instance_id: &str, state: InstanceState) -> InstanceRecord {
    InstanceRecord {
        service_kind: service_kind!("World"),
        instance_id: InstanceId::new(instance_id),
        lease_id: LeaseId(1),
        advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
        control_endpoint: format!("http://{instance_id}.world:18081").parse().unwrap(),
        version: "test".to_string(),
        state,
        capacity: InstanceCapacity::default(),
        labels: BTreeMap::new(),
    }
}
