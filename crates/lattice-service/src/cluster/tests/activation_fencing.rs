use std::{collections::BTreeSet, sync::atomic::AtomicUsize, time::Duration};

use async_trait::async_trait;
use lattice_actor_distributed::{
    error::ActorFailure,
    registry::{ActorCreateContext, ActorRegistryConfig},
};
use lattice_core::{
    actor_address::{
        ClusterId, EntityId, EntityType, NodeAddress, NodeIncarnation, ProtocolId, SingletonKind,
    },
    actor_kind,
};
use lattice_placement::types::{AssignmentGeneration, CoordinatorTerm, PlacementVersion, Revision};
use lattice_remoting::config::RemotingConfig;
use tokio::sync::Semaphore;

use super::support::*;
use crate::cluster::*;

#[derive(Clone)]
struct PausedLoader {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}

#[async_trait]
impl ActorLoader<EntityActor> for PausedLoader {
    async fn load(&self, ctx: ActorCreateContext) -> Result<EntityActor, ActorFailure> {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        CountingLoader(Arc::new(AtomicUsize::new(0)))
            .load(ctx)
            .await
    }
}

#[tokio::test]
async fn entity_and_singleton_loading_obey_drain_and_fence() {
    for singleton in [false, true] {
        for fence in [false, true] {
            loading_obeys_retirement(singleton, fence).await;
        }
    }
}

async fn loading_obeys_retirement(singleton: bool, fence: bool) {
    let cluster_id = ClusterId::new("activation-fencing").unwrap();
    let incarnation = NodeIncarnation::new(1).unwrap();
    let address = NodeAddress::new("127.0.0.1", 25570).unwrap();
    let node = NodeKey {
        node_id: "local".into(),
        address: address.clone(),
        incarnation,
    };
    let associations = Arc::new(
        AssociationManager::new(address.clone(), incarnation, RemotingConfig::default()).unwrap(),
    );
    let coordinator = attach_coordinator(
        &associations,
        &cluster_id,
        incarnation,
        NodeAddress::new("127.0.0.1", 25571).unwrap(),
        NodeIncarnation::new(2).unwrap(),
    );
    let entity = EntityConfig::new(
        domain(),
        EntityType::new("activation-fencing").unwrap(),
        ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
        16,
        "weighted-least-load",
        1,
        Vec::new(),
    )
    .unwrap();
    let singleton_config = SingletonConfig::new(
        domain(),
        SingletonKind::new("activation-fencing").unwrap(),
        ProtocolId::new(TEST_PROTOCOL_ID).unwrap(),
    );
    let entity_id = EntityId::new(b"loading".to_vec()).unwrap();
    let slot_key = if singleton {
        PlacementSlotKey::Singleton {
            domain: domain(),
            kind: singleton_config.kind.clone(),
        }
    } else {
        PlacementSlotKey::Shard {
            domain: domain(),
            entity_type: entity.entity_type.clone(),
            shard_id: entity.shard_for(&entity_id).unwrap(),
        }
    };
    let hello = test_hello(
        node.clone(),
        [entity.entity_type.clone()].into_iter().collect(),
        [singleton_config.kind.clone()].into_iter().collect(),
        BTreeSet::new(),
    );
    let slot = PlacementSlot {
        key: slot_key.clone(),
        config_fingerprint: if singleton {
            singleton_config.fingerprint()
        } else {
            entity.fingerprint()
        },
        owner: Some(node.clone()),
        target: None,
        assignment_generation: AssignmentGeneration::new(1).unwrap(),
        version: PlacementVersion::new(
            domain(),
            CoordinatorTerm::new(1).unwrap(),
            Revision::new(1).unwrap(),
        ),
        state: PlacementSlotState::Running,
        active_move: None,
        barrier_sessions: Default::default(),
    };
    let (state, _control, shutdown, runtime) =
        stage_logic_runtime(hello, coordinator.clone(), associations.clone(), vec![slot]).await;
    let binding = Arc::new(EntityProtocol::bind::<EntityActor>().unwrap());
    let registry = Arc::new(ActorRegistry::new(
        actor_kind!("FencedLoading"),
        ActorRegistryConfig::default(),
    ));
    let loader = PausedLoader {
        entered: Arc::new(Semaphore::new(0)),
        release: Arc::new(Semaphore::new(0)),
    };
    let mut router = DomainLogicalRouter::new(
        node,
        state,
        associations,
        Arc::new(OutboundMessaging::new(8).unwrap()),
        coordinator,
        LogicalBufferConfig::default(),
        8,
    )
    .unwrap();
    if singleton {
        router
            .register_singleton(
                singleton_config.clone(),
                registry.clone(),
                binding,
                loader.clone(),
            )
            .unwrap();
    } else {
        router
            .register_entity(entity.clone(), registry.clone(), binding, loader.clone())
            .unwrap();
    }
    let client = EntityProtocol::build().unwrap();
    let replacement = CountingLoader(Arc::new(AtomicUsize::new(0)));
    let replacement_binding = Arc::new(EntityProtocol::bind::<EntityActor>().unwrap());
    if singleton {
        assert!(
            router
                .register_singleton(
                    singleton_config.clone(),
                    registry.clone(),
                    replacement_binding,
                    replacement
                )
                .is_err()
        );
        assert!(
            router
                .register_singleton_proxy(singleton_config.clone(), client.fingerprint())
                .is_err()
        );
    } else {
        assert!(
            router
                .register_entity(
                    entity.clone(),
                    registry.clone(),
                    replacement_binding,
                    replacement
                )
                .is_err()
        );
        assert!(
            router
                .register_entity_proxy(entity.clone(), client.fingerprint())
                .is_err()
        );
    }
    let (_, request) = client
        .encode_request(DispatchMode::Ask, &GetValue(2))
        .unwrap();
    let router = Arc::new(router);
    let request_router = router.clone();
    let activation = tokio::spawn(async move {
        if singleton {
            request_router
                .receive_singleton_ask(
                    LogicalSingletonTarget {
                        reference: SingletonAddress::new(
                            cluster_id,
                            domain(),
                            singleton_config.kind.clone(),
                            singleton_config.protocol_id,
                            singleton_config.fingerprint(),
                        )
                        .unwrap(),
                        owner_address: address,
                        owner_incarnation: incarnation,
                        assignment_generation: 1,
                    },
                    1,
                    request,
                    Instant::now() + Duration::from_secs(1),
                )
                .await
        } else {
            request_router
                .receive_entity_ask(
                    LogicalEntityTarget {
                        reference: entity.entity_ref(cluster_id, entity_id).unwrap(),
                        owner_address: address,
                        owner_incarnation: incarnation,
                        assignment_generation: 1,
                    },
                    1,
                    request,
                    Instant::now() + Duration::from_secs(1),
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(1), loader.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(registry.active_actor_ids().len(), 1);
    if fence {
        router.stop_fenced_slot(slot_key).await.unwrap();
    } else {
        assert!(router.drain_slot(slot_key).await.unwrap());
    }
    loader.release.add_permits(1);
    assert!(
        tokio::time::timeout(Duration::from_secs(1), activation)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(registry.active_actor_ids().is_empty());
    shutdown.send(true).unwrap();
    runtime.await.unwrap().unwrap();
}
