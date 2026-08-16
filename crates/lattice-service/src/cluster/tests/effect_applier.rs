//! Placement effect application: supervised slot watchers and drain-ready fencing.

use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};

use async_trait::async_trait;
use lattice_actor::host::ProtocolHostRegistry;
use lattice_core::actor_ref::{ClusterId, NodeIncarnation};
use lattice_placement::session::{
    LogicCoordinatorConfig, LogicCoordinatorHandle, LogicPlacementEffect, PlacementDomainSession,
};
use lattice_remoting::{
    config::RemotingConfig, endpoint::RemotingEndpoint, handshake::NodeIdentity,
    watch::WatchRegistry,
};
use tokio::sync::watch;

use super::support::{attach_coordinator, domain, test_hello};
use crate::{
    backend::{DomainRouterDirectory, ServiceInboundDispatch},
    cluster::{members::MemberDirectory, peers::PeerReconciler, runtime::LogicEffectApplier, *},
    lifecycle::NodeAdmissionGate,
    supervisor::TaskSupervisor,
    test_support::{network_test_guard, unused_address},
};

struct PendingDrainRouter;

#[async_trait]
impl LogicalRouter for PendingDrainRouter {
    async fn tell_entity(
        &self,
        _target: EntityRef,
        _fingerprint: ProtocolFingerprint,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn ask_entity(
        &self,
        _target: EntityRef,
        _fingerprint: ProtocolFingerprint,
        _message_id: u64,
        _payload: Bytes,
        _deadline: Instant,
    ) -> Result<Bytes, AskError> {
        Err(AskError::Protocol(RemoteMessageError::Unauthorized))
    }

    async fn tell_singleton(
        &self,
        _target: SingletonRef,
        _fingerprint: ProtocolFingerprint,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn ask_singleton(
        &self,
        _target: SingletonRef,
        _fingerprint: ProtocolFingerprint,
        _message_id: u64,
        _payload: Bytes,
        _deadline: Instant,
    ) -> Result<Bytes, AskError> {
        Err(AskError::Protocol(RemoteMessageError::Unauthorized))
    }

    async fn resolve_entity_current(
        &self,
        _target: EntityRef,
    ) -> Result<Option<ActorRef>, WatchError> {
        Err(WatchError::Unavailable)
    }

    async fn resolve_singleton_current(
        &self,
        _target: SingletonRef,
    ) -> Result<Option<ActorRef>, WatchError> {
        Err(WatchError::Unavailable)
    }

    async fn wait_slot_drained(&self, _slot: PlacementSlotKey) -> Result<(), RemoteMessageError> {
        std::future::pending().await
    }
}

async fn test_effect_applier(
    router: Arc<dyn LogicalRouter>,
    node_id: &str,
) -> (
    LogicEffectApplier,
    LogicCoordinatorHandle,
    PlacementDomainSession,
) {
    let cluster_id = ClusterId::new("effect-applier-test").unwrap();
    let local_incarnation = NodeIncarnation::new(41).unwrap();
    let coordinator_incarnation = NodeIncarnation::new(42).unwrap();
    let local_address = unused_address().await;
    let coordinator_address = unused_address().await;
    let local_node = NodeKey {
        node_id: node_id.to_owned(),
        address: local_address.clone(),
        incarnation: local_incarnation,
    };
    let associations = Arc::new(
        AssociationManager::new(
            local_address.clone(),
            local_incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let coordinator = attach_coordinator(
        &associations,
        &cluster_id,
        local_incarnation,
        coordinator_address,
        coordinator_incarnation,
    );
    let hello = test_hello(
        local_node.clone(),
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
    );
    let (session, _effects) = PlacementDomainSession::new(
        hello.domain,
        coordinator,
        associations.clone(),
        LogicCoordinatorConfig::default(),
        32,
        1,
    )
    .unwrap();
    let handle = session.control_handle();
    let endpoint = Arc::new(
        RemotingEndpoint::builder(
            NodeIdentity {
                cluster_id: cluster_id.clone(),
                node_id: local_node.node_id.clone(),
                address: local_address,
                incarnation: local_incarnation,
            },
            RemotingConfig::default(),
            associations.clone(),
            Arc::new(OutboundMessaging::new(8).unwrap()),
            Arc::new(ServiceInboundDispatch {
                hosts: Arc::new(ProtocolHostRegistry::new(1).unwrap()),
                logical: None,
                admission: NodeAdmissionGate::closed(),
            }),
        )
        .build()
        .unwrap(),
    );
    let (drain_ready, _) = watch::channel(BTreeMap::new());
    let (drain_blockers, _) = watch::channel(BTreeMap::new());
    let applier = LogicEffectApplier {
        domain: domain(),
        incarnation: local_incarnation,
        router,
        peers: Arc::new(PeerReconciler::new(
            cluster_id,
            endpoint,
            associations,
            Arc::new(MemberDirectory::new(8).unwrap()),
        )),
        watches: Arc::new(Mutex::new(WatchRegistry::new(8, 8).unwrap())),
        drain_ready,
        drain_blockers,
        supervisor: Arc::new(TaskSupervisor::new(4).unwrap()),
    };
    (applier, handle, session)
}

#[tokio::test]
async fn stop_failed_slot_watcher_runs_under_the_task_supervisor() {
    let _network = network_test_guard().await;
    let (applier, handle, _session) =
        test_effect_applier(Arc::new(PendingDrainRouter), "stop-failed-watcher").await;
    let slot = PlacementSlotKey::Singleton {
        domain: domain(),
        kind: SingletonKind::new("watched-singleton").unwrap(),
    };

    applier.watch_stop_failed_slot(slot, &handle);

    assert_eq!(applier.supervisor.active_tasks(), 1);
    applier
        .supervisor
        .shutdown(Duration::from_millis(200))
        .await
        .unwrap();
}

#[tokio::test]
async fn drain_ready_for_a_foreign_incarnation_never_reaches_the_coordinator() {
    let _network = network_test_guard().await;
    let cluster_id = ClusterId::new("drain-ready-fencing-test").unwrap();
    let local_incarnation = NodeIncarnation::new(41).unwrap();
    let coordinator_incarnation = NodeIncarnation::new(42).unwrap();
    let local_address = unused_address().await;
    let coordinator_address = unused_address().await;
    let local_node = NodeKey {
        node_id: "drain-ready-logic".to_owned(),
        address: local_address.clone(),
        incarnation: local_incarnation,
    };
    let associations = Arc::new(
        AssociationManager::new(
            local_address.clone(),
            local_incarnation,
            RemotingConfig::default(),
        )
        .unwrap(),
    );
    let coordinator = attach_coordinator(
        &associations,
        &cluster_id,
        local_incarnation,
        coordinator_address,
        coordinator_incarnation,
    );
    let hello = test_hello(
        local_node.clone(),
        BTreeSet::new(),
        BTreeSet::new(),
        BTreeSet::new(),
    );
    let (logic, _effects) = PlacementDomainSession::new(
        hello.domain,
        coordinator,
        associations.clone(),
        LogicCoordinatorConfig::default(),
        32,
        1,
    )
    .unwrap();
    let handle = logic.control_handle();
    let endpoint = Arc::new(
        RemotingEndpoint::builder(
            NodeIdentity {
                cluster_id: cluster_id.clone(),
                node_id: local_node.node_id.clone(),
                address: local_address,
                incarnation: local_incarnation,
            },
            RemotingConfig::default(),
            associations.clone(),
            Arc::new(OutboundMessaging::new(8).unwrap()),
            Arc::new(ServiceInboundDispatch {
                hosts: Arc::new(ProtocolHostRegistry::new(1).unwrap()),
                logical: None,
                admission: NodeAdmissionGate::closed(),
            }),
        )
        .build()
        .unwrap(),
    );
    let (drain_ready, _) = watch::channel(BTreeMap::new());
    let (drain_blockers, _) = watch::channel(BTreeMap::new());
    let applier = LogicEffectApplier {
        domain: domain(),
        incarnation: local_incarnation,
        router: Arc::new(DomainRouterDirectory::new([domain()], 4).unwrap()),
        peers: Arc::new(PeerReconciler::new(
            cluster_id,
            endpoint,
            associations,
            Arc::new(MemberDirectory::new(8).unwrap()),
        )),
        watches: Arc::new(Mutex::new(WatchRegistry::new(8, 8).unwrap())),
        drain_ready: drain_ready.clone(),
        drain_blockers,
        supervisor: Arc::new(TaskSupervisor::new(4).unwrap()),
    };

    let rejected = tokio::time::timeout(
        Duration::from_millis(500),
        applier.apply(
            LogicPlacementEffect::DrainReady {
                operation_id: "leave-1".to_owned(),
                incarnation: NodeIncarnation::new(99).unwrap(),
            },
            &handle,
        ),
    )
    .await
    .expect("a foreign drain-ready must be rejected without contacting the Coordinator");

    assert!(rejected.is_err());
    assert!(drain_ready.borrow().is_empty());
}
