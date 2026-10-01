use std::{sync::Arc, time::Instant};

use bytes::Bytes;
use lattice_actor_distributed::host::ProtocolHostRegistry;
use lattice_model::{
    actor::{ActivationId, ActorId, ActorPath, EntityAddress, ProtocolId, SingletonAddress},
    cluster::{
        ActorGroupId, ClusterId, ConfigFingerprint, EntityType, NodeEndpoint, NodeIncarnation,
        SingletonKind,
    },
};
use lattice_remoting::messaging::{
    error::{AskError, RemoteFailureCode, RemoteMessageError},
    inbound::{ImmediateTellDispatch, InboundDispatch},
    target::{ExactActorTarget, InboundTell, LogicalEntityTarget, LogicalSingletonTarget},
};

use super::{ServiceInboundDispatch, map_remote_ask};
use crate::lifecycle::NodeAdmissionGate;

fn dispatch(admission: NodeAdmissionGate) -> ServiceInboundDispatch {
    ServiceInboundDispatch {
        hosts: Arc::new(ProtocolHostRegistry::new(1).unwrap()),
        logical: None,
        admission,
    }
}

fn exact_target() -> ExactActorTarget {
    ExactActorTarget {
        cluster_id: ClusterId::new("test").unwrap(),
        node_address: NodeEndpoint::new("localhost", 25520).unwrap(),
        actor_path: ActorPath::user(["user", "actor"]).unwrap(),
        activation_id: ActivationId::new(NodeIncarnation::new(1).unwrap(), 1).unwrap(),
        protocol_id: ProtocolId::new(1).unwrap(),
    }
}

fn entity_target() -> LogicalEntityTarget {
    LogicalEntityTarget {
        reference: EntityAddress::new(
            ClusterId::new("test").unwrap(),
            ActorGroupId::new("world").unwrap(),
            EntityType::new("player").unwrap(),
            ActorId::new(b"player-1".to_vec()).unwrap(),
            ProtocolId::new(1).unwrap(),
            ConfigFingerprint::new([1; 32]),
        )
        .unwrap(),
        owner_address: NodeEndpoint::new("localhost", 25520).unwrap(),
        owner_incarnation: NodeIncarnation::new(1).unwrap(),
        assignment_generation: 1,
    }
}

fn singleton_target() -> LogicalSingletonTarget {
    LogicalSingletonTarget {
        reference: SingletonAddress::new(
            ClusterId::new("test").unwrap(),
            ActorGroupId::new("world").unwrap(),
            SingletonKind::new("ranking").unwrap(),
            ProtocolId::new(1).unwrap(),
            ConfigFingerprint::new([1; 32]),
        )
        .unwrap(),
        owner_address: NodeEndpoint::new("localhost", 25520).unwrap(),
        owner_incarnation: NodeIncarnation::new(1).unwrap(),
        assignment_generation: 1,
    }
}

#[tokio::test]
async fn closed_admission_rejects_exact_messages_before_host_lookup() {
    let dispatch = dispatch(NodeAdmissionGate::closed());
    assert!(matches!(
        dispatch.try_tell_immediate(InboundTell {
            target: exact_target(),
            message_id: 1,
            payload: Bytes::new(),
        }),
        ImmediateTellDispatch::Complete(Err(RemoteMessageError::AdmissionClosed))
    ));
    assert_eq!(
        dispatch.tell(exact_target(), 1, Bytes::new()).await,
        Err(RemoteMessageError::AdmissionClosed)
    );
    assert_eq!(
        dispatch
            .ask(exact_target(), 1, Bytes::new(), Instant::now())
            .await,
        Err(RemoteMessageError::AdmissionClosed)
    );
}

#[tokio::test]
async fn logical_messages_distinguish_closed_admission_from_missing_router() {
    for (admission, expected) in [
        (
            NodeAdmissionGate::closed(),
            RemoteMessageError::AdmissionClosed,
        ),
        (
            NodeAdmissionGate::opened(),
            RemoteMessageError::LogicalRoutingUnavailable,
        ),
    ] {
        let dispatch = dispatch(admission);
        assert_eq!(
            dispatch.tell_entity(entity_target(), 1, Bytes::new()).await,
            Err(expected.clone())
        );
        assert_eq!(
            dispatch
                .ask_entity(entity_target(), 1, Bytes::new(), Instant::now())
                .await,
            Err(expected.clone())
        );
        assert_eq!(
            dispatch
                .tell_singleton(singleton_target(), 1, Bytes::new())
                .await,
            Err(expected.clone())
        );
        assert_eq!(
            dispatch
                .ask_singleton(singleton_target(), 1, Bytes::new(), Instant::now())
                .await,
            Err(expected)
        );
    }
}

#[test]
fn local_ask_preserves_admission_and_routing_failure_reasons() {
    assert_eq!(
        map_remote_ask(RemoteMessageError::AdmissionClosed),
        AskError::Remote(RemoteFailureCode::AdmissionClosed)
    );
    assert_eq!(
        map_remote_ask(RemoteMessageError::LogicalRoutingUnavailable),
        AskError::Remote(RemoteFailureCode::LogicalRoutingUnavailable)
    );
}
