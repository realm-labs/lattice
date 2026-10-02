use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::join_all;
use lattice_actor::error::ActorTellError;
use lattice_model::{
    actor::{ActivationId, ActorAddress, ActorPath, ProtocolId},
    cluster::{ClusterId, NodeEndpoint, NodeIncarnation},
};
use tokio::{net::TcpListener, sync::oneshot, task::yield_now, time::timeout};

use super::{ConnectionEvent, EndpointError, RemotingEndpoint, SupervisorHandle};
use crate::{
    association::{Association, AssociationManager, AssociationState, LaneKind},
    config::RemotingConfig,
    handshake::NodeIdentity,
    messaging::{
        error::RemoteMessageError,
        inbound::InboundDispatch,
        outbound::{OutboundMessage, OutboundMessaging},
        target::ExactActorTarget,
    },
    protocol::{ProtocolDescriptor, ProtocolFingerprint},
};

struct EchoDispatch(Arc<AtomicUsize>);

#[async_trait]
impl InboundDispatch for EchoDispatch {
    async fn tell(&self, _: ExactActorTarget, _: u64, _: Bytes) -> Result<(), RemoteMessageError> {
        self.0.fetch_add(1, Ordering::Release);
        Ok(())
    }

    async fn ask(
        &self,
        _: ExactActorTarget,
        _: u64,
        payload: Bytes,
        _: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        Ok(payload)
    }
}

struct Pair {
    client: Arc<RemotingEndpoint>,
    server: Arc<RemotingEndpoint>,
    target: ActorAddress,
    fingerprint: ProtocolFingerprint,
    tells: Arc<AtomicUsize>,
}

impl Pair {
    async fn new(config: RemotingConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let identity = |name: &str, port, incarnation| NodeIdentity {
            cluster_id: ClusterId::new("supervision").unwrap(),
            node_id: name.to_owned(),
            address: NodeEndpoint::new("127.0.0.1", port).unwrap(),
            incarnation: NodeIncarnation::new(incarnation).unwrap(),
        };
        let fingerprint = ProtocolFingerprint::digest(b"supervision/v1");
        let protocol = ProtocolDescriptor {
            protocol_id: ProtocolId::new(7).unwrap(),
            fingerprint,
        };
        let tells = Arc::new(AtomicUsize::new(0));
        let build = |identity: NodeIdentity| {
            let associations = Arc::new(
                AssociationManager::new(
                    identity.address.clone(),
                    identity.incarnation,
                    config.clone(),
                )
                .unwrap(),
            );
            Arc::new(
                RemotingEndpoint::builder(
                    identity,
                    config.clone(),
                    associations,
                    Arc::new(OutboundMessaging::new(32).unwrap()),
                    Arc::new(EchoDispatch(tells.clone())),
                )
                .catalogue(vec![protocol.clone()])
                .build()
                .unwrap(),
            )
        };
        let client = build(identity("client", port - 1, 1));
        let server = build(identity("server", port, 2));
        server.bind().await.unwrap();
        let target = ActorAddress::new(
            server.local.cluster_id.clone(),
            server.local.address.clone(),
            ActorPath::user(["echo"]).unwrap(),
            ActivationId::new(server.local.incarnation, 1).unwrap(),
            protocol.protocol_id,
        )
        .unwrap();
        Self {
            client,
            server,
            target,
            fingerprint,
            tells,
        }
    }

    async fn connect(&self) -> Arc<Association> {
        self.client
            .connect_peer(self.server.local.clone())
            .await
            .unwrap()
    }

    async fn ask(&self, association: &Association) -> Bytes {
        self.client
            .messaging
            .ask(
                association,
                &self.target,
                OutboundMessage::new(self.fingerprint, 1, Bytes::from_static(b"echo")),
                Instant::now() + Duration::from_secs(2),
            )
            .await
            .unwrap()
    }

    async fn shutdown(self) {
        self.client.shutdown().await.unwrap();
        self.server.shutdown().await.unwrap();
        assert_eq!(self.client.open_connection_count(), 0);
        assert_eq!(self.server.open_connection_count(), 0);
        assert_eq!(self.client.supervisor_count(), 0);
        assert_eq!(self.server.supervisor_count(), 0);
    }
}

fn supervisor(endpoint: &RemotingEndpoint, association: &Association) -> SupervisorHandle {
    endpoint
        .supervisors
        .lock()
        .unwrap()
        .get(&association.id())
        .unwrap()
        .clone()
}

async fn hold_turn(handle: &SupervisorHandle) -> oneshot::Sender<()> {
    let (entered, ready) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    handle
        .actor
        .tell(ConnectionEvent::HoldTurn {
            entered,
            release: wait,
        })
        .await
        .unwrap();
    timeout(Duration::from_secs(2), ready)
        .await
        .unwrap()
        .unwrap();
    release
}

async fn wait_until(mut predicate: impl FnMut() -> bool) {
    timeout(Duration::from_secs(2), async {
        while !predicate() {
            yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn concurrent_connects_share_one_actor_and_one_lane_group() {
    let pair = Pair::new(RemotingConfig::default()).await;
    let associations =
        join_all((0..16).map(|_| pair.client.connect_peer(pair.server.local.clone()))).await;
    let id = associations[0].as_ref().unwrap().id();
    for association in associations {
        assert_eq!(association.unwrap().id(), id);
    }
    assert_eq!(pair.client.supervisor_count(), 1);
    assert_eq!(pair.client.open_connection_count(), 3);
    wait_until(|| pair.server.supervisor_count() == 1 && pair.server.open_connection_count() == 3)
        .await;
    pair.shutdown().await;
}

#[tokio::test]
async fn tell_ask_and_heartbeats_continue_while_supervision_is_blocked() {
    let pair = Pair::new(RemotingConfig {
        heartbeat_interval: Duration::from_millis(40),
        heartbeat_miss_limit: 10,
        ..RemotingConfig::default()
    })
    .await;
    let association = pair.connect().await;
    let handle = supervisor(&pair.client, &association);
    let release = hold_turn(&handle).await;
    pair.client
        .messaging
        .tell(
            &association,
            &pair.target,
            OutboundMessage::new(pair.fingerprint, 1, Bytes::from_static(b"tell")),
        )
        .unwrap();
    wait_until(|| pair.tells.load(Ordering::Acquire) == 1).await;
    assert_eq!(pair.ask(&association).await, Bytes::from_static(b"echo"));
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(association.state(), AssociationState::Active);
    assert_eq!(association.attached_lane_count(), 3);
    release.send(()).unwrap();
    pair.shutdown().await;
}

#[tokio::test]
async fn queued_ask_wakes_a_detached_lane_when_the_actor_mailbox_was_full() {
    let pair = Arc::new(
        Pair::new(RemotingConfig {
            idle_data_connection_timeout: Duration::from_millis(150),
            reconnect_backoff_min: Duration::from_millis(5),
            heartbeat_interval: Duration::from_millis(50),
            heartbeat_miss_limit: 10,
            ..RemotingConfig::default()
        })
        .await,
    );
    let association = pair.connect().await;
    let handle = supervisor(&pair.client, &association);
    let release = hold_turn(&handle).await;
    // The socket has detached, but its LaneStopped event cannot be processed yet.
    wait_until(|| !association.is_lane_attached(LaneKind::Interactive)).await;
    let mut saturated = false;
    for _ in 0..64 {
        if matches!(
            handle.actor.try_tell(ConnectionEvent::EnsureConnected),
            Err(ActorTellError::MailboxFull(_))
        ) {
            saturated = true;
            break;
        }
    }
    assert!(saturated);
    let ask = {
        let pair = pair.clone();
        let association = association.clone();
        tokio::spawn(async move { pair.ask(&association).await })
    };
    wait_until(|| pair.client.messaging.pending_count() == 1).await;
    release.send(()).unwrap();
    assert_eq!(ask.await.unwrap(), Bytes::from_static(b"echo"));
    let pair = Arc::try_unwrap(pair).ok().unwrap();
    pair.shutdown().await;
}

#[tokio::test]
async fn stale_dial_retry_and_lane_exit_events_cannot_retire_the_current_connection() {
    let pair = Pair::new(RemotingConfig::default()).await;
    let association = pair.connect().await;
    let handle = supervisor(&pair.client, &association);
    for event in [
        ConnectionEvent::DialCompleted {
            lane: LaneKind::Control,
            attempt: u64::MAX,
            result: Err(EndpointError::ConnectionLimit),
        },
        ConnectionEvent::RetryDue {
            lane: LaneKind::Control,
            attempt: u64::MAX,
        },
        ConnectionEvent::LaneStopped {
            lane: LaneKind::Control,
            attempt: u64::MAX,
            nonce: u128::MAX,
            result: Err(EndpointError::SupervisorUnavailable),
        },
    ] {
        handle.actor.tell(event).await.unwrap();
    }
    let release = hold_turn(&handle).await;
    assert_eq!(association.state(), AssociationState::Active);
    assert_eq!(association.attached_lane_count(), 3);
    assert_eq!(pair.ask(&association).await, Bytes::from_static(b"echo"));
    release.send(()).unwrap();
    pair.shutdown().await;
}

#[tokio::test]
async fn supervisor_failure_fences_and_reclaims_the_generation_before_reconnecting() {
    let pair = Pair::new(RemotingConfig::default()).await;
    let old = pair.connect().await;
    let handle = supervisor(&pair.client, &old);
    handle.actor.tell(ConnectionEvent::Panic).await.unwrap();
    wait_until(|| pair.client.open_connection_count() == 0 && pair.client.supervisor_count() == 0)
        .await;
    assert_eq!(old.state(), AssociationState::Closed);
    assert!(pair.client.associations.is_empty());
    wait_until(|| {
        pair.server
            .associations
            .get_exact(
                &pair.client.local.cluster_id,
                &pair.client.local.address,
                pair.client.local.incarnation,
            )
            .is_none_or(|association| !association.has_live_connection())
    })
    .await;
    let current = pair.connect().await;
    assert_ne!(old.id(), current.id());
    assert_eq!(pair.ask(&current).await, Bytes::from_static(b"echo"));
    pair.shutdown().await;
}
