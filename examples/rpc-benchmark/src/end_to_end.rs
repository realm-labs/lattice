use lattice_actor::context::HandlerContext;
use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt as _, stream::FuturesUnordered};
use lattice_actor::{
    error::{ActorCallError, ActorError},
    handle::ActorHandle,
    registry::{ActorRegistry, ActorRegistryConfig},
    reply::ReplyTo,
    traits::{Actor, Handler, Request, Responder},
};
use lattice_core::{
    actor_kind,
    actor_ref::{
        ActivationId, ActorPath, ActorRef, ClusterId, NodeAddress, NodeIncarnation, ProtocolId,
    },
    id::ActorId,
};
use lattice_remoting::{
    association::{Association, AssociationManager},
    config::RemotingConfig,
    endpoint::RemotingEndpoint,
    handshake::NodeIdentity,
    messaging::{
        error::RemoteMessageError,
        inbound::{ImmediateTellDispatch, InboundDispatch},
        outbound::{OutboundMessage, OutboundMessaging, PreparedExactTellRoute},
        target::{ExactActorTarget, InboundTell, SenderIdentity},
    },
    protocol::{ProtocolDescriptor, ProtocolFingerprint},
};
use prost::Message as ProstMessage;
use tokio::net::TcpListener;
use tokio::sync::Notify;

use crate::metrics::WorkloadReport;

struct EchoRequest(Bytes);

impl Request for EchoRequest {
    type Response = Bytes;
}

#[derive(Clone, PartialEq, ProstMessage)]
struct TellWireMessage {
    #[prost(bytes = "bytes", tag = "1")]
    payload: Bytes,
    #[prost(uint64, tag = "2")]
    completion_generation: u64,
}

#[derive(lattice_actor::Message)]
struct EchoTell(TellWireMessage);

#[derive(Default)]
struct TellCompletion {
    generation: AtomicU64,
    changed: Notify,
}

impl TellCompletion {
    fn complete(&self, generation: u64) {
        self.generation.fetch_max(generation, Ordering::Release);
        self.changed.notify_waiters();
    }

    async fn wait(&self, generation: u64) {
        while self.generation.load(Ordering::Acquire) < generation {
            let changed = self.changed.notified();
            if self.generation.load(Ordering::Acquire) >= generation {
                return;
            }
            changed.await;
        }
    }
}

struct EchoActor {
    tell_completion: Arc<TellCompletion>,
    processed_bytes: usize,
}

impl Actor for EchoActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

impl Handler<EchoTell> for EchoActor {
    async fn handle(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        message: EchoTell,
    ) -> Result<(), Self::Error> {
        self.processed_bytes = self.processed_bytes.wrapping_add(message.0.payload.len());
        if message.0.completion_generation != 0 {
            self.tell_completion
                .complete(message.0.completion_generation);
        }
        Ok(())
    }
}

impl Responder<EchoRequest> for EchoActor {
    async fn respond(
        &mut self,
        _context: &mut HandlerContext<'_, Self>,
        request: EchoRequest,
        reply_to: ReplyTo<Bytes>,
    ) -> Result<(), Self::Error> {
        reply_to.send(request.0).map_err(ActorError::from_error)
    }
}

struct ActorDispatch {
    handle: ActorHandle<EchoActor>,
}

#[async_trait]
impl InboundDispatch for ActorDispatch {
    fn try_tell_immediate(&self, tell: InboundTell) -> ImmediateTellDispatch {
        let Ok(message) = TellWireMessage::decode(tell.payload.clone()) else {
            return ImmediateTellDispatch::Complete(Err(RemoteMessageError::InvalidPayload));
        };
        match self.handle.try_tell(EchoTell(message)) {
            Ok(()) => ImmediateTellDispatch::Complete(Ok(())),
            Err(_) => ImmediateTellDispatch::Deferred(tell),
        }
    }

    async fn tell(
        &self,
        _sender: Option<ActorRef>,
        _target: ExactActorTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        let message =
            TellWireMessage::decode(_payload).map_err(|_| RemoteMessageError::InvalidPayload)?;
        self.handle
            .tell(EchoTell(message))
            .await
            .map_err(|_| RemoteMessageError::MailboxRejected)
    }

    async fn ask(
        &self,
        _target: ExactActorTarget,
        _message_id: u64,
        payload: Bytes,
        deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        let timeout = deadline
            .checked_duration_since(Instant::now())
            .ok_or(RemoteMessageError::DeadlineExceeded)?;
        self.handle
            .ask(EchoRequest(payload), timeout)
            .await
            .map_err(map_actor_error)
    }
}

struct RejectDispatch;

#[async_trait]
impl InboundDispatch for RejectDispatch {
    async fn tell(
        &self,
        _sender: Option<ActorRef>,
        _target: ExactActorTarget,
        _message_id: u64,
        _payload: Bytes,
    ) -> Result<(), RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }

    async fn ask(
        &self,
        _target: ExactActorTarget,
        _message_id: u64,
        _payload: Bytes,
        _deadline: Instant,
    ) -> Result<Bytes, RemoteMessageError> {
        Err(RemoteMessageError::Unauthorized)
    }
}

fn map_actor_error(error: ActorCallError) -> RemoteMessageError {
    match error {
        ActorCallError::DeadlineExceeded | ActorCallError::InvalidTimeout => {
            RemoteMessageError::DeadlineExceeded
        }
        ActorCallError::MailboxFull
        | ActorCallError::MailboxClosed
        | ActorCallError::LifecycleUnavailable { .. } => RemoteMessageError::MailboxRejected,
        ActorCallError::ActorPanicked => RemoteMessageError::ActorPanicked,
        ActorCallError::ResponseDropped
        | ActorCallError::UnhandledInCurrentState
        | ActorCallError::Handler(_) => RemoteMessageError::HandlerFailed,
    }
}

pub struct RemoteActorTopology {
    actor_registry: Arc<ActorRegistry<EchoActor>>,
    client: Arc<RemotingEndpoint>,
    server: Arc<RemotingEndpoint>,
    messaging: Arc<OutboundMessaging>,
    association: Arc<Association>,
    inbound_association: Arc<Association>,
    target: ActorRef,
    fingerprint: ProtocolFingerprint,
    prepared_tell: PreparedExactTellRoute,
    tell_completion: Arc<TellCompletion>,
    tell_generation: AtomicU64,
}

impl RemoteActorTopology {
    pub fn association_metrics(
        &self,
    ) -> lattice_remoting::association::metrics::AssociationMetricsSnapshot {
        self.association.metrics()
    }

    pub fn inbound_association_metrics(
        &self,
    ) -> lattice_remoting::association::metrics::AssociationMetricsSnapshot {
        self.inbound_association.metrics()
    }

    pub async fn start(bulk_stripes: usize) -> Result<Self, Box<dyn Error>> {
        let actor_registry = Arc::new(ActorRegistry::new(
            actor_kind!("BenchmarkRemoteEcho"),
            ActorRegistryConfig::default(),
        ));
        let tell_completion = Arc::new(TellCompletion::default());
        let actor = actor_registry
            .start(
                ActorId::U64(1),
                EchoActor {
                    tell_completion: tell_completion.clone(),
                    processed_bytes: 0,
                },
            )
            .await?;
        let cluster_id = ClusterId::new("remoting-end-to-end-benchmark")?;
        let client_identity = available_identity(cluster_id.clone(), "client", 1).await?;
        let server_identity = available_identity(cluster_id.clone(), "server", 2).await?;
        let protocol_id = ProtocolId::new(crate::BENCH_PROTOCOL_ID)?;
        let fingerprint = ProtocolFingerprint::digest(b"remoting-end-to-end/v1:actor-ask");
        let descriptor = ProtocolDescriptor {
            protocol_id,
            fingerprint,
        };
        let config = RemotingConfig {
            bulk_stripes,
            connect_timeout: Duration::from_secs(2),
            shutdown_timeout: Duration::from_secs(5),
            heartbeat_interval: Duration::from_secs(1),
            ..RemotingConfig::default()
        };
        let client_messaging = Arc::new(OutboundMessaging::new(4096)?);
        let server_messaging = Arc::new(OutboundMessaging::new(4096)?);
        let (client, _) = endpoint(
            client_identity.clone(),
            config.clone(),
            client_messaging.clone(),
            Arc::new(RejectDispatch),
            descriptor.clone(),
        )?;
        let (server, server_manager) = endpoint(
            server_identity.clone(),
            config,
            server_messaging,
            Arc::new(ActorDispatch { handle: actor }),
            descriptor,
        )?;
        client.bind().await?;
        server.bind().await?;
        let association = client.connect_peer(server_identity.clone()).await?;
        let inbound_association = server_manager
            .get_exact(
                &cluster_id,
                &client_identity.address,
                client_identity.incarnation,
            )
            .ok_or("server did not register the inbound benchmark association")?;
        let target = ActorRef::new(
            cluster_id,
            server_identity.address,
            server_identity.incarnation,
            ActorPath::user(["user", "benchmark-echo"])?,
            ActivationId::new(server_identity.incarnation, 1)?,
            protocol_id,
        )?;
        let prepared_tell = client_messaging.prepare_exact_tell_route(
            association.clone(),
            &SenderIdentity::Process(1),
            &target,
            fingerprint,
        )?;
        let topology = Self {
            actor_registry,
            client,
            server,
            messaging: client_messaging,
            association,
            inbound_association,
            target,
            fingerprint,
            prepared_tell,
            tell_completion,
            tell_generation: AtomicU64::new(0),
        };
        topology.round_trip(Bytes::new()).await?;
        Ok(topology)
    }

    pub async fn run_tell(
        &self,
        requests: usize,
        payload_bytes: usize,
    ) -> Result<WorkloadReport, Box<dyn Error>> {
        let payload = Bytes::from(vec![0_u8; payload_bytes]);
        let generation = self.tell_generation.fetch_add(1, Ordering::Relaxed) + 1;
        let started = Instant::now();
        for index in 0..requests {
            let completion_generation = if index + 1 == requests { generation } else { 0 };
            let message = TellWireMessage {
                payload: payload.clone(),
                completion_generation,
            };
            let encoded = Bytes::from(message.encode_to_vec());
            self.prepared_tell.tell_wait(1, encoded).await?;
        }
        self.tell_completion.wait(generation).await;
        Ok(WorkloadReport {
            name: "remote_actor_tcp_tell",
            requests,
            successes: requests,
            errors: 0,
            elapsed: started.elapsed(),
            latencies: Vec::new(),
            observed_actor_ids: [1].into_iter().collect(),
        })
    }

    pub async fn run(
        &self,
        requests: usize,
        payload_bytes: usize,
    ) -> Result<WorkloadReport, Box<dyn Error>> {
        self.run_windowed(requests, payload_bytes, 1).await
    }

    pub async fn run_windowed(
        &self,
        requests: usize,
        payload_bytes: usize,
        window: usize,
    ) -> Result<WorkloadReport, Box<dyn Error>> {
        let payload = Bytes::from(vec![0_u8; payload_bytes]);
        let window = window.max(1).min(requests.max(1));
        let mut in_flight = FuturesUnordered::new();
        let mut sent = 0;
        let mut latencies = Vec::with_capacity(requests);
        let mut errors = 0;
        let started = Instant::now();
        while sent < requests || !in_flight.is_empty() {
            while sent < requests && in_flight.len() < window {
                let request_payload = payload.clone();
                in_flight.push(async move {
                    let request_started = Instant::now();
                    let reply = self.round_trip(request_payload).await;
                    (request_started.elapsed(), reply)
                });
                sent += 1;
            }
            let Some((latency, reply)) = in_flight.next().await else {
                break;
            };
            match reply {
                Ok(reply) if reply.len() == payload_bytes => latencies.push(latency),
                Ok(_) => return Err("remote echo returned a different payload length".into()),
                Err(_) => errors += 1,
            }
        }
        Ok(WorkloadReport {
            name: "remote_actor_tcp_ask_round_trip",
            requests,
            successes: latencies.len(),
            errors,
            elapsed: started.elapsed(),
            latencies,
            observed_actor_ids: [1].into_iter().collect(),
        })
    }

    pub async fn run_timing(
        &self,
        requests: usize,
        payload_bytes: usize,
    ) -> Result<Duration, Box<dyn Error>> {
        self.run_timing_windowed(requests, payload_bytes, 1).await
    }

    pub async fn run_timing_windowed(
        &self,
        requests: usize,
        payload_bytes: usize,
        window: usize,
    ) -> Result<Duration, Box<dyn Error>> {
        let payload = Bytes::from(vec![0_u8; payload_bytes]);
        let window = window.max(1).min(requests.max(1));
        let mut in_flight = FuturesUnordered::new();
        let mut sent = 0;
        let started = Instant::now();
        while sent < requests || !in_flight.is_empty() {
            while sent < requests && in_flight.len() < window {
                in_flight.push(self.round_trip(payload.clone()));
                sent += 1;
            }
            let Some(reply) = in_flight.next().await else {
                break;
            };
            let reply = reply?;
            if reply.len() != payload_bytes {
                return Err("remote echo returned a different payload length".into());
            }
        }
        Ok(started.elapsed())
    }

    async fn round_trip(&self, payload: Bytes) -> Result<Bytes, Box<dyn Error>> {
        self.messaging
            .ask(
                &self.association,
                &SenderIdentity::Process(1),
                &self.target,
                OutboundMessage::new(self.fingerprint, 1, payload),
                Instant::now() + Duration::from_secs(5),
            )
            .await
            .map_err(|error| Box::new(error) as Box<dyn Error>)
    }

    pub async fn shutdown(&self) -> Result<(), Box<dyn Error>> {
        self.client.shutdown().await?;
        self.server.shutdown().await?;
        let drained = self.actor_registry.drain().await;
        if !drained.completed() {
            return Err("remote benchmark actor did not drain cleanly".into());
        }
        Ok(())
    }
}

fn endpoint(
    identity: NodeIdentity,
    config: RemotingConfig,
    messaging: Arc<OutboundMessaging>,
    dispatch: Arc<dyn InboundDispatch>,
    descriptor: ProtocolDescriptor,
) -> Result<(Arc<RemotingEndpoint>, Arc<AssociationManager>), Box<dyn Error>> {
    let manager = Arc::new(AssociationManager::new(
        identity.address.clone(),
        identity.incarnation,
        config.clone(),
    )?);
    Ok((
        Arc::new(
            RemotingEndpoint::builder(identity, config, manager.clone(), messaging, dispatch)
                .catalogue(vec![descriptor])
                .build()?,
        ),
        manager,
    ))
}

async fn available_identity(
    cluster_id: ClusterId,
    node_id: &str,
    incarnation: u128,
) -> Result<NodeIdentity, Box<dyn Error>> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(NodeIdentity {
        cluster_id,
        node_id: node_id.to_owned(),
        address: NodeAddress::new("127.0.0.1", port)?,
        incarnation: NodeIncarnation::new(incarnation)?,
    })
}

#[cfg(test)]
mod tests {
    use super::RemoteActorTopology;

    #[tokio::test]
    async fn tcp_round_trip_crosses_remote_endpoint_and_actor() {
        let topology = RemoteActorTopology::start(1).await.unwrap();
        let tell = topology.run_tell(4, 64).await.unwrap();
        assert_eq!(tell.successes, 4);
        let report = topology.run_windowed(8, 64, 4).await.unwrap();
        assert_eq!(report.successes, 8);
        assert!(report.latencies.iter().all(|latency| !latency.is_zero()));
        topology.shutdown().await.unwrap();
    }
}
