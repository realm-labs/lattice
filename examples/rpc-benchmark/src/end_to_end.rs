use lattice_actor::context::HandlerContext;
use std::{
    error::Error,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use lattice_actor::{
    error::{ActorCallError, ActorError},
    handle::ActorHandle,
    registry::{ActorRegistry, ActorRegistryConfig},
    reply::ReplyTo,
    traits::{Actor, Request, Responder},
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
        inbound::InboundDispatch,
        outbound::{OutboundMessage, OutboundMessaging},
        target::{ExactActorTarget, SenderIdentity},
    },
    protocol::{ProtocolDescriptor, ProtocolFingerprint},
};
use tokio::net::TcpListener;

use crate::metrics::WorkloadReport;

struct EchoRequest(Bytes);

impl Request for EchoRequest {
    type Response = Bytes;
}

struct EchoActor;

impl Actor for EchoActor {
    type Error = ActorError;
    type Behavior = ::lattice_actor::state_machine::Stateless;
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
    target: ActorRef,
    fingerprint: ProtocolFingerprint,
}

impl RemoteActorTopology {
    pub async fn start(bulk_stripes: usize) -> Result<Self, Box<dyn Error>> {
        let actor_registry = Arc::new(ActorRegistry::new(
            actor_kind!("BenchmarkRemoteEcho"),
            ActorRegistryConfig::default(),
        ));
        let actor = actor_registry.start(ActorId::U64(1), EchoActor).await?;
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
        let client = endpoint(
            client_identity.clone(),
            config.clone(),
            client_messaging.clone(),
            Arc::new(RejectDispatch),
            descriptor.clone(),
        )?;
        let server = endpoint(
            server_identity.clone(),
            config,
            server_messaging,
            Arc::new(ActorDispatch { handle: actor }),
            descriptor,
        )?;
        client.bind().await?;
        server.bind().await?;
        let association = client.connect_peer(server_identity.clone()).await?;
        let target = ActorRef::new(
            cluster_id,
            server_identity.address,
            server_identity.incarnation,
            ActorPath::user(["user", "benchmark-echo"])?,
            ActivationId::new(server_identity.incarnation, 1)?,
            protocol_id,
        )?;
        let topology = Self {
            actor_registry,
            client,
            server,
            messaging: client_messaging,
            association,
            target,
            fingerprint,
        };
        topology.round_trip(Bytes::new()).await?;
        Ok(topology)
    }

    pub async fn run(
        &self,
        requests: usize,
        payload_bytes: usize,
    ) -> Result<WorkloadReport, Box<dyn Error>> {
        let payload = Bytes::from(vec![0_u8; payload_bytes]);
        let mut latencies = Vec::with_capacity(requests);
        let started = Instant::now();
        for _ in 0..requests {
            let request_started = Instant::now();
            let reply = self.round_trip(payload.clone()).await?;
            if reply.len() != payload_bytes {
                return Err("remote echo returned a different payload length".into());
            }
            latencies.push(request_started.elapsed());
        }
        Ok(WorkloadReport {
            name: "remote_actor_tcp_ask_round_trip",
            requests,
            successes: latencies.len(),
            errors: requests.saturating_sub(latencies.len()),
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
        let payload = Bytes::from(vec![0_u8; payload_bytes]);
        let started = Instant::now();
        for _ in 0..requests {
            let reply = self.round_trip(payload.clone()).await?;
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
) -> Result<Arc<RemotingEndpoint>, Box<dyn Error>> {
    let manager = Arc::new(AssociationManager::new(
        identity.address.clone(),
        identity.incarnation,
        config.clone(),
    )?);
    Ok(Arc::new(
        RemotingEndpoint::builder(identity, config, manager, messaging, dispatch)
            .catalogue(vec![descriptor])
            .build()?,
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
        let report = topology.run(4, 64).await.unwrap();
        assert_eq!(report.successes, 4);
        assert!(report.latencies.iter().all(|latency| !latency.is_zero()));
        topology.shutdown().await.unwrap();
    }
}
