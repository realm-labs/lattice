use std::collections::BTreeMap;
use std::sync::Arc;

use lattice_actor::traits::Actor;
use lattice_actor::{
    ActorHost, ActorProtocol, BoundRecipient, ProtocolHostRegistry, RecipientBackend,
};
use lattice_core::actor_ref::RecipientRef;
use lattice_remoting::{
    Association, AssociationManager, NodeIdentity, OutboundMessaging, ProtocolDescriptor,
    RemotingEndpoint, WatchRegistry,
};

use crate::backend::{LogicalRouter, ServiceRecipientBackend};
use crate::config::NodeConfig;
use crate::error::ServiceError;
use crate::supervisor::TaskSupervisor;

pub struct LatticeServiceBuilder {
    config: NodeConfig,
    hosts: ProtocolHostRegistry,
    protocols: BTreeMap<u64, lattice_remoting::protocol::ProtocolFingerprint>,
    logical: Option<Arc<dyn LogicalRouter>>,
}

impl LatticeServiceBuilder {
    pub fn new(config: NodeConfig) -> Result<Self, ServiceError> {
        config.validate().map_err(ServiceError::Config)?;
        Ok(Self {
            hosts: ProtocolHostRegistry::new(config.maximum_actor_protocols)
                .map_err(ServiceError::Host)?,
            config,
            protocols: BTreeMap::new(),
            logical: None,
        })
    }

    pub fn register_actor<A: Actor>(
        mut self,
        registry: Arc<lattice_actor::registry::ActorRegistry<A>>,
        protocol: Arc<ActorProtocol<A>>,
    ) -> Result<Self, ServiceError> {
        self.protocols
            .insert(protocol.protocol_id().get(), protocol.fingerprint());
        self.hosts
            .register(ActorHost::new(registry, protocol))
            .map_err(ServiceError::Host)?;
        Ok(self)
    }

    pub fn logical_router(mut self, router: Arc<dyn LogicalRouter>) -> Self {
        self.logical = Some(router);
        self
    }

    pub fn build(self) -> Result<LatticeService, ServiceError> {
        let associations = Arc::new(
            AssociationManager::new(
                self.config.address.clone(),
                self.config.incarnation,
                self.config.remoting.clone(),
            )
            .map_err(ServiceError::Association)?,
        );
        let messaging = Arc::new(
            OutboundMessaging::new(self.config.remoting.max_pending_asks)
                .map_err(ServiceError::Messaging)?,
        );
        let hosts = Arc::new(self.hosts);
        let backend: Arc<dyn RecipientBackend> = Arc::new(ServiceRecipientBackend {
            local_cluster: self.config.cluster_id.clone(),
            local_address: self.config.address.clone(),
            local_incarnation: self.config.incarnation,
            hosts: hosts.clone(),
            associations: associations.clone(),
            messaging: messaging.clone(),
            watches: std::sync::Mutex::new(
                WatchRegistry::new(self.config.maximum_watches, self.config.maximum_watches)
                    .map_err(ServiceError::Watch)?,
            ),
            logical: self.logical,
        });
        let endpoint = Arc::new(
            RemotingEndpoint::new(
                NodeIdentity {
                    cluster_id: self.config.cluster_id.clone(),
                    node_id: self.config.node_id.clone(),
                    address: self.config.address.clone(),
                    incarnation: self.config.incarnation,
                },
                self.config.remoting.clone(),
                associations.clone(),
                messaging.clone(),
                hosts,
                self.protocols
                    .into_iter()
                    .map(|(protocol_id, fingerprint)| ProtocolDescriptor {
                        protocol_id: lattice_core::actor_ref::ProtocolId::new(protocol_id)
                            .expect("registered actor protocols have nonzero IDs"),
                        fingerprint,
                    })
                    .collect(),
            )
            .map_err(ServiceError::Endpoint)?,
        );
        let supervisor = Arc::new(TaskSupervisor::new(self.config.maximum_supervised_tasks)?);
        Ok(LatticeService {
            config: self.config,
            backend,
            associations,
            messaging,
            endpoint,
            supervisor,
        })
    }
}

pub struct LatticeService {
    config: NodeConfig,
    backend: Arc<dyn RecipientBackend>,
    associations: Arc<AssociationManager>,
    messaging: Arc<OutboundMessaging>,
    endpoint: Arc<RemotingEndpoint>,
    supervisor: Arc<TaskSupervisor>,
}

impl LatticeService {
    pub fn builder(config: NodeConfig) -> Result<LatticeServiceBuilder, ServiceError> {
        LatticeServiceBuilder::new(config)
    }

    pub fn recipient<A: Actor>(
        &self,
        target: RecipientRef<A>,
        protocol: Arc<ActorProtocol<A>>,
    ) -> Result<BoundRecipient<A>, lattice_actor::recipient::RecipientError> {
        BoundRecipient::new(target, protocol, self.backend.clone())
    }

    pub fn associations(&self) -> &AssociationManager {
        &self.associations
    }

    pub fn messaging(&self) -> &OutboundMessaging {
        &self.messaging
    }

    pub fn supervisor(&self) -> &TaskSupervisor {
        &self.supervisor
    }

    pub async fn start(&self) -> Result<(), ServiceError> {
        self.endpoint.bind().await.map_err(ServiceError::Endpoint)
    }

    pub async fn connect_peer(&self, peer: NodeIdentity) -> Result<Arc<Association>, ServiceError> {
        self.endpoint
            .connect_peer(peer)
            .await
            .map_err(ServiceError::Endpoint)
    }

    pub async fn shutdown(&self) -> Result<(), ServiceError> {
        self.endpoint
            .shutdown()
            .await
            .map_err(ServiceError::Endpoint)?;
        self.supervisor.shutdown(self.config.shutdown_timeout).await
    }
}
