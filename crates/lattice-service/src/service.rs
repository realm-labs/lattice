use std::net::SocketAddr;
use std::str::FromStr;

use lattice_core::instance::InstanceCapacity;
use lattice_core::{ServiceContext, ServiceKind};
use lattice_placement::coordinator::PlacementWatchTask;
use lattice_placement::instance::{InstanceRecord, InstanceState};
use lattice_placement::store::LeaseId;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Router;
use tracing::{error, info};

use crate::component::ErasedPlacementStore;
use crate::config::InstanceConfig;
use crate::{LatticeServiceBuilder, LatticeServiceError};

#[derive(Debug)]
pub struct LatticeService {
    service_kind: ServiceKind,
    instance: InstanceConfig,
    listener: TcpListener,
    router: Router,
    service_context: ServiceContext,
    placement_store: Box<dyn ErasedPlacementStore>,
    placement_watch_tasks: Vec<PlacementWatchTask>,
    ready: Option<oneshot::Sender<SocketAddr>>,
}

impl LatticeService {
    pub fn builder(service_kind: ServiceKind) -> LatticeServiceBuilder {
        LatticeServiceBuilder::new(service_kind)
    }

    pub(crate) fn new(parts: LatticeServiceParts) -> Self {
        Self {
            service_kind: parts.service_kind,
            instance: parts.instance,
            listener: parts.listener,
            router: parts.router,
            service_context: parts.service_context,
            placement_store: parts.placement_store,
            placement_watch_tasks: parts.placement_watch_tasks,
            ready: parts.ready,
        }
    }

    pub fn service_kind(&self) -> &ServiceKind {
        &self.service_kind
    }

    pub fn instance(&self) -> &InstanceConfig {
        &self.instance
    }

    pub fn context(&self) -> &ServiceContext {
        &self.service_context
    }

    pub async fn run_until_shutdown(self) -> Result<(), LatticeServiceError> {
        let local_addr = self.listener.local_addr()?;
        let lease_id = self.placement_store.grant_instance_lease().await?;
        self.placement_store
            .keepalive_instance_lease(lease_id)
            .await?;
        self.publish_instance(local_addr, InstanceState::Ready, lease_id)
            .await?;
        if let Some(ready) = self.ready {
            let _ = ready.send(local_addr);
        }

        info!(
            service.kind = self.service_kind.as_str(),
            instance.id = self.instance.instance_id.as_str(),
            placement.watches = self.placement_watch_tasks.len(),
            %local_addr,
            "lattice service listening"
        );

        match self
            .router
            .serve_with_incoming(TcpListenerStream::new(self.listener))
            .await
        {
            Ok(()) => {
                info!(
                    service.kind = self.service_kind.as_str(),
                    instance.id = self.instance.instance_id.as_str(),
                    "lattice service stopped"
                );
                Ok(())
            }
            Err(error) => {
                error!(
                    service.kind = self.service_kind.as_str(),
                    instance.id = self.instance.instance_id.as_str(),
                    %error,
                    "lattice service failed"
                );
                Err(error.into())
            }
        }
    }

    pub(crate) async fn publish_instance(
        &self,
        local_addr: SocketAddr,
        state: InstanceState,
        lease_id: LeaseId,
    ) -> Result<(), LatticeServiceError> {
        let endpoint = self
            .instance
            .advertised_endpoint
            .clone()
            .unwrap_or_else(|| socket_addr_to_uri(local_addr));
        let record = InstanceRecord {
            service_kind: self.service_kind.clone(),
            instance_id: self.instance.instance_id.clone(),
            lease_id,
            advertised_endpoint: endpoint.clone(),
            control_endpoint: endpoint,
            version: env!("CARGO_PKG_VERSION").to_string(),
            state,
            capacity: InstanceCapacity::default(),
            labels: Default::default(),
        };
        self.placement_store.upsert_instance(record).await?;
        Ok(())
    }
}

pub(crate) struct LatticeServiceParts {
    pub service_kind: ServiceKind,
    pub instance: InstanceConfig,
    pub listener: TcpListener,
    pub router: Router,
    pub service_context: ServiceContext,
    pub placement_store: Box<dyn ErasedPlacementStore>,
    pub placement_watch_tasks: Vec<PlacementWatchTask>,
    pub ready: Option<oneshot::Sender<SocketAddr>>,
}

fn socket_addr_to_uri(addr: SocketAddr) -> http::Uri {
    http::Uri::from_str(&format!("http://{addr}")).expect("socket address URI should be valid")
}
