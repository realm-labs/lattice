use std::net::SocketAddr;
use std::str::FromStr;

use lattice_core::instance::InstanceCapacity;
use lattice_core::{ServiceContext, ServiceKind};
use lattice_placement::instance::{InstanceRecord, InstanceState};
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
    ready: Option<oneshot::Sender<SocketAddr>>,
}

impl LatticeService {
    pub fn builder(service_kind: ServiceKind) -> LatticeServiceBuilder {
        LatticeServiceBuilder::new(service_kind)
    }

    pub(crate) fn new(
        service_kind: ServiceKind,
        instance: InstanceConfig,
        listener: TcpListener,
        router: Router,
        service_context: ServiceContext,
        placement_store: Box<dyn ErasedPlacementStore>,
        ready: Option<oneshot::Sender<SocketAddr>>,
    ) -> Self {
        Self {
            service_kind,
            instance,
            listener,
            router,
            service_context,
            placement_store,
            ready,
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
        self.publish_instance(local_addr, InstanceState::Ready)
            .await?;
        if let Some(ready) = self.ready {
            let _ = ready.send(local_addr);
        }

        info!(
            service.kind = self.service_kind.as_str(),
            instance.id = self.instance.instance_id.as_str(),
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
    ) -> Result<(), LatticeServiceError> {
        let endpoint = self
            .instance
            .advertised_endpoint
            .clone()
            .unwrap_or_else(|| socket_addr_to_uri(local_addr));
        let record = InstanceRecord {
            service_kind: self.service_kind.clone(),
            instance_id: self.instance.instance_id.clone(),
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

fn socket_addr_to_uri(addr: SocketAddr) -> http::Uri {
    http::Uri::from_str(&format!("http://{addr}")).expect("socket address URI should be valid")
}
