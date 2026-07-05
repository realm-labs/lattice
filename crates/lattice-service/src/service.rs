use std::net::SocketAddr;

use lattice_core::ServiceKind;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Router;
use tracing::{error, info};

use crate::config::InstanceConfig;
use crate::{LatticeServiceBuilder, LatticeServiceError};

#[derive(Debug)]
pub struct LatticeService {
    service_kind: ServiceKind,
    instance: InstanceConfig,
    listener: TcpListener,
    router: Router,
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
        ready: Option<oneshot::Sender<SocketAddr>>,
    ) -> Self {
        Self {
            service_kind,
            instance,
            listener,
            router,
            ready,
        }
    }

    pub fn service_kind(&self) -> &ServiceKind {
        &self.service_kind
    }

    pub fn instance(&self) -> &InstanceConfig {
        &self.instance
    }

    pub async fn run_until_shutdown(self) -> Result<(), LatticeServiceError> {
        let local_addr = self.listener.local_addr()?;
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
}
