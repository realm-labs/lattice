use std::net::SocketAddr;

use lattice_core::ServiceKind;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Router;

use crate::{InstanceConfig, LatticeServiceBuilder, LatticeServiceError};

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

        self.router
            .serve_with_incoming(TcpListenerStream::new(self.listener))
            .await?;
        Ok(())
    }
}
