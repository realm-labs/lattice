use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use lattice_core::kind::ServiceKind;
use lattice_core::service_context::ServiceContext;
use lattice_placement::registry::InstanceState;
use lattice_placement::routing::placement::PlacementWatchTask;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Router;
use tracing::{debug, error, info};

use crate::actors::registration::ErasedLogicActor;
use crate::assembly::builder::LatticeServiceBuilder;
use crate::components::ErasedPlacementStore;
use crate::config::{DirectLinkConfig, InstanceConfig};
use crate::direct_links::DirectLinkServiceRuntime;
use crate::error::LatticeServiceError;
use crate::runtime::admin::{AdminHttpServer, start_admin_http_server};
use crate::runtime::direct_link_listener::{ManagedDirectLinkListener, start_direct_link_listener};
use crate::runtime::drain::{
    cancel_event_subscriptions, drain_direct_links, drain_placement, drain_runtime_actors,
    publish_instance_record, shutdown_service_scheduler,
};
use crate::runtime::shutdown::default_shutdown_signal;

#[derive(Debug)]
pub struct LatticeService {
    service_kind: ServiceKind,
    instance: InstanceConfig,
    listener: TcpListener,
    router: Router,
    service_context: ServiceContext,
    logic_actors: Vec<Arc<dyn ErasedLogicActor>>,
    placement_store: Box<dyn ErasedPlacementStore>,
    placement_watch_tasks: Vec<PlacementWatchTask>,
    admin_http: Option<AdminHttpServer>,
    instance_lease_keepalive_interval: Duration,
    direct_link: Option<DirectLinkConfig>,
    direct_link_runtime: Option<DirectLinkServiceRuntime>,
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
            logic_actors: parts.logic_actors,
            placement_store: parts.placement_store,
            placement_watch_tasks: parts.placement_watch_tasks,
            admin_http: parts.admin_http,
            instance_lease_keepalive_interval: parts.instance_lease_keepalive_interval,
            direct_link: parts.direct_link,
            direct_link_runtime: parts.direct_link_runtime,
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

    pub fn placement_watch_count(&self) -> usize {
        self.placement_watch_tasks.len()
    }

    pub fn direct_link_runtime(&self) -> Option<DirectLinkServiceRuntime> {
        self.direct_link_runtime.clone()
    }

    pub async fn run_until_shutdown(self) -> Result<(), LatticeServiceError> {
        self.run_until_shutdown_signal(default_shutdown_signal())
            .await
    }

    pub async fn run_until_shutdown_signal<F>(self, shutdown: F) -> Result<(), LatticeServiceError>
    where
        F: Future<Output = ()>,
    {
        let LatticeService {
            service_kind,
            instance,
            listener,
            router,
            service_context,
            logic_actors,
            placement_store,
            placement_watch_tasks,
            admin_http,
            instance_lease_keepalive_interval,
            direct_link,
            direct_link_runtime,
            ready,
        } = self;
        let local_addr = listener.local_addr()?;
        let direct_link_runtime_for_drain = direct_link_runtime.clone();
        let direct_link_listener =
            start_direct_link_listener(direct_link, direct_link_runtime).await?;
        let direct_link_endpoint = direct_link_listener
            .as_ref()
            .map(ManagedDirectLinkListener::endpoint);
        let lease_id = placement_store.grant_instance_lease().await?;
        placement_store.keepalive_instance_lease(lease_id).await?;
        publish_instance_record(
            placement_store.as_ref(),
            &service_kind,
            &instance,
            local_addr,
            direct_link_endpoint.as_ref(),
            InstanceState::Starting,
            lease_id,
        )
        .await?;
        publish_instance_record(
            placement_store.as_ref(),
            &service_kind,
            &instance,
            local_addr,
            direct_link_endpoint.as_ref(),
            InstanceState::Ready,
            lease_id,
        )
        .await?;
        if let Some(ready) = ready {
            let _ = ready.send(local_addr);
        }

        info!(
            service.kind = service_kind.as_str(),
            instance.id = instance.instance_id.as_str(),
            placement.watches = placement_watch_tasks.len(),
            %local_addr,
            "lattice service listening"
        );

        let (server_shutdown_tx, server_shutdown_rx) = oneshot::channel::<()>();
        let (direct_link_shutdown_tx, direct_link_task) = match direct_link_listener {
            Some(listener) => (Some(listener.shutdown), Some(listener.task)),
            None => (None, None),
        };
        let (admin_shutdown_tx, admin_task) = start_admin_http_server(
            admin_http,
            &service_context,
            placement_store.as_ref(),
            &service_kind,
            &instance.instance_id,
        )
        .await?;
        let keepalive = async {
            loop {
                tokio::time::sleep(instance_lease_keepalive_interval).await;
                placement_store.keepalive_instance_lease(lease_id).await?;
                placement_store
                    .keepalive_singleton_owner_leases(&service_kind, &instance.instance_id)
                    .await?;
            }
        };
        let lifecycle_shutdown = async {
            shutdown.await;
            let result = publish_instance_record(
                placement_store.as_ref(),
                &service_kind,
                &instance,
                local_addr,
                direct_link_endpoint.as_ref(),
                InstanceState::Draining,
                lease_id,
            )
            .await;
            let _ = server_shutdown_tx.send(());
            if let Some(direct_link_shutdown_tx) = direct_link_shutdown_tx {
                let _ = direct_link_shutdown_tx.send(());
            }
            let drained_direct_links =
                drain_direct_links(direct_link_runtime_for_drain.as_ref()).await?;
            debug!(
                service.kind = service_kind.as_str(),
                instance.id = instance.instance_id.as_str(),
                direct_links.drained = drained_direct_links,
                "drained direct links"
            );
            let placement_drain =
                drain_placement(placement_store.as_ref(), &service_kind, &instance).await;
            let cancelled_subscriptions = cancel_event_subscriptions(&service_context).await;
            debug!(
                service.kind = service_kind.as_str(),
                instance.id = instance.instance_id.as_str(),
                event.subscriptions.cancelled = cancelled_subscriptions,
                "cancelled runtime-owned event subscriptions"
            );
            shutdown_service_scheduler(&service_context).await;
            let drained_actors = drain_runtime_actors(&logic_actors).await;
            debug!(
                service.kind = service_kind.as_str(),
                instance.id = instance.instance_id.as_str(),
                actor.registries.drained = drained_actors,
                "drained runtime actor registries"
            );
            if let Some(admin_shutdown_tx) = admin_shutdown_tx {
                let _ = admin_shutdown_tx.send(());
            }
            result?;
            placement_drain
        };
        tokio::pin!(keepalive);
        tokio::pin!(lifecycle_shutdown);
        let serve =
            router.serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                let _ = server_shutdown_rx.await;
            });
        tokio::pin!(serve);
        let mut lifecycle_done = false;
        let mut lifecycle_error = None;
        let mut serve_result = None;
        let service_exit = loop {
            tokio::select! {
                result = &mut lifecycle_shutdown, if !lifecycle_done => {
                    lifecycle_done = true;
                    if let Err(error) = result {
                        lifecycle_error = Some(error);
                    }
                    if let Some(result) = serve_result.take() {
                        break ServiceExit::Server(result);
                    }
                }
                result = &mut keepalive => {
                    break ServiceExit::Keepalive(result);
                }
                result = &mut serve, if serve_result.is_none() => {
                    if lifecycle_done {
                        break ServiceExit::Server(result);
                    }
                    serve_result = Some(result);
                }
            }
        };
        if let Some(error) = lifecycle_error {
            return Err(error);
        }

        let serve_result = match service_exit {
            ServiceExit::Server(result) => result,
            ServiceExit::Keepalive(result) => return result,
        };

        match serve_result {
            Ok(()) => {
                if let Some(admin_task) = admin_task {
                    match admin_task.await {
                        Ok(result) => result?,
                        Err(error) => {
                            return Err(LatticeServiceError::ComponentBuild {
                                slot: "admin_http".to_string(),
                                message: error.to_string(),
                            });
                        }
                    }
                }
                if let Some(direct_link_task) = direct_link_task {
                    match direct_link_task.await {
                        Ok(result) => result?,
                        Err(error) => {
                            return Err(LatticeServiceError::ComponentBuild {
                                slot: "direct_links".to_string(),
                                message: error.to_string(),
                            });
                        }
                    }
                }
                publish_instance_record(
                    placement_store.as_ref(),
                    &service_kind,
                    &instance,
                    local_addr,
                    direct_link_endpoint.as_ref(),
                    InstanceState::Stopping,
                    lease_id,
                )
                .await?;
                info!(
                    service.kind = service_kind.as_str(),
                    instance.id = instance.instance_id.as_str(),
                    "lattice service stopped"
                );
                Ok(())
            }
            Err(error) => {
                error!(
                    service.kind = service_kind.as_str(),
                    instance.id = instance.instance_id.as_str(),
                    %error,
                    "lattice service failed"
                );
                Err(error.into())
            }
        }
    }
}

enum ServiceExit {
    Server(Result<(), tonic::transport::Error>),
    Keepalive(Result<(), LatticeServiceError>),
}

pub(crate) struct LatticeServiceParts {
    pub service_kind: ServiceKind,
    pub instance: InstanceConfig,
    pub listener: TcpListener,
    pub router: Router,
    pub service_context: ServiceContext,
    pub logic_actors: Vec<Arc<dyn ErasedLogicActor>>,
    pub placement_store: Box<dyn ErasedPlacementStore>,
    pub placement_watch_tasks: Vec<PlacementWatchTask>,
    pub admin_http: Option<AdminHttpServer>,
    pub instance_lease_keepalive_interval: Duration,
    pub direct_link: Option<DirectLinkConfig>,
    pub direct_link_runtime: Option<DirectLinkServiceRuntime>,
    pub ready: Option<oneshot::Sender<SocketAddr>>,
}
