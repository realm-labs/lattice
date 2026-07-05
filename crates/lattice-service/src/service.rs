use std::future::Future;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router as AxumRouter;
use lattice_core::instance::InstanceCapacity;
use lattice_core::{ServiceContext, ServiceKind};
use lattice_placement::coordinator::PlacementWatchTask;
use lattice_placement::instance::{InstanceRecord, InstanceState};
use lattice_placement::store::LeaseId;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Router;
use tracing::{debug, error, info, warn};

use crate::actor::ErasedLogicActor;
use crate::component::ErasedPlacementStore;
use crate::config::InstanceConfig;
use crate::framework::{
    ClusterEventBusComponent, LocalEventBusComponent, ServiceSchedulerComponent,
};
use crate::{LatticeServiceBuilder, LatticeServiceError};

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
            ready,
        } = self;
        let local_addr = listener.local_addr()?;
        let lease_id = placement_store.grant_instance_lease().await?;
        placement_store.keepalive_instance_lease(lease_id).await?;
        publish_instance_record(
            placement_store.as_ref(),
            &service_kind,
            &instance,
            local_addr,
            InstanceState::Starting,
            lease_id,
        )
        .await?;
        publish_instance_record(
            placement_store.as_ref(),
            &service_kind,
            &instance,
            local_addr,
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
        let (admin_shutdown_tx, admin_task) = start_admin_http_server(admin_http);
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
                InstanceState::Draining,
                lease_id,
            )
            .await;
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
            let _ = server_shutdown_tx.send(());
            if let Some(admin_shutdown_tx) = admin_shutdown_tx {
                let _ = admin_shutdown_tx.send(());
            }
            result
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
        let service_exit = loop {
            tokio::select! {
                result = &mut lifecycle_shutdown, if !lifecycle_done => {
                    lifecycle_done = true;
                    if let Err(error) = result {
                        lifecycle_error = Some(error);
                    }
                }
                result = &mut keepalive => {
                    break ServiceExit::Keepalive(result);
                }
                result = &mut serve => break ServiceExit::Server(result),
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
                publish_instance_record(
                    placement_store.as_ref(),
                    &service_kind,
                    &instance,
                    local_addr,
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

async fn default_shutdown_signal() {
    #[cfg(unix)]
    {
        let ctrl_c = async {
            if let Err(error) = tokio::signal::ctrl_c().await {
                warn!(%error, "failed to listen for ctrl-c shutdown signal");
            }
        };
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sigterm) => {
                first_shutdown_signal(ctrl_c, async move {
                    let _ = sigterm.recv().await;
                })
                .await;
            }
            Err(error) => {
                warn!(%error, "failed to listen for sigterm shutdown signal");
                ctrl_c.await;
            }
        }
    }

    #[cfg(not(unix))]
    {
        if let Err(error) = tokio::signal::ctrl_c().await {
            warn!(%error, "failed to listen for ctrl-c shutdown signal");
        }
    }
}

pub(crate) async fn first_shutdown_signal<C, T>(ctrl_c: C, terminate: T)
where
    C: Future<Output = ()>,
    T: Future<Output = ()>,
{
    tokio::pin!(ctrl_c);
    tokio::pin!(terminate);
    tokio::select! {
        () = &mut ctrl_c => {}
        () = &mut terminate => {}
    }
}

type AdminShutdownSignal = oneshot::Sender<()>;
type AdminHttpTask = tokio::task::JoinHandle<Result<(), LatticeServiceError>>;

fn start_admin_http_server(
    admin_http: Option<AdminHttpServer>,
) -> (Option<AdminShutdownSignal>, Option<AdminHttpTask>) {
    let Some(admin_http) = admin_http else {
        return (None, None);
    };
    let local_addr = admin_http.listener.local_addr().ok();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        if let Some(local_addr) = local_addr {
            info!(%local_addr, "lattice admin http listening");
        }
        axum::serve(admin_http.listener, admin_http.router)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(LatticeServiceError::from)
    });
    (Some(shutdown_tx), Some(task))
}

async fn cancel_event_subscriptions(service_context: &ServiceContext) -> usize {
    let mut cancelled = 0;
    if let Some(component) = service_context.extension::<ClusterEventBusComponent>() {
        cancelled += component.cancel_owned_subscriptions().await;
    }
    if let Some(component) = service_context.extension::<LocalEventBusComponent>() {
        cancelled += component.cancel_owned_subscriptions().await;
    }
    cancelled
}

async fn shutdown_service_scheduler(service_context: &ServiceContext) {
    if let Some(component) = service_context.extension::<ServiceSchedulerComponent>() {
        component.scheduler().shutdown().await;
    }
}

async fn drain_runtime_actors(logic_actors: &[Arc<dyn ErasedLogicActor>]) -> usize {
    let mut drained = 0;
    for actor in logic_actors {
        drained += actor.drain().await;
    }
    drained
}

async fn publish_instance_record(
    placement_store: &dyn ErasedPlacementStore,
    service_kind: &ServiceKind,
    instance: &InstanceConfig,
    local_addr: SocketAddr,
    state: InstanceState,
    lease_id: LeaseId,
) -> Result<(), LatticeServiceError> {
    let endpoint = instance
        .advertised_endpoint
        .clone()
        .unwrap_or_else(|| socket_addr_to_uri(local_addr));
    let record = InstanceRecord {
        service_kind: service_kind.clone(),
        instance_id: instance.instance_id.clone(),
        lease_id,
        advertised_endpoint: endpoint.clone(),
        control_endpoint: endpoint,
        version: env!("CARGO_PKG_VERSION").to_string(),
        state,
        capacity: InstanceCapacity::default(),
        labels: Default::default(),
    };
    placement_store.upsert_instance(record).await?;
    Ok(())
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
    pub ready: Option<oneshot::Sender<SocketAddr>>,
}

#[derive(Debug)]
pub(crate) struct AdminHttpServer {
    pub listener: TcpListener,
    pub router: AxumRouter,
}

fn socket_addr_to_uri(addr: SocketAddr) -> http::Uri {
    http::Uri::from_str(&format!("http://{addr}")).expect("socket address URI should be valid")
}
