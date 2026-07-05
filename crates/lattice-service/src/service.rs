use std::future::Future;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lattice_core::instance::InstanceCapacity;
use lattice_core::{
    ActorKind, DirectLinkEndpoint, LinkCloseReason, LinkError, ServiceContext, ServiceKind,
};
use lattice_direct_link::transport::TcpDirectLinkWriter;
use lattice_direct_link::{
    DirectLinkConnection, DirectLinkInboundRouter, DirectLinkTransport, TcpDirectLinkConnection,
    TcpDirectLinkTransport,
};
use lattice_ops::admin::{
    AdminActorTarget, AdminApiError, AdminAuth, AdminHttpAdapter, AdminMutationHandler,
    AdminMutationReply, AdminSnapshot,
};
use lattice_placement::PlacementError;
use lattice_placement::coordinator::PlacementWatchTask;
use lattice_placement::instance::{InstanceRecord, InstanceState};
use lattice_placement::store::LeaseId;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::server::Router;
use tracing::{debug, error, info, warn};

use crate::actor::ErasedLogicActor;
use crate::component::ErasedPlacementStore;
use crate::config::{DirectLinkConfig, InstanceConfig};
use crate::direct_link::DirectLinkServiceRuntime;
use crate::framework::{
    ClusterEventBusComponent, DynPlacementStore, LocalEventBusComponent, ServiceContextExt,
    ServiceSchedulerComponent,
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
            let drained_direct_links = drain_direct_links(direct_link_runtime_for_drain.as_ref())?;
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

async fn start_admin_http_server(
    admin_http: Option<AdminHttpServer>,
    service_context: &ServiceContext,
    placement_store: &dyn ErasedPlacementStore,
    service_kind: &ServiceKind,
    instance_id: &lattice_core::InstanceId,
) -> Result<(Option<AdminShutdownSignal>, Option<AdminHttpTask>), LatticeServiceError> {
    let Some(admin_http) = admin_http else {
        return Ok((None, None));
    };
    let snapshot = build_admin_snapshot(
        placement_store,
        service_kind,
        instance_id,
        admin_http.actor_kinds,
    )
    .await?;
    let router = AdminHttpAdapter::new(admin_http.auth, snapshot)
        .with_mutation_handler(ServiceAdminMutations {
            service_kind: service_kind.clone(),
            placement_store: service_context.placement_store(),
        })
        .router();
    let local_addr = admin_http.listener.local_addr().ok();
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        if let Some(local_addr) = local_addr {
            info!(%local_addr, "lattice admin http listening");
        }
        axum::serve(admin_http.listener, router)
            .with_graceful_shutdown(async move {
                let _ = shutdown_rx.await;
            })
            .await
            .map_err(LatticeServiceError::from)
    });
    Ok((Some(shutdown_tx), Some(task)))
}

#[derive(Clone)]
struct ServiceAdminMutations {
    service_kind: ServiceKind,
    placement_store: Arc<dyn DynPlacementStore>,
}

impl std::fmt::Debug for ServiceAdminMutations {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServiceAdminMutations")
            .field("service_kind", &self.service_kind)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl AdminMutationHandler for ServiceAdminMutations {
    async fn drain_instance(
        &self,
        instance_id: lattice_core::InstanceId,
    ) -> Result<AdminMutationReply, AdminApiError> {
        let report = self
            .placement_store
            .drain_instance(self.service_kind.clone(), instance_id.clone())
            .await
            .map_err(|error| AdminApiError::MutationFailed {
                message: error.to_string(),
            })?;
        Ok(AdminMutationReply::accepted(format!(
            "drained {instance_id}: migrated {} actors and {} virtual shards",
            report.migrated_actors, report.migrated_virtual_shards
        )))
    }

    async fn retry_actor_stop(
        &self,
        _target: AdminActorTarget,
    ) -> Result<AdminMutationReply, AdminApiError> {
        Err(AdminApiError::MutationUnsupported {
            operation: "retry_actor_stop",
        })
    }

    async fn force_actor_stop(
        &self,
        _target: AdminActorTarget,
    ) -> Result<AdminMutationReply, AdminApiError> {
        Err(AdminApiError::MutationUnsupported {
            operation: "force_actor_stop",
        })
    }

    async fn migrate_actor(
        &self,
        _target: AdminActorTarget,
    ) -> Result<AdminMutationReply, AdminApiError> {
        Err(AdminApiError::MutationUnsupported {
            operation: "migrate_actor",
        })
    }
}

async fn build_admin_snapshot(
    placement_store: &dyn ErasedPlacementStore,
    service_kind: &ServiceKind,
    instance_id: &lattice_core::InstanceId,
    actor_kinds: Vec<ActorKind>,
) -> Result<AdminSnapshot, LatticeServiceError> {
    let instances = placement_store.list_instances(service_kind).await?;
    let actors = placement_store
        .list_actors()
        .await?
        .into_iter()
        .map(|(_version, record)| record)
        .collect();
    let virtual_shards = placement_store
        .list_virtual_shards_for_service(service_kind)
        .await?
        .into_iter()
        .map(|(_version, record)| record)
        .collect();
    let singletons = placement_store
        .list_singletons()
        .await?
        .into_iter()
        .map(|(_version, record)| record)
        .collect();
    Ok(AdminSnapshot::from_placement_records(
        service_kind.clone(),
        instance_id.clone(),
        actor_kinds,
        instances,
        actors,
        virtual_shards,
        singletons,
    ))
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

#[derive(Debug)]
struct ManagedDirectLinkListener {
    endpoint: DirectLinkEndpoint,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<Result<(), LatticeServiceError>>,
}

impl ManagedDirectLinkListener {
    fn endpoint(&self) -> DirectLinkEndpoint {
        self.endpoint.clone()
    }
}

async fn start_direct_link_listener(
    config: Option<DirectLinkConfig>,
    runtime: Option<DirectLinkServiceRuntime>,
) -> Result<Option<ManagedDirectLinkListener>, LatticeServiceError> {
    let Some(config) = config else {
        return Ok(None);
    };
    let listen_config =
        config
            .listen_config()
            .map_err(|message| LatticeServiceError::ComponentBuild {
                slot: "direct_links".to_string(),
                message,
            })?;
    let maintenance_interval = config.maintenance_interval_config();
    let transport = TcpDirectLinkTransport::new();
    let listener = transport.bind(listen_config).await.map_err(|error| {
        LatticeServiceError::ComponentBuild {
            slot: "direct_links".to_string(),
            message: error.to_string(),
        }
    })?;
    let endpoint = listener.local_endpoint();
    let inbound_router = runtime.map(|runtime| runtime.inbound_router());
    let (shutdown, mut shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut maintenance = tokio::time::interval(maintenance_interval);
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    debug!("direct-link listener shutting down");
                    return Ok(());
                }
                _ = maintenance.tick(), if inbound_router.is_some() => {
                    if let Some(inbound_router) = &inbound_router
                        && let Err(error) = inbound_router.close_idle_links_at(Instant::now())
                    {
                        warn!(%error, "direct-link idle maintenance failed");
                    }
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok(connection) => {
                            let inbound_router = inbound_router.clone();
                            let maintenance_interval = maintenance_interval;
                            tokio::spawn(async move {
                                handle_direct_link_connection(
                                    connection,
                                    inbound_router,
                                    maintenance_interval,
                                )
                                .await;
                            });
                        }
                        Err(error) => {
                            return Err(LatticeServiceError::ComponentBuild {
                                slot: "direct_links".to_string(),
                                message: error.to_string(),
                            });
                        }
                    }
                }
            }
        }
    });
    Ok(Some(ManagedDirectLinkListener {
        endpoint,
        shutdown,
        task,
    }))
}

async fn handle_direct_link_connection(
    connection: TcpDirectLinkConnection,
    inbound_router: Option<Arc<DirectLinkInboundRouter>>,
    maintenance_interval: Duration,
) {
    let Some(inbound_router) = inbound_router else {
        let mut connection = connection;
        let _ = connection.close().await;
        return;
    };

    let (mut reader, mut writer) = connection.split();
    let mut heartbeat = tokio::time::interval(maintenance_interval);
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if let Err(error) = write_due_direct_link_heartbeats(&mut writer, &inbound_router).await {
                    debug!(%error, "closing direct-link connection after heartbeat write failure");
                    let _ = writer.close().await;
                    return;
                }
            }
            frame = reader.read_frame() => {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        debug!(%error, "closing direct-link connection after read failure");
                        let _ = writer.close().await;
                        return;
                    }
                };
                if let Err(error) = inbound_router.process_frame(frame) {
                    warn!(%error, "closing direct-link connection after inbound delivery failure");
                    let _ = writer.close().await;
                    return;
                }
            }
        }
    }
}

async fn write_due_direct_link_heartbeats(
    writer: &mut TcpDirectLinkWriter,
    inbound_router: &DirectLinkInboundRouter,
) -> Result<(), LinkError> {
    for frame in inbound_router.heartbeat_frames_due_at(Instant::now()) {
        writer.write_frame(frame).await?;
    }
    Ok(())
}

async fn drain_placement(
    placement_store: &dyn ErasedPlacementStore,
    service_kind: &ServiceKind,
    instance: &InstanceConfig,
) -> Result<(), LatticeServiceError> {
    match placement_store
        .drain_instance(service_kind.clone(), instance.instance_id.clone())
        .await
    {
        Ok(report) => {
            debug!(
                service.kind = service_kind.as_str(),
                instance.id = instance.instance_id.as_str(),
                placement.actors.migrated = report.migrated_actors,
                placement.virtual_shards.migrated = report.migrated_virtual_shards,
                "drained placement ownership"
            );
            Ok(())
        }
        Err(PlacementError::NoReadyInstances) => {
            debug!(
                service.kind = service_kind.as_str(),
                instance.id = instance.instance_id.as_str(),
                "skipping placement migration because no replacement instance is ready"
            );
            Ok(())
        }
        Err(error) => Err(error.into()),
    }
}

fn drain_direct_links(
    runtime: Option<&DirectLinkServiceRuntime>,
) -> Result<usize, LatticeServiceError> {
    let Some(runtime) = runtime else {
        return Ok(0);
    };
    runtime
        .inbound_router()
        .close_active_links(LinkCloseReason::NodeDraining)
        .map_err(|error| LatticeServiceError::ComponentBuild {
            slot: "direct_links".to_string(),
            message: error.to_string(),
        })
}

async fn publish_instance_record(
    placement_store: &dyn ErasedPlacementStore,
    service_kind: &ServiceKind,
    instance: &InstanceConfig,
    local_addr: SocketAddr,
    direct_link_endpoint: Option<&DirectLinkEndpoint>,
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
        labels: direct_link_endpoint
            .map(|endpoint| {
                [("direct_link_endpoint".to_string(), endpoint.uri.to_string())]
                    .into_iter()
                    .collect()
            })
            .unwrap_or_default(),
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
    pub direct_link: Option<DirectLinkConfig>,
    pub direct_link_runtime: Option<DirectLinkServiceRuntime>,
    pub ready: Option<oneshot::Sender<SocketAddr>>,
}

#[derive(Debug)]
pub(crate) struct AdminHttpServer {
    pub listener: TcpListener,
    pub auth: AdminAuth,
    pub actor_kinds: Vec<ActorKind>,
}

fn socket_addr_to_uri(addr: SocketAddr) -> http::Uri {
    http::Uri::from_str(&format!("http://{addr}")).expect("socket address URI should be valid")
}
