use std::sync::Arc;

use async_trait::async_trait;
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_core::service_context::ServiceContext;
use lattice_ops::admin::{
    AdminActorTarget, AdminApiError, AdminAuth, AdminHttpAdapter, AdminMutationHandler,
    AdminMutationReply, AdminSnapshot,
};
use lattice_placement::authority::PlacementAuthority;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tracing::info;

use crate::components::ErasedPlacementStore;
use crate::error::LatticeServiceError;
use crate::framework::context::ServiceContextExt;
use crate::framework::placement::DynPlacementStore;

pub(crate) type AdminShutdownSignal = oneshot::Sender<()>;
pub(crate) type AdminHttpTask = tokio::task::JoinHandle<Result<(), LatticeServiceError>>;

#[derive(Debug)]
pub(crate) struct AdminHttpServer {
    pub listener: TcpListener,
    pub auth: AdminAuth,
    pub actor_kinds: Vec<ActorKind>,
}

pub(crate) async fn start_admin_http_server(
    admin_http: Option<AdminHttpServer>,
    service_context: &ServiceContext,
    placement_store: &dyn ErasedPlacementStore,
    placement_authority: Arc<dyn PlacementAuthority>,
    service_kind: &ServiceKind,
    instance_id: &lattice_core::instance::InstanceId,
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
            placement_authority,
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
    placement_authority: Arc<dyn PlacementAuthority>,
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
        instance_id: lattice_core::instance::InstanceId,
    ) -> Result<AdminMutationReply, AdminApiError> {
        let record = self
            .placement_store
            .get_instance(&instance_id)
            .await
            .map_err(|error| AdminApiError::MutationFailed {
                message: error.to_string(),
            })?
            .filter(|record| record.service_kind == self.service_kind)
            .ok_or_else(|| AdminApiError::MutationFailed {
                message: format!("instance {instance_id} was not found"),
            })?;
        let report = self
            .placement_authority
            .drain_instance(
                self.service_kind.clone(),
                instance_id.clone(),
                record.incarnation,
                record.lease_id,
            )
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
    instance_id: &lattice_core::instance::InstanceId,
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
