use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;

use lattice_core::direct_link::target::DirectLinkEndpoint;
use lattice_core::instance::InstanceCapacity;
use lattice_core::kind::ServiceKind;
use lattice_core::service_context::ServiceContext;
use lattice_placement::authority::PlacementAuthority;
use lattice_placement::error::PlacementError;
use lattice_placement::registry::{InstanceRecord, InstanceState};
use lattice_placement::storage::LeaseId;
use tracing::debug;

use crate::actors::registration::ErasedLogicActor;
use crate::components::ErasedPlacementStore;
use crate::config::InstanceConfig;
use crate::direct_links::DirectLinkServiceRuntime;
use crate::error::LatticeServiceError;
use crate::framework::event_bus::{ClusterEventBusComponent, LocalEventBusComponent};
use crate::framework::scheduler::ServiceSchedulerComponent;

pub(crate) async fn cancel_event_subscriptions(service_context: &ServiceContext) -> usize {
    let mut cancelled = 0;
    if let Some(component) = service_context.extension::<ClusterEventBusComponent>() {
        cancelled += component.cancel_owned_subscriptions().await;
    }
    if let Some(component) = service_context.extension::<LocalEventBusComponent>() {
        cancelled += component.cancel_owned_subscriptions().await;
    }
    cancelled
}

pub(crate) async fn shutdown_service_scheduler(service_context: &ServiceContext) {
    if let Some(component) = service_context.extension::<ServiceSchedulerComponent>() {
        component.scheduler().shutdown().await;
    }
}

pub(crate) async fn drain_runtime_actors(logic_actors: &[Arc<dyn ErasedLogicActor>]) -> usize {
    let mut drained = 0;
    for actor in logic_actors {
        drained += actor.drain().await;
    }
    drained
}

pub(crate) async fn drain_placement(
    placement_authority: &dyn PlacementAuthority,
    service_kind: &ServiceKind,
    instance: &InstanceConfig,
    expected_lease_id: LeaseId,
) -> Result<(), LatticeServiceError> {
    match placement_authority
        .drain_instance(
            service_kind.clone(),
            instance.instance_id.clone(),
            instance.incarnation.clone(),
            expected_lease_id,
        )
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

pub(crate) async fn drain_direct_links(
    runtime: Option<&DirectLinkServiceRuntime>,
) -> Result<usize, LatticeServiceError> {
    let Some(runtime) = runtime else {
        return Ok(0);
    };
    runtime
        .close_for_node_drain()
        .await
        .map_err(|error| LatticeServiceError::ComponentBuild {
            slot: "direct_links".to_string(),
            message: error.to_string(),
        })
}

pub(crate) async fn publish_instance_record(
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
        incarnation: instance.incarnation.clone(),
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

pub(crate) async fn transition_instance_state(
    placement_store: &dyn ErasedPlacementStore,
    service_kind: &ServiceKind,
    instance: &InstanceConfig,
    expected_lease_id: LeaseId,
    state: InstanceState,
) -> Result<(), LatticeServiceError> {
    placement_store
        .compare_and_set_instance_state(
            service_kind,
            &instance.instance_id,
            expected_lease_id,
            state,
        )
        .await?;
    Ok(())
}

fn socket_addr_to_uri(addr: SocketAddr) -> http::Uri {
    http::Uri::from_str(&format!("http://{addr}")).expect("socket address URI should be valid")
}
