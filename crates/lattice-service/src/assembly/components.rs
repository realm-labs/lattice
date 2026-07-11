use tracing::debug;

use crate::components::{
    ErasedPlacementStore, ErasedPlacementStoreComponent, ErasedServiceComponent,
    ServiceComponentContext,
};
use crate::error::LatticeServiceError;

pub(crate) async fn build_placement_store(
    component: Box<dyn ErasedPlacementStoreComponent>,
    component_context: &ServiceComponentContext,
    service_context: &mut lattice_core::service_context::ServiceContextBuilder,
    service_kind: &str,
) -> Result<Box<dyn ErasedPlacementStore>, LatticeServiceError> {
    debug!(
        service.kind = service_kind,
        component.target = component.target_name(),
        component.type = component.type_name(),
        "building service component"
    );
    component.build(component_context, service_context).await
}

pub(crate) async fn build_framework_component_or_default(
    configured: Option<Box<dyn ErasedServiceComponent>>,
    default: Box<dyn ErasedServiceComponent>,
    component_context: &ServiceComponentContext,
    service_context: &mut lattice_core::service_context::ServiceContextBuilder,
    service_kind: &str,
) -> Result<(), LatticeServiceError> {
    build_service_component(
        configured.unwrap_or(default),
        component_context,
        service_context,
        service_kind,
    )
    .await
}

pub(crate) async fn build_service_component(
    component: Box<dyn ErasedServiceComponent>,
    component_context: &ServiceComponentContext,
    service_context: &mut lattice_core::service_context::ServiceContextBuilder,
    service_kind: &str,
) -> Result<(), LatticeServiceError> {
    debug!(
        service.kind = service_kind,
        component.target = component.target_name(),
        component.type = component.type_name(),
        "building service component"
    );
    component.build(component_context, service_context).await
}
