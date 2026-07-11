use lattice_core::service_context::ServiceContext;
use lattice_service::framework::context::ServiceContextExt;

async fn ordinary_service_cannot_activate(
    service: &ServiceContext,
    request: lattice_placement::coordination::actor::ActivateActorRequest,
) {
    let placement = service.placement_store();
    let _ = placement.activate_actor(request).await;
}

fn main() {}
