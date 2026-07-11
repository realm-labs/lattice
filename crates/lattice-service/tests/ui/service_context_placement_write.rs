use lattice_core::service_context::ServiceContext;
use lattice_service::framework::context::ServiceContextExt;

async fn ordinary_service_cannot_write_placement(service: &ServiceContext) {
    let placement = service.placement_store();
    let _ = placement.grant_instance_lease().await;
}

fn main() {}
