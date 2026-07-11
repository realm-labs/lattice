use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_core::service_context::ServiceContext;
use lattice_service::framework::context::ServiceContextExt;

async fn ordinary_service_cannot_drain(service: &ServiceContext) {
    let placement = service.placement_store();
    let _ = placement
        .drain_instance(ServiceKind::new("World"), InstanceId::new("world-a"))
        .await;
}

fn main() {}
