use lattice_core::service_context::ServiceContext;
use lattice_service::framework::context::ServiceContextExt;

async fn ordinary_service_cannot_compare_and_put(
    service: &ServiceContext,
    key: lattice_placement::storage::ActorPlacementKey,
    record: lattice_placement::storage::ActorPlacementRecord,
) {
    let placement = service.placement_store();
    let _ = placement.compare_and_put_actor(key, None, record).await;
}

fn main() {}
