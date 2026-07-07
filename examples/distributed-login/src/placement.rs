use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_placement::cache::RouteCacheConfig;
use lattice_placement::control::TonicLogicControl;
use lattice_placement::coordinator::{PlacementCoordinator, PlacementRouteResolver};
use lattice_placement::endpoint::EndpointPool;
use lattice_placement::route::{ResolvingActorRefRpcCore, ResolvingRpcCore};
use lattice_placement::store::{InMemoryPlacementStore, PlacementPrefix};
use lattice_rpc::metadata::RpcClientContextFactory;

use crate::generated::GeneratedTonicEndpointTransport;
use crate::{GATEWAY_SERVICE, PLAYER_SERVICE, WORLD_SERVICE};

pub type DemoRpcCore = ResolvingRpcCore<
    PlacementRouteResolver<InMemoryPlacementStore, TonicLogicControl>,
    GeneratedTonicEndpointTransport,
>;
pub type DemoActorRefRpcCore = ResolvingActorRefRpcCore<
    PlacementRouteResolver<InMemoryPlacementStore, TonicLogicControl>,
    GeneratedTonicEndpointTransport,
>;

pub fn local_placement_store() -> InMemoryPlacementStore {
    InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/distributed-login"))
}

pub fn world_core(store: InMemoryPlacementStore, source_instance: InstanceId) -> DemoRpcCore {
    rpc_core(WORLD_SERVICE, GATEWAY_SERVICE, source_instance, store)
}

pub fn player_core(store: InMemoryPlacementStore, source_instance: InstanceId) -> DemoRpcCore {
    rpc_core(PLAYER_SERVICE, GATEWAY_SERVICE, source_instance, store)
}

pub fn actor_ref_core(
    source_service: ServiceKind,
    source_instance: InstanceId,
    store: InMemoryPlacementStore,
) -> DemoActorRefRpcCore {
    let coordinator = PlacementCoordinator::new(store.clone(), TonicLogicControl);
    DemoActorRefRpcCore::new(
        PlacementRouteResolver::new(
            GATEWAY_SERVICE,
            store,
            coordinator,
            RouteCacheConfig::default(),
        ),
        EndpointPool::new(),
        RpcClientContextFactory::new(source_service, source_instance),
        GeneratedTonicEndpointTransport::new(),
    )
}

fn rpc_core(
    service_kind: ServiceKind,
    source_service: ServiceKind,
    source_instance: InstanceId,
    store: InMemoryPlacementStore,
) -> DemoRpcCore {
    let coordinator = PlacementCoordinator::new(store.clone(), TonicLogicControl);
    let resolver = PlacementRouteResolver::new(
        service_kind.clone(),
        store,
        coordinator,
        RouteCacheConfig::default(),
    );
    DemoRpcCore::new(
        service_kind,
        resolver,
        EndpointPool::new(),
        RpcClientContextFactory::new(source_service, source_instance),
        GeneratedTonicEndpointTransport::new(),
    )
}
