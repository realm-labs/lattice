use std::sync::Arc;

use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_placement::authority::{DevelopmentInProcessPlacementAuthority, PlacementAuthority};
use lattice_placement::control::TonicLogicControl;
use lattice_placement::endpoint::EndpointPool;
use lattice_placement::routing::cache::RouteCacheConfig;
use lattice_placement::routing::placement::PlacementRouteResolver;
use lattice_placement::routing::rpc::{ResolvingActorRefRpcCore, ResolvingRpcCore};
use lattice_placement::storage::PlacementPrefix;
use lattice_placement::storage::memory::InMemoryPlacementStore;
use lattice_rpc::metadata::RpcClientContextFactory;

use crate::generated::GeneratedTonicEndpointTransport;
use crate::{GATEWAY_SERVICE, PLAYER_SERVICE, WORLD_SERVICE};

pub type DemoRpcCore = ResolvingRpcCore<
    PlacementRouteResolver<InMemoryPlacementStore>,
    GeneratedTonicEndpointTransport,
>;
pub type DemoActorRefRpcCore = ResolvingActorRefRpcCore<
    PlacementRouteResolver<InMemoryPlacementStore>,
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
    let authority = development_authority(store.clone());
    DemoActorRefRpcCore::new(
        PlacementRouteResolver::new(
            GATEWAY_SERVICE,
            store,
            authority,
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
    let authority = development_authority(store.clone());
    let resolver = PlacementRouteResolver::new(
        service_kind.clone(),
        store,
        authority,
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

fn development_authority(store: InMemoryPlacementStore) -> Arc<dyn PlacementAuthority> {
    Arc::new(DevelopmentInProcessPlacementAuthority::new(
        store,
        TonicLogicControl,
    ))
}
