use http::Uri;
use lattice_core::InstanceId;
use lattice_placement::cache::RouteCacheConfig;
use lattice_placement::static_resolver::{
    StaticPlacementConfig, StaticRouteRange, StaticRouteResolver,
};
use lattice_placement::{EndpointPool, ResolvingActorRefRpcCore, ResolvingRpcCore};
use lattice_rpc::{RouteTarget, RpcClientContextFactory};

use crate::generated::GeneratedTonicEndpointTransport;
use crate::{PLAYER_ACTOR, PLAYER_SERVICE, WORLD_ACTOR, WORLD_SERVICE};

pub type DemoRpcCore = ResolvingRpcCore<StaticRouteResolver, GeneratedTonicEndpointTransport>;
pub type DemoActorRefRpcCore =
    ResolvingActorRefRpcCore<StaticRouteResolver, GeneratedTonicEndpointTransport>;

pub fn world_core(world_endpoint: Uri, source_instance: InstanceId) -> DemoRpcCore {
    DemoRpcCore::new(
        WORLD_SERVICE,
        resolver(WORLD_SERVICE, WORLD_ACTOR, "world-1", world_endpoint),
        EndpointPool::new(),
        RpcClientContextFactory::new(crate::WORLD_SERVICE, source_instance),
        GeneratedTonicEndpointTransport::new(),
    )
}

pub fn player_core(player_endpoint: Uri, source_instance: InstanceId) -> DemoRpcCore {
    DemoRpcCore::new(
        PLAYER_SERVICE,
        resolver(PLAYER_SERVICE, PLAYER_ACTOR, "player-1", player_endpoint),
        EndpointPool::new(),
        RpcClientContextFactory::new(crate::WORLD_SERVICE, source_instance),
        GeneratedTonicEndpointTransport::new(),
    )
}

pub fn actor_ref_core(
    source_service: lattice_core::ServiceKind,
    source_instance: InstanceId,
) -> DemoActorRefRpcCore {
    DemoActorRefRpcCore::new(
        StaticRouteResolver::new(
            StaticPlacementConfig { ranges: Vec::new() },
            RouteCacheConfig::default(),
        ),
        EndpointPool::new(),
        RpcClientContextFactory::new(source_service, source_instance),
        GeneratedTonicEndpointTransport::new(),
    )
}

fn resolver(
    service_kind: lattice_core::ServiceKind,
    actor_kind: lattice_core::ActorKind,
    instance: &str,
    endpoint: Uri,
) -> StaticRouteResolver {
    StaticRouteResolver::new(
        StaticPlacementConfig {
            ranges: vec![StaticRouteRange {
                service_kind: service_kind.clone(),
                actor_kind: actor_kind.clone(),
                start_inclusive: 0,
                end_exclusive: u64::MAX,
                target: RouteTarget {
                    service_kind,
                    instance_id: InstanceId::new(instance),
                    advertised_endpoint: endpoint,
                    owner_epoch: None,
                },
            }],
        },
        RouteCacheConfig::default(),
    )
}
