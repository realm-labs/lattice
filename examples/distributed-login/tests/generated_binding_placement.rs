use async_trait::async_trait;
use distributed_login::WORLD_ACTOR;
use distributed_login::generated::world_rpc;
use lattice_actor::error::ActorError;
use lattice_actor::traits::Actor;
use lattice_placement::error::PlacementError;
use lattice_service::clients::RpcServicePlacement;

#[derive(Debug)]
struct PlacementProbeActor;

#[async_trait]
impl Actor for PlacementProbeActor {
    type Error = ActorError;
}

#[test]
fn generated_bindings_require_an_explicit_valid_ingress_placement() {
    let explicit = world_rpc::Binding::for_explicit_actor::<PlacementProbeActor>(WORLD_ACTOR);
    assert_eq!(
        explicit.placement_mode(),
        RpcServicePlacement::ExplicitFenced
    );

    let virtual_sharded =
        world_rpc::Binding::for_virtual_sharded_actor::<PlacementProbeActor>(WORLD_ACTOR, 16)
            .unwrap();
    let RpcServicePlacement::VirtualShardFenced { mapper } = virtual_sharded.placement_mode()
    else {
        panic!("expected virtual-shard placement");
    };
    assert_eq!(mapper.shard_count(), 16);

    let invalid =
        world_rpc::Binding::for_virtual_sharded_actor::<PlacementProbeActor>(WORLD_ACTOR, 0)
            .unwrap_err();
    assert_eq!(invalid, PlacementError::InvalidShardCount);

    let static_local =
        world_rpc::Binding::for_static_local_actor_unfenced::<PlacementProbeActor>(WORLD_ACTOR);
    assert_eq!(
        static_local.placement_mode(),
        RpcServicePlacement::StaticLocalUnfenced
    );

    let singleton = world_rpc::SingletonBinding::for_actor::<PlacementProbeActor>();
    assert_eq!(
        singleton.placement_mode(),
        RpcServicePlacement::SingletonFenced
    );
}
