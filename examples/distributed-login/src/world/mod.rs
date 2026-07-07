mod actor;

use std::net::SocketAddr;

use lattice_core::instance::InstanceId;
use lattice_placement::store::InMemoryPlacementStore;
use lattice_service::actor::ActorRegistration;
use lattice_service::service::LatticeService;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::error::ExampleResult;
use crate::generated::{player_rpc, world_rpc};
use crate::world::actor::{WorldActor, WorldActorFactory};
use crate::{WORLD_ACTOR, WORLD_SERVICE};

pub async fn run_world_service(
    listener: TcpListener,
    placement_store: InMemoryPlacementStore,
    ready: Option<oneshot::Sender<SocketAddr>>,
) -> ExampleResult<()> {
    let mut builder = LatticeService::builder(WORLD_SERVICE)
        .instance_id(InstanceId::new("world-1"))
        .listen(listener);
    if let Some(ready) = ready {
        builder = builder.ready_signal(ready);
    }

    builder
        .placement_store::<InMemoryPlacementStore, _>(placement_store)
        .register_client::<player_rpc::Binding>()
        .register_actor(
            ActorRegistration::builder(WORLD_ACTOR)
                .factory(WorldActorFactory::new())
                .build(),
        )
        .register_sharded_rpc(world_rpc::Binding::for_actor::<WorldActor>(WORLD_ACTOR))
        .build()
        .await?
        .run_until_shutdown()
        .await?;
    Ok(())
}
