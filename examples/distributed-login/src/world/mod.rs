mod actor;

use std::net::SocketAddr;

use http::Uri;
use lattice_core::InstanceId;
use lattice_service::{ActorRegistration, LatticeService};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::generated::{player_rpc, world_rpc};
use crate::placement::player_core;
use crate::world::actor::{WorldActor, WorldActorFactory};
use crate::{ExampleResult, WORLD_ACTOR, WORLD_SERVICE};

pub async fn run_world_service(
    listener: TcpListener,
    player_endpoint: Uri,
    ready: Option<oneshot::Sender<SocketAddr>>,
) -> ExampleResult<()> {
    let mut builder = LatticeService::builder(WORLD_SERVICE)
        .instance_id(InstanceId::new("world-1"))
        .listen(listener);
    if let Some(ready) = ready {
        builder = builder.ready_signal(ready);
    }
    let player_core = player_core(player_endpoint, InstanceId::new("world-1"));

    builder
        .extension::<crate::placement::DemoRpcCore, _>(player_core)
        .register_client::<player_rpc::Binding<(), crate::placement::DemoRpcCore>>()
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
