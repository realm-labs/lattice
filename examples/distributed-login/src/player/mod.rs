mod actor;

use std::net::SocketAddr;

use lattice_core::InstanceId;
use lattice_rpc::ActorRefRpcClient;
use lattice_service::{ActorRegistration, LatticeService};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::generated::{gateway_push_rpc, player_rpc};
use crate::placement::actor_ref_core;
use crate::player::actor::{PlayerActor, PlayerActorFactory};
use crate::{ExampleResult, PLAYER_ACTOR, PLAYER_SERVICE};

pub async fn run_player_service(
    listener: TcpListener,
    ready: Option<oneshot::Sender<SocketAddr>>,
) -> ExampleResult<()> {
    let mut builder = LatticeService::builder(PLAYER_SERVICE)
        .instance_id(InstanceId::new("player-1"))
        .listen(listener);
    if let Some(ready) = ready {
        builder = builder.ready_signal(ready);
    }
    let actor_ref_client =
        ActorRefRpcClient::new(actor_ref_core(PLAYER_SERVICE, InstanceId::new("player-1")));

    builder
        .register_actor(
            ActorRegistration::builder(PLAYER_ACTOR)
                .factory(PlayerActorFactory::new(actor_ref_client))
                .build(),
        )
        .register_sharded_rpc(player_rpc::Binding::for_actor::<PlayerActor>(PLAYER_ACTOR))
        .register_client::<gateway_push_rpc::Binding>()
        .build()
        .await?
        .run_until_shutdown()
        .await?;
    Ok(())
}
