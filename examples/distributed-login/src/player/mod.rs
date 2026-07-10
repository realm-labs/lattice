mod actor;

use std::net::SocketAddr;

use lattice_core::instance::InstanceId;
use lattice_placement::storage::memory::InMemoryPlacementStore;
use lattice_rpc::client::ActorRefRpcClient;
use lattice_service::actors::registration::ActorRegistration;
use lattice_service::runtime::service::LatticeService;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::error::ExampleResult;
use crate::generated::player_rpc;
use crate::placement::actor_ref_core;
use crate::player::actor::{PlayerActor, PlayerActorFactory};
use crate::{PLAYER_ACTOR, PLAYER_SERVICE};

pub async fn run_player_service(
    listener: TcpListener,
    placement_store: InMemoryPlacementStore,
    ready: Option<oneshot::Sender<SocketAddr>>,
) -> ExampleResult<()> {
    let mut builder = LatticeService::builder(PLAYER_SERVICE)
        .instance_id(InstanceId::new("player-1"))
        .listen(listener);
    if let Some(ready) = ready {
        builder = builder.ready_signal(ready);
    }
    let actor_ref_client = ActorRefRpcClient::new(actor_ref_core(
        PLAYER_SERVICE,
        InstanceId::new("player-1"),
        placement_store.clone(),
    ));

    builder
        .placement_store::<InMemoryPlacementStore, _>(placement_store)
        .register_actor(
            ActorRegistration::builder(PLAYER_ACTOR)
                .factory(PlayerActorFactory::new(actor_ref_client))
                .build(),
        )
        .register_sharded_rpc(player_rpc::Binding::for_explicit_actor::<PlayerActor>(
            PLAYER_ACTOR,
        ))
        .build()
        .await?
        .run_until_shutdown()
        .await?;
    Ok(())
}
