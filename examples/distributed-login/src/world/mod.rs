mod actor;

use std::net::SocketAddr;

use http::Uri;
use lattice_actor::{ActorRuntime, ActorSpawnOptions};
use lattice_core::{ActorId, InstanceId};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::ExampleResult;
use crate::game::world_rpc_server::WorldRpcServer;
use crate::generated::{player_rpc, world_rpc};
use crate::placement::player_core;
use crate::world::actor::WorldActor;

pub async fn run_world_service(
    listener: TcpListener,
    player_endpoint: Uri,
    ready: Option<oneshot::Sender<SocketAddr>>,
) -> ExampleResult<()> {
    let local_addr = listener.local_addr()?;
    let player_client =
        player_rpc::Client::new(player_core(player_endpoint, InstanceId::new("world-1")));
    let runtime = ActorRuntime::default();
    let world = runtime
        .spawn_actor(
            WorldActor::new(1, player_client),
            ActorSpawnOptions {
                scheduler_key: Some(ActorId::U64(1)),
                ..ActorSpawnOptions::default()
            },
        )
        .await?;
    let service = world_rpc::ActorService::new(world);
    if let Some(ready) = ready {
        let _ = ready.send(local_addr);
    }
    Server::builder()
        .add_service(WorldRpcServer::new(service))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;
    Ok(())
}
