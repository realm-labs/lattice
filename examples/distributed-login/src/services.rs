use std::net::SocketAddr;
use std::sync::Arc;

use http::Uri;
use lattice_actor::{ActorRegistry, ActorRegistryConfig, ActorRuntime, ActorSpawnOptions};
use lattice_core::{ActorId, InstanceId};
use lattice_rpc::ActorRefRpcClient;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::actors::{PlayerActor, PlayerLoader, WorldActor};
use crate::game::player_rpc_server::PlayerRpcServer;
use crate::game::world_rpc_server::WorldRpcServer;
use crate::generated::{player_rpc, world_rpc};
use crate::placement::{actor_ref_core, player_core};
use crate::{PLAYER_ACTOR, PLAYER_SERVICE};

pub type ExampleResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub async fn run_player_service(
    listener: TcpListener,
    ready: Option<oneshot::Sender<SocketAddr>>,
) -> ExampleResult<()> {
    let local_addr = listener.local_addr()?;
    let registry = Arc::new(ActorRegistry::<PlayerActor>::new(
        PLAYER_ACTOR,
        ActorRegistryConfig::default(),
    ));
    let actor_ref_client =
        ActorRefRpcClient::new(actor_ref_core(PLAYER_SERVICE, InstanceId::new("player-1")));
    let service = player_rpc::RegistryService::new(registry, PlayerLoader::new(actor_ref_client));
    if let Some(ready) = ready {
        let _ = ready.send(local_addr);
    }
    Server::builder()
        .add_service(PlayerRpcServer::new(service))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;
    Ok(())
}

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
