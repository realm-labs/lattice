mod actor;

use std::net::SocketAddr;
use std::sync::Arc;

use lattice_actor::{ActorRegistry, ActorRegistryConfig};
use lattice_core::InstanceId;
use lattice_rpc::ActorRefRpcClient;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::game::player_rpc_server::PlayerRpcServer;
use crate::generated::player_rpc;
use crate::placement::actor_ref_core;
use crate::player::actor::{PlayerActor, PlayerLoader};
use crate::{ExampleResult, PLAYER_ACTOR, PLAYER_SERVICE};

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
