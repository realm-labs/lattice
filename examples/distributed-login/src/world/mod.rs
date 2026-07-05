mod actor;

use std::net::SocketAddr;
use std::sync::Arc;

use http::Uri;
use lattice_actor::{ActorRegistry, ActorRegistryConfig};
use lattice_core::InstanceId;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;

use crate::game::world_rpc_server::WorldRpcServer;
use crate::generated::world_rpc;
use crate::placement::player_core;
use crate::world::actor::{WorldActor, WorldLoader};
use crate::{ExampleResult, WORLD_ACTOR};

pub async fn run_world_service(
    listener: TcpListener,
    player_endpoint: Uri,
    ready: Option<oneshot::Sender<SocketAddr>>,
) -> ExampleResult<()> {
    let local_addr = listener.local_addr()?;
    let player_core = player_core(player_endpoint, InstanceId::new("world-1"));
    let registry = Arc::new(ActorRegistry::<WorldActor>::new(
        WORLD_ACTOR,
        ActorRegistryConfig::default(),
    ));
    let service = world_rpc::RegistryService::new(registry, WorldLoader::new(player_core));
    if let Some(ready) = ready {
        let _ = ready.send(local_addr);
    }
    Server::builder()
        .add_service(WorldRpcServer::new(service))
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await?;
    Ok(())
}
