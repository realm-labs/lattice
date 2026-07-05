mod session_actor;

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use http::Uri;
use lattice_actor::registry::{ActorCreateContext, ActorRefConfig, ActorRegistryConfig};
use lattice_actor::{ActorError, ActorLoader, ActorRegistry, StopReason};
use lattice_core::{ActorId, ActorRef, InstanceId};
use lattice_gateway::{
    ClientFrame, GatewayConnectionHandler, GatewayError, GatewayRouteTable, GatewayService,
};
use lattice_placement::InMemoryPlacementStore;
use prost::Message as ProstMessage;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::game::{LoginRequest, gateway_push_rpc_server::GatewayPushRpcServer};
use crate::gateway::session_actor::GatewaySessionActor;
use crate::generated::{GatewayDispatcher, gateway_push_rpc, register_gateway_routes};
use crate::placement::{DemoRpcCore, player_core, world_core};
use crate::tcp::{read_client_frame, write_client_frame};
use crate::{ExampleResult, GATEWAY_SERVICE, GATEWAY_SESSION_ACTOR};

type DemoGatewayDispatcher = GatewayDispatcher<DemoRpcCore, DemoRpcCore>;

pub async fn run_gateway(
    client_listener: TcpListener,
    push_listener: TcpListener,
    placement_store: InMemoryPlacementStore,
    ready: Option<oneshot::Sender<SocketAddr>>,
) -> ExampleResult<()> {
    let local_addr = client_listener.local_addr()?;
    let push_addr = push_listener.local_addr()?;
    let mut route_table = GatewayRouteTable::new();
    register_gateway_routes(&mut route_table)?;

    let world_core = world_core(placement_store.clone(), InstanceId::new("gateway-1"));
    let player_core = player_core(placement_store, InstanceId::new("gateway-1"));
    let dispatcher = DemoGatewayDispatcher::new(world_core, player_core);
    let gateway_push_endpoint = format!("http://{push_addr}").parse::<Uri>()?;
    let sessions = GatewaySessions::new(gateway_push_endpoint);
    let push_service =
        gateway_push_rpc::RegistryService::new(sessions.registry(), GatewaySessionLoader);
    let service = GatewayService::new(
        client_listener,
        DemoGatewayConnectionHandler {
            dispatcher,
            sessions,
        },
    )
    .background_tonic_service(
        "gateway-push-rpc",
        push_listener,
        GatewayPushRpcServer::new(push_service),
    );
    let service = if let Some(ready) = ready {
        service.ready_signal(ready)
    } else {
        service
    };

    info!(%local_addr, %push_addr, "gateway listening");
    service.run().await?;
    Ok(())
}

#[derive(Clone)]
struct GatewaySessions {
    registry: Arc<ActorRegistry<GatewaySessionActor>>,
}

impl GatewaySessions {
    fn new(gateway_endpoint: Uri) -> Self {
        Self {
            registry: Arc::new(ActorRegistry::new(
                GATEWAY_SESSION_ACTOR,
                ActorRegistryConfig {
                    actor_ref: Some(ActorRefConfig {
                        service_kind: GATEWAY_SERVICE,
                        instance_id: InstanceId::new("gateway-1"),
                        endpoint: gateway_endpoint,
                        owner_epoch: None,
                    }),
                    ..ActorRegistryConfig::default()
                },
            )),
        }
    }

    fn registry(&self) -> Arc<ActorRegistry<GatewaySessionActor>> {
        self.registry.clone()
    }

    async fn register(
        &self,
        session_id: String,
        tx: mpsc::Sender<ClientFrame>,
    ) -> ExampleResult<ActorRef> {
        let actor_id = ActorId::Str(session_id.clone());
        if let Some(old) = self.registry.remove(&actor_id).await {
            old.stop(StopReason::Requested).await?;
        }
        let (self_ref_tx, self_ref_rx) = oneshot::channel();
        self.registry
            .start(
                actor_id.clone(),
                GatewaySessionActor::new(session_id, tx, self_ref_tx),
            )
            .await?;
        self_ref_rx
            .await
            .map_err(|_| "gateway session actor stopped before publishing self ref".into())
    }

    async fn unregister(&self, session_id: &str) {
        let handle = self
            .registry
            .remove(&ActorId::Str(session_id.to_string()))
            .await;
        if let Some(handle) = handle {
            let _ = handle.stop(StopReason::Requested).await;
        }
    }
}

#[derive(Clone)]
struct GatewaySessionLoader;

#[async_trait]
impl ActorLoader<GatewaySessionActor> for GatewaySessionLoader {
    async fn load(&self, ctx: ActorCreateContext) -> Result<GatewaySessionActor, ActorError> {
        Err(ActorError::new(format!(
            "gateway session actor {:?} is not running on this gateway",
            ctx.actor_id
        )))
    }
}

#[derive(Clone)]
struct DemoGatewayConnectionHandler {
    dispatcher: DemoGatewayDispatcher,
    sessions: GatewaySessions,
}

#[async_trait]
impl GatewayConnectionHandler for DemoGatewayConnectionHandler {
    async fn handle_connection(
        &self,
        socket: TcpStream,
        _peer: SocketAddr,
    ) -> Result<(), GatewayError> {
        handle_connection(socket, self.dispatcher.clone(), self.sessions.clone())
            .await
            .map_err(|error| GatewayError::Io(error.to_string()))
    }
}

async fn handle_connection(
    socket: TcpStream,
    dispatcher: DemoGatewayDispatcher,
    sessions: GatewaySessions,
) -> ExampleResult<()> {
    let (reader, writer) = socket.into_split();
    let (tx, rx) = mpsc::channel::<ClientFrame>(16);

    let read_task = tokio::spawn(read_client_messages(reader, tx, dispatcher, sessions));
    let write_task = tokio::spawn(write_client_messages(writer, rx));

    let read_result = read_task.await?;
    let write_result = write_task.await?;
    read_result?;
    write_result?;
    Ok(())
}

async fn read_client_messages(
    mut reader: OwnedReadHalf,
    tx: mpsc::Sender<ClientFrame>,
    dispatcher: DemoGatewayDispatcher,
    sessions: GatewaySessions,
) -> ExampleResult<()> {
    let mut registered_session = None::<String>;
    let result = read_client_messages_inner(
        &mut reader,
        &tx,
        dispatcher,
        sessions.clone(),
        &mut registered_session,
    )
    .await;

    if let Some(session_id) = registered_session {
        sessions.unregister(&session_id).await;
    }
    result
}

async fn read_client_messages_inner(
    reader: &mut OwnedReadHalf,
    tx: &mpsc::Sender<ClientFrame>,
    dispatcher: DemoGatewayDispatcher,
    sessions: GatewaySessions,
    registered_session: &mut Option<String>,
) -> ExampleResult<()> {
    loop {
        let frame = match read_client_frame(reader).await {
            Ok(frame) => frame,
            Err(crate::tcp::TcpFrameError::Io(error))
                if error.kind() == std::io::ErrorKind::UnexpectedEof =>
            {
                break;
            }
            Err(error) => return Err(Box::new(error)),
        };

        let response = match frame.msg_id {
            crate::LOGIN_MSG_ID => {
                let mut login = LoginRequest::decode(frame.payload.as_slice())?;
                let session_id = match login.gateway_session.as_ref() {
                    Some(actor_ref) => actor_ref
                        .actor_id
                        .as_ref()
                        .and_then(|actor_id| actor_id.kind.as_ref())
                        .and_then(|kind| match kind {
                            crate::lattice::actor::actor_id::Kind::Str(value) => {
                                Some(value.clone())
                            }
                            _ => None,
                        })
                        .unwrap_or_else(|| "anonymous".to_string()),
                    None => "anonymous".to_string(),
                };
                let gateway_session_ref = sessions.register(session_id.clone(), tx.clone()).await?;
                login.gateway_session = Some(gateway_session_ref.into());
                *registered_session = Some(session_id);
                let frame = ClientFrame {
                    msg_id: frame.msg_id,
                    payload: login.encode_to_vec(),
                };
                let _ack = dispatcher.dispatch(frame).await?;
                continue;
            }
            _ => dispatcher.dispatch(frame).await?,
        };
        tx.send(response)
            .await
            .map_err(|_| "gateway connection writer task is closed")?;
    }

    Ok(())
}

async fn write_client_messages(
    mut writer: OwnedWriteHalf,
    mut rx: mpsc::Receiver<ClientFrame>,
) -> Result<(), crate::tcp::TcpFrameError> {
    while let Some(frame) = rx.recv().await {
        write_client_frame(&mut writer, frame).await?;
    }
    Ok(())
}
