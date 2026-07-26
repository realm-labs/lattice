use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
use lattice_core::{actor_kind, id::RouteKey};
use prost::Message as ProstMessage;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::{
    binding::{GatewayRecipient, ProstClientMessageBinding},
    config::GatewayServerConfig,
    error::GatewayError,
    frame::{BinaryClientCodec, ClientCodec, ClientFrame},
    route::{GatewayRouteContext, GatewayRouteSpec, MessageRouter, RouteDecision},
    server::{GatewayService, GatewayTcpServer, read_client_frame, write_client_frame},
};

#[derive(Clone, PartialEq, ProstMessage, lattice_actor::Request)]
#[request(response = Output)]
struct Input {
    #[prost(uint64, tag = "1")]
    id: u64,
}

#[derive(Clone, PartialEq, ProstMessage)]
struct Output {
    #[prost(uint64, tag = "1")]
    id: u64,
}

#[derive(Clone)]
struct FakeRecipient;

#[async_trait]
impl GatewayRecipient<Input> for FakeRecipient {
    async fn ask(&self, _route: RouteDecision, message: Input) -> Result<Output, GatewayError> {
        Ok(Output { id: message.id + 1 })
    }
}

struct Router;

impl MessageRouter for Router {
    fn route(
        &mut self,
        context: &GatewayRouteContext,
        route: &GatewayRouteSpec,
    ) -> Result<RouteDecision, GatewayError> {
        Ok(RouteDecision::new(
            route.actor_kind.clone(),
            context.require_route_key("id")?,
        ))
    }
}

#[tokio::test]
async fn prost_binding_forwards_to_actor_recipient() {
    let binding = ProstClientMessageBinding::<Input>::new(7, actor_kind!("Target"), "target/v1");
    let frame = ClientFrame {
        msg_id: 7,
        payload: Input { id: 41 }.encode_to_vec(),
    };
    let reply = binding
        .decode_and_forward(
            frame,
            FakeRecipient,
            &mut Router,
            &GatewayRouteContext::new().with_route_key("id", RouteKey::U64(41)),
        )
        .await
        .unwrap();
    assert_eq!(Output::decode(reply.payload.as_slice()).unwrap().id, 42);
}

#[test]
fn binary_client_codec_round_trips() {
    let frame = ClientFrame {
        msg_id: 9,
        payload: vec![1, 2, 3],
    };
    let encoded = BinaryClientCodec.encode(frame.clone()).unwrap();
    assert_eq!(BinaryClientCodec.decode(&encoded).unwrap(), frame);
}

struct TestGateway {
    address: SocketAddr,
    stop: oneshot::Sender<()>,
    handle: JoinHandle<Result<(), GatewayError>>,
}

impl TestGateway {
    async fn shutdown(self) {
        let _ = self.stop.send(());
        tokio::time::timeout(Duration::from_secs(5), self.handle)
            .await
            .expect("gateway shutdown timed out")
            .expect("gateway task panicked")
            .expect("gateway exited with an error");
    }
}

async fn start_gateway(config: GatewayServerConfig) -> TestGateway {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stop_rx) = oneshot::channel();
    let server = GatewayTcpServer::new(listener, |frame: ClientFrame| async move {
        if frame.msg_id == 0 {
            return Err(GatewayError::UnknownMessageId { msg_id: 0 });
        }
        Ok(Some(ClientFrame {
            msg_id: frame.msg_id + 1,
            payload: frame.payload,
        }))
    })
    .with_config(config);
    let handle = tokio::spawn(async move {
        server
            .run_until_shutdown_signal(async {
                let _ = stop_rx.await;
            })
            .await
    });
    TestGateway {
        address,
        stop,
        handle,
    }
}

async fn echo_round_trip(client: &mut TcpStream, msg_id: u32) {
    write_client_frame(
        client,
        ClientFrame {
            msg_id,
            payload: vec![9],
        },
    )
    .await
    .unwrap();
    assert_eq!(
        read_client_frame(client).await.unwrap(),
        ClientFrame {
            msg_id: msg_id + 1,
            payload: vec![9],
        }
    );
}

async fn assert_still_serving(address: SocketAddr) {
    let mut client = TcpStream::connect(address).await.unwrap();
    echo_round_trip(&mut client, 41).await;
}

async fn assert_closed_by_gateway(client: &mut TcpStream) {
    let mut trailing = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), client.read_to_end(&mut trailing))
        .await
        .expect("gateway did not close the connection")
        .unwrap();
    assert!(trailing.is_empty());
}

#[tokio::test]
async fn malformed_client_frame_closes_only_that_connection() {
    let gateway = start_gateway(GatewayServerConfig::default()).await;
    let mut client = TcpStream::connect(gateway.address).await.unwrap();
    client.write_u32(2).await.unwrap();
    client.write_all(&[0, 0]).await.unwrap();
    assert_closed_by_gateway(&mut client).await;
    drop(client);

    assert_still_serving(gateway.address).await;
    gateway.shutdown().await;
}

#[tokio::test]
async fn oversized_frame_length_closes_only_that_connection() {
    let gateway = start_gateway(GatewayServerConfig::default()).await;
    let mut client = TcpStream::connect(gateway.address).await.unwrap();
    client.write_u32(u32::MAX).await.unwrap();
    assert_closed_by_gateway(&mut client).await;
    drop(client);

    assert_still_serving(gateway.address).await;
    gateway.shutdown().await;
}

#[tokio::test]
async fn frame_handler_error_closes_only_that_connection() {
    let gateway = start_gateway(GatewayServerConfig::default()).await;
    let mut client = TcpStream::connect(gateway.address).await.unwrap();
    write_client_frame(
        &mut client,
        ClientFrame {
            msg_id: 0,
            payload: vec![],
        },
    )
    .await
    .unwrap();
    assert_closed_by_gateway(&mut client).await;
    drop(client);

    assert_still_serving(gateway.address).await;
    gateway.shutdown().await;
}

#[tokio::test]
async fn connection_reset_does_not_stop_the_gateway() {
    let gateway = start_gateway(GatewayServerConfig::default()).await;
    let mut client = TcpStream::connect(gateway.address).await.unwrap();
    #[allow(deprecated)]
    client.set_linger(Some(Duration::ZERO)).unwrap();
    client.write_u32(64).await.unwrap();
    client.write_all(&[1, 2, 3, 4]).await.unwrap();
    drop(client);
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_still_serving(gateway.address).await;
    gateway.shutdown().await;
}

#[tokio::test]
async fn connection_limit_drops_new_clients_without_stopping_the_gateway() {
    let gateway = start_gateway(GatewayServerConfig {
        max_connections: 1,
        ..GatewayServerConfig::default()
    })
    .await;
    let mut held = TcpStream::connect(gateway.address).await.unwrap();
    echo_round_trip(&mut held, 1).await;

    let mut rejected = TcpStream::connect(gateway.address).await.unwrap();
    assert_closed_by_gateway(&mut rejected).await;
    drop(rejected);

    echo_round_trip(&mut held, 3).await;
    drop(held);
    gateway.shutdown().await;
}

#[tokio::test]
async fn idle_timeout_closes_silent_connections() {
    let gateway = start_gateway(GatewayServerConfig {
        idle_timeout: Duration::from_millis(50),
        ..GatewayServerConfig::default()
    })
    .await;
    let mut client = TcpStream::connect(gateway.address).await.unwrap();
    assert_closed_by_gateway(&mut client).await;
    drop(client);

    assert_still_serving(gateway.address).await;
    gateway.shutdown().await;
}

#[tokio::test]
async fn shutdown_drains_open_connections_before_the_deadline() {
    let gateway = start_gateway(GatewayServerConfig {
        shutdown_drain_timeout: Duration::from_secs(60),
        ..GatewayServerConfig::default()
    })
    .await;
    let mut client = TcpStream::connect(gateway.address).await.unwrap();
    echo_round_trip(&mut client, 1).await;

    gateway.shutdown().await;
    let mut trailing = Vec::new();
    client.read_to_end(&mut trailing).await.unwrap();
    assert!(trailing.is_empty());
}

#[tokio::test]
async fn background_task_failure_stops_the_gateway() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let service = GatewayService::new(listener, |_socket: TcpStream, _peer: SocketAddr| async {
        Ok::<(), GatewayError>(())
    })
    .background_task("boom", async { Err(GatewayError::RateLimited) });
    assert_eq!(
        service.run().await.unwrap_err(),
        GatewayError::BackgroundTaskFailed {
            task: "boom".to_string(),
            error: GatewayError::RateLimited.to_string(),
        }
    );
}

#[tokio::test]
async fn invalid_configuration_is_rejected_before_serving() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server = GatewayTcpServer::new(listener, |frame: ClientFrame| async move {
        Ok::<_, GatewayError>(Some(frame))
    })
    .with_config(GatewayServerConfig {
        max_connections: 0,
        ..GatewayServerConfig::default()
    });
    let error = server
        .run_until_shutdown_signal(std::future::pending::<()>())
        .await
        .unwrap_err();
    assert!(matches!(error, GatewayError::InvalidConfig(_)));
}
