use crate::rate_limit::{
    GatewayRequestContext, GatewayTowerPipeline, KeyedRateLimiter, RateLimitKey,
};
use crate::session::{GatewayPush, GatewaySessionRegistry};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use lattice_core::{ActorKind, RouteKey, actor_kind};
use lattice_rpc::{RoutedRequest, RpcError, RpcRequest, ShardedRpcCore};
use prost::Message as ProstMessage;
use tokio::net::{TcpListener, TcpStream};

use crate::*;

#[derive(Clone, PartialEq, prost::Message)]
struct EnterWorldRequest {
    #[prost(uint64, tag = "1")]
    world_id: u64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct EnterWorldReply {
    #[prost(bool, tag = "1")]
    ok: bool,
}

impl RoutedRequest for EnterWorldRequest {
    fn actor_kind(&self) -> ActorKind {
        actor_kind!("World")
    }

    fn route_key(&self) -> RouteKey {
        RouteKey::U64(self.world_id)
    }
}

impl RpcRequest for EnterWorldRequest {
    type Reply = EnterWorldReply;
    const METHOD: &'static str = "world.WorldRpc/EnterWorld";
}

#[derive(Clone, Default)]
struct FakeCore {
    routed: Arc<Mutex<Vec<RouteKey>>>,
}

#[async_trait]
impl ShardedRpcCore for FakeCore {
    async fn call<Req>(&self, req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest,
    {
        self.routed.lock().unwrap().push(req.route_key());
        Ok(Req::Reply::default())
    }
}

#[test]
fn binary_client_codec_decodes_and_encodes_frame() {
    let codec = BinaryClientCodec;
    let frame = codec.decode(&[0, 0, 0, 9, 1, 2, 3]).unwrap();

    assert_eq!(
        frame,
        ClientFrame {
            msg_id: 9,
            payload: vec![1, 2, 3]
        }
    );
    assert_eq!(codec.encode(frame).unwrap(), vec![0, 0, 0, 9, 1, 2, 3]);
}

#[tokio::test]
async fn gateway_tcp_server_serves_framed_client_requests_until_shutdown() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let server = GatewayTcpServer::new(listener, |frame: ClientFrame| async move {
        Ok(Some(ClientFrame {
            msg_id: frame.msg_id + 1,
            payload: frame.payload,
        }))
    })
    .ready_signal(ready_tx);

    let task = tokio::spawn(server.run_until_shutdown_signal(async {
        let _ = shutdown_rx.await;
    }));
    let addr = ready_rx.await.unwrap();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    write_client_frame(
        &mut stream,
        ClientFrame {
            msg_id: 41,
            payload: vec![1, 2, 3],
        },
    )
    .await
    .unwrap();
    let reply = read_client_frame(&mut stream).await.unwrap();

    assert_eq!(
        reply,
        ClientFrame {
            msg_id: 42,
            payload: vec![1, 2, 3],
        }
    );
    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[test]
fn gateway_route_table_rejects_duplicate_msg_id() {
    let binding = ProstClientMessageBinding::<EnterWorldRequest>::new(100);
    let mut table = GatewayRouteTable::new();

    table.register(binding.route_spec()).unwrap();
    let duplicate = table.register(binding.route_spec());

    assert_eq!(duplicate, Err(GatewayError::DuplicateRoute { msg_id: 100 }));
    assert_eq!(table.get(100).unwrap().method, EnterWorldRequest::METHOD);
}

#[tokio::test]
async fn generated_binding_decodes_payload_and_forwards_typed_request() {
    let binding = ProstClientMessageBinding::<EnterWorldRequest>::new(100);
    let core = FakeCore::default();
    let routed = core.routed.clone();
    let request = EnterWorldRequest { world_id: 42 };
    let frame = ClientFrame {
        msg_id: 100,
        payload: request.encode_to_vec(),
    };

    let reply_frame = binding.decode_and_forward(frame, core).await.unwrap();

    assert_eq!(reply_frame.msg_id, 100);
    assert_eq!(*routed.lock().unwrap(), vec![RouteKey::U64(42)]);
    let reply = EnterWorldReply::decode(reply_frame.payload.as_slice()).unwrap();
    assert!(!reply.ok);
}

#[test]
fn gateway_push_validates_session_id_and_connection_epoch() {
    let mut sessions = GatewaySessionRegistry::new();
    let first = sessions.connect("session-1");
    let second = sessions.connect("session-1");
    let push = GatewayPush {
        session: second.clone(),
        frame: ClientFrame {
            msg_id: 9,
            payload: Vec::new(),
        },
    };
    let stale = GatewayPush {
        session: first,
        frame: ClientFrame {
            msg_id: 9,
            payload: Vec::new(),
        },
    };

    assert_eq!(sessions.validate_push(&push), Ok(()));
    assert!(matches!(
        sessions.validate_push(&stale),
        Err(GatewayError::StaleSession { .. })
    ));
}

#[test]
fn keyed_rate_limiter_is_scoped_by_principal_session_and_rate_class() {
    let limiter = KeyedRateLimiter::new(1, Duration::from_secs(60));
    let key = RateLimitKey {
        principal_id: "player-1".into(),
        session_id: "session-1".into(),
        rate_class: "move".into(),
    };
    let other_class = RateLimitKey {
        rate_class: "chat".into(),
        ..key.clone()
    };

    assert_eq!(limiter.check(key.clone()), Ok(()));
    assert_eq!(limiter.check(key), Err(GatewayError::RateLimited));
    assert_eq!(limiter.check(other_class), Ok(()));
}

#[test]
fn gateway_pipeline_load_sheds_when_concurrency_limit_is_full() {
    let pipeline = GatewayTowerPipeline::new(KeyedRateLimiter::new(10, Duration::from_secs(60)), 1);
    let ctx = GatewayRequestContext {
        principal_id: "player-1".into(),
        session_id: "session-1".into(),
        rate_class: "move".into(),
    };

    let permit = pipeline.enter(ctx.clone()).unwrap();
    assert!(matches!(
        pipeline.enter(ctx.clone()),
        Err(GatewayError::LoadShed)
    ));
    drop(permit);

    assert!(pipeline.enter(ctx).is_ok());
}

#[test]
fn gateway_pipeline_applies_keyed_rate_limit_before_forwarding() {
    let pipeline = GatewayTowerPipeline::new(KeyedRateLimiter::new(1, Duration::from_secs(60)), 8);
    let ctx = GatewayRequestContext {
        principal_id: "player-1".into(),
        session_id: "session-1".into(),
        rate_class: "chat".into(),
    };

    let _permit = pipeline.enter(ctx.clone()).unwrap();

    assert!(matches!(
        pipeline.enter(ctx),
        Err(GatewayError::RateLimited)
    ));
}
