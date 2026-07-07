use crate::rate_limit::{
    GatewayRequestContext, GatewayTowerPipeline, KeyedRateLimiter, RateLimitKey,
};
use crate::session::{GatewayPush, GatewaySessionRegistry};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use lattice_core::{ActorKind, RouteKey, actor_kind};
use lattice_rpc::{RoutedEnvelope, RoutedRequest, RpcError, RpcRequest, ShardedRpcCore};
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

#[derive(Clone, PartialEq, prost::Message)]
struct AllItemRequest {}

#[derive(Clone, PartialEq, prost::Message)]
struct AllItemReply {
    #[prost(uint32, tag = "1")]
    count: u32,
}

impl RpcRequest for AllItemRequest {
    type Reply = AllItemReply;
    const METHOD: &'static str = "item.ItemRpc/AllItem";
}

#[derive(Clone, PartialEq, prost::Message)]
struct LoginRequest {
    #[prost(string, tag = "1")]
    account: String,
}

#[derive(Clone, PartialEq, prost::Message)]
struct LoginReply {}

impl RpcRequest for LoginRequest {
    type Reply = LoginReply;
    const METHOD: &'static str = "login.LoginRpc/Login";
}

#[derive(Clone, Default)]
struct FakeCore {
    routed: Arc<Mutex<Vec<RouteKey>>>,
}

#[async_trait]
impl ShardedRpcCore for FakeCore {
    async fn call_routed<Req>(&self, envelope: RoutedEnvelope<Req>) -> Result<Req::Reply, RpcError>
    where
        Req: RpcRequest,
    {
        self.routed.lock().unwrap().push(envelope.route_key);
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

#[tokio::test]
async fn gateway_service_runs_connection_handler_and_background_task() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let background_started = Arc::new(tokio::sync::Semaphore::new(0));
    let background_started_task = background_started.clone();
    let server = GatewayService::new(listener, |mut socket: TcpStream, _peer| async move {
        let frame = read_client_frame(&mut socket).await?;
        write_client_frame(
            &mut socket,
            ClientFrame {
                msg_id: frame.msg_id + 10,
                payload: frame.payload,
            },
        )
        .await
    })
    .background_task("gateway-push-rpc", async move {
        background_started_task.add_permits(1);
        std::future::pending::<()>().await;
        Ok(())
    })
    .ready_signal(ready_tx);

    let task = tokio::spawn(server.run_until_shutdown_signal(async {
        let _ = shutdown_rx.await;
    }));
    let addr = ready_rx.await.unwrap();
    background_started.acquire().await.unwrap().forget();
    let mut stream = TcpStream::connect(addr).await.unwrap();

    write_client_frame(
        &mut stream,
        ClientFrame {
            msg_id: 30,
            payload: vec![4, 5],
        },
    )
    .await
    .unwrap();
    let reply = read_client_frame(&mut stream).await.unwrap();

    assert_eq!(
        reply,
        ClientFrame {
            msg_id: 40,
            payload: vec![4, 5],
        }
    );
    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn gateway_service_aborts_connection_tasks_before_shutdown_returns() {
    struct DropSignal(Option<tokio::sync::oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(tx) = self.0.take() {
                let _ = tx.send(());
            }
        }
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    let (dropped_tx, mut dropped_rx) = tokio::sync::oneshot::channel();
    let started = Arc::new(tokio::sync::Semaphore::new(0));
    let drop_sender = Arc::new(Mutex::new(Some(dropped_tx)));
    let handler_drop_sender = drop_sender.clone();
    let handler_started = started.clone();
    let server = GatewayService::new(listener, move |_socket: TcpStream, _peer| {
        let handler_drop_sender = handler_drop_sender.clone();
        let handler_started = handler_started.clone();
        async move {
            let _guard = DropSignal(handler_drop_sender.lock().unwrap().take());
            handler_started.add_permits(1);
            std::future::pending::<Result<(), GatewayError>>().await
        }
    })
    .ready_signal(ready_tx);

    let task = tokio::spawn(server.run_until_shutdown_signal(async {
        let _ = shutdown_rx.await;
    }));
    let addr = ready_rx.await.unwrap();
    let _stream = TcpStream::connect(addr).await.unwrap();
    started.acquire().await.unwrap().forget();

    shutdown_tx.send(()).unwrap();
    task.await.unwrap().unwrap();
    dropped_rx.try_recv().unwrap();
}

#[tokio::test]
async fn gateway_service_reports_background_task_failure() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let result = GatewayService::new(listener, |_socket: TcpStream, _peer| async { Ok(()) })
        .background_task("gateway-push-rpc", async {
            Err(GatewayError::Io("push listener closed".to_string()))
        })
        .run_until_shutdown_signal(std::future::pending::<()>())
        .await;

    assert_eq!(
        result,
        Err(GatewayError::BackgroundTaskFailed {
            task: "gateway-push-rpc".to_string(),
            error: "gateway io error: push listener closed".to_string(),
        })
    );
}

#[test]
fn gateway_route_table_rejects_duplicate_msg_id() {
    let binding = ProstClientMessageBinding::<EnterWorldRequest>::new(100);
    let mut table = GatewayRouteTable::new();

    table.register(binding.route_spec()).unwrap();
    let duplicate = table.register(binding.route_spec());

    assert_eq!(duplicate, Err(GatewayError::DuplicateRoute { msg_id: 100 }));
    assert_eq!(table.get(100).unwrap().method, EnterWorldRequest::METHOD);
    assert_eq!(
        table.get(100).unwrap().route_key_policy,
        GatewayRouteKeyPolicy::request_field("<routed-request>")
    );
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

#[tokio::test]
async fn generated_binding_forwards_empty_request_with_session_route_key() {
    let binding = ProstClientMessageBinding::<AllItemRequest>::with_context_route_key(
        101,
        actor_kind!("Player"),
        "session_actor",
    );
    let core = FakeCore::default();
    let routed = core.routed.clone();
    let frame = ClientFrame {
        msg_id: 101,
        payload: AllItemRequest::default().encode_to_vec(),
    };
    let context = GatewayRouteContext::new().with_route_key("session_actor", RouteKey::U64(42));

    let reply_frame = binding
        .decode_and_forward_with_context(frame, core, &context)
        .await
        .unwrap();

    assert_eq!(reply_frame.msg_id, 101);
    assert_eq!(*routed.lock().unwrap(), vec![RouteKey::U64(42)]);
    let reply = AllItemReply::decode(reply_frame.payload.as_slice()).unwrap();
    assert_eq!(reply.count, 0);
}

#[tokio::test]
async fn gateway_binding_routes_login_by_business_request_field_extractor() {
    let binding = ProstClientMessageBinding::<LoginRequest>::with_route_extractor(
        102,
        actor_kind!("Login"),
        GatewayRouteKeyPolicy::request_field("account"),
        |req, _context| Ok(RouteKey::Str(req.account.clone())),
    );
    let core = FakeCore::default();
    let routed = core.routed.clone();
    let frame = ClientFrame {
        msg_id: 102,
        payload: LoginRequest {
            account: "account-7".to_string(),
        }
        .encode_to_vec(),
    };

    let reply_frame = binding.decode_and_forward(frame, core).await.unwrap();

    assert_eq!(reply_frame.msg_id, 102);
    assert_eq!(
        binding.route_spec().route_key_policy,
        GatewayRouteKeyPolicy::request_field("account")
    );
    assert_eq!(
        *routed.lock().unwrap(),
        vec![RouteKey::Str("account-7".to_string())]
    );
}

#[tokio::test]
async fn generated_binding_reports_missing_session_route_key() {
    let binding = ProstClientMessageBinding::<AllItemRequest>::with_context_route_key(
        101,
        actor_kind!("Player"),
        "session_actor",
    );
    let frame = ClientFrame {
        msg_id: 101,
        payload: AllItemRequest::default().encode_to_vec(),
    };

    let error = binding
        .decode_and_forward_with_context(frame, FakeCore::default(), &GatewayRouteContext::new())
        .await
        .unwrap_err();

    assert_eq!(
        error,
        GatewayError::MissingRouteContextKey {
            key: "session_actor".to_string()
        }
    );
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
