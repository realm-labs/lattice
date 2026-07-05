use distributed_login::game::{
    LoginReply, LoginRequest, PlayerPingReply, PlayerPingRequest, WorldPingReply, WorldPingRequest,
};
use distributed_login::gateway::run_gateway;
use distributed_login::lattice::actor::{ActorId, ActorRef, actor_id};
use distributed_login::player::run_player_service;
use distributed_login::tcp::{decode_reply, read_client_frame, request_frame, write_client_frame};
use distributed_login::world::run_world_service;
use distributed_login::{
    GATEWAY_SERVICE, GATEWAY_SESSION_ACTOR, LOGIN_MSG_ID, PLAYER_PING_MSG_ID, WORLD_PING_MSG_ID,
};
use lattice_core::{ActorId as CoreActorId, ActorRef as CoreActorRef, ActorRefTarget, InstanceId};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raw_tcp_gateway_drives_world_and_player_login_flow() {
    let gateway_a_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_a_push_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_b_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let gateway_b_addr = gateway_b_listener.local_addr().unwrap();
    let gateway_b_push_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();

    let player_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let player_addr = player_listener.local_addr().unwrap();
    let (player_ready_tx, player_ready_rx) = oneshot::channel();
    let player_task = tokio::spawn(run_player_service(player_listener, Some(player_ready_tx)));
    player_ready_rx.await.unwrap();

    let world_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let world_addr = world_listener.local_addr().unwrap();
    let player_endpoint = format!("http://{player_addr}").parse().unwrap();
    let (world_ready_tx, world_ready_rx) = oneshot::channel();
    let world_task = tokio::spawn(run_world_service(
        world_listener,
        player_endpoint,
        Some(world_ready_tx),
    ));
    world_ready_rx.await.unwrap();

    let world_endpoint: http::Uri = format!("http://{world_addr}").parse().unwrap();
    let player_endpoint: http::Uri = format!("http://{player_addr}").parse().unwrap();
    let (gateway_a_ready_tx, gateway_a_ready_rx) = oneshot::channel();
    let gateway_a_task = tokio::spawn(run_gateway(
        gateway_a_listener,
        gateway_a_push_listener,
        world_endpoint.clone(),
        player_endpoint.clone(),
        Some(gateway_a_ready_tx),
    ));
    gateway_a_ready_rx.await.unwrap();

    let (gateway_b_ready_tx, gateway_b_ready_rx) = oneshot::channel();
    let gateway_b_task = tokio::spawn(run_gateway(
        gateway_b_listener,
        gateway_b_push_listener,
        world_endpoint,
        player_endpoint,
        Some(gateway_b_ready_tx),
    ));
    gateway_b_ready_rx.await.unwrap();

    let login: LoginReply = send(
        gateway_b_addr,
        LOGIN_MSG_ID,
        LoginRequest {
            world_id: 1,
            player_id: 42,
            token: "test-token".to_string(),
            gateway_session: Some(session_ref("test-session-42")),
        },
    )
    .await;
    assert!(login.ok);
    assert_eq!(login.world_id, 1);
    assert_eq!(login.player_id, 42);
    assert_eq!(login.session_id, "world-1-player-42");

    let world_ping: WorldPingReply = send(
        gateway_b_addr,
        WORLD_PING_MSG_ID,
        WorldPingRequest { world_id: 1 },
    )
    .await;
    assert!(world_ping.ok);
    assert_eq!(world_ping.session_count, 1);

    let player_ping: PlayerPingReply = send(
        gateway_b_addr,
        PLAYER_PING_MSG_ID,
        PlayerPingRequest { player_id: 42 },
    )
    .await;
    assert!(player_ping.ok);
    assert_eq!(player_ping.session_count, 1);

    gateway_a_task.abort();
    gateway_b_task.abort();
    world_task.abort();
    player_task.abort();
}

#[test]
fn generated_actor_ref_proto_round_trips_core_actor_ref() {
    let core = CoreActorRef::direct(
        GATEWAY_SERVICE,
        GATEWAY_SESSION_ACTOR,
        CoreActorId::Str("session-1".to_string()),
        InstanceId::new("gateway-a"),
        "http://127.0.0.1:19083".parse().unwrap(),
        None,
    );

    let proto: ActorRef = core.clone().into();
    let decoded = CoreActorRef::try_from(proto).unwrap();

    assert_eq!(decoded.service_kind, core.service_kind);
    assert_eq!(decoded.actor_kind, core.actor_kind);
    assert_eq!(decoded.actor_id, core.actor_id);
    assert!(matches!(decoded.target, ActorRefTarget::Direct { .. }));
}

async fn send<Req, Reply>(gateway_addr: std::net::SocketAddr, msg_id: u32, request: Req) -> Reply
where
    Req: prost::Message,
    Reply: prost::Message + Default,
{
    let mut stream = TcpStream::connect(gateway_addr).await.unwrap();
    write_client_frame(&mut stream, request_frame(msg_id, &request))
        .await
        .unwrap();
    let frame = read_client_frame(&mut stream).await.unwrap();
    decode_reply(frame, msg_id).unwrap()
}

fn session_ref(session_id: &str) -> ActorRef {
    ActorRef {
        service_kind: String::new(),
        actor_kind: String::new(),
        actor_id: Some(ActorId {
            kind: Some(actor_id::Kind::Str(session_id.to_string())),
        }),
        target: None,
    }
}
