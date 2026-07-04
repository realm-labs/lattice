use distributed_login::game::{
    LoginReply, LoginRequest, PlayerPingReply, PlayerPingRequest, WorldPingReply, WorldPingRequest,
};
use distributed_login::lattice::actor::{ActorId, ActorRef, actor_id};
use distributed_login::tcp::{decode_reply, read_client_frame, request_frame, write_client_frame};
use distributed_login::{LOGIN_MSG_ID, PLAYER_PING_MSG_ID, WORLD_PING_MSG_ID};
use tokio::net::TcpStream;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let gateway = arg_value("--gateway").unwrap_or_else(|| "127.0.0.1:19080".to_string());
    let command = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "login".to_string());
    match command.as_str() {
        "login" => login(&gateway).await?,
        "world-ping" => world_ping(&gateway).await?,
        "player-ping" => player_ping(&gateway).await?,
        _ => {
            eprintln!(
                "usage: client [login|world-ping|player-ping] [--gateway host:port] [--world-id n] [--player-id n]"
            );
            std::process::exit(2);
        }
    }
    Ok(())
}

async fn login(gateway: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let world_id = arg_u64("--world-id", 1);
    let player_id = arg_u64("--player-id", 42);
    let gateway_session_id =
        arg_value("--session-id").unwrap_or_else(|| format!("client-{player_id}"));
    let reply: LoginReply = send(
        gateway,
        LOGIN_MSG_ID,
        LoginRequest {
            world_id,
            player_id,
            token: "demo-token".to_string(),
            gateway_session: Some(session_ref(gateway_session_id)),
        },
    )
    .await?;
    println!(
        "login ok={} world_id={} player_id={} session_id={} message={}",
        reply.ok, reply.world_id, reply.player_id, reply.session_id, reply.message
    );
    Ok(())
}

async fn world_ping(gateway: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let world_id = arg_u64("--world-id", 1);
    let reply: WorldPingReply =
        send(gateway, WORLD_PING_MSG_ID, WorldPingRequest { world_id }).await?;
    println!(
        "world-ping ok={} world_id={} session_count={} message={}",
        reply.ok, reply.world_id, reply.session_count, reply.message
    );
    Ok(())
}

async fn player_ping(gateway: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let player_id = arg_u64("--player-id", 42);
    let reply: PlayerPingReply =
        send(gateway, PLAYER_PING_MSG_ID, PlayerPingRequest { player_id }).await?;
    println!(
        "player-ping ok={} player_id={} session_count={} message={}",
        reply.ok, reply.player_id, reply.session_count, reply.message
    );
    Ok(())
}

async fn send<Req, Reply>(
    gateway: &str,
    msg_id: u32,
    request: Req,
) -> Result<Reply, Box<dyn std::error::Error + Send + Sync>>
where
    Req: prost::Message,
    Reply: prost::Message + Default,
{
    let mut stream = TcpStream::connect(gateway).await?;
    write_client_frame(&mut stream, request_frame(msg_id, &request)).await?;
    let frame = read_client_frame(&mut stream).await?;
    Ok(decode_reply(frame, msg_id)?)
}

fn arg_u64(name: &str, default: u64) -> u64 {
    arg_value(name)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(default)
}

fn arg_value(name: &str) -> Option<String> {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == name {
            return args.next();
        }
    }
    None
}

fn session_ref(session_id: String) -> ActorRef {
    ActorRef {
        service_kind: String::new(),
        actor_kind: String::new(),
        actor_id: Some(ActorId {
            kind: Some(actor_id::Kind::Str(session_id)),
        }),
        target: None,
    }
}
