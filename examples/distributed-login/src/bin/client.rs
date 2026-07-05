use clap::{Args, Parser, Subcommand};
use distributed_login::game::{
    LoginReply, LoginRequest, PlayerPingReply, PlayerPingRequest, WorldPingReply, WorldPingRequest,
};
use distributed_login::lattice::actor::{ActorId, ActorRef, actor_id};
use distributed_login::tcp::{decode_reply, read_client_frame, request_frame, write_client_frame};
use distributed_login::{LOGIN_MSG_ID, PLAYER_PING_MSG_ID, WORLD_PING_MSG_ID};
use tokio::net::TcpStream;

#[derive(Debug, Parser)]
#[command(about = "Raw TCP client for the distributed login example")]
struct Cli {
    #[arg(long, default_value = "127.0.0.1:19080", global = true)]
    gateway: String,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Login(LoginArgs),
    WorldPing(WorldPingArgs),
    PlayerPing(PlayerPingArgs),
}

#[derive(Debug, Args)]
struct LoginArgs {
    #[arg(long, default_value_t = 1)]
    world_id: u64,
    #[arg(long, default_value_t = 42)]
    player_id: u64,
    #[arg(long)]
    session_id: Option<String>,
}

#[derive(Debug, Args)]
struct WorldPingArgs {
    #[arg(long, default_value_t = 1)]
    world_id: u64,
}

#[derive(Debug, Args)]
struct PlayerPingArgs {
    #[arg(long, default_value_t = 42)]
    player_id: u64,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();
    match cli.command.unwrap_or(Command::Login(LoginArgs {
        world_id: 1,
        player_id: 42,
        session_id: None,
    })) {
        Command::Login(args) => login(&cli.gateway, args).await?,
        Command::WorldPing(args) => world_ping(&cli.gateway, args).await?,
        Command::PlayerPing(args) => player_ping(&cli.gateway, args).await?,
    }
    Ok(())
}

async fn login(
    gateway: &str,
    args: LoginArgs,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let gateway_session_id = args
        .session_id
        .unwrap_or_else(|| format!("client-{}", args.player_id));
    let reply: LoginReply = send(
        gateway,
        LOGIN_MSG_ID,
        LoginRequest {
            world_id: args.world_id,
            player_id: args.player_id,
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

async fn world_ping(
    gateway: &str,
    args: WorldPingArgs,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let reply: WorldPingReply = send(
        gateway,
        WORLD_PING_MSG_ID,
        WorldPingRequest {
            world_id: args.world_id,
        },
    )
    .await?;
    println!(
        "world-ping ok={} world_id={} session_count={} message={}",
        reply.ok, reply.world_id, reply.session_count, reply.message
    );
    Ok(())
}

async fn player_ping(
    gateway: &str,
    args: PlayerPingArgs,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let reply: PlayerPingReply = send(
        gateway,
        PLAYER_PING_MSG_ID,
        PlayerPingRequest {
            player_id: args.player_id,
        },
    )
    .await?;
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
