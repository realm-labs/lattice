use clap::Parser;
use distributed_login::gateway::run_gateway;
use http::Uri;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(about = "Gateway process for the distributed login example")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:19080")]
    addr: String,
    #[arg(long, default_value = "127.0.0.1:19083")]
    push_addr: String,
    #[arg(long, default_value = "http://127.0.0.1:19081")]
    world_endpoint: Uri,
    #[arg(long, default_value = "http://127.0.0.1:19082")]
    player_endpoint: Uri,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();
    let args = Args::parse();
    let listener = TcpListener::bind(&args.addr).await?;
    let push_listener = TcpListener::bind(&args.push_addr).await?;
    run_gateway(
        listener,
        push_listener,
        args.world_endpoint,
        args.player_endpoint,
        None,
    )
    .await
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "distributed_login=info,lattice_rpc=info".to_string()),
        )
        .try_init();
}
