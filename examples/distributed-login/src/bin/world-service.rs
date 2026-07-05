use clap::Parser;
use distributed_login::world::run_world_service;
use http::Uri;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(about = "World logic service for the distributed login example")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:19081")]
    addr: String,
    #[arg(long, default_value = "http://127.0.0.1:19082")]
    player_endpoint: Uri,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();
    let args = Args::parse();
    let listener = TcpListener::bind(&args.addr).await?;
    run_world_service(listener, args.player_endpoint, None).await
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "distributed_login=info,lattice_rpc=info".to_string()),
        )
        .try_init();
}
