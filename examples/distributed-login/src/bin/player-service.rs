use clap::Parser;
use distributed_login::player::run_player_service;
use tokio::net::TcpListener;

#[derive(Debug, Parser)]
#[command(about = "Player logic service for the distributed login example")]
struct Args {
    #[arg(long, default_value = "127.0.0.1:19082")]
    addr: String,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();
    let args = Args::parse();
    let listener = TcpListener::bind(&args.addr).await?;
    run_player_service(listener, None).await
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "distributed_login=info,lattice_rpc=info".to_string()),
        )
        .try_init();
}
