use distributed_login::services::run_world_service;
use http::Uri;
use tokio::net::TcpListener;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    init_tracing();
    let addr = arg_value("--addr").unwrap_or_else(|| "127.0.0.1:19081".to_string());
    let player_endpoint = arg_value("--player-endpoint")
        .unwrap_or_else(|| "http://127.0.0.1:19082".to_string())
        .parse::<Uri>()?;
    let listener = TcpListener::bind(&addr).await?;
    run_world_service(listener, player_endpoint, None).await
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

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "distributed_login=info,lattice_rpc=info".to_string()),
        )
        .try_init();
}
