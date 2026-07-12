#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reply = distributed_login::run_demo().await?;
    println!(
        "login accepted={} message={}",
        reply.accepted, reply.message
    );
    Ok(())
}
