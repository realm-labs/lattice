#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let reply = distributed_login::run_demo().await?;
    println!("player workflow accepted={}", reply.accepted);
    Ok(())
}
