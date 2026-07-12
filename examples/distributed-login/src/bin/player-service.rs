#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let reply = distributed_login::run_demo().await?;
    println!("player workflow accepted={}", reply.accepted);
    Ok(())
}
