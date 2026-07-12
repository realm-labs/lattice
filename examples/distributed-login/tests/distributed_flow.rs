#[tokio::test]
async fn typed_login_flow_uses_actor_protocol() {
    let reply = distributed_login::run_demo().await.unwrap();
    assert!(reply.accepted);
}
