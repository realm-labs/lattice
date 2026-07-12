#[test]
fn generated_protocols_have_distinct_explicit_ids() {
    let world = distributed_login::WorldProtocol::build().unwrap();
    let player = distributed_login::PlayerProtocol::build().unwrap();
    assert_ne!(world.protocol_id(), player.protocol_id());
}
