#[test]
fn protocol_capabilities_are_checked_at_compile_time() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/client_protocol_without_actor.rs");
    tests.compile_fail("tests/ui/unsupported_protocol_message.rs");
    tests.compile_fail("tests/ui/missing_protocol_handler.rs");
}
