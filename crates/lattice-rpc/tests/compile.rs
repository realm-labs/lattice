#[test]
fn generated_client_api_compiles() {
    let tests = trybuild::TestCases::new();
    tests.pass("tests/ui/generated_client_api.rs");
}

#[test]
fn missing_rpc_handler_fails_to_compile() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/missing_rpc_handler.rs");
}
