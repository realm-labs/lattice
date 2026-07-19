#[test]
fn invalid_persistence_models_fail_with_useful_diagnostics() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/*.rs");
}
