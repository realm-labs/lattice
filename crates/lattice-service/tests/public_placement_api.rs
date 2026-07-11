#[test]
fn service_context_exposes_only_the_read_placement_view() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/service_context_placement_write.rs");
    tests.compile_fail("tests/ui/service_context_placement_activate.rs");
    tests.compile_fail("tests/ui/service_context_placement_cas.rs");
    tests.compile_fail("tests/ui/service_context_placement_drain.rs");
}
