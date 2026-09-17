#[test]
fn actor_behavior_admission_rules_compile() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/actor_behavior/pass.rs");
    cases.compile_fail("tests/ui/actor_behavior/duplicate_pattern.rs");
    cases.compile_fail("tests/ui/actor_behavior/overlapping_or_pattern.rs");
}
