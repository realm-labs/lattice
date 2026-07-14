#[test]
fn actor_message_derives_compile() {
    let cases = trybuild::TestCases::new();
    cases.pass("tests/ui/derive/pass.rs");
    cases.compile_fail("tests/ui/derive/missing_response.rs");
    cases.compile_fail("tests/ui/derive/duplicate_response.rs");
    cases.compile_fail("tests/ui/derive/unknown_option.rs");
    cases.compile_fail("tests/ui/derive/invalid_response.rs");
}
