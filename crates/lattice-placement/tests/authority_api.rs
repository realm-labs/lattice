#[test]
fn raw_etcd_floor_deletion_is_not_a_public_runtime_api() {
    let tests = trybuild::TestCases::new();
    tests.compile_fail("tests/ui/raw_etcd_transport.rs");
    tests.compile_fail("tests/ui/raw_etcd_floor_delete.rs");
    tests.compile_fail("tests/ui/raw_etcd_store_constructor.rs");
}
