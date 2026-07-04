use std::fs;
use std::path::Path;

#[test]
fn actor_handle_public_api_does_not_expose_tokio_join_handle() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let handle_source =
        fs::read_to_string(manifest_dir.join("src/handle.rs")).expect("handle source should read");

    assert!(
        !handle_source.contains("JoinHandle"),
        "ActorHandle must not expose or store Tokio JoinHandle"
    );
}
