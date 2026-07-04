use std::path::Path;
use std::process::Command;

#[test]
fn repository_is_dedicated_framework_workspace() {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let workspace_manifest = manifest_dir.join("../../Cargo.toml");

    let output = Command::new(env!("CARGO"))
        .args([
            "metadata",
            "--no-deps",
            "--format-version",
            "1",
            "--manifest-path",
        ])
        .arg(&workspace_manifest)
        .output()
        .expect("cargo metadata should run");

    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let metadata = String::from_utf8(output.stdout).expect("metadata should be utf8");
    for crate_name in ["lattice-core", "lattice-config", "lattice-actor"] {
        assert!(
            metadata.contains(&format!(r#""name":"{crate_name}""#)),
            "workspace metadata should include {crate_name}"
        );
    }

    assert!(
        !metadata.contains(r#""name":"lattice","#),
        "root lattice crate should not contain framework implementation"
    );
}
