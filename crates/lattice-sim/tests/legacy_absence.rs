use std::path::{Path, PathBuf};

#[test]
fn framework_contains_no_legacy_transport_or_placement_surface() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let workspace = std::fs::read_to_string(root.join("Cargo.toml")).unwrap();
    for removed in ["crates/lattice-rpc", "crates/lattice-direct-link"] {
        assert!(!workspace.contains(removed), "workspace retains {removed}");
    }

    let mut files = Vec::new();
    collect_sources(&root.join("crates"), &mut files);
    collect_sources(&root.join("examples"), &mut files);
    for path in files {
        if path.ends_with("lattice-sim/tests/legacy_absence.rs")
            || path.to_string_lossy().contains("lattice-sim/tests/ui/")
        {
            continue;
        }
        let source = std::fs::read_to_string(&path).unwrap();
        for forbidden in [
            "ExplicitPlacement",
            "lattice_direct_link",
            "lattice_rpc",
            "ActorEpochFloor",
            "PlacementTombstone",
            "OpenLinkRequest",
        ] {
            assert!(
                !source.contains(forbidden),
                "{} retains forbidden symbol {forbidden}",
                path.display()
            );
        }
    }
}

#[test]
fn removed_public_apis_do_not_compile() {
    trybuild::TestCases::new().compile_fail("tests/ui/removed_apis.rs");
}

fn collect_sources(directory: &Path, output: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_sources(&path, output);
        } else if matches!(
            path.extension().and_then(|extension| extension.to_str()),
            Some("rs" | "toml" | "proto")
        ) {
            output.push(path);
        }
    }
}
