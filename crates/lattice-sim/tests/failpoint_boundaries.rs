use std::path::{Path, PathBuf};

use lattice_core::failpoint::Failpoint;

#[test]
fn every_named_failpoint_is_embedded_in_production_code() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let mut production = String::new();
    for crate_name in ["lattice-remoting", "lattice-placement", "lattice-service"] {
        collect_rust(
            &root.join("crates").join(crate_name).join("src"),
            &mut production,
        );
    }
    for point in Failpoint::ALL {
        let variant = format!("{point:?}");
        assert!(
            production.contains(&variant),
            "production code has no checkpoint for {}",
            point.name()
        );
    }
}

fn collect_rust(directory: &Path, output: &mut String) {
    for entry in std::fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_rust(&path, output);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            output.push_str(&std::fs::read_to_string(path).unwrap());
        }
    }
}
