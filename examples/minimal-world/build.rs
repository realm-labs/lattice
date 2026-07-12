#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let includes = vec!["proto".into(), lattice_codegen::proto_include()];
    lattice_codegen::builder::configure().compile_messages(&["proto/world.proto"], &includes)?;
    Ok(())
}
