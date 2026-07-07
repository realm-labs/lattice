fn main() -> Result<(), Box<dyn std::error::Error>> {
    let includes = vec!["proto".into(), lattice_codegen::proto_include()];
    lattice_codegen::builder::configure().compile_protos(&["proto/world.proto"], &includes)?;
    Ok(())
}
