fn main() -> Result<(), Box<dyn std::error::Error>> {
    let includes = vec!["proto".into(), lattice_codegen::proto_include()];
    lattice_codegen::configure().compile_messages(&["proto/game.proto"], &includes)?;
    Ok(())
}
