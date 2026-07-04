fn main() -> Result<(), Box<dyn std::error::Error>> {
    let includes = vec!["proto".into(), lattice_codegen::proto_include()];
    lattice_codegen::configure()
        .gateway_route_ids([(100, "world.WorldRpc.EnterWorld")])
        .compile_protos(&["proto/world.proto"], &includes)?;
    Ok(())
}
