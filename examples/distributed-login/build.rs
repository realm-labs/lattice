fn main() -> Result<(), Box<dyn std::error::Error>> {
    let includes = vec!["proto".into(), lattice_codegen::proto_include()];
    lattice_codegen::configure()
        .gateway_route_ids([
            (100, "game.WorldRpc.Login"),
            (101, "game.WorldRpc.WorldPing"),
            (200, "game.PlayerRpc.PlayerPing"),
        ])
        .compile_protos(&["proto/game.proto"], &includes)?;
    Ok(())
}
