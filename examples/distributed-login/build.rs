#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::error::Error;

fn main() -> Result<(), Box<dyn Error>> {
    let includes = vec!["proto".into(), lattice_codegen::proto_include()];
    lattice_codegen::builder::configure()
        .message_attribute(".game.LoginRequest", "#[derive(lattice_actor::Request)]")
        .message_attribute(
            ".game.LoginRequest",
            "#[request(response = LoginAcceptedReply)]",
        )
        .message_attribute(
            ".game.InitSessionRequest",
            "#[derive(lattice_actor::Request)]",
        )
        .message_attribute(
            ".game.InitSessionRequest",
            "#[request(response = InitSessionReply)]",
        )
        .compile_messages(&["proto/game.proto"], &includes)?;
    Ok(())
}
