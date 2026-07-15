#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let includes = vec!["proto".into(), lattice_codegen::proto_include()];
    lattice_codegen::builder::configure()
        .message_attribute(
            ".world.EnterWorldRequest",
            "#[derive(lattice_actor::Request)]",
        )
        .message_attribute(
            ".world.EnterWorldRequest",
            "#[request(response = EnterWorldReply)]",
        )
        .message_attribute(
            ".world.GetClockRequest",
            "#[derive(lattice_actor::Request)]",
        )
        .message_attribute(
            ".world.GetClockRequest",
            "#[request(response = GetClockReply)]",
        )
        .compile_messages(&["proto/world.proto"], &includes)?;
    Ok(())
}
