fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .enum_attribute(
            ".lattice.placement.control.ServicePlacementWatchResponse.update",
            "#[allow(clippy::large_enum_variant)]",
        )
        .compile_protos(&["proto/lattice/placement/control.proto"], &["proto"])?;
    Ok(())
}
