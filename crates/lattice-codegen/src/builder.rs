use std::fs;
use std::path::{Path, PathBuf};

use crate::error::CodegenError;
use crate::render::render_actor_protocol;
use crate::spec::ActorProtocolSpec;

#[derive(Debug, Clone, Default)]
pub struct ActorProtocolCodegen {
    out_dir: Option<PathBuf>,
    protocols: Vec<ActorProtocolSpec>,
    message_attributes: Vec<(String, String)>,
}

pub fn configure() -> ActorProtocolCodegen {
    ActorProtocolCodegen::default()
}

impl ActorProtocolCodegen {
    pub fn out_dir(mut self, path: impl AsRef<Path>) -> Self {
        self.out_dir = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn actor_protocol(mut self, protocol: ActorProtocolSpec) -> Self {
        self.protocols.push(protocol);
        self
    }

    /// Adds an attribute to matching Protobuf message types.
    ///
    /// The path matching behavior is the same as
    /// [`prost_build::Config::message_attribute`].
    pub fn message_attribute(
        mut self,
        path: impl Into<String>,
        attribute: impl Into<String>,
    ) -> Self {
        self.message_attributes
            .push((path.into(), attribute.into()));
        self
    }

    pub fn compile_messages<PF, PI>(
        self,
        proto_files: &[PF],
        includes: &[PI],
    ) -> Result<(), CodegenError>
    where
        PF: AsRef<Path>,
        PI: AsRef<Path>,
    {
        let out_dir = self
            .out_dir
            .clone()
            .or_else(|| std::env::var_os("OUT_DIR").map(PathBuf::from))
            .ok_or(CodegenError::MissingOutDir)?;
        fs::create_dir_all(&out_dir)
            .map_err(|error| CodegenError::WriteGenerated(error.to_string()))?;
        let mut config = prost_build::Config::new();
        config.out_dir(&out_dir);
        for (path, attribute) in &self.message_attributes {
            config.message_attribute(path, attribute);
        }
        config
            .compile_protos(proto_files, includes)
            .map_err(|error| CodegenError::ProtoCompile(error.to_string()))?;

        let mut generated = String::new();
        for protocol in &self.protocols {
            generated.push_str(&render_actor_protocol(protocol)?);
            generated.push('\n');
        }
        fs::write(out_dir.join("lattice.protocols.rs"), generated)
            .map_err(|error| CodegenError::WriteGenerated(error.to_string()))?;
        for proto_file in proto_files {
            println!("cargo:rerun-if-changed={}", proto_file.as_ref().display());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::spec::{InteractionMode, ProtocolMessageSpec};

    #[test]
    fn generated_output_targets_actor_protocol_macro_only() {
        let spec = ActorProtocolSpec {
            visibility: "pub".to_owned(),
            registrar_name: "PlayerProtocol".to_owned(),
            protocol_id: 7,
            protocol_name: "player/v1".to_owned(),
            messages: vec![ProtocolMessageSpec {
                message_id: 1,
                message_type: "GetProfile".to_owned(),
                mode: InteractionMode::Ask,
                request_schema_version: 1,
                response_schema_version: Some(1),
                request_codec: "PostcardCodec".to_owned(),
                response_codec: Some("PostcardCodec".to_owned()),
            }],
        };
        let generated = render_actor_protocol(&spec).unwrap();
        assert!(generated.contains("lattice_actor::actor_protocol!"));
        assert!(!generated.contains("tonic"));
        assert!(!generated.contains("DirectLink"));
    }

    #[test]
    fn message_attributes_are_retained_for_prost_configuration() {
        let codegen = configure()
            .message_attribute(".game.LoginRequest", "#[derive(lattice_actor::Request)]")
            .message_attribute(".game.LoginRequest", "#[request(response = LoginReply)]");

        assert_eq!(
            codegen.message_attributes,
            vec![
                (
                    ".game.LoginRequest".to_owned(),
                    "#[derive(lattice_actor::Request)]".to_owned(),
                ),
                (
                    ".game.LoginRequest".to_owned(),
                    "#[request(response = LoginReply)]".to_owned(),
                ),
            ]
        );
    }
}
