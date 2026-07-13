use std::fs;
use std::path::{Path, PathBuf};

use crate::error::CodegenError;
use crate::render::render_actor_protocol;
use crate::spec::ActorProtocolSpec;

#[derive(Debug, Clone, Default)]
pub struct ActorProtocolCodegen {
    out_dir: Option<PathBuf>,
    protocols: Vec<ActorProtocolSpec>,
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
            actor_type: "PlayerActor".to_owned(),
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
}
