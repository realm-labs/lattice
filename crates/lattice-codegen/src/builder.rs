use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use prost::Message;
use prost_types::FileDescriptorSet;

use crate::descriptor::{
    messages_from_descriptor_for_files, methods_from_descriptor, parse_proto_options,
};
use crate::error::CodegenError;
use crate::proto_include;
use crate::render::{RenderOptions, generate_rpc_bindings_with_direct_link_streams};
use crate::spec::GeneratedDirectLinkStreamSpec;

#[derive(Debug, Clone)]
pub struct LatticeCodegenBuilder {
    out_dir: Option<PathBuf>,
    emit_descriptor_set: bool,
    direct_link_streams: Vec<GeneratedDirectLinkStreamSpec>,
}

pub fn configure() -> LatticeCodegenBuilder {
    LatticeCodegenBuilder {
        out_dir: None,
        emit_descriptor_set: false,
        direct_link_streams: Vec::new(),
    }
}

impl LatticeCodegenBuilder {
    pub fn out_dir(mut self, path: impl AsRef<Path>) -> Self {
        self.out_dir = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn emit_descriptor_set(mut self, emit: bool) -> Self {
        self.emit_descriptor_set = emit;
        self
    }

    pub fn direct_link_stream(mut self, stream: GeneratedDirectLinkStreamSpec) -> Self {
        self.direct_link_streams.push(stream);
        self
    }

    pub fn direct_link_streams<I>(mut self, streams: I) -> Self
    where
        I: IntoIterator<Item = GeneratedDirectLinkStreamSpec>,
    {
        self.direct_link_streams.extend(streams);
        self
    }

    pub fn compile_protos<PF, PI>(
        self,
        proto_files: &[PF],
        includes: &[PI],
    ) -> Result<(), CodegenError>
    where
        PF: AsRef<Path>,
        PI: AsRef<Path>,
    {
        let out_dir = self.out_dir.clone().unwrap_or_else(|| {
            std::env::var_os("OUT_DIR")
                .map(PathBuf::from)
                .expect("OUT_DIR must be set for lattice-codegen")
        });
        fs::create_dir_all(&out_dir)
            .map_err(|error| CodegenError::WriteGenerated(error.to_string()))?;

        let proto_paths = proto_files
            .iter()
            .map(|path| path.as_ref().to_path_buf())
            .collect::<Vec<_>>();
        let include_paths = includes
            .iter()
            .map(|path| path.as_ref().to_path_buf())
            .collect::<Vec<_>>();
        let descriptor_path = out_dir.join("lattice.descriptor.bin");
        tonic_prost_build::configure()
            .file_descriptor_set_path(&descriptor_path)
            .out_dir(&out_dir)
            .build_client(true)
            .build_server(true)
            .compile_protos(&proto_paths, &include_paths)
            .map_err(|error| CodegenError::ProtoCompile(error.to_string()))?;

        let descriptor_bytes = fs::read(&descriptor_path)
            .map_err(|error| CodegenError::DescriptorRead(error.to_string()))?;
        let descriptor = FileDescriptorSet::decode(descriptor_bytes.as_slice())
            .map_err(|error| CodegenError::DescriptorRead(error.to_string()))?;
        let options = parse_proto_options(&descriptor, &descriptor_bytes)?;
        let methods = methods_from_descriptor(&descriptor, &options)?;
        let messages = messages_from_descriptor_for_files(
            &descriptor,
            &descriptor_input_file_names(&proto_paths, &include_paths),
        );
        let generated = generate_rpc_bindings_with_direct_link_streams(
            &methods,
            &messages,
            RenderOptions {
                actor_ref_proto: descriptor_has_actor_ref_proto(&descriptor),
            },
            &self.direct_link_streams,
        )?;
        let generated_path = out_dir.join("lattice.generated.rs");
        fs::write(generated_path, generated.rust)
            .map_err(|error| CodegenError::WriteGenerated(error.to_string()))?;
        if !self.emit_descriptor_set {
            let _ = fs::remove_file(&descriptor_path);
        }

        println!("cargo:rerun-if-changed={}", proto_include().display());
        for proto_file in proto_files {
            println!("cargo:rerun-if-changed={}", proto_file.as_ref().display());
        }

        Ok(())
    }
}

fn descriptor_input_file_names(
    proto_paths: &[PathBuf],
    include_paths: &[PathBuf],
) -> BTreeSet<String> {
    let mut file_names = BTreeSet::new();
    for proto_path in proto_paths {
        file_names.insert(normalize_proto_path(proto_path));
        if let Some(name) = proto_path.file_name() {
            file_names.insert(name.to_string_lossy().replace('\\', "/"));
        }
        for include_path in include_paths {
            if let Ok(stripped) = proto_path.strip_prefix(include_path) {
                file_names.insert(normalize_proto_path(stripped));
            }
        }
    }
    file_names
}

fn normalize_proto_path(path: &Path) -> String {
    path.to_string_lossy()
        .replace('\\', "/")
        .trim_start_matches("./")
        .to_string()
}

fn descriptor_has_actor_ref_proto(descriptor: &FileDescriptorSet) -> bool {
    descriptor
        .file
        .iter()
        .any(|file| file.package.as_deref() == Some("lattice.actor"))
}
