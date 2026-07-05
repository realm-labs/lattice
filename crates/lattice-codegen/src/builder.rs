use std::fs;
use std::path::{Path, PathBuf};

use prost::Message;
use prost_types::FileDescriptorSet;
use serde::Deserialize;

use crate::descriptor::{messages_from_descriptor, methods_from_descriptor, parse_proto_options};
use crate::gateway::GatewayRoute;
use crate::render::{RenderOptions, generate_rpc_bindings_with_options};
use crate::spec::RpcMethodSpec;
use crate::{CodegenError, proto_include};

#[derive(Debug, Clone)]
pub struct LatticeCodegenBuilder {
    out_dir: Option<PathBuf>,
    emit_descriptor_set: bool,
    gateway_routes: Vec<GatewayRoute>,
    gateway_route_files: Vec<PathBuf>,
}

pub fn configure() -> LatticeCodegenBuilder {
    LatticeCodegenBuilder {
        out_dir: None,
        emit_descriptor_set: false,
        gateway_routes: Vec::new(),
        gateway_route_files: Vec::new(),
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

    pub fn gateway_route_ids<I, M>(mut self, routes: I) -> Self
    where
        I: IntoIterator<Item = (u32, M)>,
        M: Into<String>,
    {
        self.gateway_routes.extend(
            routes
                .into_iter()
                .map(|(msg_id, method)| GatewayRoute::new(msg_id, method)),
        );
        self
    }

    pub fn gateway_routes(mut self, path: impl AsRef<Path>) -> Self {
        self.gateway_route_files.push(path.as_ref().to_path_buf());
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
        let mut methods = methods_from_descriptor(&descriptor, &options)?;
        let messages = messages_from_descriptor(&descriptor);
        let gateway_routes = self.load_gateway_routes()?;
        apply_gateway_routes(&mut methods, &gateway_routes)?;
        let generated = generate_rpc_bindings_with_options(
            &methods,
            &messages,
            RenderOptions {
                actor_ref_proto: descriptor_has_actor_ref_proto(&descriptor),
            },
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
        for route_file in &self.gateway_route_files {
            println!("cargo:rerun-if-changed={}", route_file.display());
        }

        Ok(())
    }

    fn load_gateway_routes(&self) -> Result<Vec<GatewayRoute>, CodegenError> {
        let mut routes = self.gateway_routes.clone();
        for path in &self.gateway_route_files {
            let source =
                fs::read_to_string(path).map_err(|source| CodegenError::GatewayRouteRead {
                    path: path.clone(),
                    source,
                })?;
            let parsed = toml::from_str::<GatewayRouteFile>(&source).map_err(|source| {
                CodegenError::GatewayRouteParse {
                    path: path.clone(),
                    details: source.to_string(),
                }
            })?;
            routes.extend(
                parsed
                    .routes
                    .into_iter()
                    .map(|route| GatewayRoute::new(route.msg_id, route.method)),
            );
        }
        Ok(routes)
    }
}

fn descriptor_has_actor_ref_proto(descriptor: &FileDescriptorSet) -> bool {
    descriptor
        .file
        .iter()
        .any(|file| file.package.as_deref() == Some("lattice.actor"))
}

#[derive(Debug, Deserialize)]
struct GatewayRouteFile {
    routes: Vec<GatewayRouteEntry>,
}

#[derive(Debug, Deserialize)]
struct GatewayRouteEntry {
    msg_id: u32,
    method: String,
}

fn apply_gateway_routes(
    methods: &mut [RpcMethodSpec],
    routes: &[GatewayRoute],
) -> Result<(), CodegenError> {
    let mut assigned_methods = std::collections::BTreeSet::new();
    for route in routes {
        if !assigned_methods.insert(route.method.clone()) {
            return Err(CodegenError::DuplicateGatewayRouteMethod {
                method: route.method.clone(),
            });
        }
        let Some(method) = methods
            .iter_mut()
            .find(|method| gateway_method_name(method) == route.method)
        else {
            return Err(CodegenError::UnknownGatewayRouteMethod {
                method: route.method.clone(),
            });
        };
        if let Some(proto_msg_id) = method.gateway_msg_id
            && proto_msg_id != route.msg_id
        {
            return Err(CodegenError::ConflictingGatewayMessageId {
                method: route.method.clone(),
                proto_msg_id,
                route_msg_id: route.msg_id,
            });
        }
        method.gateway_msg_id = Some(route.msg_id);
    }
    Ok(())
}

fn gateway_method_name(method: &RpcMethodSpec) -> String {
    if method.package.is_empty() {
        format!("{}.{}", method.service_name, method.method_name)
    } else {
        format!(
            "{}.{}.{}",
            method.package, method.service_name, method.method_name
        )
    }
}
