pub(crate) fn push_actor_ref_conversions(rust: &mut String) {
    rust.push_str("impl From<lattice_core::id::ActorId> for crate::lattice::actor::ActorId {\n");
    rust.push_str("    fn from(value: lattice_core::id::ActorId) -> Self {\n");
    rust.push_str("        let kind = match value {\n");
    rust.push_str("            lattice_core::id::ActorId::Str(value) => crate::lattice::actor::actor_id::Kind::Str(value),\n");
    rust.push_str("            lattice_core::id::ActorId::U64(value) => crate::lattice::actor::actor_id::Kind::U64(value),\n");
    rust.push_str("            lattice_core::id::ActorId::I64(value) => crate::lattice::actor::actor_id::Kind::I64(value),\n");
    rust.push_str("            lattice_core::id::ActorId::Bytes(value) => crate::lattice::actor::actor_id::Kind::Bytes(value),\n");
    rust.push_str("        };\n");
    rust.push_str("        Self { kind: Some(kind) }\n");
    rust.push_str("    }\n");
    rust.push_str("}\n\n");
    rust.push_str("impl TryFrom<crate::lattice::actor::ActorId> for lattice_core::id::ActorId {\n");
    rust.push_str("    type Error = lattice_rpc::error::RpcError;\n\n");
    rust.push_str(
        "    fn try_from(value: crate::lattice::actor::ActorId) -> Result<Self, Self::Error> {\n",
    );
    rust.push_str("        match value.kind.ok_or_else(|| lattice_rpc::error::RpcError::Business(\"missing actor ref actor_id kind\".to_string()))? {\n");
    rust.push_str("            crate::lattice::actor::actor_id::Kind::Str(value) => Ok(lattice_core::id::ActorId::Str(value)),\n");
    rust.push_str("            crate::lattice::actor::actor_id::Kind::U64(value) => Ok(lattice_core::id::ActorId::U64(value)),\n");
    rust.push_str("            crate::lattice::actor::actor_id::Kind::I64(value) => Ok(lattice_core::id::ActorId::I64(value)),\n");
    rust.push_str("            crate::lattice::actor::actor_id::Kind::Bytes(value) => Ok(lattice_core::id::ActorId::Bytes(value)),\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n");
    rust.push_str("}\n\n");
    rust.push_str(
        "impl From<lattice_core::actor_ref::ActorRef> for crate::lattice::actor::ActorRef {\n",
    );
    rust.push_str("    fn from(value: lattice_core::actor_ref::ActorRef) -> Self {\n");
    rust.push_str("        let target = match value.target {\n");
    rust.push_str("            lattice_core::actor_ref::ActorRefTarget::Direct { instance_id, endpoint, owner_epoch } => crate::lattice::actor::Target {\n");
    rust.push_str("                kind: Some(crate::lattice::actor::target::Kind::Direct(crate::lattice::actor::DirectTarget {\n");
    rust.push_str("                    instance_id: instance_id.as_str().to_string(),\n");
    rust.push_str("                    endpoint: endpoint.to_string(),\n");
    rust.push_str("                    owner_epoch: owner_epoch.map(|epoch| epoch.0),\n");
    rust.push_str("                })),\n");
    rust.push_str("            },\n");
    rust.push_str(
        "            lattice_core::actor_ref::ActorRefTarget::Routed => crate::lattice::actor::Target {\n",
    );
    rust.push_str("                kind: Some(crate::lattice::actor::target::Kind::Routed(crate::lattice::actor::RoutedTarget {})),\n");
    rust.push_str("            },\n");
    rust.push_str("        };\n");
    rust.push_str("        Self {\n");
    rust.push_str("            service_kind: value.service_kind.as_str().to_string(),\n");
    rust.push_str("            actor_kind: value.actor_kind.as_str().to_string(),\n");
    rust.push_str("            actor_id: Some(value.actor_id.into()),\n");
    rust.push_str("            target: Some(target),\n");
    rust.push_str("        }\n");
    rust.push_str("    }\n");
    rust.push_str("}\n\n");
    rust.push_str(
        "impl TryFrom<crate::lattice::actor::ActorRef> for lattice_core::actor_ref::ActorRef {\n",
    );
    rust.push_str("    type Error = lattice_rpc::error::RpcError;\n\n");
    rust.push_str(
        "    fn try_from(value: crate::lattice::actor::ActorRef) -> Result<Self, Self::Error> {\n",
    );
    rust.push_str("        let actor_id = value.actor_id.ok_or_else(|| lattice_rpc::error::RpcError::Business(\"missing actor ref actor_id\".to_string()))?.try_into()?;\n");
    rust.push_str("        let target = value.target.ok_or_else(|| lattice_rpc::error::RpcError::Business(\"missing actor ref target\".to_string()))?;\n");
    rust.push_str("        let target = match target.kind.ok_or_else(|| lattice_rpc::error::RpcError::Business(\"missing actor ref target kind\".to_string()))? {\n");
    rust.push_str("            crate::lattice::actor::target::Kind::Direct(target) => lattice_core::actor_ref::ActorRefTarget::Direct {\n");
    rust.push_str(
        "                instance_id: lattice_core::instance::InstanceId::new(target.instance_id),\n",
    );
    rust.push_str("                endpoint: target.endpoint.parse().map_err(|error| lattice_rpc::error::RpcError::Business(format!(\"invalid actor ref endpoint: {error}\")))?,\n");
    rust.push_str(
        "                owner_epoch: target.owner_epoch.map(lattice_core::actor_ref::Epoch),\n",
    );
    rust.push_str("            },\n");
    rust.push_str("            crate::lattice::actor::target::Kind::Routed(_) => lattice_core::actor_ref::ActorRefTarget::Routed,\n");
    rust.push_str("        };\n");
    rust.push_str("        Ok(lattice_core::actor_ref::ActorRef {\n");
    rust.push_str(
        "            service_kind: lattice_core::kind::ServiceKind::new(value.service_kind),\n",
    );
    rust.push_str(
        "            actor_kind: lattice_core::kind::ActorKind::new(value.actor_kind),\n",
    );
    rust.push_str("            actor_id,\n");
    rust.push_str("            target,\n");
    rust.push_str("        })\n");
    rust.push_str("    }\n");
    rust.push_str("}\n\n");
}
