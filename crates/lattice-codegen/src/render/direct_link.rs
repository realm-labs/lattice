use std::collections::BTreeSet;

use crate::error::CodegenError;
use crate::spec::{GeneratedDirectLinkStreamSpec, ProtoMessageSpec};
pub(crate) fn push_direct_link_messages(rust: &mut String, messages: &[ProtoMessageSpec]) {
    let mut emitted = BTreeSet::new();
    for message in messages {
        if !emitted.insert(message.rust_type.clone()) {
            continue;
        }
        rust.push_str(&format!(
            "impl lattice_core::direct_link::stream::DirectLinkMessage for {rust_type} {{\n",
            rust_type = message.rust_type
        ));
        rust.push_str(&format!(
            "    const PROTO_FULL_NAME: &'static str = \"{}\";\n",
            message.proto_full_name
        ));
        rust.push_str("}\n\n");
    }
}

pub(crate) fn validate_direct_link_streams(
    streams: &[GeneratedDirectLinkStreamSpec],
) -> Result<(), CodegenError> {
    for stream in streams {
        if stream.module_name.trim().is_empty() {
            return Err(CodegenError::MissingField("direct_link_stream.module_name"));
        }
        if stream.stream_name.trim().is_empty() {
            return Err(CodegenError::MissingField("direct_link_stream.stream_name"));
        }
        if stream
            .metadata_type
            .as_ref()
            .is_some_and(|metadata_type| metadata_type.trim().is_empty())
        {
            return Err(CodegenError::MissingField(
                "direct_link_stream.metadata_type",
            ));
        }
        if stream.messages.is_empty() {
            return Err(CodegenError::MissingField("direct_link_stream.messages"));
        }
        let mut ids = BTreeSet::new();
        for message in &stream.messages {
            if message.rust_type.trim().is_empty() {
                return Err(CodegenError::MissingField(
                    "direct_link_stream.message.rust_type",
                ));
            }
            if !ids.insert(message.message_id) {
                return Err(CodegenError::DuplicateDirectLinkMessageId {
                    stream_name: stream.stream_name.clone(),
                    message_id: message.message_id,
                });
            }
        }
    }
    Ok(())
}

pub(crate) fn push_generated_direct_link_streams(
    rust: &mut String,
    streams: &[GeneratedDirectLinkStreamSpec],
) {
    for stream in streams {
        let metadata_type = direct_link_metadata_type(stream);
        rust.push_str(&format!("pub mod {} {{\n", stream.module_name));
        rust.push_str("    #[derive(Debug, Clone, Copy, Default)]\n");
        rust.push_str("    pub struct Stream;\n\n");

        rust.push_str(
            "    impl lattice_core::direct_link::stream::DirectLinkStreamType for Stream {\n",
        );
        rust.push_str(&format!("        type Metadata = {metadata_type};\n\n"));
        rust.push_str("        fn descriptor() -> lattice_core::direct_link::stream::DirectLinkStreamDescriptor {\n");
        rust.push_str("            descriptor()\n");
        rust.push_str("        }\n");
        rust.push_str("    }\n\n");

        rust.push_str("    pub fn descriptor() -> lattice_core::direct_link::stream::DirectLinkStreamDescriptor {\n");
        rust.push_str("        lattice_core::direct_link::stream::DirectLinkStreamDescriptor {\n");
        rust.push_str(&format!(
            "            stream_name: {:?}.to_string(),\n",
            stream.stream_name
        ));
        rust.push_str("            messages: vec![\n");
        for message in &stream.messages {
            rust.push_str("                lattice_core::direct_link::stream::DirectLinkMessageDescriptor {\n");
            rust.push_str(&format!(
                "                    message_id: lattice_core::direct_link::ids::DirectLinkMessageId({}),\n",
                message.message_id
            ));
            rust.push_str(&format!(
                "                    proto_full_name: <{} as lattice_core::direct_link::stream::DirectLinkMessage>::PROTO_FULL_NAME.to_string(),\n",
                message.rust_type
            ));
            rust.push_str(&format!(
                "                    rust_type_name: std::any::type_name::<{}>().to_string(),\n",
                message.rust_type
            ));
            rust.push_str("                },\n");
        }
        rust.push_str("            ],\n");
        rust.push_str("        }\n");
        rust.push_str("    }\n\n");

        rust.push_str(
            "    pub fn bind<A>(actor_kind: lattice_core::kind::ActorKind) -> lattice_direct_link::stream::DirectLinkActorBinding<A, Stream, ",
        );
        rust.push_str(&metadata_type);
        rust.push_str(">\n");
        rust.push_str("    where\n");
        push_direct_link_handler_bounds(rust, stream);
        rust.push_str("    {\n");
        rust.push_str(
            "        lattice_direct_link::stream::DirectLinkActorBinding::new(actor_kind, descriptor())\n",
        );
        rust.push_str("    }\n\n");

        rust.push_str("    impl<A> lattice_direct_link::delivery::DirectLinkDispatch<A, ");
        rust.push_str(&metadata_type);
        rust.push_str("> for Stream\n");
        rust.push_str("    where\n");
        push_direct_link_handler_bounds(rust, stream);
        rust.push_str("    {\n");
        rust.push_str("        fn try_dispatch(\n");
        rust.push_str("            handle: &lattice_actor::handle::ActorHandle<A>,\n");
        rust.push_str(
            "            _stream: &lattice_core::direct_link::stream::DirectLinkStreamDescriptor,\n",
        );
        rust.push_str(
            "            message_id: lattice_core::direct_link::ids::DirectLinkMessageId,\n",
        );
        rust.push_str("            payload: &[u8],\n");
        rust.push_str("            metadata: ");
        rust.push_str(&metadata_type);
        rust.push_str(",\n");
        rust.push_str(
            "            context: lattice_core::direct_link::messages::LinkMessageContext,\n",
        );
        rust.push_str(
            "        ) -> Result<(), lattice_direct_link::delivery::DirectLinkDeliveryError> {\n",
        );
        rust.push_str("            match message_id.0 {\n");
        for message in &stream.messages {
            rust.push_str(&format!("                {} => {{\n", message.message_id));
            rust.push_str(&format!(
                "                    let payload = <{} as prost::Message>::decode(payload)\n",
                message.rust_type
            ));
            rust.push_str("                        .map_err(|error| lattice_direct_link::delivery::DirectLinkDeliveryError::Decode(error.to_string()))?;\n");
            rust.push_str("                    lattice_direct_link::delivery::try_deliver_linked(handle, payload, metadata, context)\n");
            rust.push_str("                        .map_err(lattice_direct_link::delivery::DirectLinkDeliveryError::from)\n");
            rust.push_str("                }\n");
        }
        rust.push_str(
            "                _ => Err(lattice_direct_link::delivery::DirectLinkDeliveryError::UnsupportedMessageType),\n",
        );
        rust.push_str("            }\n");
        rust.push_str("        }\n");
        rust.push_str("    }\n");
        rust.push_str("}\n\n");
    }
}

fn push_direct_link_handler_bounds(rust: &mut String, stream: &GeneratedDirectLinkStreamSpec) {
    let metadata_type = direct_link_metadata_type(stream);
    rust.push_str("        A: lattice_actor::traits::Actor");
    for message in &stream.messages {
        rust.push_str(&format!(
            " + lattice_actor::traits::Handler<lattice_core::direct_link::messages::Linked<{}, {}>>",
            message.rust_type, metadata_type
        ));
    }
    rust.push_str(",\n");
}

fn direct_link_metadata_type(stream: &GeneratedDirectLinkStreamSpec) -> String {
    stream.metadata_type.as_deref().unwrap_or("()").to_string()
}
