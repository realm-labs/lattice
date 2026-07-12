use std::collections::BTreeSet;

use crate::error::CodegenError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InteractionMode {
    Tell,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProtocolMessageSpec {
    pub message_id: u64,
    pub message_type: String,
    pub mode: InteractionMode,
    pub request_codec: String,
    pub reply_codec: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActorProtocolSpec {
    pub visibility: String,
    pub registrar_name: String,
    pub actor_type: String,
    pub protocol_id: u64,
    pub protocol_name: String,
    pub messages: Vec<ProtocolMessageSpec>,
}

impl ActorProtocolSpec {
    pub fn validate(&self) -> Result<(), CodegenError> {
        for (field, value) in [
            ("registrar_name", self.registrar_name.as_str()),
            ("actor_type", self.actor_type.as_str()),
            ("protocol_name", self.protocol_name.as_str()),
        ] {
            if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
                return Err(CodegenError::InvalidSpec(format!("invalid {field}")));
            }
        }
        if self.protocol_id == 0 || self.messages.is_empty() {
            return Err(CodegenError::InvalidSpec(
                "protocol ID must be nonzero and messages must not be empty".to_owned(),
            ));
        }
        let mut ids = BTreeSet::new();
        for message in &self.messages {
            if message.message_id == 0 || !ids.insert(message.message_id) {
                return Err(CodegenError::InvalidSpec(
                    "message IDs must be explicit, nonzero, and unique".to_owned(),
                ));
            }
            if message.message_type.is_empty() || message.request_codec.is_empty() {
                return Err(CodegenError::InvalidSpec(
                    "message type and request codec are required".to_owned(),
                ));
            }
            match (message.mode, message.reply_codec.as_deref()) {
                (InteractionMode::Tell, None) | (InteractionMode::Ask, Some(_)) => {}
                (InteractionMode::Tell, Some(_)) => {
                    return Err(CodegenError::InvalidSpec(
                        "tell registration cannot declare a reply codec".to_owned(),
                    ));
                }
                (InteractionMode::Ask, None) => {
                    return Err(CodegenError::InvalidSpec(
                        "ask registration requires a reply codec".to_owned(),
                    ));
                }
            }
        }
        Ok(())
    }
}
