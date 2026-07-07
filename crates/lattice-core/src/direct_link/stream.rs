use std::collections::BTreeSet;

use prost::Message as ProstMessage;
use serde::{Deserialize, Serialize};

use crate::direct_link::errors::LinkMetadataError;
use crate::direct_link::ids::DirectLinkMessageId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectLinkMessageDescriptor {
    pub message_id: DirectLinkMessageId,
    pub proto_full_name: String,
    pub rust_type_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectLinkStreamDescriptor {
    pub stream_name: String,
    pub messages: Vec<DirectLinkMessageDescriptor>,
}

impl DirectLinkStreamDescriptor {
    pub fn new(stream_name: impl Into<String>) -> Self {
        Self {
            stream_name: stream_name.into(),
            messages: Vec::new(),
        }
    }

    pub fn message_id_for<T>(&self) -> Option<DirectLinkMessageId>
    where
        T: DirectLinkMessage,
    {
        self.messages
            .iter()
            .find(|message| message.proto_full_name == T::PROTO_FULL_NAME)
            .map(|message| message.message_id)
    }

    pub fn accepted_message_ids(&self) -> BTreeSet<DirectLinkMessageId> {
        self.messages
            .iter()
            .map(|message| message.message_id)
            .collect()
    }

    pub fn duplicate_message_id(&self) -> Option<DirectLinkMessageId> {
        let mut seen = BTreeSet::new();
        self.messages
            .iter()
            .map(|message| message.message_id)
            .find(|id| !seen.insert(*id))
    }
}

pub trait DirectLinkMessage: ProstMessage + Default + Send + Sync + 'static {
    const PROTO_FULL_NAME: &'static str;
}

pub trait DirectLinkMetadata: Clone + Send + Sync + 'static {
    fn encode_metadata(&self) -> Result<Vec<u8>, LinkMetadataError>;
    fn decode_metadata(bytes: &[u8]) -> Result<Self, LinkMetadataError>
    where
        Self: Sized;
}

impl DirectLinkMetadata for () {
    fn encode_metadata(&self) -> Result<Vec<u8>, LinkMetadataError> {
        Ok(Vec::new())
    }

    fn decode_metadata(bytes: &[u8]) -> Result<Self, LinkMetadataError> {
        if bytes.is_empty() {
            Ok(())
        } else {
            Err(LinkMetadataError::UnexpectedMetadata)
        }
    }
}

pub trait DirectLinkStreamSpec: Clone + Send + Sync + 'static {
    type Metadata: DirectLinkMetadata;

    fn descriptor(&self) -> DirectLinkStreamDescriptor;
}

pub trait DirectLinkStreamType: Clone + Send + Sync + 'static {
    type Metadata: DirectLinkMetadata;

    fn descriptor() -> DirectLinkStreamDescriptor;
}

impl<T> DirectLinkStreamSpec for T
where
    T: DirectLinkStreamType,
{
    type Metadata = T::Metadata;

    fn descriptor(&self) -> DirectLinkStreamDescriptor {
        T::descriptor()
    }
}
