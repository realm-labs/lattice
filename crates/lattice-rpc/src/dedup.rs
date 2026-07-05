use std::sync::Arc;

use dashmap::DashMap;
use lattice_core::RequestId;
use prost::Message as ProstMessage;
use tonic::Status;

use crate::RpcRequest;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RequestDedupKey {
    method: &'static str,
    request_id: RequestId,
}

impl RequestDedupKey {
    pub fn new(method: &'static str, request_id: &RequestId) -> Self {
        Self {
            method,
            request_id: request_id.clone(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct RequestDeduplicator {
    replies: Arc<DashMap<RequestDedupKey, Vec<u8>>>,
}

impl RequestDeduplicator {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get<Req>(&self, key: &RequestDedupKey) -> Result<Option<Req::Reply>, Status>
    where
        Req: RpcRequest,
    {
        self.replies
            .get(key)
            .map(|entry| {
                Req::Reply::decode(entry.value().as_slice())
                    .map_err(|error| Status::internal(error.to_string()))
            })
            .transpose()
    }

    pub fn record<Reply>(&self, key: &RequestDedupKey, reply: &Reply) -> Result<(), Status>
    where
        Reply: prost::Message,
    {
        self.replies.insert(key.clone(), reply.encode_to_vec());
        Ok(())
    }
}
