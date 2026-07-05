use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lattice_core::{Epoch, InstanceId, RequestId, ServiceKind, TraceContext};
use tonic::Status;
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};

pub(crate) const REQUEST_ID: &str = "lattice-request-id";
pub(crate) const ROUTE_EPOCH: &str = "lattice-route-epoch";
pub(crate) const SOURCE_SERVICE: &str = "lattice-source-service";
pub(crate) const SOURCE_INSTANCE: &str = "lattice-source-instance";
pub(crate) const TRACEPARENT: &str = "traceparent";
pub(crate) const TRACESTATE: &str = "tracestate";
pub(crate) const AUTHORIZATION: &str = "authorization";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcContext {
    pub request_id: RequestId,
    pub route_epoch: Option<Epoch>,
    pub source_service: ServiceKind,
    pub source_instance: InstanceId,
    pub trace: TraceContext,
    pub auth: Option<AuthContext>,
}

impl RpcContext {
    pub fn inject_metadata(&self, metadata: &mut MetadataMap) -> Result<(), RpcMetadataError> {
        insert_ascii(metadata, REQUEST_ID, self.request_id.as_str())?;
        if let Some(epoch) = self.route_epoch {
            insert_ascii(metadata, ROUTE_EPOCH, &epoch.0.to_string())?;
        }
        insert_ascii(metadata, SOURCE_SERVICE, self.source_service.as_str())?;
        insert_ascii(metadata, SOURCE_INSTANCE, self.source_instance.as_str())?;
        if let Some(traceparent) = &self.trace.traceparent {
            insert_ascii(metadata, TRACEPARENT, traceparent)?;
        }
        if let Some(tracestate) = &self.trace.tracestate {
            insert_ascii(metadata, TRACESTATE, tracestate)?;
        }
        if let Some(auth) = &self.auth {
            insert_ascii(metadata, AUTHORIZATION, &auth.authorization)?;
        }
        Ok(())
    }

    pub fn from_metadata(metadata: &MetadataMap) -> Result<Self, RpcMetadataError> {
        Ok(Self {
            request_id: RequestId::new(required_ascii(metadata, REQUEST_ID)?),
            route_epoch: optional_ascii(metadata, ROUTE_EPOCH)?
                .map(|value| {
                    value
                        .parse::<u64>()
                        .map(Epoch)
                        .map_err(|_| RpcMetadataError::InvalidU64 {
                            key: ROUTE_EPOCH,
                            value,
                        })
                })
                .transpose()?,
            source_service: ServiceKind::new(required_ascii(metadata, SOURCE_SERVICE)?),
            source_instance: InstanceId::new(required_ascii(metadata, SOURCE_INSTANCE)?),
            trace: TraceContext {
                traceparent: optional_ascii(metadata, TRACEPARENT)?,
                tracestate: optional_ascii(metadata, TRACESTATE)?,
            },
            auth: optional_ascii(metadata, AUTHORIZATION)?
                .map(|authorization| AuthContext { authorization }),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthContext {
    pub authorization: String,
}

#[derive(Debug, Clone)]
pub struct RpcClientContextFactory {
    source_service: ServiceKind,
    source_instance: InstanceId,
    trace: TraceContext,
    auth: Option<AuthContext>,
    request_seq: Arc<AtomicU64>,
}

impl RpcClientContextFactory {
    pub fn new(source_service: ServiceKind, source_instance: InstanceId) -> Self {
        Self {
            source_service,
            source_instance,
            trace: TraceContext::default(),
            auth: None,
            request_seq: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn with_trace(mut self, trace: TraceContext) -> Self {
        self.trace = trace;
        self
    }

    pub fn with_auth(mut self, auth: AuthContext) -> Self {
        self.auth = Some(auth);
        self
    }

    pub fn next_context(&self, route_epoch: Option<Epoch>) -> RpcContext {
        let seq = self.request_seq.fetch_add(1, Ordering::Relaxed);
        RpcContext {
            request_id: RequestId::new(format!(
                "{}:{}:{seq}",
                self.source_service.as_str(),
                self.source_instance.as_str()
            )),
            route_epoch,
            source_service: self.source_service.clone(),
            source_instance: self.source_instance.clone(),
            trace: self.trace.clone(),
            auth: self.auth.clone(),
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcMetadataError {
    #[error("missing rpc metadata key {key}")]
    Missing { key: &'static str },
    #[error("invalid rpc metadata key {key}")]
    InvalidAscii { key: &'static str },
    #[error("invalid unsigned integer in rpc metadata key {key}: {value}")]
    InvalidU64 { key: &'static str, value: String },
}

fn insert_ascii(
    metadata: &mut MetadataMap,
    key: &'static str,
    value: &str,
) -> Result<(), RpcMetadataError> {
    let value = MetadataValue::<Ascii>::try_from(value)
        .map_err(|_| RpcMetadataError::InvalidAscii { key })?;
    metadata.insert(key, value);
    Ok(())
}

fn required_ascii(metadata: &MetadataMap, key: &'static str) -> Result<String, RpcMetadataError> {
    optional_ascii(metadata, key)?.ok_or(RpcMetadataError::Missing { key })
}

fn optional_ascii(
    metadata: &MetadataMap,
    key: &'static str,
) -> Result<Option<String>, RpcMetadataError> {
    metadata
        .get(key)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| RpcMetadataError::InvalidAscii { key })
        })
        .transpose()
}

pub(crate) fn metadata_status(error: RpcMetadataError) -> Status {
    Status::invalid_argument(error.to_string())
}
