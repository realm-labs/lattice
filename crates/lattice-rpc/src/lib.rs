use async_trait::async_trait;
use http::Uri;
use lattice_actor::Message;
use lattice_core::{ActorKind, Epoch, InstanceId, RequestId, RouteKey, ServiceKind, TraceContext};
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};

const REQUEST_ID: &str = "lattice-request-id";
const ROUTE_EPOCH: &str = "lattice-route-epoch";
const SOURCE_SERVICE: &str = "lattice-source-service";
const SOURCE_INSTANCE: &str = "lattice-source-instance";
const TRACEPARENT: &str = "traceparent";
const TRACESTATE: &str = "tracestate";
const AUTHORIZATION: &str = "authorization";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rpc<T> {
    pub req: T,
    pub ctx: RpcContext,
}

impl<T> Message for Rpc<T>
where
    T: RpcRequest,
{
    type Reply = T::Reply;
}

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteTarget {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub advertised_endpoint: Uri,
    pub owner_epoch: Option<Epoch>,
}

pub trait RoutedRequest {
    fn actor_kind(&self) -> ActorKind;
    fn route_key(&self) -> RouteKey;
}

pub trait RpcRequest: prost::Message + Default + Send + Sync + 'static {
    type Reply: prost::Message + Default + Send + Sync + 'static;
    const METHOD: &'static str;
}

#[async_trait]
pub trait ShardedRpcCore: Clone + Send + Sync + 'static {
    async fn call<Req>(&self, req: Req) -> Result<Req::Reply, RpcError>
    where
        Req: RoutedRequest + RpcRequest;
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcError {
    #[error("target owner not found")]
    NoOwner,
    #[error("route target is not owner")]
    NotOwner { expected_epoch: Option<Epoch> },
    #[error("request was fenced by newer owner")]
    Fenced { current_epoch: Epoch },
    #[error("actor is unavailable")]
    ActorUnavailable,
    #[error("mailbox is full")]
    MailboxFull,
    #[error("rpc timed out; result may be unknown")]
    TimeoutUnknown,
    #[error("business error: {0}")]
    Business(String),
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

#[cfg(test)]
mod tests {
    use lattice_core::{actor_kind, service_kind};

    use super::*;

    #[derive(Clone, PartialEq, prost::Message)]
    struct EnterWorldRequest {
        #[prost(uint64, tag = "1")]
        world_id: u64,
    }

    #[derive(Clone, PartialEq, prost::Message)]
    struct EnterWorldReply {
        #[prost(bool, tag = "1")]
        ok: bool,
    }

    impl RoutedRequest for EnterWorldRequest {
        fn actor_kind(&self) -> ActorKind {
            actor_kind!("World")
        }

        fn route_key(&self) -> RouteKey {
            RouteKey::U64(self.world_id)
        }
    }

    impl RpcRequest for EnterWorldRequest {
        type Reply = EnterWorldReply;
        const METHOD: &'static str = "world.WorldRpc/EnterWorld";
    }

    #[test]
    fn rpc_context_injects_and_extracts_grpc_metadata() {
        let ctx = RpcContext {
            request_id: RequestId::new("req-1"),
            route_epoch: Some(Epoch(42)),
            source_service: service_kind!("World"),
            source_instance: InstanceId::new("world-0"),
            trace: TraceContext {
                traceparent: Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00".into()),
                tracestate: Some("rojo=00f067aa0ba902b7".into()),
            },
            auth: Some(AuthContext {
                authorization: "Bearer test".into(),
            }),
        };
        let mut metadata = MetadataMap::new();

        ctx.inject_metadata(&mut metadata).unwrap();
        let extracted = RpcContext::from_metadata(&metadata).unwrap();

        assert_eq!(extracted, ctx);
    }

    #[test]
    fn rpc_context_requires_framework_metadata() {
        let error = RpcContext::from_metadata(&MetadataMap::new()).unwrap_err();

        assert_eq!(error, RpcMetadataError::Missing { key: REQUEST_ID });
    }

    #[test]
    fn routed_request_exposes_actor_kind_and_route_key() {
        let request = EnterWorldRequest { world_id: 9 };

        assert_eq!(request.actor_kind(), actor_kind!("World"));
        assert_eq!(request.route_key(), RouteKey::U64(9));
        assert_eq!(EnterWorldRequest::METHOD, "world.WorldRpc/EnterWorld");
    }

    fn assert_actor_message<M: Message>() {}

    #[test]
    fn rpc_wrapper_is_actor_message_for_rpc_request() {
        assert_actor_message::<Rpc<EnterWorldRequest>>();
    }
}
