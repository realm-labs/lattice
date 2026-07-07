use http::Uri;
use lattice_actor::traits::Message;
use lattice_core::actor_ref::Epoch;
use lattice_core::id::RouteKey;
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};

use crate::metadata::RpcContext;
use crate::traits::{RoutedRequest, RpcRequest};

const ROUTE_ACTOR_KIND: &str = "lattice-route-actor-kind";
const ROUTE_KEY_KIND: &str = "lattice-route-key-kind";
const ROUTE_KEY_VALUE: &str = "lattice-route-key-value";

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
pub struct RouteTarget {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub advertised_endpoint: Uri,
    pub owner_epoch: Option<Epoch>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RpcRoute {
    pub actor_kind: ActorKind,
    pub route_key: RouteKey,
}

impl RpcRoute {
    pub fn new(actor_kind: ActorKind, route_key: RouteKey) -> Self {
        Self {
            actor_kind,
            route_key,
        }
    }

    pub fn from_request<Req>(req: &Req) -> Self
    where
        Req: RoutedRequest,
    {
        Self::new(req.actor_kind(), req.route_key())
    }

    pub fn inject_metadata(&self, metadata: &mut MetadataMap) -> Result<(), RpcRouteMetadataError> {
        insert_ascii(metadata, ROUTE_ACTOR_KIND, self.actor_kind.as_str())?;
        match &self.route_key {
            RouteKey::Str(value) => {
                insert_ascii(metadata, ROUTE_KEY_KIND, "str")?;
                insert_ascii(metadata, ROUTE_KEY_VALUE, value)?;
            }
            RouteKey::U64(value) => {
                insert_ascii(metadata, ROUTE_KEY_KIND, "u64")?;
                insert_ascii(metadata, ROUTE_KEY_VALUE, &value.to_string())?;
            }
            RouteKey::I64(value) => {
                insert_ascii(metadata, ROUTE_KEY_KIND, "i64")?;
                insert_ascii(metadata, ROUTE_KEY_VALUE, &value.to_string())?;
            }
            RouteKey::Bytes(value) => {
                insert_ascii(metadata, ROUTE_KEY_KIND, "bytes")?;
                insert_ascii(metadata, ROUTE_KEY_VALUE, &encode_hex(value))?;
            }
        }
        Ok(())
    }

    pub fn from_metadata(metadata: &MetadataMap) -> Result<Option<Self>, RpcRouteMetadataError> {
        let actor_kind = optional_ascii(metadata, ROUTE_ACTOR_KIND)?;
        let key_kind = optional_ascii(metadata, ROUTE_KEY_KIND)?;
        let key_value = optional_ascii(metadata, ROUTE_KEY_VALUE)?;
        match (actor_kind, key_kind, key_value) {
            (None, None, None) => Ok(None),
            (Some(actor_kind), Some(key_kind), Some(key_value)) => Ok(Some(Self {
                actor_kind: ActorKind::new(actor_kind),
                route_key: decode_route_key(&key_kind, &key_value)?,
            })),
            (actor_kind, key_kind, _key_value) => {
                let missing = if actor_kind.is_none() {
                    ROUTE_ACTOR_KIND
                } else if key_kind.is_none() {
                    ROUTE_KEY_KIND
                } else {
                    ROUTE_KEY_VALUE
                };
                Err(RpcRouteMetadataError::Missing { key: missing })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedEnvelope<Req> {
    pub req: Req,
    pub actor_kind: ActorKind,
    pub route_key: RouteKey,
}

impl<Req> RoutedEnvelope<Req> {
    pub fn new(req: Req, actor_kind: ActorKind, route_key: RouteKey) -> Self {
        Self {
            req,
            actor_kind,
            route_key,
        }
    }

    pub fn route(&self) -> RpcRoute {
        RpcRoute::new(self.actor_kind.clone(), self.route_key.clone())
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcRouteMetadataError {
    #[error("missing rpc route metadata key {key}")]
    Missing { key: &'static str },
    #[error("invalid rpc route metadata key {key}")]
    InvalidAscii { key: &'static str },
    #[error("invalid rpc route key kind {kind}")]
    InvalidRouteKeyKind { kind: String },
    #[error("invalid rpc route key value for kind {kind}: {value}")]
    InvalidRouteKeyValue { kind: String, value: String },
}

fn insert_ascii(
    metadata: &mut MetadataMap,
    key: &'static str,
    value: &str,
) -> Result<(), RpcRouteMetadataError> {
    let value = MetadataValue::<Ascii>::try_from(value)
        .map_err(|_| RpcRouteMetadataError::InvalidAscii { key })?;
    metadata.insert(key, value);
    Ok(())
}

fn optional_ascii(
    metadata: &MetadataMap,
    key: &'static str,
) -> Result<Option<String>, RpcRouteMetadataError> {
    metadata
        .get(key)
        .map(|value| {
            value
                .to_str()
                .map(str::to_owned)
                .map_err(|_| RpcRouteMetadataError::InvalidAscii { key })
        })
        .transpose()
}

fn decode_route_key(kind: &str, value: &str) -> Result<RouteKey, RpcRouteMetadataError> {
    match kind {
        "str" => Ok(RouteKey::Str(value.to_string())),
        "u64" => value
            .parse::<u64>()
            .map(RouteKey::U64)
            .map_err(|_| invalid_route_key_value(kind, value)),
        "i64" => value
            .parse::<i64>()
            .map(RouteKey::I64)
            .map_err(|_| invalid_route_key_value(kind, value)),
        "bytes" => decode_hex(value)
            .map(RouteKey::Bytes)
            .map_err(|_| invalid_route_key_value(kind, value)),
        other => Err(RpcRouteMetadataError::InvalidRouteKeyKind {
            kind: other.to_string(),
        }),
    }
}

fn invalid_route_key_value(kind: &str, value: &str) -> RpcRouteMetadataError {
    RpcRouteMetadataError::InvalidRouteKeyValue {
        kind: kind.to_string(),
        value: value.to_string(),
    }
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn decode_hex(value: &str) -> Result<Vec<u8>, ()> {
    if !value.len().is_multiple_of(2) {
        return Err(());
    }
    let mut output = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    for index in (0..bytes.len()).step_by(2) {
        let high = hex_value(bytes[index]).ok_or(())?;
        let low = hex_value(bytes[index + 1]).ok_or(())?;
        output.push((high << 4) | low);
    }
    Ok(output)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}
