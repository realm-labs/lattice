use std::collections::HashMap;
use std::marker::PhantomData;
use std::time::{Duration, Instant};

use lattice_core::ActorKind;
use lattice_rpc::{RoutedRequest, RpcError, RpcRequest, ShardedRpcCore};
use prost::Message as ProstMessage;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientFrame {
    pub msg_id: u32,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GatewaySessionRef {
    pub session_id: String,
    pub connection_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayPush {
    pub session: GatewaySessionRef,
    pub frame: ClientFrame,
}

#[derive(Debug, Default)]
pub struct GatewaySessionRegistry {
    sessions: HashMap<String, u64>,
}

impl GatewaySessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn connect(&mut self, session_id: impl Into<String>) -> GatewaySessionRef {
        let session_id = session_id.into();
        let epoch = self.sessions.get(&session_id).copied().unwrap_or(0) + 1;
        self.sessions.insert(session_id.clone(), epoch);
        GatewaySessionRef {
            session_id,
            connection_epoch: epoch,
        }
    }

    pub fn validate_push(&self, push: &GatewayPush) -> Result<(), GatewayError> {
        match self.sessions.get(&push.session.session_id) {
            Some(epoch) if *epoch == push.session.connection_epoch => Ok(()),
            Some(current_epoch) => Err(GatewayError::StaleSession {
                session_id: push.session.session_id.clone(),
                expected_epoch: *current_epoch,
                actual_epoch: push.session.connection_epoch,
            }),
            None => Err(GatewayError::UnknownSession {
                session_id: push.session.session_id.clone(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RateLimitKey {
    pub principal_id: String,
    pub session_id: String,
    pub rate_class: String,
}

#[derive(Debug)]
pub struct KeyedRateLimiter {
    limit: u32,
    window: Duration,
    buckets: HashMap<RateLimitKey, RateBucket>,
}

impl KeyedRateLimiter {
    pub fn new(limit: u32, window: Duration) -> Self {
        Self {
            limit,
            window,
            buckets: HashMap::new(),
        }
    }

    pub fn check(&mut self, key: RateLimitKey) -> Result<(), GatewayError> {
        let now = Instant::now();
        let bucket = self.buckets.entry(key).or_insert(RateBucket {
            window_started: now,
            used: 0,
        });
        if now.duration_since(bucket.window_started) >= self.window {
            bucket.window_started = now;
            bucket.used = 0;
        }
        if bucket.used >= self.limit {
            return Err(GatewayError::RateLimited);
        }
        bucket.used += 1;
        Ok(())
    }
}

#[derive(Debug)]
struct RateBucket {
    window_started: Instant,
    used: u32,
}

pub trait ClientCodec {
    fn decode(&self, bytes: &[u8]) -> Result<ClientFrame, GatewayError>;
    fn encode(&self, frame: ClientFrame) -> Result<Vec<u8>, GatewayError>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BinaryClientCodec;

impl ClientCodec for BinaryClientCodec {
    fn decode(&self, bytes: &[u8]) -> Result<ClientFrame, GatewayError> {
        if bytes.len() < 4 {
            return Err(GatewayError::FrameTooShort);
        }

        let msg_id = u32::from_be_bytes(bytes[0..4].try_into().expect("slice length checked"));
        Ok(ClientFrame {
            msg_id,
            payload: bytes[4..].to_vec(),
        })
    }

    fn encode(&self, frame: ClientFrame) -> Result<Vec<u8>, GatewayError> {
        let mut bytes = Vec::with_capacity(4 + frame.payload.len());
        bytes.extend_from_slice(&frame.msg_id.to_be_bytes());
        bytes.extend_from_slice(&frame.payload);
        Ok(bytes)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayRouteSpec {
    pub msg_id: u32,
    pub actor_kind: ActorKind,
    pub method: &'static str,
}

#[derive(Debug, Default)]
pub struct GatewayRouteTable {
    routes: HashMap<u32, GatewayRouteSpec>,
}

impl GatewayRouteTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, route: GatewayRouteSpec) -> Result<(), GatewayError> {
        if self.routes.contains_key(&route.msg_id) {
            return Err(GatewayError::DuplicateRoute {
                msg_id: route.msg_id,
            });
        }
        self.routes.insert(route.msg_id, route);
        Ok(())
    }

    pub fn get(&self, msg_id: u32) -> Option<&GatewayRouteSpec> {
        self.routes.get(&msg_id)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProstClientMessageBinding<Req> {
    msg_id: u32,
    _marker: PhantomData<Req>,
}

impl<Req> ProstClientMessageBinding<Req>
where
    Req: RoutedRequest + RpcRequest,
{
    pub const fn new(msg_id: u32) -> Self {
        Self {
            msg_id,
            _marker: PhantomData,
        }
    }

    pub fn route_spec(&self) -> GatewayRouteSpec {
        let default_req = Req::default();
        GatewayRouteSpec {
            msg_id: self.msg_id,
            actor_kind: default_req.actor_kind(),
            method: Req::METHOD,
        }
    }

    pub async fn decode_and_forward<C>(
        &self,
        frame: ClientFrame,
        core: C,
    ) -> Result<ClientFrame, GatewayError>
    where
        C: ShardedRpcCore,
    {
        if frame.msg_id != self.msg_id {
            return Err(GatewayError::UnexpectedMessageId {
                expected: self.msg_id,
                actual: frame.msg_id,
            });
        }

        let req = Req::decode(frame.payload.as_slice())
            .map_err(|source| GatewayError::DecodePayload(source.to_string()))?;
        let reply = core.call(req).await.map_err(GatewayError::Rpc)?;
        Ok(ClientFrame {
            msg_id: self.msg_id,
            payload: reply.encode_to_vec(),
        })
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GatewayError {
    #[error("client frame is too short")]
    FrameTooShort,
    #[error("duplicate gateway route for msg_id {msg_id}")]
    DuplicateRoute { msg_id: u32 },
    #[error("unexpected msg_id: expected {expected}, got {actual}")]
    UnexpectedMessageId { expected: u32, actual: u32 },
    #[error("failed to decode client payload: {0}")]
    DecodePayload(String),
    #[error("rpc failed: {0}")]
    Rpc(RpcError),
    #[error("unknown gateway session {session_id}")]
    UnknownSession { session_id: String },
    #[error(
        "stale gateway session {session_id}: expected epoch {expected_epoch}, got {actual_epoch}"
    )]
    StaleSession {
        session_id: String,
        expected_epoch: u64,
        actual_epoch: u64,
    },
    #[error("gateway rate limit exceeded")]
    RateLimited,
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use lattice_core::{ActorKind, RouteKey, actor_kind};
    use lattice_rpc::RpcRequest;

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

    #[derive(Clone, Default)]
    struct FakeCore {
        routed: Arc<Mutex<Vec<RouteKey>>>,
    }

    #[async_trait]
    impl ShardedRpcCore for FakeCore {
        async fn call<Req>(&self, req: Req) -> Result<Req::Reply, RpcError>
        where
            Req: RoutedRequest + RpcRequest,
        {
            self.routed.lock().unwrap().push(req.route_key());
            Ok(Req::Reply::default())
        }
    }

    #[test]
    fn binary_client_codec_decodes_and_encodes_frame() {
        let codec = BinaryClientCodec;
        let frame = codec.decode(&[0, 0, 0, 9, 1, 2, 3]).unwrap();

        assert_eq!(
            frame,
            ClientFrame {
                msg_id: 9,
                payload: vec![1, 2, 3]
            }
        );
        assert_eq!(codec.encode(frame).unwrap(), vec![0, 0, 0, 9, 1, 2, 3]);
    }

    #[test]
    fn gateway_route_table_rejects_duplicate_msg_id() {
        let binding = ProstClientMessageBinding::<EnterWorldRequest>::new(100);
        let mut table = GatewayRouteTable::new();

        table.register(binding.route_spec()).unwrap();
        let duplicate = table.register(binding.route_spec());

        assert_eq!(duplicate, Err(GatewayError::DuplicateRoute { msg_id: 100 }));
        assert_eq!(table.get(100).unwrap().method, EnterWorldRequest::METHOD);
    }

    #[tokio::test]
    async fn generated_binding_decodes_payload_and_forwards_typed_request() {
        let binding = ProstClientMessageBinding::<EnterWorldRequest>::new(100);
        let core = FakeCore::default();
        let routed = core.routed.clone();
        let request = EnterWorldRequest { world_id: 42 };
        let frame = ClientFrame {
            msg_id: 100,
            payload: request.encode_to_vec(),
        };

        let reply_frame = binding.decode_and_forward(frame, core).await.unwrap();

        assert_eq!(reply_frame.msg_id, 100);
        assert_eq!(*routed.lock().unwrap(), vec![RouteKey::U64(42)]);
        let reply = EnterWorldReply::decode(reply_frame.payload.as_slice()).unwrap();
        assert!(!reply.ok);
    }

    #[test]
    fn gateway_push_validates_session_id_and_connection_epoch() {
        let mut sessions = GatewaySessionRegistry::new();
        let first = sessions.connect("session-1");
        let second = sessions.connect("session-1");
        let push = GatewayPush {
            session: second.clone(),
            frame: ClientFrame {
                msg_id: 9,
                payload: Vec::new(),
            },
        };
        let stale = GatewayPush {
            session: first,
            frame: ClientFrame {
                msg_id: 9,
                payload: Vec::new(),
            },
        };

        assert_eq!(sessions.validate_push(&push), Ok(()));
        assert!(matches!(
            sessions.validate_push(&stale),
            Err(GatewayError::StaleSession { .. })
        ));
    }

    #[test]
    fn keyed_rate_limiter_is_scoped_by_principal_session_and_rate_class() {
        let mut limiter = KeyedRateLimiter::new(1, Duration::from_secs(60));
        let key = RateLimitKey {
            principal_id: "player-1".into(),
            session_id: "session-1".into(),
            rate_class: "move".into(),
        };
        let other_class = RateLimitKey {
            rate_class: "chat".into(),
            ..key.clone()
        };

        assert_eq!(limiter.check(key.clone()), Ok(()));
        assert_eq!(limiter.check(key), Err(GatewayError::RateLimited));
        assert_eq!(limiter.check(other_class), Ok(()));
    }
}
