# 02. Actor Remoting and Messaging

> The filename is retained temporarily for link compatibility. The target architecture is actor remoting, not gRPC.
> Back to: [architecture index](README.md)

---

## 1. One Messaging Model, Three Reference Semantics

All business messages use the actor runtime locally and `lattice-remoting` remotely. The reference type determines identity and routing semantics.

### 1.1 Concrete `ActorRef<A>`

```rust
pub struct ActorRef<A: Actor> {
    cluster_id: ClusterId,
    node_address: NodeAddress,
    node_incarnation: NodeIncarnation,
    actor_path: ActorPath,
    activation_id: ActivationId,
    protocol_id: ProtocolId,
    _actor: PhantomData<fn() -> A>,
}
```

An `ActorRef` may point to any live user actor or child actor. It routes directly to the exact node incarnation and exact activation. If the process restarts or the actor is replaced at the same path, the old reference becomes dead; it never silently follows the replacement.

lattice does not implement remote deployment, wildcard selection, or path-only references in the first version.

### 1.2 Logical `EntityRef<A>`

```rust
pub struct EntityRef<A: Actor> {
    cluster_id: ClusterId,
    entity_type: EntityType,
    entity_id: EntityId,
    protocol_id: ProtocolId,
    entity_config_fingerprint: EntityConfigFingerprint,
    _actor: PhantomData<fn() -> A>,
}
```

An `EntityRef` is resolved through the caller's local ShardRegion. It follows shard relocation and entity passivation. It identifies a logical entity, not one activation.

### 1.3 Logical `SingletonRef<A>`

```rust
pub struct SingletonRef<A: Actor> {
    cluster_id: ClusterId,
    singleton_kind: SingletonKind,
    protocol_id: ProtocolId,
    singleton_config_fingerprint: SingletonConfigFingerprint,
    _actor: PhantomData<fn() -> A>,
}
```

`SingletonRef` is resolved through a local SingletonProxy. It follows singleton failover and supports only configured singleton kinds.

### 1.4 Common API

```rust
recipient.tell(message).await?;
let reply = recipient.ask(message, deadline).await?;

let actor_watch = ctx.watch(actor_ref.clone()).await?;
let entity_watch = ctx.watch_current(entity_ref.clone()).await?;
let singleton_watch = ctx.watch_current(singleton_ref.clone()).await?;
ctx.unwatch(actor_watch).await?;
```

Tell and ask share a recipient surface. DeathWatch is deliberately explicit: `watch` targets the exact activation already encoded by `ActorRef`; `watch_current` resolves but never activates the current entity/singleton activation and returns `NotActive` or `Unavailable` when none exists.

## 2. Actor Protocols and Codecs

Transport framing is format-neutral. Each remotely sendable message has a stable numeric type ID and registered codecs.

### 2.1 Canonical Protocol Declaration

The canonical source is one explicit Rust declaration. It is type-checked, works without runtime registration magic, and is suitable as generated output from a business IDL/catalogue.

```rust
lattice::actor_protocol! {
    pub PlayerProtocol for PlayerActor {
        protocol_id: 0x504c_4159_4552_0001;
        name: "player/v1";

        tell 1 => PositionUpdated {
            codec: PostcardCodec::default(),
        }

        ask 2 => GetProfile {
            request_codec: PostcardCodec::default(),
            reply_codec: PostcardCodec::default(),
        }
    }
}

let player_protocol = PlayerProtocol::build()?;
```

The macro generates a typed registrar, immutable canonical descriptor, explicit `ProtocolId`, and `ProtocolFingerprint`. It verifies message ID/mode uniqueness and emits Rust bounds for `Handler<M>`, `Message::Reply`, and codecs.

Large businesses may generate this macro declaration from an existing IDL, spreadsheet, YAML, or internal message catalogue. Such external input is business tooling; lattice consumes the generated Rust declaration. Runtime configuration cannot add Rust message types or change protocol IDs, message IDs, modes, codecs, or fingerprints.

### 2.2 Low-Level Builder

The builder remains public for tests, very small protocols, and custom generators. The canonical macro expands to equivalent typed calls:

```rust
pub trait WireCodec<T>: Send + Sync + 'static {
    const CODEC_ID: u64;
    const CODEC_VERSION: u32;

    fn encode(&self, value: &T, dst: &mut BytesMut) -> Result<(), EncodeError>;
    fn decode(&self, src: &[u8]) -> Result<T, DecodeError>;
}

ActorProtocol::<PlayerActor>::builder(
    ProtocolId::new(0x504c_4159_4552_0001),
    "player/v1",
)
    .tell::<PositionUpdated>(
        1,
        PostcardCodec::default(),
    )
    .ask::<GetProfile>(
        2,
        PostcardCodec::default(),
        PostcardCodec::default(),
    )
    .build()?;
```

```rust
pub fn tell<M>(
    self,
    message_id: u64,
    message_codec: impl WireCodec<M>,
) -> Self
where
    M: Message<Reply = ()> + WireSchema,
    A: Handler<M>;

pub fn ask<M>(
    self,
    message_id: u64,
    message_codec: impl WireCodec<M>,
    reply_codec: impl WireCodec<M::Reply>,
) -> Self
where
    M: Message + WireSchema,
    M::Reply: WireSchema,
    A: Handler<M>;
```

`ActorProtocol` explicitly declares the permitted interaction mode. A `tell` registration has no reply codec and permits fire-and-forget delivery only. An `ask` registration includes the associated reply codec and permits correlation, deadline, and handler-completion response. An ask message may still use `Reply = ()` with `UnitCodec` when only completion acknowledgement is required.

### 2.3 Registration Rules

Rules:

1. IDs are explicit and stable within an actor protocol.
2. Ask request and reply codecs are registered independently; tell registers only its message codec.
3. Local sends remain typed and do not serialize.
4. Remote dispatch decodes before entering `Handler<M>`.
5. Unknown IDs, protocol fingerprint mismatch, and oversized payloads fail before mailbox admission.
6. Internal control messages may use Prost, but business protocols are not tied to Protobuf.
7. A message ID is registered exactly once as either tell or ask; duplicate IDs or conflicting modes fail protocol construction.
8. `tell` registration requires `Message<Reply = ()>`; `ask` accepts any `Message`, including a unit reply.
9. A received frame whose mode does not match the message registration is rejected as a protocol violation.
10. Macro-generated and manually built protocols pass through the same validation and produce the same fingerprint.
11. `ProtocolId` is an explicit stable `u64`; automatic IDs derived from Rust type names or declaration order are forbidden.
12. Every request/reply type implements `WireSchema { SCHEMA_ID, SCHEMA_VERSION }`; every codec exposes stable `CODEC_ID` and `CODEC_VERSION`.
13. The fingerprint is BLAKE3 over the canonical descriptor fields: protocol ID/name, ordered message IDs/modes, codec IDs/versions, and request/reply schema IDs/versions.
14. After the transport Association is established, a bounded control exchange advertises the supported `(ProtocolId, ProtocolFingerprint)` catalogue. References carry only `ProtocolId`; frames carry protocol and message IDs.

V1 requires an exact fingerprint for a given ProtocolId. A compatible rolling business-protocol upgrade registers old and new major protocols under distinct explicit ProtocolIds and keeps both during the rollout. Reusing one ProtocolId with a changed fingerprint is rejected; silent additive compatibility guesses are forbidden.

Transport compatibility and business-protocol compatibility are separate fault domains. The initial handshake negotiates only transport version, node identity, limits, security, and mandatory transport features. The protocol catalogue is exchanged afterward over the reliable control channel and is bounded/chunked like other control state. An unsupported or mismatched `ProtocolId` disables delivery for that protocol and excludes the node from hosting dependent entity/singleton types; it does not close an otherwise compatible Association or disable unrelated protocols.

The reliable control envelope and framework control schemas belong to the remoting transport version and do not depend on a business ActorProtocol catalogue, avoiding a negotiation cycle.

## 3. Delivery Semantics

### 3.1 `tell`

`tell` is at-most-once. Success means the local remoting runtime accepted the envelope into a bounded outbound queue; it does not confirm handler completion. Queue saturation, a known-dead activation, codec failure, or a closed association is returned explicitly when detectable.

### 3.2 `ask`

`ask` adds a boot-unique correlation ID, caller-local monotonic deadline, expected reply type, and one bounded pending entry. A reply or typed remote failure completes that entry.

Timeout and disconnect can produce `UnknownResult`: the caller cannot know whether the destination handler ran. The framework does not automatically retry state-changing asks. Business protocols that require retry must carry their own idempotency key.

The caller deadline is authoritative. Before local admission, Region buffering, and socket write, the sender rejects an expired ask and encodes only the remaining duration as `timeout_budget`; it never sends a wall-clock timestamp. The receiver derives a local monotonic deadline at receipt and checks it before decode/mailbox admission and again before starting the Handler. Handler execution is not forcibly cancelled after it starts, because cancellation cannot roll back business effects. A reply arriving after the caller removed its pending entry is discarded and counted.

The writer crossing from queued to a socket write is the uncertainty boundary. Failure before any frame bytes are committed is a known send failure; disconnect after writing begins yields `UnknownResult` unless a typed remote rejection/reply was received.

### 3.3 Business Replies and Remote Failures

Expected domain failures belong in the typed reply and use its registered reply codec:

```rust
impl Message for ReserveItem {
    type Reply = Result<Reservation, ReserveItemError>;
}
```

`ActorError`, panic, decode failure, stale activation, mailbox rejection, authorization failure, and deadline rejection are framework execution failures carried by a `Failure` frame with a stable `RemoteFailureCode`. Rust error type names, debug output, backtraces, and arbitrary strings are never wire contracts. Optional safe text/details are redacted and size-bounded. Tell-side Handler failures have no response and go to supervision, telemetry, and dead-letter/error inspection.

### 3.4 Ordering

Messages from the same sender to the same recipient on one stable physical lane are emitted in order. All tells use bulk and are stably striped; asks/replies use interactive. There is no ordering guarantee across tell and ask lanes, different senders, reconnect, reroute, or shard handoff.

## 4. Association Runtime

### 4.1 Logical Association and Physical Connections

`AssociationManager` keeps at most one logical Association for an exact peer pair:

```rust
pub struct AssociationKey {
    pub local_incarnation: NodeIncarnation,
    pub remote_address: NodeAddress,
    pub remote_incarnation: NodeIncarnation,
}
```

An Association owns a fixed bounded connection group:

```text
control connection       exactly 1: handshake, heartbeat, watch, Coordinator, lifecycle, close
interactive connection   exactly 1: ask, reply and failure
bulk connection stripes  default 1: every tell; configurable 1..4
```

This is not a request checkout/return pool. Every physical connection has one supervised reader and one supervised writer that owns the socket. Sending selects a lane/stripe and enqueues a frame into its bounded queue; business code never sees a socket, connection, lane, or session handle.

Separate physical connections prevent a bulk frame already written to one TCP stream from head-of-line blocking heartbeat, watch, handoff, ask, or reply traffic. The group still behaves as one peer identity and one lifecycle unit.

### 4.2 Lane and Stripe Selection

```text
handshake / heartbeat / watch / Coordinator / drain -> control
ask / reply / failure                              -> interactive
every tell                                          -> bulk stripe
```

Bulk selection is stable for the lifetime of one Association:

```text
stripe = stable_hash(sender identity, recipient identity) % bulk_stripe_count
```

For an actor sender, identity is `(ActorPath, ActivationId)`. A Gateway adapter, EventBus adapter, scheduler, or service task receives a boot-scoped stable `SenderId` from the service runtime. Recipient identity is the canonical serialized ActorRef/EntityRef/SingletonRef routing identity. Random per-message IDs and thread/task IDs are forbidden stripe inputs.

Messages from one sender to one recipient therefore remain on one TCP stream. Round-robin per frame is forbidden because multiple TCP streams could reorder consecutive tells. Reconnect, reroute, different senders, or shard handoff still provide no global ordering guarantee.

The writer may batch multiple complete frames into one socket write and use vectored I/O or buffer pooling. It must not merge business messages, change mailbox ordering guarantees, or create an unbounded batch. Domain-level coalescing or batching remains an explicit typed business message.

If bulk tell throughput is insufficient, optimize association scheduling, encoding, allocation, batching, or mailbox admission first. Do not restore public Direct Link as a parallel transport.

### 4.3 Establishment

Associations are lazy and single-flight:

```text
first remote send
  -> get_or_connect(AssociationKey)
  -> establish and authenticate control connection
  -> negotiate AssociationId, incarnations, version, limits and features
  -> attach interactive connection
  -> attach configured bulk stripes
  -> Association Ready
```

Every physical TLS connection authenticates the peer independently. Its binary handshake then binds it to the negotiated `AssociationId`, exact local/remote incarnations, lane kind, and stripe index. Plaintext uses the same structural validation but provides no cryptographic peer authentication.

The control handshake validates cluster ID, advertised endpoint, protocol version, maximum frame size, supported features, and TLS identity when enabled. A stale remote incarnation is rejected and quarantined. Simultaneous dials and duplicate lane attachments are resolved deterministically so only one Association and one connection per declared lane/stripe survive.

`AssociationId` also defines the in-memory Association epoch. It remains stable while that supervised Association reconnects physical lanes to the same peer incarnation and retains its bounded control reconciliation state. Closing/recreating the logical Association, changing either node incarnation, or losing that state creates a new epoch; old control envelopes cannot cross the boundary.

Connections are not eagerly created as a cluster-wide full mesh. An idle bulk or interactive connection may close when its queue and in-flight work are empty; a later send reattaches it through the existing control connection. The control connection and logical Association may close only when there are also no watches, pending asks, hosted control duties, or other peer dependencies. Association count, derived physical-connection/FD budget, concurrent handshakes, reconnect rate, queued bytes, and idle retention are bounded.

### 4.4 Lifecycle and Partial Failure

```text
Disconnected -> Connecting -> Handshaking -> Ready
Ready -> Degraded -> Reconnecting -> Ready
Ready/Degraded -> Closing -> Closed
```

- Control connection failure immediately stops new interactive/bulk admission for the whole Association, fails queued-but-unwritten work, and starts bounded reconnect/failure-detection handling. Already running remote Handlers are unaffected; no data lane continues independently in v1.
- Interactive failure completes affected pending asks according to dispatch knowledge, including `UnknownResult` where necessary.
- One bulk stripe failure fences new admission to that stripe and reconnects it; other ready stripes may continue.
- No lane automatically replays a business frame that might have been written.
- A changed remote `NodeIncarnation` closes the old Association; queued frames and watches never attach to the replacement process.
- Closing joins every reader, writer, reconnect task, pending ask, and bounded queue under the Association supervisor.

### 4.5 Reliable Control Delivery

TCP ordering is sufficient only while one physical control connection remains alive. Control messages whose effect must survive reconnect use one Association-level reliable control stream rather than implementing independent retry loops in DeathWatch, Coordinator, Shard, and Singleton code.

```text
ControlEnvelope {
  association_epoch,
  control_sequence,
  command_id,
  kind,
  payload,
}

ControlAck {
  association_epoch,
  cumulative_sequence,
}
```

Rules:

- Handshake, heartbeat, transient backpressure hints, and close negotiation are transport frames and are not replayed as durable control commands.
- Watch/Unwatch/Terminated, protocol catalogue pages, Coordinator snapshot/delta messages, claim grants, handoff, drain, and readiness messages use the sequenced control stream.
- The sender retains a bounded unacknowledged outbox and replays it only to the same remote `NodeIncarnation` and logical Association epoch after reconnect.
- Receivers apply commands idempotently by `(association_epoch, command_id)` and acknowledge cumulatively. Duplicate transmission is allowed; duplicate state transition is not.
- An outbox overflow, unrecoverable sequence gap, changed incarnation, or lost reconciliation state fails the affected control session and triggers its authoritative recovery path, such as a fresh Coordinator snapshot or complete watch-set reconciliation.
- Reliable control delivery does not provide exactly-once business processing and never replays tell/ask frames whose socket-write outcome is uncertain.

Higher-level control messages still carry their own Coordinator term, assignment generation, grant sequence, revision, or ActivationId. Transport sequencing preserves delivery and reconnect recovery; those domain fields reject stale commands and make application idempotent.

### 4.6 Initial Defaults

Recommended initial defaults:

| Setting | Default |
|---|---:|
| Control connections per Association | 1, fixed |
| Interactive connections per Association | 1, fixed |
| Bulk stripes per Association | 1; configurable 1..4 |
| Maximum active Associations per node | 256, configurable from the process FD budget |
| Maximum outbound bytes per Association | 16 MiB |
| Maximum total outbound bytes per node | 256 MiB |
| Maximum frame | 256 KiB |
| Handshake timeout | 3 s |
| Heartbeat interval | 2 s |
| Suspect after | 3 missed heartbeats |
| Ask deadline cap | 30 s |
| Idle data-connection timeout | 60 s |
| Reconnect | exponential backoff with jitter |

The configured FD budget must cover at least `listener sockets + active_associations × (2 + bulk_stripes)` plus operational headroom. Startup rejects an impossible connection/FD configuration rather than failing later under load.

## 5. Wire Protocol

```text
magic | major | minor | kind | flags | header_len | payload_len | header | payload
```

Frame kinds include handshake, heartbeat, tell, ask, reply, failure, watch, unwatch, terminated, Coordinator control, backpressure, and close. Headers carry only routing and protocol metadata; payload bytes are interpreted by the registered actor protocol.

Remoting protocol version negotiation is explicit. Unknown mandatory transport features close the Association. Business protocol compatibility follows the per-ProtocolId fingerprint rule above: a mismatch rejects that protocol without expanding into an Association-wide failure. The legacy gRPC-to-remoting framework migration remains full-stop.

## 6. DeathWatch

Concrete `ActorRef` watch observes one exact activation. Watching an already-dead activation yields `Terminated` without making the reference valid. Remote watchers are indexed by destination activation and cleaned up on unwatch, watcher termination, or association loss. Each `WatchId` completes with at most one terminal notification even when reliable-control replay duplicates Watch or Terminated transmission.

`watch_current(EntityRef)` asks the ShardRegion to resolve the current activation without creating one. Inactive returns `WatchError::NotActive`. Once installed, the watch is bound to that exact activation; passivation, handoff, explicit stop, claim loss, or node-down produces `Terminated`. A later activation requires a new `watch_current` call.

`watch_current(SingletonRef)` resolves the current singleton activation without forcing allocation. No active owner returns `WatchError::Unavailable`. Failover terminates the old activation watch; the logical `SingletonRef` remains usable, but observing the replacement requires a new `watch_current` call.

Temporary Association loss is not itself termination. Reconnect re-registers the same exact watch; a missing or changed `ActivationId`, or Coordinator confirmation that the node incarnation is dead, completes it with `Terminated`.

## 7. Errors

The public error model distinguishes at least:

```text
EncodeFailed / DecodeFailed / ProtocolMismatch
UnknownActor / StaleActivation / Terminated
NotActive / Unavailable
AssociationUnavailable / QueueFull / Backpressured
AskTimeout / UnknownResult / RemoteFailure
ShardUnavailable / CoordinatorUnavailable / ClaimLost
Unauthorized / FrameTooLarge
```

Errors preserve the destination kind, correlation ID when present, and enough node/path metadata for tracing without exposing message payloads.

## 8. Security

Remoting supports two deployment profiles:

1. Plain TCP inside a trusted, isolated network.
2. TLS with node identity validation for shared or untrusted networks.

Authentication is performed during association handshake. Authorization may restrict actor systems, actor path prefixes, entity types, singleton kinds, and message IDs. TLS is optional; bounded frames, handshake validation, and authorization are not.

## 9. Removed Models

```text
No generated tonic service/client for internal actor calls.
No public Direct Link or manually managed business connection.
No parallel RPC and actor-message transports.
No separate stream/session abstraction for high-throughput actor tells.
No route cache repaired through NOT_OWNER business responses.
```
