# Lattice Cluster Discovery and Member Lifecycle Execution Plan

> Status: planned, not implemented
> Authority model: Coordinator + etcd; discovery is bootstrap-only
> Compatibility policy: hard switch; no mixed-version cluster support
> Behavioral references: Apache Pekko discovery and cluster lifecycle, without Gossip or SBR

---

## 1. Goal

Implement automatic cluster bootstrap and an explicit member lifecycle so a node can discover and
join a lattice cluster without an application calling `connect_peer` with a complete
`NodeIdentity`.

The finished system must support four discovery sources:

- a static endpoint list;
- a watched `ConfigStore` document;
- DNS SRV or A/AAAA records;
- Kubernetes EndpointSlice watches.

Discovery finds reachable bootstrap endpoints only. It cannot admit members, allocate placement,
declare a node dead, or become a second membership database. The Coordinator remains the sole
authority for member identity, status, revisions, leases, placement claims, drain and removal.

`LatticeService::start()` binds remoting and then joins automatically. A node is not Ready and does
not open external admission until Coordinator admission and an atomic snapshot installation have
completed. Join retries indefinitely by default. Graceful leave drains placement before transport
shutdown; a bounded shutdown may force-stop after its deadline.

## 2. Current State and Gaps

The repository already has the main safety primitives, but they are not composed into an automatic
cluster lifecycle:

- `LatticeService::start()` binds the endpoint and starts preassembled runtimes. It performs no
  discovery and cannot construct a logic Coordinator session dynamically.
- `LatticeService::connect_peer(NodeIdentity)` is the only initial outbound connection entry point.
  Callers must already know the peer node ID, address and boot incarnation.
- `RemotingEndpoint::connect_peer` establishes exact-incarnation Associations and reconnects lanes,
  but its handshake requires the full expected remote identity before connecting.
- `CoordinatorStore::get_leader()` and `LeaderRecord` provide an authoritative leader identity.
  Coordinator snapshots already include `member/<node_id>` records, but logic sessions currently
  use them only as snapshot input and do not maintain a peer directory.
- Placement already implements `NodeHello`, heartbeat expiry, member leases, `BeginDrain`,
  `DrainComplete`, `NodeRemoved`, claim fencing and recovery. Persisted membership is a bare
  `NodeHello`, so Joining, Up and Leaving are not explicit cluster states.
- The local service reducer already models Booting, Joining, Ready, Degraded, Draining, Stopping and
  Terminated, but shutdown currently force-stops rather than coordinating leave.
- `lattice-config` provides file/env/composite bootstrap configuration and a generic watched
  `ConfigStore`; `lattice-config-etcd` provides its etcd implementation. There are no DNS or
  Kubernetes discovery dependencies.

The main protocol blocker is identity discovery. DNS and Kubernetes normally return only a host and
port. They cannot supply the exact `NodeIncarnation` required by the existing handshake and TLS
identity checks. Discovery therefore cannot be implemented as a thin wrapper around
`connect_peer`; remoting needs a separate authenticated bootstrap probe before creating an
Association.

## 3. Architectural Decisions

### 3.1 Crate boundaries

Add two workspace crates:

```text
lattice-discovery
  provider.rs       object-safe discovery contract and common types
  aggregate.rs      provider merge, deduplication and rotation
  static.rs         fixed endpoint provider
  config_store.rs   watched versioned ConfigStore document
  dns.rs            DNS SRV and A/AAAA provider

lattice-discovery-k8s
  endpoint_slice.rs Kubernetes EndpointSlice provider
```

`lattice-discovery` depends on `lattice-core`, `lattice-config`, Tokio and a proven DNS resolver.
Use `hickory-resolver` for TTL-aware asynchronous DNS resolution. The Kubernetes crate depends on
`lattice-discovery`, `kube` and `k8s-openapi`; this prevents Kubernetes dependencies from entering
the normal `lattice-service` dependency graph unless an application selects the provider.

No crate root re-exports are added. Public types are used through their defining module paths. Each
Rust file remains below 1200 physical lines and non-test wildcard imports remain forbidden.

### 3.2 Discovery contract

Define these public types under `lattice_discovery::provider`:

```rust
pub struct DiscoveryTarget {
    pub address: NodeAddress,
    pub expected_node_id: Option<String>,
    pub source: DiscoverySource,
    pub priority: u16,
}

pub struct DiscoverySnapshot {
    pub generation: u64,
    pub targets: Vec<DiscoveryTarget>,
}

pub trait ClusterDiscovery: Send + Sync {
    fn snapshots(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<DiscoverySnapshot, DiscoveryError>> + Send + '_>>;
}
```

The first stream item is a complete current snapshot. Every later item replaces that provider's
previous snapshot. Generations are nonzero and strictly increase within one provider process.
Providers reconnect internally after transient errors and retain their last valid snapshot; a watch
failure or temporary empty response is not interpreted as authoritative member removal.

`AggregateDiscovery` accepts an ordered list of providers. It keeps the latest valid snapshot from
each provider, merges targets by `NodeAddress`, retains all source metadata, and chooses the lowest
numeric priority for a duplicate address. Candidate selection rotates targets within one priority so
the first configured endpoint does not become a permanent hotspot. Provider errors are observable
but do not erase healthy targets from other providers.

### 3.3 Provider behavior

`StaticDiscovery` validates all endpoints at construction and emits one immutable snapshot.

`ConfigStoreDiscovery<S: ConfigStore>` performs `get` before `watch` to avoid missing the initial
value. Its configured key contains a complete, versioned JSON document:

```json
{
  "schema_version": 1,
  "generation": 42,
  "endpoints": [
    { "host": "node-a", "port": 7447, "node_id": "node-a", "priority": 10 }
  ]
}
```

An absent key yields an empty initial snapshot. A malformed document, unsupported schema version or
non-increasing generation reports an error and retains the last valid document. The configuration
center publishes candidate endpoints; it does not write member status or placement state.

`DnsDiscovery` supports exactly two modes:

- SRV mode obtains host, port, priority and weight from the record. SRV targets are resolved to
  A/AAAA addresses only for reachability; the advertised DNS hostname remains the TLS server name.
- Host mode resolves A/AAAA for one configured hostname and combines results with a required fixed
  remoting port.

Refresh uses the returned TTL clamped to configurable minimum and maximum intervals. NXDOMAIN and
resolver failures retain the last valid non-expired snapshot and retry with the join backoff policy.

`KubernetesEndpointSliceDiscovery` uses the Kubernetes API watch, never `kubectl`. Configuration
contains namespace, service name, optional label selector and required port name. It selects slices
with `kubernetes.io/service-name=<service>`, accepts IPv4, IPv6 and FQDN address types, and ignores
endpoints whose `ready` is false or `terminating` is true. Watch expiration performs a full list and
resumes from the returned resource version. It uses in-cluster credentials in production and
kubeconfig when explicitly requested for tests or local development.

### 3.4 Authority boundary

Discovery candidates may be the Coordinator or ordinary Up members. A contacted member returns its
current leader as a redirect. A node without a valid leader view returns retryable unavailable.

After join, the Coordinator snapshot and revisioned deltas form the only authoritative member
directory. DNS, Kubernetes and ConfigStore changes can alter future bootstrap candidates but cannot
add, remove or reroute a live member. Business message routing reads only the local authoritative
directory and never performs discovery or an etcd read on the message path.

## 4. Secure Bootstrap Protocol

### 4.1 Wire additions

Add dedicated `BootstrapRequest` and `BootstrapResponse` frame kinds and a required bootstrap
feature bit. These frames run before an Association handshake and cannot carry business or placement
traffic.

The request contains:

```text
local NodeIdentity
requested ClusterId
optional expected node ID
transport major/minor and feature bits
random nonzero probe nonce
```

The response echoes the nonce and has one result:

```text
Identity { remote NodeIdentity, optional leader record }
Redirect { remote NodeIdentity, leader NodeIdentity, term, protocol generation }
ReverseDial { remote NodeIdentity, optional leader record }
Rejected { stable rejection code }
RetryAfter { bounded delay and reason }
```

Stable rejection codes cover cluster mismatch, expected-node mismatch, incompatible transport,
missing required feature, invalid identity and authentication failure. Transient unavailability and
leader election use `RetryAfter`, not a permanent rejection.

### 4.2 Validation and TLS

The probe performs transport/TLS negotiation first, then identity discovery:

1. Validate the TLS chain and advertised endpoint hostname using the configured trust roots.
2. Decode the bounded bootstrap response and validate the echoed nonce and protocol version.
3. Require the returned cluster ID to equal the requested cluster ID.
4. If discovery supplied an expected node ID, require an exact match.
5. Extract the lattice identity from the peer certificate and require it to match the returned node
   ID and incarnation. In plaintext mode the identity remains an unauthenticated claim and is allowed
   only under the existing trusted-network policy.
6. Validate the returned address and nonzero incarnation before exposing the identity.

No Association is inserted into `AssociationManager` before these checks pass. Probe sockets accept
only bootstrap frames and close immediately after the response.

After learning the exact identity, apply the existing deterministic `(NodeAddress, incarnation)`
dial direction. If the probing node is the designated dialer, it opens the normal lanes. Otherwise
the response schedules a reverse connection using the exact source identity from the probe. A stale
Association for the same address and a different incarnation is fenced and disconnected before the
new one becomes routable.

Increase the transport minor version and required feature set. Unsupported peers fail explicitly;
there is no old-handshake fallback.

## 5. Join Controller and Peer Directory

Add an internal `lattice_service::cluster::join` controller supervised by `TaskSupervisor`. It owns
the discovery stream, current candidate set, retry state, Coordinator session and authoritative peer
directory.

The join sequence is:

1. `LatticeService::start()` binds remoting and transitions Booting to Joining.
2. The controller consumes the initial discovery snapshot and probes at most the configured number
   of candidates concurrently.
3. It follows bounded leader redirects and selects the valid leader with the highest Coordinator
   term, then protocol generation. Conflicting leaders with the same term are rejected and reported.
4. It establishes the exact-incarnation Coordinator Association and dynamically constructs the
   `LogicCoordinatorSession` from the builder's local `NodeHello` and runtime dependencies.
5. It sends `NodeHello`, stages every snapshot chunk, validates digest/revision and atomically
   installs placement plus member records.
6. It sends `JoinReady(snapshot_revision)`. Only the Coordinator's Up acknowledgement transitions
   the local service to Ready and opens admission.

Retry defaults are an initial 250 ms delay, factor 2, maximum 30 seconds, 20 percent jitter and four
concurrent probes. The default join timeout is absent, so an unreachable cluster leaves the service
in Joining and retries indefinitely. A configured timeout produces a terminal startup error and a
supervised shutdown.

Coordinator loss transitions Ready to Degraded and closes admission. Claims remain usable only
within their existing lease/safety rules; discovery does not extend them. The controller discovers
or follows the new leader, installs a new full snapshot when the revision history is discontinuous,
reconciles sessions and returns to Ready. Snapshot gaps are never applied incrementally.

The member directory is keyed by `(node_id, incarnation)` and publishes bounded snapshot/watch APIs.
The peer reconciler:

- keeps the Coordinator Association eager;
- establishes ordinary data-plane Associations lazily when routing first needs them;
- lets only the deterministic dialer initiate a connection;
- removes Associations for Removed members and superseded incarnations;
- never rewrites an `ActorRef` to a replacement node or incarnation.

## 6. Authoritative Member Lifecycle

### 6.1 Persisted model

Replace persisted bare `NodeHello` values with:

```rust
pub struct MemberRecord {
    pub node: NodeKey,
    pub hello: NodeHello,
    pub status: MemberStatus,
    pub revision: Revision,
    pub lease_id: i64,
}

pub enum MemberStatus {
    Joining,
    Up,
    Leaving,
}
```

`Removed` is a revisioned event rather than an active stored status. It contains the exact `NodeKey`
and `MemberRemovalReason` (`GracefulLeave`, `FailureDetected`, `ForceRemoved`, or
`IncarnationReplaced`). Snapshots contain only active member records; deltas deliver Upsert and
Removed events in Coordinator revision order.

Storage writes use compare-and-swap against the expected incarnation, status and revision. A node ID
cannot have two Up incarnations. Registering a newer incarnation fences and removes an older expired
record; it cannot replace a still-live lease without an explicit force-remove operation.

This storage change increments the Coordinator schema generation. No dual-read or dual-write format
is implemented.

### 6.2 Join transitions

The Coordinator validates `NodeHello`, protocol catalogue, roles, capacity and entity/singleton
configuration before granting a member lease. It persists Joining, registers the live session and
sends a snapshot. `JoinReady` is accepted only from the same Association and incarnation and only
for the exact completed snapshot revision. The Coordinator CAS-transitions Joining to Up, publishes
the member delta and replies with an Up acknowledgement.

A disconnect while Joining expires or revokes the provisional lease and never produces an Up
member. Repeated `NodeHello` and `JoinReady` from the same session are idempotent.

### 6.3 Graceful leave

`LatticeService::leave(deadline)` is idempotent and executes:

1. transition Ready or Degraded to Draining and close new external and activation admission;
2. send `BeginDrain` with an operation ID and expected incarnation;
3. Coordinator CAS-transitions Up to Leaving and starts persisted shard/singleton handoff;
4. after all owned placement is moved or safely fenced, Coordinator sends `DrainReady`;
5. the node verifies local activations are stopped and sends `DrainComplete`;
6. Coordinator revokes the member lease, removes the active record and broadcasts
   `Removed(GracefulLeave)`;
7. the node closes peer Associations, logic runtime and remoting, then reaches Terminated.

Calling leave in Joining cancels the join and stops without creating an Up member. Calling it in
Draining waits on the existing operation. Calling it after Terminated succeeds without side effects.

### 6.4 Failure and administrative removal

Existing heartbeat and lease expiry remain the failure detector. On timeout the Coordinator emits
`Removed(FailureDetected)`, fences claims/barriers, fails pending sessions and begins placement
recovery. Restarting at the same address always creates a new incarnation and cannot inherit an old
claim or Association.

Add an administrative force-remove command containing `operation_id`, `node_id` and
`expected_incarnation`. The Coordinator rejects an incarnation mismatch, records completed operation
IDs for bounded idempotency, revokes the matching lease and emits `Removed(ForceRemoved)`. It cannot
remove a newer process that reused the node ID.

## 7. Service API and Configuration

Add builder methods using explicit module paths:

```text
cluster_discovery(Arc<dyn ClusterDiscovery>)
join_config(ClusterJoinConfig)
member_event_capacity(usize)
```

`ClusterJoinConfig` contains probe concurrency, retry initial/max delay, multiplier, jitter, optional
join timeout, discovery stale grace, graceful leave timeout and overall shutdown timeout. Validation
rejects zero limits, an initial delay above the maximum, jitter outside `[0, 1)`, and a leave timeout
above the overall shutdown timeout.

Add service operations:

```text
leave(deadline)          idempotent coordinated drain and removal
force_shutdown()        immediate local fence and stop
lifecycle_state()       current local state
subscribe_lifecycle()   bounded watch receiver
member_snapshot()       current authoritative member directory
subscribe_members()     bounded revisioned event receiver
```

`shutdown()` spends the configured leave budget on graceful leave, then force-stops if the deadline
expires. `connect_peer(NodeIdentity)` remains a diagnostic/test transport operation; it neither
admits a member nor marks the service Ready.

Canonical configuration:

```yaml
cluster:
  join:
    timeout: null
    retry_initial: 250ms
    retry_max: 30s
    retry_multiplier: 2
    retry_jitter: 0.2
    probe_concurrency: 4
    discovery_stale_grace: 60s
    leave_timeout: 30s
    shutdown_timeout: 45s
  discovery:
    - type: static
      priority: 10
      endpoints: ["node-a:7447", "node-b:7447"]
    - type: config_store
      priority: 20
      key: "/lattice/clusters/prod/discovery/endpoints"
    - type: dns
      priority: 30
      service: "_lattice._tcp.cluster.example.com"
    - type: kubernetes
      priority: 40
      namespace: lattice
      service: lattice-nodes
      port_name: remoting
```

Kubernetes manifests grant list/watch on `discovery.k8s.io/v1` EndpointSlices in the selected
namespace. Workloads use a `preStop` hook that initiates leave, and
`terminationGracePeriodSeconds` exceeds the configured shutdown timeout.

## 8. Observability

Emit structured tracing and metrics for:

```text
discovery provider health, generation, target count and stale age
bootstrap attempts, latency, redirects, reverse dials and rejection code
join attempts, join duration, selected leader term and snapshot revision
member state transitions and removal reason
degraded duration, reconciliation and snapshot restart count
drain duration, moved placement count and forced shutdown count
stale incarnation rejection and Association replacement count
```

Logs include cluster ID, local node ID/incarnation, target address, Coordinator term and operation ID
where applicable. They must not include certificate private material or unbounded discovery payloads.

## 9. Execution Batches

### Batch A: discovery foundation

- Add both discovery crates, workspace dependencies and feature-independent public contracts.
- Implement static, ConfigStore, DNS, aggregate and EndpointSlice providers.
- Add deterministic provider tests with fake stores/resolvers/watch streams.
- Document provider configuration and Kubernetes RBAC.

Exit condition: every provider produces validated replacement snapshots, reconnects after transient
failure and passes aggregation/deduplication tests without depending on service or placement.

### Batch B: bootstrap remoting

- Add bootstrap frames, codecs, feature negotiation and bounded validation.
- Extend TCP/TLS accept and connect paths with probe-only sockets.
- Implement identity/certificate binding, leader redirect and reverse dial.
- Fence stale incarnations before activating a replacement Association.

Exit condition: a caller can turn an untrusted discovered `NodeAddress` into a validated exact
`NodeIdentity`, while failed probes create no Association and carry no business frames.

### Batch C: membership state and storage

- Introduce MemberRecord/status/events and bump the storage schema generation.
- Update memory and etcd stores with incarnation/revision CAS operations.
- Add JoinReady, DrainReady, revisioned member deltas and force-remove commands.
- Complete heartbeat expiry, graceful removal and recovery behavior against the new model.

Exit condition: Coordinator tests prove one Up incarnation per node ID, idempotent transitions,
fenced stale commands and correct removal events.

### Batch D: service composition

- Add discovery/join configuration and dynamic logic session construction.
- Implement the supervised join controller, member directory and peer reconciler.
- Connect service admission to Joining/Ready/Degraded/Draining transitions.
- Implement leave, graceful shutdown deadline and force shutdown.
- Migrate distributed examples and tests away from mandatory manual `connect_peer` bootstrap.

Exit condition: a multi-process cluster reaches Ready from configured endpoints alone, survives a
Coordinator rollover and removes a gracefully leaving node without stale routing.

### Batch E: acceptance and operations

- Add Docker static and ConfigStore discovery scenarios.
- Add kind EndpointSlice/RBAC/rolling-update scenarios.
- Add scoped cleanup for expired lattice test images and enforce it in Docker/kind test teardown.
- Extend simulation state machines and invariant checks.
- Add dashboards/runbooks for stuck Joining, Degraded, drain timeout and force removal.
- Update architecture, deployment and upgrade documentation.

Exit condition: all acceptance scenarios and repository quality gates pass and emit replayable
evidence for failure cases.

## 10. Test Matrix

### Discovery

- Static target validation and stable one-shot generation.
- ConfigStore initial get/watch race, malformed update, generation rollback and reconnect.
- DNS SRV priority/port, A/AAAA host mode, TTL refresh, NXDOMAIN and stale retention.
- EndpointSlice add/update/delete, not-ready/terminating filtering, watch expiration and relist.
- Aggregate merge, deduplication, priority, rotation and one-provider failure.

### Remoting bootstrap

- Unknown endpoint identity discovery over TCP and TLS.
- Cluster mismatch, expected-node mismatch, certificate mismatch and unsupported feature rejection.
- Probe nonce mismatch, oversized frame and business frame before Association rejection.
- Ordinary-member leader redirect, leader election RetryAfter and redirect-loop bound.
- Simultaneous probes, reverse dial, endpoint reuse and stale-incarnation fencing.

### Join and reconciliation

- Empty discovery remains Joining and retries without opening admission.
- Optional join timeout terminates cleanly.
- NodeHello incompatibility is terminal; unreachable candidates are retryable.
- Snapshot install plus JoinReady produces Up/Ready exactly once.
- Revision gap and leader rollover enter Degraded, close admission and require reconciliation.
- Discovery outage after join does not remove authoritative members or interrupt healthy routing.

### Leave and failure

- Repeated leave calls share one operation and result.
- Shard and singleton drain complete before member removal.
- Leader failure during drain resumes from persisted handoff state.
- Shutdown deadline forces a local fence and later lease-based removal.
- Crash/heartbeat expiry removes the exact incarnation and starts recovery.
- Force-remove retries are idempotent and reject a newer incarnation.

### End-to-end and simulation

- Docker clusters bootstrap independently from static and ConfigStore providers.
- kind clusters react to EndpointSlice watches, Pod readiness, rolling replacement and `preStop`.
- DNS endpoint rotation does not create duplicate member identities.
- Simulated reorder, duplicate, partition, crash and lease expiry preserve all lifecycle invariants.

Required repository checks:

```text
scripts/check-structure.sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
Docker quality profile
targeted distributed Docker and kind profiles
```

### Docker image lifecycle

Docker and kind acceptance tests must clean up their generated artifacts as part of the test
workflow. Repeated local or shared-runner executions must not leave enough obsolete images to fill
the host disk.

All lattice-owned test images use immutable commit/run tags and the label
`org.realm-labs.lattice.test=true`. Teardown performs the following operations even when a test
fails:

1. Run Compose down with orphan and test-volume removal, then delete the temporary kind cluster.
2. Remove stopped containers, networks and volumes created by the current test run.
3. Remove unused lattice-labeled test images older than 72 hours on shared CI runners and older than
   7 days on developer machines.
4. Preserve images used by a running container and the current run's image tags.
5. Check disk usage before image builds. At 80 percent usage, remove the oldest unused
   lattice-labeled images before continuing; fail with an actionable diagnostic if usage remains
   above 90 percent.

Implement the policy in one repository script used by `testctl`, CI and kind teardown so retention
rules cannot drift. Cleanup must always filter on the lattice test label. Do not use unscoped
`docker system prune`, `docker image prune -a`, or commands that can delete unrelated developer or
runner images. Scheduled CI cleanup is an additional safeguard, not a replacement for per-run
teardown.

## 11. Safety Invariants and Acceptance

The implementation is accepted only when all of these hold:

```text
Normal cluster startup requires no application call to connect_peer.
Each of the four discovery sources can bootstrap a cluster independently and in aggregate.
Discovery input never mutates authoritative membership or placement directly.
A node opens admission only after Coordinator Up acknowledgement for an installed snapshot revision.
At most one Up incarnation exists for one node ID.
Business routing uses only the authoritative local member directory.
A stale ActorRef or Association never retargets a replacement incarnation.
Graceful leave retains no activation, claim, member lease or Association after termination.
Failed nodes are removed after the configured heartbeat/lease deadline and trigger recovery.
Leader rollover and temporary discovery failure create no duplicate member or placement authority.
Every task, queue, stream, retry set and event history is bounded and supervised.
```

## 12. Rollout and Explicit Non-goals

The bootstrap feature bit, member storage schema and control protocol form one hard-switch release.
Drain and stop the old cluster before starting the new version. Mixed-version rolling upgrade,
deprecated API aliases, old/new storage dual writes and handshake fallback are intentionally absent.

The following remain out of scope:

- Gossip membership, decentralized leader election and split-brain resolution;
- using DNS, Kubernetes or ConfigStore as the live member database;
- cross-cluster discovery or federation;
- NAT traversal for reverse dial;
- automatically retargeting stale ActorRefs;
- exactly-once delivery or replaying business messages after reconnect.

Pekko is a behavioral reference for provider abstraction, retry and coordinated lifecycle only.
Lattice retains its Coordinator terms, etcd leases, exact incarnations, revisioned snapshots and
placement fencing as the authoritative distributed model.
