# Placement-Domain Coordinator Hard-Switch Goal

> Status: planned
> Created: 2026-07-15
> Implementation baseline: `6241fed20274801a555690c0d8a1d2459b6f7404`
> Architecture baseline: [architecture/README.md](architecture/README.md)
> Correctness baseline: [coordinator-correctness-implementation-plan.md](coordinator-correctness-implementation-plan.md)
> Compatibility policy: hard switch; no adapters, aliases, dual routing, dual storage, or mixed generations
> Target storage generation: 5
> Target placement-control generation: 5

---

## 0. Goal Prompt

```text
Your goal is to fully execute docs/placement-domain-coordinator-goal.md.

Read this document completely before changing code. Then read the architecture and correctness
documents listed in Section 1. Treat this goal as the execution authority when the old documents
assume one cluster-wide Coordinator for all placement.

Start at the Current Execution Pointer and work through Batches A-E in dependency order. A checked
item requires production implementation plus executable evidence. Update this document whenever a
batch completes or the known broken frontier materially changes.

This is a hard switch to placement-domain-scoped leadership. Do not retain the cluster-wide
placement runtime as a fallback. Do not add an implicit default-domain compatibility path,
deprecated aliases, old/new control decoding, storage dual reads or writes, or a router that falls
back to the old ClusterLogicalRouter. Intermediate code may be broken while a macro batch crosses
crate boundaries; continue forward instead of restoring removed APIs.

The final model separates cluster membership from placement authority. Each EntityType and
SingletonKind belongs to one explicit PlacementDomain. One domain has one active logical leader and
may have multiple candidates. One physical CoordinatorHost may lead or stand by for many domains.
Different domains must be able to elect, fail, reconcile, route, rebalance and drain independently.

The default framework convention is one EntityType per PlacementDomain. Applications may explicitly
group related entity and singleton types into one domain. No application actor type, protocol type,
or deployment process is itself a Coordinator.

Preserve all generation-4 safety invariants while scoping them by domain: exact lease-backed leader
guards, atomic slot/claim and slot/plan transactions, term-qualified snapshots, per-slot claims,
generation fencing, forward-only handoff recovery, bounded reconciliation, and deterministic
simulation. A failure in one domain must never clear another domain's router, terminate another
domain leader, or make unrelated EntityRef/SingletonRef traffic unavailable.

Final completion requires format, clippy, workspace tests, real-etcd contracts, simulation/model
checking, Docker multi-process and HA scenarios, domain-isolation chaos, the offline generation-4 to
generation-5 migration test, documentation updates, and structural proof that the unscoped model is
gone.
```

### 0.1 Execution Policy

- This document is the sole active implementation tracker for the hard switch.
- Work on the earliest unchecked batch whose dependencies are satisfied.
- Read all required design sources before modifying production code.
- Keep the tracker, broken frontier, implementation, tests and documentation honest in the same
  change.
- Do not mark an item complete because an API or test skeleton exists.
- Do not preserve compilation by reintroducing the old unscoped Coordinator model.
- An offline full-stop migration utility is allowed; runtime compatibility is forbidden.
- Preserve unrelated user changes and stage only files belonging to the current macro batch.
- Run focused tests while advancing a batch. Run the complete acceptance suite at Batch E.
- Keep production files below 1200 physical lines unless the goal records a concrete exception.
- Avoid new facade re-exports, non-test wildcard imports and application-specific placement logic.

### 0.2 Current Execution Pointer

```text
Overall status: planned
Current batch: Batch A — scoped identity, configuration and durable schema
Completed batches: none
Known broken frontier: none; implementation has not started
Latest executable evidence: baseline 6241fed passed workspace format, check, clippy and tests before
this goal was created
Next implementation action: implement PlacementDomainId and remove unscoped placement identity
Next coherent stopping point: Macro Batch 1 after Batches A and B compile and focused tests pass
Final completion condition: every Batch A-E item and every acceptance criterion is checked
```

### 0.3 Batch Tracker

- [ ] Batch A — scoped identity, configuration and durable schema
- [ ] Batch B — membership plane and multi-domain CoordinatorHost
- [ ] Batch C — domain-scoped service routing and lifecycle
- [ ] Batch D — domain-scoped placement operations and administration
- [ ] Batch E — migration, simulation, distributed acceptance and documentation

### 0.4 Commit Boundaries

Prefer three coherent code commits after the Goal document itself:

```text
refactor(placement): introduce scoped placement domains
refactor(service): isolate placement domain routing
test(placement): complete coordinator domain hard switch
```

Batches A and B form the first macro commit. Batches C and D form the second. Batch E forms the
third. Do not create compatibility commits or commits whose only purpose is temporarily restoring a
green build. A separate Goal-document commit may use:

```text
docs(placement): add coordinator domain hard-switch goal
```

---

## 1. Required Design Sources

Read these documents completely before implementation:

- [architecture/00-overview.md](architecture/00-overview.md)
- [architecture/01-actor-runtime.md](architecture/01-actor-runtime.md)
- [architecture/02-rpc.md](architecture/02-rpc.md)
- [architecture/03-placement.md](architecture/03-placement.md)
- [architecture/06-appendix.md](architecture/06-appendix.md)
- [architecture/07-api-examples.md](architecture/07-api-examples.md)
- [architecture/08-distributed-testing.md](architecture/08-distributed-testing.md)
- [coordinator-correctness-implementation-plan.md](coordinator-correctness-implementation-plan.md)
- [cluster-discovery-lifecycle-plan.md](cluster-discovery-lifecycle-plan.md)
- [production-hardening-plan.md](production-hardening-plan.md)

When those sources refer to one Coordinator as both membership authority and placement authority,
this Goal replaces that topology while retaining their safety, remoting and boundedness invariants.

---

## 2. Required Outcome

Lattice must support multiple independent placement domains in one cluster:

```text
Cluster
├── Membership control plane
└── CoordinatorHost processes
    ├── PlacementDomain: player
    │   ├── EntityType: player
    │   └── EntityType: account-login
    ├── PlacementDomain: world
    │   └── EntityType: world
    └── PlacementDomain: battle
        ├── EntityType: battle
        └── SingletonKind: battle-scheduler
```

For every placement domain:

- exactly one lease-backed leader is active for a term;
- any number of bounded candidates may compete;
- leadership, term, state revision, membership, claims and plans are domain-scoped;
- entity and singleton routing depends only on that domain's state;
- one domain may fail or reconcile without changing unrelated domain state;
- one CoordinatorHost process may lead some domains and stand by for others;
- loss of one leader task does not terminate the host or another leader task.

The cluster membership plane owns exact node identity and global `Joining | Up | Leaving` state. It
does not allocate shards or singletons. Placement domains admit only globally Up nodes and maintain
their own domain participation state and capacity.

### 2.1 Non-goals

- Do not create one Coordinator per shard or per actor instance.
- Do not require one operating-system process per placement domain.
- Do not make application Actor or Protocol types depend on Coordinator implementation types.
- Do not replace etcd's consensus role with framework consensus or gossip.
- Do not add cross-cluster federation or cross-domain atomic business transactions.
- Do not make placement-domain failure replay tell/ask business messages.
- Do not weaken exact `ActorRef` identity or retarget stale concrete references.
- Do not combine Shard and Singleton public semantics even though they share authority machinery.
- Do not preserve generation-4 runtime compatibility.

---

## 3. Locked Architecture Decisions

### 3.1 Placement-domain identity

Add a bounded canonical `PlacementDomainId`. It rejects empty values, path separators, control
characters and values longer than the framework limit. It is serialized explicitly and is not
derived from a Rust type name.

Every `EntityConfig` and `SingletonConfig` requires a domain ID at construction. There is no hidden
`default` domain. A convenience constructor may create a domain ID from the explicit canonical
`EntityType`, but the resulting ID is still stored and fingerprinted.

The following identities include the domain:

```text
EntityRef       { cluster, domain, entity_type, entity_id, protocol, config_fingerprint }
SingletonRef    { cluster, domain, singleton_kind, protocol, config_fingerprint }
PlacementSlotKey::Shard     { domain, entity_type, shard_id }
PlacementSlotKey::Singleton { domain, singleton_kind }
ClaimGrant      { domain, slot, owner, term, generation, sequence, ttl }
PlacementVersion { domain, term, revision }
LeaderGuard     { scope: Placement(domain), exact leader record }
```

Domain identity, shard count, hash version, policy ID/version and hard constraints participate in
the configuration fingerprint. Old logical references fail decoding or fingerprint validation;
they are never silently assigned to a domain.

### 3.2 Membership and domain participation

Split the current overloaded `NodeHello` flow into two bounded advertisements:

```text
MemberHello
  exact NodeKey
  roles and failure-domain attributes
  protocol catalogue
  remoting capabilities

PlacementDomainHello
  domain ID and domain config fingerprint
  hosted entity/singleton configs
  proxied entity/singleton subscriptions
  positive domain capacity units
  domain-specific constraints
```

The membership leader persists global node lifecycle and emits a term-qualified member directory.
It performs no shard or singleton placement. Each placement-domain leader validates
`PlacementDomainHello`, persists `DomainMemberRecord { node, status, version, capacity }`, and
allocates only to nodes that are both globally Up and domain Up.

A mismatch in one domain rejects only that domain admission. It does not evict the process from the
cluster or invalidate other admitted domains.

Global member removal is an input to every domain in which the exact node incarnation participated.
Each affected domain independently fences claims and performs recovery.

### 3.3 CoordinatorHost and elections

Introduce a runtime boundary that hosts multiple independent candidate state machines:

```text
CoordinatorHost
├── MembershipLeaderCandidate
├── PlacementLeaderCandidate(domain-a)
├── PlacementLeaderCandidate(domain-b)
└── PlacementLeaderCandidate(domain-c)
```

Leader election keys, leases, terms, task supervision and observable state are scoped. A host losing
one lease stops only that scope, generates a new scope incarnation where required, and re-enters
that election. It must not terminate the remoting endpoint or unrelated candidate runtimes.

Candidate preference uses a deterministic domain/node ordering plus bounded jitter so starting one
host first does not permanently concentrate every domain leader on that host. Configuration may
assign explicit priority/weight, but correctness never depends on balanced leader placement.

The framework must support dedicated control-plane processes, co-located CoordinatorHost runtimes,
and multiple domain leaders in one process through the same API.

### 3.4 Scoped discovery and sessions

Replace single-leader discovery with a scoped directory:

```text
CoordinatorScope::Membership
CoordinatorScope::Placement(PlacementDomainId)

CoordinatorDirectorySnapshot {
  scope,
  generation,
  candidates,
}
```

Static, ConfigStore, DNS and Kubernetes providers expose the same scoped abstraction. Discovery
generation and leader selection are independent per scope. A malformed or unavailable domain entry
does not discard valid entries for another scope.

Every node establishes one membership session plus one placement session for each domain it hosts
or proxies. Placement snapshots and deltas contain only that domain's configuration, participants,
slots, claims and plans. Snapshot and reliable-control bounds apply per session and globally per
service.

### 3.5 Durable storage generation 5

Use explicit scoped storage keys:

```text
/lattice/<cluster>/schema_generation
/lattice/<cluster>/membership/leader
/lattice/<cluster>/membership/term
/lattice/<cluster>/membership/state_revision
/lattice/<cluster>/membership/members/<node_id>

/lattice/<cluster>/domains/<domain>/leader
/lattice/<cluster>/domains/<domain>/term
/lattice/<cluster>/domains/<domain>/state_revision
/lattice/<cluster>/domains/<domain>/members/<node_id>
/lattice/<cluster>/domains/<domain>/entity_types/<entity_type>
/lattice/<cluster>/domains/<domain>/shards/<entity_type>/<shard_id>
/lattice/<cluster>/domains/<domain>/shard_claims/<entity_type>/<shard_id>
/lattice/<cluster>/domains/<domain>/singletons/<singleton_kind>
/lattice/<cluster>/domains/<domain>/singleton_claims/<singleton_kind>
/lattice/<cluster>/domains/<domain>/rebalances/<plan_id>
/lattice/<cluster>/domains/<domain>/admin/<operation_id>
```

Split storage contracts into membership operations and domain-placement operations. Domain stores
remain named-transaction APIs; do not expose a generic public etcd transaction language.

Every authoritative domain transaction compares:

- the exact live domain leader record and term;
- the exact domain state revision;
- the exact globally live member record when assigning authority;
- the exact domain member and config fingerprint;
- all slot, claim and plan predicates required by generation-4 correctness.

Transactions cannot read, mutate or count records belonging to another domain. Limits exist for
maximum domains, records per domain and total cluster records.

Set storage generation and placement-control generation to 5 in the first macro batch. Generation 4
and 5 cannot communicate or share a live cluster.

### 3.6 Wire and routing hard switch

Bump the remoting minor/required feature for domain-scoped logical targets. Every logical data-plane
envelope carries domain ID and expected assignment generation. Owners validate domain, slot,
generation and claim before activating an entity or delivering a handler message.

Replace one switchable cluster router with a bounded router directory:

```text
DomainRouterDirectory
  PlacementDomainId -> SwitchableDomainRouter
```

Known Running routes remain usable during a domain Coordinator outage only under existing claim and
generation fencing rules. Owners stop serving when their local monotonic claim deadline expires.
Unknown homes, new allocations and expired authority fail with a domain-specific unavailable error.
No request falls back to another domain or the generation-4 router.

Coordinator traffic remains outside the business-message hot path after a route is resolved.

### 3.7 Failure isolation and lifecycle

Separate node lifecycle from domain lifecycle:

```text
NodeLifecycleState
  Booting | JoiningMembership | Ready | Draining | Terminated

PlacementDomainState
  Joining | Ready | Degraded | Draining | Terminated
```

Expose a bounded health snapshot and subscriptions for membership plus configured domains. The old
single `ServiceLifecycleState::Degraded` model is removed rather than wrapped.

Failure rules:

- membership loss blocks new cluster/domain admission but does not erase still-valid domain routes;
- domain A loss changes only domain A state and router;
- domain B tell/ask/watch and exact `ActorRef` traffic continue;
- a domain leader cannot mutate before installing a full snapshot for its new term;
- losing a domain leader task never stops another leader task or the CoordinatorHost endpoint;
- application code can gate an endpoint on the exact domains it requires.

### 3.8 Capacity, rebalance and drain

Remove the single shared `capacity_units` placement input. Nodes declare an explicit positive quota
per joined domain. Two domain leaders cannot each assume ownership of the same unspecified global
capacity.

Allocation and rebalance policies remain per EntityType but consume capacity from their containing
domain. Concurrency limits are defined per domain, with optional CoordinatorHost-wide safety caps.

Graceful node drain is an aggregate operation:

1. stop new domain admission;
2. begin drain in every hosted domain concurrently within configured bounds;
3. wait for every domain to move or safely fence owned slots;
4. stop local activations and complete each domain drain;
5. leave global membership only after all required domain completions;
6. force shutdown fences every unfinished domain independently at the deadline.

One domain drain failure is observable and cannot be reported as complete, but it does not corrupt
completed drains in other domains.

### 3.9 Administration and observability

Every placement admin command includes a domain ID. Inspection can aggregate domains but mutation
never accepts an omitted domain. Idempotency keys are scoped by domain and operation type.

Add bounded, low-cardinality telemetry for:

```text
domain leader/candidate state and term
domain session and snapshot state
domain route availability and unresolved requests
domain member count, capacity and load
domain slot/claim/plan counts
domain reconciliation backlog and oldest work
cross-domain node drain progress
leader concentration per CoordinatorHost
```

Logs include cluster ID, scope, domain ID, leader term and exact node incarnation where applicable.

---

## 4. Public API Replacement Map

The exact names may be refined for cohesion, but the old concepts must be deleted rather than
deprecated:

| Remove | Replace with |
|---|---|
| unscoped `EntityConfig::new(...)` | constructor requiring `PlacementDomainId` |
| unscoped `SingletonConfig` | domain-scoped singleton config |
| logical refs without domain identity | domain-scoped `EntityRef` / `SingletonRef` |
| `CoordinatorLeader` as cluster placement leader | membership leader plus `PlacementDomainLeader` |
| monolithic `CoordinatorStore` | `MembershipStore` and named `PlacementDomainStore` contracts |
| `cluster_coordinator_runtime(...)` | `coordinator_host(...)` with registered scopes/domains |
| `ClusterLogicalRouter` | `DomainRouterDirectory` and `DomainLogicalRouter` |
| `LogicCoordinatorSession` | membership session and `PlacementDomainSession` |
| one global placement snapshot | membership snapshot plus per-domain snapshots |
| global placement `StateVersion` | membership version and domain-qualified placement version |
| global placement capacity | explicit per-domain capacity quota |
| unscoped placement admin commands | domain-required commands |
| one scalar service degradation state | node lifecycle plus domain health directory |

Structure and compile-fail tests must prove these old APIs are absent.

---

## 5. Execution Batches

### Batch A — Scoped identity, configuration and durable schema

- [ ] Add and validate `PlacementDomainId` and Coordinator scope identity.
- [ ] Require domain identity in entity, singleton, slot, claim, plan and logical-reference types.
- [ ] Include domain identity in configuration fingerprints and golden vectors.
- [ ] Split membership and domain placement versions, leader guards and errors.
- [ ] Split memory and etcd store contracts and implement generation-5 scoped keys.
- [ ] Scope named transactions, limits, pagination and reconciliation cursors by domain.
- [ ] Bump storage/control/remoting generations and delete generation-4 decoders and constructors.
- [ ] Add unit, property and real-etcd tests proving cross-domain mutation is impossible.

Exit condition: durable and wire identity cannot represent an unscoped placement operation, and
memory/etcd backends pass the same scoped contract suite.

### Batch B — Membership plane and multi-domain CoordinatorHost

- [ ] Separate global member lifecycle from domain participation and placement decisions.
- [ ] Implement `MemberHello` and bounded `PlacementDomainHello` validation.
- [ ] Implement a supervised CoordinatorHost with independent scope elections and leader tasks.
- [ ] Allow one process to lead, stand by for and lose multiple domains independently.
- [ ] Implement scoped discovery publication, candidate selection and leader rollover.
- [ ] Make domain leaders consume authoritative global member events without becoming the global
      member writer.
- [ ] Persist and reconcile domain members, configuration and placement before accepting mutations.
- [ ] Add election tests for simultaneous different-domain leaders and one-leader-per-domain safety.

Exit condition: at least two domains can elect different leaders on different hosts, and killing one
leader leaves the other leader and membership plane operational.

### Batch C — Domain-scoped service routing and lifecycle

- [ ] Replace the single logical router with a bounded domain router directory.
- [ ] Build membership plus required-domain sessions automatically from registered configs.
- [ ] Make `register_entity`, `use_entity`, singleton registration and proxy use domain-scoped refs.
- [ ] Route local and remote tell/ask/watch with domain and assignment-generation validation.
- [ ] Preserve bounded buffering and deadlines independently per domain and globally.
- [ ] Implement node lifecycle, domain lifecycle and scoped readiness/health subscriptions.
- [ ] Retain safely fenced known routes during a domain outage; reject unknown or expired authority.
- [ ] Remove global router clearing and all cluster-wide placement fallback paths.

Exit condition: failing domain A makes only A references unavailable while domain B and exact
`ActorRef` traffic continue under executable tests.

### Batch D — Domain-scoped operations, capacity and drain

- [ ] Scope allocation strategies, load tables, claims, plans and handoff barriers by domain.
- [ ] Replace global placement capacity with explicit domain quotas and enforce them in validation.
- [ ] Enforce per-domain and host-wide movement/concurrency limits without cross-domain state writes.
- [ ] Scope singleton placement and reuse the same domain authority engine.
- [ ] Aggregate graceful drain across joined domains and global membership.
- [ ] Make crash recovery and global member removal fan out safely to affected domains.
- [ ] Require domain IDs in inspect, explain, pause, rebalance, relocate and operation history APIs.
- [ ] Add metrics, dashboards and runbook steps for partial domain degradation and drain.

Exit condition: allocation, rebalance, handoff, singleton movement, administration and drain work
independently across domains and preserve every generation-4 fencing invariant.

### Batch E — Migration, simulation, distributed acceptance and documentation

- [ ] Add an offline, resumable generation-4-to-5 migration command requiring an explicit mapping
      from every EntityType and SingletonKind to a PlacementDomain.
- [ ] Require a stopped cluster, absent live leader/member/claim leases and no active handoff before
      migration; never migrate automatically during startup.
- [ ] Preserve configuration and monotonic generations, but restart old ownership as safely fenced
      or unallocated authority that must be re-established by generation 5.
- [ ] Add dry-run, apply, interruption/resume, collision, unmapped-type and rollback-boundary tests.
- [ ] Extend production/simulation reducers, invariant checking, state exploration and trace replay
      with multiple domains and independent elections.
- [ ] Add real-etcd and Docker tests with multiple CoordinatorHosts, domains and logic nodes.
- [ ] Add chaos cases for one-domain leader loss, membership loss, host loss, partition, lease expiry,
      drain and simultaneous independent handoffs.
- [ ] Update all architecture, API example, operations, migration and deployment documents.
- [ ] Update `examples/minimal-world` to demonstrate explicit domains without application-specific
      shortcuts.
- [ ] Prove generation-4 APIs, keys, control messages and implicit default routing are absent.
- [ ] Run every final acceptance command and retain replayable evidence.

Exit condition: the new model is the only model in code, docs, examples, storage and wire protocols;
all repository and distributed acceptance gates pass.

---

## 6. Required Test Matrix

### Identity and configuration

- Domain ID canonicalization, bounds and serde round-trip.
- Same entity ID in different domains resolves to independent slots.
- Domain changes alter fingerprints and reject old logical references.
- Duplicate entity/singleton registration inside a domain is rejected.
- Explicit grouping of multiple entity types into one domain succeeds.
- Config mismatch rejects only the affected domain.

### Election and storage

- One live leader per domain and term.
- Different domains may have leaders concurrently on one or many hosts.
- Loss of one lease terminates only that domain leader runtime.
- A stale leader cannot mutate its domain and can never mutate another domain.
- Exact global member plus exact domain member are required for allocation.
- Slot/claim and slot/plan atomicity remain intact per domain.
- Domain pagination/cardinality limits cannot be bypassed with cross-domain keys.
- New leaders reconcile only their domain before making decisions.

### Routing and lifecycle

- Domain A and B tell, ask and watch work concurrently.
- Domain A Coordinator outage does not interrupt B or exact references.
- Known A routes serve only while owner claim/generation remains valid.
- Unknown A homes fail explicitly while A is unavailable.
- Domain recovery installs a new-term snapshot before reopening A.
- Wrong-domain envelopes never reach ActorLoader or handlers.
- Buffer, byte and deadline limits are enforced per domain and globally.
- Membership failover blocks admission without erasing valid domain authority.

### Placement operations

- Independent allocations do not consume another domain's quota.
- Grouped entity types share their declared domain capacity correctly.
- Concurrent handoffs in different domains do not share barriers or revisions.
- Source/target/domain generation validation rejects stale completion.
- Singleton and shard claims are fenced within the correct domain.
- Node drain waits for every joined domain before membership removal.
- One domain drain timeout forces only unfinished authority and remains observable.
- Admin idempotency and pause state are isolated by domain.

### Distributed and chaos

- Three CoordinatorHosts distribute at least three domain leaders.
- Killing one leader produces a new term only in that domain.
- Killing a host fails over every domain it led without coupling their terms.
- Partitioning one domain control association leaves unrelated data paths active.
- Reordering snapshots/deltas across domains never crosses state machines.
- Global member expiry triggers bounded recovery in exactly the joined domains.
- Repeated domain and membership failover produces no duplicate owner.
- Generation-4 peers, refs and storage fail explicitly against generation 5.

---

## 7. Safety Invariants

Completion requires executable evidence for all of the following:

```text
One placement domain has at most one live leader for one term.
Different placement domains may progress under different leaders and terms.
Every slot, claim, plan, version and control effect has exactly one domain identity.
No transaction reads or writes authoritative records outside its domain.
No slot becomes Allocating or Running without its exact domain claim.
No old domain term or assignment generation can serve after fencing.
No domain may allocate to a globally non-Up or domain-non-Up node.
A domain failure never clears or replaces another domain's router state.
Logical data-plane messages never require a Coordinator hop after resolution.
Concrete ActorRef identity remains exact and domain-independent.
Node drain does not remove global membership before all domain authority is resolved.
Every queue, session, router, domain, candidate, snapshot, retry and history is bounded.
```

---

## 8. Final Acceptance

Run and retain evidence for:

```text
scripts/check-structure.sh
cargo fmt --all -- --check
cargo check --workspace --all-targets --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features
focused memory/etcd membership and placement-domain contract suites
focused lattice-service multi-domain routing/lifecycle suites
simulation, bounded model exploration and trace replay
Docker quality, sim, model, e2e, e2e-ha-etcd, chaos and k8s profiles
generation-4-to-5 migration dry-run/apply/resume acceptance
git diff --check
```

Final structure scans must find none of:

```text
cluster_coordinator_runtime
ClusterLogicalRouter
LogicCoordinatorSession
implicit default placement domain
generation-4 placement control decoder
unscoped placement mutation or admin command
runtime storage dual read/write
```

The Goal is complete only when every tracker item is checked, all invariants have executable
coverage, the architecture documents describe only the domain model, and the Current Execution
Pointer records dated final evidence.

---

## 9. Rollout Boundary

This is a full-stop framework release:

1. Stop all generation-4 logic and Coordinator processes.
2. Revoke old credentials and wait for every old leader, member and claim lease to expire.
3. Run the offline migration with a complete explicit domain mapping.
4. Validate generation-5 schema, fingerprints and migration report.
5. Start generation-5 CoordinatorHosts and membership control plane.
6. Start logic nodes and wait for required domains to become Ready.
7. Reopen application admission.

Mixed-version rolling deployment is unsupported. No old process, reference, control frame or storage
record is accepted after the cutover.
