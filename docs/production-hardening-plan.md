# lattice Production Hardening Execution Plan

> Status: not started
> Purpose: turn the current functionally complete framework into a production-safe distributed actor runtime.
> Execution model: implement the earliest unfinished phase in small, tested, committed slices.
> Source: code review performed against the workspace after the original Phase 1-9 implementation plan was marked complete.

---

## 0. Goal Prompt

Use the following text when starting a Codex goal:

```text
Your goal is to fully execute docs/production-hardening-plan.md.

Read that document completely before changing code. Treat it as the primary execution plan and use docs/architecture/ as the architecture reference.

Start from the Current Progress Tracker and work on the earliest unchecked item in the earliest unfinished phase. Do not skip phases. Work in small end-to-end slices: implementation, tests, verification, tracker update, and an English conventional commit.

Do not trust checked items without auditing their code and executable coverage. If an item is checked but incomplete, change it back to [ ] or add a precise missing-work item. Do not mark a phase complete while required behavior exists only in documentation, API sketches, mocks, or fake transports.

The highest-priority invariant is that a stale or non-owner service instance must never load or execute a state-mutating actor request. Preserve backward compatibility only where it does not weaken ownership fencing, authentication, bounded resource usage, or shutdown correctness.

After every slice:
- update docs/production-hardening-plan.md;
- run the slice-specific tests and relevant workspace checks;
- create an English conventional commit;
- report completed items, remaining items, verification results, and the commit id/message.

Do not mark the goal complete until every phase, global acceptance item, and final verification command in the plan is complete.
```

---

## 1. Scope and Priorities

This plan addresses the following review findings, in priority order:

1. Enforce actor ownership and epoch fencing at every placement-backed RPC ingress.
2. Isolate Gateway connection failures and bound connection/resource usage.
3. Complete the authenticated RPC identity chain and remove placeholder authorization.
4. Make actor activation and actor task lifecycle cancellation- and panic-safe.
5. Supervise all service-owned background tasks and make shutdown deadline-bounded.
6. Make request identity, duplicate detection, trace, auth, and deadlines correct per call.
7. Make Gateway sessions, rate limiting, and admin inspection bounded and live.
8. Align documentation, examples, and production-readiness claims with implemented behavior.

### 1.1 Non-Goals

```text
Do not redesign the business Actor/Handler programming model.
Do not add exactly-once delivery claims.
Do not put business database transactions into framework crates.
Do not route normal business RPC through the Coordinator.
Do not read etcd on every data-plane RPC.
Do not replace typed RPC with EventBus commands.
Do not add unrelated features while hardening the current architecture.
```

### 1.2 Required Invariants

```text
A stale or non-owner instance never loads or invokes an actor for a placement-backed request.
A placement-backed request cannot bypass fencing by omitting route epoch metadata.
Lease loss fences local ownership before the service can continue serving actor mutations.
NOT_OWNER and FENCED are structured protocol outcomes, not message-string heuristics.
A malformed or disconnected client affects only its Gateway connection.
All untrusted-input collections, queues, connections, and caches have explicit bounds or eviction.
Authenticated peer identity comes from the transport, not caller-controlled metadata.
Actor activation cannot remain permanently stuck because its initiating future was cancelled or panicked.
Every runtime-owned task has an owner, failure policy, cancellation path, and shutdown deadline.
Request IDs remain unique across process restarts with the same configured instance ID.
Admin inspection reports live or explicitly stale/partial state.
```

---

## 2. Current Progress Tracker

### Phase 0: Baseline and Regression Guardrails

Status: `[ ]` not started.

- [ ] Record the current workspace verification baseline in this document.
- [ ] Map every hardening invariant to at least one planned executable test.
- [ ] Confirm no existing public API must remain insecure for compatibility.
- [ ] Add a production-hardening test module/layout without committing intentionally failing tests.
- [ ] Phase 0 verification and conventional commit complete.

### Phase 1: Ownership Gate and Structured Fencing

Status: `[ ]` not started.

- [ ] Add a reusable local ownership snapshot/gate for explicit actors, virtual shards, and singletons.
- [ ] Require generated placement-backed RPC services to validate expected service, actor kind, route, owner, state, lease validity, and epoch before actor lookup.
- [ ] Prevent `ActorRegistry::get_or_load` from being reached after a failed ownership decision.
- [ ] Require route epoch for fenced placement modes; retain an explicit unfenced mode only for deliberate static/local use.
- [ ] Replace status-message substring parsing with structured NOT_OWNER/FENCED error metadata/details.
- [ ] Make old owners fence locally when ownership changes or lease keepalive fails.
- [ ] Handle placement watch lag/closure through full resync, cache invalidation, or readiness failure; never silently continue with an indefinitely stale ownership view.
- [ ] Remove per-request PlacementStore access from singleton data-plane dispatch.
- [ ] Add stale-route, missing-epoch, migration, lease-loss, and watch-lag tests.
- [ ] Update ownership/fencing architecture documentation and examples.
- [ ] Phase 1 workspace verification and conventional commits complete.

### Phase 2: Gateway Failure Isolation and Admission Control

Status: `[ ]` not started.

- [ ] Separate normal connection termination/protocol errors from critical Gateway task failures.
- [ ] Remove string matching such as `message.contains("early eof")` from connection lifecycle decisions.
- [ ] Add configurable maximum active connections with admission permits acquired before spawning handlers.
- [ ] Add configurable handshake/first-frame, read, write, idle, and handler deadlines.
- [ ] Validate minimum and maximum frame sizes before allocation and prevent integer truncation during encoding.
- [ ] Add bounded outbound queues so server push and request replies cannot create unbounded writer pressure.
- [ ] Add graceful Gateway drain with a deadline and explicit forced-abort accounting.
- [ ] Add malformed-frame, slowloris, connection-reset, overload, and shutdown tests.
- [ ] Expose connection rejection, protocol error, timeout, and forced-abort metrics without high-cardinality labels.
- [ ] Update Gateway architecture documentation and runnable example configuration.
- [ ] Phase 2 workspace verification and conventional commits complete.

### Phase 3: Transport Identity and Authorization

Status: `[ ]` not started.

- [ ] Extract authenticated peer identity from tonic TLS connection information/certificate SAN rather than caller metadata.
- [ ] Install verified `PeerIdentity` into request extensions before generated service authorization executes.
- [ ] Treat source service/instance metadata only as claims to compare with authenticated transport identity.
- [ ] Remove the fixed `Bearer lattice-internal` authorization mechanism.
- [ ] Add a pluggable internal authorizer/token verifier where application authorization is required.
- [ ] Define explicit development and production security profiles; production must fail closed.
- [ ] Reject insecure external admin/direct-link/RPC binds unless explicitly opted into with a documented development policy.
- [ ] Ensure secrets and raw credentials never appear in Debug output, traces, or errors.
- [ ] Add real TLS/mTLS integration tests for accepted identity, wrong trust domain, wrong service, missing certificate, and spoofed metadata.
- [ ] Update security configuration docs and examples to match the real certificate-to-identity path.
- [ ] Phase 3 workspace verification and conventional commits complete.

### Phase 4: Cancellation-Safe Actor Lifecycle

Status: `[ ]` not started.

- [ ] Make registry activation cleanup cancellation-safe with an activation guard or equivalent state owner.
- [ ] Ensure loader panic/cancellation publishes a terminal activation result and removes only the matching activation incarnation.
- [ ] Prevent activation completion races from overwriting a newer registry entry.
- [ ] Supervise actor task completion and convert panic/cancellation into an explicit terminal lifecycle state.
- [ ] Remove the matching running registry entry after actor termination without removing a newer incarnation.
- [ ] Define and test the policy for actor hook/handler panic: stop, restart, or escalate.
- [ ] Validate mailbox capacities and actor runtime configuration without public API panics.
- [ ] Make stop/drain robust when the system mailbox is full and report actors that fail to stop.
- [ ] Add cancelled-loader, panicking-loader, panicking-handler, concurrent-activation, stale-incarnation, and full-system-mailbox tests.
- [ ] Update actor lifecycle and supervision documentation.
- [ ] Phase 4 workspace verification and conventional commits complete.

### Phase 5: Structured Service Supervision and Shutdown

Status: `[ ]` not started.

- [ ] Introduce one service-owned supervisor for gRPC, placement watches, lease keepalive, admin HTTP, Direct Link, scheduler, and other background tasks.
- [ ] Classify tasks as critical, restartable, or connection/request scoped with explicit failure policies.
- [ ] Ensure critical task failure changes readiness and initiates one consistent shutdown path.
- [ ] Ensure placement watch/admin/direct-link failure cannot remain silent while the instance stays Ready.
- [ ] Fence local ownership immediately before returning from an unrecoverable keepalive failure.
- [ ] Use one cancellation mechanism across service components and retain all task handles until joined or aborted.
- [ ] Add configurable deadlines for readiness, drain, actor stop, placement migration, server grace, and final task join.
- [ ] Preserve and report the primary failure while also reporting cleanup failures.
- [ ] Add critical-task-failure, keepalive-loss, hung-drain, repeated-shutdown, and partial-component-start tests.
- [ ] Update service lifecycle and operational documentation.
- [ ] Phase 5 workspace verification and conventional commits complete.

### Phase 6: Per-Call RPC Context and Duplicate Safety

Status: `[ ]` not started.

- [ ] Add a boot-unique component or UUIDv7/ULID generation so RequestId cannot repeat after an instance restart.
- [ ] Scope duplicate keys by authenticated source, method, and target route/actor identity as appropriate.
- [ ] Preserve one RequestId across transparent route-correction retry.
- [ ] Define duplicate state explicitly, including in-flight and completed/unknown-result behavior.
- [ ] Ensure duplicate protection executes before business side effects and cannot be bypassed by alternate generated adapters.
- [ ] Replace client-core-static trace/auth state with a per-call RPC context API.
- [ ] Propagate the active trace context, authenticated principal/session context, and deadline on every generated call.
- [ ] Validate inbound deadline and cancellation behavior without claiming that cancellation rolls back actor side effects.
- [ ] Add restart-collision, concurrent-duplicate, cross-source, route-retry, per-call-auth, trace-chain, and deadline tests.
- [ ] Update RPC semantics and generated client examples.
- [ ] Phase 6 workspace verification and conventional commits complete.

### Phase 7: Bounded Sessions, Rate Limits, and Live Operations

Status: `[ ]` not started.

- [ ] Extend GatewaySessionRef with gateway identity and a boot-unique incarnation in addition to connection epoch.
- [ ] Add compare-and-remove disconnect semantics so an old connection cannot unregister a replacement connection.
- [ ] Store a bounded sender for each live session and reject stale pushes before enqueue.
- [ ] Replace or extend the keyed limiter with bounded cardinality, TTL eviction, and validated nonzero configuration.
- [ ] Define whether load-shed rejections consume rate quota and test the chosen policy.
- [ ] Replace startup-only AdminSnapshot data with live inspector queries or a watched cache.
- [ ] Mark partial/stale admin responses explicitly and apply pagination before large cross-node aggregation where possible.
- [ ] Require authentication and audit records for mutating admin endpoints.
- [ ] Either implement documented retry-stop/force-stop/migrate endpoints or remove/mark them unsupported in architecture docs.
- [ ] Add session reconnect/disconnect race, stale push, limiter eviction, admin freshness, partial cluster, and admin authorization tests.
- [ ] Add bounded-cardinality metrics for ownership, supervision, sessions, limiter eviction, and admin partial results.
- [ ] Phase 7 workspace verification and conventional commits complete.

### Phase 8: End-to-End Acceptance and Release Readiness

Status: `[ ]` not started.

- [ ] Add an end-to-end scenario covering Gateway -> routed RPC -> ownership gate -> Actor -> reply/push.
- [ ] Add a migration scenario proving stale owners never run the handler and clients repair their route.
- [ ] Add a lease-loss scenario proving readiness drops and local mutations are fenced before shutdown completes.
- [ ] Add a real mTLS multi-service scenario with authenticated identity and authorization.
- [ ] Add a Gateway abuse scenario proving malformed/slow clients do not terminate or exhaust the process.
- [ ] Run and document targeted performance comparisons for the ownership gate, request context, and bounded Gateway pipeline.
- [ ] Confirm hot-path ownership checks do not access etcd and do not introduce a Coordinator hop.
- [ ] Update all architecture/API/example documents and remove stale production-readiness claims.
- [ ] Run every final verification command.
- [ ] Audit every checked tracker item against implementation and executable coverage.
- [ ] Phase 8 verification and final conventional commit complete.

---

## 3. Detailed Phase Acceptance

### 3.1 Phase 0 Acceptance

Deliverables:

- A test matrix mapping every invariant in section 1.2 to a unit, integration, chaos, or end-to-end test.
- A recorded baseline for formatting, Clippy, workspace tests, and relevant benchmark targets.
- A list of compatibility breaks that are permitted because retaining compatibility would preserve unsafe behavior.

Acceptance:

```text
No intentionally failing test is committed.
Every P0 behavior has a named future test location.
The tracker reflects the actual repository state rather than review assumptions.
```

### 3.2 Phase 1 Acceptance

The ownership gate must run before registry lookup and business decode/dispatch where practical. For every placement-backed request it must be possible to answer:

```text
What ownership key is being addressed?
Which local ownership snapshot authorizes this instance?
What epoch did the caller resolve?
Is the local lease still valid?
Is the placement state allowed to serve mutations?
Why was the request accepted, rejected as NOT_OWNER, or rejected as FENCED?
```

Required tests:

- A route cached for owner A is used after ownership moves to B; A returns NOT_OWNER/FENCED and its loader/handler counters remain zero.
- Missing route epoch is rejected for explicit, virtual-shard, and singleton placement.
- A valid current epoch reaches exactly one handler.
- Lease loss fences requests before process shutdown or etcd reconciliation completes.
- Watch lag cannot leave an old owner authorized indefinitely.
- Real generated tonic transport decodes structured error details without parsing human-readable messages.

### 3.3 Phase 2 Acceptance

Required tests:

- Empty, too-short, oversized, truncated, and invalid frames close one connection only.
- A reset during read/write does not terminate the listener.
- Connection admission never exceeds the configured maximum.
- Slow clients time out without retaining permits or buffers.
- Shutdown stops admission, drains allowed connections, then aborts remaining work after the deadline.

### 3.4 Phase 3 Acceptance

Required tests must use real network TLS, not manually inserted request extensions. A caller-controlled metadata identity must never authenticate a request whose transport identity is missing or different.

Production configuration must reject at startup when required identity material is absent or internally inconsistent.

### 3.5 Phase 4 Acceptance

Required tests:

- Abort the first activation future while the loader is pending, then successfully activate the same ActorId.
- Panic in a loader and handler without leaving a Running/Activating zombie entry.
- Race termination cleanup with a new incarnation and prove the new actor remains registered.
- Fill the system mailbox and prove drain either stops the actor or returns a visible failure report by deadline.

### 3.6 Phase 5 Acceptance

Every spawned service task must appear in a supervisor inventory with:

```text
task name
task class
owner
failure action
cancellation signal
join/abort policy
readiness impact
```

No production service component may rely on dropping a `JoinHandle`, because dropping detaches the task.

### 3.7 Phase 6 Acceptance

Required tests:

- Two boots with the same service and instance IDs generate non-overlapping RequestIds.
- Two authenticated sources may use the same caller-local sequence without false duplicate rejection.
- A route correction retry preserves RequestId and invokes business logic at most once on accepted owners.
- Trace/auth/deadline values differ correctly between two concurrent calls through the same generated client.

### 3.8 Phase 7 Acceptance

Required tests:

- Disconnect from epoch N cannot remove epoch N+1.
- A push using a previous gateway boot/incarnation is rejected.
- Expired limiter keys are evicted and total key cardinality remains bounded under churn.
- Admin reads observe a placement update without restarting the HTTP server.
- An unreachable node produces an explicit partial result rather than a false complete snapshot.

### 3.9 Phase 8 Acceptance

The framework is release-ready only if all P0/P1 findings are covered by real implementation and executable tests. Passing unit tests against fake transports alone is insufficient for ownership and identity claims.

---

## 4. Execution Protocol

### 4.1 Slice Loop

```text
1. Read this plan completely on the first goal turn.
2. Audit the earliest unfinished phase and its dependency phases.
3. Select one checklist item or a few inseparable items.
4. Inspect implementation, tests, examples, and architecture docs for that slice.
5. Write or update a regression test together with the implementation; do not commit a red workspace.
6. Keep correctness checks on the data-plane hot path local and bounded.
7. Run targeted tests first, then the required phase verification.
8. Update this tracker only after executable acceptance passes.
9. Update architecture/API docs when behavior or public APIs change.
10. Commit with an English conventional commit message.
11. Report the slice, verification, commit, and next unchecked item.
12. Continue with the next earliest item; do not jump to cosmetic later phases.
```

### 4.2 Required Engineering Constraints

```text
Preserve the existing workspace crate boundaries.
Put ownership primitives in lattice-placement/lattice-service and generic RPC protocol details in lattice-rpc.
Keep generated source thin; reusable behavior belongs in normal framework modules.
Do not solve ownership by performing PlacementStore/etcd reads for every request.
Do not trust route, peer identity, service kind, or epoch solely because metadata contains it.
Do not catch and ignore task failures without metrics/readiness impact.
Do not create unbounded maps, queues, task sets, or connection pools for untrusted cardinality.
Use typed error kinds/details across RPC; human-readable text is diagnostic only.
Keep actor state single-threaded through the mailbox.
Avoid unrelated refactors and new framework features.
Do not modify or delete user changes unrelated to the current slice.
```

### 4.3 Commit Guidance

Examples:

```text
fix(rpc): fence stale actor owners before activation
feat(placement): maintain local ownership snapshots
fix(gateway): isolate malformed client connections
feat(gateway): bound connection admission and frame deadlines
fix(actor): clean up cancelled activations
feat(service): supervise critical runtime tasks
fix(security): authenticate rpc peers from mTLS identity
fix(rpc): make request ids unique across restarts
feat(ops): serve live partial-aware admin inspection
test(e2e): cover fenced migration through gateway
```

---

## 5. Verification Commands

Run targeted crate tests during each slice. At every phase exit run, at minimum:

```text
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

Phase-specific commands should be added here as tests are introduced, for example:

```text
cargo test -p lattice-rpc
cargo test -p lattice-placement --test fenced_retry
cargo test -p lattice-placement --test chaos
cargo test -p lattice-gateway
cargo test -p lattice-actor
cargo test -p lattice-service
cargo test -p distributed-login --test distributed_flow
```

Performance validation must compare before/after results using the existing benchmark harness. Do not turn unstable absolute throughput numbers into correctness gates; gate on bounded regression criteria documented with the benchmark environment.

### 5.1 Baseline Record

Fill this during Phase 0:

```text
Date:
Commit:
Rust toolchain:
cargo fmt --all -- --check:
cargo clippy --workspace --all-targets -- -D warnings:
cargo test --workspace --all-targets:
Relevant benchmark results:
Known environment limitations:
```

---

## 6. Global Completion Criteria

The goal may be marked complete only when every item below is checked:

- [ ] All phase statuses are `[x]` complete.
- [ ] Every invariant in section 1.2 has executable coverage.
- [ ] Generated placement-backed RPC adapters reject stale, missing-epoch, and non-owner requests before registry lookup.
- [ ] Lease loss and ownership watch failure cannot leave an instance serving mutations as Ready.
- [ ] Real tonic transport carries structured NOT_OWNER/FENCED outcomes.
- [ ] Gateway malformed/slow clients cannot terminate or unboundedly grow the service.
- [ ] Real mTLS tests prove transport-derived peer identity and spoof resistance.
- [ ] Actor activation and task panic/cancellation cannot leave zombie registry entries.
- [ ] All production background tasks are supervised and deadline-bounded during shutdown.
- [ ] Request IDs are boot-unique and per-call trace/auth/deadline propagation works concurrently.
- [ ] Session and rate-limit state is bounded and stale-safe.
- [ ] Admin inspection is live or explicitly partial/stale, and mutations are authenticated/audited.
- [ ] No normal data-plane RPC performs an etcd read or Coordinator hop.
- [ ] Architecture documentation and runnable examples match the hardened implementation.
- [ ] `cargo fmt --all -- --check` passes.
- [ ] `cargo clippy --workspace --all-targets -- -D warnings` passes.
- [ ] `cargo test --workspace --all-targets` passes.
- [ ] Every completed slice has an English conventional commit.
- [ ] Checked tracker items have been audited against concrete code and executable tests.

---

## 7. Initial Review Evidence

The plan was created from the following confirmed implementation observations:

- Generated `RegistryService` reaches `ActorRegistry::get_or_load` without an ownership or epoch decision.
- Route resolution intentionally serves soft-TTL stale entries, requiring the server to reject stale owners.
- Singleton dispatch reads PlacementStore on every RPC and treats route epoch as optional.
- Gateway connection errors are currently escalated as service-level background task failures.
- Gateway connection count and limiter key cardinality are not bounded/evicted.
- RPC clients emit peer identity claims in metadata while server policy expects a verified request extension; no real certificate-to-extension bridge exists.
- Internal authorization uses a fixed framework-known bearer value.
- Registry activation can retain `Activating` if the initiating future is cancelled or panics.
- Actor task JoinHandles are not retained or supervised.
- Service supervision does not monitor every placement/admin/direct-link task uniformly.
- RequestId uses a process-local sequence that restarts for the same configured instance ID.
- Admin HTTP state is built from a startup snapshot rather than live inspection.

These observations establish work to audit and fix; they do not substitute for the regression tests required by the tracker.
