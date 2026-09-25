# lattice Code Review: Architecture, Maintainability, and Cluster Lifecycle

Review date: 2026-09-08. Code baseline: `23bd4e83126b6aeb5f273e94ca35d4f3de69cfd7` (`refactor(actor): split local and distributed APIs`). The working tree was clean when the review began.

**Overall assessment: the architecture is sound, but lifecycle handling does not yet fully cover asynchronous cancellation, authority loss during loading, and connection retirement. These boundaries should be fixed before adding capabilities or pursuing further performance optimization.** The strongest opportunity for simplification is the resource management and state transition protocols maintained manually across modules.

**Remediation completed on 2026-09-08:** R1–R10, observer isolation, capacity/duplicate registration, and the prioritized API/documentation cleanup are implemented and validated. The [remediation record](review-remediation-plan.md) contains the changes, test evidence, and limitations. Control generation changes from 9 to 10 and requires a [full-stop upgrade](operations/code-only-rolling-upgrade.md#full-stop-boundary).

The original review changed only this document and its index link. Findings below describe the reviewed baseline and are preserved as the rationale for the repairs. The historical [architecture-review.md](architecture-review.md) covers a different code baseline. Findings already marked as fixed there have not been carried forward as defects in this review.

## 1. Scope and Evidence

The review uses source code as its primary evidence, cross-checked against the dependency graph, existing tests, minimal reproductions, and architecture documents. It examines `lattice-actor`, `lattice-actor-distributed`, `lattice-remoting`, `lattice-coordination`, and `lattice-service` in depth; checks relevant boundaries in discovery, the Kubernetes adapter, EventBus, Gateway, and Ops; and samples dependencies and interfaces in MongoDB, configuration, ID generation, telemetry, and examples. **This is not a line-by-line audit of all source code and does not include a complete correctness assessment of MongoDB persistence.**

The workspace contains 21 library/tool crates and 3 example packages. Rust files under `crates/` total approximately 129,000 lines, including tests, comments, and blank lines; placement, service, and remoting account for approximately 65,000 lines. This scale calls for explicit maintenance boundaries, but line count alone does not establish overengineering.

Evidence labels:

- **Reproduced**: a minimal program was run against the current source and exhibited the reported behavior.
- **Source-confirmed**: the entry point, state changes, and subsequent handling were checked, but the complete fault scenario was not executed.
- **Design recommendation**: an opportunity to reduce maintenance cost, not a claim that current functionality is incorrect.

Priorities: P1 covers cluster safety or sustained unavailability issues that should be fixed first; P2 covers correctness, availability, or resource boundary failures under specific conditions; P3 covers discrepancies in configuration behavior and recovery contracts. Cluster-wide concurrent ownership or data corruption that was not reproduced is not presented as an observed outcome.

## 2. Findings Overview

| ID | Priority | Finding | Evidence |
|---|---|---|---|
| R1 | P1 | Drain/fence omits loading Actors, which may start and execute messages after authority is revoked | Reproduced at the Registry layer; cluster call chain source-confirmed |
| R2 | P2 | Cancelling the first activation request leaves the Actor ID permanently Loading | Reproduced |
| R3 | P1 | Closing an association does not fully return the node byte budget reserved for unsent messages | Reproduced |
| R4 | P1 | Retiring a previously active association during reconnect leaves reconnect tasks holding connection permits | Reproduced over TCP |
| R5 | P1 | The final membership leave acknowledgement is not bounded by `leave(deadline)` | Source-confirmed |
| R6 | P2 | A transport ACK is treated as drain commit confirmation, and local removal bypasses the wait for authoritative removal | Source-confirmed |
| R7 | P2 | Inbound handshakes have no timeout, allowing silent connections to exhaust connection capacity | Reproduced over TCP |
| R8 | P2 | CoordinatorHost ignores task JoinError, potentially leaving a failed domain marked Active | Source-confirmed |
| R9 | P3 | Membership snapshot staging uses a constant timestamp, disabling the staging timeout | Source-confirmed |
| R10 | P2 | LocalEventBus handlers can still execute side effects after shutdown reports success | Reproduced |

### R1: Loading Actors Are Missing from the Ownership Set Used by Drain/Fence

Locations: [entity.rs:288](../crates/lattice-service/src/cluster/entity.rs#L288), [registry.rs:369](../crates/lattice-actor-distributed/src/registry.rs#L369), [registry.rs:660](../crates/lattice-actor-distributed/src/registry.rs#L660), [entity.rs:574](../crates/lattice-service/src/cluster/entity.rs#L574).

`EntityRouteHost::activate` checks the placement generation and local authority before starting the loader. After the loader returns, the Registry calls `spawn_actor` without rechecking authority when publishing the running instance. Meanwhile, `active_actor_ids()` excludes `RegistryEntry::Activating`, and shard drain, wait_drained, and fence all use this set. Once activation completes, [receive_tell](../crates/lattice-service/src/cluster/entity.rs#L407) directly dispatches the original message.

A reachable sequence is:

```mermaid
sequenceDiagram
    participant O as Old owner
    participant L as Asynchronous loader
    participant C as Placement Coordinator
    participant N as New owner
    O->>L: Validate generation and start loading
    C->>O: Drain / revoke previous authority
    O-->>C: Report drained; Activating is excluded from the stop set
    C->>N: Subsequent handoff allows the new owner to take over
    L-->>O: Loading completes
    O->>O: Publish Running and dispatch the old request
```

The reproduction suspends the loader at a barrier. While it is suspended, `registry.drain()` returns `completed=true`; releasing the barrier then produces `late_activation=Running`. This directly demonstrates that the Registry's drain result omits in-flight loading. The cluster-level sequence is supported by the production call chain above; a two-node handoff reproduction was not run in this review.

Impact: the old owner may execute `started` or business messages after losing authority, violating the single-executor invariant. A fencing token passed to the loader is not a per-message execution barrier for a general Actor. Ordinary Actor side effects, such as memory updates or HTTP calls, cannot be assumed to be rejected automatically by the storage layer.

Recommendation: treat each loading placeholder as a managed activation tied to its slot generation and cancellation state. Drain/fence must wait for it or invalidate it. After loading, a synchronized publication step must verify that the placeholder still holds authority for the same generation. Adding an unsynchronized `if` check alone still leaves a race between checking and publishing.

Regression criteria: inject handoff/claim expiry before and after loading, and before and after publishing Running. After drain succeeds, no activation from the old generation may appear, and the old request must not be dispatched.

### R2: Cancelling an Activation Request Does Not Clean Up Its Placeholder

Locations: [registry.rs:645](../crates/lattice-actor-distributed/src/registry.rs#L645), [registry.rs:660](../crates/lattice-actor-distributed/src/registry.rs#L660), [registry.rs:838](../crates/lattice-actor-distributed/src/registry.rs#L838).

The first caller inserts `Activating`, then waits on `activate().await` directly inside the caller's future. Cleanup and result publication occur only on paths where that call returns normally. An outer timeout, task abort, dropped future, or loader panic skips those operations.

Observed result: the first call is cancelled after 10ms. The second call never invokes its loader and returns `WaiterTimeout`. The Registry remains `Loading`, yet drain reports success. Subsequent requests cannot retry loading on their own, and ordinary remove/drain operations cannot clear a placeholder that has no handle.

Recommendation: define the lifecycle relationship between the first request and the activation task. The Registry could own the activation task, or a cancellation cleanup guard could handle cancellation. Either approach must clean up by placeholder instance identity and notify waiters, so that an old task cannot remove a newer placeholder. Fix this together with R1: moving activation into the background must not detach it from drain management.

Regression criteria: after the first request is cancelled, later requests receive an explicit cancellation result or can load again. Waiters must not remain attached indefinitely to a placeholder with no producer.

### R3: Association Destruction Does Not Fully Release the Shared Node Byte Budget

Locations: [budget.rs:122](../crates/lattice-remoting/src/association/budget.rs#L122), [association.rs:318](../crates/lattice-remoting/src/association.rs#L318), [lane.rs:494](../crates/lattice-remoting/src/lane.rs#L494), [lane.rs:538](../crates/lattice-remoting/src/lane.rs#L538).

Admission reserves both association and node byte budgets, and the normal send path manually calls `release_queued_bytes`. `begin_close`, `finish_close`, manager removal, and destruction of queued Frames do not account for the outstanding reservation. Cancelling a lane future during a write through the outer disconnect branch can also bypass the normal release path.

The minimal reproduction sets the node budget to 8 bytes, queues 8 bytes on the old association, then closes, removes, and drops it. After confirming that the manager is empty, it creates a new association; admission still returns `NodeByteBudgetExceeded`. This demonstrates a leak in the shared admission budget, not merely a stale diagnostic counter on the old association.

Impact: accumulated disconnects, forced removals, and rolling replacements can prevent healthy new associations from sending. Recovery may require rebuilding the entire manager.

Recommendation: move a private RAII budget token with each queued message and dequeued batch, covering successful writes, errors, cancellation, and destruction with exactly one release. Simply subtracting the balance in `remove()` risks a double release when a send task is still running.

Regression criteria: closing with queued messages, cancelling during a write, and replacing an association must all restore the shared budget. A new association must be able to use the full budget again.

### R4: Retirement Cannot Stop a Lane That Has Entered Its Reconnect Loop

Locations: [endpoint.rs:258](../crates/lattice-remoting/src/endpoint.rs#L258), [endpoint.rs:303](../crates/lattice-remoting/src/endpoint.rs#L303), [peers.rs:80](../crates/lattice-service/src/cluster/peers.rs#L80).

The association state is checked after a running lane exits, but the inner reconnect loop only observes shutdown of the entire endpoint. `establishing_timeout` applies only to associations that have never become active. Member removal closes the association, broadcasts disconnect, and removes it from the manager. A lane already inside its reconnect loop misses that exit path.

TCP reproduction: establish an association, shut down the peer endpoint, wait for reconnect to begin, then close/disconnect/remove the old local association. Although the manager is empty, **3 connection permits** remain held after several backoff rounds. They are released only when the local endpoint shuts down.

Impact: retry tasks for the old incarnation cannot be reclaimed, and repeated node replacement can exhaust connection capacity. Even if the old address becomes reachable again, attempts to attach to the Closed association fail and the loop keeps retrying.

Recommendation: use an association-level cancellation signal that retains terminal state, with a shared exit condition across backoff, dialing, idle, and running lanes. Both state checks and asynchronous waits must observe it; a one-time broadcast cannot replace persistent terminal state.

Regression criteria: removing a previously Active association during backoff, connect, or lane idle must release its tasks and permits within a bounded time without affecting other associations.

### R5: The Final Membership Leave Acknowledgement Can Wait Forever

Locations: [cluster_session.rs:75](../crates/lattice-coordination/src/cluster_session.rs#L75), [service.rs:622](../crates/lattice-service/src/builder/service.rs#L622).

After every domain reports drain completion, `leave(deadline)` directly awaits `ClusterSessionHandle::complete_drain()`. That method checks `control_command_pending` every 10ms, without a deadline or detection of session replacement. If the Membership Coordinator becomes unreachable after admission and the old association receives no ACK, the outer deadline cannot terminate this await.

Impact: `shutdown()` no longer honors `leave_timeout`, and a rolling departure can hang indefinitely. The [corresponding placement implementation](../crates/lattice-coordination/src/session/handle.rs#L131) already has an ACK timeout, showing behavioral drift between the two flows.

Recommendation: propagate one absolute deadline through the entire leave operation and record the current phase. When leadership/session changes, retry the same operation_id within the remaining time. Do not restart a full timeout on each reconnect.

Regression criteria: disconnect control traffic after the final membership command is admitted. Leave must return a diagnosable error by the deadline and must not incorrectly publish Terminated.

### R6: A Transport ACK Does Not Prove an Authoritative Drain Commit

Locations: [lane/control.rs:42](../crates/lattice-remoting/src/lane/control.rs#L42), [host/routing.rs:41](../crates/lattice-coordination/src/runtime/host/routing.rs#L41), [service.rs:628](../crates/lattice-service/src/builder/service.rs#L628), [members.rs:149](../crates/lattice-service/src/cluster/members.rs#L149).

The reliable control layer advances its sequence and sends an ACK even for `InvalidCommand`, which prevents invalid commands from blocking subsequent control traffic. However, the drain handle returns success as soon as the command is no longer pending. A stale-term `MembershipDrainComplete` rejected after leadership changes receives the same transport ACK.

The service then immediately calls `fence_incarnation`, removing itself from the local directory before checking whether that directory still contains it. The local mutation has already eliminated the wait condition, so it cannot prove that ClusterCoordinator committed removal. The comment on `fence_incarnation` assumes that authoritative removal has committed, but the caller has not established that precondition.

Impact: leave may return success while other nodes still see the old membership. A new incarnation using the same node_id may have to wait for the old lease to expire before joining. Placement's complete_member_drain also relies solely on ACK state; its `drain_ready` and resend behavior across leadership changes should be checked together. This finding does not establish a bypass of claim fencing.

Recommendation: add application-level commit confirmation bound to operation_id, incarnation, and scope/term, or wait for removal in the authoritative versioned stream before applying the local fence against stale snapshot reintroduction. Preserve the transport semantics that allow invalid commands to be acknowledged, and correct the application's interpretation of that ACK.

Regression criteria: inject rejection of an old term. Even if the transport ACK arrives, leave must not report success; only the corresponding application-level commit may advance the leave phase.

### R7: Inbound Handshakes Have No Common Establishment Deadline

Locations: [endpoint.rs:491](../crates/lattice-remoting/src/endpoint.rs#L491), [endpoint.rs:525](../crates/lattice-remoting/src/endpoint.rs#L525), [endpoint.rs:552](../crates/lattice-remoting/src/endpoint.rs#L552).

A node connection permit is acquired at TCP accept. The subsequent TLS handshake, first-frame read, and protocol negotiation observe shutdown but have no establishment timeout. The outbound connect timeout and reconnect establishing timeout do not cover this inbound path.

TCP reproduction: set `max_associations=1` and both timeouts to 50ms, then keep 3 sockets connected without sending data. After 200ms, `open=3, capacity=3`; the fourth connection is rejected, with `shed=1`. This reproduces the plaintext first-frame path. The same missing deadline in the TLS branch was confirmed by source inspection.

Impact: silent connections that can reach the listening port may hold establishment resources indefinitely, causing normal bootstrap/rejoin attempts to be rejected. Enabling TLS does not make post-authentication limits reclaim resources held before the handshake completes.

Recommendation: establish one deadline starting at accept for the entire inbound setup, including TLS, the first frame, and catalogue/bootstrap negotiation. On timeout, resource ownership should release the socket and permit automatically.

### R8: CoordinatorHost Does Not Handle JoinError from a Panicking Domain Task

Locations: [host.rs:374](../crates/lattice-coordination/src/runtime/host.rs#L374), [host.rs:391](../crates/lattice-coordination/src/runtime/host.rs#L391).

`tasks.join_next()` handles only `Ok((domain, result))`. Clearing the sender, marking the domain Failed, publishing the directory, and campaigning again all occur inside that branch. A domain runtime panic yields `Err(JoinError)`, bypassing every one of those actions. The campaign scan selects only domains whose `sender.is_none()`.

A panic in a pluggable allocation strategy is one reachable trigger. The host retains a failed sender and Active state, continues exposing stale leadership information, and does not campaign again locally. Other candidates may restore domain service, but that does not repair this host's incorrect state. Similar elections/background_tasks branches should also be checked for cleanup of task ownership sets.

Recommendation: retain a mapping from task ID to scope, and perform the same advertisement withdrawal, health degradation, and cleanup for ordinary task errors and JoinError. Logs should identify the domain and task phase. This does not require another general-purpose supervision framework.

Regression criteria: use a strategy that panics once. Verify that the domain leaves Active, health signals update, and the host either campaigns again or follows an explicit failure policy.

### R9: Membership Snapshot Timeout Checks Use a Constant Timestamp

Locations: [cluster_session.rs:254](../crates/lattice-coordination/src/cluster_session.rs#L254), [coordinator.rs:704](../crates/lattice-coordination/src/coordinator.rs#L704), [coordinator.rs:736](../crates/lattice-coordination/src/coordinator.rs#L736).

SnapshotStager's begin, push, and finish all receive `MonotonicTime::from_millis(0)`, while expiry checks depend on the supplied now value. If sequence and other conditions are satisfied, chunks or the end marker arriving after the staging timeout are still accepted by the time checks.

The impact is primarily on recovery time bounds and configuration reliability. Size, digest, and version checks remain in place; this should not be described as demonstrated data corruption.

Recommendation: use a monotonic clock adapter as the placement session does. Add a test that advances time before delivering the next chunk or end marker.

### R10: LocalEventBus Shutdown Does Not Wait for In-Flight Handlers

Locations: [local.rs:50](../crates/lattice-eventbus/src/local.rs#L50), [local.rs:225](../crates/lattice-eventbus/src/local.rs#L225), [local.rs:308](../crates/lattice-eventbus/src/local.rs#L308).

The EventBus trait promises to wait for in-flight handlers within the deadline. LocalEventBus publish clones handlers and invokes them sequentially inside the publisher's future. Shutdown only removes subscriptions and sends cancellation; it ignores the deadline and returns true immediately. Handlers already cloned by publish are not constrained by that cancellation.

The reproduction suspends a handler at a barrier. When shutdown returns true, publish is still unfinished; after the barrier is released, the handler executes its side effect. [InMemoryNatsEventBus](../crates/lattice-eventbus/src/nats.rs#L787) has similar shutdown behavior and should be included in backend contract checks.

Impact: an application may close Actor or storage resources after EventBus drain succeeds and still receive late operations. The in-memory test backend also cannot reliably validate the production backend's shutdown contract.

Recommendation: track executing handlers through managed counts/tasks. First close subscription and dispatch admission, then wait within the same deadline. If a weaker local contract is intentional, expose it through a distinct API rather than returning the same success signal as a complete drain.

## 3. Other Confirmed Boundary Issues

The following are source-confirmed, without dedicated runtime reproductions. They can be addressed alongside changes to the relevant modules:

| Location | Trigger and outcome | Recommendation |
|---|---|---|
| [ActivationDirectory::register:38](../crates/lattice-actor-distributed/src/directory.rs#L38) | Capacity checking and insertion are not atomic. Two threads inserting from maximum-1 can exceed the limit, after which `len == maximum` no longer rejects `len > maximum` | Reserve capacity atomically or check and insert under a lock, rolling back on failure. Changing only the comparison to `>=` does not remove the race |
| [ProtocolHostRegistry::register:255](../crates/lattice-actor-distributed/src/host.rs#L255) | Before capacity is full, duplicate registration overwrites the old host with insert and then returns DuplicateProtocol. An error return has already changed runtime state | Use the entry API to reject duplicates before mutation. Test that the old host still supports routing and drain after registration fails |
| [Actor runtime:667](../crates/lattice-actor/src/runtime.rs#L667), [observer:212](../crates/lattice-actor/src/observation.rs#L212) | A user observer is called after setting Running. A callback panic can bypass termination publication and registry cleanup, leaving a closed mailbox with a false Running state | Isolate observer failures and guarantee termination cleanup at the outermost runtime boundary. Cover DeathWatch notification and registry reclamation |

## 4. Architectural Assessment

### 4.1 Boundaries Worth Preserving

| Layer | Assessment and rationale |
|---|---|
| `lattice-actor` local kernel | Sound. It has no remoting dependency; local mailboxes, tasks, supervision, and DeathWatch can be used independently |
| `lattice-model` address data and distributed bound references | Sound. Serializable addresses are separated from sending capabilities, avoiding persistence of runtime resources |
| `lattice-remoting` | Sound. One association model with bounded control, interactive, and bulk lanes avoids another application-facing transport |
| `lattice-coordination` | Sound. etcd authority, leader guards, generation/claims, handoff, and pure state transitions serve explicit consistency requirements |
| Membership and placement domains | Sound. Member authority is isolated from each domain's placement authority, so a domain-local failure need not clear all data-plane state |
| `lattice-service` | Its position as the assembly layer is appropriate. Its implementation burden should be reduced where it combines assembly, routing, control effect application, and leave coordination |
| etcd, Kubernetes, MongoDB, and OTLP adapters | Separate dependency directions are appropriate. Optional integrations are not all embedded in the local Actor kernel |

These assessments follow from workspace dependencies and inspection of the actual modules; they do not imply that every internal boundary is free of defects.

The following mechanisms address real fault scenarios and should be preserved: exact incarnation/activation identity, versioned snapshots/deltas, lease and claim fencing, reliable control sequencing with bounded replay, StopFailed retention and quarantine, and drain deadlines.

The StopFailed model is particularly valuable: an instance whose persistence failed must not be treated as stopped, and an old instance must not regain business authority after being fenced. R1/R2 show that loading placeholders fall outside this model's management coverage; that coverage needs to be completed.

### 4.2 The Main Architectural Weakness

**There is still a gap between state machines and resource ownership.** A pure reducer can establish that a state transition is valid, but it cannot automatically establish that its caller's future will not be cancelled, a background task has stopped, a loading placeholder has been invalidated, or a budget reservation has been returned. Several findings occur precisely where state advances before asynchronous resources are settled.

Encode the key invariants in internal ownership objects and transition operations instead of relying on call-order conventions. Prioritize four boundaries: activation records, queued-message budgets, lane lifecycles, and drain commit results.

## 5. Cluster Lifecycle Assessment

| Phase | What is sound today | Gaps and required behavior |
|---|---|---|
| Construction and startup | The builder validates configuration, startup failures roll back, and the embedded coordinator uses a separate identity | Preserve the distinction between successful start and achieved readiness. Existing startup rollback tests passed |
| Discovery/bootstrap | Candidates indicate reachability; the handshake and control plane establish authoritative identity | R7: inbound establishment needs a deadline even before identity verification completes |
| Membership join | Exact incarnation, authoritative snapshots, and local Up status determine readiness | R9: snapshot time bounds must actually be enforced |
| Placement-domain join | Each domain has an independent session, version, and claim | R8: domain task exit must be reflected in advertisements, health, and campaign state |
| Normal service | Known data routes bypass etcd; domain authority constrains new activation | R1: authority must remain valid across asynchronous loader waits and publication |
| Disconnection and reconnect | Membership loss closes external admission while valid domain claims can continue supporting the data plane | R3/R4: association retirement must reclaim message budgets, socket permits, and reconnect tasks |
| Graceful leave | Close admission, drain each domain, then complete membership departure | R1/R5/R6: the drain set must be complete, the entire operation bounded, and success based on application-level commit confirmation |
| Terminal/force stop | Whole-deployment stop, graceful migration, and forced authority loss have distinct semantics; stop failure information is retained | R2/R10: loading placeholders and in-flight EventBus handlers must also be included in cleanup |
| Rolling replacement | Node incarnation distinguishes process lifetimes; a node with the same name does not automatically inherit authority | R6: local directory deletion cannot prove that the old incarnation was removed from shared membership |

Readiness is not a master switch for all internal messages. The current design closes the external edge during membership control-plane recovery while preserving traffic backed by valid domain authority. This agrees with the [runbook](operations/cluster-lifecycle-runbook.md) and should be preserved.

Normal paths have test coverage, but this review cannot conclude that all cluster lifecycle behavior is correct. Snapshot/reducer unit tests cannot replace tests of cancellation, process departure, and task cleanup.

## 6. Simplicity, Maintainability, and Overengineering

### 6.1 Reduce Duplicate Protocols and Entry Points Without Production Callers First

1. **Share small common mechanisms while preserving separate domain state machines.** Separating Membership and placement sessions provides reasonable isolation, but duplicate implementations of ACK waiting, monotonic clocks, and deadline propagation have already caused R5/R9. These small components are suitable for sharing; forcing the two authority state machines into one generic “large session” is not.
2. **Ops should not maintain a second set of shutdown stages.** `ops/shutdown.rs:43` (reviewed baseline; removed during remediation) defines DrainController and GracefulShutdown; no actual implementation of this trait was found anywhere in the repository. It independently manages readiness and shutdown order, while the production service already has complete leave/terminal/force flows. Remove the unused entry points or make them a thin adapter over the service lifecycle.
3. **Reduce the scope of the alternative serial inbound loop.** [serve_inbound_connection](../crates/lattice-remoting/src/messaging/inbound.rs#L90) is currently used only by messaging tests; the production endpoint uses BidirectionalLane. Move it into test support or explicitly limit its intended use, so the public API does not require long-term maintenance of a second set of ask/heartbeat scheduling semantics.
4. **Align documentation with the current API.** The [overview:82](architecture/00-overview.md#L82) still describes ActorRef as serializable, while the [API boundary document](architecture/09-actor-api-boundaries.md) explicitly states that only addresses are serializable and bound references are not. The overview's description of child actors also differs from the new boundaries. Update the main reading path rather than relying on readers to find later corrections.

These are the clearest sources of unnecessary maintenance work at present. Multiple crates, control planes for multiple domains, and typed references are not, by themselves, evidence of overengineering.

### 6.2 Recommendations for handle.rs and Large Modules

The current [handle.rs](../crates/lattice-actor/src/handle.rs) handles send admission, ask deadlines, observation events, lifecycle state, termination subscriptions, stop retries, and force-stop auditing. `Clone` and `new` manually keep several Arc fields in sync, increasing the risk of omissions when new state is added.

Within the module, separate the implementation into shared instance state, send admission, and lifecycle management, and centralize construction and state publication. This does not require new public traits or a new facade crate, nor should all state be placed behind one lock without measurement.

**Do not simply remove the dual atomic + watch representation.** The [existing performance records](baselines/current-performance.md#L123) explicitly document the benefit of using an atomic snapshot on the send hot path. This is the project's existing baseline; performance measurements were not rerun for this review. Refactoring should preserve the hot path and use one internal method to maintain state consistency.

The Registry's `entries`, `exact_entries`, `quarantined`, and ActivationDirectory also need a central definition of transition invariants. More valuable than a “1200 lines per file” limit are these constraints: an operation that returns an error must not replace the old instance; an activation must occupy only one ownership stage at a time; and loading, stop-failed, and quarantined instances must all be accurately enumerable and manageable.

### 6.3 Budget Explicitly for Validating Performance Complexity

The custom mailbox, memory pools, manual vtables, and Pin/unsafe paths do increase maintenance cost, but the amount of unsafe code alone is not a sufficient reason to replace them. The project already has throughput baselines and related tests. Document their benefits and constraints in one place, and add focused validation for allocation/deallocation, cancellation, panics, and concurrent memory ordering.

The current [CI](../.github/workflows/ci.yml) includes structure, fmt, clippy, workspace tests, simulation/model checks, and Docker/kind/chaos workflows under different trigger conditions. No dedicated Miri/Loom gate was found. This review has not established a memory safety defect; this is a recommendation to improve validation capabilities.

## 7. Repair Order and Acceptance Criteria

**Start with activation ownership, transport resource reclamation, and the leave commit protocol, in that order.** These are the first repair batch. They address possible execution after authority loss, persistent node-wide capacity loss, and incorrect or unbounded cluster departure.

The execution order below considers impact, the conditions needed to trigger a failure, reproduction evidence, and shared implementation dependencies. It does not change the severity labels in Section 2. For example, R2 remains P2 but belongs with R1 because both require a coherent activation lifecycle. Trigger frequency has not been measured in production.

### 7.1 Ordered Work Queue

| Order | Work item | Why this position | Acceptance criteria |
|---:|---|---|---|
| **1** | **R1 + R2: activation ownership across loading, cancellation, drain, and fence** | R1 threatens the single-executor invariant; R2 can strand an Actor ID indefinitely. Both originate in unmanaged loading placeholders and should share one ownership design | Add barrier-controlled loader tests before implementation. Cancellation must release or resolve the placeholder, drain must account for loading work, and an old-generation loader must not publish or dispatch after authority is revoked. Include a cluster handoff test, beyond the existing Registry reproduction |
| **2** | **R3 + R4: reclaim byte budgets and reconnect tasks on association retirement** | Both are reproduced and can turn routine disconnects or node replacements into persistent capacity loss affecting healthy peers | Queued-message destruction and write cancellation return each reservation exactly once. Retirement stops lanes during backoff, connect, idle, and active operation. Repeated churn restores byte and connection capacity without restarting the endpoint |
| **3** | **R5 + R6: bound leave and confirm the authoritative commit** | Shutdown can hang or report success without committed membership removal. Fixing only the timeout leaves false success possible | Use one absolute deadline and the same operation_id across retries. Test leader rollover, disconnection before/after ACK, and old-term rejection. A transport ACK alone must never complete leave |
| **4** | **R7: apply a deadline to the entire inbound handshake** | A small, independently fixable boundary has a reproduced node-wide capacity-exhaustion path | Silent TCP clients and stalled TLS/catalogue/bootstrap negotiation release sockets and permits by the establishment deadline; legitimate clients can connect afterward |
| **5** | **R8 + the observer panic issue in Section 3: complete failure supervision** | Panics can leave dead components advertised as Active/Running, preventing recovery and misleading health checks | A domain task panic withdraws leadership advertisements and triggers the chosen recovery policy. An observer panic cannot skip terminal publication, DeathWatch notification, or Registry cleanup |
| **6** | **R10: enforce the EventBus drain contract** | An application may tear down dependent resources while handlers still execute. This is especially relevant when shutdown depends on EventBus drain | All backends run the same drain contract tests: close admission, track in-flight handlers, and return success only after they finish; enforce the deadline when they do not |
| **7** | **Section 3: atomic ActivationDirectory capacity and non-mutating duplicate host rejection** | These are concrete, localized boundary errors with a narrower scope than the cluster-wide failures above | Concurrent registration cannot exceed capacity. Duplicate registration returns an error while preserving the old host's routing and drain behavior |
| **8** | **R9: use real monotonic time for membership snapshot staging** | The timeout contract is broken, but size, digest, and version checks remain intact. Its severity remains P3 | Advance time beyond the staging timeout before the next chunk/end and verify rejection and recovery |
| **9** | **Section 6: remove redundant APIs, align documentation, and reorganize internals** | These changes reduce future maintenance cost, but do not replace the concrete lifecycle fixes above | Remove or narrow unused shutdown/inbound entry points, align address/reference documentation, centralize invariants, and preserve the external API and measured hot-path behavior |

### 7.2 Dependencies and Parallel Work

Orders 1, 2, and 3 are separate implementation tracks and can proceed in parallel when multiple contributors are available. With one contributor, follow the table. Keep changes independently reviewable even when they belong to the same track.

- **Keep R1 and R2 consistent.** Moving activation into a detached task to address cancellation can worsen R1 if drain/fence still cannot own or invalidate that task.
- **Treat R5 and R6 as one leave contract.** A deadline and a committed outcome solve different parts of the same operation; neither substitutes for the other.
- **R7 can run alongside order 2.** If the remoting listener is accessible to untrusted or uncontrolled peers, promote it into the first repair batch because silent connections can exhaust node capacity.
- **R9 can be an early, small follow-up to session changes.** Its low severity does not require delaying an independent fix, but it should not displace work on the first three items.
- **Document corrections can accompany the relevant fixes.** Defer broad structural cleanup until regression tests establish the lifecycle invariants it must preserve.

### 7.3 Completion Gate for the First Repair Batch

The first batch is complete when activation cancellation and handoff preserve ownership, repeated association churn restores all resource budgets, and leave is both bounded and dependent on an authoritative commit. Passing the existing 329 unit tests alone is insufficient: add the fault scenarios above to the permanent test suite, then run the relevant integration and cluster lifecycle checks.

These fixes should be split into independently reviewable changes. Rewriting the control plane wholesale or introducing a generic lifecycle framework would expand the validation scope without directly addressing the specific gaps found in this review.

## 8. Validation Record and Limitations

Environment: Windows / PowerShell, `rustc 1.98.0`, `cargo 1.98.0`. CI is configured to use Rust 1.97.0; these results do not replace CI results on that toolchain.

Executed:

```powershell
cargo test -p lattice-actor -p lattice-actor-distributed -p lattice-coordination -p lattice-service -p lattice-remoting -p lattice-discovery -p lattice-discovery-k8s --lib --locked --offline
```

| crate | Passed |
|---|---:|
| lattice-actor | 44 |
| lattice-actor-distributed | 4 |
| lattice-coordination | 112 |
| lattice-service | 64 |
| lattice-remoting | 89 |
| lattice-discovery | 12 |
| lattice-discovery-k8s | 4 |
| Total | **329; 0 failed** |

A standalone Cargo test program depending only on the project's public APIs was also created in the locally ignored directory `target/review-repro/`. It was run with assertions that the current defects are present:

```powershell
cargo test --manifest-path target/review-repro/Cargo.toml --target-dir target --offline -- --nocapture
```

All 6 reproductions exhibited the expected defective behavior. Key output:

```text
cancel: state=Loading, retry=Err(WaiterTimeout { timeout: 10ms }), drain_completed=true
drain: completed=true, late_activation=Running
budget: previous association removed, new admission=Err(NodeByteBudgetExceeded)
reconnect: association removed, retained connection permits=3
handshake: after 4x configured timeout, open=3, capacity=3, shed=1
eventbus: shutdown returned true while handler pending; handler side effect ran after shutdown
```

In these reproductions, `test ... ok` means that the assertion that a defect exists held; it does not mean the feature is correct. When fixing these issues, move the scenarios into the formal test suite and change the assertions to the expected correct behavior.

Local logs are `target/review-core-tests.log` and `target/review-repro.log`. The reproduction program and logs are temporary artifacts of this review, are excluded from the commit, and will not survive cleanup of target. Each finding above records its reproduction conditions and regression requirements.

This review did not run the full workspace/all-features suite, Clippy, real etcd/NATS/MongoDB integration tests, Docker/kind, chaos, simulation/model tests, or performance benchmarks. CI configuration and relevant test code were read, but historical acceptance results, configured CI jobs, and unexecuted tests are not reported as passing in this review.
