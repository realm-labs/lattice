# Lattice Actor and Service Lifecycle Hardening Execution Plan

> Status: complete; all lifecycle batches and final acceptance gates passed
> Review date: 2026-07-16
> Primary scope: `lattice-actor`, `lattice-service`, lifecycle-facing parts of `lattice-placement`
> Architecture baseline: [architecture/](architecture/)
> Compatibility policy: hard switch; do not preserve contradictory lifecycle behavior

---

## 0. Goal Prompt

```text
Your goal is to fully execute docs/actor-service-lifecycle-hardening-plan.md.

Read this document completely before changing code. Treat it as the execution authority for Actor,
placement-domain, LogicService, and Coordinator-candidate lifecycle behavior. Use docs/architecture/
as the architecture baseline, but update architecture documents in the same change whenever their
current prose conflicts with the invariants fixed by this plan.

The highest-priority invariant is that Actor::stopping() is a business durability hook. If it fails,
the runtime must retain the exact Actor instance and its in-memory state. StopFailed is a nonterminal,
non-routable intervention state: it blocks voluntary passivation, handoff, and graceful shutdown until
the same Actor instance persists successfully or an operator explicitly authorizes a force stop with
data-loss semantics.

Do not implement StopFailed by returning from the Actor task, dropping the Actor, unregistering the
activation, treating it as a normal terminal state, or silently activating a replacement. Do not send
ActorTerminated while the failed Actor instance remains retained.

Distributed authority is stronger than local cleanup. If a shard or singleton claim is externally
lost, fence business admission immediately and never delay replacement ownership. Retain the failed
old Actor locally in a non-authoritative quarantine when the process remains alive, so an operator can
retry persistence or explicitly discard it. The quarantined instance must never receive business
messages, resolve as the current activation, or regain authority implicitly.

Make the production LogicService consume NodeLifecycle effects through one lifecycle driver. Remove
the current split where the reducer describes admission, drain, fencing, and identity-release effects
but callers separately reproduce only some of them. Publish Ready, Draining, Stopping, and Terminated
only when their resource and admission postconditions are true.

Execute the batches in dependency order. A checked item requires implementation and executable
evidence. Update the Current Execution Pointer after every completed batch or material design change.
Final completion requires focused lifecycle tests, placement handoff tests, deterministic simulation,
workspace format/clippy/tests, and documentation agreement.
```

## 0.1 Current Execution Pointer

```text
Overall status: complete
Current batch: none — final acceptance complete
Completed batches: Batch A, Batch B, Batch C, Batch D, Batch E
Known broken frontier: none within this plan
Implemented design frontier: voluntary StopFailed retains the exact Actor instance and Registry
reservation; terminal cleanup is eager and exactly scoped; external authority loss fences routing and
moves the old instance into bounded non-authoritative quarantine without waiting for persistence;
production node lifecycle effects execute through one driver before observable state commits; domain
and Coordinator-scope health enforce their documented readiness and termination postconditions
Final completion evidence: focused Actor/placement/service tests, deterministic retained-stop replay
and failpoint suites, workspace format/clippy/all-features tests, and architecture agreement passed
```

## 0.2 Execution Policy

```text
Primary plan: docs/actor-service-lifecycle-hardening-plan.md
Architecture authority: docs/architecture/
Compatibility mode or dual lifecycle semantics: forbidden
Silent Actor data loss after stopping failure: forbidden
Force stop without explicit data-loss reason and observability: forbidden
Publishing Terminated before resources are stopped: forbidden
Ignoring lifecycle effects in production: forbidden
Tracker update: after every completed batch or material broken-frontier change
Final full acceptance: mandatory
```

---

## 1. Review Conclusion

The distributed placement core is structurally sound: slot ownership is term- and generation-qualified,
claim deadlines fence stale owners, handoff uses a revision barrier, and replacement authority is not
installed before the previous authority is fenced or proven absent.

The lifecycle boundary around that core is not yet coherent. Actor runtime state, Registry ownership,
DeathWatch, service health, and production lifecycle effects currently disagree in several failure and
shutdown paths. This plan keeps the placement safety model and replaces the contradictory outer
lifecycle behavior.

### 1.1 Findings to Resolve

1. `Actor::stopping()` failure currently sets `StopFailed` and then returns from the Actor task. The
   Actor instance is dropped even though stopping is the business-state persistence hook.
2. Normal stopping failure does not publish `ActorTerminated`, while Registry and directory lookup
   treat `StopFailed` as terminal. DeathWatch, routing, and lifecycle subscribers therefore disagree.
3. The documented retry/operator path for `StopFailed` does not exist. A voluntary handoff can remain
   blocked while the only in-memory copy of the Actor state has already been destroyed.
4. Idle/business passivation can leave terminal entries in `ActorRegistry`. `running_actor_ids()`
   returns those entries without checking lifecycle, so a later shard drain can attempt to stop an
   already closed mailbox and fail the handoff.
5. Exact activation-directory cleanup is lazy and differs between Registry lookup paths. Terminal
   activations can retain capacity until a later lookup happens to clean them.
6. `NodeLifecycle::transition()` returns admission, drain, fencing, runtime-stop, and identity-release
   effects, but production `LatticeService` drops them. Only the simulator consumes the complete effect
   model; production callers manually reproduce parts of it.
7. `PlacementDomainState::Draining` is not used by the normal leave path, and normal shutdown does not
   move every configured domain to `Terminated`.
8. Forced shutdown publishes node `Terminated` before endpoint and supervised tasks have necessarily
   stopped, contradicting the documented resource postcondition.
9. `ActorLifecycleState::Activating` and `Loading` are never emitted by the Actor handle. Registry
   activation state and Actor-cell runtime state are mixed into one public enum.
10. A dedicated Coordinator candidate reports node `Ready` without exposing whether each Coordinator
    scope is `Active`, `Standby`, or `Failed`; application readiness currently hides this distinction.

---

## 2. Lifecycle Model

### 2.1 Entity Activation and Actor Cell Are Different State Machines

Use separate concepts rather than forcing Registry loading and a live Actor task into one enum.

```text
EntityActivationState
  Absent -> Activating -> Loading -> Active
  Activating/Loading -> Absent on failure
  Active -> Absent only after successful terminal cleanup
```

```text
ActorCellState
  Starting -> Running
  Starting -> Stopping after startup failure
  Running -> Passivating | Stopping
  Passivating | Stopping -> StopFailed when persistence fails
  StopFailed -> Passivating | Stopping on explicit retry
  Passivating | Stopping -> Stopped after persistence succeeds
  StopFailed -> Quarantined when external authority is lost
  StopFailed | Quarantined -> Stopped only after persistence succeeds or explicit force stop
```

`Stopped` is terminal. `StopFailed` and `Quarantined` are not terminal merely because they reject
business traffic.

### 2.2 StopFailed Contract

When `Actor::stopping()` returns an error during a voluntary stop:

- retain the same Actor instance inside its runtime cell;
- retain its exact activation identity and Registry reservation;
- close business-message admission and reject new asks/tells with an explicit lifecycle error;
- keep the system/admin lane available;
- do not publish `ActorTerminated`;
- do not permit replacement activation for the same logical entity;
- record the stop reason, previous phase, error, first-failure time, latest-attempt time, and attempt
  count;
- surface the failure through observation, metrics, health/admin inspection, and structured logs;
- block voluntary shard/singleton handoff and graceful service termination.

The persistence operation must be safe to retry. Actor implementations should use idempotent writes,
an operation ID, or version/CAS semantics. The runtime must preserve the original stop reason and retry
the same Actor instance.

### 2.3 Operator Actions

Provide explicit system/admin operations with auditable results:

```text
RetryStop
  StopFailed -> previous Passivating/Stopping phase
  invoke Actor::stopping() again on the retained instance

InspectStopFailure
  return activation identity, entity/shard ownership, failure record, and authority status

ForceStop(reason, ticket)
  explicitly discard retained in-memory state
  emit a data-loss-possible event and high-severity metric/log
  unregister and publish ActorTerminated only after the cell is actually gone
```

Force stop is never a transparent fallback from graceful shutdown. The API must make operator-approved
data loss visible in its name, reason, result, logs, and metrics.

### 2.4 External Authority Loss

Claim expiry, force removal, or an independently proven replacement cannot be delayed by local
`StopFailed`:

1. fence logical and exact business admission immediately;
2. remove the old activation from current routing and authority lookup;
3. allow Coordinator recovery and new-owner activation after the normal claim/fencing proof;
4. retain the old Actor instance in a bounded, non-authoritative quarantine when the process remains
   alive;
5. allow only inspect, retry-persist, export-diagnostics, and explicit force-discard operations;
6. never let quarantine restore authority or rejoin routing implicitly.

Quarantine must be bounded by configuration and observable. Capacity exhaustion must reject additional
unsafe retention with an explicit fatal/data-loss signal; it must not silently drop an existing failed
Actor.

### 2.5 Registry and DeathWatch Contract

- Registry distinguishes `active`, `stopping-retained`, `quarantined`, and `terminal` entries.
- `get_or_activate()` cannot replace a retained `StopFailed` Actor.
- `running_actor_ids()` returns only actors that are actually running and admitted.
- shard drain separately enumerates retained failures and reports them as blockers.
- successful stop/passivation performs eager, exact Registry and ActivationDirectory cleanup.
- startup failure and explicit force stop perform the same terminal cleanup path.
- `ActorTerminated` is emitted exactly once, after terminal cleanup commits.
- watching a retained `StopFailed` activation remains pending; watching a quarantined old activation
  resolves according to the explicit quarantine/termination contract and never follows a replacement.

---

## 3. Required Invariants

### 3.1 Actor Durability

1. A failed `Actor::stopping()` call never drops the Actor instance by itself.
2. A voluntary `StopFailed` Actor cannot receive business messages or be replaced.
3. Retry invokes the same Actor instance and preserves its in-memory state.
4. Graceful drain cannot report success while any participating Actor is `StopFailed`.
5. Force stop is the only local operation allowed to discard retained state without successful
   persistence, and it always emits data-loss evidence.

### 3.2 Distributed Authority

1. Local retention never extends an expired or externally revoked shard/singleton claim.
2. Admission closes before an old authority can be replaced.
3. A quarantined old Actor cannot serve exact or logical traffic.
4. New authority still requires the existing term, generation, claim, and handoff proof.

### 3.3 Registry and Watch

1. One logical activation has at most one active or retained local Actor cell.
2. Terminal cleanup removes Registry and ActivationDirectory entries exactly once.
3. Successful idle passivation cannot make a later shard drain fail on a closed mailbox.
4. Every accepted watch completes with exactly one terminal notification unless explicitly unwatched;
   `StopFailed` alone is not terminal.

### 3.4 Service and Domain Health

1. `Ready` means business admission is open for the declared readiness scope.
2. `JoiningMembership` and `Draining` reject new external business admission while retaining required
   internal bootstrap/control traffic.
3. every configured domain visibly transitions through `Draining` and reaches `Terminated` on normal
   shutdown;
4. node `Terminated` is published only after endpoint, associations, placement claims, Actor cells,
   supervised tasks, and runtime identity are no longer live;
5. graceful shutdown fails with an intervention report rather than hiding retained `StopFailed`
   Actors;
6. Coordinator-candidate readiness exposes per-scope `Active`, `Standby`, and `Failed` health.

---

## 4. Production Lifecycle Driver

Introduce one production lifecycle driver that serializes node lifecycle events and consumes every
`ServiceLifecycleEffect`. Callers submit events; they do not reproduce reducer effects themselves.

```text
event
  -> validate/reduce
  -> execute ordered admission/fencing/drain/runtime effects
  -> commit observable state
  -> publish node/domain health
```

Ordering requirements:

- `MembershipLost`: close business admission before publishing `JoiningMembership`.
- `SnapshotInstalled`: validate membership and required domains, open admission, then publish `Ready`.
- `BeginDrain`: close admission, publish every participating domain as `Draining`, then issue domain
  drains.
- `DrainComplete`: require no unresolved voluntary `StopFailed` Actors, fence/release claims, stop
  runtime components, and only then complete termination.
- `ForceStop`: make data-loss semantics explicit, fence admission/authority first, perform component
  shutdown, then publish `Terminated`.
- `RuntimeTerminated`: close admission immediately, reconcile resource cleanup, and publish
  `Terminated` only after cleanup completes.

Lifecycle-effect results must not be silently ignored. Illegal transitions and effect failures require
structured diagnostics and a stable degraded/stopping health state.

### 4.1 Admission Gate

Install a shared admission gate in production dispatch rather than relying only on health labels.

- Internal bootstrap, membership, placement-control, watch cleanup, stop, retry, and force-stop traffic
  remains available in non-ready states when required for recovery.
- New exact ActorRef and logical EntityRef/SingletonRef business traffic is rejected when the node gate
  or relevant domain/authority gate is closed.
- Placement authority remains the final distributed authorization check; the node gate is an
  additional process-lifecycle gate, not a replacement for claims.

---

## 5. Health and Readiness Model

### 5.1 Logic Nodes

```text
Node: Booting -> JoiningMembership -> Ready -> Draining -> Stopping -> Terminated
Domain: Joining -> Ready | Degraded -> Draining -> Terminated
```

One degraded placement domain does not necessarily demote the whole node from `Ready`; readiness APIs
must allow callers to require an explicit domain set. Membership loss closes the node admission gate
even if cached domain routes remain visible until their claim deadlines.

### 5.2 Coordinator Candidates

Node readiness and leadership are separate:

```text
Candidate process: Booting -> Ready -> Stopping -> Terminated
Coordinator scope: Active | Standby | Failed
```

`Ready + Standby` is healthy. `Ready + Failed` is observable degradation and cannot be hidden behind an
empty domain map. Dedicated-candidate `wait_ready()` requires the runtime to be alive and every
configured scope to be `Active` or `Standby`. Embedded application readiness checks both the logic
service requirements and the managed candidate runtime.

---

## 6. Implementation Batches

### Batch A — Retained Actor Cell and StopFailed Contract

- [x] Add characterization tests demonstrating the current drop-on-StopFailed defect.
- [x] Refactor the Actor runtime loop so stopping failure retains the Actor instance.
- [x] Add lifecycle-aware rejection of business messages while stopping or failed.
- [x] Preserve the previous stop phase and reason for retry.
- [x] Add structured `StopFailureRecord` inspection.
- [x] Add `RetryStop` system command and ActorHandle/admin entry point.
- [x] Add explicit force-stop command with reason, ticket, metrics, logs, and data-loss event.
- [x] Make `ActorTerminated` exactly-once and terminal-only.
- [x] Define task/child quiescence so no background activity mutates a retained failed Actor.

Batch A acceptance:

- the same Actor instance survives one or more stopping failures;
- a retry after repairing the persistence dependency succeeds and then terminates normally;
- DeathWatch remains pending during `StopFailed` and completes once after successful retry;
- force stop is observable as a distinct data-loss path.

### Batch B — Registry, Passivation, Drain, and Quarantine

- [x] Replace lazy terminal cleanup with one eager completion callback owned by Registry/cell runtime.
- [x] Clean exact ActivationDirectory entries on every terminal path.
- [x] Ensure `running_actor_ids()` excludes stopped, failed, and quarantined cells.
- [x] Report retained StopFailed actors separately as drain blockers.
- [x] Prevent replacement activation while a voluntary StopFailed cell is retained.
- [x] Add bounded quarantine for externally fenced failed Actors.
- [x] Remove quarantined activations from current exact/logical routing without dropping the instance.
- [x] Add inspect, retry-persist, diagnostics-export, and force-discard quarantine operations.
- [x] Make Registry drain return a structured result rather than only an actor count or boolean.

Batch B acceptance:

- successful idle/business passivation leaves no Registry or directory capacity leak;
- a shard containing previously passivated entities drains successfully;
- voluntary StopFailed blocks handoff without losing the Actor instance;
- external claim loss fences the old Actor, permits safe replacement, and retains bounded local
  recovery state.

### Batch C — Service Lifecycle Driver and Admission

- [x] Introduce one serialized production lifecycle driver.
- [x] Consume every `ServiceLifecycleEffect`; make ignored effects impossible or loudly diagnosed.
- [x] Install the shared node business-admission gate in inbound and local dispatch paths.
- [x] Remove duplicate hand-written effect orchestration from membership, leave, force-stop, and
  runtime-termination callers.
- [x] Define effect failure and cancellation behavior.
- [x] Add `Stopping` if required to avoid publishing `Terminated` before shutdown completion.
- [x] Make graceful shutdown return a structured intervention report for retained failures.
- [x] Keep force shutdown explicitly destructive and auditable.

Batch C acceptance:

- membership loss closes business admission before the node becomes observably non-ready;
- joining/recovery internal control traffic remains usable;
- no production transition drops a reducer effect;
- node Terminated implies all documented resource postconditions.

### Batch D — Domain and Coordinator Health

- [x] Drive every configured placement domain through Joining/Ready/Degraded/Draining/Terminated.
- [x] Publish per-domain drain progress and retained-Actor blockers.
- [x] Preserve domain-local degradation without incorrectly disabling unrelated domains.
- [x] Expose Coordinator scope Active/Standby/Failed health.
- [x] Correct client-only, embedded-candidate, and dedicated-candidate `wait_ready()` semantics.
- [x] Ensure application shutdown reports which logic/domain/candidate component blocked completion.

Batch D acceptance:

- health snapshots never show `node=Terminated` with a live or Ready domain;
- normal leave visibly enters Draining before Terminated;
- dedicated standby candidates are healthy and failed scopes are observable;
- embedded readiness covers both managed services.

### Batch E — State Naming, Documentation, and Full Acceptance

- [x] Separate activation/loading state from live Actor-cell lifecycle state.
- [x] Remove or implement currently unreachable lifecycle variants.
- [x] Update architecture diagrams, API examples, operations guidance, and error semantics.
- [x] Add an operator runbook for RetryStop, quarantine recovery, and force-stop data loss.
- [x] Add metrics and alerts for StopFailed age/count, quarantine capacity, blocked drain, forced data
  loss, lifecycle-effect failure, and termination latency.
- [x] Complete all focused, simulation, integration, and workspace gates.

---

## 7. Test Matrix

### 7.1 Actor Unit and Integration Tests

- stopping fails once, Actor memory is preserved, retry succeeds;
- stopping fails repeatedly without task exit or replacement activation;
- ask/tell is rejected while StopFailed, while retry/admin commands still work;
- DeathWatch sends nothing on StopFailed and exactly one event on terminal completion;
- explicit force stop emits data-loss evidence and exactly one termination;
- startup failure, mailbox closure, explicit stop, business passivation, idle passivation, and parent
  stop all use the unified terminal cleanup path;
- directory and Registry capacity returns to baseline after high-cardinality passivation.

### 7.2 Placement Tests

- voluntary entity/singleton StopFailed persists slot StopFailed and blocks replacement;
- retry success resumes handoff from the same Actor instance;
- external claim loss fences even when persistence still fails;
- quarantine cannot receive exact or logical messages;
- new owner requires the existing claim/generation proof and cannot retarget old ActorRefs;
- node drain reports every blocking activation deterministically.

### 7.3 Service Tests

- membership loss closes admission before publishing non-ready state;
- recovery opens admission only after membership and required domains are ready;
- graceful leave publishes domain Draining, rejects new work, and reaches full Terminated;
- StopFailed prevents graceful Terminated and produces an intervention report;
- force shutdown is explicitly destructive and publishes Terminated only after task/resource cleanup;
- partial domain degradation leaves unrelated domains usable;
- dedicated and embedded candidate readiness exposes scope health.

### 7.4 Simulation and Fault Tests

- stop persistence failure concurrent with handoff;
- claim expiry concurrent with repeated stop retry;
- membership loss during node drain;
- Coordinator rollover while a source Actor is StopFailed;
- process shutdown with voluntary failures and quarantined actors;
- duplicate/replayed retry, force-stop, drained, and termination commands;
- bounded quarantine and Registry capacity under repeated failures.

---

## 8. Final Acceptance Gates

All of the following are mandatory before marking this plan complete:

- [x] `cargo fmt --all -- --check`
- [x] `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- [x] `cargo test -p lattice-actor --all-targets`
- [x] `cargo test -p lattice-placement --all-targets`
- [x] `cargo test -p lattice-service --all-targets`
- [x] deterministic lifecycle/placement simulation and replay pass
- [x] `cargo test --workspace --all-targets --all-features`
- [x] architecture lifecycle diagrams agree with executable state machines
- [x] no ignored production `ServiceLifecycleEffect`
- [x] no path treats `StopFailed` as terminal without explicit force-stop authorization
- [x] no graceful path drops an Actor whose stopping persistence failed
- [x] node/domain/candidate health postconditions are asserted by integration tests
- [x] operator runbook demonstrates retry recovery and force-stop data-loss handling

---

## 9. Explicit Non-Goals

- Event sourcing, remembered entities, or a general persistence framework.
- Allowing local StopFailed retention to extend distributed ownership.
- Automatically resuming normal business processing after a partially failed stopping hook.
- Silently retrying forever without bounded observability and operator control.
- Treating process memory as crash-safe storage; quarantine protects against an avoidable runtime drop,
  not machine or process loss.
- Introducing compatibility aliases or dual old/new lifecycle modes.
