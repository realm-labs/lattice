# Lattice Coordinator Correctness Hardening Execution Plan

> Status: complete; Batches A-E accepted on 2026-07-14
> Design authority: [architecture-review.md](architecture-review.md)
> Architecture baseline: [architecture/README.md](architecture/README.md)
> Implemented baseline: [production-hardening-plan.md](production-hardening-plan.md)
> Compatibility policy: hard switch; no mixed storage or placement-control generations
> Storage generation: 4 (hard switch complete)
> Placement-control generation: 4 (hard switch complete)

---

## 0. Goal Prompt

```text
Your goal is to fully execute docs/coordinator-correctness-implementation-plan.md.

Read this document and docs/architecture-review.md completely before changing code. Treat this plan
as the execution authority and the review as the design/risk authority. Use docs/architecture/ as the
baseline for identity, placement, remoting, lifecycle, operations, and distributed-test invariants.

Start from the Current Execution Pointer and work through Batches A-E in dependency order. Update the
pointer and checklist after each completed batch and whenever the broken frontier changes. A checked
item requires implementation plus executable evidence; prose or an unexecuted test sketch is not
completion evidence.

This is an intentional storage and placement-control hard switch. Do not add dual writes, generation
fallback, a generic public etcd transaction language, compatibility wrappers around the old
single-key Coordinator mutation methods, or a rolling mixed-version mode. Batches A and B form the
first coherent code boundary; do not stop after Batch A merely to restore compilation through a
temporary bypass.

The highest-priority invariant is that every authoritative Coordinator mutation is accepted by
storage only while the exact lease-backed LeaderRecord for that term still exists. Record-local CAS,
an in-memory is-leader flag, and a term field stored only in the new value are not substitutes for
this guard.

The second invariant is that Coordinator commits never create an Allocating or Running slot without
the matching owner/generation claim, and never create a slot/plan active-move relationship on only one
side. Lease expiry is an external fencing event and may temporarily remove a claim; recovery must
then be bounded and must not grant a different owner authority without proof that the previous claim
is invalid.

Use named domain transactions shared by the in-memory store, etcd store, runtime, and simulator.
Preserve the explicit Fenced phase. Pre-grant leases as resource setup, attach claim keys inside the
atomic transaction, and revoke unused leases after a failed compare. Publish deltas or send control
effects only after the commit succeeds.

Term-qualify snapshots, deltas, member events, barriers, and acknowledgements with StateVersion.
Keep per-plan record revision separate. A node must install a snapshot for a new term before accepting
that term's deltas; an acknowledgement from an older term must never satisfy a current barrier.

Persist all mutating admin-operation idempotency records and automatic-balance pause settings.
Enforce shard key-domain and total etcd cardinality limits. Provide an explicit offline, resumable
generation-3-to-4 migration tool; never migrate automatically during service startup.

Do not implement fixes for rejected review claims: redirected established dials already perform exact
certificate binding, CoordinatorEvent is intentionally used only for sequence-aware load telemetry,
and AssociationId already supplies the reliable-control epoch lifecycle. Preserve regression coverage
for those properties without redesigning them.

Keep Rust files below 1200 physical lines, avoid new public re-exports and non-test wildcard imports,
and split the etcd backend before adding enough transaction/migration code to make it monolithic.
Run focused checks in every batch. Final completion requires structure, format, clippy, workspace
tests, real-etcd contract tests, deterministic simulation/model checking, Docker e2e/HA/chaos/k8s,
and bounded soak/replay evidence.
```

### 0.1 Execution Policy

```text
Primary plan: docs/coordinator-correctness-implementation-plan.md
Design authority: docs/architecture-review.md
Architecture authority: docs/architecture/
Implemented baseline: docs/production-hardening-plan.md
Compatibility mode, dual writes, and generation fallback: forbidden
Generic public storage transaction API: forbidden
Old unguarded Coordinator mutation methods after Batch B: forbidden
Batch A stopping point: forbidden; Batch A + B are one coherent hard-switch boundary
Target commit count: 4
Tracker update: after every completed batch or material broken-frontier change
Final full acceptance: mandatory
```

When this plan conflicts with the corrected review, preserve the review's leader fencing,
all-or-nothing placement, bounded recovery, and bounded storage invariants. Update both documents in
the same change before implementing a different design.

### 0.2 Current Execution Pointer

```text
Overall status: complete
Current batch: Batch E — completed; final acceptance audit complete
Completed batches: Batch A, Batch B, Batch C, Batch D, Batch E
Known broken frontier: none. The final replay frontier exposed and resolved Git-Bash container-path
conversion in scripts/test-docker.sh; replay then matched the authoritative main TraceJournal.
Latest executable evidence (2026-07-14): scripts/check-structure.sh, cargo fmt --all -- --check,
cargo clippy --workspace --all-targets --all-features -- -D warnings, cargo test --workspace
--all-targets --all-features, focused placement/service/ops/sim suites, and one-member real-etcd
contract tests all passed. Docker run IDs/artifacts: quality 20260714t055129z-252; sim
20260714t055946z-1334; model 20260714t060144z-1481; e2e 20260714t060414z-1396; three-member
e2e-ha-etcd 20260714t060829z-993; chaos 20260714t061315z-569; k8s
20260714t061831z-1624; 900-second soak seed 20260714 at 20260714t062655z-1307; replay of
trace-20347058.json at 20260714t065247z-1266. Artifacts are retained under
target/test-artifacts/<run-id> and every wrapper cleanup check passed.
Next implementation action: none; the execution plan is complete
Next coherent stopping point: final handoff
Final completion condition: every checklist and acceptance item in Sections 4-10 is complete and the
Current Execution Pointer contains dated evidence for all final gates
```

### 0.3 Batch Tracker

- [x] Batch A — guarded domain store contracts
- [x] Batch B — atomic runtime placement transitions
- [x] Batch C — term-qualified state stream and bounded reconciliation
- [x] Batch D — durable admin state, storage bounds, migration, and operability
- [x] Batch E — simulation and distributed acceptance

---

## 1. Required Outcome

At completion, lattice must satisfy all six corrected-review findings:

| Review ID | Required outcome |
|---|---|
| F1 | Every Coordinator mutation transaction compares the exact live leader record and term; stale writers receive `LeadershipLost` and stop |
| F2 | Initial allocation and replacement installation atomically commit slot plus claim; post-commit delivery is replay/reconcile safe |
| F3 | Move reservation and completion atomically update slot plus plan; no active-move linkage exists on only one side |
| F4 | Pause state and mutating admin operation IDs/results survive leader failover for a documented bounded retention window |
| F5 | Shard IDs are validated against configuration, total durable cardinality is enforced at creation, and recovery uses bounded pagination |
| F6 | Incarnation replacement while the old lease is live returns an observable retryable state; acceleration requires independent fencing proof |

The plan also completes the review's lower-priority operability work: useful etcd error categories,
per-connection failure visibility, reconciliation metrics, and explicit migration diagnostics.

### 1.1 Non-goals

- Do not change bootstrap redirect certificate binding except to preserve regression tests.
- Do not route node/shard load telemetry through reliable control; it is intentionally latest-value,
  sequence-aware, and lossy.
- Do not persist or reset `AssociationId` on ordinary lane reconnect.
- Do not replace etcd with another consensus system.
- Do not add Gossip, split-brain resolution, or discovery-backed membership.
- Do not promise exactly-once business messages.
- Do not allow a numerically newer incarnation to supersede a still-leased predecessor by assertion.
- Do not let reconciliation become the primary substitute for atomic commits.

---

## 2. Locked Architecture Decisions

These decisions are implementation constraints, not optional sketches.

### 2.1 Ordering and fencing types

Introduce and keep distinct:

```text
LeaderGuard
  exact LeaderRecord { node, protocol_generation, term }

StateVersion
  term: CoordinatorTerm
  revision: Revision

PlanRevision
  nonzero per-plan CAS counter
```

Target field changes:

- `PlacementSlot.coordinator_term + revision` becomes `version: StateVersion`; claim validation uses
  `slot.version.term`.
- `MemberRecord.revision`, `MemberEvent.revision`, snapshot revisions, `CoordinatorDelta.revision`,
  `JoinReady.snapshot_revision`, `AppliedRevision`, and `DrainSlot.revision` become `StateVersion`.
- `RebalanceProposal.base_revision` becomes `base_version: StateVersion`.
- `RebalancePlan` keeps its creation `coordinator_term`, stores `base_version`, and uses
  `record_revision: PlanRevision` for its own CAS.
- handoff `barrier_revision` becomes `StateVersion`.
- Coordinator inspection reports the current `StateVersion`.

`StateVersion` ordering is lexicographic for diagnostics, but delta continuity is stricter:

- same term: only `revision + 1` is accepted;
- higher term: a fresh snapshot is required before any delta;
- lower term: reject as stale;
- barrier ack: exact term and revision greater than or equal to the barrier revision.

The durable `coordinator/state_revision` key stores the last globally allocated node-visible
revision. A new leader forms its snapshot version as `{new_term, stored_revision}`. Member/slot
mutations compare the counter and write its next value in the same transaction. Plan-only progress
does not consume this counter.

### 2.2 Leader guard

Every authoritative etcd transaction includes:

```text
leader key value == encoded exact LeaderRecord
AND term key value == LeaderGuard.term
AND operation-specific record predicates
AND state-revision/cardinality predicates when applicable
```

The leader key comparison is mandatory because the durable term remains unchanged between lease
expiry and the next campaign. The in-memory implementation checks that the same leader record is
still attached to an active lease while holding its state mutex.

Guard failure is `StorageError::LeadershipLost`. A domain conflict remains `CompareFailed` (or a
more specific conflict variant). Backend transport/deadline/authentication failures must not be
misreported as either one.

### 2.3 Named domain transactions

The Coordinator store exposes reads plus named commits, not raw `Predicate`/`TxnOp` values. The
minimum mutation vocabulary is:

```text
create/update/remove_member
create/update/delete_plan
transition_slot
allocate_initial
reserve_move
fence_authority
install_authority
adopt_authority
complete_move
apply_admin_operation
compact_terminal_records
```

Exact Rust names may differ to keep modules cohesive, but no call site may reconstruct these
multi-record invariants from independent public writes.

`transition_slot` is deliberately narrow: it may perform audited, single-record phase changes such
as `BeginHandoff -> Stopping` and recording `StopFailed`. It must reject creating `Allocating` or
`Running`, changing owner/generation, changing `active_move`, or any transition whose invariant also
involves a claim or plan; those changes use the named multi-record transactions above.

### 2.4 Authority commit rules

- `allocate_initial`: absent slot + absent claim -> generation-1 `Allocating` slot + matching leased
  claim.
- `reserve_move`: plan `Pending -> Handoff` + slot `Running -> BeginHandoff` + next state revision.
- `fence_authority`: exact old claim (or proven absence after external expiry) is deleted while the
  slot enters `Fenced`.
- `install_authority`: `Fenced` slot + absent claim -> next-generation `Allocating` slot + matching
  target claim on a pre-granted lease.
- `adopt_authority`: matching older-term slot/claim -> same owner and generation under the new term,
  the next grant sequence, and a new leader-managed lease.
- `complete_move`: `Allocating -> Running` + plan movement `Handoff -> Completed` + cleared active
  move.

Lease grant/revoke calls remain outside etcd key transactions. Grant first, commit the claim with the
lease, and revoke the unused lease after a failed/unknown commit once reconciliation proves it is not
attached. Old-lease revoke after claim deletion is best-effort cleanup.

### 2.5 Recovery semantics

No Coordinator commit may create `Allocating` or `Running` without the exact owner/generation claim.
Lease expiry may remove a claim asynchronously; that creates a recovery obligation, not permission
to grant a different owner immediately.

Election reconciliation runs before mutation traffic and then periodically with bounded work. It
adopts compatible claims, resumes persisted phases, and records effects that require a later member
session. Member registration resends pending effects but is not the only repair trigger.

### 2.6 Compatibility boundary

- Set `STORAGE_SCHEMA_GENERATION` to 4 when new records/metadata are written.
- Set `PLACEMENT_CONTROL_GENERATION` to 4 when `StateVersion` enters the wire payload.
- Bump configured/fixture Coordinator protocol generations that represent this hard-switch contract.
- Generation 3 and 4 do not communicate and do not write the same live prefix.
- Service startup never performs automatic storage migration.

---

## 3. Code Surface and Ownership

| Area | Primary files | Required responsibility |
|---|---|---|
| Domain types | `crates/lattice-placement/src/types.rs`, `plan.rs`, `coordinator.rs` | `StateVersion`, `PlanRevision`, record validation, term-aware snapshot/barrier types |
| Control wire | `crates/lattice-placement/src/control.rs`, `session.rs` | generation 4 encoding, new-term snapshot requirement, term-aware acks |
| Store contract | `crates/lattice-placement/src/storage.rs` | read APIs, named commit requests/results, in-memory atomic implementation |
| etcd backend | `crates/lattice-placement/src/storage/etcd/` | exact leader predicates, multi-key transactions, pagination, counters, migration primitives |
| Coordinator runtime | `crates/lattice-placement/src/runtime/*.rs` | use only guarded domain commits, lease cleanup, recovery, durable admin state |
| Authority/handoff reducers | `authority.rs`, `handoff.rs` | `StateVersion` checks and forward-only recovery effects |
| Service lifecycle | `crates/lattice-service/src/cluster/*.rs`, `builder.rs` | new-term fresh sessions, generation hard switch, retryable incarnation state |
| Operations | `crates/lattice-ops/src/admin.rs`, new offline migration binary | durable operation semantics and explicit migration command |
| Failpoints | `crates/lattice-core/src/failpoint.rs` | post-atomic-commit/pre-effect and leadership-loss boundaries |
| Simulation | `crates/lattice-sim/src/{store,scenario,explorer}.rs` | atomic transaction model, invariants, liveness, replay |
| Real acceptance | `crates/lattice-placement/tests/etcd_acceptance.rs`, `tests/distributed/`, `lattice-sim testctl` | real etcd, process failover, chaos, artifacts |

Split `storage/etcd.rs` before it exceeds the repository file limit. Prefer a small `etcd/mod.rs` plus
focused transaction, pagination/codec, and migration modules. Do not create a generic abstraction
layer that obscures the exact predicates being tested.

---

## 4. Batch A — Guarded Domain Store Contracts

Batch A establishes the storage vocabulary and implementations. It is not a permitted stopping
point if runtime callers are temporarily broken; continue directly through Batch B.

### 4.1 Types and errors

- [x] Add validated `PlanRevision`; stop using node-visible `Revision` for plan record CAS.
- [x] Add `LeaderGuard` around an exact `LeaderRecord` and helpers needed by memory/etcd stores.
- [x] Add guarded commit request/result types without exposing etcd `Compare` or `TxnOp` publicly.
- [x] Add `StorageError::LeadershipLost` and useful backend error categories.
- [x] Define unknown-outcome handling: callers reconcile by re-reading the domain state; they never
  blindly repeat a lease/resource side effect.
- [x] Add validation that a claim written by an authority commit exactly matches slot key, owner,
  generation, term, and positive lease ID/TTL.

### 4.2 Store API

- [x] Separate read access needed by allocation/inspection from mutation access owned by the elected
  Coordinator.
- [x] Add leader-guarded single-record member/slot/plan commits.
- [x] Add atomic `allocate_initial`, `reserve_move`, `fence_authority`, `install_authority`,
  `adopt_authority`, and `complete_move` operations.
- [x] Include the node-visible revision counter in every member/slot transaction.
- [x] Make old unguarded `compare_and_put_*`, claim mutation, and member mutation methods private to
  backend implementation or delete them; they must not remain callable by Coordinator runtime/tests.
- [x] Return sufficient committed state from domain operations so the runtime publishes exactly what
  storage committed rather than reconstructing it from mutable local state.

### 4.3 In-memory implementation

- [x] Check the exact active leader and all record predicates under one mutex.
- [x] Apply every related write/delete/counter update only after every predicate validates.
- [x] Model leases with enough state to test leader loss, claim attachment, revoke, and expiry.
- [x] Enforce that failed transactions leave byte-for-byte-equivalent logical state.

### 4.4 etcd implementation

- [x] Split the existing backend into focused modules before adding substantial transaction code.
- [x] Compare exact leader value and exact term in every mutation `Txn`.
- [x] Compare all involved key revisions/versions and state counter in the same `when` branch.
- [x] Put/delete all related records and counters in one `then` branch.
- [x] Diagnose a false predicate from the transaction's consistent else/read result so leadership
  loss is distinguishable from a domain conflict.
- [x] Preserve transport, deadline, authentication, codec, and capacity errors instead of mapping
  every etcd client error to `Unavailable`.
- [x] Return claim lease metadata from reads needed by adoption/reconciliation without exposing raw
  etcd responses above the store layer.

### 4.5 Contract tests

Run one shared contract suite against memory and real etcd:

- [x] Deleting/expiring the leader key causes every mutation family to return `LeadershipLost` even
  before the term key changes.
- [x] After term T+1 campaigns, every term-T mutation fails and changes no record.
- [x] A record conflict with the same valid leader returns conflict, not `LeadershipLost`.
- [x] Failure of any predicate writes none of slot, claim, plan, member, or revision counter.
- [x] Initial allocation either creates slot+claim or creates neither.
- [x] Move reservation/completion either updates slot+plan or updates neither.
- [x] Fence/install/adopt commits preserve owner/generation/term/sequence invariants.
- [x] Pre-granted lease cleanup does not revoke a lease that won and became attached after an unknown
  RPC outcome.
- [x] Counter exhaustion returns a stable typed error without partial writes.

### 4.6 Batch A evidence gate

Batch A may leave runtime call sites intentionally uncompilable. Record:

- implemented domain operations;
- the list of remaining old runtime mutation call sites;
- memory contract-test results;
- real-etcd test status or the exact Docker command queued for Batch B's coherent boundary.

Do not commit or stop solely to add wrappers around the old API.

---

## 5. Batch B — Atomic Runtime Placement Transitions

Batch B converts every Coordinator writer and completes the first coherent hard-switch boundary.

### 5.1 Leadership lifecycle

- [x] Construct one `LeaderGuard` from the successful campaign result and pass it to every mutation.
- [x] Treat `LeadershipLost` from any store operation as terminal for that leader runtime.
- [x] Ensure a continuously ready control/operation queue cannot indefinitely starve leader lease
  renewal; leadership-loss/renewal branches take priority over mutation work.
- [x] Stop acknowledging a mutating control/admin command as successful unless its guarded commit
  succeeded.
- [x] Keep storage predicates as the final authority even when an in-memory leadership signal says
  the node is leader.

### 5.2 Initial allocation

- [x] Validate `shard_id < EntityConfig.shard_count` before reading or constructing a durable key.
- [x] Pre-grant the claim lease, call `allocate_initial`, and revoke the lease if the compare loses.
- [x] Publish the committed slot and send `ClaimGranted` only after the atomic commit.
- [x] On post-commit send failure, retain/recover the committed claim for replay rather than creating
  another generation or lease blindly.
- [x] Apply the same path to Singleton initial authority.

### 5.3 Handoff and plans

- [x] Replace plan-first then slot-CAS reservation with atomic `reserve_move` while preserving the
  forward-only write-ahead meaning.
- [x] Make all intermediate slot transitions leader-guarded.
- [x] Atomically delete/prove absence of the old claim while entering `Fenced`.
- [x] Pre-grant and atomically install the target slot+claim from `Fenced`.
- [x] Atomically clear `active_move`, make the slot `Running`, and complete the plan movement.
- [x] Keep plan compaction revision-conditional and leader-guarded.
- [x] Ensure higher-priority preemption can cancel only pending movements and commits all related
  reservation changes atomically.

### 5.4 Membership

- [x] Convert member create/update/remove and dynamic hello persistence to guarded commits with the
  shared state revision counter.
- [x] Ensure force removal cannot delete a replacement incarnation.
- [x] Keep member lease grant/revoke cleanup safe under compare failure and unknown outcome.
- [x] Preserve exact-incarnation session authorization on all control events.

### 5.5 Runtime state and effects

- [x] Update in-memory maps only from successful committed results.
- [x] Re-read and reconcile after an unknown outcome before retrying or responding.
- [x] Remove failpoints whose only purpose was to split records now committed atomically.
- [x] Add/retain failpoints after atomic commit but before delta, grant, drain, ready, or admin reply.
- [x] Verify control replay remains bounded and idempotent for every post-commit effect.

### 5.6 Focused tests and first commit gate

- [x] Unit-test initial shard and Singleton allocation under commit/send failure.
- [x] Unit-test every persisted handoff phase and stale expected record.
- [x] Unit-test stale leader writes through membership, allocation, rebalance, and admin entry points.
- [x] Unit-test a saturated control queue while renewal is due; the leader must renew or terminate,
  never continue mutating after lease loss.
- [x] Run `cargo test -p lattice-placement`.
- [x] Run `cargo test -p lattice-service -p lattice-ops`.
- [x] Run focused clippy with `-D warnings`.
- [x] Run the real-etcd contract suite through the Docker quality/HA environment.
- [x] Confirm `rg` finds no Coordinator call to an unguarded store mutation method.

Expected coherent commit:

```text
fix(placement): fence coordinator storage transitions
```

---

## 6. Batch C — Term-Qualified State and Bounded Reconciliation

### 6.1 StateVersion hard switch

- [x] Add `StateVersion` and migrate the persisted/runtime fields listed in Section 2.1.
- [x] Bump `PLACEMENT_CONTROL_GENERATION` from 3 to 4 with no decoder fallback.
- [x] Bump `STORAGE_SCHEMA_GENERATION` from 3 to 4 when the new record layout is enabled.
- [x] Update JSON/wire fixtures and protocol-generation configuration used by Coordinator bootstrap.
- [x] Require a new snapshot whenever a session observes a higher Coordinator term.
- [x] Reject lower-term snapshots, deltas, member events, drain commands, and acks.
- [x] Make barrier satisfaction require exact term plus sufficient revision.
- [x] Keep `PlanRevision` separate from `StateVersion` in types, storage, inspection, and tests.

### 6.2 Election reconciliation

- [x] Add a read-only bounded inventory phase immediately after campaign and before mutation traffic.
- [x] Adopt matching older-term slot/claim authority into the new term using a pre-granted lease and
  one atomic `adopt_authority` commit; owner and generation do not change.
- [x] Rebuild the new leader's claim-lease map from committed adoption results.
- [x] Resume `BeginHandoff`, `Stopping`, `Fenced`, and `Allocating` from persisted slot/plan/claim
  state without requiring a target disconnect.
- [x] Treat a missing claim on `Running` as fenced recovery work; do not choose another owner until
  prior authority invalidity is proven.
- [x] Record effects requiring a member session and replay them when that exact member becomes `Up`.
- [x] Block automatic rebalance until initial structural reconciliation is complete and required load
  samples for the new term are fresh.

### 6.3 Periodic bounded reconciliation

- [x] Add validated config for reconciliation cadence, page size, and maximum work per pass.
- [x] Add paged/indexed store reads for members, slots by phase, plans, and claims.
- [x] Run a jittered reconciler from the leader lifecycle without starving renewal/control.
- [x] Repair/resend idempotently: adopt claim, drain, install target, resend grant, finalize plan, or
  quarantine an impossible relationship.
- [x] Maintain a cursor/backlog so a large but valid store is processed over bounded passes.
- [x] Expose backlog count, oldest pending age, last successful pass, and quarantined-record details.
- [x] Trigger focused reconciliation after unknown commit outcome or reliable-control gap.

### 6.4 Member supersession

- [x] Replace ambiguous `StaleMember` for an unknown but still-leased predecessor with a retryable
  `IncarnationPending` domain/control result.
- [x] Include old incarnation and remaining lease TTL when the backend can provide it.
- [x] Keep graceful supersession/force-remove as the only acceleration paths without lease absence.
- [x] Ensure retry/backoff is bounded and observable in service lifecycle logs/metrics.

### 6.5 Tests and second commit gate

- [x] A term-T ack cannot satisfy a term-T+1 handoff barrier at the same numeric revision.
- [x] A new-term delta before snapshot is rejected and makes the session unready.
- [x] Election adopts matching claims without changing owner/generation.
- [x] Crash after authority commit but before `ClaimGranted` self-heals without a fresh allocation.
- [x] Legacy `Allocating + no claim` is repaired or quarantined deterministically.
- [x] Reconciliation work and memory remain within configured page/pass bounds.
- [x] A new incarnation receives retryable pending until old lease absence or force removal.
- [x] Run placement, service, simulation reducer, and real-etcd focused suites.

Expected commit:

```text
feat(placement): reconcile term-qualified coordinator state
```

---

## 7. Batch D — Durable Operations, Bounds, Migration, and Operability

### 7.1 Durable admin settings and idempotency

- [x] Add a durable automatic-balance settings record with global and per-entity pause state.
- [x] Add bounded admin operation records containing ID, fingerprint, status, typed result,
  Coordinator term/state version, and retention metadata.
- [x] Atomically persist pause/resume setting plus operation result.
- [x] Atomically persist manual-relocation plan creation plus the operation's original plan ID.
- [x] Persist force-remove idempotency with exact expected incarnation.
- [x] Make evaluate-now and cancel-pending consume and persist the operation IDs already present in
  the HTTP command instead of ignoring them.
- [x] On retry, return the original typed result for an equal fingerprint and conflict for a
  different fingerprint.
- [x] Load durable settings on election before automatic planning can run.
- [x] Define count/age retention; compact terminal operation records with a guarded transaction.
- [x] Document that idempotency is guaranteed for the configured retention window.

### 7.2 Durable key bounds

- [x] Rename/separate etcd list page size from total record limits in configuration.
- [x] Add explicit maxima for total slots, active/retained plans, global entity/singleton configs,
  admin operation records, and reconciliation pages/work.
- [x] Persist the cluster-wide cardinality limits (or a canonical fingerprint) as schema metadata;
  startup/election must reject a different local limit set instead of silently changing cluster
  capacity after failover.
- [x] Enforce global distinct configuration bounds, not only per-`NodeHello` vector bounds.
- [x] Add durable slot/plan/admin counters or equivalent bounded indexes.
- [x] Compare and update each counter in the same transaction as key create/delete.
- [x] Reject new keys at capacity while allowing updates/deletes needed for recovery.
- [x] Paginate inspection, election recovery, and migration within total configured bounds.
- [x] Add a diagnostic repair mode for counter/index mismatch; normal startup must not guess.

### 7.3 Offline generation-3-to-4 migration

Add an explicit command, preferably
`crates/lattice-ops/src/bin/lattice-placement-migrate.rs`, backed by migration primitives in
`lattice-placement`.

- [x] Support `inspect`, `dry-run`, `apply`, and `resume` modes with endpoints/prefix supplied through
  validated arguments/config; never log credentials.
- [x] Require the lease-backed leader key to be absent.
- [x] Atomically change schema 3 to a `migrating-to-4` marker before the first write so old/new
  services both refuse startup.
- [x] Acquire a lease-backed migration lock and make every page idempotently resumable.
- [x] Write a user-selected backup/export before apply; use restrictive file permissions where the
  platform supports them.
- [x] Require the generation-4 cardinality limits as explicit migration input and initialize their
  durable metadata together with the counters/indexes.
- [x] Initialize the state revision from `max(member, slot, 1)`. If records exist, use the persisted
  current term as their migration baseline; for an empty store, let the first generation-4 election
  establish the term. The first generation-4 leader still requires fresh snapshots.
- [x] Convert plan record revision/base-version fields, initialize durable settings, and build
  cardinality indexes/counters.
- [x] Validate/quarantine legacy transitional slot/claim/plan combinations before finalization.
- [x] Atomically finalize the schema marker to 4 only after full verification.
- [x] On interruption, leave a recognizable resumable marker; never silently roll forward on service
  startup.
- [x] Provide an operations runbook with stop, backup, dry-run, apply/resume, verify, and rollback
  decision points.

### 7.4 Observability and error handling

- [x] Emit leadership-loss count and operation family, without high-cardinality record IDs.
- [x] Emit commit conflict, unknown outcome, reconcile backlog/age, quarantine, capacity, and
  migration progress metrics.
- [x] Preserve useful etcd authentication/argument/deadline/transport categories in errors/logs.
- [x] Surface bounded per-connection task failure metrics/logs from remoting `accept_loop`.
- [x] Keep logs rate-limited/aggregated for hostile or repeatedly malformed peers.
- [x] Add inspection fields for current version, reconcile status, durable pause state, limits, and
  retained admin-operation count.

### 7.5 Tests and third commit gate

- [x] Pause state survives leader failover and remains visible through inspection.
- [x] Retrying relocation returns the original plan ID after failover.
- [x] Reusing an operation ID with another fingerprint conflicts after failover.
- [x] Evaluate/cancel operation IDs are no longer ignored.
- [x] Invalid/out-of-range shard requests create no key.
- [x] Capacity is exact at limit under retry/unknown outcome and recovers after guarded deletion.
- [x] A successor configured with different durable limits rejects startup/election before planning.
- [x] Migration dry-run changes nothing; apply converts a generation-3 fixture; interruption resumes;
  active leader/malformed/over-capacity inputs fail safely.
- [x] New generation-4 services reject generation 3 and `migrating-to-4`; old generation-3 fixtures
  cannot decode generation-4 placement control.

Expected commit:

```text
feat(ops): persist coordinator controls and bound placement storage
```

---

## 8. Batch E — Simulation and Distributed Acceptance

### 8.1 Failpoint matrix

Update `Failpoint::ALL`, name mapping, scenario coverage, and machine-checked expectations.

Required boundaries:

- [x] leader lease lost immediately before each mutation family;
- [x] initial authority commit after slot+claim commit and before delta/grant;
- [x] move reservation commit before invalidating delta;
- [x] fence commit before fenced delta;
- [x] target installation commit before delta/grant;
- [x] active slot+plan completion commit before active delta;
- [x] admin operation commit before response;
- [x] reconciliation page commit/effect boundary;
- [x] migration page commit before cursor/progress update.

Remove or rename failpoints that imply a crash can occur between records now in one etcd transaction.

### 8.2 Simulator/model invariants

- [x] Model named transactions as all-or-nothing with leader, record, counter, and lease predicates.
- [x] Assert a committed mutation had an exact valid leader guard at its linearization point.
- [x] Assert no Coordinator commit creates `Allocating/Running` without a matching claim.
- [x] Assert `active_move` and nonterminal plan movement agree after every Coordinator commit.
- [x] Allow external lease expiry to create a recovery obligation; under stable conditions require it
  to reach `Running` or a visible terminal/quarantine state within bounded passes.
- [x] Assert barrier acks match the exact term.
- [x] Assert cardinality never exceeds configured maxima.
- [x] Explore stale leader, successor campaign, compare conflict, unknown result, process pause,
  claim expiry, target disconnect, and migration interruption schedules.
- [x] Retain deterministic seeds/traces and prove replay reproduces failures/outcomes.

### 8.3 Real etcd acceptance

- [x] Expand `etcd_acceptance.rs` to execute the shared transaction contract against one-member and
  three-member etcd.
- [x] Pause/expire the leader lease while keeping the old process alive; a successor campaigns and
  every old-leader write is rejected.
- [x] Kill at every post-commit failpoint and prove election reconciliation resumes without duplicate
  owner/generation or an orphan active move.
- [x] Verify claim adoption moves the lease/term without changing owner/generation.
- [x] Verify counters/indexes remain correct through create/delete/retry/compaction.
- [x] Verify migration apply/resume on disposable real etcd prefixes.

### 8.4 Multi-process and chaos scenarios

- [x] Initial allocation commit followed by Coordinator death still reaches one `Running` owner.
- [x] Handoff installation commit followed by Coordinator death resends the grant and completes.
- [x] Saturated control/admin input cannot starve leadership renewal indefinitely.
- [x] Old-term delayed deltas/acks cannot advance new-term sessions/barriers.
- [x] Durable pause/idempotency survives Coordinator failover.
- [x] Out-of-domain shard request and capacity exhaustion remain bounded and recoverable.
- [x] Member restart during failover observes retryable incarnation pending, then joins after proof.
- [x] Chaos pause/partition/restart schedules preserve single authority and bounded recovery.

### 8.5 Regression properties for rejected findings

- [x] Redirected leader established dial still rejects a certificate for another identity/incarnation.
- [x] Only node/shard load reports use the ephemeral Coordinator-event path, and stale sequences are
  ignored.
- [x] Association epoch remains stable across lane reconnect and changes for a new association/
  incarnation.

Do not turn these tests into implementation work for the rejected original claims.

### 8.6 Full acceptance commands

Record exact command, date, seed/run ID, result, and artifact path for:

```text
sh scripts/check-structure.sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-targets --all-features

sh scripts/test-docker.sh quality
sh scripts/test-docker.sh sim
sh scripts/test-docker.sh model
sh scripts/test-docker.sh e2e
sh scripts/test-docker.sh e2e-ha-etcd
sh scripts/test-docker.sh chaos
sh scripts/test-docker.sh k8s
sh scripts/test-docker.sh soak --duration 15m --seed <recorded-seed>
sh scripts/test-docker.sh replay --artifact <recorded-trace>
```

The Docker wrapper owns scoped cleanup. A run is not passing if labeled containers, networks,
volumes, or test images leak after success or failure.

### 8.7 Final commit

```text
test(distributed): verify coordinator failover invariants
```

---

## 9. Acceptance Traceability

| Requirement | Minimum executable evidence |
|---|---|
| F1 stale-leader fence | shared memory/etcd contract plus live paused-old-leader/successor process test |
| F2 slot+claim atomicity | predicate-failure atomicity test plus post-commit/pre-send failover scenario |
| F3 slot+plan atomicity | reserve/complete transaction tests plus every-phase recovery scenario |
| Term-qualified revisions | unit/session tests plus delayed old-term delta/ack multi-process scenario |
| Election reconciliation | phase table tests, bounded simulator liveness, and real failover without manual repair |
| F4 durable admin semantics | real-etcd failover retry tests for pause, relocation, evaluate, cancel, force-remove |
| F5 key/storage bounds | shard-range tests, exact-limit concurrent/retry tests, paged recovery at maximum size |
| F6 incarnation waiting | memory/etcd lease tests plus service retry/force-remove scenario |
| Migration | generation-3 fixture dry-run/apply/resume/reject/backup tests on real etcd |
| Operability | inspection assertions, bounded metrics/log assertions, artifact capture |
| Rejected findings preserved | focused mTLS redirect, ephemeral telemetry, and association reconnect tests |

No review requirement is complete only because its unit test passes against the in-memory store.
Leader fencing, transaction atomicity, lease behavior, and migration require real etcd evidence.

---

## 10. Final Completion Criteria

The goal is complete only when all of the following are true:

- [x] Batches A-E are checked complete with dated evidence in the Current Execution Pointer.
- [x] All runtime Coordinator writes use leader-guarded named store commits.
- [x] No public/internal Coordinator path can call the old unguarded single-key mutations.
- [x] Storage generation 4 and placement-control generation 4 are enforced without fallback.
- [x] Initial allocation, handoff install, leader adoption, move reservation, and completion are
  atomic at the required record boundary.
- [x] New-term snapshot/delta/barrier semantics use `StateVersion`; plan revision is a distinct type.
- [x] Election and periodic reconciliation are bounded, observable, and proven live under stable
  conditions.
- [x] Automatic-balance settings and all mutating admin operation IDs/results are durable and bounded.
- [x] Shard key domain, total durable cardinality, list pagination, and repair behavior are enforced.
- [x] Incarnation waiting is safe, retryable, and observable.
- [x] The offline migration tool and runbook safely handle dry-run, apply, interruption/resume, and
  rejection cases.
- [x] Structure, fmt, clippy, workspace tests, Docker quality/sim/model/e2e/HA/chaos/k8s, soak, and
  replay all pass with retained artifacts.
- [x] Architecture review, architecture placement/testing docs, operations runbook, and public config
  documentation match the implemented behavior.
- [x] No Rust file exceeds 1200 physical lines without a documented and reviewed reason.
- [x] `git diff --check` passes and the final worktree contains no accidental generated artifacts or
  leaked test resources.

---

## 11. Tracker Update Protocol

After each batch:

1. Change completed `[ ]` items to `[x]` only when code and required evidence exist.
2. Add newly discovered work under the earliest affected batch; do not hide it in prose.
3. Update the Current Execution Pointer with status, broken frontier, next action, and dated commands.
4. Record any architecture decision change in this plan and `architecture-review.md` together.
5. Record focused test results and artifact paths; avoid pasting large logs into this document.
6. Do not mark the overall goal complete until Section 10 and the full acceptance matrix pass.

Suggested commit grouping:

1. Batches A+B — `fix(placement): fence coordinator storage transitions`
2. Batch C — `feat(placement): reconcile term-qualified coordinator state`
3. Batch D — `feat(ops): persist coordinator controls and bound placement storage`
4. Batch E — `test(distributed): verify coordinator failover invariants`

An extra commit is acceptable only for a genuinely independent generated-wire or migration-tool
boundary. Do not create per-checkbox commits or compatibility commits whose only purpose is to keep
obsolete APIs compiling.
