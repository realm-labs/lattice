# lattice Code Review: Ongoing Findings

Review opened: 2026-09-17. Initial code baseline: `ebf74dfaf9721bc41a9a1da54c1ff064802207e6` (`fix(actor): reject asks after authority fence`).

Status: **ongoing review**. This document is intended to collect additional findings and decisions as the review continues. Unless a finding explicitly records remediation, it describes an open issue or design question and does not imply that an implementation change has been approved.

## 1. Review Conventions

Evidence levels:

- **Reproduced**: a controlled test or minimal program exhibited the behavior.
- **Source-confirmed**: the relevant production path and state transitions were inspected, but the complete fault scenario was not executed.
- **Design risk**: the implementation permits a risk whose real impact depends on application behavior or an unresolved contract.
- **Design recommendation**: a maintainability or architecture proposal rather than a demonstrated defect.

Priorities:

- **P1**: ownership, data integrity, security, or cluster-wide safety.
- **P2**: correctness, availability, or recovery failure under specific conditions.
- **P3**: maintainability, diagnostics, API clarity, or lower-impact contract drift.

Finding IDs remain stable after being added. A resolved finding stays in the document with its remediation commit, validation evidence, and any remaining limitations.

## 2. Findings Overview

| ID | Priority | Finding | Evidence | Status |
|---|---|---|---|---|
| R1 | P1 | Authority loss enters ordinary `stopping`, while end-to-end fencing is not mandatory | Source-confirmed design risk | Decision deferred |

## 3. Findings

### R1: Authority Loss Enters Ordinary Stopping Without Mandatory End-to-End Fencing

Locations: [handle.rs:292](../crates/lattice-actor/src/handle.rs#L292), [runtime.rs:698](../crates/lattice-actor/src/runtime.rs#L698), [runtime.rs:744](../crates/lattice-actor/src/runtime.rs#L744), [runtime.rs:945](../crates/lattice-actor/src/runtime.rs#L945), [quarantine.rs:168](../crates/lattice-actor-distributed/src/registry/quarantine.rs#L168), [router.rs:114](../crates/lattice-service/src/cluster/router.rs#L114), [coordinator.rs:161](../crates/lattice-store-mongodb/src/persistence/coordinator.rs#L161), [write.rs:44](../crates/lattice-store-mongodb/src/store/write.rs#L44).

#### Current behavior

When the distributed Registry revokes an activation's authority, it first calls `fence_business_admission`. The local fence is irreversible, rejects new normal-lane asks and tells, wakes an idle runtime without requiring mailbox capacity, and prevents prefetched business work from executing.

The runtime translates that fence into `StopReason::Requested` and later invokes the ordinary application-defined `Actor::stopping` callback. It does not distinguish a graceful requested stop, during which the Actor may still have authority to persist final state, from authority loss, during which another activation may already hold a newer generation.

`stopping` is user code and may persist state, publish events, call external services, release distributed resources, or perform only local cleanup. The runtime cannot currently distinguish or restrict those effects.

#### Existing protection

Placement's `assignment_generation` is exposed as an `ActorFencingToken` and is carried by `ActorCreateContext`. Registry activation publication revalidates the generation around asynchronous loading.

The MongoDB persistence layer also implements storage-level fencing:

- `MongoPersistenceCoordinator::for_actor` derives an activation epoch from the Actor fencing token.
- Coordinated writes filter atomically on document identity, expected version, and `_lattice_epoch`, then stamp the writer's epoch.
- `fence_document` and `fence_documents` can eagerly claim storage ownership.
- A strictly newer stored epoch produces `ActivationFenced` and cannot be resolved by reloading and retrying from the stale activation.

These mechanisms protect coordinated MongoDB writes once wired into an Actor's persistence path. They do not automatically fence arbitrary HTTP calls, messaging, email, payment operations, direct-store writes, or application-defined stopping logic.

#### Gap

The final receiver of a mutation must validate the token atomically with the mutation. A local check followed by an external write has a race:

```text
old activation validates T1
authority moves to T2
old activation performs the external write with stale authority
```

A replacement activation also needs an activation barrier:

```text
authority atomically advances T1 -> T2
replacement receives T2
replacement claims the storage/resource with T2
replacement loads or revalidates state after the claim
replacement is published and opens business admission
old operations carrying T1 are rejected by the resource
```

If the resource still records T1, an old T1 operation may remain admissible until T2 is stamped. Lazy stamping on the replacement's first business write leaves that interval open. A strong contract requires eager claiming before publication, or an atomic check against an authoritative ownership record on every mutation.

At the reviewed baseline, workspace searches found no production loader that is required to execute the MongoDB eager-claim API before returning its Actor. The MongoDB primitives therefore provide a capability, not a workspace-wide activation guarantee.

For external systems that cannot validate fencing tokens, a transactional outbox plus stable idempotency keys can contain duplicate delivery. It does not make a non-idempotent external operation exactly-once; a downstream idempotency contract or compensation remains necessary.

#### Design options

1. **Fail closed after authority loss.** Add a distinct authority-loss lifecycle path, skip ordinary persistence-aware `stopping`, and retain the activation in a fenced/quarantined state until reconciliation or explicit discard.
2. **Require fenced stopping.** Continue invoking a distinct `AuthorityLost` stopping path, but require every external effect to pass through a token-enforcing adapter. This preserves finalization opportunities but cannot be enforced for arbitrary user code.
3. **Separate finalization from local teardown.** Run authority-sensitive finalization only while ownership remains valid and define a local-only cleanup hook for authority loss.

No option is selected in this review.

#### Open decisions

1. Should `StopReason` gain `AuthorityLost`?
2. Should lifecycle gain a distinct `Fenced` or `Quarantined` state?
3. Should authority loss skip `Actor::stopping`, invoke a restricted hook, or retain the current behavior with a stronger contract?
4. Is the fencing token scoped per shard, entity, singleton, or storage aggregate?
5. Must storage be claimed before Actor construction, before `started`, or before Registry publication?
6. Should MongoDB expose a raw activation epoch, or provide a mandatory `claim_and_load(ActorCreateContext, ...)` API that keeps it private?
7. How are absent documents and multi-document aggregates claimed without partial ownership or stale-read windows?
8. Which direct or unfenced persistence APIs must be prohibited for placement-managed state?
9. What is the policy for external systems that cannot enforce tokens?
10. When authority infrastructure is unavailable, must replacement activation fail closed?

#### Acceptance criteria for a future implementation

- Authority advances from T1 to T2 while the old Actor is paused; after it resumes, every T1 persistence mutation is rejected.
- A T2 Actor cannot receive business traffic before its storage claim succeeds.
- State loaded by T2 is no older than the state protected by its successful claim.
- Stale stopping/finalization code cannot mutate fenced storage.
- A long process pause followed by resume cannot restore old authority.
- Missing authority prevents replacement admission.
- Multi-document partial claims cannot produce an Actor serving a mixed-generation aggregate.
- Direct or unfenced persistence cannot silently target coordinator-owned state.
- Graceful handoff can still persist and transfer state without being forced through the authority-loss path.

#### Reference behavior

Akka Cluster does not create a replacement solely because a node is unreachable. Singleton and sharded entities wait for a downing/removal decision; Split Brain Resolver adds a removal margin, and an optional per-shard lease prevents initialization without external ownership. See the official [Akka downing documentation](https://doc.akka.io/libraries/akka-core/current/typed/cluster.html#downing), [Split Brain Resolver lifecycle](https://doc.akka.io/libraries/akka-core/current/split-brain-resolver.html#cluster-singleton-and-cluster-sharding), and [Cluster Sharding lease](https://doc.akka.io/libraries/akka-core/current/typed/cluster-sharding.html#lease).

Orleans makes the consistency trade-off explicit: its default eventually consistent directory may admit occasional duplicate activations during instability, while its stronger directory coordinates versioned ownership to prevent them. See the official [Orleans grain directory documentation](https://learn.microsoft.com/en-us/dotnet/orleans/host/grain-directory).

#### Status

Decision deferred. No runtime, Registry, placement, or persistence behavior has been changed for R1.

## 4. Cross-Cutting Observations

No cross-cutting observations have been recorded yet. Add entries here when several findings share one lifecycle, ownership, resource-management, testing, or API-design cause.

## 5. Decision Log

| Date | Finding | Decision | Rationale |
|---|---|---|---|
| 2026-09-17 | R1 | Defer implementation | The authority-loss lifecycle and storage activation barrier require an explicit consistency/availability decision before code changes |

## 6. Validation Record

This document-only review did not run tests or modify production code. R1 is source-confirmed at the listed call paths; a complete network-partition reproduction with a stale external write has not been executed and must not be reported as reproduced.

When findings are added or remediated, append the exact commands, environment assumptions, passed/failed counts, and limitations here. Do not treat unexecuted CI jobs or historical results as current validation.

## 7. Future Review Queue

Add candidate topics here before promoting them into numbered findings. A candidate should become a finding only after its behavior, impact, evidence level, and relevant source locations are recorded.

- No additional candidates recorded yet.
