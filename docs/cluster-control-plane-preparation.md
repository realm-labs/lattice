# Cluster Control Plane: P0 Source Inventory and Protocol Evidence

> Source inspection: 2026-09-25. No runtime implementation, storage migration, or etcd cleanup was performed.
> Design and implementation checklist: [cluster control-plane memo](cluster-control-plane-memo.md), especially sections 7-9.
> This is supporting evidence, not a second decision backlog. Proposed replacements require the contracts tracked in memo section 9. No tests or workload measurements were run for this inspection.

Follow-up cleanup: the old application release model, admission guards, and
release-biased allocation have since been removed. This does not implement the
target framework identity, storage layout, or cluster shutdown protocol.

## 1. Scope and notation

The workspace currently has three direct `etcd-client` consumers: `lattice-placement`, `lattice-config-etcd`, and `lattice-id-etcd`. Discovery can use the config adapter indirectly. There is no single automatically shared namespace covering all three.

- `P` is `EtcdPlacementConfig.cluster_prefix`; `D` is the current placement-domain identifier, targeted to become an Actor group.
- `C` is the separately configured config-store prefix; `I` is the separately configured worker-ID prefix.
- `scope` below expands to `membership` or `domains/<D>` in the current schema, not the proposed `runs/<epoch>` layout.
- Sizes below describe structural growth, not measured bytes or a throughput benchmark. Existing collection/count limits are not a proof of the proposed serialized-value limit.
- “Persistent” means no etcd lease is attached by the inspected writer. Application-driven deletion may still occur. Lease expiry deletes attached records, not related persistent records or counters.

## 2. Current key-family inventory

### 2.1 Coordination store

Key construction and readers are in [storage/etcd.rs](../crates/lattice-placement/src/storage/etcd.rs); writes are in [transactions.rs](../crates/lattice-placement/src/storage/etcd/transactions.rs) and [transactions_placement.rs](../crates/lattice-placement/src/storage/etcd/transactions_placement.rs). The current schema generation is **6**. The target schema in the memo is not installed.

| Current key under `P/` | Writer / trigger and principal reader | Value growth / update pattern | Lifetime and crash leftovers | Target treatment |
|---|---|---|---|---|
| `schema_generation`, `schema/limits` | Store schema initialization; startup and migration validation | Small scalar / fixed limit fields; initialization or migration | Persistent; survive shutdown | Replace the compatibility generation with a framework marker; retain required limit/configuration metadata under an explicit upgrade contract |
| `<scope>/leader` | `campaign_leader_inner`; election, directory, transaction guards | One bounded identity/scope/term record per scope; election changes | Leader lease; disappears on expiry/revoke | Retain scoped leadership, bind to run and eligibility; no cached hint substitutes for it |
| `<scope>/term` | Election CAS; next-term calculation and all guarded commits | Scalar; advances on successful election | Persistent even when leader lease expires | Preserve monotonic safety history; do not reset during generic runtime cleanup |
| `<scope>/state_revision` | Guarded state changes; recovery, snapshots/deltas | Scalar; advances with relevant state commits | Persistent | Define run-scoped update ordering; distinguish it from etcd `mod_revision` and election term |
| `membership/counters/members`; `domains/<D>/counters/{members,slots,plans,admin_operations,entity_configs,singleton_configs}` | Cardinality transaction helpers; capacity admission and diagnostic repair | Fixed scalar set per scope; updates on create/delete | Persistent; a lease deleting a member does not decrement a counter atomically | Keep only counters needed for safe admission, classify by record lifetime, and retain reconciliation/repair semantics |
| `membership/members/<node-id>` | `create_member`, `update_member`, removal; membership recovery and assignment checks | Full `MemberRecord` embeds `MemberHello`; grows with roles, attributes, protocols, capabilities; lifecycle updates | Member lease; explicit removal also deletes it | Minimal leased identity/state; reconstruct detailed descriptions through validated sessions |
| `domains/<D>/members/<node-id>` | Domain join/update/remove; group recovery, eligibility and assignment checks | Full `DomainMemberRecord` embeds capacity, type sets, configs, constraints | **Persistent: current puts use no lease.** Removed through application reconciliation/removal | Minimal session-lifetime group participation; define lease ownership and global-member dependency before changing writes |
| `domains/<D>/entity_types/<type>`, `singleton_types/<kind>` | `put_entity_config` / `put_singleton_config`; join validation, resolution and allocation | One config per type/kind; constraints and names affect size | Persistent | Keep one authoritative definition, avoid copies in every participant; specify configuration ownership and mapping-change rules |
| `domains/<D>/shards/<type>/<shard-id>`, `singletons/<kind>` | Allocation, transition, reservation, fencing, installation and completion; routing/recovery | `PlacementSlot`, including embedded `barrier_sessions`; rewritten on transitions | Persistent, including after owner/claim loss | Run-scoped assignment facts; separate participant records without losing state/generation/transfer evidence |
| `domains/<D>/shard_claims/<type>/<shard-id>`, `singleton_claims/<kind>` | Allocate/install/adopt authority; renewal, reconciliation and handoff | `ClaimGrant`; changes on authority installation/adoption; keepalive is a lease operation rather than a slot rewrite | Claim lease; explicit guarded deletion/revoke in applicable paths | Keep lease-backed authority separate from durable assignment; bind to exact owner/run/generation |
| `domains/<D>/rebalances/<32-digit-hex-plan-id>` | Plan creation/update and move transactions; scheduling/recovery/admin queries | Full `RebalancePlan`, `moves` array and per-move `barrier_sessions`; whole record rewritten | Persistent; terminal history is explicitly compacted | Replace unstarted durable proposals with bounded in-memory decisions; retain admitted transfer intent and split barriers |
| `domains/<D>/admin/<hex-encoded-operation-id>` | Admin operations, including plan/settings transactions; retry deduplication | Bounded operation result/fingerprint plus creation/expiry fields; one record per retained operation | Persistent; expiry is an application field, not an etcd lease | Retain bounded retry evidence with explicit cleanup rules; do not remove before deciding the retry horizon |
| `domains/<D>/settings/automatic_balance` | `commit_automatic_settings`; startup/recovery and rebalance policy | Small settings record; administrator updates | Persistent | Retain as explicit persistent policy/configuration, not accidental per-run residue |

Current limits and retention are defined in [storage/domain.rs](../crates/lattice-placement/src/storage/domain.rs) and [runtime.rs](../crates/lattice-placement/src/runtime.rs). Runtime defaults include 64 moves per plan, 64 completed plans, and 1,024 admin operation records with a 24-hour retention interval. These are configurable, not measured capacity guarantees. Plan-history and admin cleanup are performed by runtime code; stopping the runtime does not execute an etcd TTL over these records.

### 2.2 Administrative and non-coordination records

| Current key | Writer / reader | Lifetime and safety purpose | Treatment |
|---|---|---|---|
| `P/migration/generation-4-to-5` | Existing migration tool, apply/resume/progress/finalization | Persistent migration marker; current tooling may leave historical completion evidence | Inventory before retiring old tools; an interrupted migration is not an ordinary clean runtime namespace |
| `P/migration/generation-4-to-5-lock` | Migration execution | Lease-backed exclusive-operation lock | Respect any active operation; do not blindly reuse this old migration as the new reset implementation |
| `P/diagnostics/cardinality-repair-lock` | Cardinality repair | Lease-backed repair exclusion | Keep administrative exclusion semantics when replacing tooling |
| `C/<application-key>` | `EtcdConfigStore` get/put/watch; applications and config-backed discovery | Arbitrary JSON, no lease on put; may include persistent business configuration or an endpoint-list document | Exclude from runtime reset unless an exact key has separately approved runtime ownership; do not delete a broad shared prefix |
| `I/<cluster>/slots/<worker-id>` | `EtcdWorkerIdLeaseStore` acquire/renew/release | Lease-backed worker allocation; exact owner/token protects release/renewal | Separate worker-ID lifecycle; cluster cleanup must not bypass its ownership protocol |
| `I/<cluster>/history/<worker-id>` | First acquisition and subsequent reuse checks | Persistent marker distinguishing `FirstUse` from `Reused`; written atomically with the first slot allocation | Preserve safety history; not disposable cluster-run data |

Sources: [migration.rs](../crates/lattice-placement/src/storage/etcd/migration.rs), [config client](../crates/lattice-config-etcd/src/client.rs), [config store](../crates/lattice-config-etcd/src/store.rs), [config-backed discovery](../crates/lattice-discovery/src/config_store.rs), and [worker-ID store](../crates/lattice-id-etcd/src/store.rs).

The migration code also recognizes legacy unscoped key families such as `coordinator/leader`, `members/`, `shards/`, and old cardinality keys. They are not current generation-6 runtime writes. Inspect/reset must identify unsupported or interrupted legacy state explicitly rather than assume any unknown key is safe to delete.

### 2.3 State that is already in memory, or not implemented

- Active Actor instances and their entity IDs live in distributed Registry/runtime state; there is no per-entity placement table in these etcd writers.
- `LoadTable` is in memory. Do not propose “moving load reports out of etcd” as a new optimization without a concrete persisted writer.
- Current `BootstrapView` caches leader information; `ConfigStoreDiscovery` reads an endpoint document, not a minimal election-record view.
- The proposed run epoch, global shutdown operation/progress, managed candidate eligibility, and separate transfer participant records are target additions, not current keys to clean up.

## 3. Storage invariants that the rewrite must preserve

The source inspection gives the following transaction boundaries, not just a list of payloads to shorten:

1. **Election:** `campaign_leader_inner` compares leader absence and the previous term record revision, then writes the next term and a leased leader atomically. Managed per-scope eligibility is not currently a predicate here; W2.2/W3.2 must add it together with revocation semantics.
2. **Leader fencing:** transaction `commit` appends comparisons for the exact serialized leader record and term. `ensure_guard_live` is only a preflight; it is not a replacement for those transaction comparisons.
3. **Owner eligibility:** `assignment_compares` verifies both global and domain membership are `Up` for the exact `NodeKey`, then compares both records' etcd modification revisions in the authority transaction. Minimizing membership payloads must preserve exact-incarnation and status checks at commit.
4. **Authority replacement:** `install_authority` requires the expected `Fenced` slot, next assignment generation, absent claim, exact eligible member records, state revision, and leader guard. The slot and leased claim are published in the same transaction. Claim absence alone is not the complete protocol proof; runtime handoff checks precede this transition.
5. **Bounded cardinality and ordering:** counter/state updates use CAS in the same transaction as relevant record changes. An expired member's counter correction has its own guarded path. Removing counters requires a substitute concurrency-safe capacity mechanism, not a prior count followed by an unguarded put.
6. **Transfer progress:** move reservations/completion currently update slot and plan together; splitting the plan cannot separate publication from recoverability. Participant evidence and safe interrupted construction must be designed before changing serialization.
7. **Admin retry evidence:** settings/plan mutations may publish their operation result in the same transaction. Cleanup must preserve the intended deduplication contract across timeouts and failover.

The current paged reader in [etcd/page.rs](../crates/lattice-placement/src/storage/etcd/page.rs) groups ranges within one transaction, but its cursor carries a key position rather than a fixed revision for the entire multi-page walk. Do not interpret a series of pages as a frozen shutdown-participant set or completion proof. Sealed manifests/version checks remain necessary in the target protocol. Serialized byte/page budgets also need separate implementation and worst-case tests.

## 4. Protocol evidence and required decisions

These findings support P0.2 in the memo. They do not approve new behavior or claim a reproduced production failure.

### 4.1 Serving grant lifetime

Current path:

```text
Coordinator renew loop
→ etcd claim lease keepalive
→ ClaimGranted via ephemeral control delivery
→ owner session validates term and installs grant
→ local deadline = installation time + TTL - safety margin
```

Evidence:

- [EtcdPlacementStore](../crates/lattice-placement/src/storage/etcd.rs) returns `Result<()>` from keepalive after a positive TTL response; it does not return a usable validity interval to the caller.
- [runtime/lifecycle.rs](../crates/lattice-placement/src/runtime/lifecycle.rs) renews claims and sends grants on successful renewal. The inspected claim loop awaits leases sequentially; throughput has not been measured.
- [runtime/membership.rs](../crates/lattice-placement/src/runtime/membership.rs), `send_claim_grant`, deliberately uses ephemeral delivery rather than reliable-outbox replay. Preserve this existing protection.
- [session/dispatch.rs](../crates/lattice-placement/src/session/dispatch.rs) supplies `self.now()` when processing `ClaimGranted`; [authority.rs](../crates/lattice-placement/src/authority.rs) computes the deadline from that installation time and checks it at admission.

The source therefore does not by itself prove the target's delay/suspension-safe deadline contract. Before implementation, specify how a renewal is correlated with freshness, which interval is bounded, how backing-lease remaining validity reaches the owner, and what happens when timing assumptions cannot be met. Simply raising the safety margin is not a substitute for that contract. Build tests for delayed delivery, stalled Coordinator work, owner suspension, failed/ambiguous renewals, and early authority replacement.

The existing unit test `a_reinstalled_grant_reopens_admission_against_the_new_deadline` explicitly installs a new grant after the prior deadline. It does not model a full retired Actor instance, but it shows that “no revival after retirement” cannot be introduced as a cosmetic refactor. Decide the retirement boundary and update authority/Registry integration tests together; do not silently delete this test as obsolete.

### 4.2 Candidate qualification and revocation

Current deployment constructs `CoordinatorHost::elect` from an injected store and set of domains. Static seed membership is not authorization, and the current election transaction has no managed candidate-eligibility record. The proposed change requires an exact, revisioned qualification predicate in election and authority writes, plus a way to administer eligibility while the old leader is unavailable.

Before W2.2, specify the authenticated identity, remove/re-add fencing, and completion states: eligibility committed, campaigning stopped, leadership relinquished, registration withdrawn. A configuration write cannot promise that an unreachable process has exited. Determine whether already-issued grants remain usable until their bounded deadline; do not silently equate candidate revocation with instantaneous revocation of every owner's external activity.

### 4.3 Transfer safety

[HandoffMachine](../crates/lattice-placement/src/handoff.rs) currently moves through `Invalidating -> Draining -> ReplacingAuthority -> Starting -> Completed`. Its barrier tracks required/applied exact incarnations and accepts explicit session fencing. It distinguishes `SourceDrained`, `SourceAuthorityInvalid`, and `SourceStopFailed`; only a matching target generation can become ready. [Reconciliation](../crates/lattice-placement/src/runtime/reconciliation.rs) reconstructs persisted transitions and can adopt a surviving claim with a new term/grant sequence without rewriting the assignment generation.

Before W3.3, define the evidence behind `SourceAuthorityInvalid`, how outstanding owner deadlines are respected, and how session fencing is durably represented in a sealed participant set. A missing participant record must not become an acknowledgement. Preserve the distinction between graceful completion and failure recovery, including stop-failed state; do not flatten the state machine to a generic owner swap.

### 4.4 Shutdown, reset, and new-run startup

[Service lifecycle](../crates/lattice-service/src/builder/service.rs) currently implements `shutdown` through node leave with a deadline. `terminal_shutdown` attempts membership release, fences local authority, and stops local components without requiring shard migration; release errors/timeouts are logged and local stopping proceeds. Neither entry point commits a cross-group cluster shutdown operation or proves that all runtime keys were reclaimed.

The target must define a guarded `Running -> Closing` commit visible to assignment/admission paths, durable participant/progress evidence, and the exclusion between cleanup and new-run startup. Candidate takeover must still advance an unfinished close. A waiting caller's timeout is distinct from aborting the operation. Fix queued/in-flight work treatment, retry identity, late join handling, and confirmable completion before reusing the local shutdown methods in global orchestration.

Existing migration/repair tooling provides useful dry-run, lock, CAS, and progress examples, not a ready-made reset protocol. Its legacy schema predicates are specific to its migration. Full-stop reset still requires exact scope/run selection, persistent-data exclusions, and operator confirmation that old deployments are stopped; absent leases do not prove external writes have ended.

## 5. Focused verification map

The following tests were located, not executed. They are starting points for extension, not proof that the target design already passes.

| Area | Existing evidence to reuse | Required extension |
|---|---|---|
| Authority deadline | `lattice-placement/src/lib.rs`: `admission_closes_on_the_grant_deadline_without_any_tick`; `session/tests.rs` | End-to-end delayed renewal, suspension, early replacement and exact retired-instance behavior |
| Claim expiry | `runtime/tests/claim_expiry.rs`: heartbeat timeout, force-versus-graceful revoke, ephemeral grant delivery | Coordinator/owner network asymmetry and deadline proof assumptions |
| Election/recovery | `runtime/reconciliation_tests.rs`, `runtime/host/cluster_tests.rs` | Managed eligibility races, concurrent last-candidate removal, removal/re-addition, leader retirement |
| Transfer restart | `runtime/tests/recovery_tests.rs`, `handoff.rs` tests | Split participant publication/sealing, missing records, failover at every new commit boundary |
| Storage guards | `storage/tests.rs`; `tests/etcd_acceptance.rs`: `real_etcd_guarded_domain_commits_and_lease_expiry` | Epoch/candidate predicates, new bounded records, cross-record crash consistency |
| Storage pagination/history | `tests/etcd_acceptance/paged_reads.rs`, `runtime/tests/history.rs`, `runtime/tests/read_amplification.rs` | Byte budgets, concurrent scan mutation, bounded cleanup across repeated runs |
| Discovery/join | `lattice-discovery/src/tests.rs`, `lattice-service/src/cluster/join.rs` tests | Leader-first provider, stale seeds during replacement, scope-specific readiness |
| Actor retirement | `lattice-actor-distributed/tests/registry.rs`, `lazy_activation.rs`, `panic_termination.rs` | Shared entity/singleton revocation, loader/publication race, no reopened retired generation |
| Shutdown | `lattice-service/src/tests/node_lifecycle.rs`, service lifecycle inline tests | Persisted cross-group close, caller/leader loss, partial stop failure, reset/startup exclusion |
| Non-runtime exclusions | `lattice-config-etcd/tests/etcd_acceptance.rs`, `lattice-id-etcd/tests/etcd_acceptance.rs` | Reset preserves application config and worker-ID history |

Use existing [placement failpoints](../crates/lattice-placement/src/failpoints.rs), including pre-guarded-commit, after-commit-before-effect, partial barrier, after-drain-before-revoke, and after-new-claim-before-grant boundaries. Extend the failpoints when the transaction layout changes rather than assuming old placements exercise new races.

The real-etcd acceptance harness reads `LATTICE_ETCD_ENDPOINTS` and may return without exercising etcd when it is absent. A successful test-process exit alone is not backend evidence. Run it only against an isolated test deployment and record that the prerequisite was present. Commands will continue to use the current crate name until the rename lands.

No new failing test or pass rate is established by this source audit. Representative scale and performance budgets remain unmeasured. P0.4 is therefore partial: the test map exists, but baseline failures, workload sizes, and measured budgets still need to be recorded when focused execution begins.

## 6. Preparation outcome

- The current key families, lease attachments, major readers/writers, growth sources, and transaction invariants are identified. No key deletion is authorized by this inventory.
- Primary reduction candidates are duplicated member descriptions, embedded participant sets, and whole persisted rebalance proposals. Required assignment/claim/transfer evidence is not disposable.
- Persistent group membership, assignments, plans, counters, settings, and operation records explain why process termination alone does not empty the coordination prefix.
- P0.1's source inventory is recorded; final field-by-field removal mappings and cleanup predicates depend on the unresolved protocol contracts. P0.2 and P0.3 remain open; P0.4 has a source-level test map only. No work package is marked complete.
- Next implementation gate: settle package 1's naming/framework-identity contract, while completing the grant, eligibility, transfer, and shutdown contracts before their coupled package 2/3 slices. `AppVersion` and application rolling-release redesign are deferred to a separate effort, including old-release shard migration; they do not block package 1. Remove the old application release model, N/N+1 admission guards, and release-biased placement without a replacement. Retain independent protocol/configuration checks; deploy one application version at a time until rolling updates are separately designed. Track approvals and completion only in memo section 9.
