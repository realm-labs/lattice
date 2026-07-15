# Codex Goal Prompt

```text
Your goal is to execute the placement-domain Coordinator hard switch completely.

Primary execution plan:
- docs/placement-domain-coordinator-goal.md

Completed historical references (do not execute their old trackers):
- docs/production-hardening-plan.md
- docs/implementation-plan.md
- docs/coordinator-correctness-implementation-plan.md
- docs/cluster-discovery-lifecycle-plan.md

Architecture acceptance sources:
- docs/architecture/README.md
- docs/architecture/00-overview.md
- docs/architecture/01-actor-runtime.md
- docs/architecture/02-rpc.md
- docs/architecture/03-placement.md
- docs/architecture/04-eventbus-scheduler-config.md
- docs/architecture/05-gateway-ops.md
- docs/architecture/06-appendix.md
- docs/architecture/07-api-examples.md
- docs/architecture/08-distributed-testing.md

First, read docs/placement-domain-coordinator-goal.md completely, including its execution protocol.
Then execute its Current Execution Pointer and hard-switch macro batches through the tracker.

This is a hard switch, not an incremental compatibility migration. Do not stop at documentation or API sketches.
Implement the complete placement-domain architecture rather than substituting a cluster-wide default domain that requires a later structural rewrite. Preserve multi-lane Associations, bounded protocol catalogues, revisioned snapshots, per-slot claims, subscribed-Region handoff barriers, pluggable allocation/capacity-aware persisted rebalancing, simulation-first deterministic correctness/executable invariants, Docker-based multi-process/chaos acceptance, EntityRef/SingletonRef watch_current, and the full Singleton model. Scope membership, leadership, storage, routing, capacity, lifecycle, administration and acceptance exactly as required by the active Goal.
The implementation must use the Cargo workspace crate split defined in docs/architecture/00-overview.md and docs/production-hardening-plan.md. Do not implement lattice as one root crate with many internal modules.
Batches are dependency/checklist groupings and may overlap inside one macro commit; they are not green-build boundaries by themselves.
When resuming from an existing implementation, do not trust tracker checkmarks as proof of completion. Audit checked items in the current batch and earlier dependency batches against the codebase before continuing. A checked item is valid only if it has concrete framework implementation plus executable test or runnable example coverage, or an explicit documented rationale for why no code is required. If a checked item is not backed by implementation/coverage, change it back to `[ ]` or add a precise missing-work subitem, then work on the earliest missing item.
Follow the macro-batch grouping in the plan. Prefer a large cross-crate replacement over compatibility adapters, temporary fallback routing, dual writes, or small commits created only to keep the workspace green.
Intermediate worktrees and commits may intentionally fail to compile, test, or run while removed APIs and crates are being replaced. Record the known broken frontier and continue; do not restore obsolete APIs merely to regain compilation.
After each completed macro batch, update docs/placement-domain-coordinator-goal.md:
- mark completed tracker items with `[x]`;
- add newly discovered missing work as `[ ]` items;
- mark a batch status `[x]` only after every required checklist/evidence item in that batch is complete.
Aim for the three large English conventional commits named in the active Goal after its documentation baseline. Do not create per-checklist commits.

Final exit criteria:
- Every batch status in the active Goal is `[x]`.
- Every deliverable, invariant, migration item and acceptance test in the active Goal is complete.
- Storage/control generation 5 is the only runtime model and the generation-4 placement topology is structurally absent.
- Every architecture design under docs/architecture/ has a corresponding code implementation, example API, or executable test coverage.
- examples/minimal-world runs and covers service bootstrap, actor registration, remoting, sharding, singleton, EventBus, scheduler, Gateway, ops, and telemetry.
- The pinned Docker `quality`, `sim`, `model`, `e2e`, `e2e-ha-etcd`, `chaos`, and `k8s` profiles pass; required soak/replay evidence is retained. These profiles run fmt, clippy, tests, real TCP/TLS/etcd, fault scenarios and deployment-lifecycle checks without requiring a host Rust/Kubernetes test toolchain.
- No framework capability exists only in documentation.
- Code remains readable and modular: no `super::super` imports, avoid unnecessary `pub use`, do not pile unrelated logic into one file, and do not let any single file exceed 1200 LOC without a documented reason.
- The root crate is only a facade if needed; framework implementation lives in dedicated workspace crates such as lattice-core, lattice-actor, lattice-remoting, lattice-placement, lattice-service, lattice-eventbus, lattice-config, lattice-gateway, and lattice-ops. Deterministic test support lives in a dedicated non-production crate such as lattice-sim rather than leaking simulation APIs into business code.

If an architecture item cannot be implemented immediately, do not skip it.
Continue through other work in the same macro batch when that advances the broken frontier. Record blockers, missing context, and attempted approaches. Mark the goal blocked only after multiple consecutive rounds fail to make meaningful progress. Intermediate red commits are allowed; final completion still requires every verification and acceptance criterion to pass.
```
