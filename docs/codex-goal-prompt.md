# Codex Goal Prompt

```text
Your goal is to fully implement the lattice framework.

Primary execution plan:
- docs/implementation-plan.md

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

First, read docs/implementation-plan.md completely, including the "Codex Goal Execution Protocol" section.
Then execute the plan through the "Current Progress Tracker" in docs/implementation-plan.md.

Do not skip phases. Do not stop at documentation. Do not stop at API sketches.
The implementation must use the Cargo workspace crate split defined in docs/architecture/00-overview.md and docs/implementation-plan.md. Do not implement lattice as one root crate with many internal modules.
For each phase, complete the code implementation, examples, tests, and acceptance checklist before moving to the next phase.
When resuming from an existing implementation, do not trust tracker checkmarks as proof of completion. Audit checked items in the current phase and earlier dependency phases against the codebase before continuing. A checked item is valid only if it has concrete framework implementation plus executable test or runnable example coverage, or an explicit documented rationale for why no code is required. If a checked item is not backed by implementation/coverage, change it back to `[ ]` or add a precise missing-work subitem, then work on the earliest missing item.
Always choose the earliest unchecked item in the Current Progress Tracker unless it is explicitly blocked by an earlier missing dependency.
Within a phase, work in small slices: choose one or a few checklist items, implement them end to end, verify them, then commit that slice before continuing.
After each completed slice, update docs/implementation-plan.md:
- mark completed tracker items with `[x]`;
- add newly discovered missing work as `[ ]` items;
- mark a phase status `[x]` only after every required checklist item in that phase is checked and phase acceptance tests pass.
After each completed slice, create an English conventional commit message, such as "feat(actor): add bounded mailbox" or "test(rpc): cover metadata extraction".

Final exit criteria:
- Every phase status in the Current Progress Tracker is `[x]`.
- Every deliverable, acceptance item, and suggested test in docs/implementation-plan.md is complete or covered by an explicit equivalent test.
- Every item in the global acceptance checklist in docs/implementation-plan.md is satisfied.
- Every architecture design under docs/architecture/ has a corresponding code implementation, example API, or executable test coverage.
- examples/minimal-world runs and covers the final service bootstrap, actor registration, RPC, placement, event bus, scheduler, gateway, ops, and telemetry shape.
- cargo fmt, cargo clippy, and cargo test pass.
- No framework capability exists only in documentation.
- Code remains readable and modular: no `super::super` imports, avoid unnecessary `pub use`, do not pile unrelated logic into one file, and do not let any single file exceed 1200 LOC without a documented reason.
- The root crate is only a facade if needed; framework implementation lives in dedicated workspace crates such as lattice-core, lattice-actor, lattice-rpc, lattice-placement, lattice-eventbus, lattice-config, lattice-gateway, and lattice-ops.

If an architecture item cannot be implemented immediately, do not skip it.
First try to split it into smaller deliverables. If it still cannot move forward, record the blocker, missing context, and attempted approaches. Mark the goal blocked only after multiple consecutive rounds fail to make meaningful progress.
```
