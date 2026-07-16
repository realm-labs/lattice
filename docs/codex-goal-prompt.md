# Codex Goal Prompt

```text
Your goal is to fully execute the Actor and service lifecycle hardening plan.

Primary execution plan:
- docs/actor-service-lifecycle-hardening-plan.md

Completed historical references (do not execute their old trackers):
- docs/placement-domain-coordinator-goal.md
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

First, read docs/actor-service-lifecycle-hardening-plan.md completely. Then execute its Current
Execution Pointer and Batches A-E in dependency order. Update the pointer and checklist after every
completed batch or material change to the broken frontier.

The highest-priority invariant is that Actor::stopping() persists business state. If it fails, retain
the same Actor instance and its in-memory state in a nonterminal StopFailed cell. Freeze business
admission, retain the activation reservation, block voluntary handoff and graceful shutdown, and wait
for an explicit retry or an operator-approved force stop with visible data-loss semantics. Never return
from the Actor task, drop the Actor, publish ActorTerminated, or activate a replacement merely because
the stopping hook failed.

External placement authority remains stronger than local cleanup. Claim loss must fence the old Actor
immediately and must not delay safe replacement ownership. When the process remains alive, retain a
failed old Actor only in bounded non-authoritative quarantine for retry-persist, diagnostics, or
explicit force-discard. Quarantine must never route business messages or restore authority implicitly.

Make Registry cleanup, ActivationDirectory cleanup, DeathWatch, Actor lifecycle, shard drain, node
admission, domain health, and Coordinator-candidate health agree with the executable state machines.
Production LogicService must consume every NodeLifecycle effect through one serialized lifecycle
driver; callers must not maintain a second partial orchestration model.

This is a hard switch. Do not add old/new lifecycle modes, compatibility aliases, silent force-stop
fallbacks, or a path that reports Terminated while Actor cells, claims, associations, endpoints, or
supervised tasks remain live. Keep the existing term/generation/claim/handoff safety model intact.

A checked item requires implementation plus executable evidence. Do not trust existing prose or old
tracker checkmarks as proof. Start with failing characterization tests for the reviewed defects, then
implement the complete batch rather than weakening the invariant to preserve current tests.

Final exit criteria:
- Every batch and checklist item in docs/actor-service-lifecycle-hardening-plan.md is complete.
- A failed stopping hook retains the same Actor instance and can be retried successfully.
- Force stop is the only explicit local data-loss path and is observable and auditable.
- External claim loss fences immediately while bounded quarantine preserves recoverable local memory.
- Successful passivation eagerly cleans Registry and exact activation-directory state.
- DeathWatch emits exactly one terminal notification and never treats retained StopFailed as terminal.
- Graceful drain reports retained failures instead of losing Actor state or falsely terminating.
- Production consumes all lifecycle effects and publishes truthful node/domain/candidate health.
- Focused Actor, placement, service, simulation, workspace format, clippy, and test gates pass.
- Architecture diagrams, API documentation, and the operator runbook match executable behavior.

If implementation uncovers a conflict, preserve Actor durability, distributed fencing, exact activation
identity, bounded resources, and truthful termination postconditions. Record the conflict in the plan,
update the affected architecture document in the same change, and continue from the earliest incomplete
invariant.
```
