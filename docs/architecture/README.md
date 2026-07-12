# lattice Architecture

> Version: v0.1
> Purpose: architecture reference for lattice.
> Scope: room/instance games, open-world zone services, stateful services such as Player/Guild/Inventory, and cluster-singleton control-plane services.
> Non-goals: the first version does not implement seamless cross-machine realtime combat, a strongly consistent distributed ECS, or exactly-once semantics.
> Implemented hard-switch baseline: [../production-hardening-plan.md](../production-hardening-plan.md).
> Active cluster discovery and lifecycle plan: [../cluster-discovery-lifecycle-plan.md](../cluster-discovery-lifecycle-plan.md).
> Discovery provider configuration and RBAC: [../cluster-discovery.md](../cluster-discovery.md).
> Historical implementation record: [../implementation-plan.md](../implementation-plan.md).

These documents describe the complete target model, not a disposable minimal v1. Implementation may land in dependency order, but target capabilities are simplified through shared internal mechanisms and narrow fault domains rather than omitted in ways that require later identity, wire, or ownership redesign.

---

## Reading Order

| File | Contents |
|---|---|
| [00-overview.md](00-overview.md) | System topology, Logic Service roles, message routing, control flows, Actor/service lifecycles, invariants, and crate layout |
| [01-actor-runtime.md](01-actor-runtime.md) | Rust core types, actor runtime, mailbox, ActorHandle, lifecycle |
| [02-rpc.md](02-rpc.md) | Unified actor remoting, reference semantics, codecs, tell/ask, associations, and DeathWatch |
| [03-placement.md](03-placement.md) | Coordinator, etcd metadata, ShardRegion, allocation/rebalancing, claims, handoff, singleton, drain, and outage behavior |
| [04-eventbus-scheduler-config.md](04-eventbus-scheduler-config.md) | EventBus, actor scheduler, service scheduler, and configuration center |
| [05-gateway-ops.md](05-gateway-ops.md) | Gateway routing, security, observability, config, common call flows, and forbidden patterns |
| [06-appendix.md](06-appendix.md) | Recommended defaults, tradeoffs, framework/business boundary, summary |
| [07-api-examples.md](07-api-examples.md) | Target APIs for protocols, service bootstrap, ActorRef, EntityRef, SingletonRef, watch, Gateway, EventBus, config, and scheduler |
| [08-distributed-testing.md](08-distributed-testing.md) | Deterministic simulation, invariants, Docker multi-process testing, fault injection, chaos, and release evidence |

## Maintenance Rules

```text
Keep architecture design under docs/architecture/ by topic.
Keep docs/architecture.md as an index only so it does not grow again.
Keep the hard-switch baseline in docs/production-hardening-plan.md.
Keep the active discovery/lifecycle execution plan in docs/cluster-discovery-lifecycle-plan.md.
Keep docs/implementation-plan.md as historical evidence only; do not execute it as the target design.
```
