# lattice Architecture

> Version: v0.1  
> Purpose: architecture reference for lattice.  
> Scope: room/instance games, open-world zone services, stateful services such as Player/Guild/Inventory, and cluster-singleton control-plane services.  
> Non-goals: the first version does not implement seamless cross-machine realtime combat, a strongly consistent distributed ECS, or exactly-once semantics.  
> Implementation plan: [../implementation-plan.md](../implementation-plan.md).

---

## Reading Order

| File | Contents |
|---|---|
| [00-overview.md](00-overview.md) | Design conclusions, goals, terminology, system overview, invariants, and crate layout |
| [01-actor-runtime.md](01-actor-runtime.md) | Rust core types, actor runtime, mailbox, ActorHandle, lifecycle |
| [02-rpc.md](02-rpc.md) | Proto rules, codegen, typed sharded clients, server adapters, RPC core, direct actor links |
| [03-placement.md](03-placement.md) | Placement model, etcd metadata, RouteResolver, Coordinator, activation, singleton, migration, drain, dynamic scaling |
| [04-eventbus-scheduler-config.md](04-eventbus-scheduler-config.md) | EventBus, actor scheduler, service scheduler, and configuration center |
| [05-gateway-ops.md](05-gateway-ops.md) | Gateway routing, security, observability, config, common call flows, and forbidden patterns |
| [06-appendix.md](06-appendix.md) | Recommended defaults, tradeoffs, framework/business boundary, summary |
| [07-api-examples.md](07-api-examples.md) | Framework API examples: service bootstrap, actor factory, RPC binding, Gateway, EventBus, Config, Scheduler |

## Maintenance Rules

```text
Keep architecture design under docs/architecture/ by topic.
Keep docs/architecture.md as an index only so it does not grow again.
Keep the phased delivery plan in docs/implementation-plan.md.
```
