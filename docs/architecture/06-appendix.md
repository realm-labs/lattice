# 06. Appendix

> Defaults, tradeoffs, framework/business boundary, and summary.  
> Back to: [architecture index](README.md)

---

## 29. Recommended Defaults

```text
mailbox capacity: 4096 for normal actors, configurable per actor kind
activation waiter timeout: 5s
route cache soft ttl: 5s
route cache hard ttl: 30s
NOT_OWNER retry: one retry with same request_id
actor idle passivation: 300s for lightweight actors
drain timeout: configurable, default 30s to 120s depending on service
event subscriber retry: exponential backoff with jitter
admin inspect timeout: 1s to 3s per node
metrics labels: low-cardinality only
```

These are starting points, not hard requirements. Production services should tune them by actor kind and workload.

---

## 30. Why Not Opaque Bytes as Internal RPC

Opaque bytes are acceptable at the client protocol boundary, but not as the main internal service-to-service model.

Typed RPC gives:

```text
compile-time request/reply types
generated clients and adapters
clear method ownership
structured errors
metadata injection outside business messages
better tracing and metrics
less duplicate decoding
easier schema evolution
```

Gateway can still receive opaque client frames. It decodes them once into typed proto requests and forwards typed gRPC to logic services.

---

## 31. Why Not a Giant Enum

A giant framework-level message enum would force every business message into one central type. That creates poor modularity and makes feature teams coordinate on one enum.

lattice instead uses:

```text
Message trait
Handler<M>
type-erased internal envelopes
generated compile-time bounds
business-owned message types
```

The runtime can erase types internally while public business code stays typed and modular.

---

## 32. Framework and Business Boundary

Framework owns:

```text
actor runtime
mailbox
lifecycle
route cache
placement
generated RPC runtime
EventBus abstraction
ConfigStore abstraction
scheduler
gateway forwarding
admin/telemetry integration
```

Business owns:

```text
business actor state
business databases and repositories
business transaction and consistency model
business event definitions
business protocol messages
business auth rules
business compensation/manual repair flows
business-specific actor keys and route extractors
```

The framework must not require MySQL, Redis, or any business database. It may depend on placement/config/event abstractions and their adapters.

---

## 33. Summary

lattice should stay a framework, not a game implementation.

```text
Keep routing, placement, lifecycle, and observability in the framework.
Keep domain state, persistence, and business workflows in business crates.
Use typed RPC for commands.
Use EventBus for asynchronous events.
Use LocalEventBus for in-process decoupling.
Use epochs, leases, request_id, and idempotency for distributed correctness.
```
