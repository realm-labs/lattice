# Actor API Boundaries

This document is the canonical API model after the local/distributed hard switch.

## Crate boundaries

`lattice-actor` is the process-local Actor kernel. It owns `Actor`, `ActorHandle`,
`ActorContext`, mailboxes, scheduling, supervision, timers, local DeathWatch and
lifecycle management. It has no remoting feature and contains no address,
protocol, registry, shard, singleton or cluster-authority state.

`lattice-model::actor` contains serializable actor identity data:

- `ActorAddress<P>` identifies one exact activation;
- `EntityAddress<P>` identifies one logical sharded entity;
- `SingletonAddress<P>` identifies one logical singleton;
- `RecipientAddress<P>` is their serializable sum type.

Addresses contain no sending capability. They are the values stored in config,
persisted, or carried in wire messages.

The rest of the shared value model is grouped by domain rather than flattened
at the crate root:

- `lattice-model::cluster` owns node, placement, coordination, and release values;
- `lattice-model::service` owns service and instance names;
- `lattice-model::trace` owns propagation and telemetry resource values.

Runtime containers such as `ActorEnvironment` remain in `lattice-actor`.
Registry-local `ActorKey` and `ActorKind` remain in
`lattice-actor-distributed`; they are not wire address primitives.

`lattice-actor-distributed` owns protocols, activation registries, hosting,
routing and bound references:

- `ActorRef<P>`;
- `EntityRef<P>`;
- `SingletonRef<P>`;
- `Recipient<P>`.

A bound ref combines an address with an `ActorSystem`. It is cloneable and can
`tell`, `ask` and `watch`, but it is intentionally not serializable.

## One messaging shape

Local and distributed targets use the same call shape:

```rust,ignore
local_handle.tell(message).await?;
remote_ref.tell(message).await?;

let local_reply = local_handle.ask(request, timeout).await?;
let remote_reply = remote_ref.ask(request, timeout).await?;
```

Inside an Actor, both implement the kernel target abstractions:

```rust,ignore
ctx.tell(&target, message).await?;
let reply = ctx.ask(&target, request, timeout).await?;
let watch_id = ctx.watch(&target).await?;
```

`ActorContext` does not inspect whether a target is local or remote. The target
owns its delivery capability through `TellTarget`, `AskTarget` and
`WatchTarget`.

## Address binding

Bind once when address data enters an execution boundary:

```rust,ignore
let address: ActorAddress<WorkerProtocol> = decode(bytes)?;
let worker = service.bind_actor(address)?;
worker.tell(Work { id }).await?;
```

An Actor receiving an address in a typed message binds through its distributed
context capability:

```rust,ignore
async fn handle(
    &mut self,
    ctx: &mut HandlerContext<'_, Self>,
    message: Dispatch,
) -> Result<(), ActorFailure> {
    let target = ctx.bind_actor(message.target)?;
    ctx.tell(&target, Deliver(message.payload)).await?;
    Ok(())
}
```

`ctx.bind(...)` performs the same operation for `RecipientAddress<P>`.
Binding validates that the protocol is registered in the current
`ActorSystem`.

## Replies and sender identity

The runtime has no implicit `sender()` model. Requests receive a typed
`ReplyTo<R::Response>`. A one-way workflow that needs a later reply carries an
explicit typed reply address in its business message, then binds that address
at the receiving boundary.

This keeps reply lifetime, protocol and routing semantics in the message
contract. It also removes local-versus-remote sender variants and avoids
message-scoped sender state in `ActorContext`.

## Self identity and children

A registry-hosted Actor can obtain:

- `ctx.require_distributed()?.self_address()` when it must serialize its exact
  identity;
- `ctx.self_ref::<P>()?` when it needs a bound sending capability.

Core child Actors are deliberately process-local supervision children and are
returned as `ActorHandle<C>`. They do not automatically acquire distributed
addresses. An independently addressable distributed activation is created and
owned by an `ActorRegistry`, even when the application calls it a child.

## DeathWatch

`ActorHandle` and all bound distributed refs implement `WatchTarget`.
`ActorContext::watch(&target)` delivers `ActorTerminated { watch_id, reason }`
to the system mailbox. Business code associates the returned `WatchId` with
the target it chose; the local kernel does not carry a distributed target enum.

Process code may retain the subscription returned by `target.watch().await`.
Dropping an active distributed subscription cancels it.

## Dependency choice

A local-only application depends only on `lattice-actor`. A distributed
application depends on `lattice-actor-distributed` (and normally
`lattice-service`); the distributed crate re-exports the local Actor API as a
facade. There is no `distributed` feature on `lattice-actor`.
