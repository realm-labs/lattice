# Distributed Login Example

This crate demonstrates a small lattice login flow:

- `player-service` hosts lazy `PlayerActor` instances.
- `world-service` hosts a pre-started `WorldActor`.
- `gateway` accepts raw TCP client frames, creates a `GatewaySessionActor` per client session, forwards client requests, and exposes an internal push RPC addressed to those gateway actors.
- `client` sends login and ping commands.

## Protocol

The client connection uses a raw TCP length-prefixed frame:

```text
u32_be frame_length
u32_be msg_id
prost_payload
```

Message ids are registered in `build.rs`:

- `100`: `game.WorldRpc.Login`
- `101`: `game.WorldRpc.WorldPing`
- `200`: `game.PlayerRpc.PlayerPing`

## Run

The verified runnable topology is the integration test. It starts the gateway,
world, and player services in one process so they deliberately share the same
in-memory placement store and development authority:

```bash
cargo test -p distributed-login --test distributed_flow -- --nocapture
```

The individual binaries are component launchers, not a working distributed
deployment: each currently creates a private in-memory placement namespace.
Running them as separate processes would require a shared etcd read/liveness
store plus a coordinator-backed `TonicPlacementAuthority`, matching the
multi-process benchmark topology. The old four-terminal commands and endpoint
flags were removed from this README because they did not share placement state
and were not accepted by the binaries.

The login command routes to `WorldActor`, which records a session and calls `PlayerRpc.InitSession`.
The gateway injects a serializable `ActorRef` for the local `GatewaySessionActor` into the login request before forwarding it.
The player service lazily activates `PlayerActor(42)`, stores the session, and sends the successful `LoginReply` through the referenced `GatewaySessionActor` using framework ActorRef RPC.
The gateway actor writes that pushed frame back to the raw TCP client connection.
