# Distributed Login Example

This example runs a small lattice deployment as separate processes:

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

Open four terminals from the repository root.

```bash
cargo run -p distributed-login --bin player-service -- \
  --addr 127.0.0.1:19082
```

```bash
cargo run -p distributed-login --bin world-service -- \
  --addr 127.0.0.1:19081 \
  --player-endpoint http://127.0.0.1:19082
```

```bash
cargo run -p distributed-login --bin gateway -- \
  --addr 127.0.0.1:19080 \
  --push-addr 127.0.0.1:19083 \
  --world-endpoint http://127.0.0.1:19081 \
  --player-endpoint http://127.0.0.1:19082
```

```bash
cargo run -p distributed-login --bin client -- login --gateway 127.0.0.1:19080 --world-id 1 --player-id 42 --session-id client-42
cargo run -p distributed-login --bin client -- world-ping --gateway 127.0.0.1:19080 --world-id 1
cargo run -p distributed-login --bin client -- player-ping --gateway 127.0.0.1:19080 --player-id 42
```

The login command routes to `WorldActor`, which records a session and calls `PlayerRpc.InitSession`.
The gateway injects a serializable `ActorRef` for the local `GatewaySessionActor` into the login request before forwarding it.
The player service lazily activates `PlayerActor(42)`, stores the session, and sends the successful `LoginReply` through the referenced `GatewaySessionActor` using framework ActorRef RPC.
The gateway actor writes that pushed frame back to the raw TCP client connection.
