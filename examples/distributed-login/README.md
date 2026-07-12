# Distributed Login Example

This crate demonstrates the hard-switched actor messaging API with protobuf business messages.
`WorldActor` and `PlayerActor` each declare an explicit bounded `ActorProtocol`, codec/schema
versions, and distinct `ProtocolId`. The runnable workflow creates an exact World activation and
performs a typed ask through `LatticeService`.

Run the integration flow:

```bash
cargo test -p distributed-login --test distributed_flow -- --nocapture
```

The component-named binaries exercise the same API boundary. Real multi-process Coordinator,
ShardRegion, Gateway, and failure scenarios live in the distributed acceptance harness rather than
using private in-process placement stores that cannot represent a cluster.
