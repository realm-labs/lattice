use lattice_actor::actor_protocol;
use lattice_actor::error::ActorError;
use lattice_actor::protocol::ProstCodec;
use lattice_actor::traits::{Actor, Message};

#[derive(Clone, PartialEq, prost::Message)]
struct Ping {}

impl Message for Ping {}

actor_protocol! {
    ServerProtocol {
        protocol_id: 103;
        name: "compile/server/v1";
        tell 1 => Ping {
            schema_version: 1,
            codec: ProstCodec,
        }
    }
}

struct MissingHandlerActor;

impl Actor for MissingHandlerActor {
    type Error = ActorError;
}

fn main() {
    let _ = ServerProtocol::bind::<MissingHandlerActor>();
}
