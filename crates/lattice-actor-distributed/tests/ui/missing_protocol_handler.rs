use lattice_actor_distributed::actor_protocol;
use lattice_actor::error::ActorFailure;
use lattice_actor_distributed::protocol::ProstCodec;
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
    type Error = ActorFailure;
    type Behavior = ::lattice_actor::state_machine::Stateless;
}

fn main() {
    let _ = ServerProtocol::bind::<MissingHandlerActor>();
}
