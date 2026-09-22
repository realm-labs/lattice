use lattice_actor_distributed::actor_protocol;
use lattice_actor_distributed::protocol::ProstCodec;
use lattice_actor::traits::Message;
use lattice_actor_distributed::registry::ActorDefinition;

#[derive(Clone, PartialEq, prost::Message)]
struct Ping {}

impl Message for Ping {}

actor_protocol! {
    ClientProtocol {
        protocol_id: 101;
        name: "compile/client/v1";
        tell 1 => Ping {
            schema_version: 1,
            codec: ProstCodec,
        }
    }
}

fn main() {
    let protocol = ClientProtocol::build().unwrap();
    assert_eq!(protocol.protocol_id().get(), 101);
}

struct ClientDefinition;
impl ActorDefinition for ClientDefinition {
    const NAME: &'static str = "Client";
    type Protocol = ClientProtocol;
}
