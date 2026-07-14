use lattice_actor::actor_protocol;
use lattice_actor::protocol::ProstCodec;
use lattice_actor::traits::Message;

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
