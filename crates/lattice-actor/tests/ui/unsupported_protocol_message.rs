use lattice_actor::actor_protocol;
use lattice_actor::protocol::{ProstCodec, SupportsTell};
use lattice_actor::traits::Message;

#[derive(Clone, PartialEq, prost::Message)]
struct Allowed {}

impl Message for Allowed {}

#[derive(Clone, PartialEq, prost::Message)]
struct Unsupported {}

impl Message for Unsupported {}

actor_protocol! {
    ClientProtocol {
        protocol_id: 102;
        name: "compile/unsupported/v1";
        tell 1 => Allowed {
            schema_version: 1,
            codec: ProstCodec,
        }
    }
}

fn require_tell<P: SupportsTell<Unsupported>>() {}

fn main() {
    require_tell::<ClientProtocol>();
}
