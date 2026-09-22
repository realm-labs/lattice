use lattice_actor::runtime::ActorRuntime;
use lattice_actor::error::ActorFailure;
use lattice_actor::state_machine::Stateless;
use lattice_actor::traits::Actor;
use lattice_actor_distributed::actor_protocol;
use lattice_actor_distributed::registry::{ActorDefinition, ActorRegistry, ActorRegistryConfig};

actor_protocol! {
    ExpectedProtocol { protocol_id: 201; name: "expected"; }
}
actor_protocol! {
    WrongProtocol { protocol_id: 202; name: "wrong"; }
}

struct Definition;
impl ActorDefinition for Definition {
    const NAME: &'static str = "Example";
    type Protocol = ExpectedProtocol;
}

struct Server;
impl Actor for Server {
    type Error = ActorFailure;
    type Behavior = Stateless;
}

fn main() {
    let binding = WrongProtocol::bind::<Server>().unwrap();
    let actor_runtime = ActorRuntime::default();
    let _ = ActorRegistry::<Definition, Server>::new_bound(actor_runtime.spawner(), ActorRegistryConfig::default(), &binding);
}
