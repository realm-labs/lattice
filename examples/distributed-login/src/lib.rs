mod error;

pub mod gateway;
pub mod placement;
pub mod player;
pub mod tcp;
pub mod world;

use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_core::{actor_kind, service_kind};

pub mod game {
    tonic::include_proto!("game");
}

pub mod lattice {
    pub mod actor {
        tonic::include_proto!("lattice.actor");
    }
}

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/lattice.generated.rs"));
}

pub const WORLD_SERVICE: ServiceKind = service_kind!("World");
pub const PLAYER_SERVICE: ServiceKind = service_kind!("Player");
pub const GATEWAY_SERVICE: ServiceKind = service_kind!("Gateway");
pub const WORLD_ACTOR: ActorKind = actor_kind!("World");
pub const PLAYER_ACTOR: ActorKind = actor_kind!("Player");
pub const GATEWAY_SESSION_ACTOR: ActorKind = actor_kind!("GatewaySession");

pub const LOGIN_MSG_ID: u32 = 100;
pub const WORLD_PING_MSG_ID: u32 = 101;
pub const PLAYER_PING_MSG_ID: u32 = 200;
