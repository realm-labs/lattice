pub mod actors;
pub mod error;
pub mod metrics;
pub mod multiprocess;
pub mod topology;
pub mod workload;

use lattice_core::{ActorKind, ServiceKind, actor_kind, service_kind};

pub mod bench {
    tonic::include_proto!("bench");
}

pub mod generated {
    include!(concat!(env!("OUT_DIR"), "/lattice.generated.rs"));
}

pub const BENCH_SERVICE: ServiceKind = service_kind!("Bench");
pub const CHAIN_SERVICE: ServiceKind = service_kind!("Chain");
pub const WORKER_SERVICE: ServiceKind = service_kind!("Worker");

pub const BENCH_ACTOR: ActorKind = actor_kind!("Bench");
pub const CHAIN_ACTOR: ActorKind = actor_kind!("Chain");
pub const WORKER_ACTOR: ActorKind = actor_kind!("Worker");
