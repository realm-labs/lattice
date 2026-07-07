use std::sync::Arc;

use async_trait::async_trait;
use lattice_actor::context::ActorContext;
use lattice_actor::error::ActorError;
use lattice_actor::registry::{ActorCreateContext, ActorFactory};
use lattice_actor::traits::{Actor, Handler};
use lattice_core::id::ActorId;
use lattice_rpc::types::Rpc;

use crate::bench::{
    ChainPingReply, ChainPingRequest, PingReply, PingRequest, WorkReply, WorkRequest,
};
use crate::error::BenchmarkError;
use crate::generated::worker_rpc;

type WorkerClient = worker_rpc::Client<worker_rpc::DefaultClientCore>;

#[derive(Debug)]
pub struct BenchActor {
    actor_id: u64,
    seen: u64,
}

#[async_trait]
impl Actor for BenchActor {
    type Error = ActorError;
}

#[async_trait]
impl Handler<Rpc<PingRequest>> for BenchActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Rpc<PingRequest>,
    ) -> Result<PingReply, ActorError> {
        self.seen += 1;
        Ok(PingReply {
            actor_id: self.actor_id,
            sequence: msg.req.sequence,
            actor_seen: self.seen,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct BenchActorFactory;

#[async_trait]
impl ActorFactory<BenchActor> for BenchActorFactory {
    async fn create(&self, ctx: ActorCreateContext) -> Result<BenchActor, ActorError> {
        Ok(BenchActor {
            actor_id: actor_id_as_u64(ctx.actor_id)?,
            seen: 0,
        })
    }
}

#[derive(Debug)]
pub struct ChainActor {
    actor_id: u64,
    worker_client: Arc<WorkerClient>,
    seen: u64,
}

#[async_trait]
impl Actor for ChainActor {
    type Error = ActorError;
}

#[async_trait]
impl Handler<Rpc<ChainPingRequest>> for ChainActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Rpc<ChainPingRequest>,
    ) -> Result<ChainPingReply, ActorError> {
        self.seen += 1;
        let worker = self
            .worker_client
            .work(WorkRequest {
                worker_actor_id: msg.req.worker_actor_id,
                sequence: msg.req.sequence,
            })
            .await
            .map_err(|error| ActorError::new(format!("worker rpc failed: {error}")))?;
        Ok(ChainPingReply {
            actor_id: self.actor_id,
            worker_actor_id: worker.worker_actor_id,
            sequence: msg.req.sequence,
            chain_seen: self.seen,
            worker_seen: worker.worker_seen,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct ChainActorFactory;

#[async_trait]
impl ActorFactory<ChainActor> for ChainActorFactory {
    async fn create(&self, ctx: ActorCreateContext) -> Result<ChainActor, ActorError> {
        let worker_client = ctx
            .service
            .extension::<WorkerClient>()
            .ok_or_else(|| ActorError::new("WorkerRpc client is not registered"))?;
        Ok(ChainActor {
            actor_id: actor_id_as_u64(ctx.actor_id)?,
            worker_client,
            seen: 0,
        })
    }
}

#[derive(Debug)]
pub struct WorkerActor {
    actor_id: u64,
    seen: u64,
}

#[async_trait]
impl Actor for WorkerActor {
    type Error = ActorError;
}

#[async_trait]
impl Handler<Rpc<WorkRequest>> for WorkerActor {
    async fn handle(
        &mut self,
        _ctx: &mut ActorContext<Self>,
        msg: Rpc<WorkRequest>,
    ) -> Result<WorkReply, ActorError> {
        self.seen += 1;
        Ok(WorkReply {
            worker_actor_id: self.actor_id,
            sequence: msg.req.sequence,
            worker_seen: self.seen,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct WorkerActorFactory;

#[async_trait]
impl ActorFactory<WorkerActor> for WorkerActorFactory {
    async fn create(&self, ctx: ActorCreateContext) -> Result<WorkerActor, ActorError> {
        Ok(WorkerActor {
            actor_id: actor_id_as_u64(ctx.actor_id)?,
            seen: 0,
        })
    }
}

fn actor_id_as_u64(actor_id: ActorId) -> Result<u64, ActorError> {
    match actor_id {
        ActorId::U64(value) => Ok(value),
        actual => Err(ActorError::new(
            BenchmarkError::InvalidActorId { actual }.to_string(),
        )),
    }
}
