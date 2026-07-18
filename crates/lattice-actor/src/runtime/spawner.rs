use crate::error::ActorSpawnError;
use crate::handle::ActorHandle;
use crate::traits::Actor;

use super::{ActorExecutionPolicy, ActorScheduler, ActorSpawnContext};

#[derive(Clone)]
pub(crate) struct ActorSpawner {
    scheduler: ActorScheduler,
    default_execution: ActorExecutionPolicy,
}

impl ActorSpawner {
    pub(super) fn new(scheduler: ActorScheduler, default_execution: ActorExecutionPolicy) -> Self {
        Self {
            scheduler,
            default_execution,
        }
    }

    pub(crate) fn task_per_actor() -> Self {
        Self::new(
            ActorScheduler::default(),
            ActorExecutionPolicy::TaskPerActor,
        )
    }

    pub(super) fn spawn<A>(
        &self,
        actor: A,
        context: ActorSpawnContext,
    ) -> Result<ActorHandle<A>, ActorSpawnError>
    where
        A: Actor,
    {
        let execution = context.options.execution.unwrap_or(self.default_execution);
        self.scheduler.spawn(actor, context, execution)
    }
}
