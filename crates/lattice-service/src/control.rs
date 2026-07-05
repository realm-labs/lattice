use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use lattice_core::{ActorKind, Epoch};
use lattice_placement::PlacementError;
use lattice_placement::control::LogicControlHandler;
use lattice_placement::store::ActorPlacementKey;

use crate::actor::ErasedLogicActor;

#[derive(Clone)]
pub(crate) struct ServiceLogicControlHandler {
    actors: Arc<HashMap<ActorKind, Arc<dyn ErasedLogicActor>>>,
}

impl ServiceLogicControlHandler {
    pub(crate) fn new(actors: HashMap<ActorKind, Arc<dyn ErasedLogicActor>>) -> Self {
        Self {
            actors: Arc::new(actors),
        }
    }
}

#[async_trait]
impl LogicControlHandler for ServiceLogicControlHandler {
    async fn activate_actor(
        &self,
        key: ActorPlacementKey,
        _epoch: Epoch,
    ) -> Result<(), PlacementError> {
        let Some(actor) = self.actors.get(&key.actor_kind) else {
            return Err(PlacementError::LogicControl {
                message: format!("missing actor registration for {}", key.actor_kind),
            });
        };
        actor
            .activate(key.actor_id)
            .await
            .map_err(|error| PlacementError::LogicControl {
                message: error.to_string(),
            })
    }
}
