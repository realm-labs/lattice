use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::kind::ActorKind;
use lattice_placement::control::LogicControlHandler;
use lattice_placement::coordinator::{
    PrepareVirtualShardMigrationRequest, VirtualShardMigrationOutcome,
};
use lattice_placement::error::PlacementError;
use lattice_placement::store::{ActorPlacementKey, SingletonKey};

use crate::actor::ErasedLogicActor;
use crate::direct_link::DirectLinkServiceRuntime;

#[derive(Clone)]
pub(crate) struct ServiceLogicControlHandler {
    actors: Arc<HashMap<ActorKind, Arc<dyn ErasedLogicActor>>>,
    direct_links: Option<DirectLinkServiceRuntime>,
}

impl ServiceLogicControlHandler {
    pub(crate) fn new(
        actors: HashMap<ActorKind, Arc<dyn ErasedLogicActor>>,
        direct_links: Option<DirectLinkServiceRuntime>,
    ) -> Self {
        Self {
            actors: Arc::new(actors),
            direct_links,
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

    async fn activate_singleton(
        &self,
        key: SingletonKey,
        _epoch: Epoch,
    ) -> Result<(), PlacementError> {
        let Some(actor) = self.actors.get(&key.singleton_kind) else {
            return Err(PlacementError::LogicControl {
                message: format!("missing singleton registration for {}", key.singleton_kind),
            });
        };
        actor
            .activate(ActorId::Str(key.scope))
            .await
            .map_err(|error| PlacementError::LogicControl {
                message: error.to_string(),
            })
    }

    async fn prepare_virtual_shard_migration(
        &self,
        request: PrepareVirtualShardMigrationRequest,
    ) -> Result<VirtualShardMigrationOutcome, PlacementError> {
        let Some(actor) = self.actors.get(&request.actor_kind) else {
            return Err(PlacementError::LogicControl {
                message: format!("missing actor registration for {}", request.actor_kind),
            });
        };
        let preparation = actor
            .prepare_virtual_shard_migration(
                request.shard_id,
                request.shard_count,
                self.direct_links.clone(),
            )
            .await;
        Ok(VirtualShardMigrationOutcome {
            shard_id: request.shard_id,
            eligible: preparation.eligible,
            running_actors: preparation.running_actors,
            passivated_actors: preparation.passivated_actors,
        })
    }
}
