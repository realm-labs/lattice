use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tracing::{Instrument, warn};

use crate::coordination::actor::PlacementCoordinator;
use crate::coordination::logic::VirtualShardMigrationControl;
use crate::coordination::reports::{
    PrepareVirtualShardMigrationRequest, RebalanceVirtualShardsReport,
    RebalanceVirtualShardsRequest, VirtualShardMovementPolicy,
};
use crate::error::PlacementError;
use crate::registry::InstanceState;
use crate::routing::placement::PlacementWatchTask;
use crate::sharding::{VirtualShardAssignInput, VirtualShardAssigner, VirtualShardAssignment};
use crate::storage::{
    PlacementStore, PlacementWatchEvent, VirtualShardPlacementKey, VirtualShardPlacementRecord,
};

impl<S, L> PlacementCoordinator<S, L>
where
    S: PlacementStore,
    L: VirtualShardMigrationControl,
{
    pub async fn rebalance_virtual_shards<A>(
        &self,
        request: RebalanceVirtualShardsRequest,
        assigner: &A,
    ) -> Result<RebalanceVirtualShardsReport, PlacementError>
    where
        A: VirtualShardAssigner + ?Sized,
    {
        let span = tracing::info_span!(
            "placement.vshards.rebalance",
            otel.kind = "internal",
            service.kind = request.service_kind.as_str(),
            actor.kind = request.actor_kind.as_str(),
            shard.count = request.shard_count
        );
        async {
            let mut instances = self
                .store
                .list_instances(&request.service_kind)
                .await?
                .into_iter()
                .filter(|instance| instance.state == InstanceState::Ready)
                .map(|instance| instance.instance_id)
                .collect::<Vec<_>>();
            instances.sort();
            if instances.is_empty() {
                return Err(PlacementError::NoReadyInstances);
            }
            let ready_instances = instances.len();

            let existing = self
                .store
                .list_virtual_shards(&request.service_kind, &request.actor_kind)
                .await?;
            let previous = existing
                .iter()
                .map(|(_, record)| VirtualShardAssignment {
                    shard_id: record.shard_id,
                    owner: record.owner.clone(),
                    epoch: record.epoch,
                })
                .collect::<Vec<_>>();
            let current_by_shard = existing
                .into_iter()
                .map(|(version, record)| (record.shard_id, (version, record)))
                .collect::<BTreeMap<_, _>>();
            let plan = assigner
                .plan(VirtualShardAssignInput {
                    service_kind: request.service_kind.clone(),
                    actor_kind: request.actor_kind.clone(),
                    shard_count: request.shard_count,
                    instances,
                    previous,
                    eligible_shards: match request.movement_policy {
                        VirtualShardMovementPolicy::EligibleOnly => request.eligible_shards.clone(),
                        VirtualShardMovementPolicy::AllowRunningMigration => BTreeSet::new(),
                    },
                    max_migrations: match request.movement_policy {
                        VirtualShardMovementPolicy::EligibleOnly
                            if request.eligible_shards.is_empty() =>
                        {
                            0
                        }
                        _ => request.max_migrations,
                    },
                })
                .await?;

            let mut assignments_written = 0;
            let mut moved_shards = 0;
            for assignment in plan.assignments {
                let key = VirtualShardPlacementKey {
                    service_kind: request.service_kind.clone(),
                    actor_kind: request.actor_kind.clone(),
                    shard_id: assignment.shard_id,
                };
                let current = current_by_shard.get(&assignment.shard_id);
                if let Some((_, record)) = current
                    && record.owner == assignment.owner
                {
                    continue;
                }
                let moved = current
                    .map(|(_, current)| current.owner != assignment.owner)
                    .unwrap_or(false);
                let reservation = self
                    .store
                    .reserve_virtual_shard_epoch(key, current.map(|(version, _)| *version))
                    .await?;

                let record = VirtualShardPlacementRecord {
                    service_kind: request.service_kind.clone(),
                    actor_kind: request.actor_kind.clone(),
                    shard_id: assignment.shard_id,
                    owner: assignment.owner,
                    epoch: reservation.epoch(),
                };
                self.store
                    .commit_virtual_shard_epoch(reservation, record)
                    .await?;
                assignments_written += 1;
                if moved {
                    moved_shards += 1;
                }
            }

            Ok(RebalanceVirtualShardsReport {
                ready_instances,
                assignments_written,
                moved_shards,
            })
        }
        .instrument(span)
        .await
    }

    pub async fn prepare_and_rebalance_virtual_shards<A>(
        &self,
        request: RebalanceVirtualShardsRequest,
        assigner: &A,
    ) -> Result<RebalanceVirtualShardsReport, PlacementError>
    where
        A: VirtualShardAssigner + ?Sized,
        L: VirtualShardMigrationControl,
    {
        if request.movement_policy == VirtualShardMovementPolicy::AllowRunningMigration {
            return self.rebalance_virtual_shards(request, assigner).await;
        }

        let candidates = self
            .planned_virtual_shard_moves(request.clone(), assigner)
            .await?;
        let mut eligible_shards = request.eligible_shards.clone();
        for record in candidates {
            let Some(instance) = self.store.get_instance(&record.owner).await? else {
                continue;
            };
            if instance.state != InstanceState::Ready {
                continue;
            }

            let outcome = self
                .logic
                .prepare_virtual_shard_migration(
                    &instance,
                    PrepareVirtualShardMigrationRequest {
                        service_kind: record.service_kind.clone(),
                        actor_kind: record.actor_kind.clone(),
                        shard_id: record.shard_id,
                        shard_count: request.shard_count,
                        owner_epoch: record.epoch,
                    },
                )
                .await?;
            if outcome.eligible {
                eligible_shards.insert(record.shard_id);
            }
        }

        self.rebalance_virtual_shards(
            RebalanceVirtualShardsRequest {
                eligible_shards,
                movement_policy: VirtualShardMovementPolicy::EligibleOnly,
                ..request
            },
            assigner,
        )
        .await
    }

    pub async fn start_virtual_shard_scale_out_watch<A>(
        &self,
        requests: Vec<RebalanceVirtualShardsRequest>,
        assigner: A,
    ) -> Result<PlacementWatchTask, PlacementError>
    where
        A: VirtualShardAssigner,
        L: VirtualShardMigrationControl,
    {
        let mut watch = self.store.watch(self.store.prefix().clone()).await?;
        let coordinator = self.clone();
        let assigner: Arc<dyn VirtualShardAssigner> = Arc::new(assigner);
        let handle = tokio::spawn(async move {
            while let Ok(event) = watch.next().await {
                let PlacementWatchEvent::InstanceUpdated { record } = event else {
                    continue;
                };
                if record.state != InstanceState::Ready {
                    continue;
                }

                for request in requests
                    .iter()
                    .filter(|request| request.service_kind == record.service_kind)
                {
                    if let Err(error) = coordinator
                        .prepare_and_rebalance_virtual_shards(request.clone(), assigner.as_ref())
                        .await
                    {
                        warn!(
                            service.kind = request.service_kind.as_str(),
                            actor.kind = request.actor_kind.as_str(),
                            instance.id = record.instance_id.as_str(),
                            error = %error,
                            "automatic virtual shard scale-out rebalance failed"
                        );
                    }
                }
            }
        });

        Ok(PlacementWatchTask::new(handle))
    }

    async fn planned_virtual_shard_moves<A>(
        &self,
        request: RebalanceVirtualShardsRequest,
        assigner: &A,
    ) -> Result<Vec<VirtualShardPlacementRecord>, PlacementError>
    where
        A: VirtualShardAssigner + ?Sized,
    {
        let mut instances = self
            .store
            .list_instances(&request.service_kind)
            .await?
            .into_iter()
            .filter(|instance| instance.state == InstanceState::Ready)
            .map(|instance| instance.instance_id)
            .collect::<Vec<_>>();
        instances.sort();
        if instances.is_empty() {
            return Err(PlacementError::NoReadyInstances);
        }

        let existing = self
            .store
            .list_virtual_shards(&request.service_kind, &request.actor_kind)
            .await?;
        let previous = existing
            .iter()
            .map(|(_, record)| VirtualShardAssignment {
                shard_id: record.shard_id,
                owner: record.owner.clone(),
                epoch: record.epoch,
            })
            .collect::<Vec<_>>();
        let current_by_shard = existing
            .into_iter()
            .map(|(_, record)| (record.shard_id, record))
            .collect::<BTreeMap<_, _>>();
        let plan = assigner
            .plan(VirtualShardAssignInput {
                service_kind: request.service_kind,
                actor_kind: request.actor_kind,
                shard_count: request.shard_count,
                instances,
                previous,
                eligible_shards: BTreeSet::new(),
                max_migrations: request.max_migrations,
            })
            .await?;

        Ok(plan
            .assignments
            .into_iter()
            .filter_map(|assignment| {
                let current = current_by_shard.get(&assignment.shard_id)?;
                (current.owner != assignment.owner).then(|| current.clone())
            })
            .collect())
    }
}
