use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use lattice_core::kind::ServiceKind;
use tracing::warn;

use crate::coordination::actor::PlacementCoordinator;
use crate::coordination::logic::LogicControl;
use crate::coordination::reports::LeaseExpiryReconcileReport;
use crate::coordination::singleton::SingletonControl;
use crate::error::PlacementError;
use crate::registry::InstanceState;
use crate::routing::placement::PlacementWatchTask;
use crate::storage::PlacementStore;

impl<S, L> PlacementCoordinator<S, L>
where
    S: PlacementStore,
    L: LogicControl,
{
    pub async fn reconcile_expired_instances(
        &self,
        service_kind: ServiceKind,
    ) -> Result<LeaseExpiryReconcileReport, PlacementError>
    where
        L: SingletonControl,
    {
        let instances = self.store.list_instances(&service_kind).await?;
        let instance_states = instances
            .into_iter()
            .map(|record| (record.instance_id, record.state))
            .collect::<BTreeMap<_, _>>();
        let mut expired_instances = BTreeSet::new();
        for (instance_id, state) in &instance_states {
            if *state == InstanceState::Dead {
                expired_instances.insert(instance_id.clone());
            }
        }

        for (_version, record) in self.store.list_actors().await? {
            if !matches!(
                instance_states.get(&record.owner),
                Some(
                    InstanceState::Starting
                        | InstanceState::Ready
                        | InstanceState::Draining
                        | InstanceState::Stopping
                )
            ) {
                expired_instances.insert(record.owner);
            }
        }
        for (_version, record) in self.store.list_singletons().await? {
            if record.service_kind != service_kind {
                continue;
            }
            if !matches!(
                instance_states.get(&record.owner),
                Some(
                    InstanceState::Starting
                        | InstanceState::Ready
                        | InstanceState::Draining
                        | InstanceState::Stopping
                )
            ) {
                expired_instances.insert(record.owner);
            }
        }

        let mut failovers = Vec::new();
        let mut skipped_instances = Vec::new();
        for instance_id in &expired_instances {
            match self
                .failover_expired_instance(service_kind.clone(), instance_id.clone())
                .await
            {
                Ok(report) => failovers.push(report),
                Err(PlacementError::NoReadyInstances) => {
                    skipped_instances.push(instance_id.clone());
                }
                Err(error) => return Err(error),
            }
        }

        Ok(LeaseExpiryReconcileReport {
            service_kind,
            expired_instances: expired_instances.into_iter().collect(),
            failovers,
            skipped_instances,
        })
    }

    pub async fn reconcile_all_expired_instances(
        &self,
    ) -> Result<Vec<LeaseExpiryReconcileReport>, PlacementError>
    where
        L: SingletonControl,
    {
        let mut service_names = self
            .store
            .list_all_instances()
            .await?
            .into_iter()
            .map(|record| record.service_kind.as_str().to_string())
            .collect::<Vec<_>>();
        service_names.extend(
            self.store
                .list_singletons()
                .await?
                .into_iter()
                .map(|(_, record)| record.service_kind.as_str().to_string()),
        );
        service_names.sort();
        service_names.dedup();

        let mut reports = Vec::with_capacity(service_names.len());
        for service_name in service_names {
            reports.push(
                self.reconcile_expired_instances(ServiceKind::new(service_name))
                    .await?,
            );
        }
        Ok(reports)
    }

    pub fn start_lease_expiry_reconciler(
        &self,
        service_kind: ServiceKind,
        interval: Duration,
    ) -> PlacementWatchTask
    where
        L: SingletonControl,
    {
        let coordinator = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            loop {
                interval.tick().await;
                if let Err(error) = coordinator
                    .reconcile_expired_instances(service_kind.clone())
                    .await
                {
                    warn!(
                        service.kind = service_kind.as_str(),
                        error = %error,
                        "lease-expiry reconciliation failed"
                    );
                }
            }
        });
        PlacementWatchTask::new(handle)
    }

    pub fn start_all_service_lease_expiry_reconciler(
        &self,
        interval: Duration,
    ) -> PlacementWatchTask
    where
        L: SingletonControl,
    {
        let coordinator = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(interval);
            loop {
                interval.tick().await;
                if let Err(error) = coordinator.reconcile_all_expired_instances().await {
                    warn!(
                        error = %error,
                        "all-service lease-expiry reconciliation failed"
                    );
                }
            }
        });
        PlacementWatchTask::new(handle)
    }
}
