use super::transactions::leader_compares;
use super::{EtcdCoordinationStore, encode};
use crate::{
    coordinator::ClusterLeaderGuard,
    storage::{ClusterLifecycleStore, StorageError},
};
use async_trait::async_trait;
use etcd_client::{Compare, CompareOp, Txn, TxnOp};
use lattice_model::{
    framework::LatticeVersion,
    run::{ClosingStage, ClusterLifecycle, ControlOperationId, RunEpoch, RunPhase},
};

impl EtcdCoordinationStore {
    pub(super) async fn ensure_framework_inner(&self) -> Result<RunEpoch, StorageError> {
        let framework_key = self.root_key("meta/framework");
        let lifecycle = ClusterLifecycle::initial();
        let limits_key = self.root_key("schema/limits");
        let revision_key = self.run_key(lifecycle.epoch, "membership/state_revision");
        let expected_limits = encode(&self.limits)?;
        let counter_keys = [self.run_key(lifecycle.epoch, "membership/counters/members")];
        if self.read_raw(&framework_key).await?.is_some() {
            return self.validate_framework_metadata(&expected_limits).await;
        }
        // Only an entirely empty namespace may be initialized. Existing legacy
        // keys, missing metadata, and foreign identities are never overwritten.
        let compares = vec![Compare::version(self.root_key(""), CompareOp::Equal, 0).with_prefix()];
        let mut puts = vec![
            TxnOp::put(framework_key, LatticeVersion::CURRENT, None),
            TxnOp::put(self.root_key("meta/lifecycle"), encode(&lifecycle)?, None),
            TxnOp::put(limits_key.clone(), expected_limits.clone(), None),
        ];
        for key in &counter_keys {
            puts.push(TxnOp::put(key.clone(), "0", None));
        }
        puts.push(TxnOp::put(revision_key, "1", None));
        let mut client = self.client.clone();
        let response = self
            .write_deadline(client.txn(Txn::new().when(compares).and_then(puts)))
            .await?;
        if response.succeeded() {
            return self.bind_epoch(lifecycle.epoch);
        }

        // A matching concurrent initializer may have won the empty-prefix CAS.
        self.validate_framework_metadata(&expected_limits).await
    }

    async fn validate_framework_metadata(
        &self,
        expected_limits: &[u8],
    ) -> Result<RunEpoch, StorageError> {
        if self
            .read_raw(&self.root_key("meta/framework"))
            .await?
            .is_none_or(|(value, _, _)| value != LatticeVersion::CURRENT.as_bytes())
        {
            return Err(StorageError::FrameworkMismatch);
        }
        // Transitional or legacy schemas are not supported, even if someone has
        // manually stamped the current version onto them.
        if self
            .read_raw(&self.root_key("schema_generation"))
            .await?
            .is_some()
        {
            return Err(StorageError::FrameworkMismatch);
        }
        if self
            .read_raw(&self.root_key("schema/limits"))
            .await?
            .is_none_or(|(value, _, _)| value != expected_limits)
        {
            return Err(StorageError::StorageMetadataMismatch);
        }
        let lifecycle = self.lifecycle().await?;
        let epoch = lifecycle.epoch;
        if !lifecycle.permits_election() {
            return Err(StorageError::RunNotRunning);
        }
        if !matches!(
            lifecycle.phase,
            RunPhase::Closing {
                stage: ClosingStage::Cleaning,
                ..
            }
        ) && (self
            .read_raw(&self.run_key(epoch, "membership/counters/members"))
            .await?
            .is_none()
            || self
                .read_raw(&self.run_key(epoch, "membership/state_revision"))
                .await?
                .is_none())
        {
            return Err(StorageError::StorageMetadataMismatch);
        }
        self.bind_epoch(epoch)
    }
}

#[async_trait]
impl ClusterLifecycleStore for EtcdCoordinationStore {
    async fn lifecycle(&self) -> Result<ClusterLifecycle, StorageError> {
        self.get_json_key(&self.root_key("meta/lifecycle"))
            .await?
            .ok_or(StorageError::StorageMetadataMismatch)
    }

    async fn begin_shutdown(
        &self,
        guard: &ClusterLeaderGuard,
        operation: ControlOperationId,
    ) -> Result<ClusterLifecycle, StorageError> {
        let epoch = self.bound_epoch()?;
        let current = self.lifecycle().await?;
        if epoch != guard.record().epoch {
            return Err(StorageError::RunMismatch);
        }
        if current.epoch != guard.record().epoch {
            return Err(StorageError::RunMismatch);
        }
        let next = current
            .closing(operation)
            .map_err(|_| StorageError::RunNotRunning)?;
        let mut compares = leader_compares(self, guard)?.to_vec();
        compares.push(Compare::value(
            self.root_key("meta/lifecycle"),
            CompareOp::Equal,
            encode(&current)?,
        ));
        let mut client = self.client.clone();
        let result = self
            .write_deadline(client.txn(Txn::new().when(compares).and_then([TxnOp::put(
                self.root_key("meta/lifecycle"),
                encode(&next)?,
                None,
            )])))
            .await?;
        if result.succeeded() {
            Ok(next)
        } else {
            Err(StorageError::CompareFailed)
        }
    }
}
