use async_trait::async_trait;
use etcd_client::{Compare, CompareOp, GetOptions, SortOrder, SortTarget, Txn, TxnOp};
use lattice_model::{
    framework::LatticeVersion,
    run::{ClusterLifecycle, ControlOperationId, RunCompletion, RunEpoch, RunPhase},
};

use super::{EtcdCoordinationStore, encode};
use crate::storage::{
    ClusterLifecycleStore, StorageError,
    maintenance::{ClusterResetStore, ResetProgress, StoppedDeployment},
};

/// Keeps both compares and delete operations comfortably below etcd's default
/// transaction operation ceiling. Large namespaces take more resumable batches.
const MAX_RESET_BATCH_KEYS: usize = 32;

#[async_trait]
impl ClusterResetStore for EtcdCoordinationStore {
    async fn begin_reset(
        &self,
        expected: &ClusterLifecycle,
        operation: ControlOperationId,
        confirmation: &StoppedDeployment,
    ) -> Result<ClusterLifecycle, StorageError> {
        if confirmation.namespace != self.prefix || confirmation.epoch != expected.epoch {
            return Err(StorageError::ResetConfirmationRequired);
        }
        let current = self.lifecycle().await?;
        if current.epoch == expected.epoch
            && matches!(&current.phase,
                RunPhase::Resetting { operation: active }
                | RunPhase::Closed { operation: active, completion: RunCompletion::Reset }
                if active == &operation)
        {
            return Ok(current);
        }
        if current != *expected {
            return Err(StorageError::CompareFailed);
        }
        if matches!(
            current.phase,
            RunPhase::Closed { .. } | RunPhase::Resetting { .. }
        ) {
            return Err(StorageError::RunNotRunning);
        }
        let next = ClusterLifecycle {
            epoch: current.epoch,
            phase: RunPhase::Resetting { operation },
        };
        self.maintenance_commit(
            &current,
            Vec::new(),
            vec![TxnOp::put(
                self.root_key("meta/lifecycle"),
                encode(&next)?,
                None,
            )],
        )
        .await?;
        Ok(next)
    }

    async fn reset_batch(
        &self,
        epoch: RunEpoch,
        operation: &ControlOperationId,
        maximum_keys: usize,
    ) -> Result<ResetProgress, StorageError> {
        if maximum_keys == 0 || maximum_keys > MAX_RESET_BATCH_KEYS {
            return Err(StorageError::InvalidConfig);
        }
        let current = self.lifecycle().await?;
        if current.epoch != epoch {
            return Err(StorageError::RunMismatch);
        }
        if matches!(&current.phase, RunPhase::Closed { operation: active, completion: RunCompletion::Reset } if active == operation)
        {
            return Ok(ResetProgress {
                operation: operation.clone(),
                epoch,
                deleted_keys: 0,
                complete: true,
            });
        }
        if !matches!(&current.phase, RunPhase::Resetting { operation: active } if active == operation)
        {
            return Err(StorageError::RunNotRunning);
        }
        let prefix = self.run_key(epoch, "");
        let mut client = self.client.clone();
        let page = self
            .read_deadline(
                client.get(
                    prefix.clone(),
                    Some(
                        GetOptions::new()
                            .with_prefix()
                            .with_limit(maximum_keys as i64)
                            .with_sort(SortTarget::Key, SortOrder::Ascend),
                    ),
                ),
            )
            .await?;
        // etcd removes leased keys on expiry. An extant leased key is not stop
        // evidence, even when the operator says the deployment has stopped.
        if page.kvs().iter().any(|record| record.lease() != 0) {
            return Err(StorageError::ResetLiveLease);
        }
        let mut compares = Vec::new();
        let mut operations = Vec::new();
        for record in page.kvs() {
            if !reviewed_runtime_key(record.key(), prefix.as_bytes()) {
                return Err(StorageError::UnrecognizedRuntimeKey);
            }
            compares.push(Compare::mod_revision(
                record.key(),
                CompareOp::Equal,
                record.mod_revision(),
            ));
            operations.push(TxnOp::delete(record.key(), None));
        }
        let complete = page.kvs().is_empty();
        if complete {
            // Completion is a guarded empty-range assertion, not the result of
            // an earlier scan. A concurrent or delayed writer cannot slip in.
            compares.push(Compare::version(prefix, CompareOp::Equal, 0).with_prefix());
            let closed = ClusterLifecycle {
                epoch,
                phase: RunPhase::Closed {
                    operation: operation.clone(),
                    completion: RunCompletion::Reset,
                },
            };
            operations.push(TxnOp::put(
                self.root_key("meta/lifecycle"),
                encode(&closed)?,
                None,
            ));
            // One bounded confirmation survives new-run initialization.
            operations.push(TxnOp::put(
                self.root_key("meta/last_completion"),
                encode(&closed)?,
                None,
            ));
        }
        self.maintenance_commit(&current, compares, operations)
            .await?;
        Ok(ResetProgress {
            operation: operation.clone(),
            epoch,
            deleted_keys: page.kvs().len(),
            complete,
        })
    }

    async fn start_new_run(&self, expected: &ClusterLifecycle) -> Result<RunEpoch, StorageError> {
        if !matches!(expected.phase, RunPhase::Closed { .. }) {
            return Err(StorageError::RunNotRunning);
        }
        let epoch = expected
            .epoch
            .next()
            .map_err(|_| StorageError::CounterExhausted)?;
        let next = ClusterLifecycle {
            epoch,
            phase: RunPhase::Running,
        };
        let current = self.lifecycle().await?;
        let completion: Option<ClusterLifecycle> = self
            .get_json_key(&self.root_key("meta/last_completion"))
            .await?;
        if completion.as_ref() != Some(expected) {
            return Err(StorageError::CompareFailed);
        }
        if current == next {
            return Ok(epoch);
        }
        if current != *expected {
            return Err(StorageError::CompareFailed);
        }
        self.maintenance_commit(
            expected,
            vec![
                Compare::version(self.run_key(expected.epoch, ""), CompareOp::Equal, 0)
                    .with_prefix(),
                Compare::version(self.run_key(epoch, ""), CompareOp::Equal, 0).with_prefix(),
                Compare::value(
                    self.root_key("meta/last_completion"),
                    CompareOp::Equal,
                    encode(expected)?,
                ),
            ],
            vec![
                TxnOp::put(self.root_key("meta/lifecycle"), encode(&next)?, None),
                TxnOp::put(self.run_key(epoch, "membership/state_revision"), "1", None),
                TxnOp::put(
                    self.run_key(epoch, "membership/counters/members"),
                    "0",
                    None,
                ),
            ],
        )
        .await?;
        Ok(epoch)
    }
}

impl EtcdCoordinationStore {
    async fn maintenance_commit(
        &self,
        expected: &ClusterLifecycle,
        mut compares: Vec<Compare>,
        operations: Vec<TxnOp>,
    ) -> Result<(), StorageError> {
        compares.extend([
            Compare::value(
                self.root_key("meta/framework"),
                CompareOp::Equal,
                LatticeVersion::CURRENT,
            ),
            Compare::value(
                self.root_key("meta/lifecycle"),
                CompareOp::Equal,
                encode(expected)?,
            ),
        ]);
        let mut client = self.client.clone();
        let result = self
            .write_deadline(client.txn(Txn::new().when(compares).and_then(operations)))
            .await?;
        if result.succeeded() {
            Ok(())
        } else {
            Err(StorageError::CompareFailed)
        }
    }
}

/// Cleanup is a reviewed-family allowlist, never a generic namespace eraser.
/// An unexpected family requires inspection instead of silently extending scope.
pub(super) fn reviewed_runtime_key(key: &[u8], prefix: &[u8]) -> bool {
    let Some(suffix) = key
        .strip_prefix(prefix)
        .and_then(|bytes| std::str::from_utf8(bytes).ok())
    else {
        return false;
    };
    let parts: Vec<_> = suffix.split('/').collect();
    match parts.as_slice() {
        ["shutdown", "manifest" | "verification"] => true,
        ["shutdown", "obligations" | "reports" | "stopped", identity] => !identity.is_empty(),
        ["groups", group, "transfers", "reservations", slot] => {
            !group.is_empty() && !slot.is_empty()
        }
        ["groups", group, "transfers", "capacity", "group"] => !group.is_empty(),
        [
            "groups",
            group,
            "transfers",
            "capacity",
            "source" | "target" | "entity",
            identity,
        ] => !group.is_empty() && !identity.is_empty(),
        [
            "groups",
            group,
            "transfers",
            "barriers",
            slot,
            operation,
            "manifest",
        ] => !group.is_empty() && !slot.is_empty() && !operation.is_empty(),
        [
            "groups",
            group,
            "transfers",
            "barriers",
            slot,
            operation,
            "participants",
            incarnation,
        ] => {
            !group.is_empty()
                && !slot.is_empty()
                && !operation.is_empty()
                && !incarnation.is_empty()
        }
        ["groups", group, "transfers", "plans", plan, "moves", index] => {
            !group.is_empty() && !plan.is_empty() && !index.is_empty()
        }
        ["membership", "leader" | "state_revision"] => true,
        ["candidates", "cluster", node] => !node.is_empty(),
        ["candidates", "groups", group, node] => !group.is_empty() && !node.is_empty(),
        ["membership", "members" | "counters", name] => !name.is_empty(),
        ["groups", group, "leader" | "state_revision"] => !group.is_empty(),
        ["groups", group, "shards" | "shard_claims", kind, shard] => {
            !group.is_empty() && !kind.is_empty() && !shard.is_empty()
        }
        [
            "groups",
            group,
            "singletons" | "singleton_claims" | "members" | "rebalances" | "admin" | "counters",
            name,
        ] => !group.is_empty() && !name.is_empty(),
        _ => false,
    }
}
