use async_trait::async_trait;
use etcd_client::{Compare, CompareOp, GetOptions, SortOrder, SortTarget, Txn, TxnOp};
use lattice_model::run::{
    ClosingStage, ClusterLifecycle, ControlOperationId, RunCompletion, RunPhase,
};
use serde::{Deserialize, Serialize};

use super::{
    EtcdCoordinationStore, encode, maintenance::reviewed_runtime_key, transactions::leader_compares,
};
use crate::{
    coordinator::ClusterLeaderGuard,
    shutdown::{
        ClusterShutdownStore, NodeStopOutcome, ShutdownManifest, ShutdownProgress, ShutdownReport,
        extend_digest, participant_key,
    },
    storage::{ClusterLifecycleStore, StorageError},
    types::NodeKey,
};

const PAGE_SIZE: usize = 32;

/// Sealed identities and successful stop proofs never change during Draining.
/// Therefore a verified prefix can safely survive coordinator failover.
#[derive(Default, Serialize, Deserialize)]
struct StopVerification {
    cursor: Option<String>,
    digest: [u8; 32],
    count: u64,
}

impl EtcdCoordinationStore {
    async fn closing_run(
        &self,
        operation: &ControlOperationId,
        stage: ClosingStage,
    ) -> Result<ClusterLifecycle, StorageError> {
        let run = self.lifecycle().await?;
        if run.epoch != self.bound_epoch()? {
            return Err(StorageError::RunMismatch);
        }
        if !matches!(&run.phase, RunPhase::Closing { operation: active, stage: actual } if active == operation && *actual == stage)
        {
            return Err(StorageError::RunNotRunning);
        }
        Ok(run)
    }

    async fn shutdown_commit(
        &self,
        guard: &ClusterLeaderGuard,
        run: &ClusterLifecycle,
        mut compares: Vec<Compare>,
        operations: Vec<TxnOp>,
    ) -> Result<(), StorageError> {
        compares.extend(leader_compares(self, guard)?);
        compares.push(Compare::value(
            self.root_key("meta/lifecycle"),
            CompareOp::Equal,
            encode(run)?,
        ));
        let mut client = self.client.clone();
        let response = self
            .write_deadline(client.txn(Txn::new().when(compares).and_then(operations)))
            .await?;
        if response.succeeded() {
            Ok(())
        } else {
            Err(StorageError::CompareFailed)
        }
    }

    async fn obligation_page(
        &self,
        after: Option<&str>,
        limit: usize,
        revision: i64,
    ) -> Result<(i64, Vec<(String, NodeKey)>, bool), StorageError> {
        if limit == 0 || limit > PAGE_SIZE {
            return Err(StorageError::InvalidConfig);
        }
        let prefix = self.key("shutdown/obligations/");
        let mut start = prefix.as_bytes().to_vec();
        if let Some(after) = after {
            start.extend_from_slice(after.as_bytes());
            start.push(0);
        }
        let mut end = prefix.as_bytes().to_vec();
        *end.last_mut().expect("nonempty prefix") += 1;
        let options = GetOptions::new()
            .with_range(end)
            .with_limit(limit as i64)
            .with_revision(revision)
            .with_sort(SortTarget::Key, SortOrder::Ascend);
        let mut client = self.client.clone();
        let response = self.read_deadline(client.get(start, Some(options))).await?;
        let values = response
            .kvs()
            .iter()
            .map(|kv| {
                let key = std::str::from_utf8(kv.key())
                    .map_err(|_| StorageError::InvalidRecord)?
                    .strip_prefix(&prefix)
                    .ok_or(StorageError::InvalidRecord)?
                    .to_owned();
                let node =
                    serde_json::from_slice(kv.value()).map_err(|_| StorageError::InvalidRecord)?;
                Ok((key, node))
            })
            .collect::<Result<Vec<_>, StorageError>>()?;
        Ok((
            response
                .header()
                .ok_or(StorageError::InvalidRecord)?
                .revision(),
            values,
            response.more(),
        ))
    }
}

#[async_trait]
impl ClusterShutdownStore for EtcdCoordinationStore {
    async fn record_node_stopped(
        &self,
        guard: &ClusterLeaderGuard,
        node: &NodeKey,
    ) -> Result<(), StorageError> {
        let run = self.lifecycle().await?;
        if run.epoch != guard.record().epoch {
            return Err(StorageError::RunMismatch);
        }
        let running = run.is_running();
        if !running
            && !matches!(
                run.phase,
                RunPhase::Closing {
                    stage: ClosingStage::Draining,
                    ..
                }
            )
        {
            return Err(StorageError::RunNotRunning);
        }
        let id = participant_key(node);
        let key = self.key(&format!("shutdown/obligations/{id}"));
        let existing: Option<NodeKey> = self.get_json_key(&key).await?;
        if existing.as_ref().is_some_and(|expected| expected != node) {
            return Err(StorageError::CompareFailed);
        }
        let mut compares = vec![];
        let operations = if running {
            compares.push(Compare::version(
                self.key(&format!("membership/members/{}", node.node_id)),
                CompareOp::Equal,
                0,
            ));
            if existing.is_some() {
                compares.push(Compare::value(key.clone(), CompareOp::Equal, encode(node)?));
            } else {
                compares.push(Compare::version(key.clone(), CompareOp::Equal, 0));
            }
            vec![TxnOp::delete(key, None)]
        } else {
            compares.push(Compare::value(key, CompareOp::Equal, encode(node)?));
            vec![TxnOp::put(
                self.key(&format!("shutdown/stopped/{id}")),
                encode(node)?,
                None,
            )]
        };
        self.shutdown_commit(guard, &run, compares, operations)
            .await
    }
    async fn build_shutdown_manifest(
        &self,
        guard: &ClusterLeaderGuard,
        operation: &ControlOperationId,
    ) -> Result<ShutdownManifest, StorageError> {
        let run = self.closing_run(operation, ClosingStage::Draining).await?;
        let key = self.key("shutdown/manifest");
        let previous: Option<ShutdownManifest> = self.get_json_key(&key).await?;
        if let Some(manifest) = &previous {
            if manifest.operation != *operation || manifest.epoch != run.epoch {
                return Err(StorageError::CompareFailed);
            }
            if manifest.sealed {
                return Ok(manifest.clone());
            }
        }
        let mut manifest = previous.clone().unwrap_or(ShutdownManifest {
            epoch: run.epoch,
            operation: operation.clone(),
            source_revision: 0,
            cursor: None,
            participant_count: 0,
            digest: [0; 32],
            sealed: false,
        });
        // A compacted source fails without sealing or issuing stop effects. An
        // operator/recovery driver can restart the unsealed scan explicitly.
        let page = self
            .obligation_page(
                manifest.cursor.as_deref(),
                PAGE_SIZE,
                manifest.source_revision,
            )
            .await;
        let (revision, nodes, more) = match page {
            Ok(page) => page,
            Err(error) => {
                // Reset only unsealed build state. The frozen identity source
                // cannot acquire new participants after the Closing boundary.
                if previous.is_some() {
                    self.shutdown_commit(
                        guard,
                        &run,
                        vec![Compare::value(
                            key.clone(),
                            CompareOp::Equal,
                            encode(previous.as_ref().unwrap())?,
                        )],
                        vec![TxnOp::delete(key, None)],
                    )
                    .await?;
                }
                return Err(error);
            }
        };
        if manifest.source_revision == 0 {
            manifest.source_revision = revision;
        }
        for (key, node) in nodes {
            manifest.digest = extend_digest(manifest.digest, &key, &node)?;
            manifest.participant_count += 1;
            manifest.cursor = Some(key);
        }
        manifest.sealed = !more;
        let compare = match previous {
            Some(old) => Compare::value(key.clone(), CompareOp::Equal, encode(&old)?),
            None => Compare::version(key.clone(), CompareOp::Equal, 0),
        };
        self.shutdown_commit(
            guard,
            &run,
            vec![compare],
            vec![TxnOp::put(key, encode(&manifest)?, None)],
        )
        .await?;
        Ok(manifest)
    }

    async fn shutdown_participants(
        &self,
        operation: &ControlOperationId,
        after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<(String, NodeKey)>, StorageError> {
        self.closing_run(operation, ClosingStage::Draining).await?;
        let manifest: ShutdownManifest = self
            .get_json_key(&self.key("shutdown/manifest"))
            .await?
            .ok_or(StorageError::CompareFailed)?;
        if !manifest.sealed || manifest.operation != *operation {
            return Err(StorageError::CompareFailed);
        }
        // Identities are immutable after Closing, so compaction after sealing
        // does not require retained historical reads.
        Ok(self.obligation_page(after, limit, 0).await?.1)
    }

    async fn record_shutdown_report(
        &self,
        guard: &ClusterLeaderGuard,
        report: ShutdownReport,
    ) -> Result<(), StorageError> {
        let run = self
            .closing_run(&report.operation, ClosingStage::Draining)
            .await?;
        if report.epoch != run.epoch {
            return Err(StorageError::RunMismatch);
        }
        if matches!(&report.outcome, NodeStopOutcome::Blocked { reason } if reason.len() > 1024) {
            return Err(StorageError::InvalidRecord);
        }
        let manifest_key = self.key("shutdown/manifest");
        let manifest: ShutdownManifest = self
            .get_json_key(&manifest_key)
            .await?
            .ok_or(StorageError::CompareFailed)?;
        if !manifest.sealed || manifest.operation != report.operation {
            return Err(StorageError::CompareFailed);
        }
        let id = participant_key(&report.node);
        let obligation = self.key(&format!("shutdown/obligations/{id}"));
        let key = self.key(&format!("shutdown/reports/{id}"));
        let previous: Option<ShutdownReport> = self.get_json_key(&key).await?;
        if previous
            .as_ref()
            .is_some_and(|old| old.outcome == NodeStopOutcome::Stopped)
        {
            return if previous.as_ref() == Some(&report) {
                Ok(())
            } else {
                Err(StorageError::CompareFailed)
            };
        }
        let compare = match previous {
            Some(old) => Compare::value(key.clone(), CompareOp::Equal, encode(&old)?),
            None => Compare::version(key.clone(), CompareOp::Equal, 0),
        };
        self.shutdown_commit(
            guard,
            &run,
            vec![
                compare,
                Compare::value(obligation, CompareOp::Equal, encode(&report.node)?),
                Compare::value(manifest_key, CompareOp::Equal, encode(&manifest)?),
            ],
            vec![TxnOp::put(key, encode(&report)?, None)],
        )
        .await
    }

    async fn begin_shutdown_cleanup(
        &self,
        guard: &ClusterLeaderGuard,
        operation: &ControlOperationId,
    ) -> Result<(), StorageError> {
        let run = self.closing_run(operation, ClosingStage::Draining).await?;
        let manifest_key = self.key("shutdown/manifest");
        let manifest: ShutdownManifest = self
            .get_json_key(&manifest_key)
            .await?
            .ok_or(StorageError::CompareFailed)?;
        if !manifest.sealed || manifest.operation != *operation {
            return Err(StorageError::CompareFailed);
        }
        let verification_key = self.key("shutdown/verification");
        let previous: Option<StopVerification> = self.get_json_key(&verification_key).await?;
        let verification_compare = match &previous {
            Some(previous) => Compare::value(
                verification_key.clone(),
                CompareOp::Equal,
                encode(previous)?,
            ),
            None => Compare::version(verification_key.clone(), CompareOp::Equal, 0),
        };
        let mut verification = previous.unwrap_or_default();
        let (_, nodes, more) = self
            .obligation_page(verification.cursor.as_deref(), PAGE_SIZE, 0)
            .await?;
        for (id, node) in nodes {
            let expected = ShutdownReport {
                epoch: run.epoch,
                operation: operation.clone(),
                node: node.clone(),
                outcome: NodeStopOutcome::Stopped,
            };
            let actual: Option<ShutdownReport> = self
                .get_json_key(&self.key(&format!("shutdown/reports/{id}")))
                .await?;
            if actual.as_ref() != Some(&expected) {
                let stopped: Option<NodeKey> = self
                    .get_json_key(&self.key(&format!("shutdown/stopped/{id}")))
                    .await?;
                if stopped.as_ref() != Some(&node) {
                    return Err(StorageError::ShutdownBlocked);
                }
            }
            verification.digest = extend_digest(verification.digest, &id, &node)?;
            verification.count += 1;
            verification.cursor = Some(id);
        }
        if more {
            self.shutdown_commit(
                guard,
                &run,
                vec![
                    verification_compare,
                    Compare::value(manifest_key, CompareOp::Equal, encode(&manifest)?),
                ],
                vec![TxnOp::put(verification_key, encode(&verification)?, None)],
            )
            .await?;
            return Err(StorageError::ShutdownBlocked);
        }
        if verification.digest != manifest.digest
            || verification.count != manifest.participant_count
        {
            return Err(StorageError::InvalidRecord);
        }
        let next = ClusterLifecycle {
            epoch: run.epoch,
            phase: RunPhase::Closing {
                operation: operation.clone(),
                stage: ClosingStage::Cleaning,
            },
        };
        self.shutdown_commit(
            guard,
            &run,
            vec![
                verification_compare,
                Compare::value(manifest_key, CompareOp::Equal, encode(&manifest)?),
            ],
            vec![TxnOp::put(
                self.root_key("meta/lifecycle"),
                encode(&next)?,
                None,
            )],
        )
        .await
    }

    async fn shutdown_cleanup_batch(
        &self,
        guard: &ClusterLeaderGuard,
        operation: &ControlOperationId,
        maximum_keys: usize,
    ) -> Result<ShutdownProgress, StorageError> {
        if maximum_keys == 0 || maximum_keys > PAGE_SIZE {
            return Err(StorageError::InvalidConfig);
        }
        let current = self.lifecycle().await?;
        if current.epoch == guard.record().epoch
            && matches!(&current.phase, RunPhase::Closed { operation: active, completion:RunCompletion::Graceful } if active == operation)
        {
            return Ok(ShutdownProgress {
                lifecycle: current,
                manifest: None,
                complete: true,
            });
        }
        let run = self.closing_run(operation, ClosingStage::Cleaning).await?;
        let prefix = self.run_key(run.epoch, "");
        let mut client = self.client.clone();
        // Keep exact cluster leader and its online candidacy until final CAS.
        // The scan includes two protected keys in addition to its delete budget.
        let page = self
            .read_deadline(
                client.get(
                    prefix.clone(),
                    Some(
                        GetOptions::new()
                            .with_prefix()
                            .with_limit((maximum_keys + 2) as i64)
                            .with_sort(SortTarget::Key, SortOrder::Ascend),
                    ),
                ),
            )
            .await?;
        let mut compares = Vec::new();
        let mut deletes = Vec::new();
        let mut protected = Vec::new();
        for kv in page.kvs() {
            if !reviewed_runtime_key(kv.key(), prefix.as_bytes()) {
                return Err(StorageError::UnrecognizedRuntimeKey);
            }
            let suffix = std::str::from_utf8(kv.key())
                .map_err(|_| StorageError::InvalidRecord)?
                .strip_prefix(&prefix)
                .ok_or(StorageError::InvalidRecord)?;
            if suffix == "membership/leader"
                || kv.key()
                    == self
                        .candidate_registration_key(
                            &guard.record().scope,
                            &guard.record().node.node_id,
                        )
                        .as_bytes()
            {
                protected.push(kv.key().to_vec());
                continue;
            }
            if deletes.len() == maximum_keys {
                break;
            }
            compares.push(Compare::mod_revision(
                kv.key(),
                CompareOp::Equal,
                kv.mod_revision(),
            ));
            deletes.push(TxnOp::delete(kv.key(), None));
        }
        let complete = deletes.is_empty() && !page.more();
        let next = if complete {
            // Any new key after this scan invalidates completion; no stale
            // scan can conceal a concurrent candidate/election write.
            compares.push(
                Compare::mod_revision(
                    prefix,
                    CompareOp::Less,
                    page.header().ok_or(StorageError::InvalidRecord)?.revision() + 1,
                )
                .with_prefix(),
            );
            deletes.extend(protected.into_iter().map(|key| TxnOp::delete(key, None)));
            let closed = ClusterLifecycle {
                epoch: run.epoch,
                phase: RunPhase::Closed {
                    operation: operation.clone(),
                    completion: RunCompletion::Graceful,
                },
            };
            deletes.push(TxnOp::put(
                self.root_key("meta/lifecycle"),
                encode(&closed)?,
                None,
            ));
            deletes.push(TxnOp::put(
                self.root_key("meta/last_completion"),
                encode(&closed)?,
                None,
            ));
            closed
        } else {
            run.clone()
        };
        self.shutdown_commit(guard, &run, compares, deletes).await?;
        Ok(ShutdownProgress {
            lifecycle: next,
            manifest: None,
            complete,
        })
    }
}
