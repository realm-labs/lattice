//! Small plan metadata plus one bounded record per admitted move.
//! The metadata's digest is the atomic publication marker for its move records.

use etcd_client::{GetOptions, SortOrder, SortTarget, TxnOp};

use crate::storage::plan_records::PlanMetadata;

use super::{EtcdCoordinationStore, decode, encode};
use crate::{
    plan::{MAXIMUM_PLAN_MOVES, RebalanceMove, RebalancePlan},
    storage::{StorageError, records::MAX_DURABLE_VALUE_BYTES},
};

fn move_prefix(store: &EtcdCoordinationStore, plan: &RebalancePlan) -> String {
    store.key(&format!(
        "groups/{}/transfers/plans/{:032x}/moves/",
        plan.group.as_str(),
        plan.plan_id
    ))
}

fn bounded(value: Vec<u8>) -> Result<Vec<u8>, StorageError> {
    if value.len() > MAX_DURABLE_VALUE_BYTES {
        return Err(StorageError::Capacity);
    }
    Ok(value)
}

pub(super) fn plan_puts(
    store: &EtcdCoordinationStore,
    plan: &RebalancePlan,
) -> Result<Vec<TxnOp>, StorageError> {
    let Some(record) = plan.durable() else {
        return Ok(Vec::new());
    };
    let plan = &record;
    if plan.moves.is_empty() || plan.moves.len() > MAXIMUM_PLAN_MOVES {
        return Err(StorageError::Capacity);
    }
    let metadata = PlanMetadata::capture(plan)?;
    let mut operations = vec![TxnOp::put(
        store.plan_key(&plan.group, plan.plan_id),
        bounded(encode(&metadata)?)?,
        None,
    )];
    let prefix = move_prefix(store, plan);
    for (index, movement) in plan.moves.iter().enumerate() {
        operations.push(TxnOp::put(
            format!("{prefix}{index:08x}"),
            bounded(encode(movement)?)?,
            None,
        ));
    }
    Ok(operations)
}

pub(super) fn plan_deletes(
    store: &EtcdCoordinationStore,
    plan: &RebalancePlan,
) -> Result<Vec<TxnOp>, StorageError> {
    let Some(record) = plan.durable() else {
        return Ok(Vec::new());
    };
    let plan = &record;
    if plan.moves.len() > MAXIMUM_PLAN_MOVES {
        return Err(StorageError::Capacity);
    }
    let prefix = move_prefix(store, plan);
    let mut operations = vec![TxnOp::delete(
        store.plan_key(&plan.group, plan.plan_id),
        None,
    )];
    operations.extend(
        (0..plan.moves.len()).map(|index| TxnOp::delete(format!("{prefix}{index:08x}"), None)),
    );
    Ok(operations)
}

impl EtcdCoordinationStore {
    pub(super) async fn hydrate_plan(
        &self,
        bytes: &[u8],
        revision: i64,
    ) -> Result<RebalancePlan, StorageError> {
        let metadata: PlanMetadata = decode(bytes)?;
        let count = metadata.move_count;
        let digest = metadata.moves_digest;
        if count == 0 || count > MAXIMUM_PLAN_MOVES {
            return Err(StorageError::Capacity);
        }
        let mut plan = metadata.into_plan();
        let prefix = move_prefix(self, &plan);
        let mut client = self.client.clone();
        let response = self
            .read_deadline(
                client.get(
                    prefix.clone(),
                    Some(
                        GetOptions::new()
                            .with_prefix()
                            .with_revision(revision)
                            .with_limit((MAXIMUM_PLAN_MOVES + 1) as i64)
                            .with_sort(SortTarget::Key, SortOrder::Ascend),
                    ),
                ),
            )
            .await?;
        if response.more() || response.kvs().len() != count {
            return Err(StorageError::StorageMetadataMismatch);
        }
        let mut moves = Vec::with_capacity(count);
        for (index, kv) in response.kvs().iter().enumerate() {
            if kv.key() != format!("{prefix}{index:08x}").as_bytes()
                || kv.value().len() > MAX_DURABLE_VALUE_BYTES
            {
                return Err(StorageError::StorageMetadataMismatch);
            }
            moves.push(decode::<RebalanceMove>(kv.value())?);
        }
        let actual =
            *blake3::hash(&serde_json::to_vec(&moves).map_err(|_| StorageError::Codec)?).as_bytes();
        if actual != digest {
            return Err(StorageError::StorageMetadataMismatch);
        }
        plan.moves = moves;
        Ok(plan)
    }

    pub(super) async fn read_plan_record(
        &self,
        key: &str,
    ) -> Result<Option<(RebalancePlan, i64)>, StorageError> {
        let mut client = self.client.clone();
        let response = self.read_deadline(client.get(key, None)).await?;
        let Some(kv) = response.kvs().first() else {
            return Ok(None);
        };
        let revision = response.header().ok_or(StorageError::Codec)?.revision();
        Ok(Some((
            self.hydrate_plan(kv.value(), revision).await?,
            kv.mod_revision(),
        )))
    }
}
