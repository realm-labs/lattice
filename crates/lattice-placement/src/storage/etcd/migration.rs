use std::io::Write;
use std::path::PathBuf;

use etcd_client::{
    Client, Compare, CompareOp, GetOptions, PutOptions, SortOrder, SortTarget, Txn, TxnOp,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::STORAGE_SCHEMA_GENERATION;
use crate::plan::{MoveProgress, PlanStatus, RebalancePlan};
use crate::storage::domain::DurableStorageLimits;
use crate::types::{ClaimGrant, PlacementSlot, PlacementSlotKey, PlacementSlotState, ShardId};

const MIGRATING_SCHEMA: &str = "migrating-to-4";
const MIGRATION_LOCK_TTL_SECONDS: i64 = 300;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationMode {
    Inspect,
    DryRun,
    Apply,
    Resume,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CardinalityMode {
    Inspect,
    Repair,
}

#[derive(Debug, Clone)]
pub struct MigrationConfig {
    pub endpoints: Vec<String>,
    pub cluster_prefix: String,
    pub page_size: usize,
    pub limits: DurableStorageLimits,
    /// Required for apply. Resume uses the backup recorded by apply.
    pub backup_path: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MigrationReport {
    pub mode: String,
    pub scanned_records: usize,
    pub transformed_records: usize,
    pub slots: usize,
    pub plans: usize,
    pub members: usize,
    pub admin_operations: usize,
    pub entity_configs: usize,
    pub singleton_configs: usize,
    pub state_revision: u64,
    pub quarantined_records: usize,
    pub completed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CardinalityReport {
    pub slots: usize,
    pub plans: usize,
    pub members: usize,
    pub admin_operations: usize,
    pub stored_slots: usize,
    pub stored_plans: usize,
    pub stored_members: usize,
    pub stored_admin_operations: usize,
    pub repaired: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct MigrationMarker {
    last_key: Option<String>,
    coordinator_term: u64,
    limits: DurableStorageLimits,
    completed: bool,
    backup_path: String,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("migration configuration is invalid")]
    InvalidConfig,
    #[error("generation-3 storage is required")]
    WrongGeneration,
    #[error("migration must run while no Coordinator leader exists")]
    LeaderPresent,
    #[error("migration marker does not match the requested limits or term")]
    MarkerMismatch,
    #[error("another migration holds the lease-backed migration lock")]
    Locked,
    #[error("a record cannot be converted to generation 4")]
    InvalidRecord,
    #[error("configured durable cardinality is too small for existing records")]
    Capacity,
    #[error("migration compare failed; rerun with resume after inspecting storage")]
    CompareFailed,
    #[error("etcd migration request failed")]
    Etcd(#[from] etcd_client::Error),
    #[error("migration backup/export failed")]
    Io(#[from] std::io::Error),
    #[error("migration JSON codec failed")]
    Codec(#[from] serde_json::Error),
}

pub async fn execute(
    mode: MigrationMode,
    config: MigrationConfig,
) -> Result<MigrationReport, MigrationError> {
    if config.endpoints.is_empty()
        || config.cluster_prefix.is_empty()
        || config.page_size == 0
        || !config.limits.validate()
    {
        return Err(MigrationError::InvalidConfig);
    }
    let mut client = Client::connect(&config.endpoints, None).await?;
    let schema_key = key(&config.cluster_prefix, "schema_generation");
    let schema = read_one(&mut client, &schema_key).await?;
    let schema_value = schema
        .as_ref()
        .map(|record| record.value.as_slice())
        .ok_or(MigrationError::WrongGeneration)?;
    let schema_allowed = match mode {
        MigrationMode::Apply => schema_value == b"3",
        MigrationMode::Resume => schema_value == MIGRATING_SCHEMA.as_bytes(),
        MigrationMode::Inspect | MigrationMode::DryRun => {
            schema_value == b"3" || schema_value == MIGRATING_SCHEMA.as_bytes()
        }
    };
    if !schema_allowed {
        return Err(MigrationError::WrongGeneration);
    }
    let leader_key = key(&config.cluster_prefix, "coordinator/leader");
    if read_one(&mut client, &leader_key).await?.is_some() {
        return Err(MigrationError::LeaderPresent);
    }
    let maximum_records = config
        .limits
        .maximum_slots
        .checked_mul(2)
        .and_then(|value| value.checked_add(config.limits.maximum_plans))
        .and_then(|value| value.checked_add(config.limits.maximum_members))
        .and_then(|value| value.checked_add(config.limits.maximum_admin_operations))
        .and_then(|value| value.checked_add(128))
        .ok_or(MigrationError::InvalidConfig)?;
    let records = scan_prefix(
        &mut client,
        &config.cluster_prefix,
        config.page_size,
        maximum_records,
        None,
    )
    .await?;
    let has_state_records = records.iter().any(|record| {
        record_suffix(&config.cluster_prefix, &record.key).is_ok_and(|suffix| {
            suffix.starts_with("members/")
                || suffix.starts_with("shards/")
                || suffix.starts_with("singletons/")
        })
    });
    let term_key = key(&config.cluster_prefix, "coordinator/term");
    let term_record = read_one(&mut client, &term_key).await?;
    let term = term_record
        .as_ref()
        .map(|record| parse_u64(&record.value))
        .transpose()?
        .unwrap_or(0);
    if has_state_records && term_record.is_none() {
        return Err(MigrationError::InvalidRecord);
    }
    let marker_key = key(&config.cluster_prefix, "migration/generation-3-to-4");
    let lock_key = key(&config.cluster_prefix, "migration/generation-3-to-4-lock");
    let existing_marker = read_one(&mut client, &marker_key).await?;
    let mut marker = existing_marker
        .as_ref()
        .map(|record| serde_json::from_slice::<MigrationMarker>(&record.value))
        .transpose()?;
    if marker
        .as_ref()
        .is_some_and(|marker| marker.coordinator_term != term || marker.limits != config.limits)
    {
        return Err(MigrationError::MarkerMismatch);
    }
    match mode {
        MigrationMode::Apply if marker.is_some() => return Err(MigrationError::MarkerMismatch),
        MigrationMode::Resume if marker.as_ref().is_none_or(|marker| marker.completed) => {
            return Err(MigrationError::MarkerMismatch);
        }
        MigrationMode::Inspect
        | MigrationMode::DryRun
        | MigrationMode::Apply
        | MigrationMode::Resume => {}
    }
    let mut lock = None;
    if matches!(mode, MigrationMode::Apply | MigrationMode::Resume) {
        let backup_path = if matches!(mode, MigrationMode::Apply) {
            let path = config
                .backup_path
                .as_ref()
                .ok_or(MigrationError::InvalidConfig)?;
            write_backup(path, &records)?;
            path.to_string_lossy().into_owned()
        } else {
            let recorded = marker
                .as_ref()
                .ok_or(MigrationError::MarkerMismatch)?
                .backup_path
                .clone();
            if config
                .backup_path
                .as_ref()
                .is_some_and(|path| path.to_string_lossy() != recorded)
            {
                return Err(MigrationError::MarkerMismatch);
            }
            recorded
        };
        let initial = MigrationMarker {
            last_key: None,
            coordinator_term: term,
            limits: config.limits,
            completed: false,
            backup_path,
        };
        let current_marker = if matches!(mode, MigrationMode::Apply) {
            initial.clone()
        } else {
            marker.clone().ok_or(MigrationError::MarkerMismatch)?
        };
        let lease_id = client
            .lease_grant(MIGRATION_LOCK_TTL_SECONDS, None)
            .await?
            .id();
        let lock_value = uuid::Uuid::new_v4().to_string();
        let mut compares = vec![
            Compare::version(leader_key.clone(), CompareOp::Equal, 0),
            Compare::version(lock_key.clone(), CompareOp::Equal, 0),
        ];
        compares.push(term_compare(&term_key, term_record.as_ref(), term));
        let mut puts = Vec::new();
        if matches!(mode, MigrationMode::Apply) {
            compares.push(Compare::value(schema_key.clone(), CompareOp::Equal, "3"));
            compares.push(Compare::version(marker_key.clone(), CompareOp::Equal, 0));
            puts.push(TxnOp::put(schema_key.clone(), MIGRATING_SCHEMA, None));
            puts.push(TxnOp::put(
                marker_key.clone(),
                serde_json::to_vec(&current_marker)?,
                None,
            ));
        } else {
            compares.push(Compare::value(
                schema_key.clone(),
                CompareOp::Equal,
                MIGRATING_SCHEMA,
            ));
            compares.push(Compare::value(
                marker_key.clone(),
                CompareOp::Equal,
                serde_json::to_vec(&current_marker)?,
            ));
        }
        puts.push(TxnOp::put(
            lock_key.clone(),
            lock_value.clone(),
            Some(PutOptions::new().with_lease(lease_id)),
        ));
        let response = client.txn(Txn::new().when(compares).and_then(puts)).await?;
        if !response.succeeded() {
            let _ = client.lease_revoke(lease_id).await;
            return if read_one(&mut client, &lock_key).await?.is_some() {
                Err(MigrationError::Locked)
            } else {
                Err(MigrationError::CompareFailed)
            };
        }
        marker = Some(current_marker);
        lock = Some((lease_id, lock_value));
    }

    let resume_after = if matches!(mode, MigrationMode::Resume) {
        marker.as_ref().and_then(|marker| marker.last_key.clone())
    } else {
        None
    };
    let mut report = MigrationReport {
        mode: format!("{mode:?}").to_lowercase(),
        scanned_records: 0,
        transformed_records: 0,
        slots: 0,
        plans: 0,
        members: 0,
        admin_operations: 0,
        entity_configs: 0,
        singleton_configs: 0,
        state_revision: 1,
        quarantined_records: 0,
        completed: false,
    };
    let mut entity_configs = std::collections::BTreeMap::new();
    let mut singleton_configs = std::collections::BTreeMap::new();
    let mut prepared = std::collections::BTreeMap::new();
    for record in &records {
        let suffix = record_suffix(&config.cluster_prefix, &record.key)?;
        update_counts(
            &mut report,
            suffix,
            &record.value,
            &mut entity_configs,
            &mut singleton_configs,
        )?;
        if skip_key(suffix) {
            continue;
        }
        report.scanned_records = report.scanned_records.saturating_add(1);
        let transformed = transform_record(suffix, &record.value, term)?;
        if transformed.is_some()
            || suffix.starts_with("members/")
            || suffix.starts_with("shards/")
            || suffix.starts_with("singletons/")
            || suffix.starts_with("rebalances/")
        {
            prepared.insert(
                record.key.clone(),
                transformed.unwrap_or_else(|| record.value.clone()),
            );
        }
    }
    quarantine_transitional_records(&config.cluster_prefix, &records, &mut prepared, &mut report)?;
    validate_counts(&report, config.limits)?;
    for (index, record) in records.iter().enumerate() {
        if resume_after
            .as_ref()
            .is_some_and(|last_key| record.key.as_str() <= last_key.as_str())
        {
            continue;
        }
        let Some(transformed) = prepared.get(&record.key) else {
            continue;
        };
        if transformed == &record.value {
            continue;
        }
        report.transformed_records = report.transformed_records.saturating_add(1);
        if matches!(mode, MigrationMode::Apply | MigrationMode::Resume) {
            if index % config.page_size == 0 {
                let lease_id = lock.as_ref().ok_or(MigrationError::Locked)?.0;
                let (mut keeper, mut stream) = client.lease_keep_alive(lease_id).await?;
                keeper.keep_alive().await?;
                stream.message().await?.ok_or(MigrationError::Locked)?;
            }
            let current_marker = marker.clone().ok_or(MigrationError::MarkerMismatch)?;
            let next_marker = MigrationMarker {
                last_key: Some(record.key.clone()),
                ..current_marker.clone()
            };
            let response = client
                .txn(
                    Txn::new()
                        .when([
                            Compare::mod_revision(
                                record.key.clone(),
                                CompareOp::Equal,
                                record.mod_revision,
                            ),
                            Compare::value(
                                marker_key.clone(),
                                CompareOp::Equal,
                                serde_json::to_vec(&current_marker)?,
                            ),
                            Compare::version(leader_key.clone(), CompareOp::Equal, 0),
                            Compare::value(schema_key.clone(), CompareOp::Equal, MIGRATING_SCHEMA),
                            term_compare(&term_key, term_record.as_ref(), term),
                            Compare::value(
                                lock_key.clone(),
                                CompareOp::Equal,
                                lock.as_ref().ok_or(MigrationError::Locked)?.1.clone(),
                            ),
                        ])
                        .and_then([
                            TxnOp::put(record.key.clone(), transformed.clone(), None),
                            TxnOp::put(marker_key.clone(), serde_json::to_vec(&next_marker)?, None),
                        ]),
                )
                .await?;
            if !response.succeeded() {
                return Err(MigrationError::CompareFailed);
            }
            lattice_core::failpoint::hit(
                lattice_core::failpoint::Failpoint::MigrationAfterCommitBeforeProgress,
            );
            marker = Some(next_marker);
        }
    }
    if matches!(mode, MigrationMode::Apply | MigrationMode::Resume) {
        let finalize_context = FinalizeContext {
            schema_key: &schema_key,
            marker_key: &marker_key,
            lock_key: &lock_key,
            leader_key: &leader_key,
            term_key: &term_key,
            term_record: term_record.as_ref(),
            lock: lock.as_ref().ok_or(MigrationError::Locked)?,
        };
        finalize(
            &mut client,
            &config,
            finalize_context,
            marker.ok_or(MigrationError::MarkerMismatch)?,
            &report,
        )
        .await?;
        client
            .lease_revoke(lock.ok_or(MigrationError::Locked)?.0)
            .await?;
        report.completed = true;
    }
    Ok(report)
}

pub async fn execute_cardinality(
    mode: CardinalityMode,
    config: MigrationConfig,
) -> Result<CardinalityReport, MigrationError> {
    if config.endpoints.is_empty()
        || config.cluster_prefix.is_empty()
        || config.page_size == 0
        || !config.limits.validate()
    {
        return Err(MigrationError::InvalidConfig);
    }
    let mut client = Client::connect(&config.endpoints, None).await?;
    let schema_key = key(&config.cluster_prefix, "schema_generation");
    if read_one(&mut client, &schema_key)
        .await?
        .as_ref()
        .map(|record| record.value.as_slice())
        != Some(STORAGE_SCHEMA_GENERATION.to_string().as_bytes())
    {
        return Err(MigrationError::WrongGeneration);
    }
    let limits_key = key(&config.cluster_prefix, "schema/limits");
    let limits = read_one(&mut client, &limits_key)
        .await?
        .ok_or(MigrationError::InvalidRecord)?;
    if serde_json::from_slice::<DurableStorageLimits>(&limits.value)? != config.limits {
        return Err(MigrationError::MarkerMismatch);
    }
    let leader_key = key(&config.cluster_prefix, "coordinator/leader");
    if read_one(&mut client, &leader_key).await?.is_some() {
        return Err(MigrationError::LeaderPresent);
    }
    let maximum_records = config
        .limits
        .maximum_slots
        .checked_mul(2)
        .and_then(|value| value.checked_add(config.limits.maximum_plans))
        .and_then(|value| value.checked_add(config.limits.maximum_members))
        .and_then(|value| value.checked_add(config.limits.maximum_admin_operations))
        .and_then(|value| value.checked_add(128))
        .ok_or(MigrationError::InvalidConfig)?;
    let records = scan_prefix(
        &mut client,
        &config.cluster_prefix,
        config.page_size,
        maximum_records,
        None,
    )
    .await?;
    let mut inventory = MigrationReport {
        mode: "cardinality".to_owned(),
        scanned_records: 0,
        transformed_records: 0,
        slots: 0,
        plans: 0,
        members: 0,
        admin_operations: 0,
        entity_configs: 0,
        singleton_configs: 0,
        state_revision: 1,
        quarantined_records: 0,
        completed: false,
    };
    let mut entity_configs = std::collections::BTreeMap::new();
    let mut singleton_configs = std::collections::BTreeMap::new();
    for record in &records {
        update_counts(
            &mut inventory,
            record_suffix(&config.cluster_prefix, &record.key)?,
            &record.value,
            &mut entity_configs,
            &mut singleton_configs,
        )?;
    }
    validate_counts(&inventory, config.limits)?;
    let counter_names = ["slots", "plans", "members", "admin_operations"];
    let mut counters = Vec::new();
    for name in counter_names {
        let counter = read_one(
            &mut client,
            &key(&config.cluster_prefix, &format!("counters/{name}")),
        )
        .await?
        .ok_or(MigrationError::InvalidRecord)?;
        let value = parse_usize(&counter.value)?;
        counters.push((name, counter, value));
    }
    let mut report = CardinalityReport {
        slots: inventory.slots,
        plans: inventory.plans,
        members: inventory.members,
        admin_operations: inventory.admin_operations,
        stored_slots: counters[0].2,
        stored_plans: counters[1].2,
        stored_members: counters[2].2,
        stored_admin_operations: counters[3].2,
        repaired: false,
    };
    if mode == CardinalityMode::Repair {
        let lock_key = key(
            &config.cluster_prefix,
            "diagnostics/cardinality-repair-lock",
        );
        let lease_id = client
            .lease_grant(MIGRATION_LOCK_TTL_SECONDS, None)
            .await?
            .id();
        let lock_value = uuid::Uuid::new_v4().to_string();
        let actual = [
            inventory.slots,
            inventory.plans,
            inventory.members,
            inventory.admin_operations,
        ];
        let mut compares = vec![
            Compare::value(
                schema_key,
                CompareOp::Equal,
                STORAGE_SCHEMA_GENERATION.to_string(),
            ),
            Compare::version(leader_key, CompareOp::Equal, 0),
            Compare::mod_revision(limits_key, CompareOp::Equal, limits.mod_revision),
            Compare::version(lock_key.clone(), CompareOp::Equal, 0),
        ];
        let mut puts = vec![TxnOp::put(
            lock_key,
            lock_value,
            Some(PutOptions::new().with_lease(lease_id)),
        )];
        for (index, (name, counter, _)) in counters.iter().enumerate() {
            let key = key(&config.cluster_prefix, &format!("counters/{name}"));
            compares.push(Compare::mod_revision(
                key.clone(),
                CompareOp::Equal,
                counter.mod_revision,
            ));
            puts.push(TxnOp::put(key, actual[index].to_string(), None));
        }
        let response = client.txn(Txn::new().when(compares).and_then(puts)).await?;
        let _ = client.lease_revoke(lease_id).await;
        if !response.succeeded() {
            return Err(MigrationError::CompareFailed);
        }
        report.stored_slots = report.slots;
        report.stored_plans = report.plans;
        report.stored_members = report.members;
        report.stored_admin_operations = report.admin_operations;
        report.repaired = true;
    }
    Ok(report)
}

#[derive(Debug, Serialize)]
struct RawRecord {
    key: String,
    value: Vec<u8>,
    mod_revision: i64,
}

fn record_suffix<'a>(prefix: &str, record_key: &'a str) -> Result<&'a str, MigrationError> {
    record_key
        .strip_prefix(&format!("{}/", prefix.trim_end_matches('/')))
        .ok_or(MigrationError::InvalidRecord)
}

fn write_backup(path: &PathBuf, records: &[RawRecord]) -> Result<(), MigrationError> {
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    serde_json::to_writer_pretty(&mut file, records)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn term_compare(key: &str, record: Option<&RawRecord>, term: u64) -> Compare {
    if record.is_some() {
        Compare::value(key, CompareOp::Equal, term.to_string())
    } else {
        Compare::version(key, CompareOp::Equal, 0)
    }
}

async fn read_one(client: &mut Client, key: &str) -> Result<Option<RawRecord>, etcd_client::Error> {
    let response = client.get(key, None).await?;
    Ok(response.kvs().first().map(|record| RawRecord {
        key: String::from_utf8_lossy(record.key()).into_owned(),
        value: record.value().to_vec(),
        mod_revision: record.mod_revision(),
    }))
}

async fn scan_prefix(
    client: &mut Client,
    prefix: &str,
    page_size: usize,
    maximum_records: usize,
    start_after: Option<&str>,
) -> Result<Vec<RawRecord>, MigrationError> {
    let prefix = format!("{}/", prefix.trim_end_matches('/')).into_bytes();
    let mut end = prefix.clone();
    *end.last_mut().ok_or(MigrationError::InvalidConfig)? = b'0';
    let mut start = start_after
        .map(|value| {
            let mut bytes = value.as_bytes().to_vec();
            bytes.push(0);
            bytes
        })
        .unwrap_or(prefix);
    let limit = i64::try_from(page_size).map_err(|_| MigrationError::InvalidConfig)?;
    let mut records = Vec::new();
    loop {
        let response = client
            .get(
                start.clone(),
                Some(
                    GetOptions::new()
                        .with_range(end.clone())
                        .with_limit(limit)
                        .with_sort(SortTarget::Key, SortOrder::Ascend),
                ),
            )
            .await?;
        records.extend(response.kvs().iter().map(|record| RawRecord {
            key: String::from_utf8_lossy(record.key()).into_owned(),
            value: record.value().to_vec(),
            mod_revision: record.mod_revision(),
        }));
        if records.len() > maximum_records {
            return Err(MigrationError::Capacity);
        }
        if !response.more() {
            break;
        }
        let last = response.kvs().last().ok_or(MigrationError::InvalidRecord)?;
        start = last.key().to_vec();
        start.push(0);
    }
    Ok(records)
}

fn transform_record(
    suffix: &str,
    value: &[u8],
    current_term: u64,
) -> Result<Option<Vec<u8>>, MigrationError> {
    if suffix.starts_with("members/") {
        let mut record: serde_json::Value = serde_json::from_slice(value)?;
        if record.get("version").is_some() {
            return Ok(None);
        }
        let revision = record
            .as_object_mut()
            .and_then(|object| object.remove("revision"))
            .ok_or(MigrationError::InvalidRecord)?;
        record["version"] = serde_json::json!({"term": current_term, "revision": revision});
        return Ok(Some(serde_json::to_vec(&record)?));
    }
    if suffix.starts_with("shards/") || suffix.starts_with("singletons/") {
        let mut record: serde_json::Value = serde_json::from_slice(value)?;
        let object = record
            .as_object_mut()
            .ok_or(MigrationError::InvalidRecord)?;
        if object.contains_key("version") {
            return Ok(None);
        }
        object
            .remove("coordinator_term")
            .ok_or(MigrationError::InvalidRecord)?;
        let revision = object
            .remove("revision")
            .ok_or(MigrationError::InvalidRecord)?;
        object.insert(
            "version".to_owned(),
            serde_json::json!({"term": current_term, "revision": revision}),
        );
        return Ok(Some(serde_json::to_vec(&record)?));
    }
    if suffix.starts_with("rebalances/") {
        let mut record: serde_json::Value = serde_json::from_slice(value)?;
        let object = record
            .as_object_mut()
            .ok_or(MigrationError::InvalidRecord)?;
        if object.contains_key("base_version") {
            return Ok(None);
        }
        object
            .get("coordinator_term")
            .ok_or(MigrationError::InvalidRecord)?;
        let base = object
            .remove("base_revision")
            .ok_or(MigrationError::InvalidRecord)?;
        object.insert(
            "base_version".to_owned(),
            serde_json::json!({"term": current_term, "revision": base}),
        );
        if let Some(revision) = object.remove("revision") {
            object.insert("record_revision".to_owned(), revision);
        } else if !object.contains_key("record_revision") {
            return Err(MigrationError::InvalidRecord);
        }
        let moves = object
            .get_mut("moves")
            .and_then(serde_json::Value::as_array_mut)
            .ok_or(MigrationError::InvalidRecord)?;
        for movement in moves {
            let movement = movement
                .as_object_mut()
                .ok_or(MigrationError::InvalidRecord)?;
            if let Some(barrier) = movement.remove("barrier_revision") {
                movement.insert(
                    "barrier_version".to_owned(),
                    if barrier.is_null() {
                        barrier
                    } else {
                        serde_json::json!({"term": current_term, "revision": barrier})
                    },
                );
            }
        }
        return Ok(Some(serde_json::to_vec(&record)?));
    }
    Ok(None)
}

fn quarantine_transitional_records(
    prefix: &str,
    records: &[RawRecord],
    prepared: &mut std::collections::BTreeMap<String, Vec<u8>>,
    report: &mut MigrationReport,
) -> Result<(), MigrationError> {
    let mut slots = std::collections::BTreeMap::<PlacementSlotKey, (String, PlacementSlot)>::new();
    let mut plans = std::collections::BTreeMap::<u128, (String, RebalancePlan)>::new();
    let mut claims = std::collections::BTreeMap::<PlacementSlotKey, ClaimGrant>::new();
    for record in records {
        let suffix = record_suffix(prefix, &record.key)?;
        if suffix.starts_with("shard_claims/") || suffix.starts_with("singleton_claims/") {
            let claim: ClaimGrant = serde_json::from_slice(&record.value)?;
            claims.insert(claim.slot.clone(), claim);
        }
    }
    for (record_key, value) in prepared.iter() {
        let suffix = record_suffix(prefix, record_key)?;
        if suffix.starts_with("shards/") || suffix.starts_with("singletons/") {
            let slot: PlacementSlot = serde_json::from_slice(value)?;
            slot.validate().map_err(|_| MigrationError::InvalidRecord)?;
            slots.insert(slot.key.clone(), (record_key.clone(), slot));
        } else if suffix.starts_with("rebalances/") {
            let plan: RebalancePlan = serde_json::from_slice(value)?;
            plans.insert(plan.plan_id, (record_key.clone(), plan));
        }
    }

    let mut bad_slots = std::collections::BTreeSet::new();
    let mut bad_moves = std::collections::BTreeSet::<(u128, ShardId)>::new();
    for (slot_key, (_, slot)) in &slots {
        if matches!(
            slot.state,
            PlacementSlotState::Allocating | PlacementSlotState::Running
        ) && !claims
            .get(slot_key)
            .is_some_and(|claim| claim_matches_slot(claim, slot))
        {
            bad_slots.insert(slot_key.clone());
        }
        if let Some(plan_id) = slot.active_move {
            let related = plans.get(&plan_id).is_some_and(|(_, plan)| {
                plan_movement(plan, slot_key)
                    .is_some_and(|movement| movement.progress == MoveProgress::Handoff)
            });
            if !related {
                bad_slots.insert(slot_key.clone());
            }
        }
    }
    for (plan_id, (_, plan)) in &plans {
        for movement in &plan.moves {
            if movement.progress != MoveProgress::Handoff {
                continue;
            }
            let slot_key = PlacementSlotKey::Shard {
                entity_type: plan.entity_type.clone(),
                shard_id: movement.shard_id,
            };
            if slots
                .get(&slot_key)
                .is_none_or(|(_, slot)| slot.active_move != Some(*plan_id))
            {
                bad_moves.insert((*plan_id, movement.shard_id));
            }
        }
    }
    for slot_key in &bad_slots {
        let (record_key, mut slot) = slots
            .get(slot_key)
            .cloned()
            .ok_or(MigrationError::InvalidRecord)?;
        if let Some(plan_id) = slot.active_move
            && let PlacementSlotKey::Shard { shard_id, .. } = &slot.key
        {
            bad_moves.insert((plan_id, *shard_id));
        }
        slot.state = PlacementSlotState::Fenced;
        slot.target = None;
        slot.active_move = None;
        slot.barrier_sessions.clear();
        prepared.insert(record_key, serde_json::to_vec(&slot)?);
    }
    let mut bad_plan_records = std::collections::BTreeSet::new();
    for (plan_id, (record_key, original)) in &plans {
        if !bad_moves
            .iter()
            .any(|(bad_plan_id, _)| bad_plan_id == plan_id)
        {
            continue;
        }
        let mut plan = original.clone();
        for movement in &mut plan.moves {
            if bad_moves.contains(&(*plan_id, movement.shard_id)) {
                movement.progress = MoveProgress::Failed;
                movement.barrier_version = None;
                movement.barrier_sessions.clear();
            }
        }
        plan.status = PlanStatus::Failed;
        plan.record_revision = plan
            .record_revision
            .next()
            .map_err(|_| MigrationError::InvalidRecord)?;
        prepared.insert(record_key.clone(), serde_json::to_vec(&plan)?);
        bad_plan_records.insert(*plan_id);
    }
    report.quarantined_records = bad_slots.len().saturating_add(bad_plan_records.len());
    Ok(())
}

fn claim_matches_slot(claim: &ClaimGrant, slot: &PlacementSlot) -> bool {
    claim.slot == slot.key
        && slot.owner.as_ref() == Some(&claim.owner)
        && claim.assignment_generation == slot.assignment_generation
        && claim.coordinator_term == slot.version.term
        && !claim.ttl.is_zero()
}

fn plan_movement<'a>(
    plan: &'a RebalancePlan,
    slot: &PlacementSlotKey,
) -> Option<&'a crate::plan::RebalanceMove> {
    let PlacementSlotKey::Shard {
        entity_type,
        shard_id,
    } = slot
    else {
        return None;
    };
    (plan.entity_type == *entity_type)
        .then(|| {
            plan.moves
                .iter()
                .find(|movement| movement.shard_id == *shard_id)
        })
        .flatten()
}

fn skip_key(suffix: &str) -> bool {
    suffix == "schema_generation"
        || suffix.starts_with("schema/")
        || suffix.starts_with("coordinator/")
        || suffix.starts_with("counters/")
        || suffix.starts_with("migration/")
        || suffix.starts_with("shard_claims/")
        || suffix.starts_with("singleton_claims/")
}

fn update_counts(
    report: &mut MigrationReport,
    suffix: &str,
    value: &[u8],
    entity_configs: &mut std::collections::BTreeMap<String, String>,
    singleton_configs: &mut std::collections::BTreeMap<String, String>,
) -> Result<(), MigrationError> {
    if suffix.starts_with("shards/") || suffix.starts_with("singletons/") {
        report.slots = report.slots.saturating_add(1);
        report.state_revision = report.state_revision.max(record_revision(value)?);
    } else if suffix.starts_with("rebalances/") {
        report.plans = report.plans.saturating_add(1);
    } else if suffix.starts_with("members/") {
        report.members = report.members.saturating_add(1);
        report.state_revision = report.state_revision.max(record_revision(value)?);
        collect_member_configs(value, entity_configs, singleton_configs)?;
        report.entity_configs = entity_configs.len();
        report.singleton_configs = singleton_configs.len();
    } else if suffix.starts_with("operations/") {
        report.admin_operations = report.admin_operations.saturating_add(1);
    }
    Ok(())
}

fn record_revision(value: &[u8]) -> Result<u64, MigrationError> {
    let value: serde_json::Value = serde_json::from_slice(value)?;
    value
        .get("revision")
        .or_else(|| {
            value
                .get("version")
                .and_then(|version| version.get("revision"))
        })
        .and_then(serde_json::Value::as_u64)
        .ok_or(MigrationError::InvalidRecord)
}

fn collect_member_configs(
    value: &[u8],
    entity_configs: &mut std::collections::BTreeMap<String, String>,
    singleton_configs: &mut std::collections::BTreeMap<String, String>,
) -> Result<(), MigrationError> {
    let value: serde_json::Value = serde_json::from_slice(value)?;
    let hello = value.get("hello").ok_or(MigrationError::InvalidRecord)?;
    collect_configs(hello, "entity_configs", "entity_type", entity_configs)?;
    collect_configs(hello, "singleton_configs", "kind", singleton_configs)
}

fn collect_configs(
    hello: &serde_json::Value,
    collection: &str,
    key_name: &str,
    configs: &mut std::collections::BTreeMap<String, String>,
) -> Result<(), MigrationError> {
    let values = hello
        .get(collection)
        .and_then(serde_json::Value::as_array)
        .ok_or(MigrationError::InvalidRecord)?;
    for value in values {
        let key = serde_json::to_string(value.get(key_name).ok_or(MigrationError::InvalidRecord)?)?;
        let canonical = serde_json::to_string(value)?;
        if configs
            .get(&key)
            .is_some_and(|existing| existing != &canonical)
        {
            return Err(MigrationError::InvalidRecord);
        }
        configs.insert(key, canonical);
    }
    Ok(())
}

fn validate_counts(
    report: &MigrationReport,
    limits: DurableStorageLimits,
) -> Result<(), MigrationError> {
    if report.slots > limits.maximum_slots
        || report.plans > limits.maximum_plans
        || report.members > limits.maximum_members
        || report.admin_operations > limits.maximum_admin_operations
        || report.entity_configs > limits.maximum_entity_configs
        || report.singleton_configs > limits.maximum_singleton_configs
    {
        return Err(MigrationError::Capacity);
    }
    Ok(())
}

struct FinalizeContext<'a> {
    schema_key: &'a str,
    marker_key: &'a str,
    lock_key: &'a str,
    leader_key: &'a str,
    term_key: &'a str,
    term_record: Option<&'a RawRecord>,
    lock: &'a (i64, String),
}

async fn finalize(
    client: &mut Client,
    config: &MigrationConfig,
    context: FinalizeContext<'_>,
    marker: MigrationMarker,
    report: &MigrationReport,
) -> Result<(), MigrationError> {
    let complete = MigrationMarker {
        completed: true,
        ..marker.clone()
    };
    let limits = serde_json::to_vec(&config.limits)?;
    let mut operations = vec![
        TxnOp::put(
            context.schema_key,
            STORAGE_SCHEMA_GENERATION.to_string(),
            None,
        ),
        TxnOp::put(key(&config.cluster_prefix, "schema/limits"), limits, None),
        TxnOp::put(
            key(&config.cluster_prefix, "counters/slots"),
            report.slots.to_string(),
            None,
        ),
        TxnOp::put(
            key(&config.cluster_prefix, "counters/plans"),
            report.plans.to_string(),
            None,
        ),
        TxnOp::put(
            key(&config.cluster_prefix, "counters/members"),
            report.members.to_string(),
            None,
        ),
        TxnOp::put(
            key(&config.cluster_prefix, "counters/admin_operations"),
            report.admin_operations.to_string(),
            None,
        ),
        TxnOp::put(
            key(&config.cluster_prefix, "coordinator/state_revision"),
            report.state_revision.to_string(),
            None,
        ),
        TxnOp::put(context.marker_key, serde_json::to_vec(&complete)?, None),
    ];
    if marker.coordinator_term > 0 {
        operations.push(TxnOp::put(
            key(&config.cluster_prefix, "settings/automatic_balance"),
            serde_json::to_vec(&serde_json::json!({
                "globally_paused": false,
                "paused_entity_types": [],
                "version": {
                    "term": marker.coordinator_term,
                    "revision": report.state_revision,
                }
            }))?,
            None,
        ));
    }
    let response = client
        .txn(
            Txn::new()
                .when([
                    Compare::value(context.schema_key, CompareOp::Equal, MIGRATING_SCHEMA),
                    Compare::version(context.leader_key, CompareOp::Equal, 0),
                    term_compare(
                        context.term_key,
                        context.term_record,
                        marker.coordinator_term,
                    ),
                    Compare::value(context.lock_key, CompareOp::Equal, context.lock.1.clone()),
                    Compare::value(
                        context.marker_key,
                        CompareOp::Equal,
                        serde_json::to_vec(&marker)?,
                    ),
                ])
                .and_then(operations),
        )
        .await?;
    if response.succeeded() {
        Ok(())
    } else {
        Err(MigrationError::CompareFailed)
    }
}

fn key(prefix: &str, suffix: &str) -> String {
    format!("{}/{}", prefix.trim_end_matches('/'), suffix)
}

fn parse_u64(value: &[u8]) -> Result<u64, MigrationError> {
    std::str::from_utf8(value)
        .map_err(|_| MigrationError::InvalidRecord)?
        .parse()
        .map_err(|_| MigrationError::InvalidRecord)
}

fn parse_usize(value: &[u8]) -> Result<usize, MigrationError> {
    std::str::from_utf8(value)
        .map_err(|_| MigrationError::InvalidRecord)?
        .parse()
        .map_err(|_| MigrationError::InvalidRecord)
}

#[cfg(test)]
mod tests {
    use super::transform_record;

    #[test]
    fn generation_three_json_is_term_qualified_without_touching_claims() {
        let slot = br#"{"coordinator_term":2,"revision":7,"state":"Running"}"#;
        let transformed = transform_record("shards/entity/1", slot, 9)
            .unwrap()
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&transformed).unwrap();
        assert_eq!(value["version"]["term"], 9);
        assert_eq!(value["version"]["revision"], 7);
        assert!(value.get("coordinator_term").is_none());
        assert!(
            transform_record("shard_claims/entity/1", b"{}", 9)
                .unwrap()
                .is_none()
        );
    }
}
