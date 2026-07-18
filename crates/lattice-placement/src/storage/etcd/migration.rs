use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;

use etcd_client::{
    Client, Compare, CompareOp, GetOptions, PutOptions, SortOrder, SortTarget, Txn, TxnOp,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::STORAGE_SCHEMA_GENERATION;
use crate::coordinator::SingletonConfig;
use crate::plan::{MoveProgress, PlanStatus, RebalancePlan};
use crate::region::EntityConfig;
use crate::storage::domain::DurableStorageLimits;
use crate::types::{CoordinatorTerm, PlacementSlot, PlacementSlotState, PlacementVersion};
use lattice_core::actor_ref::PlacementDomainId;

const MIGRATING_SCHEMA: &str = "migrating-to-5";
const MIGRATION_LOCK_TTL_SECONDS: i64 = 300;
// etcd defaults to 128 operations per transaction. Each target contributes
// one compare and one put in addition to the common fencing comparisons.
const MIGRATION_TXN_TARGET_BATCH: usize = 32;

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
    pub mapping: MigrationDomainMapping,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationDomainMapping {
    pub entity_types: BTreeMap<String, PlacementDomainId>,
    pub singleton_kinds: BTreeMap<String, PlacementDomainId>,
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
    mapping: MigrationDomainMapping,
}

#[derive(Debug, Error)]
pub enum MigrationError {
    #[error("migration configuration is invalid")]
    InvalidConfig,
    #[error("generation-4 storage is required")]
    WrongGeneration,
    #[error("migration must run while no Coordinator leader exists")]
    LeaderPresent,
    #[error("migration requires all member and claim leases to be absent")]
    LiveLeasePresent,
    #[error("migration requires every handoff to be completed or cancelled")]
    ActiveHandoff,
    #[error("a generation-5 destination key already contains a different record")]
    Collision,
    #[error("migration marker does not match the requested limits or term")]
    MarkerMismatch,
    #[error("another migration holds the lease-backed migration lock")]
    Locked,
    #[error("a record cannot be converted to generation 5")]
    InvalidRecord,
    #[error("every entity type and singleton kind requires an explicit placement-domain mapping")]
    UnmappedType,
    #[error("configured durable cardinality is too small for existing records")]
    Capacity,
    #[error("migration compare failed; rerun with resume after inspecting storage")]
    CompareFailed,
    #[error("migration record/progress compare failed; rerun with resume after inspecting storage")]
    ProgressCompareFailed,
    #[error("migration finalization compare failed; storage remains resumable")]
    FinalizeCompareFailed,
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
        MigrationMode::Apply => schema_value == b"4",
        MigrationMode::Resume => schema_value == MIGRATING_SCHEMA.as_bytes(),
        MigrationMode::Inspect | MigrationMode::DryRun => {
            schema_value == b"4" || schema_value == MIGRATING_SCHEMA.as_bytes()
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
    validate_full_stop(&config.cluster_prefix, &records)?;
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
    let marker_key = key(&config.cluster_prefix, "migration/generation-4-to-5");
    let lock_key = key(&config.cluster_prefix, "migration/generation-4-to-5-lock");
    let existing_marker = read_one(&mut client, &marker_key).await?;
    let mut marker = existing_marker
        .as_ref()
        .map(|record| serde_json::from_slice::<MigrationMarker>(&record.value))
        .transpose()?;
    if marker.as_ref().is_some_and(|marker| {
        marker.coordinator_term != term
            || marker.limits != config.limits
            || marker.mapping != config.mapping
    }) {
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
            mapping: config.mapping.clone(),
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
            compares.push(Compare::value(schema_key.clone(), CompareOp::Equal, "4"));
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
            compares.push(Compare::mod_revision(
                marker_key.clone(),
                CompareOp::Equal,
                existing_marker
                    .as_ref()
                    .ok_or(MigrationError::MarkerMismatch)?
                    .mod_revision,
            ));
            puts.push(TxnOp::put(
                marker_key.clone(),
                serde_json::to_vec(&current_marker)?,
                None,
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
    for record in &records {
        let suffix = record_suffix(&config.cluster_prefix, &record.key)?;
        update_counts(
            &mut report,
            suffix,
            &record.value,
            &mut entity_configs,
            &mut singleton_configs,
        )?;
        collect_legacy_config_record(
            suffix,
            &record.value,
            &mut entity_configs,
            &mut singleton_configs,
        )?;
    }
    validate_mapping(&entity_configs, &singleton_configs, &config.mapping)?;
    let (config_targets, entity_fingerprints) = build_config_targets(
        &config.cluster_prefix,
        &entity_configs,
        &singleton_configs,
        &config.mapping,
    )?;
    let config_writer = records.iter().find(|record| {
        record_suffix(&config.cluster_prefix, &record.key).is_ok_and(|suffix| {
            suffix.starts_with("members/")
                || suffix.starts_with("entity_types/")
                || suffix.starts_with("singleton_types/")
        })
    });
    let existing = records
        .iter()
        .map(|record| (record.key.as_str(), record))
        .collect::<BTreeMap<_, _>>();
    let mut prepared = BTreeMap::new();
    for (target, value) in &config_targets {
        if let Some(current) = existing.get(target.as_str())
            && (schema_value == b"4" || current.value != *value)
        {
            return Err(MigrationError::Collision);
        }
    }
    for record in &records {
        let suffix = record_suffix(&config.cluster_prefix, &record.key)?;
        if let Some(operation) = prepare_record(
            &config.cluster_prefix,
            suffix,
            &record.value,
            &config.mapping,
            &entity_fingerprints,
            BTreeMap::new(),
            term,
        )? {
            for (target, value) in &operation.targets {
                if let Some(current) = existing.get(target.as_str())
                    && (schema_value == b"4" || current.value != *value)
                {
                    return Err(MigrationError::Collision);
                }
            }
            report.scanned_records = report.scanned_records.saturating_add(1);
            report.quarantined_records = report
                .quarantined_records
                .saturating_add(usize::from(operation.fenced));
            prepared.insert(record.key.clone(), operation);
        }
    }
    let inventory = build_target_inventory(
        &config.cluster_prefix,
        &config.mapping,
        &records,
        &prepared,
        &config_targets,
    )?;
    validate_counts(&report, config.limits)?;
    if matches!(mode, MigrationMode::Apply | MigrationMode::Resume) {
        let missing_config_targets = config_targets
            .iter()
            .filter(|(target, _)| !existing.contains_key(target.as_str()))
            .map(|(target, value)| (target.clone(), value.clone()))
            .collect::<BTreeMap<_, _>>();
        if !missing_config_targets.is_empty() {
            let writer = config_writer.ok_or(MigrationError::InvalidRecord)?;
            let write_result = write_bounded_targets(
                &mut client,
                BoundedWriteContext {
                    schema_key: &schema_key,
                    leader_key: &leader_key,
                    term_key: &term_key,
                    term_record: term_record.as_ref(),
                    term,
                    lock_key: &lock_key,
                    lock: lock.as_ref().ok_or(MigrationError::Locked)?,
                    marker_key: &marker_key,
                    marker: marker.as_ref().ok_or(MigrationError::MarkerMismatch)?,
                    source: Some(writer),
                },
                &missing_config_targets,
            )
            .await;
            if let Err(error) = write_result {
                let _ = client
                    .lease_revoke(lock.as_ref().ok_or(MigrationError::Locked)?.0)
                    .await;
                return Err(error);
            }
        }
    }
    for (index, record) in records.iter().enumerate() {
        if resume_after
            .as_ref()
            .is_some_and(|last_key| record.key.as_str() <= last_key.as_str())
        {
            continue;
        }
        let Some(operation) = prepared.get(&record.key) else {
            continue;
        };
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
            let mut compares = vec![
                Compare::mod_revision(record.key.clone(), CompareOp::Equal, record.mod_revision),
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
            ];
            let mut operations = Vec::new();
            for (target, value) in &operation.targets {
                if let Some(current) = existing.get(target.as_str()) {
                    compares.push(Compare::value(
                        target.clone(),
                        CompareOp::Equal,
                        current.value.clone(),
                    ));
                } else {
                    compares.push(Compare::version(target.clone(), CompareOp::Equal, 0));
                    operations.push(TxnOp::put(target.clone(), value.clone(), None));
                }
            }
            operations.push(TxnOp::delete(record.key.clone(), None));
            operations.push(TxnOp::put(
                marker_key.clone(),
                serde_json::to_vec(&next_marker)?,
                None,
            ));
            let response = client
                .txn(Txn::new().when(compares).and_then(operations))
                .await?;
            if !response.succeeded() {
                return Err(MigrationError::ProgressCompareFailed);
            }
            lattice_core::failpoint::hit(
                lattice_core::failpoint::Failpoint::MigrationAfterCommitBeforeProgress,
            );
            marker = Some(next_marker);
        }
    }
    if matches!(mode, MigrationMode::Apply | MigrationMode::Resume) {
        lattice_core::failpoint::hit(lattice_core::failpoint::Failpoint::MigrationBeforeFinalize);
        let finalize_context = FinalizeContext {
            schema_key: &schema_key,
            marker_key: &marker_key,
            lock_key: &lock_key,
            leader_key: &leader_key,
            term_key: &term_key,
            term_record: term_record.as_ref(),
            lock: lock.as_ref().ok_or(MigrationError::Locked)?,
        };
        let finalize_result = finalize(
            &mut client,
            &config,
            finalize_context,
            marker.ok_or(MigrationError::MarkerMismatch)?,
            &report,
            &inventory,
        )
        .await;
        client
            .lease_revoke(lock.ok_or(MigrationError::Locked)?.0)
            .await?;
        finalize_result?;
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
    validate_full_stop(&config.cluster_prefix, &records)?;
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
    let actual_counters = generation_five_counters(&config.cluster_prefix, &records)?;
    let stored_counters = records
        .iter()
        .filter(|record| actual_counters.contains_key(&record.key))
        .map(|record| parse_usize(&record.value).map(|value| (record.key.clone(), value)))
        .collect::<Result<BTreeMap<_, _>, _>>()?;
    let stored_total = |name: &str| {
        stored_counters
            .iter()
            .filter(|(counter, _)| counter.ends_with(&format!("/counters/{name}")))
            .map(|(_, value)| *value)
            .sum()
    };
    let mut report = CardinalityReport {
        slots: inventory.slots,
        plans: inventory.plans,
        members: inventory.members,
        admin_operations: inventory.admin_operations,
        stored_slots: stored_total("slots"),
        stored_plans: stored_total("plans"),
        stored_members: stored_total("members"),
        stored_admin_operations: stored_total("admin_operations"),
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
        let compares = vec![
            Compare::value(
                schema_key.clone(),
                CompareOp::Equal,
                STORAGE_SCHEMA_GENERATION.to_string(),
            ),
            Compare::version(leader_key.clone(), CompareOp::Equal, 0),
            Compare::mod_revision(limits_key.clone(), CompareOp::Equal, limits.mod_revision),
            Compare::version(lock_key.clone(), CompareOp::Equal, 0),
        ];
        let puts = vec![TxnOp::put(
            lock_key.clone(),
            lock_value.clone(),
            Some(PutOptions::new().with_lease(lease_id)),
        )];
        let response = client.txn(Txn::new().when(compares).and_then(puts)).await?;
        if !response.succeeded() {
            let _ = client.lease_revoke(lease_id).await;
            return Err(MigrationError::CompareFailed);
        }
        let records_by_key = records
            .iter()
            .map(|record| (record.key.as_str(), record))
            .collect::<BTreeMap<_, _>>();
        for chunk in actual_counters
            .iter()
            .collect::<Vec<_>>()
            .chunks(MIGRATION_TXN_TARGET_BATCH)
        {
            let mut compares = vec![
                Compare::value(
                    schema_key.clone(),
                    CompareOp::Equal,
                    STORAGE_SCHEMA_GENERATION.to_string(),
                ),
                Compare::version(leader_key.clone(), CompareOp::Equal, 0),
                Compare::mod_revision(limits_key.clone(), CompareOp::Equal, limits.mod_revision),
                Compare::value(lock_key.clone(), CompareOp::Equal, lock_value.clone()),
            ];
            let mut puts = Vec::with_capacity(chunk.len());
            for (counter_key, actual) in chunk {
                if let Some(counter) = records_by_key.get(counter_key.as_str()) {
                    compares.push(Compare::mod_revision(
                        (*counter_key).clone(),
                        CompareOp::Equal,
                        counter.mod_revision,
                    ));
                } else {
                    compares.push(Compare::version(
                        (*counter_key).clone(),
                        CompareOp::Equal,
                        0,
                    ));
                }
                puts.push(TxnOp::put((*counter_key).clone(), actual.to_string(), None));
            }
            let response = client.txn(Txn::new().when(compares).and_then(puts)).await?;
            if !response.succeeded() {
                let _ = client.lease_revoke(lease_id).await;
                return Err(MigrationError::CompareFailed);
            }
        }
        let _ = client.lease_revoke(lease_id).await;
        report.stored_slots = report.slots;
        report.stored_plans = report.plans;
        report.stored_members = report.members;
        report.stored_admin_operations = report.admin_operations;
        report.repaired = true;
    }
    Ok(report)
}

fn generation_five_counters(
    prefix: &str,
    records: &[RawRecord],
) -> Result<BTreeMap<String, usize>, MigrationError> {
    let membership_members = key(prefix, "membership/counters/members");
    let mut counters = BTreeMap::from([(membership_members.clone(), 0usize)]);
    for record in records {
        let suffix = record_suffix(prefix, &record.key)?;
        if suffix.starts_with("membership/members/") {
            *counters.entry(membership_members.clone()).or_default() += 1;
            continue;
        }
        let Some(rest) = suffix.strip_prefix("domains/") else {
            continue;
        };
        let (domain, scoped) = rest.split_once('/').ok_or(MigrationError::InvalidRecord)?;
        PlacementDomainId::new(domain).map_err(|_| MigrationError::InvalidRecord)?;
        for name in ["slots", "plans", "members", "admin_operations"] {
            counters
                .entry(key(prefix, &format!("domains/{domain}/counters/{name}")))
                .or_default();
        }
        let counter = if scoped.starts_with("shards/") || scoped.starts_with("singletons/") {
            Some("slots")
        } else if scoped.starts_with("rebalances/") {
            Some("plans")
        } else if scoped.starts_with("members/") {
            Some("members")
        } else if scoped.starts_with("admin/") {
            Some("admin_operations")
        } else {
            None
        };
        if let Some(counter) = counter {
            *counters
                .entry(key(prefix, &format!("domains/{domain}/counters/{counter}")))
                .or_default() += 1;
        }
    }
    Ok(counters)
}

include!("migration_helpers.rs");

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
    inventory: &TargetInventory,
) -> Result<(), MigrationError> {
    let complete = MigrationMarker {
        completed: true,
        ..marker.clone()
    };
    let metadata = finalization_targets(config, &marker, report, inventory)?;
    write_bounded_targets(
        client,
        BoundedWriteContext {
            schema_key: context.schema_key,
            leader_key: context.leader_key,
            term_key: context.term_key,
            term_record: context.term_record,
            term: marker.coordinator_term,
            lock_key: context.lock_key,
            lock: context.lock,
            marker_key: context.marker_key,
            marker: &marker,
            source: None,
        },
        &metadata,
    )
    .await?;
    let limits = serde_json::to_vec(&config.limits)?;
    let mut operations = vec![
        TxnOp::put(
            context.schema_key,
            STORAGE_SCHEMA_GENERATION.to_string(),
            None,
        ),
        TxnOp::put(key(&config.cluster_prefix, "schema/limits"), limits, None),
        TxnOp::put(
            key(&config.cluster_prefix, "membership/state_revision"),
            report.state_revision.to_string(),
            None,
        ),
        TxnOp::put(
            key(&config.cluster_prefix, "membership/counters/members"),
            inventory.membership_members.to_string(),
            None,
        ),
        TxnOp::put(context.marker_key, serde_json::to_vec(&complete)?, None),
        TxnOp::delete(context.term_key, None),
        TxnOp::delete(
            key(&config.cluster_prefix, "coordinator/state_revision"),
            None,
        ),
        TxnOp::delete(key(&config.cluster_prefix, "counters/slots"), None),
        TxnOp::delete(key(&config.cluster_prefix, "counters/plans"), None),
        TxnOp::delete(key(&config.cluster_prefix, "counters/members"), None),
        TxnOp::delete(
            key(&config.cluster_prefix, "counters/admin_operations"),
            None,
        ),
        TxnOp::delete(
            key(&config.cluster_prefix, "settings/automatic_balance"),
            None,
        ),
    ];
    if marker.coordinator_term > 0 {
        operations.push(TxnOp::put(
            key(&config.cluster_prefix, "membership/term"),
            marker.coordinator_term.to_string(),
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
        Err(MigrationError::FinalizeCompareFailed)
    }
}

fn finalization_targets(
    config: &MigrationConfig,
    marker: &MigrationMarker,
    report: &MigrationReport,
    inventory: &TargetInventory,
) -> Result<BTreeMap<String, Vec<u8>>, MigrationError> {
    let mut targets = BTreeMap::new();
    for (domain, counts) in &inventory.domains {
        let scope = format!("domains/{}", domain.as_str());
        for (suffix, value) in [
            ("state_revision", report.state_revision),
            ("counters/slots", counts.slots as u64),
            ("counters/plans", counts.plans as u64),
            ("counters/members", counts.members as u64),
            ("counters/admin_operations", counts.admin_operations as u64),
            ("counters/entity_configs", counts.entity_configs as u64),
            (
                "counters/singleton_configs",
                counts.singleton_configs as u64,
            ),
        ] {
            targets.insert(
                key(&config.cluster_prefix, &format!("{scope}/{suffix}")),
                value.to_string().into_bytes(),
            );
        }
        if marker.coordinator_term > 0 {
            targets.insert(
                key(&config.cluster_prefix, &format!("{scope}/term")),
                marker.coordinator_term.to_string().into_bytes(),
            );
            targets.insert(
                key(
                    &config.cluster_prefix,
                    &format!("{scope}/settings/automatic_balance"),
                ),
                serde_json::to_vec(&serde_json::json!({
                    "globally_paused": false,
                    "paused_entity_types": [],
                    "version": {
                        "domain": domain,
                        "term": marker.coordinator_term,
                        "revision": report.state_revision,
                    }
                }))?,
            );
        }
    }
    Ok(targets)
}

struct BoundedWriteContext<'a> {
    schema_key: &'a str,
    leader_key: &'a str,
    term_key: &'a str,
    term_record: Option<&'a RawRecord>,
    term: u64,
    lock_key: &'a str,
    lock: &'a (i64, String),
    marker_key: &'a str,
    marker: &'a MigrationMarker,
    source: Option<&'a RawRecord>,
}

async fn write_bounded_targets(
    client: &mut Client,
    context: BoundedWriteContext<'_>,
    targets: &BTreeMap<String, Vec<u8>>,
) -> Result<(), MigrationError> {
    for chunk in targets
        .iter()
        .collect::<Vec<_>>()
        .chunks(MIGRATION_TXN_TARGET_BATCH)
    {
        let mut compares = vec![
            Compare::value(context.schema_key, CompareOp::Equal, MIGRATING_SCHEMA),
            Compare::version(context.leader_key, CompareOp::Equal, 0),
            term_compare(context.term_key, context.term_record, context.term),
            Compare::value(context.lock_key, CompareOp::Equal, context.lock.1.clone()),
            Compare::value(
                context.marker_key,
                CompareOp::Equal,
                serde_json::to_vec(context.marker)?,
            ),
        ];
        if let Some(source) = context.source {
            compares.push(Compare::mod_revision(
                source.key.clone(),
                CompareOp::Equal,
                source.mod_revision,
            ));
        }
        let mut puts = Vec::with_capacity(chunk.len());
        for (target, value) in chunk {
            let current = read_one(client, target).await?;
            if let Some(current) = current {
                if current.value != **value {
                    return Err(MigrationError::Collision);
                }
                continue;
            }
            compares.push(Compare::version((*target).clone(), CompareOp::Equal, 0));
            puts.push(TxnOp::put((*target).clone(), (*value).clone(), None));
        }
        if puts.is_empty() {
            continue;
        }
        let response = client.txn(Txn::new().when(compares).and_then(puts)).await?;
        if !response.succeeded() {
            return Err(MigrationError::FinalizeCompareFailed);
        }
    }
    Ok(())
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
    use super::{
        MigrationDomainMapping, MigrationError, RawRecord, prepare_record, validate_full_stop,
        validate_mapping,
    };
    use crate::types::{
        AssignmentGeneration, CoordinatorTerm, NodeKey, PlacementSlot, PlacementSlotKey,
        PlacementSlotState, PlacementVersion, Revision, ShardId,
    };
    use lattice_core::actor_ref::{
        ConfigFingerprint, EntityType, NodeAddress, NodeIncarnation, PlacementDomainId,
    };

    #[test]
    fn generation_four_slot_moves_to_its_mapped_domain_and_restarts_fenced() {
        let domain = PlacementDomainId::new("payments").unwrap();
        let entity = EntityType::new("invoice").unwrap();
        let slot = PlacementSlot {
            key: PlacementSlotKey::Shard {
                domain: domain.clone(),
                entity_type: entity.clone(),
                shard_id: ShardId::new(7),
            },
            config_fingerprint: ConfigFingerprint::new([4; 32]),
            owner: Some(NodeKey {
                node_id: "old-owner".to_owned(),
                address: NodeAddress::new("127.0.0.1", 29001).unwrap(),
                incarnation: NodeIncarnation::new(1).unwrap(),
            }),
            target: None,
            assignment_generation: AssignmentGeneration::new(9).unwrap(),
            version: PlacementVersion::new(
                domain.clone(),
                CoordinatorTerm::new(3).unwrap(),
                Revision::new(11).unwrap(),
            ),
            state: PlacementSlotState::Running,
            active_move: None,
            barrier_sessions: Default::default(),
        };
        let mut legacy = serde_json::to_value(slot).unwrap();
        legacy["version"].as_object_mut().unwrap().remove("domain");
        let mapping = MigrationDomainMapping {
            entity_types: [(entity.as_str().to_owned(), domain.clone())]
                .into_iter()
                .collect(),
            singleton_kinds: Default::default(),
        };
        let prepared = prepare_record(
            "/cluster",
            "shards/invoice/7",
            &serde_json::to_vec(&legacy).unwrap(),
            &mapping,
            &Default::default(),
            Default::default(),
            3,
        )
        .unwrap()
        .unwrap();
        let migrated: PlacementSlot = serde_json::from_slice(
            prepared
                .targets
                .get("/cluster/domains/payments/shards/invoice/7")
                .unwrap(),
        )
        .unwrap();
        assert!(prepared.fenced);
        assert_eq!(migrated.state, PlacementSlotState::Fenced);
        assert_eq!(migrated.assignment_generation.get(), 9);
        assert_eq!(migrated.version.domain, domain);
    }

    #[test]
    fn live_generation_four_member_lease_blocks_offline_migration() {
        let records = [RawRecord {
            key: "/cluster/members/node-a".to_owned(),
            value: b"{}".to_vec(),
            mod_revision: 1,
            lease_id: 42,
        }];
        assert!(matches!(
            validate_full_stop("/cluster", &records),
            Err(MigrationError::LiveLeasePresent)
        ));
    }

    #[test]
    fn active_generation_four_handoff_blocks_offline_migration() {
        let records = [RawRecord {
            key: "/cluster/shards/invoice/7".to_owned(),
            value: serde_json::to_vec(&serde_json::json!({
                "state": "Running",
                "active_move": { "operation_id": "move-1" }
            }))
            .unwrap(),
            mod_revision: 1,
            lease_id: 0,
        }];
        assert!(matches!(
            validate_full_stop("/cluster", &records),
            Err(MigrationError::ActiveHandoff)
        ));
    }

    #[test]
    fn every_discovered_type_requires_an_explicit_mapping() {
        let entity_configs = [(serde_json::to_string("invoice").unwrap(), "{}".to_owned())]
            .into_iter()
            .collect();
        assert!(matches!(
            validate_mapping(
                &entity_configs,
                &Default::default(),
                &MigrationDomainMapping {
                    entity_types: Default::default(),
                    singleton_kinds: Default::default(),
                },
            ),
            Err(MigrationError::UnmappedType)
        ));
    }
}
