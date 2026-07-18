use std::fs::OpenOptions;

use lattice_core::actor_ref::ConfigFingerprint;

#[derive(Debug, Serialize)]
struct RawRecord {
    key: String,
    value: Vec<u8>,
    mod_revision: i64,
    lease_id: i64,
}

fn record_suffix<'a>(prefix: &str, record_key: &'a str) -> Result<&'a str, MigrationError> {
    record_key
        .strip_prefix(&format!("{}/", prefix.trim_end_matches('/')))
        .ok_or(MigrationError::InvalidRecord)
}

fn write_backup(path: &PathBuf, records: &[RawRecord]) -> Result<(), MigrationError> {
    let mut options = OpenOptions::new();
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
        lease_id: record.lease(),
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
            lease_id: record.lease(),
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

fn validate_full_stop(prefix: &str, records: &[RawRecord]) -> Result<(), MigrationError> {
    for record in records {
        let suffix = record_suffix(prefix, &record.key)?;
        if suffix == "coordinator/leader"
            || suffix == "membership/leader"
            || (suffix.starts_with("domains/") && suffix.ends_with("/leader"))
        {
            return Err(MigrationError::LeaderPresent);
        }
        let lease_sensitive = suffix.starts_with("members/")
            || suffix.starts_with("shard_claims/")
            || suffix.starts_with("singleton_claims/");
        if lease_sensitive && record.lease_id != 0 {
            return Err(MigrationError::LiveLeasePresent);
        }
        if suffix.starts_with("shards/") || suffix.starts_with("singletons/") {
            let value: serde_json::Value = serde_json::from_slice(&record.value)?;
            let active_move = value
                .get("active_move")
                .is_some_and(|value| !value.is_null());
            let transitional = value
                .get("state")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|state| matches!(state, "BeginHandoff" | "Stopping" | "StopFailed"));
            if active_move || transitional {
                return Err(MigrationError::ActiveHandoff);
            }
        }
        if suffix.starts_with("rebalances/") {
            let value: serde_json::Value = serde_json::from_slice(&record.value)?;
            let active = value
                .get("moves")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|moves| {
                    moves.iter().any(|movement| {
                        movement.get("progress").and_then(serde_json::Value::as_str)
                            == Some("Handoff")
                    })
                });
            if active {
                return Err(MigrationError::ActiveHandoff);
            }
        }
    }
    Ok(())
}

#[derive(Debug)]
struct PreparedRecord {
    targets: BTreeMap<String, Vec<u8>>,
    fenced: bool,
}

fn prepare_record(
    prefix: &str,
    suffix: &str,
    value: &[u8],
    mapping: &MigrationDomainMapping,
    entity_fingerprints: &BTreeMap<String, ConfigFingerprint>,
    mut extra_targets: BTreeMap<String, Vec<u8>>,
    coordinator_term: u64,
) -> Result<Option<PreparedRecord>, MigrationError> {
    if skip_key(suffix) || suffix.starts_with("domains/") || suffix.starts_with("membership/") {
        return Ok(None);
    }
    if suffix.starts_with("members/")
        || suffix.starts_with("operations/")
        || suffix.starts_with("settings/")
        || suffix.starts_with("entity_types/")
        || suffix.starts_with("singleton_types/")
        || suffix.starts_with("shard_claims/")
        || suffix.starts_with("singleton_claims/")
    {
        return Ok(Some(PreparedRecord {
            targets: extra_targets,
            fenced: false,
        }));
    }
    if let Some(rest) = suffix.strip_prefix("shards/") {
        let entity = rest
            .split('/')
            .next()
            .ok_or(MigrationError::InvalidRecord)?;
        let domain = mapping
            .entity_types
            .get(entity)
            .ok_or(MigrationError::UnmappedType)?;
        let transformed = value.to_vec();
        let mut slot = qualify_slot(
            &transformed,
            domain,
            entity_fingerprints.get(entity).copied(),
        )?;
        slot.version = PlacementVersion::new(
            domain.clone(),
            CoordinatorTerm::new(coordinator_term).map_err(|_| MigrationError::InvalidRecord)?,
            slot.version.revision,
        );
        let fenced = slot.owner.is_some();
        slot.state = if fenced {
            PlacementSlotState::Fenced
        } else {
            PlacementSlotState::Unallocated
        };
        slot.target = None;
        slot.active_move = None;
        slot.barrier_sessions.clear();
        extra_targets.insert(
            key(
                prefix,
                &format!("domains/{}/shards/{rest}", domain.as_str()),
            ),
            serde_json::to_vec(&slot)?,
        );
        return Ok(Some(PreparedRecord {
            targets: extra_targets,
            fenced,
        }));
    }
    if let Some(kind) = suffix.strip_prefix("singletons/") {
        let domain = mapping
            .singleton_kinds
            .get(kind)
            .ok_or(MigrationError::UnmappedType)?;
        let transformed = value.to_vec();
        let mut slot = qualify_slot(&transformed, domain, None)?;
        slot.version = PlacementVersion::new(
            domain.clone(),
            CoordinatorTerm::new(coordinator_term).map_err(|_| MigrationError::InvalidRecord)?,
            slot.version.revision,
        );
        let fenced = slot.owner.is_some();
        slot.state = if fenced {
            PlacementSlotState::Fenced
        } else {
            PlacementSlotState::Unallocated
        };
        slot.target = None;
        slot.active_move = None;
        slot.barrier_sessions.clear();
        extra_targets.insert(
            key(
                prefix,
                &format!("domains/{}/singletons/{kind}", domain.as_str()),
            ),
            serde_json::to_vec(&slot)?,
        );
        return Ok(Some(PreparedRecord {
            targets: extra_targets,
            fenced,
        }));
    }
    if let Some(plan_id) = suffix.strip_prefix("rebalances/") {
        let transformed = value.to_vec();
        let plan = qualify_plan(&transformed, mapping, coordinator_term)?;
        extra_targets.insert(
            key(
                prefix,
                &format!("domains/{}/rebalances/{plan_id}", plan.domain.as_str()),
            ),
            serde_json::to_vec(&plan)?,
        );
        return Ok(Some(PreparedRecord {
            targets: extra_targets,
            fenced: false,
        }));
    }
    Err(MigrationError::InvalidRecord)
}

fn qualify_slot(
    value: &[u8],
    domain: &PlacementDomainId,
    fingerprint: Option<ConfigFingerprint>,
) -> Result<PlacementSlot, MigrationError> {
    let mut value: serde_json::Value = serde_json::from_slice(value)?;
    let object = value.as_object_mut().ok_or(MigrationError::InvalidRecord)?;
    let version = object
        .get_mut("version")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or(MigrationError::InvalidRecord)?;
    version.insert("domain".to_owned(), serde_json::to_value(domain)?);
    let key = object
        .get_mut("key")
        .and_then(serde_json::Value::as_object_mut)
        .and_then(|variants| variants.values_mut().next())
        .and_then(serde_json::Value::as_object_mut)
        .ok_or(MigrationError::InvalidRecord)?;
    key.insert("domain".to_owned(), serde_json::to_value(domain)?);
    if let Some(fingerprint) = fingerprint {
        object.insert(
            "config_fingerprint".to_owned(),
            serde_json::to_value(fingerprint)?,
        );
    }
    let slot: PlacementSlot = serde_json::from_value(value)?;
    if slot.key.domain() != domain || slot.version.domain != *domain {
        return Err(MigrationError::InvalidRecord);
    }
    Ok(slot)
}

fn qualify_plan(
    value: &[u8],
    mapping: &MigrationDomainMapping,
    coordinator_term: u64,
) -> Result<RebalancePlan, MigrationError> {
    let mut value: serde_json::Value = serde_json::from_slice(value)?;
    let object = value.as_object_mut().ok_or(MigrationError::InvalidRecord)?;
    let entity = object
        .get("entity_type")
        .and_then(serde_json::Value::as_str)
        .ok_or(MigrationError::InvalidRecord)?;
    let domain = mapping
        .entity_types
        .get(entity)
        .ok_or(MigrationError::UnmappedType)?;
    object.insert("domain".to_owned(), serde_json::to_value(domain)?);
    let base_version = object
        .get_mut("base_version")
        .and_then(serde_json::Value::as_object_mut)
        .ok_or(MigrationError::InvalidRecord)?;
    base_version.insert("domain".to_owned(), serde_json::to_value(domain)?);
    base_version.insert("term".to_owned(), serde_json::json!(coordinator_term));
    if let Some(moves) = object
        .get_mut("moves")
        .and_then(serde_json::Value::as_array_mut)
    {
        for movement in moves {
            if let Some(version) = movement
                .get_mut("barrier_version")
                .and_then(serde_json::Value::as_object_mut)
            {
                version.insert("domain".to_owned(), serde_json::to_value(domain)?);
                version.insert("term".to_owned(), serde_json::json!(coordinator_term));
            }
        }
    }
    let mut plan: RebalancePlan = serde_json::from_value(value)?;
    if matches!(plan.status, PlanStatus::Planned | PlanStatus::Running) {
        plan.status = PlanStatus::Failed;
        for movement in &mut plan.moves {
            if matches!(
                movement.progress,
                MoveProgress::Pending | MoveProgress::Handoff
            ) {
                movement.progress = MoveProgress::Failed;
                movement.barrier_version = None;
                movement.barrier_sessions.clear();
            }
        }
    }
    Ok(plan)
}

fn skip_key(suffix: &str) -> bool {
    suffix == "schema_generation"
        || suffix.starts_with("schema/")
        || suffix.starts_with("coordinator/")
        || suffix.starts_with("counters/")
        || suffix.starts_with("migration/")
}

fn update_counts(
    report: &mut MigrationReport,
    suffix: &str,
    value: &[u8],
    entity_configs: &mut BTreeMap<String, String>,
    singleton_configs: &mut BTreeMap<String, String>,
) -> Result<(), MigrationError> {
    let scoped = if let Some(rest) = suffix.strip_prefix("domains/") {
        rest.split_once('/')
            .map(|(_, scoped)| scoped)
            .ok_or(MigrationError::InvalidRecord)?
    } else if let Some(scoped) = suffix.strip_prefix("membership/") {
        scoped
    } else {
        suffix
    };
    if scoped.starts_with("shards/") || scoped.starts_with("singletons/") {
        report.slots = report.slots.saturating_add(1);
        report.state_revision = report.state_revision.max(record_revision(value)?);
    } else if scoped.starts_with("rebalances/") {
        report.plans = report.plans.saturating_add(1);
    } else if scoped.starts_with("members/") {
        report.members = report.members.saturating_add(1);
        report.state_revision = report.state_revision.max(record_revision(value)?);
        collect_member_configs(value, entity_configs, singleton_configs)?;
        report.entity_configs = entity_configs.len();
        report.singleton_configs = singleton_configs.len();
    } else if scoped.starts_with("admin/") || scoped.starts_with("operations/") {
        report.admin_operations = report.admin_operations.saturating_add(1);
    } else if scoped.starts_with("entity_types/") {
        report.entity_configs = report.entity_configs.saturating_add(1);
    } else if scoped.starts_with("singleton_types/") {
        report.singleton_configs = report.singleton_configs.saturating_add(1);
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
    entity_configs: &mut BTreeMap<String, String>,
    singleton_configs: &mut BTreeMap<String, String>,
) -> Result<(), MigrationError> {
    let value: serde_json::Value = serde_json::from_slice(value)?;
    let hello = value.get("hello").ok_or(MigrationError::InvalidRecord)?;
    collect_configs(hello, "entity_configs", "entity_type", entity_configs)?;
    collect_configs(hello, "singleton_configs", "kind", singleton_configs)
}

fn collect_legacy_config_record(
    suffix: &str,
    value: &[u8],
    entity_configs: &mut BTreeMap<String, String>,
    singleton_configs: &mut BTreeMap<String, String>,
) -> Result<(), MigrationError> {
    let (name, key_name, configs) = if let Some(name) = suffix.strip_prefix("entity_types/") {
        (name, "entity_type", entity_configs)
    } else if let Some(name) = suffix.strip_prefix("singleton_types/") {
        (name, "kind", singleton_configs)
    } else {
        return Ok(());
    };
    if name.is_empty() || name.contains('/') {
        return Err(MigrationError::InvalidRecord);
    }
    let config: serde_json::Value = serde_json::from_slice(value)?;
    if config.get(key_name).and_then(serde_json::Value::as_str) != Some(name) {
        return Err(MigrationError::InvalidRecord);
    }
    let encoded_name = serde_json::to_string(name)?;
    let canonical = serde_json::to_string(&config)?;
    if configs
        .get(&encoded_name)
        .is_some_and(|existing| existing != &canonical)
    {
        return Err(MigrationError::InvalidRecord);
    }
    configs.insert(encoded_name, canonical);
    Ok(())
}

fn collect_configs(
    hello: &serde_json::Value,
    collection: &str,
    key_name: &str,
    configs: &mut BTreeMap<String, String>,
) -> Result<(), MigrationError> {
    let Some(values) = hello.get(collection).and_then(serde_json::Value::as_array) else {
        return Ok(());
    };
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

type ConfigTargets = (
    BTreeMap<String, Vec<u8>>,
    BTreeMap<String, ConfigFingerprint>,
);

fn build_config_targets(
    prefix: &str,
    entity_configs: &BTreeMap<String, String>,
    singleton_configs: &BTreeMap<String, String>,
    mapping: &MigrationDomainMapping,
) -> Result<ConfigTargets, MigrationError> {
    let mut targets = BTreeMap::new();
    let mut fingerprints = BTreeMap::new();
    for (encoded_name, encoded_config) in entity_configs {
        let name: String = serde_json::from_str(encoded_name)?;
        let domain = mapping
            .entity_types
            .get(&name)
            .ok_or(MigrationError::UnmappedType)?;
        let mut value: serde_json::Value = serde_json::from_str(encoded_config)?;
        value
            .as_object_mut()
            .ok_or(MigrationError::InvalidRecord)?
            .insert("domain".to_owned(), serde_json::to_value(domain)?);
        let legacy: EntityConfig = serde_json::from_value(value)?;
        let config = EntityConfig::new(
            domain.clone(),
            legacy.entity_type,
            legacy.protocol_id,
            legacy.shard_count,
            legacy.allocation_policy_id,
            legacy.allocation_policy_version,
            legacy.hard_constraints,
        )
        .map_err(|_| MigrationError::InvalidRecord)?;
        fingerprints.insert(name.clone(), config.fingerprint());
        targets.insert(
            key(
                prefix,
                &format!("domains/{}/entity_types/{name}", domain.as_str()),
            ),
            serde_json::to_vec(&config)?,
        );
    }
    for (encoded_name, encoded_config) in singleton_configs {
        let name: String = serde_json::from_str(encoded_name)?;
        let domain = mapping
            .singleton_kinds
            .get(&name)
            .ok_or(MigrationError::UnmappedType)?;
        let mut value: serde_json::Value = serde_json::from_str(encoded_config)?;
        value
            .as_object_mut()
            .ok_or(MigrationError::InvalidRecord)?
            .insert("domain".to_owned(), serde_json::to_value(domain)?);
        let legacy: SingletonConfig = serde_json::from_value(value)?;
        let config = SingletonConfig::new(domain.clone(), legacy.kind, legacy.protocol_id);
        targets.insert(
            key(
                prefix,
                &format!("domains/{}/singleton_types/{name}", domain.as_str()),
            ),
            serde_json::to_vec(&config)?,
        );
    }
    Ok((targets, fingerprints))
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

fn validate_mapping(
    entity_configs: &BTreeMap<String, String>,
    singleton_configs: &BTreeMap<String, String>,
    mapping: &MigrationDomainMapping,
) -> Result<(), MigrationError> {
    let entities_mapped = entity_configs.keys().all(|key| {
        serde_json::from_str::<String>(key)
            .ok()
            .is_some_and(|key| mapping.entity_types.contains_key(&key))
    });
    let singletons_mapped = singleton_configs.keys().all(|key| {
        serde_json::from_str::<String>(key)
            .ok()
            .is_some_and(|key| mapping.singleton_kinds.contains_key(&key))
    });
    if entities_mapped && singletons_mapped {
        Ok(())
    } else {
        Err(MigrationError::UnmappedType)
    }
}

#[derive(Debug, Default)]
struct DomainInventory {
    slots: usize,
    plans: usize,
    members: usize,
    admin_operations: usize,
    entity_configs: usize,
    singleton_configs: usize,
}

#[derive(Debug, Default)]
struct TargetInventory {
    membership_members: usize,
    domains: BTreeMap<PlacementDomainId, DomainInventory>,
}

fn build_target_inventory(
    prefix: &str,
    mapping: &MigrationDomainMapping,
    records: &[RawRecord],
    prepared: &BTreeMap<String, PreparedRecord>,
    config_targets: &BTreeMap<String, Vec<u8>>,
) -> Result<TargetInventory, MigrationError> {
    let mut targets = BTreeMap::<String, Vec<u8>>::new();
    for record in records {
        let suffix = record_suffix(prefix, &record.key)?;
        if suffix.starts_with("domains/") || suffix.starts_with("membership/") {
            targets.insert(record.key.clone(), record.value.clone());
        }
    }
    for operation in prepared.values() {
        for (target, value) in &operation.targets {
            targets.insert(target.clone(), value.clone());
        }
    }
    targets.extend(config_targets.clone());
    let mut inventory = TargetInventory::default();
    for domain in mapping
        .entity_types
        .values()
        .chain(mapping.singleton_kinds.values())
    {
        inventory.domains.entry(domain.clone()).or_default();
    }
    for target in targets.keys() {
        let suffix = record_suffix(prefix, target)?;
        if suffix.starts_with("membership/members/") {
            inventory.membership_members = inventory.membership_members.saturating_add(1);
            continue;
        }
        let Some(rest) = suffix.strip_prefix("domains/") else {
            continue;
        };
        let (domain, scoped) = rest.split_once('/').ok_or(MigrationError::InvalidRecord)?;
        let domain = PlacementDomainId::new(domain).map_err(|_| MigrationError::InvalidRecord)?;
        let counts = inventory.domains.entry(domain).or_default();
        if scoped.starts_with("shards/") || scoped.starts_with("singletons/") {
            counts.slots = counts.slots.saturating_add(1);
        } else if scoped.starts_with("rebalances/") {
            counts.plans = counts.plans.saturating_add(1);
        } else if scoped.starts_with("members/") {
            counts.members = counts.members.saturating_add(1);
        } else if scoped.starts_with("admin/") {
            counts.admin_operations = counts.admin_operations.saturating_add(1);
        } else if scoped.starts_with("entity_types/") {
            counts.entity_configs = counts.entity_configs.saturating_add(1);
        } else if scoped.starts_with("singleton_types/") {
            counts.singleton_configs = counts.singleton_configs.saturating_add(1);
        }
    }
    Ok(inventory)
}
