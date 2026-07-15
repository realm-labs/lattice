use std::collections::BTreeMap;
use std::path::PathBuf;

use lattice_placement::storage::domain::DurableStorageLimits;
use lattice_placement::storage::etcd::migration::{
    CardinalityMode, MigrationConfig, MigrationDomainMapping, MigrationMode, execute,
    execute_cardinality,
};

const USAGE: &str = "usage: lattice-placement-migrate \
<inspect|dry-run|apply|resume|inspect-counters|repair-counters> \
--endpoints <url[,url]> --prefix <cluster-prefix> --page-size <n> \
--max-slots <n> --max-plans <n> --max-members <n> --max-admin-operations <n> \
--max-entity-configs <n> --max-singleton-configs <n> \
--mapping <domain-mapping.json> [--backup <create-new-path>]";

#[derive(Clone, Copy)]
enum Command {
    Migration(MigrationMode),
    Cardinality(CardinalityMode),
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(()) => {}
        Err(error) => {
            eprintln!("migration failed: {error}");
            std::process::exit(2);
        }
    }
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let command = match args.next().as_deref() {
        Some("inspect") => Command::Migration(MigrationMode::Inspect),
        Some("dry-run") => Command::Migration(MigrationMode::DryRun),
        Some("apply") => Command::Migration(MigrationMode::Apply),
        Some("resume") => Command::Migration(MigrationMode::Resume),
        Some("inspect-counters") => Command::Cardinality(CardinalityMode::Inspect),
        Some("repair-counters") => Command::Cardinality(CardinalityMode::Repair),
        _ => return Err(USAGE.into()),
    };
    let mut values = BTreeMap::new();
    while let Some(flag) = args.next() {
        if !flag.starts_with("--") || values.contains_key(&flag) {
            return Err(USAGE.into());
        }
        let value = args.next().ok_or(USAGE)?;
        values.insert(flag, value);
    }
    let endpoints = required(&values, "--endpoints")?
        .split(',')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    let mapping_path = PathBuf::from(required(&values, "--mapping")?);
    let mapping: MigrationDomainMapping = serde_json::from_slice(&std::fs::read(&mapping_path)?)?;
    let config = MigrationConfig {
        endpoints,
        cluster_prefix: required(&values, "--prefix")?.to_owned(),
        page_size: number(&values, "--page-size")?,
        limits: DurableStorageLimits {
            maximum_slots: number(&values, "--max-slots")?,
            maximum_plans: number(&values, "--max-plans")?,
            maximum_members: number(&values, "--max-members")?,
            maximum_admin_operations: number(&values, "--max-admin-operations")?,
            maximum_entity_configs: number(&values, "--max-entity-configs")?,
            maximum_singleton_configs: number(&values, "--max-singleton-configs")?,
        },
        backup_path: values.get("--backup").map(PathBuf::from),
        mapping,
    };
    let known = [
        "--endpoints",
        "--prefix",
        "--page-size",
        "--max-slots",
        "--max-plans",
        "--max-members",
        "--max-admin-operations",
        "--max-entity-configs",
        "--max-singleton-configs",
        "--mapping",
        "--backup",
    ];
    if values.keys().any(|key| !known.contains(&key.as_str()))
        || (matches!(command, Command::Migration(MigrationMode::Apply))
            && config.backup_path.is_none())
    {
        return Err(USAGE.into());
    }
    let output = match command {
        Command::Migration(mode) => serde_json::to_value(execute(mode, config).await?)?,
        Command::Cardinality(mode) => {
            serde_json::to_value(execute_cardinality(mode, config).await?)?
        }
    };
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn required<'a>(
    values: &'a BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str, Box<dyn std::error::Error>> {
    values
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| USAGE.into())
}

fn number(
    values: &BTreeMap<String, String>,
    name: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    Ok(required(values, name)?.parse()?)
}
