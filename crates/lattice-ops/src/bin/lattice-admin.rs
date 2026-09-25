//! Privileged control-plane policy and offline run maintenance. Credentials are intentionally independent
//! of node configuration; this tool never starts a Coordinator or grants leases.

use std::{error::Error, fs, net::SocketAddr, path::PathBuf, time::Duration};

use clap::{Parser, Subcommand};
use etcd_client::{Certificate, Client, ConnectOptions, GetOptions, Identity, TlsOptions};
use lattice_coordination::candidates::CandidateChange;
use lattice_coordination::storage::{
    ClusterLifecycleStore, CoordinatorLeaseStore,
    candidates::CandidateStore,
    etcd::EtcdCoordinationStore,
    maintenance::{ClusterResetStore, StoppedDeployment},
    records::DurableStorageLimits,
};
use lattice_model::{
    cluster::{ActorGroupId, CoordinatorScope},
    framework::LatticeVersion,
    run::{ControlOperationId, RunEpoch},
};

type AdminResult<T> = Result<T, Box<dyn Error>>;

#[derive(Parser)]
#[command(
    about = "Inspect candidate policy, explicitly change eligibility, or maintain an offline run"
)]
struct Arguments {
    #[arg(long, value_delimiter = ',', required = true)]
    endpoints: Vec<String>,
    #[arg(long)]
    namespace: String,
    #[arg(long)]
    epoch: u64,
    /// Allow plaintext only for loopback test endpoints. Never use in production.
    #[arg(long)]
    allow_insecure_local: bool,
    #[arg(long)]
    ca: Option<PathBuf>,
    #[arg(long, requires = "key")]
    cert: Option<PathBuf>,
    #[arg(long, requires = "cert")]
    key: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Inspect eligibility and leased online candidates for one exact scope.
    Candidates {
        #[arg(long)]
        group: Option<String>,
    },
    /// Explicit privileged policy change. No election or implicit self-enrollment.
    Candidate {
        #[arg(long)]
        group: Option<String>,
        #[arg(long)]
        node_id: String,
        #[arg(long)]
        expected_revision: u64,
        #[arg(long)]
        operation: String,
        #[arg(long)]
        disable: bool,
        #[arg(long)]
        execute: bool,
    },
    /// Show current lifecycle and remaining runtime-key count; never writes.
    Inspect,
    /// Preview by default. Execute only after confirming all old processes stopped.
    Reset {
        #[arg(long)]
        operation: String,
        #[arg(long)]
        execute: bool,
        /// Must exactly equal <namespace>@<epoch>, including the leading slash.
        #[arg(long)]
        confirm_stopped: Option<String>,
        #[arg(long, default_value_t = 32)]
        batch_keys: usize,
        #[arg(long, default_value_t = 256)]
        maximum_batches: usize,
    },
    /// Preview by default. Requires a Closed run with confirmed empty runtime data.
    StartRun {
        #[arg(long)]
        execute: bool,
    },
}

#[tokio::main]
async fn main() -> AdminResult<()> {
    let arguments = Arguments::parse();
    let epoch = RunEpoch::new(arguments.epoch).ok_or("epoch must be nonzero")?;
    let (store, mut raw) = connect(&arguments).await?;
    let lifecycle = store.lifecycle().await?;
    if lifecycle.epoch != epoch {
        return Err("requested epoch is not the current run".into());
    }
    let prefix = format!("{}/runs/{}/", arguments.namespace, epoch.get());
    let count = raw
        .get(
            prefix,
            Some(GetOptions::new().with_prefix().with_count_only()),
        )
        .await?
        .count();
    println!(
        "namespace={} epoch={} runtime_keys={count}",
        arguments.namespace,
        epoch.get()
    );
    println!("lifecycle={}", serde_json::to_string(&lifecycle)?);
    match arguments.command {
        Command::Inspect => {}
        Command::Candidates { group } => {
            store.ensure_framework().await?;
            let scope = candidate_scope(group)?;
            println!(
                "policy={}",
                serde_json::to_string(&store.candidate_set(&scope).await?)?
            );
            println!(
                "online={}",
                serde_json::to_string(&store.online_candidates(&scope).await?)?
            );
        }
        Command::Candidate {
            group,
            node_id,
            expected_revision,
            operation,
            disable,
            execute,
        } => {
            let scope = candidate_scope(group)?;
            let request = CandidateChange {
                epoch,
                scope,
                node_id,
                expected_revision,
                operation: ControlOperationId::new(operation)?,
                enabled: !disable,
            };
            println!("candidate_change={}", serde_json::to_string(&request)?);
            if !execute {
                println!("DRY RUN: no candidate policy changed");
                return Ok(());
            }
            store.ensure_framework().await?;
            println!(
                "result={}",
                serde_json::to_string(&store.change_candidate(request).await?)?
            );
        }
        Command::Reset {
            operation,
            execute,
            confirm_stopped,
            batch_keys,
            maximum_batches,
        } => {
            let operation = ControlOperationId::new(operation)?;
            if !(1..=32).contains(&batch_keys) || maximum_batches == 0 {
                return Err(
                    "batch-keys must be 1..=32 and maximum-batches must be positive".into(),
                );
            }
            if !execute {
                println!(
                    "DRY RUN: reset freezes writers and deletes only reviewed runtime families; definitions and safety history remain."
                );
                return Ok(());
            }
            let required = format!("{}@{}", arguments.namespace, epoch.get());
            if confirm_stopped.as_deref() != Some(&required) {
                return Err(format!("explicit stopped-deployment confirmation required: --confirm-stopped {required}").into());
            }
            store
                .begin_reset(
                    &lifecycle,
                    operation.clone(),
                    &StoppedDeployment {
                        namespace: arguments.namespace,
                        epoch,
                    },
                )
                .await?;
            for _ in 0..maximum_batches {
                let progress = store.reset_batch(epoch, &operation, batch_keys).await?;
                println!(
                    "operation={} deleted={} complete={}",
                    operation.as_str(),
                    progress.deleted_keys,
                    progress.complete
                );
                if progress.complete {
                    return Ok(());
                }
            }
            return Err(
                "batch budget exhausted; reset remains fenced. Resume with the same operation ID"
                    .into(),
            );
        }
        Command::StartRun { execute } => {
            if !execute {
                println!(
                    "DRY RUN: start a new epoch only if Closed and cleanup is confirmed complete."
                );
                return Ok(());
            }
            let next = store.start_new_run(&lifecycle).await?;
            println!("new_epoch={}", next.get());
        }
    }
    Ok(())
}

async fn connect(arguments: &Arguments) -> AdminResult<(EtcdCoordinationStore, Client)> {
    for endpoint in &arguments.endpoints {
        if endpoint.starts_with("https://") {
            continue;
        }
        let local = endpoint
            .strip_prefix("http://")
            .and_then(|address| address.parse::<SocketAddr>().ok())
            .is_some_and(|address| address.ip().is_loopback());
        if !arguments.allow_insecure_local || !local {
            return Err(
                "use HTTPS; plaintext requires --allow-insecure-local and a loopback endpoint"
                    .into(),
            );
        }
    }
    let mut options = ConnectOptions::new()
        .with_connect_timeout(Duration::from_secs(5))
        .with_timeout(Duration::from_secs(5));
    let mut tls = TlsOptions::new();
    if let Some(path) = &arguments.ca {
        tls = tls.ca_certificate(Certificate::from_pem(fs::read(path)?));
    }
    if let (Some(cert), Some(key)) = (&arguments.cert, &arguments.key) {
        tls = tls.identity(Identity::from_pem(fs::read(cert)?, fs::read(key)?));
    }
    if arguments
        .endpoints
        .iter()
        .any(|endpoint| endpoint.starts_with("https://"))
    {
        options = options.with_tls(tls);
    }
    // Use separate administrative credentials. No passwords in process arguments,
    // output, or Debug logs; deployment tooling supplies these environment values.
    match (
        std::env::var("LATTICE_ADMIN_ETCD_USER"),
        std::env::var("LATTICE_ADMIN_ETCD_PASSWORD"),
    ) {
        (Ok(user), Ok(password)) => options = options.with_user(user, password),
        (Err(_), Err(_)) => {}
        _ => return Err("set both LATTICE_ADMIN_ETCD_USER and LATTICE_ADMIN_ETCD_PASSWORD".into()),
    }
    let mut client = Client::connect(&arguments.endpoints, Some(options)).await?;
    let marker = client
        .get(format!("{}/meta/framework", arguments.namespace), None)
        .await?;
    if marker
        .kvs()
        .first()
        .is_none_or(|entry| entry.value() != LatticeVersion::CURRENT.as_bytes())
    {
        return Err(
            "framework marker missing or mismatched; this tool does not upgrade legacy schemas"
                .into(),
        );
    }
    let limits = client
        .get(format!("{}/schema/limits", arguments.namespace), None)
        .await?;
    let limits: DurableStorageLimits = serde_json::from_slice(
        limits
            .kvs()
            .first()
            .ok_or("durable limits missing")?
            .value(),
    )?;
    let store = EtcdCoordinationStore::from_client(
        client.clone(),
        arguments.namespace.clone(),
        32,
        limits,
    )?;
    Ok((store, client))
}
fn candidate_scope(group: Option<String>) -> AdminResult<CoordinatorScope> {
    Ok(match group {
        Some(group) => CoordinatorScope::Group(ActorGroupId::new(group)?),
        None => CoordinatorScope::Cluster,
    })
}
