//! Read-only etcd discovery for ordinary nodes. This adapter reads only framework,
//! run lifecycle, the requested scope's leader and leased candidate endpoints. It
//! never initializes a namespace, grants a lease, or loads membership/shard data.
//!
//! Wrap one instance in `lattice_discovery::shared::SharedDiscovery` to fan its
//! watch out to multiple consumers. Endpoint hints still require bootstrap and
//! authenticated control-session admission; they are never serving authority.

use std::{future::Future, pin::Pin, time::Duration};

use etcd_client::{Client, ConnectOptions, GetOptions, WatchOptions, WatchStream};
use futures_util::Stream;
use lattice_discovery::provider::{
    CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, DiscoveryOrigin,
    DiscoverySource, DiscoveryTarget, TerminalLifecycleFuture, validate_snapshot,
};
use lattice_model::{
    cluster::CoordinatorScope,
    framework::LatticeVersion,
    run::{ClusterLifecycle, RunEpoch, RunPhase},
};

use crate::{
    candidates::{CandidateRegistration, MAX_CANDIDATES_PER_SCOPE},
    coordinator::LeaderRecord,
};

const MAX_VALUE_BYTES: usize = 4096;

#[derive(Clone)]
pub struct EtcdCoordinatorDiscovery {
    client: Client,
    prefix: String,
    scope: CoordinatorScope,
    timeout: Duration,
    reconnect_delay: Duration,
}

impl EtcdCoordinatorDiscovery {
    pub async fn connect(
        endpoints: &[String],
        options: Option<ConnectOptions>,
        prefix: impl Into<String>,
        scope: CoordinatorScope,
    ) -> Result<Self, DiscoveryError> {
        let client =
            tokio::time::timeout(Duration::from_secs(5), Client::connect(endpoints, options))
                .await
                .map_err(|_| error("etcd connection timed out"))?
                .map_err(error)?;
        Self::from_client(client, prefix, scope)
    }

    /// Supply a client configured with read/watch-only credentials. No write capability
    /// is used by this adapter, even for an empty or incompatible cluster namespace.
    pub fn from_client(
        client: Client,
        prefix: impl Into<String>,
        scope: CoordinatorScope,
    ) -> Result<Self, DiscoveryError> {
        let prefix = prefix.into();
        if !prefix.starts_with('/')
            || prefix.ends_with('/')
            || prefix.len() > 1024
            || prefix.split('/').any(|part| matches!(part, "." | ".."))
        {
            return Err(DiscoveryError::InvalidConfiguration {
                message: "discovery requires an exact non-root namespace without trailing slash"
                    .into(),
            });
        }
        Ok(Self {
            client,
            prefix,
            scope,
            timeout: Duration::from_secs(5),
            reconnect_delay: Duration::from_millis(250),
        })
    }

    async fn call<T>(
        &self,
        operation: impl Future<Output = Result<T, etcd_client::Error>>,
    ) -> Result<T, DiscoveryError> {
        tokio::time::timeout(self.timeout, operation)
            .await
            .map_err(|_| error("etcd read/watch setup timed out"))?
            .map_err(error)
    }

    async fn read_directory(
        &self,
    ) -> Result<(CoordinatorDirectorySnapshot, i64, Vec<(String, bool)>), DiscoveryError> {
        let framework_key = format!("{}/meta/framework", self.prefix);
        let lifecycle_key = format!("{}/meta/lifecycle", self.prefix);
        let mut client = self.client.clone();
        // A linearizable first read fixes the MVCC revision for every following
        // record, so run changes cannot mix old candidates with a new lifecycle.
        let framework = self.call(client.get(framework_key.clone(), None)).await?;
        if framework
            .kvs()
            .first()
            .is_none_or(|kv| kv.value() != LatticeVersion::CURRENT.as_bytes())
        {
            return Err(error("framework marker is missing or incompatible"));
        }
        let revision = framework
            .header()
            .ok_or_else(|| error("missing revision"))?
            .revision();
        let lifecycle = self
            .call(client.get(
                lifecycle_key.clone(),
                Some(GetOptions::new().with_revision(revision)),
            ))
            .await?;
        let lifecycle: ClusterLifecycle = decode(
            lifecycle
                .kvs()
                .first()
                .ok_or_else(|| error("missing lifecycle"))?
                .value(),
        )?;
        let mut snapshot = CoordinatorDirectorySnapshot {
            scope: self.scope.clone(),
            generation: 1,
            leader_hint: None,
            targets: Vec::new(),
        };
        let mut watches = vec![(framework_key, false), (lifecycle_key, false)];
        if !lifecycle.permits_election() {
            return Ok((snapshot, revision, watches));
        }
        let run = format!("{}/runs/{}", self.prefix, lifecycle.epoch.get());
        let (leader_key, candidates) = match &self.scope {
            CoordinatorScope::Cluster => (
                format!("{run}/membership/leader"),
                format!("{run}/candidates/cluster/"),
            ),
            CoordinatorScope::Group(group) => (
                format!("{run}/groups/{}/leader", group.as_str()),
                format!("{run}/candidates/groups/{}/", group.as_str()),
            ),
        };
        let leader = self
            .call(client.get(
                leader_key.clone(),
                Some(GetOptions::new().with_revision(revision)),
            ))
            .await?;
        let online = self
            .call(
                client.get(
                    candidates.clone(),
                    Some(
                        GetOptions::new()
                            .with_prefix()
                            .with_revision(revision)
                            .with_limit((MAX_CANDIDATES_PER_SCOPE + 1) as i64),
                    ),
                ),
            )
            .await?;
        if online.more() || online.kvs().len() > MAX_CANDIDATES_PER_SCOPE {
            return Err(error("candidate discovery limit exceeded"));
        }
        let source = DiscoverySource::single(DiscoveryOrigin::Etcd {
            prefix: self.prefix.clone(),
            run_epoch: lifecycle.epoch,
        });
        let mut registrations = Vec::with_capacity(online.kvs().len());
        for kv in online.kvs() {
            let registration: CandidateRegistration = decode(kv.value())?;
            if registration.epoch != lifecycle.epoch
                || registration.authorization.scope != self.scope
                || registration.node.node_id != registration.authorization.node_id
                || registration.lease_id != kv.lease()
                || kv.lease() <= 0
            {
                return Err(error(
                    "candidate registration does not match its run/scope/lease",
                ));
            }
            registration.node.validate().map_err(error)?;
            registrations.push(registration.clone());
            snapshot.targets.push(DiscoveryTarget {
                address: registration.node.address,
                expected_node_id: Some(registration.node.node_id),
                source: source.clone(),
                priority: 1,
            });
        }
        if let Some(kv) = leader.kvs().first() {
            let leader: LeaderRecord = decode(kv.value())?;
            if leader.epoch != lifecycle.epoch || leader.scope != self.scope || kv.lease() <= 0 {
                return Err(error("leader hint does not match its run/scope/lease"));
            }
            leader.validate().map_err(error)?;
            if registrations.contains(&leader.candidate_registration()) {
                snapshot.leader_hint = Some(DiscoveryTarget {
                    address: leader.node.address,
                    expected_node_id: Some(leader.node.node_id),
                    source,
                    priority: 0,
                });
            }
        }
        watches.extend([(leader_key, false), (candidates, true)]);
        validate_snapshot(&snapshot)?;
        Ok((snapshot, revision, watches))
    }

    async fn watch_directory(
        &self,
        revision: i64,
        keys: Vec<(String, bool)>,
    ) -> Result<WatchStream, DiscoveryError> {
        let mut client = self.client.clone();
        let mut keys = keys.into_iter();
        let (key, prefix) = keys.next().ok_or_else(|| error("empty discovery watch"))?;
        let options = |prefix| {
            let opts = WatchOptions::new().with_start_revision(revision.saturating_add(1));
            if prefix { opts.with_prefix() } else { opts }
        };
        let mut stream = self.call(client.watch(key, Some(options(prefix)))).await?;
        for (key, prefix) in keys {
            self.call(stream.watch(key, Some(options(prefix)))).await?;
        }
        Ok(stream)
    }
}

impl CoordinatorDiscovery for EtcdCoordinatorDiscovery {
    fn terminal_lifecycle(&self, epoch: RunEpoch) -> TerminalLifecycleFuture<'_> {
        Box::pin(async move {
            let mut client = self.client.clone();
            let framework = self
                .call(client.get(format!("{}/meta/framework", self.prefix), None))
                .await?;
            if framework
                .kvs()
                .first()
                .is_none_or(|kv| kv.value() != LatticeVersion::CURRENT.as_bytes())
            {
                return Err(error("framework marker missing or incompatible"));
            }
            let revision = framework
                .header()
                .ok_or_else(|| error("missing revision"))?
                .revision();
            for suffix in ["meta/lifecycle", "meta/last_completion"] {
                let result = self
                    .call(client.get(
                        format!("{}/{suffix}", self.prefix),
                        Some(GetOptions::new().with_revision(revision)),
                    ))
                    .await?;
                if let Some(kv) = result.kvs().first() {
                    let lifecycle: ClusterLifecycle = decode(kv.value())?;
                    if lifecycle.epoch == epoch
                        && matches!(lifecycle.phase, RunPhase::Closed { .. })
                    {
                        return Ok(Some(lifecycle));
                    }
                }
            }
            Ok(None)
        })
    }
    fn scope(&self) -> &CoordinatorScope {
        &self.scope
    }
    fn snapshots(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CoordinatorDirectorySnapshot, DiscoveryError>> + Send + '_>>
    {
        Box::pin(async_stream::stream! {
            let mut generation = 0_u64;
            loop {
                let (mut snapshot, revision, keys) = match self.read_directory().await {
                    Ok(value) => value,
                    Err(error) => { yield Err(error); tokio::time::sleep(self.reconnect_delay).await; continue; }
                };
                generation = match generation.checked_add(1) { Some(next) => next, None => { yield Err(error("discovery generation exhausted")); break; } };
                snapshot.generation = generation;
                yield Ok(snapshot);
                let mut watch = match self.watch_directory(revision, keys).await {
                    Ok(watch) => watch,
                    Err(error) => { yield Err(error); tokio::time::sleep(self.reconnect_delay).await; continue; }
                };
                loop {
                    match watch.message().await {
                        Ok(Some(update)) if update.canceled() => { yield Err(error("discovery watch canceled or compacted; rebuilding snapshot")); break; }
                        Ok(Some(update)) if !update.events().is_empty() => break,
                        Ok(Some(_)) => continue,
                        Ok(None) => { yield Err(error("discovery watch closed")); break; }
                        Err(failure) => { yield Err(error(failure)); break; }
                    }
                }
                // Drop the complete watch stream before resnapshot/reconnect. No
                // detached watcher task survives this subscription's cancellation.
                drop(watch);
                tokio::time::sleep(self.reconnect_delay).await;
            }
        })
    }
}

fn decode<T: serde::de::DeserializeOwned>(value: &[u8]) -> Result<T, DiscoveryError> {
    if value.len() > MAX_VALUE_BYTES {
        return Err(error("discovery value exceeds 4 KiB"));
    }
    serde_json::from_slice(value).map_err(error)
}
fn error(message: impl std::fmt::Display) -> DiscoveryError {
    DiscoveryError::Provider {
        provider: "etcd-coordinator",
        message: message.to_string(),
    }
}
