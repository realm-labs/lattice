use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Formatter, Result as FmtResult},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use futures_util::{Stream, StreamExt, future::join_all};
use lattice_model::{cluster::CoordinatorScope, run::RunEpoch};

use crate::provider::{
    CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, DiscoveryTarget,
    TerminalLifecycleFuture, validate_snapshot,
};

/// How long the aggregate waits for every provider's first snapshot before it
/// bootstraps from the providers that already answered.
pub const DEFAULT_FIRST_SNAPSHOT_GRACE: Duration = Duration::from_secs(5);

pub struct AggregateDiscovery {
    scope: CoordinatorScope,
    providers: Vec<Arc<dyn CoordinatorDiscovery>>,
    first_snapshot_grace: Duration,
}

impl Debug for AggregateDiscovery {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> FmtResult {
        formatter
            .debug_struct("AggregateDiscovery")
            .field("provider_count", &self.providers.len())
            .finish()
    }
}

impl AggregateDiscovery {
    pub fn new(providers: Vec<Arc<dyn CoordinatorDiscovery>>) -> Result<Self, DiscoveryError> {
        Self::with_first_snapshot_grace(providers, DEFAULT_FIRST_SNAPSHOT_GRACE)
    }

    /// Bounds bootstrap: a provider that has not produced its first snapshot
    /// within `first_snapshot_grace` joins the merge as temporarily empty, so
    /// one unreachable backend cannot hide the providers that did answer.
    pub fn with_first_snapshot_grace(
        providers: Vec<Arc<dyn CoordinatorDiscovery>>,
        first_snapshot_grace: Duration,
    ) -> Result<Self, DiscoveryError> {
        if providers.is_empty() {
            return Err(DiscoveryError::InvalidConfiguration {
                message: "aggregate discovery requires at least one provider".to_string(),
            });
        }
        if first_snapshot_grace.is_zero() {
            return Err(DiscoveryError::InvalidConfiguration {
                message: "aggregate discovery first snapshot grace must be nonzero".to_string(),
            });
        }
        let scope = providers[0].scope().clone();
        if providers.iter().any(|provider| provider.scope() != &scope) {
            return Err(DiscoveryError::InvalidConfiguration {
                message: "aggregate discovery providers must have one Coordinator scope"
                    .to_string(),
            });
        }
        Ok(Self {
            scope,
            providers,
            first_snapshot_grace,
        })
    }
}

impl CoordinatorDiscovery for AggregateDiscovery {
    fn terminal_lifecycle(&self, epoch: RunEpoch) -> TerminalLifecycleFuture<'_> {
        Box::pin(async move {
            let results = join_all(
                self.providers
                    .iter()
                    .map(|provider| provider.terminal_lifecycle(epoch)),
            )
            .await;
            let mut selected = None;
            for result in results {
                if let Ok(Some(completion)) = result {
                    if selected
                        .as_ref()
                        .is_some_and(|current| current != &completion)
                    {
                        return Err(DiscoveryError::InvalidSnapshot {
                            message: "discovery providers returned conflicting terminal receipts"
                                .to_owned(),
                        });
                    }
                    selected = Some(completion);
                }
            }
            Ok(selected)
        })
    }
    fn scope(&self) -> &CoordinatorScope {
        &self.scope
    }

    fn snapshots(
        &self,
    ) -> Pin<Box<dyn Stream<Item = Result<CoordinatorDirectorySnapshot, DiscoveryError>> + Send + '_>>
    {
        let streams = self
            .providers
            .iter()
            .enumerate()
            .map(|(index, provider)| provider.snapshots().map(move |item| (index, item)))
            .collect::<Vec<_>>();

        Box::pin(async_stream::stream! {
            let mut incoming = futures_util::stream::select_all(streams);
            let mut provider_snapshots = vec![None; self.providers.len()];
            let mut provider_generations = vec![0_u64; self.providers.len()];
            let mut observed = BTreeSet::new();
            let mut rotations = BTreeMap::<u16, usize>::new();
            let mut output_generation = 0_u64;
            let mut emitted = false;
            let mut grace_expired = false;
            let mut last_merged = None;
            let grace = tokio::time::sleep(self.first_snapshot_grace);
            tokio::pin!(grace);

            loop {
                let event = tokio::select! {
                    biased;
                    item = incoming.next() => match item {
                        Some((index, item)) => AggregateEvent::Update(index, item),
                        None => AggregateEvent::Closed,
                    },
                    () = &mut grace, if !grace_expired && !emitted => AggregateEvent::Grace,
                };

                let mut updated = false;
                match event {
                    AggregateEvent::Closed => break,
                    AggregateEvent::Grace => grace_expired = true,
                    AggregateEvent::Update(index, item) => {
                        observed.insert(index);
                        match item {
                            Ok(snapshot) => {
                                if let Err(error) = validate_snapshot(&snapshot) {
                                    yield Err(error);
                                } else if snapshot.scope != self.scope {
                                    yield Err(DiscoveryError::InvalidSnapshot {
                                        message: format!("provider {index} returned a different scope"),
                                    });
                                } else if snapshot.generation <= provider_generations[index] {
                                    yield Err(DiscoveryError::InvalidSnapshot {
                                        message: format!(
                                            "provider {index} generation {} does not follow {}",
                                            snapshot.generation, provider_generations[index]
                                        ),
                                    });
                                } else {
                                    provider_generations[index] = snapshot.generation;
                                    provider_snapshots[index] = Some(snapshot);
                                    updated = true;
                                }
                            }
                            Err(error) => yield Err(error),
                        }
                    }
                }

                let complete = grace_expired || observed.len() == self.providers.len();
                if !complete || (!updated && emitted) {
                    continue;
                }
                match merge_targets(&provider_snapshots) {
                    Ok(targets) => {
                        let hint_addresses = provider_snapshots.iter()
                            .filter_map(Option::as_ref)
                            .filter_map(|snapshot| snapshot.leader_hint.as_ref())
                            .map(|target| &target.address)
                            .collect::<BTreeSet<_>>();
                        // Conflicting hints are merely seeds. Probe all of them
                        // rather than inventing an authoritative winner here.
                        let leader_hint = if hint_addresses.len() == 1 {
                            targets.iter().find(|target| hint_addresses.contains(&target.address)).cloned()
                        } else {
                            None
                        };
                        let merged = (targets.clone(), leader_hint.clone());
                        if last_merged.as_ref() == Some(&merged) {
                            continue;
                        }
                        last_merged = Some(merged);
                        output_generation += 1;
                        emitted = true;
                        yield Ok(CoordinatorDirectorySnapshot {
                            scope: self.scope.clone(),
                            generation: output_generation,
                            leader_hint,
                            targets: rotate_targets(targets, &mut rotations),
                        });
                    }
                    Err(error) => yield Err(error),
                }
            }
        })
    }
}

enum AggregateEvent {
    Update(usize, Result<CoordinatorDirectorySnapshot, DiscoveryError>),
    Grace,
    Closed,
}

fn merge_targets(
    snapshots: &[Option<CoordinatorDirectorySnapshot>],
) -> Result<Vec<DiscoveryTarget>, DiscoveryError> {
    let mut merged = BTreeMap::new();
    for target in snapshots
        .iter()
        .filter_map(Option::as_ref)
        .flat_map(|snapshot| snapshot.targets.iter().chain(snapshot.leader_hint.iter()))
    {
        match merged.get_mut(&target.address) {
            None => {
                merged.insert(target.address.clone(), target.clone());
            }
            Some(current) => {
                if current.tls_server_name() != target.tls_server_name() {
                    return Err(DiscoveryError::InvalidSnapshot {
                        message: format!(
                            "target {} has conflicting TLS expectations",
                            target.address
                        ),
                    });
                }
                if let (Some(left), Some(right)) =
                    (&current.expected_node_id, &target.expected_node_id)
                    && left != right
                {
                    return Err(DiscoveryError::InvalidSnapshot {
                        message: format!(
                            "target {} has conflicting expected node IDs {left} and {right}",
                            target.address
                        ),
                    });
                }
                if current.expected_node_id.is_none() {
                    current
                        .expected_node_id
                        .clone_from(&target.expected_node_id);
                }
                current.priority = current.priority.min(target.priority);
                current.source.merge(&target.source);
            }
        }
    }

    let mut output = merged.into_values().collect::<Vec<_>>();
    output.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| left.address.cmp(&right.address))
    });
    Ok(output)
}

fn rotate_targets(
    targets: Vec<DiscoveryTarget>,
    rotations: &mut BTreeMap<u16, usize>,
) -> Vec<DiscoveryTarget> {
    let mut by_priority = BTreeMap::<u16, Vec<DiscoveryTarget>>::new();
    for target in targets {
        by_priority.entry(target.priority).or_default().push(target);
    }
    let mut output = Vec::new();
    for (priority, mut targets) in by_priority {
        let cursor = rotations.entry(priority).or_default();
        if !targets.is_empty() {
            let target_count = targets.len();
            targets.rotate_left(*cursor % target_count);
            *cursor = cursor.wrapping_add(1);
        }
        output.extend(targets);
    }
    output
}
