use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Formatter, Result as FmtResult},
    pin::Pin,
    sync::Arc,
    time::Duration,
};

use futures_util::{Stream, StreamExt};
use lattice_core::coordinator::CoordinatorScope;

use crate::provider::{
    CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, DiscoveryTarget,
    validate_snapshot,
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
            let mut last_merged: Option<Vec<DiscoveryTarget>> = None;
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
                        if last_merged.as_ref() == Some(&targets) {
                            continue;
                        }
                        last_merged = Some(targets.clone());
                        output_generation += 1;
                        emitted = true;
                        yield Ok(CoordinatorDirectorySnapshot {
                            scope: self.scope.clone(),
                            generation: output_generation,
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
        .flat_map(|snapshot| &snapshot.targets)
    {
        match merged.get_mut(&target.address) {
            None => {
                merged.insert(target.address.clone(), target.clone());
            }
            Some(current) => {
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
