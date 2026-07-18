use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::{Debug, Formatter, Result as FmtResult},
    pin::Pin,
    sync::Arc,
};

use futures_util::{Stream, StreamExt};
use lattice_core::coordinator::CoordinatorScope;

use crate::provider::{
    CoordinatorDirectorySnapshot, CoordinatorDiscovery, DiscoveryError, DiscoveryTarget,
    validate_snapshot,
};

pub struct AggregateDiscovery {
    scope: CoordinatorScope,
    providers: Vec<Arc<dyn CoordinatorDiscovery>>,
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
        if providers.is_empty() {
            return Err(DiscoveryError::InvalidConfiguration {
                message: "aggregate discovery requires at least one provider".to_string(),
            });
        }
        let scope = providers[0].scope().clone();
        if providers.iter().any(|provider| provider.scope() != &scope) {
            return Err(DiscoveryError::InvalidConfiguration {
                message: "aggregate discovery providers must have one Coordinator scope"
                    .to_string(),
            });
        }
        Ok(Self { scope, providers })
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

            while let Some((index, item)) = incoming.next().await {
                observed.insert(index);
                let updated = match item {
                    Ok(snapshot) => {
                        if let Err(error) = validate_snapshot(&snapshot) {
                            yield Err(error);
                            false
                        } else if snapshot.generation <= provider_generations[index] {
                            yield Err(DiscoveryError::InvalidSnapshot {
                                message: format!(
                                    "provider {index} generation {} does not follow {}",
                                    snapshot.generation, provider_generations[index]
                                ),
                            });
                            false
                        } else {
                            provider_generations[index] = snapshot.generation;
                            provider_snapshots[index] = Some(snapshot);
                            true
                        }
                    }
                    Err(error) => {
                        yield Err(error);
                        false
                    }
                };

                if observed.len() != self.providers.len() || (!updated && emitted) {
                    continue;
                }
                match merge_targets(&provider_snapshots, &mut rotations) {
                    Ok(targets) => {
                        output_generation += 1;
                        emitted = true;
                        yield Ok(CoordinatorDirectorySnapshot { scope: self.scope.clone(), generation: output_generation, targets });
                    }
                    Err(error) => yield Err(error),
                }
            }
        })
    }
}

fn merge_targets(
    snapshots: &[Option<CoordinatorDirectorySnapshot>],
    rotations: &mut BTreeMap<u16, usize>,
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

    let mut by_priority = BTreeMap::<u16, Vec<DiscoveryTarget>>::new();
    for target in merged.into_values() {
        by_priority.entry(target.priority).or_default().push(target);
    }
    let mut output = Vec::new();
    for (priority, mut targets) in by_priority {
        targets.sort_by(|left, right| left.address.cmp(&right.address));
        let cursor = rotations.entry(priority).or_default();
        if !targets.is_empty() {
            let target_count = targets.len();
            targets.rotate_left(*cursor % target_count);
            *cursor = cursor.wrapping_add(1);
        }
        output.extend(targets);
    }
    Ok(output)
}
