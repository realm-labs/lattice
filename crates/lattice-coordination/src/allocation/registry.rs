use std::{collections::BTreeMap, fmt, sync::Arc};

use thiserror::Error;

use super::{ShardAllocationStrategy, WeightedLeastLoad};

type StrategyKey = (String, u32);

/// Strategies installed into every placement leader incarnation.
#[derive(Clone)]
pub struct ShardAllocationStrategies {
    strategies: BTreeMap<StrategyKey, Arc<dyn ShardAllocationStrategy>>,
}

impl Default for ShardAllocationStrategies {
    fn default() -> Self {
        let default: Arc<dyn ShardAllocationStrategy> = Arc::new(WeightedLeastLoad::default());
        Self {
            strategies: BTreeMap::from([(
                (default.policy_id().to_owned(), default.policy_version()),
                default,
            )]),
        }
    }
}

impl fmt::Debug for ShardAllocationStrategies {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ShardAllocationStrategies")
            .field("registered", &self.strategies.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl ShardAllocationStrategies {
    pub fn register(
        &mut self,
        strategy: Arc<dyn ShardAllocationStrategy>,
    ) -> Result<(), StrategyRegistrationError> {
        let id = strategy.policy_id();
        let version = strategy.policy_version();
        if id.is_empty() || id.len() > 128 || version == 0 {
            return Err(StrategyRegistrationError::InvalidIdentity);
        }
        let key = (id.to_owned(), version);
        if self.strategies.contains_key(&key) {
            return Err(StrategyRegistrationError::Duplicate);
        }
        self.strategies.insert(key, strategy);
        Ok(())
    }

    pub fn with_strategy(
        mut self,
        strategy: Arc<dyn ShardAllocationStrategy>,
    ) -> Result<Self, StrategyRegistrationError> {
        self.register(strategy)?;
        Ok(self)
    }

    pub fn replace(
        &mut self,
        strategy: Arc<dyn ShardAllocationStrategy>,
    ) -> Result<(), StrategyRegistrationError> {
        let id = strategy.policy_id();
        let version = strategy.policy_version();
        if id.is_empty() || id.len() > 128 || version == 0 {
            return Err(StrategyRegistrationError::InvalidIdentity);
        }
        let key = (id.to_owned(), version);
        if !self.strategies.contains_key(&key) {
            return Err(StrategyRegistrationError::NotRegistered);
        }
        self.strategies.insert(key, strategy);
        Ok(())
    }

    pub(crate) fn into_inner(self) -> BTreeMap<StrategyKey, Arc<dyn ShardAllocationStrategy>> {
        self.strategies
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum StrategyRegistrationError {
    #[error("allocation strategy ID/version is invalid")]
    InvalidIdentity,
    #[error("allocation strategy ID/version is already registered")]
    Duplicate,
    #[error("allocation strategy ID/version is not registered")]
    NotRegistered,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_include_the_stable_weighted_strategy() {
        let mut strategies = ShardAllocationStrategies::default();
        assert!(
            strategies
                .strategies
                .contains_key(&("weighted-least-load".to_owned(), 1))
        );
        let tuned = WeightedLeastLoad {
            cooldown_millis: 5_000,
            ..WeightedLeastLoad::default()
        };
        strategies.replace(Arc::new(tuned)).unwrap();
    }
}
