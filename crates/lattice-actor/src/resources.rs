//! Immutable capabilities installed for one Actor activation.

use std::{
    any::{Any, TypeId, type_name},
    collections::HashMap,
    fmt,
    sync::Arc,
};

use thiserror::Error;

/// Immutable, type-indexed capabilities supplied by a runtime integration.
///
/// The local Actor runtime does not interpret these values. Integrations can
/// use them to expose addressability, persistence, or other capabilities
/// without adding integration-specific fields to [`crate::context::ActorContext`].
#[derive(Clone, Default)]
pub struct ActorResources {
    values: Arc<HashMap<TypeId, StoredResource>>,
}

impl ActorResources {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn builder() -> ActorResourcesBuilder {
        ActorResourcesBuilder::default()
    }

    pub fn get<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|resource| resource.value.clone().downcast::<T>().ok())
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

impl fmt::Debug for ActorResources {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorResources")
            .field("count", &self.values.len())
            .finish()
    }
}

#[derive(Default)]
pub struct ActorResourcesBuilder {
    values: HashMap<TypeId, StoredResource>,
}

impl ActorResourcesBuilder {
    pub fn insert<T>(&mut self, value: T) -> Result<&mut Self, ActorResourceError>
    where
        T: Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        if self.values.contains_key(&type_id) {
            return Err(ActorResourceError::Duplicate {
                type_name: type_name::<T>(),
            });
        }
        self.values.insert(
            type_id,
            StoredResource {
                value: Arc::new(value),
            },
        );
        Ok(self)
    }

    pub fn build(self) -> ActorResources {
        ActorResources {
            values: Arc::new(self.values),
        }
    }
}

struct StoredResource {
    value: Arc<dyn Any + Send + Sync>,
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ActorResourceError {
    #[error("Actor resource {type_name} is already installed")]
    Duplicate { type_name: &'static str },
}
