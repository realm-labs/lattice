//! Immutable dependencies shared by Actors in one runtime.

use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ActorEnvironmentError {
    #[error("value {type_name} is already registered")]
    DuplicateValue { type_name: &'static str },
}

/// Type-indexed environment shared by every Actor in one runtime.
///
/// An environment is configured on [`crate::runtime::ActorRuntimeConfig`] and cloned
/// into every Actor spawned by that runtime. Values are immutable and shared by
/// `Arc`; activation-specific integration metadata belongs in
/// [`crate::attachments::ActorRuntimeAttachments`], while mutable business state
/// belongs on the Actor itself.
#[derive(Clone)]
pub struct ActorEnvironment {
    inner: Arc<ActorEnvironmentInner>,
}

struct ActorEnvironmentInner {
    values: HashMap<TypeId, StoredValue>,
}

impl fmt::Debug for ActorEnvironmentInner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorEnvironmentInner")
            .field("value_count", &self.values.len())
            .finish()
    }
}

impl fmt::Debug for ActorEnvironment {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorEnvironment")
            .field("value_count", &self.len())
            .finish()
    }
}

#[derive(Clone)]
struct StoredValue {
    type_name: &'static str,
    value: Arc<dyn Any + Send + Sync>,
}

impl fmt::Debug for StoredValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredValue")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

impl ActorEnvironment {
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(ActorEnvironmentInner {
                values: HashMap::new(),
            }),
        }
    }

    pub fn builder() -> ActorEnvironmentBuilder {
        ActorEnvironmentBuilder {
            values: HashMap::new(),
        }
    }

    pub fn get<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.inner
            .values
            .get(&TypeId::of::<T>())
            .and_then(|value| value.value.clone().downcast::<T>().ok())
    }

    /// Returns a new environment with one additional immutable value.
    ///
    /// The original environment is unchanged. This is primarily useful when a runtime integration
    /// adds one of its own capabilities while preserving the application-provided environment.
    pub fn with<T>(&self, value: T) -> Result<Self, ActorEnvironmentError>
    where
        T: Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        if self.inner.values.contains_key(&type_id) {
            return Err(ActorEnvironmentError::DuplicateValue {
                type_name: type_name::<T>(),
            });
        }
        let mut values = self.inner.values.clone();
        values.insert(
            type_id,
            StoredValue {
                type_name: type_name::<T>(),
                value: Arc::new(value),
            },
        );
        Ok(Self {
            inner: Arc::new(ActorEnvironmentInner { values }),
        })
    }

    pub fn len(&self) -> usize {
        self.inner.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.values.is_empty()
    }
}

impl Default for ActorEnvironment {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug)]
pub struct ActorEnvironmentBuilder {
    values: HashMap<TypeId, StoredValue>,
}

impl ActorEnvironmentBuilder {
    pub fn get<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.values
            .get(&TypeId::of::<T>())
            .and_then(|value| value.value.clone().downcast::<T>().ok())
    }

    pub fn insert<T>(&mut self, value: T) -> Result<(), ActorEnvironmentError>
    where
        T: Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        if self.values.contains_key(&type_id) {
            return Err(ActorEnvironmentError::DuplicateValue {
                type_name: type_name::<T>(),
            });
        }
        self.values.insert(
            type_id,
            StoredValue {
                type_name: type_name::<T>(),
                value: Arc::new(value),
            },
        );
        Ok(())
    }

    pub fn build(self) -> ActorEnvironment {
        ActorEnvironment {
            inner: Arc::new(ActorEnvironmentInner {
                values: self.values,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adding_a_value_preserves_the_original_environment() {
        let original = ActorEnvironment::empty();
        let extended = original.with(42_u64).unwrap();

        assert!(original.get::<u64>().is_none());
        assert_eq!(*extended.get::<u64>().unwrap(), 42);
        assert!(extended.with(7_u64).is_err());
    }
}
