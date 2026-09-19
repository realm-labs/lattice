use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ServiceContextError {
    #[error("extension {type_name} is already registered")]
    DuplicateExtension { type_name: &'static str },
}

#[derive(Clone)]
pub struct ServiceContext {
    inner: Arc<ServiceContextInner>,
}

struct ServiceContextInner {
    extensions: HashMap<TypeId, StoredComponent>,
}

impl fmt::Debug for ServiceContextInner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceContextInner")
            .field("extension_count", &self.extensions.len())
            .finish()
    }
}

impl fmt::Debug for ServiceContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceContext")
            .field("extension_count", &self.extension_count())
            .finish()
    }
}

#[derive(Clone)]
struct StoredComponent {
    type_name: &'static str,
    value: Arc<dyn Any + Send + Sync>,
}

impl fmt::Debug for StoredComponent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("StoredComponent")
            .field("type_name", &self.type_name)
            .finish_non_exhaustive()
    }
}

impl ServiceContext {
    pub fn empty() -> Self {
        Self {
            inner: Arc::new(ServiceContextInner {
                extensions: HashMap::new(),
            }),
        }
    }

    pub fn builder() -> ServiceContextBuilder {
        ServiceContextBuilder {
            extensions: HashMap::new(),
        }
    }

    pub fn extension<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.inner
            .extensions
            .get(&TypeId::of::<T>())
            .and_then(|extension| extension.value.clone().downcast::<T>().ok())
    }

    /// Returns a new context with one additional immutable extension.
    ///
    /// Existing contexts are unchanged. This is primarily useful when a runtime integration adds
    /// one of its own service-scoped capabilities while preserving the application-provided
    /// service context.
    pub fn with_extension<T>(&self, extension: T) -> Result<Self, ServiceContextError>
    where
        T: Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        if self.inner.extensions.contains_key(&type_id) {
            return Err(ServiceContextError::DuplicateExtension {
                type_name: type_name::<T>(),
            });
        }
        let mut extensions = self.inner.extensions.clone();
        extensions.insert(
            type_id,
            StoredComponent {
                type_name: type_name::<T>(),
                value: Arc::new(extension),
            },
        );
        Ok(Self {
            inner: Arc::new(ServiceContextInner { extensions }),
        })
    }

    pub fn extension_count(&self) -> usize {
        self.inner.extensions.len()
    }
}

impl Default for ServiceContext {
    fn default() -> Self {
        Self::empty()
    }
}

#[derive(Debug)]
pub struct ServiceContextBuilder {
    extensions: HashMap<TypeId, StoredComponent>,
}

impl ServiceContextBuilder {
    pub fn extension<T>(&self) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.extensions
            .get(&TypeId::of::<T>())
            .and_then(|extension| extension.value.clone().downcast::<T>().ok())
    }

    pub fn insert_extension<T>(&mut self, extension: T) -> Result<(), ServiceContextError>
    where
        T: Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        if self.extensions.contains_key(&type_id) {
            return Err(ServiceContextError::DuplicateExtension {
                type_name: type_name::<T>(),
            });
        }
        self.extensions.insert(
            type_id,
            StoredComponent {
                type_name: type_name::<T>(),
                value: Arc::new(extension),
            },
        );
        Ok(())
    }

    pub fn build(self) -> ServiceContext {
        ServiceContext {
            inner: Arc::new(ServiceContextInner {
                extensions: self.extensions,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn adding_an_extension_preserves_the_original_context() {
        let original = ServiceContext::empty();
        let extended = original.with_extension(42_u64).unwrap();

        assert!(original.extension::<u64>().is_none());
        assert_eq!(*extended.extension::<u64>().unwrap(), 42);
        assert!(extended.with_extension(7_u64).is_err());
    }
}
