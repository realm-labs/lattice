use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use thiserror::Error;

use crate::instance::InstanceId;
use crate::kind::ServiceKind;

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
    service_kind: ServiceKind,
    instance_id: InstanceId,
    extensions: HashMap<TypeId, StoredComponent>,
}

impl fmt::Debug for ServiceContextInner {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceContextInner")
            .field("service_kind", &self.service_kind)
            .field("instance_id", &self.instance_id)
            .field("extension_count", &self.extensions.len())
            .finish()
    }
}

impl fmt::Debug for ServiceContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceContext")
            .field("service_kind", &self.service_kind())
            .field("instance_id", &self.instance_id())
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
        Self::new(ServiceKind::from_static("local"), InstanceId::new("local"))
    }

    pub fn new(service_kind: ServiceKind, instance_id: InstanceId) -> Self {
        Self {
            inner: Arc::new(ServiceContextInner {
                service_kind,
                instance_id,
                extensions: HashMap::new(),
            }),
        }
    }

    pub fn builder(service_kind: ServiceKind, instance_id: InstanceId) -> ServiceContextBuilder {
        ServiceContextBuilder {
            service_kind,
            instance_id,
            extensions: HashMap::new(),
        }
    }

    pub fn service_kind(&self) -> &ServiceKind {
        &self.inner.service_kind
    }

    pub fn instance_id(&self) -> &InstanceId {
        &self.inner.instance_id
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
    service_kind: ServiceKind,
    instance_id: InstanceId,
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
                service_kind: self.service_kind,
                instance_id: self.instance_id,
                extensions: self.extensions,
            }),
        }
    }
}
