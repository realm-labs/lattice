use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use lattice_config::{BootstrapConfig, ConfigError};
use serde::de::DeserializeOwned;
use thiserror::Error;

use crate::{InstanceId, ServiceKind};

type ComponentFuture<T> = Pin<Box<dyn Future<Output = Result<T, ServiceComponentError>> + Send>>;
type ComponentBuildFn<T> = dyn Fn(BootstrapConfig) -> ComponentFuture<T> + Send + Sync;

#[derive(Debug, Error)]
pub enum ServiceComponentError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error("failed to build service component: {message}")]
    Build { message: String },
}

impl ServiceComponentError {
    pub fn build(error: impl fmt::Display) -> Self {
        Self::Build {
            message: error.to_string(),
        }
    }
}

pub struct ConfiguredComponent<T> {
    builder: ConfiguredComponentBuilder<T>,
    not_ready_value: PhantomData<Rc<()>>,
}

pub struct ConfiguredComponentBuilder<T> {
    section: &'static str,
    build: Arc<ComponentBuildFn<T>>,
}

impl<T> Clone for ConfiguredComponent<T> {
    fn clone(&self) -> Self {
        Self {
            builder: self.builder.clone(),
            not_ready_value: PhantomData,
        }
    }
}

impl<T> Clone for ConfiguredComponentBuilder<T> {
    fn clone(&self) -> Self {
        Self {
            section: self.section,
            build: self.build.clone(),
        }
    }
}

impl<T> fmt::Debug for ConfiguredComponent<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfiguredComponent")
            .field("section", &self.builder.section)
            .field("component_type", &type_name::<T>())
            .finish()
    }
}

impl<T> fmt::Debug for ConfiguredComponentBuilder<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ConfiguredComponentBuilder")
            .field("section", &self.section)
            .field("component_type", &type_name::<T>())
            .finish()
    }
}

impl<T> ConfiguredComponent<T>
where
    T: Send + Sync + 'static,
{
    pub fn from_section<C, F, Fut, E>(section: &'static str, build: F) -> Self
    where
        C: DeserializeOwned + Send + 'static,
        F: Fn(C) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<T, E>> + Send + 'static,
        E: fmt::Display + Send + Sync + 'static,
    {
        Self {
            builder: ConfiguredComponentBuilder {
                section,
                build: Arc::new(move |config| {
                    let decoded = config.section::<C>(section);
                    match decoded {
                        Ok(value) => {
                            let future = build(value);
                            Box::pin(
                                async move { future.await.map_err(ServiceComponentError::build) },
                            ) as ComponentFuture<T>
                        }
                        Err(error) => {
                            Box::pin(async move { Err(ServiceComponentError::Config(error)) })
                                as ComponentFuture<T>
                        }
                    }
                }),
            },
            not_ready_value: PhantomData,
        }
    }

    pub fn section(&self) -> &'static str {
        self.builder.section
    }

    pub async fn build(&self, config: &BootstrapConfig) -> Result<T, ServiceComponentError> {
        self.builder.build(config).await
    }

    pub fn into_builder(self) -> ConfiguredComponentBuilder<T> {
        self.builder
    }
}

impl<T> ConfiguredComponentBuilder<T>
where
    T: Send + Sync + 'static,
{
    pub fn section(&self) -> &'static str {
        self.section
    }

    pub async fn build(&self, config: &BootstrapConfig) -> Result<T, ServiceComponentError> {
        (self.build)(config.clone()).await
    }
}

#[derive(Clone)]
pub struct ServiceContext {
    inner: Arc<ServiceContextInner>,
}

struct ServiceContextInner {
    service_kind: ServiceKind,
    instance_id: InstanceId,
    bootstrap_config: BootstrapConfig,
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
        Self::new(
            ServiceKind::from_static("local"),
            InstanceId::new("local"),
            BootstrapConfig::default(),
        )
    }

    pub fn new(
        service_kind: ServiceKind,
        instance_id: InstanceId,
        bootstrap_config: BootstrapConfig,
    ) -> Self {
        Self {
            inner: Arc::new(ServiceContextInner {
                service_kind,
                instance_id,
                bootstrap_config,
                extensions: HashMap::new(),
            }),
        }
    }

    pub fn builder(
        service_kind: ServiceKind,
        instance_id: InstanceId,
        bootstrap_config: BootstrapConfig,
    ) -> ServiceContextBuilder {
        ServiceContextBuilder {
            service_kind,
            instance_id,
            bootstrap_config,
            extensions: HashMap::new(),
        }
    }

    pub fn service_kind(&self) -> &ServiceKind {
        &self.inner.service_kind
    }

    pub fn instance_id(&self) -> &InstanceId {
        &self.inner.instance_id
    }

    pub fn bootstrap_config(&self) -> &BootstrapConfig {
        &self.inner.bootstrap_config
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
    bootstrap_config: BootstrapConfig,
    extensions: HashMap<TypeId, StoredComponent>,
}

impl ServiceContextBuilder {
    pub fn insert_extension<T>(&mut self, extension: T) -> Result<(), &'static str>
    where
        T: Send + Sync + 'static,
    {
        let type_id = TypeId::of::<T>();
        if self.extensions.contains_key(&type_id) {
            return Err(type_name::<T>());
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
                bootstrap_config: self.bootstrap_config,
                extensions: self.extensions,
            }),
        }
    }
}
