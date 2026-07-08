use std::sync::Arc;

use lattice_core::service_context::ServiceContext;
use lattice_eventbus::publisher::ServiceEvents;
use lattice_ops::scheduler::ServiceScheduler;

use crate::framework::config_store::{ConfigStoreComponent, DynConfigStore};
use crate::framework::event_bus::{
    ClusterEventBusComponent, LocalEventBusComponent, ServiceEventBus,
};
use crate::framework::placement::{DynPlacementStore, PlacementStoreComponent};
use crate::framework::scheduler::ServiceSchedulerComponent;

pub trait ServiceContextExt {
    fn placement_store(&self) -> Arc<dyn DynPlacementStore>;
    fn cluster_event_bus(&self) -> ServiceEventBus;
    fn local_event_bus(&self) -> ServiceEventBus;
    fn cluster_events(&self) -> ServiceEvents<ServiceEventBus>;
    fn local_events(&self) -> ServiceEvents<ServiceEventBus>;
    fn scheduler(&self) -> ServiceScheduler;
    fn config_store(&self) -> Arc<dyn DynConfigStore>;
}

impl ServiceContextExt for ServiceContext {
    fn placement_store(&self) -> Arc<dyn DynPlacementStore> {
        self.extension::<PlacementStoreComponent>()
            .map(|component| component.inner())
            .expect("placement_store should be registered in ServiceContext")
    }

    fn cluster_event_bus(&self) -> ServiceEventBus {
        self.extension::<ClusterEventBusComponent>()
            .map(|component| component.bus())
            .expect("cluster_event_bus should be registered in ServiceContext")
    }

    fn local_event_bus(&self) -> ServiceEventBus {
        self.extension::<LocalEventBusComponent>()
            .map(|component| component.bus())
            .or_else(|| {
                self.extension::<ClusterEventBusComponent>()
                    .map(|component| component.bus())
            })
            .expect("local_event_bus should be registered in ServiceContext")
    }

    fn cluster_events(&self) -> ServiceEvents<ServiceEventBus> {
        ServiceEvents::new(self.cluster_event_bus())
    }

    fn local_events(&self) -> ServiceEvents<ServiceEventBus> {
        ServiceEvents::new(self.local_event_bus())
    }

    fn scheduler(&self) -> ServiceScheduler {
        self.extension::<ServiceSchedulerComponent>()
            .map(|component| component.scheduler())
            .expect("service scheduler should be registered in ServiceContext")
    }

    fn config_store(&self) -> Arc<dyn DynConfigStore> {
        self.extension::<ConfigStoreComponent>()
            .map(|component| component.inner())
            .expect("config_store should be registered in ServiceContext")
    }
}
