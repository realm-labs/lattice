use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_eventbus::local::EventSubscriptionHandle;
use lattice_placement::coordinator::{DrainReport, LogicControl, PlacementCoordinator};
use lattice_placement::store::PlacementStore;
use tokio::sync::Mutex;

use crate::error::OpsError;
use crate::scheduler::ServiceScheduler;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownTrigger {
    Sigterm,
    KubernetesPreStop,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownStage {
    ReadinessFalse,
    LeaseKeptAlive,
    SubscriptionsCancelled,
    Drained,
    SchedulerStopped,
    LeaseReleased,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GracefulShutdownReport {
    pub trigger: ShutdownTrigger,
    pub stages: Vec<ShutdownStage>,
    pub drain: DrainReport,
}

#[async_trait]
pub trait ShutdownLeaseController: Clone + Send + Sync + 'static {
    async fn keep_alive_during_drain(&self, instance_id: &InstanceId) -> Result<(), OpsError>;
    async fn release_after_drain(&self, instance_id: &InstanceId) -> Result<(), OpsError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LeaseEvent {
    KeepAlive(InstanceId),
    Release(InstanceId),
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryShutdownLeaseController {
    events: Arc<Mutex<Vec<LeaseEvent>>>,
}

impl InMemoryShutdownLeaseController {
    pub async fn events(&self) -> Vec<LeaseEvent> {
        self.events.lock().await.clone()
    }
}

#[async_trait]
impl ShutdownLeaseController for InMemoryShutdownLeaseController {
    async fn keep_alive_during_drain(&self, instance_id: &InstanceId) -> Result<(), OpsError> {
        self.events
            .lock()
            .await
            .push(LeaseEvent::KeepAlive(instance_id.clone()));
        Ok(())
    }

    async fn release_after_drain(&self, instance_id: &InstanceId) -> Result<(), OpsError> {
        self.events
            .lock()
            .await
            .push(LeaseEvent::Release(instance_id.clone()));
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct GracefulShutdown<S, L, LC> {
    service_kind: ServiceKind,
    instance_id: InstanceId,
    coordinator: PlacementCoordinator<S, L>,
    lease_controller: LC,
    scheduler: ServiceScheduler,
    subscriptions: Arc<Mutex<Vec<EventSubscriptionHandle>>>,
    ready: Arc<AtomicBool>,
}

impl<S, L, LC> GracefulShutdown<S, L, LC> {
    pub fn new(
        service_kind: ServiceKind,
        instance_id: InstanceId,
        coordinator: PlacementCoordinator<S, L>,
        lease_controller: LC,
        scheduler: ServiceScheduler,
    ) -> Self {
        Self {
            service_kind,
            instance_id,
            coordinator,
            lease_controller,
            scheduler,
            subscriptions: Arc::new(Mutex::new(Vec::new())),
            ready: Arc::new(AtomicBool::new(true)),
        }
    }

    pub async fn own_subscription(&self, handle: EventSubscriptionHandle) {
        self.subscriptions.lock().await.push(handle);
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
}

impl<S, L, LC> GracefulShutdown<S, L, LC>
where
    S: PlacementStore,
    L: LogicControl,
    LC: ShutdownLeaseController,
{
    pub async fn shutdown(
        &self,
        trigger: ShutdownTrigger,
    ) -> Result<GracefulShutdownReport, OpsError> {
        let mut stages = Vec::new();

        self.ready.store(false, Ordering::SeqCst);
        stages.push(ShutdownStage::ReadinessFalse);

        self.lease_controller
            .keep_alive_during_drain(&self.instance_id)
            .await?;
        stages.push(ShutdownStage::LeaseKeptAlive);

        for subscription in self.subscriptions.lock().await.drain(..) {
            subscription.cancel();
        }
        stages.push(ShutdownStage::SubscriptionsCancelled);

        let drain = self
            .coordinator
            .drain_instance(self.service_kind.clone(), self.instance_id.clone())
            .await?;
        stages.push(ShutdownStage::Drained);

        self.scheduler.shutdown().await;
        stages.push(ShutdownStage::SchedulerStopped);

        self.lease_controller
            .release_after_drain(&self.instance_id)
            .await?;
        stages.push(ShutdownStage::LeaseReleased);

        Ok(GracefulShutdownReport {
            trigger,
            stages,
            drain,
        })
    }
}
