use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_eventbus::local::EventSubscriptionHandle;
use lattice_placement::authority::PlacementAuthority;
use lattice_placement::coordination::reports::DrainReport;
use lattice_placement::storage::LeaseId;
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
pub struct GracefulShutdown<A, LC> {
    service_kind: ServiceKind,
    instance_id: InstanceId,
    expected_lease_id: LeaseId,
    authority: A,
    lease_controller: LC,
    scheduler: ServiceScheduler,
    subscriptions: Arc<Mutex<Vec<EventSubscriptionHandle>>>,
    ready: Arc<AtomicBool>,
}

impl<A, LC> GracefulShutdown<A, LC> {
    pub fn new(
        service_kind: ServiceKind,
        instance_id: InstanceId,
        expected_lease_id: LeaseId,
        authority: A,
        lease_controller: LC,
        scheduler: ServiceScheduler,
    ) -> Self {
        Self {
            service_kind,
            instance_id,
            expected_lease_id,
            authority,
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

impl<A, LC> GracefulShutdown<A, LC>
where
    A: PlacementAuthority,
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
            .authority
            .drain_instance(
                self.service_kind.clone(),
                self.instance_id.clone(),
                self.expected_lease_id,
            )
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
