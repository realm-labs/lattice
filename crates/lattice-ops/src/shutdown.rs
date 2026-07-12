use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::error::OpsError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownTrigger {
    Sigterm,
    KubernetesPreStop,
    Admin,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownStage {
    ReadinessFalse,
    ExternalAdmissionStopped,
    ShardsDrained,
    SingletonsRelocated,
    ActorsStopped,
    AssociationsClosed,
    MembershipReleased,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DrainReport {
    pub shards_moved: usize,
    pub singletons_moved: usize,
    pub blocked_slots: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GracefulShutdownReport {
    pub trigger: ShutdownTrigger,
    pub stages: Vec<ShutdownStage>,
    pub drain: DrainReport,
}

#[async_trait]
pub trait DrainController: Send + Sync + 'static {
    async fn stop_external_admission(&self) -> Result<(), OpsError>;
    async fn drain_shards(&self) -> Result<DrainReport, OpsError>;
    async fn relocate_singletons(&self) -> Result<usize, OpsError>;
    async fn stop_actors(&self) -> Result<(), OpsError>;
    async fn close_associations(&self) -> Result<(), OpsError>;
    async fn release_membership(&self) -> Result<(), OpsError>;
}

pub struct GracefulShutdown {
    controller: Arc<dyn DrainController>,
    ready: AtomicBool,
    gate: Mutex<()>,
    timeout: Duration,
}

impl GracefulShutdown {
    pub fn new(controller: Arc<dyn DrainController>, timeout: Duration) -> Result<Self, OpsError> {
        if timeout.is_zero() {
            return Err(OpsError::Drain {
                message: "shutdown timeout must be nonzero".to_owned(),
            });
        }
        Ok(Self {
            controller,
            ready: AtomicBool::new(true),
            gate: Mutex::new(()),
            timeout,
        })
    }

    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    pub async fn shutdown(
        &self,
        trigger: ShutdownTrigger,
    ) -> Result<GracefulShutdownReport, OpsError> {
        let _guard = self.gate.lock().await;
        tokio::time::timeout(self.timeout, self.shutdown_inner(trigger))
            .await
            .map_err(|_| OpsError::Drain {
                message: "shutdown deadline exceeded".to_owned(),
            })?
    }

    async fn shutdown_inner(
        &self,
        trigger: ShutdownTrigger,
    ) -> Result<GracefulShutdownReport, OpsError> {
        self.ready.store(false, Ordering::Release);
        let mut stages = vec![ShutdownStage::ReadinessFalse];
        self.controller.stop_external_admission().await?;
        stages.push(ShutdownStage::ExternalAdmissionStopped);
        let mut drain = self.controller.drain_shards().await?;
        stages.push(ShutdownStage::ShardsDrained);
        drain.singletons_moved = self.controller.relocate_singletons().await?;
        stages.push(ShutdownStage::SingletonsRelocated);
        self.controller.stop_actors().await?;
        stages.push(ShutdownStage::ActorsStopped);
        self.controller.close_associations().await?;
        stages.push(ShutdownStage::AssociationsClosed);
        self.controller.release_membership().await?;
        stages.push(ShutdownStage::MembershipReleased);
        Ok(GracefulShutdownReport {
            trigger,
            stages,
            drain,
        })
    }
}
