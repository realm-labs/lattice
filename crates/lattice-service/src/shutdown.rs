//! Application integration for whole-cluster stopping, distinct from node leave.
use async_trait::async_trait;

/// Quiesces application-owned ingress and detached work. Implementations must
/// be idempotent: a blocked/timed-out stop can be retried after leader recovery.
/// Framework-managed Actors are drained separately; no force-stop is implied.
#[async_trait]
pub trait ClusterStopHook: Send + Sync {
    async fn stop(&self) -> Result<(), String>;
}
