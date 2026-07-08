use lattice_placement::routing::placement::{PlacementWatchStarter, PlacementWatchTask};
use tracing::debug;

use crate::error::LatticeServiceError;

#[async_trait::async_trait]
pub(crate) trait ErasedPlacementWatchStarter: Send + Sync {
    fn type_name(&self) -> &'static str;
    async fn start(self: Box<Self>) -> Result<PlacementWatchTask, LatticeServiceError>;
}

pub(crate) struct PlacementWatchRegistration<W> {
    pub(crate) watcher: W,
}

#[async_trait::async_trait]
impl<W> ErasedPlacementWatchStarter for PlacementWatchRegistration<W>
where
    W: PlacementWatchStarter,
{
    fn type_name(&self) -> &'static str {
        std::any::type_name::<W>()
    }

    async fn start(self: Box<Self>) -> Result<PlacementWatchTask, LatticeServiceError> {
        self.watcher
            .start_placement_watch()
            .await
            .map_err(Into::into)
    }
}

pub(crate) async fn start_placement_watchers(
    watchers: Vec<Box<dyn ErasedPlacementWatchStarter>>,
    service_kind: &str,
) -> Result<Vec<PlacementWatchTask>, LatticeServiceError> {
    let mut tasks = Vec::with_capacity(watchers.len());
    for watcher in watchers {
        debug!(
            service.kind = service_kind,
            placement.watch.type = watcher.type_name(),
            "starting placement cache watch"
        );
        tasks.push(watcher.start().await?);
    }
    Ok(tasks)
}
