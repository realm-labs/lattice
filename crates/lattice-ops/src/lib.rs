use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use lattice_core::{ActorKind, InstanceId, ServiceKind};
use lattice_placement::{ActorPlacementRecord, InstanceRecord, PlacementError, PlacementStore};
use tokio::sync::{Mutex, watch};

#[derive(Debug, Clone)]
pub struct ServiceScheduler {
    inner: Arc<ServiceSchedulerInner>,
}

impl ServiceScheduler {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(ServiceSchedulerInner {
                stopped: Arc::new(AtomicBool::new(false)),
                tasks: Mutex::new(Vec::new()),
            }),
        }
    }

    pub async fn interval<F, Fut>(&self, every: Duration, mut job: F) -> ServiceTaskHandle
    where
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled = cancelled.clone();
        let stopped = self.inner.stopped.clone();
        let join = tokio::spawn(async move {
            let mut interval = tokio::time::interval(every);
            loop {
                interval.tick().await;
                if stopped.load(Ordering::SeqCst) || task_cancelled.load(Ordering::SeqCst) {
                    break;
                }
                job().await;
            }
        });
        self.inner.tasks.lock().await.push(join.abort_handle());
        ServiceTaskHandle { cancelled }
    }

    pub async fn after<Fut>(&self, delay: Duration, job: Fut) -> ServiceTaskHandle
    where
        Fut: Future<Output = ()> + Send + 'static,
    {
        let cancelled = Arc::new(AtomicBool::new(false));
        let task_cancelled = cancelled.clone();
        let stopped = self.inner.stopped.clone();
        let join = tokio::spawn(async move {
            tokio::time::sleep(delay).await;
            if !stopped.load(Ordering::SeqCst) && !task_cancelled.load(Ordering::SeqCst) {
                job.await;
            }
        });
        self.inner.tasks.lock().await.push(join.abort_handle());
        ServiceTaskHandle { cancelled }
    }

    pub async fn shutdown(&self) {
        self.inner.stopped.store(true, Ordering::SeqCst);
        for task in self.inner.tasks.lock().await.drain(..) {
            task.abort();
        }
    }
}

impl Default for ServiceScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug)]
struct ServiceSchedulerInner {
    stopped: Arc<AtomicBool>,
    tasks: Mutex<Vec<tokio::task::AbortHandle>>,
}

#[derive(Debug, Clone)]
pub struct ServiceTaskHandle {
    cancelled: Arc<AtomicBool>,
}

impl ServiceTaskHandle {
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
pub trait ConfigStore: Clone + Send + Sync + 'static {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, OpsError>;
    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), OpsError>;
    async fn watch(&self, key: &str) -> Result<ConfigWatch, OpsError>;
}

#[derive(Debug, Clone, Default)]
pub struct LocalConfigStore {
    values: Arc<Mutex<HashMap<String, serde_json::Value>>>,
    watches: Arc<Mutex<HashMap<String, watch::Sender<Option<serde_json::Value>>>>>,
}

#[async_trait]
impl ConfigStore for LocalConfigStore {
    async fn get(&self, key: &str) -> Result<Option<serde_json::Value>, OpsError> {
        Ok(self.values.lock().await.get(key).cloned())
    }

    async fn put(&self, key: String, value: serde_json::Value) -> Result<(), OpsError> {
        self.values.lock().await.insert(key.clone(), value.clone());
        let mut watches = self.watches.lock().await;
        let tx = watches.entry(key).or_insert_with(|| {
            let (tx, _rx) = watch::channel(None);
            tx
        });
        tx.send_replace(Some(value));
        Ok(())
    }

    async fn watch(&self, key: &str) -> Result<ConfigWatch, OpsError> {
        let current = self.values.lock().await.get(key).cloned();
        let mut watches = self.watches.lock().await;
        let rx = watches
            .entry(key.to_string())
            .or_insert_with(|| {
                let (tx, _rx) = watch::channel(current.clone());
                tx
            })
            .subscribe();
        Ok(ConfigWatch { rx })
    }
}

pub struct ConfigWatch {
    rx: watch::Receiver<Option<serde_json::Value>>,
}

impl ConfigWatch {
    pub async fn changed(&mut self) -> Result<Option<serde_json::Value>, OpsError> {
        self.rx
            .changed()
            .await
            .map_err(|_| OpsError::ConfigWatchClosed)?;
        Ok(self.rx.borrow().clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSummary {
    pub instance_count: usize,
    pub actor_owner_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NodeSummary {
    pub instance_id: InstanceId,
    pub service_kind: ServiceKind,
    pub actor_kinds: Vec<ActorKind>,
}

#[derive(Debug, Clone)]
pub struct ClusterInspector<S> {
    store: S,
}

impl<S> ClusterInspector<S>
where
    S: PlacementStore,
{
    pub fn new(store: S) -> Self {
        Self { store }
    }

    pub async fn summarize(
        &self,
        service_kind: &ServiceKind,
        actors: &[ActorPlacementRecord],
    ) -> Result<ClusterSummary, OpsError> {
        let instances = self.store.list_instances(service_kind).await?;
        Ok(ClusterSummary {
            instance_count: instances.len(),
            actor_owner_count: actors.len(),
        })
    }

    pub fn summarize_node(
        &self,
        instance: &InstanceRecord,
        actors: &[ActorPlacementRecord],
    ) -> NodeSummary {
        let mut actor_kinds = actors
            .iter()
            .filter(|record| record.owner == instance.instance_id)
            .map(|record| record.actor_kind.clone())
            .collect::<Vec<_>>();
        actor_kinds.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        actor_kinds.dedup();
        NodeSummary {
            instance_id: instance.instance_id.clone(),
            service_kind: instance.service_kind.clone(),
            actor_kinds,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum OpsError {
    #[error("config watch closed")]
    ConfigWatchClosed,
    #[error("placement failed: {0}")]
    Placement(#[from] PlacementError),
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;

    use lattice_core::{ActorId, Epoch, InstanceCapacity, actor_kind, service_kind};
    use lattice_placement::{
        ActorPlacementRecord, InMemoryPlacementStore, InstanceState, LeaseId, PlacementPrefix,
        PlacementState, PlacementStore,
    };
    use serde_json::json;

    use super::*;

    #[tokio::test]
    async fn service_scheduler_cancels_interval_on_shutdown() {
        let scheduler = ServiceScheduler::new();
        let ticks = Arc::new(AtomicUsize::new(0));
        let ticks_clone = ticks.clone();
        scheduler
            .interval(Duration::from_millis(5), move || {
                let ticks = ticks_clone.clone();
                async move {
                    ticks.fetch_add(1, Ordering::SeqCst);
                }
            })
            .await;

        tokio::time::sleep(Duration::from_millis(20)).await;
        scheduler.shutdown().await;
        let after_shutdown = ticks.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(after_shutdown > 0);
        assert_eq!(ticks.load(Ordering::SeqCst), after_shutdown);
    }

    #[tokio::test]
    async fn local_config_store_supports_watch_reload() {
        let store = LocalConfigStore::default();
        let mut watch = store.watch("world.tick_ms").await.unwrap();

        store
            .put("world.tick_ms".to_string(), json!(50))
            .await
            .unwrap();
        let value = watch.changed().await.unwrap();

        assert_eq!(value, Some(json!(50)));
        assert_eq!(store.get("world.tick_ms").await.unwrap(), Some(json!(50)));
    }

    #[tokio::test]
    async fn cluster_inspector_summarizes_instances_and_actor_owners() {
        let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
        let instance = instance_record("world-a");
        store.upsert_instance(instance.clone()).await.unwrap();
        let actors = vec![ActorPlacementRecord {
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(7),
            owner: InstanceId::new("world-a"),
            epoch: Epoch(1),
            lease_id: LeaseId(1),
            state: PlacementState::Running,
        }];
        let inspector = ClusterInspector::new(store);

        let cluster = inspector
            .summarize(&service_kind!("World"), &actors)
            .await
            .unwrap();
        let node = inspector.summarize_node(&instance, &actors);

        assert_eq!(
            cluster,
            ClusterSummary {
                instance_count: 1,
                actor_owner_count: 1
            }
        );
        assert_eq!(node.actor_kinds, vec![actor_kind!("World")]);
    }

    fn instance_record(instance_id: &str) -> InstanceRecord {
        InstanceRecord {
            service_kind: service_kind!("World"),
            instance_id: InstanceId::new(instance_id),
            advertised_endpoint: format!("http://{instance_id}.world:18080").parse().unwrap(),
            control_endpoint: format!("http://{instance_id}.world:18081").parse().unwrap(),
            version: "test".to_string(),
            state: InstanceState::Ready,
            capacity: InstanceCapacity::default(),
            labels: BTreeMap::new(),
        }
    }
}
