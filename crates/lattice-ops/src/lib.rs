use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use axum::Json;
use axum::Router;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use lattice_core::{ActorKind, InstanceId, ServiceKind};
use lattice_placement::{ActorPlacementRecord, InstanceRecord, PlacementError, PlacementStore};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, watch};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct OperationId(String);

impl OperationId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum OperationStatus {
    Pending,
    Retrying { attempts: u32 },
    Completed,
    CompensationRequired { reason: String },
    ManualRequired { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PendingOperation {
    pub operation_id: OperationId,
    pub status: OperationStatus,
}

#[derive(Debug, Default, Clone)]
pub struct OperationTracker {
    operations: Arc<Mutex<HashMap<OperationId, PendingOperation>>>,
}

impl OperationTracker {
    pub async fn start(&self, operation_id: OperationId) -> Result<(), OpsError> {
        let mut operations = self.operations.lock().await;
        if operations.contains_key(&operation_id) {
            return Err(OpsError::DuplicateOperation {
                operation_id: operation_id.as_str().to_string(),
            });
        }
        operations.insert(
            operation_id.clone(),
            PendingOperation {
                operation_id,
                status: OperationStatus::Pending,
            },
        );
        Ok(())
    }

    pub async fn mark_retrying(
        &self,
        operation_id: &OperationId,
        attempts: u32,
    ) -> Result<(), OpsError> {
        self.update(operation_id, OperationStatus::Retrying { attempts })
            .await
    }

    pub async fn mark_compensation_required(
        &self,
        operation_id: &OperationId,
        reason: impl Into<String>,
    ) -> Result<(), OpsError> {
        self.update(
            operation_id,
            OperationStatus::CompensationRequired {
                reason: reason.into(),
            },
        )
        .await
    }

    pub async fn mark_manual_required(
        &self,
        operation_id: &OperationId,
        reason: impl Into<String>,
    ) -> Result<(), OpsError> {
        self.update(
            operation_id,
            OperationStatus::ManualRequired {
                reason: reason.into(),
            },
        )
        .await
    }

    pub async fn complete(&self, operation_id: &OperationId) -> Result<(), OpsError> {
        self.update(operation_id, OperationStatus::Completed).await
    }

    pub async fn get(&self, operation_id: &OperationId) -> Option<PendingOperation> {
        self.operations.lock().await.get(operation_id).cloned()
    }

    async fn update(
        &self,
        operation_id: &OperationId,
        status: OperationStatus,
    ) -> Result<(), OpsError> {
        let mut operations = self.operations.lock().await;
        let operation =
            operations
                .get_mut(operation_id)
                .ok_or_else(|| OpsError::UnknownOperation {
                    operation_id: operation_id.as_str().to_string(),
                })?;
        operation.status = status;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
pub struct OutboxEventId(String);

impl OutboxEventId {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct OutboxEvent {
    pub event_id: OutboxEventId,
    pub topic: String,
    pub payload: serde_json::Value,
    pub published: bool,
}

#[derive(Debug, Default, Clone)]
pub struct TransactionalOutbox {
    events: Arc<Mutex<HashMap<OutboxEventId, OutboxEvent>>>,
}

impl TransactionalOutbox {
    pub async fn enqueue(&self, event: OutboxEvent) -> Result<(), OpsError> {
        let mut events = self.events.lock().await;
        if events.contains_key(&event.event_id) {
            return Err(OpsError::DuplicateOutboxEvent);
        }
        events.insert(event.event_id.clone(), event);
        Ok(())
    }

    pub async fn unpublished(&self) -> Vec<OutboxEvent> {
        self.events
            .lock()
            .await
            .values()
            .filter(|event| !event.published)
            .cloned()
            .collect()
    }

    pub async fn mark_published(&self, event_id: &OutboxEventId) -> Result<(), OpsError> {
        let mut events = self.events.lock().await;
        let event = events
            .get_mut(event_id)
            .ok_or(OpsError::UnknownOutboxEvent)?;
        event.published = true;
        Ok(())
    }
}

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ClusterSummary {
    pub instance_count: usize,
    pub actor_owner_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NodeSummary {
    pub instance_id: InstanceId,
    pub service_kind: ServiceKind,
    pub actor_kinds: Vec<ActorKind>,
}

#[derive(Debug, Clone)]
pub struct AdminAuth {
    token: Option<String>,
}

impl AdminAuth {
    pub fn disabled() -> Self {
        Self { token: None }
    }

    pub fn bearer_token(token: impl Into<String>) -> Self {
        Self {
            token: Some(token.into()),
        }
    }

    pub fn authorize(&self, headers: &HeaderMap) -> Result<(), AdminApiError> {
        let Some(expected) = &self.token else {
            return Ok(());
        };
        let actual = headers
            .get("x-lattice-admin-token")
            .and_then(|value| value.to_str().ok());
        if actual == Some(expected.as_str()) {
            Ok(())
        } else {
            Err(AdminApiError::Unauthorized)
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct PageRequest {
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_page_limit")]
    pub limit: usize,
}

fn default_page_limit() -> usize {
    100
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub offset: usize,
    pub limit: usize,
    pub total: usize,
    pub partial: bool,
}

pub fn paginate<T: Clone>(items: &[T], request: PageRequest) -> Page<T> {
    let limit = request.limit.clamp(1, 500);
    let offset = request.offset.min(items.len());
    let end = (offset + limit).min(items.len());
    Page {
        items: items[offset..end].to_vec(),
        offset,
        limit,
        total: items.len(),
        partial: end < items.len(),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct InstanceView {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub state: String,
    pub advertised_endpoint: String,
    pub control_endpoint: String,
    pub version: String,
}

impl From<InstanceRecord> for InstanceView {
    fn from(record: InstanceRecord) -> Self {
        Self {
            service_kind: record.service_kind,
            instance_id: record.instance_id,
            state: format!("{:?}", record.state),
            advertised_endpoint: record.advertised_endpoint.to_string(),
            control_endpoint: record.control_endpoint.to_string(),
            version: record.version,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AdminSnapshot {
    pub summary: ClusterSummary,
    pub instances: Vec<InstanceView>,
}

#[derive(Debug, Clone)]
pub struct AdminHttpState {
    auth: AdminAuth,
    snapshot: AdminSnapshot,
}

#[derive(Debug, Clone)]
pub struct AdminHttpAdapter {
    state: AdminHttpState,
}

impl AdminHttpAdapter {
    pub fn new(auth: AdminAuth, snapshot: AdminSnapshot) -> Self {
        Self {
            state: AdminHttpState { auth, snapshot },
        }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/admin/cluster/summary", get(admin_cluster_summary))
            .route("/admin/instances", get(admin_instances))
            .with_state(self.state)
    }
}

async fn admin_cluster_summary(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
) -> Result<Json<ClusterSummary>, AdminApiError> {
    state.auth.authorize(&headers)?;
    Ok(Json(state.snapshot.summary))
}

async fn admin_instances(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Query(page): Query<PageRequest>,
) -> Result<Json<Page<InstanceView>>, AdminApiError> {
    state.auth.authorize(&headers)?;
    Ok(Json(paginate(&state.snapshot.instances, page)))
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AdminApiError {
    #[error("admin request is unauthorized")]
    Unauthorized,
}

impl axum::response::IntoResponse for AdminApiError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AdminApiError::Unauthorized => {
                (StatusCode::UNAUTHORIZED, self.to_string()).into_response()
            }
        }
    }
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
    #[error("duplicate operation {operation_id}")]
    DuplicateOperation { operation_id: String },
    #[error("unknown operation {operation_id}")]
    UnknownOperation { operation_id: String },
    #[error("duplicate outbox event")]
    DuplicateOutboxEvent,
    #[error("unknown outbox event")]
    UnknownOutboxEvent,
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

    #[test]
    fn admin_auth_requires_configured_token() {
        let auth = AdminAuth::bearer_token("secret");
        let mut headers = HeaderMap::new();

        assert_eq!(auth.authorize(&headers), Err(AdminApiError::Unauthorized));
        headers.insert("x-lattice-admin-token", "secret".parse().unwrap());

        assert_eq!(auth.authorize(&headers), Ok(()));
    }

    #[test]
    fn admin_pagination_reports_partial_results() {
        let page = paginate(
            &[1, 2, 3, 4],
            PageRequest {
                offset: 1,
                limit: 2,
            },
        );

        assert_eq!(page.items, vec![2, 3]);
        assert_eq!(page.total, 4);
        assert!(page.partial);
    }

    #[test]
    fn admin_http_adapter_builds_axum_router() {
        let snapshot = AdminSnapshot {
            summary: ClusterSummary {
                instance_count: 1,
                actor_owner_count: 0,
            },
            instances: vec![InstanceView::from(instance_record("world-a"))],
        };

        let _router = AdminHttpAdapter::new(AdminAuth::disabled(), snapshot).router();
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

    #[tokio::test]
    async fn operation_tracker_models_retry_compensation_and_manual_repair() {
        let tracker = OperationTracker::default();
        let operation_id = OperationId::new("trade-1");

        tracker.start(operation_id.clone()).await.unwrap();
        tracker.mark_retrying(&operation_id, 1).await.unwrap();
        assert_eq!(
            tracker.get(&operation_id).await.unwrap().status,
            OperationStatus::Retrying { attempts: 1 }
        );

        tracker
            .mark_compensation_required(&operation_id, "debit applied but credit unknown")
            .await
            .unwrap();
        assert!(matches!(
            tracker.get(&operation_id).await.unwrap().status,
            OperationStatus::CompensationRequired { .. }
        ));

        tracker
            .mark_manual_required(&operation_id, "operator review")
            .await
            .unwrap();
        assert!(matches!(
            tracker.get(&operation_id).await.unwrap().status,
            OperationStatus::ManualRequired { .. }
        ));
    }

    #[tokio::test]
    async fn transactional_outbox_tracks_unpublished_events_idempotently() {
        let outbox = TransactionalOutbox::default();
        let event_id = OutboxEventId::new("event-1");
        let event = OutboxEvent {
            event_id: event_id.clone(),
            topic: "game.world.player_entered".to_string(),
            payload: json!({ "world_id": 1, "player_id": 1001 }),
            published: false,
        };

        outbox.enqueue(event.clone()).await.unwrap();
        let duplicate = outbox.enqueue(event).await;
        assert!(matches!(duplicate, Err(OpsError::DuplicateOutboxEvent)));
        assert_eq!(outbox.unpublished().await.len(), 1);

        outbox.mark_published(&event_id).await.unwrap();
        assert!(outbox.unpublished().await.is_empty());
    }
}
