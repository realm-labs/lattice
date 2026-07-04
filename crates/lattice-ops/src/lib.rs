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
use lattice_core::{ActorKind, InstanceId, ServiceKind, TraceContext};
use lattice_placement::{ActorPlacementRecord, InstanceRecord, PlacementError, PlacementStore};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

mod config_store;
mod shutdown;

pub use config_store::{
    ConfigStore, ConfigWatch, EtcdConfigStore, EtcdConfigStoreConfig, InMemoryEtcdConfigClient,
    LocalConfigStore,
};
pub use shutdown::{
    GracefulShutdown, GracefulShutdownReport, InMemoryShutdownLeaseController, LeaseEvent,
    ShutdownLeaseController, ShutdownStage, ShutdownTrigger,
};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum TraceSpanKind {
    Rpc,
    EventBus,
    Scheduler,
    ActorHandler,
    Admin,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TraceSpan {
    pub name: String,
    pub kind: TraceSpanKind,
    pub context: TraceContext,
    pub links: Vec<TraceContext>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct MetricSample {
    pub name: String,
    pub value: u64,
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TelemetryResource {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub service_version: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TelemetryBatch {
    pub resource: TelemetryResource,
    pub spans: Vec<TraceSpan>,
    pub metrics: Vec<MetricSample>,
}

#[async_trait]
pub trait TelemetryExporter: Clone + Send + Sync + 'static {
    async fn export(&self, batch: TelemetryBatch) -> Result<(), OpsError>;
}

#[derive(Debug, Clone)]
pub struct OpenTelemetryPipeline<E> {
    resource: TelemetryResource,
    exporter: E,
}

impl<E> OpenTelemetryPipeline<E>
where
    E: TelemetryExporter,
{
    pub fn new(resource: TelemetryResource, exporter: E) -> Self {
        Self { resource, exporter }
    }

    pub async fn export_from(&self, recorder: &TelemetryRecorder) -> Result<(), OpsError> {
        self.exporter
            .export(TelemetryBatch {
                resource: self.resource.clone(),
                spans: recorder.spans().await,
                metrics: recorder.metrics().await,
            })
            .await
    }
}

#[derive(Debug, Default, Clone)]
pub struct InMemoryTelemetryExporter {
    batches: Arc<Mutex<Vec<TelemetryBatch>>>,
}

impl InMemoryTelemetryExporter {
    pub async fn batches(&self) -> Vec<TelemetryBatch> {
        self.batches.lock().await.clone()
    }
}

#[async_trait]
impl TelemetryExporter for InMemoryTelemetryExporter {
    async fn export(&self, batch: TelemetryBatch) -> Result<(), OpsError> {
        self.batches.lock().await.push(batch);
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub struct TelemetryRecorder {
    spans: Arc<Mutex<Vec<TraceSpan>>>,
    metrics: Arc<Mutex<Vec<MetricSample>>>,
}

impl TelemetryRecorder {
    pub async fn record_span(&self, span: TraceSpan) {
        self.spans.lock().await.push(span);
    }

    pub async fn record_metric(&self, sample: MetricSample) -> Result<(), OpsError> {
        validate_metric_labels(&sample.labels)?;
        self.metrics.lock().await.push(sample);
        Ok(())
    }

    pub async fn spans(&self) -> Vec<TraceSpan> {
        self.spans.lock().await.clone()
    }

    pub async fn metrics(&self) -> Vec<MetricSample> {
        self.metrics.lock().await.clone()
    }
}

fn validate_metric_labels(labels: &HashMap<String, String>) -> Result<(), OpsError> {
    const DISALLOWED: &[&str] = &["actor_id", "request_id", "event_id", "session_id"];
    for label in DISALLOWED {
        if labels.contains_key(*label) {
            return Err(OpsError::HighCardinalityMetricLabel {
                label: (*label).to_string(),
            });
        }
    }
    Ok(())
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
    #[error("metric label {label} is too high-cardinality")]
    HighCardinalityMetricLabel { label: String },
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::AtomicUsize;

    use lattice_core::{ActorId, Epoch, InstanceCapacity, actor_kind, service_kind};
    use lattice_eventbus::{
        EventBus, EventEnvelope, EventId, EventSubscription, LocalEventBus, Subject, SubjectFilter,
    };
    use lattice_placement::{
        ActorPlacementKey, ActorPlacementRecord, InMemoryPlacementStore, InstanceState, LeaseId,
        NoopLogicControl, PlacementCoordinator, PlacementPrefix, PlacementState, PlacementStore,
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
    async fn graceful_shutdown_drains_before_releasing_lease_and_cancels_runtime_work() {
        let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
        store
            .upsert_instance(instance_record("world-a"))
            .await
            .unwrap();
        store
            .upsert_instance(instance_record("world-b"))
            .await
            .unwrap();
        let actor_key = ActorPlacementKey {
            actor_kind: actor_kind!("World"),
            actor_id: ActorId::U64(7),
        };
        store
            .compare_and_put_actor(
                actor_key.clone(),
                None,
                ActorPlacementRecord {
                    actor_kind: actor_kind!("World"),
                    actor_id: ActorId::U64(7),
                    owner: InstanceId::new("world-a"),
                    epoch: Epoch(1),
                    lease_id: LeaseId(1),
                    state: PlacementState::Running,
                },
            )
            .await
            .unwrap();
        let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
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
        let bus = LocalEventBus::new();
        let deliveries = Arc::new(AtomicUsize::new(0));
        let deliveries_clone = deliveries.clone();
        let subscription = bus
            .subscribe(
                EventSubscription::local(SubjectFilter::new("system.shutdown.*")),
                move |_event| {
                    let deliveries = deliveries_clone.clone();
                    async move {
                        deliveries.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                },
            )
            .await
            .unwrap();
        let lease_controller = InMemoryShutdownLeaseController::default();
        let shutdown = GracefulShutdown::new(
            service_kind!("World"),
            InstanceId::new("world-a"),
            coordinator,
            lease_controller.clone(),
            scheduler,
        );
        shutdown.own_subscription(subscription).await;

        let report = shutdown
            .shutdown(ShutdownTrigger::KubernetesPreStop)
            .await
            .unwrap();
        let migrated = store.get_actor(&actor_key).await.unwrap().unwrap().1;
        let drained = store
            .get_instance(&InstanceId::new("world-a"))
            .await
            .unwrap()
            .unwrap();
        let ticks_after_shutdown = ticks.load(Ordering::SeqCst);
        bus.publish(test_event("system.shutdown.done"))
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        assert!(!shutdown.is_ready());
        assert_eq!(
            report.stages,
            vec![
                ShutdownStage::ReadinessFalse,
                ShutdownStage::LeaseKeptAlive,
                ShutdownStage::SubscriptionsCancelled,
                ShutdownStage::Drained,
                ShutdownStage::SchedulerStopped,
                ShutdownStage::LeaseReleased,
            ]
        );
        assert_eq!(report.drain.migrated_actors, 1);
        assert_eq!(migrated.owner, InstanceId::new("world-b"));
        assert_eq!(drained.state, InstanceState::Draining);
        assert_eq!(deliveries.load(Ordering::SeqCst), 0);
        assert_eq!(ticks.load(Ordering::SeqCst), ticks_after_shutdown);
        assert_eq!(
            lease_controller.events().await,
            vec![
                LeaseEvent::KeepAlive(InstanceId::new("world-a")),
                LeaseEvent::Release(InstanceId::new("world-a")),
            ]
        );
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
    async fn etcd_config_store_supports_watch_reload() {
        let store = EtcdConfigStore::new(InMemoryEtcdConfigClient::new(), "/lattice/test/config");
        let mut watch = store.watch("gateway.rate_limit").await.unwrap();

        store
            .put(
                "gateway.rate_limit".to_string(),
                json!({ "per_second": 100 }),
            )
            .await
            .unwrap();
        let value = watch.changed().await.unwrap();

        assert_eq!(value, Some(json!({ "per_second": 100 })));
        assert_eq!(
            store.get("gateway.rate_limit").await.unwrap(),
            Some(json!({ "per_second": 100 }))
        );
    }

    #[tokio::test]
    async fn etcd_config_store_isolates_cluster_prefixes() {
        let client = InMemoryEtcdConfigClient::new();
        let prod = EtcdConfigStore::new(client.clone(), "/lattice/prod/config");
        let staging = EtcdConfigStore::new(client, "/lattice/staging/config");

        prod.put("feature.matchmaking".to_string(), json!(true))
            .await
            .unwrap();
        staging
            .put("feature.matchmaking".to_string(), json!(false))
            .await
            .unwrap();

        assert_eq!(
            prod.get("feature.matchmaking").await.unwrap(),
            Some(json!(true))
        );
        assert_eq!(
            staging.get("feature.matchmaking").await.unwrap(),
            Some(json!(false))
        );
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

    fn test_event(subject: &str) -> EventEnvelope {
        EventEnvelope {
            event_id: EventId::new("event-1"),
            subject: Subject::new(subject),
            event_type: "ShutdownEvent".to_string(),
            source_service: service_kind!("World"),
            source_instance: InstanceId::new("world-a"),
            actor_kind: None,
            actor_id: None,
            request_id: None,
            trace: TraceContext::default(),
            occurred_unix_ms: 1,
            payload: Vec::new(),
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

    #[tokio::test]
    async fn telemetry_records_span_links_and_rejects_high_cardinality_metric_labels() {
        let telemetry = TelemetryRecorder::default();
        let trace = TraceContext {
            traceparent: Some("trace-a".to_string()),
            tracestate: None,
        };
        let linked = TraceContext {
            traceparent: Some("trace-b".to_string()),
            tracestate: None,
        };

        telemetry
            .record_span(TraceSpan {
                name: "event fanout".to_string(),
                kind: TraceSpanKind::EventBus,
                context: trace.clone(),
                links: vec![linked.clone()],
            })
            .await;
        telemetry
            .record_metric(MetricSample {
                name: "actor_mailbox_depth".to_string(),
                value: 4,
                labels: HashMap::from([("actor_kind".to_string(), "World".to_string())]),
            })
            .await
            .unwrap();
        let bad_metric = telemetry
            .record_metric(MetricSample {
                name: "rpc_latency".to_string(),
                value: 10,
                labels: HashMap::from([("request_id".to_string(), "req-1".to_string())]),
            })
            .await;

        assert_eq!(telemetry.spans().await[0].links, vec![linked]);
        assert_eq!(telemetry.metrics().await.len(), 1);
        assert!(matches!(
            bad_metric,
            Err(OpsError::HighCardinalityMetricLabel { .. })
        ));
    }

    #[tokio::test]
    async fn opentelemetry_pipeline_exports_resource_spans_metrics_and_links() {
        let telemetry = TelemetryRecorder::default();
        let exporter = InMemoryTelemetryExporter::default();
        let pipeline = OpenTelemetryPipeline::new(
            TelemetryResource {
                service_kind: service_kind!("World"),
                instance_id: InstanceId::new("world-a"),
                service_version: "test".to_string(),
            },
            exporter.clone(),
        );
        let producer = TraceContext {
            traceparent: Some("producer-trace".to_string()),
            tracestate: None,
        };
        telemetry
            .record_span(TraceSpan {
                name: "event consumer".to_string(),
                kind: TraceSpanKind::EventBus,
                context: TraceContext {
                    traceparent: Some("consumer-trace".to_string()),
                    tracestate: None,
                },
                links: vec![producer.clone()],
            })
            .await;
        telemetry
            .record_metric(MetricSample {
                name: "eventbus_deliveries".to_string(),
                value: 1,
                labels: HashMap::from([("event_type".to_string(), "PlayerEntered".to_string())]),
            })
            .await
            .unwrap();

        pipeline.export_from(&telemetry).await.unwrap();
        let batches = exporter.batches().await;

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].resource.service_kind, service_kind!("World"));
        assert_eq!(batches[0].spans[0].links, vec![producer]);
        assert_eq!(batches[0].metrics[0].name, "eventbus_deliveries");
    }
}
