use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use axum::Json;
use axum::Router;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use lattice_core::instance::InstanceId;
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_placement::instance::InstanceRecord;
use lattice_placement::store::{
    ActorPlacementRecord, PlacementStore, SingletonPlacementRecord, VirtualShardPlacementRecord,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::error::OpsError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterSummary {
    pub instance_count: usize,
    pub actor_owner_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize)]
pub struct NodeInspectView {
    pub instance_id: InstanceId,
    pub reachable: bool,
    pub summary: Option<NodeSummary>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InspectionView {
    pub name: String,
    pub owner: Option<InstanceId>,
    pub state: String,
    pub details: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct AdminSnapshot {
    pub summary: ClusterSummary,
    pub node_summary: Option<NodeSummary>,
    pub instances: Vec<InstanceView>,
    pub nodes: Vec<NodeInspectView>,
    pub placements: Vec<InspectionView>,
    pub virtual_shards: Vec<InspectionView>,
    pub singletons: Vec<InspectionView>,
    pub mailboxes: Vec<InspectionView>,
    pub schedulers: Vec<InspectionView>,
    pub event_subscriptions: Vec<InspectionView>,
}

impl AdminSnapshot {
    pub fn new(summary: ClusterSummary, instances: Vec<InstanceView>) -> Self {
        Self {
            summary,
            node_summary: None,
            instances,
            nodes: Vec::new(),
            placements: Vec::new(),
            virtual_shards: Vec::new(),
            singletons: Vec::new(),
            mailboxes: Vec::new(),
            schedulers: Vec::new(),
            event_subscriptions: Vec::new(),
        }
    }

    pub fn from_placement_records(
        service_kind: ServiceKind,
        instance_id: InstanceId,
        mut actor_kinds: Vec<ActorKind>,
        instances: Vec<InstanceRecord>,
        actors: Vec<ActorPlacementRecord>,
        virtual_shards: Vec<VirtualShardPlacementRecord>,
        singletons: Vec<SingletonPlacementRecord>,
    ) -> Self {
        actor_kinds.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        actor_kinds.dedup();
        let actor_kind_set = actor_kinds
            .iter()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        let actors = actors
            .into_iter()
            .filter(|record| actor_kind_set.contains(&record.actor_kind))
            .collect::<Vec<_>>();
        let service_kind_filter = service_kind.clone();
        let mut snapshot = Self::new(
            ClusterSummary {
                instance_count: instances.len(),
                actor_owner_count: actors.len(),
            },
            instances.into_iter().map(InstanceView::from).collect(),
        );
        snapshot.node_summary = Some(NodeSummary {
            instance_id,
            service_kind,
            actor_kinds,
        });
        snapshot.placements = actors
            .into_iter()
            .map(|record| InspectionView {
                name: format!("{}/{:?}", record.actor_kind.as_str(), record.actor_id),
                owner: Some(record.owner),
                state: format!("{:?}", record.state),
                details: HashMap::from([
                    ("epoch".to_string(), record.epoch.0.to_string()),
                    ("lease_id".to_string(), record.lease_id.0.to_string()),
                ]),
            })
            .collect();
        let virtual_shard_service_filter = service_kind_filter.clone();
        snapshot.virtual_shards = virtual_shards
            .into_iter()
            .filter(|record| record.service_kind == virtual_shard_service_filter)
            .map(|record| InspectionView {
                name: format!("{}/#{}", record.actor_kind.as_str(), record.shard_id.0),
                owner: Some(record.owner),
                state: "Assigned".to_string(),
                details: HashMap::from([
                    ("service_kind".to_string(), record.service_kind.to_string()),
                    ("epoch".to_string(), record.epoch.0.to_string()),
                ]),
            })
            .collect();
        snapshot.singletons = singletons
            .into_iter()
            .filter(|record| record.service_kind == service_kind_filter)
            .map(|record| InspectionView {
                name: format!("{}/{}", record.singleton_kind.as_str(), record.scope),
                owner: Some(record.owner),
                state: format!("{:?}", record.state),
                details: HashMap::from([
                    ("service_kind".to_string(), record.service_kind.to_string()),
                    ("epoch".to_string(), record.epoch.0.to_string()),
                    ("lease_id".to_string(), record.lease_id.0.to_string()),
                ]),
            })
            .collect();
        snapshot
    }
}

#[derive(Debug, Clone)]
pub struct AdminHttpState {
    auth: AdminAuth,
    snapshot: AdminSnapshot,
    mutations: Arc<dyn AdminMutationHandler>,
    mutation_limiter: AdminMutationRateLimiter,
}

#[derive(Debug, Clone)]
pub struct AdminHttpAdapter {
    state: AdminHttpState,
}

impl AdminHttpAdapter {
    pub fn new(auth: AdminAuth, snapshot: AdminSnapshot) -> Self {
        Self {
            state: AdminHttpState {
                auth,
                snapshot,
                mutations: Arc::new(DisabledAdminMutationHandler),
                mutation_limiter: AdminMutationRateLimiter::default(),
            },
        }
    }

    pub fn with_mutation_handler<H>(mut self, handler: H) -> Self
    where
        H: AdminMutationHandler,
    {
        self.state.mutations = Arc::new(handler);
        self
    }

    pub fn with_mutation_rate_limit(mut self, max_per_minute: u32) -> Self {
        self.state.mutation_limiter = AdminMutationRateLimiter::new(max_per_minute);
        self
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/healthz", get(admin_healthz))
            .route("/readyz", get(admin_readyz))
            .route("/metrics", get(admin_metrics))
            .route("/admin/cluster/summary", get(admin_cluster_summary))
            .route("/admin/node/summary", get(admin_node_summary))
            .route("/admin/instances", get(admin_instances))
            .route("/admin/nodes", get(admin_nodes))
            .route("/admin/placements", get(admin_placements))
            .route("/admin/vshards", get(admin_virtual_shards))
            .route("/admin/singletons", get(admin_singletons))
            .route("/admin/mailboxes", get(admin_mailboxes))
            .route("/admin/node/mailboxes", get(admin_mailboxes))
            .route("/admin/schedulers", get(admin_schedulers))
            .route("/admin/node/schedulers", get(admin_schedulers))
            .route("/admin/event-subscriptions", get(admin_event_subscriptions))
            .route(
                "/admin/node/event-subscriptions",
                get(admin_event_subscriptions),
            )
            .route("/admin/instances/{id}/drain", post(admin_drain_instance))
            .route(
                "/admin/actors/{kind}/{id}/retry-stop",
                post(admin_retry_actor_stop),
            )
            .route(
                "/admin/actors/{kind}/{id}/force-stop",
                post(admin_force_actor_stop),
            )
            .route(
                "/admin/actors/{kind}/{id}/migrate",
                post(admin_migrate_actor),
            )
            .with_state(self.state)
    }
}

async fn admin_healthz() -> &'static str {
    "ok\n"
}

async fn admin_readyz(State(state): State<AdminHttpState>) -> Result<&'static str, AdminApiError> {
    if state.snapshot.node_summary.is_some() {
        Ok("ready\n")
    } else {
        Err(AdminApiError::NotFound)
    }
}

async fn admin_metrics(State(state): State<AdminHttpState>) -> String {
    format!(
        "lattice_admin_instances {}\nlattice_admin_actor_owners {}\n",
        state.snapshot.summary.instance_count, state.snapshot.summary.actor_owner_count
    )
}

async fn admin_cluster_summary(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
) -> Result<Json<ClusterSummary>, AdminApiError> {
    state.auth.authorize(&headers)?;
    Ok(Json(state.snapshot.summary))
}

async fn admin_node_summary(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
) -> Result<Json<NodeSummary>, AdminApiError> {
    state.auth.authorize(&headers)?;
    state
        .snapshot
        .node_summary
        .ok_or(AdminApiError::NotFound)
        .map(Json)
}

async fn admin_instances(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Query(page): Query<PageRequest>,
) -> Result<Json<Page<InstanceView>>, AdminApiError> {
    state.auth.authorize(&headers)?;
    Ok(Json(paginate(&state.snapshot.instances, page)))
}

async fn admin_nodes(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Query(page): Query<PageRequest>,
) -> Result<Json<Page<NodeInspectView>>, AdminApiError> {
    state.auth.authorize(&headers)?;
    Ok(Json(paginate(&state.snapshot.nodes, page)))
}

macro_rules! admin_inspection_handler {
    ($name:ident, $field:ident) => {
        async fn $name(
            State(state): State<AdminHttpState>,
            headers: HeaderMap,
            Query(page): Query<PageRequest>,
        ) -> Result<Json<Page<InspectionView>>, AdminApiError> {
            state.auth.authorize(&headers)?;
            Ok(Json(paginate(&state.snapshot.$field, page)))
        }
    };
}

admin_inspection_handler!(admin_placements, placements);
admin_inspection_handler!(admin_virtual_shards, virtual_shards);
admin_inspection_handler!(admin_singletons, singletons);
admin_inspection_handler!(admin_mailboxes, mailboxes);
admin_inspection_handler!(admin_schedulers, schedulers);
admin_inspection_handler!(admin_event_subscriptions, event_subscriptions);

async fn admin_drain_instance(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Path(instance_id): Path<String>,
) -> Result<Json<AdminMutationReply>, AdminApiError> {
    authorize_mutation(&state, &headers, "drain_instance").await?;
    let instance_id = InstanceId::new(instance_id);
    let reply = state.mutations.drain_instance(instance_id.clone()).await?;
    audit_mutation("drain_instance", &instance_id.to_string(), &reply);
    Ok(Json(reply))
}

async fn admin_retry_actor_stop(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Path((kind, id)): Path<(String, String)>,
) -> Result<Json<AdminMutationReply>, AdminApiError> {
    authorize_mutation(&state, &headers, "retry_actor_stop").await?;
    let target = AdminActorTarget::new(kind, id);
    let reply = state.mutations.retry_actor_stop(target.clone()).await?;
    audit_mutation("retry_actor_stop", &target.audit_key(), &reply);
    Ok(Json(reply))
}

async fn admin_force_actor_stop(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Path((kind, id)): Path<(String, String)>,
) -> Result<Json<AdminMutationReply>, AdminApiError> {
    authorize_mutation(&state, &headers, "force_actor_stop").await?;
    let target = AdminActorTarget::new(kind, id);
    let reply = state.mutations.force_actor_stop(target.clone()).await?;
    audit_mutation("force_actor_stop", &target.audit_key(), &reply);
    Ok(Json(reply))
}

async fn admin_migrate_actor(
    State(state): State<AdminHttpState>,
    headers: HeaderMap,
    Path((kind, id)): Path<(String, String)>,
) -> Result<Json<AdminMutationReply>, AdminApiError> {
    authorize_mutation(&state, &headers, "migrate_actor").await?;
    let target = AdminActorTarget::new(kind, id);
    let reply = state.mutations.migrate_actor(target.clone()).await?;
    audit_mutation("migrate_actor", &target.audit_key(), &reply);
    Ok(Json(reply))
}

async fn authorize_mutation(
    state: &AdminHttpState,
    headers: &HeaderMap,
    operation: &'static str,
) -> Result<(), AdminApiError> {
    state.auth.authorize(headers)?;
    state.mutation_limiter.check(operation).await?;
    Ok(())
}

fn audit_mutation(operation: &'static str, target: &str, reply: &AdminMutationReply) {
    if reply.accepted {
        info!(
            admin.operation = operation,
            admin.target = target,
            "admin mutation accepted"
        );
    } else {
        warn!(
            admin.operation = operation,
            admin.target = target,
            admin.message = reply.message,
            "admin mutation rejected"
        );
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AdminMutationReply {
    pub accepted: bool,
    pub message: String,
}

impl AdminMutationReply {
    pub fn accepted(message: impl Into<String>) -> Self {
        Self {
            accepted: true,
            message: message.into(),
        }
    }

    pub fn rejected(message: impl Into<String>) -> Self {
        Self {
            accepted: false,
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminActorTarget {
    pub actor_kind: ActorKind,
    pub actor_id: String,
}

impl AdminActorTarget {
    pub fn new(actor_kind: impl Into<String>, actor_id: impl Into<String>) -> Self {
        Self {
            actor_kind: ActorKind::new(actor_kind.into()),
            actor_id: actor_id.into(),
        }
    }

    fn audit_key(&self) -> String {
        format!("{}/{}", self.actor_kind.as_str(), self.actor_id)
    }
}

#[async_trait]
pub trait AdminMutationHandler: Send + Sync + std::fmt::Debug + 'static {
    async fn drain_instance(
        &self,
        instance_id: InstanceId,
    ) -> Result<AdminMutationReply, AdminApiError>;

    async fn retry_actor_stop(
        &self,
        target: AdminActorTarget,
    ) -> Result<AdminMutationReply, AdminApiError>;

    async fn force_actor_stop(
        &self,
        target: AdminActorTarget,
    ) -> Result<AdminMutationReply, AdminApiError>;

    async fn migrate_actor(
        &self,
        target: AdminActorTarget,
    ) -> Result<AdminMutationReply, AdminApiError>;
}

#[derive(Debug)]
pub struct DisabledAdminMutationHandler;

#[async_trait]
impl AdminMutationHandler for DisabledAdminMutationHandler {
    async fn drain_instance(
        &self,
        _instance_id: InstanceId,
    ) -> Result<AdminMutationReply, AdminApiError> {
        Err(AdminApiError::MutationUnsupported {
            operation: "drain_instance",
        })
    }

    async fn retry_actor_stop(
        &self,
        _target: AdminActorTarget,
    ) -> Result<AdminMutationReply, AdminApiError> {
        Err(AdminApiError::MutationUnsupported {
            operation: "retry_actor_stop",
        })
    }

    async fn force_actor_stop(
        &self,
        _target: AdminActorTarget,
    ) -> Result<AdminMutationReply, AdminApiError> {
        Err(AdminApiError::MutationUnsupported {
            operation: "force_actor_stop",
        })
    }

    async fn migrate_actor(
        &self,
        _target: AdminActorTarget,
    ) -> Result<AdminMutationReply, AdminApiError> {
        Err(AdminApiError::MutationUnsupported {
            operation: "migrate_actor",
        })
    }
}

#[derive(Debug, Clone)]
pub struct AdminMutationRateLimiter {
    max_per_minute: u32,
    state: Arc<Mutex<RateLimitState>>,
}

impl AdminMutationRateLimiter {
    pub fn new(max_per_minute: u32) -> Self {
        Self {
            max_per_minute,
            state: Arc::new(Mutex::new(RateLimitState::new())),
        }
    }

    async fn check(&self, operation: &'static str) -> Result<(), AdminApiError> {
        if self.max_per_minute == 0 {
            return Err(AdminApiError::RateLimited { operation });
        }
        let mut state = self.state.lock().await;
        if state.window_started.elapsed() >= Duration::from_secs(60) {
            *state = RateLimitState::new();
        }
        if state.count >= self.max_per_minute {
            return Err(AdminApiError::RateLimited { operation });
        }
        state.count += 1;
        Ok(())
    }
}

impl Default for AdminMutationRateLimiter {
    fn default() -> Self {
        Self::new(60)
    }
}

#[derive(Debug)]
struct RateLimitState {
    window_started: Instant,
    count: u32,
}

impl RateLimitState {
    fn new() -> Self {
        Self {
            window_started: Instant::now(),
            count: 0,
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AdminApiError {
    #[error("admin request is unauthorized")]
    Unauthorized,
    #[error("admin resource was not found")]
    NotFound,
    #[error("admin mutation {operation} is unsupported")]
    MutationUnsupported { operation: &'static str },
    #[error("admin mutation {operation} is rate limited")]
    RateLimited { operation: &'static str },
    #[error("admin mutation failed: {message}")]
    MutationFailed { message: String },
}

impl axum::response::IntoResponse for AdminApiError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AdminApiError::Unauthorized => {
                (StatusCode::UNAUTHORIZED, self.to_string()).into_response()
            }
            AdminApiError::NotFound => (StatusCode::NOT_FOUND, self.to_string()).into_response(),
            AdminApiError::MutationUnsupported { .. } => {
                (StatusCode::NOT_IMPLEMENTED, self.to_string()).into_response()
            }
            AdminApiError::RateLimited { .. } => {
                (StatusCode::TOO_MANY_REQUESTS, self.to_string()).into_response()
            }
            AdminApiError::MutationFailed { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()).into_response()
            }
        }
    }
}

#[async_trait]
pub trait NodeInspectorClient: Clone + Send + Sync + 'static {
    async fn inspect_node(&self, instance: InstanceRecord) -> Result<NodeSummary, OpsError>;
}

#[derive(Debug, Clone, Default)]
pub struct HttpNodeInspectorClient {
    admin_token: Option<String>,
}

impl HttpNodeInspectorClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_admin_token(token: impl Into<String>) -> Self {
        Self {
            admin_token: Some(token.into()),
        }
    }
}

#[async_trait]
impl NodeInspectorClient for HttpNodeInspectorClient {
    async fn inspect_node(&self, instance: InstanceRecord) -> Result<NodeSummary, OpsError> {
        let endpoint = instance.control_endpoint;
        let host = endpoint.host().ok_or_else(|| OpsError::Admin {
            message: format!("control endpoint {endpoint} has no host"),
        })?;
        let port = endpoint.port_u16().unwrap_or(80);
        let mut stream =
            TcpStream::connect((host, port))
                .await
                .map_err(|error| OpsError::Admin {
                    message: error.to_string(),
                })?;
        let token_header = self
            .admin_token
            .as_ref()
            .map(|token| format!("x-lattice-admin-token: {token}\r\n"))
            .unwrap_or_default();
        let request = format!(
            "GET /admin/node/summary HTTP/1.1\r\nHost: {host}\r\n{token_header}Connection: close\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .map_err(|error| OpsError::Admin {
                message: error.to_string(),
            })?;
        let mut response = String::new();
        stream
            .read_to_string(&mut response)
            .await
            .map_err(|error| OpsError::Admin {
                message: error.to_string(),
            })?;
        let (head, body) = response
            .split_once("\r\n\r\n")
            .ok_or_else(|| OpsError::Admin {
                message: "admin response is missing headers".to_string(),
            })?;
        if !head.starts_with("HTTP/1.1 200") {
            return Err(OpsError::Admin {
                message: head.lines().next().unwrap_or("HTTP error").to_string(),
            });
        }
        serde_json::from_str(body).map_err(|error| OpsError::Admin {
            message: error.to_string(),
        })
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

    pub async fn inspect_nodes<C>(
        &self,
        service_kind: &ServiceKind,
        client: C,
    ) -> Result<Vec<NodeInspectView>, OpsError>
    where
        C: NodeInspectorClient,
    {
        let instances = self.store.list_instances(service_kind).await?;
        let mut views = Vec::with_capacity(instances.len());
        for instance in instances {
            let instance_id = instance.instance_id.clone();
            match client.clone().inspect_node(instance).await {
                Ok(summary) => views.push(NodeInspectView {
                    instance_id,
                    reachable: true,
                    summary: Some(summary),
                    error: None,
                }),
                Err(error) => views.push(NodeInspectView {
                    instance_id,
                    reachable: false,
                    summary: None,
                    error: Some(error.to_string()),
                }),
            }
        }
        Ok(views)
    }
}
