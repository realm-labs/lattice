use std::collections::HashMap;

use async_trait::async_trait;
use axum::Json;
use axum::Router;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use lattice_core::{ActorKind, InstanceId, ServiceKind};
use lattice_placement::instance::InstanceRecord;
use lattice_placement::store::{ActorPlacementRecord, PlacementStore};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::OpsError;

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
            .route("/admin/node/summary", get(admin_node_summary))
            .route("/admin/instances", get(admin_instances))
            .route("/admin/nodes", get(admin_nodes))
            .route("/admin/placements", get(admin_placements))
            .route("/admin/vshards", get(admin_virtual_shards))
            .route("/admin/singletons", get(admin_singletons))
            .route("/admin/mailboxes", get(admin_mailboxes))
            .route("/admin/schedulers", get(admin_schedulers))
            .route("/admin/event-subscriptions", get(admin_event_subscriptions))
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

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum AdminApiError {
    #[error("admin request is unauthorized")]
    Unauthorized,
    #[error("admin resource was not found")]
    NotFound,
}

impl axum::response::IntoResponse for AdminApiError {
    fn into_response(self) -> axum::response::Response {
        match self {
            AdminApiError::Unauthorized => {
                (StatusCode::UNAUTHORIZED, self.to_string()).into_response()
            }
            AdminApiError::NotFound => (StatusCode::NOT_FOUND, self.to_string()).into_response(),
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
