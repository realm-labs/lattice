use std::{fmt, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{ConnectInfo, Query, State},
    http::{HeaderMap, StatusCode, request::Parts},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use lattice_core::actor_ref::{EntityType, PlacementDomainId};
use lattice_core::release::ClusterReleaseState;
use lattice_placement::{
    allocation::RebalanceTrigger,
    plan::RebalancePlan,
    runtime::{CoordinatorHandle, CoordinatorRuntimeError, ManualRelocationRequest},
    types::{AssignmentGeneration, PlacementSlot, ShardId},
};
use serde::{Deserialize, Serialize};

/// Header that carries the admin bearer token.
pub const ADMIN_TOKEN_HEADER: &str = "x-lattice-admin-token";

/// Cap applied to every unbounded snapshot collection when the deployment does not configure one.
pub const DEFAULT_SNAPSHOT_LIMIT: usize = 500;

/// Tracing target of the admin audit trail.
const ADMIN_TARGET: &str = "lattice.ops.admin";

const UNKNOWN_PEER: &str = "unknown";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeView {
    pub node_id: String,
    pub address: String,
    pub incarnation: String,
    pub roles: Vec<String>,
    pub ready: bool,
    pub draining: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssociationView {
    pub remote_node_id: String,
    pub remote_address: String,
    pub remote_incarnation: String,
    pub association_id: String,
    pub state: String,
    pub attached_lanes: usize,
    pub queued_frames: usize,
    pub queued_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorPathView {
    pub path: String,
    pub activation_id: String,
    pub protocol_id: u64,
    pub mailbox_depth: usize,
    pub lifecycle: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatchView {
    pub watch_id: String,
    pub exact_path: String,
    pub activation_id: String,
    pub acknowledged: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AdminSnapshot {
    pub partial: bool,
    pub coordinator_term: Option<u64>,
    pub coordinator_revision: Option<u64>,
    pub release: Option<ClusterReleaseState>,
    pub nodes: Vec<NodeView>,
    pub associations: Vec<AssociationView>,
    pub actor_paths: Vec<ActorPathView>,
    pub slots: Vec<PlacementSlot>,
    pub watches: Vec<WatchView>,
    pub rebalance_plans: Vec<RebalancePlan>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdminSurface {
    Inspection,
    Mutation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdminPrincipal {
    BearerToken,
    Unauthenticated,
}

impl AdminPrincipal {
    fn as_str(self) -> &'static str {
        match self {
            Self::BearerToken => "bearer-token",
            Self::Unauthenticated => "unauthenticated",
        }
    }
}

/// Credential policy of the admin HTTP surface.
#[derive(Clone)]
pub struct AdminAuth {
    token: Option<String>,
    allow_unauthenticated_admin: bool,
}

impl fmt::Debug for AdminAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminAuth")
            .field("token_configured", &self.token.is_some())
            .field(
                "allow_unauthenticated_admin",
                &self.allow_unauthenticated_admin,
            )
            .finish()
    }
}

impl AdminAuth {
    /// Serves inspection without a credential and keeps every mutation route unmounted.
    pub fn disabled() -> Self {
        Self {
            token: None,
            allow_unauthenticated_admin: false,
        }
    }

    /// Requires the bearer token on every admin route.
    pub fn bearer_token(token: impl Into<String>) -> Self {
        Self {
            token: Some(token.into()),
            allow_unauthenticated_admin: false,
        }
    }

    /// Mounts mutation routes without any credential; local development only.
    pub fn allow_unauthenticated_admin() -> Self {
        Self {
            token: None,
            allow_unauthenticated_admin: true,
        }
    }

    pub(crate) fn mutations_mounted(&self) -> bool {
        self.token.is_some() || self.allow_unauthenticated_admin
    }

    pub(crate) fn unauthenticated_mutations(&self) -> bool {
        self.allow_unauthenticated_admin
    }

    pub(crate) fn authorize(
        &self,
        headers: &HeaderMap,
        surface: AdminSurface,
    ) -> Result<AdminPrincipal, AdminApiError> {
        let Some(expected) = &self.token else {
            return match surface {
                AdminSurface::Inspection => Ok(AdminPrincipal::Unauthenticated),
                AdminSurface::Mutation if self.allow_unauthenticated_admin => {
                    Ok(AdminPrincipal::Unauthenticated)
                }
                AdminSurface::Mutation => Err(AdminApiError::Unauthorized),
            };
        };
        let actual = headers
            .get(ADMIN_TOKEN_HEADER)
            .map_or(&[][..], |value| value.as_bytes());
        if constant_time_eq(actual, expected.as_bytes()) {
            Ok(AdminPrincipal::BearerToken)
        } else {
            Err(AdminApiError::Unauthorized)
        }
    }
}

fn constant_time_eq(actual: &[u8], expected: &[u8]) -> bool {
    let mut difference = u8::from(actual.len() != expected.len());
    for (index, expected_byte) in expected.iter().enumerate() {
        difference |= actual.get(index).copied().unwrap_or_default() ^ expected_byte;
    }
    difference == 0
}

#[derive(Debug, Clone, Deserialize)]
pub struct ManualRelocation {
    pub domain: String,
    pub operation_id: String,
    pub entity_type: String,
    pub shard_id: u32,
    pub expected_generation: u64,
    pub target_node_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlanCommand {
    pub domain: String,
    pub operation_id: String,
    pub entity_type: Option<String>,
    pub plan_id: Option<String>,
    pub shard_id: Option<u32>,
}

#[async_trait]
pub trait AdminMutationHandler: Send + Sync + 'static {
    async fn pause_automatic_rebalance(&self, command: PlanCommand) -> Result<(), AdminApiError>;
    async fn resume_automatic_rebalance(&self, command: PlanCommand) -> Result<(), AdminApiError>;
    async fn evaluate_now(&self, command: PlanCommand) -> Result<(), AdminApiError>;
    async fn relocate_shard(&self, command: ManualRelocation) -> Result<(), AdminApiError>;
    async fn cancel_pending_move(&self, command: PlanCommand) -> Result<(), AdminApiError>;
}

#[derive(Clone)]
pub struct CoordinatorAdminHandler {
    coordinator: CoordinatorHandle,
}

impl CoordinatorAdminHandler {
    pub fn new(coordinator: CoordinatorHandle) -> Self {
        Self { coordinator }
    }
}

#[async_trait]
impl AdminMutationHandler for CoordinatorAdminHandler {
    async fn pause_automatic_rebalance(&self, command: PlanCommand) -> Result<(), AdminApiError> {
        self.coordinator
            .set_automatic_paused(
                parse_domain(command.domain)?,
                command.operation_id,
                parse_entity_type(command.entity_type)?,
                true,
            )
            .await
            .map_err(map_coordinator_error)
    }

    async fn resume_automatic_rebalance(&self, command: PlanCommand) -> Result<(), AdminApiError> {
        self.coordinator
            .set_automatic_paused(
                parse_domain(command.domain)?,
                command.operation_id,
                parse_entity_type(command.entity_type)?,
                false,
            )
            .await
            .map_err(map_coordinator_error)
    }

    async fn evaluate_now(&self, command: PlanCommand) -> Result<(), AdminApiError> {
        let entity_type = parse_entity_type(command.entity_type)?.ok_or(AdminApiError::Invalid)?;
        self.coordinator
            .evaluate_rebalance(
                parse_domain(command.domain)?,
                command.operation_id,
                entity_type,
                operator_trigger(),
            )
            .await
            .map(|_| ())
            .map_err(map_coordinator_error)
    }

    async fn relocate_shard(&self, command: ManualRelocation) -> Result<(), AdminApiError> {
        self.coordinator
            .relocate_shard(ManualRelocationRequest {
                domain: parse_domain(command.domain)?,
                operation_id: command.operation_id,
                entity_type: EntityType::new(command.entity_type)
                    .map_err(|_| AdminApiError::Invalid)?,
                shard_id: ShardId::new(command.shard_id),
                expected_generation: AssignmentGeneration::new(command.expected_generation)
                    .map_err(|_| AdminApiError::Invalid)?,
                target_node_id: command.target_node_id,
            })
            .await
            .map(|_| ())
            .map_err(map_coordinator_error)
    }

    async fn cancel_pending_move(&self, command: PlanCommand) -> Result<(), AdminApiError> {
        let plan_id = command
            .plan_id
            .as_deref()
            .and_then(|value| u128::from_str_radix(value.trim_start_matches("0x"), 16).ok())
            .ok_or(AdminApiError::Invalid)?;
        let shard_id = command.shard_id.ok_or(AdminApiError::Invalid)?;
        self.coordinator
            .cancel_pending(
                parse_domain(command.domain)?,
                command.operation_id,
                plan_id,
                ShardId::new(shard_id),
            )
            .await
            .map_err(map_coordinator_error)
    }
}

fn operator_trigger() -> RebalanceTrigger {
    RebalanceTrigger::Manual {
        source: None,
        target: None,
        bypass_improvement: false,
    }
}

fn parse_domain(value: String) -> Result<PlacementDomainId, AdminApiError> {
    PlacementDomainId::new(value).map_err(|_| AdminApiError::Invalid)
}

fn parse_entity_type(value: Option<String>) -> Result<Option<EntityType>, AdminApiError> {
    value
        .map(|value| EntityType::new(value).map_err(|_| AdminApiError::Invalid))
        .transpose()
}

fn map_coordinator_error(error: CoordinatorRuntimeError) -> AdminApiError {
    match error {
        CoordinatorRuntimeError::InvalidAdminOperation
        | CoordinatorRuntimeError::UnknownEntityConfig
        | CoordinatorRuntimeError::UnknownPlan
        | CoordinatorRuntimeError::UnknownSlot => AdminApiError::Invalid,
        CoordinatorRuntimeError::IdempotencyConflict
        | CoordinatorRuntimeError::StaleProposal
        | CoordinatorRuntimeError::PlanConflict
        | CoordinatorRuntimeError::IneligibleTarget => AdminApiError::Conflict,
        _ => AdminApiError::Unavailable,
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct SnapshotQuery {
    /// Requested entries per snapshot collection; never exceeds the configured cap.
    #[serde(default)]
    pub limit: Option<usize>,
}

#[derive(Clone)]
struct AdminState {
    auth: AdminAuth,
    snapshot: Arc<dyn Fn() -> AdminSnapshot + Send + Sync>,
    snapshot_limit: usize,
    mutations: Arc<dyn AdminMutationHandler>,
}

pub struct AdminHttpAdapter {
    state: AdminState,
}

impl AdminHttpAdapter {
    pub fn new<S, M>(auth: AdminAuth, snapshot: S, mutations: M) -> Self
    where
        S: Fn() -> AdminSnapshot + Send + Sync + 'static,
        M: AdminMutationHandler,
    {
        Self {
            state: AdminState {
                auth,
                snapshot: Arc::new(snapshot),
                snapshot_limit: DEFAULT_SNAPSHOT_LIMIT,
                mutations: Arc::new(mutations),
            },
        }
    }

    /// Caps every unbounded snapshot collection; requests may only ask for fewer entries.
    pub fn with_snapshot_limit(mut self, limit: usize) -> Self {
        self.state.snapshot_limit = limit.max(1);
        self
    }

    pub fn router(self) -> Router {
        let router = Router::new().route("/admin/snapshot", get(snapshot));
        if !self.state.auth.mutations_mounted() {
            tracing::warn!(
                target: ADMIN_TARGET,
                "admin mutation routes are not mounted because no admin credential is configured"
            );
            return router.with_state(self.state);
        }
        if self.state.auth.unauthenticated_mutations() {
            tracing::warn!(
                target: ADMIN_TARGET,
                "admin mutation routes accept unauthenticated requests"
            );
        }
        router
            .route("/admin/rebalance/pause", post(pause))
            .route("/admin/rebalance/resume", post(resume))
            .route("/admin/rebalance/evaluate", post(evaluate))
            .route("/admin/rebalance/relocate", post(relocate))
            .route("/admin/rebalance/cancel-pending", post(cancel))
            .with_state(self.state)
    }
}

async fn snapshot(
    State(state): State<AdminState>,
    Query(query): Query<SnapshotQuery>,
    headers: HeaderMap,
) -> Result<Json<AdminSnapshot>, AdminApiError> {
    state.auth.authorize(&headers, AdminSurface::Inspection)?;
    let limit = match query.limit {
        Some(0) => return Err(AdminApiError::Invalid),
        Some(limit) => limit.min(state.snapshot_limit),
        None => state.snapshot_limit,
    };
    Ok(Json(limited_snapshot((state.snapshot)(), limit)))
}

fn limited_snapshot(mut snapshot: AdminSnapshot, limit: usize) -> AdminSnapshot {
    let truncated = truncate(&mut snapshot.nodes, limit)
        | truncate(&mut snapshot.associations, limit)
        | truncate(&mut snapshot.actor_paths, limit)
        | truncate(&mut snapshot.slots, limit)
        | truncate(&mut snapshot.watches, limit)
        | truncate(&mut snapshot.rebalance_plans, limit);
    snapshot.partial |= truncated;
    snapshot
}

fn truncate<T>(entries: &mut Vec<T>, limit: usize) -> bool {
    if entries.len() <= limit {
        return false;
    }
    entries.truncate(limit);
    true
}

/// Identifies the caller of an admin mutation for the audit trail.
struct AdminCaller {
    peer: String,
    outcome: Result<AdminPrincipal, AdminApiError>,
}

impl AdminCaller {
    fn resolve(auth: &AdminAuth, parts: &Parts) -> Self {
        Self {
            peer: parts
                .extensions
                .get::<ConnectInfo<SocketAddr>>()
                .map_or_else(
                    || UNKNOWN_PEER.to_owned(),
                    |ConnectInfo(peer)| peer.to_string(),
                ),
            outcome: auth.authorize(&parts.headers, AdminSurface::Mutation),
        }
    }

    fn authorized(&self) -> Result<(), AdminApiError> {
        self.outcome.clone().map(|_| ())
    }

    fn principal(&self) -> &'static str {
        match &self.outcome {
            Ok(principal) => principal.as_str(),
            Err(_) => "unauthorized",
        }
    }
}

fn audit_plan_command(
    operation: &'static str,
    caller: &AdminCaller,
    command: &PlanCommand,
    outcome: &Result<(), AdminApiError>,
) {
    match outcome {
        Ok(()) => tracing::info!(
            target: ADMIN_TARGET,
            operation,
            principal = caller.principal(),
            peer = caller.peer.as_str(),
            domain = command.domain.as_str(),
            operation_id = command.operation_id.as_str(),
            entity_type = ?command.entity_type,
            plan_id = ?command.plan_id,
            shard_id = ?command.shard_id,
            "admin mutation applied"
        ),
        Err(error) => tracing::warn!(
            target: ADMIN_TARGET,
            operation,
            principal = caller.principal(),
            peer = caller.peer.as_str(),
            domain = command.domain.as_str(),
            operation_id = command.operation_id.as_str(),
            entity_type = ?command.entity_type,
            plan_id = ?command.plan_id,
            shard_id = ?command.shard_id,
            %error,
            "admin mutation rejected"
        ),
    }
}

fn audit_relocation(
    caller: &AdminCaller,
    command: &ManualRelocation,
    outcome: &Result<(), AdminApiError>,
) {
    match outcome {
        Ok(()) => tracing::info!(
            target: ADMIN_TARGET,
            operation = "rebalance.relocate",
            principal = caller.principal(),
            peer = caller.peer.as_str(),
            domain = command.domain.as_str(),
            operation_id = command.operation_id.as_str(),
            entity_type = command.entity_type.as_str(),
            shard_id = command.shard_id,
            expected_generation = command.expected_generation,
            target_node_id = command.target_node_id.as_str(),
            "admin mutation applied"
        ),
        Err(error) => tracing::warn!(
            target: ADMIN_TARGET,
            operation = "rebalance.relocate",
            principal = caller.principal(),
            peer = caller.peer.as_str(),
            domain = command.domain.as_str(),
            operation_id = command.operation_id.as_str(),
            entity_type = command.entity_type.as_str(),
            shard_id = command.shard_id,
            expected_generation = command.expected_generation,
            target_node_id = command.target_node_id.as_str(),
            %error,
            "admin mutation rejected"
        ),
    }
}

macro_rules! command_handler {
    ($name:ident, $method:ident, $operation:literal) => {
        async fn $name(
            State(state): State<AdminState>,
            parts: Parts,
            Json(command): Json<PlanCommand>,
        ) -> Result<StatusCode, AdminApiError> {
            let caller = AdminCaller::resolve(&state.auth, &parts);
            let outcome = match caller.authorized() {
                Ok(()) => state.mutations.$method(command.clone()).await,
                Err(error) => Err(error),
            };
            audit_plan_command($operation, &caller, &command, &outcome);
            outcome.map(|()| StatusCode::ACCEPTED)
        }
    };
}

command_handler!(pause, pause_automatic_rebalance, "rebalance.pause");
command_handler!(resume, resume_automatic_rebalance, "rebalance.resume");
command_handler!(evaluate, evaluate_now, "rebalance.evaluate");
command_handler!(cancel, cancel_pending_move, "rebalance.cancel-pending");

async fn relocate(
    State(state): State<AdminState>,
    parts: Parts,
    Json(command): Json<ManualRelocation>,
) -> Result<StatusCode, AdminApiError> {
    let caller = AdminCaller::resolve(&state.auth, &parts);
    let outcome = match caller.authorized() {
        Ok(()) => state.mutations.relocate_shard(command.clone()).await,
        Err(error) => Err(error),
    };
    audit_relocation(&caller, &command, &outcome);
    outcome.map(|()| StatusCode::ACCEPTED)
}

#[derive(Debug, Clone, thiserror::Error)]
pub enum AdminApiError {
    #[error("admin authentication failed")]
    Unauthorized,
    #[error("admin command is invalid")]
    Invalid,
    #[error("admin operation ID was already applied")]
    Duplicate,
    #[error("admin command conflicts with current plan or generation")]
    Conflict,
    #[error("admin backend is unavailable")]
    Unavailable,
}

impl IntoResponse for AdminApiError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Invalid => StatusCode::BAD_REQUEST,
            Self::Duplicate | Self::Conflict => StatusCode::CONFLICT,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        };
        status.into_response()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use axum::{
        body::{Body, to_bytes},
        http::{Request, header::CONTENT_TYPE},
    };
    use serde_json::{Value, json};
    use tower::ServiceExt;

    use super::*;

    #[derive(Debug, Default)]
    struct RecordingMutations {
        applied: Mutex<Vec<String>>,
    }

    impl RecordingMutations {
        fn record(&self, operation: &str) {
            self.applied.lock().unwrap().push(operation.to_owned());
        }

        fn applied(&self) -> Vec<String> {
            self.applied.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl AdminMutationHandler for Arc<RecordingMutations> {
        async fn pause_automatic_rebalance(
            &self,
            _command: PlanCommand,
        ) -> Result<(), AdminApiError> {
            self.record("pause");
            Ok(())
        }

        async fn resume_automatic_rebalance(
            &self,
            _command: PlanCommand,
        ) -> Result<(), AdminApiError> {
            self.record("resume");
            Ok(())
        }

        async fn evaluate_now(&self, _command: PlanCommand) -> Result<(), AdminApiError> {
            self.record("evaluate");
            Ok(())
        }

        async fn relocate_shard(&self, _command: ManualRelocation) -> Result<(), AdminApiError> {
            self.record("relocate");
            Ok(())
        }

        async fn cancel_pending_move(&self, _command: PlanCommand) -> Result<(), AdminApiError> {
            self.record("cancel");
            Ok(())
        }
    }

    fn node(node_id: &str) -> NodeView {
        NodeView {
            node_id: node_id.to_owned(),
            address: "127.0.0.1:25520".to_owned(),
            incarnation: "1".to_owned(),
            roles: vec!["logic".to_owned()],
            ready: true,
            draining: false,
        }
    }

    fn populated_snapshot() -> AdminSnapshot {
        AdminSnapshot {
            nodes: vec![node("node-a"), node("node-b"), node("node-c")],
            watches: (0..3)
                .map(|index| WatchView {
                    watch_id: index.to_string(),
                    exact_path: format!("/world/{index}"),
                    activation_id: index.to_string(),
                    acknowledged: true,
                })
                .collect(),
            ..AdminSnapshot::default()
        }
    }

    fn router(auth: AdminAuth) -> (Router, Arc<RecordingMutations>) {
        let mutations = Arc::new(RecordingMutations::default());
        let router = AdminHttpAdapter::new(auth, populated_snapshot, mutations.clone()).router();
        (router, mutations)
    }

    fn plan_request(uri: &str, token: Option<&str>) -> Request<Body> {
        let body = json!({
            "domain": "world",
            "operation_id": "op-1",
            "entity_type": "player",
            "plan_id": null,
            "shard_id": 7,
        });
        let mut request = Request::builder()
            .method("POST")
            .uri(uri)
            .header(CONTENT_TYPE, "application/json");
        if let Some(token) = token {
            request = request.header(ADMIN_TOKEN_HEADER, token);
        }
        request
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn relocate_request(token: Option<&str>) -> Request<Body> {
        let body = json!({
            "domain": "world",
            "operation_id": "op-2",
            "entity_type": "player",
            "shard_id": 7,
            "expected_generation": 4,
            "target_node_id": "node-b",
        });
        let mut request = Request::builder()
            .method("POST")
            .uri("/admin/rebalance/relocate")
            .header(CONTENT_TYPE, "application/json");
        if let Some(token) = token {
            request = request.header(ADMIN_TOKEN_HEADER, token);
        }
        request
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap()
    }

    fn snapshot_request(uri: &str, token: Option<&str>) -> Request<Body> {
        let mut request = Request::builder().uri(uri);
        if let Some(token) = token {
            request = request.header(ADMIN_TOKEN_HEADER, token);
        }
        request.body(Body::empty()).unwrap()
    }

    async fn snapshot_json(router: Router, uri: &str, token: Option<&str>) -> Value {
        let response = router.oneshot(snapshot_request(uri, token)).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), 64 * 1024).await.unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    #[tokio::test]
    async fn missing_credential_keeps_mutation_routes_unmounted() {
        let (router, mutations) = router(AdminAuth::disabled());

        for uri in [
            "/admin/rebalance/pause",
            "/admin/rebalance/resume",
            "/admin/rebalance/evaluate",
            "/admin/rebalance/cancel-pending",
        ] {
            let response = router
                .clone()
                .oneshot(plan_request(uri, None))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
        }
        let relocation = router
            .clone()
            .oneshot(relocate_request(None))
            .await
            .unwrap();
        assert_eq!(relocation.status(), StatusCode::NOT_FOUND);
        assert!(mutations.applied().is_empty());

        let snapshot = snapshot_json(router, "/admin/snapshot", None).await;
        assert_eq!(snapshot["nodes"][0]["node_id"], "node-a");
    }

    #[tokio::test]
    async fn bearer_token_guards_inspection_and_mutation_routes() {
        let (router, mutations) = router(AdminAuth::bearer_token("secret"));

        let missing = router
            .clone()
            .oneshot(plan_request("/admin/rebalance/pause", None))
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::UNAUTHORIZED);
        let wrong = router
            .clone()
            .oneshot(plan_request("/admin/rebalance/pause", Some("secre")))
            .await
            .unwrap();
        assert_eq!(wrong.status(), StatusCode::UNAUTHORIZED);
        let unauthenticated_snapshot = router
            .clone()
            .oneshot(snapshot_request("/admin/snapshot", None))
            .await
            .unwrap();
        assert_eq!(unauthenticated_snapshot.status(), StatusCode::UNAUTHORIZED);
        assert!(mutations.applied().is_empty());

        let accepted = router
            .clone()
            .oneshot(plan_request("/admin/rebalance/pause", Some("secret")))
            .await
            .unwrap();
        assert_eq!(accepted.status(), StatusCode::ACCEPTED);
        let relocation = router
            .clone()
            .oneshot(relocate_request(Some("secret")))
            .await
            .unwrap();
        assert_eq!(relocation.status(), StatusCode::ACCEPTED);
        assert_eq!(mutations.applied(), vec!["pause", "relocate"]);

        let snapshot = snapshot_json(router, "/admin/snapshot", Some("secret")).await;
        assert_eq!(snapshot["nodes"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn unauthenticated_opt_in_mounts_mutation_routes() {
        let (router, mutations) = router(AdminAuth::allow_unauthenticated_admin());

        let response = router
            .oneshot(plan_request("/admin/rebalance/evaluate", None))
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(mutations.applied(), vec!["evaluate"]);
    }

    #[tokio::test]
    async fn snapshot_limit_bounds_collections_and_marks_partial() {
        let router = AdminHttpAdapter::new(
            AdminAuth::disabled(),
            populated_snapshot,
            Arc::new(RecordingMutations::default()),
        )
        .with_snapshot_limit(2)
        .router();

        let capped = snapshot_json(router.clone(), "/admin/snapshot", None).await;
        assert_eq!(capped["nodes"].as_array().unwrap().len(), 2);
        assert_eq!(capped["watches"].as_array().unwrap().len(), 2);
        assert_eq!(capped["partial"], true);

        let requested = snapshot_json(router.clone(), "/admin/snapshot?limit=1", None).await;
        assert_eq!(requested["nodes"].as_array().unwrap().len(), 1);

        let clamped = snapshot_json(router.clone(), "/admin/snapshot?limit=99", None).await;
        assert_eq!(clamped["nodes"].as_array().unwrap().len(), 2);

        let rejected = router
            .oneshot(snapshot_request("/admin/snapshot?limit=0", None))
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn token_comparison_rejects_length_and_content_mismatch() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secre", b"secret"));
        assert!(!constant_time_eq(b"secretx", b"secret"));
        assert!(!constant_time_eq(b"secreT", b"secret"));
        assert!(!constant_time_eq(b"", b"secret"));
    }

    #[test]
    fn operator_evaluation_outranks_automatic_planning() {
        let trigger = operator_trigger();

        assert_eq!(
            trigger,
            RebalanceTrigger::Manual {
                source: None,
                target: None,
                bypass_improvement: false,
            }
        );
        assert!(trigger.priority() < RebalanceTrigger::Automatic.priority());
    }
}
