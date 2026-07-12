use std::sync::Arc;

use async_trait::async_trait;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::{get, post};
use axum::{Json, Router};
use lattice_placement::plan::RebalancePlan;
use lattice_placement::types::PlacementSlot;
use serde::{Deserialize, Serialize};

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
    pub nodes: Vec<NodeView>,
    pub associations: Vec<AssociationView>,
    pub actor_paths: Vec<ActorPathView>,
    pub slots: Vec<PlacementSlot>,
    pub watches: Vec<WatchView>,
    pub rebalance_plans: Vec<RebalancePlan>,
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

    pub(crate) fn authorize(&self, headers: &HeaderMap) -> Result<(), AdminApiError> {
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

#[derive(Debug, Clone, Deserialize)]
pub struct ManualRelocation {
    pub operation_id: String,
    pub entity_type: String,
    pub shard_id: u32,
    pub expected_generation: u64,
    pub target_node_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PlanCommand {
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
    coordinator: lattice_placement::runtime::CoordinatorHandle,
}

impl CoordinatorAdminHandler {
    pub fn new(coordinator: lattice_placement::runtime::CoordinatorHandle) -> Self {
        Self { coordinator }
    }
}

#[async_trait]
impl AdminMutationHandler for CoordinatorAdminHandler {
    async fn pause_automatic_rebalance(&self, command: PlanCommand) -> Result<(), AdminApiError> {
        self.coordinator
            .set_automatic_paused(
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
                entity_type,
                lattice_placement::allocation::RebalanceTrigger::Automatic,
            )
            .await
            .map(|_| ())
            .map_err(map_coordinator_error)
    }

    async fn relocate_shard(&self, command: ManualRelocation) -> Result<(), AdminApiError> {
        self.coordinator
            .relocate_shard(lattice_placement::runtime::ManualRelocationRequest {
                operation_id: command.operation_id,
                entity_type: lattice_core::actor_ref::EntityType::new(command.entity_type)
                    .map_err(|_| AdminApiError::Invalid)?,
                shard_id: lattice_placement::types::ShardId::new(command.shard_id),
                expected_generation: lattice_placement::types::AssignmentGeneration::new(
                    command.expected_generation,
                )
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
            .cancel_pending(plan_id, lattice_placement::types::ShardId::new(shard_id))
            .await
            .map_err(map_coordinator_error)
    }
}

fn parse_entity_type(
    value: Option<String>,
) -> Result<Option<lattice_core::actor_ref::EntityType>, AdminApiError> {
    value
        .map(|value| {
            lattice_core::actor_ref::EntityType::new(value).map_err(|_| AdminApiError::Invalid)
        })
        .transpose()
}

fn map_coordinator_error(
    error: lattice_placement::runtime::CoordinatorRuntimeError,
) -> AdminApiError {
    match error {
        lattice_placement::runtime::CoordinatorRuntimeError::InvalidAdminOperation
        | lattice_placement::runtime::CoordinatorRuntimeError::UnknownEntityConfig
        | lattice_placement::runtime::CoordinatorRuntimeError::UnknownPlan
        | lattice_placement::runtime::CoordinatorRuntimeError::UnknownSlot => {
            AdminApiError::Invalid
        }
        lattice_placement::runtime::CoordinatorRuntimeError::IdempotencyConflict
        | lattice_placement::runtime::CoordinatorRuntimeError::StaleProposal
        | lattice_placement::runtime::CoordinatorRuntimeError::PlanConflict
        | lattice_placement::runtime::CoordinatorRuntimeError::IneligibleTarget => {
            AdminApiError::Conflict
        }
        _ => AdminApiError::Unavailable,
    }
}

#[derive(Clone)]
struct AdminState {
    auth: AdminAuth,
    snapshot: Arc<dyn Fn() -> AdminSnapshot + Send + Sync>,
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
                mutations: Arc::new(mutations),
            },
        }
    }

    pub fn router(self) -> Router {
        Router::new()
            .route("/healthz", get(|| async { StatusCode::OK }))
            .route("/readyz", get(|| async { StatusCode::OK }))
            .route("/admin/snapshot", get(snapshot))
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
    headers: HeaderMap,
) -> Result<Json<AdminSnapshot>, AdminApiError> {
    state.auth.authorize(&headers)?;
    Ok(Json((state.snapshot)()))
}

macro_rules! command_handler {
    ($name:ident, $method:ident) => {
        async fn $name(
            State(state): State<AdminState>,
            headers: HeaderMap,
            Json(command): Json<PlanCommand>,
        ) -> Result<StatusCode, AdminApiError> {
            state.auth.authorize(&headers)?;
            state.mutations.$method(command).await?;
            Ok(StatusCode::ACCEPTED)
        }
    };
}

command_handler!(pause, pause_automatic_rebalance);
command_handler!(resume, resume_automatic_rebalance);
command_handler!(evaluate, evaluate_now);
command_handler!(cancel, cancel_pending_move);

async fn relocate(
    State(state): State<AdminState>,
    headers: HeaderMap,
    Json(command): Json<ManualRelocation>,
) -> Result<StatusCode, AdminApiError> {
    state.auth.authorize(&headers)?;
    state.mutations.relocate_shard(command).await?;
    Ok(StatusCode::ACCEPTED)
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

impl axum::response::IntoResponse for AdminApiError {
    fn into_response(self) -> axum::response::Response {
        let status = match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Invalid => StatusCode::BAD_REQUEST,
            Self::Duplicate | Self::Conflict => StatusCode::CONFLICT,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        };
        status.into_response()
    }
}
