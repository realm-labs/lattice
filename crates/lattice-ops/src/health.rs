use std::{
    collections::{BTreeMap, BTreeSet},
    future::{Future, pending},
    io,
    net::SocketAddr,
    sync::Arc,
};

use axum::{
    Json, Router,
    extract::State,
    http::{
        HeaderValue, StatusCode,
        header::{CACHE_CONTROL, CONTENT_TYPE},
    },
    response::{IntoResponse, Response},
    routing::get,
};
use lattice_model::{cluster::ActorGroupId, cluster::CoordinatorScope};
use lattice_service::{
    builder::LatticeService,
    deployment::LatticeApplication,
    lifecycle::{
        ActorGroupState, CoordinatorScopeState, NodeLifecycleState, ServiceHealthSnapshot,
    },
};
use serde::Serialize;
use tokio::net::TcpListener;

/// Selects the placement groups that must be ready before the logic component accepts traffic.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HealthReadinessPolicy {
    required_logic_groups: Option<BTreeSet<ActorGroupId>>,
}

impl HealthReadinessPolicy {
    /// Requires every placement group configured on the logic component.
    pub fn all_groups() -> Self {
        Self::default()
    }

    /// Requires only the supplied placement groups.
    pub fn required_groups(groups: impl IntoIterator<Item = ActorGroupId>) -> Self {
        Self {
            required_logic_groups: Some(groups.into_iter().collect()),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum HealthConfigError {
    #[error("required logic group {group} is not configured")]
    UnknownRequiredGroup { group: ActorGroupId },
    #[error("required logic groups were configured for an application without a logic component")]
    LogicComponentUnavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeKind {
    Startup,
    Liveness,
    Readiness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeStatus {
    Ok,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthComponentKind {
    Logic,
    Coordinator,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProbeReasonCode {
    ComponentBooting,
    ComponentTerminated,
    NodeNotReady,
    RequiredGroupMissing,
    RequiredGroupNotReady,
    CoordinatorScopesEmpty,
    CoordinatorScopeFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProbeReason {
    pub code: ProbeReasonCode,
    pub component: HealthComponentKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    pub state: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ComponentHealthView {
    pub component: HealthComponentKind,
    pub node: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub required_groups: BTreeMap<String, String>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub coordinator_scopes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ProbeResponse {
    pub probe: ProbeKind,
    pub status: ProbeStatus,
    pub reasons: Vec<ProbeReason>,
    pub components: Vec<ComponentHealthView>,
}

type SnapshotSource = Arc<dyn Fn() -> ServiceHealthSnapshot + Send + Sync>;

#[derive(Clone)]
struct ManagedComponent {
    kind: HealthComponentKind,
    snapshot: SnapshotSource,
}

impl ManagedComponent {
    fn service(kind: HealthComponentKind, service: Arc<LatticeService>) -> Self {
        Self {
            kind,
            snapshot: Arc::new(move || service.health_snapshot()),
        }
    }
}

#[derive(Clone)]
struct HealthState {
    components: Vec<ManagedComponent>,
    required_logic_groups: BTreeSet<ActorGroupId>,
}

#[derive(Clone)]
pub struct HealthHttpAdapter {
    state: HealthState,
}

impl HealthHttpAdapter {
    pub fn for_application(
        application: &LatticeApplication,
        policy: HealthReadinessPolicy,
    ) -> Result<Self, HealthConfigError> {
        let logic = application.logic().cloned();
        let coordinator = application.coordinator_service().cloned();
        let configured_logic_groups = logic
            .as_ref()
            .map(|logic| logic.health_snapshot().groups.into_keys().collect());
        let required_logic_groups = resolve_required_logic_groups(configured_logic_groups, policy)?;

        let mut components = Vec::with_capacity(2);
        if let Some(logic) = logic {
            components.push(ManagedComponent::service(HealthComponentKind::Logic, logic));
        }
        if let Some(coordinator) = coordinator {
            components.push(ManagedComponent::service(
                HealthComponentKind::Coordinator,
                coordinator,
            ));
        }
        Ok(Self {
            state: HealthState {
                components,
                required_logic_groups,
            },
        })
    }

    /// Returns a state-complete router that can be merged with other Axum routers.
    pub fn router(self) -> Router {
        Router::new()
            .route("/startupz", get(startup))
            .route("/livez", get(liveness))
            .route("/readyz", get(readiness))
            .with_state(self.state)
    }
}

fn resolve_required_logic_groups(
    configured: Option<BTreeSet<ActorGroupId>>,
    policy: HealthReadinessPolicy,
) -> Result<BTreeSet<ActorGroupId>, HealthConfigError> {
    match (configured, policy.required_logic_groups) {
        (Some(configured), None) => Ok(configured),
        (Some(configured), Some(required)) => {
            if let Some(group) = required.difference(&configured).next() {
                return Err(HealthConfigError::UnknownRequiredGroup {
                    group: group.clone(),
                });
            }
            Ok(required)
        }
        (None, Some(required)) if !required.is_empty() => {
            Err(HealthConfigError::LogicComponentUnavailable)
        }
        (None, None | Some(_)) => Ok(BTreeSet::new()),
    }
}

pub struct HealthHttpServer {
    listener: TcpListener,
    router: Router,
}

impl HealthHttpServer {
    pub fn new(listener: TcpListener, adapter: HealthHttpAdapter) -> Self {
        Self {
            listener,
            router: adapter.router(),
        }
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn run(self) -> io::Result<()> {
        self.run_until_shutdown_signal(pending::<()>()).await
    }

    pub async fn run_until_shutdown_signal<F>(self, shutdown: F) -> io::Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        axum::serve(self.listener, self.router)
            .with_graceful_shutdown(shutdown)
            .await
    }
}

async fn startup(State(state): State<HealthState>) -> Response {
    probe_response(&state, ProbeKind::Startup)
}

async fn liveness(State(state): State<HealthState>) -> Response {
    probe_response(&state, ProbeKind::Liveness)
}

async fn readiness(State(state): State<HealthState>) -> Response {
    probe_response(&state, ProbeKind::Readiness)
}

fn probe_response(state: &HealthState, probe: ProbeKind) -> Response {
    let response = evaluate(state, probe);
    let status = if response.status == ProbeStatus::Ok {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let mut response = (status, Json(response)).into_response();
    response
        .headers_mut()
        .insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
    response
}

fn evaluate(state: &HealthState, probe: ProbeKind) -> ProbeResponse {
    let snapshots = state
        .components
        .iter()
        .map(|component| (component.kind, (component.snapshot)()))
        .collect::<Vec<_>>();
    let mut reasons = Vec::new();
    for (kind, snapshot) in &snapshots {
        match probe {
            ProbeKind::Startup => evaluate_startup(*kind, snapshot, &mut reasons),
            ProbeKind::Liveness => evaluate_liveness(*kind, snapshot, &mut reasons),
            ProbeKind::Readiness => {
                evaluate_readiness(*kind, snapshot, &state.required_logic_groups, &mut reasons);
            }
        }
    }
    let components = snapshots
        .into_iter()
        .map(|(kind, snapshot)| component_view(kind, &snapshot, &state.required_logic_groups))
        .collect();
    ProbeResponse {
        probe,
        status: if reasons.is_empty() {
            ProbeStatus::Ok
        } else {
            ProbeStatus::Failed
        },
        reasons,
        components,
    }
}

fn evaluate_startup(
    component: HealthComponentKind,
    snapshot: &ServiceHealthSnapshot,
    reasons: &mut Vec<ProbeReason>,
) {
    let code = match snapshot.node {
        NodeLifecycleState::Booting => Some(ProbeReasonCode::ComponentBooting),
        NodeLifecycleState::Terminated => Some(ProbeReasonCode::ComponentTerminated),
        NodeLifecycleState::JoiningMembership
        | NodeLifecycleState::Ready
        | NodeLifecycleState::Draining
        | NodeLifecycleState::Stopping => None,
    };
    if let Some(code) = code {
        reasons.push(reason(code, component, None, node_state(snapshot.node)));
    }
}

fn evaluate_liveness(
    component: HealthComponentKind,
    snapshot: &ServiceHealthSnapshot,
    reasons: &mut Vec<ProbeReason>,
) {
    if snapshot.node == NodeLifecycleState::Terminated {
        reasons.push(reason(
            ProbeReasonCode::ComponentTerminated,
            component,
            None,
            node_state(snapshot.node),
        ));
    }
}

fn evaluate_readiness(
    component: HealthComponentKind,
    snapshot: &ServiceHealthSnapshot,
    required_logic_groups: &BTreeSet<ActorGroupId>,
    reasons: &mut Vec<ProbeReason>,
) {
    if snapshot.node != NodeLifecycleState::Ready {
        reasons.push(reason(
            ProbeReasonCode::NodeNotReady,
            component,
            None,
            node_state(snapshot.node),
        ));
    }
    match component {
        HealthComponentKind::Logic => {
            for group in required_logic_groups {
                match snapshot.groups.get(group) {
                    Some(ActorGroupState::Ready) => {}
                    Some(state) => reasons.push(reason(
                        ProbeReasonCode::RequiredGroupNotReady,
                        component,
                        Some(group.as_str().to_owned()),
                        group_state(*state),
                    )),
                    None => reasons.push(reason(
                        ProbeReasonCode::RequiredGroupMissing,
                        component,
                        Some(group.as_str().to_owned()),
                        "missing",
                    )),
                }
            }
        }
        HealthComponentKind::Coordinator => {
            if snapshot.coordinator_scopes.is_empty() {
                reasons.push(reason(
                    ProbeReasonCode::CoordinatorScopesEmpty,
                    component,
                    None,
                    "empty",
                ));
            }
            for (scope, scope_state) in &snapshot.coordinator_scopes {
                if *scope_state == CoordinatorScopeState::Failed {
                    reasons.push(reason(
                        ProbeReasonCode::CoordinatorScopeFailed,
                        component,
                        Some(scope_name(scope)),
                        coordinator_state(*scope_state),
                    ));
                }
            }
        }
    }
}

fn component_view(
    component: HealthComponentKind,
    snapshot: &ServiceHealthSnapshot,
    required_logic_groups: &BTreeSet<ActorGroupId>,
) -> ComponentHealthView {
    let required_groups = if component == HealthComponentKind::Logic {
        required_logic_groups
            .iter()
            .map(|group| {
                let state = snapshot
                    .groups
                    .get(group)
                    .map_or("missing", |state| group_state(*state));
                (group.as_str().to_owned(), state.to_owned())
            })
            .collect()
    } else {
        BTreeMap::new()
    };
    let coordinator_scopes = if component == HealthComponentKind::Coordinator {
        snapshot
            .coordinator_scopes
            .iter()
            .map(|(scope, state)| (scope_name(scope), coordinator_state(*state).to_owned()))
            .collect()
    } else {
        BTreeMap::new()
    };
    ComponentHealthView {
        component,
        node: node_state(snapshot.node).to_owned(),
        required_groups,
        coordinator_scopes,
    }
}

fn reason(
    code: ProbeReasonCode,
    component: HealthComponentKind,
    subject: Option<String>,
    state: &str,
) -> ProbeReason {
    ProbeReason {
        code,
        component,
        subject,
        state: state.to_owned(),
    }
}

fn node_state(state: NodeLifecycleState) -> &'static str {
    match state {
        NodeLifecycleState::Booting => "booting",
        NodeLifecycleState::JoiningMembership => "joining_membership",
        NodeLifecycleState::Ready => "ready",
        NodeLifecycleState::Draining => "draining",
        NodeLifecycleState::Stopping => "stopping",
        NodeLifecycleState::Terminated => "terminated",
    }
}

fn group_state(state: ActorGroupState) -> &'static str {
    match state {
        ActorGroupState::Joining => "joining",
        ActorGroupState::Ready => "ready",
        ActorGroupState::Degraded => "degraded",
        ActorGroupState::Draining => "draining",
        ActorGroupState::Terminated => "terminated",
    }
}

fn coordinator_state(state: CoordinatorScopeState) -> &'static str {
    match state {
        CoordinatorScopeState::Active => "active",
        CoordinatorScopeState::Standby => "standby",
        CoordinatorScopeState::Failed => "failed",
    }
}

fn scope_name(scope: &CoordinatorScope) -> String {
    match scope {
        CoordinatorScope::Cluster => "membership".to_owned(),
        CoordinatorScope::Group(group) => format!("placement/{}", group.as_str()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use axum::{
        body::{Body, to_bytes},
        http::{Request, header::CACHE_CONTROL},
    };
    use serde_json::Value;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        sync::oneshot,
    };
    use tower::ServiceExt;

    use super::*;

    type MutableSnapshot = Arc<Mutex<ServiceHealthSnapshot>>;

    fn group(name: &str) -> ActorGroupId {
        ActorGroupId::new(name).unwrap()
    }

    fn snapshot(node: NodeLifecycleState) -> ServiceHealthSnapshot {
        ServiceHealthSnapshot {
            node,
            groups: BTreeMap::new(),
            coordinator_scopes: BTreeMap::new(),
        }
    }

    fn mutable_component(
        kind: HealthComponentKind,
        snapshot: ServiceHealthSnapshot,
    ) -> (ManagedComponent, MutableSnapshot) {
        let snapshot = Arc::new(Mutex::new(snapshot));
        let source = snapshot.clone();
        (
            ManagedComponent {
                kind,
                snapshot: Arc::new(move || source.lock().unwrap().clone()),
            },
            snapshot,
        )
    }

    fn adapter(
        components: Vec<ManagedComponent>,
        required_logic_groups: impl IntoIterator<Item = ActorGroupId>,
    ) -> HealthHttpAdapter {
        HealthHttpAdapter {
            state: HealthState {
                components,
                required_logic_groups: required_logic_groups.into_iter().collect(),
            },
        }
    }

    #[test]
    fn lifecycle_state_matrix_matches_kubernetes_probe_semantics() {
        let alpha = group("alpha");
        let cases = [
            (NodeLifecycleState::Booting, false, true, false),
            (NodeLifecycleState::JoiningMembership, true, true, false),
            (NodeLifecycleState::Ready, true, true, true),
            (NodeLifecycleState::Draining, true, true, false),
            (NodeLifecycleState::Stopping, true, true, false),
            (NodeLifecycleState::Terminated, false, false, false),
        ];
        for (node, startup_ok, live_ok, ready_ok) in cases {
            let mut health = snapshot(node);
            health.groups.insert(alpha.clone(), ActorGroupState::Ready);
            let (logic, _) = mutable_component(HealthComponentKind::Logic, health);
            let state = adapter(vec![logic], [alpha.clone()]).state;

            assert_eq!(
                evaluate(&state, ProbeKind::Startup).status == ProbeStatus::Ok,
                startup_ok,
                "startup state {node:?}"
            );
            assert_eq!(
                evaluate(&state, ProbeKind::Liveness).status == ProbeStatus::Ok,
                live_ok,
                "liveness state {node:?}"
            );
            assert_eq!(
                evaluate(&state, ProbeKind::Readiness).status == ProbeStatus::Ok,
                ready_ok,
                "readiness state {node:?}"
            );
        }
    }

    #[test]
    fn readiness_ignores_optional_group_degradation_but_requires_selected_groups() {
        let alpha = group("alpha");
        let beta = group("beta");
        let mut health = snapshot(NodeLifecycleState::Ready);
        health.groups.insert(alpha.clone(), ActorGroupState::Ready);
        health.groups.insert(beta, ActorGroupState::Degraded);
        let (logic, current) = mutable_component(HealthComponentKind::Logic, health);
        let state = adapter(vec![logic], [alpha.clone()]).state;

        assert_eq!(
            evaluate(&state, ProbeKind::Readiness).status,
            ProbeStatus::Ok
        );

        current
            .lock()
            .unwrap()
            .groups
            .insert(alpha, ActorGroupState::Degraded);
        let response = evaluate(&state, ProbeKind::Readiness);
        assert_eq!(response.status, ProbeStatus::Failed);
        assert!(response.reasons.iter().any(|reason| {
            reason.code == ProbeReasonCode::RequiredGroupNotReady && reason.state == "degraded"
        }));
    }

    #[test]
    fn embedded_readiness_requires_healthy_logic_and_coordinator_scopes() {
        let alpha = group("alpha");
        let mut logic_health = snapshot(NodeLifecycleState::Ready);
        logic_health
            .groups
            .insert(alpha.clone(), ActorGroupState::Ready);
        let mut coordinator_health = snapshot(NodeLifecycleState::Ready);
        coordinator_health
            .coordinator_scopes
            .insert(CoordinatorScope::Cluster, CoordinatorScopeState::Standby);
        coordinator_health.coordinator_scopes.insert(
            CoordinatorScope::Group(alpha.clone()),
            CoordinatorScopeState::Active,
        );
        let (logic, _) = mutable_component(HealthComponentKind::Logic, logic_health);
        let (coordinator, current_coordinator) =
            mutable_component(HealthComponentKind::Coordinator, coordinator_health);
        let state = adapter(vec![logic, coordinator], [alpha.clone()]).state;

        assert_eq!(
            evaluate(&state, ProbeKind::Readiness).status,
            ProbeStatus::Ok
        );

        current_coordinator
            .lock()
            .unwrap()
            .coordinator_scopes
            .insert(
                CoordinatorScope::Group(alpha),
                CoordinatorScopeState::Failed,
            );
        let response = evaluate(&state, ProbeKind::Readiness);
        assert_eq!(response.status, ProbeStatus::Failed);
        assert!(response.reasons.iter().any(|reason| {
            reason.code == ProbeReasonCode::CoordinatorScopeFailed
                && reason.component == HealthComponentKind::Coordinator
        }));
    }

    #[test]
    fn readiness_policy_validates_required_groups_and_component_shape() {
        let alpha = group("alpha");
        let beta = group("beta");
        let configured = Some(BTreeSet::from([alpha.clone()]));
        assert_eq!(
            resolve_required_logic_groups(
                configured,
                HealthReadinessPolicy::required_groups([beta.clone()])
            ),
            Err(HealthConfigError::UnknownRequiredGroup { group: beta })
        );
        assert_eq!(
            resolve_required_logic_groups(None, HealthReadinessPolicy::required_groups([alpha])),
            Err(HealthConfigError::LogicComponentUnavailable)
        );
    }

    #[tokio::test]
    async fn http_contract_uses_json_no_store_and_excludes_removed_healthz() {
        let alpha = group("alpha");
        let mut health = snapshot(NodeLifecycleState::Draining);
        health
            .groups
            .insert(alpha.clone(), ActorGroupState::Draining);
        let (logic, _) = mutable_component(HealthComponentKind::Logic, health);
        let router = adapter(vec![logic], [alpha]).router();

        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/readyz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(response.headers()[CACHE_CONTROL], "no-store");
        let body = to_bytes(response.into_body(), 16 * 1024).await.unwrap();
        let json: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["probe"], "readiness");
        assert_eq!(json["status"], "failed");
        assert_eq!(json["reasons"][0]["code"], "node_not_ready");
        assert_eq!(json["components"][0]["node"], "draining");

        let live = router
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/livez")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(live.status(), StatusCode::OK);

        let removed = router
            .oneshot(
                Request::builder()
                    .uri("/healthz")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(removed.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn hosted_server_binds_reports_and_stops_gracefully() {
        let (logic, _) = mutable_component(
            HealthComponentKind::Logic,
            snapshot(NodeLifecycleState::JoiningMembership),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = HealthHttpServer::new(listener, adapter(vec![logic], []));
        let address = server.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let task = tokio::spawn(server.run_until_shutdown_signal(async move {
            let _ = shutdown_rx.await;
        }));

        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        stream
            .write_all(b"GET /startupz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        assert!(response.starts_with("HTTP/1.1 200 OK"), "{response}");

        shutdown_tx.send(()).unwrap();
        task.await.unwrap().unwrap();
    }
}
