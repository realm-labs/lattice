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
use lattice_core::{actor_ref::PlacementDomainId, coordinator::CoordinatorScope};
use lattice_service::{
    builder::LatticeService,
    deployment::LatticeApplication,
    lifecycle::{
        CoordinatorScopeState, NodeLifecycleState, PlacementDomainState, ServiceHealthSnapshot,
    },
};
use serde::Serialize;
use tokio::net::TcpListener;

/// Selects the placement domains that must be ready before the logic component accepts traffic.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HealthReadinessPolicy {
    required_logic_domains: Option<BTreeSet<PlacementDomainId>>,
}

impl HealthReadinessPolicy {
    /// Requires every placement domain configured on the logic component.
    pub fn all_domains() -> Self {
        Self::default()
    }

    /// Requires only the supplied placement domains.
    pub fn required_domains(domains: impl IntoIterator<Item = PlacementDomainId>) -> Self {
        Self {
            required_logic_domains: Some(domains.into_iter().collect()),
        }
    }
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum HealthConfigError {
    #[error("required logic domain {domain} is not configured")]
    UnknownRequiredDomain { domain: PlacementDomainId },
    #[error("required logic domains were configured for an application without a logic component")]
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
    RequiredDomainMissing,
    RequiredDomainNotReady,
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
    pub required_domains: BTreeMap<String, String>,
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
    required_logic_domains: BTreeSet<PlacementDomainId>,
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
        let configured_logic_domains = logic
            .as_ref()
            .map(|logic| logic.health_snapshot().domains.into_keys().collect());
        let required_logic_domains =
            resolve_required_logic_domains(configured_logic_domains, policy)?;

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
                required_logic_domains,
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

fn resolve_required_logic_domains(
    configured: Option<BTreeSet<PlacementDomainId>>,
    policy: HealthReadinessPolicy,
) -> Result<BTreeSet<PlacementDomainId>, HealthConfigError> {
    match (configured, policy.required_logic_domains) {
        (Some(configured), None) => Ok(configured),
        (Some(configured), Some(required)) => {
            if let Some(domain) = required.difference(&configured).next() {
                return Err(HealthConfigError::UnknownRequiredDomain {
                    domain: domain.clone(),
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
                evaluate_readiness(*kind, snapshot, &state.required_logic_domains, &mut reasons);
            }
        }
    }
    let components = snapshots
        .into_iter()
        .map(|(kind, snapshot)| component_view(kind, &snapshot, &state.required_logic_domains))
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
    required_logic_domains: &BTreeSet<PlacementDomainId>,
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
            for domain in required_logic_domains {
                match snapshot.domains.get(domain) {
                    Some(PlacementDomainState::Ready) => {}
                    Some(state) => reasons.push(reason(
                        ProbeReasonCode::RequiredDomainNotReady,
                        component,
                        Some(domain.as_str().to_owned()),
                        domain_state(*state),
                    )),
                    None => reasons.push(reason(
                        ProbeReasonCode::RequiredDomainMissing,
                        component,
                        Some(domain.as_str().to_owned()),
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
    required_logic_domains: &BTreeSet<PlacementDomainId>,
) -> ComponentHealthView {
    let required_domains = if component == HealthComponentKind::Logic {
        required_logic_domains
            .iter()
            .map(|domain| {
                let state = snapshot
                    .domains
                    .get(domain)
                    .map_or("missing", |state| domain_state(*state));
                (domain.as_str().to_owned(), state.to_owned())
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
        required_domains,
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

fn domain_state(state: PlacementDomainState) -> &'static str {
    match state {
        PlacementDomainState::Joining => "joining",
        PlacementDomainState::Ready => "ready",
        PlacementDomainState::Degraded => "degraded",
        PlacementDomainState::Draining => "draining",
        PlacementDomainState::Terminated => "terminated",
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
        CoordinatorScope::Membership => "membership".to_owned(),
        CoordinatorScope::Placement(domain) => format!("placement/{}", domain.as_str()),
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

    fn domain(name: &str) -> PlacementDomainId {
        PlacementDomainId::new(name).unwrap()
    }

    fn snapshot(node: NodeLifecycleState) -> ServiceHealthSnapshot {
        ServiceHealthSnapshot {
            node,
            domains: BTreeMap::new(),
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
        required_logic_domains: impl IntoIterator<Item = PlacementDomainId>,
    ) -> HealthHttpAdapter {
        HealthHttpAdapter {
            state: HealthState {
                components,
                required_logic_domains: required_logic_domains.into_iter().collect(),
            },
        }
    }

    #[test]
    fn lifecycle_state_matrix_matches_kubernetes_probe_semantics() {
        let alpha = domain("alpha");
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
            health
                .domains
                .insert(alpha.clone(), PlacementDomainState::Ready);
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
    fn readiness_ignores_optional_domain_degradation_but_requires_selected_domains() {
        let alpha = domain("alpha");
        let beta = domain("beta");
        let mut health = snapshot(NodeLifecycleState::Ready);
        health
            .domains
            .insert(alpha.clone(), PlacementDomainState::Ready);
        health.domains.insert(beta, PlacementDomainState::Degraded);
        let (logic, current) = mutable_component(HealthComponentKind::Logic, health);
        let state = adapter(vec![logic], [alpha.clone()]).state;

        assert_eq!(
            evaluate(&state, ProbeKind::Readiness).status,
            ProbeStatus::Ok
        );

        current
            .lock()
            .unwrap()
            .domains
            .insert(alpha, PlacementDomainState::Degraded);
        let response = evaluate(&state, ProbeKind::Readiness);
        assert_eq!(response.status, ProbeStatus::Failed);
        assert!(response.reasons.iter().any(|reason| {
            reason.code == ProbeReasonCode::RequiredDomainNotReady && reason.state == "degraded"
        }));
    }

    #[test]
    fn embedded_readiness_requires_healthy_logic_and_coordinator_scopes() {
        let alpha = domain("alpha");
        let mut logic_health = snapshot(NodeLifecycleState::Ready);
        logic_health
            .domains
            .insert(alpha.clone(), PlacementDomainState::Ready);
        let mut coordinator_health = snapshot(NodeLifecycleState::Ready);
        coordinator_health
            .coordinator_scopes
            .insert(CoordinatorScope::Membership, CoordinatorScopeState::Standby);
        coordinator_health.coordinator_scopes.insert(
            CoordinatorScope::Placement(alpha.clone()),
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
                CoordinatorScope::Placement(alpha),
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
    fn readiness_policy_validates_required_domains_and_component_shape() {
        let alpha = domain("alpha");
        let beta = domain("beta");
        let configured = Some(BTreeSet::from([alpha.clone()]));
        assert_eq!(
            resolve_required_logic_domains(
                configured,
                HealthReadinessPolicy::required_domains([beta.clone()])
            ),
            Err(HealthConfigError::UnknownRequiredDomain { domain: beta })
        );
        assert_eq!(
            resolve_required_logic_domains(None, HealthReadinessPolicy::required_domains([alpha])),
            Err(HealthConfigError::LogicComponentUnavailable)
        );
    }

    #[tokio::test]
    async fn http_contract_uses_json_no_store_and_excludes_removed_healthz() {
        let alpha = domain("alpha");
        let mut health = snapshot(NodeLifecycleState::Draining);
        health
            .domains
            .insert(alpha.clone(), PlacementDomainState::Draining);
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
