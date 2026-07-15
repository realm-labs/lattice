use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use lattice_core::actor_ref::PlacementDomainId;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use lattice_core::trace::TraceContext;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::error::OpsError;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementDomainTelemetry {
    pub cluster: String,
    pub domain: PlacementDomainId,
    pub candidate_state: String,
    pub leader_term: u64,
    pub session_ready: bool,
    pub route_available: bool,
    pub unresolved_requests: u64,
    pub members: u64,
    pub capacity_units: u64,
    pub load_units: u64,
    pub slots: u64,
    pub claims: u64,
    pub plans: u64,
    pub reconciliation_backlog: u64,
    pub oldest_reconciliation_millis: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrossDomainDrainTelemetry {
    pub cluster: String,
    pub domain: PlacementDomainId,
    pub state: String,
    pub remaining_authorities: u64,
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

    pub async fn record_placement_domain(
        &self,
        snapshot: &PlacementDomainTelemetry,
    ) -> Result<(), OpsError> {
        for sample in placement_domain_metrics(snapshot) {
            self.record_metric(sample).await?;
        }
        Ok(())
    }

    pub async fn record_domain_drain(
        &self,
        snapshot: &CrossDomainDrainTelemetry,
    ) -> Result<(), OpsError> {
        for sample in domain_drain_metrics(snapshot) {
            self.record_metric(sample).await?;
        }
        Ok(())
    }

    pub async fn record_leader_concentration(
        &self,
        cluster: impl Into<String>,
        maximum_leaders_on_one_host: u64,
    ) -> Result<(), OpsError> {
        self.record_metric(MetricSample {
            name: "lattice_domain_leader_concentration_max".to_owned(),
            value: maximum_leaders_on_one_host,
            labels: HashMap::from([("cluster".to_owned(), cluster.into())]),
        })
        .await
    }

    pub async fn spans(&self) -> Vec<TraceSpan> {
        self.spans.lock().await.clone()
    }

    pub async fn metrics(&self) -> Vec<MetricSample> {
        self.metrics.lock().await.clone()
    }
}

fn placement_domain_metrics(snapshot: &PlacementDomainTelemetry) -> Vec<MetricSample> {
    let labels = HashMap::from([
        ("cluster".to_owned(), snapshot.cluster.clone()),
        ("domain".to_owned(), snapshot.domain.as_str().to_owned()),
        ("state".to_owned(), snapshot.candidate_state.clone()),
    ]);
    [
        ("lattice_domain_leader_term", snapshot.leader_term),
        (
            "lattice_domain_session_ready",
            u64::from(snapshot.session_ready),
        ),
        (
            "lattice_domain_route_available",
            u64::from(snapshot.route_available),
        ),
        (
            "lattice_domain_unresolved_requests",
            snapshot.unresolved_requests,
        ),
        ("lattice_domain_members", snapshot.members),
        ("lattice_domain_capacity_units", snapshot.capacity_units),
        ("lattice_domain_load_units", snapshot.load_units),
        ("lattice_domain_slots", snapshot.slots),
        ("lattice_domain_claims", snapshot.claims),
        ("lattice_domain_plans", snapshot.plans),
        (
            "lattice_domain_reconciliation_backlog",
            snapshot.reconciliation_backlog,
        ),
        (
            "lattice_domain_reconciliation_oldest_millis",
            snapshot.oldest_reconciliation_millis,
        ),
    ]
    .into_iter()
    .map(|(name, value)| MetricSample {
        name: name.to_owned(),
        value,
        labels: labels.clone(),
    })
    .collect()
}

fn domain_drain_metrics(snapshot: &CrossDomainDrainTelemetry) -> Vec<MetricSample> {
    let labels = HashMap::from([
        ("cluster".to_owned(), snapshot.cluster.clone()),
        ("domain".to_owned(), snapshot.domain.as_str().to_owned()),
        ("state".to_owned(), snapshot.state.clone()),
    ]);
    vec![MetricSample {
        name: "lattice_domain_drain_remaining_authorities".to_owned(),
        value: snapshot.remaining_authorities,
        labels,
    }]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn domain_metrics_are_scoped_and_never_label_exact_node_identity() {
        let recorder = TelemetryRecorder::default();
        recorder
            .record_placement_domain(&PlacementDomainTelemetry {
                cluster: "telemetry-cluster".to_owned(),
                domain: PlacementDomainId::new("battle").unwrap(),
                candidate_state: "active".to_owned(),
                leader_term: 7,
                session_ready: true,
                route_available: true,
                unresolved_requests: 2,
                members: 3,
                capacity_units: 12,
                load_units: 5,
                slots: 32,
                claims: 31,
                plans: 1,
                reconciliation_backlog: 4,
                oldest_reconciliation_millis: 250,
            })
            .await
            .unwrap();
        recorder
            .record_domain_drain(&CrossDomainDrainTelemetry {
                cluster: "telemetry-cluster".to_owned(),
                domain: PlacementDomainId::new("battle").unwrap(),
                state: "draining".to_owned(),
                remaining_authorities: 2,
            })
            .await
            .unwrap();
        recorder
            .record_leader_concentration("telemetry-cluster", 3)
            .await
            .unwrap();
        let metrics = recorder.metrics().await;
        assert_eq!(metrics.len(), 14);
        assert!(metrics.iter().all(|metric| {
            !metric.labels.contains_key("node_id")
                && !metric.labels.contains_key("incarnation")
                && metric.labels.contains_key("cluster")
        }));
        assert!(metrics.iter().any(|metric| {
            metric.name == "lattice_domain_route_available"
                && metric.labels.get("domain").map(String::as_str) == Some("battle")
        }));
    }
}
