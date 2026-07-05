use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use lattice_core::{InstanceId, ServiceKind, TraceContext};
use serde::Serialize;
use tokio::sync::Mutex;

use crate::OpsError;

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
