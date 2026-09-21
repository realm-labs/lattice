use serde::{Deserialize, Serialize};

use crate::service::{ServiceInstanceId, ServiceName};

/// Identity every telemetry backend attaches to the spans and metrics it emits.
///
/// Backends translate this into their own resource encoding; the workspace keeps
/// a single definition so exporters cannot drift apart on service labelling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryResource {
    pub service_name: ServiceName,
    pub instance_id: ServiceInstanceId,
    pub service_version: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceContext {
    #[serde(default)]
    pub traceparent: Option<String>,
    #[serde(default)]
    pub tracestate: Option<String>,
}

impl TraceContext {
    pub fn is_empty(&self) -> bool {
        self.traceparent.is_none() && self.tracestate.is_none()
    }
}
