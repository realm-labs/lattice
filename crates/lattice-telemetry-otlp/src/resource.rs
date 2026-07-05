use lattice_core::{InstanceId, ServiceKind};
use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryResource {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub service_version: String,
}

impl TelemetryResource {
    pub(crate) fn to_otel_resource(&self) -> Resource {
        Resource::builder()
            .with_service_name(self.service_kind.as_str().to_string())
            .with_attribute(KeyValue::new(
                "service.version",
                self.service_version.clone(),
            ))
            .with_attribute(KeyValue::new(
                "service.instance.id",
                self.instance_id.as_str().to_string(),
            ))
            .build()
    }
}
