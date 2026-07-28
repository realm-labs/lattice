use lattice_core::trace::TelemetryResource;
use opentelemetry::KeyValue;
use opentelemetry_sdk::Resource;

pub(crate) fn to_otel_resource(resource: &TelemetryResource) -> Resource {
    Resource::builder()
        .with_service_name(resource.service_kind.as_str().to_string())
        .with_attribute(KeyValue::new(
            "service.version",
            resource.service_version.clone(),
        ))
        .with_attribute(KeyValue::new(
            "service.instance.id",
            resource.instance_id.as_str().to_string(),
        ))
        .build()
}
