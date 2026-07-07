use lattice_core::instance::InstanceId;
use lattice_core::service_kind;

use crate::config::TelemetryConfig;
use crate::telemetry::LatticeTelemetry;

#[test]
fn telemetry_config_defaults_to_fmt_only() {
    let config = TelemetryConfig::fmt_only("1.2.3");

    assert!(config.fmt_enabled);
    assert!(config.otlp.is_none());
    assert_eq!(config.env_filter, "info,lattice=debug");
}

#[test]
fn telemetry_config_can_enable_otlp_endpoint() {
    let config =
        TelemetryConfig::fmt_only("1.2.3").with_otlp_endpoint("http://otel-collector:4317");

    assert_eq!(
        config.otlp.unwrap().endpoint.as_deref(),
        Some("http://otel-collector:4317")
    );
}

#[test]
fn telemetry_resource_maps_to_service_attributes() {
    let telemetry = LatticeTelemetry::from_config(
        service_kind!("World"),
        InstanceId::new("world-a"),
        TelemetryConfig::fmt_only("1.2.3"),
    );

    assert_eq!(telemetry.resource.service_kind.as_str(), "World");
    assert_eq!(telemetry.resource.instance_id.as_str(), "world-a");
    assert_eq!(telemetry.resource.service_version, "1.2.3");
}
