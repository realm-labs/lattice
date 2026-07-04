use std::time::Duration;

use lattice_core::{InstanceId, ServiceKind};
use opentelemetry::KeyValue;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use serde::{Deserialize, Serialize};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryConfig {
    pub service_version: String,
    #[serde(default = "default_env_filter")]
    pub env_filter: String,
    #[serde(default = "default_fmt_enabled")]
    pub fmt_enabled: bool,
    #[serde(default)]
    pub otlp: Option<OtlpTraceConfig>,
}

impl TelemetryConfig {
    pub fn fmt_only(service_version: impl Into<String>) -> Self {
        Self {
            service_version: service_version.into(),
            env_filter: default_env_filter(),
            fmt_enabled: true,
            otlp: None,
        }
    }

    pub fn with_otlp_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.otlp = Some(OtlpTraceConfig {
            endpoint: Some(endpoint.into()),
            timeout_millis: default_otlp_timeout_millis(),
        });
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OtlpTraceConfig {
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default = "default_otlp_timeout_millis")]
    pub timeout_millis: u64,
}

#[derive(Debug, Clone)]
pub struct LatticeTelemetry {
    resource: TelemetryResource,
    config: TelemetryConfig,
}

impl LatticeTelemetry {
    pub fn new(resource: TelemetryResource, config: TelemetryConfig) -> Self {
        Self { resource, config }
    }

    pub fn from_config(
        service_kind: ServiceKind,
        instance_id: InstanceId,
        config: TelemetryConfig,
    ) -> Self {
        Self::new(
            TelemetryResource {
                service_kind,
                instance_id,
                service_version: config.service_version.clone(),
            },
            config,
        )
    }

    pub fn install(self) -> Result<TelemetryGuard, TelemetryInitError> {
        let env_filter = EnvFilter::try_new(&self.config.env_filter).map_err(|error| {
            TelemetryInitError::Filter {
                message: error.to_string(),
            }
        })?;

        match (&self.config.otlp, self.config.fmt_enabled) {
            (Some(otlp), true) => self.install_otlp_with_fmt(env_filter, otlp),
            (Some(otlp), false) => self.install_otlp(env_filter, otlp),
            (None, true) => {
                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(tracing_subscriber::fmt::layer().compact())
                    .try_init()
                    .map_err(|error| TelemetryInitError::Subscriber {
                        message: error.to_string(),
                    })?;
                Ok(TelemetryGuard::default())
            }
            (None, false) => {
                tracing_subscriber::registry()
                    .with(env_filter)
                    .try_init()
                    .map_err(|error| TelemetryInitError::Subscriber {
                        message: error.to_string(),
                    })?;
                Ok(TelemetryGuard::default())
            }
        }
    }

    fn install_otlp_with_fmt(
        &self,
        env_filter: EnvFilter,
        otlp: &OtlpTraceConfig,
    ) -> Result<TelemetryGuard, TelemetryInitError> {
        let provider = self.tracer_provider(otlp)?;
        let tracer = provider.tracer("lattice");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(tracing_subscriber::fmt::layer().compact())
            .with(otel_layer)
            .try_init()
            .map_err(|error| TelemetryInitError::Subscriber {
                message: error.to_string(),
            })?;

        Ok(TelemetryGuard {
            tracer_provider: Some(provider),
        })
    }

    fn install_otlp(
        &self,
        env_filter: EnvFilter,
        otlp: &OtlpTraceConfig,
    ) -> Result<TelemetryGuard, TelemetryInitError> {
        let provider = self.tracer_provider(otlp)?;
        let tracer = provider.tracer("lattice");
        let otel_layer = tracing_opentelemetry::layer().with_tracer(tracer);

        tracing_subscriber::registry()
            .with(env_filter)
            .with(otel_layer)
            .try_init()
            .map_err(|error| TelemetryInitError::Subscriber {
                message: error.to_string(),
            })?;

        Ok(TelemetryGuard {
            tracer_provider: Some(provider),
        })
    }

    fn tracer_provider(
        &self,
        otlp: &OtlpTraceConfig,
    ) -> Result<SdkTracerProvider, TelemetryInitError> {
        let mut exporter = opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_timeout(Duration::from_millis(otlp.timeout_millis));
        if let Some(endpoint) = &otlp.endpoint {
            exporter = exporter.with_endpoint(endpoint);
        }
        let exporter = exporter
            .build()
            .map_err(|error| TelemetryInitError::Exporter {
                message: error.to_string(),
            })?;

        Ok(SdkTracerProvider::builder()
            .with_resource(self.resource.to_otel_resource())
            .with_batch_exporter(exporter)
            .build())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TelemetryResource {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub service_version: String,
}

impl TelemetryResource {
    fn to_otel_resource(&self) -> Resource {
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

#[derive(Debug, Default)]
pub struct TelemetryGuard {
    tracer_provider: Option<SdkTracerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take() {
            let _ = provider.shutdown();
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum TelemetryInitError {
    #[error("invalid telemetry filter: {message}")]
    Filter { message: String },
    #[error("failed to build telemetry exporter: {message}")]
    Exporter { message: String },
    #[error("failed to install telemetry subscriber: {message}")]
    Subscriber { message: String },
}

fn default_env_filter() -> String {
    "info,lattice=debug".to_string()
}

fn default_fmt_enabled() -> bool {
    true
}

fn default_otlp_timeout_millis() -> u64 {
    10_000
}

#[cfg(test)]
mod tests {
    use lattice_core::{InstanceId, service_kind};

    use super::*;

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
}
