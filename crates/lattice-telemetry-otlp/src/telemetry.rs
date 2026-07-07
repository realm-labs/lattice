use std::time::Duration;

use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::config::{OtlpTraceConfig, TelemetryConfig};
use crate::error::TelemetryInitError;
use crate::guard::TelemetryGuard;
use crate::resource::TelemetryResource;

#[derive(Debug, Clone)]
pub struct LatticeTelemetry {
    pub(crate) resource: TelemetryResource,
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
