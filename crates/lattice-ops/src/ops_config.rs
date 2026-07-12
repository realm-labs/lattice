use std::net::SocketAddr;

use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use serde::{Deserialize, Serialize};

use crate::admin::AdminAuth;
use crate::telemetry::{InMemoryTelemetryExporter, OpenTelemetryPipeline, TelemetryResource};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryConfig {
    pub service_version: String,
    #[serde(default = "default_env_filter")]
    pub env_filter: String,
    #[serde(default = "default_fmt_enabled")]
    pub fmt_enabled: bool,
    #[serde(default)]
    pub otlp_endpoint: Option<String>,
    #[serde(default = "default_otlp_timeout_millis")]
    pub otlp_timeout_millis: u64,
    #[serde(default)]
    pub sample_ratio: Option<f64>,
}

impl TelemetryConfig {
    pub fn new(service_version: impl Into<String>) -> Self {
        Self {
            service_version: service_version.into(),
            env_filter: default_env_filter(),
            fmt_enabled: true,
            otlp_endpoint: None,
            otlp_timeout_millis: default_otlp_timeout_millis(),
            sample_ratio: None,
        }
    }

    pub fn with_otlp_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.otlp_endpoint = Some(endpoint.into());
        self
    }

    pub fn build_in_memory_pipeline(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
        exporter: InMemoryTelemetryExporter,
    ) -> OpenTelemetryPipeline<InMemoryTelemetryExporter> {
        OpenTelemetryPipeline::new(
            TelemetryResource {
                service_kind,
                instance_id,
                service_version: self.service_version.clone(),
            },
            exporter,
        )
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminHttpConfig {
    #[serde(default)]
    pub bind: Option<SocketAddr>,
    #[serde(default)]
    pub bearer_token: Option<String>,
}

impl AdminHttpConfig {
    pub fn build_auth(&self) -> AdminAuth {
        match &self.bearer_token {
            Some(token) => AdminAuth::bearer_token(token.clone()),
            None => AdminAuth::disabled(),
        }
    }
}

#[cfg(test)]
mod tests {
    use axum::http::HeaderMap;

    use super::*;
    use crate::admin::AdminApiError;

    #[test]
    fn admin_http_config_builds_auth_policy() {
        let auth = AdminHttpConfig {
            bind: None,
            bearer_token: Some("secret".to_string()),
        }
        .build_auth();
        let mut headers = HeaderMap::new();

        assert!(matches!(
            auth.authorize(&headers),
            Err(AdminApiError::Unauthorized)
        ));
        headers.insert("x-lattice-admin-token", "secret".parse().unwrap());
        assert!(auth.authorize(&headers).is_ok());
    }

    #[test]
    fn telemetry_config_defaults_to_fmt_and_optional_otlp() {
        let config = TelemetryConfig::new("1.2.3").with_otlp_endpoint("http://otel-collector:4317");

        assert_eq!(config.service_version, "1.2.3");
        assert_eq!(config.env_filter, "info,lattice=debug");
        assert!(config.fmt_enabled);
        assert_eq!(
            config.otlp_endpoint.as_deref(),
            Some("http://otel-collector:4317")
        );
        assert_eq!(config.otlp_timeout_millis, 10_000);
    }
}
