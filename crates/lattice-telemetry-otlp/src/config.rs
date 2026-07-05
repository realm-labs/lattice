use serde::{Deserialize, Serialize};

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

pub(crate) fn default_env_filter() -> String {
    "info,lattice=debug".to_string()
}

pub(crate) fn default_fmt_enabled() -> bool {
    true
}

pub(crate) fn default_otlp_timeout_millis() -> u64 {
    10_000
}
