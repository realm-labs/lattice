use lattice_core::{InstanceId, ServiceKind};
use serde::{Deserialize, Serialize};

use crate::{AdminAuth, InMemoryTelemetryExporter, OpenTelemetryPipeline, TelemetryResource};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryConfig {
    pub service_version: String,
}

impl TelemetryConfig {
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminHttpConfig {
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
    use crate::AdminApiError;

    #[test]
    fn admin_http_config_builds_auth_policy() {
        let auth = AdminHttpConfig {
            bearer_token: Some("secret".to_string()),
        }
        .build_auth();
        let mut headers = HeaderMap::new();

        assert_eq!(auth.authorize(&headers), Err(AdminApiError::Unauthorized));
        headers.insert("x-lattice-admin-token", "secret".parse().unwrap());
        assert_eq!(auth.authorize(&headers), Ok(()));
    }
}
