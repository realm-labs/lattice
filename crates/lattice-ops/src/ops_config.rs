use std::fmt;
use std::fs;
use std::io::Error as IoError;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::admin::{
    AdminAuth, AdminHttpAdapter, AdminMutationHandler, AdminSnapshot, DEFAULT_SNAPSHOT_LIMIT,
};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdminHttpConfig {
    #[serde(default)]
    pub bind: Option<SocketAddr>,
    /// Inline admin bearer token. Development only; deployments mount `bearer_token_file`.
    #[serde(default)]
    pub bearer_token: Option<String>,
    /// File holding the admin bearer token, typically a mounted secret.
    #[serde(default)]
    pub bearer_token_file: Option<PathBuf>,
    /// Mounts admin mutation routes without any credential; requires a loopback bind.
    #[serde(default)]
    pub allow_unauthenticated_admin: bool,
    /// Caps every unbounded collection returned by the admin snapshot route.
    #[serde(default = "default_snapshot_limit")]
    pub snapshot_limit: usize,
}

impl fmt::Debug for AdminHttpConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdminHttpConfig")
            .field("bind", &self.bind)
            .field("bearer_token_configured", &self.bearer_token.is_some())
            .field("bearer_token_file", &self.bearer_token_file)
            .field(
                "allow_unauthenticated_admin",
                &self.allow_unauthenticated_admin,
            )
            .field("snapshot_limit", &self.snapshot_limit)
            .finish()
    }
}

impl Default for AdminHttpConfig {
    fn default() -> Self {
        Self {
            bind: None,
            bearer_token: None,
            bearer_token_file: None,
            allow_unauthenticated_admin: false,
            snapshot_limit: default_snapshot_limit(),
        }
    }
}

impl AdminHttpConfig {
    pub fn validate(&self) -> Result<(), AdminConfigError> {
        if self.bearer_token.is_some() && self.bearer_token_file.is_some() {
            return Err(AdminConfigError::AmbiguousCredential);
        }
        if self.allow_unauthenticated_admin
            && (self.bearer_token.is_some() || self.bearer_token_file.is_some())
        {
            return Err(AdminConfigError::AmbiguousCredential);
        }
        if self.allow_unauthenticated_admin
            && self.bind.is_some_and(|bind| !bind.ip().is_loopback())
        {
            return Err(AdminConfigError::UnauthenticatedRemoteBind);
        }
        if self
            .bearer_token
            .as_ref()
            .is_some_and(|token| token.trim().is_empty())
        {
            return Err(AdminConfigError::EmptyCredential);
        }
        if self.snapshot_limit == 0 {
            return Err(AdminConfigError::ZeroSnapshotLimit);
        }
        Ok(())
    }

    pub fn build_auth(&self) -> Result<AdminAuth, AdminConfigError> {
        self.validate()?;
        if let Some(path) = &self.bearer_token_file {
            let token = fs::read_to_string(path)
                .map_err(|source| AdminConfigError::ReadCredentialFile {
                    path: path.clone(),
                    source,
                })?
                .trim()
                .to_owned();
            if token.is_empty() {
                return Err(AdminConfigError::EmptyCredential);
            }
            return Ok(AdminAuth::bearer_token(token));
        }
        if let Some(token) = &self.bearer_token {
            return Ok(AdminAuth::bearer_token(token.clone()));
        }
        if self.allow_unauthenticated_admin {
            return Ok(AdminAuth::allow_unauthenticated_admin());
        }
        Ok(AdminAuth::disabled())
    }

    /// Builds the admin adapter with the configured credential policy and snapshot cap.
    pub fn build_adapter<S, M>(
        &self,
        snapshot: S,
        mutations: M,
    ) -> Result<AdminHttpAdapter, AdminConfigError>
    where
        S: Fn() -> AdminSnapshot + Send + Sync + 'static,
        M: AdminMutationHandler,
    {
        Ok(
            AdminHttpAdapter::new(self.build_auth()?, snapshot, mutations)
                .with_snapshot_limit(self.snapshot_limit),
        )
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AdminConfigError {
    #[error("admin credentials must come from exactly one source")]
    AmbiguousCredential,
    #[error("admin credential is empty")]
    EmptyCredential,
    #[error("failed to read admin credential file {path}: {source}")]
    ReadCredentialFile {
        path: PathBuf,
        #[source]
        source: IoError,
    },
    #[error("unauthenticated admin mutations require a loopback bind")]
    UnauthenticatedRemoteBind,
    #[error("admin snapshot limit must be nonzero")]
    ZeroSnapshotLimit,
}

fn default_snapshot_limit() -> usize {
    DEFAULT_SNAPSHOT_LIMIT
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use axum::http::HeaderMap;

    use super::*;
    use crate::admin::{ADMIN_TOKEN_HEADER, AdminApiError, AdminSurface};

    fn headers_with_token(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(ADMIN_TOKEN_HEADER, token.parse().unwrap());
        headers
    }

    #[test]
    fn admin_http_config_builds_auth_policy() {
        let auth = AdminHttpConfig {
            bearer_token: Some("secret".to_string()),
            ..AdminHttpConfig::default()
        }
        .build_auth()
        .unwrap();

        assert!(matches!(
            auth.authorize(&HeaderMap::new(), AdminSurface::Mutation),
            Err(AdminApiError::Unauthorized)
        ));
        assert!(
            auth.authorize(&headers_with_token("secret"), AdminSurface::Mutation)
                .is_ok()
        );
    }

    #[test]
    fn admin_http_config_without_credentials_refuses_mutations() {
        let auth = AdminHttpConfig::default().build_auth().unwrap();

        assert!(
            auth.authorize(&HeaderMap::new(), AdminSurface::Inspection)
                .is_ok()
        );
        assert!(matches!(
            auth.authorize(&HeaderMap::new(), AdminSurface::Mutation),
            Err(AdminApiError::Unauthorized)
        ));

        let opt_in = AdminHttpConfig {
            allow_unauthenticated_admin: true,
            ..AdminHttpConfig::default()
        }
        .build_auth()
        .unwrap();
        assert!(
            opt_in
                .authorize(&HeaderMap::new(), AdminSurface::Mutation)
                .is_ok()
        );
    }

    #[test]
    fn admin_http_config_loads_credential_file() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        writeln!(file, "file-secret").unwrap();
        let auth = AdminHttpConfig {
            bearer_token_file: Some(file.path().to_path_buf()),
            ..AdminHttpConfig::default()
        }
        .build_auth()
        .unwrap();

        assert!(
            auth.authorize(&headers_with_token("file-secret"), AdminSurface::Mutation)
                .is_ok()
        );
        assert!(matches!(
            auth.authorize(&headers_with_token("file-secre"), AdminSurface::Mutation),
            Err(AdminApiError::Unauthorized)
        ));

        let missing = AdminHttpConfig {
            bearer_token_file: Some(file.path().with_extension("absent")),
            ..AdminHttpConfig::default()
        }
        .build_auth();
        assert!(matches!(
            missing,
            Err(AdminConfigError::ReadCredentialFile { .. })
        ));
    }

    #[test]
    fn admin_credentials_never_reach_debug_output() {
        let config = AdminHttpConfig {
            bearer_token: Some("secret".to_string()),
            ..AdminHttpConfig::default()
        };

        assert!(!format!("{config:?}").contains("secret"));
        assert!(!format!("{:?}", config.build_auth().unwrap()).contains("secret"));
    }

    #[test]
    fn admin_http_config_validation_rejects_unsafe_combinations() {
        assert!(matches!(
            AdminHttpConfig {
                bearer_token: Some("secret".to_string()),
                bearer_token_file: Some(PathBuf::from("/run/secrets/admin-token")),
                ..AdminHttpConfig::default()
            }
            .validate(),
            Err(AdminConfigError::AmbiguousCredential)
        ));
        assert!(matches!(
            AdminHttpConfig {
                bind: Some("0.0.0.0:19090".parse().unwrap()),
                allow_unauthenticated_admin: true,
                ..AdminHttpConfig::default()
            }
            .validate(),
            Err(AdminConfigError::UnauthenticatedRemoteBind)
        ));
        assert!(
            AdminHttpConfig {
                bind: Some("127.0.0.1:19090".parse().unwrap()),
                allow_unauthenticated_admin: true,
                ..AdminHttpConfig::default()
            }
            .validate()
            .is_ok()
        );
        assert!(matches!(
            AdminHttpConfig {
                bearer_token: Some("  ".to_string()),
                ..AdminHttpConfig::default()
            }
            .validate(),
            Err(AdminConfigError::EmptyCredential)
        ));
        assert!(matches!(
            AdminHttpConfig {
                snapshot_limit: 0,
                ..AdminHttpConfig::default()
            }
            .validate(),
            Err(AdminConfigError::ZeroSnapshotLimit)
        ));
    }
}
