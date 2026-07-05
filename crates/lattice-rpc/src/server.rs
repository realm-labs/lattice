use crate::RouteTarget;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegisteredRpcService {
    pub name: String,
    pub target: RouteTarget,
}

#[derive(Debug, Default)]
pub struct RpcServerBuilder {
    services: Vec<RegisteredRpcService>,
}

impl RpcServerBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_service(
        &mut self,
        name: impl Into<String>,
        target: RouteTarget,
    ) -> Result<(), RpcServerBuildError> {
        let name = name.into();
        if self.services.iter().any(|service| service.name == name) {
            return Err(RpcServerBuildError::DuplicateService { name });
        }
        self.services.push(RegisteredRpcService { name, target });
        Ok(())
    }

    pub fn services(&self) -> &[RegisteredRpcService] {
        &self.services
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum RpcServerBuildError {
    #[error("duplicate rpc service registration {name}")]
    DuplicateService { name: String },
}
