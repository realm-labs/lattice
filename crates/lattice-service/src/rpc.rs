use crate::LatticeServiceError;
use crate::context::ServiceBuildContext;

pub trait RpcServiceBinding: Send + Sync + 'static {
    fn service_name(&self) -> &'static str;
    fn register(
        self: Box<Self>,
        context: &mut ServiceBuildContext,
    ) -> Result<(), LatticeServiceError>;
}

pub trait RpcClientBinding: Send + Sync + 'static {
    const SERVICE_KIND: &'static str;
}
