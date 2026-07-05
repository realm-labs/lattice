use crate::LatticeServiceError;
use crate::context::ServiceBuildContext;

pub trait RpcServiceBinding: Send + Sync + 'static {
    fn service_name(&self) -> &'static str;
    fn register(
        self: Box<Self>,
        context: &mut ServiceBuildContext,
    ) -> Result<(), LatticeServiceError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcClientPlacement {
    Actor,
    Singleton,
}

pub trait RpcClientBinding: Send + Sync + 'static {
    type Core: lattice_rpc::ShardedRpcCore + Clone + Send + Sync + 'static;
    type Client: Send + Sync + 'static;

    const SERVICE_KIND: &'static str;

    fn build_client(core: Self::Core) -> Self::Client;

    fn placement() -> RpcClientPlacement {
        RpcClientPlacement::Actor
    }

    fn build_default_core(
        _resolver: lattice_placement::BoxRouteResolver,
        _context_factory: lattice_rpc::RpcClientContextFactory,
        _retry_policy: lattice_placement::RpcRetryPolicy,
    ) -> Option<Self::Core> {
        None
    }
}

pub(crate) trait ErasedRpcClientBinding: Send + Sync + 'static {
    fn service_kind(&self) -> lattice_core::ServiceKind;
    fn core_type(&self) -> &'static str;
    fn placement(&self) -> RpcClientPlacement;

    fn register(
        self: Box<Self>,
        service_context: &mut lattice_core::ServiceContextBuilder,
        default_resolver: Option<lattice_placement::BoxRouteResolver>,
        context_factory: lattice_rpc::RpcClientContextFactory,
        retry_policy: lattice_placement::RpcRetryPolicy,
    ) -> Result<(), LatticeServiceError>;
}

pub(crate) struct RpcClientRegistration<B> {
    _binding: std::marker::PhantomData<fn() -> B>,
}

impl<B> RpcClientRegistration<B> {
    pub(crate) fn new() -> Self {
        Self {
            _binding: std::marker::PhantomData,
        }
    }
}

impl<B> ErasedRpcClientBinding for RpcClientRegistration<B>
where
    B: RpcClientBinding,
{
    fn service_kind(&self) -> lattice_core::ServiceKind {
        lattice_core::ServiceKind::from_static(B::SERVICE_KIND)
    }

    fn core_type(&self) -> &'static str {
        std::any::type_name::<B::Core>()
    }

    fn placement(&self) -> RpcClientPlacement {
        B::placement()
    }

    fn register(
        self: Box<Self>,
        service_context: &mut lattice_core::ServiceContextBuilder,
        default_resolver: Option<lattice_placement::BoxRouteResolver>,
        context_factory: lattice_rpc::RpcClientContextFactory,
        retry_policy: lattice_placement::RpcRetryPolicy,
    ) -> Result<(), LatticeServiceError> {
        let service_kind = self.service_kind();
        let core = service_context
            .extension::<B::Core>()
            .map(|core| (*core).clone())
            .or_else(|| {
                default_resolver
                    .map(|resolver| B::build_default_core(resolver, context_factory, retry_policy))
                    .unwrap_or(None)
            })
            .ok_or(LatticeServiceError::MissingRpcClientCore {
                service_kind,
                core_type: self.core_type(),
            })?;
        service_context
            .insert_extension(B::build_client(core))
            .map_err(|type_name| LatticeServiceError::DuplicateServiceExtension {
                type_name: type_name.to_string(),
            })
    }
}
