use crate::context::ServiceBuildContext;
use crate::error::LatticeServiceError;

use lattice_placement::error::PlacementError;
use lattice_placement::ownership::OwnershipPlacement;
use lattice_placement::sharding::VirtualShardMapper;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcServicePlacement {
    ExplicitFenced,
    VirtualShardFenced { mapper: VirtualShardMapper },
    SingletonFenced,
    StaticLocalUnfenced,
}

impl RpcServicePlacement {
    pub fn virtual_shards(shard_count: u32) -> Result<Self, PlacementError> {
        Ok(Self::VirtualShardFenced {
            mapper: VirtualShardMapper::new(shard_count)?,
        })
    }

    pub fn ownership_placement(self) -> Option<OwnershipPlacement> {
        match self {
            Self::ExplicitFenced => Some(OwnershipPlacement::Explicit),
            Self::VirtualShardFenced { mapper } => {
                Some(OwnershipPlacement::VirtualShard { mapper })
            }
            Self::SingletonFenced => Some(OwnershipPlacement::Singleton),
            Self::StaticLocalUnfenced => None,
        }
    }
}

pub trait RpcServiceBinding: Send + Sync + 'static {
    fn service_name(&self) -> &'static str;
    fn ingress_placement(&self) -> RpcServicePlacement;
    fn register(
        self: Box<Self>,
        context: &mut ServiceBuildContext,
    ) -> Result<(), LatticeServiceError>;
}

#[cfg(test)]
mod tests {
    use lattice_core::id::RouteKey;

    use super::*;

    #[test]
    fn rpc_service_placement_requires_valid_virtual_shard_count() {
        assert_eq!(
            RpcServicePlacement::virtual_shards(0),
            Err(PlacementError::InvalidShardCount)
        );

        let placement = RpcServicePlacement::virtual_shards(16).unwrap();
        let RpcServicePlacement::VirtualShardFenced { mapper } = placement else {
            panic!("expected virtual-shard placement");
        };
        assert_eq!(mapper.shard_count(), 16);
        assert!(mapper.shard_for_route_key(&RouteKey::U64(7)).0 < 16);
    }

    #[test]
    fn rpc_service_placement_exposes_only_fenced_modes_as_ownership_placements() {
        assert_eq!(
            RpcServicePlacement::ExplicitFenced.ownership_placement(),
            Some(OwnershipPlacement::Explicit)
        );
        assert_eq!(
            RpcServicePlacement::SingletonFenced.ownership_placement(),
            Some(OwnershipPlacement::Singleton)
        );
        assert_eq!(
            RpcServicePlacement::StaticLocalUnfenced.ownership_placement(),
            None
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcClientPlacement {
    Actor,
    Singleton,
}

pub trait RpcClientBinding: Send + Sync + 'static {
    type Core: lattice_rpc::traits::ShardedRpcCore + Clone + Send + Sync + 'static;
    type Client: Send + Sync + 'static;

    const SERVICE_KIND: &'static str;

    fn build_client(core: Self::Core) -> Self::Client;

    fn placement() -> RpcClientPlacement {
        RpcClientPlacement::Actor
    }

    fn build_default_core(
        _resolver: lattice_placement::routing::resolver::BoxRouteResolver,
        _context_factory: lattice_rpc::metadata::RpcClientContextFactory,
        _retry_policy: lattice_placement::routing::rpc::RpcRetryPolicy,
        _transport_security: lattice_rpc::security::RpcTransportSecurity,
        _transport_config: lattice_rpc::client::TonicEndpointChannelPoolConfig,
    ) -> Option<Self::Core> {
        None
    }
}

pub(crate) trait ErasedRpcClientBinding: Send + Sync + 'static {
    fn service_kind(&self) -> lattice_core::kind::ServiceKind;
    fn core_type(&self) -> &'static str;
    fn placement(&self) -> RpcClientPlacement;

    fn register(
        self: Box<Self>,
        service_context: &mut lattice_core::service_context::ServiceContextBuilder,
        default_resolver: Option<lattice_placement::routing::resolver::BoxRouteResolver>,
        context_factory: lattice_rpc::metadata::RpcClientContextFactory,
        retry_policy: lattice_placement::routing::rpc::RpcRetryPolicy,
        transport_security: lattice_rpc::security::RpcTransportSecurity,
        transport_config: lattice_rpc::client::TonicEndpointChannelPoolConfig,
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
    fn service_kind(&self) -> lattice_core::kind::ServiceKind {
        lattice_core::kind::ServiceKind::from_static(B::SERVICE_KIND)
    }

    fn core_type(&self) -> &'static str {
        std::any::type_name::<B::Core>()
    }

    fn placement(&self) -> RpcClientPlacement {
        B::placement()
    }

    fn register(
        self: Box<Self>,
        service_context: &mut lattice_core::service_context::ServiceContextBuilder,
        default_resolver: Option<lattice_placement::routing::resolver::BoxRouteResolver>,
        context_factory: lattice_rpc::metadata::RpcClientContextFactory,
        retry_policy: lattice_placement::routing::rpc::RpcRetryPolicy,
        transport_security: lattice_rpc::security::RpcTransportSecurity,
        transport_config: lattice_rpc::client::TonicEndpointChannelPoolConfig,
    ) -> Result<(), LatticeServiceError> {
        let service_kind = self.service_kind();
        let core = service_context
            .extension::<B::Core>()
            .map(|core| (*core).clone())
            .or_else(|| {
                default_resolver
                    .map(|resolver| {
                        B::build_default_core(
                            resolver,
                            context_factory,
                            retry_policy,
                            transport_security,
                            transport_config,
                        )
                    })
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
