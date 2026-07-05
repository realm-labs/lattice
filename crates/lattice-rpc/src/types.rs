use http::Uri;
use lattice_actor::Message;
use lattice_core::{Epoch, InstanceId, ServiceKind};

use crate::{RpcContext, RpcRequest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rpc<T> {
    pub req: T,
    pub ctx: RpcContext,
}

impl<T> Message for Rpc<T>
where
    T: RpcRequest,
{
    type Reply = T::Reply;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteTarget {
    pub service_kind: ServiceKind,
    pub instance_id: InstanceId,
    pub advertised_endpoint: Uri,
    pub owner_epoch: Option<Epoch>,
}
