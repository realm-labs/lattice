use async_trait::async_trait;
use lattice_core::{ActorId, ActorKind, Epoch, ServiceKind};
use tonic::{Request, Response, Status};

use crate::coordinator::LogicControl as CoordinatorLogicControl;
use crate::error::PlacementError;
use crate::instance::InstanceRecord;
use crate::store::ActorPlacementKey;

pub mod proto {
    tonic::include_proto!("lattice.placement.control");
}

pub use proto::logic_control_client::LogicControlClient;
pub use proto::logic_control_server::LogicControlServer;

#[async_trait]
pub trait LogicControlHandler: Clone + Send + Sync + 'static {
    async fn activate_actor(
        &self,
        key: ActorPlacementKey,
        epoch: Epoch,
    ) -> Result<(), PlacementError>;
}

#[derive(Debug, Clone)]
pub struct LogicControlService<H> {
    handler: H,
}

impl<H> LogicControlService<H> {
    pub fn new(handler: H) -> Self {
        Self { handler }
    }
}

#[async_trait]
impl<H> proto::logic_control_server::LogicControl for LogicControlService<H>
where
    H: LogicControlHandler,
{
    async fn activate_actor(
        &self,
        request: Request<proto::ActivateActorRequest>,
    ) -> Result<Response<proto::ActivateActorReply>, Status> {
        let request = request.into_inner();
        let key = ActorPlacementKey {
            actor_kind: ActorKind::new(request.actor_kind),
            actor_id: actor_id_from_proto(
                request
                    .actor_id
                    .ok_or_else(|| Status::invalid_argument("actor_id is required"))?,
            )?,
        };
        self.handler
            .activate_actor(key, Epoch(request.epoch))
            .await
            .map_err(status_from_placement)?;
        Ok(Response::new(proto::ActivateActorReply {}))
    }
}

#[derive(Debug, Clone, Default)]
pub struct TonicLogicControl;

#[async_trait]
impl CoordinatorLogicControl for TonicLogicControl {
    async fn activate_actor(
        &self,
        instance: &InstanceRecord,
        key: &ActorPlacementKey,
        epoch: Epoch,
    ) -> Result<(), PlacementError> {
        let mut client = LogicControlClient::connect(instance.control_endpoint.to_string())
            .await
            .map_err(logic_error)?;
        client
            .activate_actor(proto::ActivateActorRequest {
                service_kind: instance.service_kind.as_str().to_string(),
                actor_kind: key.actor_kind.as_str().to_string(),
                actor_id: Some(actor_id_to_proto(&key.actor_id)),
                epoch: epoch.0,
            })
            .await
            .map_err(logic_error)?;
        Ok(())
    }
}

pub fn actor_id_to_proto(actor_id: &ActorId) -> proto::ActorId {
    use proto::actor_id::Value;

    let value = match actor_id {
        ActorId::Str(value) => Value::StrValue(value.clone()),
        ActorId::U64(value) => Value::U64Value(*value),
        ActorId::I64(value) => Value::I64Value(*value),
        ActorId::Bytes(value) => Value::BytesValue(value.clone()),
    };
    proto::ActorId { value: Some(value) }
}

pub fn actor_id_from_proto(actor_id: proto::ActorId) -> Result<ActorId, Status> {
    use proto::actor_id::Value;

    match actor_id.value {
        Some(Value::StrValue(value)) => Ok(ActorId::Str(value)),
        Some(Value::U64Value(value)) => Ok(ActorId::U64(value)),
        Some(Value::I64Value(value)) => Ok(ActorId::I64(value)),
        Some(Value::BytesValue(value)) => Ok(ActorId::Bytes(value)),
        None => Err(Status::invalid_argument("actor_id value is required")),
    }
}

pub fn service_kind_from_request(request: &proto::ActivateActorRequest) -> ServiceKind {
    ServiceKind::new(request.service_kind.clone())
}

fn status_from_placement(error: PlacementError) -> Status {
    match error {
        PlacementError::InstanceNotFound { .. }
        | PlacementError::NoRoute
        | PlacementError::NoReadyInstances => Status::not_found(error.to_string()),
        PlacementError::InstanceNotReady { .. }
        | PlacementError::ActivationLockHeld
        | PlacementError::SingletonLockHeld
        | PlacementError::CompareAndPutFailed => Status::failed_precondition(error.to_string()),
        PlacementError::UnsupportedRouteKey | PlacementError::InvalidShardCount => {
            Status::invalid_argument(error.to_string())
        }
        PlacementError::PlacementWatchClosed
        | PlacementError::Etcd { .. }
        | PlacementError::PlacementCodec { .. }
        | PlacementError::InstanceLeaseNotFound { .. }
        | PlacementError::DuplicateAssigner { .. }
        | PlacementError::LogicControl { .. } => Status::internal(error.to_string()),
    }
}

fn logic_error(error: impl std::fmt::Display) -> PlacementError {
    PlacementError::LogicControl {
        message: error.to_string(),
    }
}
