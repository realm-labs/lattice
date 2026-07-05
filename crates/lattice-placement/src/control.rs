use async_trait::async_trait;
use lattice_core::{ActorId, ActorKind, Epoch, ServiceKind};
use tonic::{Request, Response, Status};

use crate::coordinator::{
    ActivateActorRequest as CoordinatorActivateActorRequest,
    LogicControl as CoordinatorLogicControl, PlacementCoordinator,
};
use crate::error::PlacementError;
use crate::instance::InstanceRecord;
use crate::store::{ActorPlacementKey, ActorPlacementRecord, PlacementState, PlacementStore};

pub mod proto {
    tonic::include_proto!("lattice.placement.control");
}

pub use proto::logic_control_client::LogicControlClient;
pub use proto::logic_control_server::LogicControlServer;
pub use proto::placement_coordinator_client::PlacementCoordinatorClient;
pub use proto::placement_coordinator_server::PlacementCoordinatorServer;

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

#[derive(Debug, Clone)]
pub struct PlacementCoordinatorService<S, L> {
    coordinator: PlacementCoordinator<S, L>,
}

impl<S, L> PlacementCoordinatorService<S, L> {
    pub fn new(coordinator: PlacementCoordinator<S, L>) -> Self {
        Self { coordinator }
    }
}

#[async_trait]
impl<S, L> proto::placement_coordinator_server::PlacementCoordinator
    for PlacementCoordinatorService<S, L>
where
    S: PlacementStore,
    L: CoordinatorLogicControl,
{
    async fn activate_actor(
        &self,
        request: Request<proto::ActivateActorRequest>,
    ) -> Result<Response<proto::ActorPlacementReply>, Status> {
        let request = request.into_inner();
        let record = self
            .coordinator
            .activate_actor(CoordinatorActivateActorRequest {
                service_kind: ServiceKind::new(request.service_kind),
                actor_kind: ActorKind::new(request.actor_kind),
                actor_id: actor_id_from_proto(
                    request
                        .actor_id
                        .ok_or_else(|| Status::invalid_argument("actor_id is required"))?,
                )?,
            })
            .await
            .map_err(status_from_placement)?;
        Ok(Response::new(actor_placement_to_proto(record)))
    }
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

fn actor_placement_to_proto(record: ActorPlacementRecord) -> proto::ActorPlacementReply {
    proto::ActorPlacementReply {
        actor_kind: record.actor_kind.as_str().to_string(),
        actor_id: Some(actor_id_to_proto(&record.actor_id)),
        owner_instance_id: record.owner.as_str().to_string(),
        epoch: record.epoch.0,
        lease_id: record.lease_id.0,
        state: placement_state_name(record.state).to_string(),
    }
}

fn placement_state_name(state: PlacementState) -> &'static str {
    match state {
        PlacementState::Activating => "activating",
        PlacementState::Running => "running",
        PlacementState::Draining => "draining",
        PlacementState::Migrating => "migrating",
        PlacementState::Stopped => "stopped",
    }
}

fn status_from_placement(error: PlacementError) -> Status {
    match error {
        PlacementError::InstanceNotFound { .. }
        | PlacementError::NoRoute
        | PlacementError::NoReadyInstances => Status::not_found(error.to_string()),
        PlacementError::InstanceNotReady { .. }
        | PlacementError::ActivationLockHeld
        | PlacementError::SingletonLockHeld
        | PlacementError::CompareAndPutFailed
        | PlacementError::CoordinatorLeadershipLost => {
            Status::failed_precondition(error.to_string())
        }
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

#[cfg(test)]
mod tests {
    use lattice_core::instance::InstanceCapacity;
    use lattice_core::{InstanceId, service_kind};
    use std::collections::BTreeMap;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::*;
    use crate::coordinator::NoopLogicControl;
    use crate::instance::{InstanceRecord, InstanceState};
    use crate::store::{InMemoryPlacementStore, LeaseId, PlacementPrefix};

    #[tokio::test]
    async fn coordinator_rpc_activates_actor_and_returns_owner_record() {
        let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
        store
            .upsert_instance(instance_record("world-a", InstanceState::Ready))
            .await
            .unwrap();
        let coordinator = PlacementCoordinator::new(store, NoopLogicControl);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = Server::builder()
            .add_service(PlacementCoordinatorServer::new(
                PlacementCoordinatorService::new(coordinator),
            ))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                let _ = shutdown_rx.await;
            });
        let task = tokio::spawn(server);

        let mut client = PlacementCoordinatorClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let response = client
            .activate_actor(proto::ActivateActorRequest {
                service_kind: "World".to_string(),
                actor_kind: "World".to_string(),
                actor_id: Some(actor_id_to_proto(&ActorId::U64(7))),
                epoch: 0,
            })
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.owner_instance_id, "world-a");
        assert_eq!(response.epoch, 1);
        assert_eq!(response.lease_id, 1);
        assert_eq!(response.state, "running");
        assert_eq!(
            actor_id_from_proto(response.actor_id.unwrap()).unwrap(),
            ActorId::U64(7)
        );
        shutdown_tx.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    fn instance_record(instance_id: &str, state: InstanceState) -> InstanceRecord {
        InstanceRecord {
            service_kind: service_kind!("World"),
            instance_id: InstanceId::new(instance_id),
            lease_id: LeaseId(1),
            advertised_endpoint: endpoint(instance_id, 18080),
            control_endpoint: endpoint(instance_id, 18081),
            version: "test".to_string(),
            state,
            capacity: InstanceCapacity::default(),
            labels: BTreeMap::new(),
        }
    }

    fn endpoint(instance_id: &str, port: u16) -> http::Uri {
        format!("http://{instance_id}.world:{port}")
            .parse()
            .unwrap()
    }
}
