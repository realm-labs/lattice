use async_trait::async_trait;
use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::kind::{ActorKind, ServiceKind};
use tonic::{Request, Response, Status};

use crate::control::proto::logic_control_client::LogicControlClient;
use crate::coordination::actor::{
    ActivateActorRequest as CoordinatorActivateActorRequest, PlacementCoordinator,
};
use crate::coordination::logic::{
    LogicControl as CoordinatorLogicControl, VirtualShardMigrationControl,
};
use crate::coordination::reports::{
    PrepareVirtualShardMigrationRequest, VirtualShardMigrationOutcome,
};
use crate::coordination::singleton::{
    ActivateSingletonRequest as CoordinatorActivateSingletonRequest, SingletonControl,
    SingletonCoordinator,
};
use crate::error::PlacementError;
use crate::registry::InstanceRecord;
use crate::sharding::VirtualShardId;
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, PlacementState, PlacementStore, SingletonKey,
    SingletonPlacementRecord,
};

pub mod proto {
    tonic::include_proto!("lattice.placement.control");
}

#[async_trait]
pub trait LogicControlHandler: Clone + Send + Sync + 'static {
    async fn activate_actor(
        &self,
        key: ActorPlacementKey,
        epoch: Epoch,
    ) -> Result<(), PlacementError>;
    async fn activate_singleton(
        &self,
        key: SingletonKey,
        epoch: Epoch,
    ) -> Result<(), PlacementError>;
    async fn prepare_virtual_shard_migration(
        &self,
        request: PrepareVirtualShardMigrationRequest,
    ) -> Result<VirtualShardMigrationOutcome, PlacementError>;
}

#[derive(Debug, Clone)]
pub struct LogicControlService<H> {
    handler: H,
}

#[derive(Debug, Clone)]
pub struct PlacementCoordinatorService<S, L> {
    coordinator: PlacementCoordinator<S, L>,
    singleton_coordinator: SingletonCoordinator<S, L>,
}

impl<S, L> PlacementCoordinatorService<S, L> {
    pub fn new(coordinator: PlacementCoordinator<S, L>) -> Self
    where
        S: Clone,
        L: Clone,
    {
        let (store, logic) = coordinator.parts();
        Self {
            coordinator,
            singleton_coordinator: SingletonCoordinator::from_store(store, logic),
        }
    }

    pub fn with_singleton_coordinator(
        coordinator: PlacementCoordinator<S, L>,
        singleton_coordinator: SingletonCoordinator<S, L>,
    ) -> Self {
        Self {
            coordinator,
            singleton_coordinator,
        }
    }
}

#[async_trait]
impl<S, L> proto::placement_coordinator_server::PlacementCoordinator
    for PlacementCoordinatorService<S, L>
where
    S: PlacementStore,
    L: CoordinatorLogicControl + SingletonControl,
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

    async fn activate_singleton(
        &self,
        request: Request<proto::ActivateSingletonRequest>,
    ) -> Result<Response<proto::SingletonPlacementReply>, Status> {
        let request = request.into_inner();
        let record = self
            .singleton_coordinator
            .activate_singleton(CoordinatorActivateSingletonRequest {
                service_kind: ServiceKind::new(request.service_kind),
                singleton_kind: ActorKind::new(request.singleton_kind),
                scope: request.scope,
            })
            .await
            .map_err(status_from_placement)?;
        Ok(Response::new(singleton_placement_to_proto(record)))
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
            service_kind: ServiceKind::new(request.service_kind),
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

    async fn activate_singleton(
        &self,
        request: Request<proto::ActivateSingletonRequest>,
    ) -> Result<Response<proto::ActivateSingletonReply>, Status> {
        let request = request.into_inner();
        let key = SingletonKey {
            service_kind: ServiceKind::new(request.service_kind),
            singleton_kind: ActorKind::new(request.singleton_kind),
            scope: request.scope,
        };
        self.handler
            .activate_singleton(key, Epoch(request.epoch))
            .await
            .map_err(status_from_placement)?;
        Ok(Response::new(proto::ActivateSingletonReply {}))
    }

    async fn prepare_virtual_shard_migration(
        &self,
        request: Request<proto::PrepareVirtualShardMigrationRequest>,
    ) -> Result<Response<proto::PrepareVirtualShardMigrationReply>, Status> {
        let request = request.into_inner();
        let outcome = self
            .handler
            .prepare_virtual_shard_migration(PrepareVirtualShardMigrationRequest {
                service_kind: ServiceKind::new(request.service_kind),
                actor_kind: ActorKind::new(request.actor_kind),
                shard_id: VirtualShardId(request.shard_id),
                shard_count: request.shard_count,
                owner_epoch: Epoch(request.owner_epoch),
            })
            .await
            .map_err(status_from_placement)?;
        Ok(Response::new(proto::PrepareVirtualShardMigrationReply {
            eligible: outcome.eligible,
            running_actors: outcome.running_actors as u64,
            passivated_actors: outcome.passivated_actors as u64,
        }))
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

#[async_trait]
impl SingletonControl for TonicLogicControl {
    async fn activate_singleton(
        &self,
        instance: &InstanceRecord,
        key: &SingletonKey,
        epoch: Epoch,
    ) -> Result<(), PlacementError> {
        let mut client = LogicControlClient::connect(instance.control_endpoint.to_string())
            .await
            .map_err(logic_error)?;
        client
            .activate_singleton(proto::ActivateSingletonRequest {
                service_kind: key.service_kind.as_str().to_string(),
                singleton_kind: key.singleton_kind.as_str().to_string(),
                scope: key.scope.clone(),
                epoch: epoch.0,
            })
            .await
            .map_err(logic_error)?;
        Ok(())
    }
}

#[async_trait]
impl VirtualShardMigrationControl for TonicLogicControl {
    async fn prepare_virtual_shard_migration(
        &self,
        instance: &InstanceRecord,
        request: PrepareVirtualShardMigrationRequest,
    ) -> Result<VirtualShardMigrationOutcome, PlacementError> {
        let mut client = LogicControlClient::connect(instance.control_endpoint.to_string())
            .await
            .map_err(logic_error)?;
        let response = client
            .prepare_virtual_shard_migration(proto::PrepareVirtualShardMigrationRequest {
                service_kind: request.service_kind.as_str().to_string(),
                actor_kind: request.actor_kind.as_str().to_string(),
                shard_id: request.shard_id.0,
                shard_count: request.shard_count,
                owner_epoch: request.owner_epoch.0,
            })
            .await
            .map_err(logic_error)?
            .into_inner();
        Ok(VirtualShardMigrationOutcome {
            shard_id: request.shard_id,
            eligible: response.eligible,
            running_actors: response.running_actors as usize,
            passivated_actors: response.passivated_actors as usize,
        })
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

fn singleton_placement_to_proto(
    record: SingletonPlacementRecord,
) -> proto::SingletonPlacementReply {
    proto::SingletonPlacementReply {
        service_kind: record.service_kind.as_str().to_string(),
        singleton_kind: record.singleton_kind.as_str().to_string(),
        scope: record.scope,
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
        | PlacementError::ActivationLockLost
        | PlacementError::SingletonLockHeld
        | PlacementError::SingletonLockLost
        | PlacementError::CompareAndPutFailed
        | PlacementError::EpochExhausted
        | PlacementError::EpochRegression { .. }
        | PlacementError::EpochAuthorityConflict { .. }
        | PlacementError::EpochReactivation { .. }
        | PlacementError::EpochMismatch { .. }
        | PlacementError::EpochFloorCorrupt { .. }
        | PlacementError::EpochReservationMismatch
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
        | PlacementError::EpochReservationsUnsupported
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
    use lattice_core::instance::InstanceId;
    use lattice_core::service_kind;
    use std::collections::BTreeMap;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;

    use super::*;
    use crate::control::proto::placement_coordinator_client::PlacementCoordinatorClient;
    use crate::control::proto::placement_coordinator_server::PlacementCoordinatorServer;
    use crate::coordination::logic::NoopLogicControl;
    use crate::registry::{InstanceRecord, InstanceState};
    use crate::storage::memory::InMemoryPlacementStore;
    use crate::storage::{LeaseId, PlacementPrefix};

    #[tokio::test]
    async fn coordinator_rpc_activates_actor_and_returns_owner_record() {
        let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test"));
        let owner_lease = store.grant_instance_lease().await.unwrap();
        store.keepalive_instance_lease(owner_lease).await.unwrap();
        store
            .upsert_instance(instance_record_with_lease(
                "world-a",
                InstanceState::Ready,
                owner_lease,
            ))
            .await
            .unwrap();
        let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
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
        assert_eq!(response.lease_id, owner_lease.0);
        assert_eq!(store.instance_lease_keepalive_count(owner_lease), Some(1));
        assert_eq!(response.state, "running");
        assert_eq!(
            actor_id_from_proto(response.actor_id.unwrap()).unwrap(),
            ActorId::U64(7)
        );
        shutdown_tx.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn coordinator_rpc_activates_singleton_and_returns_owner_record() {
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
            .activate_singleton(proto::ActivateSingletonRequest {
                service_kind: "World".to_string(),
                singleton_kind: "SeasonManager".to_string(),
                scope: "global".to_string(),
                epoch: 0,
            })
            .await
            .unwrap()
            .into_inner();

        assert_eq!(response.service_kind, "World");
        assert_eq!(response.singleton_kind, "SeasonManager");
        assert_eq!(response.scope, "global");
        assert_eq!(response.owner_instance_id, "world-a");
        assert_eq!(response.epoch, 1);
        assert_eq!(response.lease_id, 2);
        assert_eq!(response.state, "running");
        shutdown_tx.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    fn instance_record(instance_id: &str, state: InstanceState) -> InstanceRecord {
        instance_record_with_lease(instance_id, state, LeaseId(1))
    }

    fn instance_record_with_lease(
        instance_id: &str,
        state: InstanceState,
        lease_id: LeaseId,
    ) -> InstanceRecord {
        InstanceRecord {
            service_kind: service_kind!("World"),
            instance_id: InstanceId::new(instance_id),
            lease_id,
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
