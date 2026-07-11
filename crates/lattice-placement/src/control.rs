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
    DrainReport, PrepareVirtualShardMigrationRequest, VirtualShardMigrationOutcome,
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
        if request.epoch != 0 {
            return Err(Status::invalid_argument(
                "coordinator activation requests must not supply a target epoch",
            ));
        }
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
        if request.epoch != 0 {
            return Err(Status::invalid_argument(
                "coordinator activation requests must not supply a target epoch",
            ));
        }
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

    async fn drain_instance(
        &self,
        request: Request<proto::DrainInstanceRequest>,
    ) -> Result<Response<proto::DrainInstanceReply>, Status> {
        let request = request.into_inner();
        if request.expected_lease_id == 0 {
            return Err(Status::invalid_argument("expected_lease_id is required"));
        }
        let service_kind = ServiceKind::new(request.service_kind);
        let instance_id = lattice_core::instance::InstanceId::new(request.instance_id);
        let expected_lease_id = crate::storage::LeaseId(request.expected_lease_id);
        let report = self
            .coordinator
            .drain_instance(service_kind.clone(), instance_id.clone(), expected_lease_id)
            .await;
        let reply = match report {
            Ok(report) => drain_report_to_proto(service_kind.clone(), expected_lease_id, report)?,
            Err(PlacementError::NoReadyInstances) => proto::DrainInstanceReply {
                service_kind: service_kind.as_str().to_string(),
                drained_instance_id: instance_id.as_str().to_string(),
                migrated_actors: 0,
                migrated_virtual_shards: 0,
                drained_lease_id: expected_lease_id.0,
                outcome: proto::DrainInstanceOutcome::NoReadyReplacement as i32,
            },
            Err(error) => return Err(status_from_placement(error)),
        };
        Ok(Response::new(reply))
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
        service_kind: record.service_kind.as_str().to_string(),
        actor_kind: record.actor_kind.as_str().to_string(),
        actor_id: Some(actor_id_to_proto(&record.actor_id)),
        owner_instance_id: record.owner.as_str().to_string(),
        epoch: record.epoch.0,
        lease_id: record.lease_id.0,
        state: placement_state_name(record.state).to_string(),
    }
}

fn drain_report_to_proto(
    service_kind: ServiceKind,
    drained_lease_id: crate::storage::LeaseId,
    report: DrainReport,
) -> Result<proto::DrainInstanceReply, Status> {
    Ok(proto::DrainInstanceReply {
        service_kind: service_kind.as_str().to_string(),
        drained_instance_id: report.drained_instance.as_str().to_string(),
        migrated_actors: u64::try_from(report.migrated_actors)
            .map_err(|_| Status::internal("drain actor count exceeds protocol range"))?,
        migrated_virtual_shards: u64::try_from(report.migrated_virtual_shards)
            .map_err(|_| Status::internal("drain virtual-shard count exceeds protocol range"))?,
        drained_lease_id: drained_lease_id.0,
        outcome: proto::DrainInstanceOutcome::Completed as i32,
    })
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
        PlacementError::InstanceNotFound { .. } | PlacementError::NoRoute => {
            Status::not_found(error.to_string())
        }
        PlacementError::NoReadyInstances => Status::resource_exhausted(error.to_string()),
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
        | PlacementError::EpochFloorUnproven { .. }
        | PlacementError::EpochReservationMismatch
        | PlacementError::InstanceLeaseMismatch { .. }
        | PlacementError::CoordinatorLeadershipLost => {
            Status::failed_precondition(error.to_string())
        }
        PlacementError::UnsupportedRouteKey | PlacementError::InvalidShardCount => {
            Status::invalid_argument(error.to_string())
        }
        PlacementError::PlacementWatchClosed
        | PlacementError::Etcd { .. }
        | PlacementError::InvalidEtcdAuthentication
        | PlacementError::EtcdPasswordFile { .. }
        | PlacementError::EtcdTlsCaFile { .. }
        | PlacementError::AuthenticatedEtcdConnect
        | PlacementError::EtcdAuthenticationFailed
        | PlacementError::InvalidEtcdEndpoint
        | PlacementError::InsecureEtcdAuthenticationTransport
        | PlacementError::InsecureEtcdUnauthenticatedTransport
        | PlacementError::EtcdEndpointUserinfoUnsupported
        | PlacementError::PlacementCodec { .. }
        | PlacementError::InstanceLeaseNotFound { .. }
        | PlacementError::DuplicateAssigner { .. }
        | PlacementError::EpochReservationsUnsupported
        | PlacementError::LogicControl { .. }
        | PlacementError::InvalidPlacementAuthorityTimeout
        | PlacementError::PlacementAuthorityTimeout
        | PlacementError::PlacementAuthorityRpc { .. }
        | PlacementError::InvalidPlacementAuthorityReply { .. } => {
            Status::internal(error.to_string())
        }
    }
}

fn logic_error(error: impl std::fmt::Display) -> PlacementError {
    PlacementError::LogicControl {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use crate::authority::{PlacementAuthority, TonicPlacementAuthority};
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
    use crate::storage::{
        LeaseId, PlacementPrefix, VirtualShardPlacementKey, VirtualShardPlacementRecord,
    };

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

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut raw_client = PlacementCoordinatorClient::new(channel.clone());
        let client = TonicPlacementAuthority::new(channel);
        let response = client
            .activate_actor(CoordinatorActivateActorRequest {
                service_kind: service_kind!("World"),
                actor_kind: ActorKind::new("World"),
                actor_id: ActorId::U64(7),
            })
            .await
            .unwrap();

        assert_eq!(response.service_kind, service_kind!("World"));
        assert_eq!(response.owner, InstanceId::new("world-a"));
        assert_eq!(response.epoch, Epoch(1));
        assert_eq!(response.lease_id, owner_lease);
        assert_eq!(store.instance_lease_keepalive_count(owner_lease), Some(1));
        assert_eq!(response.state, PlacementState::Running);
        assert_eq!(response.actor_id, ActorId::U64(7));
        assert_eq!(
            raw_client
                .activate_actor(proto::ActivateActorRequest {
                    service_kind: "World".to_string(),
                    actor_kind: "World".to_string(),
                    actor_id: Some(actor_id_to_proto(&ActorId::U64(8))),
                    epoch: 99,
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            raw_client
                .activate_singleton(proto::ActivateSingletonRequest {
                    service_kind: "World".to_string(),
                    singleton_kind: "SeasonManager".to_string(),
                    scope: "global".to_string(),
                    epoch: 99,
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
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

        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let client = TonicPlacementAuthority::new(channel);
        let response = client
            .activate_singleton(CoordinatorActivateSingletonRequest {
                service_kind: service_kind!("World"),
                singleton_kind: ActorKind::new("SeasonManager"),
                scope: "global".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(response.service_kind, service_kind!("World"));
        assert_eq!(response.singleton_kind, ActorKind::new("SeasonManager"));
        assert_eq!(response.scope, "global");
        assert_eq!(response.owner, InstanceId::new("world-a"));
        assert_eq!(response.epoch, Epoch(1));
        assert_eq!(response.lease_id, LeaseId(2));
        assert_eq!(response.state, PlacementState::Running);
        shutdown_tx.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn coordinator_rpc_drain_maps_no_replacement_for_graceful_shutdown() {
        let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test-drain"));
        store
            .upsert_instance(instance_record("world-a", InstanceState::Ready))
            .await
            .unwrap();
        let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(
            Server::builder()
                .add_service(PlacementCoordinatorServer::new(
                    PlacementCoordinatorService::new(coordinator),
                ))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                }),
        );
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let client = TonicPlacementAuthority::new(channel);

        assert_eq!(
            client
                .drain_instance(
                    service_kind!("Other"),
                    InstanceId::new("world-a"),
                    LeaseId(1),
                )
                .await
                .unwrap_err(),
            PlacementError::PlacementAuthorityRpc {
                code: tonic::Code::NotFound
            }
        );
        assert_eq!(
            store
                .get_instance(&InstanceId::new("world-a"))
                .await
                .unwrap()
                .unwrap()
                .state,
            InstanceState::Ready
        );

        assert_eq!(
            client
                .drain_instance(
                    service_kind!("World"),
                    InstanceId::new("world-a"),
                    LeaseId(2),
                )
                .await
                .unwrap_err(),
            PlacementError::PlacementAuthorityRpc {
                code: tonic::Code::FailedPrecondition
            }
        );
        assert_eq!(
            store
                .get_instance(&InstanceId::new("world-a"))
                .await
                .unwrap()
                .unwrap()
                .state,
            InstanceState::Ready
        );

        assert_eq!(
            client
                .drain_instance(
                    service_kind!("World"),
                    InstanceId::new("world-a"),
                    LeaseId(1),
                )
                .await
                .unwrap_err(),
            PlacementError::NoReadyInstances
        );

        shutdown_tx.send(()).unwrap();
        task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn coordinator_rpc_drain_round_trips_identity_and_counts() {
        let store = InMemoryPlacementStore::new(PlacementPrefix::new("/lattice/test-drain-ok"));
        store
            .upsert_instance(instance_record("world-a", InstanceState::Ready))
            .await
            .unwrap();
        store
            .upsert_instance(instance_record("world-b", InstanceState::Ready))
            .await
            .unwrap();
        let shard_key = VirtualShardPlacementKey {
            service_kind: service_kind!("World"),
            actor_kind: ActorKind::new("World"),
            shard_id: VirtualShardId(3),
        };
        store
            .compare_and_put_virtual_shard(
                shard_key.clone(),
                None,
                VirtualShardPlacementRecord {
                    service_kind: service_kind!("World"),
                    actor_kind: ActorKind::new("World"),
                    shard_id: VirtualShardId(3),
                    owner: InstanceId::new("world-a"),
                    epoch: Epoch(1),
                },
            )
            .await
            .unwrap();
        let coordinator = PlacementCoordinator::new(store.clone(), NoopLogicControl);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(
            Server::builder()
                .add_service(PlacementCoordinatorServer::new(
                    PlacementCoordinatorService::new(coordinator),
                ))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                }),
        );
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let client = TonicPlacementAuthority::new(channel);
        client
            .activate_actor(CoordinatorActivateActorRequest {
                service_kind: service_kind!("World"),
                actor_kind: ActorKind::new("World"),
                actor_id: ActorId::U64(9),
            })
            .await
            .unwrap();

        let report = client
            .drain_instance(
                service_kind!("World"),
                InstanceId::new("world-a"),
                LeaseId(1),
            )
            .await
            .unwrap();

        assert_eq!(
            report,
            DrainReport {
                drained_instance: InstanceId::new("world-a"),
                migrated_actors: 1,
                migrated_virtual_shards: 1,
            }
        );
        let migrated_shard = store
            .get_virtual_shard(&shard_key)
            .await
            .unwrap()
            .unwrap()
            .1;
        assert_eq!(migrated_shard.owner, InstanceId::new("world-b"));
        assert_eq!(migrated_shard.epoch, Epoch(2));
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
