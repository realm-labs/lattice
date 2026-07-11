use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use tonic::transport::Channel;
use tonic::{Code, Request, Status};

use crate::control::actor_id_to_proto;
use crate::control::proto;
use crate::control::proto::placement_coordinator_client::PlacementCoordinatorClient;
use crate::coordination::actor::{ActivateActorRequest, PlacementCoordinator};
use crate::coordination::logic::LogicControl;
use crate::coordination::reports::DrainReport;
use crate::coordination::singleton::{
    ActivateSingletonRequest, SingletonControl, SingletonCoordinator,
};
use crate::error::PlacementError;
use crate::storage::{
    ActorPlacementRecord, LeaseId, PlacementState, PlacementStore, SingletonPlacementRecord,
};

pub const DEFAULT_PLACEMENT_AUTHORITY_TIMEOUT: Duration = Duration::from_secs(5);
pub const MAX_PLACEMENT_AUTHORITY_TIMEOUT: Duration = Duration::from_secs(60);

/// The semantic placement mutation boundary used by ordinary runtime clients.
///
/// Implementations deliberately expose no arbitrary record, lock, epoch-floor,
/// or leadership mutation API.
#[async_trait]
pub trait PlacementAuthority: Send + Sync + 'static {
    async fn activate_actor(
        &self,
        request: ActivateActorRequest,
    ) -> Result<ActorPlacementRecord, PlacementError>;

    async fn activate_singleton(
        &self,
        request: ActivateSingletonRequest,
    ) -> Result<SingletonPlacementRecord, PlacementError>;

    async fn drain_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
        expected_lease_id: LeaseId,
    ) -> Result<DrainReport, PlacementError>;
}

/// Development-only in-process placement authority.
///
/// Production service processes must use a remote semantic authority and must
/// not receive the writable placement store held by this adapter.
#[derive(Clone)]
pub struct DevelopmentInProcessPlacementAuthority<S, L> {
    coordinator: PlacementCoordinator<S, L>,
    singleton_coordinator: SingletonCoordinator<S, L>,
}

impl<S, L> std::fmt::Debug for DevelopmentInProcessPlacementAuthority<S, L> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DevelopmentInProcessPlacementAuthority")
            .finish_non_exhaustive()
    }
}

impl<S, L> DevelopmentInProcessPlacementAuthority<S, L>
where
    S: Clone,
    L: Clone,
{
    pub fn new(store: S, logic: L) -> Self {
        Self {
            coordinator: PlacementCoordinator::new(store.clone(), logic.clone()),
            singleton_coordinator: SingletonCoordinator::from_store(store, logic),
        }
    }

    pub fn from_coordinator(coordinator: PlacementCoordinator<S, L>) -> Self {
        let (store, logic) = coordinator.parts();
        Self {
            coordinator,
            singleton_coordinator: SingletonCoordinator::from_store(store, logic),
        }
    }

    pub fn shared(self) -> Arc<dyn PlacementAuthority>
    where
        S: PlacementStore,
        L: LogicControl + SingletonControl,
    {
        Arc::new(self)
    }
}

#[async_trait]
impl<S, L> PlacementAuthority for DevelopmentInProcessPlacementAuthority<S, L>
where
    S: PlacementStore,
    L: LogicControl + SingletonControl,
{
    async fn activate_actor(
        &self,
        request: ActivateActorRequest,
    ) -> Result<ActorPlacementRecord, PlacementError> {
        self.coordinator.activate_actor(request).await
    }

    async fn activate_singleton(
        &self,
        request: ActivateSingletonRequest,
    ) -> Result<SingletonPlacementRecord, PlacementError> {
        self.singleton_coordinator.activate_singleton(request).await
    }

    async fn drain_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
        expected_lease_id: LeaseId,
    ) -> Result<DrainReport, PlacementError> {
        self.coordinator
            .drain_instance(service_kind, instance_id, expected_lease_id)
            .await
    }
}

#[derive(Clone)]
pub struct TonicPlacementAuthority {
    client: PlacementCoordinatorClient<Channel>,
    request_timeout: Duration,
}

impl std::fmt::Debug for TonicPlacementAuthority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TonicPlacementAuthority")
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl TonicPlacementAuthority {
    pub fn new(channel: Channel) -> Self {
        Self {
            client: PlacementCoordinatorClient::new(channel),
            request_timeout: DEFAULT_PLACEMENT_AUTHORITY_TIMEOUT,
        }
    }

    pub fn with_timeout(
        channel: Channel,
        request_timeout: Duration,
    ) -> Result<Self, PlacementError> {
        if request_timeout.is_zero() || request_timeout > MAX_PLACEMENT_AUTHORITY_TIMEOUT {
            return Err(PlacementError::InvalidPlacementAuthorityTimeout);
        }
        Ok(Self {
            client: PlacementCoordinatorClient::new(channel),
            request_timeout,
        })
    }

    pub const fn request_timeout(&self) -> Duration {
        self.request_timeout
    }
}

#[async_trait]
impl PlacementAuthority for TonicPlacementAuthority {
    async fn activate_actor(
        &self,
        request: ActivateActorRequest,
    ) -> Result<ActorPlacementRecord, PlacementError> {
        let expected_service = request.service_kind.clone();
        let expected_actor_kind = request.actor_kind.clone();
        let expected_actor_id = request.actor_id.clone();
        let mut rpc_request = Request::new(proto::ActivateActorRequest {
            service_kind: request.service_kind.as_str().to_string(),
            actor_kind: request.actor_kind.as_str().to_string(),
            actor_id: Some(actor_id_to_proto(&request.actor_id)),
            epoch: 0,
        });
        rpc_request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        let reply = tokio::time::timeout(self.request_timeout, client.activate_actor(rpc_request))
            .await
            .map_err(|_| PlacementError::PlacementAuthorityTimeout)?
            .map_err(authority_status)?
            .into_inner();
        decode_actor_reply(
            reply,
            &expected_service,
            &expected_actor_kind,
            &expected_actor_id,
        )
    }

    async fn activate_singleton(
        &self,
        request: ActivateSingletonRequest,
    ) -> Result<SingletonPlacementRecord, PlacementError> {
        let expected = request.clone();
        let mut rpc_request = Request::new(proto::ActivateSingletonRequest {
            service_kind: request.service_kind.as_str().to_string(),
            singleton_kind: request.singleton_kind.as_str().to_string(),
            scope: request.scope,
            epoch: 0,
        });
        rpc_request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        let reply =
            tokio::time::timeout(self.request_timeout, client.activate_singleton(rpc_request))
                .await
                .map_err(|_| PlacementError::PlacementAuthorityTimeout)?
                .map_err(authority_status)?
                .into_inner();
        decode_singleton_reply(reply, &expected)
    }

    async fn drain_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
        expected_lease_id: LeaseId,
    ) -> Result<DrainReport, PlacementError> {
        let mut rpc_request = Request::new(proto::DrainInstanceRequest {
            service_kind: service_kind.as_str().to_string(),
            instance_id: instance_id.as_str().to_string(),
            expected_lease_id: expected_lease_id.0,
        });
        rpc_request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        let reply = tokio::time::timeout(self.request_timeout, client.drain_instance(rpc_request))
            .await
            .map_err(|_| PlacementError::PlacementAuthorityTimeout)?
            .map_err(authority_status)?
            .into_inner();
        decode_drain_reply(reply, &service_kind, &instance_id, expected_lease_id)
    }
}

fn authority_status(status: Status) -> PlacementError {
    if matches!(status.code(), Code::Cancelled | Code::DeadlineExceeded) {
        PlacementError::PlacementAuthorityTimeout
    } else {
        PlacementError::PlacementAuthorityRpc {
            code: status.code(),
        }
    }
}

fn decode_actor_reply(
    reply: proto::ActorPlacementReply,
    expected_service: &ServiceKind,
    expected_actor_kind: &lattice_core::kind::ActorKind,
    expected_actor_id: &ActorId,
) -> Result<ActorPlacementRecord, PlacementError> {
    require_equal(
        reply.service_kind.as_str(),
        expected_service.as_str(),
        "service_kind",
    )?;
    require_equal(
        reply.actor_kind.as_str(),
        expected_actor_kind.as_str(),
        "actor_kind",
    )?;
    let actor_id = decode_actor_id(reply.actor_id, "actor_id")?;
    if &actor_id != expected_actor_id {
        return invalid_reply("actor_id");
    }
    let owner = decode_instance_id(reply.owner_instance_id, "owner_instance_id")?;
    let epoch = decode_epoch(reply.epoch)?;
    let lease_id = decode_lease(reply.lease_id)?;
    let state = decode_state(&reply.state)?;
    if state != PlacementState::Running {
        return invalid_reply("state");
    }
    Ok(ActorPlacementRecord {
        service_kind: expected_service.clone(),
        actor_kind: expected_actor_kind.clone(),
        actor_id,
        owner,
        epoch,
        lease_id,
        state,
    })
}

fn decode_singleton_reply(
    reply: proto::SingletonPlacementReply,
    expected: &ActivateSingletonRequest,
) -> Result<SingletonPlacementRecord, PlacementError> {
    require_equal(
        reply.service_kind.as_str(),
        expected.service_kind.as_str(),
        "service_kind",
    )?;
    require_equal(
        reply.singleton_kind.as_str(),
        expected.singleton_kind.as_str(),
        "singleton_kind",
    )?;
    require_equal(reply.scope.as_str(), expected.scope.as_str(), "scope")?;
    let owner = decode_instance_id(reply.owner_instance_id, "owner_instance_id")?;
    let epoch = decode_epoch(reply.epoch)?;
    let lease_id = decode_lease(reply.lease_id)?;
    let state = decode_state(&reply.state)?;
    if state != PlacementState::Running {
        return invalid_reply("state");
    }
    Ok(SingletonPlacementRecord {
        service_kind: expected.service_kind.clone(),
        singleton_kind: expected.singleton_kind.clone(),
        scope: expected.scope.clone(),
        owner,
        epoch,
        lease_id,
        state,
    })
}

fn decode_drain_reply(
    reply: proto::DrainInstanceReply,
    expected_service: &ServiceKind,
    expected_instance: &InstanceId,
    expected_lease_id: LeaseId,
) -> Result<DrainReport, PlacementError> {
    require_equal(
        reply.service_kind.as_str(),
        expected_service.as_str(),
        "service_kind",
    )?;
    require_equal(
        reply.drained_instance_id.as_str(),
        expected_instance.as_str(),
        "drained_instance_id",
    )?;
    if reply.drained_lease_id != expected_lease_id.0 {
        return invalid_reply("drained_lease_id");
    }
    let outcome = proto::DrainInstanceOutcome::try_from(reply.outcome)
        .map_err(|_| invalid_reply_error("outcome"))?;
    if outcome == proto::DrainInstanceOutcome::NoReadyReplacement {
        if reply.migrated_actors != 0 || reply.migrated_virtual_shards != 0 {
            return invalid_reply("outcome");
        }
        return Err(PlacementError::NoReadyInstances);
    }
    if outcome != proto::DrainInstanceOutcome::Completed {
        return invalid_reply("outcome");
    }
    let migrated_actors = usize::try_from(reply.migrated_actors)
        .map_err(|_| invalid_reply_error("migrated_actors"))?;
    let migrated_virtual_shards = usize::try_from(reply.migrated_virtual_shards)
        .map_err(|_| invalid_reply_error("migrated_virtual_shards"))?;
    Ok(DrainReport {
        drained_instance: expected_instance.clone(),
        migrated_actors,
        migrated_virtual_shards,
    })
}

fn decode_actor_id(
    actor_id: Option<proto::ActorId>,
    field: &'static str,
) -> Result<ActorId, PlacementError> {
    use proto::actor_id::Value;

    match actor_id.and_then(|actor_id| actor_id.value) {
        Some(Value::StrValue(value)) => Ok(ActorId::Str(value)),
        Some(Value::U64Value(value)) => Ok(ActorId::U64(value)),
        Some(Value::I64Value(value)) => Ok(ActorId::I64(value)),
        Some(Value::BytesValue(value)) => Ok(ActorId::Bytes(value)),
        None => invalid_reply(field),
    }
}

fn decode_instance_id(value: String, field: &'static str) -> Result<InstanceId, PlacementError> {
    if value.is_empty() || value.contains('/') {
        return invalid_reply(field);
    }
    Ok(InstanceId::new(value))
}

fn decode_epoch(value: u64) -> Result<Epoch, PlacementError> {
    if value == 0 {
        return invalid_reply("epoch");
    }
    Ok(Epoch(value))
}

fn decode_lease(value: u64) -> Result<LeaseId, PlacementError> {
    if value == 0 {
        return invalid_reply("lease_id");
    }
    Ok(LeaseId(value))
}

fn decode_state(value: &str) -> Result<PlacementState, PlacementError> {
    match value {
        "activating" => Ok(PlacementState::Activating),
        "running" => Ok(PlacementState::Running),
        "draining" => Ok(PlacementState::Draining),
        "migrating" => Ok(PlacementState::Migrating),
        "stopped" => Ok(PlacementState::Stopped),
        _ => invalid_reply("state"),
    }
}

fn require_equal(actual: &str, expected: &str, field: &'static str) -> Result<(), PlacementError> {
    if actual != expected {
        return invalid_reply(field);
    }
    Ok(())
}

fn invalid_reply<T>(field: &'static str) -> Result<T, PlacementError> {
    Err(invalid_reply_error(field))
}

fn invalid_reply_error(field: &'static str) -> PlacementError {
    PlacementError::InvalidPlacementAuthorityReply { field }
}

#[cfg(test)]
mod tests {
    use lattice_core::{actor_kind, service_kind};
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::{Response, Status};

    use super::*;

    type ActorReplyMutation = (&'static str, fn(&mut proto::ActorPlacementReply));

    #[derive(Debug, Clone)]
    struct SlowAndRejectingAuthority;

    #[tonic::async_trait]
    impl proto::placement_coordinator_server::PlacementCoordinator for SlowAndRejectingAuthority {
        async fn activate_actor(
            &self,
            _request: Request<proto::ActivateActorRequest>,
        ) -> Result<Response<proto::ActorPlacementReply>, Status> {
            tokio::time::sleep(Duration::from_millis(200)).await;
            Err(Status::internal("slow authority unexpectedly completed"))
        }

        async fn activate_singleton(
            &self,
            _request: Request<proto::ActivateSingletonRequest>,
        ) -> Result<Response<proto::SingletonPlacementReply>, Status> {
            Err(Status::permission_denied("rejected"))
        }

        async fn drain_instance(
            &self,
            _request: Request<proto::DrainInstanceRequest>,
        ) -> Result<Response<proto::DrainInstanceReply>, Status> {
            Err(Status::resource_exhausted("proxy overloaded"))
        }
    }

    #[tokio::test]
    async fn out_of_range_timeouts_are_rejected() {
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        assert_eq!(
            TonicPlacementAuthority::with_timeout(channel, Duration::ZERO).unwrap_err(),
            PlacementError::InvalidPlacementAuthorityTimeout
        );
        let channel = Channel::from_static("http://127.0.0.1:1").connect_lazy();
        assert_eq!(
            TonicPlacementAuthority::with_timeout(
                channel,
                MAX_PLACEMENT_AUTHORITY_TIMEOUT + Duration::from_nanos(1),
            )
            .unwrap_err(),
            PlacementError::InvalidPlacementAuthorityTimeout
        );
    }

    #[test]
    fn actor_reply_validation_rejects_each_authority_field() {
        let service = service_kind!("World");
        let kind = actor_kind!("World");
        let actor_id = ActorId::U64(7);
        let valid = || proto::ActorPlacementReply {
            service_kind: "World".to_string(),
            actor_kind: "World".to_string(),
            actor_id: Some(actor_id_to_proto(&actor_id)),
            owner_instance_id: "world-a".to_string(),
            epoch: 3,
            lease_id: 4,
            state: "running".to_string(),
        };

        assert!(decode_actor_reply(valid(), &service, &kind, &actor_id).is_ok());
        let mutations: [ActorReplyMutation; 6] = [
            ("service_kind", |reply: &mut proto::ActorPlacementReply| {
                reply.service_kind = "Other".to_string()
            }),
            ("actor_kind", |reply: &mut proto::ActorPlacementReply| {
                reply.actor_kind = "Other".to_string()
            }),
            (
                "owner_instance_id",
                |reply: &mut proto::ActorPlacementReply| reply.owner_instance_id.clear(),
            ),
            ("epoch", |reply: &mut proto::ActorPlacementReply| {
                reply.epoch = 0
            }),
            ("lease_id", |reply: &mut proto::ActorPlacementReply| {
                reply.lease_id = 0
            }),
            ("state", |reply: &mut proto::ActorPlacementReply| {
                reply.state = "unknown".to_string()
            }),
        ];
        for (field, mutate) in mutations {
            let mut reply = valid();
            mutate(&mut reply);
            assert_eq!(
                decode_actor_reply(reply, &service, &kind, &actor_id).unwrap_err(),
                PlacementError::InvalidPlacementAuthorityReply { field }
            );
        }
        let mut reply = valid();
        reply.actor_id = Some(actor_id_to_proto(&ActorId::U64(8)));
        assert_eq!(
            decode_actor_reply(reply, &service, &kind, &actor_id).unwrap_err(),
            PlacementError::InvalidPlacementAuthorityReply { field: "actor_id" }
        );
        let mut reply = valid();
        reply.state = "stopped".to_string();
        assert_eq!(
            decode_actor_reply(reply, &service, &kind, &actor_id).unwrap_err(),
            PlacementError::InvalidPlacementAuthorityReply { field: "state" }
        );
    }

    #[test]
    fn singleton_and_drain_reply_validation_is_fail_closed() {
        let expected = ActivateSingletonRequest {
            service_kind: service_kind!("World"),
            singleton_kind: actor_kind!("SeasonManager"),
            scope: "global".to_string(),
        };
        let singleton = proto::SingletonPlacementReply {
            service_kind: "World".to_string(),
            singleton_kind: "SeasonManager".to_string(),
            scope: "global".to_string(),
            owner_instance_id: "world-a".to_string(),
            epoch: 2,
            lease_id: 4,
            state: "running".to_string(),
        };
        assert!(decode_singleton_reply(singleton.clone(), &expected).is_ok());
        let mut invalid_singleton = singleton;
        invalid_singleton.scope = "other".to_string();
        assert_eq!(
            decode_singleton_reply(invalid_singleton, &expected).unwrap_err(),
            PlacementError::InvalidPlacementAuthorityReply { field: "scope" }
        );

        let service = service_kind!("World");
        let instance = InstanceId::new("world-a");
        let drain = proto::DrainInstanceReply {
            service_kind: "World".to_string(),
            drained_instance_id: "world-a".to_string(),
            migrated_actors: 3,
            migrated_virtual_shards: 4,
            drained_lease_id: 5,
            outcome: proto::DrainInstanceOutcome::Completed as i32,
        };
        assert_eq!(
            decode_drain_reply(drain.clone(), &service, &instance, LeaseId(5)).unwrap(),
            DrainReport {
                drained_instance: instance.clone(),
                migrated_actors: 3,
                migrated_virtual_shards: 4,
            }
        );
        let mut invalid_drain = drain.clone();
        invalid_drain.drained_instance_id = "world-b".to_string();
        assert_eq!(
            decode_drain_reply(invalid_drain, &service, &instance, LeaseId(5)).unwrap_err(),
            PlacementError::InvalidPlacementAuthorityReply {
                field: "drained_instance_id"
            }
        );
        let mut invalid_drain = drain;
        invalid_drain.drained_lease_id = 6;
        assert_eq!(
            decode_drain_reply(invalid_drain, &service, &instance, LeaseId(5)).unwrap_err(),
            PlacementError::InvalidPlacementAuthorityReply {
                field: "drained_lease_id"
            }
        );
        let no_replacement = proto::DrainInstanceReply {
            service_kind: "World".to_string(),
            drained_instance_id: "world-a".to_string(),
            migrated_actors: 0,
            migrated_virtual_shards: 0,
            drained_lease_id: 5,
            outcome: proto::DrainInstanceOutcome::NoReadyReplacement as i32,
        };
        assert_eq!(
            decode_drain_reply(no_replacement.clone(), &service, &instance, LeaseId(5))
                .unwrap_err(),
            PlacementError::NoReadyInstances
        );
        let mut invalid_no_replacement = no_replacement;
        invalid_no_replacement.migrated_actors = 1;
        assert_eq!(
            decode_drain_reply(invalid_no_replacement, &service, &instance, LeaseId(5))
                .unwrap_err(),
            PlacementError::InvalidPlacementAuthorityReply { field: "outcome" }
        );
    }

    #[tokio::test]
    async fn tonic_authority_enforces_timeout_and_maps_statuses_without_messages() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(
                    proto::placement_coordinator_server::PlacementCoordinatorServer::new(
                        SlowAndRejectingAuthority,
                    ),
                )
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                }),
        );
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let authority =
            TonicPlacementAuthority::with_timeout(channel, Duration::from_millis(20)).unwrap();

        assert_eq!(
            authority
                .activate_actor(ActivateActorRequest {
                    service_kind: service_kind!("World"),
                    actor_kind: actor_kind!("World"),
                    actor_id: ActorId::U64(7),
                })
                .await
                .unwrap_err(),
            PlacementError::PlacementAuthorityTimeout
        );
        assert_eq!(
            authority
                .activate_singleton(ActivateSingletonRequest {
                    service_kind: service_kind!("World"),
                    singleton_kind: actor_kind!("SeasonManager"),
                    scope: "global".to_string(),
                })
                .await
                .unwrap_err(),
            PlacementError::PlacementAuthorityRpc {
                code: Code::PermissionDenied
            }
        );
        assert_eq!(
            authority
                .drain_instance(
                    service_kind!("World"),
                    InstanceId::new("world-a"),
                    LeaseId(1),
                )
                .await
                .unwrap_err(),
            PlacementError::PlacementAuthorityRpc {
                code: Code::ResourceExhausted
            }
        );

        shutdown_tx.send(()).unwrap();
        server.await.unwrap().unwrap();
    }
}
