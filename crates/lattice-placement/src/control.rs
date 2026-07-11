use std::num::NonZeroUsize;

use async_trait::async_trait;
use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceCapacity;
use lattice_core::instance::InstanceIncarnation;
use lattice_core::kind::{ActorKind, ServiceKind};
use lattice_rpc::security::{
    MtlsPeerIdentityExtractor, PeerIdentity, RpcSecurityError, ServiceIdentityConfig,
};
use tonic::{Request, Response, Status};

use crate::authority::{
    MAX_PLACEMENT_SNAPSHOT_ENTRIES, MAX_SINGLETON_RENEWAL_CLAIMS, keepalive_singleton_claims,
};
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
    ActorPlacementKey, ActorPlacementRecord, OwnershipViewError, OwnershipViewRecord,
    PlacementState, PlacementStore, SingletonKey, SingletonPlacementRecord,
    VirtualShardPlacementRecord,
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
    admission: PlacementCoordinatorAdmission,
}

impl<S, L> PlacementCoordinatorService<S, L> {
    /// Builds a coordinator service that accepts only verified mTLS workload identities.
    pub fn authenticated(
        coordinator: PlacementCoordinator<S, L>,
        identity: ServiceIdentityConfig,
    ) -> Result<Self, RpcSecurityError>
    where
        S: Clone,
        L: Clone,
    {
        let (store, logic) = coordinator.parts();
        Ok(Self {
            coordinator,
            singleton_coordinator: SingletonCoordinator::from_store(store, logic),
            admission: PlacementCoordinatorAdmission::Authenticated(
                MtlsPeerIdentityExtractor::try_new(identity)?,
            ),
        })
    }

    /// Development-only plaintext coordinator admission.
    ///
    /// The peer socket must be loopback. Production deployments must use
    /// [`Self::authenticated`] together with tonic server mTLS.
    pub fn dangerously_allow_unauthenticated_loopback(
        coordinator: PlacementCoordinator<S, L>,
    ) -> Self
    where
        S: Clone,
        L: Clone,
    {
        let (store, logic) = coordinator.parts();
        Self {
            coordinator,
            singleton_coordinator: SingletonCoordinator::from_store(store, logic),
            admission: PlacementCoordinatorAdmission::DangerousLoopback,
        }
    }

    pub fn authenticated_with_singleton_coordinator(
        coordinator: PlacementCoordinator<S, L>,
        singleton_coordinator: SingletonCoordinator<S, L>,
        identity: ServiceIdentityConfig,
    ) -> Result<Self, RpcSecurityError> {
        Ok(Self {
            coordinator,
            singleton_coordinator,
            admission: PlacementCoordinatorAdmission::Authenticated(
                MtlsPeerIdentityExtractor::try_new(identity)?,
            ),
        })
    }
}

impl<S, L> PlacementCoordinatorService<S, L>
where
    S: PlacementStore,
{
    async fn authenticate_current_instance<T>(
        &self,
        request: &Request<T>,
        require_ready: bool,
    ) -> Result<Option<PeerIdentity>, Status> {
        let peer = self.admission.authenticate(request)?;
        let Some(peer) = peer else {
            return Ok(None);
        };
        let Some(incarnation) = peer.incarnation.as_ref() else {
            return Err(Status::permission_denied(
                "authenticated workload has no boot incarnation",
            ));
        };
        let record = self
            .coordinator
            .store
            .get_service_instance(&peer.service_kind, &peer.instance_id)
            .await
            .map_err(|_| Status::unavailable("workload liveness lookup failed"))?
            .filter(|record| {
                &record.incarnation == incarnation
                    && (!require_ready || record.state == crate::registry::InstanceState::Ready)
            })
            .ok_or_else(|| {
                Status::permission_denied(
                    "authenticated workload is not the current admitted instance incarnation",
                )
            })?;
        if record.lease_id.0 == 0 {
            return Err(Status::permission_denied(
                "authenticated workload has no live instance lease",
            ));
        }
        Ok(Some(peer))
    }

    async fn authenticate_current_peer<T>(
        &self,
        request: &Request<T>,
    ) -> Result<Option<PeerIdentity>, Status> {
        self.authenticate_current_instance(request, true).await
    }
}

#[derive(Debug, Clone)]
enum PlacementCoordinatorAdmission {
    Authenticated(MtlsPeerIdentityExtractor),
    DangerousLoopback,
}

impl PlacementCoordinatorAdmission {
    fn authenticate<T>(&self, request: &Request<T>) -> Result<Option<PeerIdentity>, Status> {
        match self {
            Self::Authenticated(extractor) => extractor.authenticate(request).map(Some),
            Self::DangerousLoopback => match request.remote_addr() {
                Some(address) if address.ip().is_loopback() => Ok(None),
                _ => Err(Status::unauthenticated(
                    "unauthenticated coordinator access is restricted to loopback development",
                )),
            },
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
    async fn register_instance(
        &self,
        request: Request<proto::RegisterInstanceRequest>,
    ) -> Result<Response<proto::InstanceLivenessReply>, Status> {
        let peer = self.admission.authenticate(&request)?;
        let request = request.into_inner();
        let record = registration_from_proto(request)?;
        if peer.is_some_and(|peer| {
            peer.service_kind != record.service_kind
                || peer.instance_id != record.instance_id
                || peer.incarnation.as_ref() != Some(&record.incarnation)
        }) {
            return Err(Status::permission_denied(
                "authenticated workload identity cannot register the requested instance",
            ));
        }
        let record = self
            .coordinator
            .store
            .register_instance(record)
            .await
            .map_err(status_from_placement)?;
        Ok(Response::new(instance_liveness_to_proto(&record)))
    }

    async fn keepalive_instance(
        &self,
        request: Request<proto::InstanceLivenessRequest>,
    ) -> Result<Response<proto::InstanceLivenessReply>, Status> {
        let peer = self.authenticate_current_instance(&request, false).await?;
        let (service_kind, instance_id, incarnation, lease_id) =
            liveness_identity_from_proto(request.into_inner())?;
        require_peer_instance(peer, &service_kind, &instance_id, &incarnation)?;
        let record = self
            .coordinator
            .store
            .get_service_instance(&service_kind, &instance_id)
            .await
            .map_err(status_from_placement)?
            .ok_or_else(|| Status::not_found("instance is not registered"))?;
        if record.incarnation != incarnation || record.lease_id != lease_id {
            return Err(Status::failed_precondition("instance authority changed"));
        }
        self.coordinator
            .store
            .keepalive_instance_lease(lease_id)
            .await
            .map_err(status_from_placement)?;
        Ok(Response::new(instance_liveness_to_proto(&record)))
    }

    async fn transition_instance(
        &self,
        request: Request<proto::TransitionInstanceRequest>,
    ) -> Result<Response<proto::InstanceLivenessReply>, Status> {
        let peer = self.authenticate_current_instance(&request, false).await?;
        let request = request.into_inner();
        let state = match request.state.as_str() {
            "Ready" => crate::registry::InstanceState::Ready,
            "Stopping" => crate::registry::InstanceState::Stopping,
            _ => return Err(Status::invalid_argument("unsupported instance transition")),
        };
        let (service_kind, instance_id, incarnation, lease_id) = liveness_identity_from_fields(
            request.service_kind,
            request.instance_id,
            request.instance_incarnation,
            request.expected_lease_id,
        )?;
        require_peer_instance(peer, &service_kind, &instance_id, &incarnation)?;
        let record = self
            .coordinator
            .store
            .compare_and_set_instance_state(
                &service_kind,
                &instance_id,
                &incarnation,
                lease_id,
                state,
            )
            .await
            .map_err(status_from_placement)?;
        Ok(Response::new(instance_liveness_to_proto(&record)))
    }

    async fn keepalive_singletons(
        &self,
        request: Request<proto::KeepaliveSingletonsRequest>,
    ) -> Result<Response<proto::KeepaliveSingletonsReply>, Status> {
        let peer = self.authenticate_current_peer(&request).await?;
        let request = request.into_inner();
        if request.claims.len() > MAX_SINGLETON_RENEWAL_CLAIMS {
            return Err(Status::resource_exhausted(
                "singleton renewal batch exceeds its bound",
            ));
        }
        let service_kind = ServiceKind::new(request.service_kind);
        let instance_id = lattice_core::instance::InstanceId::new(request.instance_id);
        let incarnation = instance_incarnation_from_proto(request.instance_incarnation)?;
        if !canonical_identity_segment(service_kind.as_str())
            || !canonical_identity_segment(instance_id.as_str())
        {
            return Err(Status::invalid_argument(
                "instance identity is not canonical",
            ));
        }
        require_peer_instance(peer, &service_kind, &instance_id, &incarnation)?;
        let claims = request
            .claims
            .into_iter()
            .map(singleton_claim_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        let renewed = keepalive_singleton_claims(
            &self.coordinator.store,
            &service_kind,
            &instance_id,
            &incarnation,
            claims,
        )
        .await
        .map_err(status_from_placement)?;
        Ok(Response::new(proto::KeepaliveSingletonsReply {
            service_kind: service_kind.as_str().to_string(),
            instance_id: instance_id.as_str().to_string(),
            instance_incarnation: incarnation.as_str().to_string(),
            renewed: u64::try_from(renewed)
                .map_err(|_| Status::internal("singleton renewal count exceeds protocol range"))?,
        }))
    }

    async fn activate_actor(
        &self,
        request: Request<proto::ActivateActorRequest>,
    ) -> Result<Response<proto::ActorPlacementReply>, Status> {
        self.authenticate_current_peer(&request).await?;
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
        self.authenticate_current_peer(&request).await?;
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

    async fn get_instance(
        &self,
        request: Request<proto::GetInstanceRequest>,
    ) -> Result<Response<proto::GetInstanceReply>, Status> {
        self.authenticate_current_peer(&request).await?;
        let request = request.into_inner();
        if !canonical_identity_segment(&request.instance_id) {
            return Err(Status::invalid_argument(
                "instance identity is not canonical",
            ));
        }
        let record = self
            .coordinator
            .store
            .get_instance(&lattice_core::instance::InstanceId::new(
                request.instance_id,
            ))
            .await
            .map_err(status_from_placement)?;
        Ok(Response::new(proto::GetInstanceReply {
            record: record.as_ref().map(instance_record_to_proto).transpose()?,
        }))
    }

    async fn get_actor(
        &self,
        request: Request<proto::GetActorRequest>,
    ) -> Result<Response<proto::GetActorReply>, Status> {
        self.authenticate_current_peer(&request).await?;
        let request = request.into_inner();
        if !canonical_identity_segment(&request.service_kind)
            || !canonical_identity_segment(&request.actor_kind)
        {
            return Err(Status::invalid_argument("actor identity is not canonical"));
        }
        let actor_id = actor_id_from_proto(
            request
                .actor_id
                .ok_or_else(|| Status::invalid_argument("actor_id is required"))?,
        )?;
        let actor_id_bytes = match &actor_id {
            ActorId::Str(value) => value.len(),
            ActorId::Bytes(value) => value.len(),
            ActorId::U64(_) | ActorId::I64(_) => 8,
        };
        if actor_id_bytes > 4_096 {
            return Err(Status::invalid_argument("actor identity exceeds its bound"));
        }
        let key = ActorPlacementKey {
            service_kind: ServiceKind::new(request.service_kind),
            actor_kind: ActorKind::new(request.actor_kind),
            actor_id,
        };
        let record = self
            .coordinator
            .store
            .get_actor(&key)
            .await
            .map_err(status_from_placement)?
            .map(|(version, record)| proto::VersionedActorPlacement {
                version: version.modification_revision(),
                placement: Some(actor_placement_to_proto(record)),
            });
        Ok(Response::new(proto::GetActorReply { record }))
    }

    async fn get_singleton(
        &self,
        request: Request<proto::GetSingletonRequest>,
    ) -> Result<Response<proto::GetSingletonReply>, Status> {
        const MAX_SCOPE_BYTES: usize = 256;
        self.authenticate_current_peer(&request).await?;
        let request = request.into_inner();
        if !canonical_identity_segment(&request.service_kind)
            || !canonical_identity_segment(&request.singleton_kind)
            || request.scope.is_empty()
            || request.scope.len() > MAX_SCOPE_BYTES
        {
            return Err(Status::invalid_argument(
                "singleton identity is not canonical",
            ));
        }
        let key = SingletonKey {
            service_kind: ServiceKind::new(request.service_kind),
            singleton_kind: ActorKind::new(request.singleton_kind),
            scope: request.scope,
        };
        let record = self
            .coordinator
            .store
            .get_singleton(&key)
            .await
            .map_err(status_from_placement)?
            .map(|(version, record)| proto::VersionedSingletonPlacement {
                version: version.modification_revision(),
                placement: Some(singleton_placement_to_proto(record)),
            });
        Ok(Response::new(proto::GetSingletonReply { record }))
    }

    async fn get_service_placement_snapshot(
        &self,
        request: Request<proto::GetServicePlacementSnapshotRequest>,
    ) -> Result<Response<proto::GetServicePlacementSnapshotReply>, Status> {
        self.authenticate_current_peer(&request).await?;
        let request = request.into_inner();
        if !canonical_identity_segment(&request.service_kind)
            || !canonical_identity_segment(&request.instance_id)
        {
            return Err(Status::invalid_argument(
                "snapshot identity is not canonical",
            ));
        }
        let max_entries = usize::try_from(request.max_entries)
            .ok()
            .and_then(NonZeroUsize::new)
            .filter(|limit| limit.get() <= MAX_PLACEMENT_SNAPSHOT_ENTRIES)
            .ok_or_else(|| Status::invalid_argument("snapshot entry limit is out of range"))?;
        let view = self
            .coordinator
            .store
            .open_ownership_view(
                &ServiceKind::new(request.service_kind),
                &lattice_core::instance::InstanceId::new(request.instance_id),
                max_entries,
            )
            .await
            .map_err(snapshot_status)?;
        let records = view
            .snapshot
            .records
            .into_iter()
            .map(service_snapshot_record_to_proto)
            .collect::<Vec<_>>();
        if records.len() > max_entries.get() {
            return Err(Status::resource_exhausted(
                "snapshot exceeded its entry limit",
            ));
        }
        Ok(Response::new(proto::GetServicePlacementSnapshotReply {
            revision: view.snapshot.revision.0,
            local_instance: view
                .snapshot
                .local_instance
                .as_ref()
                .map(instance_record_to_proto)
                .transpose()?,
            records,
        }))
    }

    async fn drain_instance(
        &self,
        request: Request<proto::DrainInstanceRequest>,
    ) -> Result<Response<proto::DrainInstanceReply>, Status> {
        let peer = self.authenticate_current_peer(&request).await?;
        let request = request.into_inner();
        if request.expected_lease_id == 0 {
            return Err(Status::invalid_argument("expected_lease_id is required"));
        }
        let service_kind = ServiceKind::new(request.service_kind);
        let instance_id = lattice_core::instance::InstanceId::new(request.instance_id);
        let instance_incarnation = instance_incarnation_from_proto(request.instance_incarnation)?;
        if peer.is_some_and(|peer| {
            peer.service_kind != service_kind
                || peer.instance_id != instance_id
                || peer.incarnation.as_ref() != Some(&instance_incarnation)
        }) {
            return Err(Status::permission_denied(
                "authenticated workload identity cannot drain the requested instance",
            ));
        }
        let expected_lease_id = crate::storage::LeaseId(request.expected_lease_id);
        let report = self
            .coordinator
            .drain_instance(
                service_kind.clone(),
                instance_id.clone(),
                instance_incarnation.clone(),
                expected_lease_id,
            )
            .await;
        let reply = match report {
            Ok(report) => drain_report_to_proto(service_kind.clone(), expected_lease_id, report)?,
            Err(PlacementError::NoReadyInstances) => proto::DrainInstanceReply {
                service_kind: service_kind.as_str().to_string(),
                drained_instance_id: instance_id.as_str().to_string(),
                drained_instance_incarnation: instance_incarnation.as_str().to_string(),
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
        drained_instance_incarnation: report.drained_incarnation.as_str().to_string(),
        migrated_actors: u64::try_from(report.migrated_actors)
            .map_err(|_| Status::internal("drain actor count exceeds protocol range"))?,
        migrated_virtual_shards: u64::try_from(report.migrated_virtual_shards)
            .map_err(|_| Status::internal("drain virtual-shard count exceeds protocol range"))?,
        drained_lease_id: drained_lease_id.0,
        outcome: proto::DrainInstanceOutcome::Completed as i32,
    })
}

fn registration_from_proto(
    request: proto::RegisterInstanceRequest,
) -> Result<crate::registry::InstanceRecord, Status> {
    const MAX_ENDPOINT_BYTES: usize = 2_048;
    const MAX_VERSION_BYTES: usize = 256;
    const MAX_LABELS: usize = 64;
    const MAX_LABEL_KEY_BYTES: usize = 128;
    const MAX_LABEL_VALUE_BYTES: usize = 1_024;
    let service_kind = ServiceKind::new(request.service_kind);
    let instance_id = lattice_core::instance::InstanceId::new(request.instance_id);
    let incarnation = instance_incarnation_from_proto(request.instance_incarnation)?;
    if !canonical_identity_segment(service_kind.as_str())
        || !canonical_identity_segment(instance_id.as_str())
        || request.advertised_endpoint.len() > MAX_ENDPOINT_BYTES
        || request.control_endpoint.len() > MAX_ENDPOINT_BYTES
        || request.version.is_empty()
        || request.version.len() > MAX_VERSION_BYTES
        || request.labels.len() > MAX_LABELS
        || request.labels.iter().any(|(key, value)| {
            key.is_empty() || key.len() > MAX_LABEL_KEY_BYTES || value.len() > MAX_LABEL_VALUE_BYTES
        })
    {
        return Err(Status::invalid_argument(
            "instance registration metadata exceeds its bounds",
        ));
    }
    let advertised_endpoint = request
        .advertised_endpoint
        .parse()
        .map_err(|_| Status::invalid_argument("advertised endpoint is invalid"))?;
    let control_endpoint = request
        .control_endpoint
        .parse()
        .map_err(|_| Status::invalid_argument("control endpoint is invalid"))?;
    Ok(crate::registry::InstanceRecord {
        service_kind,
        instance_id,
        incarnation,
        lease_id: crate::storage::LeaseId(0),
        advertised_endpoint,
        control_endpoint,
        version: request.version,
        state: crate::registry::InstanceState::Starting,
        capacity: InstanceCapacity {
            max_actors: request.max_actors,
            max_connections: request.max_connections,
        },
        labels: request.labels.into_iter().collect(),
    })
}

fn liveness_identity_from_proto(
    request: proto::InstanceLivenessRequest,
) -> Result<
    (
        ServiceKind,
        lattice_core::instance::InstanceId,
        InstanceIncarnation,
        crate::storage::LeaseId,
    ),
    Status,
> {
    liveness_identity_from_fields(
        request.service_kind,
        request.instance_id,
        request.instance_incarnation,
        request.expected_lease_id,
    )
}

fn liveness_identity_from_fields(
    service_kind: String,
    instance_id: String,
    incarnation: String,
    lease_id: u64,
) -> Result<
    (
        ServiceKind,
        lattice_core::instance::InstanceId,
        InstanceIncarnation,
        crate::storage::LeaseId,
    ),
    Status,
> {
    if lease_id == 0 {
        return Err(Status::invalid_argument("expected_lease_id is required"));
    }
    if !canonical_identity_segment(&service_kind) || !canonical_identity_segment(&instance_id) {
        return Err(Status::invalid_argument(
            "instance identity is not canonical",
        ));
    }
    Ok((
        ServiceKind::new(service_kind),
        lattice_core::instance::InstanceId::new(instance_id),
        instance_incarnation_from_proto(incarnation)?,
        crate::storage::LeaseId(lease_id),
    ))
}

fn canonical_identity_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

fn require_peer_instance(
    peer: Option<PeerIdentity>,
    service_kind: &ServiceKind,
    instance_id: &lattice_core::instance::InstanceId,
    incarnation: &InstanceIncarnation,
) -> Result<(), Status> {
    if peer.is_some_and(|peer| {
        &peer.service_kind != service_kind
            || &peer.instance_id != instance_id
            || peer.incarnation.as_ref() != Some(incarnation)
    }) {
        return Err(Status::permission_denied(
            "authenticated workload identity cannot manage the requested instance",
        ));
    }
    Ok(())
}

fn instance_liveness_to_proto(
    record: &crate::registry::InstanceRecord,
) -> proto::InstanceLivenessReply {
    proto::InstanceLivenessReply {
        service_kind: record.service_kind.as_str().to_string(),
        instance_id: record.instance_id.as_str().to_string(),
        instance_incarnation: record.incarnation.as_str().to_string(),
        lease_id: record.lease_id.0,
        state: format!("{:?}", record.state),
    }
}

fn instance_record_to_proto(
    record: &crate::registry::InstanceRecord,
) -> Result<proto::InstanceRecord, Status> {
    const MAX_ENDPOINT_BYTES: usize = 2_048;
    const MAX_VERSION_BYTES: usize = 256;
    const MAX_LABELS: usize = 64;
    const MAX_LABEL_KEY_BYTES: usize = 128;
    const MAX_LABEL_VALUE_BYTES: usize = 1_024;
    let advertised_endpoint = record.advertised_endpoint.to_string();
    let control_endpoint = record.control_endpoint.to_string();
    if advertised_endpoint.len() > MAX_ENDPOINT_BYTES
        || control_endpoint.len() > MAX_ENDPOINT_BYTES
        || record.version.is_empty()
        || record.version.len() > MAX_VERSION_BYTES
        || record.labels.len() > MAX_LABELS
        || record.labels.iter().any(|(key, value)| {
            key.is_empty() || key.len() > MAX_LABEL_KEY_BYTES || value.len() > MAX_LABEL_VALUE_BYTES
        })
    {
        return Err(Status::data_loss(
            "stored instance metadata exceeds proxy bounds",
        ));
    }
    Ok(proto::InstanceRecord {
        service_kind: record.service_kind.as_str().to_string(),
        instance_id: record.instance_id.as_str().to_string(),
        instance_incarnation: record.incarnation.as_str().to_string(),
        lease_id: record.lease_id.0,
        advertised_endpoint,
        control_endpoint,
        version: record.version.clone(),
        state: format!("{:?}", record.state),
        max_actors: record.capacity.max_actors,
        max_connections: record.capacity.max_connections,
        labels: record.labels.clone().into_iter().collect(),
    })
}

fn instance_incarnation_from_proto(value: String) -> Result<InstanceIncarnation, Status> {
    let incarnation = InstanceIncarnation::new(value);
    if !incarnation.is_canonical() {
        return Err(Status::invalid_argument(
            "instance_incarnation is required and must be one canonical path segment",
        ));
    }
    Ok(incarnation)
}

fn singleton_placement_to_proto(
    record: SingletonPlacementRecord,
) -> proto::SingletonPlacementReply {
    proto::SingletonPlacementReply {
        service_kind: record.service_kind.as_str().to_string(),
        singleton_kind: record.singleton_kind.as_str().to_string(),
        scope: record.scope,
        owner_instance_id: record.owner.as_str().to_string(),
        owner_incarnation: record.owner_incarnation.as_str().to_string(),
        epoch: record.epoch.0,
        lease_id: record.lease_id.0,
        state: placement_state_name(record.state).to_string(),
    }
}

fn service_snapshot_record_to_proto(record: OwnershipViewRecord) -> proto::ServicePlacementRecord {
    use proto::service_placement_record::Record;

    match record {
        OwnershipViewRecord::Actor {
            revision, record, ..
        } => proto::ServicePlacementRecord {
            revision: revision.0,
            record: Some(Record::Actor(actor_placement_to_proto(record))),
        },
        OwnershipViewRecord::VirtualShard {
            revision, record, ..
        } => proto::ServicePlacementRecord {
            revision: revision.0,
            record: Some(Record::VirtualShard(virtual_shard_placement_to_proto(
                record,
            ))),
        },
        OwnershipViewRecord::Singleton {
            revision, record, ..
        } => proto::ServicePlacementRecord {
            revision: revision.0,
            record: Some(Record::Singleton(singleton_placement_to_proto(record))),
        },
    }
}

fn virtual_shard_placement_to_proto(
    record: VirtualShardPlacementRecord,
) -> proto::VirtualShardPlacement {
    proto::VirtualShardPlacement {
        service_kind: record.service_kind.as_str().to_string(),
        actor_kind: record.actor_kind.as_str().to_string(),
        shard_id: record.shard_id.0,
        owner_instance_id: record.owner.as_str().to_string(),
        epoch: record.epoch.0,
    }
}

fn snapshot_status(error: OwnershipViewError) -> Status {
    match error {
        OwnershipViewError::CapacityExceeded { .. } => {
            Status::resource_exhausted("snapshot exceeded its entry limit")
        }
        OwnershipViewError::Unsupported => Status::unimplemented("snapshot backend is unsupported"),
        OwnershipViewError::Backend { .. } | OwnershipViewError::WatchStart { .. } => {
            Status::unavailable("snapshot backend is unavailable")
        }
        OwnershipViewError::Protocol { .. } | OwnershipViewError::Proof { .. } => {
            Status::data_loss("snapshot ownership proof is invalid")
        }
    }
}

fn singleton_claim_from_proto(
    claim: proto::SingletonLeaseClaim,
) -> Result<SingletonPlacementRecord, Status> {
    const MAX_SCOPE_BYTES: usize = 256;
    if !canonical_identity_segment(&claim.service_kind)
        || !canonical_identity_segment(&claim.singleton_kind)
        || !canonical_identity_segment(&claim.owner_instance_id)
        || claim.scope.is_empty()
        || claim.scope.len() > MAX_SCOPE_BYTES
        || claim.epoch == 0
        || claim.lease_id == 0
    {
        return Err(Status::invalid_argument(
            "singleton renewal claim is invalid or exceeds its bounds",
        ));
    }
    let owner_incarnation = instance_incarnation_from_proto(claim.owner_incarnation)?;
    Ok(SingletonPlacementRecord {
        service_kind: ServiceKind::new(claim.service_kind),
        singleton_kind: ActorKind::new(claim.singleton_kind),
        scope: claim.scope,
        owner: lattice_core::instance::InstanceId::new(claim.owner_instance_id),
        owner_incarnation,
        epoch: Epoch(claim.epoch),
        lease_id: crate::storage::LeaseId(claim.lease_id),
        state: PlacementState::Running,
    })
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
        PlacementError::InstanceAlreadyRegistered { .. } => {
            Status::already_exists("instance is already registered")
        }
        PlacementError::NoReadyInstances
        | PlacementError::SingletonRenewalLimitExceeded { .. }
        | PlacementError::PlacementReadLimitExceeded { .. } => {
            Status::resource_exhausted(error.to_string())
        }
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
        | PlacementError::InstanceIncarnationMismatch { .. }
        | PlacementError::InvalidInstanceStateTransition { .. }
        | PlacementError::InvalidSingletonRenewalClaim
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
                PlacementCoordinatorService::dangerously_allow_unauthenticated_loopback(
                    coordinator,
                ),
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
        assert_eq!(
            raw_client
                .get_actor(proto::GetActorRequest {
                    service_kind: "World".to_string(),
                    actor_kind: "World".to_string(),
                    actor_id: Some(actor_id_to_proto(&ActorId::Bytes(vec![0; 4_097]))),
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let mut corrupt = instance_record_with_lease("world-a", InstanceState::Ready, owner_lease);
        corrupt.version = "x".repeat(257);
        store.upsert_instance(corrupt).await.unwrap();
        assert_eq!(
            raw_client
                .get_instance(proto::GetInstanceRequest {
                    instance_id: "world-a".to_string(),
                })
                .await
                .unwrap_err()
                .code(),
            tonic::Code::DataLoss
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
                PlacementCoordinatorService::dangerously_allow_unauthenticated_loopback(
                    coordinator,
                ),
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
                    PlacementCoordinatorService::dangerously_allow_unauthenticated_loopback(
                        coordinator,
                    ),
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
                    InstanceIncarnation::new("world-a-boot"),
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
                    InstanceIncarnation::new("world-a-boot"),
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
                    InstanceIncarnation::new("stale-world-a-boot"),
                    LeaseId(1),
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
                    InstanceIncarnation::new("world-a-boot"),
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
                    PlacementCoordinatorService::dangerously_allow_unauthenticated_loopback(
                        coordinator,
                    ),
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
                InstanceIncarnation::new("world-a-boot"),
                LeaseId(1),
            )
            .await
            .unwrap();

        assert_eq!(
            report,
            DrainReport {
                drained_instance: InstanceId::new("world-a"),
                drained_incarnation: InstanceIncarnation::new("world-a-boot"),
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
            incarnation: InstanceIncarnation::new(format!("{instance_id}-boot")),
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
