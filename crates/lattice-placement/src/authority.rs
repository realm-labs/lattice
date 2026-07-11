use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use lattice_core::actor_ref::Epoch;
use lattice_core::id::ActorId;
use lattice_core::instance::{InstanceId, InstanceIncarnation};
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
use crate::registry::{InstanceRecord, InstanceState};
use crate::routing::placement::PlacementRoutingStore;
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, EpochFloorRecord, LeaseId, OwnershipEpochFloorProof,
    OwnershipProofContext, OwnershipRecordBinding, OwnershipView, OwnershipViewRecord,
    OwnershipViewSnapshot, OwnershipWatch, OwnershipWatchBatch, OwnershipWatchError,
    OwnershipWatchEvent, OwnershipWatchMessage, OwnershipWatchUpdate, PlacementRevision,
    PlacementState, PlacementStore, PlacementVersion, PlacementWatch, PlacementWatchEvent,
    SingletonKey, SingletonPlacementRecord, VirtualShardPlacementRecord,
};

pub const DEFAULT_PLACEMENT_AUTHORITY_TIMEOUT: Duration = Duration::from_secs(5);
pub const MAX_PLACEMENT_AUTHORITY_TIMEOUT: Duration = Duration::from_secs(60);
pub const MAX_SINGLETON_RENEWAL_CLAIMS: usize = 4_096;
pub const MAX_PLACEMENT_SNAPSHOT_ENTRIES: usize = 4_096;

#[async_trait]
pub trait OwnershipViewReader: Send + Sync + 'static {
    async fn open_ownership_view(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        max_entries: std::num::NonZeroUsize,
    ) -> Result<OwnershipView, crate::storage::OwnershipViewError>;
}

#[async_trait]
impl<T> OwnershipViewReader for T
where
    T: crate::storage::PlacementReadStore,
{
    async fn open_ownership_view(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        max_entries: std::num::NonZeroUsize,
    ) -> Result<OwnershipView, crate::storage::OwnershipViewError> {
        crate::storage::PlacementReadStore::open_ownership_view(
            self,
            service_kind,
            instance_id,
            max_entries,
        )
        .await
    }
}

#[async_trait]
pub trait SingletonClaimReader: Send + Sync + 'static {
    async fn singleton_owner_lease_claims(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        instance_incarnation: &InstanceIncarnation,
    ) -> Result<Vec<SingletonPlacementRecord>, PlacementError>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceAdminPlacementSnapshot {
    pub instances: Vec<InstanceRecord>,
    pub actors: Vec<ActorPlacementRecord>,
    pub virtual_shards: Vec<VirtualShardPlacementRecord>,
    pub singletons: Vec<SingletonPlacementRecord>,
}

#[async_trait]
pub trait AdminPlacementReader: Send + Sync + 'static {
    async fn service_admin_snapshot(
        &self,
        service_kind: &ServiceKind,
        local_instance_id: &InstanceId,
    ) -> Result<ServiceAdminPlacementSnapshot, PlacementError>;
    async fn admin_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError>;
}

#[async_trait]
impl<T> SingletonClaimReader for T
where
    T: PlacementStore,
{
    async fn singleton_owner_lease_claims(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        instance_incarnation: &InstanceIncarnation,
    ) -> Result<Vec<SingletonPlacementRecord>, PlacementError> {
        let mut claims = Vec::new();
        for (_version, record) in PlacementStore::list_singletons(self).await? {
            if &record.service_kind == service_kind
                && &record.owner == instance_id
                && &record.owner_incarnation == instance_incarnation
            {
                if claims.len() == MAX_SINGLETON_RENEWAL_CLAIMS {
                    return Err(PlacementError::SingletonRenewalLimitExceeded {
                        limit: MAX_SINGLETON_RENEWAL_CLAIMS,
                    });
                }
                claims.push(record);
            }
        }
        Ok(claims)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServicePlacementSnapshot {
    pub revision: PlacementRevision,
    pub local_instance: Option<InstanceRecord>,
    pub records: Vec<ServicePlacementSnapshotRecord>,
}

impl ServicePlacementSnapshot {
    pub fn into_ownership_view_snapshot(self) -> OwnershipViewSnapshot {
        OwnershipViewSnapshot {
            revision: self.revision,
            local_instance: self.local_instance,
            records: self
                .records
                .into_iter()
                .map(|record| match record {
                    ServicePlacementSnapshotRecord::Actor {
                        revision,
                        record,
                        proof,
                    } => OwnershipViewRecord::Actor {
                        revision,
                        record,
                        proof,
                    },
                    ServicePlacementSnapshotRecord::VirtualShard {
                        revision,
                        record,
                        proof,
                    } => OwnershipViewRecord::VirtualShard {
                        revision,
                        record,
                        proof,
                    },
                    ServicePlacementSnapshotRecord::Singleton {
                        revision,
                        record,
                        proof,
                    } => OwnershipViewRecord::Singleton {
                        revision,
                        record,
                        proof,
                    },
                })
                .collect(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServicePlacementSnapshotRecord {
    Actor {
        revision: PlacementRevision,
        record: ActorPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
    VirtualShard {
        revision: PlacementRevision,
        record: VirtualShardPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
    Singleton {
        revision: PlacementRevision,
        record: SingletonPlacementRecord,
        proof: OwnershipEpochFloorProof,
    },
}

/// Bounded semantic read client for runtime placement lookups and snapshots.
///
/// This client never receives a backend handle. Watch operations are
/// intentionally absent until their bounded streaming protocol is implemented.
#[derive(Clone)]
pub struct TonicPlacementReader {
    client: PlacementCoordinatorClient<Channel>,
    request_timeout: Duration,
}

#[derive(Debug, Clone)]
pub struct TonicPlacementRoutingStore {
    reader: TonicPlacementReader,
    service_kind: ServiceKind,
    local_instance_id: InstanceId,
    max_entries: std::num::NonZeroUsize,
}

impl TonicPlacementRoutingStore {
    pub fn new(
        reader: TonicPlacementReader,
        service_kind: ServiceKind,
        local_instance_id: InstanceId,
        max_entries: std::num::NonZeroUsize,
    ) -> Result<Self, PlacementError> {
        if max_entries.get() > MAX_PLACEMENT_SNAPSHOT_ENTRIES {
            return Err(PlacementError::PlacementReadLimitExceeded {
                limit: MAX_PLACEMENT_SNAPSHOT_ENTRIES,
            });
        }
        Ok(Self {
            reader,
            service_kind,
            local_instance_id,
            max_entries,
        })
    }
}

#[async_trait]
impl PlacementRoutingStore for TonicPlacementRoutingStore {
    async fn get_routing_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError> {
        self.reader.get_instance(instance_id).await
    }

    async fn get_routing_actor(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<Option<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        self.reader.get_actor(key).await
    }

    async fn get_routing_singleton(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
        self.reader.get_singleton(key).await
    }

    async fn watch_routing(
        &self,
        service_kind: &ServiceKind,
    ) -> Result<PlacementWatch, PlacementError> {
        if service_kind != &self.service_kind {
            return Err(PlacementError::NoRoute);
        }
        let mut view = self
            .reader
            .open_ownership_view(
                &self.service_kind,
                &self.local_instance_id,
                self.max_entries,
            )
            .await
            .map_err(|error| PlacementError::LogicControl {
                message: format!("remote placement watch failed: {error}"),
            })?;
        let (tx, rx) = tokio::sync::broadcast::channel(16);
        let task = tokio::spawn(async move {
            while let Ok(batch) = view.watch.next().await {
                for event in batch.events {
                    if tx.send(routing_watch_event(event)).is_err() {
                        return;
                    }
                }
            }
        });
        Ok(PlacementWatch::new_cancellable(rx, task.abort_handle()))
    }
}

fn routing_watch_event(event: OwnershipWatchEvent) -> PlacementWatchEvent {
    match event {
        OwnershipWatchEvent::InstanceUpserted { record }
        | OwnershipWatchEvent::InstanceDeleted { record } => {
            PlacementWatchEvent::InstanceUpdated { record }
        }
        OwnershipWatchEvent::ActorUpserted { key, record, proof }
        | OwnershipWatchEvent::ActorDeleted {
            key,
            previous_record: record,
            proof,
        } => PlacementWatchEvent::ActorUpdated {
            key,
            version: proof.record_version(),
            record,
        },
        OwnershipWatchEvent::VirtualShardUpserted { key, record, proof }
        | OwnershipWatchEvent::VirtualShardDeleted {
            key,
            previous_record: record,
            proof,
        } => PlacementWatchEvent::VirtualShardUpdated {
            key,
            version: proof.record_version(),
            record,
        },
        OwnershipWatchEvent::SingletonUpserted { key, record, proof }
        | OwnershipWatchEvent::SingletonDeleted {
            key,
            previous_record: record,
            proof,
        } => PlacementWatchEvent::SingletonUpdated {
            key,
            version: proof.record_version(),
            record,
        },
    }
}

impl std::fmt::Debug for TonicPlacementReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TonicPlacementReader")
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}

impl TonicPlacementReader {
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

    pub async fn get_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError> {
        let reply = self
            .call_get_instance(proto::GetInstanceRequest {
                instance_id: instance_id.as_str().to_string(),
            })
            .await?;
        reply
            .record
            .map(|record| decode_instance_record(record, instance_id))
            .transpose()
    }

    pub async fn get_actor(
        &self,
        key: &ActorPlacementKey,
    ) -> Result<Option<(PlacementVersion, ActorPlacementRecord)>, PlacementError> {
        let reply = self
            .call_get_actor(proto::GetActorRequest {
                service_kind: key.service_kind.as_str().to_string(),
                actor_kind: key.actor_kind.as_str().to_string(),
                actor_id: Some(actor_id_to_proto(&key.actor_id)),
            })
            .await?;
        reply
            .record
            .map(|versioned| {
                let placement = versioned
                    .placement
                    .ok_or_else(|| invalid_reply_error("placement"))?;
                Ok((
                    decode_version(versioned.version)?,
                    decode_actor_reply(
                        placement,
                        &key.service_kind,
                        &key.actor_kind,
                        &key.actor_id,
                    )?,
                ))
            })
            .transpose()
    }

    pub async fn get_singleton(
        &self,
        key: &SingletonKey,
    ) -> Result<Option<(PlacementVersion, SingletonPlacementRecord)>, PlacementError> {
        let reply = self
            .call_get_singleton(proto::GetSingletonRequest {
                service_kind: key.service_kind.as_str().to_string(),
                singleton_kind: key.singleton_kind.as_str().to_string(),
                scope: key.scope.clone(),
            })
            .await?;
        reply
            .record
            .map(|versioned| {
                let placement = versioned
                    .placement
                    .ok_or_else(|| invalid_reply_error("placement"))?;
                Ok((
                    decode_version(versioned.version)?,
                    decode_singleton_reply(
                        placement,
                        &ActivateSingletonRequest {
                            service_kind: key.service_kind.clone(),
                            singleton_kind: key.singleton_kind.clone(),
                            scope: key.scope.clone(),
                        },
                    )?,
                ))
            })
            .transpose()
    }

    pub async fn get_service_placement_snapshot(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        max_entries: std::num::NonZeroUsize,
    ) -> Result<ServicePlacementSnapshot, PlacementError> {
        if max_entries.get() > MAX_PLACEMENT_SNAPSHOT_ENTRIES {
            return Err(PlacementError::PlacementReadLimitExceeded {
                limit: MAX_PLACEMENT_SNAPSHOT_ENTRIES,
            });
        }
        let mut request = Request::new(proto::GetServicePlacementSnapshotRequest {
            service_kind: service_kind.as_str().to_string(),
            instance_id: instance_id.as_str().to_string(),
            max_entries: u32::try_from(max_entries.get()).map_err(|_| {
                PlacementError::PlacementReadLimitExceeded {
                    limit: MAX_PLACEMENT_SNAPSHOT_ENTRIES,
                }
            })?,
        });
        request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        let reply = tokio::time::timeout(
            self.request_timeout,
            client.get_service_placement_snapshot(request),
        )
        .await
        .map_err(|_| PlacementError::PlacementAuthorityTimeout)?
        .map_err(authority_status)?
        .into_inner();
        decode_service_snapshot(reply, service_kind, instance_id, max_entries)
    }

    pub async fn list_service_instances(
        &self,
        service_kind: &ServiceKind,
        max_entries: std::num::NonZeroUsize,
    ) -> Result<Vec<InstanceRecord>, PlacementError> {
        if max_entries.get() > MAX_PLACEMENT_SNAPSHOT_ENTRIES {
            return Err(PlacementError::PlacementReadLimitExceeded {
                limit: MAX_PLACEMENT_SNAPSHOT_ENTRIES,
            });
        }
        let mut request = Request::new(proto::ListServiceInstancesRequest {
            service_kind: service_kind.as_str().to_string(),
            max_entries: u32::try_from(max_entries.get()).map_err(|_| {
                PlacementError::PlacementReadLimitExceeded {
                    limit: MAX_PLACEMENT_SNAPSHOT_ENTRIES,
                }
            })?,
        });
        request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        let reply =
            tokio::time::timeout(self.request_timeout, client.list_service_instances(request))
                .await
                .map_err(|_| PlacementError::PlacementAuthorityTimeout)?
                .map_err(authority_status)?
                .into_inner();
        if reply.instances.len() > max_entries.get() {
            return Err(PlacementError::PlacementReadLimitExceeded {
                limit: max_entries.get(),
            });
        }
        let mut instance_ids = HashSet::new();
        reply
            .instances
            .into_iter()
            .map(|record| {
                let expected = InstanceId::new(record.instance_id.clone());
                let record = decode_instance_record(record, &expected)?;
                if &record.service_kind != service_kind
                    || !instance_ids.insert(record.instance_id.clone())
                {
                    return invalid_reply("instances");
                }
                Ok(record)
            })
            .collect()
    }

    pub async fn open_ownership_view(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        max_entries: std::num::NonZeroUsize,
    ) -> Result<OwnershipView, crate::storage::OwnershipViewError> {
        if max_entries.get() > MAX_PLACEMENT_SNAPSHOT_ENTRIES {
            return Err(crate::storage::OwnershipViewError::CapacityExceeded {
                max_entries: MAX_PLACEMENT_SNAPSHOT_ENTRIES,
            });
        }
        let mut request = Request::new(proto::GetServicePlacementSnapshotRequest {
            service_kind: service_kind.as_str().to_string(),
            instance_id: instance_id.as_str().to_string(),
            max_entries: u32::try_from(max_entries.get()).map_err(|_| {
                crate::storage::OwnershipViewError::CapacityExceeded {
                    max_entries: MAX_PLACEMENT_SNAPSHOT_ENTRIES,
                }
            })?,
        });
        request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        let mut stream = tokio::time::timeout(
            self.request_timeout,
            client.watch_service_placement(request),
        )
        .await
        .map_err(|_| remote_view_backend("placement watch RPC timed out"))?
        .map_err(remote_view_status)?
        .into_inner();
        let first = tokio::time::timeout(self.request_timeout, stream.message())
            .await
            .map_err(|_| remote_view_backend("placement watch snapshot timed out"))?
            .map_err(remote_view_status)?
            .ok_or_else(|| remote_view_protocol("placement watch omitted its snapshot"))?;
        let snapshot = match first.update {
            Some(proto::service_placement_watch_response::Update::Snapshot(snapshot)) => {
                decode_service_snapshot(snapshot, service_kind, instance_id, max_entries)
                    .map_err(|error| remote_view_protocol(error.to_string()))?
                    .into_ownership_view_snapshot()
            }
            _ => {
                return Err(remote_view_protocol(
                    "placement watch did not start with a snapshot",
                ));
            }
        };
        let (tx, rx) = tokio::sync::broadcast::channel(16);
        let expected_service = service_kind.clone();
        let expected_instance = instance_id.clone();
        let task = tokio::spawn(async move {
            loop {
                let message = match stream.message().await {
                    Ok(Some(message)) => match decode_watch_update(
                        message,
                        &expected_service,
                        &expected_instance,
                        max_entries,
                    ) {
                        Ok(update) => OwnershipWatchMessage::Update(update),
                        Err(error) => OwnershipWatchMessage::Failed(error),
                    },
                    Ok(None) => OwnershipWatchMessage::Failed(OwnershipWatchError::Closed),
                    Err(status) => OwnershipWatchMessage::Failed(remote_watch_status(status)),
                };
                let terminal = matches!(message, OwnershipWatchMessage::Failed(_));
                if tx.send(message).is_err() || terminal {
                    return;
                }
            }
        });
        Ok(OwnershipView {
            snapshot,
            watch: OwnershipWatch::new_cancellable(rx, task.abort_handle()),
        })
    }

    async fn call_get_instance(
        &self,
        request: proto::GetInstanceRequest,
    ) -> Result<proto::GetInstanceReply, PlacementError> {
        let mut request = Request::new(request);
        request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        tokio::time::timeout(self.request_timeout, client.get_instance(request))
            .await
            .map_err(|_| PlacementError::PlacementAuthorityTimeout)?
            .map(|response| response.into_inner())
            .map_err(authority_status)
    }

    async fn call_get_actor(
        &self,
        request: proto::GetActorRequest,
    ) -> Result<proto::GetActorReply, PlacementError> {
        let mut request = Request::new(request);
        request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        tokio::time::timeout(self.request_timeout, client.get_actor(request))
            .await
            .map_err(|_| PlacementError::PlacementAuthorityTimeout)?
            .map(|response| response.into_inner())
            .map_err(authority_status)
    }

    async fn call_get_singleton(
        &self,
        request: proto::GetSingletonRequest,
    ) -> Result<proto::GetSingletonReply, PlacementError> {
        let mut request = Request::new(request);
        request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        tokio::time::timeout(self.request_timeout, client.get_singleton(request))
            .await
            .map_err(|_| PlacementError::PlacementAuthorityTimeout)?
            .map(|response| response.into_inner())
            .map_err(authority_status)
    }
}

#[async_trait]
impl OwnershipViewReader for TonicPlacementReader {
    async fn open_ownership_view(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        max_entries: std::num::NonZeroUsize,
    ) -> Result<OwnershipView, crate::storage::OwnershipViewError> {
        TonicPlacementReader::open_ownership_view(self, service_kind, instance_id, max_entries)
            .await
    }
}

#[async_trait]
impl SingletonClaimReader for TonicPlacementReader {
    async fn singleton_owner_lease_claims(
        &self,
        service_kind: &ServiceKind,
        instance_id: &InstanceId,
        instance_incarnation: &InstanceIncarnation,
    ) -> Result<Vec<SingletonPlacementRecord>, PlacementError> {
        let snapshot = self
            .get_service_placement_snapshot(
                service_kind,
                instance_id,
                std::num::NonZeroUsize::new(MAX_PLACEMENT_SNAPSHOT_ENTRIES)
                    .expect("snapshot limit is nonzero"),
            )
            .await?;
        let mut claims = Vec::new();
        for record in snapshot.records {
            let ServicePlacementSnapshotRecord::Singleton { record, .. } = record else {
                continue;
            };
            if &record.owner == instance_id && &record.owner_incarnation == instance_incarnation {
                if claims.len() == MAX_SINGLETON_RENEWAL_CLAIMS {
                    return Err(PlacementError::SingletonRenewalLimitExceeded {
                        limit: MAX_SINGLETON_RENEWAL_CLAIMS,
                    });
                }
                claims.push(record);
            }
        }
        Ok(claims)
    }
}

#[async_trait]
impl AdminPlacementReader for TonicPlacementReader {
    async fn service_admin_snapshot(
        &self,
        service_kind: &ServiceKind,
        local_instance_id: &InstanceId,
    ) -> Result<ServiceAdminPlacementSnapshot, PlacementError> {
        let max_entries = std::num::NonZeroUsize::new(MAX_PLACEMENT_SNAPSHOT_ENTRIES)
            .expect("snapshot limit is nonzero");
        let (instances, snapshot) = tokio::try_join!(
            self.list_service_instances(service_kind, max_entries),
            self.get_service_placement_snapshot(service_kind, local_instance_id, max_entries),
        )?;
        let mut actors = Vec::new();
        let mut virtual_shards = Vec::new();
        let mut singletons = Vec::new();
        for record in snapshot.records {
            match record {
                ServicePlacementSnapshotRecord::Actor { record, .. } => actors.push(record),
                ServicePlacementSnapshotRecord::VirtualShard { record, .. } => {
                    virtual_shards.push(record);
                }
                ServicePlacementSnapshotRecord::Singleton { record, .. } => {
                    singletons.push(record);
                }
            }
        }
        Ok(ServiceAdminPlacementSnapshot {
            instances,
            actors,
            virtual_shards,
            singletons,
        })
    }

    async fn admin_instance(
        &self,
        instance_id: &InstanceId,
    ) -> Result<Option<InstanceRecord>, PlacementError> {
        self.get_instance(instance_id).await
    }
}

/// The semantic placement mutation boundary used by ordinary runtime clients.
///
/// Implementations deliberately expose no arbitrary record, lock, epoch-floor,
/// or leadership mutation API.
#[async_trait]
pub trait PlacementAuthority: Send + Sync + 'static {
    async fn register_instance(
        &self,
        record: InstanceRecord,
    ) -> Result<InstanceRecord, PlacementError>;

    async fn keepalive_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
        instance_incarnation: InstanceIncarnation,
        expected_lease_id: LeaseId,
    ) -> Result<(), PlacementError>;

    async fn transition_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
        instance_incarnation: InstanceIncarnation,
        expected_lease_id: LeaseId,
        state: InstanceState,
    ) -> Result<(), PlacementError>;

    async fn keepalive_singletons(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
        instance_incarnation: InstanceIncarnation,
        claims: Vec<SingletonPlacementRecord>,
    ) -> Result<usize, PlacementError>;

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
        instance_incarnation: InstanceIncarnation,
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
    async fn register_instance(
        &self,
        record: InstanceRecord,
    ) -> Result<InstanceRecord, PlacementError> {
        self.coordinator.store.register_instance(record).await
    }

    async fn keepalive_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
        instance_incarnation: InstanceIncarnation,
        expected_lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        let record = self
            .coordinator
            .store
            .get_service_instance(&service_kind, &instance_id)
            .await?
            .ok_or_else(|| PlacementError::InstanceNotFound {
                instance_id: instance_id.clone(),
            })?;
        require_instance_authority(&record, &instance_incarnation, expected_lease_id)?;
        self.coordinator
            .store
            .keepalive_instance_lease(expected_lease_id)
            .await
    }

    async fn transition_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
        instance_incarnation: InstanceIncarnation,
        expected_lease_id: LeaseId,
        state: InstanceState,
    ) -> Result<(), PlacementError> {
        self.coordinator
            .store
            .compare_and_set_instance_state(
                &service_kind,
                &instance_id,
                &instance_incarnation,
                expected_lease_id,
                state,
            )
            .await
            .map(|_| ())
    }

    async fn keepalive_singletons(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
        instance_incarnation: InstanceIncarnation,
        claims: Vec<SingletonPlacementRecord>,
    ) -> Result<usize, PlacementError> {
        keepalive_singleton_claims(
            &self.coordinator.store,
            &service_kind,
            &instance_id,
            &instance_incarnation,
            claims,
        )
        .await
    }

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
        instance_incarnation: InstanceIncarnation,
        expected_lease_id: LeaseId,
    ) -> Result<DrainReport, PlacementError> {
        self.coordinator
            .drain_instance(
                service_kind,
                instance_id,
                instance_incarnation,
                expected_lease_id,
            )
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
    async fn register_instance(
        &self,
        record: InstanceRecord,
    ) -> Result<InstanceRecord, PlacementError> {
        if record.state != InstanceState::Starting || record.lease_id.0 != 0 {
            return Err(PlacementError::PlacementCodec {
                message: "instance registration requires Starting state and no caller lease"
                    .to_string(),
            });
        }
        let mut rpc_request = Request::new(proto::RegisterInstanceRequest {
            service_kind: record.service_kind.as_str().to_string(),
            instance_id: record.instance_id.as_str().to_string(),
            instance_incarnation: record.incarnation.as_str().to_string(),
            advertised_endpoint: record.advertised_endpoint.to_string(),
            control_endpoint: record.control_endpoint.to_string(),
            version: record.version.clone(),
            max_actors: record.capacity.max_actors,
            max_connections: record.capacity.max_connections,
            labels: record.labels.clone().into_iter().collect(),
        });
        rpc_request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        let reply =
            tokio::time::timeout(self.request_timeout, client.register_instance(rpc_request))
                .await
                .map_err(|_| PlacementError::PlacementAuthorityTimeout)?
                .map_err(authority_status)?
                .into_inner();
        let (lease_id, state) = decode_instance_liveness_reply(&reply, &record)?;
        if state != InstanceState::Starting {
            return invalid_reply("state");
        }
        Ok(InstanceRecord {
            lease_id,
            state,
            ..record
        })
    }

    async fn keepalive_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
        instance_incarnation: InstanceIncarnation,
        expected_lease_id: LeaseId,
    ) -> Result<(), PlacementError> {
        let mut request = Request::new(proto::InstanceLivenessRequest {
            service_kind: service_kind.as_str().to_string(),
            instance_id: instance_id.as_str().to_string(),
            instance_incarnation: instance_incarnation.as_str().to_string(),
            expected_lease_id: expected_lease_id.0,
        });
        request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        let reply = tokio::time::timeout(self.request_timeout, client.keepalive_instance(request))
            .await
            .map_err(|_| PlacementError::PlacementAuthorityTimeout)?
            .map_err(authority_status)?
            .into_inner();
        validate_liveness_identity(
            &reply,
            &service_kind,
            &instance_id,
            &instance_incarnation,
            expected_lease_id,
        )
    }

    async fn transition_instance(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
        instance_incarnation: InstanceIncarnation,
        expected_lease_id: LeaseId,
        state: InstanceState,
    ) -> Result<(), PlacementError> {
        let mut request = Request::new(proto::TransitionInstanceRequest {
            service_kind: service_kind.as_str().to_string(),
            instance_id: instance_id.as_str().to_string(),
            instance_incarnation: instance_incarnation.as_str().to_string(),
            expected_lease_id: expected_lease_id.0,
            state: format!("{state:?}"),
        });
        request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        let reply = tokio::time::timeout(self.request_timeout, client.transition_instance(request))
            .await
            .map_err(|_| PlacementError::PlacementAuthorityTimeout)?
            .map_err(authority_status)?
            .into_inner();
        validate_liveness_identity(
            &reply,
            &service_kind,
            &instance_id,
            &instance_incarnation,
            expected_lease_id,
        )?;
        let actual_state = decode_instance_state(&reply.state)?;
        if actual_state != state {
            return invalid_reply("state");
        }
        Ok(())
    }

    async fn keepalive_singletons(
        &self,
        service_kind: ServiceKind,
        instance_id: InstanceId,
        instance_incarnation: InstanceIncarnation,
        claims: Vec<SingletonPlacementRecord>,
    ) -> Result<usize, PlacementError> {
        if claims.len() > MAX_SINGLETON_RENEWAL_CLAIMS {
            return Err(PlacementError::SingletonRenewalLimitExceeded {
                limit: MAX_SINGLETON_RENEWAL_CLAIMS,
            });
        }
        let expected_renewed = claims.len();
        let mut request = Request::new(proto::KeepaliveSingletonsRequest {
            service_kind: service_kind.as_str().to_string(),
            instance_id: instance_id.as_str().to_string(),
            instance_incarnation: instance_incarnation.as_str().to_string(),
            claims: claims.into_iter().map(singleton_claim_to_proto).collect(),
        });
        request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        let reply =
            tokio::time::timeout(self.request_timeout, client.keepalive_singletons(request))
                .await
                .map_err(|_| PlacementError::PlacementAuthorityTimeout)?
                .map_err(authority_status)?
                .into_inner();
        require_equal(&reply.service_kind, service_kind.as_str(), "service_kind")?;
        require_equal(&reply.instance_id, instance_id.as_str(), "instance_id")?;
        require_equal(
            &reply.instance_incarnation,
            instance_incarnation.as_str(),
            "instance_incarnation",
        )?;
        let renewed = usize::try_from(reply.renewed).map_err(|_| invalid_reply_error("renewed"))?;
        if renewed != expected_renewed {
            return invalid_reply("renewed");
        }
        Ok(renewed)
    }

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
        instance_incarnation: InstanceIncarnation,
        expected_lease_id: LeaseId,
    ) -> Result<DrainReport, PlacementError> {
        let mut rpc_request = Request::new(proto::DrainInstanceRequest {
            service_kind: service_kind.as_str().to_string(),
            instance_id: instance_id.as_str().to_string(),
            expected_lease_id: expected_lease_id.0,
            instance_incarnation: instance_incarnation.as_str().to_string(),
        });
        rpc_request.set_timeout(self.request_timeout);
        let mut client = self.client.clone();
        let reply = tokio::time::timeout(self.request_timeout, client.drain_instance(rpc_request))
            .await
            .map_err(|_| PlacementError::PlacementAuthorityTimeout)?
            .map_err(authority_status)?
            .into_inner();
        decode_drain_reply(
            reply,
            &service_kind,
            &instance_id,
            &instance_incarnation,
            expected_lease_id,
        )
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

pub(crate) async fn keepalive_singleton_claims<S: PlacementStore>(
    store: &S,
    service_kind: &ServiceKind,
    instance_id: &InstanceId,
    instance_incarnation: &InstanceIncarnation,
    claims: Vec<SingletonPlacementRecord>,
) -> Result<usize, PlacementError> {
    if claims.len() > MAX_SINGLETON_RENEWAL_CLAIMS {
        return Err(PlacementError::SingletonRenewalLimitExceeded {
            limit: MAX_SINGLETON_RENEWAL_CLAIMS,
        });
    }
    let instance = store
        .get_service_instance(service_kind, instance_id)
        .await?
        .ok_or_else(|| PlacementError::InstanceNotFound {
            instance_id: instance_id.clone(),
        })?;
    require_instance_authority(&instance, instance_incarnation, instance.lease_id)?;
    if instance.state != InstanceState::Ready {
        return Err(PlacementError::InstanceNotReady {
            instance_id: instance_id.clone(),
            state: instance.state,
        });
    }

    let mut keys = HashSet::with_capacity(claims.len());
    let mut leases = HashSet::with_capacity(claims.len());
    for claim in &claims {
        let key = SingletonKey {
            service_kind: claim.service_kind.clone(),
            singleton_kind: claim.singleton_kind.clone(),
            scope: claim.scope.clone(),
        };
        if claim.service_kind != *service_kind
            || claim.owner != *instance_id
            || claim.owner_incarnation != *instance_incarnation
            || claim.state != PlacementState::Running
            || !keys.insert(key.clone())
            || !leases.insert(claim.lease_id)
        {
            return Err(PlacementError::InvalidSingletonRenewalClaim);
        }
        let current = store
            .get_singleton(&key)
            .await?
            .map(|(_, record)| record)
            .ok_or(PlacementError::InvalidSingletonRenewalClaim)?;
        if &current != claim {
            return Err(PlacementError::InvalidSingletonRenewalClaim);
        }
    }
    for lease_id in leases {
        store.keepalive_instance_lease(lease_id).await?;
    }
    Ok(claims.len())
}

fn singleton_claim_to_proto(record: SingletonPlacementRecord) -> proto::SingletonLeaseClaim {
    proto::SingletonLeaseClaim {
        service_kind: record.service_kind.as_str().to_string(),
        singleton_kind: record.singleton_kind.as_str().to_string(),
        scope: record.scope,
        owner_instance_id: record.owner.as_str().to_string(),
        owner_incarnation: record.owner_incarnation.as_str().to_string(),
        epoch: record.epoch.0,
        lease_id: record.lease_id.0,
    }
}

fn require_instance_authority(
    record: &InstanceRecord,
    expected_incarnation: &InstanceIncarnation,
    expected_lease_id: LeaseId,
) -> Result<(), PlacementError> {
    if &record.incarnation != expected_incarnation {
        return Err(PlacementError::InstanceIncarnationMismatch {
            instance_id: record.instance_id.clone(),
            expected: expected_incarnation.clone(),
            actual: record.incarnation.clone(),
        });
    }
    if record.lease_id != expected_lease_id {
        return Err(PlacementError::InstanceLeaseMismatch {
            instance_id: record.instance_id.clone(),
            expected: expected_lease_id,
            actual: record.lease_id,
        });
    }
    Ok(())
}

fn decode_instance_liveness_reply(
    reply: &proto::InstanceLivenessReply,
    expected: &InstanceRecord,
) -> Result<(LeaseId, InstanceState), PlacementError> {
    let lease_id = decode_lease(reply.lease_id)?;
    validate_liveness_identity(
        reply,
        &expected.service_kind,
        &expected.instance_id,
        &expected.incarnation,
        lease_id,
    )?;
    Ok((lease_id, decode_instance_state(&reply.state)?))
}

fn validate_liveness_identity(
    reply: &proto::InstanceLivenessReply,
    service_kind: &ServiceKind,
    instance_id: &InstanceId,
    incarnation: &InstanceIncarnation,
    lease_id: LeaseId,
) -> Result<(), PlacementError> {
    require_equal(&reply.service_kind, service_kind.as_str(), "service_kind")?;
    require_equal(&reply.instance_id, instance_id.as_str(), "instance_id")?;
    require_equal(
        &reply.instance_incarnation,
        incarnation.as_str(),
        "instance_incarnation",
    )?;
    if reply.lease_id != lease_id.0 {
        return invalid_reply("lease_id");
    }
    Ok(())
}

fn decode_instance_state(value: &str) -> Result<InstanceState, PlacementError> {
    match value {
        "Starting" => Ok(InstanceState::Starting),
        "Ready" => Ok(InstanceState::Ready),
        "Draining" => Ok(InstanceState::Draining),
        "Stopping" => Ok(InstanceState::Stopping),
        _ => invalid_reply("state"),
    }
}

fn decode_instance_record(
    record: proto::InstanceRecord,
    expected_instance_id: &InstanceId,
) -> Result<InstanceRecord, PlacementError> {
    const MAX_ENDPOINT_BYTES: usize = 2_048;
    const MAX_VERSION_BYTES: usize = 256;
    const MAX_LABELS: usize = 64;
    const MAX_LABEL_KEY_BYTES: usize = 128;
    const MAX_LABEL_VALUE_BYTES: usize = 1_024;
    let instance_id = decode_instance_id(record.instance_id, "instance_id")?;
    if &instance_id != expected_instance_id {
        return invalid_reply("instance_id");
    }
    let incarnation = InstanceIncarnation::new(record.instance_incarnation);
    if !incarnation.is_canonical() {
        return invalid_reply("instance_incarnation");
    }
    let lease_id = decode_lease(record.lease_id)?;
    let advertised_endpoint = record
        .advertised_endpoint
        .parse()
        .map_err(|_| invalid_reply_error("advertised_endpoint"))?;
    let control_endpoint = record
        .control_endpoint
        .parse()
        .map_err(|_| invalid_reply_error("control_endpoint"))?;
    if !valid_identity_segment(&record.service_kind)
        || record.advertised_endpoint.len() > MAX_ENDPOINT_BYTES
        || record.control_endpoint.len() > MAX_ENDPOINT_BYTES
        || record.version.is_empty()
        || record.version.len() > MAX_VERSION_BYTES
        || record.labels.len() > MAX_LABELS
        || record.labels.iter().any(|(key, value)| {
            key.is_empty() || key.len() > MAX_LABEL_KEY_BYTES || value.len() > MAX_LABEL_VALUE_BYTES
        })
    {
        return invalid_reply("instance_metadata");
    }
    let state = match record.state.as_str() {
        "Starting" => InstanceState::Starting,
        "Ready" => InstanceState::Ready,
        "Draining" => InstanceState::Draining,
        "Stopping" => InstanceState::Stopping,
        "Dead" => InstanceState::Dead,
        _ => return invalid_reply("state"),
    };
    Ok(InstanceRecord {
        service_kind: ServiceKind::new(record.service_kind),
        instance_id,
        incarnation,
        lease_id,
        advertised_endpoint,
        control_endpoint,
        version: record.version,
        state,
        capacity: lattice_core::instance::InstanceCapacity {
            max_actors: record.max_actors,
            max_connections: record.max_connections,
        },
        labels: record.labels.into_iter().collect(),
    })
}

fn valid_identity_segment(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value != "."
        && value != ".."
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
}

fn decode_version(value: u64) -> Result<PlacementVersion, PlacementError> {
    if value == 0 {
        return invalid_reply("version");
    }
    Ok(PlacementVersion::from_modification_revision(value))
}

fn decode_service_snapshot(
    reply: proto::GetServicePlacementSnapshotReply,
    expected_service: &ServiceKind,
    expected_instance: &InstanceId,
    max_entries: std::num::NonZeroUsize,
) -> Result<ServicePlacementSnapshot, PlacementError> {
    use proto::service_placement_record::Record;

    if reply.records.len() > max_entries.get() {
        return Err(PlacementError::PlacementReadLimitExceeded {
            limit: max_entries.get(),
        });
    }
    let snapshot_revision = PlacementRevision(reply.revision);
    let local_instance = reply
        .local_instance
        .map(|record| decode_instance_record(record, expected_instance))
        .transpose()?;
    if local_instance
        .as_ref()
        .is_some_and(|record| &record.service_kind != expected_service)
    {
        return invalid_reply("local_instance.service_kind");
    }

    let mut actor_keys = HashSet::new();
    let mut shard_keys = HashSet::new();
    let mut singleton_keys = HashSet::new();
    let mut records = Vec::with_capacity(reply.records.len());
    for entry in reply.records {
        if entry.revision == 0 || entry.revision > reply.revision {
            return invalid_reply("record.revision");
        }
        let revision = PlacementRevision(entry.revision);
        let proof = entry
            .proof
            .ok_or_else(|| invalid_reply_error("record.proof"))?;
        let record = match entry.record.ok_or_else(|| invalid_reply_error("record"))? {
            Record::Actor(record) => {
                let record = decode_snapshot_actor(record, expected_service)?;
                let key = ActorPlacementKey {
                    service_kind: record.service_kind.clone(),
                    actor_kind: record.actor_kind.clone(),
                    actor_id: record.actor_id.clone(),
                };
                if !actor_keys.insert(key) {
                    return invalid_reply("record.duplicate");
                }
                let proof = decode_snapshot_proof(
                    proof,
                    snapshot_revision,
                    revision,
                    OwnershipRecordBinding::Actor(record.clone()),
                )?;
                ServicePlacementSnapshotRecord::Actor {
                    revision,
                    record,
                    proof,
                }
            }
            Record::VirtualShard(record) => {
                let record = decode_snapshot_virtual_shard(record, expected_service)?;
                let key = crate::storage::VirtualShardPlacementKey {
                    service_kind: record.service_kind.clone(),
                    actor_kind: record.actor_kind.clone(),
                    shard_id: record.shard_id,
                };
                if !shard_keys.insert(key) {
                    return invalid_reply("record.duplicate");
                }
                let proof = decode_snapshot_proof(
                    proof,
                    snapshot_revision,
                    revision,
                    OwnershipRecordBinding::VirtualShard(record.clone()),
                )?;
                ServicePlacementSnapshotRecord::VirtualShard {
                    revision,
                    record,
                    proof,
                }
            }
            Record::Singleton(record) => {
                let record = decode_snapshot_singleton(record, expected_service)?;
                let key = SingletonKey {
                    service_kind: record.service_kind.clone(),
                    singleton_kind: record.singleton_kind.clone(),
                    scope: record.scope.clone(),
                };
                if !singleton_keys.insert(key) {
                    return invalid_reply("record.duplicate");
                }
                let proof = decode_snapshot_proof(
                    proof,
                    snapshot_revision,
                    revision,
                    OwnershipRecordBinding::Singleton(record.clone()),
                )?;
                ServicePlacementSnapshotRecord::Singleton {
                    revision,
                    record,
                    proof,
                }
            }
        };
        records.push(record);
    }
    Ok(ServicePlacementSnapshot {
        revision: snapshot_revision,
        local_instance,
        records,
    })
}

fn decode_snapshot_proof(
    proof: proto::SnapshotEpochFloorProof,
    snapshot_revision: PlacementRevision,
    record_revision: PlacementRevision,
    binding: OwnershipRecordBinding,
) -> Result<OwnershipEpochFloorProof, PlacementError> {
    decode_floor_proof(
        proof,
        proto::OwnershipProofContext::Snapshot,
        snapshot_revision,
        record_revision,
        binding,
    )
}

fn decode_floor_proof(
    proof: proto::SnapshotEpochFloorProof,
    expected_context: proto::OwnershipProofContext,
    observed_revision: PlacementRevision,
    record_revision: PlacementRevision,
    binding: OwnershipRecordBinding,
) -> Result<OwnershipEpochFloorProof, PlacementError> {
    if proof.observed_revision != observed_revision.0
        || proof.record_version != record_revision.0
        || proof.record_version == 0
        || proof.floor_version == 0
        || proof.floor_epoch == 0
        || proto::OwnershipProofContext::try_from(proof.context)
            .ok()
            .filter(|context| *context == expected_context)
            .is_none()
    {
        return invalid_reply("record.proof");
    }
    let proof = OwnershipEpochFloorProof::new(
        match expected_context {
            proto::OwnershipProofContext::Snapshot => OwnershipProofContext::Snapshot,
            proto::OwnershipProofContext::Upsert => OwnershipProofContext::Upsert,
            proto::OwnershipProofContext::Delete => OwnershipProofContext::Delete,
            proto::OwnershipProofContext::Unspecified => return invalid_reply("record.proof"),
        },
        observed_revision,
        PlacementVersion::from_modification_revision(proof.record_version),
        binding.clone(),
        PlacementVersion::from_modification_revision(proof.floor_version),
        EpochFloorRecord {
            key: binding.epoch_key(),
            epoch: Epoch(proof.floor_epoch),
        },
        None,
    )
    .map_err(|_| invalid_reply_error("record.proof"))?;
    let validation = match expected_context {
        proto::OwnershipProofContext::Snapshot => {
            proof.validate_snapshot(observed_revision, record_revision, &binding)
        }
        proto::OwnershipProofContext::Upsert => proof.validate_upsert(observed_revision, &binding),
        proto::OwnershipProofContext::Delete => proof.validate_delete(observed_revision, &binding),
        proto::OwnershipProofContext::Unspecified => return invalid_reply("record.proof"),
    };
    validation.map_err(|_| invalid_reply_error("record.proof"))?;
    Ok(proof)
}

fn decode_snapshot_actor(
    reply: proto::ActorPlacementReply,
    expected_service: &ServiceKind,
) -> Result<ActorPlacementRecord, PlacementError> {
    require_equal(
        &reply.service_kind,
        expected_service.as_str(),
        "service_kind",
    )?;
    if !valid_identity_segment(&reply.actor_kind) {
        return invalid_reply("actor_kind");
    }
    let actor_id = decode_actor_id(reply.actor_id, "actor_id")?;
    if matches!(&actor_id, ActorId::Str(value) if value.len() > 4_096)
        || matches!(&actor_id, ActorId::Bytes(value) if value.len() > 4_096)
    {
        return invalid_reply("actor_id");
    }
    Ok(ActorPlacementRecord {
        service_kind: expected_service.clone(),
        actor_kind: lattice_core::kind::ActorKind::new(reply.actor_kind),
        actor_id,
        owner: decode_instance_id(reply.owner_instance_id, "owner_instance_id")?,
        epoch: decode_epoch(reply.epoch)?,
        lease_id: decode_lease(reply.lease_id)?,
        state: decode_state(&reply.state)?,
    })
}

fn decode_snapshot_virtual_shard(
    reply: proto::VirtualShardPlacement,
    expected_service: &ServiceKind,
) -> Result<VirtualShardPlacementRecord, PlacementError> {
    require_equal(
        &reply.service_kind,
        expected_service.as_str(),
        "service_kind",
    )?;
    if !valid_identity_segment(&reply.actor_kind) {
        return invalid_reply("actor_kind");
    }
    Ok(VirtualShardPlacementRecord {
        service_kind: expected_service.clone(),
        actor_kind: lattice_core::kind::ActorKind::new(reply.actor_kind),
        shard_id: crate::sharding::VirtualShardId(reply.shard_id),
        owner: decode_instance_id(reply.owner_instance_id, "owner_instance_id")?,
        epoch: decode_epoch(reply.epoch)?,
    })
}

fn decode_snapshot_singleton(
    reply: proto::SingletonPlacementReply,
    expected_service: &ServiceKind,
) -> Result<SingletonPlacementRecord, PlacementError> {
    require_equal(
        &reply.service_kind,
        expected_service.as_str(),
        "service_kind",
    )?;
    if !valid_identity_segment(&reply.singleton_kind)
        || reply.scope.is_empty()
        || reply.scope.len() > 256
    {
        return invalid_reply("singleton_key");
    }
    let owner_incarnation = InstanceIncarnation::new(reply.owner_incarnation);
    if !owner_incarnation.is_canonical() {
        return invalid_reply("owner_incarnation");
    }
    Ok(SingletonPlacementRecord {
        service_kind: expected_service.clone(),
        singleton_kind: lattice_core::kind::ActorKind::new(reply.singleton_kind),
        scope: reply.scope,
        owner: decode_instance_id(reply.owner_instance_id, "owner_instance_id")?,
        owner_incarnation,
        epoch: decode_epoch(reply.epoch)?,
        lease_id: decode_lease(reply.lease_id)?,
        state: decode_state(&reply.state)?,
    })
}

fn decode_watch_update(
    response: proto::ServicePlacementWatchResponse,
    expected_service: &ServiceKind,
    expected_instance: &InstanceId,
    max_entries: std::num::NonZeroUsize,
) -> Result<OwnershipWatchUpdate, OwnershipWatchError> {
    use proto::service_placement_watch_response::Update;

    match response.update {
        Some(Update::ProgressRevision(revision)) if revision > 0 => {
            Ok(OwnershipWatchUpdate::Progress {
                revision: PlacementRevision(revision),
            })
        }
        Some(Update::Batch(batch)) => {
            if batch.revision == 0 {
                return Err(remote_watch_protocol("watch batch revision is invalid"));
            }
            if batch.events.len() > max_entries.get().saturating_add(1) {
                return Err(OwnershipWatchError::BatchCapacityExceeded {
                    max_events: max_entries.get().saturating_add(1),
                });
            }
            let revision = PlacementRevision(batch.revision);
            let events = batch
                .events
                .into_iter()
                .map(|event| {
                    decode_watch_event(event, expected_service, expected_instance, revision)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(OwnershipWatchUpdate::Batch(OwnershipWatchBatch {
                revision,
                events,
            }))
        }
        Some(Update::Snapshot(_)) => Err(remote_watch_protocol(
            "placement watch repeated its initial snapshot",
        )),
        Some(Update::Failure(failure)) => Err(decode_watch_failure(failure)),
        Some(Update::ProgressRevision(_)) | None => {
            Err(remote_watch_protocol("placement watch update is invalid"))
        }
    }
}

fn decode_watch_failure(failure: proto::ServicePlacementWatchFailure) -> OwnershipWatchError {
    use proto::ServicePlacementWatchFailureKind as Kind;

    let bound = usize::try_from(failure.bound).unwrap_or(usize::MAX);
    match Kind::try_from(failure.kind).unwrap_or(Kind::Unspecified) {
        Kind::Lagged => OwnershipWatchError::Lagged {
            skipped: failure.bound,
        },
        Kind::Closed => OwnershipWatchError::Closed,
        Kind::Backend => OwnershipWatchError::Backend {
            message: "remote placement watch backend failed".to_string(),
        },
        Kind::Compacted => OwnershipWatchError::Compacted {
            requested_revision: PlacementRevision(failure.requested_revision),
            compact_revision: PlacementRevision(failure.compact_revision),
        },
        Kind::Canceled => OwnershipWatchError::Canceled {
            reason: "remote placement watch was canceled".to_string(),
        },
        Kind::Protocol | Kind::Unspecified => {
            remote_watch_protocol("remote placement watch protocol failed")
        }
        Kind::Capacity => OwnershipWatchError::CapacityExceeded { max_entries: bound },
        Kind::BatchCapacity => OwnershipWatchError::BatchCapacityExceeded { max_events: bound },
        Kind::StartupBacklog => OwnershipWatchError::StartupBacklogExceeded { max_updates: bound },
        Kind::Proof => OwnershipWatchError::Protocol {
            message: "remote placement watch proof failed".to_string(),
        },
    }
}

fn decode_watch_event(
    event: proto::ServicePlacementWatchEvent,
    expected_service: &ServiceKind,
    expected_instance: &InstanceId,
    batch_revision: PlacementRevision,
) -> Result<OwnershipWatchEvent, OwnershipWatchError> {
    use proto::service_placement_watch_event::Event;

    match event
        .event
        .ok_or_else(|| remote_watch_protocol("placement watch event is missing"))?
    {
        Event::InstanceUpserted(record) => {
            let record = decode_instance_record(record, expected_instance)
                .map_err(|error| remote_watch_protocol(error.to_string()))?;
            if &record.service_kind != expected_service {
                return Err(remote_watch_protocol(
                    "placement watch instance service is invalid",
                ));
            }
            Ok(OwnershipWatchEvent::InstanceUpserted { record })
        }
        Event::InstanceDeleted(record) => {
            let record = decode_instance_record(record, expected_instance)
                .map_err(|error| remote_watch_protocol(error.to_string()))?;
            if &record.service_kind != expected_service {
                return Err(remote_watch_protocol(
                    "placement watch instance service is invalid",
                ));
            }
            Ok(OwnershipWatchEvent::InstanceDeleted { record })
        }
        Event::PlacementUpserted(record) => {
            decode_watch_placement(record, expected_service, batch_revision, false)
        }
        Event::PlacementDeleted(record) => {
            decode_watch_placement(record, expected_service, batch_revision, true)
        }
    }
}

fn decode_watch_placement(
    entry: proto::ServicePlacementRecord,
    expected_service: &ServiceKind,
    batch_revision: PlacementRevision,
    deleted: bool,
) -> Result<OwnershipWatchEvent, OwnershipWatchError> {
    use proto::service_placement_record::Record;

    if entry.revision == 0 || entry.revision > batch_revision.0 {
        return Err(remote_watch_protocol(
            "placement watch record revision is invalid",
        ));
    }
    let record_revision = PlacementRevision(entry.revision);
    let proof = entry
        .proof
        .ok_or_else(|| remote_watch_protocol("placement watch proof is missing"))?;
    let context = if deleted {
        proto::OwnershipProofContext::Delete
    } else {
        proto::OwnershipProofContext::Upsert
    };
    match entry
        .record
        .ok_or_else(|| remote_watch_protocol("placement watch record is missing"))?
    {
        Record::Actor(record) => {
            let record = decode_snapshot_actor(record, expected_service)
                .map_err(|error| remote_watch_protocol(error.to_string()))?;
            let binding = OwnershipRecordBinding::Actor(record.clone());
            let proof =
                decode_floor_proof(proof, context, batch_revision, record_revision, binding)
                    .map_err(|error| remote_watch_protocol(error.to_string()))?;
            let key = ActorPlacementKey {
                service_kind: record.service_kind.clone(),
                actor_kind: record.actor_kind.clone(),
                actor_id: record.actor_id.clone(),
            };
            Ok(if deleted {
                OwnershipWatchEvent::ActorDeleted {
                    key,
                    previous_record: record,
                    proof,
                }
            } else {
                OwnershipWatchEvent::ActorUpserted { key, record, proof }
            })
        }
        Record::VirtualShard(record) => {
            let record = decode_snapshot_virtual_shard(record, expected_service)
                .map_err(|error| remote_watch_protocol(error.to_string()))?;
            let binding = OwnershipRecordBinding::VirtualShard(record.clone());
            let proof =
                decode_floor_proof(proof, context, batch_revision, record_revision, binding)
                    .map_err(|error| remote_watch_protocol(error.to_string()))?;
            let key = crate::storage::VirtualShardPlacementKey {
                service_kind: record.service_kind.clone(),
                actor_kind: record.actor_kind.clone(),
                shard_id: record.shard_id,
            };
            Ok(if deleted {
                OwnershipWatchEvent::VirtualShardDeleted {
                    key,
                    previous_record: record,
                    proof,
                }
            } else {
                OwnershipWatchEvent::VirtualShardUpserted { key, record, proof }
            })
        }
        Record::Singleton(record) => {
            let record = decode_snapshot_singleton(record, expected_service)
                .map_err(|error| remote_watch_protocol(error.to_string()))?;
            let binding = OwnershipRecordBinding::Singleton(record.clone());
            let proof =
                decode_floor_proof(proof, context, batch_revision, record_revision, binding)
                    .map_err(|error| remote_watch_protocol(error.to_string()))?;
            let key = SingletonKey {
                service_kind: record.service_kind.clone(),
                singleton_kind: record.singleton_kind.clone(),
                scope: record.scope.clone(),
            };
            Ok(if deleted {
                OwnershipWatchEvent::SingletonDeleted {
                    key,
                    previous_record: record,
                    proof,
                }
            } else {
                OwnershipWatchEvent::SingletonUpserted { key, record, proof }
            })
        }
    }
}

fn remote_view_backend(message: impl Into<String>) -> crate::storage::OwnershipViewError {
    crate::storage::OwnershipViewError::Backend {
        message: message.into(),
    }
}

fn remote_view_protocol(message: impl Into<String>) -> crate::storage::OwnershipViewError {
    crate::storage::OwnershipViewError::Protocol {
        message: message.into(),
    }
}

fn remote_view_status(status: Status) -> crate::storage::OwnershipViewError {
    match status.code() {
        Code::ResourceExhausted => crate::storage::OwnershipViewError::CapacityExceeded {
            max_entries: MAX_PLACEMENT_SNAPSHOT_ENTRIES,
        },
        _ => remote_view_backend(format!("placement watch RPC failed: {:?}", status.code())),
    }
}

fn remote_watch_protocol(message: impl Into<String>) -> OwnershipWatchError {
    OwnershipWatchError::Protocol {
        message: message.into(),
    }
}

fn remote_watch_status(status: Status) -> OwnershipWatchError {
    match status.code() {
        Code::ResourceExhausted => OwnershipWatchError::CapacityExceeded {
            max_entries: MAX_PLACEMENT_SNAPSHOT_ENTRIES,
        },
        Code::OutOfRange => OwnershipWatchError::Compacted {
            requested_revision: PlacementRevision(0),
            compact_revision: PlacementRevision(0),
        },
        Code::DataLoss => remote_watch_protocol("remote placement watch proof is invalid"),
        Code::Cancelled => OwnershipWatchError::Canceled {
            reason: "remote placement watch was canceled".to_string(),
        },
        _ => OwnershipWatchError::Backend {
            message: format!("remote placement watch failed: {:?}", status.code()),
        },
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
    let owner_incarnation = InstanceIncarnation::new(reply.owner_incarnation);
    if !owner_incarnation.is_canonical() {
        return invalid_reply("owner_incarnation");
    }
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
        owner_incarnation,
        epoch,
        lease_id,
        state,
    })
}

fn decode_drain_reply(
    reply: proto::DrainInstanceReply,
    expected_service: &ServiceKind,
    expected_instance: &InstanceId,
    expected_incarnation: &InstanceIncarnation,
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
    require_equal(
        reply.drained_instance_incarnation.as_str(),
        expected_incarnation.as_str(),
        "drained_instance_incarnation",
    )?;
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
        drained_incarnation: expected_incarnation.clone(),
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
        type WatchServicePlacementStream = tokio_stream::wrappers::ReceiverStream<
            Result<proto::ServicePlacementWatchResponse, Status>,
        >;

        async fn register_instance(
            &self,
            _request: Request<proto::RegisterInstanceRequest>,
        ) -> Result<Response<proto::InstanceLivenessReply>, Status> {
            Err(Status::unimplemented("test authority"))
        }

        async fn keepalive_instance(
            &self,
            _request: Request<proto::InstanceLivenessRequest>,
        ) -> Result<Response<proto::InstanceLivenessReply>, Status> {
            Err(Status::unimplemented("test authority"))
        }

        async fn transition_instance(
            &self,
            _request: Request<proto::TransitionInstanceRequest>,
        ) -> Result<Response<proto::InstanceLivenessReply>, Status> {
            Err(Status::unimplemented("test authority"))
        }

        async fn keepalive_singletons(
            &self,
            _request: Request<proto::KeepaliveSingletonsRequest>,
        ) -> Result<Response<proto::KeepaliveSingletonsReply>, Status> {
            Err(Status::unimplemented("test authority"))
        }

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

        async fn get_instance(
            &self,
            _request: Request<proto::GetInstanceRequest>,
        ) -> Result<Response<proto::GetInstanceReply>, Status> {
            Err(Status::unimplemented("test authority"))
        }

        async fn get_actor(
            &self,
            _request: Request<proto::GetActorRequest>,
        ) -> Result<Response<proto::GetActorReply>, Status> {
            Err(Status::unimplemented("test authority"))
        }

        async fn get_singleton(
            &self,
            _request: Request<proto::GetSingletonRequest>,
        ) -> Result<Response<proto::GetSingletonReply>, Status> {
            Err(Status::unimplemented("test authority"))
        }

        async fn get_service_placement_snapshot(
            &self,
            _request: Request<proto::GetServicePlacementSnapshotRequest>,
        ) -> Result<Response<proto::GetServicePlacementSnapshotReply>, Status> {
            Err(Status::unimplemented("test authority"))
        }

        async fn watch_service_placement(
            &self,
            _request: Request<proto::GetServicePlacementSnapshotRequest>,
        ) -> Result<Response<Self::WatchServicePlacementStream>, Status> {
            Err(Status::unimplemented("test authority"))
        }

        async fn list_service_instances(
            &self,
            _request: Request<proto::ListServiceInstancesRequest>,
        ) -> Result<Response<proto::ListServiceInstancesReply>, Status> {
            Err(Status::unimplemented("test authority"))
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
        let reader =
            TonicPlacementReader::new(Channel::from_static("http://127.0.0.1:1").connect_lazy());
        assert_eq!(
            reader
                .get_service_placement_snapshot(
                    &service_kind!("World"),
                    &InstanceId::new("world-a"),
                    std::num::NonZeroUsize::new(MAX_PLACEMENT_SNAPSHOT_ENTRIES + 1).unwrap(),
                )
                .await,
            Err(PlacementError::PlacementReadLimitExceeded {
                limit: MAX_PLACEMENT_SNAPSHOT_ENTRIES
            })
        );
    }

    #[tokio::test]
    async fn singleton_renewal_rejects_an_oversized_batch_before_transport() {
        let authority =
            TonicPlacementAuthority::new(Channel::from_static("http://127.0.0.1:1").connect_lazy());
        let claim = SingletonPlacementRecord {
            service_kind: service_kind!("World"),
            singleton_kind: actor_kind!("SeasonManager"),
            scope: "global".to_string(),
            owner: InstanceId::new("world-a"),
            owner_incarnation: InstanceIncarnation::new("world-a-boot"),
            epoch: Epoch(1),
            lease_id: LeaseId(7),
            state: PlacementState::Running,
        };

        assert_eq!(
            authority
                .keepalive_singletons(
                    service_kind!("World"),
                    InstanceId::new("world-a"),
                    InstanceIncarnation::new("world-a-boot"),
                    vec![claim; MAX_SINGLETON_RENEWAL_CLAIMS + 1],
                )
                .await,
            Err(PlacementError::SingletonRenewalLimitExceeded {
                limit: MAX_SINGLETON_RENEWAL_CLAIMS
            })
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
            owner_incarnation: "world-a-boot".to_string(),
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
        let incarnation = InstanceIncarnation::new("world-a-boot");
        let drain = proto::DrainInstanceReply {
            service_kind: "World".to_string(),
            drained_instance_id: "world-a".to_string(),
            drained_instance_incarnation: incarnation.as_str().to_string(),
            migrated_actors: 3,
            migrated_virtual_shards: 4,
            drained_lease_id: 5,
            outcome: proto::DrainInstanceOutcome::Completed as i32,
        };
        assert_eq!(
            decode_drain_reply(drain.clone(), &service, &instance, &incarnation, LeaseId(5),)
                .unwrap(),
            DrainReport {
                drained_instance: instance.clone(),
                drained_incarnation: incarnation.clone(),
                migrated_actors: 3,
                migrated_virtual_shards: 4,
            }
        );
        let mut invalid_drain = drain.clone();
        invalid_drain.drained_instance_id = "world-b".to_string();
        assert_eq!(
            decode_drain_reply(invalid_drain, &service, &instance, &incarnation, LeaseId(5),)
                .unwrap_err(),
            PlacementError::InvalidPlacementAuthorityReply {
                field: "drained_instance_id"
            }
        );
        let mut invalid_drain = drain;
        invalid_drain.drained_lease_id = 6;
        assert_eq!(
            decode_drain_reply(invalid_drain, &service, &instance, &incarnation, LeaseId(5),)
                .unwrap_err(),
            PlacementError::InvalidPlacementAuthorityReply {
                field: "drained_lease_id"
            }
        );
        let no_replacement = proto::DrainInstanceReply {
            service_kind: "World".to_string(),
            drained_instance_id: "world-a".to_string(),
            drained_instance_incarnation: incarnation.as_str().to_string(),
            migrated_actors: 0,
            migrated_virtual_shards: 0,
            drained_lease_id: 5,
            outcome: proto::DrainInstanceOutcome::NoReadyReplacement as i32,
        };
        assert_eq!(
            decode_drain_reply(
                no_replacement.clone(),
                &service,
                &instance,
                &incarnation,
                LeaseId(5),
            )
            .unwrap_err(),
            PlacementError::NoReadyInstances
        );
        let mut invalid_no_replacement = no_replacement;
        invalid_no_replacement.migrated_actors = 1;
        assert_eq!(
            decode_drain_reply(
                invalid_no_replacement,
                &service,
                &instance,
                &incarnation,
                LeaseId(5),
            )
            .unwrap_err(),
            PlacementError::InvalidPlacementAuthorityReply { field: "outcome" }
        );
    }

    #[test]
    fn point_read_instance_reply_validation_rejects_mismatched_and_oversized_records() {
        let valid = || proto::InstanceRecord {
            service_kind: "World".to_string(),
            instance_id: "world-a".to_string(),
            instance_incarnation: "world-a-boot".to_string(),
            lease_id: 7,
            advertised_endpoint: "http://127.0.0.1:50051".to_string(),
            control_endpoint: "http://127.0.0.1:50052".to_string(),
            version: "test".to_string(),
            state: "Ready".to_string(),
            max_actors: None,
            max_connections: None,
            labels: Default::default(),
        };
        assert!(decode_instance_record(valid(), &InstanceId::new("world-a")).is_ok());
        assert_eq!(
            decode_instance_record(valid(), &InstanceId::new("world-b")),
            Err(PlacementError::InvalidPlacementAuthorityReply {
                field: "instance_id"
            })
        );
        let mut oversized = valid();
        oversized.version = "x".repeat(257);
        assert_eq!(
            decode_instance_record(oversized, &InstanceId::new("world-a")),
            Err(PlacementError::InvalidPlacementAuthorityReply {
                field: "instance_metadata"
            })
        );
    }

    #[test]
    fn service_snapshot_reply_rejects_duplicate_keys_and_future_record_revisions() {
        let actor = || proto::ServicePlacementRecord {
            revision: 1,
            record: Some(proto::service_placement_record::Record::Actor(
                proto::ActorPlacementReply {
                    service_kind: "World".to_string(),
                    actor_kind: "World".to_string(),
                    actor_id: Some(actor_id_to_proto(&ActorId::U64(7))),
                    owner_instance_id: "world-a".to_string(),
                    epoch: 1,
                    lease_id: 2,
                    state: "running".to_string(),
                },
            )),
            proof: Some(proto::SnapshotEpochFloorProof {
                observed_revision: 2,
                record_version: 1,
                floor_version: 1,
                floor_epoch: 1,
                context: proto::OwnershipProofContext::Snapshot as i32,
            }),
        };
        let reply = proto::GetServicePlacementSnapshotReply {
            revision: 2,
            local_instance: None,
            records: vec![actor(), actor()],
        };
        assert_eq!(
            decode_service_snapshot(
                reply,
                &service_kind!("World"),
                &InstanceId::new("world-a"),
                std::num::NonZeroUsize::new(8).unwrap(),
            ),
            Err(PlacementError::InvalidPlacementAuthorityReply {
                field: "record.duplicate"
            })
        );

        let mut future = actor();
        future.revision = 3;
        assert_eq!(
            decode_service_snapshot(
                proto::GetServicePlacementSnapshotReply {
                    revision: 2,
                    local_instance: None,
                    records: vec![future],
                },
                &service_kind!("World"),
                &InstanceId::new("world-a"),
                std::num::NonZeroUsize::new(8).unwrap(),
            ),
            Err(PlacementError::InvalidPlacementAuthorityReply {
                field: "record.revision"
            })
        );

        let mut tampered = actor();
        tampered.proof.as_mut().unwrap().floor_epoch = 2;
        assert_eq!(
            decode_service_snapshot(
                proto::GetServicePlacementSnapshotReply {
                    revision: 2,
                    local_instance: None,
                    records: vec![tampered],
                },
                &service_kind!("World"),
                &InstanceId::new("world-a"),
                std::num::NonZeroUsize::new(8).unwrap(),
            ),
            Err(PlacementError::InvalidPlacementAuthorityReply {
                field: "record.proof"
            })
        );
    }

    #[test]
    fn remote_watch_preserves_structured_compaction_failure() {
        let error = decode_watch_update(
            proto::ServicePlacementWatchResponse {
                update: Some(proto::service_placement_watch_response::Update::Failure(
                    proto::ServicePlacementWatchFailure {
                        kind: proto::ServicePlacementWatchFailureKind::Compacted as i32,
                        requested_revision: 11,
                        compact_revision: 17,
                        bound: 0,
                    },
                )),
            },
            &service_kind!("World"),
            &InstanceId::new("world-a"),
            std::num::NonZeroUsize::new(8).unwrap(),
        )
        .unwrap_err();
        assert_eq!(
            error,
            OwnershipWatchError::Compacted {
                requested_revision: PlacementRevision(11),
                compact_revision: PlacementRevision(17),
            }
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
                    InstanceIncarnation::new("world-a-boot"),
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
