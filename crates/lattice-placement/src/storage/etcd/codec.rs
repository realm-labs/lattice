use etcd_client::PutOptions;
use lattice_core::id::ActorId;
use lattice_core::instance::InstanceId;
use lattice_core::kind::ServiceKind;
use serde::{Deserialize, Serialize};

use crate::error::PlacementError;
use crate::registry::InstanceRecord;
use crate::storage::{
    ActorPlacementKey, ActorPlacementRecord, CoordinatorLeadership, LeaseId, PlacementPrefix,
    PlacementVersion, SingletonKey, SingletonPlacementRecord, VirtualShardPlacementKey,
    VirtualShardPlacementRecord,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EtcdValue {
    Instance(Box<InstanceRecord>),
    Actor(Box<ActorPlacementRecord>),
    VirtualShard(Box<VirtualShardPlacementRecord>),
    Singleton(Box<SingletonPlacementRecord>),
    CoordinatorLeader(Box<CoordinatorLeadership>),
    ActivationLock(LeaseId),
    SingletonLock(LeaseId),
}

pub(crate) fn clean_prefix(prefix: &PlacementPrefix) -> &str {
    prefix.as_str().trim_end_matches('/')
}

pub(crate) fn encode_etcd_value(value: &EtcdValue) -> Result<Vec<u8>, PlacementError> {
    serde_json::to_vec(value).map_err(codec_error)
}

pub(crate) fn decode_etcd_value(bytes: &[u8]) -> Result<EtcdValue, PlacementError> {
    serde_json::from_slice(bytes).map_err(codec_error)
}

pub(crate) fn put_options_for(value: &EtcdValue) -> Result<Option<PutOptions>, PlacementError> {
    match value {
        EtcdValue::Instance(record) => {
            let lease_id = i64::try_from(record.lease_id.0).map_err(codec_error)?;
            Ok(Some(PutOptions::new().with_lease(lease_id)))
        }
        EtcdValue::CoordinatorLeader(leadership) => {
            let lease_id = i64::try_from(leadership.lease_id.0).map_err(codec_error)?;
            Ok(Some(PutOptions::new().with_lease(lease_id)))
        }
        EtcdValue::ActivationLock(lease_id) | EtcdValue::SingletonLock(lease_id) => {
            let lease_id = i64::try_from(lease_id.0).map_err(codec_error)?;
            Ok(Some(PutOptions::new().with_lease(lease_id)))
        }
        EtcdValue::Singleton(record) => {
            let lease_id = i64::try_from(record.lease_id.0).map_err(codec_error)?;
            Ok(Some(PutOptions::new().with_lease(lease_id)))
        }
        EtcdValue::Actor(_) | EtcdValue::VirtualShard(_) => Ok(None),
    }
}

pub(crate) fn default_instance_lease_ttl_secs() -> i64 {
    30
}

pub(crate) fn placement_version(version: i64) -> Result<PlacementVersion, PlacementError> {
    let version = u64::try_from(version).map_err(codec_error)?;
    Ok(PlacementVersion(version))
}

pub(crate) fn lease_id(id: i64) -> Result<LeaseId, PlacementError> {
    let id = u64::try_from(id).map_err(codec_error)?;
    Ok(LeaseId(id))
}

pub(crate) fn etcd_error(error: etcd_client::Error) -> PlacementError {
    PlacementError::Etcd {
        message: error.to_string(),
    }
}

pub(crate) fn codec_error(error: impl std::fmt::Display) -> PlacementError {
    PlacementError::PlacementCodec {
        message: error.to_string(),
    }
}

pub(crate) fn instance_key(
    prefix: &PlacementPrefix,
    service_kind: &ServiceKind,
    instance_id: &InstanceId,
) -> String {
    format!(
        "{}/logic/instances/{}/{}",
        clean_prefix(prefix),
        service_kind.as_str(),
        instance_id.as_str()
    )
}

pub(crate) fn actor_key(prefix: &PlacementPrefix, key: &ActorPlacementKey) -> String {
    format!(
        "{}/logic/actors/{}/{}/{}",
        clean_prefix(prefix),
        key.service_kind.as_str(),
        key.actor_kind.as_str(),
        actor_id_segment(&key.actor_id)
    )
}

pub(crate) fn vshard_key(prefix: &PlacementPrefix, key: &VirtualShardPlacementKey) -> String {
    format!(
        "{}/logic/vshards/{}/{}/{}",
        clean_prefix(prefix),
        key.service_kind.as_str(),
        key.actor_kind.as_str(),
        key.shard_id.0
    )
}

pub(crate) fn singleton_key(prefix: &PlacementPrefix, key: &SingletonKey) -> String {
    format!(
        "{}/logic/singletons/{}/{}/{}",
        clean_prefix(prefix),
        key.service_kind.as_str(),
        key.singleton_kind.as_str(),
        scope_segment(&key.scope)
    )
}

pub(crate) fn activation_lock_key(prefix: &PlacementPrefix, key: &ActorPlacementKey) -> String {
    format!(
        "{}/logic/activation_locks/{}/{}/{}",
        clean_prefix(prefix),
        key.service_kind.as_str(),
        key.actor_kind.as_str(),
        actor_id_segment(&key.actor_id)
    )
}

pub(crate) fn singleton_lock_key(prefix: &PlacementPrefix, key: &SingletonKey) -> String {
    format!(
        "{}/logic/singleton_locks/{}/{}/{}",
        clean_prefix(prefix),
        key.service_kind.as_str(),
        key.singleton_kind.as_str(),
        scope_segment(&key.scope)
    )
}

pub(crate) fn coordinator_leader_key(prefix: &PlacementPrefix) -> String {
    format!("{}/coordinator/leader", clean_prefix(prefix))
}

pub(crate) fn scope_segment(scope: &str) -> String {
    hex_encode(scope.as_bytes())
}

pub(crate) fn actor_id_segment(actor_id: &ActorId) -> String {
    match actor_id {
        ActorId::Str(value) => format!("str:{value}"),
        ActorId::U64(value) => format!("u64:{value}"),
        ActorId::I64(value) => format!("i64:{value}"),
        ActorId::Bytes(value) => format!("bytes:{}", hex_encode(value)),
    }
}

pub(crate) fn hex_encode(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push_str(&format!("{byte:02x}"));
    }
    output
}
