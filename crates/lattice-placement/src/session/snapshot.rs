use std::collections::BTreeMap;

use super::LogicSessionError;
use crate::{
    coordinator::{MemberRecord, SnapshotRecord},
    types::{PlacementSlot, PlacementSlotKey},
};

pub(super) fn decode_slots(
    records: &[SnapshotRecord],
) -> Result<BTreeMap<PlacementSlotKey, PlacementSlot>, LogicSessionError> {
    let mut slots = BTreeMap::new();
    for record in records {
        if !record.key.starts_with("domain/") || record.key.contains("/member/") {
            continue;
        }
        let slot: PlacementSlot =
            serde_json::from_slice(&record.value).map_err(|_| LogicSessionError::Codec)?;
        slot.validate().map_err(|_| LogicSessionError::Codec)?;
        let expected_key = match &slot.key {
            PlacementSlotKey::Shard {
                domain,
                entity_type,
                shard_id,
            } => format!(
                "domain/{}/shard/{}/{}",
                domain.as_str(),
                entity_type.as_str(),
                shard_id.get()
            ),
            PlacementSlotKey::Singleton { domain, kind } => {
                format!("domain/{}/singleton/{}", domain.as_str(), kind.as_str())
            }
        };
        if record.key != expected_key {
            return Err(LogicSessionError::Codec);
        }
        if slots.insert(slot.key.clone(), slot).is_some() {
            return Err(LogicSessionError::Codec);
        }
    }
    Ok(slots)
}

#[allow(dead_code)]
pub(super) fn decode_members(
    records: &[SnapshotRecord],
) -> Result<Vec<MemberRecord>, LogicSessionError> {
    let mut members = BTreeMap::new();
    for record in records {
        if !record.key.starts_with("member/") {
            continue;
        }
        let member: MemberRecord =
            serde_json::from_slice(&record.value).map_err(|_| LogicSessionError::Codec)?;
        if member.node != member.hello.node
            || members
                .insert(
                    (member.node.node_id.clone(), member.node.incarnation),
                    member,
                )
                .is_some()
        {
            return Err(LogicSessionError::Codec);
        }
    }
    Ok(members.into_values().collect())
}
