use std::collections::BTreeMap;

use super::GroupSessionError;
use crate::{
    coordinator::{MemberRecord, SnapshotRecord},
    types::{PlacementSlot, PlacementSlotKey},
};

pub(super) fn decode_slots(
    records: &[SnapshotRecord],
) -> Result<BTreeMap<PlacementSlotKey, PlacementSlot>, GroupSessionError> {
    let mut slots = BTreeMap::new();
    for record in records {
        if !record.key.starts_with("group/") || record.key.contains("/member/") {
            continue;
        }
        let slot: PlacementSlot =
            serde_json::from_slice(&record.value).map_err(|_| GroupSessionError::Codec)?;
        slot.validate().map_err(|_| GroupSessionError::Codec)?;
        let expected_key = match &slot.key {
            PlacementSlotKey::Shard {
                group,
                entity_type,
                shard_id,
            } => format!(
                "group/{}/shard/{}/{}",
                group.as_str(),
                entity_type.as_str(),
                shard_id.get()
            ),
            PlacementSlotKey::Singleton { group, kind } => {
                format!("group/{}/singleton/{}", group.as_str(), kind.as_str())
            }
        };
        if record.key != expected_key {
            return Err(GroupSessionError::Codec);
        }
        if slots.insert(slot.key.clone(), slot).is_some() {
            return Err(GroupSessionError::Codec);
        }
    }
    Ok(slots)
}

#[allow(dead_code)]
pub(super) fn decode_members(
    records: &[SnapshotRecord],
) -> Result<Vec<MemberRecord>, GroupSessionError> {
    let mut members = BTreeMap::new();
    for record in records {
        if !record.key.starts_with("member/") {
            continue;
        }
        let member: MemberRecord =
            serde_json::from_slice(&record.value).map_err(|_| GroupSessionError::Codec)?;
        if member.node != member.hello.node
            || members
                .insert(
                    (member.node.node_id.clone(), member.node.incarnation),
                    member,
                )
                .is_some()
        {
            return Err(GroupSessionError::Codec);
        }
    }
    Ok(members.into_values().collect())
}
