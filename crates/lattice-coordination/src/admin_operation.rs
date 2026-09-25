//! Canonical run- and revision-bound identities shared by runtime and store.

use crate::types::{PlacementVersion, Revision};
use lattice_model::run::RunEpoch;

fn identity(epoch: RunEpoch, version: &PlacementVersion, nonce: u128) -> String {
    format!(
        "a1.{:x}.{:x}.{:x}.{}.{nonce:032x}",
        epoch.get(),
        version.term.get(),
        version.revision.get(),
        blake3::hash(version.group.as_str().as_bytes()).to_hex()
    )
}

pub(crate) fn new_identity(epoch: RunEpoch, version: &PlacementVersion) -> String {
    identity(epoch, version, uuid::Uuid::new_v4().as_u128())
}

pub(crate) fn matches_current(id: &str, epoch: RunEpoch, version: &PlacementVersion) -> bool {
    id.rsplit('.')
        .next()
        .filter(|nonce| nonce.len() == 32)
        .and_then(|nonce| u128::from_str_radix(nonce, 16).ok())
        .is_some_and(|nonce| nonce != 0 && id == identity(epoch, version, nonce))
}

pub(crate) fn matches_committed(id: &str, epoch: RunEpoch, committed: &PlacementVersion) -> bool {
    committed
        .revision
        .get()
        .checked_sub(1)
        .and_then(|revision| Revision::new(revision).ok())
        .is_some_and(|revision| {
            matches_current(
                id,
                epoch,
                &PlacementVersion::new(committed.group.clone(), committed.term, revision),
            )
        })
}
