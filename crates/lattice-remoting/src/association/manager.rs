//! Exact-peer registry, dial direction and connection-generation reconciliation.
//!
//! Address-to-incarnation bindings prevent an old process from taking over a current peer's
//! address. A trusted authority or validated bootstrap exchange explicitly replaces the binding.
//! Within one incarnation, an incoming generation may replace a locally inactive or sufficiently
//! silent generation; recently responsive live connections reject that takeover.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use lattice_model::cluster::{ClusterId, NodeEndpoint, NodeIncarnation};

use super::{
    Association, AssociationError, AssociationId, AssociationKey, AssociationManager,
    OutboundByteBudget,
};
use crate::config::RemotingConfig;

impl AssociationManager {
    /// Creates an empty registry with a shared node-wide outbound payload budget.
    ///
    /// # Errors
    ///
    /// Returns [`AssociationError::InvalidConfig`] for unsupported limits or timeouts.
    pub fn new(
        local_address: NodeEndpoint,
        local_incarnation: NodeIncarnation,
        config: RemotingConfig,
    ) -> Result<Self, AssociationError> {
        config.validate().map_err(AssociationError::InvalidConfig)?;
        Ok(Self {
            local_address,
            local_incarnation,
            config,
            associations: Mutex::new(HashMap::new()),
            remote_incarnations: Mutex::new(HashMap::new()),
            queued_bytes: Arc::new(OutboundByteBudget::new()),
        })
    }

    /// Returns the registered exact peer, or allocates a new outgoing generation and queues.
    ///
    /// Does not establish connections. The remote address is bound to its first observed
    /// incarnation until explicitly reconciled, even if the association is later removed.
    ///
    /// # Errors
    ///
    /// Returns an error for a conflicting incarnation or an exhausted association limit.
    pub fn get_or_create(
        &self,
        cluster_id: ClusterId,
        remote_address: NodeEndpoint,
        remote_incarnation: NodeIncarnation,
    ) -> Result<Arc<Association>, AssociationError> {
        {
            let mut incarnations = self
                .remote_incarnations
                .lock()
                .expect("remote incarnation registry poisoned");
            match incarnations.get(&remote_address) {
                Some(current) if *current != remote_incarnation => {
                    return Err(AssociationError::OldOrUnreconciledIncarnation);
                }
                Some(_) => {}
                None => {
                    incarnations.insert(remote_address.clone(), remote_incarnation);
                }
            }
        }
        let key = AssociationKey {
            cluster_id,
            local_incarnation: self.local_incarnation,
            remote_address,
            remote_incarnation,
        };
        let mut associations = self
            .associations
            .lock()
            .expect("association registry poisoned");
        if let Some(existing) = associations.get(&key) {
            return Ok(existing.clone());
        }
        if associations.len() == self.config.max_associations {
            return Err(AssociationError::AssociationLimit);
        }
        let association = Arc::new(Association::new_with_id_and_budget(
            key.clone(),
            AssociationId::generate(),
            self.config.clone(),
            self.queued_bytes.clone(),
        )?);
        associations.insert(key, association.clone());
        Ok(association)
    }

    /// Resolves an incoming handshake to its exact association generation.
    ///
    /// A matching identifier reuses the current entry. A different identifier replaces it only
    /// when no live lane remains or peer silence has reached the liveness window. Replacement
    /// logically retires the old generation; its lifetime owner handles asynchronous cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error for a conflicting incarnation, a recently responsive live generation,
    /// or an exhausted association limit.
    pub fn get_or_accept(
        &self,
        cluster_id: ClusterId,
        remote_address: NodeEndpoint,
        remote_incarnation: NodeIncarnation,
        association_id: AssociationId,
    ) -> Result<Arc<Association>, AssociationError> {
        {
            let mut incarnations = self
                .remote_incarnations
                .lock()
                .expect("remote incarnation registry poisoned");
            match incarnations.get(&remote_address) {
                Some(current) if *current != remote_incarnation => {
                    return Err(AssociationError::OldOrUnreconciledIncarnation);
                }
                Some(_) => {}
                None => {
                    incarnations.insert(remote_address.clone(), remote_incarnation);
                }
            }
        }
        let key = AssociationKey {
            cluster_id,
            local_incarnation: self.local_incarnation,
            remote_address,
            remote_incarnation,
        };
        let mut associations = self
            .associations
            .lock()
            .expect("association registry poisoned");
        if let Some(existing) = associations.get(&key).cloned() {
            if existing.id() == association_id {
                return Ok(existing);
            }
            // The key pins the peer incarnation, so a differing id is a newer connection
            // generation from that same peer; it may only take over an entry that no longer
            // runs a live lane connection.
            //
            // A peer that freezes or is blackholed and then rejoins keeps its incarnation,
            // so the key alone proves nothing about whether the entry is still real. Local
            // lane bookkeeping is therefore only believed while the peer keeps proving it:
            // a healthy association exchanges a control heartbeat every heartbeat interval,
            // so one that has been silent past the control lane's own liveness window is
            // stale no matter how many lanes it still claims.
            if existing.has_live_connection()
                && existing.peer_silence() < self.config.peer_liveness_window()
            {
                return Err(AssociationError::IncomingAssociationConflict);
            }
            associations.remove(&key);
            existing.begin_close();
            existing.finish_close();
        }
        if associations.len() == self.config.max_associations {
            return Err(AssociationError::AssociationLimit);
        }
        let association = Arc::new(Association::new_with_id_and_budget(
            key.clone(),
            association_id,
            self.config.clone(),
            self.queued_bytes.clone(),
        )?);
        associations.insert(key, association.clone());
        Ok(association)
    }

    /// Returns whether this node is the deterministic dialer for the exact peer pair.
    ///
    /// The lower `(endpoint, incarnation)` tuple dials every lane. The other side requests
    /// reverse dialing, avoiding competing outbound lane groups.
    pub fn should_dial(
        &self,
        remote_address: &NodeEndpoint,
        remote_incarnation: NodeIncarnation,
    ) -> bool {
        (&self.local_address, self.local_incarnation.get())
            < (remote_address, remote_incarnation.get())
    }

    /// Removes and logically closes the entry only if its key and generation still match.
    ///
    /// Returns `false` if the entry is absent or has been replaced. This protects a new
    /// generation from delayed cleanup belonging to its predecessor; it does not join tasks.
    pub fn remove(&self, key: &AssociationKey, id: AssociationId) -> bool {
        let mut associations = self
            .associations
            .lock()
            .expect("association registry poisoned");
        if associations
            .get(key)
            .is_some_and(|association| association.id() == id)
        {
            let removed = associations
                .remove(key)
                .expect("matched association exists");
            removed.begin_close();
            removed.finish_close();
            true
        } else {
            false
        }
    }

    /// Returns the registered association for an exact key, regardless of lifecycle state.
    pub fn get(&self, key: &AssociationKey) -> Option<Arc<Association>> {
        self.associations
            .lock()
            .expect("association registry poisoned")
            .get(key)
            .cloned()
    }

    /// Looks up a peer using this manager's local incarnation.
    pub fn get_exact(
        &self,
        cluster_id: &ClusterId,
        remote_address: &NodeEndpoint,
        remote_incarnation: NodeIncarnation,
    ) -> Option<Arc<Association>> {
        self.get(&AssociationKey {
            cluster_id: cluster_id.clone(),
            local_incarnation: self.local_incarnation,
            remote_address: remote_address.clone(),
            remote_incarnation,
        })
    }

    /// Finds a currently registered generation by identifier.
    pub fn get_by_id(&self, id: AssociationId) -> Option<Arc<Association>> {
        self.associations
            .lock()
            .expect("association registry poisoned")
            .values()
            .find(|association| association.id() == id)
            .cloned()
    }

    /// The incarnation this address is currently bound to, if it is bound at all.
    pub fn remote_incarnation(&self, address: &NodeEndpoint) -> Option<NodeIncarnation> {
        self.remote_incarnations
            .lock()
            .expect("remote incarnation registry poisoned")
            .get(address)
            .copied()
    }

    /// Releases an address that is still bound to `incarnation`, and only then.
    ///
    /// The binding exists to keep an old or unreconciled incarnation from taking an address over
    /// from the one in use. Once the incarnation it names has been retired by an authority, keeping
    /// it would instead refuse that address' successor, so the address is released rather than left
    /// pointing at something that no longer exists. A binding that has already moved on belongs to
    /// a later incarnation and is left alone.
    pub fn forget_remote_incarnation(
        &self,
        address: &NodeEndpoint,
        incarnation: NodeIncarnation,
    ) -> bool {
        let mut incarnations = self
            .remote_incarnations
            .lock()
            .expect("remote incarnation registry poisoned");
        if incarnations.get(address) == Some(&incarnation) {
            incarnations.remove(address);
            return true;
        }
        false
    }

    /// Binds an address to an authoritative incarnation and retires its other incarnations.
    ///
    /// Returns the number of associations removed. The caller must establish that the new
    /// incarnation is authoritative; this method does not authenticate a peer or join cleanup.
    pub fn replace_remote_incarnation(
        &self,
        address: NodeEndpoint,
        incarnation: NodeIncarnation,
    ) -> usize {
        self.remote_incarnations
            .lock()
            .expect("remote incarnation registry poisoned")
            .insert(address.clone(), incarnation);
        let mut associations = self
            .associations
            .lock()
            .expect("association registry poisoned");
        let old_keys = associations
            .keys()
            .filter(|key| key.remote_address == address && key.remote_incarnation != incarnation)
            .cloned()
            .collect::<Vec<_>>();
        for key in &old_keys {
            if let Some(association) = associations.remove(key) {
                association.begin_close();
                association.finish_close();
            }
        }
        old_keys.len()
    }

    /// Returns the number of registered associations, including non-active entries.
    pub fn len(&self) -> usize {
        self.associations
            .lock()
            .expect("association registry poisoned")
            .len()
    }

    /// Returns whether the registry has no associations.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
