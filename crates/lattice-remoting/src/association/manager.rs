use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use lattice_core::actor_ref::{ClusterId, NodeAddress, NodeIncarnation};

use super::{
    Association, AssociationError, AssociationId, AssociationKey, AssociationManager,
    OutboundByteBudget,
};
use crate::config::RemotingConfig;

impl AssociationManager {
    pub fn new(
        local_address: NodeAddress,
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

    pub fn get_or_create(
        &self,
        cluster_id: ClusterId,
        remote_address: NodeAddress,
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

    pub fn get_or_accept(
        &self,
        cluster_id: ClusterId,
        remote_address: NodeAddress,
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
        if let Some(existing) = associations.get(&key) {
            return if existing.id() == association_id {
                Ok(existing.clone())
            } else {
                Err(AssociationError::IncomingAssociationConflict)
            };
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

    pub fn should_dial(
        &self,
        remote_address: &NodeAddress,
        remote_incarnation: NodeIncarnation,
    ) -> bool {
        (&self.local_address, self.local_incarnation.get())
            < (remote_address, remote_incarnation.get())
    }

    pub fn remove(&self, key: &AssociationKey, id: AssociationId) -> bool {
        let mut associations = self
            .associations
            .lock()
            .expect("association registry poisoned");
        if associations
            .get(key)
            .is_some_and(|association| association.id() == id)
        {
            associations.remove(key);
            true
        } else {
            false
        }
    }

    pub fn get(&self, key: &AssociationKey) -> Option<Arc<Association>> {
        self.associations
            .lock()
            .expect("association registry poisoned")
            .get(key)
            .cloned()
    }

    pub fn get_exact(
        &self,
        cluster_id: &ClusterId,
        remote_address: &NodeAddress,
        remote_incarnation: NodeIncarnation,
    ) -> Option<Arc<Association>> {
        self.get(&AssociationKey {
            cluster_id: cluster_id.clone(),
            local_incarnation: self.local_incarnation,
            remote_address: remote_address.clone(),
            remote_incarnation,
        })
    }

    pub fn get_by_id(&self, id: AssociationId) -> Option<Arc<Association>> {
        self.associations
            .lock()
            .expect("association registry poisoned")
            .values()
            .find(|association| association.id() == id)
            .cloned()
    }

    pub fn replace_remote_incarnation(
        &self,
        address: NodeAddress,
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

    pub fn len(&self) -> usize {
        self.associations
            .lock()
            .expect("association registry poisoned")
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
