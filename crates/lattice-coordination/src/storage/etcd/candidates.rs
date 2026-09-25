use async_trait::async_trait;
use etcd_client::{Compare, CompareOp, PutOptions, Txn, TxnOp};
use lattice_model::{
    cluster::CoordinatorScope,
    run::{ClusterLifecycle, RunEpoch},
};

use super::{EtcdCoordinationStore, decode, encode};
use crate::{
    candidates::{
        CandidateAuthorization, CandidateChange, CandidateChangeResult, CandidateRegistration,
        CandidateSetState, MAX_CANDIDATES_PER_SCOPE,
    },
    storage::{ClusterLifecycleStore, StorageError, candidates::CandidateStore},
};

#[async_trait]
impl CandidateStore for EtcdCoordinationStore {
    async fn candidate_set(
        &self,
        scope: &CoordinatorScope,
    ) -> Result<CandidateSetState, StorageError> {
        self.bound_epoch()?;
        Ok(self
            .get_json_key(&self.candidate_state_key(scope))
            .await?
            .unwrap_or_default())
    }

    async fn candidate_authorization(
        &self,
        scope: &CoordinatorScope,
        node_id: &str,
    ) -> Result<Option<CandidateAuthorization>, StorageError> {
        self.bound_epoch()?;
        self.get_json_key(&self.candidate_authorization_key(scope, node_id))
            .await
    }

    async fn change_candidate(
        &self,
        request: CandidateChange,
    ) -> Result<CandidateChangeResult, StorageError> {
        request.validate()?;
        let lifecycle = self.candidate_lifecycle(request.epoch).await?;
        let state_key = self.candidate_state_key(&request.scope);
        let state = self.read_raw(&state_key).await?;
        let current: CandidateSetState = state
            .as_ref()
            .map(|(value, _, _)| decode(value))
            .transpose()?
            .unwrap_or_default();
        let key = self.candidate_authorization_key(&request.scope, &request.node_id);
        let record = self.read_raw(&key).await?;
        let authorization: Option<CandidateAuthorization> = record
            .as_ref()
            .map(|(value, _, _)| decode(value))
            .transpose()?;
        let (next, result) = current.apply(&request, authorization.as_ref())?;
        if next == current {
            return Ok(result);
        }
        let mutation = match &result.authorization {
            Some(authorization) => TxnOp::put(key.clone(), encode(authorization)?, None),
            None => TxnOp::delete(key.clone(), None),
        };
        let mut operations = vec![
            TxnOp::put(state_key.clone(), encode(&next)?, None),
            mutation,
        ];
        if !request.enabled {
            operations.push(TxnOp::delete(
                self.candidate_registration_key(&request.scope, &request.node_id),
                None,
            ));
        }
        self.candidate_commit(
            &lifecycle,
            vec![
                compare_revision(&state_key, state.as_ref().map(|(_, revision, _)| *revision)),
                compare_revision(&key, record.as_ref().map(|(_, revision, _)| *revision)),
            ],
            operations,
        )
        .await?;
        Ok(result)
    }

    async fn register_candidate(
        &self,
        registration: &CandidateRegistration,
    ) -> Result<(), StorageError> {
        registration
            .node
            .validate()
            .map_err(|_| StorageError::InvalidRecord)?;
        if registration.node.node_id != registration.authorization.node_id
            || registration.lease_id <= 0
        {
            return Err(StorageError::InvalidRecord);
        }
        let lifecycle = self.candidate_lifecycle(registration.epoch).await?;
        let authorization_key = self.candidate_authorization_key(
            &registration.authorization.scope,
            &registration.node.node_id,
        );
        if self
            .get_json_key::<CandidateAuthorization>(&authorization_key)
            .await?
            .as_ref()
            != Some(&registration.authorization)
        {
            return Err(StorageError::CandidateNotEligible);
        }
        let key = self.candidate_registration_key(
            &registration.authorization.scope,
            &registration.node.node_id,
        );
        let previous = self.read_raw(&key).await?;
        if let Some((bytes, _, _)) = &previous {
            let previous: CandidateRegistration = decode(bytes)?;
            if previous != *registration && previous.authorization == registration.authorization {
                return Err(StorageError::IncarnationConflict);
            }
        }
        self.candidate_commit(
            &lifecycle,
            vec![
                Compare::value(
                    authorization_key,
                    CompareOp::Equal,
                    encode(&registration.authorization)?,
                ),
                compare_revision(&key, previous.as_ref().map(|(_, revision, _)| *revision)),
            ],
            vec![TxnOp::put(
                key,
                encode(registration)?,
                Some(PutOptions::new().with_lease(registration.lease_id)),
            )],
        )
        .await
    }

    async fn unregister_candidate(
        &self,
        expected: &CandidateRegistration,
    ) -> Result<(), StorageError> {
        let lifecycle = self.candidate_lifecycle(expected.epoch).await?;
        let key =
            self.candidate_registration_key(&expected.authorization.scope, &expected.node.node_id);
        self.candidate_commit(
            &lifecycle,
            vec![Compare::value(
                key.clone(),
                CompareOp::Equal,
                encode(expected)?,
            )],
            vec![TxnOp::delete(key, None)],
        )
        .await
    }

    async fn online_candidates(
        &self,
        scope: &CoordinatorScope,
    ) -> Result<Vec<CandidateRegistration>, StorageError> {
        self.bound_epoch()?;
        self.list_json(
            &format!("candidates/{}/", scope_path(scope)),
            MAX_CANDIDATES_PER_SCOPE,
        )
        .await
    }
}

impl EtcdCoordinationStore {
    pub(super) fn candidate_state_key(&self, scope: &CoordinatorScope) -> String {
        self.root_key(&format!("meta/candidates/{}/state", scope_path(scope)))
    }
    pub(super) fn candidate_authorization_key(
        &self,
        scope: &CoordinatorScope,
        node_id: &str,
    ) -> String {
        self.root_key(&format!(
            "meta/candidates/{}/authorized/{}",
            scope_path(scope),
            node_key(node_id)
        ))
    }
    pub(super) fn candidate_registration_key(
        &self,
        scope: &CoordinatorScope,
        node_id: &str,
    ) -> String {
        self.key(&format!(
            "candidates/{}/{}",
            scope_path(scope),
            node_key(node_id)
        ))
    }
    async fn candidate_lifecycle(&self, epoch: RunEpoch) -> Result<ClusterLifecycle, StorageError> {
        if self.bound_epoch()? != epoch {
            return Err(StorageError::RunMismatch);
        }
        let lifecycle = self.lifecycle().await?;
        if lifecycle.epoch != epoch {
            return Err(StorageError::RunMismatch);
        }
        if !lifecycle.permits_election() {
            return Err(StorageError::RunNotRunning);
        }
        Ok(lifecycle)
    }
    async fn candidate_commit(
        &self,
        lifecycle: &ClusterLifecycle,
        mut compares: Vec<Compare>,
        operations: Vec<TxnOp>,
    ) -> Result<(), StorageError> {
        compares.push(Compare::value(
            self.root_key("meta/lifecycle"),
            CompareOp::Equal,
            encode(lifecycle)?,
        ));
        let mut client = self.client.clone();
        let result = self
            .write_deadline(client.txn(Txn::new().when(compares).and_then(operations)))
            .await?;
        if result.succeeded() {
            Ok(())
        } else {
            Err(StorageError::CompareFailed)
        }
    }
}

fn compare_revision(key: &str, revision: Option<i64>) -> Compare {
    match revision {
        Some(revision) => Compare::mod_revision(key, CompareOp::Equal, revision),
        None => Compare::version(key, CompareOp::Equal, 0),
    }
}

fn scope_path(scope: &CoordinatorScope) -> String {
    match scope {
        CoordinatorScope::Cluster => "cluster".to_owned(),
        CoordinatorScope::Group(group) => format!("groups/{}", group.as_str()),
    }
}

fn node_key(node_id: &str) -> String {
    node_id
        .as_bytes()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}
