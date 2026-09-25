use lattice_coordination::{
    coordinator::LeaderRecord,
    storage::{
        StorageError,
        candidates::{CandidateStore, provision_candidate},
    },
};

pub async fn prepare_leader<S: CandidateStore + ?Sized>(
    store: &S,
    mut leader: LeaderRecord,
    lease: i64,
) -> Result<LeaderRecord, StorageError> {
    let authorization =
        provision_candidate(store, leader.scope.clone(), leader.node.node_id.clone()).await?;
    leader.candidate_generation = authorization.generation;
    leader.candidate_lease_id = lease;
    store
        .register_candidate(&leader.candidate_registration())
        .await?;
    Ok(leader)
}
