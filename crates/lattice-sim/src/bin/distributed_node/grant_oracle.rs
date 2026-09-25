/// Synthetic-coordinator fixture: still uses the owner's real correlation ID and
/// bounded deadline instead of bypassing production authority admission.
struct FixtureGrantOracle(tokio::task::JoinHandle<()>);

impl Drop for FixtureGrantOracle {
    fn drop(&mut self) { self.0.abort(); }
}

impl FixtureGrantOracle {
    fn spawn(control: Arc<PlacementControlRouter>, coordinator: AssociationKey,
        state: Arc<Mutex<LogicPlacementState>>, slot: PlacementSlot) -> Self {
        Self(tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(100));
            let mut sequence = 1;
            loop {
                tick.tick().await;
                let request_id = state.lock().unwrap().pending_claim_request(&slot.key);
                let Some(request_id) = request_id else { continue; };
                sequence += 1;
                let grant = ClaimGrant {
                    request_id, group: slot.key.group().clone(), slot: slot.key.clone(),
                    owner: slot.owner.clone().unwrap(), coordinator_term: slot.version.term,
                    assignment_generation: slot.assignment_generation,
                    grant_sequence: GrantSequence::new(sequence).unwrap(), ttl: Duration::from_secs(15),
                };
                let scope = CoordinatorScope::Group(slot.key.group().clone());
                let payload = encode_control_command_for_term(&scope, slot.version.term.get(),
                    &PlacementControlCommand::ClaimGranted(grant), DEFAULT_MAX_CONTROL_PAYLOAD).unwrap();
                if control.apply(coordinator.clone(), control_stream_id(&scope), CommandId::generate(), payload).await.is_err() { break; }
            }
        }))
    }
}
