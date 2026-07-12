use super::{
    BTreeMap, BTreeSet, Instant, LogicalBufferConfig, Mutex, PlacementSlotKey, RemoteMessageError,
};

#[derive(Default)]
pub(super) struct RouteBufferState {
    per_slot: BTreeMap<PlacementSlotKey, usize>,
    resolving: BTreeSet<PlacementSlotKey>,
    messages: usize,
    bytes: usize,
}

pub(super) struct RouteBuffer {
    pub(super) config: LogicalBufferConfig,
    state: Mutex<RouteBufferState>,
}

impl RouteBuffer {
    pub(super) fn new(config: LogicalBufferConfig) -> Self {
        Self {
            config,
            state: Mutex::new(RouteBufferState::default()),
        }
    }

    pub(super) fn admit(
        &self,
        slot: PlacementSlotKey,
        bytes: usize,
        requested_deadline: Option<Instant>,
    ) -> Result<(RouteBufferAdmission<'_>, Instant, bool), RemoteMessageError> {
        let now = Instant::now();
        let residence_deadline = now + self.config.maximum_residence;
        let deadline = requested_deadline
            .map(|deadline| deadline.min(residence_deadline))
            .unwrap_or(residence_deadline);
        if deadline <= now {
            return Err(RemoteMessageError::DeadlineExceeded);
        }
        let mut state = self.state.lock().expect("logical route buffer poisoned");
        let slot_messages = state.per_slot.get(&slot).copied().unwrap_or(0);
        if slot_messages == self.config.maximum_messages_per_slot
            || state.messages == self.config.maximum_messages
            || state.bytes.saturating_add(bytes) > self.config.maximum_bytes
        {
            return Err(RemoteMessageError::BufferFull);
        }
        state.messages += 1;
        state.bytes += bytes;
        *state.per_slot.entry(slot.clone()).or_default() += 1;
        let start_resolution = state.resolving.insert(slot.clone());
        Ok((
            RouteBufferAdmission {
                buffer: self,
                slot,
                bytes,
            },
            deadline,
            start_resolution,
        ))
    }

    pub(super) fn resolved(&self, slot: &PlacementSlotKey) {
        self.state
            .lock()
            .expect("logical route buffer poisoned")
            .resolving
            .remove(slot);
    }
}

pub(super) struct RouteBufferAdmission<'a> {
    buffer: &'a RouteBuffer,
    slot: PlacementSlotKey,
    bytes: usize,
}

impl Drop for RouteBufferAdmission<'_> {
    fn drop(&mut self) {
        let mut state = self
            .buffer
            .state
            .lock()
            .expect("logical route buffer poisoned");
        state.messages = state.messages.saturating_sub(1);
        state.bytes = state.bytes.saturating_sub(self.bytes);
        if let Some(count) = state.per_slot.get_mut(&self.slot) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.per_slot.remove(&self.slot);
                state.resolving.remove(&self.slot);
            }
        }
    }
}
