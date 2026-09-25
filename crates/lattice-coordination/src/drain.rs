use tokio::{sync::watch, time::Instant};

use crate::session::GroupSessionError;

#[derive(Clone, Default)]
struct DrainState {
    request: Option<(String, u64)>,
    committed: bool,
    closed: bool,
}

/// One session's application-level drain confirmation. Transport acknowledgements deliberately
/// do not participate: a stale or rejected command is acknowledged by the reliable control lane.
#[derive(Clone)]
pub(crate) struct DrainConfirmation(watch::Sender<DrainState>);

impl Default for DrainConfirmation {
    fn default() -> Self {
        Self(watch::channel(DrainState::default()).0)
    }
}

impl DrainConfirmation {
    pub(crate) fn request(&self, operation_id: &str, term: u64) -> Result<(), GroupSessionError> {
        if operation_id.is_empty() || operation_id.len() > 256 {
            return Err(GroupSessionError::InvalidConfig);
        }
        let mut result = Ok(());
        self.0.send_modify(|state| {
            if state.closed {
                result = Err(GroupSessionError::ControlClosed);
            } else if state
                .request
                .as_ref()
                .is_some_and(|(current, _)| current != operation_id)
            {
                result = Err(GroupSessionError::InvalidConfig);
            } else if state
                .request
                .as_ref()
                .is_none_or(|(_, current_term)| *current_term != term)
            {
                state.request = Some((operation_id.to_owned(), term));
                state.committed = false;
            }
        });
        result
    }

    pub(crate) fn confirm(&self, operation_id: &str, term: u64) -> Result<(), GroupSessionError> {
        let mut accepted = false;
        self.0.send_modify(|state| {
            if !state.closed
                && state
                    .request
                    .as_ref()
                    .is_some_and(|(current, current_term)| {
                        current == operation_id && *current_term == term
                    })
            {
                state.committed = true;
                accepted = true;
            }
        });
        if accepted {
            Ok(())
        } else {
            Err(GroupSessionError::StaleGeneration)
        }
    }

    pub(crate) fn requested(&self) -> bool {
        self.0.borrow().request.is_some()
    }

    pub(crate) fn committed_operation(&self) -> Option<String> {
        let state = self.0.borrow();
        if state.committed {
            state
                .request
                .as_ref()
                .map(|(operation, _)| operation.clone())
        } else {
            None
        }
    }

    pub(crate) fn close(&self) {
        self.0.send_modify(|state| state.closed = true);
    }

    pub(crate) async fn wait(
        &self,
        operation_id: &str,
        term: u64,
        deadline: Instant,
    ) -> Result<(), GroupSessionError> {
        let mut state = self.0.subscribe();
        loop {
            {
                let current = state.borrow_and_update();
                if current
                    .request
                    .as_ref()
                    .is_some_and(|(operation, expected_term)| {
                        operation == operation_id && *expected_term == term
                    })
                    && current.committed
                {
                    return Ok(());
                }
                if current.closed {
                    return Err(GroupSessionError::ControlClosed);
                }
            }
            tokio::time::timeout_at(deadline, state.changed())
                .await
                .map_err(|_| GroupSessionError::DrainNotAcknowledged)?
                .map_err(|_| GroupSessionError::ControlClosed)?;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn commit_confirmation_is_bound_to_operation_and_coordinator_term() {
        let confirmation = DrainConfirmation::default();
        confirmation.request("leave", 2).unwrap();
        assert!(confirmation.confirm("different", 2).is_err());
        assert!(confirmation.confirm("leave", 1).is_err());
        assert!(matches!(
            confirmation.wait("leave", 2, Instant::now()).await,
            Err(GroupSessionError::DrainNotAcknowledged)
        ));
        confirmation.confirm("leave", 2).unwrap();
        confirmation.wait("leave", 2, Instant::now()).await.unwrap();
    }
}
