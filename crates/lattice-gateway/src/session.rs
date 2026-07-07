use std::collections::HashMap;

use crate::error::GatewayError;
use crate::frame::ClientFrame;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GatewaySessionRef {
    pub session_id: String,
    pub connection_epoch: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GatewayPush {
    pub session: GatewaySessionRef,
    pub frame: ClientFrame,
}

#[derive(Debug, Default)]
pub struct GatewaySessionRegistry {
    sessions: HashMap<String, u64>,
}

impl GatewaySessionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn connect(&mut self, session_id: impl Into<String>) -> GatewaySessionRef {
        let session_id = session_id.into();
        let epoch = self.sessions.get(&session_id).copied().unwrap_or(0) + 1;
        self.sessions.insert(session_id.clone(), epoch);
        GatewaySessionRef {
            session_id,
            connection_epoch: epoch,
        }
    }

    pub fn validate_push(&self, push: &GatewayPush) -> Result<(), GatewayError> {
        match self.sessions.get(&push.session.session_id) {
            Some(epoch) if *epoch == push.session.connection_epoch => Ok(()),
            Some(current_epoch) => Err(GatewayError::StaleSession {
                session_id: push.session.session_id.clone(),
                expected_epoch: *current_epoch,
                actual_epoch: push.session.connection_epoch,
            }),
            None => Err(GatewayError::UnknownSession {
                session_id: push.session.session_id.clone(),
            }),
        }
    }
}
