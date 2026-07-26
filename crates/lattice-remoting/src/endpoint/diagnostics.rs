use std::{
    io::{self, ErrorKind},
    net::SocketAddr,
    sync::atomic::{AtomicU64, Ordering},
};

use tokio::sync::broadcast::{self, error::RecvError};

use super::EndpointError;
use crate::{association::AssociationId, lane::LaneError, wire::WireError};

pub(super) fn observe_connection_result(result: &Result<(), EndpointError>) {
    static FAILURES: AtomicU64 = AtomicU64::new(0);
    let Err(error) = result else {
        return;
    };
    if is_peer_disconnect(error) {
        tracing::debug!(error = ?error, "inbound remoting peer disconnected");
        return;
    }
    let count = FAILURES.fetch_add(1, Ordering::Relaxed).saturating_add(1);
    if count == 1 || count.is_multiple_of(100) {
        tracing::warn!(
            connection_failure_count = count,
            error = ?error,
            "inbound remoting connection task failed (subsequent failures are aggregated)"
        );
    }
}

pub(super) fn is_peer_disconnect(error: &EndpointError) -> bool {
    let io = match error {
        EndpointError::Wire(WireError::Io(io))
        | EndpointError::Lane(LaneError::Wire(WireError::Io(io))) => io,
        _ => return false,
    };
    matches!(
        io.kind(),
        ErrorKind::UnexpectedEof
            | ErrorKind::ConnectionReset
            | ErrorKind::ConnectionAborted
            | ErrorKind::BrokenPipe
    )
}

/// How the accept loop recovers from a failed [`tokio::net::TcpListener::accept`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AcceptRecovery {
    /// The failure belongs to one inbound connection, so the next accept may run immediately.
    Immediate,
    /// The listener hit a transient shortage and must be retried after a backoff.
    Delayed,
    /// The listener itself can no longer produce connections.
    Fatal,
}

/// Descriptor and buffer exhaustion (`EMFILE`, `ENFILE`, `ENOBUFS`) carries no dedicated
/// [`ErrorKind`], so unrecognised kinds are retried: dropping the listener costs the node every
/// future inbound connection, and peers that must be dialled by us can never reconnect.
pub(super) fn classify_accept_failure(kind: ErrorKind) -> AcceptRecovery {
    match kind {
        ErrorKind::ConnectionAborted
        | ErrorKind::ConnectionReset
        | ErrorKind::ConnectionRefused
        | ErrorKind::Interrupted
        | ErrorKind::PermissionDenied
        | ErrorKind::TimedOut => AcceptRecovery::Immediate,
        ErrorKind::InvalidInput
        | ErrorKind::NotConnected
        | ErrorKind::AddrNotAvailable
        | ErrorKind::Unsupported => AcceptRecovery::Fatal,
        _ => AcceptRecovery::Delayed,
    }
}

#[derive(Debug, Default)]
pub(super) struct AcceptDiagnostics {
    connection_limit_rejections: AtomicU64,
    accept_failures: AtomicU64,
}

impl AcceptDiagnostics {
    pub(super) fn observe_connection_limit_rejection(&self, peer: SocketAddr) {
        let count = self
            .connection_limit_rejections
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if count == 1 || count.is_multiple_of(100) {
            tracing::warn!(
                connection_limit_rejection_count = count,
                peer = %peer,
                "inbound remoting connection shed at the connection cap (subsequent rejections are aggregated)"
            );
        }
    }

    pub(super) fn observe_accept_failure(&self, error: &io::Error, recovery: AcceptRecovery) {
        let count = self
            .accept_failures
            .fetch_add(1, Ordering::Relaxed)
            .saturating_add(1);
        if recovery == AcceptRecovery::Fatal {
            tracing::error!(
                accept_failure_count = count,
                error = ?error,
                "remoting listener stopped accepting inbound connections"
            );
        } else if count == 1 || count.is_multiple_of(100) {
            tracing::warn!(
                accept_failure_count = count,
                recovery = ?recovery,
                error = ?error,
                "inbound remoting accept failed (subsequent failures are aggregated)"
            );
        }
    }

    pub(super) fn connection_limit_rejections(&self) -> u64 {
        self.connection_limit_rejections.load(Ordering::Relaxed)
    }

    pub(super) fn accept_failures(&self) -> u64 {
        self.accept_failures.load(Ordering::Relaxed)
    }
}

pub(super) async fn wait_for_disconnect(
    receiver: &mut broadcast::Receiver<AssociationId>,
    association_id: AssociationId,
) {
    loop {
        match receiver.recv().await {
            Ok(received) if received == association_id => return,
            Ok(_) => {}
            Err(RecvError::Lagged(_)) | Err(RecvError::Closed) => return,
        }
    }
}
