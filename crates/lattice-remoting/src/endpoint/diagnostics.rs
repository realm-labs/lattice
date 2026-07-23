use std::{
    io::ErrorKind,
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
