//! Bounded inbound acceptance, authentication and transfer to association supervision.
//!
//! The listener reserves connection capacity before launching a setup task. One absolute
//! deadline starts at TCP accept and covers TLS, the first frame, bootstrap or handshake,
//! catalogue exchange, and actor adoption. Each step observes endpoint shutdown.
//!
//! Setup never takes a lane receiver. Once identity and dial direction are valid, it transfers
//! the negotiated socket and its permit to the actor. Until that transfer succeeds, cancellation
//! or rejection destroys the candidate locally. Bootstrap sockets remain short-lived setup work.

use super::{
    ACCEPT_BACKOFF_MAX, ACCEPT_BACKOFF_MIN, EndpointError, RemotingEndpoint,
    diagnostics::{AcceptRecovery, classify_accept_failure, observe_connection_result},
    lifecycle::during_setup,
    stream::EndpointStream,
    supervision::InboundConnection,
};
#[cfg(feature = "tls")]
use crate::transport::verify_peer_certificate_identity;
use crate::{
    handshake::HandshakeValidator,
    transport::{FramedConnection, negotiate_inbound_from_frame},
    wire::{FrameCodec, FrameKind, WireError},
};
use std::sync::Arc;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::OwnedSemaphorePermit,
    task::JoinSet,
    time::Instant,
};
#[cfg(feature = "tls")]
use tokio_rustls::TlsAcceptor;

impl RemotingEndpoint {
    /// Accepts sockets under a permit cap and retains setup tasks in a listener-owned JoinSet.
    ///
    /// Recoverable accept failures are retried with bounded delay where needed. Successful
    /// adoption transfers ownership away from setup; shutdown joins the remaining setup tasks.
    pub(super) async fn accept_loop(
        self: Arc<Self>,
        listener: TcpListener,
    ) -> Result<(), EndpointError> {
        let mut shutdown = self.shutdown_tx.subscribe();
        if *shutdown.borrow() {
            return Ok(());
        }
        let mut connections = JoinSet::new();
        let mut accept_backoff = ACCEPT_BACKOFF_MIN;
        loop {
            tokio::select! {
                biased;
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                completed = connections.join_next(), if !connections.is_empty() => {
                    if let Some(result) = completed {
                        let connection_result = result.map_err(EndpointError::Join)?;
                        observe_connection_result(&connection_result);
                    }
                }
                accepted = listener.accept() => {
                    let (stream, peer) = match accepted {
                        Ok(accepted) => accepted,
                        Err(error) => {
                            let recovery = classify_accept_failure(error.kind());
                            self.accept_diagnostics.observe_accept_failure(&error, recovery);
                            if recovery == AcceptRecovery::Fatal {
                                return Err(WireError::Io(error).into());
                            }
                            if recovery == AcceptRecovery::Delayed {
                                tokio::select! {
                                    biased;
                                    changed = shutdown.changed() => {
                                        if changed.is_err() || *shutdown.borrow() {
                                            break;
                                        }
                                    }
                                    () = tokio::time::sleep(accept_backoff) => {}
                                }
                                accept_backoff =
                                    accept_backoff.saturating_mul(2).min(ACCEPT_BACKOFF_MAX);
                            }
                            continue;
                        }
                    };
                    accept_backoff = ACCEPT_BACKOFF_MIN;
                    let setup_deadline = tokio::time::Instant::now() + self.config.establishing_timeout;
                    let Ok(permit) = self.connections.clone().try_acquire_owned() else {
                        drop(stream);
                        self.accept_diagnostics.observe_connection_limit_rejection(peer);
                        continue;
                    };
                    let endpoint = self.clone();
                    connections.spawn(async move {
                        endpoint.accept_connection(stream, setup_deadline, permit).await
                    });
                }
            }
        }
        // Every connection owns a receiver for the endpoint-wide shutdown watch and can leave
        // cleanly. Let those tasks observe the fence instead of aborting them immediately:
        // aborting synchronously drops their nested async state on a Tokio worker stack.
        while let Some(result) = connections.join_next().await {
            let connection_result = result.map_err(EndpointError::Join)?;
            observe_connection_result(&connection_result);
        }
        Ok(())
    }

    /// Validates one accepted socket and dispatches bootstrap or exact-generation adoption.
    ///
    /// Uses the same deadline through all stages so a slow peer cannot restart its setup budget.
    async fn accept_connection(
        self: Arc<Self>,
        stream: TcpStream,
        setup_deadline: Instant,
        permit: OwnedSemaphorePermit,
    ) -> Result<(), EndpointError> {
        let mut shutdown = self.shutdown_tx.subscribe();
        if *shutdown.borrow() {
            return Ok(());
        }
        let validator = HandshakeValidator::new(
            self.local.clone(),
            self.config.max_frame_size,
            self.config.bulk_stripes,
        )?;
        stream.set_nodelay(true).map_err(WireError::Io)?;
        #[cfg(feature = "tls")]
        let (stream, peer_certificate) = if let Some(security) = &self.security {
            let Some(stream) = during_setup(&mut shutdown, setup_deadline, async {
                TlsAcceptor::from(security.server.clone())
                    .accept(stream)
                    .await
                    .map_err(|_| WireError::Tls("server handshake failed"))
            })
            .await?
            else {
                return Ok(());
            };
            let certificate = stream
                .get_ref()
                .1
                .peer_certificates()
                .and_then(|certificates| certificates.first())
                .map(|certificate| certificate.as_ref().to_vec())
                .ok_or(WireError::Tls("peer certificate missing"))?;
            (EndpointStream::TlsServer(stream), Some(certificate))
        } else {
            (EndpointStream::Plain(stream), None)
        };
        #[cfg(not(feature = "tls"))]
        let (stream, peer_certificate) = (EndpointStream::Plain(stream), Option::<Vec<u8>>::None);
        let mut connection =
            FramedConnection::new(stream, FrameCodec::new(self.config.max_frame_size)?);
        let Some(first_frame) =
            during_setup(&mut shutdown, setup_deadline, connection.read_frame()).await?
        else {
            return Ok(());
        };
        if first_frame.kind == FrameKind::BootstrapRequest {
            return during_setup(
                &mut shutdown,
                setup_deadline,
                self.accept_bootstrap(connection, peer_certificate.as_deref(), first_frame),
            )
            .await
            .map(|_| ());
        }
        let Some((handshake, peer_catalogue)) = during_setup(
            &mut shutdown,
            setup_deadline,
            negotiate_inbound_from_frame(
                &mut connection,
                first_frame,
                &validator,
                &self.catalogue,
                self.config.max_protocols_per_peer,
            ),
        )
        .await?
        else {
            return Ok(());
        };
        #[cfg(feature = "tls")]
        {
            if let Some(certificate) = peer_certificate.as_deref() {
                verify_peer_certificate_identity(certificate, &handshake.source)?;
            }
        }
        if self
            .associations
            .should_dial(&handshake.source.address, handshake.source.incarnation)
        {
            return Err(EndpointError::WrongDialDirection);
        }
        let association = self.associations.get_or_accept(
            handshake.source.cluster_id.clone(),
            handshake.source.address.clone(),
            handshake.source.incarnation,
            handshake.association_id,
        )?;
        self.supervisor_for(association, handshake.source.clone())?
            .accept(InboundConnection {
                stream: connection.into_inner(),
                handshake,
                peer_catalogue,
                authenticated: peer_certificate.is_some(),
                deadline: setup_deadline,
                permit,
            })
            .await
    }
}
