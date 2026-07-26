use std::{sync::Arc, time::Duration};

#[cfg(feature = "tls")]
use crate::transport::{connect_tls_candidate, verify_peer_certificate_identity};
use crate::{
    bootstrap::{
        BootstrapProbeTarget, BootstrapPurpose, BootstrapRejectionCode, BootstrapRequest,
        BootstrapResponse, BootstrapResult, BootstrapRoute,
    },
    transport::{FramedConnection, connect_tcp},
    wire::{Frame, FrameCodec},
};

use super::{
    EndpointError, RemotingEndpoint, lifecycle::wait_for_shutdown, stream::EndpointStream,
};

impl RemotingEndpoint {
    pub async fn probe_candidate(
        self: &Arc<Self>,
        target: BootstrapProbeTarget,
    ) -> Result<BootstrapResponse, EndpointError> {
        let mut shutdown = self.shutdown_tx.subscribe();
        self.ensure_running()?;
        if target
            .expected_node_id
            .as_ref()
            .is_some_and(String::is_empty)
            || target
                .tls_server_name
                .as_ref()
                .is_some_and(String::is_empty)
        {
            return Err(EndpointError::InvalidBootstrapTarget);
        }
        let permit = self
            .connections
            .clone()
            .try_acquire_owned()
            .map_err(|_| EndpointError::ConnectionLimit)?;
        let result = tokio::select! {
            biased;
            () = wait_for_shutdown(&mut shutdown) => Err(EndpointError::ShuttingDown),
            result = tokio::time::timeout(
                self.config.connect_timeout,
                self.probe_candidate_inner(target),
            ) => result.map_err(|_| EndpointError::ConnectTimeout)?,
        };
        drop(permit);
        result
    }

    async fn probe_candidate_inner(
        &self,
        target: BootstrapProbeTarget,
    ) -> Result<BootstrapResponse, EndpointError> {
        let request = BootstrapRequest::new(
            target.scope,
            self.local.clone(),
            self.local.cluster_id.clone(),
            target.expected_node_id,
        );
        self.probe_request_inner(target.address, target.tls_server_name, request)
            .await
    }

    pub(super) async fn probe_request_inner(
        &self,
        address: lattice_core::actor_ref::NodeAddress,
        tls_server_name: Option<String>,
        request: BootstrapRequest,
    ) -> Result<BootstrapResponse, EndpointError> {
        let codec = FrameCodec::new(self.config.max_frame_size)?;
        #[cfg(feature = "tls")]
        let (mut connection, peer_certificate) = match &self.security {
            Some(security) => {
                let server_name = tls_server_name.unwrap_or_else(|| security.server_name.clone());
                let (connection, certificate) =
                    connect_tls_candidate(&address, server_name, security.client.clone(), codec)
                        .await?;
                (
                    FramedConnection::new(
                        EndpointStream::TlsClient(connection.into_inner()),
                        FrameCodec::new(self.config.max_frame_size)?,
                    ),
                    Some(certificate),
                )
            }
            None => (
                FramedConnection::new(
                    EndpointStream::Plain(connect_tcp(&address, codec).await?.into_inner()),
                    FrameCodec::new(self.config.max_frame_size)?,
                ),
                None,
            ),
        };
        #[cfg(not(feature = "tls"))]
        let mut connection = {
            let _ = tls_server_name;
            FramedConnection::new(
                EndpointStream::Plain(connect_tcp(&address, codec).await?.into_inner()),
                FrameCodec::new(self.config.max_frame_size)?,
            )
        };
        connection.write_frame(&request.to_frame()).await?;
        connection.flush().await?;
        let response = BootstrapResponse::from_frame(&connection.read_frame().await?)?;
        response.validate_for(&request)?;
        #[cfg(feature = "tls")]
        {
            if let (Some(certificate), Some(remote)) =
                (peer_certificate.as_deref(), response.remote_identity())
            {
                verify_peer_certificate_identity(certificate, remote)?;
            }
        }
        connection.close().await?;
        Ok(response)
    }

    pub(super) async fn accept_bootstrap(
        self: Arc<Self>,
        mut connection: FramedConnection<EndpointStream>,
        peer_certificate: Option<&[u8]>,
        first_frame: Frame,
    ) -> Result<(), EndpointError> {
        let request = BootstrapRequest::from_frame(&first_frame)?;
        #[cfg(feature = "tls")]
        let authentication_failed = peer_certificate.is_some_and(|certificate| {
            verify_peer_certificate_identity(certificate, &request.local).is_err()
        });
        #[cfg(not(feature = "tls"))]
        let authentication_failed = {
            let _ = peer_certificate;
            false
        };
        let mut response = if let Some(code) = request.rejection(&self.local) {
            BootstrapResponse::rejected(request.nonce, code)
        } else if authentication_failed {
            BootstrapResponse::rejected(
                request.nonce,
                BootstrapRejectionCode::AuthenticationFailure,
            )
        } else {
            self.bootstrap_response(&request)
        };
        if response.validate_for(&request).is_err() {
            response = BootstrapResponse::new(
                request.nonce,
                BootstrapResult::RetryAfter {
                    delay: Duration::from_secs(1),
                    reason: "bootstrap route is temporarily unavailable".to_string(),
                },
            );
        }
        if !matches!(&response.result, BootstrapResult::Rejected { .. }) {
            self.associations.replace_remote_incarnation(
                request.local.address.clone(),
                request.local.incarnation,
            );
        }
        let reverse_peer = match &response.result {
            BootstrapResult::ReverseDial { .. } => Some(request.local.clone()),
            _ => None,
        };
        connection.write_frame(&response.to_frame()).await?;
        connection.flush().await?;
        connection.close().await?;
        if let Some(peer) = reverse_peer {
            let endpoint = self.clone();
            self.spawn(async move {
                let _result = endpoint.connect_peer(peer).await;
                Ok(())
            })?;
        }
        Ok(())
    }

    fn bootstrap_response(&self, request: &BootstrapRequest) -> BootstrapResponse {
        if request.purpose == BootstrapPurpose::DirectPeer {
            let result = if self
                .associations
                .should_dial(&request.local.address, request.local.incarnation)
            {
                BootstrapResult::ReverseDial {
                    remote: self.local.clone(),
                    leader: None,
                }
            } else {
                BootstrapResult::Identity {
                    remote: self.local.clone(),
                    leader: None,
                }
            };
            return BootstrapResponse::new(request.nonce, result);
        }
        let route = self
            .bootstrap_handler
            .read()
            .expect("bootstrap handler lock poisoned")
            .route(request);
        let result = match route {
            BootstrapRoute::Accept { leader } => {
                if self
                    .associations
                    .should_dial(&request.local.address, request.local.incarnation)
                {
                    BootstrapResult::ReverseDial {
                        remote: self.local.clone(),
                        leader,
                    }
                } else {
                    BootstrapResult::Identity {
                        remote: self.local.clone(),
                        leader,
                    }
                }
            }
            BootstrapRoute::Redirect { leader } => BootstrapResult::Redirect {
                remote: self.local.clone(),
                leader,
            },
            BootstrapRoute::RetryAfter { delay, reason } => {
                BootstrapResult::RetryAfter { delay, reason }
            }
            BootstrapRoute::Reject { code } => BootstrapResult::Rejected { code },
        };
        BootstrapResponse::new(request.nonce, result)
    }
}
