use std::sync::Arc;
use std::time::{Duration, Instant};

use lattice_core::direct_link::errors::LinkError;
use lattice_core::direct_link::target::DirectLinkEndpoint;
use lattice_direct_link::inbound::{DirectLinkInboundRouter, InboundConnectionSender};
use lattice_direct_link::protocol::DirectLinkFrameKind;
use lattice_direct_link::transport::{
    DirectLinkConnection, DirectLinkTransport, TcpDirectLinkConnection, TcpDirectLinkTransport,
    TcpDirectLinkWriter,
};
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::config::DirectLinkConfig;
use crate::direct_links::DirectLinkServiceRuntime;
use crate::error::LatticeServiceError;

#[derive(Debug)]
pub(crate) struct ManagedDirectLinkListener {
    endpoint: DirectLinkEndpoint,
    pub(crate) shutdown: oneshot::Sender<()>,
    pub(crate) task: JoinHandle<Result<(), LatticeServiceError>>,
}

impl ManagedDirectLinkListener {
    pub(crate) fn endpoint(&self) -> DirectLinkEndpoint {
        self.endpoint.clone()
    }
}

pub(crate) async fn start_direct_link_listener(
    config: Option<DirectLinkConfig>,
    runtime: Option<DirectLinkServiceRuntime>,
) -> Result<Option<ManagedDirectLinkListener>, LatticeServiceError> {
    let Some(config) = config else {
        return Ok(None);
    };
    let listen_config =
        config
            .listen_config()
            .map_err(|message| LatticeServiceError::ComponentBuild {
                slot: "direct_links".to_string(),
                message,
            })?;
    let maintenance_interval = config.maintenance_interval_config();
    let connection_limit = config.max_connections_config();
    let transport = TcpDirectLinkTransport::new();
    let listener = transport.bind(listen_config).await.map_err(|error| {
        LatticeServiceError::ComponentBuild {
            slot: "direct_links".to_string(),
            message: error.to_string(),
        }
    })?;
    let endpoint = listener.local_endpoint();
    let inbound_router = runtime.map(|runtime| runtime.inbound_router());
    let connection_permits = connection_limit.map(|limit| Arc::new(Semaphore::new(limit)));
    let (shutdown, mut shutdown_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let mut maintenance = tokio::time::interval(maintenance_interval);
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    debug!("direct-link listener shutting down");
                    return Ok(());
                }
                _ = maintenance.tick(), if inbound_router.is_some() => {
                    if let Some(inbound_router) = &inbound_router
                        && let Err(error) = inbound_router.close_idle_links_at(Instant::now())
                    {
                        warn!(%error, "direct-link idle maintenance failed");
                    }
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok(mut connection) => {
                            let permit = if let Some(connection_permits) = &connection_permits {
                                match connection_permits.clone().try_acquire_owned() {
                                    Ok(permit) => Some(permit),
                                    Err(error) => {
                                        warn!(%error, "rejecting direct-link connection after connection limit reached");
                                        let _ = connection.close().await;
                                        continue;
                                    }
                                }
                            } else {
                                None
                            };
                            let inbound_router = inbound_router.clone();
                            let maintenance_interval = maintenance_interval;
                            tokio::spawn(async move {
                                let _permit = permit;
                                handle_direct_link_connection(
                                    connection,
                                    inbound_router,
                                    maintenance_interval,
                                )
                                .await;
                            });
                        }
                        Err(error) => {
                            return Err(LatticeServiceError::ComponentBuild {
                                slot: "direct_links".to_string(),
                                message: error.to_string(),
                            });
                        }
                    }
                }
            }
        }
    });
    Ok(Some(ManagedDirectLinkListener {
        endpoint,
        shutdown,
        task,
    }))
}

pub(crate) async fn handle_direct_link_connection(
    connection: TcpDirectLinkConnection,
    inbound_router: Option<Arc<DirectLinkInboundRouter>>,
    maintenance_interval: Duration,
) {
    let Some(inbound_router) = inbound_router else {
        let mut connection = connection;
        let _ = connection.close().await;
        return;
    };

    let (mut reader, mut writer) = connection.split();
    let (outbound_tx, mut outbound_rx) = mpsc::channel(1024);
    let mut registered_links = Vec::new();
    let mut heartbeat = tokio::time::interval(maintenance_interval);
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                if let Err(error) = write_due_direct_link_heartbeats(&mut writer, &inbound_router).await {
                    debug!(%error, "closing direct-link connection after heartbeat write failure");
                    break;
                }
            }
            outbound = outbound_rx.recv() => {
                let Some(frame) = outbound else {
                    break;
                };
                if let Err(error) = writer.write_frame(frame).await {
                    debug!(%error, "closing direct-link connection after outbound write failure");
                    break;
                }
            }
            frame = reader.read_frame() => {
                let frame = match frame {
                    Ok(frame) => frame,
                    Err(error) => {
                        debug!(%error, "closing direct-link connection after read failure");
                        break;
                    }
                };
                if frame.kind == DirectLinkFrameKind::OpenLink {
                    match inbound_router.process_open_link_frame(frame, None).await {
                        Ok(response) => {
                            if response.kind == DirectLinkFrameKind::OpenLinkAck {
                                let link_id = response.link_id.clone();
                                inbound_router.register_outbound_sender(
                                    link_id.clone(),
                                    Arc::new(InboundConnectionSender::new(
                                        link_id.clone(),
                                        outbound_tx.clone(),
                                    )),
                                );
                                registered_links.push(link_id);
                            }
                            if let Err(error) = writer.write_frame(response).await {
                                warn!(%error, "closing direct-link connection after open-link response write failure");
                                break;
                            }
                        }
                        Err(error) => {
                            warn!(%error, "closing direct-link connection after open-link handling failure");
                            break;
                        }
                    }
                } else if let Err(error) = inbound_router.process_frame(frame) {
                    warn!(%error, "closing direct-link connection after inbound delivery failure");
                    break;
                }
            }
        }
    }
    for link_id in registered_links {
        inbound_router.unregister_outbound_sender(&link_id);
    }
    let _ = writer.close().await;
}

async fn write_due_direct_link_heartbeats(
    writer: &mut TcpDirectLinkWriter,
    inbound_router: &DirectLinkInboundRouter,
) -> Result<(), LinkError> {
    for frame in inbound_router.heartbeat_frames_due_at(Instant::now()) {
        writer.write_frame(frame).await?;
    }
    Ok(())
}
