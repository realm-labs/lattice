#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::{error::Error as StdError, io::Error as IoError, path::Path, sync::Arc};

use futures_util::StreamExt;
use lattice_core::coordinator::CoordinatorScope;
use lattice_discovery::provider::CoordinatorDiscovery;
use lattice_discovery_k8s::endpoint_slice::{
    KubernetesCredentials, KubernetesEndpointSliceConfig, KubernetesEndpointSliceDiscovery,
};
use serde::Serialize;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::RwLock,
};

const DRAINING: &str = "/tmp/lattice-draining";
const DRAIN_ACK: &str = "/tmp/lattice-drain-ack";

#[derive(Debug, Default, Serialize)]
struct DiscoveryView {
    generation: u64,
    targets: Vec<String>,
    last_error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();
    let discovery = Arc::new(RwLock::new(DiscoveryView::default()));
    spawn_endpoint_slice_watch(discovery.clone()).await?;
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let discovery = discovery.clone();
                tokio::spawn(async move {
                    let _ = respond(stream, discovery).await;
                });
            }
            () = shutdown_signal() => break,
        }
    }
    Ok(())
}

async fn spawn_endpoint_slice_watch(
    view: Arc<RwLock<DiscoveryView>>,
) -> Result<(), Box<dyn StdError>> {
    let namespace = std::env::var("POD_NAMESPACE").unwrap_or_else(|_| "default".to_owned());
    let discovery = KubernetesEndpointSliceDiscovery::connect(KubernetesEndpointSliceConfig {
        scope: CoordinatorScope::Membership,
        namespace,
        service: "lattice-probe".to_owned(),
        label_selector: None,
        port_name: "http".to_owned(),
        priority: 40,
        credentials: KubernetesCredentials::InCluster,
    })
    .await?;
    tokio::spawn(async move {
        let mut snapshots = discovery.snapshots();
        while let Some(item) = snapshots.next().await {
            let mut current = view.write().await;
            match item {
                Ok(snapshot) => {
                    current.generation = snapshot.generation;
                    current.targets = snapshot
                        .targets
                        .into_iter()
                        .map(|target| target.address.to_string())
                        .collect();
                    current.last_error = None;
                }
                Err(error) => current.last_error = Some(error.to_string()),
            }
        }
    });
    Ok(())
}

async fn respond(
    mut stream: TcpStream,
    discovery: Arc<RwLock<DiscoveryView>>,
) -> Result<(), IoError> {
    let mut request = [0_u8; 1024];
    let read = stream.read(&mut request).await?;
    let path = std::str::from_utf8(&request[..read])
        .ok()
        .and_then(|request| request.lines().next())
        .and_then(|line| line.split_whitespace().nth(1))
        .unwrap_or("/");
    let draining = Path::new(DRAINING).exists();
    if draining {
        let _ = std::fs::write(DRAIN_ACK, b"acknowledged\n");
    }
    let healthy = match path {
        "/startup" | "/live" | "/" => true,
        "/ready" => !draining,
        "/discovery" => !discovery.read().await.targets.is_empty(),
        _ => false,
    };
    let (status, body, content_type) = if path == "/discovery" {
        let body = serde_json::to_string(&*discovery.read().await)
            .unwrap_or_else(|_| "{\"last_error\":\"serialization\"}".to_owned());
        (
            if healthy {
                "200 OK"
            } else {
                "503 Service Unavailable"
            },
            body,
            "application/json",
        )
    } else if healthy {
        ("200 OK", "ready\n".to_owned(), "text/plain")
    } else {
        (
            "503 Service Unavailable",
            "draining\n".to_owned(),
            "text/plain",
        )
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};

    let mut terminate = signal(SignalKind::terminate()).expect("install SIGTERM handler");
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate.recv() => {}
    }
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}
