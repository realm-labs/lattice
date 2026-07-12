#![cfg_attr(not(test), deny(clippy::wildcard_imports))]

use std::path::Path;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const DRAINING: &str = "/tmp/lattice-draining";
const DRAIN_ACK: &str = "/tmp/lattice-drain-ack";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:8080").await?;
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                tokio::spawn(async move {
                    let _ = respond(stream).await;
                });
            }
            () = shutdown_signal() => break,
        }
    }
    Ok(())
}

async fn respond(mut stream: TcpStream) -> Result<(), std::io::Error> {
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
        _ => false,
    };
    let (status, body) = if healthy {
        ("200 OK", "ready\n")
    } else {
        ("503 Service Unavailable", "draining\n")
    };
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
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
