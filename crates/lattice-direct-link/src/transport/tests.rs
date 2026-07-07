use crate::transport::*;

use lattice_core::direct_link::ids::{DirectLinkMessageId, LinkId, LinkSequence};

use crate::protocol::DirectLinkFrame;

#[tokio::test]
async fn tcp_transport_round_trips_frame() {
    let transport = TcpDirectLinkTransport::new();
    let listener = transport
        .bind(DirectLinkListenConfig {
            endpoint: DirectLinkEndpoint::new("tcp://127.0.0.1:0".parse().unwrap()),
            max_frame_size: 1024,
        })
        .await
        .unwrap();
    let endpoint = listener.local_endpoint();
    let accept = tokio::spawn(async move {
        let mut server = listener.accept().await.unwrap();
        server.read_frame().await.unwrap()
    });
    let mut client = transport.connect_physical(endpoint, 1024).await.unwrap();
    let frame = DirectLinkFrame::message(
        LinkId::new("link-1"),
        LinkSequence(1),
        DirectLinkMessageId(7),
        b"hello".to_vec(),
    );

    client.write_frame(frame.clone()).await.unwrap();

    assert_eq!(accept.await.unwrap(), frame);
    let metrics = transport.metrics().snapshot();
    assert_eq!(metrics.sent, 1);
    assert_eq!(metrics.received, 1);
}

#[tokio::test]
async fn tcp_outbound_connection_enforces_client_frame_limit() {
    let transport = TcpDirectLinkTransport::new();
    let listener = transport
        .bind(DirectLinkListenConfig {
            endpoint: DirectLinkEndpoint::new("tcp://127.0.0.1:0".parse().unwrap()),
            max_frame_size: 1024,
        })
        .await
        .unwrap();
    let endpoint = listener.local_endpoint();
    let server = tokio::spawn(async move {
        let mut server = listener.accept().await.unwrap();
        server
            .write_frame(DirectLinkFrame::message(
                LinkId::new("link-1"),
                LinkSequence(1),
                DirectLinkMessageId(7),
                vec![0; 128],
            ))
            .await
            .unwrap();
    });
    let mut client = transport.connect_physical(endpoint, 16).await.unwrap();

    let error = client.read_frame().await.unwrap_err();

    assert!(
        matches!(error, LinkError::Protocol(message) if message.contains("exceeds maximum size"))
    );
    server.await.unwrap();
}

#[tokio::test]
async fn tcp_reader_rejects_oversized_length_before_payload_allocation() {
    let (mut writer, mut reader) = tokio::io::duplex(64);
    writer.write_u32(1024).await.unwrap();

    let metrics = DirectLinkMetrics::default();
    let error = read_tcp_frame(&mut reader, DirectLinkFrameCodec::new(128), &metrics)
        .await
        .unwrap_err();

    assert!(
        matches!(error, LinkError::Protocol(message) if message.contains("exceeds maximum size"))
    );
    assert_eq!(metrics.snapshot().decode_errors, 1);
}
