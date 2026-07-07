use super::*;
use std::collections::BTreeSet;

use lattice_core::{
    ActorId, ActorKind, ActorRef, BackpressurePolicy, DirectLinkMode, DirectLinkOptions,
    InstanceId, ServiceKind,
};

use crate::session::{
    DIRECT_LINK_PROTOCOL_VERSION, NegotiatedDirection, OpenLinkDirection, OpenLinkRejectReason,
};

#[test]
fn frame_codec_round_trips_message_frame() {
    let codec = DirectLinkFrameCodec::new(1024);
    let frame = DirectLinkFrame::message(
        LinkId::new("link-1"),
        LinkSequence(7),
        DirectLinkMessageId(42),
        b"payload".to_vec(),
    );

    let encoded = codec.encode(&frame).unwrap();
    let decoded = codec.decode(&encoded).unwrap();

    assert_eq!(decoded, frame);
}

#[test]
fn frame_codec_round_trips_target_to_source_message_frame() {
    let codec = DirectLinkFrameCodec::new(1024);
    let frame = DirectLinkFrame::directed_message(
        LinkId::new("link-1"),
        LinkDirection::TargetToSource,
        LinkSequence(7),
        DirectLinkMessageId(42),
        b"payload".to_vec(),
    );

    let encoded = codec.encode(&frame).unwrap();
    let decoded = codec.decode(&encoded).unwrap();

    assert_eq!(decoded, frame);
    assert_eq!(decoded.direction(), LinkDirection::TargetToSource);
}

#[test]
fn frame_codec_rejects_oversized_frames() {
    let codec = DirectLinkFrameCodec::new(8);
    let frame = DirectLinkFrame::message(
        LinkId::new("link-1"),
        LinkSequence(7),
        DirectLinkMessageId(42),
        b"payload".to_vec(),
    );

    assert_eq!(codec.encode(&frame), Err(FrameCodecError::FrameTooLarge));
}

#[test]
fn frame_codec_round_trips_open_link_handshake_frames() {
    let codec = DirectLinkFrameCodec::new(4096);
    let link_id = LinkId::new("link-open");
    let message_id = DirectLinkMessageId(11);
    let request = OpenLinkRequest {
        protocol_version: DIRECT_LINK_PROTOCOL_VERSION,
        link_id: link_id.clone(),
        source: test_actor_ref("Gateway", "GatewaySession", 99),
        target: test_actor_ref("World", "World", 7),
        mode: DirectLinkMode::Unidirectional,
        source_to_target: OpenLinkDirection {
            link_id: link_id.clone(),
            stream_name: "movement".to_string(),
            supported_message_type_ids: BTreeSet::from([message_id]),
        },
        target_to_source: None,
        options: DirectLinkOptions::default(),
    };

    let open_frame = DirectLinkFrame::open_link(&request).unwrap();
    let decoded_open_frame = codec.decode(&codec.encode(&open_frame).unwrap()).unwrap();
    let decoded_request = decoded_open_frame.decode_open_link().unwrap();
    assert_eq!(decoded_request.link_id, request.link_id);
    assert_eq!(decoded_request.source, request.source);
    assert_eq!(decoded_request.target, request.target);
    assert_eq!(
        decoded_request.source_to_target.supported_message_type_ids,
        BTreeSet::from([message_id])
    );

    let peer_identity = DirectLinkPeerIdentity::new(
        ServiceKind::new("Gateway"),
        InstanceId::new("instance-99"),
        "spiffe://lattice.test/svc/Gateway/instance/instance-99",
    );
    let authenticated_open_frame =
        DirectLinkFrame::open_link_with_peer_identity(&request, peer_identity.clone()).unwrap();
    let decoded_authenticated_frame = codec
        .decode(&codec.encode(&authenticated_open_frame).unwrap())
        .unwrap();
    let decoded_envelope = decoded_authenticated_frame
        .decode_open_link_envelope()
        .unwrap();
    assert_eq!(decoded_envelope.request.source, request.source);
    assert_eq!(decoded_envelope.peer_identity, Some(peer_identity));

    let ack = OpenLinkAck {
        link_id: link_id.clone(),
        source_to_target: NegotiatedDirection {
            direction: LinkDirection::SourceToTarget,
            stream_name: "movement".to_string(),
            accepted_message_type_ids: BTreeSet::from([message_id]),
            next_receive_sequence: LinkSequence(1),
            backpressure: BackpressurePolicy::FailFast { max_pending: 8 },
            closed: false,
        },
        target_to_source: None,
    };
    let ack_frame = DirectLinkFrame::open_link_ack(&ack).unwrap();
    let decoded_ack_frame = codec.decode(&codec.encode(&ack_frame).unwrap()).unwrap();
    assert_eq!(decoded_ack_frame.decode_open_link_ack().unwrap(), ack);

    let reject = OpenLinkReject::new(link_id, OpenLinkRejectReason::Unauthorized);
    let reject_frame = DirectLinkFrame::open_link_reject(&reject).unwrap();
    let decoded_reject_frame = codec.decode(&codec.encode(&reject_frame).unwrap()).unwrap();
    assert_eq!(
        decoded_reject_frame.decode_open_link_reject().unwrap(),
        reject
    );
}

fn test_actor_ref(service: &str, actor: &str, id: u64) -> ActorRef {
    ActorRef::direct(
        ServiceKind::new(service),
        ActorKind::new(actor),
        ActorId::U64(id),
        InstanceId::new("codec-test"),
        "tcp://127.0.0.1:1".parse().unwrap(),
        None,
    )
}
