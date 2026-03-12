use super::*;
use rkyv::{Archive, Deserialize, Serialize};
use std::{
    future::Future,
    pin::pin,
    sync::Arc,
    task::{Context as TaskContext, Poll, Wake, Waker},
};

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
struct DemoEvent {
    id: u64,
}

struct NoopWaker;

impl Wake for NoopWaker {
    fn wake(self: Arc<Self>) {}
}

fn poll_ready<F: Future>(future: F) -> F::Output {
    let waker = Waker::from(Arc::new(NoopWaker));
    let mut cx = TaskContext::from_waker(&waker);
    let mut future = pin!(future);
    if let Poll::Ready(output) = future.as_mut().poll(&mut cx) {
        output
    } else {
        panic!("future unexpectedly pending")
    }
}

#[test]
fn typed_publish_replay_round_trip() {
    let context = Context::new();
    context
        .create_channel(
            "demo.events",
            ChannelKind::Event,
            RetentionPolicy::default(),
        )
        .expect("create channel");
    context
        .publisher::<DemoEvent>("demo.events")
        .publish(DemoEvent { id: 7 })
        .expect("publish");

    let replay = context
        .replay_frames("demo.events", ReplayStart::Earliest, 10)
        .expect("replay frames");
    assert_eq!(replay[0].sequence, 1);
    assert_eq!(
        replay[0].headers.get("content-type"),
        Some(&"application/rkyv".to_string())
    );
    let value: DemoEvent = decode_rkyv(&replay[0].payload).expect("decode");
    let replay_bytes = context
        .replay_bytes("demo.events", ReplayStart::Earliest, 10)
        .expect("replay bytes");
    assert_eq!(replay_bytes[0], replay[0].payload);
    assert_eq!(value.id, 7);
}

#[test]
fn runtime_encoding_defaults_to_rkyv() {
    let context = Context::new();
    assert_eq!(context.runtime_settings().enforced_encoding, "rkyv");
}

#[test]
fn byte_subscriber_recv_frame_preserves_frame_metadata() {
    let io = CoreIo::new();
    io.create_channel("demo.events", ChannelConfig::default())
        .expect("create channel");

    let mut headers = BTreeMap::new();
    headers.insert(
        "external_account_ref".to_string(),
        "acct-ext-456".to_string(),
    );
    headers.insert("workload_key".to_string(), "camera/edge-a".to_string());
    io.publish("demo.events", headers.clone(), b"frame".to_vec())
        .expect("publish");

    let context = Context::builder().with_core_io(io).build();
    let mut subscriber = context
        .byte_subscriber("demo.events", ReplayStart::Earliest)
        .expect("subscribe");

    let frame = poll_ready(subscriber.recv_frame()).expect("recv frame");
    assert_eq!(frame.channel, "demo.events");
    assert_eq!(frame.sequence, 1);
    assert_eq!(frame.headers, headers);
    assert_eq!(frame.payload, b"frame".to_vec());
    assert!(frame.timestamp_ms > 0);
}

#[test]
fn external_consumer_can_handoff_from_replay_to_live_frames() {
    let io = CoreIo::new();
    io.create_channel("demo.events", ChannelConfig::default())
        .expect("create channel");

    let context = Context::builder().with_core_io(io.clone()).build();

    let mut first_headers = BTreeMap::new();
    first_headers.insert(
        "external_account_ref".to_string(),
        "acct-ext-456".to_string(),
    );
    first_headers.insert("workload_key".to_string(), "camera/edge-a".to_string());
    io.publish("demo.events", first_headers.clone(), b"frame-1".to_vec())
        .expect("publish first frame");

    let mut second_headers = first_headers.clone();
    second_headers.insert("module_id".to_string(), "vision.detector".to_string());
    io.publish("demo.events", second_headers.clone(), b"frame-2".to_vec())
        .expect("publish second frame");

    let replay = context
        .replay_frames("demo.events", ReplayStart::Sequence(1), 2)
        .expect("replay frames");
    assert_eq!(replay.len(), 2);
    assert_eq!(replay[0].sequence, 1);
    assert_eq!(replay[0].headers, first_headers);
    assert_eq!(replay[1].sequence, 2);
    assert_eq!(replay[1].headers, second_headers);

    let next_sequence = replay.last().map(|frame| frame.sequence + 1).unwrap_or(1);
    let mut subscriber = context
        .byte_subscriber("demo.events", ReplayStart::Sequence(next_sequence))
        .expect("subscribe from next sequence");

    let mut live_headers = BTreeMap::new();
    live_headers.insert(
        "external_account_ref".to_string(),
        "acct-ext-456".to_string(),
    );
    live_headers.insert("workload_key".to_string(), "camera/edge-a".to_string());
    live_headers.insert("module_id".to_string(), "vision.detector".to_string());
    io.publish("demo.events", live_headers.clone(), b"frame-3".to_vec())
        .expect("publish live frame");

    let live_frame = poll_ready(subscriber.recv_frame()).expect("recv live frame");
    assert_eq!(live_frame.sequence, next_sequence);
    assert_eq!(live_frame.headers, live_headers);
    assert_eq!(live_frame.payload, b"frame-3".to_vec());
}
