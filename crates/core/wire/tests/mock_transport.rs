//! Smoke test exercising `selium-wire` patterns over a tokio duplex mock transport.

use std::pin::Pin;

use selium_encoding::FlatMsg;
use selium_wire::{
    FramedRead, FramedWrite, MessageTransport, Publisher, Subscriber,
    error::Result,
    frame::FrameHeader,
};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

struct MockTransport(DuplexStream);

impl AsyncRead for MockTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl AsyncWrite for MockTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

impl MessageTransport for MockTransport {
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<bool>> {
        std::task::Poll::Ready(Ok(true))
    }

    fn poll_peer_closed(
        self: Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<bool>> {
        std::task::Poll::Ready(Ok(false))
    }

    fn generation(&self) -> Result<u64> {
        Ok(0)
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Greeting(String);

impl FlatMsg for Greeting {
    fn encode(value: &Self) -> Vec<u8> {
        value.0.clone().into_bytes()
    }

    fn decode(bytes: &[u8]) -> std::result::Result<Self, flatbuffers::InvalidFlatbuffer> {
        Ok(Self(
            String::from_utf8(bytes.to_vec())
                .map_err(|_error| flatbuffers::InvalidFlatbuffer::ApparentSizeTooLarge)?,
        ))
    }
}

#[tokio::test]
async fn pubsub_round_trip_over_mock_transport() {
    let (client, server) = tokio::io::duplex(1024);

    let mut publisher: Publisher<Greeting, MockTransport> =
        Publisher::new(FramedWrite::new(MockTransport(client)));
    publisher.set_writer_id(7);

    let mut subscriber: Subscriber<Greeting, MockTransport> =
        Subscriber::new(FramedRead::new(MockTransport(server)), None);

    publisher
        .publish(&Greeting("hello".to_string()))
        .expect("publish greeting");

    let (received, writer_id) = subscriber.read_with_tag().expect("read greeting");
    assert_eq!(received, Greeting("hello".to_string()));
    assert_eq!(writer_id, 7);
}

#[test]
fn frame_header_round_trip() {
    let header = FrameHeader {
        len: 5,
        tag: 99,
        flags: FrameHeader::FLAG_READY,
        _reserved: [0; 3],
    };
    let encoded = header.encode();
    let decoded = FrameHeader::decode(&encoded).unwrap();
    assert_eq!(decoded, header);
}

#[test]
fn publisher_sink_start_send_over_mock_transport() {
    let (client, server) = tokio::io::duplex(1024);

    let mut publisher: Publisher<Greeting, MockTransport> =
        Publisher::new(FramedWrite::new(MockTransport(client)));

    use futures::SinkExt;
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        publisher
            .send(Greeting("sink".to_string()))
            .await
            .expect("sink send");
    });

    let mut subscriber: Subscriber<Greeting, MockTransport> =
        Subscriber::new(FramedRead::new(MockTransport(server)), None);
    let (received, _) = subscriber.read_with_tag().unwrap();
    assert_eq!(received, Greeting("sink".to_string()));
}
