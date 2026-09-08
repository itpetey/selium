//! Per-stream bridge channel system guest.
//!
//! A bridge channel bridges exactly one external QUIC stream to exactly one
//! fabric channel. It receives a relayed two-ring byte-channel region id as its
//! entrypoint argument (delivered by `bridge-server` via the queue handoff),
//! reads a typed handshake frame naming the fabric channel URI, resolves and
//! attaches that channel, and then splices `selium-wire` frames between the
//! relayed byte stream and the fabric ring — preserving correlation tags,
//! flags, and payload bytes verbatim (it never inspects application payloads).
//!
//! Closing either end tears down the whole pipe: a client FIN drops the fabric
//! membership and exits; a fabric close finishes the client's stream and exits.
//!
//! This crate is a deployable guest, but its core ([`bridge_pipe`]) is
//! dependency-injected (the channel resolver) and exercised natively by unit
//! tests with the heap region provider, mirroring the connector's `stream.rs`.

use std::{
    pin::Pin,
    task::{Context as TaskContext, Poll},
};

use anyhow::Context as _;
use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::{decode_rkyv, encode_rkyv};
use selium_guest::{
    Context, GuestError, Result, entrypoint, info, mark_ready,
    net::{
        ByteStream,
        bytes::{ByteStreamReader, ByteStreamWriter},
    },
};
use selium_shm::{
    Channel,
    channels::{BlockingReader, BlockingWriter},
};
use selium_wire::{
    MessageTransport,
    error::Error as WireError,
    framed::{FramedRead, FramedWrite},
};

const OWN_RING_READERS: u64 = 1;
/// The pipe's own contribution to the fabric ring's member counts: its
/// single counting writer (the write adapter's) and single blocking reader
/// (the read adapter's). "All inner peers gone" is observable as
/// `writer_count == OWN_RING_WRITERS && reader_count == OWN_RING_READERS`.
///
/// Count-based liveness cannot distinguish "every inner peer left" from "no
/// inner peer ever attached": a pipe bridging a memberless fabric finishes
/// the client's stream (a clean FIN the client can retry) rather than
/// parking forever. Non-blocking reader-only inner peers are invisible to
/// the reader count; writers and blocking readers — the norms for fabric
/// members — are both counted.
const OWN_RING_WRITERS: u64 = 1;
pub const TERMINATE_ATTACH_FAILED: u32 = 2;
/// Termination codes carried by [`PipeControl::Terminate`].
pub const TERMINATE_BAD_HANDSHAKE: u32 = 1;

/// Typed per-stream control frames.
///
/// Layered on the `selium-wire` codec (an rkyv payload carried as a normal
/// frame with tag 0 before data relay begins). Data frames are relayed
/// verbatim and are never decoded as control frames.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum PipeControl {
    /// A client request to bridge the given channel URI.
    Handshake {
        /// Discovery URI of the fabric channel to bridge.
        uri: String,
    },
    /// A terminal failure reply; the stream closes after it.
    Terminate {
        /// Machine-readable termination code.
        code: u32,
    },
}

/// A [`MessageTransport`] adapting the read half of the relayed byte stream.
///
/// [`FramedRead`] only exercises the read side, so the write side is stubbed.
struct StreamReadTransport {
    reader: ByteStreamReader,
}

/// A [`MessageTransport`] adapting the write half of the relayed byte stream.
///
/// [`FramedWrite`] only exercises the write side, so the read side is stubbed.
struct StreamWriteTransport {
    writer: ByteStreamWriter,
}

/// A read-only [`MessageTransport`] over the fabric ring's blocking reader.
///
/// Holds **no writer** on the ring: the pipe's single counting writer lives
/// in [`RingWriteTransport`], so the pipe's `writer_count` contribution is
/// exactly one and "all inner writers gone" stays observable. The write
/// side is stubbed — [`FramedRead`] only exercises the read side.
struct RingReadTransport {
    reader: BlockingReader,
}

/// A write-only [`MessageTransport`] over the fabric ring's blocking writer.
///
/// This is the pipe's **only counting writer** on the fabric ring: inner
/// guests observe the pipe's membership (and its death, as a `writer_count`
/// drop) through it. The read side is stubbed — [`FramedWrite`] only
/// exercises the write side.
struct RingWriteTransport {
    writer: BlockingWriter,
}

impl PipeControl {
    /// Encodes the control frame to its framed payload bytes.
    pub fn encode(&self) -> Vec<u8> {
        encode_rkyv(self).expect("encode PipeControl")
    }

    /// Decodes a control frame from framed payload bytes.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        decode_rkyv(bytes).ok()
    }
}

impl tokio::io::AsyncRead for StreamReadTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for StreamReadTransport {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Err(std::io::ErrorKind::Unsupported.into()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl MessageTransport for StreamReadTransport {
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<selium_wire::Result<bool>> {
        Poll::Ready(Ok(true))
    }

    fn poll_peer_closed(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<selium_wire::Result<bool>> {
        Poll::Ready(Ok(false))
    }

    fn generation(&self) -> selium_wire::Result<u64> {
        Ok(0)
    }
}

impl tokio::io::AsyncRead for StreamWriteTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(std::io::ErrorKind::Unsupported.into()))
    }
}

impl tokio::io::AsyncWrite for StreamWriteTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}

impl MessageTransport for StreamWriteTransport {
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<selium_wire::Result<bool>> {
        Poll::Ready(Ok(true))
    }

    fn poll_peer_closed(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<selium_wire::Result<bool>> {
        Poll::Ready(Ok(false))
    }

    fn generation(&self) -> selium_wire::Result<u64> {
        Ok(0)
    }
}

impl tokio::io::AsyncRead for RingReadTransport {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.reader).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for RingReadTransport {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        _buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Poll::Ready(Err(std::io::ErrorKind::Unsupported.into()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl MessageTransport for RingReadTransport {
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<selium_wire::Result<bool>> {
        Poll::Ready(Ok(true))
    }

    fn poll_peer_closed(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<selium_wire::Result<bool>> {
        Poll::Ready(Ok(false))
    }

    fn generation(&self) -> selium_wire::Result<u64> {
        Ok(0)
    }
}

impl tokio::io::AsyncRead for RingWriteTransport {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
        _buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Ready(Err(std::io::ErrorKind::Unsupported.into()))
    }
}

impl tokio::io::AsyncWrite for RingWriteTransport {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.writer).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.writer).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.writer).poll_shutdown(cx)
    }
}

impl MessageTransport for RingWriteTransport {
    type Error = std::io::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<selium_wire::Result<bool>> {
        Poll::Ready(Ok(true))
    }

    fn poll_peer_closed(
        self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<selium_wire::Result<bool>> {
        Poll::Ready(Ok(false))
    }

    fn generation(&self) -> selium_wire::Result<u64> {
        Ok(0)
    }
}

/// The bridge pipe core: handshake → resolve/attach → splice → teardown.
///
/// `resolve` maps a channel URI to that channel's shared region id (the
/// discovery lookup in production). The function stays generic for the native
/// test seam; the entrypoint supplies the discovery-backed resolver.
pub async fn bridge_pipe<Resolve, Fut>(stream: ByteStream, resolve: Resolve)
where
    Resolve: FnOnce(String) -> Fut,
    Fut: std::future::Future<Output = Result<u64>>,
{
    let (reader, writer) = stream.split();
    let mut stream_read = FramedRead::new(StreamReadTransport { reader });
    let mut stream_write = FramedWrite::new(StreamWriteTransport { writer });

    // 1. Typed pipe handshake: the first stream frame names the channel URI.
    let Some((handshake_payload, _, _)) = next_frame(&mut stream_read).await else {
        // Client closed before the handshake; nothing more to do.
        return;
    };
    let uri = match PipeControl::decode(&handshake_payload) {
        Some(PipeControl::Handshake { uri }) => uri,
        _ => {
            terminate(&mut stream_write, TERMINATE_BAD_HANDSHAKE).await;
            return;
        }
    };

    // 2. Resolve + attach the fabric channel ring. Resolution is
    // tenant-scoped by the bridge channel's grants.
    let region_id = match resolve(uri).await {
        Ok(region_id) => region_id,
        Err(_) => {
            terminate(&mut stream_write, TERMINATE_ATTACH_FAILED).await;
            return;
        }
    };
    let channel = match Channel::attach(region_id) {
        Ok(channel) => channel,
        Err(_) => {
            terminate(&mut stream_write, TERMINATE_ATTACH_FAILED).await;
            return;
        }
    };

    // Fabric ring adapters, split read/write so the pipe contributes exactly
    // ONE counting writer (the write transport's): inner guests see the
    // pipe's membership via writer_count (its death is visible as a count
    // drop), and the pipe can observe "all inner writers gone" as
    // writer_count == OWN_RING_WRITERS (only itself remains).
    let ring_read = match channel.blocking_reader() {
        Ok(reader) => FramedRead::new(RingReadTransport { reader }),
        Err(_) => {
            terminate(&mut stream_write, TERMINATE_ATTACH_FAILED).await;
            return;
        }
    };
    let ring_write = match channel.blocking_writer() {
        Ok(writer) => FramedWrite::new(RingWriteTransport { writer }),
        Err(_) => {
            terminate(&mut stream_write, TERMINATE_ATTACH_FAILED).await;
            return;
        }
    };

    // 3. Splice until either half closes; `select!` cancels the loser, whose
    //    dropped halves release the fabric membership / finish the stream.
    let to_ring = pump_stream_to_ring(stream_read, ring_write);
    let to_stream = pump_ring_to_stream(ring_read, stream_write, channel);
    tokio::select! {
        _ = to_ring => {}
        _ = to_stream => {}
    }
}

/// Bridge channel entrypoint.
///
/// Arguments: the bootstrap discovery `Context` (built by the entrypoint
/// macro, used for channel-URI resolution) and the relayed byte-channel
/// region `shared_id` (delivered by `bridge-server`).
#[entrypoint]
async fn bridge_channel(mut ctx: Context, shared_id: u64) -> anyhow::Result<()> {
    drop(selium_guest::log::init());
    info!("bridge-channel: started");

    let stream = ByteStream::attach_blocking(shared_id)
        .with_context(|| "bridge-channel: attach stream region failed")?;

    mark_ready();

    bridge_pipe(stream, move |uri| async move {
        let target = ctx
            .lookup(&uri)
            .await?
            .ok_or_else(|| GuestError::Host(format!("channel not found: {uri}")))?;
        Ok(target.resource_id)
    })
    .await;

    Ok(())
}

/// Reads the next complete frame, yielding between attempts.
async fn next_frame<M: MessageTransport>(reader: &mut FramedRead<M>) -> Option<(Vec<u8>, u32, u8)> {
    loop {
        match reader.read_frame() {
            Ok(frame) => return Some(frame),
            Err(WireError::BufferEmpty) => selium_guest::yield_now().await,
            Err(_) => return None,
        }
    }
}

/// Copies frames from the fabric ring to the relayed stream (fabric → client).
///
/// The fabric half ends when the ring read errors, or when the ring quiesces:
/// no data is pending and only the pipe's own members remain (every inner
/// writer and blocking reader is gone). `fabric` is moved in so the quiesce
/// check can read the live member counts.
async fn pump_ring_to_stream(
    mut ring_read: FramedRead<RingReadTransport>,
    mut stream_write: FramedWrite<StreamWriteTransport>,
    fabric: Channel,
) {
    // Fabric close ends the loop; dropping `stream_write` finishes the client
    // stream.
    loop {
        match ring_read.read_frame() {
            Ok((payload, tag, flags)) => {
                if stream_write
                    .write_frame_with_flags_async(&payload, tag, flags)
                    .await
                    .is_err()
                {
                    break;
                }
            }
            Err(WireError::BufferEmpty) => {
                // No data pending. If only the pipe's own members remain,
                // the fabric is closed: every inner writer and blocking
                // reader is gone, so nothing further can ever arrive.
                // Finish the client's stream instead of parking in
                // BufferEmpty forever.
                let writers = fabric.ring().region().load_writer_count().unwrap_or(0);
                let readers = fabric.ring().region().read_reader_count().unwrap_or(0);
                if writers <= OWN_RING_WRITERS && readers <= OWN_RING_READERS {
                    break;
                }
                selium_guest::yield_now().await;
            }
            Err(_) => break,
        }
    }
}

/// Copies frames from the relayed stream to the fabric ring (client → fabric).
async fn pump_stream_to_ring(
    mut stream_read: FramedRead<StreamReadTransport>,
    mut ring_write: FramedWrite<RingWriteTransport>,
) {
    // Client FIN (stream EOF) ends the loop; dropping `ring_write` releases
    // the fabric membership.
    while let Some((payload, tag, flags)) = next_frame(&mut stream_read).await {
        if ring_write
            .write_frame_with_flags_async(&payload, tag, flags)
            .await
            .is_err()
        {
            break;
        }
    }
}

/// Sends a termination frame, best-effort (the writer is dropped after, closing
/// the stream and surfacing EOF to the connector).
async fn terminate(stream_write: &mut FramedWrite<StreamWriteTransport>, code: u32) {
    let payload = PipeControl::Terminate { code }.encode();
    drop(stream_write.write_frame(&payload, 0));
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_abi::RegionProt;
    use selium_memory::FrameHeader;
    use selium_shm::{byte_channel, transport::ShmTransport};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn setup() {
        drop(selium_memory::set_region_provider(Box::new(
            selium_memory::HeapRegionProvider::new(),
        )));
    }

    fn connector_peer(
        shared_id: u64,
        ring_from_guest: &Channel,
        ring_to_guest: &Channel,
    ) -> ByteStream {
        let region = selium_memory::region_provider()
            .expect("provider")
            .attach(shared_id, None, RegionProt::ReadWrite)
            .expect("attach");
        ByteStream::from_ring_channels(ring_from_guest, ring_to_guest, region, true)
            .expect("connector peer")
    }

    /// 5.2: the delivered region attaches as a `ByteStream`; bytes round-trip
    /// against a connector-style peer half (mirrors the connector `stream.rs`).
    #[tokio::test]
    async fn delivered_region_round_trips_bytes() {
        setup();
        let (ring_to_guest, ring_from_guest, shared_id, _region) =
            byte_channel::create(4096, 4096).expect("create");
        let mut peer = connector_peer(shared_id, &ring_from_guest, &ring_to_guest);
        let mut guest = ByteStream::attach_blocking(shared_id).expect("guest attach");

        peer.write_all(b"request").await.expect("peer write");
        let mut buf = [0u8; 7];
        guest.read_exact(&mut buf).await.expect("guest read");
        assert_eq!(&buf, b"request");

        guest.write_all(b"response").await.expect("guest write");
        let mut buf = [0u8; 8];
        peer.read_exact(&mut buf).await.expect("peer read");
        assert_eq!(&buf, b"response");
    }

    /// 5.5: transport-agnostic framing + channel creation work through the
    /// heap region provider.
    #[test]
    fn channel_and_transport_attach_through_heap_provider() {
        setup();
        let fabric = Channel::create_with_backpressure(
            4096,
            selium_shm::ChannelBackpressure::Park,
            selium_abi::ResourceKind::SharedMemory,
        )
        .expect("create fabric channel");
        let read = ShmTransport::new(&fabric, &fabric).expect("read transport");
        let write = ShmTransport::new(&fabric, &fabric).expect("write transport");

        let mut reader = FramedRead::new(read);
        let mut writer = FramedWrite::new(write);

        writer.write_frame(b"ping", 42).expect("write frame");
        let (payload, tag, flags) = reader.read_frame().expect("read frame");
        assert_eq!(payload, b"ping");
        assert_eq!(tag, 42);
        assert_ne!(flags & FrameHeader::FLAG_READY, 0);
    }

    /// 5.1/5.3 + 5.5: full handshake, resolve-and-attach, tagged frame
    /// round-trip, and client-FIN teardown of the whole pipe.
    #[tokio::test]
    async fn splice_preserves_tags_and_client_fin_tears_down() {
        setup();

        // Fabric channel bridged by the guest, and its inner reader.
        let fabric = Channel::create_with_backpressure(
            4096,
            selium_shm::ChannelBackpressure::Park,
            selium_abi::ResourceKind::SharedMemory,
        )
        .expect("fabric channel");
        let fabric_region_id = fabric.region_id();
        let inner_transport = ShmTransport::new(&fabric, &fabric).expect("inner transport");
        let mut inner_read = FramedRead::new(inner_transport);

        // Relay byte channel between the "connector" (this test) and the guest.
        let (ring_to_guest, ring_from_guest, shared_id, _region) =
            byte_channel::create(65_536, 65_536).expect("create");
        let mut peer = connector_peer(shared_id, &ring_from_guest, &ring_to_guest);
        let guest_stream = ByteStream::attach_blocking(shared_id).expect("guest attach");

        // Drive the guest pipe against the fixed fabric channel.
        let guest_task = tokio::spawn(bridge_pipe(guest_stream, move |_uri| async move {
            Ok(fabric_region_id)
        }));

        // Client sends the typed handshake frame, then a data frame (tag 7).
        let handshake = PipeControl::Handshake {
            uri: "sel://acme/lobby".to_string(),
        }
        .encode();
        write_raw_frame(&mut peer, &handshake, 0, FrameHeader::FLAG_READY)
            .await
            .expect("write handshake");

        let payload = vec![0x42u8; 32];
        write_raw_frame(&mut peer, &payload, 7, FrameHeader::FLAG_READY)
            .await
            .expect("write data");

        // The inner fabric peer observes the frame with tag/flags/payload intact.
        let (relayed, tag, flags) = loop {
            match inner_read.read_frame() {
                Ok(frame) => break frame,
                Err(WireError::BufferEmpty) => tokio::task::yield_now().await,
                Err(e) => panic!("inner read: {e}"),
            }
        };
        assert_eq!(relayed, payload, "payload preserved end-to-end");
        assert_eq!(tag, 7, "correlation tag preserved");
        assert_ne!(flags & FrameHeader::FLAG_READY, 0, "flags preserved");

        // Client FIN: the guest pump exits and the whole pipe tears down.
        drop(peer);
        tokio::time::timeout(std::time::Duration::from_secs(5), guest_task)
            .await
            .expect("guest task completes within timeout")
            .expect("guest task succeeds");
    }

    /// 5.4: a denied/broken resolve yields a typed termination frame.
    #[tokio::test]
    async fn denied_attach_sends_termination_frame() {
        setup();

        let (ring_to_guest, ring_from_guest, shared_id, _region) =
            byte_channel::create(65_536, 65_536).expect("create");
        let mut peer = connector_peer(shared_id, &ring_from_guest, &ring_to_guest);
        let guest_stream = ByteStream::attach_blocking(shared_id).expect("guest attach");

        // Resolver always fails (channel not found / denied).
        let guest_task = tokio::spawn(bridge_pipe(guest_stream, |_uri| async move {
            Err(GuestError::Host("denied".to_string()))
        }));

        // Send a valid handshake; the guest replies with a termination frame
        // then closes the stream.
        let handshake = PipeControl::Handshake {
            uri: "sel://acme/forbidden".to_string(),
        }
        .encode();
        write_raw_frame(&mut peer, &handshake, 0, FrameHeader::FLAG_READY)
            .await
            .expect("write handshake");

        tokio::time::timeout(std::time::Duration::from_secs(5), guest_task)
            .await
            .expect("guest task completes")
            .expect("guest task ok");

        // Read the termination frame back out of the stream bytes.
        let mut buf = Vec::new();
        peer.read_to_end(&mut buf).await.expect("read stream");

        // Buffer holds the wire bytes: [byte-channel header was already stripped
        // by the peer's ByteStream reader], so `buf` contains the raw
        // frame the guest sent: [FrameHeader][PipeControl::Terminate payload].
        assert!(
            buf.len() >= FrameHeader::ENCODED_SIZE,
            "termination frame present"
        );
        let header = FrameHeader::decode(&buf[..FrameHeader::ENCODED_SIZE]).expect("frame header");
        assert_eq!(header.tag, 0);
        let payload = &buf[FrameHeader::ENCODED_SIZE..];
        let control = PipeControl::decode(payload).expect("control frame");
        assert_eq!(
            control,
            PipeControl::Terminate {
                code: TERMINATE_ATTACH_FAILED
            }
        );
    }

    /// 6.2: killing a bridge-channel (dropping its fabric writer) surfaces
    /// `writer_count == 0` to inner guests, without affecting sibling pipes.
    #[test]
    fn dropped_bridge_writer_surfaces_zero_writer_count_without_sibling_effect() {
        setup();
        let fabric = Channel::create_with_backpressure(
            4096,
            selium_shm::ChannelBackpressure::Park,
            selium_abi::ResourceKind::SharedMemory,
        )
        .expect("fabric channel");
        let sibling = Channel::create_with_backpressure(
            4096,
            selium_shm::ChannelBackpressure::Park,
            selium_abi::ResourceKind::SharedMemory,
        )
        .expect("sibling channel");

        // A bridge-channel registers its fabric writer (writer_count = 1).
        let writer = fabric.blocking_writer().expect("fabric writer");
        assert_eq!(
            fabric.ring().region().load_writer_count().expect("count"),
            1
        );
        assert_eq!(
            sibling.ring().region().load_writer_count().expect("count"),
            0
        );

        // Killing the bridge-channel releases the writer.
        drop(writer);
        assert_eq!(
            fabric.ring().region().load_writer_count().expect("count"),
            0,
            "inner guests observe writer_count == 0 after a bridge-channel dies"
        );
        assert_eq!(
            sibling.ring().region().load_writer_count().expect("count"),
            0,
            "other pipes are unaffected"
        );
    }

    /// 5.6 (teardown, fabric-close direction): when the fabric channel
    /// closes (all inner writers gone), the bridge-channel finishes the
    /// client's stream and the whole pipe terminates — the client peer
    /// observes the final frame followed by EOF, not a hang.
    #[tokio::test]
    async fn fabric_close_finishes_client_stream_and_tears_down() {
        setup();

        // Fabric channel bridged by the guest, with an inner writer peer.
        let fabric = Channel::create_with_backpressure(
            4096,
            selium_shm::ChannelBackpressure::Park,
            selium_abi::ResourceKind::SharedMemory,
        )
        .expect("fabric channel");
        let fabric_region_id = fabric.region_id();
        let mut inner_writer = fabric.blocking_writer().expect("inner writer");

        // Relay byte channel between the "connector" (this test) and the guest.
        let (ring_to_guest, ring_from_guest, shared_id, _region) =
            byte_channel::create(65_536, 65_536).expect("create");
        let mut peer = connector_peer(shared_id, &ring_from_guest, &ring_to_guest);
        let guest_stream = ByteStream::attach_blocking(shared_id).expect("guest attach");

        let guest_task = tokio::spawn(bridge_pipe(guest_stream, move |_uri| async move {
            Ok(fabric_region_id)
        }));

        // Handshake so the pipe attaches the fabric channel.
        let handshake = PipeControl::Handshake {
            uri: "sel://acme/lobby".to_string(),
        }
        .encode();
        write_raw_frame(&mut peer, &handshake, 0, FrameHeader::FLAG_READY)
            .await
            .expect("write handshake");

        // Wait until the pipe has attached: its own counting writer brings
        // the fabric writer_count to 2 (inner peer + pipe).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while fabric.ring().region().load_writer_count().expect("count") < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "pipe must attach the fabric channel"
            );
            tokio::task::yield_now().await;
        }

        // The inner peer writes one final frame, then leaves (drops its
        // writer — all inner writers gone).
        write_raw_frame(&mut inner_writer, b"bye", 1, FrameHeader::FLAG_READY)
            .await
            .expect("inner write");
        drop(inner_writer);

        // The guest task must terminate on the fabric close (the client peer
        // is still connected, so only the fabric half ended).
        tokio::time::timeout(std::time::Duration::from_secs(5), guest_task)
            .await
            .expect("guest task must tear down on fabric close")
            .expect("guest task succeeds");

        // The client observes the relayed frame followed by stream EOF.
        let mut buf = Vec::new();
        let n = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            use tokio::io::AsyncReadExt;
            peer.read_to_end(&mut buf).await
        })
        .await
        .expect("peer must observe stream finish promptly")
        .expect("peer read must succeed");
        assert!(n > 0, "relayed frame must reach the client before EOF");
        assert!(buf.len() > FrameHeader::ENCODED_SIZE, "frame bytes present");
        let header = FrameHeader::decode(&buf[..FrameHeader::ENCODED_SIZE]).expect("header");
        assert_eq!(header.tag, 1, "fabric frame relayed before finish");
    }

    /// 6.2 (uplift): killing a bridge-channel mid-pipe (here: aborting the
    /// pipe task, the guest-level equivalent of a supervisor kill) drops its
    /// fabric membership — inner guests observe `writer_count == 0` — while a
    /// sibling pipe on another fabric channel is unaffected and keeps
    /// relaying.
    #[tokio::test]
    async fn killing_bridge_pipe_mid_stream_is_isolated_from_siblings() {
        setup();

        // Fabric channel for the killed pipe, with an inner reader peer
        // (writerless: the bridge-channel is the ring's only writer, so the
        // inner guest observes `writer_count == 0` when the pipe dies).
        let fabric = Channel::create_with_backpressure(
            4096,
            selium_shm::ChannelBackpressure::Park,
            selium_abi::ResourceKind::SharedMemory,
        )
        .expect("fabric channel");
        let fabric_region_id = fabric.region_id();
        let _fabric_inner_reader = fabric.blocking_reader().expect("inner reader");

        // Sibling fabric channel with its own bridge pipe and a full inner
        // peer (reader + writer), so the sibling keeps relaying.
        let sibling = Channel::create_with_backpressure(
            4096,
            selium_shm::ChannelBackpressure::Park,
            selium_abi::ResourceKind::SharedMemory,
        )
        .expect("sibling fabric channel");
        let sibling_region_id = sibling.region_id();
        // Full inner peer on the sibling fabric (blocking reader + counting
        // writer), created before the pipe attaches so the pipe's quiesce
        // check always sees another member.
        let mut sibling_read =
            FramedRead::new(ShmTransport::new(&sibling, &sibling).expect("sibling inner"));

        // Both pipes are bridged from their own relay byte channels.
        let (ring_to_guest, ring_from_guest, shared_id, _region) =
            byte_channel::create(65_536, 65_536).expect("create");
        let mut peer = connector_peer(shared_id, &ring_from_guest, &ring_to_guest);
        let guest_stream = ByteStream::attach_blocking(shared_id).expect("guest attach");
        let killed_task = tokio::spawn(bridge_pipe(guest_stream, move |_uri| async move {
            Ok(fabric_region_id)
        }));

        let (s_ring_to, s_ring_from, s_shared, _s_region) =
            byte_channel::create(65_536, 65_536).expect("create sibling channel");
        let mut sibling_peer = connector_peer(s_shared, &s_ring_from, &s_ring_to);
        let sibling_stream = ByteStream::attach_blocking(s_shared).expect("sibling attach");
        let sibling_task = tokio::spawn(bridge_pipe(sibling_stream, move |_uri| async move {
            Ok(sibling_region_id)
        }));

        // Both pipes complete the handshake.
        let handshake = PipeControl::Handshake {
            uri: "sel://acme/lobby".to_string(),
        }
        .encode();
        write_raw_frame(&mut peer, &handshake, 0, FrameHeader::FLAG_READY)
            .await
            .expect("killed pipe handshake");
        write_raw_frame(&mut sibling_peer, &handshake, 0, FrameHeader::FLAG_READY)
            .await
            .expect("sibling pipe handshake");

        // Wait until both pipes have attached their fabric channels: the
        // killed pipe registers its writer on the writerless fabric (count
        // 0 → 1); the sibling pipe brings its fabric to 2 (inner peer +
        // pipe).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while fabric.ring().region().load_writer_count().expect("count") < 1
            || sibling.ring().region().load_writer_count().expect("count") < 2
        {
            assert!(
                std::time::Instant::now() < deadline,
                "both pipes must attach their fabric channels"
            );
            tokio::task::yield_now().await;
        }

        // Kill the first pipe mid-stream (supervisor-kill analogue).
        killed_task.abort();

        // The killed pipe's fabric membership is gone: inner guests observe
        // `writer_count == 0`.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while fabric.ring().region().load_writer_count().expect("count") != 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "killed pipe must release its fabric writer"
            );
            tokio::task::yield_now().await;
        }
        assert_eq!(
            fabric.ring().region().load_writer_count().expect("count"),
            0,
            "inner guests observe writer_count == 0 after the kill"
        );

        // The sibling pipe is unaffected: it still relays frames.
        let payload = b"still-alive".to_vec();
        write_raw_frame(&mut sibling_peer, &payload, 9, FrameHeader::FLAG_READY)
            .await
            .expect("sibling data frame");

        let (relayed, tag, _) = loop {
            match sibling_read.read_frame() {
                Ok(frame) => break frame,
                Err(WireError::BufferEmpty) => tokio::task::yield_now().await,
                Err(e) => panic!("sibling inner read: {e}"),
            }
        };
        assert_eq!(relayed, payload, "sibling pipe still relays after the kill");
        assert_eq!(tag, 9);

        // Clean teardown of the sibling.
        drop(sibling_peer);
        tokio::time::timeout(std::time::Duration::from_secs(5), sibling_task)
            .await
            .expect("sibling pipe tears down on client FIN")
            .expect("sibling pipe succeeds");
    }

    /// Encodes + writes one frame onto a byte channel in its raw frame format.
    async fn write_raw_frame<W: tokio::io::AsyncWrite + Unpin>(
        writer: &mut W,
        payload: &[u8],
        tag: u32,
        flags: u8,
    ) -> std::io::Result<()> {
        let header = FrameHeader {
            len: payload.len() as u32,
            tag,
            flags,
            _reserved: [0; 3],
        };
        let mut framed = Vec::with_capacity(FrameHeader::ENCODED_SIZE + payload.len());
        framed.extend_from_slice(&header.encode());
        framed.extend_from_slice(payload);
        writer.write_all(&framed).await
    }
}
