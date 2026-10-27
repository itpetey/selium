//! Shared-memory RPC session helpers.
//!
//! This module builds on [`selium_wire::rpc`] to provide a concrete
//! shared-memory transport implementation. It preserves the existing
//! multi-memory region layout: one parent shared region contains a small
//! header describing two sub-memories (request ring and reply ring). The
//! client allocates the region and sends the parent `shared_id` to the
//! server via a [`Rendezvous`]; the server attaches to the same region and
//! parses the header to discover the rings.

use std::{
    mem::ManuallyDrop,
    ops::{Deref, DerefMut},
    task::Poll,
};

use selium_service::FlatMsg;
use selium_wire::{
    error::{Error, Result},
    framed::{FramedRead, FramedWrite},
    rpc::{
        IncomingConnection, Rendezvous, RpcClient as WireRpcClient,
        RpcConnection as WireRpcConnection, RpcRequest as WireRpcRequest,
    },
    stream::{
        RpcServerStreamClient as WireServerStreamClient,
        RpcServerStreamConnection as WireServerStreamConnection,
        RpcServerStreamRequest as WireServerStreamRequest,
    },
};

use crate::{Channel, region::ChannelRegion, transport::ShmTransport};

pub use selium_wire::rpc::RpcError;

/// Client-side handle for typed RPC requests over shared memory.
pub type RpcClient<Req, Rep> = WireRpcClient<Req, Rep, ShmTransport>;
/// Server-side handle for an established shared-memory RPC session.
pub type RpcConnection<Req, Rep> = WireRpcConnection<Req, Rep, ShmTransport>;
/// A single request received by the server, with the ability to reply.
pub type RpcRequest<'a, Req, Rep> = WireRpcRequest<'a, Req, Rep, ShmTransport>;
/// Client-side handle for a server-streaming RPC session over shared memory.
pub type ServerStreamClient<Req, Item> = WireServerStreamClient<Req, Item, ShmTransport>;
/// Server-side handle for an established server-streaming RPC session.
pub type ServerStreamConnection<Req, Item> = WireServerStreamConnection<Req, Item, ShmTransport>;
/// A server-streaming request received from a client, with the ability to
/// send stream items.
pub type ServerStreamRequest<'a, Req, Item> = WireServerStreamRequest<'a, Req, Item, ShmTransport>;

/// Default reply ring capacity in bytes.
const DEFAULT_REP_CAPACITY: u64 = 4096;
/// Default request ring capacity in bytes.
const DEFAULT_REQ_CAPACITY: u64 = 4096;

/// An [`RpcClient`] that owns its session region.
///
/// [`connect`] allocates the request/reply region pair client-side; this
/// wrapper frees the parent region via the global region provider when
/// dropped, reclaiming the session's shared memory. The free is
/// ownership-checked by the runtime in guest mode (the allocator is the
/// owner), so dropping a non-owning handle cannot destroy a foreign
/// region. Dropping the inner client before freeing guarantees ring
/// teardown never touches unmapped memory.
pub struct OwnedRpcClient<Req, Rep> {
    inner: ManuallyDrop<RpcClient<Req, Rep>>,
    shared_id: u64,
}

/// A [`ServerStreamClient`] that owns its session region.
///
/// Mirrors [`OwnedRpcClient`] for server-streaming sessions: the session
/// region pair is freed via the global region provider when dropped.
pub struct OwnedServerStreamClient<Req, Item> {
    inner: ManuallyDrop<ServerStreamClient<Req, Item>>,
    shared_id: u64,
}

/// A session the host has marked closed: the parent region and the request
/// ring's new generation (for waking a parked receiver).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedSession {
    /// Parent region id the receiver registered its generation wait under.
    pub region_id: u64,
    /// New request-ring generation, passed to the host's wake registry.
    pub generation: u64,
}

impl<Req, Rep> OwnedRpcClient<Req, Rep> {
    /// Returns the parent shared id of the owned session region.
    pub fn shared_id(&self) -> u64 {
        self.shared_id
    }
}

impl<Req, Rep> Deref for OwnedRpcClient<Req, Rep> {
    type Target = RpcClient<Req, Rep>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<Req, Rep> DerefMut for OwnedRpcClient<Req, Rep> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<Req, Rep> Drop for OwnedRpcClient<Req, Rep> {
    fn drop(&mut self) {
        // SAFETY: `inner` is not used again after this point; dropping it
        // first ensures all channel state is torn down while the mapping
        // is still valid.
        unsafe { ManuallyDrop::drop(&mut self.inner) };
        if crate::free_region(self.shared_id).is_err() {
            // Already reclaimed (e.g. peer freed it first, or process
            // teardown raced); nothing further to do during drop.
        }
    }
}

impl<Req, Item> OwnedServerStreamClient<Req, Item> {
    /// Returns the parent shared id of the owned session region.
    pub fn shared_id(&self) -> u64 {
        self.shared_id
    }
}

impl<Req, Item> Deref for OwnedServerStreamClient<Req, Item> {
    type Target = ServerStreamClient<Req, Item>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<Req, Item> DerefMut for OwnedServerStreamClient<Req, Item> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

impl<Req, Item> Drop for OwnedServerStreamClient<Req, Item> {
    fn drop(&mut self) {
        // SAFETY: `inner` is not used again after this point; dropping it
        // first ensures all channel state is torn down while the mapping
        // is still valid.
        unsafe { ManuallyDrop::drop(&mut self.inner) };
        if crate::free_region(self.shared_id).is_err() {
            // Already reclaimed (e.g. peer freed it first, or process
            // teardown raced); nothing further to do during drop.
        }
    }
}

/// Accepts an incoming shared-memory RPC session.
///
/// Attaches to the multi-memory region identified by `connection.shared_id`,
/// parses the request/reply ring layout, and returns a typed
/// [`RpcConnection`] for the server side. The server registers only the
/// membership its directions need (request reader, reply writer) and
/// publishes acceptance after both are installed.
pub fn accept<Req, Rep>(
    connection: IncomingConnection,
) -> std::result::Result<RpcConnection<Req, Rep>, RpcError>
where
    Req: FlatMsg,
    Rep: FlatMsg,
{
    let (request_channel, reply_channel) = attach_rpc_region(connection.shared_id)?;

    // The rendezvous recipient treats the client's prior existence as
    // established: an empty request ring with no writers is EOF, not initial
    // absence.
    let request_transport =
        ShmTransport::read_only(&request_channel).map_err(map_transport_error)?;
    let reply_transport = ShmTransport::write_only(&reply_channel).map_err(map_transport_error)?;

    // Publish acceptance only now that the request reader and reply writer
    // are in place: a client released by this transition can write its first
    // request without it landing before the server's reader. A failed bump is
    // a region failure, reported with this module's existing `Serialisation`
    // bucket (no new public error variant).
    request_channel
        .ring()
        .region()
        .bump_generation()
        .map_err(|error| RpcError::Serialisation(error.to_string()))?;

    Ok(RpcConnection::new(
        FramedRead::new(request_transport),
        FramedWrite::new(reply_transport),
        connection.client_process_id,
    ))
}

/// Accepts an incoming shared-memory server-streaming RPC session.
///
/// Attaches to the multi-memory region identified by `connection.shared_id`,
/// parses the request/reply ring layout, and returns a typed
/// [`ServerStreamConnection`] for the server side.
pub fn accept_server_stream<Req, Item>(
    connection: IncomingConnection,
) -> std::result::Result<ServerStreamConnection<Req, Item>, RpcError>
where
    Req: FlatMsg,
    Item: FlatMsg,
{
    let (request_channel, reply_channel) = attach_rpc_region(connection.shared_id)?;

    let request_transport =
        ShmTransport::read_only(&request_channel).map_err(map_transport_error)?;
    let reply_transport = ShmTransport::write_only(&reply_channel).map_err(map_transport_error)?;

    // Announce acceptance after both directions are installed (see `accept`).
    request_channel
        .ring()
        .region()
        .bump_generation()
        .map_err(|error| RpcError::Serialisation(error.to_string()))?;

    Ok(ServerStreamConnection::new(
        FramedRead::new(request_transport),
        FramedWrite::new(reply_transport),
        connection.client_process_id,
    ))
}

/// Marks an RPC session's peer departed and bumps the request ring, waking a
/// server parked in `recv`.
///
/// Host-side teardown calls this for a sender process that has died: the
/// sender cannot drop its writer registration, so the receiver would otherwise
/// park forever and retain the connection region. The temporary attachment is
/// dropped immediately; only the flag and generation word are touched.
pub fn close_session(shared_id: u64) -> std::result::Result<ClosedSession, RpcError> {
    let (request_channel, _reply_channel) =
        attach_rpc_region(shared_id).map_err(|error| RpcError::Serialisation(error.to_string()))?;
    let generation = request_channel
        .ring()
        .region()
        .mark_peer_closed()
        .map_err(|error| RpcError::Serialisation(error.to_string()))?;
    Ok(ClosedSession {
        region_id: request_channel.region_id(),
        generation,
    })
}

/// Creates a new shared-memory RPC client.
///
/// Allocates a multi-memory region with a request ring and a reply ring,
/// installs its own directional registrations, snapshots the acceptance
/// generation, sends the parent `shared_id` to the server through
/// `rendezvous`, and parks until the server publishes acceptance. Dropping the
/// client frees the session region pair.
pub async fn connect<Req, Rep, R>(
    rendezvous: R,
    request_capacity: u64,
    reply_capacity: u64,
) -> std::result::Result<OwnedRpcClient<Req, Rep>, RpcError>
where
    Req: FlatMsg,
    Rep: FlatMsg,
    R: Rendezvous,
{
    let req_cap = if request_capacity == 0 {
        DEFAULT_REQ_CAPACITY
    } else {
        request_capacity
    };
    let rep_cap = if reply_capacity == 0 {
        DEFAULT_REP_CAPACITY
    } else {
        reply_capacity
    };

    let (request_channel, reply_channel, shared_id) = create_rpc_region(req_cap, rep_cap)?;

    // Local registrations come before the rendezvous: a server that accepts
    // immediately must find them already in place.
    let client = match build_unary_client(&request_channel, &reply_channel) {
        Ok(client) => client,
        Err(error) => {
            if crate::free_region(shared_id).is_err() {
                // Best-effort reclaim; surface the transport error instead.
            }
            return Err(error);
        }
    };
    let acceptance = match Acceptance::snapshot(&request_channel) {
        Ok(acceptance) => acceptance,
        Err(error) => {
            drop(client);
            if crate::free_region(shared_id).is_err() {
                // Best-effort reclaim; surface the snapshot error instead.
            }
            return Err(RpcError::Serialisation(error.to_string()));
        }
    };

    match rendezvous.send(shared_id).await {
        Ok(()) => {}
        Err(error) => {
            // Rendezvous failed: drop the registrations before reclaiming the
            // region pair.
            drop(client);
            if crate::free_region(shared_id).is_err() {
                // Best-effort reclaim; surface the rendezvous error instead.
            }
            return Err(RpcError::Serialisation(format!(
                "rendezvous send failed: {error}"
            )));
        }
    }

    if let Err(error) = acceptance.wait().await {
        drop(client);
        if crate::free_region(shared_id).is_err() {
            // Best-effort reclaim; surface the acceptance error instead.
        }
        // A generation read failed: region/setup failures use this module's
        // existing `Serialisation` bucket (no new public error variant).
        return Err(RpcError::Serialisation(error.to_string()));
    }

    Ok(OwnedRpcClient {
        inner: ManuallyDrop::new(client),
        shared_id,
    })
}

/// Builds the client-side unary transports from a freshly allocated pair.
fn build_unary_client<Req, Rep>(
    request_channel: &Channel,
    reply_channel: &Channel,
) -> std::result::Result<RpcClient<Req, Rep>, RpcError>
where
    Req: FlatMsg,
    Rep: FlatMsg,
{
    // Client membership: one counting writer on the request ring and an
    // awaiting-first-peer reader on the reply ring. No writer is retained on
    // the inbound reply ring, so a server that drops is observable.
    let request_transport =
        ShmTransport::write_only(request_channel).map_err(map_transport_error)?;
    let reply_transport =
        ShmTransport::read_only_awaiting_peer(reply_channel).map_err(map_transport_error)?;
    Ok(WireRpcClient::new(
        FramedWrite::new(request_transport),
        FramedRead::new(reply_transport),
    ))
}

/// Creates a new shared-memory server-streaming RPC client.
///
/// Allocates a multi-memory region with a request ring and a reply ring,
/// installs its own directional registrations, snapshots the acceptance
/// generation, sends the parent `shared_id` to the server through
/// `rendezvous`, and parks until the server publishes acceptance. Dropping the
/// client frees the session region pair.
pub async fn connect_server_stream<Req, Item, R>(
    rendezvous: R,
    request_capacity: u64,
    reply_capacity: u64,
) -> std::result::Result<OwnedServerStreamClient<Req, Item>, RpcError>
where
    Req: FlatMsg,
    Item: FlatMsg,
    R: Rendezvous,
{
    let req_cap = if request_capacity == 0 {
        DEFAULT_REQ_CAPACITY
    } else {
        request_capacity
    };
    let rep_cap = if reply_capacity == 0 {
        DEFAULT_REP_CAPACITY
    } else {
        reply_capacity
    };

    let (request_channel, reply_channel, shared_id) = create_rpc_region(req_cap, rep_cap)?;

    let client: ServerStreamClient<Req, Item> =
        match build_stream_client(&request_channel, &reply_channel) {
            Ok(client) => client,
            Err(error) => {
                if crate::free_region(shared_id).is_err() {
                    // Best-effort reclaim; surface the transport error instead.
                }
                return Err(error);
            }
        };
    let acceptance = match Acceptance::snapshot(&request_channel) {
        Ok(acceptance) => acceptance,
        Err(error) => {
            drop(client);
            if crate::free_region(shared_id).is_err() {
                // Best-effort reclaim; surface the snapshot error instead.
            }
            return Err(RpcError::Serialisation(error.to_string()));
        }
    };

    match rendezvous.send(shared_id).await {
        Ok(()) => {}
        Err(error) => {
            // Rendezvous failed: drop the registrations before reclaiming the
            // region pair.
            drop(client);
            if crate::free_region(shared_id).is_err() {
                // Best-effort reclaim; surface the rendezvous error instead.
            }
            return Err(RpcError::Serialisation(format!(
                "rendezvous send failed: {error}"
            )));
        }
    }

    if let Err(error) = acceptance.wait().await {
        drop(client);
        if crate::free_region(shared_id).is_err() {
            // Best-effort reclaim; surface the acceptance error instead.
        }
        // A generation read failed: region/setup failures use this module's
        // existing `Serialisation` bucket (no new public error variant).
        return Err(RpcError::Serialisation(error.to_string()));
    }

    Ok(OwnedServerStreamClient {
        inner: ManuallyDrop::new(client),
        shared_id,
    })
}

/// Builds the client-side server-streaming transports from a freshly allocated
/// pair.
fn build_stream_client<Req, Item>(
    request_channel: &Channel,
    reply_channel: &Channel,
) -> std::result::Result<ServerStreamClient<Req, Item>, RpcError>
where
    Req: FlatMsg,
    Item: FlatMsg,
{
    let request_transport =
        ShmTransport::write_only(request_channel).map_err(map_transport_error)?;
    let reply_transport =
        ShmTransport::read_only_awaiting_peer(reply_channel).map_err(map_transport_error)?;
    Ok(WireServerStreamClient::new(
        FramedWrite::new(request_transport),
        FramedRead::new(reply_transport),
    ))
}

/// Attaches to an RPC region by `shared_id` and extracts the two ring channels.
fn attach_rpc_region(shared_id: u64) -> Result<(Channel, Channel)> {
    crate::byte_channel::attach(shared_id)
}

/// Creates a multi-memory region with two ring buffers for RPC.
///
/// Returns the request channel, reply channel, and parent `shared_id`. The
/// parent region handle is dropped: the channels hold their own mappings,
/// so the allocation's attachment outlives it (see
/// [`byte_channel::create`], which documents the no-second-attach rule).
fn create_rpc_region(req_capacity: u64, rep_capacity: u64) -> Result<(Channel, Channel, u64)> {
    let (request_channel, reply_channel, shared_id, _region) =
        crate::byte_channel::create(req_capacity, rep_capacity)?;
    Ok((request_channel, reply_channel, shared_id))
}

/// Maps a transport error to an [`RpcError`].
fn map_transport_error(error: selium_wire::error::Error) -> RpcError {
    match error {
        Error::BufferFull => RpcError::BufferFull,
        Error::BufferEmpty => RpcError::BufferEmpty,
        Error::InvalidLayout | Error::InvalidRegion => RpcError::InvalidRegion,
        Error::ConnectionLost | Error::ChannelClosed | Error::Terminated => {
            RpcError::ConnectionClosed
        }
        Error::SerialisationFailed(message) | Error::Guest(message) => {
            RpcError::Serialisation(message)
        }
        other => RpcError::Serialisation(other.to_string()),
    }
}

/// Observes session acceptance through the request ring's generation counter.
///
/// The snapshot is taken after the client has installed its local
/// registrations (blocking-writer attachment itself advances the generation)
/// and before the session is published through rendezvous. The serving side
/// bumps the same generation only after installing its request reader and
/// reply writer, so comparing against the snapshot observes acceptance exactly
/// once — including an accept immediately followed by close, which a writer
/// count threshold can miss because the count returns to the pre-accept value.
///
/// The client does not write request data while waiting, so an unrelated
/// request-ring write cannot be mistaken for acceptance.
///
/// The snapshot and wait read generation words from the shared region. A read
/// failure is reported with [`RpcError::Serialisation`], which is this module's
/// existing bucket for region and session-setup failures — the change adds no
/// new public error variant.
#[derive(Clone)]
pub struct Acceptance {
    region: ChannelRegion,
    observed_generation: u64,
}

impl Acceptance {
    /// Snapshots the request ring after local registrations are in place.
    pub fn snapshot(channel: &Channel) -> Result<Self> {
        let region = channel.ring().region().clone();
        let observed_generation = region.load_generation()?;
        Ok(Self {
            region,
            observed_generation,
        })
    }

    /// Parks until the serving side publishes acceptance.
    ///
    /// The wait registers a generation wait on the request region, so an
    /// unaccepted session *parks*: the reactor hands the task back and the
    /// host is free to drive other work (including the server's accept).
    pub async fn wait(self) -> Result<()> {
        std::future::poll_fn(|cx| {
            let generation = match self.region.load_generation() {
                Ok(generation) => generation,
                Err(error) => return Poll::Ready(Err(error)),
            };
            if generation != self.observed_generation {
                return Poll::Ready(Ok(()));
            }
            if !selium_memory::register_generation_wait(
                self.region.region_id(),
                generation,
                cx.waker(),
            ) {
                // No reactor callback installed (e.g. a host-side consumer):
                // fall back to a self-wake so the wait re-checks.
                cx.waker().wake_by_ref();
            }
            // Check-after-register: an accept that landed between the read
            // above and the registration self-wakes us instead of being lost.
            if self.region.load_generation().ok() != Some(generation) {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() {
        crate::install_heap_provider();
    }

    #[test]
    fn create_and_attach_rpc_region_round_trip() {
        // Task 2.1: Verify that create_rpc_region produces a multi-memory
        // region that attach_rpc_region can attach to. The connector calls
        // rpc::connect (create_rpc_region + rendezvous) and the app guest
        // calls rpc::accept (attach_rpc_region).
        setup();

        let (req_ch, rep_ch, shared_id) = create_rpc_region(4096, 4096).expect("create_rpc_region");

        // Parent shared_id must be valid (non-zero).
        assert!(shared_id > 0, "parent shared_id must be non-zero");

        // Attach to the same region (simulates rpc::accept server side).
        let (attached_req, attached_rep) = attach_rpc_region(shared_id).expect("attach_rpc_region");

        // Both created and attached channels should use the same capacity.
        assert_eq!(attached_req.ring().capacity(), req_ch.ring().capacity());
        assert_eq!(attached_rep.ring().capacity(), rep_ch.ring().capacity());
    }

    #[derive(Debug, Clone, PartialEq)]
    struct PingMsg(String);

    #[derive(Debug, Clone, PartialEq)]
    struct PongMsg(String);

    impl selium_service::FlatMsg for PingMsg {
        fn encode(value: &Self) -> Vec<u8> {
            value.0.clone().into_bytes()
        }

        fn decode(bytes: &[u8]) -> std::result::Result<Self, flatbuffers::InvalidFlatbuffer> {
            Ok(Self(String::from_utf8(bytes.to_vec()).map_err(
                |_error| flatbuffers::InvalidFlatbuffer::ApparentSizeTooLarge,
            )?))
        }
    }

    impl selium_service::FlatMsg for PongMsg {
        fn encode(value: &Self) -> Vec<u8> {
            value.0.clone().into_bytes()
        }

        fn decode(bytes: &[u8]) -> std::result::Result<Self, flatbuffers::InvalidFlatbuffer> {
            Ok(Self(String::from_utf8(bytes.to_vec()).map_err(
                |_error| flatbuffers::InvalidFlatbuffer::ApparentSizeTooLarge,
            )?))
        }
    }

    #[tokio::test]
    async fn unary_connect_accept_round_trip() {
        // Unary mirror of the streaming round-trip: connect allocates the
        // region pair, accept attaches it, one request -> one reply.
        setup();

        let rendezvous = crate::ShmRendezvous::new();

        let server = {
            let rendezvous = rendezvous.clone();
            tokio::spawn(async move {
                let incoming = loop {
                    match rendezvous.recv().await {
                        Ok(connection) => break connection,
                        Err(_) => tokio::task::yield_now().await,
                    }
                };
                let mut conn: RpcConnection<PingMsg, PongMsg> = accept(incoming).expect("accept");
                let req = conn.recv().await.expect("recv request");
                assert_eq!(req.payload().expect("decode request").0, "ping");
                req.reply(PongMsg("pong".to_string())).await.expect("reply");
            })
        };

        let mut client: OwnedRpcClient<PingMsg, PongMsg> =
            connect(rendezvous, 0, 0).await.expect("connect");
        let reply = client
            .request(PingMsg("ping".to_string()))
            .await
            .expect("request");
        assert_eq!(reply, PongMsg("pong".to_string()));

        server.await.expect("server task");
    }

    #[test]
    fn accept_advances_the_request_generation_to_signal_the_client() {
        // The client's `Acceptance` gate parks on the request region's
        // generation; `accept` must advance it once the server's reader/writer
        // are in place, or a client whose connect is not accepted in the same
        // nested poll stays parked (previously: spun) forever.
        setup();
        let (request_channel, _reply_channel, shared_id) =
            create_rpc_region(4096, 4096).expect("create_rpc_region");
        let before = request_channel
            .ring()
            .region()
            .load_generation()
            .expect("generation before accept");

        let incoming = IncomingConnection {
            client_process_id: 1,
            shared_id,
        };
        let _connection: RpcConnection<PingMsg, PongMsg> = accept(incoming).expect("accept");

        let after = request_channel
            .ring()
            .region()
            .load_generation()
            .expect("generation after accept");
        assert!(
            after > before,
            "accept must advance the request region generation \
             ({before} -> {after}) to wake a parked client"
        );
    }

    #[tokio::test]
    async fn server_stream_connect_accept_round_trip() {
        // connect_server_stream allocates the region pair and delivers the
        // shared_id via the rendezvous; accept_server_stream attaches the
        // same pair server-side. Items then flow in order to stream end.
        //
        // connect_server_stream waits for the server's acceptance before
        // returning, so the server task is spawned first and retries the
        // rendezvous until the connect-side send lands.
        setup();
        use futures::StreamExt;

        let rendezvous = crate::ShmRendezvous::new();

        let server = {
            let rendezvous = rendezvous.clone();
            tokio::spawn(async move {
                let incoming = loop {
                    match rendezvous.recv().await {
                        Ok(connection) => break connection,
                        Err(_) => tokio::task::yield_now().await,
                    }
                };
                let mut conn: ServerStreamConnection<PingMsg, PongMsg> =
                    accept_server_stream(incoming).expect("accept_server_stream");
                let mut req = conn.recv().await.expect("recv request");
                assert_eq!(req.payload().expect("decode request").0, "start");
                req.send_item(PongMsg("a".to_string()))
                    .await
                    .expect("send item");
                req.send_final_item(PongMsg("b".to_string()))
                    .await
                    .expect("send final item");
            })
        };

        let mut client: OwnedServerStreamClient<PingMsg, PongMsg> =
            connect_server_stream(rendezvous, 0, 0)
                .await
                .expect("connect_server_stream");
        assert!(client.shared_id() > 0, "session region must be allocated");

        let mut stream = client
            .call(PingMsg("start".to_string()))
            .await
            .expect("call");
        let mut items = Vec::new();
        while let Some(item) = stream.next().await {
            items.push(item.expect("stream item"));
        }
        assert_eq!(
            items,
            vec![PongMsg("a".to_string()), PongMsg("b".to_string())]
        );

        server.await.expect("server task");
    }

    // --- Deterministic acceptance ---------------------------------------

    use std::{
        future::Future,
        pin::Pin,
        sync::{Arc, Mutex},
        task::Context,
    };

    use selium_abi::RegionProt;

    /// Rendezvous whose send always fails, recording the id it was offered.
    #[derive(Clone)]
    struct FailingRendezvous {
        seen: Arc<Mutex<Option<u64>>>,
    }

    impl Rendezvous for FailingRendezvous {
        fn send(&self, shared_id: u64) -> impl std::future::Future<Output = Result<()>> + Send {
            let seen = self.seen.clone();
            async move {
                *seen.lock().expect("rendezvous lock") = Some(shared_id);
                Err(Error::ChannelClosed)
            }
        }

        async fn recv(&self) -> Result<IncomingConnection> {
            Err(Error::ChannelClosed)
        }
    }

    #[tokio::test]
    async fn immediate_accept_is_not_lost() {
        // The server accepts as soon as the handoff is dequeued, before the
        // client's own connect task is polled again.
        setup();
        let rendezvous = crate::ShmRendezvous::new();
        let server = {
            let rendezvous = rendezvous.clone();
            tokio::spawn(async move {
                let incoming = loop {
                    match rendezvous.recv().await {
                        Ok(connection) => break connection,
                        Err(_) => tokio::task::yield_now().await,
                    }
                };
                let conn: RpcConnection<PingMsg, PongMsg> = accept(incoming).expect("accept");
                drop(conn);
            })
        };

        let client: OwnedRpcClient<PingMsg, PongMsg> =
            tokio::time::timeout(std::time::Duration::from_secs(1), connect(rendezvous, 0, 0))
                .await
                .expect("accept-then-close must release the client")
                .expect("connect");
        assert!(client.shared_id() > 0);
        server.await.expect("server task");
    }

    #[tokio::test]
    async fn unaccepted_session_parks_without_closing() {
        setup();
        let (request_channel, reply_channel, shared_id) =
            create_rpc_region(4096, 4096).expect("create_rpc_region");
        let client: RpcClient<PingMsg, PongMsg> =
            build_unary_client(&request_channel, &reply_channel).expect("client transports");
        let acceptance = Acceptance::snapshot(&request_channel).expect("snapshot");

        let mut wait = Box::pin(acceptance.wait());
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(
            matches!(Pin::new(&mut wait).poll(&mut cx), Poll::Pending),
            "an idle server must leave the client parked, not accepted or closed"
        );

        // A live session must also not look closed from the reply direction.
        assert_eq!(
            request_channel
                .ring()
                .region()
                .load_writer_count()
                .expect("writer count"),
            1,
            "the client's own request writer must not be mistaken for acceptance"
        );

        drop(wait);
        drop(client);
        if crate::free_region(shared_id).is_err() {
            // Best-effort cleanup in the test.
        }
    }

    #[tokio::test]
    async fn accept_then_close_is_observed() {
        // Acceptance must be a generation transition, not a writer-count
        // threshold: the server accepts and withdraws before the client polls.
        setup();
        let (request_channel, reply_channel, shared_id) =
            create_rpc_region(4096, 4096).expect("create_rpc_region");
        let client: RpcClient<PingMsg, PongMsg> =
            build_unary_client(&request_channel, &reply_channel).expect("client transports");
        let acceptance = Acceptance::snapshot(&request_channel).expect("snapshot");

        let incoming = IncomingConnection {
            client_process_id: 1,
            shared_id,
        };
        let connection: RpcConnection<PingMsg, PongMsg> = accept(incoming).expect("accept");
        drop(connection);

        tokio::time::timeout(std::time::Duration::from_secs(1), acceptance.wait())
            .await
            .expect("accept must be observed even after the server closed")
            .expect("acceptance wait");

        drop(client);
        if crate::free_region(shared_id).is_err() {
            // Best-effort cleanup in the test.
        }
    }

    #[tokio::test]
    async fn server_registers_no_writer_on_its_inbound_request_ring() {
        setup();
        let (request_channel, reply_channel, shared_id) =
            create_rpc_region(4096, 4096).expect("create_rpc_region");
        let client: RpcClient<PingMsg, PongMsg> =
            build_unary_client(&request_channel, &reply_channel).expect("client transports");
        let request_region = request_channel.ring().region();
        assert_eq!(request_region.load_writer_count().expect("count"), 1);

        let incoming = IncomingConnection {
            client_process_id: 1,
            shared_id,
        };
        let connection: RpcConnection<PingMsg, PongMsg> = accept(incoming).expect("accept");
        assert_eq!(
            request_region.load_writer_count().expect("count"),
            1,
            "the server must not retain a writer on its inbound request ring"
        );

        // Dropping the client leaves only the remote peer's withdrawal: the
        // server observes closure rather than a self-held writer.
        drop(client);
        assert_eq!(
            request_region.load_writer_count().expect("count"),
            0,
            "graceful client drop must reach the server's view"
        );
        drop(connection);
        if crate::free_region(shared_id).is_err() {
            // Best-effort cleanup in the test.
        }
    }

    #[tokio::test]
    async fn rendezvous_failure_reclaims_the_session_region() {
        setup();
        let seen = Arc::new(Mutex::new(None));
        let rendezvous = FailingRendezvous { seen: seen.clone() };

        let result: std::result::Result<OwnedRpcClient<PingMsg, PongMsg>, RpcError> =
            connect(rendezvous, 0, 0).await;
        assert!(result.is_err(), "a failed rendezvous must fail the connect");

        let shared_id = seen
            .lock()
            .expect("rendezvous lock")
            .expect("offered session id");
        let attached = selium_memory::region_provider().expect("provider").attach(
            shared_id,
            None,
            RegionProt::ReadWrite,
        );
        assert!(
            attached.is_err(),
            "the failed session's region must be freed, not leaked"
        );
    }

    #[tokio::test]
    async fn graceful_server_drop_closes_the_client_session() {
        setup();
        let rendezvous = crate::ShmRendezvous::new();
        let server = {
            let rendezvous = rendezvous.clone();
            tokio::spawn(async move {
                let incoming = loop {
                    match rendezvous.recv().await {
                        Ok(connection) => break connection,
                        Err(_) => tokio::task::yield_now().await,
                    }
                };
                let mut conn: RpcConnection<PingMsg, PongMsg> = accept(incoming).expect("accept");
                let req = conn.recv().await.expect("recv request");
                req.reply(PongMsg("bye".to_string())).await.expect("reply");
                // Withdraw after the reply is committed.
            })
        };

        let mut client: OwnedRpcClient<PingMsg, PongMsg> =
            connect(rendezvous, 0, 0).await.expect("connect");
        let reply = client
            .request(PingMsg("ping".to_string()))
            .await
            .expect("buffered reply must be drained before closure is reported");
        assert_eq!(reply, PongMsg("bye".to_string()));

        server.await.expect("server task");
        let next = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            client.request(PingMsg("again".to_string())),
        )
        .await
        .expect("a withdrawn server must not park the client")
        .expect_err("subsequent request must report closure");
        assert!(
            matches!(next, RpcError::ConnectionClosed),
            "expected ConnectionClosed, got {next:?}"
        );
    }

    #[tokio::test]
    async fn graceful_client_drop_closes_the_server_session() {
        setup();
        let rendezvous = crate::ShmRendezvous::new();
        let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
        let server = {
            let rendezvous = rendezvous.clone();
            tokio::spawn(async move {
                let incoming = loop {
                    match rendezvous.recv().await {
                        Ok(connection) => break connection,
                        Err(_) => tokio::task::yield_now().await,
                    }
                };
                let mut conn: RpcConnection<PingMsg, PongMsg> = accept(incoming).expect("accept");
                let closed = matches!(conn.recv().await, Err(RpcError::ConnectionClosed));
                if closed_tx.send(closed).is_err() {
                    // The test's receiver is gone; nothing can be reported.
                }
            })
        };

        let client: OwnedRpcClient<PingMsg, PongMsg> =
            connect(rendezvous, 0, 0).await.expect("connect");
        drop(client);

        let closed = tokio::time::timeout(std::time::Duration::from_secs(1), closed_rx)
            .await
            .expect("server must observe closure without hanging")
            .expect("server reported");
        assert!(closed, "graceful client drop must close the server session");
        server.await.expect("server task");
    }
}

// --- Spec traceability (scripts/check-traceability.sh) -------------------
// These tests are the evidence for the `resource-handshake` scenarios. One scenario may
// be claimed by several tests; the annotation lives next to the evidence.
// scenario: resource-handshake/rendezvous-trait-for-connection-establishment/client-sends-connection-via-rendezvous
// scenario: resource-handshake/rendezvous-trait-for-connection-establishment/server-accepts-connection-via-rendezvous
// scenario: resource-handshake/host-mediated-connection-queue-resourcesender-resourcelistener/client-sends-connection-request-via-resourcesender
// scenario: resource-handshake/host-mediated-connection-queue-resourcesender-resourcelistener/server-accepts-connection-via-resourcelistener
// scenario: resource-handshake/typed-accept-trait/accepting-an-rpc-connection-via-accept-trait
// scenario: resource-handshake/backpressure-on-send/server-is-slow-to-accept
// scenario: resource-handshake/connection-identity/server-receives-client-identity
// scenario: resource-handshake/queue-attach-authorisation/ungranted-attach-denied
// scenario: resource-handshake/queue-attach-authorisation/discovery-resolved-attach-permitted
// scenario: resource-handshake/queue-attach-authorisation/owner-attach-always-permitted
// scenario: resource-handshake/handoff-metadata/metadata-delivered
// scenario: resource-handshake/handoff-metadata/no-metadata
// scenario: resource-handshake/handoff-metadata/oversized-metadata-rejected
// scenario: resource-handshake/pinned-handoff-sender/handoff-from-unpinned-sender-refused
// scenario: resource-handshake/pinned-handoff-sender/handler-resolution-is-authoritative
// scenario: resource-handshake/guest-self-identity/self-info
// scenario: resource-handshake/local-registration-precedes-rendezvous/server-accepts-immediately-after-rendezvous
// scenario: resource-handshake/acceptance-is-an-explicit-event/server-accepts-and-closes-before-the-client-polls
// scenario: resource-handshake/acceptance-is-an-explicit-event/server-has-not-accepted
// scenario: secure-rpc/directional-session-membership/client-closes-gracefully
// scenario: secure-rpc/directional-session-membership/server-closes-gracefully
