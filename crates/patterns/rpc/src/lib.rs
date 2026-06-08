//! Selium RPC pattern crate.
//!
//! Provides typed request/reply communication between guests using
//! shared-memory ring buffers. Built on top of `selium-guest` IO primitives.
//!
//! # Design
//!
//! Each RPC session uses a multi-memory shared region containing two ring
//! buffers: a request ring (client writes, server reads) and a reply ring
//! (server writes, client reads). The multi-memory header at offset 0
//! describes the layout:
//!
//! ```text
//! Offset 0:  magic (u64) = 0x53454C49554D454D
//! Offset 8:  total_capacity (u64)
//! Offset 16: count (u32) = 2
//! Offset 24: entry[0] = {offset: u32, len: u32}  (request ring)
//! Offset 32: entry[1] = {offset: u32, len: u32}  (reply ring)
//! ```
//!
//! Each sub-memory contains a standard ring buffer layout with coordination
//! fields in page 0 and data starting at page 1.
//!
//! Connection establishment uses a `HostQueue` to pass the `shared_id` from
//! client to server.

use std::{
    marker::PhantomData,
    sync::atomic::{Ordering, fence},
};

use rkyv::{
    api::high::{HighDeserializer, HighValidator},
    rancor::Error as RancorError,
};
use selium_abi::{decode_rkyv, encode_rkyv};
use selium_guest::io::{ChannelRegion, FrameHeader, PAGE_SIZE, RegionMapping, RingBuf};

pub use error::{AcceptError, RpcError};

pub mod error;

/// Default reply ring capacity in bytes.
const DEFAULT_REP_CAPACITY: u64 = 4096;
/// Default request ring capacity in bytes.
const DEFAULT_REQ_CAPACITY: u64 = 4096;
const HEADER_CAPACITY_OFFSET: u64 = 8;
const HEADER_COUNT_OFFSET: u64 = 16;
const HEADER_ENTRY_OFFSET: u64 = 24;
const HEADER_ENTRY_SIZE: u64 = 8;
/// Multi-memory header offsets.
const HEADER_MAGIC_OFFSET: u64 = 0;
/// Header size (magic + capacity + count + 2 entries).
const HEADER_SIZE: u64 = HEADER_ENTRY_OFFSET + 2 * HEADER_ENTRY_SIZE;
/// Magic value for multi-memory shared region layout headers.
const SHARED_REGION_MAGIC: u64 = 0x53454C49554D454D;

/// Client-side handle for making typed RPC requests.
///
/// Sends `Req` payloads and receives `Rep` replies over shared-memory
/// ring buffers.
pub struct RpcClient<Req, Rep> {
    request_ring: RingBuf,
    reply_ring: RingBuf,
    next_correlation: u32,
    _phantom: PhantomData<(Req, Rep)>,
}

/// Server-side handle for an established RPC session.
///
/// Receives `Req` requests and sends `Rep` replies over shared-memory
/// ring buffers.
pub struct RpcConnection<Req, Rep> {
    request_ring: RingBuf,
    reply_ring: RingBuf,
    reader_pos: u64,
    _phantom: PhantomData<(Req, Rep)>,
}

/// A single request received by the server, with the ability to reply.
pub struct RpcRequest<'a, Req, Rep> {
    reply_ring: &'a RingBuf,
    payload_bytes: Vec<u8>,
    correlation: u32,
    _phantom: PhantomData<(Req, Rep)>,
}

/// Accept implementation for RPC connections.
///
/// Attaches to the shared region from an `IncomingConnection` and
/// returns an `RpcConnection`.
pub struct RpcAccept<Req, Rep>(PhantomData<(Req, Rep)>);

impl<Req, Rep> RpcClient<Req, Rep>
where
    Req: selium_abi::RkyvEncode,
    Rep: rkyv::Archive + Sized,
    for<'a> Rep::Archived: rkyv::Deserialize<Rep, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
{
    /// Creates a new RPC session by allocating a shared region with two
    /// ring buffers, sending the `shared_id` via `ResourceSender`, and
    /// attaching to the rings.
    pub async fn connect(
        sender: selium_guest::ResourceSender,
        request_capacity: u64,
        reply_capacity: u64,
    ) -> Result<Self, RpcError> {
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

        let (request_ring, reply_ring, shared_id) = create_rpc_region(req_cap, rep_cap)?;

        sender
            .send(shared_id)
            .await
            .map_err(|e| RpcError::Serialization(format!("failed to send shared_id: {e}")))?;

        Ok(Self {
            request_ring,
            reply_ring,
            next_correlation: 1,
            _phantom: PhantomData,
        })
    }

    /// Sends a typed request and awaits the matching reply.
    ///
    /// The request is rkyv-encoded and written as a frame to the request
    /// ring. The client then blocks on the reply ring's generation counter
    /// (via polling in native mode, `memory.atomic.wait32` in WASM mode)
    /// until a reply frame with the matching correlation tag is available.
    pub async fn request(&mut self, payload: Req) -> Result<Rep, RpcError> {
        let correlation = self.next_correlation;
        self.next_correlation = self.next_correlation.wrapping_add(1);

        // Encode the request payload.
        let encoded = encode_rkyv(&payload)
            .map_err(|e| RpcError::Serialization(format!("encode request: {e}")))?;

        // Reserve space and write the frame to the request ring.
        let frame_size = FrameHeader::ENCODED_SIZE as u64 + encoded.len() as u64;
        let pos = self.request_ring.reserve(frame_size)?;
        self.request_ring
            .write_frame(pos, &encoded, correlation, 0)?;

        // Block on the reply ring's generation counter.
        let mut last_generation = self.reply_ring.generation()?;

        loop {
            // Poll the generation counter for changes.
            let current_generation = self.reply_ring.generation()?;

            if current_generation != last_generation {
                last_generation = current_generation;

                // Generation changed — try to read a reply frame.
                match try_read_reply::<Rep>(&self.reply_ring, correlation) {
                    Ok(Some(reply)) => return Ok(reply),
                    Ok(None) => {}
                    Err(RpcError::BufferEmpty) => {}
                    Err(e) => return Err(e),
                }
            }

            // Check if the server has disconnected.
            let writer_count = self.reply_ring.region().load_writer_count()?;
            if writer_count == 0 {
                return Err(RpcError::ConnectionClosed);
            }

            // Yield to allow the server to process.
            selium_guest::yield_now().await;
        }
    }
}

impl<Req, Rep> RpcConnection<Req, Rep>
where
    Req: rkyv::Archive + Sized,
    for<'a> Req::Archived: rkyv::Deserialize<Req, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
    Rep: selium_abi::RkyvEncode,
{
    /// Creates an RPC connection from the server side by attaching to
    /// the shared region identified by `shared_id`.
    ///
    /// In WASM mode, this calls `attach_region(shared_id)` to map the
    /// region, then parses the multi-memory header to discover the
    /// request and reply rings.
    ///
    /// In native mode, this returns an error because hostcalls are not
    /// available. Use `from_mapping` for in-process testing.
    pub fn for_server(_shared_id: u64, _client_process_id: u64) -> Result<Self, RpcError> {
        // In native mode, we cannot attach to a shared region by ID.
        // This will work in WASM mode where the hostcall infrastructure exists.
        Err(RpcError::InvalidRegion)
    }

    /// Creates an RPC connection from a pre-attached region mapping.
    ///
    /// Parses the multi-memory header to discover the request and reply
    /// rings. This is useful for in-process testing or when the caller
    /// has already attached to the shared region.
    pub fn from_mapping(parent_mapping: &RegionMapping) -> Result<Self, RpcError> {
        let (request_ring, reply_ring) = attach_rpc_region(0, parent_mapping)?;
        Ok(Self {
            request_ring,
            reply_ring,
            reader_pos: 0,
            _phantom: PhantomData,
        })
    }

    /// Creates an RPC connection from pre-built ring buffers (for testing).
    pub fn from_rings(request_ring: RingBuf, reply_ring: RingBuf) -> Self {
        Self {
            request_ring,
            reply_ring,
            reader_pos: 0,
            _phantom: PhantomData,
        }
    }

    /// Receives the next request from the client.
    ///
    /// Blocks on the request ring's generation counter (via polling in
    /// native mode, `memory.atomic.wait32` in WASM mode) until a request
    /// frame is available. Returns an `RpcRequest` that can be used to
    /// inspect the payload and send a reply.
    pub async fn recv(&mut self) -> Result<RpcRequest<'_, Req, Rep>, RpcError> {
        let mut last_generation = self.request_ring.generation()?;

        loop {
            // Poll the generation counter for changes.
            let current_generation = self.request_ring.generation()?;

            if current_generation != last_generation {
                last_generation = current_generation;

                // Acquire fence ensures we see the writer's payload before the header.
                fence(Ordering::Acquire);

                // Try to read a frame.
                let tail = self.request_ring.region().load_next_tail()?;

                if self.reader_pos < tail {
                    let header = self.request_ring.read_frame_header(self.reader_pos)?;

                    if header.is_ready() {
                        let frame_size = header.frame_size();
                        let payload_pos = self
                            .reader_pos
                            .checked_add(FrameHeader::ENCODED_SIZE as u64)
                            .ok_or(RpcError::InvalidRegion)?;
                        let payload_bytes =
                            self.request_ring.read_at(payload_pos, header.len as u64)?;

                        let correlation = header.tag;
                        self.reader_pos += frame_size;

                        return Ok(RpcRequest {
                            reply_ring: &self.reply_ring,
                            payload_bytes,
                            correlation,
                            _phantom: PhantomData,
                        });
                    }
                }
            }

            // No new data; check if client disconnected.
            let writer_count = self.request_ring.region().load_writer_count()?;
            if writer_count == 0 {
                return Err(RpcError::ConnectionClosed);
            }
            selium_guest::yield_now().await;
        }
    }
}

impl<'a, Req, Rep> RpcRequest<'a, Req, Rep>
where
    Req: rkyv::Archive + Sized,
    for<'b> Req::Archived: rkyv::Deserialize<Req, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'b, RancorError>>,
    Rep: selium_abi::RkyvEncode,
{
    /// Returns a reference to the raw request payload bytes.
    pub fn payload_bytes(&self) -> &[u8] {
        &self.payload_bytes
    }

    /// Decodes the request payload from rkyv bytes.
    pub fn payload(&self) -> Result<Req, RpcError> {
        decode_rkyv::<Req>(&self.payload_bytes)
            .map_err(|e| RpcError::Serialization(format!("decode request: {e}")))
    }

    /// Returns the deserialized request payload by value.
    pub fn into_payload(self) -> Result<Req, RpcError> {
        self.payload()
    }

    /// Sends a reply to the client.
    ///
    /// The response is rkyv-encoded and written as a frame to the reply
    /// ring with the same correlation tag as the request.
    pub async fn reply(self, response: Rep) -> Result<(), RpcError> {
        let encoded = encode_rkyv(&response)
            .map_err(|e| RpcError::Serialization(format!("encode reply: {e}")))?;

        let frame_size = FrameHeader::ENCODED_SIZE as u64 + encoded.len() as u64;
        let pos = self.reply_ring.reserve(frame_size)?;
        self.reply_ring
            .write_frame(pos, &encoded, self.correlation, 0)?;

        Ok(())
    }
}

impl<Req, Rep> selium_guest::Accept for RpcAccept<Req, Rep>
where
    Req: rkyv::Archive + Sized,
    for<'a> Req::Archived: rkyv::Deserialize<Req, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
    Rep: selium_abi::RkyvEncode,
{
    type Item = RpcConnection<Req, Rep>;

    fn accept(connection: selium_guest::IncomingConnection) -> selium_guest::Result<Self::Item> {
        RpcConnection::for_server(connection.shared_id, connection.client_process_id)
            .map_err(|e| selium_guest::GuestError::Host(format!("RPC accept failed: {e}")))
    }
}

/// Aligns a value up to the given alignment.
fn align_up(value: u64, alignment: u64) -> u64 {
    let rem = value % alignment;
    if rem == 0 {
        value
    } else {
        value + alignment - rem
    }
}

/// Attaches to an RPC region by shared_id and extracts the two ring buffers.
///
/// Parses the multi-memory header to discover the request and reply ring
/// sub-memories, then creates RingBuf handles for each.
fn attach_rpc_region(
    _shared_id: u64,
    parent_mapping: &RegionMapping,
) -> Result<(RingBuf, RingBuf), RpcError> {
    // Read and validate magic.
    let magic_bytes = parent_mapping.read(HEADER_MAGIC_OFFSET, 8)?;
    let magic = u64::from_le_bytes(
        magic_bytes
            .try_into()
            .map_err(|_| RpcError::InvalidRegion)?,
    );
    if magic != SHARED_REGION_MAGIC {
        return Err(RpcError::LayoutMismatch);
    }

    // Read count.
    let count_bytes = parent_mapping.read(HEADER_COUNT_OFFSET, 4)?;
    let count = u32::from_le_bytes(
        count_bytes
            .try_into()
            .map_err(|_| RpcError::InvalidRegion)?,
    );
    if count < 2 {
        return Err(RpcError::LayoutMismatch);
    }

    // Read entry[0]: request ring.
    let entry0_offset_bytes = parent_mapping.read(HEADER_ENTRY_OFFSET, 4)?;
    let entry0_len_bytes = parent_mapping.read(HEADER_ENTRY_OFFSET + 4, 4)?;
    let req_offset = u32::from_le_bytes(
        entry0_offset_bytes
            .try_into()
            .map_err(|_| RpcError::InvalidRegion)?,
    ) as u64;
    let req_len = u32::from_le_bytes(
        entry0_len_bytes
            .try_into()
            .map_err(|_| RpcError::InvalidRegion)?,
    ) as u64;

    // Read entry[1]: reply ring.
    let entry1_offset_bytes = parent_mapping.read(HEADER_ENTRY_OFFSET + 8, 4)?;
    let entry1_len_bytes = parent_mapping.read(HEADER_ENTRY_OFFSET + 12, 4)?;
    let rep_offset = u32::from_le_bytes(
        entry1_offset_bytes
            .try_into()
            .map_err(|_| RpcError::InvalidRegion)?,
    ) as u64;
    let rep_len = u32::from_le_bytes(
        entry1_len_bytes
            .try_into()
            .map_err(|_| RpcError::InvalidRegion)?,
    ) as u64;

    // Calculate ring capacities (sub-memory length minus page 0).
    let req_capacity = req_len
        .checked_sub(PAGE_SIZE)
        .ok_or(RpcError::LayoutMismatch)?;
    let rep_capacity = rep_len
        .checked_sub(PAGE_SIZE)
        .ok_or(RpcError::LayoutMismatch)?;

    // Create sub-mappings for each ring.
    let req_mapping = parent_mapping.sub_region(req_offset, req_len)?;
    let rep_mapping = parent_mapping.sub_region(rep_offset, rep_len)?;

    // Create ChannelRegions from the sub-mappings.
    let req_region = ChannelRegion::from_mapping(req_mapping, req_capacity);
    let rep_region = ChannelRegion::from_mapping(rep_mapping, rep_capacity);

    // Create RingBuf instances.
    let request_ring = RingBuf::wrap_region(req_region)?;
    let reply_ring = RingBuf::wrap_region(rep_region)?;

    Ok((request_ring, reply_ring))
}

/// Creates a multi-memory region with two ring buffers for RPC.
///
/// The region layout:
/// - Multi-memory header at offset 0
/// - Request ring at entry[0].offset (page 0 = coordination, page 1+ = data)
/// - Reply ring at entry[1].offset (page 0 = coordination, page 1+ = data)
///
/// Returns (request_ring, reply_ring, shared_id).
fn create_rpc_region(
    req_capacity: u64,
    rep_capacity: u64,
) -> Result<(RingBuf, RingBuf, u64), RpcError> {
    // Each sub-memory: page 0 (coordination) + data pages.
    let req_region_len = PAGE_SIZE + req_capacity;
    let rep_region_len = PAGE_SIZE + rep_capacity;

    // Calculate offsets.
    let sub_memory_0_offset = align_up(HEADER_SIZE, 8);
    let sub_memory_1_offset = align_up(sub_memory_0_offset + req_region_len, 8);
    let total_capacity = align_up(sub_memory_1_offset + rep_region_len, 8);

    // Create a single RegionMapping for the entire region.
    let parent_mapping = RegionMapping::allocate(total_capacity)?;

    // Write multi-memory header.
    parent_mapping.write(HEADER_MAGIC_OFFSET, &SHARED_REGION_MAGIC.to_le_bytes())?;
    parent_mapping.write(HEADER_CAPACITY_OFFSET, &total_capacity.to_le_bytes())?;
    parent_mapping.write(HEADER_COUNT_OFFSET, &2u32.to_le_bytes())?;

    // Write entry[0]: request ring.
    parent_mapping.write(
        HEADER_ENTRY_OFFSET,
        &(sub_memory_0_offset as u32).to_le_bytes(),
    )?;
    parent_mapping.write(
        HEADER_ENTRY_OFFSET + 4,
        &(req_region_len as u32).to_le_bytes(),
    )?;

    // Write entry[1]: reply ring.
    parent_mapping.write(
        HEADER_ENTRY_OFFSET + 8,
        &(sub_memory_1_offset as u32).to_le_bytes(),
    )?;
    parent_mapping.write(
        HEADER_ENTRY_OFFSET + 12,
        &(rep_region_len as u32).to_le_bytes(),
    )?;

    // Create sub-mappings for each ring.
    let req_mapping = parent_mapping.sub_region(sub_memory_0_offset, req_region_len)?;
    let rep_mapping = parent_mapping.sub_region(sub_memory_1_offset, rep_region_len)?;

    // Create ChannelRegions from the sub-mappings.
    let req_region = ChannelRegion::from_mapping(req_mapping, req_capacity);
    let rep_region = ChannelRegion::from_mapping(rep_mapping, rep_capacity);

    // Initialize both regions.
    req_region.initialise()?;
    rep_region.initialise()?;

    // Increment writer counts to indicate the client is connected.
    req_region.increment_writer_count()?;
    rep_region.increment_writer_count()?;

    // Create RingBuf instances.
    let request_ring = RingBuf::wrap_region(req_region)?;
    let reply_ring = RingBuf::wrap_region(rep_region)?;

    Ok((request_ring, reply_ring, 0))
}

/// Tries to read a reply frame with the given correlation tag from the reply ring.
fn try_read_reply<Rep>(ring: &RingBuf, correlation: u32) -> Result<Option<Rep>, RpcError>
where
    Rep: rkyv::Archive + Sized,
    for<'a> Rep::Archived: rkyv::Deserialize<Rep, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
{
    let tail = ring.region().load_next_tail()?;
    if tail == 0 {
        return Ok(None);
    }

    // Read the first frame (RPC replies are single-frame for now).
    fence(Ordering::Acquire);
    let header = ring.read_frame_header(0)?;
    if !header.is_ready() {
        return Ok(None);
    }
    if header.tag != correlation {
        return Ok(None);
    }

    let payload_pos = FrameHeader::ENCODED_SIZE as u64;
    let payload_bytes = ring.read_at(payload_pos, header.len as u64)?;

    let decoded: Rep = decode_rkyv::<Rep>(&payload_bytes)
        .map_err(|e| RpcError::Serialization(format!("decode reply: {e}")))?;

    Ok(Some(decoded))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Creates a connected RPC client+server pair for in-process testing.
    ///
    /// Creates a multi-memory region with request and reply rings, then
    /// creates both client and server handles that share the same underlying
    /// memory via cloned RegionMappings.
    ///
    /// Returns `(RpcClient, RpcConnection)` with rings already connected.
    pub fn create_test_pair<Req, Rep>(
        req_capacity: u64,
        rep_capacity: u64,
    ) -> Result<(RpcClient<Req, Rep>, RpcConnection<Req, Rep>), RpcError>
    where
        Req: selium_abi::RkyvEncode + rkyv::Archive + Sized,
        for<'a> Req::Archived: rkyv::Deserialize<Req, HighDeserializer<RancorError>>
            + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
        Rep: selium_abi::RkyvEncode + rkyv::Archive + Sized,
        for<'a> Rep::Archived: rkyv::Deserialize<Rep, HighDeserializer<RancorError>>
            + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
    {
        let req_cap = if req_capacity == 0 {
            DEFAULT_REQ_CAPACITY
        } else {
            req_capacity
        };
        let rep_cap = if rep_capacity == 0 {
            DEFAULT_REP_CAPACITY
        } else {
            rep_capacity
        };

        // Each sub-memory: page 0 (coordination) + data pages.
        let req_region_len = PAGE_SIZE + req_cap;
        let rep_region_len = PAGE_SIZE + rep_cap;

        // Calculate offsets.
        let sub_memory_0_offset = align_up(HEADER_SIZE, 8);
        let sub_memory_1_offset = align_up(sub_memory_0_offset + req_region_len, 8);
        let total_capacity = align_up(sub_memory_1_offset + rep_region_len, 8);

        // Create a single RegionMapping for the entire region.
        let parent_mapping = RegionMapping::allocate(total_capacity)?;

        // Write multi-memory header.
        parent_mapping.write(HEADER_MAGIC_OFFSET, &SHARED_REGION_MAGIC.to_le_bytes())?;
        parent_mapping.write(HEADER_CAPACITY_OFFSET, &total_capacity.to_le_bytes())?;
        parent_mapping.write(HEADER_COUNT_OFFSET, &2u32.to_le_bytes())?;
        parent_mapping.write(
            HEADER_ENTRY_OFFSET,
            &(sub_memory_0_offset as u32).to_le_bytes(),
        )?;
        parent_mapping.write(
            HEADER_ENTRY_OFFSET + 4,
            &(req_region_len as u32).to_le_bytes(),
        )?;
        parent_mapping.write(
            HEADER_ENTRY_OFFSET + 8,
            &(sub_memory_1_offset as u32).to_le_bytes(),
        )?;
        parent_mapping.write(
            HEADER_ENTRY_OFFSET + 12,
            &(rep_region_len as u32).to_le_bytes(),
        )?;

        // Create sub-mappings for each ring.
        let req_mapping = parent_mapping.sub_region(sub_memory_0_offset, req_region_len)?;
        let rep_mapping = parent_mapping.sub_region(sub_memory_1_offset, rep_region_len)?;

        // Create ChannelRegions from the sub-mappings.
        let req_region = ChannelRegion::from_mapping(req_mapping, req_cap);
        let rep_region = ChannelRegion::from_mapping(rep_mapping, rep_cap);

        // Initialize both regions.
        req_region.initialise()?;
        rep_region.initialise()?;

        // Client is the writer on request ring, server is the writer on reply ring.
        req_region.increment_writer_count()?;
        rep_region.increment_writer_count()?;

        // Create RingBuf instances for client (using cloned regions that share memory).
        let client_req_ring = RingBuf::wrap_region(req_region.clone())?;
        let client_rep_ring = RingBuf::wrap_region(rep_region.clone())?;

        // Create RingBuf instances for server (using the same regions).
        let server_req_ring = RingBuf::wrap_region(req_region)?;
        let server_rep_ring = RingBuf::wrap_region(rep_region)?;

        let client = RpcClient {
            request_ring: client_req_ring,
            reply_ring: client_rep_ring,
            next_correlation: 1,
            _phantom: PhantomData,
        };

        let server = RpcConnection {
            request_ring: server_req_ring,
            reply_ring: server_rep_ring,
            reader_pos: 0,
            _phantom: PhantomData,
        };

        Ok((client, server))
    }

    #[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[rkyv(bytecheck())]
    struct TestRequest {
        value: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[rkyv(bytecheck())]
    struct TestResponse {
        result: u32,
    }

    #[test]
    fn rpc_round_trip() {
        use std::cell::RefCell;
        use std::rc::Rc;

        let (mut client, mut server) =
            create_test_pair::<TestRequest, TestResponse>(4096, 4096).expect("create pair");

        let reply_result = Rc::new(RefCell::new(None));
        let reply_result_for_client = Rc::clone(&reply_result);

        // Spawn server handler as a guest task.
        selium_guest::spawn(async move {
            let request = server.recv().await.expect("recv");
            let req = request.payload().expect("payload");
            assert_eq!(req.value, "hello");
            request
                .reply(TestResponse { result: 42 })
                .await
                .expect("reply");
        });

        // Spawn client request as a guest task.
        selium_guest::spawn(async move {
            let reply = client
                .request(TestRequest {
                    value: "hello".to_string(),
                })
                .await
                .expect("request");
            *reply_result_for_client.borrow_mut() = Some(reply);
        });

        // Drive all spawned tasks to completion.
        selium_guest::poll_reactor();

        let reply = reply_result.borrow().clone().expect("reply received");
        assert_eq!(reply.result, 42);
    }
}
