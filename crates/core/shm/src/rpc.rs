//! Shared-memory RPC session helpers.
//!
//! This module builds on [`selium_wire::rpc`] to provide a concrete
//! shared-memory transport implementation. It preserves the existing
//! multi-memory region layout: one parent shared region contains a small
//! header describing two sub-memories (request ring and reply ring). The
//! client allocates the region and sends the parent `shared_id` to the
//! server via a [`Rendezvous`]; the server attaches to the same region and
//! parses the header to discover the rings.

use selium_abi::{RegionProt, ResourceKind};
use selium_encoding::FlatMsg;
use selium_memory::{RING_HEADER_SIZE, RegionMapping, SHARED_REGION_MAGIC, WASM_PAGE_SIZE};
use selium_wire::{
    error::{Error, Result},
    framed::{FramedRead, FramedWrite},
    rpc::{
        IncomingConnection, Rendezvous, RpcClient as WireRpcClient,
        RpcConnection as WireRpcConnection, RpcRequest as WireRpcRequest,
    },
};

use crate::{
    Channel, ChannelBackpressure, ChannelRegion, ring_buf::RingBuf, transport::ShmTransport,
};

pub use selium_wire::rpc::RpcError;

/// Client-side handle for typed RPC requests over shared memory.
pub type RpcClient<Req, Rep> = WireRpcClient<Req, Rep, ShmTransport>;
/// Server-side handle for an established shared-memory RPC session.
pub type RpcConnection<Req, Rep> = WireRpcConnection<Req, Rep, ShmTransport>;
/// A single request received by the server, with the ability to reply.
pub type RpcRequest<'a, Req, Rep> = WireRpcRequest<'a, Req, Rep, ShmTransport>;

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

/// Accepts an incoming shared-memory RPC session.
///
/// Attaches to the multi-memory region identified by `connection.shared_id`,
/// parses the request/reply ring layout, and returns a typed
/// [`RpcConnection`] for the server side.
pub fn accept<Req, Rep>(
    connection: IncomingConnection,
) -> std::result::Result<RpcConnection<Req, Rep>, RpcError>
where
    Req: FlatMsg,
    Rep: FlatMsg,
{
    let (request_channel, reply_channel) = attach_rpc_region(connection.shared_id)?;

    let request_transport =
        ShmTransport::new(&request_channel, &request_channel).map_err(map_transport_error)?;
    let reply_transport =
        ShmTransport::new(&reply_channel, &reply_channel).map_err(map_transport_error)?;

    Ok(RpcConnection::new(
        FramedRead::new(request_transport),
        FramedWrite::new(reply_transport),
        connection.client_process_id,
    ))
}

/// Creates a new shared-memory RPC client.
///
/// Allocates a multi-memory region with a request ring and a reply ring,
/// sends the parent `shared_id` to the server through `rendezvous`, and
/// returns a typed [`RpcClient`] ready for requests.
pub async fn connect<Req, Rep, R>(
    rendezvous: R,
    request_capacity: u64,
    reply_capacity: u64,
) -> std::result::Result<RpcClient<Req, Rep>, RpcError>
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

    rendezvous
        .send(shared_id)
        .await
        .map_err(|error| RpcError::Serialization(format!("rendezvous send failed: {error}")))?;

    let request_transport =
        ShmTransport::new(&request_channel, &request_channel).map_err(map_transport_error)?;
    let reply_transport =
        ShmTransport::new(&reply_channel, &reply_channel).map_err(map_transport_error)?;

    Ok(RpcClient::new(
        FramedWrite::new(request_transport),
        FramedRead::new(reply_transport),
    ))
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

/// Attaches to an RPC region by `shared_id` and extracts the two ring channels.
fn attach_rpc_region(shared_id: u64) -> Result<(Channel, Channel)> {
    let region = selium_memory::region_provider()?
        .attach(shared_id, None, RegionProt::ReadWrite)
        .map_err(Error::from)?;
    let parent_mapping = region.mapping();

    // Read and validate magic.
    let magic_bytes = parent_mapping.read(HEADER_MAGIC_OFFSET, 8)?;
    let magic = u64::from_le_bytes(
        magic_bytes
            .try_into()
            .map_err(|_error| Error::InvalidLayout)?,
    );
    if magic != SHARED_REGION_MAGIC {
        return Err(Error::InvalidLayout);
    }

    // Read count.
    let count_bytes = parent_mapping.read(HEADER_COUNT_OFFSET, 4)?;
    let count = u32::from_le_bytes(
        count_bytes
            .try_into()
            .map_err(|_error| Error::InvalidLayout)?,
    );
    if count < 2 {
        return Err(Error::InvalidLayout);
    }

    let (req_offset, req_len) = read_entry(&parent_mapping, 0)?;
    let (rep_offset, rep_len) = read_entry(&parent_mapping, 1)?;

    let req_capacity = req_len
        .checked_sub(RING_HEADER_SIZE)
        .ok_or(Error::InvalidLayout)?;
    let rep_capacity = rep_len
        .checked_sub(RING_HEADER_SIZE)
        .ok_or(Error::InvalidLayout)?;

    let request_channel =
        channel_from_sub_mapping(&parent_mapping, req_offset, req_len, req_capacity)?;
    let reply_channel =
        channel_from_sub_mapping(&parent_mapping, rep_offset, rep_len, rep_capacity)?;

    Ok((request_channel, reply_channel))
}

/// Wraps an existing sub-mapping as a channel without initialising it.
fn channel_from_sub_mapping(
    parent_mapping: &RegionMapping,
    offset: u64,
    len: u64,
    capacity: u64,
) -> Result<Channel> {
    let mapping = parent_mapping.sub_region(offset, len)?;
    let region = ChannelRegion::from_mapping(mapping, capacity);
    let ring = RingBuf::wrap_region(region)?;
    Ok(Channel::from_ring(ring, ChannelBackpressure::Park))
}

/// Creates a multi-memory region with two ring buffers for RPC.
///
/// Returns the request channel, reply channel, and parent `shared_id`.
fn create_rpc_region(req_capacity: u64, rep_capacity: u64) -> Result<(Channel, Channel, u64)> {
    // Each sub-memory: coordination header + data area.
    let req_region_len = RING_HEADER_SIZE + req_capacity;
    let rep_region_len = RING_HEADER_SIZE + rep_capacity;

    // Calculate offsets.
    let sub_memory_0_offset = align_up(HEADER_SIZE, 8);
    let sub_memory_1_offset = align_up(sub_memory_0_offset + req_region_len, 8);
    let total_capacity = align_up(sub_memory_1_offset + rep_region_len, WASM_PAGE_SIZE);

    // Allocate the parent region via the global provider.
    let pages = pages_for_bytes(total_capacity);
    let region = selium_memory::region_provider()?
        .allocate(pages, RegionProt::ReadWrite, ResourceKind::SharedMemory)
        .map_err(Error::from)?;
    let shared_id = region.region_id();
    let parent_mapping = region.mapping();

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

    let request_channel = initialise_sub_channel(
        &parent_mapping,
        sub_memory_0_offset,
        req_region_len,
        req_capacity,
    )?;
    let reply_channel = initialise_sub_channel(
        &parent_mapping,
        sub_memory_1_offset,
        rep_region_len,
        rep_capacity,
    )?;

    Ok((request_channel, reply_channel, shared_id))
}

/// Initialises a sub-region for a freshly-allocated RPC channel.
fn initialise_sub_channel(
    parent_mapping: &RegionMapping,
    offset: u64,
    len: u64,
    capacity: u64,
) -> Result<Channel> {
    let mapping = parent_mapping.sub_region(offset, len)?;
    let region = ChannelRegion::from_mapping(mapping, capacity);
    region.initialise()?;
    region.store_backpressure(ChannelBackpressure::Park.to_u8())?;
    region.store_shared_capacity(capacity)?;
    let ring = RingBuf::wrap_region(region)?;
    Ok(Channel::from_ring(ring, ChannelBackpressure::Park))
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
        Error::SerializationFailed(message) | Error::Guest(message) => {
            RpcError::Serialization(message)
        }
        other => RpcError::Serialization(other.to_string()),
    }
}

/// Computes the number of WASM pages needed to hold `bytes`.
fn pages_for_bytes(bytes: u64) -> u32 {
    bytes.div_ceil(WASM_PAGE_SIZE) as u32
}

/// Reads entry `index` from the multi-memory header.
fn read_entry(parent_mapping: &RegionMapping, index: u32) -> Result<(u64, u64)> {
    let offset = HEADER_ENTRY_OFFSET + u64::from(index) * HEADER_ENTRY_SIZE;
    let offset_bytes = parent_mapping.read(offset, 4)?;
    let len_bytes = parent_mapping.read(offset + 4, 4)?;
    let entry_offset = u32::from_le_bytes(
        offset_bytes
            .try_into()
            .map_err(|_error| Error::InvalidLayout)?,
    ) as u64;
    let entry_len = u32::from_le_bytes(
        len_bytes
            .try_into()
            .map_err(|_error| Error::InvalidLayout)?,
    ) as u64;
    Ok((entry_offset, entry_len))
}
