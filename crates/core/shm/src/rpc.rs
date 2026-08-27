//! Shared-memory RPC session helpers.
//!
//! This module builds on [`selium_wire::rpc`] to provide a concrete
//! shared-memory transport implementation. It preserves the existing
//! multi-memory region layout: one parent shared region contains a small
//! header describing two sub-memories (request ring and reply ring). The
//! client allocates the region and sends the parent `shared_id` to the
//! server via a [`Rendezvous`]; the server attaches to the same region and
//! parses the header to discover the rings.

use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};

use selium_abi::{RegionProt, ResourceKind};
use selium_encoding::FlatMsg;
use selium_memory::{
    HEADER_SIZE_TWO_ENTRIES, MultiMemoryHeader, RING_HEADER_SIZE, RegionMapping, WASM_PAGE_SIZE,
};
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
/// Server-side handle for an established shared-memory RPC session.
pub type RpcConnection<Req, Rep> = WireRpcConnection<Req, Rep, ShmTransport>;
/// A single request received by the server, with the ability to reply.
pub type RpcRequest<'a, Req, Rep> = WireRpcRequest<'a, Req, Rep, ShmTransport>;

/// Default reply ring capacity in bytes.
const DEFAULT_REP_CAPACITY: u64 = 4096;
/// Default request ring capacity in bytes.
const DEFAULT_REQ_CAPACITY: u64 = 4096;

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
/// returns an [`OwnedRpcClient`] ready for requests. Dropping the client
/// frees the session region pair.
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

    let build_client = || -> std::result::Result<RpcClient<Req, Rep>, RpcError> {
        let request_transport =
            ShmTransport::new(&request_channel, &request_channel).map_err(map_transport_error)?;
        let reply_transport =
            ShmTransport::new(&reply_channel, &reply_channel).map_err(map_transport_error)?;
        Ok(WireRpcClient::new(
            FramedWrite::new(request_transport),
            FramedRead::new(reply_transport),
        ))
    };

    match rendezvous.send(shared_id).await {
        Ok(()) => {}
        Err(error) => {
            // Rendezvous failed: reclaim the region pair before returning.
            if crate::free_region(shared_id).is_err() {
                // Best-effort reclaim; surface the rendezvous error instead.
            }
            return Err(RpcError::Serialization(format!(
                "rendezvous send failed: {error}"
            )));
        }
    }

    let client = match build_client() {
        Ok(client) => client,
        Err(error) => {
            if crate::free_region(shared_id).is_err() {
                // Best-effort reclaim; surface the transport error instead.
            }
            return Err(error);
        }
    };

    Ok(OwnedRpcClient {
        inner: ManuallyDrop::new(client),
        shared_id,
    })
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

    // Parse the multi-memory header using the shared definition.
    let header = MultiMemoryHeader::parse(parent_mapping.backend(), 0).map_err(Error::from)?;
    if header.count < 2 {
        return Err(Error::InvalidLayout);
    }

    let req_entry = header.entry(0)?;
    let rep_entry = header.entry(1)?;

    let req_capacity = req_entry
        .length
        .checked_sub(RING_HEADER_SIZE)
        .ok_or(Error::InvalidLayout)?;
    let rep_capacity = rep_entry
        .length
        .checked_sub(RING_HEADER_SIZE)
        .ok_or(Error::InvalidLayout)?;

    let request_channel = channel_from_sub_mapping(
        &parent_mapping,
        req_entry.offset,
        req_entry.length,
        req_capacity,
    )?;
    let reply_channel = channel_from_sub_mapping(
        &parent_mapping,
        rep_entry.offset,
        rep_entry.length,
        rep_capacity,
    )?;

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
    let sub_memory_0_offset = align_up(HEADER_SIZE_TWO_ENTRIES, 8);
    let sub_memory_1_offset = align_up(sub_memory_0_offset + req_region_len, 8);
    let total_capacity = align_up(sub_memory_1_offset + rep_region_len, WASM_PAGE_SIZE);

    // Allocate the parent region via the global provider.
    let pages = pages_for_bytes(total_capacity);
    let region = selium_memory::region_provider()?
        .allocate(pages, RegionProt::ReadWrite, ResourceKind::SharedMemory)
        .map_err(Error::from)?;
    let shared_id = region.region_id();
    let parent_mapping = region.mapping();

    // Write the multi-memory header using the shared definition.
    MultiMemoryHeader::write_two_entries(
        parent_mapping.backend(),
        0,
        total_capacity,
        [
            (sub_memory_0_offset, req_region_len),
            (sub_memory_1_offset, rep_region_len),
        ],
    )
    .map_err(Error::from)?;

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

        let (req_ch, rep_ch, shared_id) =
            create_rpc_region(4096, 4096).expect("create_rpc_region");

        // Parent shared_id must be valid (non-zero).
        assert!(shared_id > 0, "parent shared_id must be non-zero");

        // Attach to the same region (simulates rpc::accept server side).
        let (attached_req, attached_rep) =
            attach_rpc_region(shared_id).expect("attach_rpc_region");

        // Both created and attached channels should use the same capacity.
        assert_eq!(attached_req.ring().capacity(), req_ch.ring().capacity());
        assert_eq!(attached_rep.ring().capacity(), rep_ch.ring().capacity());
    }
}
