//! RPC module for typed request/reply communication between guests.

use crate::{SHARED_REGION_MAGIC, SharedMemory, io::region::REGION_HEADER_BYTES};

pub use accept::RpcAccept;
pub use client::RpcClient;
pub use connection::{RpcConnection, RpcRequest};
pub use error::RpcError;

pub mod accept;
pub mod client;
pub mod connection;
pub mod error;

/// Parsed layout of the two ring-buffer sub-memories within an RPC shared region.
pub(crate) struct RpcChannelLayout {
    pub req_mapping: SharedMemory,
    pub req_data_capacity: u64,
    pub rep_mapping: SharedMemory,
    pub rep_data_capacity: u64,
}

/// Attaches to a multi-memory shared region, validates it contains exactly two
/// sub-memories, and returns mappings for the request and reply ring buffers.
pub(crate) fn attach_rpc_channels(shared_id: u64) -> Result<RpcChannelLayout, RpcError> {
    // Map enough of the header to read the multi-memory layout (256 bytes is
    // ample for two entries).
    let header = SharedMemory::attach_shared(shared_id, 0, 256)
        .map_err(|e| RpcError::Serialization(e.to_string()))?;

    let magic_bytes = header
        .read(0, 8)
        .map_err(|e| RpcError::Serialization(e.to_string()))?;
    let magic = u64::from_le_bytes(
        magic_bytes
            .try_into()
            .map_err(|_error| RpcError::InvalidRegion)?,
    );
    if magic != SHARED_REGION_MAGIC {
        return Err(RpcError::InvalidRegion);
    }

    let count = header
        .memory_count()
        .map_err(|e| RpcError::Serialization(e.to_string()))?;
    if count != 2 {
        return Err(RpcError::LayoutMismatch);
    }

    let (req_offset, req_len) = header
        .memory(0)
        .map_err(|e| RpcError::Serialization(e.to_string()))?;
    let (rep_offset, rep_len) = header
        .memory(1)
        .map_err(|e| RpcError::Serialization(e.to_string()))?;

    header
        .detach()
        .map_err(|e| RpcError::Serialization(e.to_string()))?;

    let req_mapping = SharedMemory::attach_shared(shared_id, req_offset, req_len)
        .map_err(|e| RpcError::Serialization(e.to_string()))?;
    let rep_mapping = SharedMemory::attach_shared(shared_id, rep_offset, rep_len)
        .map_err(|e| RpcError::Serialization(e.to_string()))?;

    let req_data_capacity = (req_len as u64).saturating_sub(REGION_HEADER_BYTES);
    let rep_data_capacity = (rep_len as u64).saturating_sub(REGION_HEADER_BYTES);

    Ok(RpcChannelLayout {
        req_mapping,
        req_data_capacity,
        rep_mapping,
        rep_data_capacity,
    })
}
