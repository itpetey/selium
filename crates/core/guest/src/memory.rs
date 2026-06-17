use selium_abi::{
    HostcallOutput, HostcallRequest, RegionAllocation, RegionAttachment, RegionProt, ResourceKind,
};

use crate::{GuestError, Result, hostcall::hostcall_ready};

/// Magic value for multi-memory shared region layout headers.
pub const SHARED_REGION_MAGIC: u64 = 0x53454C49554D454D;

/// An allocated shared memory region.
///
/// The region is mapped into the allocating guest's linear memory at the
/// returned `page_offset`. Other guests can attach via `attach_region`.
#[derive(Clone, Copy, Debug)]
pub struct SharedRegion {
    allocation: RegionAllocation,
}

impl SharedRegion {
    /// Allocates a shared memory region with the requested number of pages.
    pub fn allocate(pages: u32, prot: RegionProt, purpose: ResourceKind) -> Result<Self> {
        match hostcall_ready(HostcallRequest::AllocRegion {
            pages,
            prot,
            purpose,
        })? {
            HostcallOutput::RegionAlloc(allocation) => Ok(Self { allocation }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Returns the region allocation descriptor.
    pub fn allocation(&self) -> RegionAllocation {
        self.allocation
    }

    /// Returns the region id.
    pub fn region_id(&self) -> u64 {
        self.allocation.region_id
    }

    /// Returns the page offset within guest linear memory.
    pub fn page_offset(&self) -> u32 {
        self.allocation.page_offset
    }

    /// Frees the shared region.
    pub fn free(self) -> Result<()> {
        match hostcall_ready(HostcallRequest::FreeRegion {
            region_id: self.allocation.region_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

/// Attaches an existing shared region into this guest's linear memory.
pub fn attach_region(
    region_id: u64,
    reader_slot: Option<u32>,
    prot: RegionProt,
) -> Result<RegionAttachment> {
    match hostcall_ready(HostcallRequest::AttachRegion {
        region_id,
        reader_slot,
        prot,
    })? {
        HostcallOutput::RegionAttach(attachment) => Ok(attachment),
        _ => Err(GuestError::UnexpectedHostcallOutput),
    }
}

/// Frees a shared region by id.
pub fn free_region(region_id: u64) -> Result<()> {
    match hostcall_ready(HostcallRequest::FreeRegion { region_id })? {
        HostcallOutput::Empty => Ok(()),
        _ => Err(GuestError::UnexpectedHostcallOutput),
    }
}
