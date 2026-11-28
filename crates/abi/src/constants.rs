//! ABI identifiers, status codes, and protocol limits.

/// Identifier for a resource handle that is local to one process or host context.
pub type LocalResourceId = u64;
/// Identifier for an asynchronous hostcall operation.
pub type OperationId = u64;
/// Identifier for a Selium process.
pub type ProcessId = u64;
/// Identifier for a resource that may be shared across local handles.
pub type SharedResourceId = u64;
/// Identifier for a guest task waiting on host progress.
pub type TaskId = u32;

/// Packed status code for a dropped hostcall.
pub const HOSTCALL_STATUS_DROPPED: u32 = 4;
/// Packed status code for a failed hostcall.
pub const HOSTCALL_STATUS_FAILED: u32 = 2;
/// Packed status code for an output buffer that is too small.
pub const HOSTCALL_STATUS_OUTPUT_TOO_SMALL: u32 = 3;
/// Packed status code for a pending hostcall.
pub const HOSTCALL_STATUS_PENDING: u32 = 1;
/// Packed status code for a ready hostcall.
pub const HOSTCALL_STATUS_READY: u32 = 0;
/// Maximum byte length of a `HostQueueSend` metadata payload. Senders
/// exceeding this bound are rejected by the runtime before the payload
/// reaches the kernel queue, bounding per-entry queue memory. Generous
/// headroom over the ~45-byte [`crate::client_identity::ClientIdentity`] encoding
/// for future metadata users (e.g. HTTP identity hints).
pub const METADATA_MAX_BYTES: usize = 4096;

/// Maximum encoded byte length of a [`crate::HostcallEnvelope`] accepted from a guest.
///
/// The host rejects a declared envelope length above this ceiling before
/// allocating a decode buffer, so hostile input cannot request an unbounded
/// allocation. The ceiling accommodates inline system-guest modules (hundreds
/// of KiB) with headroom while keeping per-request host memory bounded.
pub const HOSTCALL_ENVELOPE_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Greatest sleep duration, in milliseconds, that a guest may request.
///
/// A hundred years is far beyond any legitimate platform timer while leaving
/// comfortable headroom for `Instant` representation on every supported
/// target. Requests above this ceiling (including `u64::MAX`) are rejected
/// with `MalformedPayload` before a deadline or timer is created.
pub const MAX_SLEEP_MILLIS: u64 = 100 * 365 * 24 * 60 * 60 * 1000;

/// Magic value identifying a Selium hostcall request preamble.
pub const HOSTCALL_MAGIC: u32 = 0x5345_4C4D;
/// Current host/guest ABI version for hostcall request encoding.
///
/// Host and guest packages share this version in lockstep during the alpha:
/// a request carrying any other version is rejected before rkyv enum
/// dispatch, so an incompatible layout is never decoded as another variant.
/// Version 2 also makes hostcall creation return the full [`OperationId`];
/// status/length packing remains confined to polling, whose value is a `u32`.
/// Version 3 replaces executable negotiation with the passive custom section
/// below, so no guest code runs before its declared version is validated.
pub const SELIUM_ABI_VERSION: u32 = 3;
/// Custom section containing the guest's ABI version as four little-endian
/// bytes. The runtime reads this declaration from the module bytes without
/// instantiating or executing guest code.
pub const SELIUM_ABI_VERSION_SECTION: &str = "selium_abi_version";
/// Fixed preamble size: `[magic: u32 LE][abi version: u32 LE]`.
pub const HOSTCALL_PREAMBLE_SIZE: usize = 8;
