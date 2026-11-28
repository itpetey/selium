//! Selium ABI contracts shared by host and guest crates.
//!
//! # Capability Enforcement Matrix
//!
//! Grants are validated against an admission matrix at spawn time. A selector
//! is either **enforced** (evaluated against the runtime-built `ScopeContext`)
//! or **rejected** (the grant fails at spawn with a precise error naming the
//! selector). This prevents the "accept-then-always-deny" trap.
//!
//! | Selector | Status | Scope context field |
//! |---|---|---|
//! | `ResourceClass` | **Enforced** | `resource_class` |
//! | `Locality` | **Enforced** | `locality` |
//! | `ExplicitResource` | **Enforced** | `resource_id` |
//! | `Tenant` | **Enforced** | `tenant` (from process authority) |
//! | `Namespace` | **Enforced** | `tenant` (from process authority) |
//! | `Children` | **Enforced** | requires process-tree access (runtime) |
//! | `Uri` | **Enforced** | `uri` (exact, canonicalised) |
//! | `UriPrefix` | **Enforced*** | `uri` (network endpoints and `sel://`) |
//!
//! \* `UriPrefix` is evaluatable only when the same grant also carries a
//! selector the prefix applies to: a network `ResourceClass` selector
//! (`TcpListener`, `TcpStream`, or `UdpSocket`), or `Volume`. Otherwise the
//! grant is rejected at registration time. For `sel://` URIs the prefix is
//! path-segment aware rather than a plain string prefix.
//!
//! An **empty** selector list means "unrestricted within the capability" and is
//! explicitly accepted. Intersection semantics apply: all selectors in a grant
//! must match for the grant to apply to a given scope context.

pub mod client_identity;
/// Layout constants for the guest wake mailbox shared with the host.
///
/// The mailbox carries two wake channels:
///
/// - The **ring** (cooperative guests): the host enqueues task ids into the
///   ring and the guest drains it during a reactor poll. Single-producer
///   (host) / single-consumer (guest) by construction. Both sides access the
///   ring words with atomics; the `FLAG_OFFSET` word is an advisory hint only
///   (pending is derived from `head != tail`).
/// - The **parking-word area** (multithreaded guests): one shared `WAKE_WORD`
///   plus one `u32` parking word per task id. A wake is delivered by bumping
///   the target task's parking word and notifying, so a worker parked on the
///   shared wake word resumes in place — the host never drives the guest's
///   reactor as a unit. See `parking_word_offset`. Both the host and the guest
///   bump parking/wake words with genuine atomic read-modify-writes (the guest
///   never takes the host's memory lock), so the counter is never lost to a
///   torn read-modify-write.
pub mod mailbox {
    /// Byte offset of the ring head word.
    pub const HEAD_OFFSET: usize = 0;
    /// Byte offset of the wake flag word. Advisory only: the producer sets it
    /// on enqueue and the consumer clears it after draining, but wake-delivery
    /// correctness derives from `head != tail` (the flag set can be lost to a
    /// racing flag clear). Retained for ABI-layout stability.
    pub const FLAG_OFFSET: usize = 4;
    /// Byte offset of the ring tail word.
    pub const TAIL_OFFSET: usize = 8;
    /// Byte offset of the ring capacity word.
    pub const CAPACITY_OFFSET: usize = 12;
    /// Byte offset where ring slots begin.
    pub const RING_OFFSET: usize = 16;
    /// Number of task wake slots in the mailbox ring.
    pub const CAPACITY: usize = 32;
    /// Size in bytes of each mailbox slot.
    pub const SLOT_SIZE: usize = 4;
    /// Byte offset of the shared wake word (multithreaded guests). Bumped and
    /// notified whenever any task is enqueued or woken, so a worker parked on
    /// this word resumes to re-check the run queue.
    pub const WAKE_WORD_OFFSET: usize = RING_OFFSET + CAPACITY * SLOT_SIZE;
    /// Byte offset where the per-task parking-word table begins. Each entry is
    /// one `u32` (see [`parking_word_offset`]).
    pub const PARK_WORDS_OFFSET: usize = WAKE_WORD_OFFSET + 4;
    /// Number of per-task parking words. Task ids below this bound have a
    /// host-reachable parking word; ids at or above it fall back to the ring
    /// mailbox wake path.
    pub const PARK_WORDS_CAPACITY: usize = 1024;
    /// Byte offset of the process-stop word. The host sets it to a nonzero
    /// value and notifies the shared wake word to make multithreaded workers
    /// return from the worker entry (process teardown). Kept after the
    /// parking-word table so the ring/wake-word offsets stay stable.
    pub const STOP_OFFSET: usize = PARK_WORDS_OFFSET + PARK_WORDS_CAPACITY * SLOT_SIZE;
    /// Total mailbox byte length.
    pub const BYTE_LEN: usize = STOP_OFFSET + 4;

    /// Returns the byte offset of `task_id`'s parking word within the mailbox,
    /// or `None` when the task id is beyond [`PARK_WORDS_CAPACITY`].
    ///
    /// The word is a `u32` that the host bumps (with an atomic fetch-add) and
    /// notifies to resume a worker parked on the task; the guest's wake path
    /// bumps the same word so both sides observe one consistent counter.
    pub const fn parking_word_offset(task_id: super::TaskId) -> Option<usize> {
        let task_id = task_id as usize;
        if task_id < PARK_WORDS_CAPACITY {
            Some(PARK_WORDS_OFFSET + task_id * SLOT_SIZE)
        } else {
            None
        }
    }
}
pub mod uri;

mod capability;
mod codec;
mod constants;
mod error;
mod hostcall;
mod network;
mod process;
mod resource;

pub use capability::*;
pub use codec::*;
pub use constants::*;
pub use error::*;
pub use hostcall::*;
pub use network::*;
pub use process::*;
pub use resource::*;

#[cfg(test)]
mod tests;
