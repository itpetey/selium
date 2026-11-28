//! Process, activity, log, and metering descriptors.

use crate::ProcessId;
use rkyv::{Archive, Deserialize, Serialize};

/// Metadata describing a guest entrypoint export.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct EntrypointMetadata {
    /// Entrypoint export name.
    pub name: String,
}

/// Descriptor for a guest process.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ProcessDescriptor {
    /// Process id.
    pub local_id: ProcessId,
    /// Module id used to start the process.
    pub module_id: String,
    /// Entrypoint export used to start the process.
    pub entrypoint: String,
}

/// Source of a process's module bytes.
///
/// The runtime holds no module catalogue: every spawn supplies the module
/// bytes, either inline in the hostcall payload or as a durable slice the
/// runtime reads from its own volume layer under the caller's storage
/// authority. A slice keeps module bytes from round-tripping through a guest.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ModuleSource {
    /// Inline module bytes carried in the hostcall payload.
    Inline(Vec<u8>),
    /// A durable slice: the runtime reads `len` bytes at `offset` from the
    /// named volume through its own volume layer.
    Slice {
        /// Volume name holding the module bytes.
        volume: String,
        /// Byte offset of the module within the volume.
        offset: u64,
        /// Byte length of the module.
        len: u64,
    },
}

/// Kind of event recorded in the activity log.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ActivityKind {
    /// A process was started.
    ProcessStarted,
    /// A system guest was bootstrapped.
    GuestBootstrapped,
    /// A process was stopped.
    ProcessStopped,
    /// A process exited or trapped.
    ProcessExited,
    /// Metering was updated for a process.
    MeteringObserved,
}

/// Activity log event emitted by the kernel or runtime.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ActivityEvent {
    /// Event kind.
    pub kind: ActivityKind,
    /// Process associated with the event, when any.
    pub process_id: Option<ProcessId>,
    /// Event message.
    pub message: String,
}

/// Log entry emitted by a guest.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct GuestLogEntry {
    /// Process associated with the entry, when known.
    pub process_id: Option<ProcessId>,
    /// Log level name.
    pub level: String,
    /// Log target name.
    pub target: String,
    /// Log message.
    pub message: String,
}

/// Resource usage observation for a process.
#[derive(Debug, Clone, PartialEq, Eq, Default, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct MeteringObservation {
    /// Cumulative WebAssembly instructions executed by the process.
    pub cpu_instructions: u64,
    /// Memory usage in bytes.
    pub memory_bytes: u64,
    /// Network bandwidth usage in bytes.
    pub bandwidth_bytes: u64,
}

impl EntrypointMetadata {
    /// Creates entrypoint metadata with the supplied export name.
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}
