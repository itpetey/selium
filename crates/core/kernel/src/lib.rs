//! Selium kernel primitives.

pub use backend::KernelBackend;
pub use error::{Error, Result};
pub use host_queue::HostQueueRegistry;
pub use kernel::Kernel;
pub use memory::MemoryRegistry;
pub use network::{NetworkState, TcpListenerState, TcpStreamState, UdpSocketState};
pub use process::ProcessTable;
pub use storage::StorageRegistry;

mod backend;
mod error;
mod host_queue;
mod kernel;
mod memory;
mod network;
mod network_runtime;
mod process;
mod storage;
