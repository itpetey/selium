use selium_abi::{HostQueueDescriptor, HostcallOutput, HostcallRequest};

use crate::{
    GuestError, Result,
    hostcall::{hostcall_async, hostcall_ready},
};

/// An incoming connection from a client.
#[derive(Debug, Clone, Copy)]
pub struct IncomingConnection {
    /// Process id of the connecting client.
    pub client_process_id: u64,
    /// Shared region id of the session.
    pub shared_id: u64,
}

/// Trait for accepting raw connections and turning them into typed resources.
pub trait Accept {
    /// The typed resource produced by acceptance.
    type Item;
    /// Accepts a raw incoming connection and produces a typed resource.
    fn accept(connection: IncomingConnection) -> Result<Self::Item>;
}

/// A sender that enqueues connections into a host-mediated queue.
#[derive(Clone, Debug)]
pub struct ResourceSender {
    descriptor: selium_abi::HostQueueDescriptor,
}

impl ResourceSender {
    /// Creates a new sender by attaching to an existing shared queue.
    pub fn attach(shared_id: u64) -> Result<Self> {
        match hostcall_ready(HostcallRequest::HostQueueAttach { shared_id })? {
            HostcallOutput::HostQueue(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Returns the queue descriptor.
    pub fn descriptor(&self) -> selium_abi::HostQueueDescriptor {
        self.descriptor
    }

    /// Sends a value to the connection queue.
    pub async fn send(&self, value: u64) -> Result<()> {
        match hostcall_async(HostcallRequest::HostQueueSend {
            local_id: self.descriptor.local_id,
            value,
        })
        .await?
        {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

/// A listener that accepts incoming typed connections from a host-mediated queue.
#[derive(Clone, Debug)]
pub struct ResourceListener {
    descriptor: selium_abi::HostQueueDescriptor,
}

impl ResourceListener {
    /// Creates a new host-mediated connection queue.
    pub fn create() -> Result<Self> {
        match hostcall_ready(HostcallRequest::HostQueueCreate)? {
            HostcallOutput::HostQueue(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Creates `Self` from an externally created queue.
    pub fn from_queue(descriptor: HostQueueDescriptor) -> Self {
        Self { descriptor }
    }

    /// Attaches to an existing shared queue.
    pub fn attach(shared_id: u64) -> Result<Self> {
        match hostcall_ready(HostcallRequest::HostQueueAttach { shared_id })? {
            HostcallOutput::HostQueue(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Returns the queue descriptor.
    pub fn descriptor(&self) -> selium_abi::HostQueueDescriptor {
        self.descriptor
    }

    /// Accepts the next incoming connection, mapping it through `A::accept`.
    pub async fn accept<A: Accept>(&self) -> Result<A::Item> {
        let connection = self.recv().await?;
        A::accept(connection)
    }

    /// Receives the next pending connection entry.
    pub async fn recv(&self) -> Result<IncomingConnection> {
        match hostcall_async(HostcallRequest::HostQueueRecv {
            local_id: self.descriptor.local_id,
        })
        .await?
        {
            HostcallOutput::ConnectionInfo {
                client_process_id,
                value,
            } => Ok(IncomingConnection {
                client_process_id,
                shared_id: value,
            }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}
