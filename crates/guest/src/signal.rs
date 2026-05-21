use selium_abi::{HostcallOutput, HostcallRequest, SignalDescriptor};

use crate::{
    GuestError, Result,
    hostcall::{hostcall_async, hostcall_ready},
};

/// Guest handle for a host signal.
#[derive(Clone, Debug)]
pub struct Signal {
    descriptor: SignalDescriptor,
}

impl Signal {
    /// Creates a new signal.
    pub fn create() -> Result<Self> {
        match hostcall_ready(HostcallRequest::SignalCreate)? {
            HostcallOutput::Signal(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Attaches to an existing signal by shared id.
    pub fn attach(shared_id: u64) -> Result<Self> {
        match hostcall_ready(HostcallRequest::SignalAttach { shared_id })? {
            HostcallOutput::Signal(descriptor) => Ok(Self { descriptor }),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Returns the signal descriptor.
    pub fn descriptor(&self) -> SignalDescriptor {
        self.descriptor
    }

    /// Returns the local signal handle id.
    pub fn local_id(&self) -> u64 {
        self.descriptor.local_id
    }

    /// Returns the shared signal id.
    pub fn shared_id(&self) -> u64 {
        self.descriptor.shared_id
    }

    /// Notifies signal waiters and returns the new generation.
    pub fn notify(&self) -> Result<u64> {
        match hostcall_ready(HostcallRequest::SignalNotify {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::SignalGeneration(generation) => Ok(generation),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Waits for the signal generation to advance beyond the observed value.
    pub async fn wait(&self, observed_generation: u64, timeout_ms: u64) -> Result<u64> {
        match hostcall_async(HostcallRequest::SignalWait {
            local_id: self.descriptor.local_id,
            observed_generation,
            timeout_ms,
        })
        .await?
        {
            HostcallOutput::SignalGeneration(generation) => Ok(generation),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }

    /// Closes the local signal handle.
    pub fn close(self) -> Result<()> {
        match hostcall_ready(HostcallRequest::SignalClose {
            local_id: self.descriptor.local_id,
        })? {
            HostcallOutput::Empty => Ok(()),
            _ => Err(GuestError::UnexpectedHostcallOutput),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_hostcalls_are_unavailable() {
        let result = Signal::create();

        assert!(matches!(result, Err(GuestError::Host(_))));
    }
}
