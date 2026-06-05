//! UDP socket backed by shared-memory ring buffers.
//!
//! This module is a stub. The full implementation relied on the removed
//! `Signal` and host-mediated `SharedMemory` APIs. It will be re-implemented
//! against the new `alloc_region` / `attach_region` ABI in a follow-up
//! networking change.

use std::net::SocketAddr;

use selium_abi::{HostcallOutput, HostcallRequest};

use crate::{GuestError, Result, hostcall::hostcall_async};

/// A UDP socket backed by shared-memory ring buffers (stub).
#[derive(Clone)]
pub struct UdpSocket {
    pub(super) local_addr: SocketAddr,
}

impl UdpSocket {
    /// Binds a UDP socket via the host.
    pub async fn bind(address: impl Into<String>) -> Result<Self> {
        let address = address.into();
        let _descriptor = match hostcall_async(HostcallRequest::UdpBind {
            address: address.clone(),
        })
        .await?
        {
            HostcallOutput::SharedRegion(descriptor) => descriptor,
            _ => return Err(GuestError::UnexpectedHostcallOutput),
        };

        let _local_addr: SocketAddr = address
            .parse()
            .map_err(|_error| GuestError::Host(format!("invalid socket address: {address}")))?;

        Err(GuestError::Host(
            "UDP socket not yet implemented against new shared memory ABI".to_string(),
        ))
    }

    /// Returns the local socket address.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        Ok(self.local_addr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_returns_error_for_stub() {
        // The stub always returns an error since the old API is removed.
        let rt = tokio::runtime::Runtime::new().expect("runtime");
        let result = rt.block_on(UdpSocket::bind("127.0.0.1:0"));
        assert!(result.is_err());
    }
}
