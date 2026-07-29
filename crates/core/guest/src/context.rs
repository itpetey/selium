use selium_abi::{DiscoveryRequest, DiscoveryResponse, ResourceTarget};
use selium_shm::rpc::{self, RpcClient};

use crate::{GuestError, resource::ResourceSender};

/// RPC ring capacity for discovery replies.
const RPC_REP_CAPACITY: u64 = 4096;
/// RPC ring capacity for discovery requests.
const RPC_REQ_CAPACITY: u64 = 4096;

/// Guest context injected by the runtime during bootstrap.
///
/// Provides a pre-connected discovery client for URI resolution via RPC
/// over shared-memory ring buffers.
pub struct Context {
    client: RpcClient<DiscoveryRequest, DiscoveryResponse>,
}

impl Context {
    /// Returns a mutable reference to the pre-connected discovery RPC client.
    ///
    /// Use this to send custom discovery requests beyond the convenience
    /// `lookup()` method.
    pub fn discovery(&mut self) -> &mut RpcClient<DiscoveryRequest, DiscoveryResponse> {
        &mut self.client
    }

    /// Creates a Context from a raw discovery handle.
    ///
    /// Attaches a `ResourceSender` to the discovery host queue and creates
    /// an `RpcClient` for discovery requests.
    pub async fn from_raw(discovery_handle: u64) -> Result<Self, GuestError> {
        // Attach to the discovery host queue.
        let sender = ResourceSender::attach(discovery_handle)?;

        // Create RPC client for discovery.
        let client = rpc::connect(sender, RPC_REQ_CAPACITY, RPC_REP_CAPACITY)
            .await
            .map_err(|e| GuestError::Host(format!("create RPC client: {e}")))?;

        Ok(Self { client })
    }

    /// Resolves a URI to a resource via the discovery service.
    ///
    /// Convenience method that delegates to `self.discovery().request()`.
    pub async fn lookup(&mut self, uri: &str) -> Result<Option<ResourceTarget>, GuestError> {
        let request = DiscoveryRequest::Resolve(uri.to_string());

        let response = self
            .discovery()
            .request(request)
            .await
            .map_err(|e| GuestError::Host(format!("discovery request: {e}")))?;

        match response {
            DiscoveryResponse::Found(target) => Ok(Some(target)),
            DiscoveryResponse::NotFound => Ok(None),
            DiscoveryResponse::Registered
            | DiscoveryResponse::Revoked
            | DiscoveryResponse::Forbidden => Err(GuestError::Host(
                "unexpected discovery response variant".to_string(),
            )),
        }
    }

    /// Registers a URI→target mapping in the discovery service.
    ///
    /// Convenience method that delegates to `self.discovery().request()`.
    /// Returns `Err(GuestError::Host("registration forbidden"))` if the discovery
    /// service rejects the registration.
    pub async fn register(&mut self, uri: &str, target: ResourceTarget) -> Result<(), GuestError> {
        let request = DiscoveryRequest::Register {
            uri: uri.to_string(),
            target,
        };

        let response = self
            .discovery()
            .request(request)
            .await
            .map_err(|e| GuestError::Host(format!("discovery register: {e}")))?;

        match response {
            DiscoveryResponse::Registered => Ok(()),
            DiscoveryResponse::Forbidden => Err(GuestError::Host(
                "registration forbidden: process does not own resource".to_string(),
            )),
            other => Err(GuestError::Host(format!(
                "unexpected discovery response: {other:?}"
            ))),
        }
    }

    /// Revokes a URI→target mapping in the discovery service.
    ///
    /// Convenience method that delegates to `self.discovery().request()`.
    pub async fn revoke(&mut self, uri: &str) -> Result<(), GuestError> {
        let request = DiscoveryRequest::Revoke {
            uri: uri.to_string(),
        };

        let response = self
            .discovery()
            .request(request)
            .await
            .map_err(|e| GuestError::Host(format!("discovery revoke: {e}")))?;

        match response {
            DiscoveryResponse::Revoked => Ok(()),
            other => Err(GuestError::Host(format!(
                "unexpected discovery response: {other:?}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn from_raw_with_invalid_handle_fails() {
        let result = Context::from_raw(0).await;
        assert!(result.is_err());
        // In native mode, ResourceSender::attach(0) fails because there's
        // no host queue infrastructure.
    }
}
