use selium_abi::{DiscoveryRequest, DiscoveryResponse, ResourceTarget};

use crate::{
    SharedRegion,
    io::rpc::{RpcClient, error::RpcError},
};

/// Standard size for an RPC session region.
pub const RPC_SESSION_REGION_SIZE: u32 = 32768;

/// Guest context injected by the runtime during bootstrap.
///
/// Provides a pre-connected discovery RPC client.
pub struct Context {
    discovery: RpcClient<DiscoveryRequest, DiscoveryResponse>,
}

impl Context {
    /// Creates a Context from a raw discovery handle (shared region id).
    pub fn from_raw(discovery_handle: u64) -> Result<Self, RpcError> {
        let region = SharedRegion::attach(discovery_handle, RPC_SESSION_REGION_SIZE);
        let discovery = RpcClient::attach(region)?;
        Ok(Self { discovery })
    }

    /// Resolves a URI to a resource.
    pub async fn lookup(&self, uri: &str) -> Result<Option<ResourceTarget>, RpcError> {
        match self
            .discovery
            .request(DiscoveryRequest::Resolve(uri.to_owned()))
            .await?
        {
            DiscoveryResponse::Found(t) => Ok(Some(t)),
            DiscoveryResponse::NotFound => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_raw_with_invalid_handle_fails() {
        let result = Context::from_raw(0);
        assert!(result.is_err());
    }
}
