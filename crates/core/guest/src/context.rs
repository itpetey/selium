#[cfg(feature = "io")]
use selium_abi::{DiscoveryRequest, DiscoveryResponse, ResourceTarget};

#[cfg(feature = "io")]
use crate::{
    GuestError, ResourceSender,
    io::rpc::{RpcClient, error::RpcError},
};

/// Size of RPC request buf (min. 512 URI chars)
#[cfg(feature = "io")]
pub const RPC_REQ_CAPACITY: u32 = 2048;
/// Size of RPC reply buf (4x shared_id replies)
#[cfg(feature = "io")]
pub const RPC_REP_CAPACITY: u32 = 36;

/// Guest context injected by the runtime during bootstrap.
///
/// Provides a pre-connected discovery RPC client.
pub struct Context {
    #[cfg(feature = "io")]
    discovery: RpcClient<DiscoveryRequest, DiscoveryResponse>,
}

impl Context {
    /// Creates a Context from a raw discovery handle (shared region id).
    #[cfg(feature = "io")]
    pub async fn from_raw(discovery_handle: u64) -> Result<Self, GuestError> {
        use crate::io;

        let sender = ResourceSender::attach(discovery_handle)?;
        let discovery = RpcClient::connect(sender, RPC_REQ_CAPACITY, RPC_REP_CAPACITY)
            .await
            .map_err(|e| GuestError::Io(io::Error::Rpc(e)))?;
        Ok(Self { discovery })
    }
    #[cfg(not(feature = "io"))]
    pub async fn from_raw() -> Result<Self, ()> {
        Ok(Self {})
    }

    /// Resolves a URI to a resource.
    #[cfg(feature = "io")]
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

    #[tokio::test]
    async fn from_raw_with_invalid_handle_fails() {
        let result = Context::from_raw(0).await;
        assert!(result.is_err());
    }
}
