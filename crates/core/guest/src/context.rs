#[cfg(feature = "io")]
use selium_abi::ResourceTarget;

#[cfg(feature = "io")]
use crate::GuestError;

/// Guest context injected by the runtime during bootstrap.
///
/// Provides a pre-connected discovery client. The RPC-based implementation
/// has been removed alongside the Signal/SharedMemory ABI changes. Discovery
/// will be re-implemented against the new shared memory ABI in a follow-up.
pub struct Context {
    _private: (),
}

impl Context {
    /// Creates a Context from a raw discovery handle.
    ///
    /// Currently returns an error since the RPC client has been stubbed out.
    #[cfg(feature = "io")]
    pub async fn from_raw(_discovery_handle: u64) -> Result<Self, GuestError> {
        Err(GuestError::Host(
            "discovery RPC not yet implemented against new shared memory ABI".to_string(),
        ))
    }

    #[cfg(not(feature = "io"))]
    pub async fn from_raw() -> Result<Self, ()> {
        Ok(Self { _private: () })
    }

    /// Resolves a URI to a resource.
    #[cfg(feature = "io")]
    pub async fn lookup(&self, _uri: &str) -> Result<Option<ResourceTarget>, GuestError> {
        Err(GuestError::Host(
            "discovery RPC not yet implemented against new shared memory ABI".to_string(),
        ))
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
