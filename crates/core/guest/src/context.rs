#[cfg(feature = "io")]
use selium_abi::{DiscoveryRequest, DiscoveryResponse, ResourceTarget};

#[cfg(feature = "io")]
use crate::GuestError;

#[cfg(feature = "io")]
use crate::resource::ResourceSender;

#[cfg(feature = "io")]
use crate::io::{FrameHeader, RingBuf};

#[cfg(feature = "io")]
use std::sync::atomic::{Ordering, fence};

/// RPC ring capacity for discovery requests.
#[cfg(feature = "io")]
const RPC_REQ_CAPACITY: u64 = 4096;

/// RPC ring capacity for discovery replies.
#[cfg(feature = "io")]
const RPC_REP_CAPACITY: u64 = 4096;

/// Guest context injected by the runtime during bootstrap.
///
/// Provides a pre-connected discovery client for URI resolution via RPC
/// over shared-memory ring buffers.
///
/// # Implementation Note
///
/// This struct implements the discovery RPC protocol inline rather than
/// using `RpcClient<DiscoveryRequest, DiscoveryResponse>` from `selium-rpc`.
/// This is because `selium-rpc` depends on `selium-guest`, creating a
/// circular dependency if `selium-guest` were to depend on `selium-rpc`.
///
/// The inline implementation uses the same ring buffer protocol as
/// `selium-rpc`: a multi-memory region with request and reply rings,
/// rkyv-encoded frames with correlation tags, and generation counter
/// polling for blocking.
///
/// In a future refactoring, the RPC protocol could be extracted into a
/// shared crate that both `selium-guest` and `selium-rpc` depend on,
/// eliminating the duplication.
pub struct Context {
    #[cfg(feature = "io")]
    request_ring: RingBuf,
    #[cfg(feature = "io")]
    reply_ring: RingBuf,
    #[cfg(feature = "io")]
    next_correlation: u32,
    #[cfg(not(feature = "io"))]
    _private: (),
}

impl Context {
    /// Creates a Context from a raw discovery handle.
    ///
    /// Attaches a `ResourceSender` to the discovery host queue and creates
    /// RPC ring buffers for discovery requests.
    #[cfg(feature = "io")]
    pub async fn from_raw(discovery_handle: u64) -> Result<Self, GuestError> {
        // Attach to the discovery host queue.
        let sender = ResourceSender::attach(discovery_handle)?;

        // Create RPC ring buffers for discovery.
        let request_ring = RingBuf::create(RPC_REQ_CAPACITY)
            .map_err(|e| GuestError::Host(format!("create request ring: {e}")))?;
        let reply_ring = RingBuf::create(RPC_REP_CAPACITY)
            .map_err(|e| GuestError::Host(format!("create reply ring: {e}")))?;

        // Increment writer counts to indicate we're connected.
        request_ring
            .region()
            .increment_writer_count()
            .map_err(|e| GuestError::Host(format!("increment request writer count: {e}")))?;
        reply_ring
            .region()
            .increment_writer_count()
            .map_err(|e| GuestError::Host(format!("increment reply writer count: {e}")))?;

        // Send the request ring's shared_id to the discovery service.
        // In a real implementation, we'd send a shared_id that identifies
        // both rings. For now, we send 0 as a placeholder.
        sender
            .send(0)
            .await
            .map_err(|e| GuestError::Host(format!("send discovery handle: {e}")))?;

        Ok(Self {
            request_ring,
            reply_ring,
            next_correlation: 1,
        })
    }

    #[cfg(not(feature = "io"))]
    pub async fn from_raw() -> Result<Self, ()> {
        Ok(Self { _private: () })
    }

    /// Resolves a URI to a resource via the discovery service.
    #[cfg(feature = "io")]
    pub async fn lookup(&mut self, uri: &str) -> Result<Option<ResourceTarget>, GuestError> {
        let correlation = self.next_correlation;
        self.next_correlation = self.next_correlation.wrapping_add(1);

        // Encode the discovery request.
        let request = DiscoveryRequest::Resolve(uri.to_string());
        let encoded = selium_abi::encode_rkyv(&request)
            .map_err(|e| GuestError::Host(format!("encode discovery request: {e}")))?;

        // Write the request frame.
        let frame_size = FrameHeader::ENCODED_SIZE as u64 + encoded.len() as u64;
        let pos = self
            .request_ring
            .reserve(frame_size)
            .map_err(|e| GuestError::Host(format!("reserve request: {e}")))?;
        self.request_ring
            .write_frame(pos, &encoded, correlation, 0)
            .map_err(|e| GuestError::Host(format!("write request: {e}")))?;

        // Block on the reply ring's generation counter.
        let mut last_generation = self
            .reply_ring
            .generation()
            .map_err(|e| GuestError::Host(format!("load generation: {e}")))?;

        loop {
            // Poll the generation counter for changes.
            let current_generation = self
                .reply_ring
                .generation()
                .map_err(|e| GuestError::Host(format!("load generation: {e}")))?;

            if current_generation != last_generation {
                last_generation = current_generation;

                // Acquire fence ensures we see the writer's payload.
                fence(Ordering::Acquire);

                // Try to read a reply frame.
                match self.reply_ring.read_frame_header(0) {
                    Ok(header) if header.is_ready() && header.tag == correlation => {
                        let payload_pos = FrameHeader::ENCODED_SIZE as u64;
                        let payload_bytes = self
                            .reply_ring
                            .read_at(payload_pos, header.len as u64)
                            .map_err(|e| GuestError::Host(format!("read reply: {e}")))?;

                        let response: DiscoveryResponse =
                            selium_abi::decode_rkyv(&payload_bytes)
                                .map_err(|e| GuestError::Host(format!("decode reply: {e}")))?;

                        return match response {
                            DiscoveryResponse::Found(target) => Ok(Some(target)),
                            DiscoveryResponse::NotFound => Ok(None),
                        };
                    }
                    _ => {}
                }
            }

            // Check if the discovery service has disconnected.
            let writer_count = self
                .reply_ring
                .region()
                .load_writer_count()
                .map_err(|e| GuestError::Host(format!("load writer count: {e}")))?;
            if writer_count == 0 {
                return Err(GuestError::Host(
                    "discovery service disconnected".to_string(),
                ));
            }

            // Yield to allow the discovery service to process.
            crate::yield_now().await;
        }
    }

    #[cfg(not(feature = "io"))]
    pub async fn lookup(&self, _uri: &str) -> Result<Option<()>, ()> {
        Err(())
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
