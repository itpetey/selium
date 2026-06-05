//! Selium RPC pattern crate.
//!
//! Provides typed request/reply communication between guests using
//! shared-memory ring buffers. Built on top of `selium-guest` IO primitives.
//!
//! # Status
//!
//! This crate contains stub implementations. The full RPC protocol relied on
//! the removed Signal and host-mediated SharedMemory APIs. A complete
//! implementation against the new `alloc_region`/`attach_region` ABI will
//! follow in a subsequent change.

pub use error::{AcceptError, RpcError};

pub mod error;

use std::marker::PhantomData;

/// Client-side handle for making typed RPC requests.
///
/// Stub: the full implementation will allocate a shared region with two
/// sub-ring-buffers (request + reply), create the writer/reader pair, and
/// transmit the region id to the server via a `ResourceSender`.
pub struct RpcClient<Req, Rep> {
    _phantom: PhantomData<(Req, Rep)>,
}

impl<Req, Rep> RpcClient<Req, Rep> {
    /// Creates a new RPC session (stub — always returns error).
    pub async fn connect(
        _sender: selium_guest::ResourceSender,
        _request_capacity: u64,
        _reply_capacity: u64,
    ) -> Result<Self, RpcError> {
        Err(RpcError::ConnectionClosed)
    }

    /// Sends a typed request and awaits the matching reply (stub).
    pub async fn request(&self, _payload: Req) -> Result<Rep, RpcError> {
        Err(RpcError::ConnectionClosed)
    }
}

/// Server-side handle for an established RPC session.
pub struct RpcConnection<Req, Rep> {
    _phantom: PhantomData<(Req, Rep)>,
}

impl<Req, Rep> RpcConnection<Req, Rep> {
    /// Creates an RPC connection from the server side (stub).
    pub fn for_server(_shared_id: u64, _client_process_id: u64) -> Result<Self, RpcError> {
        Err(RpcError::ConnectionClosed)
    }

    /// Receives the next request from the client (stub).
    pub async fn recv(&mut self) -> Result<RpcRequest<'_, Req, Rep>, RpcError> {
        Err(RpcError::ConnectionClosed)
    }
}

/// A single request received by the server, with the ability to reply.
pub struct RpcRequest<'a, Req, Rep> {
    _phantom: PhantomData<(&'a (), Req, Rep)>,
}

impl<'a, Req, Rep> RpcRequest<'a, Req, Rep> {
    /// Returns a reference to the request payload (stub).
    pub fn payload(&self) -> &Req {
        unreachable!("RPC stub: payload not available")
    }

    /// Returns the deserialized request payload (stub).
    pub fn into_payload(self) -> Req {
        unreachable!("RPC stub: payload not available")
    }

    /// Sends a reply to the client (stub).
    pub async fn reply(self, _response: Rep) -> Result<(), RpcError> {
        Err(RpcError::ConnectionClosed)
    }
}

/// Accept implementation for RPC connections.
pub struct RpcAccept<Req, Rep>(PhantomData<(Req, Rep)>);

impl<Req, Rep> selium_guest::Accept for RpcAccept<Req, Rep> {
    type Item = RpcConnection<Req, Rep>;

    fn accept(
        _connection: selium_guest::IncomingConnection,
    ) -> selium_guest::Result<Self::Item> {
        Err(selium_guest::GuestError::Host(
            "RPC not yet implemented against new shared memory ABI".to_string(),
        ))
    }
}
