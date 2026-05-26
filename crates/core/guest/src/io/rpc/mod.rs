//! RPC module for typed request/reply communication between guests.

pub use accept::RpcAccept;
pub use client::RpcClient;
pub use connection::{RpcConnection, RpcRequest};
pub use context::{Context, RPC_SESSION_REGION_SIZE};
pub use error::RpcError;

pub mod accept;
pub mod client;
pub mod connection;
pub mod context;
pub mod error;
