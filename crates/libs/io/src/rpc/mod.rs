//! RPC module for typed request/reply communication between guests.

pub mod accept;
pub mod client;
pub mod connection;
pub mod error;

pub use accept::RpcAccept;
pub use client::RpcClient;
pub use connection::{RpcConnection, RpcRequest};
pub use error::RpcError;
