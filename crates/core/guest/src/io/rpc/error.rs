use std::fmt;

use crate::io::{Error, channels};

/// Error type for RPC operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    /// The peer has closed the connection.
    ConnectionClosed,
    /// The shared region is invalid or corrupted.
    InvalidRegion,
    /// The region layout does not match the expected structure.
    LayoutMismatch,
    /// The ring buffer is full.
    BufferFull,
    /// The ring buffer is empty.
    BufferEmpty,
    /// Encoding or decoding failed.
    Serialization(String),
}

/// Error type for connection acceptance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AcceptError {
    /// The shared region is invalid or corrupted.
    InvalidRegion,
    /// The region layout does not match the expected structure.
    LayoutMismatch,
    /// The peer has closed the connection.
    ConnectionClosed,
    /// The ring buffer is full.
    BufferFull,
    /// The ring buffer is empty.
    BufferEmpty,
    /// Encoding or decoding failed.
    Serialization(String),
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectionClosed => write!(f, "RPC connection closed"),
            Self::InvalidRegion => write!(f, "invalid shared region"),
            Self::LayoutMismatch => write!(f, "region layout mismatch"),
            Self::BufferFull => write!(f, "RPC buffer full"),
            Self::BufferEmpty => write!(f, "RPC buffer empty"),
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for RpcError {}

impl From<Error> for RpcError {
    fn from(error: Error) -> Self {
        match error {
            Error::BufferFull => Self::BufferFull,
            Error::BufferEmpty => Self::BufferEmpty,
            Error::InvalidLayout => Self::InvalidRegion,
            Error::Guest(msg) => Self::Serialization(msg),
            other => Self::Serialization(other.to_string()),
        }
    }
}

impl From<channels::Error> for RpcError {
    fn from(error: channels::Error) -> Self {
        match error {
            channels::Error::ChannelFull => Self::BufferFull,
            channels::Error::ChannelEmpty => Self::BufferEmpty,
            channels::Error::InvalidFrame => Self::Serialization("invalid frame".to_string()),
            channels::Error::Core(e) => e.into(),
            other => Self::Serialization(other.to_string()),
        }
    }
}

impl fmt::Display for AcceptError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRegion => write!(f, "invalid shared region"),
            Self::LayoutMismatch => write!(f, "region layout mismatch"),
            Self::ConnectionClosed => write!(f, "connection closed"),
            Self::BufferFull => write!(f, "accept buffer full"),
            Self::BufferEmpty => write!(f, "accept buffer empty"),
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for AcceptError {}

impl From<Error> for AcceptError {
    fn from(error: Error) -> Self {
        match error {
            Error::BufferFull => Self::BufferFull,
            Error::BufferEmpty => Self::BufferEmpty,
            Error::InvalidLayout => Self::InvalidRegion,
            Error::Guest(msg) => Self::Serialization(msg),
            other => Self::Serialization(other.to_string()),
        }
    }
}

impl From<channels::Error> for AcceptError {
    fn from(error: channels::Error) -> Self {
        match error {
            channels::Error::ChannelFull => Self::BufferFull,
            channels::Error::ChannelEmpty => Self::BufferEmpty,
            channels::Error::InvalidFrame => Self::Serialization("invalid frame".to_string()),
            channels::Error::Core(e) => e.into(),
            other => Self::Serialization(other.to_string()),
        }
    }
}
