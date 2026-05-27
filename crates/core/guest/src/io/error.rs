use thiserror::Error;

use crate::io::rpc::RpcError;

/// Result type for selium-io core operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for selium-io core operations.
#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error("invalid ring buffer layout")]
    InvalidLayout,
    #[error("ring buffer full")]
    BufferFull,
    #[error("ring buffer empty")]
    BufferEmpty,
    #[error("reader was overtaken by writers")]
    ReaderBehind,
    #[error("ring buffer reservation contended")]
    ReservationContended,
    #[error("invalid frame header")]
    InvalidFrame,
    #[error("capacity exceeded")]
    CapacityExceeded,
    #[error("compare-and-set failed: expected {expected}, got {actual:?}")]
    CasConflict { expected: u64, actual: Option<u64> },
    #[error("invalid signal")]
    InvalidSignal,
    #[error("ABI error: {0:?}")]
    Abi(selium_abi::AbiErrorCode),
    #[error("guest error: {0}")]
    Guest(String),
    #[error("index out of bounds")]
    IndexOutOfBounds,
    #[error("RPC error: {0}")]
    Rpc(RpcError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_covers_core_error_variants() {
        assert_eq!(
            Error::InvalidLayout.to_string(),
            "invalid ring buffer layout"
        );
        assert_eq!(Error::BufferFull.to_string(), "ring buffer full");
        assert_eq!(Error::BufferEmpty.to_string(), "ring buffer empty");
        assert_eq!(
            Error::ReaderBehind.to_string(),
            "reader was overtaken by writers"
        );
        assert_eq!(
            Error::ReservationContended.to_string(),
            "ring buffer reservation contended"
        );
        assert_eq!(Error::InvalidFrame.to_string(), "invalid frame header");
        assert_eq!(Error::CapacityExceeded.to_string(), "capacity exceeded");
        assert_eq!(Error::InvalidSignal.to_string(), "invalid signal");
        assert_eq!(
            Error::Guest("failed".to_string()).to_string(),
            "guest error: failed"
        );
        assert_eq!(Error::IndexOutOfBounds.to_string(), "index out of bounds");
    }
}
