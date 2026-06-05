use thiserror::Error;

/// Result type for selium-io operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Single flat error type covering all messaging failure modes.
///
/// Replaces the previous three-layer hierarchy (`io::Error` → `channels::Error`
/// → `RpcError`) with one enum. Each variant maps directly to a distinct failure
/// mode — no `From` chains or nested wrapping.
#[derive(Debug, Error, PartialEq)]
pub enum Error {
    #[error("invalid ring buffer layout")]
    InvalidLayout,
    #[error("buffer full")]
    BufferFull,
    #[error("buffer empty")]
    BufferEmpty,
    #[error("reader was overtaken by writers")]
    ReaderBehind,
    #[error("invalid frame header")]
    InvalidFrame,
    #[error("capacity exceeded")]
    CapacityExceeded,
    #[error("compare-and-set failed: expected {expected}, got {actual:?}")]
    CasConflict {
        /// Expected value.
        expected: u64,
        /// Actual value found.
        actual: Option<u64>,
    },
    #[error("channel closed")]
    ChannelClosed,
    #[error("connection lost")]
    ConnectionLost,
    #[error("channel terminated")]
    Terminated,
    #[error("serialization error: {0}")]
    SerializationFailed(String),
    #[error("invalid shared region")]
    InvalidRegion,
    #[error("region layout mismatch")]
    LayoutMismatch,
    #[error("ABI error: {0:?}")]
    Abi(selium_abi::AbiErrorCode),
    #[error("guest error: {0}")]
    Guest(String),
    #[error("index out of bounds")]
    IndexOutOfBounds,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_covers_core_error_variants() {
        assert_eq!(Error::InvalidLayout.to_string(), "invalid ring buffer layout");
        assert_eq!(Error::BufferFull.to_string(), "buffer full");
        assert_eq!(Error::BufferEmpty.to_string(), "buffer empty");
        assert_eq!(
            Error::ReaderBehind.to_string(),
            "reader was overtaken by writers"
        );
        assert_eq!(Error::InvalidFrame.to_string(), "invalid frame header");
        assert_eq!(Error::CapacityExceeded.to_string(), "capacity exceeded");
        assert_eq!(Error::ChannelClosed.to_string(), "channel closed");
        assert_eq!(Error::ConnectionLost.to_string(), "connection lost");
        assert_eq!(Error::Terminated.to_string(), "channel terminated");
        assert_eq!(
            Error::SerializationFailed("bad".to_string()).to_string(),
            "serialization error: bad"
        );
        assert_eq!(Error::InvalidRegion.to_string(), "invalid shared region");
        assert_eq!(Error::LayoutMismatch.to_string(), "region layout mismatch");
        assert_eq!(
            Error::Guest("failed".to_string()).to_string(),
            "guest error: failed"
        );
        assert_eq!(Error::IndexOutOfBounds.to_string(), "index out of bounds");
    }

    #[test]
    fn flat_error_matching() {
        let err = Error::BufferFull;
        match err {
            Error::BufferFull => {} // direct match, no unwrapping
            _ => panic!("expected BufferFull"),
        }
    }
}
