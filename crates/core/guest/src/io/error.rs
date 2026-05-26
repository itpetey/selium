use core::fmt;

/// Result type for selium-io core operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for selium-io core operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    InvalidLayout,
    BufferFull,
    BufferEmpty,
    ReaderBehind,
    ReservationContended,
    InvalidFrame,
    CapacityExceeded,
    CasConflict { expected: u64, actual: Option<u64> },
    InvalidSignal,
    Abi(selium_abi::AbiErrorCode),
    Guest(String),
    IndexOutOfBounds,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLayout => write!(f, "invalid ring buffer layout"),
            Self::BufferFull => write!(f, "ring buffer full"),
            Self::BufferEmpty => write!(f, "ring buffer empty"),
            Self::ReaderBehind => write!(f, "reader was overtaken by writers"),
            Self::ReservationContended => write!(f, "ring buffer reservation contended"),
            Self::InvalidFrame => write!(f, "invalid frame header"),
            Self::CapacityExceeded => write!(f, "capacity exceeded"),
            Self::CasConflict { expected, actual } => {
                write!(
                    f,
                    "compare-and-set failed expected version {expected} got {actual:?}"
                )
            }
            Self::InvalidSignal => write!(f, "invalid signal"),
            Self::Abi(code) => write!(f, "ABI error: {code:?}"),
            Self::Guest(msg) => write!(f, "guest error: {msg}"),
            Self::IndexOutOfBounds => write!(f, "index out of bounds"),
        }
    }
}

impl std::error::Error for Error {}

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
