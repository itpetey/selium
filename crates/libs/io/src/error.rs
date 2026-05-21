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
    InvalidFrame,
    CapacityExceeded,
    CasConflict { expected: u64, actual: Option<u64> },
    InvalidSignal,
    Abi(selium_abi::AbiErrorCode),
    Guest(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLayout => write!(f, "invalid ring buffer layout"),
            Self::BufferFull => write!(f, "ring buffer full"),
            Self::BufferEmpty => write!(f, "ring buffer empty"),
            Self::ReaderBehind => write!(f, "reader was overtaken by writers"),
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
        }
    }
}

impl std::error::Error for Error {}
