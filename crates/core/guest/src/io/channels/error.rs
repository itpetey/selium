use std::fmt;

use crate::io;

/// Result type for channel operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for channel operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// Channel buffer is full.
    ChannelFull,
    /// Channel buffer is empty.
    ChannelEmpty,
    /// Reader fell behind and lost data.
    ReaderBehind,
    /// Writers contended for the reservation cursor.
    ReservationContended,
    /// Invalid frame header encountered.
    InvalidFrame,
    /// Channel has been terminated.
    Terminated,
    /// Channel has been closed.
    Closed,
    /// Error from the core I/O layer.
    Core(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChannelFull => write!(f, "channel buffer full"),
            Self::ChannelEmpty => write!(f, "channel buffer empty"),
            Self::ReaderBehind => write!(f, "reader fell behind writer"),
            Self::ReservationContended => write!(f, "channel reservation contended"),
            Self::InvalidFrame => write!(f, "invalid frame"),
            Self::Terminated => write!(f, "channel terminated"),
            Self::Closed => write!(f, "channel closed"),
            Self::Core(e) => write!(f, "core error: {e}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Self {
        Self::Core(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_implements_display() {
        assert_eq!(Error::ChannelFull.to_string(), "channel buffer full");
        assert_eq!(Error::ChannelEmpty.to_string(), "channel buffer empty");
        assert_eq!(
            Error::ReservationContended.to_string(),
            "channel reservation contended"
        );
    }

    #[test]
    fn core_error_conversion() {
        let core_err = io::Error::BufferFull;
        let err: Error = core_err.into();
        assert_eq!(err, Error::Core(io::Error::BufferFull));
    }
}
