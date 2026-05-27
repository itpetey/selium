use thiserror::Error;

use crate::io;

/// Result type for channel operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for channel operations.
#[derive(Debug, Error, PartialEq)]
pub enum Error {
    /// Channel buffer is full.
    #[error("channel buffer full")]
    ChannelFull,
    /// Channel buffer is empty.
    #[error("channel buffer empty")]
    ChannelEmpty,
    /// Reader fell behind and lost data.
    #[error("reader fell behind writer")]
    ReaderBehind,
    /// Writers contended for the reservation cursor.
    #[error("channel reservation contended")]
    ReservationContended,
    /// Invalid frame header encountered.
    #[error("invalid frame")]
    InvalidFrame,
    /// Channel has been terminated.
    #[error("channel terminated")]
    Terminated,
    /// Channel has been closed.
    #[error("channel closed")]
    Closed,
    /// Error from the core I/O layer.
    #[error("core error: {0}")]
    Core(#[from] io::Error),
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
