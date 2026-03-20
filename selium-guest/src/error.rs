//! Guest error types.

/// Result type for guest functions.
pub type GuestResult<T = ()> = std::result::Result<T, GuestError>;

/// Guest error types that inform supervisor decisions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuestError {
    /// Generic error with message
    Error(String),
    /// Guest supports hot-swap (drain → replace → resume)
    HotSwap,
    /// Guest requests restart
    Restart,
}

impl GuestError {
    /// Create an error with a message.
    pub fn error<S: Into<String>>(msg: S) -> Self {
        Self::Error(msg.into())
    }
}

impl std::fmt::Display for GuestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GuestError::Error(msg) => write!(f, "Error: {}", msg),
            GuestError::HotSwap => write!(f, "HotSwap"),
            GuestError::Restart => write!(f, "Restart"),
        }
    }
}

impl std::error::Error for GuestError {}

impl From<String> for GuestError {
    fn from(s: String) -> Self {
        Self::Error(s)
    }
}

impl From<&str> for GuestError {
    fn from(s: &str) -> Self {
        Self::Error(s.to_string())
    }
}
