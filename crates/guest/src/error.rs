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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guest_error_error() {
        let err = GuestError::Error("test".to_string());
        assert_eq!(format!("{}", err), "Error: test");
    }

    #[test]
    fn test_guest_error_hotswap() {
        let err = GuestError::HotSwap;
        assert_eq!(format!("{}", err), "HotSwap");
    }

    #[test]
    fn test_guest_error_restart() {
        let err = GuestError::Restart;
        assert_eq!(format!("{}", err), "Restart");
    }

    #[test]
    fn test_guest_error_eq() {
        assert_eq!(
            GuestError::Error("msg".to_string()),
            GuestError::Error("msg".to_string())
        );
        assert_eq!(GuestError::HotSwap, GuestError::HotSwap);
        assert_eq!(GuestError::Restart, GuestError::Restart);
        assert_ne!(
            GuestError::Error("a".to_string()),
            GuestError::Error("b".to_string())
        );
    }

    #[test]
    fn test_guest_error_clone() {
        let err = GuestError::Error("clone".to_string());
        let cloned = err.clone();
        assert_eq!(err, cloned);
    }

    #[test]
    fn test_guest_error_debug() {
        assert_eq!(format!("{:?}", GuestError::HotSwap), "HotSwap");
    }

    #[test]
    fn test_guest_error_error_helper() {
        let err = GuestError::error("helper");
        assert_eq!(format!("{}", err), "Error: helper");
    }

    #[test]
    fn test_guest_result_ok() {
        let result: GuestResult<i32> = Ok(42);
        assert!(result.is_ok());
        let Ok(value) = result else {
            panic!("expected Ok")
        };
        assert_eq!(value, 42);
    }

    #[test]
    fn test_guest_result_err() {
        let result: GuestResult<i32> = Err(GuestError::Error("failed".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_from_string() {
        let err: GuestError = String::from("from string").into();
        assert_eq!(format!("{}", err), "Error: from string");
    }

    #[test]
    fn test_from_str() {
        let err: GuestError = "from str".into();
        assert_eq!(format!("{}", err), "Error: from str");
    }

    #[test]
    fn test_error_implements_std_error() {
        fn assert_error<T: std::error::Error>() {}
        assert_error::<GuestError>();
    }
}
