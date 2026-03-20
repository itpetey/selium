//! Error types for Selium Host.

use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Guest error: {0}")]
    Guest(String),

    #[error("Capability not found: {0}")]
    CapabilityNotFound(String),

    #[error("Invalid handle: {0}")]
    InvalidHandle(String),

    #[error("Guest exited with: {0:?}")]
    GuestExited(GuestExitStatus),

    #[error("Hostcall error: {0}")]
    Hostcall(String),

    #[error("Deprecated hostcall: {name} (removed in v{removed_version})")]
    DeprecatedHostcall { name: String, removed_version: u32 },

    #[error("WASM error: {0}")]
    Wasm(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Capability revoked")]
    CapabilityRevoked,
}

pub type Result<T> = std::result::Result<T, Error>;

/// Guest exit status types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GuestExitStatus {
    /// Guest completed successfully
    Ok,
    /// Guest returned an error
    Error(String),
    /// Guest requested hot-swap (drain → replace → resume)
    HotSwap,
    /// Guest requested restart
    Restart,
}

impl From<GuestExitStatus> for i32 {
    fn from(status: GuestExitStatus) -> Self {
        match status {
            GuestExitStatus::Ok => 0,
            GuestExitStatus::Error(_) => 1,
            GuestExitStatus::HotSwap => 42,
            GuestExitStatus::Restart => 43,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exit_status_ok_to_i32() {
        let status = GuestExitStatus::Ok;
        assert_eq!(i32::from(status), 0);
    }

    #[test]
    fn test_exit_status_error_to_i32() {
        let status = GuestExitStatus::Error("test error".to_string());
        assert_eq!(i32::from(status), 1);
    }

    #[test]
    fn test_exit_status_hotswap_to_i32() {
        let status = GuestExitStatus::HotSwap;
        assert_eq!(i32::from(status), 42);
    }

    #[test]
    fn test_exit_status_restart_to_i32() {
        let status = GuestExitStatus::Restart;
        assert_eq!(i32::from(status), 43);
    }

    #[test]
    fn test_exit_status_debug() {
        let status = GuestExitStatus::Ok;
        assert_eq!(format!("{:?}", status), "Ok");
    }

    #[test]
    fn test_exit_status_error_debug() {
        let status = GuestExitStatus::Error("something went wrong".to_string());
        assert_eq!(format!("{:?}", status), r#"Error("something went wrong")"#);
    }

    #[test]
    fn test_exit_status_eq() {
        assert_eq!(GuestExitStatus::Ok, GuestExitStatus::Ok);
        assert_eq!(
            GuestExitStatus::Error("msg".to_string()),
            GuestExitStatus::Error("msg".to_string())
        );
        assert_ne!(GuestExitStatus::Ok, GuestExitStatus::Error("".to_string()));
    }

    #[test]
    fn test_exit_status_clone() {
        let status = GuestExitStatus::Error("clone me".to_string());
        let cloned = status.clone();
        assert_eq!(status, cloned);
    }

    #[test]
    fn test_error_display() {
        let err = Error::Guest("test".to_string());
        assert_eq!(format!("{}", err), "Guest error: test");

        let err = Error::CapabilityNotFound("Storage".to_string());
        assert_eq!(format!("{}", err), "Capability not found: Storage");

        let err = Error::InvalidHandle("dead".to_string());
        assert_eq!(format!("{}", err), "Invalid handle: dead");

        let err = Error::GuestExited(GuestExitStatus::Ok);
        assert_eq!(format!("{}", err), "Guest exited with: Ok");

        let err = Error::Hostcall("timeout".to_string());
        assert_eq!(format!("{}", err), "Hostcall error: timeout");

        let err = Error::DeprecatedHostcall {
            name: "old_api".to_string(),
            removed_version: 2,
        };
        assert_eq!(
            format!("{}", err),
            "Deprecated hostcall: old_api (removed in v2)"
        );

        let err = Error::Wasm("link error".to_string());
        assert_eq!(format!("{}", err), "WASM error: link error");
    }
}
