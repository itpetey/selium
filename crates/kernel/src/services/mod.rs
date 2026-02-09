//! Kernel service implementations.

pub mod session_service;
#[cfg(feature = "service-singleton")]
pub mod singleton_service;
#[cfg(feature = "service-time")]
pub mod time_service;
