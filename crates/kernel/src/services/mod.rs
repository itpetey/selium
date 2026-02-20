//! Kernel service implementations.

#[cfg(feature = "service-session")]
pub mod session_service;
#[cfg(feature = "service-shared-memory")]
pub mod shared_memory_service;
#[cfg(feature = "service-singleton")]
pub mod singleton_service;
#[cfg(feature = "service-time")]
pub mod time_service;
