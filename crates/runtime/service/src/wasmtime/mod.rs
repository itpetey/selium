//! Wasmtime runtime integration and link-time glue.

pub mod guest_async;
pub mod guest_data;
pub mod guest_logs;
pub mod hostcall_linker;
pub mod mailbox;
pub mod runtime;
