//! Guest-side helpers for Selium hostcalls.
//!
//! This crate mirrors the current Selium hostcall surface:
//! session, process, singleton, time, and shared-memory operations.

extern crate self as selium_userland;

macro_rules! driver_module {
    ($mod_name:ident, $import:ident, $import_module:literal) => {
        mod $mod_name {
            use selium_abi::{GuestInt, GuestUint};

            use crate::driver::DriverModule;

            #[allow(dead_code)]
            pub struct Module;

            #[cfg(target_arch = "wasm32")]
            #[link(wasm_import_module = $import_module)]
            unsafe extern "C" {
                pub fn create(args_ptr: GuestInt, args_len: GuestUint) -> GuestUint;
                pub fn poll(
                    handle: GuestUint,
                    task_id: GuestUint,
                    result_ptr: GuestInt,
                    result_len: GuestUint,
                ) -> GuestUint;
                pub fn drop(
                    handle: GuestUint,
                    result_ptr: GuestInt,
                    result_len: GuestUint,
                ) -> GuestUint;
            }

            #[allow(dead_code)]
            #[cfg(not(target_arch = "wasm32"))]
            unsafe fn create(_args_ptr: GuestInt, _args_len: GuestUint) -> GuestUint {
                selium_abi::driver_encode_error(2)
            }

            #[allow(dead_code)]
            #[cfg(not(target_arch = "wasm32"))]
            unsafe fn poll(
                _handle: GuestUint,
                _task_id: GuestUint,
                _result_ptr: GuestInt,
                _result_len: GuestUint,
            ) -> GuestUint {
                selium_abi::driver_encode_error(2)
            }

            #[allow(dead_code)]
            #[cfg(not(target_arch = "wasm32"))]
            unsafe fn drop(
                _handle: GuestUint,
                _result_ptr: GuestInt,
                _result_len: GuestUint,
            ) -> GuestUint {
                0
            }

            impl DriverModule for Module {
                unsafe fn create(args_ptr: GuestInt, args_len: GuestUint) -> GuestUint {
                    unsafe { create(args_ptr, args_len) }
                }

                unsafe fn poll(
                    handle: GuestUint,
                    task_id: GuestUint,
                    result_ptr: GuestInt,
                    result_len: GuestUint,
                ) -> GuestUint {
                    unsafe { poll(handle, task_id, result_ptr, result_len) }
                }

                unsafe fn drop(
                    handle: GuestUint,
                    result_ptr: GuestInt,
                    result_len: GuestUint,
                ) -> GuestUint {
                    unsafe { drop(handle, result_ptr, result_len) }
                }
            }
        }
    };
}

pub mod abi;
mod r#async;
pub mod context;
pub mod driver;
pub mod process;
pub mod session;
pub mod shm;
pub mod singleton;
pub mod time;

/// Re-export of the `rkyv` crate used for internal Selium serialisation.
pub use rkyv;

pub use r#async::{block_on, spawn, yield_now};
pub use context::{Context, Dependency, DependencyDescriptor};
/// Re-export of supported Selium macros for guest crates.
pub use selium_userland_macros::{dependency_id, entrypoint};

/// Re-export of singleton dependency identifiers.
pub use selium_abi::DependencyId;

pub trait FromHandle: Sized {
    /// Construct a typed wrapper from raw handle(s).
    ///
    /// # Safety
    /// The provided handles must have been minted by Selium for the current guest and must match
    /// the expected capability type. Supplying forged or stale handles may be rejected by the host
    /// kernel or result in undefined guest behaviour.
    unsafe fn from_handle(handles: Self::Handles) -> Self;
    /// The raw handle type(s) required by this wrapper.
    type Handles;
}
