//! Guest-side APIs for writing Selium modules.
//!
//! `selium-guest` is the main crate used inside guest modules compiled for the Selium runtime.
//! A typical guest crate exposes one or more async entrypoints with [`entrypoint`], coordinates
//! work with helpers like [`spawn`], [`yield_now`], and [`shutdown`], and calls capability-
//! specific modules such as [`process`], [`session`], and [`time`] from those entrypoints.
//!
//! The crate keeps the root surface intentionally small:
//!
//! - the crate root re-exports the entrypoint macro and executor helpers used by most guests,
//! - modules such as [`process`], [`session`], and [`time`] cover lifecycle-oriented hostcalls,
//! - modules such as [`io`], [`network`], [`queue`], and [`shm`] expose data-plane operations.
//!
//! # Example
//! ```no_run
//! use std::time::Duration;
//!
//! #[selium_guest::entrypoint]
//! pub async fn start() -> Result<(), ()> {
//!     let worker = selium_guest::spawn(async {
//!         let _ = selium_guest::time::sleep(Duration::from_millis(10)).await;
//!     });
//!
//!     worker.await;
//!     Ok(())
//! }
//! ```

extern crate self as selium_guest;

macro_rules! driver_module {
    ($mod_name:ident, $import_module:literal) => {
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
pub mod bindings;
pub mod driver;
pub mod durability;
mod guest_log;
pub mod io;
pub mod network;
pub mod process;
pub mod queue;
pub mod session;
pub mod shm;
pub mod storage;
pub mod time;

/// Re-export of the `rkyv` crate for guest code that needs Selium-compatible serialisation.
pub use rkyv;

#[cfg(not(target_arch = "wasm32"))]
#[doc(hidden)]
pub use r#async::{__reset_shutdown_for_tests, __signal_shutdown_for_tests};
pub use r#async::{block_on, shutdown, spawn, yield_now};
#[doc(hidden)]
pub use guest_log::__enter_guest_logging;
/// Attribute macro for declaring async guest entrypoints that the Selium runtime can invoke.
pub use selium_guest_macros::entrypoint;
