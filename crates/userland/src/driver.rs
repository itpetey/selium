//! Guest-side driver plumbing.
//!
//! Selium guest APIs are implemented in terms of host drivers. This module provides
//! [`DriverFuture`], which wraps each driver's `create/poll/drop` hooks into a typed
//! asynchronous future.
//!
//! # Examples
//! ```
//! use selium_userland::driver::{DriverError, encode_args};
//!
//! fn main() -> Result<(), DriverError> {
//!     let bytes = encode_args(&42u32)?;
//!     assert!(!bytes.is_empty());
//!     Ok(())
//! }
//! ```

use core::{marker::PhantomData, slice};
use std::{
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use selium_abi::{
    DRIVER_ERROR_MESSAGE_CODE, DriverPollResult, GuestInt, GuestUint, RkyvEncode,
    decode_driver_error_message, decode_rkyv, driver_decode_result, encode_rkyv,
};
use thiserror::Error;

use crate::r#async;

/// Estimated overhead of a `Vec<u8>` when rkyv archives it.
pub const RKYV_VEC_OVERHEAD: usize = 16;
/// Minimum buffer capacity reserved for driver replies.
pub const MIN_RESULT_CAPACITY: usize = 256;

/// Guest pointer type used by Selium driver hooks.
pub type DriverInt = GuestInt;
/// Guest integer type used by Selium driver hooks.
pub type DriverUint = GuestUint;

/// Contract implemented by each host driver exposed to guests.
pub trait DriverModule {
    /// Create a new driver handle.
    ///
    /// # Safety
    /// `args_ptr..args_ptr+args_len` must describe a readable byte range in the guest's linear
    /// memory for the duration of this call.
    unsafe fn create(args_ptr: DriverInt, args_len: DriverUint) -> DriverUint;

    /// Poll an existing driver handle.
    ///
    /// # Safety
    /// - `result_ptr..result_ptr+result_len` must describe a writable byte range in the guest's
    ///   linear memory for the duration of this call.
    /// - `task_id` must be a valid identifier obtained from the Selium async runtime.
    unsafe fn poll(
        handle: DriverUint,
        task_id: DriverUint,
        result_ptr: DriverInt,
        result_len: DriverUint,
    ) -> DriverUint;

    /// Drop a driver handle, optionally writing a final result payload.
    ///
    /// # Safety
    /// `result_ptr..result_ptr+result_len` must describe a writable byte range in the guest's
    /// linear memory for the duration of this call.
    unsafe fn drop(handle: DriverUint, result_ptr: DriverInt, result_len: DriverUint)
    -> DriverUint;
}

/// Decodes the bytes returned by a driver into a concrete output type.
pub trait DriverDecoder: Unpin {
    /// Output type produced by this decoder.
    type Output;

    /// Decode a driver payload.
    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, DriverError>;
}

/// Decoder that deserialises an rkyv payload into the requested type.
pub struct RkyvDecoder<T> {
    _marker: PhantomData<T>,
}

/// Generic error returned by host driver invocations.
#[derive(Debug, Error)]
pub enum DriverError {
    /// The driver returned a structured error string.
    #[error("driver error: {0}")]
    Driver(String),
    /// The kernel returned a numeric error code.
    #[error("kernel error: {0}")]
    Kernel(DriverUint),
    /// The caller supplied invalid arguments (for example, a length overflow).
    #[error("invalid argument")]
    InvalidArgument,
}

struct GuestPtr {
    raw: DriverInt,
}

/// Guest-side future that drives a host driver through create/poll/drop FFI hooks.
pub struct DriverFuture<M, D>
where
    M: DriverModule,
    D: DriverDecoder,
{
    handle: Option<DriverUint>,
    result: Vec<u8>,
    decoder: D,
    _marker: PhantomData<M>,
}

impl<T> RkyvDecoder<T> {
    /// Create a new decoder instance.
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<T> Default for RkyvDecoder<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> DriverDecoder for RkyvDecoder<T>
where
    T: rkyv::Archive + Sized + Unpin,
    for<'a> T::Archived: 'a
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    type Output = T;

    fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, DriverError> {
        decode_rkyv_value(bytes)
    }
}

impl From<DriverError> for io::Error {
    fn from(value: DriverError) -> Self {
        match value {
            DriverError::Driver(msg) => io::Error::other(msg),
            DriverError::Kernel(code) => {
                io::Error::from_raw_os_error(i32::try_from(-(code as i64)).unwrap_or(-1))
            }
            DriverError::InvalidArgument => {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid argument")
            }
        }
    }
}

impl GuestPtr {
    fn new(ptr: *const u8) -> Result<Self, DriverError> {
        #[cfg(target_arch = "wasm32")]
        {
            let addr = ptr as usize;
            let raw = DriverInt::try_from(addr).map_err(|_| DriverError::InvalidArgument)?;
            Ok(Self { raw })
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let raw = host_compat::register(ptr)?;
            Ok(Self { raw })
        }
    }

    fn raw(&self) -> DriverInt {
        self.raw
    }
}

#[cfg(not(target_arch = "wasm32"))]
impl Drop for GuestPtr {
    fn drop(&mut self) {
        host_compat::unregister(self.raw);
    }
}

#[cfg(target_arch = "wasm32")]
impl Drop for GuestPtr {
    fn drop(&mut self) {}
}

impl<M, D> DriverFuture<M, D>
where
    M: DriverModule,
    D: DriverDecoder,
{
    /// Create a new future by calling the driver's `create` hook with the supplied arguments.
    pub fn new(args: &[u8], capacity: usize, decoder: D) -> Result<Self, DriverError> {
        let len = guest_len(args.len())?;
        let ptr = GuestPtr::new(args.as_ptr())?;
        let handle = unsafe { M::create(ptr.raw(), len) };

        let cap = capacity.max(MIN_RESULT_CAPACITY);
        Ok(Self {
            handle: Some(handle),
            result: vec![0; cap],
            decoder,
            _marker: core::marker::PhantomData,
        })
    }

    fn poll_inner(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<D::Output, DriverError>> {
        let handle = match self.handle {
            Some(handle) => handle,
            None => return Poll::Ready(Err(DriverError::InvalidArgument)),
        };

        let task_id = r#async::register(cx);
        let capacity = match guest_len(self.result.len()) {
            Ok(len) => len,
            Err(err) => return Poll::Ready(Err(err)),
        };
        let ptr = match GuestPtr::new(self.result.as_mut_ptr()) {
            Ok(ptr) => ptr,
            Err(err) => return Poll::Ready(Err(err)),
        };
        let rc = unsafe { M::poll(handle, task_id, ptr.raw(), capacity) };

        match driver_decode_result(rc) {
            DriverPollResult::Pending => Poll::Pending,
            DriverPollResult::Error(code) => {
                self.handle = None;
                if code == DRIVER_ERROR_MESSAGE_CODE {
                    let msg = decode_driver_error(&self.result);
                    Poll::Ready(Err(DriverError::Driver(msg)))
                } else {
                    Poll::Ready(Err(DriverError::Kernel(code)))
                }
            }
            DriverPollResult::Ready(value) => {
                if value > capacity {
                    self.handle = None;
                    return Poll::Ready(Err(DriverError::Kernel(value)));
                }

                let used = match host_len(value) {
                    Ok(len) => len,
                    Err(err) => {
                        self.handle = None;
                        return Poll::Ready(Err(err));
                    }
                };
                if used > self.result.len() {
                    self.handle = None;
                    return Poll::Ready(Err(DriverError::InvalidArgument));
                }

                self.handle = None;
                let ptr = self.result.as_ptr();
                let output = {
                    let bytes = unsafe { slice::from_raw_parts(ptr, used) };
                    let decoded = self.decoder.decode(bytes);
                    if let Err(DriverError::Driver(ref msg)) = decoded {
                        tracing::warn!(
                            "driver decode failed (module={}, used={}): {msg}",
                            std::any::type_name::<M>(),
                            used
                        );
                    }
                    decoded
                };
                Poll::Ready(output)
            }
        }
    }
}

impl<M, D> Future for DriverFuture<M, D>
where
    M: DriverModule,
    D: DriverDecoder,
{
    type Output = Result<D::Output, DriverError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_inner(cx)
    }
}

impl<M, D> Drop for DriverFuture<M, D>
where
    M: DriverModule,
    D: DriverDecoder,
{
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take()
            && let (Ok(len), Ok(ptr)) = (
                guest_len(self.result.len()),
                GuestPtr::new(self.result.as_mut_ptr()),
            )
        {
            let _ = unsafe { M::drop(handle, ptr.raw(), len) };
        }
    }
}

impl<M, D> Unpin for DriverFuture<M, D>
where
    M: DriverModule,
    D: DriverDecoder,
{
}

/// Encode a driver argument value using Selium's rkyv configuration.
pub fn encode_args<T: RkyvEncode>(value: &T) -> Result<Vec<u8>, DriverError> {
    encode_rkyv(value).map_err(|err| DriverError::Driver(err.to_string()))
}

fn decode_driver_error(buf: &[u8]) -> String {
    decode_driver_error_message(buf).unwrap_or_else(|_| "driver error".to_string())
}

fn decode_rkyv_value<T>(bytes: &[u8]) -> Result<T, DriverError>
where
    T: rkyv::Archive + Sized,
    for<'a> T::Archived: 'a
        + rkyv::Deserialize<T, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
        + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
{
    decode_rkyv(bytes).map_err(|err| DriverError::Driver(err.to_string()))
}

fn guest_len(len: usize) -> Result<DriverUint, DriverError> {
    DriverUint::try_from(len).map_err(|_| DriverError::InvalidArgument)
}

fn host_len(value: DriverUint) -> Result<usize, DriverError> {
    usize::try_from(value).map_err(|_| DriverError::InvalidArgument)
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) mod host_compat {
    use super::{DriverError, DriverInt};
    use std::{
        collections::HashMap,
        sync::{Mutex, OnceLock},
    };

    struct PtrRegistry {
        next: DriverInt,
        entries: HashMap<DriverInt, usize>,
    }

    impl PtrRegistry {
        fn new() -> Self {
            Self {
                next: 1,
                entries: HashMap::new(),
            }
        }
    }

    impl Default for PtrRegistry {
        fn default() -> Self {
            Self::new()
        }
    }

    static REGISTRY: OnceLock<Mutex<PtrRegistry>> = OnceLock::new();

    fn registry() -> &'static Mutex<PtrRegistry> {
        REGISTRY.get_or_init(|| Mutex::new(PtrRegistry::new()))
    }

    pub fn register(ptr: *const u8) -> Result<DriverInt, DriverError> {
        let mut guard = registry().lock().expect("pointer registry poisoned");
        let id = guard.next;
        guard.next = guard
            .next
            .checked_add(1)
            .ok_or(DriverError::InvalidArgument)?;
        guard.entries.insert(id, ptr as usize);
        Ok(id)
    }

    pub fn unregister(id: DriverInt) {
        if let Some(registry) = REGISTRY.get()
            && let Ok(mut guard) = registry.lock()
        {
            guard.entries.remove(&id);
        }
    }

    #[cfg(all(not(target_arch = "wasm32"), test))]
    pub unsafe fn ptr_from_guest(id: DriverInt) -> *const u8 {
        registry()
            .lock()
            .expect("pointer registry poisoned")
            .entries
            .get(&id)
            .copied()
            .map(|addr| addr as *const u8)
            .unwrap_or(core::ptr::null())
    }

    #[cfg(all(not(target_arch = "wasm32"), test))]
    pub unsafe fn ptr_from_guest_mut(id: DriverInt) -> *mut u8 {
        (unsafe { ptr_from_guest(id) }) as *mut u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::task::noop_waker;
    use selium_abi::{DRIVER_RESULT_PENDING, driver_encode_error, driver_encode_ready};
    use std::{
        pin::Pin,
        sync::atomic::{AtomicU32, Ordering},
    };

    #[cfg(not(target_arch = "wasm32"))]
    use super::host_compat;

    #[cfg(target_arch = "wasm32")]
    unsafe fn test_ptr_mut(ptr: DriverInt) -> *mut u8 {
        ptr as *mut u8
    }

    #[cfg(not(target_arch = "wasm32"))]
    unsafe fn test_ptr_mut(ptr: DriverInt) -> *mut u8 {
        unsafe { host_compat::ptr_from_guest_mut(ptr) }
    }

    fn run_ready<F>(fut: F) -> F::Output
    where
        F: Future,
    {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = Box::pin(fut);
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(val) => break val,
                Poll::Pending => continue,
            }
        }
    }

    struct ReadyModule;

    impl DriverModule for ReadyModule {
        unsafe fn create(_args_ptr: DriverInt, _args_len: DriverUint) -> DriverUint {
            1
        }

        unsafe fn poll(
            _handle: DriverUint,
            _task_id: DriverUint,
            result_ptr: DriverInt,
            _result_len: DriverUint,
        ) -> DriverUint {
            let payload = b"ok";
            unsafe {
                core::ptr::copy_nonoverlapping(
                    payload.as_ptr(),
                    test_ptr_mut(result_ptr),
                    payload.len(),
                );
            }
            let len = DriverUint::try_from(payload.len()).expect("payload length fits");
            driver_encode_ready(len).expect("payload length fits")
        }

        unsafe fn drop(
            _handle: DriverUint,
            _result_ptr: DriverInt,
            _result_len: DriverUint,
        ) -> DriverUint {
            0
        }
    }

    struct StrDecoder;

    impl DriverDecoder for StrDecoder {
        type Output = String;

        fn decode(&mut self, bytes: &[u8]) -> Result<Self::Output, DriverError> {
            Ok(std::str::from_utf8(bytes)
                .expect("UTF-8 payload")
                .to_string())
        }
    }

    #[test]
    fn driver_future_yields_ready_bytes() {
        let fut = DriverFuture::<ReadyModule, StrDecoder>::new(&[], 4, StrDecoder)
            .expect("create future");
        let out = run_ready(fut).expect("future output");
        assert_eq!(out, "ok");
    }

    struct DriverErrorModule;

    impl DriverModule for DriverErrorModule {
        unsafe fn create(_args_ptr: DriverInt, _args_len: DriverUint) -> DriverUint {
            2
        }

        unsafe fn poll(
            _handle: DriverUint,
            _task_id: DriverUint,
            result_ptr: DriverInt,
            _result_len: DriverUint,
        ) -> DriverUint {
            let encoded =
                selium_abi::encode_driver_error_message("boom").expect("encode error payload");
            unsafe {
                core::ptr::copy_nonoverlapping(
                    encoded.as_ptr(),
                    test_ptr_mut(result_ptr),
                    encoded.len(),
                )
            };
            driver_encode_error(DRIVER_ERROR_MESSAGE_CODE)
        }

        unsafe fn drop(
            _handle: DriverUint,
            _result_ptr: DriverInt,
            _result_len: DriverUint,
        ) -> DriverUint {
            0
        }
    }

    struct UnitDecoder;

    impl DriverDecoder for UnitDecoder {
        type Output = ();

        fn decode(&mut self, _bytes: &[u8]) -> Result<Self::Output, DriverError> {
            Ok(())
        }
    }

    #[test]
    fn driver_future_surfaces_driver_message_error() {
        let fut = DriverFuture::<DriverErrorModule, UnitDecoder>::new(&[], 64, UnitDecoder)
            .expect("create future");
        let err = run_ready(fut).expect_err("expected driver error");
        match err {
            DriverError::Driver(msg) => assert_eq!(msg, "boom"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    struct PendingThenReadyModule;

    static POLL_COUNT: AtomicU32 = AtomicU32::new(0);

    impl DriverModule for PendingThenReadyModule {
        unsafe fn create(_args_ptr: DriverInt, _args_len: DriverUint) -> DriverUint {
            POLL_COUNT.store(0, Ordering::Relaxed);
            3
        }

        unsafe fn poll(
            _handle: DriverUint,
            _task_id: DriverUint,
            result_ptr: DriverInt,
            _result_len: DriverUint,
        ) -> DriverUint {
            let prev = POLL_COUNT.fetch_add(1, Ordering::Relaxed);
            if prev == 0 {
                DRIVER_RESULT_PENDING
            } else {
                let payload = b"done";
                unsafe {
                    core::ptr::copy_nonoverlapping(
                        payload.as_ptr(),
                        test_ptr_mut(result_ptr),
                        payload.len(),
                    )
                };
                driver_encode_ready(
                    DriverUint::try_from(payload.len()).expect("payload length fits"),
                )
                .expect("payload length fits")
            }
        }

        unsafe fn drop(
            _handle: DriverUint,
            _result_ptr: DriverInt,
            _result_len: DriverUint,
        ) -> DriverUint {
            0
        }
    }

    #[test]
    fn driver_future_handles_pending_then_ready() {
        let fut = DriverFuture::<PendingThenReadyModule, StrDecoder>::new(&[], 8, StrDecoder)
            .expect("create future");
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut fut = Box::pin(fut);

        assert!(matches!(Pin::new(&mut fut).poll(&mut cx), Poll::Pending));
        let out = run_ready(fut).expect("future output");
        assert_eq!(out, "done");
    }
}
