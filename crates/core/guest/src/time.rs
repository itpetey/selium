//! Time primitives for Selium guests.
//!
//! Provides an ABI surface ([`time_now`], [`time_monotonic`]) that calls into
//! the host via the hostcall system on wasm32 and fails gracefully on native
//! targets.
//!
//! Also provides a [`Instant`] type backed by the hostcall monotonic clock,
//! a [`TimeSource`] convenience wrapper, and a Quinn-compatible [`Timer`] for
//! the guest's cooperative single-threaded scheduler.

use std::ops::{Add, AddAssign, Sub};
use std::time::{Duration, SystemTime};

use selium_abi::{HostcallOutput, HostcallRequest};

use crate::{GuestError, Result, hostcall::hostcall_ready};
use crate::{
    async_runtime::current_task_id, hostcall::poll_operation, platform::selium_hostcall_create,
};
use selium_abi::{HOSTCALL_STATUS_FAILED, HostcallEnvelope, encode_rkyv, unpack_hostcall_status};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use selium_abi::OperationId;

use crate::platform::selium_hostcall_drop;

/// A measurement of the monotonic clock, backed by the Selium host.
///
/// `Instant` mirrors the [`std::time::Instant`] API but uses the hostcall-based
/// [`time_monotonic`] clock as its source, making it reliable on `wasm32`
/// targets where [`std::time::Instant::now`] is unavailable or unreliable
/// depending on the WASM runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Instant {
    nanos: u64,
}

impl Instant {
    /// The smallest possible `Instant` value (epoch start of the hostcall clock).
    pub const MIN: Instant = Instant { nanos: u64::MIN };

    /// The largest possible `Instant` value.
    pub const MAX: Instant = Instant { nanos: u64::MAX };

    /// Creates an `Instant` from a raw count of nanoseconds since the
    /// hostcall monotonic epoch.
    #[must_use]
    pub const fn from_nanos(nanos: u64) -> Self {
        Self { nanos }
    }

    /// Returns the raw count of nanoseconds since the hostcall monotonic epoch.
    #[must_use]
    pub const fn as_nanos(self) -> u64 {
        self.nanos
    }

    /// Returns an `Instant` corresponding to "now" according to the
    /// hostcall monotonic clock.
    ///
    /// # Panics
    ///
    /// Panics on native (non-WASM) targets where the hostcall is not
    /// available. On WASM this always succeeds.
    #[must_use]
    pub fn now() -> Self {
        let nanos = time_monotonic().expect("hostcall monotonic clock is available on WASM");
        Self { nanos }
    }

    /// Returns the amount of time elapsed from `earlier` to `self`.
    ///
    /// # Panics
    ///
    /// Panics if `earlier` is later than `self`.
    #[must_use]
    pub fn duration_since(&self, earlier: Self) -> Duration {
        self.checked_duration_since(earlier)
            .expect("supplied instant is later than self")
    }

    /// Returns the amount of time elapsed from `earlier` to `self`, or
    /// `None` if `earlier` is later than `self`.
    #[must_use]
    pub fn checked_duration_since(&self, earlier: Self) -> Option<Duration> {
        self.nanos
            .checked_sub(earlier.nanos)
            .map(Duration::from_nanos)
    }

    /// Returns the amount of time elapsed from `earlier` to `self`, or
    /// zero if `earlier` is later than `self`.
    #[must_use]
    pub fn saturating_duration_since(&self, earlier: Self) -> Duration {
        Duration::from_nanos(self.nanos.saturating_sub(earlier.nanos))
    }

    /// Returns the amount of time elapsed since this `Instant` was created.
    #[must_use]
    pub fn elapsed(&self) -> Duration {
        Self::now().saturating_duration_since(*self)
    }

    /// Returns `Some(t)` where `t` is the time `self + duration`, or `None`
    /// if overflow occurred.
    #[must_use]
    pub fn checked_add(&self, duration: Duration) -> Option<Self> {
        self.nanos
            .checked_add(duration.as_nanos() as u64)
            .map(|nanos| Self { nanos })
    }

    /// Returns `Some(t)` where `t` is the time `self - duration`, or `None`
    /// if underflow occurred.
    #[must_use]
    pub fn checked_sub(&self, duration: Duration) -> Option<Self> {
        self.nanos
            .checked_sub(duration.as_nanos() as u64)
            .map(|nanos| Self { nanos })
    }
}

impl Add<Duration> for Instant {
    type Output = Instant;

    /// # Panics
    ///
    /// Panics if overflow occurs.
    fn add(self, rhs: Duration) -> Self {
        self.checked_add(rhs)
            .expect("overflow when adding duration to instant")
    }
}

impl AddAssign<Duration> for Instant {
    fn add_assign(&mut self, rhs: Duration) {
        self.nanos = self
            .nanos
            .checked_add(rhs.as_nanos() as u64)
            .expect("overflow when adding duration to instant");
    }
}

impl Sub<Duration> for Instant {
    type Output = Instant;

    /// # Panics
    ///
    /// Panics if underflow occurs.
    fn sub(self, rhs: Duration) -> Self {
        self.checked_sub(rhs)
            .expect("underflow when subtracting duration from instant")
    }
}

impl Sub<Instant> for Instant {
    type Output = Duration;

    /// # Panics
    ///
    /// Panics if `other` is later than `self`.
    fn sub(self, other: Instant) -> Duration {
        self.duration_since(other)
    }
}

/// Returns the current wall-clock time as nanoseconds since the UNIX epoch.
///
/// This function issues a [`HostcallRequest::TimeNow`] hostcall.
pub fn time_now() -> Result<u64> {
    match hostcall_ready(HostcallRequest::TimeNow)? {
        HostcallOutput::U64(nanos) => Ok(nanos),
        _ => Err(GuestError::UnexpectedHostcallOutput),
    }
}

/// Returns the current monotonic time as nanoseconds since an arbitrary epoch.
///
/// This function issues a [`HostcallRequest::TimeMonotonic`] hostcall.
pub fn time_monotonic() -> Result<u64> {
    match hostcall_ready(HostcallRequest::TimeMonotonic)? {
        HostcallOutput::U64(nanos) => Ok(nanos),
        _ => Err(GuestError::UnexpectedHostcallOutput),
    }
}

/// Time source backed by the Selium host clock (or the local clock on native).
///
/// Uses [`time_monotonic`] as its epoch baseline so that it works correctly on
/// both `wasm32` (where [`Instant::now`] is unreliable) and native targets.
pub struct TimeSource {
    start_nanos: u64,
}

impl TimeSource {
    /// Create a new time source.
    pub fn new() -> Result<Self> {
        Ok(Self {
            start_nanos: time_monotonic()?,
        })
    }

    /// Returns the number of nanoseconds since the time source was created.
    ///
    /// This is a monotonic clock suitable for measuring durations.
    pub fn now_nanos(&self) -> Result<u64> {
        Ok(time_monotonic()?.saturating_sub(self.start_nanos))
    }

    /// Returns the number of milliseconds since the time source was created.
    pub fn now_millis(&self) -> Result<u64> {
        Ok(self.now_nanos()? / 1_000_000)
    }

    /// Returns the current wall-clock time as nanoseconds since UNIX epoch.
    pub fn wall_nanos(&self) -> Result<u64> {
        time_now()
    }

    /// Returns the current wall-clock time.
    pub fn wall(&self) -> SystemTime {
        SystemTime::now()
    }

    /// Returns the elapsed time since this time source was created.
    pub fn elapsed(&self) -> Result<Duration> {
        Ok(Duration::from_nanos(self.now_nanos()?))
    }
}

/// A Quinn-compatible timer that uses a private [`SignalWait`] hostcall to
/// sleep until the deadline.
///
/// On the first `poll()` where the deadline has not yet passed, this timer
/// lazily creates a private signal and then issues a `SignalWait` with the
/// remaining time as the timeout.  When the host completes the wait (timeout
/// expires), it writes to the guest's mailbox, which wakes the guest reactor
/// and causes the timer to be re-polled.
///
/// This avoids spawning OS threads, which are unavailable in a WASM guest,
/// and integrates with the guest's cooperative single-threaded scheduler via
/// the existing signal hostcall machinery.
///
/// This type is constructed indirectly via [`SeliumQuinnRuntime::new_timer`],
/// so the `dead_code` lint does not see usage through the trait object
/// dispatch.
///
/// [`SignalWait`]: selium_abi::HostcallRequest::SignalWait
#[derive(Debug)]
pub struct Timer {
    deadline: Instant,
    /// Local id of a private signal used for timeout-based wakeup.
    /// Created lazily on the first poll where the deadline hasn't passed.
    signal_id: Option<u64>,
    /// In-flight SignalWait hostcall operation, if any.
    operation_id: Option<OperationId>,
}

impl Timer {
    /// Create a new timer with the given deadline (expressed in our
    /// hostcall-backed [`Instant`]).
    pub fn new(deadline: Instant) -> Self {
        Self {
            deadline,
            signal_id: None,
            operation_id: None,
        }
    }

    /// Drops any in-flight hostcall operation without closing the signal.
    fn cancel_wait(&mut self) {
        if let Some(op_id) = self.operation_id.take() {
            // SAFETY: `op_id` was returned by `selium_hostcall_create`.
            unsafe { selium_hostcall_drop(op_id) };
        }
    }

    /// Creates a private signal via a synchronous hostcall.
    fn create_signal() -> Option<u64> {
        match hostcall_ready(HostcallRequest::SignalCreate) {
            Ok(HostcallOutput::Signal(desc)) => Some(desc.local_id),
            _ => None,
        }
    }

    /// Closes a signal handle via a synchronous hostcall (best-effort).
    fn close_signal(local_id: u64) {
        let _ = hostcall_ready(HostcallRequest::SignalClose { local_id });
    }
}

impl Future for Timer {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();

        // Fast path: deadline already passed.
        // Uses our hostcall-backed Instant::now() — NOT std::time::Instant.
        if Instant::now() >= this.deadline {
            this.cancel_wait();
            return Poll::Ready(());
        }

        // Lazily create a private signal for timeout-based wakeup.
        if this.signal_id.is_none() {
            this.signal_id = Self::create_signal();
        }

        // If we have an in-flight wait, poll it.
        if let Some(op_id) = this.operation_id {
            return match poll_operation(op_id) {
                Ok(Some(_)) => {
                    // Wait completed (signalled — shouldn't happen for a private
                    // signal, but handle gracefully as "timer expired").
                    unsafe { selium_hostcall_drop(op_id) };
                    this.operation_id = None;
                    Poll::Ready(())
                }
                Ok(None) => Poll::Pending,
                Err(_) => {
                    // Wait completed with timeout (or other error).
                    // Either way, the timer has expired.
                    unsafe { selium_hostcall_drop(op_id) };
                    this.operation_id = None;
                    Poll::Ready(())
                }
            };
        }

        // No wait in-flight — start one, but only if we have a signal.
        let Some(signal_id) = this.signal_id else {
            // Could not create a signal (e.g. native test stubs).
            // Return Pending and rely on eventual re-polling.
            return Poll::Pending;
        };

        let now = Instant::now();
        let timeout_ms = this.deadline.saturating_duration_since(now).as_millis() as u64;

        let request = HostcallRequest::SignalWait {
            local_id: signal_id,
            observed_generation: 0,
            timeout_ms,
        };

        let envelope = HostcallEnvelope {
            request,
            task_id: current_task_id(),
        };

        let encoded = match encode_rkyv(&envelope) {
            Ok(e) => e,
            Err(_) => return Poll::Ready(()),
        };

        // SAFETY: `encoded` is a valid byte buffer; the host validates the request.
        let create_status = unsafe { selium_hostcall_create(encoded.as_ptr(), encoded.len()) };
        let (status, operation_id) = unpack_hostcall_status(create_status);
        if status == HOSTCALL_STATUS_FAILED {
            return Poll::Ready(()); // Can't wait; best-effort expiry.
        }

        this.operation_id = Some(operation_id as OperationId);
        Poll::Pending
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        self.cancel_wait();
        if let Some(signal_id) = self.signal_id {
            Self::close_signal(signal_id);
        }
    }
}

#[cfg(feature = "quinn")]
impl quinn::AsyncTimer for Timer {
    fn reset(self: Pin<&mut Self>, deadline: std::time::Instant) {
        let this = self.get_mut();
        this.cancel_wait();
        // Keep the signal — it can be reused for the new deadline.
        // Convert std::time::Instant to our Instant by capturing the offset
        // between the two clocks at this moment.
        let now_std = std::time::Instant::now();
        let now_hostcall = time_monotonic().expect("hostcall monotonic clock is available on WASM");
        let remaining = deadline.saturating_duration_since(now_std);
        this.deadline =
            Instant::from_nanos(now_hostcall.saturating_add(remaining.as_nanos() as u64));
    }

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        futures::Future::poll(self, cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_time_now_fails() {
        let result = time_now();
        assert!(matches!(result, Err(GuestError::Host(_))));
    }

    #[test]
    fn native_time_monotonic_fails() {
        let result = time_monotonic();
        assert!(matches!(result, Err(GuestError::Host(_))));
    }

    #[test]
    fn native_time_source_creation_fails() {
        let result = TimeSource::new();
        assert!(matches!(result, Err(GuestError::Host(_))));
    }

    #[test]
    fn instant_now_on_native_panics() {
        // On native (non-WASM) the hostcall is unavailable, so Instant::now()
        // panics. Catch the panic to verify this is expected.
        let result = std::panic::catch_unwind(Instant::now);
        assert!(result.is_err());
    }

    #[test]
    fn instant_arithmetic() {
        let t0 = Instant::from_nanos(1_000_000);
        let t1 = Instant::from_nanos(2_000_000);
        assert_eq!(t1.duration_since(t0), Duration::from_micros(1000));
        assert_eq!(t0.saturating_duration_since(t1), Duration::ZERO);

        let added = t0.checked_add(Duration::from_micros(500)).unwrap();
        assert_eq!(added.as_nanos(), 1_500_000);

        let subbed = t1.checked_sub(Duration::from_micros(500)).unwrap();
        assert_eq!(subbed.as_nanos(), 1_500_000);
    }

    #[test]
    fn instant_duration_since_normal_order() {
        let t0 = Instant::from_nanos(2_000_000);
        let t1 = Instant::from_nanos(1_000_000);
        assert_eq!(t0.duration_since(t1), Duration::from_micros(1000));
    }

    #[test]
    fn instant_duration_since_reverse_panics() {
        let t0 = Instant::from_nanos(1_000_000);
        let t1 = Instant::from_nanos(2_000_000);
        let result = std::panic::catch_unwind(|| t0.duration_since(t1));
        assert!(result.is_err()); // panics because t0 < t1
    }

    #[test]
    fn instant_checked_duration_since_reverse_is_none() {
        let t0 = Instant::from_nanos(1_000_000);
        let t1 = Instant::from_nanos(2_000_000);
        assert!(t0.checked_duration_since(t1).is_none());
    }

    #[test]
    fn instant_order() {
        let a = Instant::from_nanos(100);
        let b = Instant::from_nanos(200);
        assert!(a < b);
        assert!(b > a);
        assert!(a <= a);
        assert!(b >= b);
    }
}
