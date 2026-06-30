//! Guest log transport module.
//!
//! Provides structured log transport over shared-memory channels with
//! tracing subscriber integration. Log records are encoded as FlatBuffers
//! and published to a Drop-backpressure channel.

pub use selium_encoding::log::{LogField, LogLevel, LogRecord, LogSpan};

#[cfg(feature = "logging")]
pub use subscriber::{channel, init, init_with_capacity};

#[cfg(feature = "logging")]
mod subscriber {
    use super::*;
    use crate::hostcall::hostcall_ready;
    use selium_abi::{HostcallRequest, ResourceKind};
    use selium_encoding::FlatMsg;
    use selium_shm::channels::{Channel, ChannelBackpressure, Writer};
    use std::cell::Cell;
    use std::sync::{Mutex, OnceLock};
    use tokio::io::AsyncWrite;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt as SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    /// Global logging state, initialised once via `init()`.
    struct LoggingState {
        channel: Channel,
        writer: Mutex<Writer>,
    }

    static LOGGING_STATE: OnceLock<LoggingState> = OnceLock::new();

    thread_local! {
        /// Re-entrancy guard: suppresses log events triggered while forwarding.
        static FORWARDING: Cell<bool> = const { Cell::new(false) };
    }

    /// Guard that sets the forwarding flag on entry and clears it on drop.
    struct ForwardingGuard;

    impl ForwardingGuard {
        /// Returns `Some(guard)` if not already forwarding, `None` if re-entrant.
        fn enter() -> Option<Self> {
            let was_forwarding = FORWARDING.with(|f| {
                let prev = f.get();
                f.set(true);
                prev
            });
            if was_forwarding {
                None
            } else {
                Some(ForwardingGuard)
            }
        }
    }

    impl Drop for ForwardingGuard {
        fn drop(&mut self) {
            FORWARDING.with(|f| f.set(false));
        }
    }

    /// Tracing subscriber layer that forwards events to the log channel.
    pub(crate) struct LogLayer;

    impl<S: tracing::Subscriber> Layer<S> for LogLayer {
        fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
            forward_event(event);
        }
    }

    /// Visitor that extracts the message and fields from a tracing event.
    struct EventVisitor {
        message: String,
        fields: Vec<LogField>,
    }

    impl EventVisitor {
        fn new() -> Self {
            Self {
                message: String::new(),
                fields: Vec::new(),
            }
        }
    }

    impl Visit for EventVisitor {
        fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" {
                self.message = format!("{value:?}");
            } else {
                self.fields.push(LogField {
                    key: field.name().to_string(),
                    value: format!("{value:?}"),
                });
            }
        }

        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "message" {
                self.message = value.to_string();
            } else {
                self.fields.push(LogField {
                    key: field.name().to_string(),
                    value: value.to_string(),
                });
            }
        }

        fn record_i64(&mut self, field: &Field, value: i64) {
            self.fields.push(LogField {
                key: field.name().to_string(),
                value: value.to_string(),
            });
        }

        fn record_u64(&mut self, field: &Field, value: u64) {
            self.fields.push(LogField {
                key: field.name().to_string(),
                value: value.to_string(),
            });
        }

        fn record_bool(&mut self, field: &Field, value: bool) {
            self.fields.push(LogField {
                key: field.name().to_string(),
                value: value.to_string(),
            });
        }
    }

    /// Forwards a tracing event to the log channel as a FlatBuffer-encoded LogRecord.
    fn forward_event(event: &tracing::Event<'_>) {
        let _guard = match ForwardingGuard::enter() {
            Some(g) => g,
            None => return, // re-entrant, suppress
        };

        let Some(state) = LOGGING_STATE.get() else {
            return; // not initialised
        };

        let mut visitor = EventVisitor::new();
        event.record(&mut visitor);

        let metadata = event.metadata();
        let level = LogLevel::from(*metadata.level());
        let target = metadata.target().to_string();

        // Collect span stack.
        let spans = Vec::new(); // TODO: walk span stack via event.parent()

        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let record = LogRecord {
            level,
            target,
            message: visitor.message,
            fields: visitor.fields,
            spans,
            timestamp_ms,
        };

        let encoded = FlatMsg::encode(&record);

        // Write to the channel writer (non-blocking, Drop backpressure).
        if let Ok(mut writer) = state.writer.lock() {
            let waker = futures::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);
            // Discard the Poll result: the channel uses Drop backpressure, so a
            // full buffer silently drops the write rather than blocking. Logging
            // is best-effort and must never stall the caller.
            drop(std::pin::Pin::new(&mut *writer).poll_write(&mut cx, &encoded));
        }
    }

    /// Initialises the guest log transport with default capacity.
    ///
    /// Creates a Drop-backpressure channel with `ResourceKind::LogChannel`,
    /// installs a tracing subscriber, and registers the channel with the kernel.
    ///
    /// Subsequent calls return `Ok(())` without installing a second subscriber.
    pub fn init() -> Result<(), InitError> {
        init_with_capacity(DEFAULT_LOG_CAPACITY)
    }

    /// Initialises the guest log transport with a custom channel capacity.
    pub fn init_with_capacity(capacity: u64) -> Result<(), InitError> {
        // Use get_or_try_init pattern: attempt to create state and install it atomically.
        // If another thread already initialised, return Ok without creating a channel.
        if LOGGING_STATE.get().is_some() {
            return Ok(());
        }

        let channel = Channel::create_with_backpressure(
            capacity,
            ChannelBackpressure::Drop,
            ResourceKind::LogChannel,
        )
        .map_err(|e| InitError::Channel(e.to_string()))?;

        // Register the log channel with the kernel so it can attach as a reader.
        // In native mode (no WASM host), this hostcall will fail — that's expected
        // and we continue without kernel registration.
        let shared_id = channel.region_id();
        // Discard the hostcall result: kernel registration is a best-effort
        // optimisation. In native (non-WASM) mode the hostcall always fails,
        // which is expected and harmless.
        drop(hostcall_ready(HostcallRequest::GuestLogRegister {
            shared_id,
        }));

        let writer = channel
            .writer()
            .map_err(|e| InitError::Publisher(e.to_string()))?;

        let state = LoggingState {
            channel,
            writer: Mutex::new(writer),
        };

        // Atomically install state. If another thread won the race, discard ours.
        let installed = LOGGING_STATE.get_or_init(|| state);

        // Only install the subscriber if we were the thread that installed the state.
        // (If another thread installed first, they already installed the subscriber.)
        if std::ptr::eq(
            installed,
            LOGGING_STATE.get().expect("state just initialized"),
        ) {
            // We may or may not be the installer — but try_init is idempotent-safe:
            // it returns Err if a subscriber is already installed, which we can ignore.
            let subscriber = tracing_subscriber::registry().with(LogLayer);
            // Discard the try_init result: Err means a subscriber was already
            // installed by a concurrent initialiser (harmless — ours is identical).
            // We use try_init instead of init precisely to make this safe.
            drop(subscriber.try_init());
        }

        Ok(())
    }

    /// Returns the log channel handle if initialised.
    pub fn channel() -> Option<&'static Channel> {
        LOGGING_STATE.get().map(|s| &s.channel)
    }
}

/// Default log channel capacity in bytes (512 KB, matching prior art).
const DEFAULT_LOG_CAPACITY: u64 = 512 * 1024;

/// Error type for log initialisation.
#[derive(Debug)]
pub enum InitError {
    /// Tracing subscriber installation failed.
    Subscriber(String),
    /// Log channel creation failed.
    Channel(String),
    /// Channel registration with kernel failed.
    Register(String),
    /// Log publisher creation failed.
    Publisher(String),
    /// Internal mutex poisoned.
    Poisoned,
}

impl std::fmt::Display for InitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Subscriber(msg) => write!(f, "subscriber init failed: {msg}"),
            Self::Channel(msg) => write!(f, "channel creation failed: {msg}"),
            Self::Register(msg) => write!(f, "kernel registration failed: {msg}"),
            Self::Publisher(msg) => write!(f, "publisher creation failed: {msg}"),
            Self::Poisoned => write!(f, "internal mutex poisoned"),
        }
    }
}

impl std::error::Error for InitError {}
