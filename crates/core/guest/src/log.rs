//! Guest log transport module.
//!
//! Provides structured log transport over shared-memory channels with
//! tracing subscriber integration. Log records are encoded as FlatBuffers
//! and published to a Drop-backpressure channel as ready frames.

pub use selium_encoding::log::{LogField, LogLevel, LogRecord, LogSpan};
pub use subscriber::{channel, init, init_with_capacity};

mod subscriber {
    use super::*;
    use selium_abi::{HostcallRequest, ResourceKind};
    use selium_encoding::FlatMsg;
    use selium_shm::channels::{Channel, ChannelBackpressure};
    use std::cell::Cell;
    use std::sync::OnceLock;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::prelude::__tracing_subscriber_SubscriberExt as SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    use crate::hostcall::hostcall_ready;

    /// Global logging state, initialised once via `init()`.
    struct LoggingState {
        channel: Channel,
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

    /// Returns the current wall-clock time in milliseconds, using the host
    /// clock (`std::time::SystemTime::now()` panics on
    /// `wasm32-unknown-unknown`). Falls back to 0 when the host clock is
    /// unavailable (native test contexts).
    fn timestamp_ms() -> u64 {
        crate::time::now()
            .map(|nanos| nanos / 1_000_000)
            .unwrap_or(0)
    }

    /// Forwards a tracing event to the log channel as a framed FlatBuffer LogRecord.
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

        let record = LogRecord {
            level,
            target,
            message: visitor.message,
            fields: visitor.fields,
            spans,
            timestamp_ms: timestamp_ms(),
        };

        let encoded = FlatMsg::encode(&record);

        // Write a ready frame to the channel ring. The log channel uses Drop
        // backpressure: if the ring is full the record is silently dropped
        // rather than blocking the caller. Logging is best-effort.
        let ring = state.channel.ring();
        if let Ok(pos) = ring
            .reserve(selium_wire::frame::FrameHeader::ENCODED_SIZE as u64 + encoded.len() as u64)
        {
            drop(ring.write_frame(pos, &encoded, 0, 0));
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

        let state = LoggingState { channel };

        // Atomically install state. If another thread won the race, discard ours.
        drop(LOGGING_STATE.set(state));

        // Install the subscriber. try_init returns Err if a subscriber is
        // already installed (harmless — the existing one is equivalent).
        let subscriber = tracing_subscriber::registry().with(LogLayer);
        drop(subscriber.try_init());

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
    /// Log channel creation failed.
    Channel(String),
}

impl std::fmt::Display for InitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Channel(msg) => write!(f, "channel creation failed: {msg}"),
        }
    }
}

impl std::error::Error for InitError {}
