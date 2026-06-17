//! Guest log transport module.
//!
//! Provides structured log transport over shared-memory channels with
//! tracing subscriber integration. Log records are encoded as FlatBuffers
//! and published to a Drop-backpressure channel.

use flatbuffers::{FlatBufferBuilder, InvalidFlatbuffer, WIPOffset};

use crate::{
    encoding::{FlatMsg, HasSchema, SchemaDescriptor},
    fbs::selium::logging::{
        Field, FieldArgs, LogLevel as FbsLogLevel, LogRecord as FbsLogRecord, LogRecordArgs, Span,
        SpanArgs,
    },
};

#[cfg(feature = "logging")]
pub use subscriber::{channel, init, init_with_capacity};

#[cfg(feature = "logging")]
mod subscriber {
    use super::*;
    use crate::hostcall::hostcall_ready;
    use crate::io::channels::{Channel, ChannelBackpressure, Writer};
    use selium_abi::{HostcallRequest, ResourceKind};
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
        error: Mutex<Option<InitError>>,
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
            let _ = std::pin::Pin::new(&mut *writer).poll_write(&mut cx, &encoded);
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
        let _ = hostcall_ready(HostcallRequest::GuestLogRegister { shared_id });

        let writer = channel
            .writer()
            .map_err(|e| InitError::Publisher(e.to_string()))?;

        let state = LoggingState {
            channel,
            writer: Mutex::new(writer),
            error: Mutex::new(None),
        };

        // Atomically install state. If another thread won the race, discard ours.
        let installed = LOGGING_STATE.get_or_init(|| state);

        // Only install the subscriber if we were the thread that installed the state.
        // (If another thread installed first, they already installed the subscriber.)
        if std::ptr::eq(installed, LOGGING_STATE.get().unwrap()) {
            // We may or may not be the installer — but try_init is idempotent-safe:
            // it returns Err if a subscriber is already installed, which we can ignore.
            let subscriber = tracing_subscriber::registry().with(LogLayer);
            let _ = subscriber.try_init();
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

/// Log severity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LogLevel {
    /// Trace level.
    Trace,
    /// Debug level.
    Debug,
    /// Info level.
    Info,
    /// Warn level.
    Warn,
    /// Error level.
    Error,
}

/// A single key-value field attached to a log record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogField {
    /// Field key.
    pub key: String,
    /// Field value (stringified).
    pub value: String,
}

/// A span in the tracing span stack at the time the event was emitted.
#[derive(Debug, Clone, PartialEq)]
pub struct LogSpan {
    /// Span name.
    pub name: String,
    /// Fields attached to the span.
    pub fields: Vec<LogField>,
}

/// A structured log record published to the guest log channel.
#[derive(Debug, Clone, PartialEq)]
pub struct LogRecord {
    /// Severity level.
    pub level: LogLevel,
    /// Tracing target (typically the module path).
    pub target: String,
    /// Log message.
    pub message: String,
    /// Key-value fields attached to the event.
    pub fields: Vec<LogField>,
    /// Span stack at the time of the event.
    pub spans: Vec<LogSpan>,
    /// Timestamp in milliseconds since UNIX epoch.
    pub timestamp_ms: u64,
}

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

impl From<tracing::Level> for LogLevel {
    fn from(level: tracing::Level) -> Self {
        match level {
            tracing::Level::TRACE => LogLevel::Trace,
            tracing::Level::DEBUG => LogLevel::Debug,
            tracing::Level::INFO => LogLevel::Info,
            tracing::Level::WARN => LogLevel::Warn,
            tracing::Level::ERROR => LogLevel::Error,
        }
    }
}

impl FlatMsg for LogRecord {
    fn encode(value: &Self) -> Vec<u8> {
        let mut fbb = FlatBufferBuilder::new();

        let target = fbb.create_string(&value.target);
        let message = fbb.create_string(&value.message);

        let fields_vec: Vec<WIPOffset<Field>> = value
            .fields
            .iter()
            .map(|f| encode_field(&mut fbb, f))
            .collect();
        let fields = fbb.create_vector(&fields_vec);

        let spans_vec: Vec<WIPOffset<Span>> = value
            .spans
            .iter()
            .map(|s| encode_span(&mut fbb, s))
            .collect();
        let spans = fbb.create_vector(&spans_vec);

        let record = FbsLogRecord::create(
            &mut fbb,
            &LogRecordArgs {
                level: to_fbs_level(value.level),
                target: Some(target),
                message: Some(message),
                fields: Some(fields),
                spans: Some(spans),
                timestamp_ms: value.timestamp_ms,
            },
        );

        fbb.finish(record, None);
        fbb.finished_data().to_vec()
    }

    fn decode(bytes: &[u8]) -> Result<Self, InvalidFlatbuffer> {
        let record = flatbuffers::root::<FbsLogRecord>(bytes)?;

        let level = from_fbs_level(record.level());
        let target = record.target().unwrap_or("").to_string();
        let message = record.message().unwrap_or("").to_string();
        let timestamp_ms = record.timestamp_ms();

        let fields = record
            .fields()
            .map(|v| {
                (0..v.len())
                    .map(|i| {
                        let f = v.get(i);
                        LogField {
                            key: f.key().unwrap_or("").to_string(),
                            value: f.value().unwrap_or("").to_string(),
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        let spans = record
            .spans()
            .map(|v| {
                (0..v.len())
                    .map(|i| {
                        let s = v.get(i);
                        let span_fields = s
                            .fields()
                            .map(|fv| {
                                (0..fv.len())
                                    .map(|j| {
                                        let f = fv.get(j);
                                        LogField {
                                            key: f.key().unwrap_or("").to_string(),
                                            value: f.value().unwrap_or("").to_string(),
                                        }
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        LogSpan {
                            name: s.name().unwrap_or("").to_string(),
                            fields: span_fields,
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        Ok(Self {
            level,
            target,
            message,
            fields,
            spans,
            timestamp_ms,
        })
    }
}

impl HasSchema for LogRecord {
    const SCHEMA: SchemaDescriptor = SchemaDescriptor {
        fqname: "selium.logging.LogRecord",
        hash: [
            0x4C, 0x4F, 0x47, 0x52, 0x45, 0x43, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00,
        ],
    };
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

impl From<LogLevel> for tracing::Level {
    fn from(level: LogLevel) -> Self {
        match level {
            LogLevel::Trace => tracing::Level::TRACE,
            LogLevel::Debug => tracing::Level::DEBUG,
            LogLevel::Info => tracing::Level::INFO,
            LogLevel::Warn => tracing::Level::WARN,
            LogLevel::Error => tracing::Level::ERROR,
        }
    }
}

fn encode_field<'a>(fbb: &mut FlatBufferBuilder<'a>, field: &LogField) -> WIPOffset<Field<'a>> {
    let key = fbb.create_string(&field.key);
    let value = fbb.create_string(&field.value);
    Field::create(
        fbb,
        &FieldArgs {
            key: Some(key),
            value: Some(value),
        },
    )
}

fn encode_span<'a>(fbb: &mut FlatBufferBuilder<'a>, span: &LogSpan) -> WIPOffset<Span<'a>> {
    let name = fbb.create_string(&span.name);
    let fields_vec: Vec<WIPOffset<Field>> =
        span.fields.iter().map(|f| encode_field(fbb, f)).collect();
    let fields = fbb.create_vector(&fields_vec);
    Span::create(
        fbb,
        &SpanArgs {
            name: Some(name),
            fields: Some(fields),
        },
    )
}

fn from_fbs_level(level: FbsLogLevel) -> LogLevel {
    match level {
        FbsLogLevel::Trace => LogLevel::Trace,
        FbsLogLevel::Debug => LogLevel::Debug,
        FbsLogLevel::Info => LogLevel::Info,
        FbsLogLevel::Warn => LogLevel::Warn,
        FbsLogLevel::Error => LogLevel::Error,
        _ => LogLevel::Info,
    }
}

fn to_fbs_level(level: LogLevel) -> FbsLogLevel {
    match level {
        LogLevel::Trace => FbsLogLevel::Trace,
        LogLevel::Debug => FbsLogLevel::Debug,
        LogLevel::Info => FbsLogLevel::Info,
        LogLevel::Warn => FbsLogLevel::Warn,
        LogLevel::Error => FbsLogLevel::Error,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_level_from_tracing_level() {
        assert_eq!(LogLevel::from(tracing::Level::TRACE), LogLevel::Trace);
        assert_eq!(LogLevel::from(tracing::Level::DEBUG), LogLevel::Debug);
        assert_eq!(LogLevel::from(tracing::Level::INFO), LogLevel::Info);
        assert_eq!(LogLevel::from(tracing::Level::WARN), LogLevel::Warn);
        assert_eq!(LogLevel::from(tracing::Level::ERROR), LogLevel::Error);
    }

    #[test]
    fn tracing_level_from_log_level() {
        assert_eq!(tracing::Level::from(LogLevel::Trace), tracing::Level::TRACE);
        assert_eq!(tracing::Level::from(LogLevel::Debug), tracing::Level::DEBUG);
        assert_eq!(tracing::Level::from(LogLevel::Info), tracing::Level::INFO);
        assert_eq!(tracing::Level::from(LogLevel::Warn), tracing::Level::WARN);
        assert_eq!(tracing::Level::from(LogLevel::Error), tracing::Level::ERROR);
    }

    #[test]
    fn log_record_encode_decode_round_trip() {
        let record = LogRecord {
            level: LogLevel::Info,
            target: "my_module".to_string(),
            message: "hello world".to_string(),
            fields: vec![
                LogField {
                    key: "user_id".to_string(),
                    value: "42".to_string(),
                },
                LogField {
                    key: "action".to_string(),
                    value: "login".to_string(),
                },
            ],
            spans: vec![LogSpan {
                name: "request".to_string(),
                fields: vec![LogField {
                    key: "method".to_string(),
                    value: "GET".to_string(),
                }],
            }],
            timestamp_ms: 1700000000000,
        };

        let encoded = FlatMsg::encode(&record);
        let decoded: LogRecord = FlatMsg::decode(&encoded).expect("decode");

        assert_eq!(decoded.level, record.level);
        assert_eq!(decoded.target, record.target);
        assert_eq!(decoded.message, record.message);
        assert_eq!(decoded.fields.len(), record.fields.len());
        assert_eq!(decoded.fields[0].key, "user_id");
        assert_eq!(decoded.fields[0].value, "42");
        assert_eq!(decoded.spans.len(), 1);
        assert_eq!(decoded.spans[0].name, "request");
        assert_eq!(decoded.spans[0].fields[0].key, "method");
        assert_eq!(decoded.timestamp_ms, record.timestamp_ms);
    }

    #[test]
    fn log_record_minimal_round_trip() {
        let record = LogRecord {
            level: LogLevel::Error,
            target: "test".to_string(),
            message: "oops".to_string(),
            fields: vec![],
            spans: vec![],
            timestamp_ms: 0,
        };

        let encoded = FlatMsg::encode(&record);
        let decoded: LogRecord = FlatMsg::decode(&encoded).expect("decode");

        assert_eq!(decoded.level, LogLevel::Error);
        assert_eq!(decoded.message, "oops");
        assert!(decoded.fields.is_empty());
        assert!(decoded.spans.is_empty());
    }
}
