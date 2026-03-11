#[cfg(any(target_arch = "wasm32", test))]
use std::fmt;
#[cfg(target_arch = "wasm32")]
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[cfg(any(target_arch = "wasm32", test))]
use selium_abi::GuestLogStream;
#[cfg(target_arch = "wasm32")]
use tracing::{
    Event, Metadata, Subscriber,
    span::{Attributes, Id, Record},
    subscriber::Interest,
};
#[cfg(any(target_arch = "wasm32", test))]
use tracing::{
    Level,
    field::{Field, Visit},
};

#[cfg(target_arch = "wasm32")]
pub struct GuestLoggingGuard {
    _guard: tracing::subscriber::DefaultGuard,
}

#[cfg(not(target_arch = "wasm32"))]
pub struct GuestLoggingGuard;

#[doc(hidden)]
#[cfg(target_arch = "wasm32")]
pub fn __enter_guest_logging() -> GuestLoggingGuard {
    let guard = tracing::subscriber::set_default(GuestLogSubscriber::new(Arc::new(HostLogSink)));
    GuestLoggingGuard { _guard: guard }
}

#[doc(hidden)]
#[cfg(not(target_arch = "wasm32"))]
pub fn __enter_guest_logging() -> GuestLoggingGuard {
    GuestLoggingGuard
}

#[cfg(target_arch = "wasm32")]
trait GuestLogSink: Send + Sync + 'static {
    fn write(&self, stream: GuestLogStream, payload: &[u8]);
}

#[cfg(target_arch = "wasm32")]
struct HostLogSink;

#[cfg(target_arch = "wasm32")]
impl GuestLogSink for HostLogSink {
    fn write(&self, stream: GuestLogStream, payload: &[u8]) {
        host_write(stream, payload);
    }
}

#[cfg(target_arch = "wasm32")]
struct GuestLogSubscriber {
    sink: Arc<dyn GuestLogSink>,
    next_span_id: AtomicU64,
}

#[cfg(target_arch = "wasm32")]
impl GuestLogSubscriber {
    fn new(sink: Arc<dyn GuestLogSink>) -> Self {
        Self {
            sink,
            next_span_id: AtomicU64::new(1),
        }
    }
}

#[cfg(target_arch = "wasm32")]
impl Subscriber for GuestLogSubscriber {
    fn enabled(&self, _metadata: &Metadata<'_>) -> bool {
        true
    }

    fn register_callsite(&self, _metadata: &'static Metadata<'static>) -> Interest {
        Interest::always()
    }

    fn new_span(&self, _span: &Attributes<'_>) -> Id {
        Id::from_u64(self.next_span_id.fetch_add(1, Ordering::Relaxed))
    }

    fn record(&self, _span: &Id, _values: &Record<'_>) {}

    fn record_follows_from(&self, _span: &Id, _follows: &Id) {}

    fn enter(&self, _span: &Id) {}

    fn exit(&self, _span: &Id) {}

    fn event(&self, event: &Event<'_>) {
        let (stream, payload) = format_event(event);
        self.sink.write(stream, &payload);
    }
}

#[cfg(target_arch = "wasm32")]
fn format_event(event: &Event<'_>) -> (GuestLogStream, Vec<u8>) {
    let metadata = event.metadata();
    let mut fields = EventFields::default();
    event.record(&mut fields);

    render_event(metadata.level(), metadata.target(), fields)
}

#[cfg(any(target_arch = "wasm32", test))]
fn render_event(level: &Level, target: &str, mut fields: EventFields) -> (GuestLogStream, Vec<u8>) {
    let stream = match *level {
        Level::ERROR | Level::WARN => GuestLogStream::Stderr,
        Level::INFO | Level::DEBUG | Level::TRACE => GuestLogStream::Stdout,
    };
    let mut rendered = format!("{level} {target}");
    if let Some(message) = fields.message.take() {
        rendered.push_str(": ");
        rendered.push_str(&message);
    }
    if !fields.extras.is_empty() {
        rendered.push(' ');
        rendered.push_str(&fields.extras.join(" "));
    }
    rendered.push('\n');
    (stream, rendered.into_bytes())
}

#[cfg(any(target_arch = "wasm32", test))]
#[derive(Default)]
struct EventFields {
    message: Option<String>,
    extras: Vec<String>,
}

#[cfg(any(target_arch = "wasm32", test))]
impl EventFields {
    fn push_value(&mut self, field: &Field, value: String) {
        if field.name() == "message" {
            self.message = Some(value);
        } else {
            self.extras.push(format!("{}={value}", field.name()));
        }
    }
}

#[cfg(any(target_arch = "wasm32", test))]
impl Visit for EventFields {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.push_value(field, value.to_string());
    }

    fn record_debug(&mut self, field: &Field, value: &dyn fmt::Debug) {
        self.push_value(field, format!("{value:?}"));
    }
}

#[cfg(target_arch = "wasm32")]
fn host_write(stream: GuestLogStream, payload: &[u8]) {
    use selium_abi::{GuestInt, GuestUint};

    #[link(wasm_import_module = "selium::log")]
    unsafe extern "C" {
        fn write(stream: GuestUint, payload_ptr: GuestInt, payload_len: GuestUint);
    }

    unsafe {
        write(
            stream.as_raw(),
            payload.as_ptr() as GuestInt,
            payload.len() as GuestUint,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rendered_events_route_info_and_warn_to_expected_streams() {
        let (stdout_stream, stdout_payload) = render_event(
            &Level::INFO,
            "guest::tests",
            EventFields {
                message: Some("hello".to_string()),
                extras: vec!["answer=42".to_string()],
            },
        );
        assert_eq!(stdout_stream, GuestLogStream::Stdout);
        let stdout = String::from_utf8(stdout_payload).expect("utf8");
        assert!(stdout.contains("INFO guest::tests: hello"));
        assert!(stdout.contains("answer=42"));

        let (stderr_stream, stderr_payload) = render_event(
            &Level::WARN,
            "guest::tests",
            EventFields {
                message: Some("warned".to_string()),
                extras: vec!["reason=boom".to_string()],
            },
        );
        assert_eq!(stderr_stream, GuestLogStream::Stderr);
        let stderr = String::from_utf8(stderr_payload).expect("utf8");
        assert!(stderr.contains("WARN guest::tests: warned"));
        assert!(stderr.contains("reason=boom"));
    }
}
