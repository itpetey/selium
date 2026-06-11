## Purpose

`selium-guest` SHALL provide frame-level I/O abstractions (`FramedRead`, `FramedWrite`, `FrameRead`, `FrameWrite`) that wrap shared-memory ring buffer readers and writers with frame header encoding, tag correlation, and generation counter tracking, enabling higher-level messaging patterns (pub/sub, RPC) to operate over a common framed transport.

## Requirements

### Requirement: FramedRead Type
`selium-guest` SHALL provide a `FramedRead<R>` type that wraps any `FrameRead` implementor to provide frame-level read operations with `FrameHeader` decoding and tag extraction. The `FrameRead` trait provides `read_frame()`, `generation()`, and `poll_ready()` methods, enabling `FramedRead` to work generically over both strong and weak reader types while preserving frame-level semantics that `AsyncRead` cannot express.

#### Scenario: Read a complete frame
- **WHEN** a caller invokes `FramedRead::read_frame()` and a frame with the READY flag set is available on the underlying reader
- **THEN** the method SHALL return `Ok((payload_bytes, tag))` where `payload_bytes` is the frame payload and `tag` is the frame's correlation tag

#### Scenario: No frame ready
- **WHEN** a caller invokes `FramedRead::read_frame()` and no complete frame is available
- **THEN** the method SHALL return `Err(Error::BufferEmpty)`

#### Scenario: Underlying reader returns overwrite error
- **WHEN** a caller invokes `FramedRead::read_frame()` and the inner reader's `read_frame` returns an overwrite error
- **THEN** `FramedRead` SHALL propagate the error as `Error::Overwritten`

### Requirement: FramedWrite Type
`selium-guest` SHALL provide a `FramedWrite<W>` type that wraps any `FrameWrite` implementor to provide frame-level write operations with `FrameHeader` encoding. The `FrameWrite` trait provides `write_frame()` methods, enabling `FramedWrite` to work generically over both strong and weak writer types.

#### Scenario: Write a framed payload
- **WHEN** a caller invokes `FramedWrite::write_frame(payload, tag)` with a payload and correlation tag
- **THEN** the method SHALL encode a `FrameHeader` with the given tag and READY flag, write the frame to the underlying writer, and return `Ok(())`

#### Scenario: Payload too large for frame
- **WHEN** a caller invokes `FramedWrite::write_frame(payload, tag)` with a payload whose length exceeds `u32::MAX`
- **THEN** the method SHALL return `Err(Error::InvalidFrame)`

### Requirement: FramedRead/FramedWrite Generic Over Inner Type
`FramedRead<R>` and `FramedWrite<W>` SHALL be generic over the inner reader/writer type, allowing composition with `Reader`, `WeakReader`, `Writer`, `WeakWriter`, or any other `FrameRead`/`FrameWrite` implementor.

#### Scenario: FramedRead wraps a StrongReader
- **WHEN** a `FramedRead<Reader>` is constructed with a strong `Reader`
- **THEN** `read_frame()` SHALL read frames through the strong reader, benefiting from backpressure protection

#### Scenario: FramedRead wraps a WeakReader
- **WHEN** a `FramedRead<WeakReader>` is constructed with a weak `WeakReader`
- **THEN** `read_frame()` SHALL read frames without backpressure protection

### Requirement: FramedRead exposes generation counter
`FramedRead<R>` SHALL expose a `generation()` method that delegates to the inner reader's generation counter, enabling overwrite detection by higher-level types such as `Subscriber`.

#### Scenario: Read generation counter
- **WHEN** a caller invokes `framed_read.generation()`
- **THEN** the method SHALL return the current generation counter value from the underlying ring buffer

### Requirement: FrameRead and FrameWrite Traits
`selium-guest` SHALL provide `FrameRead` and `FrameWrite` traits that abstract frame-level operations over reader and writer types. These traits exist because `tokio::io::AsyncRead`/`AsyncWrite` cannot express frame-level semantics (tag correlation, frame boundaries). `Reader` and `WeakReader` implement `FrameRead`; `Writer` and `WeakWriter` implement `FrameWrite`.

#### Scenario: FrameRead provides frame-level read
- **WHEN** a type implements `FrameRead`
- **THEN** it SHALL provide `read_frame() -> Result<(Vec<u8>, u32)>`, `generation() -> Result<u64>`, and `poll_ready() -> Result<bool>`

#### Scenario: FrameWrite provides frame-level write
- **WHEN** a type implements `FrameWrite`
- **THEN** it SHALL provide `write_frame(payload: &[u8], tag: u32) -> Result<()>`

### Requirement: Channel creation with backpressure
`Channel` SHALL provide `create(capacity: u64) -> Result<Self>` that creates a channel with default backpressure behaviour where writers respect blocking reader positions.

#### Scenario: Channel created with default backpressure
- **WHEN** a caller invokes `Channel::create(65536)`
- **THEN** `blocking_writer()` SHALL succeed and writers SHALL respect blocking reader positions

### Requirement: Channel backpressure error variant
`selium_guest::io::Error` SHALL provide error variants appropriate for channel operations.

#### Scenario: Error variant exists for channel operations
- **WHEN** a channel operation encounters a backpressure-related error
- **THEN** the error SHALL be representable via the existing `io::Error` type

### Requirement: Strong Writer Backpressure via AsyncWrite
`Writer` SHALL implement `tokio::io::AsyncWrite`. The `poll_write` method SHALL write bytes as a framed payload to the outbound ring buffer with strong-reader backpressure (`protect_readers = true`). If the buffer is full, `poll_write` SHALL return `Poll::Pending`.

#### Scenario: Writer sends bytes
- **WHEN** a caller invokes `poll_write(buf)` on a `Writer`
- **THEN** the method SHALL reserve space in the ring buffer, write a frame containing the buffer bytes, and return `Poll::Ready(Ok(buf.len()))`

#### Scenario: Writer buffer full
- **WHEN** a caller invokes `poll_write(buf)` and the ring buffer cannot accommodate the frame without overwriting unread strong-reader data
- **THEN** the method SHALL return `Poll::Pending`

### Requirement: RingBuf creation with ResourceKind
`RingBuf::create` SHALL accept `capacity: u64` and create a ring buffer with the standard coordination layout for shared memory communication.

#### Scenario: RingBuf created with default parameters
- **WHEN** a caller invokes `RingBuf::create(65536)`
- **THEN** a ring buffer SHALL be created with the standard coordination layout
