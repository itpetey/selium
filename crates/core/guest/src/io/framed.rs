//! Framed read/write wrappers for byte-stream readers and writers.
//!
//! `FramedRead<R>` and `FramedWrite<W>` wrap any type that provides
//! frame-level read/write operations, adding a consistent API for
//! frame-based communication with tag correlation.

use crate::io::{
    channels::{Reader, WeakReader, WeakWriter, Writer},
    error::{Error, Result},
};

/// A framed reader that wraps a byte-stream reader to provide frame-level
/// read operations with `FrameHeader` decoding and tag extraction.
///
/// Generic over the inner reader type, allowing composition with `Reader`,
/// `WeakReader`, or any other type that provides `read_frame()` and
/// `generation()` methods.
pub struct FramedRead<R> {
    inner: R,
}

/// A framed writer that wraps a byte-stream writer to provide frame-level
/// write operations with `FrameHeader` encoding.
///
/// Generic over the inner writer type, allowing composition with `Writer`,
/// `WeakWriter`, or any other type that provides `write_frame()` methods.
pub struct FramedWrite<W> {
    inner: W,
}

/// Trait for types that can read frames and expose a generation counter.
///
/// Implemented by `Reader` and `WeakReader` to allow `FramedRead` to work
/// generically over both strong and weak reader types.
pub trait FrameRead {
    /// Read the next complete frame, returning `(payload, tag)`.
    fn read_frame(&mut self) -> Result<(Vec<u8>, u32)>;
    /// Returns the current generation counter.
    fn generation(&self) -> Result<u64>;
    /// Non-blocking check for frame readiness.
    fn poll_ready(&mut self) -> Result<bool>;
}

/// Trait for types that can write frames.
///
/// Implemented by `Writer` and `WeakWriter` to allow `FramedWrite` to work
/// generically over both strong and weak writer types.
pub trait FrameWrite {
    /// Write a framed payload with the given tag.
    fn write_frame(&mut self, payload: &[u8], tag: u32) -> Result<()>;
}

impl FrameRead for Reader {
    fn read_frame(&mut self) -> Result<(Vec<u8>, u32)> {
        Reader::read_frame(self)
    }

    fn generation(&self) -> Result<u64> {
        Reader::generation(self)
    }

    fn poll_ready(&mut self) -> Result<bool> {
        Reader::poll_ready(self)
    }
}

impl FrameRead for WeakReader {
    fn read_frame(&mut self) -> Result<(Vec<u8>, u32)> {
        WeakReader::read_frame(self)
    }

    fn generation(&self) -> Result<u64> {
        WeakReader::generation(self)
    }

    fn poll_ready(&mut self) -> Result<bool> {
        WeakReader::poll_ready(self)
    }
}

impl FrameWrite for Writer {
    fn write_frame(&mut self, payload: &[u8], tag: u32) -> Result<()> {
        Writer::write_frame(self, payload, tag)
    }
}

impl FrameWrite for WeakWriter {
    fn write_frame(&mut self, payload: &[u8], tag: u32) -> Result<()> {
        WeakWriter::write_frame(self, payload, tag)
    }
}

impl<R: FrameRead> FramedRead<R> {
    /// Creates a new `FramedRead` wrapping the given reader.
    pub fn new(inner: R) -> Self {
        Self { inner }
    }

    /// Reads the next complete frame, returning `(payload, tag)`.
    ///
    /// Uses the inner reader's frame-level read operation to decode
    /// the `FrameHeader` and extract the payload and correlation tag.
    pub fn read_frame(&mut self) -> Result<(Vec<u8>, u32)> {
        self.inner.read_frame()
    }

    /// Returns the current generation counter from the underlying ring buffer.
    pub fn generation(&self) -> Result<u64> {
        self.inner.generation()
    }

    /// Non-blocking check for frame readiness.
    pub fn poll_ready(&mut self) -> Result<bool> {
        self.inner.poll_ready()
    }

    /// Returns a reference to the inner reader.
    pub fn inner(&self) -> &R {
        &self.inner
    }

    /// Returns a mutable reference to the inner reader.
    pub fn inner_mut(&mut self) -> &mut R {
        &mut self.inner
    }

    /// Consumes this `FramedRead` and returns the inner reader.
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<W: FrameWrite> FramedWrite<W> {
    /// Creates a new `FramedWrite` wrapping the given writer.
    pub fn new(inner: W) -> Self {
        Self { inner }
    }

    /// Writes a framed payload with the given correlation tag.
    ///
    /// Encodes a `FrameHeader` with the payload length and tag, writes
    /// the frame to the underlying writer.
    pub fn write_frame(&mut self, payload: &[u8], tag: u32) -> Result<()> {
        if payload.len() > u32::MAX as usize {
            return Err(Error::InvalidFrame);
        }
        self.inner.write_frame(payload, tag)
    }

    /// Returns a reference to the inner writer.
    pub fn inner(&self) -> &W {
        &self.inner
    }

    /// Returns a mutable reference to the inner writer.
    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.inner
    }

    /// Consumes this `FramedWrite` and returns the inner writer.
    pub fn into_inner(self) -> W {
        self.inner
    }
}

// Upgrade/downgrade support for FramedRead
impl FramedRead<WeakReader> {
    /// Upgrade the inner weak reader to a strong reader.
    pub fn upgrade(self) -> Result<FramedRead<Reader>> {
        let strong = self.inner.upgrade()?;
        Ok(FramedRead::new(strong))
    }
}

impl FramedRead<Reader> {
    /// Downgrade the inner strong reader to a weak reader.
    pub fn downgrade(self) -> FramedRead<WeakReader> {
        let weak = self.inner.downgrade();
        FramedRead::new(weak)
    }
}

// Upgrade/downgrade support for FramedWrite
impl FramedWrite<WeakWriter> {
    /// Upgrade the inner weak writer to a strong writer.
    pub fn upgrade(self) -> Result<FramedWrite<Writer>> {
        let strong = self.inner.upgrade()?;
        Ok(FramedWrite::new(strong))
    }
}

impl FramedWrite<Writer> {
    /// Downgrade the inner strong writer to a weak writer.
    pub fn downgrade(self) -> FramedWrite<WeakWriter> {
        let weak = self.inner.downgrade();
        FramedWrite::new(weak)
    }
}
