use crate::error::{Error, Result};

/// A frame header stored at the start of each message in a ring buffer.
///
/// Layout: [len: u32 little-endian] [flags: u16] [writer_id: u16] = 8 bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Payload length in bytes (not including the header).
    pub len: u32,
    /// Flags for frame metadata.
    pub flags: u16,
    /// Identifier of the writer that produced this frame.
    pub writer_id: u16,
}

impl FrameHeader {
    /// Total encoded header size in bytes.
    pub const ENCODED_SIZE: usize = 8;
    /// Frame flag set once the payload bytes are fully written.
    pub const FLAG_READY: u16 = 1;
    /// Frame flag set when a writer abandons a reserved span.
    pub const FLAG_ABORTED: u16 = 1 << 1;

    /// Encodes the header to a byte array.
    pub fn encode(&self) -> [u8; 8] {
        let mut bytes = [0u8; 8];
        bytes[..4].copy_from_slice(&self.len.to_le_bytes());
        bytes[4..6].copy_from_slice(&self.flags.to_le_bytes());
        bytes[6..8].copy_from_slice(&self.writer_id.to_le_bytes());
        bytes
    }

    /// Decodes a header from a byte array.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::ENCODED_SIZE {
            return Err(Error::InvalidFrame);
        }
        let len = u32::from_le_bytes(bytes[..4].try_into().unwrap());
        let flags = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
        let writer_id = u16::from_le_bytes(bytes[6..8].try_into().unwrap());
        Ok(Self {
            len,
            flags,
            writer_id,
        })
    }

    /// Returns the total frame size including the header.
    pub fn frame_size(&self) -> u64 {
        Self::ENCODED_SIZE as u64 + self.len as u64
    }

    /// Returns whether this frame has been fully published.
    pub fn is_ready(&self) -> bool {
        self.flags & Self::FLAG_READY != 0
    }

    /// Returns whether this frame represents an abandoned reservation.
    pub fn is_aborted(&self) -> bool {
        self.flags & Self::FLAG_ABORTED != 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_encodes_and_decodes() {
        let header = FrameHeader {
            len: 1024,
            flags: 1,
            writer_id: 42,
        };
        let encoded = header.encode();
        let decoded = FrameHeader::decode(&encoded).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    fn header_requires_eight_bytes() {
        assert!(FrameHeader::decode(&[0; 7]).is_err());
        assert!(FrameHeader::decode(&[0; 8]).is_ok());
    }

    #[test]
    fn frame_size_includes_header() {
        let header = FrameHeader {
            len: 100,
            flags: 0,
            writer_id: 0,
        };
        assert_eq!(header.frame_size(), 108);
    }

    #[test]
    fn flags_report_ready_and_aborted_state() {
        let header = FrameHeader {
            len: 0,
            flags: FrameHeader::FLAG_READY | FrameHeader::FLAG_ABORTED,
            writer_id: 0,
        };

        assert!(header.is_ready());
        assert!(header.is_aborted());
    }
}
