use crate::error::{Error, Result};

/// A frame header stored at the start of each message in a ring buffer.
///
/// Layout: [len: u32 little-endian] [tag: u32 little-endian] [flags: u8] [_reserved: [u8; 3]] = 12 bytes
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Payload length in bytes (not including the header).
    pub len: u32,
    /// Semantic tag: `writer_id` in pub/sub contexts, `correlation_id` in RPC contexts.
    pub tag: u32,
    /// Flags for frame metadata.
    pub flags: u8,
    /// Reserved padding for alignment.
    pub _reserved: [u8; 3],
}

impl FrameHeader {
    /// Total encoded header size in bytes.
    pub const ENCODED_SIZE: usize = 12;
    /// Frame flag set once the payload bytes are fully written.
    pub const FLAG_READY: u8 = 1;
    /// Frame flag set when a writer abandons a reserved span.
    pub const FLAG_ABORTED: u8 = 1 << 1;

    /// Encodes the header to a byte array.
    pub fn encode(&self) -> [u8; 12] {
        let mut bytes = [0u8; 12];
        bytes[..4].copy_from_slice(&self.len.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.tag.to_le_bytes());
        bytes[8] = self.flags;
        bytes[9..12].copy_from_slice(&self._reserved);
        bytes
    }

    /// Decodes a header from a byte array.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::ENCODED_SIZE {
            return Err(Error::InvalidFrame);
        }
        let len = u32::from_le_bytes(
            bytes
                .get(..4)
                .ok_or(Error::InvalidFrame)?
                .try_into()
                .map_err(|_invalid_layout| Error::InvalidFrame)?,
        );
        let tag = u32::from_le_bytes(
            bytes
                .get(4..8)
                .ok_or(Error::InvalidFrame)?
                .try_into()
                .map_err(|_invalid_layout| Error::InvalidFrame)?,
        );
        let flags = bytes
            .get(8)
            .copied()
            .ok_or(Error::InvalidFrame)?;
        let _reserved = bytes
            .get(9..12)
            .ok_or(Error::InvalidFrame)?
            .try_into()
            .map_err(|_invalid_layout| Error::InvalidFrame)?;
        Ok(Self {
            len,
            tag,
            flags,
            _reserved,
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
            tag: 42,
            flags: 1,
            _reserved: [0; 3],
        };
        let encoded = header.encode();
        let decoded = FrameHeader::decode(&encoded).unwrap();
        assert_eq!(decoded, header);
    }

    #[test]
    #[expect(
        clippy::assertions_on_result_states,
        reason = "unwrap_used lint conflicts with clippy's suggested fix"
    )]
    fn header_requires_twelve_bytes() {
        assert!(FrameHeader::decode(&[0; 11]).is_err());
        assert!(FrameHeader::decode(&[0; 12]).is_ok());
    }

    #[test]
    fn frame_size_includes_header() {
        let header = FrameHeader {
            len: 100,
            tag: 0,
            flags: 0,
            _reserved: [0; 3],
        };
        assert_eq!(header.frame_size(), 112);
    }

    #[test]
    fn flags_report_ready_and_aborted_state() {
        let header = FrameHeader {
            len: 0,
            tag: 0,
            flags: FrameHeader::FLAG_READY | FrameHeader::FLAG_ABORTED,
            _reserved: [0; 3],
        };

        assert!(header.is_ready());
        assert!(header.is_aborted());
    }
}
