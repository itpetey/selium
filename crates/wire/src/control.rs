//! Typed per-stream control frames shared between the external client and the
//! bridge channel.
//!
//! Layered on the `selium-wire` codec: an rkyv payload carried as a normal
//! frame with tag 0 before data relay begins. The handshake is deterministic:
//! the bridge replies with exactly one control frame — [`PipeControl::Accepted`]
//! once the channel is resolved and attached, or [`PipeControl::Terminate`]
//! (followed by stream teardown) on refusal. Data frames are relayed verbatim
//! and are never decoded as control frames.

use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::{decode_rkyv, encode_rkyv};

/// Termination code for a malformed or missing client handshake.
pub const TERMINATE_BAD_HANDSHAKE: u32 = 1;
/// Termination code for a channel the bridge could not resolve or attach.
pub const TERMINATE_ATTACH_FAILED: u32 = 2;

/// Typed per-stream control frames.
///
/// Carried as a normal frame with tag 0 before data relay begins.
#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum PipeControl {
    /// A client request to bridge the given channel URI.
    Handshake {
        /// Discovery URI of the fabric channel to bridge.
        uri: String,
    },
    /// A success reply: the named channel is resolved and attached, and the
    /// data relay begins immediately after this frame.
    Accepted,
    /// A terminal failure reply; the stream closes after it.
    Terminate {
        /// Machine-readable termination code.
        code: u32,
    },
}

impl PipeControl {
    /// Encodes the control frame to its framed payload bytes.
    pub fn encode(&self) -> Vec<u8> {
        encode_rkyv(self).expect("encode PipeControl")
    }

    /// Decodes a control frame from framed payload bytes.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        decode_rkyv(bytes).ok()
    }
}
