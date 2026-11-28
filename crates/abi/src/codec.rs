//! rkyv encoding, framing, and hostcall status packing.

use crate::{
    AbiError, AbiErrorCode, HOSTCALL_MAGIC, HOSTCALL_PREAMBLE_SIZE, HostcallEnvelope,
    HostcallPreambleError, RkyvError, SELIUM_ABI_VERSION,
};
use rkyv::{
    Archive, Deserialize, Serialize,
    api::high::{HighDeserializer, HighSerializer, HighValidator},
    rancor::Error as RancorError,
    ser::allocator::ArenaHandle,
    util::AlignedVec,
};

/// Marker trait for values that can be encoded with Selium's rkyv codec.
pub trait RkyvEncode:
    Archive + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>
{
}

impl<T> RkyvEncode for T where
    T: Archive + for<'a> Serialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RancorError>>
{
}

/// Encodes a hostcall request with the fixed ABI magic/version preamble.
///
/// The preamble precedes the rkyv envelope so the host can validate
/// compatibility before attempting to interpret enum variant layouts.
pub fn encode_hostcall_request(envelope: &HostcallEnvelope) -> Result<Vec<u8>, RkyvError> {
    let payload = encode_rkyv(envelope)?;
    let mut encoded = Vec::with_capacity(HOSTCALL_PREAMBLE_SIZE + payload.len());
    encoded.extend_from_slice(&HOSTCALL_MAGIC.to_le_bytes());
    encoded.extend_from_slice(&SELIUM_ABI_VERSION.to_le_bytes());
    encoded.extend_from_slice(&payload);
    Ok(encoded)
}

/// Validates a hostcall request preamble, returning the rkyv payload.
///
/// Missing preambles, wrong magic, and mismatched ABI versions are rejected
/// before any rkyv decoding; a version mismatch reports both versions.
pub fn hostcall_request_payload(bytes: &[u8]) -> Result<&[u8], HostcallPreambleError> {
    let magic_bytes = bytes
        .get(..4)
        .ok_or(HostcallPreambleError::Missing)?
        .try_into()
        .map_err(|_error| HostcallPreambleError::Missing)?;
    let magic = u32::from_le_bytes(magic_bytes);
    if magic != HOSTCALL_MAGIC {
        return Err(HostcallPreambleError::BadMagic { found: magic });
    }
    let version_bytes = bytes
        .get(4..HOSTCALL_PREAMBLE_SIZE)
        .ok_or(HostcallPreambleError::Missing)?
        .try_into()
        .map_err(|_error| HostcallPreambleError::Missing)?;
    let guest = u32::from_le_bytes(version_bytes);
    if guest != SELIUM_ABI_VERSION {
        return Err(HostcallPreambleError::VersionMismatch {
            host: SELIUM_ABI_VERSION,
            guest,
        });
    }
    bytes
        .get(HOSTCALL_PREAMBLE_SIZE..)
        .ok_or(HostcallPreambleError::Missing)
}

/// Decodes an rkyv value from bytes after validation.
pub fn decode_rkyv<T>(bytes: &[u8]) -> Result<T, RkyvError>
where
    T: Archive + Sized,
    for<'a> T::Archived: Deserialize<T, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
{
    rkyv::from_bytes::<T, RancorError>(bytes).map_err(|error| RkyvError::Decode(error.to_string()))
}

/// Removes a length prefix from a framed payload and validates the frame length.
pub fn deframe_bytes(payload: &[u8]) -> Result<&[u8], AbiError> {
    let prefix = payload.get(..4).ok_or_else(|| {
        AbiError::new(
            AbiErrorCode::MalformedPayload,
            "missing frame length prefix",
        )
    })?;
    let len = u32::from_le_bytes(prefix.try_into().map_err(|_error| {
        AbiError::new(
            AbiErrorCode::MalformedPayload,
            "invalid frame length prefix",
        )
    })?) as usize;
    let frame = payload.get(4..4 + len).ok_or_else(|| {
        AbiError::new(
            AbiErrorCode::MalformedPayload,
            "frame length exceeds buffer",
        )
    })?;
    if payload.len() != len + 4 {
        return Err(AbiError::new(
            AbiErrorCode::MalformedPayload,
            "frame contains trailing bytes",
        ));
    }
    Ok(frame)
}

/// Encodes a value to rkyv bytes.
pub fn encode_rkyv<T>(value: &T) -> Result<Vec<u8>, RkyvError>
where
    T: RkyvEncode,
{
    rkyv::to_bytes::<RancorError>(value)
        .map(|bytes| bytes.into_vec())
        .map_err(|error| RkyvError::Encode(error.to_string()))
}

/// Prefixes a payload with its little-endian `u32` length.
pub fn frame_bytes(payload: &[u8]) -> Result<Vec<u8>, AbiError> {
    let len = u32::try_from(payload.len()).map_err(|_error| {
        AbiError::new(
            AbiErrorCode::MalformedPayload,
            "frame payload length exceeds u32",
        )
    })?;
    let mut framed = Vec::with_capacity(payload.len() + 4);
    framed.extend_from_slice(&len.to_le_bytes());
    framed.extend_from_slice(payload);
    Ok(framed)
}

/// Packs a hostcall status and value into one `u64` ABI return value.
pub fn pack_hostcall_status(status: u32, value: u32) -> u64 {
    ((status as u64) << 32) | value as u64
}

/// Unpacks a hostcall status and value from one `u64` ABI return value.
pub fn unpack_hostcall_status(encoded: u64) -> (u32, u32) {
    ((encoded >> 32) as u32, encoded as u32)
}
