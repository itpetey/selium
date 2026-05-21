use rkyv::{
    api::high::{HighDeserializer, HighValidator},
    rancor::Error as RancorError,
};
use selium_abi::{decode_rkyv, deframe_bytes, encode_rkyv, frame_bytes};

use crate::{Result, error::abi_error_to_guest_error};

/// Decodes a framed rkyv value received by a guest interface.
pub fn decode_typed<T>(bytes: &[u8]) -> Result<T>
where
    T: rkyv::Archive + Sized,
    for<'a> T::Archived: rkyv::Deserialize<T, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
{
    let payload = deframe_bytes(bytes).map_err(abi_error_to_guest_error)?;
    Ok(decode_rkyv(payload)?)
}

/// Encodes a value as framed rkyv bytes for a guest interface.
pub fn encode_typed<T>(value: &T) -> Result<Vec<u8>>
where
    T: selium_abi::RkyvEncode,
{
    frame_bytes(&encode_rkyv(value)?).map_err(abi_error_to_guest_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[rkyv(bytecheck())]
    struct DemoPayload {
        message: String,
    }

    #[test]
    fn typed_codec_round_trips() {
        let payload = DemoPayload {
            message: "hello".to_string(),
        };

        let encoded = encode_typed(&payload).expect("encode payload");
        let decoded: DemoPayload = decode_typed(&encoded).expect("decode payload");

        assert_eq!(decoded, payload);
    }
}
