use std::collections::BTreeMap;

use thiserror::Error;

use crate::DataValue;

pub const CONTRACT_CODEC_MAJOR_VERSION: u8 = 1;
const CONTRACT_CODEC_HEADER: [u8; 4] = [b'S', b'C', b'C', CONTRACT_CODEC_MAJOR_VERSION];

#[derive(Debug, Error)]
pub enum ContractCodecError {
    #[error("canonical contract codec encode failed: {0}")]
    Encode(String),
    #[error("canonical contract codec decode failed: {0}")]
    Decode(String),
}

pub trait CanonicalSerialize {
    fn encode_to(&self, encoder: &mut CanonicalEncoder) -> Result<(), ContractCodecError>;
}

pub trait CanonicalDeserialize: Sized {
    fn decode_from(decoder: &mut CanonicalDecoder<'_>) -> Result<Self, ContractCodecError>;
}

pub struct CanonicalEncoder {
    bytes: Vec<u8>,
}

pub struct CanonicalDecoder<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

pub fn encode_canonical<T>(value: &T) -> Result<Vec<u8>, ContractCodecError>
where
    T: CanonicalSerialize + ?Sized,
{
    let mut encoder = CanonicalEncoder::new();
    value.encode_to(&mut encoder)?;
    Ok(encoder.finish())
}

pub fn decode_canonical<T>(bytes: &[u8]) -> Result<T, ContractCodecError>
where
    T: CanonicalDeserialize,
{
    let payload = bytes
        .strip_prefix(&CONTRACT_CODEC_HEADER)
        .ok_or_else(|| ContractCodecError::Decode("missing canonical codec header".to_string()))?;
    let mut decoder = CanonicalDecoder::new(payload);
    let value = T::decode_from(&mut decoder)?;
    decoder.finish()?;
    Ok(value)
}

impl CanonicalEncoder {
    fn new() -> Self {
        let mut bytes = Vec::with_capacity(CONTRACT_CODEC_HEADER.len());
        bytes.extend_from_slice(&CONTRACT_CODEC_HEADER);
        Self { bytes }
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }

    pub fn encode_value<T>(&mut self, value: &T) -> Result<(), ContractCodecError>
    where
        T: CanonicalSerialize + ?Sized,
    {
        value.encode_to(self)
    }

    pub fn write_u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    pub fn write_u16(&mut self, value: u16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_u32(&mut self, value: u32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_u64(&mut self, value: u64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_i8(&mut self, value: i8) {
        self.write_u8(value as u8);
    }

    pub fn write_i16(&mut self, value: i16) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_i32(&mut self, value: i32) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_i64(&mut self, value: i64) {
        self.bytes.extend_from_slice(&value.to_be_bytes());
    }

    pub fn write_bytes(&mut self, value: &[u8]) -> Result<(), ContractCodecError> {
        let len = u32::try_from(value.len()).map_err(|_| {
            ContractCodecError::Encode("byte sequence length exceeds u32".to_string())
        })?;
        self.write_u32(len);
        self.bytes.extend_from_slice(value);
        Ok(())
    }

    pub fn write_string(&mut self, value: &str) -> Result<(), ContractCodecError> {
        self.write_bytes(value.as_bytes())
    }
}

impl<'a> CanonicalDecoder<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn finish(&self) -> Result<(), ContractCodecError> {
        if self.cursor == self.bytes.len() {
            Ok(())
        } else {
            Err(ContractCodecError::Decode(
                "trailing bytes remain after canonical decode".to_string(),
            ))
        }
    }

    pub fn decode_value<T>(&mut self) -> Result<T, ContractCodecError>
    where
        T: CanonicalDeserialize,
    {
        T::decode_from(self)
    }

    pub fn read_u8(&mut self) -> Result<u8, ContractCodecError> {
        let byte = *self.bytes.get(self.cursor).ok_or_else(|| {
            ContractCodecError::Decode("unexpected EOF while reading u8".to_string())
        })?;
        self.cursor += 1;
        Ok(byte)
    }

    pub fn read_u16(&mut self) -> Result<u16, ContractCodecError> {
        Ok(u16::from_be_bytes(self.read_array::<2>()?))
    }

    pub fn read_u32(&mut self) -> Result<u32, ContractCodecError> {
        Ok(u32::from_be_bytes(self.read_array::<4>()?))
    }

    pub fn read_u64(&mut self) -> Result<u64, ContractCodecError> {
        Ok(u64::from_be_bytes(self.read_array::<8>()?))
    }

    pub fn read_i8(&mut self) -> Result<i8, ContractCodecError> {
        Ok(self.read_u8()? as i8)
    }

    pub fn read_i16(&mut self) -> Result<i16, ContractCodecError> {
        Ok(i16::from_be_bytes(self.read_array::<2>()?))
    }

    pub fn read_i32(&mut self) -> Result<i32, ContractCodecError> {
        Ok(i32::from_be_bytes(self.read_array::<4>()?))
    }

    pub fn read_i64(&mut self) -> Result<i64, ContractCodecError> {
        Ok(i64::from_be_bytes(self.read_array::<8>()?))
    }

    pub fn read_bytes(&mut self) -> Result<Vec<u8>, ContractCodecError> {
        let len = usize::try_from(self.read_u32()?).map_err(|_| {
            ContractCodecError::Decode("byte sequence length does not fit usize".to_string())
        })?;
        Ok(self.read_exact(len)?.to_vec())
    }

    pub fn read_string(&mut self) -> Result<String, ContractCodecError> {
        String::from_utf8(self.read_bytes()?)
            .map_err(|err| ContractCodecError::Decode(format!("invalid UTF-8 string: {err}")))
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ContractCodecError> {
        self.read_exact(N)?
            .try_into()
            .map_err(|_| ContractCodecError::Decode(format!("failed to read {N} bytes")))
    }

    fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ContractCodecError> {
        let end = self.cursor.checked_add(len).ok_or_else(|| {
            ContractCodecError::Decode("byte cursor overflowed during decode".to_string())
        })?;
        let slice = self.bytes.get(self.cursor..end).ok_or_else(|| {
            ContractCodecError::Decode(format!("unexpected EOF while reading {len} bytes"))
        })?;
        self.cursor = end;
        Ok(slice)
    }
}

impl CanonicalSerialize for bool {
    fn encode_to(&self, encoder: &mut CanonicalEncoder) -> Result<(), ContractCodecError> {
        encoder.write_u8(u8::from(*self));
        Ok(())
    }
}

impl CanonicalDeserialize for bool {
    fn decode_from(decoder: &mut CanonicalDecoder<'_>) -> Result<Self, ContractCodecError> {
        match decoder.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(ContractCodecError::Decode(format!(
                "invalid boolean tag {value}"
            ))),
        }
    }
}

macro_rules! impl_canonical_scalar {
    ($ty:ty, $write:ident, $read:ident) => {
        impl CanonicalSerialize for $ty {
            fn encode_to(&self, encoder: &mut CanonicalEncoder) -> Result<(), ContractCodecError> {
                encoder.$write(*self);
                Ok(())
            }
        }

        impl CanonicalDeserialize for $ty {
            fn decode_from(decoder: &mut CanonicalDecoder<'_>) -> Result<Self, ContractCodecError> {
                decoder.$read()
            }
        }
    };
}

impl_canonical_scalar!(u8, write_u8, read_u8);
impl_canonical_scalar!(u16, write_u16, read_u16);
impl_canonical_scalar!(u32, write_u32, read_u32);
impl_canonical_scalar!(u64, write_u64, read_u64);
impl_canonical_scalar!(i8, write_i8, read_i8);
impl_canonical_scalar!(i16, write_i16, read_i16);
impl_canonical_scalar!(i32, write_i32, read_i32);
impl_canonical_scalar!(i64, write_i64, read_i64);

impl CanonicalSerialize for f32 {
    fn encode_to(&self, encoder: &mut CanonicalEncoder) -> Result<(), ContractCodecError> {
        encoder.write_u32(self.to_bits());
        Ok(())
    }
}

impl CanonicalDeserialize for f32 {
    fn decode_from(decoder: &mut CanonicalDecoder<'_>) -> Result<Self, ContractCodecError> {
        Ok(Self::from_bits(decoder.read_u32()?))
    }
}

impl CanonicalSerialize for f64 {
    fn encode_to(&self, encoder: &mut CanonicalEncoder) -> Result<(), ContractCodecError> {
        encoder.write_u64(self.to_bits());
        Ok(())
    }
}

impl CanonicalDeserialize for f64 {
    fn decode_from(decoder: &mut CanonicalDecoder<'_>) -> Result<Self, ContractCodecError> {
        Ok(Self::from_bits(decoder.read_u64()?))
    }
}

impl CanonicalSerialize for String {
    fn encode_to(&self, encoder: &mut CanonicalEncoder) -> Result<(), ContractCodecError> {
        encoder.write_string(self)
    }
}

impl CanonicalSerialize for str {
    fn encode_to(&self, encoder: &mut CanonicalEncoder) -> Result<(), ContractCodecError> {
        encoder.write_string(self)
    }
}

impl CanonicalDeserialize for String {
    fn decode_from(decoder: &mut CanonicalDecoder<'_>) -> Result<Self, ContractCodecError> {
        decoder.read_string()
    }
}

impl CanonicalSerialize for Vec<u8> {
    fn encode_to(&self, encoder: &mut CanonicalEncoder) -> Result<(), ContractCodecError> {
        encoder.write_bytes(self)
    }
}

impl CanonicalSerialize for [u8] {
    fn encode_to(&self, encoder: &mut CanonicalEncoder) -> Result<(), ContractCodecError> {
        encoder.write_bytes(self)
    }
}

impl CanonicalDeserialize for Vec<u8> {
    fn decode_from(decoder: &mut CanonicalDecoder<'_>) -> Result<Self, ContractCodecError> {
        decoder.read_bytes()
    }
}

impl CanonicalSerialize for DataValue {
    fn encode_to(&self, encoder: &mut CanonicalEncoder) -> Result<(), ContractCodecError> {
        match self {
            DataValue::Null => encoder.write_u8(0),
            DataValue::Bool(value) => {
                encoder.write_u8(1);
                encoder.encode_value(value)?;
            }
            DataValue::U64(value) => {
                encoder.write_u8(2);
                encoder.encode_value(value)?;
            }
            DataValue::I64(value) => {
                encoder.write_u8(3);
                encoder.encode_value(value)?;
            }
            DataValue::String(value) => {
                encoder.write_u8(4);
                encoder.encode_value(value)?;
            }
            DataValue::Bytes(value) => {
                encoder.write_u8(5);
                encoder.encode_value(value)?;
            }
            DataValue::List(values) => {
                encoder.write_u8(6);
                encoder.write_u32(u32::try_from(values.len()).map_err(|_| {
                    ContractCodecError::Encode("list length exceeds u32".to_string())
                })?);
                for value in values {
                    encoder.encode_value(value)?;
                }
            }
            DataValue::Map(values) => {
                encoder.write_u8(7);
                encoder.write_u32(u32::try_from(values.len()).map_err(|_| {
                    ContractCodecError::Encode("map length exceeds u32".to_string())
                })?);
                for (key, value) in values {
                    encoder.encode_value(key)?;
                    encoder.encode_value(value)?;
                }
            }
        }
        Ok(())
    }
}

impl CanonicalDeserialize for DataValue {
    fn decode_from(decoder: &mut CanonicalDecoder<'_>) -> Result<Self, ContractCodecError> {
        match decoder.read_u8()? {
            0 => Ok(DataValue::Null),
            1 => Ok(DataValue::Bool(decoder.decode_value()?)),
            2 => Ok(DataValue::U64(decoder.decode_value()?)),
            3 => Ok(DataValue::I64(decoder.decode_value()?)),
            4 => Ok(DataValue::String(decoder.decode_value()?)),
            5 => Ok(DataValue::Bytes(decoder.decode_value()?)),
            6 => {
                let len = usize::try_from(decoder.read_u32()?).map_err(|_| {
                    ContractCodecError::Decode("list length does not fit usize".to_string())
                })?;
                let mut values = Vec::with_capacity(len);
                for _ in 0..len {
                    values.push(decoder.decode_value()?);
                }
                Ok(DataValue::List(values))
            }
            7 => {
                let len = usize::try_from(decoder.read_u32()?).map_err(|_| {
                    ContractCodecError::Decode("map length does not fit usize".to_string())
                })?;
                let mut values = BTreeMap::new();
                for _ in 0..len {
                    let key: String = decoder.decode_value()?;
                    let value: DataValue = decoder.decode_value()?;
                    values.insert(key, value);
                }
                Ok(DataValue::Map(values))
            }
            tag => Err(ContractCodecError::Decode(format!(
                "unknown DataValue tag {tag}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CONTRACT_CODEC_HEADER, CanonicalDecoder, CanonicalDeserialize, CanonicalEncoder,
        CanonicalSerialize, ContractCodecError, decode_canonical, encode_canonical,
    };
    use crate::DataValue;
    use std::collections::BTreeMap;

    use selium_control_plane_api::{
        CanonicalCodec as SharedCanonicalCodec, FieldDef, FieldPresence, FixtureSchema, TypeDef,
        TypeRef, Value, VariantDef, canonical_contract_fixture_suite, run_fixture_suite,
    };

    #[derive(Debug, PartialEq)]
    struct SampleRecord {
        topic: String,
        version: u32,
    }

    impl CanonicalSerialize for SampleRecord {
        fn encode_to(&self, encoder: &mut CanonicalEncoder) -> Result<(), ContractCodecError> {
            encoder.encode_value(&self.topic)?;
            encoder.encode_value(&self.version)?;
            Ok(())
        }
    }

    impl CanonicalDeserialize for SampleRecord {
        fn decode_from(decoder: &mut CanonicalDecoder<'_>) -> Result<Self, ContractCodecError> {
            Ok(Self {
                topic: decoder.decode_value()?,
                version: decoder.decode_value()?,
            })
        }
    }

    #[test]
    fn round_trips_structured_values() {
        let bytes = encode_canonical(&SampleRecord {
            topic: "inventory".to_string(),
            version: 7,
        })
        .expect("encode canonical record");

        let decoded = decode_canonical::<SampleRecord>(&bytes).expect("decode canonical record");

        assert_eq!(
            decoded,
            SampleRecord {
                topic: "inventory".to_string(),
                version: 7,
            }
        );
    }

    #[test]
    fn round_trips_nested_data_values() {
        let value = DataValue::Map(BTreeMap::from([(
            "bindings".to_string(),
            DataValue::List(vec![
                DataValue::from(true),
                DataValue::from(42_u64),
                DataValue::from("hello"),
            ]),
        )]));

        let bytes = encode_canonical(&value).expect("encode data value");
        let decoded = decode_canonical::<DataValue>(&bytes).expect("decode data value");

        assert_eq!(decoded, value);
    }

    #[test]
    fn rejects_payload_without_canonical_header() {
        let err = decode_canonical::<String>(b"not-canonical").expect_err("missing header");
        assert!(matches!(err, ContractCodecError::Decode(message) if message.contains("header")));
    }

    #[derive(Debug, Clone, Copy, Default)]
    struct ProductionCodecAdapter;

    impl SharedCanonicalCodec for ProductionCodecAdapter {
        fn encode(
            &self,
            schema: &FixtureSchema,
            root: &TypeRef,
            value: &Value,
        ) -> Result<Vec<u8>, String> {
            encode_fixture_value(schema, root, value)
        }

        fn decode(
            &self,
            schema: &FixtureSchema,
            root: &TypeRef,
            bytes: &[u8],
        ) -> Result<Value, String> {
            decode_fixture_value(schema, root, bytes)
        }
    }

    #[test]
    fn production_codec_passes_shared_fixture_suite() {
        let report =
            run_fixture_suite(&ProductionCodecAdapter, &canonical_contract_fixture_suite());
        report
            .into_result()
            .expect("production rust codec should pass shared fixture suite");
    }

    fn encode_fixture_value(
        schema: &FixtureSchema,
        root: &TypeRef,
        value: &Value,
    ) -> Result<Vec<u8>, String> {
        let mut encoder = CanonicalEncoder::new();
        encode_into(&mut encoder, schema, root, value)?;
        Ok(strip_header(&encoder.finish()))
    }

    fn encode_into(
        encoder: &mut CanonicalEncoder,
        schema: &FixtureSchema,
        root: &TypeRef,
        value: &Value,
    ) -> Result<(), String> {
        match root {
            TypeRef::Bool => match value {
                Value::Bool(value) => {
                    encoder.write_u8(u8::from(*value));
                    Ok(())
                }
                other => Err(format!("expected bool, got {other:?}")),
            },
            TypeRef::U64 => match value {
                Value::U64(value) => {
                    write_varint(encoder, *value);
                    Ok(())
                }
                other => Err(format!("expected u64, got {other:?}")),
            },
            TypeRef::String => match value {
                Value::String(value) => {
                    encoder.bytes.extend_from_slice(value.as_bytes());
                    Ok(())
                }
                other => Err(format!("expected string, got {other:?}")),
            },
            TypeRef::Bytes => match value {
                Value::Bytes(value) => {
                    encoder.bytes.extend_from_slice(value);
                    Ok(())
                }
                other => Err(format!("expected bytes, got {other:?}")),
            },
            TypeRef::List(item) => match value {
                Value::List(items) => {
                    write_varint(
                        encoder,
                        u64::try_from(items.len())
                            .map_err(|_| "list length exceeds u64".to_string())?,
                    );
                    for item_value in items {
                        let item_bytes = encode_fixture_value(schema, item, item_value)?;
                        write_len_delimited(encoder, &item_bytes)?;
                    }
                    Ok(())
                }
                other => Err(format!("expected list, got {other:?}")),
            },
            TypeRef::Map(key, value_type) => match value {
                Value::Map(entries) => {
                    let mut encoded_entries = Vec::with_capacity(entries.len());
                    for (entry_key, entry_value) in entries {
                        let key_bytes = encode_fixture_value(schema, key, entry_key)?;
                        let value_bytes = encode_fixture_value(schema, value_type, entry_value)?;
                        encoded_entries.push((key_bytes, value_bytes));
                    }
                    encoded_entries.sort_by(|left, right| left.0.cmp(&right.0));
                    for pair in encoded_entries.windows(2) {
                        if pair[0].0 == pair[1].0 {
                            return Err("duplicate map key".to_string());
                        }
                    }
                    write_varint(
                        encoder,
                        u64::try_from(encoded_entries.len())
                            .map_err(|_| "map length exceeds u64".to_string())?,
                    );
                    for (key_bytes, value_bytes) in encoded_entries {
                        write_len_delimited(encoder, &key_bytes)?;
                        write_len_delimited(encoder, &value_bytes)?;
                    }
                    Ok(())
                }
                other => Err(format!("expected map, got {other:?}")),
            },
            TypeRef::Named(name) => match schema.types.get(name) {
                Some(TypeDef::Record { fields }) => encode_record(encoder, schema, fields, value),
                Some(TypeDef::Enum { variants }) => encode_enum(encoder, schema, variants, value),
                None => Err(format!("unknown type `{name}` in schema `{}`", schema.name)),
            },
        }
    }

    fn encode_record(
        encoder: &mut CanonicalEncoder,
        schema: &FixtureSchema,
        fields: &[FieldDef],
        value: &Value,
    ) -> Result<(), String> {
        let Value::Record(record) = value else {
            return Err(format!("expected record, got {value:?}"));
        };

        let mut encoded_fields = Vec::new();
        for field in fields {
            match record.get(&field.name) {
                Some(field_value) => {
                    let field_value = normalize_field_value(field, field_value);
                    if matches!(&field.presence, FieldPresence::Defaulted(default) if default == field_value)
                    {
                        continue;
                    }
                    let bytes = encode_fixture_value(schema, &field.ty, field_value)?;
                    encoded_fields.push((field.id, bytes));
                }
                None => match &field.presence {
                    FieldPresence::Required => {
                        return Err(format!("missing required field `{}`", field.name));
                    }
                    FieldPresence::Optional | FieldPresence::Defaulted(_) => {}
                },
            }
        }

        for key in record.keys() {
            if !fields.iter().any(|field| field.name == *key) {
                return Err(format!("unknown field `{key}`"));
            }
        }

        encoded_fields.sort_by_key(|field| field.0);
        write_varint(
            encoder,
            u64::try_from(encoded_fields.len())
                .map_err(|_| "field count exceeds u64".to_string())?,
        );
        for (field_id, bytes) in encoded_fields {
            write_varint(encoder, u64::from(field_id));
            write_len_delimited(encoder, &bytes)?;
        }
        Ok(())
    }

    fn encode_enum(
        encoder: &mut CanonicalEncoder,
        schema: &FixtureSchema,
        variants: &[VariantDef],
        value: &Value,
    ) -> Result<(), String> {
        let Value::Enum { variant, value } = value else {
            return Err(format!("expected enum, got {value:?}"));
        };

        let variant_def = variants
            .iter()
            .find(|candidate| candidate.name == *variant)
            .ok_or_else(|| format!("unknown variant `{variant}`"))?;

        let payload = match (&variant_def.payload, value) {
            (Some(ty), Some(value)) => encode_fixture_value(schema, ty, value)?,
            (Some(_), None) => return Err(format!("variant `{variant}` requires payload")),
            (None, Some(_)) => return Err(format!("variant `{variant}` must not carry payload")),
            (None, None) => Vec::new(),
        };

        write_varint(encoder, u64::from(variant_def.id));
        write_len_delimited(encoder, &payload)
    }

    fn decode_fixture_value(
        schema: &FixtureSchema,
        root: &TypeRef,
        bytes: &[u8],
    ) -> Result<Value, String> {
        let mut decoder = CanonicalDecoder::new(bytes);
        let value = decode_from(&mut decoder, schema, root)?;
        decoder.finish().map_err(|err| err.to_string())?;
        Ok(value)
    }

    fn decode_from(
        decoder: &mut CanonicalDecoder<'_>,
        schema: &FixtureSchema,
        root: &TypeRef,
    ) -> Result<Value, String> {
        match root {
            TypeRef::Bool => match decoder.read_u8().map_err(|err| err.to_string())? {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                value => Err(format!("invalid boolean tag {value}")),
            },
            TypeRef::U64 => read_varint(decoder).map(Value::U64),
            TypeRef::String => read_remaining(decoder)
                .and_then(|bytes| String::from_utf8(bytes).map_err(|err| err.to_string()))
                .map(Value::String),
            TypeRef::Bytes => read_remaining(decoder).map(Value::Bytes),
            TypeRef::List(item) => {
                let count = usize::try_from(read_varint(decoder)?)
                    .map_err(|_| "list length does not fit usize".to_string())?;
                let mut items = Vec::with_capacity(count);
                for _ in 0..count {
                    let item_bytes = read_len_delimited(decoder)?;
                    items.push(decode_fixture_value(schema, item, &item_bytes)?);
                }
                Ok(Value::List(items))
            }
            TypeRef::Map(key, value_type) => {
                let count = usize::try_from(read_varint(decoder)?)
                    .map_err(|_| "map length does not fit usize".to_string())?;
                let mut entries = Vec::with_capacity(count);
                for _ in 0..count {
                    let key_bytes = read_len_delimited(decoder)?;
                    let value_bytes = read_len_delimited(decoder)?;
                    entries.push((
                        decode_fixture_value(schema, key, &key_bytes)?,
                        decode_fixture_value(schema, value_type, &value_bytes)?,
                    ));
                }
                Ok(Value::Map(entries))
            }
            TypeRef::Named(name) => match schema.types.get(name) {
                Some(TypeDef::Record { fields }) => decode_record(decoder, schema, fields),
                Some(TypeDef::Enum { variants }) => decode_enum(decoder, schema, variants),
                None => Err(format!("unknown type `{name}` in schema `{}`", schema.name)),
            },
        }
    }

    fn decode_record(
        decoder: &mut CanonicalDecoder<'_>,
        schema: &FixtureSchema,
        fields: &[FieldDef],
    ) -> Result<Value, String> {
        let count = usize::try_from(read_varint(decoder)?)
            .map_err(|_| "field count does not fit usize".to_string())?;
        let field_by_id = fields
            .iter()
            .map(|field| (field.id, field))
            .collect::<BTreeMap<_, _>>();
        let mut seen_ids = BTreeMap::new();
        let mut values = BTreeMap::new();

        for _ in 0..count {
            let field_id = read_varint(decoder)?;
            let field_bytes = read_len_delimited(decoder)?;

            if seen_ids.insert(field_id, ()).is_some() {
                return Err(format!("duplicate field id {field_id}"));
            }

            let Some(field) = field_by_id.get(&(field_id as u32)) else {
                continue;
            };
            let decoded = decode_fixture_value(schema, &field.ty, &field_bytes)?;
            values.insert(field.name.clone(), decoded);
        }

        for field in fields {
            if values.contains_key(&field.name) {
                continue;
            }
            match &field.presence {
                FieldPresence::Required => {
                    return Err(format!("missing required field `{}`", field.name));
                }
                FieldPresence::Optional => {}
                FieldPresence::Defaulted(default) => {
                    values.insert(field.name.clone(), default.clone());
                }
            }
        }

        Ok(Value::Record(values))
    }

    fn decode_enum(
        decoder: &mut CanonicalDecoder<'_>,
        schema: &FixtureSchema,
        variants: &[VariantDef],
    ) -> Result<Value, String> {
        let variant_id = read_varint(decoder)?;
        let payload = read_len_delimited(decoder)?;
        let variant = variants
            .iter()
            .find(|candidate| candidate.id == variant_id as u32)
            .ok_or_else(|| format!("unknown variant id {variant_id}"))?;
        let value = match &variant.payload {
            Some(ty) => Some(Box::new(decode_fixture_value(schema, ty, &payload)?)),
            None if payload.is_empty() => None,
            None => return Err(format!("variant `{}` must not carry payload", variant.name)),
        };
        Ok(Value::Enum {
            variant: variant.name.clone(),
            value,
        })
    }

    fn normalize_field_value<'a>(field: &'a FieldDef, value: &'a Value) -> &'a Value {
        match (&field.presence, value) {
            (FieldPresence::Defaulted(default), candidate) if default == candidate => default,
            _ => value,
        }
    }

    fn strip_header(bytes: &[u8]) -> Vec<u8> {
        bytes
            .strip_prefix(&CONTRACT_CODEC_HEADER)
            .expect("canonical header prefix")
            .to_vec()
    }

    fn write_len_delimited(encoder: &mut CanonicalEncoder, bytes: &[u8]) -> Result<(), String> {
        write_varint(
            encoder,
            u64::try_from(bytes.len()).map_err(|_| "payload length exceeds u64".to_string())?,
        );
        encoder.bytes.extend_from_slice(bytes);
        Ok(())
    }

    fn read_len_delimited(decoder: &mut CanonicalDecoder<'_>) -> Result<Vec<u8>, String> {
        let len = usize::try_from(read_varint(decoder)?)
            .map_err(|_| "payload length does not fit usize".to_string())?;
        decoder
            .read_exact(len)
            .map(|bytes| bytes.to_vec())
            .map_err(|err| err.to_string())
    }

    fn read_remaining(decoder: &mut CanonicalDecoder<'_>) -> Result<Vec<u8>, String> {
        let remaining = decoder
            .bytes
            .len()
            .checked_sub(decoder.cursor)
            .ok_or_else(|| "decoder cursor overflowed".to_string())?;
        decoder
            .read_exact(remaining)
            .map(|bytes| bytes.to_vec())
            .map_err(|err| err.to_string())
    }

    fn write_varint(encoder: &mut CanonicalEncoder, mut value: u64) {
        loop {
            let mut byte = (value & 0x7f) as u8;
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            encoder.write_u8(byte);
            if value == 0 {
                break;
            }
        }
    }

    fn read_varint(decoder: &mut CanonicalDecoder<'_>) -> Result<u64, String> {
        let mut shift = 0;
        let mut value = 0u64;

        loop {
            let byte = decoder.read_u8().map_err(|err| err.to_string())?;
            let byte_value = (byte & 0x7f) as u64;
            value |= byte_value << shift;
            if byte & 0x80 == 0 {
                return Ok(value);
            }
            shift += 7;
            if shift >= 64 {
                return Err("varint exceeds 64 bits".to_string());
            }
        }
    }
}
