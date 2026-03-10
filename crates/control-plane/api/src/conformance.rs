use std::{collections::BTreeMap, fmt};

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FixtureSchema {
    pub name: String,
    pub types: BTreeMap<String, TypeDef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeDef {
    Record { fields: Vec<FieldDef> },
    Enum { variants: Vec<VariantDef> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TypeRef {
    Bool,
    U64,
    String,
    Bytes,
    List(Box<TypeRef>),
    Map(Box<TypeRef>, Box<TypeRef>),
    Named(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldDef {
    pub name: String,
    pub id: u32,
    pub ty: TypeRef,
    pub presence: FieldPresence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldPresence {
    Required,
    Optional,
    Defaulted(Value),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VariantDef {
    pub name: String,
    pub id: u32,
    pub payload: Option<TypeRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Value {
    Bool(bool),
    U64(u64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<Value>),
    Map(Vec<(Value, Value)>),
    Record(BTreeMap<String, Value>),
    Enum {
        variant: String,
        value: Option<Box<Value>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoldenCase {
    pub name: &'static str,
    pub schema: FixtureSchema,
    pub root: TypeRef,
    pub value: Value,
    pub expected_bytes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeCase {
    pub name: &'static str,
    pub schema: FixtureSchema,
    pub root: TypeRef,
    pub bytes: Vec<u8>,
    pub expected_value: Value,
    pub expected_canonical_bytes: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureCase {
    pub name: &'static str,
    pub schema: FixtureSchema,
    pub root: TypeRef,
    pub bytes: Vec<u8>,
    pub expected_error: &'static str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SuiteCase {
    Golden(GoldenCase),
    Decode(DecodeCase),
    DecodeFailure(FailureCase),
}

pub trait CanonicalCodec {
    fn encode(
        &self,
        schema: &FixtureSchema,
        root: &TypeRef,
        value: &Value,
    ) -> Result<Vec<u8>, String>;

    fn decode(&self, schema: &FixtureSchema, root: &TypeRef, bytes: &[u8])
    -> Result<Value, String>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConformanceFailure {
    pub case: String,
    pub phase: String,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConformanceReport {
    pub passed: usize,
    pub failures: Vec<ConformanceFailure>,
}

impl ConformanceReport {
    pub fn succeeded(&self) -> bool {
        self.failures.is_empty()
    }

    pub fn into_result(self) -> Result<Self, ConformanceError> {
        if self.failures.is_empty() {
            Ok(self)
        } else {
            Err(ConformanceError {
                failures: self.failures,
            })
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("conformance failures:\n{0}")]
pub struct ConformanceErrorDisplay(String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConformanceError {
    pub failures: Vec<ConformanceFailure>,
}

impl fmt::Display for ConformanceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let summary = self
            .failures
            .iter()
            .map(|failure| format!("- {} [{}]: {}", failure.case, failure.phase, failure.detail))
            .collect::<Vec<_>>()
            .join("\n");
        ConformanceErrorDisplay(summary).fmt(f)
    }
}

impl std::error::Error for ConformanceError {}

pub fn run_fixture_suite(codec: &impl CanonicalCodec, cases: &[SuiteCase]) -> ConformanceReport {
    let mut report = ConformanceReport::default();
    let reference = ReferenceCodec;

    for case in cases {
        match case {
            SuiteCase::Golden(case) => {
                match codec.encode(&case.schema, &case.root, &case.value) {
                    Ok(actual) if actual == case.expected_bytes => {}
                    Ok(actual) => {
                        report.failures.push(ConformanceFailure {
                            case: case.name.to_string(),
                            phase: "encode".to_string(),
                            detail: format!(
                                "expected {}, got {}",
                                hex(&case.expected_bytes),
                                hex(&actual)
                            ),
                        });
                        continue;
                    }
                    Err(err) => {
                        report.failures.push(ConformanceFailure {
                            case: case.name.to_string(),
                            phase: "encode".to_string(),
                            detail: err,
                        });
                        continue;
                    }
                }

                match codec.decode(&case.schema, &case.root, &case.expected_bytes) {
                    Ok(actual) => {
                        match normalize_value(&reference, &case.schema, &case.root, &actual) {
                            Ok(actual) => match normalize_value(
                                &reference,
                                &case.schema,
                                &case.root,
                                &case.value,
                            ) {
                                Ok(expected) if actual == expected => {
                                    report.passed += 1;
                                }
                                Ok(expected) => report.failures.push(ConformanceFailure {
                                    case: case.name.to_string(),
                                    phase: "decode".to_string(),
                                    detail: format!("expected {:?}, got {:?}", expected, actual),
                                }),
                                Err(err) => report.failures.push(ConformanceFailure {
                                    case: case.name.to_string(),
                                    phase: "fixture".to_string(),
                                    detail: err,
                                }),
                            },
                            Err(err) => report.failures.push(ConformanceFailure {
                                case: case.name.to_string(),
                                phase: "decode".to_string(),
                                detail: err,
                            }),
                        }
                    }
                    Err(err) => report.failures.push(ConformanceFailure {
                        case: case.name.to_string(),
                        phase: "decode".to_string(),
                        detail: err,
                    }),
                }
            }
            SuiteCase::Decode(case) => match codec.decode(&case.schema, &case.root, &case.bytes) {
                Ok(actual) => {
                    match normalize_value(&reference, &case.schema, &case.root, &actual) {
                        Ok(actual) => match normalize_value(
                            &reference,
                            &case.schema,
                            &case.root,
                            &case.expected_value,
                        ) {
                            Ok(expected) if actual == expected => {
                                if let Some(expected_bytes) = &case.expected_canonical_bytes {
                                    match codec.encode(&case.schema, &case.root, &actual) {
                                        Ok(actual_bytes) if actual_bytes == *expected_bytes => {
                                            report.passed += 1;
                                        }
                                        Ok(actual_bytes) => {
                                            report.failures.push(ConformanceFailure {
                                                case: case.name.to_string(),
                                                phase: "re-encode".to_string(),
                                                detail: format!(
                                                    "expected {}, got {}",
                                                    hex(expected_bytes),
                                                    hex(&actual_bytes)
                                                ),
                                            })
                                        }
                                        Err(err) => report.failures.push(ConformanceFailure {
                                            case: case.name.to_string(),
                                            phase: "re-encode".to_string(),
                                            detail: err,
                                        }),
                                    }
                                } else {
                                    report.passed += 1;
                                }
                            }
                            Ok(expected) => report.failures.push(ConformanceFailure {
                                case: case.name.to_string(),
                                phase: "decode".to_string(),
                                detail: format!("expected {:?}, got {:?}", expected, actual),
                            }),
                            Err(err) => report.failures.push(ConformanceFailure {
                                case: case.name.to_string(),
                                phase: "fixture".to_string(),
                                detail: err,
                            }),
                        },
                        Err(err) => report.failures.push(ConformanceFailure {
                            case: case.name.to_string(),
                            phase: "decode".to_string(),
                            detail: err,
                        }),
                    }
                }
                Err(err) => report.failures.push(ConformanceFailure {
                    case: case.name.to_string(),
                    phase: "decode".to_string(),
                    detail: err,
                }),
            },
            SuiteCase::DecodeFailure(case) => {
                match codec.decode(&case.schema, &case.root, &case.bytes) {
                    Ok(actual) => report.failures.push(ConformanceFailure {
                        case: case.name.to_string(),
                        phase: "decode".to_string(),
                        detail: format!(
                            "expected error containing `{}`, got value {actual:?}`",
                            case.expected_error
                        ),
                    }),
                    Err(err) if err.contains(case.expected_error) => {
                        report.passed += 1;
                    }
                    Err(err) => report.failures.push(ConformanceFailure {
                        case: case.name.to_string(),
                        phase: "decode".to_string(),
                        detail: format!(
                            "expected error containing `{}`, got `{err}`",
                            case.expected_error
                        ),
                    }),
                }
            }
        }
    }

    report
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ReferenceCodec;

impl CanonicalCodec for ReferenceCodec {
    fn encode(
        &self,
        schema: &FixtureSchema,
        root: &TypeRef,
        value: &Value,
    ) -> Result<Vec<u8>, String> {
        self.encode_value(schema, root, value)
    }

    fn decode(
        &self,
        schema: &FixtureSchema,
        root: &TypeRef,
        bytes: &[u8],
    ) -> Result<Value, String> {
        self.decode_value(schema, root, bytes)
    }
}

impl ReferenceCodec {
    fn encode_value(
        &self,
        schema: &FixtureSchema,
        root: &TypeRef,
        value: &Value,
    ) -> Result<Vec<u8>, String> {
        match root {
            TypeRef::Bool => match value {
                Value::Bool(value) => Ok(vec![u8::from(*value)]),
                other => Err(format!("expected bool, got {other:?}")),
            },
            TypeRef::U64 => match value {
                Value::U64(value) => Ok(encode_varint(*value)),
                other => Err(format!("expected u64, got {other:?}")),
            },
            TypeRef::String => match value {
                Value::String(value) => Ok(value.as_bytes().to_vec()),
                other => Err(format!("expected string, got {other:?}")),
            },
            TypeRef::Bytes => match value {
                Value::Bytes(value) => Ok(value.clone()),
                other => Err(format!("expected bytes, got {other:?}")),
            },
            TypeRef::List(item) => match value {
                Value::List(items) => {
                    let mut out = encode_varint(items.len() as u64);
                    for item_value in items {
                        let item_bytes = self.encode_value(schema, item, item_value)?;
                        out.extend(encode_varint(item_bytes.len() as u64));
                        out.extend(item_bytes);
                    }
                    Ok(out)
                }
                other => Err(format!("expected list, got {other:?}")),
            },
            TypeRef::Map(key, value_type) => match value {
                Value::Map(entries) => {
                    let mut encoded_entries = Vec::with_capacity(entries.len());
                    for (entry_key, entry_value) in entries {
                        let key_bytes = self.encode_value(schema, key, entry_key)?;
                        let value_bytes = self.encode_value(schema, value_type, entry_value)?;
                        encoded_entries.push((key_bytes, value_bytes));
                    }
                    encoded_entries.sort_by(|left, right| left.0.cmp(&right.0));
                    for pair in encoded_entries.windows(2) {
                        if pair[0].0 == pair[1].0 {
                            return Err("duplicate map key".to_string());
                        }
                    }
                    let mut out = encode_varint(encoded_entries.len() as u64);
                    for (key_bytes, value_bytes) in encoded_entries {
                        out.extend(encode_varint(key_bytes.len() as u64));
                        out.extend(key_bytes);
                        out.extend(encode_varint(value_bytes.len() as u64));
                        out.extend(value_bytes);
                    }
                    Ok(out)
                }
                other => Err(format!("expected map, got {other:?}")),
            },
            TypeRef::Named(name) => match schema.types.get(name) {
                Some(TypeDef::Record { fields }) => self.encode_record(schema, fields, value),
                Some(TypeDef::Enum { variants }) => self.encode_enum(schema, variants, value),
                None => Err(format!("unknown type `{name}` in schema `{}`", schema.name)),
            },
        }
    }

    fn encode_record(
        &self,
        schema: &FixtureSchema,
        fields: &[FieldDef],
        value: &Value,
    ) -> Result<Vec<u8>, String> {
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
                    let bytes = self.encode_value(schema, &field.ty, field_value)?;
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
        let mut out = encode_varint(encoded_fields.len() as u64);
        for (field_id, bytes) in encoded_fields {
            out.extend(encode_varint(field_id as u64));
            out.extend(encode_varint(bytes.len() as u64));
            out.extend(bytes);
        }
        Ok(out)
    }

    fn encode_enum(
        &self,
        schema: &FixtureSchema,
        variants: &[VariantDef],
        value: &Value,
    ) -> Result<Vec<u8>, String> {
        let Value::Enum { variant, value } = value else {
            return Err(format!("expected enum, got {value:?}"));
        };

        let variant_def = variants
            .iter()
            .find(|candidate| candidate.name == *variant)
            .ok_or_else(|| format!("unknown variant `{variant}`"))?;

        let payload = match (&variant_def.payload, value) {
            (Some(ty), Some(value)) => self.encode_value(schema, ty, value)?,
            (Some(_), None) => return Err(format!("variant `{variant}` requires payload")),
            (None, Some(_)) => return Err(format!("variant `{variant}` must not carry payload")),
            (None, None) => Vec::new(),
        };

        let mut out = encode_varint(variant_def.id as u64);
        out.extend(encode_varint(payload.len() as u64));
        out.extend(payload);
        Ok(out)
    }

    fn decode_value(
        &self,
        schema: &FixtureSchema,
        root: &TypeRef,
        bytes: &[u8],
    ) -> Result<Value, String> {
        match root {
            TypeRef::Bool => match bytes {
                [0] => Ok(Value::Bool(false)),
                [1] => Ok(Value::Bool(true)),
                _ => Err("bool payload must be a single 0x00 or 0x01 byte".to_string()),
            },
            TypeRef::U64 => {
                let (value, consumed) = decode_varint(bytes)?;
                ensure_consumed(bytes, consumed, "u64")?;
                Ok(Value::U64(value))
            }
            TypeRef::String => String::from_utf8(bytes.to_vec())
                .map(Value::String)
                .map_err(|err| format!("invalid utf-8 string: {err}")),
            TypeRef::Bytes => Ok(Value::Bytes(bytes.to_vec())),
            TypeRef::List(item) => {
                let (count, mut offset) = decode_varint(bytes)?;
                let mut items = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let (item_len, next) = decode_varint(&bytes[offset..])?;
                    offset += next;
                    let end = offset + item_len as usize;
                    let slice = bytes
                        .get(offset..end)
                        .ok_or_else(|| "list item length exceeds payload".to_string())?;
                    items.push(self.decode_value(schema, item, slice)?);
                    offset = end;
                }
                ensure_consumed(bytes, offset, "list")?;
                Ok(Value::List(items))
            }
            TypeRef::Map(key, value_type) => {
                let (count, mut offset) = decode_varint(bytes)?;
                let mut entries = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    let (key_len, next) = decode_varint(&bytes[offset..])?;
                    offset += next;
                    let key_end = offset + key_len as usize;
                    let key_bytes = bytes
                        .get(offset..key_end)
                        .ok_or_else(|| "map key length exceeds payload".to_string())?;
                    let key_value = self.decode_value(schema, key, key_bytes)?;
                    offset = key_end;

                    let (value_len, next) = decode_varint(&bytes[offset..])?;
                    offset += next;
                    let value_end = offset + value_len as usize;
                    let value_bytes = bytes
                        .get(offset..value_end)
                        .ok_or_else(|| "map value length exceeds payload".to_string())?;
                    let value_value = self.decode_value(schema, value_type, value_bytes)?;
                    offset = value_end;
                    entries.push((key_value, value_value));
                }
                ensure_consumed(bytes, offset, "map")?;
                let normalized = normalize_value(self, schema, root, &Value::Map(entries))?;
                Ok(normalized)
            }
            TypeRef::Named(name) => match schema.types.get(name) {
                Some(TypeDef::Record { fields }) => self.decode_record(schema, fields, bytes),
                Some(TypeDef::Enum { variants }) => self.decode_enum(schema, variants, bytes),
                None => Err(format!("unknown type `{name}` in schema `{}`", schema.name)),
            },
        }
    }

    fn decode_record(
        &self,
        schema: &FixtureSchema,
        fields: &[FieldDef],
        bytes: &[u8],
    ) -> Result<Value, String> {
        let (count, mut offset) = decode_varint(bytes)?;
        let field_by_id = fields
            .iter()
            .map(|field| (field.id, field))
            .collect::<BTreeMap<_, _>>();
        let mut seen_ids = BTreeMap::new();
        let mut values = BTreeMap::new();

        for _ in 0..count {
            let (field_id, next) = decode_varint(&bytes[offset..])?;
            offset += next;
            let (field_len, next) = decode_varint(&bytes[offset..])?;
            offset += next;
            let end = offset + field_len as usize;
            let field_bytes = bytes
                .get(offset..end)
                .ok_or_else(|| "field length exceeds payload".to_string())?;
            offset = end;

            if seen_ids.insert(field_id, ()).is_some() {
                return Err(format!("duplicate field id {field_id}"));
            }

            let Some(field) = field_by_id.get(&(field_id as u32)) else {
                continue;
            };
            let value = self.decode_value(schema, &field.ty, field_bytes)?;
            values.insert(field.name.clone(), value);
        }

        ensure_consumed(bytes, offset, "record")?;

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
        &self,
        schema: &FixtureSchema,
        variants: &[VariantDef],
        bytes: &[u8],
    ) -> Result<Value, String> {
        let (variant_id, offset) = decode_varint(bytes)?;
        let (payload_len, next) = decode_varint(&bytes[offset..])?;
        let payload_offset = offset + next;
        let payload_end = payload_offset + payload_len as usize;
        let payload = bytes
            .get(payload_offset..payload_end)
            .ok_or_else(|| "enum payload length exceeds payload".to_string())?;
        ensure_consumed(bytes, payload_end, "enum")?;

        let variant = variants
            .iter()
            .find(|candidate| candidate.id == variant_id as u32)
            .ok_or_else(|| format!("unknown variant id {variant_id}"))?;
        let value = match &variant.payload {
            Some(ty) => Some(Box::new(self.decode_value(schema, ty, payload)?)),
            None if payload.is_empty() => None,
            None => return Err(format!("variant `{}` must not carry payload", variant.name)),
        };
        Ok(Value::Enum {
            variant: variant.name.clone(),
            value,
        })
    }
}

pub fn canonical_contract_fixture_suite() -> Vec<SuiteCase> {
    let telemetry_v1 = telemetry_v1_schema();
    let telemetry_v2 = telemetry_v2_schema();
    let telemetry_v3 = telemetry_v3_required_source_schema();
    let nested_v1 = envelope_v1_schema();
    let nested_v2 = envelope_v2_schema();
    let audit_v1 = audit_v1_schema();
    let audit_v2 = audit_v2_removed_note_schema();
    let status_v1 = status_v1_schema();
    let status_v2 = status_v2_schema();
    let directory = directory_schema();

    vec![
        SuiteCase::Golden(GoldenCase {
            name: "golden.telemetry.default-omitted",
            schema: telemetry_v1.clone(),
            root: named("Telemetry"),
            value: record([
                ("count", Value::U64(7)),
                ("sensor", Value::String("cam-a".to_string())),
            ]),
            expected_bytes: vec![2, 1, 1, 7, 2, 5, 99, 97, 109, 45, 97],
        }),
        SuiteCase::Decode(DecodeCase {
            name: "decode.telemetry.explicit-default-normalizes",
            schema: telemetry_v1.clone(),
            root: named("Telemetry"),
            bytes: vec![3, 1, 1, 7, 2, 5, 99, 97, 109, 45, 97, 4, 1, 0],
            expected_value: record([
                ("count", Value::U64(7)),
                ("sensor", Value::String("cam-a".to_string())),
            ]),
            expected_canonical_bytes: Some(vec![2, 1, 1, 7, 2, 5, 99, 97, 109, 45, 97]),
        }),
        SuiteCase::Golden(GoldenCase {
            name: "golden.directory.map-keys-sorted",
            schema: directory,
            root: named("Directory"),
            value: record([(
                "entries",
                Value::Map(vec![
                    (Value::String("beta".to_string()), Value::U64(2)),
                    (Value::String("alpha".to_string()), Value::U64(1)),
                ]),
            )]),
            expected_bytes: vec![
                1, 1, 16, 2, 5, 97, 108, 112, 104, 97, 1, 1, 4, 98, 101, 116, 97, 1, 2,
            ],
        }),
        SuiteCase::Golden(GoldenCase {
            name: "golden.status.variant-id-stable",
            schema: status_v1.clone(),
            root: named("Status"),
            value: Value::Enum {
                variant: "Error".to_string(),
                value: Some(Box::new(Value::String("disk-full".to_string()))),
            },
            expected_bytes: vec![2, 9, 100, 105, 115, 107, 45, 102, 117, 108, 108],
        }),
        SuiteCase::Decode(DecodeCase {
            name: "compat.forward.old-reader-ignores-new-defaulted-field",
            schema: telemetry_v1.clone(),
            root: named("Telemetry"),
            bytes: vec![
                3, 1, 1, 7, 2, 5, 99, 97, 109, 45, 97, 5, 10, 102, 97, 104, 114, 101, 110, 104,
                101, 105, 116,
            ],
            expected_value: record([
                ("count", Value::U64(7)),
                ("sensor", Value::String("cam-a".to_string())),
            ]),
            expected_canonical_bytes: Some(vec![2, 1, 1, 7, 2, 5, 99, 97, 109, 45, 97]),
        }),
        SuiteCase::Decode(DecodeCase {
            name: "compat.backward.new-reader-materializes-default",
            schema: telemetry_v2.clone(),
            root: named("Telemetry"),
            bytes: vec![2, 1, 1, 7, 2, 5, 99, 97, 109, 45, 97],
            expected_value: record([
                ("count", Value::U64(7)),
                ("sensor", Value::String("cam-a".to_string())),
                ("unit", Value::String("celsius".to_string())),
            ]),
            expected_canonical_bytes: Some(vec![2, 1, 1, 7, 2, 5, 99, 97, 109, 45, 97]),
        }),
        SuiteCase::Decode(DecodeCase {
            name: "compat.nested-additive-default",
            schema: nested_v2,
            root: named("Envelope"),
            bytes: vec![1, 1, 11, 2, 1, 1, 7, 2, 5, 99, 97, 109, 45, 97],
            expected_value: record([(
                "telemetry",
                record([
                    ("count", Value::U64(7)),
                    ("sensor", Value::String("cam-a".to_string())),
                    ("unit", Value::String("celsius".to_string())),
                ]),
            )]),
            expected_canonical_bytes: Some(vec![1, 1, 11, 2, 1, 1, 7, 2, 5, 99, 97, 109, 45, 97]),
        }),
        SuiteCase::Decode(DecodeCase {
            name: "compat.removed-optional-field-ignored",
            schema: audit_v2,
            root: named("Audit"),
            bytes: vec![2, 1, 1, 9, 3, 6, 108, 101, 103, 97, 99, 121],
            expected_value: record([("id", Value::U64(9))]),
            expected_canonical_bytes: Some(vec![1, 1, 1, 9]),
        }),
        SuiteCase::DecodeFailure(FailureCase {
            name: "compat.missing-required-field-fails",
            schema: telemetry_v3,
            root: named("Telemetry"),
            bytes: vec![2, 1, 1, 7, 2, 5, 99, 97, 109, 45, 97],
            expected_error: "missing required field `source`",
        }),
        SuiteCase::DecodeFailure(FailureCase {
            name: "compat.unknown-enum-variant-fails",
            schema: status_v1,
            root: named("Status"),
            bytes: vec![3, 0],
            expected_error: "unknown variant id 3",
        }),
        SuiteCase::Golden(GoldenCase {
            name: "golden.compat-writer-schema-kept-for-new-variant",
            schema: status_v2,
            root: named("Status"),
            value: Value::Enum {
                variant: "Maintenance".to_string(),
                value: None,
            },
            expected_bytes: vec![3, 0],
        }),
        SuiteCase::Golden(GoldenCase {
            name: "golden.nested-envelope-v1",
            schema: nested_v1,
            root: named("Envelope"),
            value: record([(
                "telemetry",
                record([
                    ("count", Value::U64(7)),
                    ("sensor", Value::String("cam-a".to_string())),
                ]),
            )]),
            expected_bytes: vec![1, 1, 11, 2, 1, 1, 7, 2, 5, 99, 97, 109, 45, 97],
        }),
        SuiteCase::Golden(GoldenCase {
            name: "golden.audit-v1-with-note",
            schema: audit_v1,
            root: named("Audit"),
            value: record([
                ("id", Value::U64(9)),
                ("note", Value::String("legacy".to_string())),
            ]),
            expected_bytes: vec![2, 1, 1, 9, 3, 6, 108, 101, 103, 97, 99, 121],
        }),
    ]
}

fn normalize_value(
    codec: &ReferenceCodec,
    schema: &FixtureSchema,
    root: &TypeRef,
    value: &Value,
) -> Result<Value, String> {
    match root {
        TypeRef::Bool | TypeRef::U64 | TypeRef::String | TypeRef::Bytes => Ok(value.clone()),
        TypeRef::List(item) => match value {
            Value::List(items) => items
                .iter()
                .map(|item_value| normalize_value(codec, schema, item, item_value))
                .collect::<Result<Vec<_>, _>>()
                .map(Value::List),
            other => Err(format!("expected list, got {other:?}")),
        },
        TypeRef::Map(key, value_type) => match value {
            Value::Map(entries) => {
                let mut normalized = entries
                    .iter()
                    .map(|(entry_key, entry_value)| {
                        let normalized_key = normalize_value(codec, schema, key, entry_key)?;
                        let normalized_value =
                            normalize_value(codec, schema, value_type, entry_value)?;
                        let key_bytes = codec.encode_value(schema, key, &normalized_key)?;
                        Ok((key_bytes, normalized_key, normalized_value))
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                normalized.sort_by(|left, right| left.0.cmp(&right.0));
                let entries = normalized
                    .into_iter()
                    .map(|(_, key_value, entry_value)| (key_value, entry_value))
                    .collect();
                Ok(Value::Map(entries))
            }
            other => Err(format!("expected map, got {other:?}")),
        },
        TypeRef::Named(name) => match schema.types.get(name) {
            Some(TypeDef::Record { fields }) => match value {
                Value::Record(record) => {
                    let mut normalized = BTreeMap::new();
                    for field in fields {
                        match record.get(&field.name) {
                            Some(field_value) => {
                                normalized.insert(
                                    field.name.clone(),
                                    normalize_value(codec, schema, &field.ty, field_value)?,
                                );
                            }
                            None => match &field.presence {
                                FieldPresence::Required => {
                                    return Err(format!("missing required field `{}`", field.name));
                                }
                                FieldPresence::Optional => {}
                                FieldPresence::Defaulted(default) => {
                                    normalized.insert(
                                        field.name.clone(),
                                        normalize_value(codec, schema, &field.ty, default)?,
                                    );
                                }
                            },
                        }
                    }
                    Ok(Value::Record(normalized))
                }
                other => Err(format!("expected record, got {other:?}")),
            },
            Some(TypeDef::Enum { variants }) => match value {
                Value::Enum { variant, value } => {
                    let variant_def = variants
                        .iter()
                        .find(|candidate| candidate.name == *variant)
                        .ok_or_else(|| format!("unknown variant `{variant}`"))?;
                    let normalized_value = match (&variant_def.payload, value) {
                        (Some(ty), Some(value)) => {
                            Some(Box::new(normalize_value(codec, schema, ty, value)?))
                        }
                        (None, None) => None,
                        (Some(_), None) => {
                            return Err(format!("variant `{variant}` requires payload"));
                        }
                        (None, Some(_)) => {
                            return Err(format!("variant `{variant}` must not carry payload"));
                        }
                    };
                    Ok(Value::Enum {
                        variant: variant.clone(),
                        value: normalized_value,
                    })
                }
                other => Err(format!("expected enum, got {other:?}")),
            },
            None => Err(format!("unknown type `{name}` in schema `{}`", schema.name)),
        },
    }
}

fn telemetry_v1_schema() -> FixtureSchema {
    schema(
        "telemetry.v1",
        [(
            "Telemetry",
            TypeDef::Record {
                fields: vec![
                    field("count", 1, TypeRef::U64, FieldPresence::Required),
                    field("sensor", 2, TypeRef::String, FieldPresence::Required),
                    field(
                        "priority",
                        4,
                        TypeRef::U64,
                        FieldPresence::Defaulted(Value::U64(0)),
                    ),
                ],
            },
        )],
    )
}

fn telemetry_v2_schema() -> FixtureSchema {
    schema(
        "telemetry.v2",
        [(
            "Telemetry",
            TypeDef::Record {
                fields: vec![
                    field("count", 1, TypeRef::U64, FieldPresence::Required),
                    field("sensor", 2, TypeRef::String, FieldPresence::Required),
                    field(
                        "priority",
                        4,
                        TypeRef::U64,
                        FieldPresence::Defaulted(Value::U64(0)),
                    ),
                    field(
                        "unit",
                        5,
                        TypeRef::String,
                        FieldPresence::Defaulted(Value::String("celsius".to_string())),
                    ),
                ],
            },
        )],
    )
}

fn telemetry_v3_required_source_schema() -> FixtureSchema {
    schema(
        "telemetry.v3",
        [(
            "Telemetry",
            TypeDef::Record {
                fields: vec![
                    field("count", 1, TypeRef::U64, FieldPresence::Required),
                    field("sensor", 2, TypeRef::String, FieldPresence::Required),
                    field(
                        "priority",
                        4,
                        TypeRef::U64,
                        FieldPresence::Defaulted(Value::U64(0)),
                    ),
                    field("source", 6, TypeRef::String, FieldPresence::Required),
                ],
            },
        )],
    )
}

fn envelope_v1_schema() -> FixtureSchema {
    schema(
        "envelope.v1",
        [
            (
                "Telemetry",
                TypeDef::Record {
                    fields: vec![
                        field("count", 1, TypeRef::U64, FieldPresence::Required),
                        field("sensor", 2, TypeRef::String, FieldPresence::Required),
                    ],
                },
            ),
            (
                "Envelope",
                TypeDef::Record {
                    fields: vec![field(
                        "telemetry",
                        1,
                        named("Telemetry"),
                        FieldPresence::Required,
                    )],
                },
            ),
        ],
    )
}

fn envelope_v2_schema() -> FixtureSchema {
    schema(
        "envelope.v2",
        [
            (
                "Telemetry",
                TypeDef::Record {
                    fields: vec![
                        field("count", 1, TypeRef::U64, FieldPresence::Required),
                        field("sensor", 2, TypeRef::String, FieldPresence::Required),
                        field(
                            "unit",
                            5,
                            TypeRef::String,
                            FieldPresence::Defaulted(Value::String("celsius".to_string())),
                        ),
                    ],
                },
            ),
            (
                "Envelope",
                TypeDef::Record {
                    fields: vec![field(
                        "telemetry",
                        1,
                        named("Telemetry"),
                        FieldPresence::Required,
                    )],
                },
            ),
        ],
    )
}

fn audit_v1_schema() -> FixtureSchema {
    schema(
        "audit.v1",
        [(
            "Audit",
            TypeDef::Record {
                fields: vec![
                    field("id", 1, TypeRef::U64, FieldPresence::Required),
                    field("note", 3, TypeRef::String, FieldPresence::Optional),
                ],
            },
        )],
    )
}

fn audit_v2_removed_note_schema() -> FixtureSchema {
    schema(
        "audit.v2",
        [(
            "Audit",
            TypeDef::Record {
                fields: vec![field("id", 1, TypeRef::U64, FieldPresence::Required)],
            },
        )],
    )
}

fn directory_schema() -> FixtureSchema {
    schema(
        "directory.v1",
        [(
            "Directory",
            TypeDef::Record {
                fields: vec![field(
                    "entries",
                    1,
                    TypeRef::Map(Box::new(TypeRef::String), Box::new(TypeRef::U64)),
                    FieldPresence::Required,
                )],
            },
        )],
    )
}

fn status_v1_schema() -> FixtureSchema {
    schema(
        "status.v1",
        [(
            "Status",
            TypeDef::Enum {
                variants: vec![
                    variant("Ok", 1, None),
                    variant("Error", 2, Some(TypeRef::String)),
                ],
            },
        )],
    )
}

fn status_v2_schema() -> FixtureSchema {
    schema(
        "status.v2",
        [(
            "Status",
            TypeDef::Enum {
                variants: vec![
                    variant("Ok", 1, None),
                    variant("Error", 2, Some(TypeRef::String)),
                    variant("Maintenance", 3, None),
                ],
            },
        )],
    )
}

fn schema<const N: usize>(name: &str, defs: [(&str, TypeDef); N]) -> FixtureSchema {
    FixtureSchema {
        name: name.to_string(),
        types: defs
            .into_iter()
            .map(|(type_name, ty)| (type_name.to_string(), ty))
            .collect(),
    }
}

fn field(name: &str, id: u32, ty: TypeRef, presence: FieldPresence) -> FieldDef {
    FieldDef {
        name: name.to_string(),
        id,
        ty,
        presence,
    }
}

fn variant(name: &str, id: u32, payload: Option<TypeRef>) -> VariantDef {
    VariantDef {
        name: name.to_string(),
        id,
        payload,
    }
}

fn named(name: &str) -> TypeRef {
    TypeRef::Named(name.to_string())
}

fn record<const N: usize>(fields: [(&str, Value); N]) -> Value {
    Value::Record(
        fields
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect(),
    )
}

fn normalize_field_value<'a>(field: &'a FieldDef, value: &'a Value) -> &'a Value {
    match (&field.presence, value) {
        (FieldPresence::Defaulted(default), candidate) if default == candidate => default,
        _ => value,
    }
}

fn encode_varint(mut value: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
    out
}

fn decode_varint(bytes: &[u8]) -> Result<(u64, usize), String> {
    let mut shift = 0;
    let mut value = 0u64;

    for (index, byte) in bytes.iter().enumerate() {
        let byte_value = (byte & 0x7f) as u64;
        value |= byte_value << shift;
        if byte & 0x80 == 0 {
            return Ok((value, index + 1));
        }
        shift += 7;
        if shift >= 64 {
            return Err("varint exceeds 64 bits".to_string());
        }
    }

    Err("unexpected end of varint".to_string())
}

fn ensure_consumed(bytes: &[u8], consumed: usize, context: &str) -> Result<(), String> {
    if consumed == bytes.len() {
        Ok(())
    } else {
        Err(format!("{context} payload has trailing bytes"))
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}

#[cfg(test)]
mod tests {
    use super::{ReferenceCodec, canonical_contract_fixture_suite, run_fixture_suite};

    #[test]
    fn reference_codec_passes_shared_fixture_suite() {
        let report = run_fixture_suite(&ReferenceCodec, &canonical_contract_fixture_suite());
        report.into_result().expect("reference codec should pass");
    }
}
