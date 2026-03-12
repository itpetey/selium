//! Shared support types and helpers for generated guest bindings.

use std::collections::BTreeMap;

/// Describes how a generated service body is transported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceBodyMode {
    /// The service does not use a body.
    None,
    /// The service body is buffered in memory.
    Buffered,
    /// The service body is streamed over the transport.
    Stream,
}

/// Describes the schema binding for a generated service body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceBodyBinding {
    /// The transport mode used for the body.
    pub mode: ServiceBodyMode,
    /// The optional schema name for the body payload.
    pub schema: Option<&'static str>,
}

/// Describes a generated request field to transport target mapping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceFieldBinding {
    /// The schema field name.
    pub field: &'static str,
    /// The transport target name such as an HTTP header.
    pub target: &'static str,
}

/// Describes a generated service contract binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceBinding {
    /// The service name.
    pub name: &'static str,
    /// The request schema name.
    pub request_schema: &'static str,
    /// The response schema name.
    pub response_schema: &'static str,
    /// The transport protocol, when one is defined.
    pub protocol: Option<&'static str>,
    /// The transport method, when one is defined.
    pub method: Option<&'static str>,
    /// The transport path, when one is defined.
    pub path: Option<&'static str>,
    /// The request field bindings for metadata targets.
    pub request_headers: &'static [ServiceFieldBinding],
    /// The request body binding metadata.
    pub request_body: ServiceBodyBinding,
    /// The response body binding metadata.
    pub response_body: ServiceBodyBinding,
}

/// Mirrors the runtime codec major version used by generated bindings.
pub const SELIUM_CONTRACT_CODEC_MAJOR_VERSION: u8 = selium_abi::CONTRACT_CODEC_MAJOR_VERSION;

/// Extracts named path parameters from a request path using a generated template.
pub fn extract_path_params(template: &str, actual: &str) -> Option<BTreeMap<String, String>> {
    let template_segments = if template.trim_matches('/').is_empty() {
        Vec::new()
    } else {
        template.trim_matches('/').split('/').collect::<Vec<_>>()
    };
    let actual_segments = if actual.trim_matches('/').is_empty() {
        Vec::new()
    } else {
        actual.trim_matches('/').split('/').collect::<Vec<_>>()
    };
    if template_segments.len() != actual_segments.len() {
        return None;
    }

    let mut params = BTreeMap::new();
    for (template_segment, actual_segment) in template_segments.into_iter().zip(actual_segments) {
        if template_segment.starts_with('{')
            && template_segment.ends_with('}')
            && template_segment.len() > 2
        {
            params.insert(
                template_segment[1..template_segment.len() - 1].to_string(),
                actual_segment.to_string(),
            );
        } else if template_segment != actual_segment {
            return None;
        }
    }
    Some(params)
}

/// Reads a complete stream message into memory until a finishing chunk arrives.
pub async fn read_stream_message(
    stream: &crate::network::StreamChannel,
    max_bytes: u32,
    timeout_ms: u32,
) -> anyhow::Result<Vec<u8>> {
    let mut payload = Vec::new();
    loop {
        let Some(chunk) = stream.recv(max_bytes, timeout_ms).await? else {
            continue;
        };
        payload.extend_from_slice(&chunk.bytes);
        if chunk.finish {
            return Ok(payload);
        }
    }
}
