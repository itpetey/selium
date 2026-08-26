//! Selium HTTP protocol wire types.
//!
//! Schema-backed FlatBuffers types for HTTP/1.1 request/response, body chunks,
//! and trailers. Used by the HTTP connector for typed forwarding over
//! shared-memory channels.

extern crate self as selium_proto_http;

use selium_guest_macros::schema;

pub mod fbs;

/// A single HTTP header name-value pair.
#[schema(
    path = "schemas/http.fbs",
    ty = "selium.http.HttpHeader",
    binding = "selium_proto_http::fbs::selium::http::HttpHeader"
)]
#[derive(Debug, Clone, PartialEq)]
pub struct HttpHeader {
    pub name: String,
    pub value: String,
}

impl HttpHeader {
    /// Convenience constructor that accepts `impl Into<String>`.
    pub fn from_str(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

/// A typed HTTP request forwarded by the connector.
///
/// The `body` field carries inline bytes for requests with `Content-Length`
/// below a connector-configured threshold. For streaming (chunked) bodies,
/// `body` is empty and chunks are delivered via server-streaming RPC.
#[schema(
    path = "schemas/http.fbs",
    ty = "selium.http.HttpRequest",
    binding = "selium_proto_http::fbs::selium::http::HttpRequest"
)]
#[derive(Debug, Clone, PartialEq)]
pub struct HttpRequest {
    pub method: String,
    pub uri: String,
    pub headers: Vec<HttpHeader>,
    pub body: Vec<u8>,
}

impl HttpRequest {
    /// Convenience constructor that accepts `impl Into<String>` for the
    /// string fields.
    pub fn from_str(
        method: impl Into<String>,
        uri: impl Into<String>,
        headers: Vec<HttpHeader>,
        body: Vec<u8>,
    ) -> Self {
        Self {
            method: method.into(),
            uri: uri.into(),
            headers,
            body,
        }
    }
}

/// A typed HTTP response sent back through the connector.
///
/// The `body` field carries inline bytes for responses. For streaming
/// (chunked) responses, `body` is empty and chunks are delivered via
/// server-streaming RPC.
#[schema(
    path = "schemas/http.fbs",
    ty = "selium.http.HttpResponse",
    binding = "selium_proto_http::fbs::selium::http::HttpResponse"
)]
#[derive(Debug, Clone, PartialEq)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<HttpHeader>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    /// Convenience constructor that accepts `impl Into<String>` for header
    /// fields.
    pub fn from_str(
        status: u16,
        headers: Vec<HttpHeader>,
        body: Vec<u8>,
    ) -> Self {
        Self {
            status,
            headers,
            body,
        }
    }
}

/// A chunk of streaming body data.
#[schema(
    path = "schemas/http.fbs",
    ty = "selium.http.HttpBodyChunk",
    binding = "selium_proto_http::fbs::selium::http::HttpBodyChunk"
)]
#[derive(Debug, Clone, PartialEq)]
pub struct HttpBodyChunk {
    pub data: Vec<u8>,
}

/// A single HTTP trailer header (sent after the body in chunked transfer
/// encoding).
#[schema(
    path = "schemas/http.fbs",
    ty = "selium.http.HttpTrailer",
    binding = "selium_proto_http::fbs::selium::http::HttpTrailer"
)]
#[derive(Debug, Clone, PartialEq)]
pub struct HttpTrailer {
    pub name: String,
    pub value: String,
}

impl HttpTrailer {
    /// Convenience constructor that accepts `impl Into<String>`.
    pub fn from_str(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use selium_encoding::FlatMsg;

    fn round_trip<T: FlatMsg + Clone + PartialEq + std::fmt::Debug>(value: &T) {
        let encoded = T::encode(value);
        let decoded = T::decode(&encoded).expect("decode should succeed");
        assert_eq!(value, &decoded, "round-trip mismatch");
    }

    #[test]
    fn http_header_round_trip() {
        let header = HttpHeader::from_str("content-type", "application/json");
        round_trip(&header);
    }

    #[test]
    fn http_header_empty_value() {
        let header = HttpHeader::from_str("x-empty", "");
        round_trip(&header);
    }

    #[test]
    fn http_request_get_no_body() {
        let req = HttpRequest::from_str(
            "GET",
            "/api/v1/status",
            vec![HttpHeader::from_str("host", "example.com")],
            vec![],
        );
        round_trip(&req);
    }

    #[test]
    fn http_request_post_with_body() {
        let body = b"{\"key\":\"value\"}".to_vec();
        let req = HttpRequest::from_str(
            "POST",
            "/api/v1/data",
            vec![
                HttpHeader::from_str("host", "example.com"),
                HttpHeader::from_str("content-type", "application/json"),
                HttpHeader::from_str("content-length", "16"),
            ],
            body,
        );
        round_trip(&req);
    }

    #[test]
    fn http_request_multiple_headers() {
        let req = HttpRequest::from_str(
            "GET",
            "/",
            vec![
                HttpHeader::from_str("host", "example.com"),
                HttpHeader::from_str("accept", "text/html"),
                HttpHeader::from_str("accept", "application/json"),
                HttpHeader::from_str("user-agent", "curl/8.0"),
                HttpHeader::from_str("connection", "keep-alive"),
            ],
            vec![],
        );
        round_trip(&req);
    }

    #[test]
    fn http_response_200_with_body() {
        let body = b"<html><body>hello</body></html>".to_vec();
        let resp = HttpResponse::from_str(
            200,
            vec![
                HttpHeader::from_str("content-type", "text/html"),
                HttpHeader::from_str("content-length", "31"),
            ],
            body,
        );
        round_trip(&resp);
    }

    #[test]
    fn http_response_404_no_body() {
        let resp = HttpResponse::from_str(404, vec![], vec![]);
        round_trip(&resp);
    }

    #[test]
    fn http_response_500() {
        let body = b"Internal Server Error".to_vec();
        let resp = HttpResponse::from_str(
            500,
            vec![HttpHeader::from_str("content-type", "text/plain")],
            body,
        );
        round_trip(&resp);
    }

    #[test]
    fn http_body_chunk_round_trip() {
        let chunk = HttpBodyChunk::new(b"chunk1".to_vec());
        round_trip(&chunk);
    }

    #[test]
    fn http_body_chunk_empty() {
        let chunk = HttpBodyChunk::new(vec![]);
        round_trip(&chunk);
    }

    #[test]
    fn http_body_chunk_large() {
        let data = vec![0xAB_u8; 4096];
        let chunk = HttpBodyChunk::new(data);
        round_trip(&chunk);
    }

    #[test]
    fn chunked_body_sequence() {
        // Simulate a chunked transfer encoding sequence:
        // [chunk1, chunk2, chunk3]
        let chunks = vec![
            HttpBodyChunk::new(b"Hello, ".to_vec()),
            HttpBodyChunk::new(b"World!".to_vec()),
            HttpBodyChunk::new(b"".to_vec()),
        ];
        for chunk in &chunks {
            round_trip(chunk);
        }
    }

    #[test]
    fn chunked_body_sequence_binary() {
        // Binary chunks with varied sizes
        let chunks: Vec<HttpBodyChunk> = (0..5)
            .map(|i| {
                let size = 1 << (i + 4); // 16, 32, 64, 128, 256 bytes
                HttpBodyChunk::new(vec![i as u8; size])
            })
            .collect();
        for chunk in &chunks {
            round_trip(chunk);
        }
    }

    #[test]
    fn http_trailer_round_trip() {
        let trailer = HttpTrailer::from_str("x-checksum", "abc123");
        round_trip(&trailer);
    }

    #[test]
    fn http_trailer_empty_value() {
        let trailer = HttpTrailer::from_str("x-custom", "");
        round_trip(&trailer);
    }

    #[test]
    fn full_response_with_trailer_equiv() {
        // Trailers are separate messages but we test them alongside response
        let resp = HttpResponse::from_str(200, vec![], vec![]);
        let trailers = vec![
            HttpTrailer::from_str("x-processing-time", "42ms"),
            HttpTrailer::from_str("x-request-id", "req-001"),
        ];
        round_trip(&resp);
        for t in &trailers {
            round_trip(t);
        }
    }

    #[test]
    fn has_schema_verification() {
        use selium_encoding::HasSchema;
        // Every schema type should report its FQ name
        assert_eq!(HttpRequest::SCHEMA.fqname, "selium.http.HttpRequest");
        assert_eq!(HttpResponse::SCHEMA.fqname, "selium.http.HttpResponse");
        assert_eq!(HttpBodyChunk::SCHEMA.fqname, "selium.http.HttpBodyChunk");
        assert_eq!(HttpHeader::SCHEMA.fqname, "selium.http.HttpHeader");
        assert_eq!(HttpTrailer::SCHEMA.fqname, "selium.http.HttpTrailer");

        // SCHEMA hash should be non-zero
        assert_ne!(HttpRequest::SCHEMA.hash, [0u8; 16]);
        assert_ne!(HttpResponse::SCHEMA.hash, [0u8; 16]);
    }

    #[test]
    fn request_with_large_body_inline() {
        let body = vec![b'x'; 65536];
        let req = HttpRequest::from_str(
            "PUT",
            "/upload",
            vec![HttpHeader::from_str("content-length", "65536")],
            body,
        );
        round_trip(&req);
    }
}
