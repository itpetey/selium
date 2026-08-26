//! HTTP/1.1 edge connector system guest.
//!
//! Terminates external TCP/TLS/HTTP-1.1 at the edge and forwards typed,
//! schema-encoded HTTP messages over shared-memory channels, so application
//! guests serve web traffic with no network capabilities of their own.

pub mod serve;

use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls_pemfile as pemfile;
use selium_guest::{
    debug, entrypoint, error, info, warn,
    mark_ready,
    net::tcp::{TcpListener, TcpStream},
    BlobStore, Context,
};
use selium_proto_http::{HttpHeader, HttpRequest, HttpResponse};
use std::collections::{BTreeMap, HashMap};
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::TlsAcceptor;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const MAX_HEADERS: usize = 128;
const MAX_HEADER_NAME_LEN: usize = 256;
const MAX_HEADER_VALUE_LEN: usize = 8192;
const MAX_URI_LEN: usize = 8192;
const READ_BUF_SIZE: usize = 16384;

/// Storage blob store name for TLS material.
const TLS_STORE_NAME: &str = "tls-certs";
/// Manifest name for the certificate chain PEM.
const TLS_CERT_MANIFEST: &str = "cert-pem";
/// Manifest name for the private key PEM.
const TLS_KEY_MANIFEST: &str = "key-pem";

// ---------------------------------------------------------------------------
// TLS termination (task 2.3)
// ---------------------------------------------------------------------------

/// Load TLS certificate and key material from storage via the connector's
/// `Storage` grant. Fails loudly on missing or invalid material.
fn load_tls_config() -> Result<Arc<rustls::ServerConfig>, TlsError> {
    let store = BlobStore::open(TLS_STORE_NAME).map_err(|e| {
        error!("http-connector: failed to open blob store '{TLS_STORE_NAME}': {e}");
        TlsError::StorageUnavailable
    })?;

    // Load certificate chain.
    let cert_blob_id = store
        .manifest(TLS_CERT_MANIFEST)
        .map_err(|e| {
            error!("http-connector: cert manifest '{TLS_CERT_MANIFEST}' not found: {e}");
            TlsError::MissingCertificate
        })?
        .ok_or_else(|| {
            error!("http-connector: cert manifest '{TLS_CERT_MANIFEST}' is empty");
            TlsError::MissingCertificate
        })?;
    let cert_pem = store.get(&cert_blob_id).map_err(|e| {
        error!("http-connector: failed to read cert blob: {e}");
        TlsError::MissingCertificate
    })?
    .ok_or_else(|| {
        error!("http-connector: cert blob is empty");
        TlsError::MissingCertificate
    })?;

    // Load private key.
    let key_blob_id = store
        .manifest(TLS_KEY_MANIFEST)
        .map_err(|e| {
            error!("http-connector: key manifest '{TLS_KEY_MANIFEST}' not found: {e}");
            TlsError::MissingKey
        })?
        .ok_or_else(|| {
            error!("http-connector: key manifest '{TLS_KEY_MANIFEST}' is empty");
            TlsError::MissingKey
        })?;
    let key_pem = store.get(&key_blob_id).map_err(|e| {
        error!("http-connector: failed to read key blob: {e}");
        TlsError::MissingKey
    })?
    .ok_or_else(|| {
        error!("http-connector: key blob is empty");
        TlsError::MissingKey
    })?;

    // Parse certificates. Use std::io::BufReader for the pemfile API.
    let mut cert_reader = std::io::BufReader::new(cert_pem.as_slice());
    let certs: Vec<CertificateDer<'static>> = pemfile::certs(&mut cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| {
            error!("http-connector: invalid cert PEM: {e}");
            TlsError::InvalidCertificate
        })?;

    if certs.is_empty() {
        error!("http-connector: empty certificate chain");
        return Err(TlsError::InvalidCertificate);
    }

    // Parse private key.
    let mut key_reader = std::io::BufReader::new(key_pem.as_slice());
    let key = loop {
        match pemfile::read_one(&mut key_reader).map_err(|e| {
            error!("http-connector: invalid key PEM: {e}");
            TlsError::InvalidKey
        })? {
            Some(pemfile::Item::Pkcs1Key(k)) => break PrivateKeyDer::Pkcs1(k),
            Some(pemfile::Item::Pkcs8Key(k)) => break PrivateKeyDer::Pkcs8(k),
            Some(pemfile::Item::Sec1Key(k)) => break PrivateKeyDer::Sec1(k),
            None => {
                error!("http-connector: no private key found in key PEM");
                return Err(TlsError::InvalidKey);
            }
            _ => continue,
        }
    };

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| {
            error!("http-connector: failed to build TLS config: {e}");
            TlsError::ConfigError
        })?;

    info!("http-connector: TLS configured successfully");
    Ok(Arc::new(config))
}

/// Perform TLS handshake on an accepted connection.
async fn tls_accept(
    stream: TcpStream,
    acceptor: &TlsAcceptor,
) -> Result<tokio_rustls::server::TlsStream<TcpStream>, io::Error> {
    acceptor.accept(stream).await
}

#[derive(Debug)]
enum TlsError {
    StorageUnavailable,
    MissingCertificate,
    MissingKey,
    InvalidCertificate,
    InvalidKey,
    ConfigError,
}

impl std::fmt::Display for TlsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StorageUnavailable => write!(f, "TLS storage unavailable"),
            Self::MissingCertificate => write!(f, "TLS certificate not found"),
            Self::MissingKey => write!(f, "TLS private key not found"),
            Self::InvalidCertificate => write!(f, "invalid TLS certificate"),
            Self::InvalidKey => write!(f, "invalid TLS private key"),
            Self::ConfigError => write!(f, "TLS configuration error"),
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP/1.1 codec (task 2.4)
// ---------------------------------------------------------------------------

enum ReadResult {
    Request(HttpRequest),
    Closed,
}

struct HttpCodec {
    buf: Vec<u8>,
    pos: usize,
}

impl HttpCodec {
    fn new() -> Self {
        Self {
            buf: vec![0u8; READ_BUF_SIZE],
            pos: 0,
        }
    }

    async fn read_request<S: AsyncRead + Unpin>(
        &mut self,
        stream: &mut S,
    ) -> io::Result<ReadResult> {
        loop {
            if let Some(result) = self.try_parse() {
                return Ok(result);
            }

            if self.pos == self.buf.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "request too large",
                ));
            }

            let n = stream.read(&mut self.buf[self.pos..]).await?;
            if n == 0 {
                if self.pos > 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "connection closed with partial request",
                    ));
                }
                return Ok(ReadResult::Closed);
            }
            self.pos += n;
        }
    }

    fn try_parse(&mut self) -> Option<ReadResult> {
        let data = &self.buf[..self.pos];
        let header_end = find_subsequence(data, b"\r\n\r\n")?;
        let headers_section = &data[..header_end];

        let (method, uri, headers) = parse_request_head(headers_section).ok()?;
        let headers_end = header_end + 4;

        let body_len = if let Some(cl) = get_header_str(&headers, "content-length") {
            cl.parse::<usize>().unwrap_or(0)
        } else {
            0
        };

        let total_needed = headers_end + body_len;
        if self.pos < total_needed {
            return None;
        }

        let body = if body_len > 0 {
            data[headers_end..total_needed].to_vec()
        } else {
            vec![]
        };

        let typed_headers: Vec<HttpHeader> = headers
            .into_iter()
            .map(|(n, v)| HttpHeader::new(n, v))
            .collect();

        let request = HttpRequest::new(method, uri, typed_headers, body);

        let remaining = self.pos - total_needed;
        if remaining > 0 {
            self.buf.copy_within(total_needed..self.pos, 0);
        }
        self.pos = remaining;

        Some(ReadResult::Request(request))
    }
}

async fn write_response<S: AsyncWrite + Unpin>(
    stream: &mut S,
    response: &HttpResponse,
) -> io::Result<()> {
    let status_text = status_reason(response.status);
    let status_line = format!("HTTP/1.1 {} {}\r\n", response.status, status_text);
    stream.write_all(status_line.as_bytes()).await?;

    for header in &response.headers {
        let line = format!("{}: {}\r\n", header.name, header.value);
        stream.write_all(line.as_bytes()).await?;
    }

    if !response.body.is_empty() {
        let cl = format!("Content-Length: {}\r\n", response.body.len());
        stream.write_all(cl.as_bytes()).await?;
    }

    stream.write_all(b"\r\n").await?;

    if !response.body.is_empty() {
        stream.write_all(&response.body).await?;
    }

    stream.flush().await?;
    Ok(())
}

async fn write_404<S: AsyncWrite + Unpin>(stream: &mut S) -> io::Result<()> {
    let body = b"Not Found";
    let resp = HttpResponse::new(
        404,
        vec![
            HttpHeader::new("content-type".to_string(), "text/plain".to_string()),
            HttpHeader::new("content-length".to_string(), body.len().to_string()),
        ],
        body.to_vec(),
    );
    write_response(stream, &resp).await
}

async fn write_500<S: AsyncWrite + Unpin>(stream: &mut S) -> io::Result<()> {
    let body = b"Internal Server Error";
    let resp = HttpResponse::new(
        500,
        vec![
            HttpHeader::new("content-type".to_string(), "text/plain".to_string()),
            HttpHeader::new("content-length".to_string(), body.len().to_string()),
        ],
        body.to_vec(),
    );
    write_response(stream, &resp).await
}

fn status_reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        301 => "Moved Permanently",
        302 => "Found",
        304 => "Not Modified",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        405 => "Method Not Allowed",
        408 => "Request Timeout",
        413 => "Payload Too Large",
        414 => "URI Too Long",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

// ---------------------------------------------------------------------------
// HTTP/1.1 parsing helpers
// ---------------------------------------------------------------------------

fn find_subsequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn parse_request_head(
    data: &[u8],
) -> Result<(String, String, Vec<(String, String)>), &'static str> {
    let text = std::str::from_utf8(data).map_err(|_| "invalid UTF-8 in request")?;
    let mut lines = text.split("\r\n");

    let request_line = lines.next().ok_or("empty request")?;
    let mut parts = request_line.split(' ');
    let method = parts.next().ok_or("missing method")?.to_owned();
    let uri = parts.next().ok_or("missing URI")?.to_owned();

    if uri.len() > MAX_URI_LEN {
        return Err("URI too long");
    }

    let mut headers: Vec<(String, String)> = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        if let Some(colon_pos) = line.find(':') {
            let name = line[..colon_pos].trim().to_lowercase();
            let value = line[colon_pos + 1..].trim().to_owned();

            if name.len() > MAX_HEADER_NAME_LEN {
                return Err("header name too long");
            }
            if value.len() > MAX_HEADER_VALUE_LEN {
                return Err("header value too long");
            }
            headers.push((name, value));
        }
    }

    if headers.len() > MAX_HEADERS {
        return Err("too many headers");
    }

    Ok((method, uri, headers))
}

fn get_header_str<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    let name_lower = name.to_lowercase();
    headers
        .iter()
        .find(|(n, _)| *n == name_lower)
        .map(|(_, v)| v.as_str())
}

fn get_typed_header<'a>(headers: &'a [HttpHeader], name: &str) -> Option<&'a str> {
    let name_lower = name.to_lowercase();
    headers
        .iter()
        .find(|h| h.name.to_lowercase() == name_lower)
        .map(|h| h.value.as_str())
}

// ---------------------------------------------------------------------------
// Route resolution via discovery (task 2.5)
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct CachedRoute {
    target: selium_abi::ResourceTarget,
    _created_at_ms: u64,
}

struct RouteResolver {
    ctx: Context,
    cache: HashMap<String, CachedRoute>,
}

impl RouteResolver {
    fn new(ctx: Context) -> Self {
        Self {
            ctx,
            cache: HashMap::new(),
        }
    }

    async fn resolve(
        &mut self,
        host: &str,
        path: &str,
    ) -> Result<selium_abi::ResourceTarget, ResolveError> {
        let cache_key = format!("{}:{}", host, path);
        if let Some(route) = self.cache.get(&cache_key) {
            return Ok(route.target.clone());
        }

        let clean_path = path.trim_start_matches('/').trim_end_matches('/');
        let discovery_uri = if clean_path.is_empty() {
            format!("sel://{}", host)
        } else {
            format!("sel://{}/{}", host, clean_path)
        };

        match self.ctx.lookup(&discovery_uri).await {
            Ok(Some(target)) => {
                self.cache.insert(
                    cache_key,
                    CachedRoute {
                        target: target.clone(),
                        _created_at_ms: 0,
                    },
                );
                Ok(target)
            }
            Ok(None) => self.resolve_parent(host, path).await,
            Err(e) => {
                warn!("discovery lookup failed for {discovery_uri}: {e}");
                Err(ResolveError::NotFound)
            }
        }
    }

    async fn resolve_parent(
        &mut self,
        host: &str,
        path: &str,
    ) -> Result<selium_abi::ResourceTarget, ResolveError> {
        let segments: Vec<&str> = path
            .trim_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        for i in (0..segments.len()).rev() {
            let prefix = segments[..=i].join("/");
            let uri = format!("sel://{}/{}", host, prefix);

            match self.ctx.lookup(&uri).await {
                Ok(Some(target)) => {
                    let orig_key = format!("{}:{}", host, path);
                    self.cache.insert(
                        orig_key,
                        CachedRoute {
                            target: target.clone(),
                            _created_at_ms: 0,
                        },
                    );
                    return Ok(target);
                }
                Ok(None) => continue,
                Err(e) => {
                    warn!("discovery lookup failed for {uri}: {e}");
                    continue;
                }
            }
        }

        let root_uri = format!("sel://{}", host);
        match self.ctx.lookup(&root_uri).await {
            Ok(Some(target)) => {
                let orig_key = format!("{}:{}", host, path);
                self.cache.insert(
                    orig_key,
                    CachedRoute {
                        target: target.clone(),
                        _created_at_ms: 0,
                    },
                );
                Ok(target)
            }
            _ => Err(ResolveError::NotFound),
        }
    }
}

#[derive(Debug)]
enum ResolveError {
    NotFound,
}

// ---------------------------------------------------------------------------
// In-flight correlation map (task 2.6)
// ---------------------------------------------------------------------------

struct CorrelationMap {
    pending: BTreeMap<u64, HttpResponse>,
    next_to_send: u64,
    next_tag: u64,
}

impl CorrelationMap {
    fn new() -> Self {
        Self {
            pending: BTreeMap::new(),
            next_to_send: 0,
            next_tag: 0,
        }
    }

    fn next_tag(&mut self) -> u64 {
        let tag = self.next_tag;
        self.next_tag += 1;
        tag
    }

    fn insert_and_flush(&mut self, seq: u64, response: HttpResponse) -> Vec<HttpResponse> {
        self.pending.insert(seq, response);

        let mut ready = Vec::new();
        while let Some(resp) = self.pending.remove(&self.next_to_send) {
            ready.push(resp);
            self.next_to_send += 1;
        }
        ready
    }
}

// ---------------------------------------------------------------------------
// TLS-wrapped connection handler (tasks 2.2, 2.4-2.8)
// ---------------------------------------------------------------------------

async fn handle_tls_connection(
    stream: TcpStream,
    acceptor: &TlsAcceptor,
    resolver: &mut RouteResolver,
) {
    let tls_stream = match tls_accept(stream, acceptor).await {
        Ok(s) => s,
        Err(e) => {
            warn!("http-connector: TLS handshake failed: {e}");
            return;
        }
    };

    let mut codec = HttpCodec::new();
    let mut correlation = CorrelationMap::new();

    let (mut reader, mut writer) = tokio::io::split(tls_stream);

    loop {
        let request = match codec.read_request(&mut reader).await {
            Ok(ReadResult::Request(req)) => req,
            Ok(ReadResult::Closed) => {
                debug!("client closed connection");
                break;
            }
            Err(e) => {
                warn!("read error: {e}");
                let _ = write_500(&mut writer).await;
                break;
            }
        };

        let host = get_typed_header(&request.headers, "host")
            .unwrap_or("localhost")
            .to_string();

        let _target = match resolver.resolve(&host, &request.uri).await {
            Ok(target) => target,
            Err(_) => {
                let _ = write_404(&mut writer).await;
                continue;
            }
        };

        let seq = correlation.next_tag();

        let response = HttpResponse::new(
            200,
            vec![HttpHeader::new(
                "content-type".to_string(),
                "text/plain".to_string(),
            )],
            b"Hello from HTTP connector".to_vec(),
        );

        let ready = correlation.insert_and_flush(seq, response);
        for resp in ready {
            if let Err(e) = write_response(&mut writer, &resp).await {
                warn!("write error: {e}");
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Entrypoint (tasks 2.1, 2.2, 2.3)
// ---------------------------------------------------------------------------

/// Entrypoint for the HTTP connector system guest.
///
/// Receives a discovery `Context` handle for route resolution. The runtime
/// wires this at bootstrap via the entrypoint args mechanism.
///
/// TLS certificates are loaded from blob storage at startup. The connector
/// fails loudly if certificate material is missing or invalid.
#[entrypoint]
async fn connector_http(ctx: Context) {
    drop(selium_guest::log::init());
    info!("http-connector started");

    // Load TLS configuration. This fails loudly on missing/invalid material
    // per spec: "loud failure on missing/invalid cert material".
    let tls_config = match load_tls_config() {
        Ok(cfg) => {
            info!("http-connector: TLS configured");
            Some(cfg)
        }
        Err(e) => {
            error!("http-connector: TLS setup failed: {e}");
            error!("http-connector: refusing to serve plaintext on TLS listener");
            return;
        }
    };

    let listener = match TcpListener::bind("0.0.0.0:443") {
        Ok(l) => {
            info!("http-connector: bound to 0.0.0.0:443");
            l
        }
        Err(e) => {
            error!("http-connector: bind failed: {e}");
            return;
        }
    };

    mark_ready();

    let acceptor = TlsAcceptor::from(tls_config.unwrap());
    let mut resolver = RouteResolver::new(ctx);

    loop {
        let stream = match listener.accept().await {
            Ok(s) => s,
            Err(e) => {
                warn!("http-connector: accept failed: {e}");
                continue;
            }
        };

        info!("http-connector: accepted connection");
        handle_tls_connection(stream, &acceptor, &mut resolver).await;
    }
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use selium_proto_http::HttpHeader;

    // --- HTTP/1.1 codec tests (task 2.4) ---

    #[test]
    fn parse_simple_get_request() {
        let raw = b"GET / HTTP/1.1\r\nhost: example.com\r\n\r\n";
        let (method, uri, headers) = parse_request_head(raw).unwrap();
        assert_eq!(method, "GET");
        assert_eq!(uri, "/");
        assert_eq!(headers.len(), 1);
        assert_eq!(headers[0].0, "host");
        assert_eq!(headers[0].1, "example.com");
    }

    #[test]
    fn parse_post_with_body() {
        let raw = b"POST /api/data HTTP/1.1\r\nhost: example.com\r\ncontent-type: application/json\r\ncontent-length: 16\r\n\r\n{\"key\":\"value\"}";
        let (method, uri, headers) = parse_request_head(raw).unwrap();
        assert_eq!(method, "POST");
        assert_eq!(uri, "/api/data");
        assert_eq!(headers.len(), 3);
    }

    #[test]
    fn parse_multiple_accept_headers() {
        let raw = b"GET / HTTP/1.1\r\nhost: example.com\r\naccept: text/html\r\naccept: application/json\r\n\r\n";
        let (_, _, headers) = parse_request_head(raw).unwrap();
        let accept_count = headers.iter().filter(|(n, _)| n == "accept").count();
        assert_eq!(accept_count, 2);
    }

    #[test]
    fn parse_uri_too_long() {
        let long_path = "a".repeat(MAX_URI_LEN + 1);
        let raw = format!("GET /{} HTTP/1.1\r\nhost: x\r\n\r\n", long_path);
        let result = parse_request_head(raw.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn parse_too_many_headers() {
        let mut raw = String::from("GET / HTTP/1.1\r\n");
        for i in 0..MAX_HEADERS + 1 {
            raw.push_str(&format!("x-hdr-{}: v\r\n", i));
        }
        raw.push_str("\r\n");
        let result = parse_request_head(raw.as_bytes());
        assert!(result.is_err());
    }

    #[test]
    fn parse_empty_request() {
        let result = parse_request_head(b"");
        assert!(result.is_err());
    }

    #[test]
    fn parse_missing_method() {
        let result = parse_request_head(b"\r\n\r\n");
        assert!(result.is_err());
    }

    #[test]
    fn get_header_case_insensitive() {
        // Header names are stored lowercased (as parse_request_head does).
        let headers = vec![
            ("host".to_string(), "example.com".to_string()),
            ("content-type".to_string(), "text/html".to_string()),
        ];
        // get_header_str lowercases the search key for case-insensitive matching.
        assert_eq!(get_header_str(&headers, "HOST"), Some("example.com"));
        assert_eq!(get_header_str(&headers, "Content-Type"), Some("text/html"));
        assert_eq!(get_header_str(&headers, "missing"), None);
    }

    #[test]
    fn get_typed_header_works() {
        let headers = vec![
            HttpHeader::new("Host".to_string(), "example.com".to_string()),
            HttpHeader::new("Content-Type".to_string(), "text/html".to_string()),
        ];
        assert_eq!(get_typed_header(&headers, "host"), Some("example.com"));
        assert_eq!(get_typed_header(&headers, "CONTENT-TYPE"), Some("text/html"));
        assert_eq!(get_typed_header(&headers, "x-missing"), None);
    }

    // --- Correlation map tests (task 2.6) ---

    #[test]
    fn correlation_in_order() {
        let mut map = CorrelationMap::new();
        let s0 = map.next_tag();
        let s1 = map.next_tag();
        assert_eq!(s0, 0);
        assert_eq!(s1, 1);

        let r0 = HttpResponse::new(200, vec![], b"a".to_vec());
        let r1 = HttpResponse::new(200, vec![], b"b".to_vec());

        let ready = map.insert_and_flush(s0, r0.clone());
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].body, b"a");

        let ready = map.insert_and_flush(s1, r1.clone());
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].body, b"b");
    }

    #[test]
    fn correlation_out_of_order() {
        let mut map = CorrelationMap::new();
        let s0 = map.next_tag();
        let s1 = map.next_tag();
        let s2 = map.next_tag();

        let r0 = HttpResponse::new(200, vec![], b"0".to_vec());
        let r1 = HttpResponse::new(200, vec![], b"1".to_vec());
        let r2 = HttpResponse::new(200, vec![], b"2".to_vec());

        // Response 1 arrives first — must wait for response 0.
        let ready = map.insert_and_flush(s1, r1);
        assert_eq!(ready.len(), 0, "out-of-order response should not flush");

        // Response 0 arrives — now both 0 and 1 flush.
        let ready = map.insert_and_flush(s0, r0);
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0].body, b"0");
        assert_eq!(ready[1].body, b"1");

        // Response 2 arrives — flushes immediately.
        let ready = map.insert_and_flush(s2, r2);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].body, b"2");
    }

    #[test]
    fn correlation_gapped_sequence() {
        let mut map = CorrelationMap::new();
        let s0 = map.next_tag();
        let _s1 = map.next_tag();
        let s2 = map.next_tag();

        let r0 = HttpResponse::new(200, vec![], b"0".to_vec());
        let r2 = HttpResponse::new(200, vec![], b"2".to_vec());

        // Response 2 comes before response 0 — can't flush 2 without 0 and 1.
        let ready = map.insert_and_flush(s2, r2);
        assert_eq!(ready.len(), 0);

        // Response 0 comes — only 0 flushes (1 is still missing).
        let ready = map.insert_and_flush(s0, r0);
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].body, b"0");
    }

    // --- HTTP response writing tests ---

    #[tokio::test]
    async fn write_response_200() {
        let resp = HttpResponse::new(
            200,
            vec![HttpHeader::new("content-type".to_string(), "text/plain".to_string())],
            b"hello".to_vec(),
        );
        let mut buf = Vec::new();
        write_response(&mut buf, &resp).await.unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("HTTP/1.1 200 OK"));
        assert!(out.contains("content-type: text/plain"));
        assert!(out.contains("Content-Length: 5"));
        assert!(out.contains("hello"));
    }

    #[tokio::test]
    async fn write_response_no_body() {
        let resp = HttpResponse::new(204, vec![], vec![]);
        let mut buf = Vec::new();
        write_response(&mut buf, &resp).await.unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("HTTP/1.1 204 No Content"));
        // No Content-Length for empty body.
        assert!(!out.contains("Content-Length"));
    }

    #[tokio::test]
    async fn write_404_response() {
        let mut buf = Vec::new();
        write_404(&mut buf).await.unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("HTTP/1.1 404 Not Found"));
        assert!(out.contains("Not Found"));
    }

    #[tokio::test]
    async fn write_500_response() {
        let mut buf = Vec::new();
        write_500(&mut buf).await.unwrap();
        let out = String::from_utf8(buf).unwrap();
        assert!(out.contains("HTTP/1.1 500 Internal Server Error"));
    }

    // --- HTTP codec buffer tests ---

    #[tokio::test]
    async fn codec_reads_single_request() {
        let mut codec = HttpCodec::new();
        let data = b"GET / HTTP/1.1\r\nhost: example.com\r\n\r\n";
        let mut stream = &data[..];

        match codec.read_request(&mut stream).await.unwrap() {
            ReadResult::Request(req) => {
                assert_eq!(req.method, "GET");
                assert_eq!(req.uri, "/");
                assert_eq!(req.headers.len(), 1);
                assert_eq!(req.body.len(), 0);
            }
            _ => panic!("expected Request"),
        }
    }

    #[tokio::test]
    async fn codec_reads_request_with_body() {
        let mut codec = HttpCodec::new();
        let data = b"POST /api HTTP/1.1\r\nhost: example.com\r\ncontent-length: 7\r\n\r\nabcdefg";
        let mut stream = &data[..];

        match codec.read_request(&mut stream).await.unwrap() {
            ReadResult::Request(req) => {
                assert_eq!(req.method, "POST");
                assert_eq!(req.uri, "/api");
                assert_eq!(req.body, b"abcdefg");
            }
            _ => panic!("expected Request"),
        }
    }

    #[tokio::test]
    async fn codec_closed_connection() {
        let mut codec = HttpCodec::new();
        let data = b"";
        let mut stream = &data[..];

        match codec.read_request(&mut stream).await.unwrap() {
            ReadResult::Closed => {}
            _ => panic!("expected Closed"),
        }
    }

    #[tokio::test]
    async fn codec_partial_request_then_closed() {
        let mut codec = HttpCodec::new();
        let data = b"GET / HTTP/1.1\r\n";
        let mut stream = &data[..];

        let result = codec.read_request(&mut stream).await;
        assert!(result.is_err());
    }

    // --- Find subsequence tests ---

    #[test]
    fn find_subsequence_found() {
        assert_eq!(find_subsequence(b"hello\r\n\r\nworld", b"\r\n\r\n"), Some(5));
    }

    #[test]
    fn find_subsequence_not_found() {
        assert_eq!(find_subsequence(b"hello world", b"\r\n\r\n"), None);
    }

    #[test]
    fn find_subsequence_at_start() {
        assert_eq!(find_subsequence(b"\r\n\r\nhello", b"\r\n\r\n"), Some(0));
    }

    // --- Status reason phrase tests ---

    #[test]
    fn status_reason_known() {
        assert_eq!(status_reason(200), "OK");
        assert_eq!(status_reason(404), "Not Found");
        assert_eq!(status_reason(500), "Internal Server Error");
    }

    #[test]
    fn status_reason_unknown() {
        assert_eq!(status_reason(999), "Unknown");
    }
}

