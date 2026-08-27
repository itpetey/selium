//! Typed HTTP serve API for application guests.
//!
//! This module provides the app-guest side of the HTTP connector:
//! register a URI subtree with discovery, accept typed RPC connections,
//! and handle `HttpRequest` → `HttpResponse` in a loop.
//!
//! ## Capability Model
//!
//! App guests using this API require **zero `Network` grants** — their
//! entire attack surface is channel attach. The HTTP connector terminates
//! TCP/TLS at the edge; plaintext crosses only capability-gated
//! shared-memory channels.
//!
//! The recommended grant is `ExplicitResource` scoped to the per-connection
//! channel region. Broad `UriPrefix` shared-memory grants widen exposure
//! and are documented as an anti-pattern for connector-served channels.
//!
//! ## Example
//!
//! ```ignore
//! use selium_connector_http::serve::HttpServe;
//! use selium_guest::{entrypoint, Context};
//!
//! #[entrypoint]
//! async fn my_app(mut ctx: Context) {
//!     let mut serve = HttpServe::bind(&mut ctx, "sel://example.com/api")
//!         .await
//!         .expect("bind failed");
//!
//!     while let Ok(mut conn) = serve.accept().await {
//!         while let Ok(req) = conn.recv().await {
//!             let typed = req.payload().unwrap();
//!             let response = HttpResponse::from_str(200, vec![], vec![]);
//!             req.reply(response).await.unwrap();
//!         }
//!     }
//! }
//! ```

use selium_abi::{InterfaceMetadata, ResourceTarget};
use selium_guest::{Context, GuestError, ResourceListener};
use selium_proto_http::{HttpHeader, HttpRequest, HttpResponse, HttpStreamItem};
use selium_shm::rpc::{self, RpcConnection, RpcError};

/// Interface marker for streamed HTTP serving.
///
/// [`HttpServeStream::bind`] registers this interface with discovery; the
/// connector routes matching requests through server-streaming RPC so
/// response bodies stream to the wire as chunked transfer encoding.
pub const HTTP_STREAM_INTERFACE: &str = crate::HTTP_STREAM_INTERFACE;

/// A typed HTTP serve handle.
///
/// Wraps a `ResourceListener` and discovery registration for a URI subtree.
/// Each accepted connection is a typed `RpcConnection<HttpRequest, HttpResponse>`
/// that carries schema-encoded HTTP messages.
pub struct HttpServe {
    listener: ResourceListener,
    uri: String,
}

impl HttpServe {
    /// Bind to a URI subtree and register it with discovery.
    ///
    /// The `uri` should be in the form `sel://<host>/<prefix>` (e.g.
    /// `sel://my-app/api`). The runtime allocates a host queue for the
    /// listener and registers the URI→queue mapping with the discovery
    /// service.
    ///
    /// The guest requires a channel attach grant but **no `Network` grant**
    /// — networking is handled by the connector.
    pub async fn bind(ctx: &mut Context, uri: &str) -> Result<Self, GuestError> {
        // Allocate a host queue for incoming connections (synchronous).
        let listener = ResourceListener::create()
            .map_err(|e| GuestError::Host(format!("create listener: {e}")))?;

        // Register the URI subtree with discovery.
        let target = ResourceTarget {
            uri: uri.to_string(),
            host_id: String::new(),
            resource_id: listener.descriptor().shared_id,
            interface: None,
            tenant: None,
        };
        ctx.register(uri, target).await?;

        Ok(Self {
            listener,
            uri: uri.to_string(),
        })
    }

    /// Accept an incoming typed HTTP connection.
    ///
    /// Blocks until an incoming connection arrives from the connector,
    /// then builds a typed `RpcConnection<HttpRequest, HttpResponse>` over
    /// the shared-memory ring channel.
    pub async fn accept(&mut self) -> Result<HttpConnection, HttpServeError> {
        let incoming = self
            .listener
            .recv()
            .await
            .map_err(|e| HttpServeError::Accept(format!("recv: {e}")))?;

        let conn = rpc::accept::<HttpRequest, HttpResponse>(incoming.into())
            .map_err(HttpServeError::Rpc)?;

        Ok(HttpConnection { conn })
    }

    /// Returns the URI subtree this handle is bound to.
    pub fn uri(&self) -> &str {
        &self.uri
    }
}

/// A single typed HTTP connection from the connector.
///
/// Wraps `RpcConnection<HttpRequest, HttpResponse>` for per-connection
/// channel hygiene. Each connection carries one request at a time with
/// tag-based correlation preserved end-to-end.
pub struct HttpConnection {
    conn: RpcConnection<HttpRequest, HttpResponse>,
}

impl HttpConnection {
    /// Receive the next HTTP request on this connection.
    ///
    /// Returns an `HttpRequestHandle` that provides:
    /// - `payload()` / `into_payload()`: decode the typed `HttpRequest`
    /// - `reply(response)`: send a typed `HttpResponse` with correct
    ///   tag correlation
    pub async fn recv(&mut self) -> Result<HttpRequestHandle<'_>, HttpServeError> {
        self.conn
            .recv()
            .await
            .map(|req| HttpRequestHandle { req })
            .map_err(|e| match e {
                RpcError::ConnectionClosed => HttpServeError::ConnectionClosed,
                other => HttpServeError::Rpc(other),
            })
    }

    /// Returns the client process ID (the connector's process ID).
    pub fn client_process_id(&self) -> u64 {
        self.conn.client_process_id()
    }
}

/// A received HTTP request with the ability to reply.
pub struct HttpRequestHandle<'a> {
    req: selium_shm::rpc::RpcRequest<'a, HttpRequest, HttpResponse>,
}

impl HttpRequestHandle<'_> {
    /// Decode the typed `HttpRequest` payload.
    pub fn payload(&self) -> Result<HttpRequest, HttpServeError> {
        self.req.payload().map_err(HttpServeError::Rpc)
    }

    /// Decode and consume the typed `HttpRequest` payload.
    pub fn into_payload(self) -> Result<HttpRequest, HttpServeError> {
        self.req.into_payload().map_err(HttpServeError::Rpc)
    }

    /// Access the raw payload bytes.
    pub fn payload_bytes(&self) -> &[u8] {
        self.req.payload_bytes()
    }

    /// Send a typed `HttpResponse` back through the connector.
    ///
    /// The response carries the correct correlation tag so the connector
    /// can match it to the original request on the wire.
    pub async fn reply(self, response: HttpResponse) -> Result<(), HttpServeError> {
        self.req.reply(response).await.map_err(HttpServeError::Rpc)
    }
}

/// Errors that can occur during typed HTTP serving.
#[derive(Debug)]
pub enum HttpServeError {
    /// Failed to accept an incoming connection.
    Accept(String),
    /// The remote connection was closed.
    ConnectionClosed,
    /// An RPC-level error occurred.
    Rpc(RpcError),
}

impl std::fmt::Display for HttpServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Accept(msg) => write!(f, "accept error: {msg}"),
            Self::ConnectionClosed => write!(f, "connection closed"),
            Self::Rpc(e) => write!(f, "RPC error: {e}"),
        }
    }
}

impl std::error::Error for HttpServeError {}

// ---------------------------------------------------------------------------
// Streaming serve API (server-streaming RPC; chunked/SSE response bodies)
// ---------------------------------------------------------------------------

/// A typed HTTP serve handle for **streamed** responses.
///
/// Like [`HttpServe`], but registers [`HTTP_STREAM_INTERFACE`] with
/// discovery so the connector establishes server-streaming sessions. The
/// app guest produces a response head, then body chunks (and optional
/// trailers), which the connector writes to the wire incrementally with
/// chunked transfer encoding — the edge never buffers the whole body.
///
/// Same capability model as [`HttpServe`]: zero `Network` grants required.
///
/// ## Example
///
/// ```ignore
/// use selium_connector_http::serve::HttpServeStream;
/// use selium_guest::entrypoint;
///
/// #[entrypoint]
/// async fn my_app(mut ctx: Context) {
///     let mut serve = HttpServeStream::bind(&mut ctx, "sel://example.com/events")
///         .await
///         .expect("bind failed");
///
///     while let Ok(mut conn) = serve.accept().await {
///         while let Ok(mut req) = conn.recv().await {
///             let _request = req.payload().unwrap();
///             req.send_head(200, vec![]).await.unwrap();
///             req.send_chunk(b"data: tick\n\n".to_vec()).await.unwrap();
///             req.finish().await.unwrap();
///         }
///     }
/// }
/// ```
pub struct HttpServeStream {
    listener: ResourceListener,
    uri: String,
}

impl HttpServeStream {
    /// Bind to a URI subtree and register it with discovery as a streamed
    /// HTTP route.
    ///
    /// The guest requires a channel attach grant but **no `Network`
    /// grant** — networking is handled by the connector.
    pub async fn bind(ctx: &mut Context, uri: &str) -> Result<Self, GuestError> {
        let listener = ResourceListener::create()
            .map_err(|e| GuestError::Host(format!("create listener: {e}")))?;

        let target = ResourceTarget {
            uri: uri.to_string(),
            host_id: String::new(),
            resource_id: listener.descriptor().shared_id,
            interface: Some(InterfaceMetadata {
                name: HTTP_STREAM_INTERFACE.to_string(),
                methods: Vec::new(),
            }),
            tenant: None,
        };
        ctx.register(uri, target).await?;

        Ok(Self {
            listener,
            uri: uri.to_string(),
        })
    }

    /// Accept an incoming streamed HTTP connection.
    pub async fn accept(&mut self) -> Result<HttpStreamConnection, HttpServeError> {
        let incoming = self
            .listener
            .recv()
            .await
            .map_err(|e| HttpServeError::Accept(format!("recv: {e}")))?;

        let conn = rpc::accept_server_stream::<HttpRequest, HttpStreamItem>(incoming.into())
            .map_err(HttpServeError::Rpc)?;

        Ok(HttpStreamConnection { conn })
    }

    /// Returns the URI subtree this handle is bound to.
    pub fn uri(&self) -> &str {
        &self.uri
    }
}

/// A single streamed HTTP connection from the connector.
pub struct HttpStreamConnection {
    conn: rpc::ServerStreamConnection<HttpRequest, HttpStreamItem>,
}

impl HttpStreamConnection {
    /// Receive the next HTTP request on this connection.
    pub async fn recv(&mut self) -> Result<HttpStreamRequestHandle<'_>, HttpServeError> {
        self.conn
            .recv()
            .await
            .map(|req| HttpStreamRequestHandle { req })
            .map_err(|e| match e {
                RpcError::ConnectionClosed => HttpServeError::ConnectionClosed,
                other => HttpServeError::Rpc(other),
            })
    }

    /// Returns the client process ID (the connector's process ID).
    pub fn client_process_id(&self) -> u64 {
        self.conn.client_process_id()
    }
}

/// A received HTTP request whose response is produced as a stream.
///
/// Response protocol: exactly one [`send_head`](Self::send_head) first,
/// then zero or more [`send_chunk`](Self::send_chunk) /
/// [`send_trailer`](Self::send_trailer) calls, then
/// [`finish`](Self::finish). The connector writes the head immediately
/// (chunked transfer encoding) and relays chunks to the wire as they are
/// produced — ring backpressure parks `send_chunk` when the client is
/// slow, so a slow consumer throttles the producer, not the edge buffer.
pub struct HttpStreamRequestHandle<'a> {
    req: rpc::ServerStreamRequest<'a, HttpRequest, HttpStreamItem>,
}

impl HttpStreamRequestHandle<'_> {
    /// Decode the typed `HttpRequest` payload.
    pub fn payload(&self) -> Result<HttpRequest, HttpServeError> {
        self.req.payload().map_err(HttpServeError::Rpc)
    }

    /// Decode and consume the typed `HttpRequest` payload.
    pub fn into_payload(self) -> Result<HttpRequest, HttpServeError> {
        self.req.into_payload().map_err(HttpServeError::Rpc)
    }

    /// Access the raw payload bytes.
    pub fn payload_bytes(&self) -> &[u8] {
        self.req.payload_bytes()
    }

    /// Send the response head (status + headers). Must be called exactly
    /// once before any chunks or trailers.
    pub async fn send_head(
        &mut self,
        status: u16,
        headers: Vec<HttpHeader>,
    ) -> Result<(), HttpServeError> {
        self.req
            .send_item(HttpStreamItem::head(status, headers))
            .await
            .map_err(HttpServeError::Rpc)
    }

    /// Send a body chunk to the client.
    pub async fn send_chunk(&mut self, data: Vec<u8>) -> Result<(), HttpServeError> {
        self.req
            .send_item(HttpStreamItem::chunk(data))
            .await
            .map_err(HttpServeError::Rpc)
    }

    /// Send a trailer header (written after the final chunk).
    pub async fn send_trailer(
        &mut self,
        name: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<(), HttpServeError> {
        self.req
            .send_item(HttpStreamItem::trailer(name, value))
            .await
            .map_err(HttpServeError::Rpc)
    }

    /// Signal end-of-stream. The connector terminates the chunked body.
    pub async fn finish(&mut self) -> Result<(), HttpServeError> {
        self.req.finish().await.map_err(HttpServeError::Rpc)
    }

    /// Terminate the stream with an application error.
    pub async fn send_error(&mut self, message: impl Into<String>) -> Result<(), HttpServeError> {
        self.req
            .send_error(message)
            .await
            .map_err(HttpServeError::Rpc)
    }

    /// Check whether the client cancelled the stream (call between chunks).
    pub fn check_cancel(&mut self) -> bool {
        self.req.check_cancel()
    }
}
