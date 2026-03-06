//! Protocol-neutral network SPI contracts.

use std::{collections::HashSet, future::Future, pin::Pin, sync::Arc};

use selium_abi::{
    InteractionKind, NetworkAcceptResult, NetworkConnect, NetworkListen, NetworkProtocol,
    NetworkRpcAcceptResult, NetworkRpcAwait, NetworkRpcBodyRead, NetworkRpcBodyReadResult,
    NetworkRpcBodyWrite, NetworkRpcInvoke, NetworkRpcInvokeResult, NetworkRpcRequestHead,
    NetworkRpcRespond, NetworkRpcRespondResult, NetworkRpcResponseHead, NetworkRpcResponseResult,
    NetworkStatus, NetworkStatusCode, NetworkStreamRecv, NetworkStreamRecvResult,
    NetworkStreamResult, NetworkStreamSend,
};

use crate::guest_error::GuestError;

/// Boxed future returned by runtime network capabilities.
pub type NetworkFuture<T, E> = Pin<Box<dyn Future<Output = Result<T, E>> + Send + 'static>>;

/// Per-process network grants attached to a guest instance.
#[derive(Debug, Clone, Default)]
pub struct NetworkProcessPolicy {
    egress_profiles: HashSet<String>,
    ingress_bindings: HashSet<String>,
}

impl NetworkProcessPolicy {
    /// Construct policy from granted profile and binding names.
    pub fn new(
        egress_profiles: impl IntoIterator<Item = String>,
        ingress_bindings: impl IntoIterator<Item = String>,
    ) -> Self {
        Self {
            egress_profiles: egress_profiles.into_iter().collect(),
            ingress_bindings: ingress_bindings.into_iter().collect(),
        }
    }

    /// Return whether the process may use the named egress profile.
    pub fn allows_egress_profile(&self, name: &str) -> bool {
        self.egress_profiles.contains(name)
    }

    /// Return whether the process may use the named ingress binding.
    pub fn allows_ingress_binding(&self, name: &str) -> bool {
        self.ingress_bindings.contains(name)
    }
}

/// Listener resource returned by a runtime network backend.
#[derive(Debug, Clone)]
pub struct ListenerHandle<L> {
    /// Served protocol.
    pub protocol: NetworkProtocol,
    /// Supported interaction modes.
    pub interactions: Vec<InteractionKind>,
    /// Runtime-specific listener state.
    pub inner: L,
}

/// Protocol session resource returned by a runtime network backend.
#[derive(Debug, Clone)]
pub struct SessionHandle<S> {
    /// Session protocol.
    pub protocol: NetworkProtocol,
    /// Supported interaction modes.
    pub interactions: Vec<InteractionKind>,
    /// Runtime-specific session state.
    pub inner: S,
}

/// Stream resource returned by a runtime network backend.
#[derive(Debug, Clone)]
pub struct StreamHandle<S> {
    /// Runtime-specific stream state.
    pub inner: S,
}

/// RPC body reader returned by a runtime network backend.
#[derive(Debug, Clone)]
pub struct RpcBodyReaderHandle<R> {
    /// Runtime-specific body reader state.
    pub inner: R,
}

/// RPC body writer returned by a runtime network backend.
#[derive(Debug, Clone)]
pub struct RpcBodyWriterHandle<W> {
    /// Runtime-specific body writer state.
    pub inner: W,
}

/// Runtime result of accepting a protocol session.
pub struct AcceptedSession<S> {
    /// Operation status.
    pub code: NetworkStatusCode,
    /// Accepted session when available.
    pub session: Option<SessionHandle<S>>,
}

/// Runtime result of opening or accepting a stream.
pub struct AcceptedStream<S> {
    /// Operation status.
    pub code: NetworkStatusCode,
    /// Stream when available.
    pub stream: Option<StreamHandle<S>>,
}

/// Runtime result of accepting an inbound RPC.
pub struct AcceptedRpc<X, R> {
    /// Operation status.
    pub code: NetworkStatusCode,
    /// Runtime-specific exchange state when available.
    pub exchange: Option<X>,
    /// Accepted request payload.
    pub request: Option<NetworkRpcRequestHead>,
    /// Accepted request body reader when available.
    pub request_body: Option<RpcBodyReaderHandle<R>>,
}

/// Runtime result of starting an outbound RPC exchange.
pub struct StartedRpc<X, W> {
    /// Operation status.
    pub code: NetworkStatusCode,
    /// Client exchange when available.
    pub exchange: Option<X>,
    /// Request body writer when available.
    pub request_body: Option<RpcBodyWriterHandle<W>>,
}

/// Runtime result of awaiting an outbound RPC response head.
pub struct AwaitedRpc<R> {
    /// Operation status.
    pub code: NetworkStatusCode,
    /// Response head when available.
    pub response: Option<NetworkRpcResponseHead>,
    /// Response body reader when available.
    pub response_body: Option<RpcBodyReaderHandle<R>>,
}

/// Runtime result of writing an inbound RPC response head.
pub struct RespondedRpc<W> {
    /// Operation status.
    pub code: NetworkStatusCode,
    /// Response body writer when available.
    pub response_body: Option<RpcBodyWriterHandle<W>>,
}

/// Capability responsible for guest network operations.
pub trait NetworkCapability {
    /// Runtime-specific error type.
    type Error: Into<GuestError>;
    /// Runtime-specific listener state.
    type Listener: Clone + Send + Sync + 'static;
    /// Runtime-specific protocol session state.
    type Session: Clone + Send + Sync + 'static;
    /// Runtime-specific stream state.
    type Stream: Clone + Send + Sync + 'static;
    /// Runtime-specific outbound RPC client exchange state.
    type RpcClientExchange: Send + 'static;
    /// Runtime-specific RPC exchange state.
    type RpcExchange: Send + 'static;
    /// Runtime-specific RPC body reader state.
    type RpcBodyReader: Clone + Send + Sync + 'static;
    /// Runtime-specific RPC body writer state.
    type RpcBodyWriter: Clone + Send + Sync + 'static;

    /// Open a runtime-managed ingress binding.
    fn listen(
        &self,
        policy: Arc<NetworkProcessPolicy>,
        input: NetworkListen,
    ) -> NetworkFuture<ListenerHandle<Self::Listener>, Self::Error>;

    /// Open an outbound protocol session.
    fn connect(
        &self,
        policy: Arc<NetworkProcessPolicy>,
        input: NetworkConnect,
    ) -> NetworkFuture<SessionHandle<Self::Session>, Self::Error>;

    /// Accept an inbound protocol session from an existing listener.
    fn accept(
        &self,
        listener: &Self::Listener,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedSession<Self::Session>, Self::Error>;

    /// Open an outbound stream on an existing session.
    fn stream_open(
        &self,
        session: &Self::Session,
    ) -> NetworkFuture<AcceptedStream<Self::Stream>, Self::Error>;

    /// Accept an inbound stream on an existing session.
    fn stream_accept(
        &self,
        session: &Self::Session,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedStream<Self::Stream>, Self::Error>;

    /// Send one byte chunk on a stream.
    fn stream_send(
        &self,
        stream: &Self::Stream,
        input: NetworkStreamSend,
    ) -> NetworkFuture<NetworkStatus, Self::Error>;

    /// Receive one byte chunk from a stream.
    fn stream_recv(
        &self,
        stream: &Self::Stream,
        input: NetworkStreamRecv,
    ) -> NetworkFuture<NetworkStreamRecvResult, Self::Error>;

    /// Perform one buffered RPC on a session.
    fn rpc_invoke(
        &self,
        session: &Self::Session,
        input: NetworkRpcInvoke,
    ) -> NetworkFuture<StartedRpc<Self::RpcClientExchange, Self::RpcBodyWriter>, Self::Error>;

    /// Await the response head for an outbound RPC exchange.
    fn rpc_await(
        &self,
        exchange: Self::RpcClientExchange,
        input: NetworkRpcAwait,
    ) -> NetworkFuture<AwaitedRpc<Self::RpcBodyReader>, Self::Error>;

    /// Accept one inbound buffered RPC on a session.
    fn rpc_accept(
        &self,
        session: &Self::Session,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedRpc<Self::RpcExchange, Self::RpcBodyReader>, Self::Error>;

    /// Respond to one inbound buffered RPC.
    fn rpc_respond(
        &self,
        exchange: Self::RpcExchange,
        input: NetworkRpcRespond,
    ) -> NetworkFuture<RespondedRpc<Self::RpcBodyWriter>, Self::Error>;

    /// Read one byte chunk from an RPC body reader.
    fn rpc_body_read(
        &self,
        body: &Self::RpcBodyReader,
        input: NetworkRpcBodyRead,
    ) -> NetworkFuture<NetworkRpcBodyReadResult, Self::Error>;

    /// Write one byte chunk to an RPC body writer.
    fn rpc_body_write(
        &self,
        body: &Self::RpcBodyWriter,
        input: NetworkRpcBodyWrite,
    ) -> NetworkFuture<NetworkStatus, Self::Error>;

    /// Close a listener resource.
    fn close_listener(
        &self,
        listener: ListenerHandle<Self::Listener>,
    ) -> NetworkFuture<NetworkStatus, Self::Error>;

    /// Close a protocol session.
    fn close_session(
        &self,
        session: SessionHandle<Self::Session>,
    ) -> NetworkFuture<NetworkStatus, Self::Error>;

    /// Close a stream resource.
    fn close_stream(
        &self,
        stream: StreamHandle<Self::Stream>,
    ) -> NetworkFuture<NetworkStatus, Self::Error>;

    /// Close an inbound RPC exchange without responding.
    fn close_rpc_exchange(
        &self,
        exchange: Self::RpcExchange,
    ) -> NetworkFuture<NetworkStatus, Self::Error>;

    /// Close an outbound RPC client exchange.
    fn close_rpc_client_exchange(
        &self,
        exchange: Self::RpcClientExchange,
    ) -> NetworkFuture<NetworkStatus, Self::Error>;

    /// Close an RPC body reader.
    fn close_rpc_body_reader(
        &self,
        body: RpcBodyReaderHandle<Self::RpcBodyReader>,
    ) -> NetworkFuture<NetworkStatus, Self::Error>;

    /// Close an RPC body writer.
    fn close_rpc_body_writer(
        &self,
        body: RpcBodyWriterHandle<Self::RpcBodyWriter>,
    ) -> NetworkFuture<NetworkStatus, Self::Error>;
}

impl<T> NetworkCapability for Arc<T>
where
    T: NetworkCapability,
{
    type Error = T::Error;
    type Listener = T::Listener;
    type Session = T::Session;
    type Stream = T::Stream;
    type RpcClientExchange = T::RpcClientExchange;
    type RpcExchange = T::RpcExchange;
    type RpcBodyReader = T::RpcBodyReader;
    type RpcBodyWriter = T::RpcBodyWriter;

    fn listen(
        &self,
        policy: Arc<NetworkProcessPolicy>,
        input: NetworkListen,
    ) -> NetworkFuture<ListenerHandle<Self::Listener>, Self::Error> {
        self.as_ref().listen(policy, input)
    }

    fn connect(
        &self,
        policy: Arc<NetworkProcessPolicy>,
        input: NetworkConnect,
    ) -> NetworkFuture<SessionHandle<Self::Session>, Self::Error> {
        self.as_ref().connect(policy, input)
    }

    fn accept(
        &self,
        listener: &Self::Listener,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedSession<Self::Session>, Self::Error> {
        self.as_ref().accept(listener, timeout_ms)
    }

    fn stream_open(
        &self,
        session: &Self::Session,
    ) -> NetworkFuture<AcceptedStream<Self::Stream>, Self::Error> {
        self.as_ref().stream_open(session)
    }

    fn stream_accept(
        &self,
        session: &Self::Session,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedStream<Self::Stream>, Self::Error> {
        self.as_ref().stream_accept(session, timeout_ms)
    }

    fn stream_send(
        &self,
        stream: &Self::Stream,
        input: NetworkStreamSend,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        self.as_ref().stream_send(stream, input)
    }

    fn stream_recv(
        &self,
        stream: &Self::Stream,
        input: NetworkStreamRecv,
    ) -> NetworkFuture<NetworkStreamRecvResult, Self::Error> {
        self.as_ref().stream_recv(stream, input)
    }

    fn rpc_invoke(
        &self,
        session: &Self::Session,
        input: NetworkRpcInvoke,
    ) -> NetworkFuture<StartedRpc<Self::RpcClientExchange, Self::RpcBodyWriter>, Self::Error> {
        self.as_ref().rpc_invoke(session, input)
    }

    fn rpc_await(
        &self,
        exchange: Self::RpcClientExchange,
        input: NetworkRpcAwait,
    ) -> NetworkFuture<AwaitedRpc<Self::RpcBodyReader>, Self::Error> {
        self.as_ref().rpc_await(exchange, input)
    }

    fn rpc_accept(
        &self,
        session: &Self::Session,
        timeout_ms: u32,
    ) -> NetworkFuture<AcceptedRpc<Self::RpcExchange, Self::RpcBodyReader>, Self::Error> {
        self.as_ref().rpc_accept(session, timeout_ms)
    }

    fn rpc_respond(
        &self,
        exchange: Self::RpcExchange,
        input: NetworkRpcRespond,
    ) -> NetworkFuture<RespondedRpc<Self::RpcBodyWriter>, Self::Error> {
        self.as_ref().rpc_respond(exchange, input)
    }

    fn rpc_body_read(
        &self,
        body: &Self::RpcBodyReader,
        input: NetworkRpcBodyRead,
    ) -> NetworkFuture<NetworkRpcBodyReadResult, Self::Error> {
        self.as_ref().rpc_body_read(body, input)
    }

    fn rpc_body_write(
        &self,
        body: &Self::RpcBodyWriter,
        input: NetworkRpcBodyWrite,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        self.as_ref().rpc_body_write(body, input)
    }

    fn close_listener(
        &self,
        listener: ListenerHandle<Self::Listener>,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        self.as_ref().close_listener(listener)
    }

    fn close_session(
        &self,
        session: SessionHandle<Self::Session>,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        self.as_ref().close_session(session)
    }

    fn close_stream(
        &self,
        stream: StreamHandle<Self::Stream>,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        self.as_ref().close_stream(stream)
    }

    fn close_rpc_exchange(
        &self,
        exchange: Self::RpcExchange,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        self.as_ref().close_rpc_exchange(exchange)
    }

    fn close_rpc_client_exchange(
        &self,
        exchange: Self::RpcClientExchange,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        self.as_ref().close_rpc_client_exchange(exchange)
    }

    fn close_rpc_body_reader(
        &self,
        body: RpcBodyReaderHandle<Self::RpcBodyReader>,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        self.as_ref().close_rpc_body_reader(body)
    }

    fn close_rpc_body_writer(
        &self,
        body: RpcBodyWriterHandle<Self::RpcBodyWriter>,
    ) -> NetworkFuture<NetworkStatus, Self::Error> {
        self.as_ref().close_rpc_body_writer(body)
    }
}

impl<S> From<AcceptedSession<S>> for NetworkAcceptResult {
    fn from(value: AcceptedSession<S>) -> Self {
        Self {
            code: value.code,
            session: None,
        }
    }
}

impl<S> From<AcceptedStream<S>> for NetworkStreamResult {
    fn from(value: AcceptedStream<S>) -> Self {
        Self {
            code: value.code,
            stream: None,
        }
    }
}

impl<X, R> From<AcceptedRpc<X, R>> for NetworkRpcAcceptResult {
    fn from(value: AcceptedRpc<X, R>) -> Self {
        Self {
            code: value.code,
            exchange: None,
            request: value.request,
            request_body: None,
        }
    }
}

impl<X, W> From<StartedRpc<X, W>> for NetworkRpcInvokeResult {
    fn from(value: StartedRpc<X, W>) -> Self {
        Self {
            code: value.code,
            exchange: None,
            request_body: None,
        }
    }
}

impl<R> From<AwaitedRpc<R>> for NetworkRpcResponseResult {
    fn from(value: AwaitedRpc<R>) -> Self {
        Self {
            code: value.code,
            response: value.response,
            response_body: None,
        }
    }
}

impl<W> From<RespondedRpc<W>> for NetworkRpcRespondResult {
    fn from(value: RespondedRpc<W>) -> Self {
        Self {
            code: value.code,
            response_body: None,
        }
    }
}
