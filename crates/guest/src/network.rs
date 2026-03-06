//! Protocol-neutral guest network APIs.

use rkyv::Archive;
use selium_abi::{
    InteractionKind, NetworkAccept, NetworkAcceptResult, NetworkClose, NetworkConnect,
    NetworkListenerDescriptor, NetworkProtocol, NetworkRpcAccept, NetworkRpcAcceptResult,
    NetworkRpcAwait, NetworkRpcBodyRead, NetworkRpcBodyReadResult, NetworkRpcBodyReaderDescriptor,
    NetworkRpcBodyWrite, NetworkRpcBodyWriterDescriptor, NetworkRpcClientExchangeDescriptor,
    NetworkRpcExchangeDescriptor, NetworkRpcInvoke, NetworkRpcInvokeResult, NetworkRpcRequest,
    NetworkRpcRequestHead, NetworkRpcRespond, NetworkRpcRespondResult, NetworkRpcResponse,
    NetworkRpcResponseHead, NetworkRpcResponseResult, NetworkSessionDescriptor, NetworkStatus,
    NetworkStatusCode, NetworkStreamAccept, NetworkStreamChunk, NetworkStreamOpen,
    NetworkStreamRecv, NetworkStreamRecvResult, NetworkStreamResult, NetworkStreamSend,
};
use thiserror::Error;

use crate::driver::{DriverError, DriverFuture, RkyvDecoder, encode_args};

const LISTENER_CAPACITY: usize = 1024;
const SESSION_CAPACITY: usize = 1024;
const STATUS_CAPACITY: usize = core::mem::size_of::<<NetworkStatus as Archive>::Archived>();
const ACCEPT_CAPACITY: usize = 1024;
const STREAM_RESULT_CAPACITY: usize = 256;
const STREAM_RECV_CAPACITY: usize = 2048;
const RPC_INVOKE_CAPACITY: usize = 1024;
const RPC_RESPONSE_CAPACITY: usize = 2048;
const RPC_ACCEPT_CAPACITY: usize = 2048;
const RPC_RESPOND_CAPACITY: usize = 1024;
const RPC_BODY_READ_CAPACITY: usize = 2048;

/// Error returned by the guest network APIs.
#[derive(Debug, Error)]
pub enum NetworkError {
    #[error(transparent)]
    Driver(#[from] DriverError),
    #[error("network operation `{operation}` failed with status {status:?}")]
    Status {
        operation: &'static str,
        status: NetworkStatusCode,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RpcBodyKind {
    Request,
    Response,
}

/// Guest-visible listener handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Listener {
    descriptor: NetworkListenerDescriptor,
}

/// Guest-visible protocol session handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    descriptor: NetworkSessionDescriptor,
}

/// Guest-visible stream handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StreamChannel {
    resource_id: u32,
}

/// Guest-visible outbound RPC exchange.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClientExchange {
    descriptor: NetworkRpcClientExchangeDescriptor,
}

/// Guest-visible RPC body reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyReader {
    descriptor: NetworkRpcBodyReaderDescriptor,
    kind: RpcBodyKind,
}

/// Guest-visible RPC body writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BodyWriter {
    descriptor: NetworkRpcBodyWriterDescriptor,
    kind: RpcBodyKind,
}

/// Accepted inbound RPC request plus response exchange handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerExchange {
    descriptor: NetworkRpcExchangeDescriptor,
    request: NetworkRpcRequestHead,
    request_body: BodyReader,
}

impl Listener {
    pub fn protocol(&self) -> NetworkProtocol {
        self.descriptor.protocol
    }

    pub fn interactions(&self) -> &[InteractionKind] {
        self.descriptor.interactions.as_slice()
    }

    pub async fn accept(&self, timeout_ms: u32) -> Result<Option<Session>, NetworkError> {
        let args = encode_args(&NetworkAccept {
            listener_id: self.descriptor.resource_id,
            timeout_ms,
        })?;
        let accepted =
            DriverFuture::<network_accept::Module, RkyvDecoder<NetworkAcceptResult>>::new(
                &args,
                ACCEPT_CAPACITY,
                RkyvDecoder::new(),
            )?
            .await?;

        match accepted.code {
            NetworkStatusCode::Ok => Ok(accepted.session.map(|descriptor| Session { descriptor })),
            NetworkStatusCode::WouldBlock | NetworkStatusCode::Timeout => Ok(None),
            status => Err(NetworkError::Status {
                operation: "accept",
                status,
            }),
        }
    }

    pub async fn close(self) -> Result<(), NetworkError> {
        close(self.descriptor.resource_id, "close(listener)").await
    }
}

impl Session {
    pub fn protocol(&self) -> NetworkProtocol {
        self.descriptor.protocol
    }

    pub fn interactions(&self) -> &[InteractionKind] {
        self.descriptor.interactions.as_slice()
    }

    pub async fn close(self) -> Result<(), NetworkError> {
        close(self.descriptor.resource_id, "close(session)").await
    }
}

impl StreamChannel {
    pub async fn send(
        &self,
        bytes: impl Into<Vec<u8>>,
        finish: bool,
        timeout_ms: u32,
    ) -> Result<(), NetworkError> {
        let args = encode_args(&NetworkStreamSend {
            stream_id: self.resource_id,
            bytes: bytes.into(),
            finish,
            timeout_ms,
        })?;
        let status = DriverFuture::<network_stream_send::Module, RkyvDecoder<NetworkStatus>>::new(
            &args,
            STATUS_CAPACITY,
            RkyvDecoder::new(),
        )?
        .await?;
        ensure_ok("stream.send", status.code)
    }

    pub async fn recv(
        &self,
        max_bytes: u32,
        timeout_ms: u32,
    ) -> Result<Option<NetworkStreamChunk>, NetworkError> {
        let args = encode_args(&NetworkStreamRecv {
            stream_id: self.resource_id,
            max_bytes,
            timeout_ms,
        })?;
        let result = DriverFuture::<
            network_stream_recv::Module,
            RkyvDecoder<NetworkStreamRecvResult>,
        >::new(&args, STREAM_RECV_CAPACITY, RkyvDecoder::new())?
        .await?;
        match result.code {
            NetworkStatusCode::Ok => Ok(result.chunk),
            NetworkStatusCode::WouldBlock | NetworkStatusCode::Timeout => Ok(None),
            status => Err(NetworkError::Status {
                operation: "stream.recv",
                status,
            }),
        }
    }

    pub async fn close(self) -> Result<(), NetworkError> {
        close(self.resource_id, "close(stream)").await
    }
}

impl BodyReader {
    pub async fn recv(
        &self,
        max_bytes: u32,
        timeout_ms: u32,
    ) -> Result<Option<NetworkStreamChunk>, NetworkError> {
        let args = encode_args(&NetworkRpcBodyRead {
            body_id: self.descriptor.resource_id,
            max_bytes,
            timeout_ms,
        })?;
        let result = match self.kind {
            RpcBodyKind::Request => {
                DriverFuture::<
                    network_rpc_request_body_read::Module,
                    RkyvDecoder<NetworkRpcBodyReadResult>,
                >::new(&args, RPC_BODY_READ_CAPACITY, RkyvDecoder::new())?
                .await?
            }
            RpcBodyKind::Response => {
                DriverFuture::<
                    network_rpc_response_body_read::Module,
                    RkyvDecoder<NetworkRpcBodyReadResult>,
                >::new(&args, RPC_BODY_READ_CAPACITY, RkyvDecoder::new())?
                .await?
            }
        };

        match result.code {
            NetworkStatusCode::Ok => Ok(result.chunk),
            NetworkStatusCode::WouldBlock | NetworkStatusCode::Timeout => Ok(None),
            status => Err(NetworkError::Status {
                operation: "rpc.body.recv",
                status,
            }),
        }
    }

    pub async fn read_all(&self, max_bytes: u32, timeout_ms: u32) -> Result<Vec<u8>, NetworkError> {
        let mut body = Vec::new();
        loop {
            let chunk = self.recv(max_bytes, timeout_ms).await?;
            let Some(chunk) = chunk else {
                continue;
            };
            body.extend_from_slice(&chunk.bytes);
            if chunk.finish {
                break;
            }
        }
        Ok(body)
    }

    pub async fn close(self) -> Result<(), NetworkError> {
        close(self.descriptor.resource_id, "close(rpc-body-reader)").await
    }
}

impl BodyWriter {
    pub async fn send(
        &self,
        bytes: impl Into<Vec<u8>>,
        finish: bool,
        timeout_ms: u32,
    ) -> Result<(), NetworkError> {
        let args = encode_args(&NetworkRpcBodyWrite {
            body_id: self.descriptor.resource_id,
            bytes: bytes.into(),
            finish,
            timeout_ms,
        })?;
        let status = match self.kind {
            RpcBodyKind::Request => DriverFuture::<
                network_rpc_request_body_write::Module,
                RkyvDecoder<NetworkStatus>,
            >::new(&args, STATUS_CAPACITY, RkyvDecoder::new())?
            .await?,
            RpcBodyKind::Response => DriverFuture::<
                network_rpc_response_body_write::Module,
                RkyvDecoder<NetworkStatus>,
            >::new(&args, STATUS_CAPACITY, RkyvDecoder::new())?
            .await?,
        };
        ensure_ok("rpc.body.send", status.code)
    }

    pub async fn finish(&self, timeout_ms: u32) -> Result<(), NetworkError> {
        self.send(Vec::new(), true, timeout_ms).await
    }

    pub async fn write_all(
        &self,
        bytes: impl Into<Vec<u8>>,
        timeout_ms: u32,
    ) -> Result<(), NetworkError> {
        let bytes = bytes.into();
        if bytes.is_empty() {
            self.finish(timeout_ms).await
        } else {
            self.send(bytes, true, timeout_ms).await
        }
    }

    pub async fn close(self) -> Result<(), NetworkError> {
        close(self.descriptor.resource_id, "close(rpc-body-writer)").await
    }
}

impl ClientExchange {
    pub async fn await_response(
        self,
        timeout_ms: u32,
    ) -> Result<Option<(NetworkRpcResponseHead, BodyReader)>, NetworkError> {
        let args = encode_args(&NetworkRpcAwait {
            exchange_id: self.descriptor.resource_id,
            timeout_ms,
        })?;
        let result = DriverFuture::<
            network_rpc_await::Module,
            RkyvDecoder<NetworkRpcResponseResult>,
        >::new(&args, RPC_RESPONSE_CAPACITY, RkyvDecoder::new())?
        .await?;

        match result.code {
            NetworkStatusCode::Ok => Ok(match (result.response, result.response_body) {
                (Some(response), Some(response_body)) => Some((
                    response,
                    BodyReader {
                        descriptor: response_body,
                        kind: RpcBodyKind::Response,
                    },
                )),
                _ => None,
            }),
            NetworkStatusCode::WouldBlock | NetworkStatusCode::Timeout => Ok(None),
            status => Err(NetworkError::Status {
                operation: "rpc.await_response",
                status,
            }),
        }
    }

    pub async fn close(self) -> Result<(), NetworkError> {
        close(self.descriptor.resource_id, "close(rpc-client-exchange)").await
    }
}

impl ServerExchange {
    pub fn request_head(&self) -> &NetworkRpcRequestHead {
        &self.request
    }

    pub fn request_body(&self) -> &BodyReader {
        &self.request_body
    }

    pub async fn buffered_request(
        &self,
        max_bytes: u32,
        timeout_ms: u32,
    ) -> Result<NetworkRpcRequest, NetworkError> {
        Ok(NetworkRpcRequest {
            head: self.request.clone(),
            body: self.request_body.read_all(max_bytes, timeout_ms).await?,
        })
    }

    pub async fn respond_head(
        self,
        response: NetworkRpcResponseHead,
        timeout_ms: u32,
    ) -> Result<Option<BodyWriter>, NetworkError> {
        let args = encode_args(&NetworkRpcRespond {
            exchange_id: self.descriptor.resource_id,
            response,
            timeout_ms,
        })?;
        let result = DriverFuture::<
            network_rpc_respond::Module,
            RkyvDecoder<NetworkRpcRespondResult>,
        >::new(&args, RPC_RESPOND_CAPACITY, RkyvDecoder::new())?
        .await?;

        match result.code {
            NetworkStatusCode::Ok => Ok(result.response_body.map(|descriptor| BodyWriter {
                descriptor,
                kind: RpcBodyKind::Response,
            })),
            NetworkStatusCode::WouldBlock | NetworkStatusCode::Timeout => Ok(None),
            status => Err(NetworkError::Status {
                operation: "rpc.respond_head",
                status,
            }),
        }
    }

    pub async fn respond(
        self,
        response: NetworkRpcResponse,
        timeout_ms: u32,
    ) -> Result<(), NetworkError> {
        let body = self.respond_head(response.head, timeout_ms).await?;
        let Some(body) = body else {
            return Err(NetworkError::Status {
                operation: "rpc.respond_head",
                status: NetworkStatusCode::WouldBlock,
            });
        };
        body.write_all(response.body, timeout_ms).await
    }

    pub async fn close(self) -> Result<(), NetworkError> {
        close(self.descriptor.resource_id, "close(rpc-exchange)").await
    }
}

pub async fn listen(binding_name: impl Into<String>) -> Result<Listener, NetworkError> {
    let args = encode_args(&selium_abi::NetworkListen {
        binding_name: binding_name.into(),
    })?;
    let descriptor =
        DriverFuture::<network_listen::Module, RkyvDecoder<NetworkListenerDescriptor>>::new(
            &args,
            LISTENER_CAPACITY,
            RkyvDecoder::new(),
        )?
        .await?;
    Ok(Listener { descriptor })
}

pub async fn connect(
    protocol: NetworkProtocol,
    profile_name: impl Into<String>,
    authority: impl Into<String>,
) -> Result<Session, NetworkError> {
    let args = encode_args(&NetworkConnect {
        protocol,
        profile_name: profile_name.into(),
        authority: authority.into(),
    })?;
    let descriptor =
        DriverFuture::<network_connect::Module, RkyvDecoder<NetworkSessionDescriptor>>::new(
            &args,
            SESSION_CAPACITY,
            RkyvDecoder::new(),
        )?
        .await?;
    Ok(Session { descriptor })
}

pub mod stream {
    use super::*;

    pub async fn open(session: &Session) -> Result<Option<StreamChannel>, NetworkError> {
        let args = encode_args(&NetworkStreamOpen {
            session_id: session.descriptor.resource_id,
        })?;
        let result =
            DriverFuture::<network_stream_open::Module, RkyvDecoder<NetworkStreamResult>>::new(
                &args,
                STREAM_RESULT_CAPACITY,
                RkyvDecoder::new(),
            )?
            .await?;
        match result.code {
            NetworkStatusCode::Ok => Ok(result.stream.map(|stream| StreamChannel {
                resource_id: stream.resource_id,
            })),
            NetworkStatusCode::WouldBlock | NetworkStatusCode::Timeout => Ok(None),
            status => Err(NetworkError::Status {
                operation: "stream.open",
                status,
            }),
        }
    }

    pub async fn accept(
        session: &Session,
        timeout_ms: u32,
    ) -> Result<Option<StreamChannel>, NetworkError> {
        let args = encode_args(&NetworkStreamAccept {
            session_id: session.descriptor.resource_id,
            timeout_ms,
        })?;
        let result =
            DriverFuture::<network_stream_accept::Module, RkyvDecoder<NetworkStreamResult>>::new(
                &args,
                STREAM_RESULT_CAPACITY,
                RkyvDecoder::new(),
            )?
            .await?;
        match result.code {
            NetworkStatusCode::Ok => Ok(result.stream.map(|stream| StreamChannel {
                resource_id: stream.resource_id,
            })),
            NetworkStatusCode::WouldBlock | NetworkStatusCode::Timeout => Ok(None),
            status => Err(NetworkError::Status {
                operation: "stream.accept",
                status,
            }),
        }
    }
}

pub mod rpc {
    use super::*;

    pub async fn start(
        session: &Session,
        request: NetworkRpcRequestHead,
        timeout_ms: u32,
    ) -> Result<Option<(ClientExchange, BodyWriter)>, NetworkError> {
        let args = encode_args(&NetworkRpcInvoke {
            session_id: session.descriptor.resource_id,
            request,
            timeout_ms,
        })?;
        let result =
            DriverFuture::<network_rpc_invoke::Module, RkyvDecoder<NetworkRpcInvokeResult>>::new(
                &args,
                RPC_INVOKE_CAPACITY,
                RkyvDecoder::new(),
            )?
            .await?;

        match result.code {
            NetworkStatusCode::Ok => Ok(match (result.exchange, result.request_body) {
                (Some(exchange), Some(request_body)) => Some((
                    ClientExchange {
                        descriptor: exchange,
                    },
                    BodyWriter {
                        descriptor: request_body,
                        kind: RpcBodyKind::Request,
                    },
                )),
                _ => None,
            }),
            NetworkStatusCode::WouldBlock | NetworkStatusCode::Timeout => Ok(None),
            status => Err(NetworkError::Status {
                operation: "rpc.start",
                status,
            }),
        }
    }

    pub async fn invoke(
        session: &Session,
        request: NetworkRpcRequest,
        timeout_ms: u32,
    ) -> Result<Option<NetworkRpcResponse>, NetworkError> {
        let (head, body) = request.into_head_and_body();
        let started = start(session, head, timeout_ms).await?;
        let Some((exchange, request_body)) = started else {
            return Ok(None);
        };
        request_body.write_all(body, timeout_ms).await?;
        let response = exchange.await_response(timeout_ms).await?;
        let Some((response_head, response_body)) = response else {
            return Ok(None);
        };
        let body = response_body.read_all(8192, timeout_ms).await?;
        Ok(Some(NetworkRpcResponse::from_head_and_body(
            response_head,
            body,
        )))
    }

    pub async fn accept(
        session: &Session,
        timeout_ms: u32,
    ) -> Result<Option<ServerExchange>, NetworkError> {
        let args = encode_args(&NetworkRpcAccept {
            session_id: session.descriptor.resource_id,
            timeout_ms,
        })?;
        let result =
            DriverFuture::<network_rpc_accept::Module, RkyvDecoder<NetworkRpcAcceptResult>>::new(
                &args,
                RPC_ACCEPT_CAPACITY,
                RkyvDecoder::new(),
            )?
            .await?;
        match result.code {
            NetworkStatusCode::Ok => Ok(
                match (result.exchange, result.request, result.request_body) {
                    (Some(exchange), Some(request), Some(request_body)) => Some(ServerExchange {
                        descriptor: exchange,
                        request,
                        request_body: BodyReader {
                            descriptor: request_body,
                            kind: RpcBodyKind::Request,
                        },
                    }),
                    _ => None,
                },
            ),
            NetworkStatusCode::WouldBlock | NetworkStatusCode::Timeout => Ok(None),
            status => Err(NetworkError::Status {
                operation: "rpc.accept",
                status,
            }),
        }
    }
}

pub mod quic {
    use super::*;

    pub async fn connect(
        profile_name: impl Into<String>,
        authority: impl Into<String>,
    ) -> Result<Session, NetworkError> {
        super::connect(NetworkProtocol::Quic, profile_name, authority).await
    }
}

pub mod http {
    use super::*;

    pub async fn connect(
        profile_name: impl Into<String>,
        authority: impl Into<String>,
    ) -> Result<Session, NetworkError> {
        super::connect(NetworkProtocol::Http, profile_name, authority).await
    }
}

async fn close(resource_id: u32, operation: &'static str) -> Result<(), NetworkError> {
    let args = encode_args(&NetworkClose { resource_id })?;
    let status = DriverFuture::<network_close::Module, RkyvDecoder<NetworkStatus>>::new(
        &args,
        STATUS_CAPACITY,
        RkyvDecoder::new(),
    )?
    .await?;
    ensure_ok(operation, status.code)
}

fn ensure_ok(operation: &'static str, status: NetworkStatusCode) -> Result<(), NetworkError> {
    if status == NetworkStatusCode::Ok {
        Ok(())
    } else {
        Err(NetworkError::Status { operation, status })
    }
}

driver_module!(network_listen, "selium::network::listen");
driver_module!(network_close, "selium::network::close");
driver_module!(network_connect, "selium::network::connect");
driver_module!(network_accept, "selium::network::accept");
driver_module!(network_stream_open, "selium::network::stream_open");
driver_module!(network_stream_accept, "selium::network::stream_accept");
driver_module!(network_stream_send, "selium::network::stream_send");
driver_module!(network_stream_recv, "selium::network::stream_recv");
driver_module!(network_rpc_invoke, "selium::network::rpc_invoke");
driver_module!(network_rpc_await, "selium::network::rpc_await");
driver_module!(
    network_rpc_request_body_write,
    "selium::network::rpc_request_body_write"
);
driver_module!(
    network_rpc_response_body_read,
    "selium::network::rpc_response_body_read"
);
driver_module!(network_rpc_accept, "selium::network::rpc_accept");
driver_module!(network_rpc_respond, "selium::network::rpc_respond");
driver_module!(
    network_rpc_request_body_read,
    "selium::network::rpc_request_body_read"
);
driver_module!(
    network_rpc_response_body_write,
    "selium::network::rpc_response_body_write"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listen_returns_kernel_error_with_native_stub_driver() {
        let err = crate::block_on(listen("public")).expect_err("stub should fail");
        assert!(matches!(err, NetworkError::Driver(DriverError::Kernel(2))));
    }

    #[test]
    fn session_descriptor_capacity_covers_archived_payload() {
        let descriptor = NetworkSessionDescriptor {
            resource_id: 7,
            protocol: NetworkProtocol::Quic,
            interactions: vec![InteractionKind::Stream, InteractionKind::Rpc],
        };
        let encoded = selium_abi::encode_rkyv(&descriptor).expect("encode");
        assert!(encoded.len() <= SESSION_CAPACITY);
    }
}
