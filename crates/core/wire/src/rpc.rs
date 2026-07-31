//! Transport-agnostic typed RPC pattern.

use std::{fmt, marker::PhantomData};

use selium_encoding::FlatMsg;

use crate::{
    MessageTransport,
    error::{Error, Result},
    framed::{FramedRead, FramedWrite},
};

/// Abstraction over the mechanism used to pass connection identifiers from
/// client to server.
pub trait Rendezvous: Send + Sync {
    /// Sends a connection identifier to the server.
    ///
    /// # Errors
    ///
    /// Returns an error if the rendezvous channel is closed or the send fails.
    fn send(&self, shared_id: u64) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Receives a connection identifier from a client.
    ///
    /// # Errors
    ///
    /// Returns an error if the rendezvous channel is closed.
    fn recv(&self) -> impl std::future::Future<Output = Result<IncomingConnection>> + Send;
}

/// Information about an incoming connection delivered by a [`Rendezvous`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IncomingConnection {
    /// Process id of the connecting peer.
    pub client_process_id: u64,
    /// Connection identifier passed through the rendezvous (e.g. shared region id).
    pub shared_id: u64,
}

/// Error type for RPC operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    /// The peer has closed the connection.
    ConnectionClosed,
    /// The shared region is invalid or corrupted.
    InvalidRegion,
    /// The region layout does not match the expected structure.
    LayoutMismatch,
    /// The ring buffer is full.
    BufferFull,
    /// The ring buffer is empty.
    BufferEmpty,
    /// Encoding or decoding failed.
    Serialization(String),
}

/// Client-side handle for making typed RPC requests over a [`MessageTransport`].
pub struct RpcClient<Req, Rep, M> {
    request_writer: FramedWrite<M>,
    reply_reader: FramedRead<M>,
    next_correlation: u32,
    _phantom: PhantomData<(Req, Rep)>,
}

/// Server-side handle for an established RPC session.
pub struct RpcConnection<Req, Rep, M> {
    request_reader: FramedRead<M>,
    reply_writer: FramedWrite<M>,
    client_process_id: u64,
    _phantom: PhantomData<(Req, Rep)>,
}

/// A single request received by the server, with the ability to reply.
pub struct RpcRequest<'a, Req, Rep, M> {
    reply_writer: &'a mut FramedWrite<M>,
    payload_bytes: Vec<u8>,
    correlation: u32,
    _phantom: PhantomData<(Req, Rep)>,
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConnectionClosed => write!(f, "RPC connection closed"),
            Self::InvalidRegion => write!(f, "invalid shared region"),
            Self::LayoutMismatch => write!(f, "region layout mismatch"),
            Self::BufferFull => write!(f, "RPC buffer full"),
            Self::BufferEmpty => write!(f, "RPC buffer empty"),
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for RpcError {}

impl From<Error> for RpcError {
    fn from(error: Error) -> Self {
        match error {
            Error::BufferFull => Self::BufferFull,
            Error::BufferEmpty => Self::BufferEmpty,
            Error::InvalidLayout | Error::InvalidRegion => Self::InvalidRegion,
            Error::ConnectionLost | Error::ChannelClosed | Error::Terminated => {
                Self::ConnectionClosed
            }
            Error::SerializationFailed(msg) | Error::Guest(msg) => Self::Serialization(msg),
            other => Self::Serialization(other.to_string()),
        }
    }
}

impl<Req, Rep, M> RpcClient<Req, Rep, M>
where
    Req: FlatMsg,
    Rep: FlatMsg,
    M: MessageTransport,
{
    /// Creates a new RPC client from pre-established request and reply transports.
    pub fn new(request_writer: FramedWrite<M>, reply_reader: FramedRead<M>) -> Self {
        Self {
            request_writer,
            reply_reader,
            next_correlation: 1,
            _phantom: PhantomData,
        }
    }

    /// Sends a typed request and awaits the matching reply.
    pub async fn request(&mut self, payload: Req) -> std::result::Result<Rep, RpcError> {
        let correlation = self.next_correlation;
        self.next_correlation = self.next_correlation.wrapping_add(1);

        let encoded = FlatMsg::encode(&payload);
        self.request_writer.write_frame(&encoded, correlation)?;

        let region_id = self.reply_reader.inner().region_id();
        let mut last_generation = self.reply_reader.generation().unwrap_or(0).wrapping_sub(1);

        loop {
            let current_generation = self.reply_reader.generation().unwrap_or(0);

            if current_generation != last_generation {
                last_generation = current_generation;

                match try_read_reply::<Rep, M>(&mut self.reply_reader, correlation) {
                    Ok(Some(reply)) => return Ok(reply),
                    Ok(None) => {}
                    Err(RpcError::BufferEmpty) => {}
                    Err(e) => return Err(e),
                }
            }

            if self.reply_reader.poll_peer_closed()? {
                return Err(RpcError::ConnectionClosed);
            }

            // Wait on generation change instead of busy-spinning.
            if region_id != 0 {
                crate::generation_wait(region_id, last_generation).await;
            } else {
                crate::yield_now().await;
            }
        }
    }
}

impl<Req, Rep, M> RpcConnection<Req, Rep, M>
where
    Req: FlatMsg,
    Rep: FlatMsg,
    M: MessageTransport,
{
    /// Creates a new RPC connection from pre-established request and reply transports.
    pub fn new(
        request_reader: FramedRead<M>,
        reply_writer: FramedWrite<M>,
        client_process_id: u64,
    ) -> Self {
        Self {
            request_reader,
            reply_writer,
            client_process_id,
            _phantom: PhantomData,
        }
    }

    /// Returns the process id of the connected client.
    pub fn client_process_id(&self) -> u64 {
        self.client_process_id
    }

    /// Receives the next request from the client.
    pub async fn recv(&mut self) -> std::result::Result<RpcRequest<'_, Req, Rep, M>, RpcError> {
        let region_id = self.request_reader.inner().region_id();
        let mut last_generation = self
            .request_reader
            .generation()
            .unwrap_or(0)
            .wrapping_sub(1);

        loop {
            let current_generation = self.request_reader.generation().unwrap_or(0);

            if current_generation != last_generation {
                last_generation = current_generation;

                match self.request_reader.read_frame() {
                    Ok((payload_bytes, correlation)) => {
                        return Ok(RpcRequest {
                            reply_writer: &mut self.reply_writer,
                            payload_bytes,
                            correlation,
                            _phantom: PhantomData,
                        });
                    }
                    Err(Error::BufferEmpty) => {}
                    Err(e) => return Err(e.into()),
                }
            }

            if self.request_reader.poll_peer_closed()? {
                return Err(RpcError::ConnectionClosed);
            }

            // Wait on generation change instead of busy-spinning.
            if region_id != 0 {
                crate::generation_wait(region_id, last_generation).await;
            } else {
                crate::yield_now().await;
            }
        }
    }
}

impl<'a, Req, Rep, M> RpcRequest<'a, Req, Rep, M>
where
    Req: FlatMsg,
    Rep: FlatMsg,
    M: MessageTransport,
{
    /// Returns a reference to the raw request payload bytes.
    pub fn payload_bytes(&self) -> &[u8] {
        &self.payload_bytes
    }

    /// Decodes the request payload from Flatbuffers bytes.
    pub fn payload(&self) -> std::result::Result<Req, RpcError> {
        Req::decode(&self.payload_bytes)
            .map_err(|e| RpcError::Serialization(format!("decode request: {e}")))
    }

    /// Returns the deserialized request payload by value.
    pub fn into_payload(self) -> std::result::Result<Req, RpcError> {
        self.payload()
    }

    /// Sends a reply to the client with the same correlation tag as the request.
    pub async fn reply(self, response: Rep) -> std::result::Result<(), RpcError> {
        let encoded = FlatMsg::encode(&response);
        self.reply_writer.write_frame(&encoded, self.correlation)?;
        Ok(())
    }
}

/// Tries to read a reply frame with the given correlation tag.
fn try_read_reply<Rep, M>(
    reply_reader: &mut FramedRead<M>,
    correlation: u32,
) -> std::result::Result<Option<Rep>, RpcError>
where
    Rep: FlatMsg,
    M: MessageTransport,
{
    match reply_reader.read_frame() {
        Ok((payload_bytes, tag)) => {
            if tag != correlation {
                return Ok(None);
            }

            let decoded: Rep = FlatMsg::decode(&payload_bytes)
                .map_err(|e| RpcError::Serialization(format!("decode reply: {e}")))?;

            Ok(Some(decoded))
        }
        Err(Error::BufferEmpty) => Ok(None),
        Err(e) => Err(e.into()),
    }
}
