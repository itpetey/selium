use crate::{
    Accept, GuestError, IncomingConnection, Result,
    io::rpc::{
        RpcConnection,
        error::{AcceptError, RpcError},
    },
};

/// Accept implementation for RPC connections.
pub struct RpcAccept<Req, Rep>(std::marker::PhantomData<(Req, Rep)>);

impl<Req, Rep> Accept for RpcAccept<Req, Rep> {
    type Item = RpcConnection<Req, Rep>;

    fn accept(connection: IncomingConnection) -> Result<Self::Item> {
        RpcConnection::for_server(connection.shared_id, connection.client_process_id).map_err(|e| {
            GuestError::Host(match e {
                RpcError::InvalidRegion => AcceptError::InvalidRegion.to_string(),
                RpcError::LayoutMismatch => AcceptError::LayoutMismatch.to_string(),
                RpcError::BufferFull => AcceptError::BufferFull.to_string(),
                RpcError::BufferEmpty => AcceptError::BufferEmpty.to_string(),
                RpcError::ConnectionClosed => AcceptError::ConnectionClosed.to_string(),
                RpcError::Serialization(msg) => AcceptError::Serialization(msg).to_string(),
            })
        })
    }
}
