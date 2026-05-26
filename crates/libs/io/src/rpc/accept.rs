use crate::rpc::{
    RpcConnection,
    error::{AcceptError, RpcError},
};

/// Accept implementation for RPC connections.
pub struct RpcAccept<Req, Rep>(std::marker::PhantomData<(Req, Rep)>);

impl<Req, Rep> selium_guest::Accept for RpcAccept<Req, Rep> {
    type Item = RpcConnection<Req, Rep>;

    fn accept(
        connection: selium_guest::IncomingConnection,
    ) -> selium_guest::Result<Self::Item> {
        RpcConnection::for_server(connection.shared_id, connection.client_process_id)
            .map_err(|e| {
                selium_guest::GuestError::Host(match e {
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
