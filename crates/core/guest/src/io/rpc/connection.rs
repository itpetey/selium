use crate::{
    Signal,
    io::rpc::{attach_rpc_channels, error::RpcError},
    io::{
        ChannelRegion, RingBuf,
        channels::{self, StrongReader, StrongWriter},
        region::SIGNAL_SHARED_ID_OFFSET,
    },
};

/// Server-side handle for an established RPC session.
pub struct RpcConnection<Req, Rep> {
    pub(crate) request_reader: StrongReader,
    pub(crate) reply_writer: StrongWriter,
    pub(crate) request_signal: Signal,
    #[expect(dead_code, reason = "stored for future logging and audit")]
    pub(crate) client_process_id: u64,
    _phantom: std::marker::PhantomData<(Req, Rep)>,
}

/// A single request received by the server, with the ability to reply.
pub struct RpcRequest<'a, Req, Rep> {
    payload: Req,
    #[expect(
        dead_code,
        reason = "correlation_id not yet propagated to reply frames"
    )]
    correlation_id: u32,
    reply_writer: &'a mut StrongWriter,
    _phantom: std::marker::PhantomData<Rep>,
}

impl<Req, Rep> RpcConnection<Req, Rep> {
    /// Creates an RPC connection from the server side.
    pub fn for_server(shared_id: u64, client_process_id: u64) -> Result<Self, RpcError> {
        let layout = attach_rpc_channels(shared_id)?;

        let req_region = ChannelRegion::from_mapping(layout.req_mapping, layout.req_data_capacity);
        let req_ring = RingBuf::wrap_region(req_region, None).map_err(RpcError::from)?;
        let req_signal_shared_id = req_ring
            .region()
            .read_header_u64(SIGNAL_SHARED_ID_OFFSET)
            .map_err(|_error| RpcError::InvalidRegion)?;
        let request_signal = Signal::attach(req_signal_shared_id)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

        let rep_region = ChannelRegion::from_mapping(layout.rep_mapping, layout.rep_data_capacity);
        let rep_ring = RingBuf::wrap_region(rep_region, None).map_err(RpcError::from)?;
        let rep_signal_shared_id = rep_ring
            .region()
            .read_header_u64(SIGNAL_SHARED_ID_OFFSET)
            .map_err(|_error| RpcError::InvalidRegion)?;
        let rep_signal = Signal::attach(rep_signal_shared_id)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

        let request_reader = {
            let tail = req_ring.read_next_tail().map_err(RpcError::from)?;
            let reader_id = req_ring
                .region()
                .allocate_reader_slot(tail)
                .map_err(|_error| RpcError::InvalidRegion)?;
            StrongReader::new(req_ring.region().clone(), tail, reader_id)
        };

        rep_ring
            .region()
            .increment_writer_count()
            .map_err(|_error| RpcError::InvalidRegion)?;
        let writer_id = match rep_ring.region().allocate_writer_id() {
            Ok(id) => id,
            Err(error) => {
                let _unused = rep_ring.region().decrement_writer_count();
                return Err(error.into());
            }
        };
        let reply_writer =
            StrongWriter::new(rep_ring.region().clone(), writer_id, Some(rep_signal));

        Ok(Self {
            request_reader,
            reply_writer,
            request_signal,
            client_process_id,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Receives the next request from the client.
    pub async fn recv(&mut self) -> Result<RpcRequest<'_, Req, Rep>, RpcError>
    where
        Req: selium_abi::RkyvEncode + Sized,
        for<'a> Req::Archived: rkyv::Deserialize<Req, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    {
        loop {
            match self.request_reader.read() {
                Ok((payload, tag)) => {
                    let request = selium_abi::decode_rkyv(&payload)
                        .map_err(|e| RpcError::Serialization(e.to_string()))?;
                    return Ok(RpcRequest {
                        payload: request,
                        correlation_id: tag,
                        reply_writer: &mut self.reply_writer,
                        _phantom: std::marker::PhantomData,
                    });
                }
                Err(channels::Error::ChannelEmpty) => {
                    if self
                        .request_reader
                        .region()
                        .read_writer_count()
                        .map_err(RpcError::from)?
                        == 0
                    {
                        return Err(RpcError::ConnectionClosed);
                    }
                    let generation = self
                        .request_signal
                        .generation()
                        .map_err(|e| RpcError::Serialization(e.to_string()))?;
                    self.request_signal
                        .wait(generation, 1000)
                        .await
                        .map_err(|e| RpcError::Serialization(e.to_string()))?;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

impl<'a, Req, Rep> RpcRequest<'a, Req, Rep> {
    /// Returns a reference to the deserialized request payload.
    pub fn payload(&self) -> &Req {
        &self.payload
    }

    /// Returns the deserialized request payload.
    pub fn into_payload(self) -> Req {
        self.payload
    }

    /// Sends a reply to the client, consuming the request handle.
    pub async fn reply(self, response: Rep) -> Result<(), RpcError>
    where
        Rep: selium_abi::RkyvEncode + Sized,
    {
        let bytes = selium_abi::encode_rkyv(&response)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

        self.reply_writer.write(&bytes).map_err(|e| match e {
            channels::Error::ChannelFull => RpcError::BufferFull,
            channels::Error::Core(err) => err.into(),
            other => RpcError::Serialization(other.to_string()),
        })?;

        // reply_writer already notifies via its stored signal.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use selium_abi::{DiscoveryRequest, DiscoveryResponse};

    use super::*;

    #[test]
    fn for_server_rejects_invalid_region() {
        let result = RpcConnection::<DiscoveryRequest, DiscoveryResponse>::for_server(0, 1);
        assert!(result.is_err());
    }
}
