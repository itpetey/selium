use selium_guest::Signal;

use crate::{
    RingBuf,
    channels::{StrongReader, StrongWriter},
    rpc::error::RpcError,
};

/// Server-side handle for an established RPC session.
pub struct RpcConnection<Req, Rep> {
    pub(crate) request_reader: StrongReader,
    pub(crate) reply_writer: StrongWriter,
    pub(crate) reply_signal: Signal,
    pub(crate) client_process_id: u64,
    _phantom: std::marker::PhantomData<(Req, Rep)>,
}

impl<Req, Rep> RpcConnection<Req, Rep> {
    /// Creates an RPC connection from the server side.
    pub fn for_server(shared_id: u64, client_process_id: u64) -> Result<Self, RpcError> {
        let mapping = selium_guest::SharedMemory::attach_shared(shared_id, 0, 8192)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

        let magic_bytes = mapping.read(0, 8).map_err(|e| RpcError::Serialization(e.to_string()))?;
        let magic = u64::from_le_bytes(
            magic_bytes.try_into().map_err(|_| RpcError::InvalidRegion)?,
        );
        if magic != crate::region::SHARED_REGION_MAGIC {
            return Err(RpcError::InvalidRegion);
        }

        let count_bytes = mapping.read(16, 4).map_err(|e| RpcError::Serialization(e.to_string()))?;
        let count = u32::from_le_bytes(
            count_bytes.try_into().map_err(|_| RpcError::InvalidRegion)?,
        );
        if count != 2 {
            return Err(RpcError::LayoutMismatch);
        }

        let req_offset_bytes = mapping.read(24, 4).map_err(|e| RpcError::Serialization(e.to_string()))?;
        let req_len_bytes = mapping.read(28, 4).map_err(|e| RpcError::Serialization(e.to_string()))?;
        let req_offset = u32::from_le_bytes(
            req_offset_bytes.try_into().map_err(|_| RpcError::InvalidRegion)?,
        ) as u64;
        let req_len = u32::from_le_bytes(
            req_len_bytes.try_into().map_err(|_| RpcError::InvalidRegion)?,
        ) as u64;

        let rep_offset_bytes = mapping.read(32, 4).map_err(|e| RpcError::Serialization(e.to_string()))?;
        let rep_len_bytes = mapping.read(36, 4).map_err(|e| RpcError::Serialization(e.to_string()))?;
        let rep_offset = u32::from_le_bytes(
            rep_offset_bytes.try_into().map_err(|_| RpcError::InvalidRegion)?,
        ) as u64;
        let rep_len = u32::from_le_bytes(
            rep_len_bytes.try_into().map_err(|_| RpcError::InvalidRegion)?,
        ) as u64;

        drop(mapping);

        let req_data_capacity = req_len.saturating_sub(crate::region::REGION_HEADER_BYTES);
        let rep_data_capacity = rep_len.saturating_sub(crate::region::REGION_HEADER_BYTES);

        let req_region = crate::region::ChannelRegion::from_mapping(
            selium_guest::SharedMemory::attach_shared(shared_id, req_offset as u32, req_len as u32)
                .map_err(|e| RpcError::Serialization(e.to_string()))?,
            req_data_capacity,
        );
        let req_ring = RingBuf::wrap_region(req_region, None)?;
        let req_signal_shared_id = req_ring
            .region()
            .read_header_u64(crate::region::SIGNAL_SHARED_ID_OFFSET)
            .map_err(|_| RpcError::InvalidRegion)?;
        let req_signal = Signal::attach(req_signal_shared_id)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

        let rep_region = crate::region::ChannelRegion::from_mapping(
            selium_guest::SharedMemory::attach_shared(shared_id, rep_offset as u32, rep_len as u32)
                .map_err(|e| RpcError::Serialization(e.to_string()))?,
            rep_data_capacity,
        );
        let rep_ring = RingBuf::wrap_region(rep_region, None)?;
        let rep_signal_shared_id = rep_ring
            .region()
            .read_header_u64(crate::region::SIGNAL_SHARED_ID_OFFSET)
            .map_err(|_| RpcError::InvalidRegion)?;
        let rep_signal = Signal::attach(rep_signal_shared_id)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

        let request_reader = {
            let tail = req_ring.read_next_tail()?;
            let reader_id = req_ring
                .region()
                .allocate_reader_slot(tail)
                .map_err(|_| RpcError::InvalidRegion)?;
            StrongReader::new(req_ring.region().clone(), tail, reader_id)
        };

        rep_ring.region().increment_writer_count().map_err(|_| RpcError::InvalidRegion)?;
        let writer_id = match rep_ring.region().allocate_writer_id() {
            Ok(id) => id,
            Err(error) => {
                let _ = rep_ring.region().decrement_writer_count();
                return Err(error.into());
            }
        };
        let reply_writer = StrongWriter::new(rep_ring.region().clone(), writer_id, Some(rep_signal.clone()));

        Ok(Self {
            request_reader,
            reply_writer,
            reply_signal: rep_signal,
            client_process_id,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Receives the next request from the client.
    pub async fn recv(&mut self) -> Result<RpcRequest<Req, Rep>, RpcError>
    where
        Req: selium_abi::RkyvEncode + Sized,
        for<'a> Req::Archived: rkyv::Deserialize<Req, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    {
        loop {
            match self.request_reader.read() {
                Ok((payload, tag)) => {
                    let request = selium_abi::decode_rkyv(&payload).map_err(|e| {
                        RpcError::Serialization(e.to_string())
                    })?;
                    return Ok(RpcRequest {
                        payload: request,
                        correlation_id: tag,
                        reply_writer: &mut self.reply_writer,
                        reply_signal: &self.reply_signal,
                        _phantom: std::marker::PhantomData,
                    });
                }
                Err(crate::channels::Error::ChannelEmpty) => {
                    if self.request_reader.region().read_writer_count().map_err(|e| RpcError::from(e))? == 0 {
                        return Err(RpcError::ConnectionClosed);
                    }
                    let generation = self.reply_signal.generation().map_err(|e| {
                        RpcError::Serialization(e.to_string())
                    })?;
                    self.reply_signal.wait(generation, 1000).await.map_err(|e| {
                        RpcError::Serialization(e.to_string())
                    })?;
                }
                Err(e) => return Err(e.into()),
            }
        }
    }
}

/// A single request received by the server, with the ability to reply.
pub struct RpcRequest<'a, Req, Rep> {
    payload: Req,
    correlation_id: u32,
    reply_writer: &'a mut StrongWriter,
    reply_signal: &'a Signal,
    _phantom: std::marker::PhantomData<Rep>,
}

impl<'a, Req, Rep> RpcRequest<'a, Req, Rep> {
    /// Returns the deserialized request payload.
    pub fn into_payload(self) -> Req {
        self.payload
    }

    /// Sends a reply to the client, consuming the request handle.
    pub async fn reply(self, response: Rep) -> Result<(), RpcError>
    where
        Rep: selium_abi::RkyvEncode + Sized,
    {
        let bytes = selium_abi::encode_rkyv(&response).map_err(|e| {
            RpcError::Serialization(e.to_string())
        })?;

        self.reply_writer.write(&bytes).map_err(|e| {
            match e {
                crate::channels::Error::ChannelFull => RpcError::BufferFull,
                crate::channels::Error::Core(err) => err.into(),
                other => RpcError::Serialization(other.to_string()),
            }
        })?;

        self.reply_signal.notify().map_err(|e| {
            RpcError::Serialization(e.to_string())
        })?;

        Ok(())
    }
}
