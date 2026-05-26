use std::cell::{Cell, RefCell};

use selium_guest::Signal;

use crate::{
    RingBuf,
    channels::{StrongReader, StrongWriter},
    rpc::error::RpcError,
};

/// Client-side handle for making typed RPC requests.
pub struct RpcClient<Req, Rep> {
    request_writer: RefCell<StrongWriter>,
    reply_reader: RefCell<StrongReader>,
    reply_signal: Signal,
    correlation_id: Cell<u32>,
    _phantom: std::marker::PhantomData<(Req, Rep)>,
}

impl<Req, Rep> RpcClient<Req, Rep> {
    /// Attaches to an RPC session region from the client side.
    pub fn attach(region: selium_guest::SharedRegion) -> Result<Self, RpcError> {
        let mapping = selium_guest::SharedMemory::attach(region.descriptor(), 0, region.len())
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
            selium_guest::SharedMemory::attach_shared(region.shared_id(), req_offset as u32, req_len as u32)
                .map_err(|e| RpcError::Serialization(e.to_string()))?,
            req_data_capacity,
        );
        let req_ring = RingBuf::wrap_region(req_region, None)?;
        let req_signal_shared_id = req_ring
            .region()
            .read_header_u64(crate::region::SIGNAL_SHARED_ID_OFFSET)
            .map_err(|_| RpcError::InvalidRegion)?;
        let _req_signal = Signal::attach(req_signal_shared_id)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

        let rep_region = crate::region::ChannelRegion::from_mapping(
            selium_guest::SharedMemory::attach_shared(region.shared_id(), rep_offset as u32, rep_len as u32)
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

        req_ring.region().increment_writer_count().map_err(|_| RpcError::InvalidRegion)?;
        let writer_id = match req_ring.region().allocate_writer_id() {
            Ok(id) => id,
            Err(error) => {
                let _ = req_ring.region().decrement_writer_count();
                return Err(error.into());
            }
        };
        let request_writer = RefCell::new(StrongWriter::new(req_ring.region().clone(), writer_id, Some(_req_signal)));

        let reply_reader = {
            let tail = rep_ring.read_next_tail()?;
            let reader_id = rep_ring
                .region()
                .allocate_reader_slot(tail)
                .map_err(|_| RpcError::InvalidRegion)?;
            RefCell::new(StrongReader::new(rep_ring.region().clone(), tail, reader_id))
        };

        Ok(Self {
            request_writer,
            reply_reader,
            reply_signal: rep_signal,
            correlation_id: Cell::new(0),
            _phantom: std::marker::PhantomData,
        })
    }

    /// Sends a typed request and awaits the matching reply.
    pub async fn request(&self, payload: Req) -> Result<Rep, RpcError>
    where
        Req: selium_abi::RkyvEncode + Sized,
        Rep: selium_abi::RkyvEncode + Sized,
        for<'a> Rep::Archived: rkyv::Deserialize<Rep, rkyv::api::high::HighDeserializer<rkyv::rancor::Error>>
            + rkyv::bytecheck::CheckBytes<rkyv::api::high::HighValidator<'a, rkyv::rancor::Error>>,
    {
        let correlation_id = self.next_correlation_id();
        let bytes = selium_abi::encode_rkyv(&payload).map_err(|e| {
            RpcError::Serialization(e.to_string())
        })?;

        {
            let mut writer = self.request_writer.borrow_mut();
            writer.write(&bytes).map_err(|e| match e {
                crate::channels::Error::ChannelFull => RpcError::BufferFull,
                crate::channels::Error::Core(err) => err.into(),
                other => RpcError::Serialization(other.to_string()),
            })?;
        }

        loop {
            let result = {
                let mut reader = self.reply_reader.borrow_mut();
                reader.read()
            };
            match result {
                Ok((payload, tag)) => {
                    if tag == correlation_id {
                        let reply = selium_abi::decode_rkyv(&payload).map_err(|e| {
                            RpcError::Serialization(e.to_string())
                        })?;
                        return Ok(reply);
                    }
                    return Err(RpcError::Serialization(
                        "reply correlation id mismatch".to_string(),
                    ));
                }
                Err(crate::channels::Error::ChannelEmpty) => {
                    if self.reply_reader.borrow().region().read_writer_count().map_err(|e| RpcError::from(e))? == 0 {
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

    fn next_correlation_id(&self) -> u32 {
        let id = self.correlation_id.get();
        self.correlation_id.set(id.wrapping_add(1));
        id
    }
}
