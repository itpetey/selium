use std::cell::{Cell, RefCell};

use crate::{
    ResourceSender, SharedMemory, SharedRegionBuilder, Signal,
    io::rpc::error::RpcError,
    io::{
        ChannelRegion, RingBuf,
        channels::{self, StrongReader, StrongWriter},
        region::REGION_HEADER_BYTES,
        ring_buf,
    },
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
    /// Creates a new RPC session, initialises the shared region, and transmits
    /// the region id to the server via the supplied listener queue.
    pub async fn connect(
        sender: ResourceSender,
        request_capacity: u64,
        reply_capacity: u64,
    ) -> Result<Self, RpcError> {
        let request_data_cap = ring_buf::round_capacity(request_capacity)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;
        let reply_data_cap = ring_buf::round_capacity(reply_capacity)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

        let request_sub_len = request_data_cap + REGION_HEADER_BYTES;
        let reply_sub_len = reply_data_cap + REGION_HEADER_BYTES;

        let total_capacity = {
            let header_size = 40; // @todo Magic number!
            let mut total = header_size;
            total = align_up(total, 8) + request_sub_len;
            total = align_up(total, 8) + reply_sub_len;
            total
        };

        // Step 2: Create SharedRegion with two sub-memories.
        let mut builder = SharedRegionBuilder::new(total_capacity);
        builder
            .add_memory(request_sub_len)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;
        builder
            .add_memory(reply_sub_len)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;
        let region = builder
            .seal()
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

        // Create signals for each ring buffer.
        let req_signal = Signal::create().map_err(|e| RpcError::Serialization(e.to_string()))?;
        let rep_signal = Signal::create().map_err(|e| RpcError::Serialization(e.to_string()))?;

        // Read sub-memory offsets and lengths from the region header.
        let (req_offset, req_len) = region
            .memory(0)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;
        let (rep_offset, rep_len) = region
            .memory(1)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

        // Map sub-memories and initialise ring buffer headers.
        let req_mapping = SharedMemory::attach(region.descriptor(), req_offset, req_len)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;
        let req_region = ChannelRegion::from_mapping(
            req_mapping,
            (req_len as u64).saturating_sub(REGION_HEADER_BYTES),
        );
        req_region
            .initialise(req_signal.shared_id())
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

        let rep_mapping = SharedMemory::attach(region.descriptor(), rep_offset, rep_len)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;
        let rep_region = ChannelRegion::from_mapping(
            rep_mapping,
            (rep_len as u64).saturating_sub(REGION_HEADER_BYTES),
        );
        rep_region
            .initialise(rep_signal.shared_id())
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

        // Wrap as RingBufs with signals.
        let req_ring =
            RingBuf::wrap_region(req_region, Some(req_signal.clone())).map_err(RpcError::from)?;
        let rep_ring =
            RingBuf::wrap_region(rep_region, Some(rep_signal.clone())).map_err(RpcError::from)?;

        // Set up writer for request channel.
        req_ring
            .region()
            .increment_writer_count()
            .map_err(|_error| RpcError::InvalidRegion)?;
        let writer_id = match req_ring.region().allocate_writer_id() {
            Ok(id) => id,
            Err(error) => {
                let _unused = req_ring.region().decrement_writer_count();
                return Err(error.into());
            }
        };
        let request_writer = RefCell::new(StrongWriter::new(
            req_ring.region().clone(),
            writer_id,
            Some(req_signal),
        ));

        // Set up reader for reply channel.
        let reply_reader = {
            let tail = rep_ring.read_next_tail().map_err(RpcError::from)?;
            let reader_id = rep_ring
                .region()
                .allocate_reader_slot(tail)
                .map_err(|_error| RpcError::InvalidRegion)?;
            RefCell::new(StrongReader::new(
                rep_ring.region().clone(),
                tail,
                reader_id,
            ))
        };

        sender
            .send(region.shared_id())
            .await
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

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
        let bytes = selium_abi::encode_rkyv(&payload)
            .map_err(|e| RpcError::Serialization(e.to_string()))?;

        {
            let mut writer = self.request_writer.borrow_mut();
            writer.write(&bytes).map_err(|e| match e {
                channels::Error::ChannelFull => RpcError::BufferFull,
                channels::Error::Core(err) => err.into(),
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
                        let reply = selium_abi::decode_rkyv(&payload)
                            .map_err(|e| RpcError::Serialization(e.to_string()))?;
                        return Ok(reply);
                    }
                    return Err(RpcError::Serialization(
                        "reply correlation id mismatch".to_string(),
                    ));
                }
                Err(channels::Error::ChannelEmpty) => {
                    if self
                        .reply_reader
                        .borrow()
                        .region()
                        .read_writer_count()
                        .map_err(RpcError::from)?
                        == 0
                    {
                        return Err(RpcError::ConnectionClosed);
                    }
                    let generation = self
                        .reply_signal
                        .generation()
                        .map_err(|e| RpcError::Serialization(e.to_string()))?;
                    self.reply_signal
                        .wait(generation, 1000)
                        .await
                        .map_err(|e| RpcError::Serialization(e.to_string()))?;
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

fn align_up(value: u64, alignment: u64) -> u64 {
    let rem = value % alignment;
    if rem == 0 {
        value
    } else {
        value + alignment - rem
    }
}
