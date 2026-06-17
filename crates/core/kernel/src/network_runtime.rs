use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering, fence},
    },
    thread,
    time::Duration,
};

use selium_abi::{HostQueueDescriptor, SharedRegionDescriptor};

use crate::{
    Error, Result,
    state::{Kernel, TcpListenerState, TcpStreamState, UdpSocketState},
};

/// Offset where ring buffer data begins (page 1).
const DATA_OFFSET: u64 = PAGE_SIZE;
/// Default ring buffer data capacity (64 KiB).
const DEFAULT_RING_CAPACITY: u32 = 64 * 1024;
/// Frame flag indicating a ready frame.
const FLAG_READY: u8 = 1;
/// Frame header size in bytes.
const FRAME_HEADER_SIZE: u64 = 12;
/// Offset of the generation counter (u64) in page 0.
const GENERATION_COUNTER_OFFSET: u64 = 0;
/// Kernel's reader slot index (used for backpressure on outbound rings).
const KERNEL_READER_SLOT: u32 = 0;
/// Maximum number of blocking reader slots.
const MAX_READER_SLOTS: u32 = 128;
/// Maximum number of blocking writer slots.
const MAX_WRITER_SLOTS: u32 = 128;
/// Offset of the shared `next_tail` cursor (u64) in page 0.
const NEXT_TAIL_OFFSET: u64 = 8;
/// Offset of the shared `next_writer_id` counter (u64).
const NEXT_WRITER_ID_OFFSET: u64 = 1048;
/// Page size for ring buffer layout.
const PAGE_SIZE: u64 = 4096;
/// Polling interval for proxy threads waiting on guest writes.
const PROXY_POLL_INTERVAL_MS: u64 = 1;
/// Offset where the shared `reader_slots` array begins (128 × u64).
const READER_SLOTS_OFFSET: u64 = 24;
/// Offset of the shared `reader_slot_counter` (u64).
const READER_SLOT_COUNTER_OFFSET: u64 = 1056;
const SHARED_REGION_HEADER_CAPACITY_OFFSET: u32 = 8;
const SHARED_REGION_HEADER_COUNT_OFFSET: u32 = 16;
const SHARED_REGION_HEADER_ENTRY_OFFSET: u32 = 24;
const SHARED_REGION_HEADER_ENTRY_SIZE: u32 = 8;
const SHARED_REGION_MAGIC: u64 = 0x53454C49554D454D;
/// Offset of the shared `writer_count` (u64) in page 0.
const WRITER_COUNT_OFFSET: u64 = 16;
/// Offset where the shared `writer_slots` array begins (128 × u64).
const WRITER_SLOTS_OFFSET: u64 = 1080;
/// Offset of the shared `writer_slot_counter` (u64).
const WRITER_SLOT_COUNTER_OFFSET: u64 = 2104;

#[derive(Debug, Clone, Copy)]
struct FrameHeader {
    len: u32,
    tag: u32,
    flags: u8,
    _reserved: [u8; 3],
}

impl FrameHeader {
    fn encode(&self) -> [u8; 12] {
        let mut bytes = [0u8; 12];
        bytes[..4].copy_from_slice(&self.len.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.tag.to_le_bytes());
        bytes[8] = self.flags;
        bytes[9..12].copy_from_slice(&self._reserved);
        bytes
    }

    #[expect(clippy::indexing_slicing, reason = "length checked at start")]
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 12 {
            return Err(Error::Wasm("frame header too short".to_string()));
        }
        let len = u32::from_le_bytes(
            bytes[..4]
                .try_into()
                .map_err(|_e| Error::Wasm("invalid frame header len".to_string()))?,
        );
        let tag = u32::from_le_bytes(
            bytes[4..8]
                .try_into()
                .map_err(|_e| Error::Wasm("invalid frame header tag".to_string()))?,
        );
        let flags = bytes[8];
        let _reserved = bytes[9..12]
            .try_into()
            .map_err(|_e| Error::Wasm("invalid frame header reserved".to_string()))?;
        Ok(Self {
            len,
            tag,
            flags,
            _reserved,
        })
    }

    fn frame_size(&self) -> u64 {
        FRAME_HEADER_SIZE + self.len as u64
    }

    fn is_ready(&self) -> bool {
        self.flags & FLAG_READY != 0
    }
}

impl Kernel {
    pub fn tcp_bind(&self, address: impl Into<String>) -> Result<HostQueueDescriptor> {
        let address = address.into();
        let listener = TcpListener::bind(&address)
            .map_err(|e| Error::Wasm(format!("tcp bind failed: {e}")))?;

        let descriptor = self.create_host_queue();
        let local_id = descriptor.local_id;
        let shared_id = descriptor.shared_id;
        let running = Arc::new(AtomicBool::new(true));

        self.inner.tcp_listeners.lock().insert(
            local_id,
            TcpListenerState {
                shared_id,
                running: running.clone(),
                _listener: listener
                    .try_clone()
                    .map_err(|e| Error::Wasm(format!("listener clone failed: {e}")))?,
            },
        );

        let kernel = self.clone();
        thread::spawn(move || {
            if let Err(_e) = tcp_accept_loop(&kernel, listener, local_id, shared_id, running) {}
        });

        Ok(descriptor)
    }

    pub fn tcp_connect(&self, address: impl Into<String>) -> Result<SharedRegionDescriptor> {
        let address = address.into();
        let stream = TcpStream::connect(&address)
            .map_err(|e| Error::Wasm(format!("tcp connect failed: {e}")))?;

        let (region, inbound_offset, outbound_offset) = create_stream_region(self)?;

        let shared_id = region.shared_id;
        let running = Arc::new(AtomicBool::new(true));

        self.inner.tcp_streams.lock().insert(
            shared_id,
            TcpStreamState {
                running: running.clone(),
            },
        );

        let proxy_local_id = self
            .attach_shared_region(shared_id)
            .map_err(|e| Error::Wasm(format!("proxy pre-attach failed: {e}")))?;

        let kernel = self.clone();
        thread::spawn(move || {
            let result = run_proxy(
                &kernel,
                stream,
                proxy_local_id,
                inbound_offset,
                outbound_offset,
                DEFAULT_RING_CAPACITY as u64,
                running,
            );
            drop(kernel.detach_shared_region(proxy_local_id));
            if let Err(_e) = result {}
        });

        Ok(region)
    }

    pub fn close_tcp_listener(&self, local_id: u64) -> Result<()> {
        let mut listeners = self.inner.tcp_listeners.lock();
        let state = listeners
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("tcp listener {local_id}")))?;
        state.running.store(false, Ordering::Relaxed);
        let shared_id = state.shared_id;
        drop(listeners);

        if let Some(queue) = self
            .inner
            .host_queues_by_shared
            .lock()
            .get(&shared_id)
            .cloned()
        {
            queue.notify.notify_waiters();
        }

        self.inner.host_queues_by_shared.lock().remove(&shared_id);
        self.inner.local_host_queues.lock().remove(&local_id);
        Ok(())
    }

    pub fn close_tcp_stream(&self, shared_id: u64) -> Result<()> {
        let state = self
            .inner
            .tcp_streams
            .lock()
            .remove(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("tcp stream {shared_id}")))?;
        state.running.store(false, Ordering::Release);
        Ok(())
    }

    pub fn udp_bind(&self, address: impl Into<String>) -> Result<SharedRegionDescriptor> {
        let address = address.into();
        let socket =
            UdpSocket::bind(&address).map_err(|e| Error::Wasm(format!("udp bind failed: {e}")))?;
        socket
            .set_read_timeout(Some(Duration::from_millis(100)))
            .map_err(|e| Error::Wasm(format!("udp set_read_timeout failed: {e}")))?;

        let (region, recv_offset, send_offset) = create_stream_region(self)?;

        let shared_id = region.shared_id;
        let running = Arc::new(AtomicBool::new(true));

        self.inner.udp_sockets.lock().insert(
            shared_id,
            UdpSocketState {
                running: running.clone(),
            },
        );

        let proxy_local_id = self
            .attach_shared_region(shared_id)
            .map_err(|e| Error::Wasm(format!("proxy pre-attach failed: {e}")))?;

        let kernel = self.clone();
        thread::spawn(move || {
            let result = run_udp_proxy(
                &kernel,
                socket,
                proxy_local_id,
                recv_offset,
                send_offset,
                DEFAULT_RING_CAPACITY as u64,
                running,
            );
            drop(kernel.detach_shared_region(proxy_local_id));
            if let Err(_e) = result {}
        });

        Ok(region)
    }

    pub fn close_udp_socket(&self, shared_id: u64) -> Result<()> {
        let state = self
            .inner
            .udp_sockets
            .lock()
            .remove(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("udp socket {shared_id}")))?;
        state.running.store(false, Ordering::Release);
        Ok(())
    }
}

fn align_up(value: u32, alignment: u32) -> u32 {
    let rem = value % alignment;
    if rem == 0 {
        value
    } else {
        value + alignment - rem
    }
}

/// Creates a multi-memory shared region with two ring buffers (inbound + outbound)
/// using the standard coordination layout that matches the guest-side `region.rs`.
fn create_stream_region(kernel: &Kernel) -> Result<(SharedRegionDescriptor, u64, u64)> {
    let ring_data_cap = DEFAULT_RING_CAPACITY;
    let ring_region_len = (PAGE_SIZE as u32) + ring_data_cap;
    let header_size = SHARED_REGION_HEADER_ENTRY_OFFSET + 2 * SHARED_REGION_HEADER_ENTRY_SIZE;
    let total_capacity = align_up(
        header_size + align_up(header_size + ring_region_len, 8) + ring_region_len,
        8,
    );

    let (shared_id, _len) = kernel
        .allocate_shared_region(total_capacity)
        .map_err(|e| Error::Wasm(format!("allocate region failed: {e}")))?;

    let mapping_id = kernel
        .attach_shared_region(shared_id)
        .map_err(|e| Error::Wasm(format!("attach region failed: {e}")))?;

    // Write multi-memory header.
    kernel
        .write_shared_memory(mapping_id, 0, &SHARED_REGION_MAGIC.to_le_bytes())
        .map_err(|e| Error::Wasm(e.to_string()))?;
    kernel
        .write_shared_memory(
            mapping_id,
            SHARED_REGION_HEADER_CAPACITY_OFFSET as u64,
            &(total_capacity as u64).to_le_bytes(),
        )
        .map_err(|e| Error::Wasm(e.to_string()))?;
    kernel
        .write_shared_memory(
            mapping_id,
            SHARED_REGION_HEADER_COUNT_OFFSET as u64,
            &2u32.to_le_bytes(),
        )
        .map_err(|e| Error::Wasm(e.to_string()))?;

    let sub_memory_0_offset = align_up(header_size, 8) as u64;
    let sub_memory_1_offset = align_up(sub_memory_0_offset as u32 + ring_region_len, 8) as u64;

    // Write entry[0]: inbound ring offset + length.
    kernel
        .write_shared_memory(
            mapping_id,
            SHARED_REGION_HEADER_ENTRY_OFFSET as u64,
            &(sub_memory_0_offset as u32).to_le_bytes(),
        )
        .map_err(|e| Error::Wasm(e.to_string()))?;
    kernel
        .write_shared_memory(
            mapping_id,
            SHARED_REGION_HEADER_ENTRY_OFFSET as u64 + 4,
            &ring_region_len.to_le_bytes(),
        )
        .map_err(|e| Error::Wasm(e.to_string()))?;

    // Write entry[1]: outbound ring offset + length.
    kernel
        .write_shared_memory(
            mapping_id,
            SHARED_REGION_HEADER_ENTRY_OFFSET as u64 + 8,
            &(sub_memory_1_offset as u32).to_le_bytes(),
        )
        .map_err(|e| Error::Wasm(e.to_string()))?;
    kernel
        .write_shared_memory(
            mapping_id,
            SHARED_REGION_HEADER_ENTRY_OFFSET as u64 + 12,
            &ring_region_len.to_le_bytes(),
        )
        .map_err(|e| Error::Wasm(e.to_string()))?;

    // Initialize both ring buffers with the standard coordination layout.
    init_ring_buffer(kernel, mapping_id, sub_memory_0_offset)?;
    init_ring_buffer(kernel, mapping_id, sub_memory_1_offset)?;

    // Kernel is the sole writer on the inbound ring; register writer count.
    kernel
        .fetch_add_shared_memory_u64(mapping_id, sub_memory_0_offset + WRITER_COUNT_OFFSET, 1)
        .map_err(|e| Error::Wasm(format!("increment inbound writer count failed: {e}")))?;

    // Allocate kernel reader slot 0 on the outbound ring at position 0.
    let slot_offset = sub_memory_1_offset + READER_SLOTS_OFFSET;
    kernel
        .write_shared_memory(mapping_id, slot_offset, &1u64.to_le_bytes())
        .map_err(|e| Error::Wasm(format!("allocate outbound reader slot failed: {e}")))?;

    drop(kernel.detach_shared_region(mapping_id));

    let region = SharedRegionDescriptor {
        shared_id,
        len: total_capacity as u64,
    };
    Ok((region, sub_memory_0_offset, sub_memory_1_offset))
}

/// Initialises a ring buffer with the standard coordination layout.
///
/// Zeros out all coordination fields in page 0:
/// - generation_counter (offset 0)
/// - next_tail (offset 8)
/// - writer_count (offset 16)
/// - reader_slots[128] (offset 24..1048)
/// - next_writer_id (offset 1048)
/// - reader_slot_counter (offset 1056)
/// - writer_slots[128] (offset 1080..2104)
/// - writer_slot_counter (offset 2104)
fn init_ring_buffer(kernel: &Kernel, mapping_id: u64, offset: u64) -> Result<()> {
    let zero_u64 = 0u64.to_le_bytes();

    kernel.write_shared_memory(mapping_id, offset + GENERATION_COUNTER_OFFSET, &zero_u64)?;
    kernel.write_shared_memory(mapping_id, offset + NEXT_TAIL_OFFSET, &zero_u64)?;
    kernel.write_shared_memory(mapping_id, offset + WRITER_COUNT_OFFSET, &zero_u64)?;

    // Zero out reader slots (128 × u64).
    for i in 0..MAX_READER_SLOTS {
        let slot_offset = offset + READER_SLOTS_OFFSET + i as u64 * 8;
        kernel.write_shared_memory(mapping_id, slot_offset, &zero_u64)?;
    }

    kernel.write_shared_memory(mapping_id, offset + NEXT_WRITER_ID_OFFSET, &zero_u64)?;
    kernel.write_shared_memory(mapping_id, offset + READER_SLOT_COUNTER_OFFSET, &zero_u64)?;

    // Zero out writer slots (128 × u64).
    for i in 0..MAX_WRITER_SLOTS {
        let slot_offset = offset + WRITER_SLOTS_OFFSET + i as u64 * 8;
        kernel.write_shared_memory(mapping_id, slot_offset, &zero_u64)?;
    }

    kernel.write_shared_memory(mapping_id, offset + WRITER_SLOT_COUNTER_OFFSET, &zero_u64)?;

    Ok(())
}

/// Returns the minimum active reader position from the shared reader_slots array.
fn minimum_reader_position(kernel: &Kernel, mapping_id: u64, offset: u64) -> Result<Option<u64>> {
    let mut minimum = None;
    for slot in 0..MAX_READER_SLOTS {
        let slot_offset = offset + READER_SLOTS_OFFSET + slot as u64 * 8;
        let encoded = read_u64(kernel, mapping_id, slot_offset)?;
        if encoded == 0 {
            continue;
        }
        let position = encoded - 1;
        minimum = Some(minimum.map_or(position, |current: u64| current.min(position)));
    }
    Ok(minimum)
}

/// Returns the minimum active writer position from the shared writer_slots array.
fn minimum_writer_position(kernel: &Kernel, mapping_id: u64, offset: u64) -> Result<Option<u64>> {
    let mut minimum = None;
    for slot in 0..MAX_WRITER_SLOTS {
        let slot_offset = offset + WRITER_SLOTS_OFFSET + slot as u64 * 8;
        let encoded = read_u64(kernel, mapping_id, slot_offset)?;
        if encoded == 0 {
            continue;
        }
        let position = encoded - 1;
        minimum = Some(minimum.map_or(position, |current: u64| current.min(position)));
    }
    Ok(minimum)
}

/// Inbound proxy: reads from TCP socket, writes frames to the inbound ring.
fn proxy_inbound(
    kernel: &Kernel,
    mut stream: TcpStream,
    mapping_id: u64,
    offset: u64,
    capacity: u64,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut buf = vec![0u8; 8192];

    while running.load(Ordering::Relaxed) {
        match stream.read(&mut buf) {
            Ok(0) => {
                // EOF detected; decrement inbound writer count to 0.
                drop(kernel.fetch_add_shared_memory_u64(
                    mapping_id,
                    offset + WRITER_COUNT_OFFSET,
                    u64::MAX,
                ));
                break;
            }
            Ok(n) => {
                if let Err(_e) = write_frame(
                    kernel,
                    mapping_id,
                    offset,
                    capacity,
                    buf.get(..n).unwrap_or(&[]),
                ) {
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(e) => {
                eprintln!("inbound read error: {e}");
                break;
            }
        }
    }

    running.store(false, Ordering::Relaxed);
    Ok(())
}

/// Outbound proxy: reads frames from the outbound ring, writes to TCP socket.
///
/// Polls the generation counter to detect new data written by the guest.
/// When the generation counter changes, reads available frames and writes
/// them to the TCP socket.
fn proxy_outbound(
    kernel: &Kernel,
    mut stream: TcpStream,
    mapping_id: u64,
    offset: u64,
    capacity: u64,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut reader_pos: u64 = 0;
    let mut last_generation: u64 = 0;

    while running.load(Ordering::Relaxed) {
        // Poll the generation counter to detect new data.
        let current_generation = read_u64(kernel, mapping_id, offset + GENERATION_COUNTER_OFFSET)?;

        if current_generation == last_generation {
            // No new data; check if all writers have disconnected.
            match read_u64(kernel, mapping_id, offset + WRITER_COUNT_OFFSET) {
                Ok(0) => {
                    drop(stream.shutdown(std::net::Shutdown::Write));
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
            thread::sleep(Duration::from_millis(PROXY_POLL_INTERVAL_MS));
            continue;
        }

        last_generation = current_generation;

        // Acquire fence ensures we see the writer's payload before the header.
        fence(Ordering::Acquire);

        // Read all available frames.
        loop {
            match read_frame(kernel, mapping_id, offset, capacity, &mut reader_pos) {
                Ok(Some(payload)) => {
                    update_kernel_reader_slot(
                        kernel,
                        mapping_id,
                        offset,
                        KERNEL_READER_SLOT,
                        reader_pos,
                    )?;
                    if let Err(_e) = stream.write_all(&payload) {
                        running.store(false, Ordering::Relaxed);
                        drop(release_kernel_reader_slot(
                            kernel,
                            mapping_id,
                            offset,
                            KERNEL_READER_SLOT,
                        ));
                        return Ok(());
                    }
                    if let Err(_e) = stream.flush() {
                        running.store(false, Ordering::Relaxed);
                        drop(release_kernel_reader_slot(
                            kernel,
                            mapping_id,
                            offset,
                            KERNEL_READER_SLOT,
                        ));
                        return Ok(());
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    running.store(false, Ordering::Relaxed);
                    drop(release_kernel_reader_slot(
                        kernel,
                        mapping_id,
                        offset,
                        KERNEL_READER_SLOT,
                    ));
                    return Ok(());
                }
            }
        }
    }

    drop(release_kernel_reader_slot(
        kernel,
        mapping_id,
        offset,
        KERNEL_READER_SLOT,
    ));

    running.store(false, Ordering::Relaxed);
    Ok(())
}

/// Reads data from a logical position in the ring buffer, handling wraparound.
fn read_at(
    kernel: &Kernel,
    mapping_id: u64,
    region_offset: u64,
    capacity: u64,
    pos: u64,
    len: u64,
) -> Result<Vec<u8>> {
    let data_offset = region_offset + DATA_OFFSET;
    let raw_pos = data_offset + (pos & (capacity - 1));
    let ring_end = data_offset + capacity;

    if raw_pos + len <= ring_end {
        kernel
            .read_shared_memory(mapping_id, raw_pos, len as usize)
            .map_err(|e| Error::Wasm(format!("read_at failed: {e}")))
    } else {
        let tail_len = ring_end - raw_pos;
        let head_len = len - tail_len;
        let mut result = Vec::with_capacity(len as usize);
        let tail_bytes = kernel
            .read_shared_memory(mapping_id, raw_pos, tail_len as usize)
            .map_err(|e| Error::Wasm(format!("read_at tail failed: {e}")))?;
        result.extend_from_slice(&tail_bytes);
        let head_bytes = kernel
            .read_shared_memory(mapping_id, data_offset, head_len as usize)
            .map_err(|e| Error::Wasm(format!("read_at head failed: {e}")))?;
        result.extend_from_slice(&head_bytes);
        Ok(result)
    }
}

/// Reads a frame from the ring buffer at the given reader position.
///
/// Returns `None` if no ready frame is available.
fn read_frame(
    kernel: &Kernel,
    mapping_id: u64,
    offset: u64,
    capacity: u64,
    reader_pos: &mut u64,
) -> Result<Option<Vec<u8>>> {
    let tail = read_u64(kernel, mapping_id, offset + NEXT_TAIL_OFFSET)?;
    if *reader_pos >= tail {
        return Ok(None);
    }

    let header_bytes = read_at(
        kernel,
        mapping_id,
        offset,
        capacity,
        *reader_pos,
        FRAME_HEADER_SIZE,
    )?;
    let header = FrameHeader::decode(&header_bytes)?;

    if !header.is_ready() {
        return Ok(None);
    }

    let frame_size = header.frame_size();
    if frame_size > capacity {
        return Err(Error::Wasm("invalid frame size".to_string()));
    }

    let payload_pos = *reader_pos + FRAME_HEADER_SIZE;
    let payload = read_at(
        kernel,
        mapping_id,
        offset,
        capacity,
        payload_pos,
        header.len as u64,
    )?;

    *reader_pos += frame_size;
    Ok(Some(payload))
}

/// Reads a little-endian u64 from shared memory.
fn read_u64(kernel: &Kernel, mapping_id: u64, offset: u64) -> Result<u64> {
    let bytes = kernel
        .read_shared_memory(mapping_id, offset, 8)
        .map_err(|e| Error::Wasm(format!("read u64 failed: {e}")))?;
    Ok(u64::from_le_bytes(bytes.try_into().map_err(|_e| {
        Error::Wasm("invalid u64 bytes".to_string())
    })?))
}

/// Releases a reader slot by setting it to 0.
fn release_kernel_reader_slot(
    kernel: &Kernel,
    mapping_id: u64,
    offset: u64,
    slot: u32,
) -> Result<()> {
    let slot_offset = offset + READER_SLOTS_OFFSET + slot as u64 * 8;
    kernel
        .write_shared_memory(mapping_id, slot_offset, &0u64.to_le_bytes())
        .map_err(|e| Error::Wasm(format!("release reader slot failed: {e}")))?;
    Ok(())
}

/// Reserves `len` bytes at the tail via CAS on the shared `next_tail` field.
///
/// Uses exponential backoff on contention and checks backpressure against
/// the shared `reader_slots` and `writer_slots` arrays.
fn reserve_tail(
    kernel: &Kernel,
    mapping_id: u64,
    offset: u64,
    capacity: u64,
    len: u64,
) -> Result<u64> {
    if len == 0 || len > capacity {
        return Err(Error::Wasm("invalid reservation length".to_string()));
    }

    let mut delay: usize = 1;
    loop {
        let tail = read_u64(kernel, mapping_id, offset + NEXT_TAIL_OFFSET)?;

        // Check backpressure against readers and writers.
        let min_reader_pos = minimum_reader_position(kernel, mapping_id, offset)?;
        let min_writer_pos = minimum_writer_position(kernel, mapping_id, offset)?;
        let next = tail
            .checked_add(len)
            .ok_or_else(|| Error::Wasm("tail reservation overflow".to_string()))?;

        let blocked_by_reader = min_reader_pos
            .map(|min_pos| next.saturating_sub(min_pos) > capacity)
            .unwrap_or(false);
        let blocked_by_writer = min_writer_pos
            .map(|min_pos| next.saturating_sub(min_pos) > capacity)
            .unwrap_or(false);

        if blocked_by_reader || blocked_by_writer {
            // Backpressure: ring is full. Use spin_loop for consistency
            // with guest-side reserve_tail.
            for _ in 0..delay {
                std::hint::spin_loop();
            }
            delay = (delay * 2).min(64);
            continue;
        }

        let prev = kernel
            .compare_exchange_shared_memory_u64(mapping_id, offset + NEXT_TAIL_OFFSET, tail, next)
            .map_err(|e| Error::Wasm(format!("cas failed: {e}")))?;

        if prev == tail {
            return Ok(tail);
        }

        // Exponential backoff on contention.
        for _ in 0..delay {
            std::hint::spin_loop();
        }
        delay = (delay * 2).min(64);
    }
}

#[expect(
    clippy::too_many_arguments,
    reason = "internal proxy function needs many params"
)]
fn run_proxy(
    kernel: &Kernel,
    stream: TcpStream,
    proxy_local_id: u64,
    inbound_offset: u64,
    outbound_offset: u64,
    capacity: u64,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mapping_id = proxy_local_id;

    let stream_inbound = stream
        .try_clone()
        .map_err(|e| Error::Wasm(format!("stream clone failed: {e}")))?;
    let stream_outbound = stream;

    let k_in = kernel.clone();
    let running_in = running.clone();

    let inbound_handle = thread::spawn(move || {
        if let Err(_e) = proxy_inbound(
            &k_in,
            stream_inbound,
            mapping_id,
            inbound_offset,
            capacity,
            running_in,
        ) {}
    });

    let k_out = kernel.clone();

    let outbound_handle = thread::spawn(move || {
        if let Err(_e) = proxy_outbound(
            &k_out,
            stream_outbound,
            mapping_id,
            outbound_offset,
            capacity,
            running,
        ) {}
    });

    drop(inbound_handle.join());
    drop(outbound_handle.join());

    Ok(())
}

fn run_udp_proxy(
    kernel: &Kernel,
    socket: UdpSocket,
    proxy_local_id: u64,
    recv_offset: u64,
    send_offset: u64,
    capacity: u64,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mapping_id = proxy_local_id;

    let socket_recv = socket
        .try_clone()
        .map_err(|e| Error::Wasm(format!("udp socket clone failed: {e}")))?;
    let socket_send = socket;

    let k_recv = kernel.clone();
    let running_recv = running.clone();

    let recv_handle = thread::spawn(move || {
        if let Err(_e) = udp_proxy_recv(
            &k_recv,
            socket_recv,
            mapping_id,
            recv_offset,
            capacity,
            running_recv,
        ) {}
    });

    let k_send = kernel.clone();

    let send_handle = thread::spawn(move || {
        if let Err(_e) = udp_proxy_send(
            &k_send,
            socket_send,
            mapping_id,
            send_offset,
            capacity,
            running,
        ) {}
    });

    drop(recv_handle.join());
    drop(send_handle.join());

    Ok(())
}

fn tcp_accept_loop(
    kernel: &Kernel,
    listener: TcpListener,
    queue_local_id: u64,
    _queue_shared_id: u64,
    running: Arc<AtomicBool>,
) -> Result<()> {
    listener
        .set_nonblocking(true)
        .map_err(|e| Error::Wasm(format!("set_nonblocking failed: {e}")))?;

    while running.load(Ordering::Relaxed) {
        match listener.accept() {
            Ok((stream, _addr)) => {
                let (region, inbound_offset, outbound_offset) = match create_stream_region(kernel) {
                    Ok(r) => r,
                    Err(e) => {
                        eprintln!("failed to create stream region: {e}");
                        continue;
                    }
                };

                let shared_id = region.shared_id;
                let running = Arc::new(AtomicBool::new(true));

                kernel.inner.tcp_streams.lock().insert(
                    shared_id,
                    TcpStreamState {
                        running: running.clone(),
                    },
                );

                let proxy_mapping = match kernel.attach_shared_region(shared_id) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("failed to pre-attach proxy mapping: {e}");
                        continue;
                    }
                };
                let proxy_local_id = proxy_mapping;

                let k = kernel.clone();

                thread::spawn(move || {
                    let result = run_proxy(
                        &k,
                        stream,
                        proxy_local_id,
                        inbound_offset,
                        outbound_offset,
                        DEFAULT_RING_CAPACITY as u64,
                        running,
                    );
                    drop(k.detach_shared_region(proxy_local_id));
                    if let Err(_e) = result {}
                });

                if let Err(e) = kernel.host_queue_send(queue_local_id, 0, shared_id) {
                    eprintln!("failed to enqueue connection: {e}");
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(e) => {
                return Err(Error::Wasm(format!("accept error: {e}")));
            }
        }
    }
    Ok(())
}

/// UDP recv proxy: reads datagrams from socket, writes frames to recv ring.
fn udp_proxy_recv(
    kernel: &Kernel,
    socket: UdpSocket,
    mapping_id: u64,
    offset: u64,
    capacity: u64,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut buf = vec![0u8; 65536];

    while running.load(Ordering::Relaxed) {
        match socket.recv_from(&mut buf) {
            Ok((n, addr)) => {
                // Frame format: [addr_len u16][addr bytes][payload]
                let addr_bytes = addr.to_string().into_bytes();
                let addr_len = addr_bytes.len() as u16;
                let payload_len = n;
                let frame_len = 2 + addr_bytes.len() + payload_len;

                let mut frame = Vec::with_capacity(frame_len);
                frame.extend_from_slice(&addr_len.to_le_bytes());
                frame.extend_from_slice(&addr_bytes);
                frame.extend_from_slice(&buf[..payload_len]);

                if let Err(_e) = write_frame(kernel, mapping_id, offset, capacity, &frame) {
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {}
            Err(e) => {
                eprintln!("udp recv error: {e}");
                break;
            }
        }
    }

    running.store(false, Ordering::Relaxed);
    Ok(())
}

/// UDP send proxy: reads frames from send ring, sends datagrams via socket.
///
/// Polls the generation counter to detect new data written by the guest.
/// When the generation counter changes, reads available frames and sends
/// them as UDP datagrams.
fn udp_proxy_send(
    kernel: &Kernel,
    socket: UdpSocket,
    mapping_id: u64,
    offset: u64,
    capacity: u64,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut reader_pos: u64 = 0;
    let mut last_generation: u64 = 0;

    while running.load(Ordering::Relaxed) {
        // Poll the generation counter to detect new data.
        let current_generation = read_u64(kernel, mapping_id, offset + GENERATION_COUNTER_OFFSET)?;

        if current_generation == last_generation {
            // No new data; check if all writers have disconnected.
            match read_u64(kernel, mapping_id, offset + WRITER_COUNT_OFFSET) {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => break,
            }
            thread::sleep(Duration::from_millis(PROXY_POLL_INTERVAL_MS));
            continue;
        }

        last_generation = current_generation;

        // Acquire fence ensures we see the writer's payload before the header.
        fence(Ordering::Acquire);

        // Read all available frames.
        loop {
            match read_frame(kernel, mapping_id, offset, capacity, &mut reader_pos) {
                Ok(Some(frame)) => {
                    update_kernel_reader_slot(
                        kernel,
                        mapping_id,
                        offset,
                        KERNEL_READER_SLOT,
                        reader_pos,
                    )?;

                    // Parse frame: [addr_len u16][addr bytes][payload]
                    if frame.len() < 2 {
                        continue;
                    }
                    let addr_len = u16::from_le_bytes([frame[0], frame[1]]) as usize;
                    if frame.len() < 2 + addr_len {
                        continue;
                    }
                    let addr_str = std::str::from_utf8(&frame[2..2 + addr_len])
                        .map_err(|e| Error::Wasm(format!("invalid address bytes: {e}")))?;
                    let addr: SocketAddr = addr_str
                        .parse()
                        .map_err(|e| Error::Wasm(format!("invalid address: {e}")))?;
                    let payload = &frame[2 + addr_len..];

                    if let Err(_e) = socket.send_to(payload, addr) {
                        running.store(false, Ordering::Relaxed);
                        drop(release_kernel_reader_slot(
                            kernel,
                            mapping_id,
                            offset,
                            KERNEL_READER_SLOT,
                        ));
                        return Ok(());
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    running.store(false, Ordering::Relaxed);
                    drop(release_kernel_reader_slot(
                        kernel,
                        mapping_id,
                        offset,
                        KERNEL_READER_SLOT,
                    ));
                    return Ok(());
                }
            }
        }
    }

    drop(release_kernel_reader_slot(
        kernel,
        mapping_id,
        offset,
        KERNEL_READER_SLOT,
    ));

    running.store(false, Ordering::Relaxed);
    Ok(())
}

/// Updates a reader slot in the shared `reader_slots` array.
fn update_kernel_reader_slot(
    kernel: &Kernel,
    mapping_id: u64,
    offset: u64,
    slot: u32,
    position: u64,
) -> Result<()> {
    let slot_offset = offset + READER_SLOTS_OFFSET + slot as u64 * 8;
    let encoded = position
        .checked_add(1)
        .ok_or_else(|| Error::Wasm("reader position overflow".to_string()))?;
    kernel
        .write_shared_memory(mapping_id, slot_offset, &encoded.to_le_bytes())
        .map_err(|e| Error::Wasm(format!("update reader slot failed: {e}")))?;
    Ok(())
}

/// Writes data at a logical position in the ring buffer, handling wraparound.
fn write_at(
    kernel: &Kernel,
    mapping_id: u64,
    region_offset: u64,
    capacity: u64,
    pos: u64,
    data: &[u8],
) -> Result<()> {
    let data_offset = region_offset + DATA_OFFSET;
    let raw_pos = data_offset + (pos & (capacity - 1));
    let ring_end = data_offset + capacity;

    if raw_pos + data.len() as u64 <= ring_end {
        kernel
            .write_shared_memory(mapping_id, raw_pos, data)
            .map_err(|e| Error::Wasm(format!("write_at failed: {e}")))
    } else {
        let tail_len = (ring_end - raw_pos) as usize;
        kernel
            .write_shared_memory(mapping_id, raw_pos, data.get(..tail_len).unwrap_or(&[]))
            .map_err(|e| Error::Wasm(format!("write_at tail failed: {e}")))?;
        kernel
            .write_shared_memory(mapping_id, data_offset, data.get(tail_len..).unwrap_or(&[]))
            .map_err(|e| Error::Wasm(format!("write_at head failed: {e}")))?;
        Ok(())
    }
}

/// Single-phase frame write with release fencing.
///
/// 1. Write payload at `pos + FRAME_HEADER_SIZE`
/// 2. Release fence (ensures payload is visible before header)
/// 3. Write header with READY flag at `pos`
/// 4. Bump generation counter
///
/// The kernel uses `write_shared_memory` which writes through the same
/// `mmap` as the guest. The release fence ensures the payload bytes are
/// committed to shared memory before the READY header becomes visible to
/// readers using acquire semantics.
fn write_frame(
    kernel: &Kernel,
    mapping_id: u64,
    offset: u64,
    capacity: u64,
    payload: &[u8],
) -> Result<()> {
    let frame_size = FRAME_HEADER_SIZE + payload.len() as u64;
    if frame_size > capacity {
        return Err(Error::Wasm("frame exceeds capacity".to_string()));
    }

    let pos = reserve_tail(kernel, mapping_id, offset, capacity, frame_size)?;

    // Step 1: Write payload.
    let payload_pos = pos + FRAME_HEADER_SIZE;
    write_at(kernel, mapping_id, offset, capacity, payload_pos, payload)?;

    // Step 2: Release fence ensures payload is visible before the header.
    fence(Ordering::Release);

    // Step 3: Write header with READY flag (single-phase).
    let ready_header = FrameHeader {
        len: payload.len() as u32,
        tag: 0,
        flags: FLAG_READY,
        _reserved: [0; 3],
    };
    write_at(
        kernel,
        mapping_id,
        offset,
        capacity,
        pos,
        &ready_header.encode(),
    )?;

    // Step 4: Bump generation counter.
    kernel
        .fetch_add_shared_memory_u64(mapping_id, offset + GENERATION_COUNTER_OFFSET, 1)
        .map_err(|e| Error::Wasm(format!("bump generation failed: {e}")))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Kernel;
    use std::time::Duration;

    #[test]
    fn tcp_bind_creates_host_queue() {
        let kernel = Kernel::default();
        let descriptor = kernel.tcp_bind("127.0.0.1:0").expect("tcp bind");
        assert!(descriptor.shared_id > 0);
        assert!(descriptor.local_id > 0);
        kernel.close_tcp_listener(descriptor.local_id).unwrap();
    }

    #[test]
    fn tcp_connect_returns_shared_region() {
        let kernel = Kernel::default();
        // Create a helper listener to accept the connection.
        let helper = std::net::TcpListener::bind("127.0.0.1:0").expect("bind helper");
        let addr = helper.local_addr().expect("helper addr");

        std::thread::spawn(move || {
            drop(helper.accept());
        });

        let descriptor = kernel.tcp_connect(addr.to_string()).expect("tcp connect");
        assert!(descriptor.shared_id > 0);
        assert!(descriptor.len > 0);

        // Verify the multi-memory header.
        let mapping_id = kernel
            .attach_shared_region(descriptor.shared_id)
            .expect("attach");
        let magic_bytes = kernel
            .read_shared_memory(mapping_id, 0, 8)
            .expect("read magic");
        let magic = u64::from_le_bytes(magic_bytes.try_into().unwrap());
        assert_eq!(magic, SHARED_REGION_MAGIC, "expected shared region magic");

        let count_bytes = kernel
            .read_shared_memory(mapping_id, SHARED_REGION_HEADER_COUNT_OFFSET as u64, 4)
            .expect("read count");
        let count = u32::from_le_bytes(count_bytes.try_into().unwrap());
        assert_eq!(count, 2, "expected 2 sub-memories");

        drop(kernel.detach_shared_region(mapping_id));
    }

    #[test]
    fn tcp_connect_proxy_reads_and_writes_frames() {
        let kernel = Kernel::default();
        let helper = std::net::TcpListener::bind("127.0.0.1:0").expect("bind helper");
        let addr = helper.local_addr().expect("helper addr");

        // Echo server: read data and send it back.
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = helper.accept() {
                let mut buf = [0u8; 256];
                if let Ok(n) = stream.read(&mut buf) {
                    drop(stream.write_all(buf.get(..n).unwrap_or(&[])));
                    drop(stream.flush());
                }
            }
        });

        let descriptor = kernel.tcp_connect(addr.to_string()).expect("tcp connect");
        let shared_id = descriptor.shared_id;

        // Attach to the shared region.
        let mapping_id = kernel.attach_shared_region(shared_id).expect("attach");

        // Read multi-memory header to get sub-memory offsets.
        let inbound_offset = {
            let bytes = kernel
                .read_shared_memory(mapping_id, SHARED_REGION_HEADER_ENTRY_OFFSET as u64, 4)
                .expect("read inbound offset");
            u32::from_le_bytes(bytes.try_into().unwrap()) as u64
        };
        let outbound_offset = {
            let bytes = kernel
                .read_shared_memory(
                    mapping_id,
                    (SHARED_REGION_HEADER_ENTRY_OFFSET + 8) as u64,
                    4,
                )
                .expect("read outbound offset");
            u32::from_le_bytes(bytes.try_into().unwrap()) as u64
        };

        // Increment outbound writer count to simulate the guest writer.
        kernel
            .fetch_add_shared_memory_u64(mapping_id, outbound_offset + WRITER_COUNT_OFFSET, 1)
            .expect("increment outbound writer count");

        // Write a frame to the outbound ring (guest -> kernel -> socket).
        let payload = b"hello proxy";
        write_frame(
            &kernel,
            mapping_id,
            outbound_offset,
            DEFAULT_RING_CAPACITY as u64,
            payload,
        )
        .expect("write frame");

        // Wait up to 5 seconds for the echo to come back on the inbound ring.
        let mut found = false;
        for _attempt in 0..50 {
            let mut reader_pos: u64 = 0;
            match read_frame(
                &kernel,
                mapping_id,
                inbound_offset,
                DEFAULT_RING_CAPACITY as u64,
                &mut reader_pos,
            ) {
                Ok(Some(data)) => {
                    if data == payload {
                        found = true;
                        break;
                    }
                }
                _ => std::thread::sleep(Duration::from_millis(100)),
            }
        }
        assert!(found, "expected a frame on inbound ring");

        drop(kernel.detach_shared_region(mapping_id));
    }

    #[test]
    fn tcp_connect_proxy_eof_propagation() {
        let kernel = Kernel::default();
        let helper = std::net::TcpListener::bind("127.0.0.1:0").expect("bind helper");
        let addr = helper.local_addr().expect("helper addr");

        // Server that reads, echoes, then closes.
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = helper.accept() {
                let mut buf = [0u8; 256];
                if let Ok(n) = stream.read(&mut buf) {
                    drop(stream.write_all(buf.get(..n).unwrap_or(&[])));
                    drop(stream.flush());
                }
                drop(stream); // Close the connection.
            }
        });

        let descriptor = kernel.tcp_connect(addr.to_string()).expect("tcp connect");
        let shared_id = descriptor.shared_id;
        let mapping_id = kernel.attach_shared_region(shared_id).expect("attach");

        let inbound_offset = {
            let bytes = kernel
                .read_shared_memory(mapping_id, SHARED_REGION_HEADER_ENTRY_OFFSET as u64, 4)
                .expect("read inbound offset");
            u32::from_le_bytes(bytes.try_into().unwrap()) as u64
        };
        let outbound_offset = {
            let bytes = kernel
                .read_shared_memory(
                    mapping_id,
                    (SHARED_REGION_HEADER_ENTRY_OFFSET + 8) as u64,
                    4,
                )
                .expect("read outbound offset");
            u32::from_le_bytes(bytes.try_into().unwrap()) as u64
        };

        // Increment outbound writer count.
        kernel
            .fetch_add_shared_memory_u64(mapping_id, outbound_offset + WRITER_COUNT_OFFSET, 1)
            .expect("increment outbound writer count");

        // Write a frame.
        let payload = b"hello proxy";
        write_frame(
            &kernel,
            mapping_id,
            outbound_offset,
            DEFAULT_RING_CAPACITY as u64,
            payload,
        )
        .expect("write frame");

        // Wait for the echo.
        let mut found = false;
        for _ in 0..50 {
            let mut reader_pos: u64 = 0;
            match read_frame(
                &kernel,
                mapping_id,
                inbound_offset,
                DEFAULT_RING_CAPACITY as u64,
                &mut reader_pos,
            ) {
                Ok(Some(data)) => {
                    if data == payload {
                        found = true;
                        break;
                    }
                }
                _ => std::thread::sleep(Duration::from_millis(100)),
            }
        }
        assert!(found, "expected a frame on inbound ring");

        // Decrement outbound writer count to simulate guest shutdown.
        kernel
            .fetch_add_shared_memory_u64(
                mapping_id,
                outbound_offset + WRITER_COUNT_OFFSET,
                u64::MAX,
            )
            .expect("decrement outbound writer count");

        // Wait for proxy to detect close and for server EOF to propagate back.
        std::thread::sleep(Duration::from_millis(1000));

        // Verify inbound ring writer_count is 0 (proxy_inbound decremented on EOF).
        let inbound_wc_bytes = kernel
            .read_shared_memory(mapping_id, inbound_offset + WRITER_COUNT_OFFSET, 8)
            .expect("read inbound writer count");
        let inbound_wc = u64::from_le_bytes(inbound_wc_bytes.try_into().unwrap());
        assert_eq!(
            inbound_wc, 0,
            "expected inbound writer_count to be 0 after EOF propagation"
        );

        drop(kernel.detach_shared_region(mapping_id));
    }

    #[test]
    fn udp_bind_returns_shared_region() {
        let kernel = Kernel::default();
        let descriptor = kernel.udp_bind("127.0.0.1:0").expect("udp bind");
        assert!(descriptor.shared_id > 0);
        assert!(descriptor.len > 0);
        kernel.close_udp_socket(descriptor.shared_id).unwrap();
    }

    #[test]
    fn create_stream_region_has_correct_layout() {
        let kernel = Kernel::default();
        let (region, inbound_offset, outbound_offset) =
            create_stream_region(&kernel).expect("create stream region");

        let mapping_id = kernel
            .attach_shared_region(region.shared_id)
            .expect("attach");

        // Verify magic.
        let magic_bytes = kernel
            .read_shared_memory(mapping_id, 0, 8)
            .expect("read magic");
        let magic = u64::from_le_bytes(magic_bytes.try_into().unwrap());
        assert_eq!(magic, SHARED_REGION_MAGIC);

        // Verify count.
        let count_bytes = kernel
            .read_shared_memory(mapping_id, SHARED_REGION_HEADER_COUNT_OFFSET as u64, 4)
            .expect("read count");
        let count = u32::from_le_bytes(count_bytes.try_into().unwrap());
        assert_eq!(count, 2);

        // Verify inbound ring coordination fields are initialized.
        let generation = read_u64(
            &kernel,
            mapping_id,
            inbound_offset + GENERATION_COUNTER_OFFSET,
        )
        .unwrap();
        assert_eq!(generation, 0, "generation counter should be 0");

        let next_tail = read_u64(&kernel, mapping_id, inbound_offset + NEXT_TAIL_OFFSET).unwrap();
        assert_eq!(next_tail, 0, "next_tail should be 0");

        // Inbound writer_count should be 1 (kernel is the writer).
        let wc = read_u64(&kernel, mapping_id, inbound_offset + WRITER_COUNT_OFFSET).unwrap();
        assert_eq!(wc, 1, "inbound writer_count should be 1");

        // Verify outbound ring has kernel reader slot 0 allocated.
        let slot0 = read_u64(&kernel, mapping_id, outbound_offset + READER_SLOTS_OFFSET).unwrap();
        assert_eq!(
            slot0, 1,
            "outbound reader slot 0 should be allocated (encoded position 1 = position 0)"
        );

        drop(kernel.detach_shared_region(mapping_id));
    }

    #[test]
    fn kernel_write_frame_visible_to_guest_layout() {
        // This test verifies that frames written by the kernel's write_frame
        // function are readable using the same layout as the guest's RingBuf.
        let kernel = Kernel::default();
        let (region, inbound_offset, _outbound_offset) =
            create_stream_region(&kernel).expect("create stream region");

        let mapping_id = kernel
            .attach_shared_region(region.shared_id)
            .expect("attach");

        // Write a frame using the kernel's write_frame.
        let payload = b"test payload";
        write_frame(
            &kernel,
            mapping_id,
            inbound_offset,
            DEFAULT_RING_CAPACITY as u64,
            payload,
        )
        .expect("write frame");

        // Verify the frame is readable.
        let mut reader_pos: u64 = 0;
        let frame = read_frame(
            &kernel,
            mapping_id,
            inbound_offset,
            DEFAULT_RING_CAPACITY as u64,
            &mut reader_pos,
        )
        .expect("read frame");

        assert_eq!(frame, Some(payload.to_vec()), "frame payload should match");

        // Verify generation counter was bumped.
        let generation = read_u64(
            &kernel,
            mapping_id,
            inbound_offset + GENERATION_COUNTER_OFFSET,
        )
        .unwrap();
        assert!(
            generation > 0,
            "generation counter should be > 0 after write"
        );

        // Verify next_tail was advanced.
        let next_tail = read_u64(&kernel, mapping_id, inbound_offset + NEXT_TAIL_OFFSET).unwrap();
        assert!(next_tail > 0, "next_tail should be > 0 after write");

        drop(kernel.detach_shared_region(mapping_id));
    }

    #[test]
    fn udp_socket_loopback_test() {
        let kernel = Kernel::default();

        // Create a helper socket to receive the datagram.
        let helper = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind helper");
        let helper_addr = helper.local_addr().expect("helper addr");

        // Bind a kernel UDP socket.
        let descriptor = kernel.udp_bind("127.0.0.1:0").expect("udp bind");
        let shared_id = descriptor.shared_id;

        // Attach to the shared region.
        let mapping_id = kernel.attach_shared_region(shared_id).expect("attach");

        // Read multi-memory header to get sub-memory offsets.
        let send_offset = {
            let bytes = kernel
                .read_shared_memory(
                    mapping_id,
                    (SHARED_REGION_HEADER_ENTRY_OFFSET + 8) as u64,
                    4,
                )
                .expect("read send offset");
            u32::from_le_bytes(bytes.try_into().unwrap()) as u64
        };

        // Increment send writer count.
        kernel
            .fetch_add_shared_memory_u64(mapping_id, send_offset + WRITER_COUNT_OFFSET, 1)
            .expect("increment send writer count");

        // Write a frame to the send ring addressed to the helper socket.
        let addr_str = helper_addr.to_string();
        let addr_bytes = addr_str.as_bytes();
        let mut frame = Vec::new();
        frame.extend_from_slice(&(addr_bytes.len() as u16).to_le_bytes());
        frame.extend_from_slice(addr_bytes);
        frame.extend_from_slice(b"loopback test");

        write_frame(
            &kernel,
            mapping_id,
            send_offset,
            DEFAULT_RING_CAPACITY as u64,
            &frame,
        )
        .expect("write frame");

        // Receive the datagram on the helper socket.
        let mut buf = [0u8; 256];
        helper
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set timeout");
        let (n, src_addr) = helper.recv_from(&mut buf).expect("recv from helper");
        assert_eq!(&buf[..n], b"loopback test");
        assert!(src_addr.ip().is_loopback());

        // Clean up
        kernel.close_udp_socket(shared_id).unwrap();
        drop(kernel.detach_shared_region(mapping_id));
    }
}
