use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use selium_abi::{HostQueueDescriptor, SharedRegionDescriptor};

use crate::{
    Error, Result,
    state::{Kernel, TcpListenerState, TcpStreamState},
};

const SHARED_REGION_MAGIC: u64 = 0x53454C49554D454D;
const SHARED_REGION_HEADER_CAPACITY_OFFSET: u32 = 8;
const SHARED_REGION_HEADER_COUNT_OFFSET: u32 = 16;
const SHARED_REGION_HEADER_ENTRY_OFFSET: u32 = 24;
const SHARED_REGION_HEADER_ENTRY_SIZE: u32 = 8;

const REGION_HEADER_BYTES: u64 = 4096;
const RING_MAGIC_PREFIX: u64 = 0x53454C494F524E47;
const CAPACITY_OFFSET: u64 = 8;
const WRITER_COUNT_OFFSET: u64 = 16;
const READER_COUNT_OFFSET: u64 = 24;
const NEXT_TAIL_OFFSET: u64 = 32;
const TAIL_CACHE_OFFSET: u64 = 40;
const SIGNAL_SHARED_ID_OFFSET: u64 = 48;
const NEXT_WRITER_ID_OFFSET: u64 = 56;
const NEXT_MUTATION_ID_OFFSET: u64 = 64;
const READER_SLOTS_OFFSET: u64 = 72;
const READER_SLOT_BYTES: u64 = 16;
const READER_ACTIVE_OFFSET: u64 = 0;
const MAX_READER_SLOTS: u16 = 128;
const KERNEL_READER_SLOT: u32 = 0;

const FRAME_HEADER_SIZE: u64 = 12;
const FLAG_READY: u8 = 1;

const DEFAULT_RING_CAPACITY: u32 = 64 * 1024;
const PROXY_POLL_INTERVAL_MS: u64 = 1;

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

        let (region, inbound_signal, outbound_signal, inbound_offset, outbound_offset) =
            create_stream_region(self)?;

        let shared_id = region.shared_id;
        let region_len = region.len;
        let running = Arc::new(AtomicBool::new(true));

        let inbound_sig = self
            .signal_state(inbound_signal.local_id)
            .map_err(|e| Error::Wasm(format!("inbound signal not found: {e}")))?;
        let outbound_sig = self
            .signal_state(outbound_signal.local_id)
            .map_err(|e| Error::Wasm(format!("outbound signal not found: {e}")))?;

        self.inner.tcp_streams.lock().insert(
            shared_id,
            TcpStreamState {
                running: running.clone(),
                inbound_signal: inbound_sig,
                outbound_signal: outbound_sig,
            },
        );

        // Pre-attach the proxy mapping in the caller thread so the shared memory
        // state is consistent across thread boundaries.
        let proxy_mapping = self
            .attach_shared_region(shared_id, 0, region_len)
            .map_err(|e| Error::Wasm(format!("proxy pre-attach failed: {e}")))?;
        let proxy_local_id = proxy_mapping.local_id;

        let kernel = self.clone();
        thread::spawn(move || {
            let result = run_proxy(
                &kernel,
                stream,
                proxy_local_id,
                inbound_offset,
                outbound_offset,
                DEFAULT_RING_CAPACITY as u64,
                inbound_signal,
                outbound_signal,
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

        // Notify any waiters on the host queue so they detect closure.
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
        state.inbound_signal.notify.notify_waiters();
        state.outbound_signal.notify.notify_waiters();
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

fn create_stream_region(
    kernel: &Kernel,
) -> Result<(
    SharedRegionDescriptor,
    selium_abi::SignalDescriptor,
    selium_abi::SignalDescriptor,
    u64,
    u64,
)> {
    let ring_data_cap = DEFAULT_RING_CAPACITY;
    let ring_region_len = (REGION_HEADER_BYTES + ring_data_cap as u64) as u32;
    let header_size = SHARED_REGION_HEADER_ENTRY_OFFSET + 2 * SHARED_REGION_HEADER_ENTRY_SIZE;
    let total_capacity = align_up(
        header_size + align_up(header_size + ring_region_len, 8) + ring_region_len,
        8,
    );

    let region = kernel
        .allocate_shared_region(total_capacity, 8)
        .map_err(|e| Error::Wasm(format!("allocate region failed: {e}")))?;
    let shared_id = region.shared_id;

    let mapping = kernel
        .attach_shared_region(shared_id, 0, total_capacity)
        .map_err(|e| Error::Wasm(format!("attach region failed: {e}")))?;
    let mapping_id = mapping.local_id;

    kernel
        .write_shared_memory(mapping_id, 0, &SHARED_REGION_MAGIC.to_le_bytes())
        .map_err(|e| Error::Wasm(e.to_string()))?;
    kernel
        .write_shared_memory(
            mapping_id,
            SHARED_REGION_HEADER_CAPACITY_OFFSET,
            &(total_capacity as u64).to_le_bytes(),
        )
        .map_err(|e| Error::Wasm(e.to_string()))?;
    kernel
        .write_shared_memory(
            mapping_id,
            SHARED_REGION_HEADER_COUNT_OFFSET,
            &2u32.to_le_bytes(),
        )
        .map_err(|e| Error::Wasm(e.to_string()))?;

    let sub_memory_0_offset = align_up(header_size, 8) as u64;
    let sub_memory_1_offset = align_up(sub_memory_0_offset as u32 + ring_region_len, 8) as u64;

    kernel
        .write_shared_memory(
            mapping_id,
            SHARED_REGION_HEADER_ENTRY_OFFSET,
            &(sub_memory_0_offset as u32).to_le_bytes(),
        )
        .map_err(|e| Error::Wasm(e.to_string()))?;
    kernel
        .write_shared_memory(
            mapping_id,
            SHARED_REGION_HEADER_ENTRY_OFFSET + 4,
            &ring_region_len.to_le_bytes(),
        )
        .map_err(|e| Error::Wasm(e.to_string()))?;
    kernel
        .write_shared_memory(
            mapping_id,
            SHARED_REGION_HEADER_ENTRY_OFFSET + 8,
            &(sub_memory_1_offset as u32).to_le_bytes(),
        )
        .map_err(|e| Error::Wasm(e.to_string()))?;
    kernel
        .write_shared_memory(
            mapping_id,
            SHARED_REGION_HEADER_ENTRY_OFFSET + 12,
            &ring_region_len.to_le_bytes(),
        )
        .map_err(|e| Error::Wasm(e.to_string()))?;

    let inbound_signal = kernel.create_signal();
    let outbound_signal = kernel.create_signal();

    init_ring_buffer(
        kernel,
        mapping_id,
        sub_memory_0_offset,
        ring_data_cap as u64,
        inbound_signal.shared_id,
    )
    .map_err(|e| Error::Wasm(format!("init inbound ring failed: {e}")))?;

    // Kernel is the sole writer on the inbound ring; register before proxy starts.
    kernel
        .fetch_add_shared_memory_u64(
            mapping_id,
            (sub_memory_0_offset + WRITER_COUNT_OFFSET) as u32,
            1,
        )
        .map_err(|e| Error::Wasm(format!("increment inbound writer count failed: {e}")))?;

    init_ring_buffer(
        kernel,
        mapping_id,
        sub_memory_1_offset,
        ring_data_cap as u64,
        outbound_signal.shared_id,
    )
    .map_err(|e| Error::Wasm(format!("init outbound ring failed: {e}")))?;

    // Allocate kernel reader slot 0 on the outbound ring at position 0.
    let slot_offset = sub_memory_1_offset + READER_SLOTS_OFFSET;
    kernel
        .write_shared_memory(mapping_id, slot_offset as u32, &1u64.to_le_bytes())
        .map_err(|e| Error::Wasm(format!("allocate outbound reader slot failed: {e}")))?;

    drop(kernel.detach_shared_region(mapping_id));

    Ok((
        region,
        inbound_signal,
        outbound_signal,
        sub_memory_0_offset,
        sub_memory_1_offset,
    ))
}

fn init_ring_buffer(
    kernel: &Kernel,
    mapping_id: u64,
    offset: u64,
    capacity: u64,
    signal_shared_id: u64,
) -> Result<()> {
    let base = offset as u32;
    kernel.write_shared_memory(mapping_id, base, &RING_MAGIC_PREFIX.to_le_bytes())?;
    kernel.write_shared_memory(
        mapping_id,
        base + CAPACITY_OFFSET as u32,
        &capacity.to_le_bytes(),
    )?;
    kernel.write_shared_memory(
        mapping_id,
        base + WRITER_COUNT_OFFSET as u32,
        &0u64.to_le_bytes(),
    )?;
    kernel.write_shared_memory(
        mapping_id,
        base + READER_COUNT_OFFSET as u32,
        &0u64.to_le_bytes(),
    )?;
    kernel.write_shared_memory(
        mapping_id,
        base + NEXT_TAIL_OFFSET as u32,
        &0u64.to_le_bytes(),
    )?;
    kernel.write_shared_memory(
        mapping_id,
        base + TAIL_CACHE_OFFSET as u32,
        &0u64.to_le_bytes(),
    )?;
    kernel.write_shared_memory(
        mapping_id,
        base + NEXT_WRITER_ID_OFFSET as u32,
        &0u64.to_le_bytes(),
    )?;
    kernel.write_shared_memory(
        mapping_id,
        base + NEXT_MUTATION_ID_OFFSET as u32,
        &0u64.to_le_bytes(),
    )?;
    kernel.write_shared_memory(
        mapping_id,
        base + SIGNAL_SHARED_ID_OFFSET as u32,
        &signal_shared_id.to_le_bytes(),
    )?;
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
                let (region, inbound_signal, outbound_signal, inbound_offset, outbound_offset) =
                    match create_stream_region(kernel) {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!("failed to create stream region: {e}");
                            continue;
                        }
                    };

                let shared_id = region.shared_id;
                let running = Arc::new(AtomicBool::new(true));

                let inbound_sig = match kernel.signal_state(inbound_signal.local_id) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("failed to get inbound signal state: {e}");
                        continue;
                    }
                };
                let outbound_sig = match kernel.signal_state(outbound_signal.local_id) {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("failed to get outbound signal state: {e}");
                        continue;
                    }
                };

                kernel.inner.tcp_streams.lock().insert(
                    shared_id,
                    TcpStreamState {
                        running: running.clone(),
                        inbound_signal: inbound_sig,
                        outbound_signal: outbound_sig,
                    },
                );

                let proxy_mapping = match kernel.attach_shared_region(shared_id, 0, region.len) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("failed to pre-attach proxy mapping: {e}");
                        continue;
                    }
                };
                let proxy_local_id = proxy_mapping.local_id;

                let k = kernel.clone();

                thread::spawn(move || {
                    let result = run_proxy(
                        &k,
                        stream,
                        proxy_local_id,
                        inbound_offset,
                        outbound_offset,
                        DEFAULT_RING_CAPACITY as u64,
                        inbound_signal,
                        outbound_signal,
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
    inbound_signal: selium_abi::SignalDescriptor,
    outbound_signal: selium_abi::SignalDescriptor,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mapping_id = proxy_local_id;

    let stream_inbound = stream
        .try_clone()
        .map_err(|e| Error::Wasm(format!("stream clone failed: {e}")))?;
    let stream_outbound = stream;

    let k_in = kernel.clone();
    let running_in = running.clone();
    let inbound_sig = kernel
        .signal_state(inbound_signal.local_id)
        .map_err(|e| Error::Wasm(format!("inbound signal not found: {e}")))?;

    let inbound_handle = thread::spawn(move || {
        if let Err(_e) = proxy_inbound(
            &k_in,
            stream_inbound,
            mapping_id,
            inbound_offset,
            capacity,
            inbound_sig,
            running_in,
        ) {}
    });

    let k_out = kernel.clone();
    let outbound_sig = kernel
        .signal_state(outbound_signal.local_id)
        .map_err(|e| Error::Wasm(format!("outbound signal not found: {e}")))?;

    let outbound_handle = thread::spawn(move || {
        if let Err(_e) = proxy_outbound(
            &k_out,
            stream_outbound,
            mapping_id,
            outbound_offset,
            capacity,
            outbound_sig,
            running,
        ) {}
    });

    drop(inbound_handle.join());
    drop(outbound_handle.join());

    Ok(())
}

fn proxy_inbound(
    kernel: &Kernel,
    mut stream: TcpStream,
    mapping_id: u64,
    offset: u64,
    capacity: u64,
    signal: Arc<crate::state::SignalState>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut buf = vec![0u8; 8192];

    while running.load(Ordering::Relaxed) {
        match stream.read(&mut buf) {
            Ok(0) => {
                // EOF detected; decrement inbound writer count
                let base = offset as u32;
                drop(kernel.fetch_add_shared_memory_u64(
                    mapping_id,
                    base + WRITER_COUNT_OFFSET as u32,
                    u64::MAX,
                ));
                signal.notify.notify_waiters();
                break;
            }
            Ok(n) => {
                if let Err(_e) = write_frame(
                    kernel,
                    mapping_id,
                    offset,
                    capacity,
                    buf.get(..n).unwrap_or(&[]),
                    &signal,
                ) {
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::TimedOut => {
                // Read timeout expired; check `running` and continue.
            }
            Err(e) => {
                eprintln!("inbound read error: {e}");
                break;
            }
        }
    }

    running.store(false, Ordering::Relaxed);
    Ok(())
}

fn proxy_outbound(
    kernel: &Kernel,
    mut stream: TcpStream,
    mapping_id: u64,
    offset: u64,
    capacity: u64,
    signal: Arc<crate::state::SignalState>,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut reader_pos: u64 = 0;

    while running.load(Ordering::Relaxed) {
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
                    break;
                }
                if let Err(_e) = stream.flush() {
                    break;
                }
                let _generation = signal.generation.fetch_add(1, Ordering::Release) + 1;
                signal.notify.notify_waiters();
            }
            Ok(None) => {
                let base = offset as u32;
                match kernel.read_shared_memory(mapping_id, base + WRITER_COUNT_OFFSET as u32, 8) {
                    Ok(bytes) => {
                        let writer_count = u64::from_le_bytes(bytes.try_into().unwrap_or([0; 8]));
                        if writer_count == 0 {
                            drop(stream.shutdown(std::net::Shutdown::Write));
                            break;
                        }
                    }
                    Err(_) => {
                        break;
                    }
                }
                thread::sleep(Duration::from_millis(PROXY_POLL_INTERVAL_MS));
            }
            Err(_) => {
                break;
            }
        }
    }

    // Release kernel reader slot on exit
    drop(release_kernel_reader_slot(
        kernel,
        mapping_id,
        offset,
        KERNEL_READER_SLOT,
    ));

    running.store(false, Ordering::Relaxed);
    Ok(())
}

fn write_frame(
    kernel: &Kernel,
    mapping_id: u64,
    offset: u64,
    capacity: u64,
    payload: &[u8],
    signal: &Arc<crate::state::SignalState>,
) -> Result<()> {
    let frame_size = FRAME_HEADER_SIZE + payload.len() as u64;
    if frame_size > capacity {
        return Err(Error::Wasm("frame exceeds capacity".to_string()));
    }

    let pos = reserve_tail(kernel, mapping_id, offset, capacity, frame_size)?;

    // Write pending header
    let header = FrameHeader {
        len: payload.len() as u32,
        tag: 0,
        flags: 0,
        _reserved: [0; 3],
    };
    write_at(kernel, mapping_id, offset, capacity, pos, &header.encode())?;

    // Write payload
    let payload_pos = pos + FRAME_HEADER_SIZE;
    write_at(kernel, mapping_id, offset, capacity, payload_pos, payload)?;

    // Write ready header
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

    // Notify signal
    signal.notify.notify_waiters();
    Ok(())
}

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
    let header = decode_frame_header(&header_bytes)?;

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

    for _ in 0..1024 {
        let tail = read_u64(kernel, mapping_id, offset + NEXT_TAIL_OFFSET)?;

        // Check backpressure: ensure we don't overtake the slowest reader.
        let min_reader_pos = minimum_reader_position(kernel, mapping_id, offset)?;
        let next = tail
            .checked_add(len)
            .ok_or_else(|| Error::Wasm("tail reservation overflow".to_string()))?;

        if let Some(min_pos) = min_reader_pos
            && next.saturating_sub(min_pos) > capacity
        {
            thread::yield_now();
            continue;
        }

        let prev = kernel
            .compare_exchange_shared_memory_u64(
                mapping_id,
                (offset + NEXT_TAIL_OFFSET) as u32,
                tail,
                next,
            )
            .map_err(|e| Error::Wasm(format!("cas failed: {e}")))?;

        if prev == tail {
            return Ok(tail);
        }
    }

    Err(Error::Wasm("reservation contended".to_string()))
}

fn read_u64(kernel: &Kernel, mapping_id: u64, offset: u64) -> Result<u64> {
    let bytes = kernel
        .read_shared_memory(mapping_id, offset as u32, 8)
        .map_err(|e| Error::Wasm(format!("read u64 failed: {e}")))?;
    Ok(u64::from_le_bytes(bytes.try_into().map_err(|_e| {
        Error::Wasm("invalid u64 bytes".to_string())
    })?))
}

fn minimum_reader_position(kernel: &Kernel, mapping_id: u64, offset: u64) -> Result<Option<u64>> {
    let mut minimum = None;
    for slot in 0..MAX_READER_SLOTS {
        let slot_offset =
            offset + READER_SLOTS_OFFSET + slot as u64 * READER_SLOT_BYTES + READER_ACTIVE_OFFSET;
        let encoded = read_u64(kernel, mapping_id, slot_offset)?;
        if encoded == 0 {
            continue;
        }
        let position = encoded - 1;
        minimum = Some(minimum.map_or(position, |current: u64| current.min(position)));
    }
    Ok(minimum)
}

fn update_kernel_reader_slot(
    kernel: &Kernel,
    mapping_id: u64,
    offset: u64,
    slot: u32,
    position: u64,
) -> Result<()> {
    let slot_offset = offset + READER_SLOTS_OFFSET + slot as u64 * READER_SLOT_BYTES;
    let encoded = position
        .checked_add(1)
        .ok_or_else(|| Error::Wasm("reader position overflow".to_string()))?;
    kernel
        .write_shared_memory(mapping_id, slot_offset as u32, &encoded.to_le_bytes())
        .map_err(|e| Error::Wasm(format!("update reader slot failed: {e}")))?;
    Ok(())
}

fn release_kernel_reader_slot(
    kernel: &Kernel,
    mapping_id: u64,
    offset: u64,
    slot: u32,
) -> Result<()> {
    let slot_offset = offset + READER_SLOTS_OFFSET + slot as u64 * READER_SLOT_BYTES;
    kernel
        .write_shared_memory(mapping_id, slot_offset as u32, &0u64.to_le_bytes())
        .map_err(|e| Error::Wasm(format!("release reader slot failed: {e}")))?;
    Ok(())
}

fn read_at(
    kernel: &Kernel,
    mapping_id: u64,
    region_offset: u64,
    capacity: u64,
    pos: u64,
    len: u64,
) -> Result<Vec<u8>> {
    let data_offset = region_offset + REGION_HEADER_BYTES;
    let raw_pos = data_offset + (pos & (capacity - 1));
    let ring_end = data_offset + capacity;

    if raw_pos + len <= ring_end {
        kernel
            .read_shared_memory(mapping_id, raw_pos as u32, len as usize)
            .map_err(|e| Error::Wasm(format!("read_at failed: {e}")))
    } else {
        let tail_len = ring_end - raw_pos;
        let head_len = len - tail_len;
        let mut result = Vec::with_capacity(len as usize);
        let tail_bytes = kernel
            .read_shared_memory(mapping_id, raw_pos as u32, tail_len as usize)
            .map_err(|e| Error::Wasm(format!("read_at tail failed: {e}")))?;
        result.extend_from_slice(&tail_bytes);
        let head_bytes = kernel
            .read_shared_memory(mapping_id, data_offset as u32, head_len as usize)
            .map_err(|e| Error::Wasm(format!("read_at head failed: {e}")))?;
        result.extend_from_slice(&head_bytes);
        Ok(result)
    }
}

fn write_at(
    kernel: &Kernel,
    mapping_id: u64,
    region_offset: u64,
    capacity: u64,
    pos: u64,
    data: &[u8],
) -> Result<()> {
    let data_offset = region_offset + REGION_HEADER_BYTES;
    let raw_pos = data_offset + (pos & (capacity - 1));
    let ring_end = data_offset + capacity;

    if raw_pos + data.len() as u64 <= ring_end {
        kernel
            .write_shared_memory(mapping_id, raw_pos as u32, data)
            .map_err(|e| Error::Wasm(format!("write_at failed: {e}")))
    } else {
        let tail_len = ring_end - raw_pos;
        kernel
            .write_shared_memory(
                mapping_id,
                raw_pos as u32,
                data.get(..tail_len as usize).unwrap_or(&[]),
            )
            .map_err(|e| Error::Wasm(format!("write_at tail failed: {e}")))?;
        kernel
            .write_shared_memory(
                mapping_id,
                data_offset as u32,
                data.get(tail_len as usize..).unwrap_or(&[]),
            )
            .map_err(|e| Error::Wasm(format!("write_at head failed: {e}")))?;
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct FrameHeader {
    len: u32,
    tag: u32,
    flags: u8,
    _reserved: [u8; 3],
}

impl FrameHeader {
    const FLAG_READY: u8 = 1;
    #[expect(dead_code, reason = "reserved for future use")]
    const FLAG_ABORTED: u8 = 1 << 1;

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
        12 + self.len as u64
    }

    fn is_ready(&self) -> bool {
        self.flags & Self::FLAG_READY != 0
    }
}

fn decode_frame_header(bytes: &[u8]) -> Result<FrameHeader> {
    FrameHeader::decode(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    use tokio::sync::Notify;

    #[test]
    fn tcp_bind_creates_host_queue() {
        let kernel = Kernel::default();
        let descriptor = kernel.tcp_bind("127.0.0.1:0").expect("tcp bind");
        assert!(descriptor.shared_id > 0);
        kernel.close_tcp_listener(descriptor.local_id).unwrap();
    }

    #[test]
    fn tcp_connect_returns_shared_region() {
        let kernel = Kernel::default();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || {
            drop(listener.accept());
        });

        let descriptor = kernel.tcp_connect(addr.to_string()).expect("tcp connect");
        assert!(descriptor.shared_id > 0);
    }

    #[test]
    fn tcp_bind_accept_enqueues_connection() {
        let kernel = Kernel::default();
        let descriptor = kernel.tcp_bind("127.0.0.1:0").expect("tcp bind");
        let shared_id = descriptor.shared_id;

        let addr = {
            let listeners = kernel.inner.tcp_listeners.lock();
            let state = listeners.get(&descriptor.local_id).expect("listener state");
            state._listener.local_addr().unwrap()
        };

        std::thread::spawn(move || {
            drop(std::net::TcpStream::connect(addr));
        });

        // Wait briefly for the accept thread to process the connection.
        std::thread::sleep(Duration::from_millis(500));

        let _result = kernel.host_queue_recv(shared_id);
        // The recv is async in kernel, but try_host_queue_recv is sync.
        let entry = kernel
            .try_host_queue_recv(descriptor.local_id)
            .expect("try recv");
        assert!(entry.is_some(), "expected a connection to be enqueued");
        let (client_process_id, value) = entry.unwrap();
        assert_eq!(client_process_id, 0);
        assert!(value > 0, "expected shared region id");

        kernel.close_tcp_listener(descriptor.local_id).unwrap();
    }

    #[test]
    fn tcp_connect_proxy_reads_and_writes_frames() {
        let kernel = Kernel::default();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 256];
                if let Ok(n) = stream.read(&mut buf) {
                    drop(stream.write_all(buf.get(..n).unwrap_or(&[])));
                }
            }
        });

        let descriptor = kernel.tcp_connect(addr.to_string()).expect("tcp connect");
        let shared_id = descriptor.shared_id;

        // Attach to the shared region and verify layout.
        let mapping = kernel
            .attach_shared_region(shared_id, 0, descriptor.len)
            .expect("attach");
        let mapping_id = mapping.local_id;

        // Read multi-memory header to get sub-memory offsets.
        let count_bytes = kernel
            .read_shared_memory(mapping_id, SHARED_REGION_HEADER_COUNT_OFFSET, 4)
            .expect("read count");
        let count = u32::from_le_bytes(count_bytes.try_into().unwrap());
        assert_eq!(count, 2);

        let inbound_offset_bytes = kernel
            .read_shared_memory(mapping_id, SHARED_REGION_HEADER_ENTRY_OFFSET, 4)
            .expect("read inbound offset");
        let inbound_offset = u32::from_le_bytes(inbound_offset_bytes.try_into().unwrap()) as u64;

        let outbound_offset_bytes = kernel
            .read_shared_memory(mapping_id, SHARED_REGION_HEADER_ENTRY_OFFSET + 8, 4)
            .expect("read outbound offset");
        let outbound_offset = u32::from_le_bytes(outbound_offset_bytes.try_into().unwrap()) as u64;

        // Increment outbound writer count to simulate the guest writer.
        kernel
            .fetch_add_shared_memory_u64(
                mapping_id,
                (outbound_offset + WRITER_COUNT_OFFSET) as u32,
                1,
            )
            .expect("increment outbound writer count");

        // Write a frame to the outbound ring (guest -> kernel -> socket).
        let payload = b"hello proxy";
        write_frame(
            &kernel,
            mapping_id,
            outbound_offset,
            DEFAULT_RING_CAPACITY as u64,
            payload,
            &Arc::new(crate::state::SignalState {
                generation: AtomicU64::new(0),
                notify: Notify::new(),
            }),
        )
        .expect("write frame");

        // Wait up to 10 seconds for the echo to come back on the inbound ring.
        let mut found = false;
        for _attempt in 0..100 {
            let mut reader_pos: u64 = 0;
            let frame = read_frame(
                &kernel,
                mapping_id,
                inbound_offset,
                DEFAULT_RING_CAPACITY as u64,
                &mut reader_pos,
            );
            match frame {
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
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 256];
                if let Ok(n) = stream.read(&mut buf) {
                    drop(stream.write_all(buf.get(..n).unwrap_or(&[])));
                }
                drop(stream);
            }
        });

        let descriptor = kernel.tcp_connect(addr.to_string()).expect("tcp connect");
        let shared_id = descriptor.shared_id;
        let mapping = kernel
            .attach_shared_region(shared_id, 0, descriptor.len)
            .expect("attach");
        let mapping_id = mapping.local_id;

        let inbound_offset_bytes = kernel
            .read_shared_memory(mapping_id, SHARED_REGION_HEADER_ENTRY_OFFSET, 4)
            .expect("read inbound offset");
        let inbound_offset = u32::from_le_bytes(inbound_offset_bytes.try_into().unwrap()) as u64;

        let outbound_offset_bytes = kernel
            .read_shared_memory(mapping_id, SHARED_REGION_HEADER_ENTRY_OFFSET + 8, 4)
            .expect("read outbound offset");
        let outbound_offset = u32::from_le_bytes(outbound_offset_bytes.try_into().unwrap()) as u64;

        kernel
            .fetch_add_shared_memory_u64(
                mapping_id,
                (outbound_offset + WRITER_COUNT_OFFSET) as u32,
                1,
            )
            .expect("increment outbound writer count");

        let payload = b"hello proxy";
        write_frame(
            &kernel,
            mapping_id,
            outbound_offset,
            DEFAULT_RING_CAPACITY as u64,
            payload,
            &Arc::new(crate::state::SignalState {
                generation: AtomicU64::new(0),
                notify: Notify::new(),
            }),
        )
        .expect("write frame");

        let mut found = false;
        for _ in 0..100 {
            let mut reader_pos: u64 = 0;
            let frame = read_frame(
                &kernel,
                mapping_id,
                inbound_offset,
                DEFAULT_RING_CAPACITY as u64,
                &mut reader_pos,
            );
            match frame {
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

        // Verify outbound writer_count is 1 before shutdown.
        let wc_bytes = kernel
            .read_shared_memory(
                mapping_id,
                (outbound_offset + WRITER_COUNT_OFFSET) as u32,
                8,
            )
            .expect("read writer count");
        let wc = u64::from_le_bytes(wc_bytes.try_into().unwrap());
        assert_eq!(wc, 1, "expected writer_count to be 1 before shutdown");

        // Decrement writer count to 0, simulating guest shutdown.
        kernel
            .fetch_add_shared_memory_u64(
                mapping_id,
                (outbound_offset + WRITER_COUNT_OFFSET) as u32,
                u64::MAX,
            )
            .expect("decrement outbound writer count");

        // Wait for proxy to detect close and for server EOF to propagate back.
        std::thread::sleep(Duration::from_millis(1000));

        // Verify inbound ring writer_count is 0 (proxy_inbound decremented on EOF).
        let inbound_wc_bytes = kernel
            .read_shared_memory(mapping_id, (inbound_offset + WRITER_COUNT_OFFSET) as u32, 8)
            .expect("read inbound writer count");
        let inbound_wc = u64::from_le_bytes(inbound_wc_bytes.try_into().unwrap());
        assert_eq!(
            inbound_wc, 0,
            "expected inbound writer_count to be 0 after EOF propagation"
        );

        drop(kernel.detach_shared_region(mapping_id));
    }

    #[test]
    fn tcp_connect_proxy_backpressure_on_full_ring() {
        let kernel = Kernel::default();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Server sends a large payload then keeps the connection alive briefly.
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let large_payload = vec![0xABu8; 256 * 1024];
                drop(stream.write_all(&large_payload));
                drop(stream.flush());
                std::thread::sleep(Duration::from_millis(2000));
                drop(stream);
            }
        });

        let descriptor = kernel.tcp_connect(addr.to_string()).expect("tcp connect");
        let shared_id = descriptor.shared_id;
        let mapping = kernel
            .attach_shared_region(shared_id, 0, descriptor.len)
            .expect("attach");
        let mapping_id = mapping.local_id;

        let inbound_offset_bytes = kernel
            .read_shared_memory(mapping_id, SHARED_REGION_HEADER_ENTRY_OFFSET, 4)
            .expect("read inbound offset");
        let inbound_offset =
            u32::from_le_bytes(inbound_offset_bytes.try_into().unwrap()) as u64;

        // Register a reader slot at position 0 on the inbound ring.
        // Without a reader, minimum_reader_position() returns None and
        // reserve_tail never enforces backpressure.
        let reader_slot_offset =
            inbound_offset + READER_SLOTS_OFFSET + READER_ACTIVE_OFFSET;
        kernel
            .write_shared_memory(mapping_id, reader_slot_offset as u32, &1u64.to_le_bytes())
            .expect("register inbound reader slot");

        // Allow proxy to start receiving data.
        std::thread::sleep(Duration::from_millis(500));

        // Verify the inbound ring has received data.
        let tail_bytes = kernel
            .read_shared_memory(mapping_id, (inbound_offset + NEXT_TAIL_OFFSET) as u32, 8)
            .expect("read tail");
        let tail = u64::from_le_bytes(tail_bytes.try_into().unwrap());
        assert!(tail > 0, "expected inbound ring to have received data");

        // Wait for backpressure to kick in.
        std::thread::sleep(Duration::from_millis(1000));

        // Tail should be bounded by ring capacity because proxy_inbound
        // cannot reserve space while the reader slot stays at position 0.
        let tail_bytes2 = kernel
            .read_shared_memory(mapping_id, (inbound_offset + NEXT_TAIL_OFFSET) as u32, 8)
            .expect("read tail 2");
        let tail2 = u64::from_le_bytes(tail_bytes2.try_into().unwrap());
        assert!(
            tail2 <= DEFAULT_RING_CAPACITY as u64 + FRAME_HEADER_SIZE,
            "expected inbound tail bounded by ring capacity, got {}",
            tail2
        );

        drop(kernel.detach_shared_region(mapping_id));
    }
}
