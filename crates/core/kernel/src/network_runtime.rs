use std::{
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream, UdpSocket},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

use selium_abi::{HostQueueDescriptor, SharedRegionDescriptor};
use selium_memory::{MappingBackend, MultiMemoryHeader};
use selium_shm::layout::{self, RingReader, RingWriter};

use crate::kernel::Kernel;
use crate::network::{TcpListenerState, TcpStreamState, UdpSocketState};
use crate::{Error, Result};

/// Default ring buffer data capacity (64 KiB, power of two).
const DEFAULT_RING_CAPACITY: u64 = 64 * 1024;
/// Polling interval for proxy threads waiting on guest writes.
const PROXY_POLL_INTERVAL_MS: u64 = 1;

impl Kernel {
    pub fn tcp_bind(&self, address: impl Into<String>) -> Result<HostQueueDescriptor> {
        let address = address.into();
        let listener = TcpListener::bind(&address)
            .map_err(|e| Error::Wasm(format!("tcp bind failed: {e}")))?;

        let queues = self.queues();
        let memory = self.memory();
        let descriptor = queues.create_host_queue(&memory);
        let local_id = descriptor.local_id;
        let shared_id = descriptor.shared_id;
        let running = Arc::new(AtomicBool::new(true));

        self.inner.network.inner.tcp_listeners.lock().insert(
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

        let (descriptor, inbound_writer, outbound_reader, parent_local_id) =
            create_stream_region(self)?;

        let shared_id = descriptor.shared_id;
        let running = Arc::new(AtomicBool::new(true));

        self.inner.network.inner.tcp_streams.lock().insert(
            shared_id,
            TcpStreamState {
                running: running.clone(),
            },
        );

        let kernel = self.clone();
        thread::spawn(move || {
            let result = run_proxy(
                stream,
                inbound_writer,
                outbound_reader,
                DEFAULT_RING_CAPACITY,
                running,
            );
            drop(kernel.memory().detach_shared_region(parent_local_id));
            if let Err(_e) = result {}
        });

        Ok(descriptor)
    }

    pub fn close_tcp_listener(&self, local_id: u64) -> Result<()> {
        let mut listeners = self.inner.network.inner.tcp_listeners.lock();
        let state = listeners
            .remove(&local_id)
            .ok_or_else(|| Error::NotFound(format!("tcp listener {local_id}")))?;
        state.running.store(false, Ordering::Relaxed);
        let shared_id = state.shared_id;
        drop(listeners);

        if let Some(queue) = self
            .inner
            .queues
            .inner
            .queues_by_shared
            .lock()
            .get(&shared_id)
            .cloned()
        {
            queue.notify.notify_all();
        }

        self.inner
            .queues
            .inner
            .queues_by_shared
            .lock()
            .remove(&shared_id);
        self.inner
            .queues
            .inner
            .local_queues
            .lock()
            .remove(&local_id);
        Ok(())
    }

    pub fn close_tcp_stream(&self, shared_id: u64) -> Result<()> {
        let state = self
            .inner
            .network
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

        let (descriptor, recv_writer, send_reader, parent_local_id) = create_stream_region(self)?;

        let shared_id = descriptor.shared_id;
        let running = Arc::new(AtomicBool::new(true));

        self.inner.network.inner.udp_sockets.lock().insert(
            shared_id,
            UdpSocketState {
                running: running.clone(),
            },
        );

        let kernel = self.clone();
        thread::spawn(move || {
            let result = run_udp_proxy(
                socket,
                recv_writer,
                send_reader,
                DEFAULT_RING_CAPACITY,
                running,
            );
            drop(kernel.memory().detach_shared_region(parent_local_id));
            if let Err(_e) = result {}
        });

        Ok(descriptor)
    }

    pub fn close_udp_socket(&self, shared_id: u64) -> Result<()> {
        let state = self
            .inner
            .network
            .inner
            .udp_sockets
            .lock()
            .remove(&shared_id)
            .ok_or_else(|| Error::NotFound(format!("udp socket {shared_id}")))?;
        state.running.store(false, Ordering::Release);
        Ok(())
    }
}

/// Aligns a value up to the given alignment.
fn align_up(value: u32, alignment: u32) -> u32 {
    let rem = value % alignment;
    if rem == 0 {
        value
    } else {
        value + alignment - rem
    }
}

/// Creates a multi-memory shared region with two ring buffers (inbound + outbound)
/// using the shared multi-memory header and ring layout primitives.
///
/// Returns the descriptor, inbound writer, outbound reader, and the parent
/// local mapping id (for cleanup via `detach_shared_region` when the proxies
/// finish).
fn create_stream_region(
    kernel: &Kernel,
) -> Result<(SharedRegionDescriptor, RingWriter, RingReader, u64)> {
    let ring_data_cap = DEFAULT_RING_CAPACITY as u32;
    let ring_region_len = selium_memory::RING_HEADER_SIZE as u32 + ring_data_cap;
    let header_size =
        selium_memory::HEADER_ENTRY_OFFSET as u32 + 2 * selium_memory::HEADER_ENTRY_SIZE as u32;
    let total_capacity = align_up(
        header_size + align_up(header_size + ring_region_len, 8) + ring_region_len,
        8,
    );

    let memory = kernel.memory();

    let (shared_id, _len) = memory
        .allocate_shared_region(total_capacity)
        .map_err(|e| Error::Wasm(e.to_string()))?;

    let parent_backend = memory.attach_backend(shared_id)?;
    let parent_local_id = parent_backend.local_id;

    let sub_memory_0_offset = align_up(header_size, 8) as u64;
    let sub_memory_1_offset = align_up(sub_memory_0_offset as u32 + ring_region_len, 8) as u64;

    MultiMemoryHeader::write_two_entries(
        &parent_backend,
        0,
        total_capacity as u64,
        [
            (sub_memory_0_offset, ring_region_len as u64),
            (sub_memory_1_offset, ring_region_len as u64),
        ],
    )
    .map_err(|e| Error::Wasm(e.to_string()))?;

    let inbound_backend = parent_backend
        .sub_region(sub_memory_0_offset, ring_region_len as u64)
        .map_err(|e| Error::Wasm(e.to_string()))?;
    let outbound_backend = parent_backend
        .sub_region(sub_memory_1_offset, ring_region_len as u64)
        .map_err(|e| Error::Wasm(e.to_string()))?;

    layout::init_ring(inbound_backend.as_ref()).map_err(ring_err)?;
    layout::init_ring(outbound_backend.as_ref()).map_err(ring_err)?;

    layout::store_capacity(inbound_backend.as_ref(), DEFAULT_RING_CAPACITY).map_err(ring_err)?;
    layout::store_capacity(outbound_backend.as_ref(), DEFAULT_RING_CAPACITY).map_err(ring_err)?;

    let inbound_writer =
        RingWriter::open(inbound_backend, DEFAULT_RING_CAPACITY).map_err(ring_err)?;
    inbound_writer.increment_writer_count().map_err(ring_err)?;

    let outbound_reader =
        RingReader::open(outbound_backend, DEFAULT_RING_CAPACITY, true).map_err(ring_err)?;

    let region = SharedRegionDescriptor {
        shared_id,
        len: total_capacity as u64,
    };
    Ok((region, inbound_writer, outbound_reader, parent_local_id))
}

/// Inbound proxy: reads from TCP socket, writes frames to the inbound ring.
fn proxy_inbound(
    mut stream: TcpStream,
    writer: RingWriter,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut buf = vec![0u8; 8192];

    while running.load(Ordering::Relaxed) {
        match stream.read(&mut buf) {
            Ok(0) => {
                drop(writer.decrement_writer_count());
                break;
            }
            Ok(n) => {
                if let Err(_e) = writer.write_frame(buf.get(..n).unwrap_or(&[]), 0, 0) {
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
fn proxy_outbound(
    mut stream: TcpStream,
    mut reader: RingReader,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut last_generation: u64 = 0;

    while running.load(Ordering::Relaxed) {
        let current_generation = match reader.generation() {
            Ok(g) => g,
            Err(_) => break,
        };

        if current_generation == last_generation {
            match reader.writer_count() {
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

        loop {
            match reader.read_frame() {
                Ok(Some((_header, payload))) => {
                    if let Err(_e) = stream.write_all(&payload) {
                        running.store(false, Ordering::Relaxed);
                        return Ok(());
                    }
                    if let Err(_e) = stream.flush() {
                        running.store(false, Ordering::Relaxed);
                        return Ok(());
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    running.store(false, Ordering::Relaxed);
                    return Ok(());
                }
            }
        }
    }

    running.store(false, Ordering::Relaxed);
    Ok(())
}

/// Maps a ring protocol error to a kernel error.
fn ring_err<E: std::fmt::Display>(e: E) -> Error {
    Error::Wasm(e.to_string())
}

fn run_proxy(
    stream: TcpStream,
    inbound_writer: RingWriter,
    outbound_reader: RingReader,
    _capacity: u64,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let stream_inbound = stream
        .try_clone()
        .map_err(|e| Error::Wasm(format!("stream clone failed: {e}")))?;
    let stream_outbound = stream;

    let running_in = running.clone();

    let inbound_handle = thread::spawn(move || {
        if let Err(_e) = proxy_inbound(stream_inbound, inbound_writer, running_in) {}
    });

    let outbound_handle = thread::spawn(move || {
        if let Err(_e) = proxy_outbound(stream_outbound, outbound_reader, running) {}
    });

    drop(inbound_handle.join());
    drop(outbound_handle.join());

    Ok(())
}

fn run_udp_proxy(
    socket: UdpSocket,
    recv_writer: RingWriter,
    send_reader: RingReader,
    _capacity: u64,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let socket_recv = socket
        .try_clone()
        .map_err(|e| Error::Wasm(format!("udp socket clone failed: {e}")))?;
    let socket_send = socket;

    let running_recv = running.clone();

    let recv_handle =
        thread::spawn(
            move || {
                if let Err(_e) = udp_proxy_recv(socket_recv, recv_writer, running_recv) {}
            },
        );

    let send_handle =
        thread::spawn(
            move || {
                if let Err(_e) = udp_proxy_send(socket_send, send_reader, running) {}
            },
        );

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
                let (region, inbound_writer, outbound_reader, parent_local_id) =
                    match create_stream_region(kernel) {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!("failed to create stream region: {e}");
                            continue;
                        }
                    };

                let shared_id = region.shared_id;
                let running = Arc::new(AtomicBool::new(true));

                kernel.inner.network.inner.tcp_streams.lock().insert(
                    shared_id,
                    TcpStreamState {
                        running: running.clone(),
                    },
                );

                let k = kernel.clone();

                thread::spawn(move || {
                    let result = run_proxy(
                        stream,
                        inbound_writer,
                        outbound_reader,
                        DEFAULT_RING_CAPACITY,
                        running,
                    );
                    drop(k.memory().detach_shared_region(parent_local_id));
                    if let Err(_e) = result {}
                });

                if let Err(e) = kernel
                    .queues()
                    .host_queue_send(queue_local_id, 0, shared_id)
                {
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
fn udp_proxy_recv(socket: UdpSocket, writer: RingWriter, running: Arc<AtomicBool>) -> Result<()> {
    let mut buf = vec![0u8; 65536];

    while running.load(Ordering::Relaxed) {
        match socket.recv_from(&mut buf) {
            Ok((n, addr)) => {
                let addr_bytes = addr.to_string().into_bytes();
                let addr_len = addr_bytes.len() as u16;
                let payload_len = n;
                let frame_len = 2 + addr_bytes.len() + payload_len;

                let mut frame = Vec::with_capacity(frame_len);
                frame.extend_from_slice(&addr_len.to_le_bytes());
                frame.extend_from_slice(&addr_bytes);
                frame.extend_from_slice(
                    buf.get(..payload_len)
                        .ok_or_else(|| Error::Wasm("payload exceeds buffer".to_string()))?,
                );

                if let Err(_e) = writer.write_frame(&frame, 0, 0) {
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
fn udp_proxy_send(
    socket: UdpSocket,
    mut reader: RingReader,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut last_generation: u64 = 0;

    while running.load(Ordering::Relaxed) {
        let current_generation = match reader.generation() {
            Ok(g) => g,
            Err(_) => break,
        };

        if current_generation == last_generation {
            match reader.writer_count() {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => break,
            }
            thread::sleep(Duration::from_millis(PROXY_POLL_INTERVAL_MS));
            continue;
        }

        last_generation = current_generation;

        loop {
            match reader.read_frame() {
                Ok(Some((_header, frame))) => {
                    if frame.len() < 2 {
                        continue;
                    }
                    let prefix = frame
                        .get(0..2)
                        .ok_or_else(|| Error::Wasm("short udp frame".to_string()))?;
                    let addr_len = u16::from_le_bytes(
                        prefix
                            .try_into()
                            .map_err(|_error| Error::Wasm("short udp frame".to_string()))?,
                    ) as usize;
                    let addr_bytes = frame
                        .get(2..2 + addr_len)
                        .ok_or_else(|| Error::Wasm("udp frame missing address".to_string()))?;
                    let addr_str = std::str::from_utf8(addr_bytes)
                        .map_err(|e| Error::Wasm(format!("invalid address bytes: {e}")))?;
                    let addr: SocketAddr = addr_str
                        .parse()
                        .map_err(|e| Error::Wasm(format!("invalid address: {e}")))?;
                    let payload = frame
                        .get(2 + addr_len..)
                        .ok_or_else(|| Error::Wasm("udp frame missing payload".to_string()))?;

                    if let Err(_e) = socket.send_to(payload, addr) {
                        running.store(false, Ordering::Relaxed);
                        return Ok(());
                    }
                }
                Ok(None) => break,
                Err(_) => {
                    running.store(false, Ordering::Relaxed);
                    return Ok(());
                }
            }
        }
    }

    running.store(false, Ordering::Relaxed);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::Kernel;
    use selium_memory::MultiMemoryHeader;
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
        let helper = std::net::TcpListener::bind("127.0.0.1:0").expect("bind helper");
        let addr = helper.local_addr().expect("helper addr");

        std::thread::spawn(move || {
            drop(helper.accept());
        });

        let descriptor = kernel.tcp_connect(addr.to_string()).expect("tcp connect");
        assert!(descriptor.shared_id > 0);
        assert!(descriptor.len > 0);

        let memory = kernel.memory();
        let backend = memory.attach_backend(descriptor.shared_id).expect("attach");
        let header = MultiMemoryHeader::parse(&backend, 0).expect("parse header");
        assert_eq!(header.count, 2, "expected 2 sub-memories");
    }

    #[test]
    fn tcp_connect_proxy_echo() {
        let kernel = Kernel::default();
        let helper = std::net::TcpListener::bind("127.0.0.1:0").expect("bind helper");
        let addr = helper.local_addr().expect("helper addr");

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

        let memory = kernel.memory();
        let parent_backend = memory.attach_backend(shared_id).expect("attach");
        let header = MultiMemoryHeader::parse(&parent_backend, 0).expect("parse");
        let outbound_entry = header.entry(1).expect("outbound entry");

        let outbound_backend = parent_backend
            .sub_region(outbound_entry.offset, outbound_entry.length)
            .expect("outbound sub");

        let guest_writer =
            RingWriter::open(outbound_backend, DEFAULT_RING_CAPACITY).expect("open writer");
        guest_writer.increment_writer_count().expect("inc wc");
        guest_writer
            .write_frame(b"hello proxy", 0, 0)
            .expect("write frame");

        let inbound_entry = header.entry(0).expect("inbound entry");
        let inbound_backend = parent_backend
            .sub_region(inbound_entry.offset, inbound_entry.length)
            .expect("inbound sub");
        let mut guest_reader =
            RingReader::open(inbound_backend, DEFAULT_RING_CAPACITY, false).expect("open reader");

        let mut found = false;
        for _ in 0..50 {
            if let Ok(Some((_h, payload))) = guest_reader.read_frame()
                && payload == b"hello proxy"
            {
                found = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        assert!(found, "expected a frame on inbound ring");

        drop(guest_writer);
        drop(kernel.close_tcp_stream(shared_id));
    }

    #[test]
    fn tcp_connect_proxy_eof_propagation() {
        let kernel = Kernel::default();
        let helper = std::net::TcpListener::bind("127.0.0.1:0").expect("bind helper");
        let addr = helper.local_addr().expect("helper addr");

        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = helper.accept() {
                let mut buf = [0u8; 256];
                if let Ok(n) = stream.read(&mut buf) {
                    drop(stream.write_all(buf.get(..n).unwrap_or(&[])));
                    drop(stream.flush());
                }
                drop(stream);
            }
        });

        let descriptor = kernel.tcp_connect(addr.to_string()).expect("tcp connect");
        let shared_id = descriptor.shared_id;

        let memory = kernel.memory();
        let parent_backend = memory.attach_backend(shared_id).expect("attach");
        let header = MultiMemoryHeader::parse(&parent_backend, 0).expect("parse");
        let outbound_entry = header.entry(1).expect("outbound entry");

        let outbound_backend = parent_backend
            .sub_region(outbound_entry.offset, outbound_entry.length)
            .expect("outbound sub");
        let guest_writer =
            RingWriter::open(outbound_backend, DEFAULT_RING_CAPACITY).expect("open writer");
        guest_writer.increment_writer_count().expect("inc wc");
        guest_writer
            .write_frame(b"hello proxy", 0, 0)
            .expect("write frame");

        let inbound_entry = header.entry(0).expect("inbound entry");
        let inbound_backend = parent_backend
            .sub_region(inbound_entry.offset, inbound_entry.length)
            .expect("inbound sub");
        let mut guest_reader =
            RingReader::open(inbound_backend, DEFAULT_RING_CAPACITY, false).expect("open reader");

        let mut found = false;
        for _ in 0..50 {
            if let Ok(Some((_h, payload))) = guest_reader.read_frame()
                && payload == b"hello proxy"
            {
                found = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        assert!(found, "expected a frame on inbound ring");

        drop(guest_writer);
        std::thread::sleep(Duration::from_millis(1000));

        let inbound_backend2 = parent_backend
            .sub_region(inbound_entry.offset, inbound_entry.length)
            .expect("inbound sub2");
        let wc = inbound_backend2
            .atomic_load_u64(layout::WRITER_COUNT_OFFSET, Ordering::Acquire)
            .expect("read wc");
        assert_eq!(wc, 0, "expected inbound writer_count to be 0 after EOF");

        drop(kernel.close_tcp_stream(shared_id));
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
        let (region, _inbound_writer, _outbound_reader, parent_local_id) =
            create_stream_region(&kernel).expect("create stream region");

        let memory = kernel.memory();
        let backend = memory.attach_backend(region.shared_id).expect("attach");
        let header = MultiMemoryHeader::parse(&backend, 0).expect("parse");
        assert_eq!(header.count, 2);

        drop(memory.detach_shared_region(backend.local_id));
        drop(memory.detach_shared_region(parent_local_id));
    }

    #[test]
    fn kernel_write_frame_visible_to_guest_layout() {
        let kernel = Kernel::default();
        let (_region, inbound_writer, _outbound_reader, _parent_local_id) =
            create_stream_region(&kernel).expect("create stream region");

        inbound_writer
            .write_frame(b"test payload", 0, 0)
            .expect("write frame");

        let backend = inbound_writer.backend();
        let mut reader =
            RingReader::open(backend, DEFAULT_RING_CAPACITY, false).expect("open reader");
        let (_header, payload) = reader.read_frame().expect("read frame").expect("got frame");
        assert_eq!(payload, b"test payload");

        let generation = reader.generation().expect("generation");
        assert!(
            generation > 0,
            "generation counter should be > 0 after write"
        );
    }

    #[test]
    fn udp_socket_loopback_test() {
        let kernel = Kernel::default();

        let helper = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind helper");
        let helper_addr = helper.local_addr().expect("helper addr");

        let descriptor = kernel.udp_bind("127.0.0.1:0").expect("udp bind");
        let shared_id = descriptor.shared_id;

        let memory = kernel.memory();
        let parent_backend = memory.attach_backend(shared_id).expect("attach parent");
        let header = MultiMemoryHeader::parse(&parent_backend, 0).expect("parse header");
        let send_entry = header.entry(1).expect("send entry");

        let send_backend = parent_backend
            .sub_region(send_entry.offset, send_entry.length)
            .expect("sub region");

        let writer = RingWriter::open(send_backend, DEFAULT_RING_CAPACITY).expect("open writer");
        writer.increment_writer_count().expect("inc wc");

        let addr_str = helper_addr.to_string();
        let addr_bytes = addr_str.as_bytes();
        let mut frame = Vec::new();
        frame.extend_from_slice(&(addr_bytes.len() as u16).to_le_bytes());
        frame.extend_from_slice(addr_bytes);
        frame.extend_from_slice(b"loopback test");

        writer.write_frame(&frame, 0, 0).expect("write frame");

        let mut buf = [0u8; 256];
        helper
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set timeout");
        let (n, src_addr) = helper.recv_from(&mut buf).expect("recv from helper");
        assert_eq!(&buf[..n], b"loopback test");
        assert!(src_addr.ip().is_loopback());

        kernel.close_udp_socket(shared_id).unwrap();
    }
}
