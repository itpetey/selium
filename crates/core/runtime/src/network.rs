//! Async network I/O for the Selium runtime.
//!
//! Replaces the old `selium-kernel::network_runtime` module (which used
//! `std::thread::spawn` + blocking I/O) with `tokio::spawn` + async I/O.

use std::{
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use selium_abi::{HostQueueDescriptor, SharedRegionDescriptor};
use selium_kernel::Kernel;
use selium_memory::{MappingBackend, MultiMemoryHeader};
use selium_shm::layout::{self, RingReader, RingWriter};

use crate::error::Result;

/// Default ring buffer data capacity (64 KiB, power of two).
const DEFAULT_RING_CAPACITY: u64 = 64 * 1024;

/// Bind a TCP listener and spawn an async accept loop.
pub fn tcp_bind(kernel: &Kernel, address: String) -> Result<HostQueueDescriptor> {
    let std_listener = std::net::TcpListener::bind(&address)
        .map_err(|e| crate::Error::Host(format!("tcp bind failed: {e}")))?;
    std_listener
        .set_nonblocking(true)
        .map_err(|e| crate::Error::Host(format!("set_nonblocking failed: {e}")))?;
    let listener = tokio::net::TcpListener::from_std(
        std_listener
            .try_clone()
            .map_err(|e| crate::Error::Host(format!("listener clone failed: {e}")))?,
    )
    .map_err(|e| crate::Error::Host(format!("tcp listener from_std failed: {e}")))?;

    let memory = kernel.memory();
    let queues = kernel.queues();
    let descriptor = queues.create_host_queue(&memory);

    let local_id = descriptor.local_id;
    let shared_id = descriptor.shared_id;
    let running = Arc::new(AtomicBool::new(true));

    kernel.network().insert_tcp_listener(
        local_id,
        selium_kernel::TcpListenerState {
            shared_id,
            running: running.clone(),
            _listener: std_listener,
        },
    );

    let k = kernel.clone();
    tokio::spawn(accept_loop(k, listener, local_id, running));

    Ok(descriptor)
}

/// Connect to a TCP endpoint and spawn async proxy tasks.
pub fn tcp_connect(kernel: &Kernel, address: String) -> Result<SharedRegionDescriptor> {
    let std_stream = std::net::TcpStream::connect(&address)
        .map_err(|e| crate::Error::Host(format!("tcp connect failed: {e}")))?;
    std_stream
        .set_nonblocking(true)
        .map_err(|e| crate::Error::Host(format!("tcp set_nonblocking failed: {e}")))?;
    let stream = tokio::net::TcpStream::from_std(std_stream)
        .map_err(|e| crate::Error::Host(format!("tcp stream from_std failed: {e}")))?;

    let (descriptor, inbound_writer, outbound_reader, parent_local_id) =
        create_stream_region(kernel)?;

    let shared_id = descriptor.shared_id;
    let running = Arc::new(AtomicBool::new(true));

    kernel.network().insert_tcp_stream(
        shared_id,
        selium_kernel::TcpStreamState {
            running: running.clone(),
        },
    );

    let memory = kernel.memory();
    tokio::spawn(async move {
        let result = run_tcp_proxy(stream, inbound_writer, outbound_reader, running).await;
        drop(memory.detach_shared_region(parent_local_id));
        if let Err(_e) = result {}
    });

    Ok(descriptor)
}

/// Bind a UDP socket and spawn async recv/send proxy tasks.
pub fn udp_bind(kernel: &Kernel, address: String) -> Result<SharedRegionDescriptor> {
    let std_socket = std::net::UdpSocket::bind(&address)
        .map_err(|e| crate::Error::Host(format!("udp bind failed: {e}")))?;
    std_socket
        .set_nonblocking(true)
        .map_err(|e| crate::Error::Host(format!("udp set_nonblocking failed: {e}")))?;
    let socket = tokio::net::UdpSocket::from_std(std_socket)
        .map_err(|e| crate::Error::Host(format!("udp socket from_std failed: {e}")))?;

    let (descriptor, recv_writer, send_reader, parent_local_id) = create_stream_region(kernel)?;

    let shared_id = descriptor.shared_id;
    let running = Arc::new(AtomicBool::new(true));

    kernel.network().insert_udp_socket(
        shared_id,
        selium_kernel::UdpSocketState {
            running: running.clone(),
        },
    );

    let memory = kernel.memory();
    tokio::spawn(async move {
        let result = run_udp_proxy(socket, recv_writer, send_reader, running).await;
        drop(memory.detach_shared_region(parent_local_id));
        if let Err(_e) = result {}
    });

    Ok(descriptor)
}

async fn accept_loop(
    kernel: Kernel,
    listener: tokio::net::TcpListener,
    queue_local_id: u64,
    running: Arc<AtomicBool>,
) {
    while running.load(Ordering::Relaxed) {
        match listener.accept().await {
            Ok((stream, _addr)) => {
                let (region, inbound_writer, outbound_reader, parent_local_id) =
                    match create_stream_region(&kernel) {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!("failed to create stream region: {e}");
                            continue;
                        }
                    };

                let shared_id = region.shared_id;
                let stream_running = Arc::new(AtomicBool::new(true));

                kernel.network().insert_tcp_stream(
                    shared_id,
                    selium_kernel::TcpStreamState {
                        running: stream_running.clone(),
                    },
                );

                let memory = kernel.memory();
                tokio::spawn(async move {
                    let result =
                        run_tcp_proxy(stream, inbound_writer, outbound_reader, stream_running)
                            .await;
                    drop(memory.detach_shared_region(parent_local_id));
                    if let Err(_e) = result {}
                });

                let queues = kernel.queues();
                if let Err(e) = queues.host_queue_send(queue_local_id, 0, shared_id) {
                    eprintln!("failed to enqueue connection: {e}");
                }
            }
            Err(e) => {
                eprintln!("accept error in async loop: {e}");
                return;
            }
        }
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
        .map_err(|e| crate::Error::Host(e.to_string()))?;

    let parent_backend = memory
        .attach_backend(shared_id)
        .map_err(|e| crate::Error::Host(e.to_string()))?;
    let parent_local_id = parent_backend.local_id();

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
    .map_err(|e| crate::Error::Host(e.to_string()))?;

    let inbound_backend = parent_backend
        .sub_region(sub_memory_0_offset, ring_region_len as u64)
        .map_err(|e| crate::Error::Host(e.to_string()))?;
    let outbound_backend = parent_backend
        .sub_region(sub_memory_1_offset, ring_region_len as u64)
        .map_err(|e| crate::Error::Host(e.to_string()))?;

    layout::init_ring(inbound_backend.as_ref()).map_err(|e| crate::Error::Host(e.to_string()))?;
    layout::init_ring(outbound_backend.as_ref()).map_err(|e| crate::Error::Host(e.to_string()))?;

    layout::store_capacity(inbound_backend.as_ref(), DEFAULT_RING_CAPACITY)
        .map_err(|e| crate::Error::Host(e.to_string()))?;
    layout::store_capacity(outbound_backend.as_ref(), DEFAULT_RING_CAPACITY)
        .map_err(|e| crate::Error::Host(e.to_string()))?;

    let inbound_writer = RingWriter::open(inbound_backend, DEFAULT_RING_CAPACITY)
        .map_err(|e| crate::Error::Host(e.to_string()))?;
    inbound_writer
        .increment_writer_count()
        .map_err(|e| crate::Error::Host(e.to_string()))?;

    let outbound_reader = RingReader::open(outbound_backend, DEFAULT_RING_CAPACITY, true)
        .map_err(|e| crate::Error::Host(e.to_string()))?;

    let region = SharedRegionDescriptor {
        shared_id,
        len: total_capacity as u64,
    };
    Ok((region, inbound_writer, outbound_reader, parent_local_id))
}

/// Decodes a binary datagram frame into `(SocketAddr, payload_bytes)`.
/// Returns `None` if the frame is malformed.
fn decode_udp_frame(frame: &[u8]) -> Option<(std::net::SocketAddr, &[u8])> {
    if frame.len() < 8 {
        return None;
    }
    if *frame.first()? != 1 {
        return None;
    }
    let family = *frame.get(1)?;
    match family {
        4 => {
            if frame.len() < 8 {
                return None;
            }
            let ip = std::net::Ipv4Addr::new(
                *frame.get(2)?,
                *frame.get(3)?,
                *frame.get(4)?,
                *frame.get(5)?,
            );
            let port = u16::from_le_bytes([*frame.get(6)?, *frame.get(7)?]);
            let addr = std::net::SocketAddr::V4(std::net::SocketAddrV4::new(ip, port));
            Some((addr, frame.get(8..)?))
        }
        6 => {
            if frame.len() < 20 {
                return None;
            }
            let mut octets = [0u8; 16];
            octets.copy_from_slice(frame.get(2..18)?);
            let ip = std::net::Ipv6Addr::from(octets);
            let port = u16::from_le_bytes([*frame.get(18)?, *frame.get(19)?]);
            let addr = std::net::SocketAddr::V6(std::net::SocketAddrV6::new(ip, port, 0, 0));
            Some((addr, frame.get(20..)?))
        }
        _ => None,
    }
}

/// Encodes a `SocketAddr` + payload into the binary datagram frame format:
/// `[ver u8 = 1][family u8: 4|6][addr 4|16 bytes][port u16 LE][payload…]`
fn encode_udp_frame(addr: std::net::SocketAddr, payload: &[u8]) -> Vec<u8> {
    let addr_len = match addr {
        std::net::SocketAddr::V4(_) => 4usize,
        std::net::SocketAddr::V6(_) => 16usize,
    };
    let header_len = 2 + addr_len + 2;
    let mut frame = Vec::with_capacity(header_len + payload.len());
    frame.push(1u8); // version
    match addr {
        std::net::SocketAddr::V4(v4) => {
            frame.push(4u8);
            frame.extend_from_slice(&v4.ip().octets());
            frame.extend_from_slice(&v4.port().to_le_bytes());
        }
        std::net::SocketAddr::V6(v6) => {
            frame.push(6u8);
            frame.extend_from_slice(&v6.ip().octets());
            frame.extend_from_slice(&v6.port().to_le_bytes());
        }
    }
    frame.extend_from_slice(payload);
    frame
}

async fn proxy_inbound(
    stream: &mut (impl tokio::io::AsyncRead + Unpin + Send),
    writer: RingWriter,
    running: Arc<AtomicBool>,
) -> Result<()> {
    use tokio::io::AsyncReadExt;
    let mut buf = vec![0u8; 8192];

    while running.load(Ordering::Relaxed) {
        match stream.read(&mut buf).await {
            Ok(0) => {
                drop(writer.decrement_writer_count());
                break;
            }
            Ok(n) => {
                if let Err(_e) = writer.write_frame(buf.get(..n).unwrap_or(&[]), 0, 0) {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
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

async fn proxy_outbound(
    stream: &mut (impl tokio::io::AsyncWrite + Unpin + Send),
    mut reader: RingReader,
    running: Arc<AtomicBool>,
) -> Result<()> {
    use tokio::io::AsyncWriteExt;
    let mut last_generation: u64 = 0;

    while running.load(Ordering::Relaxed) {
        let current_generation = match reader.generation() {
            Ok(g) => g,
            Err(_) => break,
        };

        if current_generation == last_generation {
            match reader.writer_count() {
                Ok(0) => {
                    drop(stream.shutdown().await);
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            continue;
        }

        last_generation = current_generation;

        loop {
            match reader.read_frame() {
                Ok(Some((_header, payload))) => {
                    if let Err(_e) = stream.write_all(&payload).await {
                        running.store(false, Ordering::Relaxed);
                        return Ok(());
                    }
                    if let Err(_e) = stream.flush().await {
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

async fn run_tcp_proxy(
    stream: tokio::net::TcpStream,
    inbound_writer: RingWriter,
    outbound_reader: RingReader,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let running_in = running.clone();
    let (mut read_half, mut write_half) = tokio::io::split(stream);

    let inbound = tokio::spawn(async move {
        if let Err(_e) = proxy_inbound(&mut read_half, inbound_writer, running_in).await {}
    });

    let outbound = tokio::spawn(async move {
        if let Err(_e) = proxy_outbound(&mut write_half, outbound_reader, running).await {}
    });

    drop(inbound.await);
    drop(outbound.await);

    Ok(())
}

async fn run_udp_proxy(
    socket: tokio::net::UdpSocket,
    recv_writer: RingWriter,
    send_reader: RingReader,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let socket = Arc::new(socket);
    let running_recv = running.clone();
    let running_send = running.clone();

    let socket_recv = socket.clone();
    let recv_task = tokio::spawn(async move {
        if let Err(_e) = udp_proxy_recv(socket_recv, recv_writer, running_recv).await {}
    });

    let socket_send = socket.clone();
    let send_task = tokio::spawn(async move {
        if let Err(_e) = udp_proxy_send(socket_send, send_reader, running_send).await {}
    });

    drop(recv_task.await);
    drop(send_task.await);

    Ok(())
}

async fn udp_proxy_recv(
    socket: Arc<tokio::net::UdpSocket>,
    writer: RingWriter,
    running: Arc<AtomicBool>,
) -> Result<()> {
    let mut buf = vec![0u8; 65536];

    while running.load(Ordering::Relaxed) {
        match socket.recv_from(&mut buf).await {
            Ok((n, addr)) => {
                let payload = buf
                    .get(..n)
                    .ok_or_else(|| crate::Error::Host("payload exceeds buffer".to_string()))?;

                let frame = encode_udp_frame(addr, payload);

                if let Err(_e) = writer.write_frame(&frame, 0, 0) {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    continue;
                }
            }
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            Err(e) => {
                eprintln!("udp recv error: {e}");
                break;
            }
        }
    }

    running.store(false, Ordering::Relaxed);
    Ok(())
}

async fn udp_proxy_send(
    socket: Arc<tokio::net::UdpSocket>,
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
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            continue;
        }

        last_generation = current_generation;

        loop {
            match reader.read_frame() {
                Ok(Some((_header, frame))) => {
                    let (addr, payload) = match decode_udp_frame(&frame) {
                        Some(d) => (d.0, d.1),
                        None => continue,
                    };

                    if let Err(_e) = socket.send_to(payload, addr).await {
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
    use selium_kernel::Kernel;
    use selium_memory::MultiMemoryHeader;

    #[tokio::test]
    async fn tcp_bind_creates_host_queue() {
        let kernel = Kernel::default();
        let descriptor = tcp_bind(&kernel, "127.0.0.1:0".to_string()).expect("tcp bind");
        assert!(descriptor.shared_id > 0);
        assert!(descriptor.local_id > 0);
        kernel.close_tcp_listener(descriptor.local_id).unwrap();
    }

    #[tokio::test]
    async fn tcp_connect_returns_shared_region() {
        let kernel = Kernel::default();
        let helper = std::net::TcpListener::bind("127.0.0.1:0").expect("bind helper");
        let addr = helper.local_addr().expect("helper addr");

        std::thread::spawn(move || {
            drop(helper.accept());
        });

        let descriptor = tcp_connect(&kernel, addr.to_string()).expect("tcp connect");
        assert!(descriptor.shared_id > 0);
        assert!(descriptor.len > 0);

        let memory = kernel.memory();
        let backend = memory.attach_backend(descriptor.shared_id).expect("attach");
        let header = MultiMemoryHeader::parse(&backend, 0).expect("parse header");
        assert_eq!(header.count, 2, "expected 2 sub-memories");
    }

    #[tokio::test]
    async fn tcp_connect_proxy_echo() {
        let kernel = Kernel::default();
        let helper = std::net::TcpListener::bind("127.0.0.1:0").expect("bind helper");
        let addr = helper.local_addr().expect("helper addr");

        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = helper.accept() {
                use std::io::{Read, Write};
                let mut buf = [0u8; 256];
                if let Ok(n) = stream.read(&mut buf) {
                    drop(stream.write_all(buf.get(..n).unwrap_or(&[])));
                    drop(stream.flush());
                }
            }
        });

        let descriptor = tcp_connect(&kernel, addr.to_string()).expect("tcp connect");
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
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(found, "expected a frame on inbound ring");

        drop(guest_writer);
        drop(kernel.close_tcp_stream(shared_id));
    }

    #[tokio::test]
    async fn udp_bind_returns_shared_region() {
        let kernel = Kernel::default();
        let descriptor = udp_bind(&kernel, "127.0.0.1:0".to_string()).expect("udp bind");
        assert!(descriptor.shared_id > 0);
        assert!(descriptor.len > 0);
        kernel.close_udp_socket(descriptor.shared_id).unwrap();
    }

    #[tokio::test]
    async fn create_stream_region_has_correct_layout() {
        let kernel = Kernel::default();
        let (region, _inbound_writer, _outbound_reader, parent_local_id) =
            create_stream_region(&kernel).expect("create stream region");

        let memory = kernel.memory();
        let backend = memory.attach_backend(region.shared_id).expect("attach");
        let header = MultiMemoryHeader::parse(&backend, 0).expect("parse");
        assert_eq!(header.count, 2);

        drop(memory.detach_shared_region(backend.local_id()));
        drop(memory.detach_shared_region(parent_local_id));
    }

    #[tokio::test]
    async fn udp_socket_loopback_test() {
        let kernel = Kernel::default();

        let helper = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind helper");
        let helper_addr = helper.local_addr().expect("helper addr");

        let descriptor = udp_bind(&kernel, "127.0.0.1:0".to_string()).expect("udp bind");
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

        // Build binary datagram frame
        let frame = encode_udp_frame(helper_addr, b"loopback test");

        writer.write_frame(&frame, 0, 0).expect("write frame");

        // Yield to let the async proxy task process the frame.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        let mut buf = [0u8; 256];
        helper
            .set_read_timeout(Some(std::time::Duration::from_secs(5)))
            .expect("set timeout");
        let (n, src_addr) = helper.recv_from(&mut buf).expect("recv from helper");
        assert_eq!(&buf[..n], b"loopback test");
        assert!(src_addr.ip().is_loopback());

        kernel.close_udp_socket(shared_id).unwrap();
    }
}
