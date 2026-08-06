//! Guest networking: raw TCP and UDP sockets over shared-memory ring buffers.
//!
//! `TcpStream` and `TcpListener` provide byte-stream TCP, while `UdpSocket`
//! provides datagram UDP with binary-addressed frames. All addresses are
//! IP literals only — name resolution is a capability-gated typed RPC via
//! the DNS connector.

pub use tcp::{TcpListener, TcpStream};
pub use udp::{Datagram, UdpSocket};

pub mod tcp;
pub mod udp;
