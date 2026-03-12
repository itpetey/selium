//! Shared host-side runtime utilities for framing, QUIC/TLS, and TOML config loading.

mod config;
mod framing;
mod quic;

pub use config::load_toml_config;
pub use framing::{read_framed, write_framed};
pub use quic::{
    ClientTlsPaths, ServerTlsPaths, build_quic_client_config, build_quic_client_endpoint,
    build_quic_server_endpoint, build_tls_acceptor, build_tls_connector, derive_server_name,
    load_cert_chain, load_private_key, load_root_store, parse_authority_addr, parse_socket_addr,
};
