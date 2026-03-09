//! Control-plane contracts, IDL parsing, registry, and desired-state resources.

mod codegen;
mod idl;
mod model;
mod refs;
mod registry;
mod state;

pub use codegen::generate_rust_bindings;
pub use idl::parse_idl;
pub use model::*;
pub use refs::parse_contract_ref;
pub use state::{
    build_discovery_state, collect_contracts_for_app, collect_contracts_for_workload,
    ensure_pipeline_consistency,
};
