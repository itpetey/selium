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
pub use state::{collect_contracts_for_app, ensure_pipeline_consistency};
