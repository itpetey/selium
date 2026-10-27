//! Durable-slice deploy integration test.
//!
//! Deploys the real `selium-discovery`, `selium-store` and
//! `selium-deploy-probe` WASM guests on a single host. The probe uploads a
//! child module to the store, records a manifest, resolves the module's
//! durable slice, and spawns the child from it. A second boot of the probe
//! (with the same store) resolves the persisted manifest and spawns from the
//! durable slice again — module bytes never round-trip through the guest.
//!
//! This test is `#[ignore]`d by default because it requires the WASM guests to
//! be built for `wasm32-unknown-unknown` first:
//!
//! ```sh
//! cargo build --target wasm32-unknown-unknown \
//!   -p selium-discovery -p selium-store -p selium-deploy-probe
//! cargo test -p selium-runtime --test deploy_slice -- --ignored
//! ```

use selium_abi::{Capability, CapabilityGrant, ResourceClass, ResourceSelector};
use selium_runtime::{BootstrapReport, RuntimeConfig, SystemGuestArg, SystemGuestDescriptor};

mod common;
mod spine_common;

fn child_module() -> Vec<u8> {
    wat::parse_str("(module (memory 1) (func (export \"child\")))").expect("compile child wat")
}

fn deploy_probe_descriptor(module_bytes: Vec<u8>, child_module: Vec<u8>) -> SystemGuestDescriptor {
    SystemGuestDescriptor {
        name: "deploy-probe".to_string(),
        module_id: "deploy-probe-module".to_string(),
        module_bytes,
        entrypoint: "deploy_probe".to_string(),
        arguments: vec![SystemGuestArg::Pointer(child_module)],
        grants: vec![
            CapabilityGrant::new(
                Capability::Storage,
                vec![ResourceSelector::ResourceClass(ResourceClass::Volume)],
            ),
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            ),
            CapabilityGrant::new(
                Capability::HostQueue,
                vec![ResourceSelector::ResourceClass(ResourceClass::HostQueue)],
            ),
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::ResourceClass(ResourceClass::Process)],
            ),
        ],
        tenant: None,
        discovery_service: false,
        handlers: Vec::new(),
    }
}

#[test]
#[ignore = "requires the discovery, store and deploy-probe guests built for wasm32-unknown-unknown"]
fn deploy_spawns_from_a_durable_slice_across_restart() {
    let state = spine_common::StateRoot::new("deploy-slice");
    let runtime = spine_common::runtime_in_state_root(&state);

    // ── First lifetime: discovery, store, and the probe ────────────────────
    let report = runtime
        .bootstrap_system_guests(RuntimeConfig {
            start_discovery: true,
            system_guests: vec![
                spine_common::discovery_descriptor(common::read_guest_wasm_debug(
                    "selium-discovery",
                    "selium_discovery.wasm",
                )),
                spine_common::store_descriptor(vec!["discovery".to_string()]),
                deploy_probe_descriptor(probe_wasm(), child_module()),
            ],
            state_dir: state.config_dir(),
        })
        .expect("bootstrap system guests");

    let probe = find(&report, "deploy-probe");
    // Waited, not drained: the probe parks on its blob-store and store RPCs,
    // so on a correct reactor the entrypoint hands back before it has logged —
    // its own wake resumes it, but only after this returns.
    let logs = spine_common::wait_for_logs(
        &runtime,
        probe,
        &[("uploaded module", 1), ("durable slice", 1)],
        spine_common::SHORT_BOUND,
    );
    assert!(
        logs.iter().any(|line| line.contains("uploaded module")),
        "expected the probe to upload the module, got: {logs:?}"
    );
    assert!(
        logs.iter()
            .any(|line| line.contains("spawned") && line.contains("durable slice")),
        "expected the probe to spawn from a durable slice, got: {logs:?}"
    );
    runtime.stop_process(probe).expect("stop probe");

    // ── Second lifetime: the manifest is durable; spawn from the slice ─────
    let descriptor = deploy_probe_descriptor(probe_wasm(), child_module());
    let report = runtime
        .bootstrap_system_guests(RuntimeConfig {
            start_discovery: false,
            system_guests: vec![descriptor],
            state_dir: state.config_dir(),
        })
        .expect("re-bootstrap probe");
    let probe = find(&report, "deploy-probe");
    let logs = spine_common::wait_for_logs(
        &runtime,
        probe,
        &[("reusing persisted manifest", 1), ("durable slice", 1)],
        spine_common::SHORT_BOUND,
    );
    assert!(
        logs.iter()
            .any(|line| line.contains("reusing persisted manifest")),
        "expected the probe to reuse the durable manifest, got: {logs:?}"
    );
    assert!(
        logs.iter()
            .any(|line| line.contains("spawned") && line.contains("durable slice")),
        "expected the probe to spawn from a durable slice after restart, got: {logs:?}"
    );
    runtime.stop_process(probe).expect("stop probe");
}

#[expect(clippy::panic, reason = "test helper panics on a missing guest")]
fn find(report: &BootstrapReport, name: &str) -> selium_abi::ProcessId {
    report
        .guests
        .iter()
        .find(|guest| guest.name == name)
        .unwrap_or_else(|| panic!("bootstrap report contains guest {name}"))
        .process_id
}

fn probe_wasm() -> Vec<u8> {
    common::read_guest_wasm_debug("selium-deploy-probe", "selium_deploy_probe.wasm")
}
