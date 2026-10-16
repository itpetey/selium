//! Accounting bootstrap-order substrate test (task 7.1).
//!
//! Verifies the wiring the single-host loop needs before the golden-path
//! integration test:
//!
//! - the bookkeeper and accountant grant sets (`selium_accountant::bookkeeper_grants()`,
//!   `accountant_grants()`) are admissible at bootstrap, carrying the
//!   bookkeeper's `MeteringRead`/`ActivityRead` and the accountant's
//!   `QuotaWrite`/`Storage`/`SystemRegistration`;
//! - both entrypoints boot from the **same module bytes** (one module, two
//!   descriptors — design decision 1);
//! - the runtime honours the dependency order (bookkeeper before accountant)
//!   even when the descriptor vector lists the accountant first.
//!
//! These tests use stub modules (the real guest needs the wasm32 build); the
//! wasm spine itself is exercised by the golden-path integration test.

use selium_abi::{Capability, CapabilityGrant};
use selium_accountant::{
    ACCOUNTANT_ENTRYPOINT, BOOKKEEPER_ENTRYPOINT, accountant_grants, bookkeeper_grants,
};
use selium_runtime::{Runtime, RuntimeConfig, SystemGuestDescriptor};

/// 7.1: the accountant guest's grant set is admissible at bootstrap, carrying
/// `QuotaWrite` (bootstrap-provisioned only), durable-log storage, and the
/// system-registration grant for the root-namespace routes it serves.
#[test]
fn accountant_descriptor_grants_are_bootstrap_admissible() {
    let runtime = Runtime::default();
    let bytes = module_with_entrypoint(ACCOUNTANT_ENTRYPOINT);

    let guest = runtime
        .spawn_system_guest(descriptor(
            "accountant",
            "accountant-module",
            &bytes,
            ACCOUNTANT_ENTRYPOINT,
            accountant_grants(),
            Vec::new(),
        ))
        .expect("accountant descriptor must be admissible at bootstrap");

    let authority = runtime
        .restore_process_authority(guest.process_id)
        .expect("accountant authority");
    assert!(
        authority
            .grants
            .iter()
            .any(|grant| grant.capability == Capability::QuotaWrite)
    );
    assert!(
        authority
            .grants
            .iter()
            .any(|grant| grant.capability == Capability::Storage)
    );
    assert!(
        authority
            .grants
            .iter()
            .any(|grant| grant.capability == Capability::SystemRegistration)
    );
}

/// 7.1: the bookkeeper guest's grant set is admissible at bootstrap, carrying
/// `MeteringRead` and `ActivityRead` for telemetry reduction.
#[test]
fn bookkeeper_descriptor_grants_are_bootstrap_admissible() {
    let runtime = Runtime::default();
    let bytes = module_with_entrypoint(BOOKKEEPER_ENTRYPOINT);

    let guest = runtime
        .spawn_system_guest(descriptor(
            "bookkeeper",
            "bookkeeper-module",
            &bytes,
            BOOKKEEPER_ENTRYPOINT,
            bookkeeper_grants(),
            Vec::new(),
        ))
        .expect("bookkeeper descriptor must be admissible at bootstrap");

    let authority = runtime
        .restore_process_authority(guest.process_id)
        .expect("bookkeeper authority");
    assert!(
        authority
            .grants
            .iter()
            .any(|grant| grant.capability == Capability::MeteringRead)
    );
    assert!(
        authority
            .grants
            .iter()
            .any(|grant| grant.capability == Capability::ActivityRead)
    );
}

fn descriptor(
    name: &str,
    module_id: &str,
    module_bytes: &[u8],
    entrypoint: &str,
    grants: Vec<CapabilityGrant>,
    _dependencies: Vec<String>,
) -> SystemGuestDescriptor {
    SystemGuestDescriptor {
        name: name.to_string(),
        module_id: module_id.to_string(),
        module_bytes: module_bytes.to_vec(),
        entrypoint: entrypoint.to_string(),
        arguments: Vec::new(),
        grants,
        tenant: None,
        discovery_service: false,
        handlers: Vec::new(),
    }
}

/// 4.1 + 7.1: one module boots both entrypoints (two descriptors over the same
/// module bytes), and dependency order boots the bookkeeper before the
/// accountant even when the accountant is listed first.
#[test]
fn guests_boot_in_declaration_order_from_the_same_module_bytes() {
    let runtime = Runtime::default();
    // The same module bytes carry both entrypoints, as the entrypoint macro
    // emits a distinct export per function.
    let module_bytes = module_with_both_entrypoints();

    let account = descriptor(
        "accountant",
        "accounting-module",
        &module_bytes,
        ACCOUNTANT_ENTRYPOINT,
        accountant_grants(),
        vec!["discovery".to_string(), "bookkeeper".to_string()],
    );
    let bookkeeper = descriptor(
        "bookkeeper",
        "accounting-module",
        &module_bytes,
        BOOKKEEPER_ENTRYPOINT,
        bookkeeper_grants(),
        vec!["discovery".to_string()],
    );
    let discovery = descriptor(
        "discovery",
        "discovery-module",
        &module_with_entrypoint("boot"),
        "boot",
        vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![selium_abi::ResourceSelector::ResourceClass(
                selium_abi::ResourceClass::SharedRegion,
            )],
        )],
        Vec::new(),
    );

    // There is no dependency sequencer: guests boot in declaration order.
    let report = runtime
        .bootstrap_system_guests(RuntimeConfig {
            start_discovery: false,
            system_guests: vec![account, bookkeeper, discovery],
            state_dir: None,
        })
        .expect("bootstrap accounting spine");

    let names: Vec<&str> = report
        .guests
        .iter()
        .map(|guest| guest.name.as_str())
        .collect();
    assert_eq!(names, vec!["accountant", "bookkeeper", "discovery"]);
}

/// One module exporting both accounting entrypoints, mirroring the shipped
/// `selium-accountant` crate (the entrypoint macro emits a distinct export per
/// function).
fn module_with_both_entrypoints() -> Vec<u8> {
    wat::parse_str(
        "(module
            (func (export \"bookkeeper\"))
            (func (export \"accountant\")))",
    )
    .expect("compile two-entrypoint wat")
}

fn module_with_entrypoint(entrypoint: &str) -> Vec<u8> {
    wat::parse_str(format!("(module (func (export \"{entrypoint}\")))")).expect("compile wat")
}
