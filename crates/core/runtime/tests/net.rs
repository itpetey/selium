//! Network integration tests.
//!
//! Validates network hostcall address validation (literals-only enforcement)
//! via the runtime API path.

use selium_abi::{
    AbiErrorCode, Capability, CapabilityGrant, CompletionState, HostcallRequest, ResourceClass,
    ResourceSelector,
};
use selium_runtime::{ReadinessCondition, Runtime, SystemGuestDescriptor};

fn assert_failed_with_code(
    runtime: &Runtime,
    process_id: u64,
    op: u64,
    expected_code: AbiErrorCode,
    context: &str,
) {
    match runtime.poll_hostcall(process_id, op) {
        CompletionState::Failed(error) => {
            assert_eq!(
                error.code, expected_code,
                "{context}: expected {:?}, got {:?}",
                expected_code, error.code
            );
        }
        other => panic!("{context}: expected failed hostcall, got {other:?}"),
    }
}

fn empty_module() -> Vec<u8> {
    wat::parse_str("(module (func (export \"boot\")))").expect("compile wat")
}

#[test]
fn ip_literal_passes_validation() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::Network,
            vec![ResourceSelector::ResourceClass(ResourceClass::TcpStream)],
        )],
    );
    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::TcpConnect {
            address: "127.0.0.1:1".to_string(),
        },
    );
    if status == selium_abi::HOSTCALL_STATUS_FAILED {
        match runtime.poll_hostcall(guest.process_id, op) {
            CompletionState::Failed(error) => {
                assert_ne!(
                    error.code,
                    AbiErrorCode::MalformedPayload,
                    "IP literal must not be rejected as MalformedPayload"
                );
            }
            other => panic!("expected failed, got {other:?}"),
        }
    }
}

fn spawn_with_grants(
    runtime: &Runtime,
    grants: Vec<CapabilityGrant>,
) -> selium_runtime::BootstrappedGuest {
    runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "net-test".to_string(),
            module_id: "net-test-module".to_string(),
            module_bytes: empty_module(),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants,
            dependencies: Vec::new(),
            readiness: ReadinessCondition::Immediate,
            tenant: None,
        })
        .expect("spawn net test guest")
}

#[test]
fn tcp_bind_rejects_hostname() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::Network,
            vec![ResourceSelector::ResourceClass(ResourceClass::TcpListener)],
        )],
    );
    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::TcpBind {
            address: "example.com:0".to_string(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert_failed_with_code(
        &runtime,
        guest.process_id,
        op,
        AbiErrorCode::MalformedPayload,
        "hostname bind",
    );
}

#[test]
fn tcp_connect_rejects_hostname() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::Network,
            vec![ResourceSelector::ResourceClass(ResourceClass::TcpStream)],
        )],
    );
    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::TcpConnect {
            address: "localhost:80".to_string(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert_failed_with_code(
        &runtime,
        guest.process_id,
        op,
        AbiErrorCode::MalformedPayload,
        "hostname connect",
    );
}

#[test]
fn udp_bind_rejects_hostname() {
    let runtime = Runtime::default();
    let guest = spawn_with_grants(
        &runtime,
        vec![CapabilityGrant::new(
            Capability::Network,
            vec![ResourceSelector::ResourceClass(ResourceClass::UdpSocket)],
        )],
    );
    let (status, op) = runtime.begin_hostcall(
        guest.process_id,
        HostcallRequest::UdpBind {
            address: "myhost.local:8080".to_string(),
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_FAILED);
    assert_failed_with_code(
        &runtime,
        guest.process_id,
        op,
        AbiErrorCode::MalformedPayload,
        "hostname udp bind",
    );
}
