//! Integration test: Sleep hostcall timer completion.
//!
//! Tests that a `Sleep` hostcall with a 50 ms deadline completes at or after
//! the deadline, exercising the runtime's timer driver thread and
//! `poll_hostcall` deadline check.
//!
//! The `wasm32_guest_timer_resumes_at_deadline` test drives the same contract
//! through a *real* guest: it deploys the `selium-timer-probe` WASM guest,
//! which parks its poll-owner task on a `Sleep` hostcall and must be resumed
//! by the runtime's timer driver alone — the test never polls the guest.

use std::time::{Duration, Instant};

use selium_abi::{
    Capability, CapabilityGrant, CompletionState, HostcallOutput, HostcallRequest, ResourceClass,
    ResourceSelector,
};
use selium_runtime::{Runtime, SystemGuestDescriptor};
use selium_service::FlatMsg;

mod common;

/// Multiple concurrent sleep hostcalls should all complete independently.
#[test]
fn multiple_concurrent_sleeps_complete() {
    let runtime = Runtime::default();
    let pid = spawn_guest(&runtime, "timer-multi");

    let start = Instant::now();

    // Start three sleep hostcalls with different durations.
    let (_, op1) = runtime.begin_hostcall(pid, HostcallRequest::Sleep { millis: 30 });
    let (_, op2) = runtime.begin_hostcall(pid, HostcallRequest::Sleep { millis: 60 });
    let (_, op3) = runtime.begin_hostcall(pid, HostcallRequest::Sleep { millis: 90 });

    // All should be pending initially.
    assert!(matches!(
        runtime.poll_hostcall(pid, op1),
        CompletionState::Pending { .. }
    ));
    assert!(matches!(
        runtime.poll_hostcall(pid, op2),
        CompletionState::Pending { .. }
    ));
    assert!(matches!(
        runtime.poll_hostcall(pid, op3),
        CompletionState::Pending { .. }
    ));

    // Wait for the first to complete.
    std::thread::sleep(std::time::Duration::from_millis(40));
    assert!(matches!(
        runtime.poll_hostcall(pid, op1),
        CompletionState::Ready(HostcallOutput::Empty)
    ));

    // The others should still be pending (60ms and 90ms).
    let elapsed = start.elapsed();
    if elapsed.as_millis() < 60 {
        assert!(matches!(
            runtime.poll_hostcall(pid, op2),
            CompletionState::Pending { .. }
        ));
    }

    // Wait for the second to complete.
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert!(matches!(
        runtime.poll_hostcall(pid, op2),
        CompletionState::Ready(HostcallOutput::Empty)
    ));

    // Wait for the third to complete.
    std::thread::sleep(std::time::Duration::from_millis(30));
    assert!(matches!(
        runtime.poll_hostcall(pid, op3),
        CompletionState::Ready(HostcallOutput::Empty)
    ));

    let total = start.elapsed();
    assert!(
        total.as_millis() >= 90,
        "all three sleeps should take at least 90ms total: {total:?}"
    );
}

/// A 50 ms `Sleep` hostcall must return `Pending` immediately and `Ready`
/// after the deadline. The elapsed wall-clock time must be ≥ 50 ms.
#[test]
fn sleep_50ms_completes_at_or_after_deadline() {
    let runtime = Runtime::default();
    let pid = spawn_guest(&runtime, "timer-test");

    let start = Instant::now();

    // Begin a Sleep hostcall for 50 ms.
    let (status, op_id) = runtime.begin_hostcall(pid, HostcallRequest::Sleep { millis: 50 });
    assert_eq!(
        status,
        selium_abi::HOSTCALL_STATUS_PENDING,
        "Sleep should be pending immediately"
    );

    // Poll immediately — should be Pending (deadline not reached).
    match runtime.poll_hostcall(pid, op_id) {
        CompletionState::Pending { .. } => {}
        other => panic!("expected Pending on first poll, got {other:?}"),
    }

    // Elapsed so far should be well under 50 ms.
    let early_elapsed = start.elapsed();
    assert!(
        early_elapsed.as_millis() < 50,
        "first poll happened too late: {early_elapsed:?}"
    );

    // Wait for the deadline to pass.
    std::thread::sleep(std::time::Duration::from_millis(60));

    // Poll again — should be Ready (deadline reached).
    match runtime.poll_hostcall(pid, op_id) {
        CompletionState::Ready(HostcallOutput::Empty) => {}
        other => panic!("expected Ready(Empty) after deadline, got {other:?}"),
    }

    // The total elapsed time must be >= 50 ms.
    let elapsed = start.elapsed();
    assert!(
        elapsed.as_millis() >= 50,
        "sleep completed before the 50ms deadline: {elapsed:?}"
    );
}

/// A zero-millisecond sleep should complete immediately.
#[test]
fn sleep_zero_completes_immediately() {
    let runtime = Runtime::default();
    let pid = spawn_guest(&runtime, "timer-zero");

    let (status, op_id) = runtime.begin_hostcall(pid, HostcallRequest::Sleep { millis: 0 });
    // Even 0ms might be Pending for one tick.
    assert!(
        status == selium_abi::HOSTCALL_STATUS_PENDING
            || status == selium_abi::HOSTCALL_STATUS_READY,
        "unexpected status {status}"
    );

    // Poll — should be ready immediately (0ms deadline).
    match runtime.poll_hostcall(pid, op_id) {
        CompletionState::Ready(HostcallOutput::Empty) => {}
        CompletionState::Pending { .. } => {
            // Might need one more poll if the timer thread hasn't fired yet.
            std::thread::sleep(std::time::Duration::from_millis(5));
            match runtime.poll_hostcall(pid, op_id) {
                CompletionState::Ready(HostcallOutput::Empty) => {}
                other => panic!("expected Ready(Empty) for 0ms sleep, got {other:?}"),
            }
        }
        other => panic!("expected Ready or Pending for 0ms sleep, got {other:?}"),
    }
}

/// Spawns a minimal guest with shared-memory grants.
#[expect(
    clippy::indexing_slicing,
    reason = "test helper: bootstrap always returns one guest"
)]
fn spawn_guest(runtime: &Runtime, name: &str) -> u64 {
    let module = wat::parse_str(format!("(module (memory 1) (func (export \"{name}\") ))"))
        .expect("compile wat");

    let report = runtime
        .bootstrap_system_guests(selium_runtime::RuntimeConfig {
            start_discovery: false,
            system_guests: vec![SystemGuestDescriptor {
                name: name.to_string(),
                module_id: format!("{name}-module"),
                module_bytes: module,
                entrypoint: name.to_string(),
                arguments: Vec::new(),
                grants: vec![CapabilityGrant::new(
                    Capability::SharedMemory,
                    vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
                )],
                tenant: None,
                discovery_service: false,
                handlers: Vec::new(),
            }],
            state_dir: None,
        })
        .expect("bootstrap");

    report.guests[0].process_id
}

fn timer_probe_descriptor() -> SystemGuestDescriptor {
    SystemGuestDescriptor {
        name: "timer-probe".to_string(),
        module_id: "timer-probe-module".to_string(),
        module_bytes: common::read_guest_wasm_debug(
            "selium-timer-probe",
            "selium_timer_probe.wasm",
        ),
        entrypoint: "timer_probe".to_string(),
        arguments: Vec::new(),
        // The guest creates its own log channel, so it needs SharedMemory.
        grants: vec![CapabilityGrant::new(
            Capability::SharedMemory,
            vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
        )],
        tenant: None,
        discovery_service: false,
        handlers: Vec::new(),
    }
}

/// Polls the guest log channel until a message containing `needle` appears.
#[expect(clippy::panic, reason = "test helper")]
fn wait_for_log(runtime: &Runtime, process_id: u64, needle: &str, timeout: Duration) -> Duration {
    let mut seen: Vec<String> = Vec::new();
    let start = Instant::now();
    while start.elapsed() < timeout {
        let frames = runtime
            .kernel()
            .processes()
            .drain_log_channel(process_id)
            .expect("drain log channel");
        seen.extend(frames.iter().map(|frame| {
            selium_service::log::LogRecord::decode(frame)
                .expect("decode log record")
                .message
        }));
        if seen.iter().any(|message| message.contains(needle)) {
            return start.elapsed();
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("timed out waiting for {needle:?} in guest log; got {seen:?}");
}

/// Task 2.2: a real `wasm32` guest awaiting `Timer` is resumed at its
/// deadline when nothing else happens in the platform.
///
/// The probe guest's poll-owner task parks on the `Sleep` hostcall and logs
/// only once the timer fires, so the log line appearing at all proves the
/// wake was delivered by the runtime's timer driver. The test deliberately
/// never calls `poll_guest`: a host poll would mask a missing timer wake.
/// The probe sleeps 250 ms, so the wake must also land no earlier than that.
///
/// `#[ignore]`d by default because it requires the probe guest built for
/// `wasm32-unknown-unknown`:
///
/// ```sh
/// cargo build --target wasm32-unknown-unknown -p selium-timer-probe
/// cargo test -p selium-runtime --test timer -- --ignored
/// ```
#[test]
#[ignore = "requires the timer-probe guest built for wasm32-unknown-unknown"]
fn wasm32_guest_timer_resumes_at_deadline() {
    let runtime = Runtime::default();
    let bootstrapped = runtime
        .spawn_system_guest(timer_probe_descriptor())
        .expect("bootstrap timer probe guest");
    let process_id = bootstrapped.process_id;

    let start = Instant::now();
    wait_for_log(
        &runtime,
        process_id,
        "timer-probe: timer fired",
        Duration::from_secs(10),
    );
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(250),
        "the guest resumed before its deadline: {elapsed:?}"
    );

    runtime.stop_process(process_id).expect("stop process");
}
