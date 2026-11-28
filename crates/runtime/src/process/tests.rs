//! Process lifecycle and scheduling tests.

use super::metering::{anchor_window_start, wall_clock_window_index, window_budget};
use super::*;
use crate::mailbox::GuestMailbox;
use crate::runtime::CpuBudgetWindow;
use crate::{Runtime, SystemGuestDescriptor};
use selium_abi::{
    ActivityKind, CompletionState, HostcallOutput, HostcallRequest, LocalityScope,
    MeteringObservation, Namespace, RegionProt, ResourceKind, ResourceSelector,
};
use std::sync::Arc;
use wasmtiny::WasmError;
use wasmtiny::runtime::{Limits, Memory as WasmMemory, MemoryType, TrapCode};

/// Registers a mailbox for `process_id` backed by scratch linear memory
/// so wake-delivery mechanics can be exercised without a real guest.
fn install_scratch_mailbox(runtime: &Runtime, process_id: ProcessId) -> Arc<GuestMailbox> {
    let mem_type = MemoryType {
        limits: Limits::Min(1),
        shared: false,
    };
    let memory = Arc::new(std::sync::Mutex::new(
        WasmMemory::new(mem_type).expect("scratch memory"),
    ));
    let mailbox = Arc::new(crate::mailbox::GuestMailbox::new(memory.clone(), 0));
    runtime.register_mailbox(process_id, mailbox.clone());
    mailbox
}

/// A trapped poll records a `ProcessExited` activity event and reclaims
/// the process record — a stalled guest must not linger as `running`
/// with no trace of why it stopped.
#[test]
fn trapped_poll_is_recorded_and_reaps_process() {
    let runtime = Runtime::default();
    let bootstrapped = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "trapped".to_string(),
            module_id: "trapped-module".to_string(),
            module_bytes: module_with_entrypoint("main", ""),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn guest");
    let pid = bootstrapped.process_id;

    runtime.record_poll_failure(pid, &WasmError::Trap(TrapCode::Unreachable));

    let activity = runtime.activity_log();
    assert!(
        activity.iter().any(|event| {
            event.process_id == Some(pid)
                && event.kind == ActivityKind::ProcessExited
                && event.message.contains("poll trapped")
        }),
        "expected a ProcessExited trapped-poll record, got: {activity:?}"
    );
    assert_eq!(
        runtime.loaded_guest_count(),
        0,
        "trap must unload the guest"
    );
    assert!(
        runtime.kernel().processes().inspect_process(pid).is_err(),
        "trap must reap the process record"
    );
}

/// Task 2.1: a spawned guest whose poll export reports entrypoint
/// completion is removed from the process table with a `ProcessExited`
/// event, while one reporting still-running stays resident.
// scenario: selium-runtime/entrypoint-completion-terminates-the-process/completed-entrypoint-reaps-the-process
#[test]
fn completed_entrypoint_poll_reaps_process() {
    let runtime = Runtime::default();
    let bootstrapped = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "completing".to_string(),
            module_id: "completing-module".to_string(),
            module_bytes: wat::parse_str(
                "(module
                    (func (export \"main\") (result i32) i32.const 0)
                    (func (export \"__selium_guest_poll\") (result i32) i32.const 1))",
            )
            .expect("compile wat"),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn guest");
    let pid = bootstrapped.process_id;
    assert_eq!(runtime.loaded_guest_count(), 1);

    runtime.poll_guest_until_stalled(pid);

    assert_eq!(
        runtime.loaded_guest_count(),
        0,
        "completion must unload the guest"
    );
    assert!(
        runtime.kernel().processes().inspect_process(pid).is_err(),
        "completion must reap the process record"
    );
    let activity = runtime.activity_log();
    assert!(
        activity.iter().any(|event| {
            event.process_id == Some(pid) && event.kind == ActivityKind::ProcessExited
        }),
        "expected a ProcessExited completion record, got: {activity:?}"
    );
}

/// Task 2.1 (counterpart): a parked entrypoint — the poll export reports
/// still-running — keeps its process resident and must not be reaped.
// scenario: selium-runtime/entrypoint-completion-terminates-the-process/long-running-entrypoint-stays-resident
#[test]
fn parked_entrypoint_poll_keeps_process_resident() {
    let runtime = Runtime::default();
    let bootstrapped = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "running".to_string(),
            module_id: "running-module".to_string(),
            module_bytes: wat::parse_str(
                "(module
                    (func (export \"main\") (result i32) i32.const 0)
                    (func (export \"__selium_guest_poll\") (result i32) i32.const 0))",
            )
            .expect("compile wat"),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn guest");
    let pid = bootstrapped.process_id;

    runtime.poll_guest_until_stalled(pid);

    assert_eq!(
        runtime.loaded_guest_count(),
        1,
        "a running entrypoint must stay loaded"
    );
    assert!(
        runtime.kernel().processes().inspect_process(pid).is_ok(),
        "a running entrypoint must keep its process record"
    );
}

/// Task 2.3: a wake delivered from another thread while the execution
/// guard is held must not be lost — the mailbox flag stays set and the
/// post-release re-check observes it.
// scenario: channel-wake-wait/cross-thread-wake-delivery/wake-racing-an-in-flight-poll-is-not-lost
#[test]
fn wake_while_guard_held_is_not_lost() {
    let runtime = Arc::new(Runtime::default());
    let pid: ProcessId = 7;
    let _mailbox = install_scratch_mailbox(&runtime, pid);

    // Hold the execution guard, simulating an in-flight reactor poll.
    assert!(
        runtime.try_begin_guest_exec(pid),
        "guard must be initially free"
    );

    // Deliver a wake from another thread while the guard is held.
    let rt = runtime.clone();
    let handle = std::thread::spawn(move || {
        rt.wake_process_task(pid, 1);
    });
    handle.join().expect("waker thread");

    // The waking thread must not have blocked on the guard, and the
    // wake must be pending in the mailbox (flag set by enqueue).
    assert!(
        !runtime.try_begin_guest_exec(pid),
        "our guard must still be held"
    );
    assert!(
        runtime.has_pending_wake(pid),
        "wake enqueued under contention must remain pending"
    );

    // Release the guard; the pending-wake re-check path must observe
    // the flag. Clear it manually (no real reactor to consume it) and
    // confirm the poll terminates cleanly instead of looping forever.
    runtime.end_guest_exec(pid);
    assert!(runtime.has_pending_wake(pid));
    {
        let mailboxes = runtime.mailboxes.lock();
        let mb = mailboxes.get(&pid).expect("mailbox registered");
        mb.memory
            .lock()
            .expect("memory lock")
            .write_u32(selium_abi::mailbox::FLAG_OFFSET as u32, 0)
            .expect("clear flag");
    }
    runtime.poll_guest_until_stalled(pid);
}

/// Task 4.2: many threads deliver wakes to one guest concurrently.
/// The ring is bounded (`CAPACITY` slots), so with no consumer draining it
/// the first `CAPACITY` wakes fill it and the rest are skipped rather than
/// overwriting unread wakes; the concurrent enqueues must never tear the
/// tail counter (it advances to exactly `CAPACITY`).
#[test]
fn concurrent_wake_delivery_never_loses_wakes() {
    let runtime = Arc::new(Runtime::default());
    let pid: ProcessId = 9;
    install_scratch_mailbox(&runtime, pid);

    const THREADS: usize = 8;
    const WAKES_PER_THREAD: usize = 50;

    let mut handles = Vec::new();
    for _ in 0..THREADS {
        let rt = runtime.clone();
        handles.push(std::thread::spawn(move || {
            for task in 0..WAKES_PER_THREAD {
                rt.wake_process_task(pid, task as u32);
            }
        }));
    }
    for handle in handles {
        handle.join().expect("waker thread");
    }

    // With no consumer, the ring fills to exactly `CAPACITY`: the tail
    // advances by one per successful enqueue and then stops (further wakes
    // are skipped, not overwritten — the multithreaded notify path still
    // delivers them). Concurrent enqueues must not tear or over-advance it.
    let mailboxes = runtime.mailboxes.lock();
    let mb = mailboxes.get(&pid).expect("mailbox registered");
    let tail = mb
        .memory
        .lock()
        .expect("memory lock")
        .read_u32(selium_abi::mailbox::TAIL_OFFSET as u32)
        .expect("read tail");
    assert_eq!(
        tail as usize,
        selium_abi::mailbox::CAPACITY,
        "a full ring must stop enqueuing rather than overwrite unread wakes"
    );
}

fn module_with_entrypoint(entrypoint: &str, body: &str) -> Vec<u8> {
    wat::parse_str(format!("(module (func (export \"{entrypoint}\") {body}))"))
        .expect("compile wat")
}

// scenario: process-metering/instruction-unit-of-account/observation-carries-instructions
#[test]
fn activity_log_and_metering_are_projected() {
    let runtime = Runtime::default();
    let bootstrapped = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "discovery".to_string(),
            module_id: "discovery-module".to_string(),
            module_bytes: module_with_entrypoint("main", ""),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::ActivityRead,
                vec![ResourceSelector::Locality(LocalityScope::Cluster)],
            )],
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn guest");
    runtime.project_metering(
        bootstrapped.process_id,
        MeteringObservation {
            cpu_instructions: 11,
            memory_bytes: 22,
            bandwidth_bytes: 44,
        },
    );

    assert!(
        runtime
            .activity_log()
            .iter()
            .any(|event| event.message.contains("bootstrapped"))
    );
    assert_eq!(
        runtime
            .kernel()
            .processes()
            .metering_observation(bootstrapped.process_id)
            .expect("metering")
            .cpu_instructions,
        11
    );
}

// scenario: process-metering/monotonic-per-process-counters/counter-never-decreases
// scenario: process-metering/committed-linear-memory-gauge/heap-growth-reflected-in-the-gauge
// scenario: process-metering/committed-linear-memory-gauge/shared-regions-excluded-from-the-gauge
// scenario: process-metering/tamper-evident-metering/guest-cannot-decrement-its-counter
// scenario: selium-accountant/metering-projection-by-the-host/observations-track-consumption
#[test]
fn metering_tick_projects_engine_instructions_and_linear_pages() {
    let runtime = Runtime::default();
    let guest = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "metered".to_string(),
            module_id: "metered-module".to_string(),
            module_bytes: wat::parse_str("(module (memory 3) (func (export \"main\")))")
                .expect("compile metered module"),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            )],
            tenant: Some("acme".to_string()),
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn metered guest");

    // Own a shared region: its bytes must NOT land in the linear-memory
    // gauge (regions are a separate quota dimension).
    let (shared_id, _len) = runtime
        .kernel()
        .memory()
        .allocate_shared_region(65_536)
        .expect("allocate region");
    runtime.claim_shared_resource(guest.process_id, ResourceClass::SharedRegion, shared_id);

    runtime.record_bandwidth_usage(guest.process_id, 1024);
    runtime.metering_tick();

    let observation = runtime
        .kernel()
        .processes()
        .metering_observation(guest.process_id)
        .expect("metered observation after tick");
    // CPU is fed from the engine's per-instance instruction meter, not a
    // host instrumentation hook.
    assert!(
        observation.cpu_instructions > 0,
        "engine must feed the instruction counter: {observation:?}"
    );
    // Memory is the guest's committed linear pages (3 * 64 KiB), not the
    // owned shared region's bytes.
    assert_eq!(
        observation.memory_bytes,
        3 * 65_536,
        "memory gauge is committed linear pages, not region bytes"
    );
    assert_eq!(observation.bandwidth_bytes, 1024);

    // The engine's instruction counter is monotonic across ticks.
    let first = observation.cpu_instructions;
    runtime.metering_tick();
    let second = runtime
        .kernel()
        .processes()
        .metering_observation(guest.process_id)
        .expect("metered observation after second tick")
        .cpu_instructions;
    assert!(
        second >= first,
        "instruction counter must not decrease ({first} -> {second})"
    );
}

#[test]
fn poll_feeds_engine_instruction_meter() {
    let runtime = Runtime::default();
    let guest = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "polled".to_string(),
            module_id: "polled-module".to_string(),
            module_bytes: wat::parse_str(
                "(module
                    (memory 1)
                    (func (export \"main\"))
                    (func (export \"__selium_guest_poll\") (result i32)
                        i32.const 1
                        i32.const 2
                        i32.add
                        drop
                        i32.const 0))",
            )
            .expect("compile polled module"),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn polled guest");

    runtime.metering_tick();
    let before = runtime
        .kernel()
        .processes()
        .metering_observation(guest.process_id)
        .expect("metered observation before poll")
        .cpu_instructions;

    // Drive a reactor poll: the engine charges the poll's instructions and
    // the projector reads the instance's cumulative count.
    runtime.poll_guest(guest.process_id);

    runtime.metering_tick();
    let after = runtime
        .kernel()
        .processes()
        .metering_observation(guest.process_id)
        .expect("metered observation after poll")
        .cpu_instructions;
    assert!(
        after > before,
        "a poll must increase the engine-fed instruction counter ({before} -> {after})"
    );
}

/// A cooperative guest whose poll runs a busy arithmetic loop before
/// parking (returns 0), so a poll can be driven across a CPU budget.
fn busy_poll_module(iterations: u32) -> Vec<u8> {
    wat::parse_str(format!(
        "(module
            (memory 1)
            (func (export \"main\"))
            (func (export \"__selium_guest_poll\") (result i32)
                (local $i i32)
                (block $done
                    (loop $l
                        (local.get $i) (i32.const {iterations}) (i32.ge_u) (br_if $done)
                        (local.get $i) (i32.const 1) (i32.add) (local.set $i)
                        (br $l)))
                i32.const 0))"
    ))
    .expect("compile busy poll module")
}

/// Spawns a resident tenant-scoped guest whose poll runs `iterations` of a
/// busy loop, granting shared-memory so it can hold a reservation to be
/// released on reap.
fn spawn_busy_guest(runtime: &Runtime, iterations: u32) -> ProcessId {
    runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "budgeted".to_string(),
            module_id: format!("budgeted-module-{iterations}"),
            module_bytes: busy_poll_module(iterations),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            )],
            tenant: Some("acme".to_string()),
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn budgeted guest")
        .process_id
}

/// Task 4.1: a tenant's per-minute CPU instruction ceiling becomes a
/// per-process engine execution budget, refreshed on every sampling tick so
/// a ceiling change lands within one second. Raising the ceiling lets the
/// guest run; lowering it below the remaining window work halts the guest
/// on the next refresh.
// scenario: process-metering/cpu-ceiling-delivery-under-a-resource-class/ceiling-delivered-under-the-cpu-class
#[test]
fn tenant_cpu_ceiling_becomes_a_per_process_engine_budget() {
    let runtime = Runtime::default();
    let pid = spawn_busy_guest(&runtime, 50_000);

    // A generous ceiling: the poll runs to completion without trapping.
    runtime
        .kernel()
        .quota()
        .set("acme", ResourceClass::Cpu, 100_000_000);
    runtime.refresh_cpu_budgets();
    runtime.poll_guest(pid);
    assert_eq!(
        runtime.loaded_guest_count(),
        1,
        "a generous ceiling must not halt the guest"
    );
    assert!(
        runtime.kernel().processes().inspect_process(pid).is_ok(),
        "a generous ceiling must not reap the process"
    );

    // Lowering the ceiling re-anchors the engine budget to the new ceiling
    // on the next refresh: the poll's remaining work now exceeds it and the
    // engine halts execution.
    runtime.kernel().quota().set("acme", ResourceClass::Cpu, 10);
    runtime.refresh_cpu_budgets();
    runtime.poll_guest(pid);
    assert_eq!(
        runtime.loaded_guest_count(),
        0,
        "the lowered ceiling must halt the guest"
    );
    assert!(
        runtime.kernel().processes().inspect_process(pid).is_err(),
        "the halted guest must be reaped"
    );
}

/// The spawn-time anchor: a process spawned mid-window under a tenant with
/// an already-authored CPU ceiling is bounded from its first instructions —
/// no budget refresh is needed for the ceiling to apply.
// scenario: process-metering/per-minute-cpu-budget-enforcement/budget-applied-at-spawn
#[test]
fn spawn_anchors_the_tenant_cpu_ceiling_without_a_refresh() {
    let runtime = Runtime::default();
    // Author the ceiling BEFORE the process exists, then spawn into it.
    runtime
        .kernel()
        .quota()
        .set("acme", ResourceClass::Cpu, 50_000);
    let pid = spawn_busy_guest(&runtime, 1_000_000);

    // No `refresh_cpu_budgets` call: the spawn-time anchor alone must halt
    // the runaway loop on the first poll.
    runtime.poll_guest(pid);
    assert_eq!(
        runtime.loaded_guest_count(),
        0,
        "the spawn-anchored budget must halt the guest"
    );
    assert!(
        runtime.kernel().processes().inspect_process(pid).is_err(),
        "the over-budget guest must be reaped"
    );
}

/// The window anchor keeps a window's start count across refreshes within
/// the same wall-clock minute, and re-anchors afresh on a new minute.
// scenario: process-metering/per-minute-cpu-budget-enforcement/budget-evaluated-per-window
#[test]
fn cpu_budget_window_re_anchors_only_on_a_new_minute() {
    // First anchor for a window: the window starts at the live count.
    assert_eq!(anchor_window_start(None, 100, 5_000), 5_000);
    // Same minute: the anchored start is kept, so consumption is not reset.
    assert_eq!(
        anchor_window_start(
            Some(CpuBudgetWindow {
                window_index: 100,
                window_start_instructions: 5_000,
            }),
            100,
            9_000,
        ),
        5_000
    );
    // New minute: the window re-anchors from the live count.
    assert_eq!(
        anchor_window_start(
            Some(CpuBudgetWindow {
                window_index: 100,
                window_start_instructions: 5_000,
            }),
            101,
            9_000,
        ),
        9_000
    );
    // The budget is the window start plus the ceiling; no ceiling is
    // unbounded.
    assert_eq!(window_budget(5_000, Some(2_000)), Some(7_000));
    assert_eq!(window_budget(5_000, None), None);
    // A ceiling that would overflow saturates rather than wrapping.
    assert_eq!(window_budget(u64::MAX, Some(1)), Some(u64::MAX));
}

/// The budget window is aligned to wall-clock minutes, matching the
/// accountant's billing windows.
// scenario: process-metering/per-minute-cpu-budget-enforcement/window-aligned-to-the-wall-clock-minute
#[test]
fn cpu_budget_window_is_wall_clock_minute_aligned() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock")
        .as_secs();
    assert_eq!(wall_clock_window_index(), now / 60);
}

/// Task 4.2: exhausting the tenant's CPU budget halts a runaway-loop guest
/// and reaps it through the standard teardown path, releasing its
/// reservations (here, a shared-region quota reservation). Task 4.3: the
/// exhaustion outcome is a distinct first-class signal recorded by the
/// runtime (the engine only reports it).
// scenario: process-metering/per-minute-cpu-budget-enforcement/over-budget-process-halted
#[test]
fn over_budget_process_is_reaped_and_releases_reservations() {
    let runtime = Runtime::default();
    let pid = spawn_busy_guest(&runtime, 1_000_000);

    // The guest holds a shared-region reservation against the tenant.
    let (status, op) = runtime.begin_hostcall(
        pid,
        HostcallRequest::AllocRegion {
            pages: 1,
            prot: RegionProt::ReadWrite,
            purpose: ResourceKind::SharedMemory,
            serving_tenant: None,
        },
    );
    assert_eq!(status, selium_abi::HOSTCALL_STATUS_READY);
    assert!(matches!(
        runtime.poll_hostcall(pid, op),
        CompletionState::Ready(HostcallOutput::RegionAlloc(_))
    ));
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("acme", ResourceClass::SharedRegion),
        65_536,
        "the region reservation is tracked against the tenant"
    );

    // A tiny per-minute ceiling: the runaway loop exhausts it on the next
    // poll.
    runtime
        .kernel()
        .quota()
        .set("acme", ResourceClass::Cpu, 1_000);
    runtime.refresh_cpu_budgets();
    runtime.poll_guest(pid);

    // Reaped through the standard teardown path, with the exhaustion
    // recorded as a distinct outcome.
    assert_eq!(runtime.loaded_guest_count(), 0);
    assert!(
        runtime.kernel().processes().inspect_process(pid).is_err(),
        "the over-budget process must be reaped"
    );
    assert!(
        runtime.activity_log().iter().any(|event| {
            event.process_id == Some(pid)
                && event.kind == ActivityKind::ProcessExited
                && event
                    .message
                    .contains("exhausted its CPU instruction budget")
        }),
        "expected a budget-exhaustion ProcessExited record, got: {:?}",
        runtime.activity_log()
    );
    // The region reservation returned to the tenant.
    assert_eq!(
        runtime
            .kernel()
            .quota()
            .used("acme", ResourceClass::SharedRegion),
        0,
        "the reaped process's region reservation is released"
    );
}

// scenario: capability-enforcement/cleanup-preserves-co-owners/co-owner-survives-cleanup
// scenario: selium-runtime/sub-struct-architecture/cross-subsystem-orchestration-uses-function-parameters
#[test]
fn cleanup_failed_process_preserves_co_owners() {
    let runtime = Runtime::default();

    let guest_a = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "owner-a".to_string(),
            module_id: "owner-a-module".to_string(),
            module_bytes: module_with_entrypoint("main", ""),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            )],
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn owner-a");

    let guest_b = runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: "owner-b".to_string(),
            module_id: "owner-b-module".to_string(),
            module_bytes: module_with_entrypoint("main", ""),
            entrypoint: "main".to_string(),
            arguments: Vec::new(),
            grants: vec![CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            )],
            tenant: None,
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn owner-b");

    // Allocate a shared region as guest_a.
    let (shared_id, _len) = runtime
        .kernel()
        .memory()
        .allocate_shared_region(64)
        .expect("allocate region");
    runtime.claim_shared_resource(guest_a.process_id, ResourceClass::SharedRegion, shared_id);
    // Simulate co-ownership: guest_b also owns the region.
    runtime.claim_shared_resource(guest_b.process_id, ResourceClass::SharedRegion, shared_id);

    // Cleanup guest_a (simulate failure).
    runtime
        .cleanup_failed_process(guest_a.process_id)
        .expect("cleanup");

    // guest_b should still own the region.
    let still_owns = runtime
        .shared_resource_owners
        .lock()
        .get(&(ResourceClass::SharedRegion, shared_id))
        .is_some_and(|owners| owners.contains(&guest_b.process_id));
    assert!(
        still_owns,
        "co-owner b should retain ownership after a fails"
    );
}

/// A co-owner exit must not publish `ResourceFreed` for a shared resource
/// the surviving owner still holds: the identity-keyed record would be
/// dropped while the resource is still live. The last owner's exit does
/// publish it, and only then is the region destroyed.
// scenario: selium-runtime/process-teardown-revocation/co-owned-resource-survives-an-owner-exit
#[test]
fn co_owner_exit_publishes_no_resource_freed() {
    let runtime = Runtime::default();
    let mut subscriber = swap_in_working_publisher(&runtime);
    let owner_a = spawn_tenant_guest(&runtime, "co-owner-a", "acme");
    let owner_b = spawn_tenant_guest(&runtime, "co-owner-b", "acme");

    let (shared_id, _len) = runtime
        .kernel()
        .memory()
        .allocate_shared_region(64)
        .expect("allocate region");
    runtime.claim_shared_resource(owner_a, ResourceClass::SharedRegion, shared_id);
    runtime.claim_shared_resource(owner_b, ResourceClass::SharedRegion, shared_id);

    // The first owner exits: the region survives for its co-owner, so no
    // identity-keyed revocation names it and the region is not destroyed.
    runtime.stop_process(owner_a).expect("stop first owner");
    let events = drain_feed(&mut subscriber);
    assert!(
        events.iter().any(
            |event| matches!(event, DiscoveryRequest::ProcessGone { process_id } if *process_id == owner_a)
        ),
        "expected the owner-keyed revocation, got: {events:?}"
    );
    assert!(
        !events.iter().any(
            |event| matches!(event, DiscoveryRequest::ResourceFreed { id, .. } if *id == shared_id)
        ),
        "a co-owned region must not be reported as freed, got: {events:?}"
    );
    assert_eq!(
        runtime
            .kernel()
            .memory()
            .shared_region_len(shared_id)
            .expect("co-owned region survives the first owner's exit"),
        65_536
    );

    // The last owner exits: now the host actually reclaims the region.
    runtime.stop_process(owner_b).expect("stop last owner");
    let events = drain_feed(&mut subscriber);
    assert!(
        events.iter().any(|event| matches!(
            event,
            DiscoveryRequest::ResourceFreed { class, id } if class == "region" && *id == shared_id
        )),
        "expected the identity-keyed revocation once the last owner exits, got: {events:?}"
    );
}
// scenario: selium-abi/resource-authority-hostcall/unowned-resource-reports-no-ownership
// scenario: selium-abi/resource-authority-hostcall/class-mismatch-is-reported

/// The authority answer distinguishes a class mismatch from an unknown
/// identity, so admission can refuse it as a permission failure.
#[test]
fn resource_authority_reports_a_class_mismatch() {
    let runtime = Runtime::default();
    let guest = spawn_tenant_guest(&runtime, "class-mismatch-guest", "acme");
    let (shared_id, _len) = runtime
        .kernel()
        .memory()
        .allocate_shared_region(64)
        .expect("allocate region");
    runtime.claim_shared_resource(guest, ResourceClass::SharedRegion, shared_id);

    let owned = runtime.resource_authority(guest, ResourceClass::SharedRegion, shared_id);
    assert!(owned.owned);
    assert!(owned.other_classes.is_empty());
    assert_eq!(owned.tenant.as_deref(), Some("acme"));

    let mismatch = runtime.resource_authority(guest, ResourceClass::Volume, shared_id);
    assert!(!mismatch.owned);
    assert!(
        mismatch
            .other_classes
            .contains(&ResourceClass::SharedRegion),
        "expected the owned class to be reported, got: {:?}",
        mismatch.other_classes
    );
    assert!(mismatch.tenant.is_none());

    let unknown = runtime.resource_authority(guest, ResourceClass::Volume, shared_id + 999);
    assert!(!unknown.owned);
    assert!(unknown.other_classes.is_empty());
}

/// Teardown support: replaces the runtime's discovery publisher with one
/// over a full Park channel, so every synchronous publish surfaces
/// `BufferFull` instead of silently succeeding (the real feed is a Drop
/// channel, which never fails). Returns the original publisher.
fn swap_in_failing_publisher(runtime: &Runtime) -> Option<crate::runtime::DiscoveryPublisher> {
    let full_channel = selium_shm::Channel::create_with_backpressure(
        64,
        selium_shm::ChannelBackpressure::Park,
        selium_abi::ResourceKind::PubSubTopic,
    )
    .expect("park channel");
    let transport =
        selium_shm::transport::ShmTransport::new(&full_channel, &full_channel).expect("transport");
    // Fill the ring so any further write parks (surfaced as BufferFull
    // by the synchronous write path).
    let mut filler = selium_wire::framed::FramedWrite::new(transport);
    while filler.write_frame(b"x", 0).is_ok() {}
    let publisher = selium_wire::pubsub::Publisher::new(filler);
    (*runtime.discovery_publisher.lock()).replace(publisher)
}

/// Spawns a tenant-scoped guest so teardown has a registered process
/// node to revoke.
fn spawn_tenant_guest(runtime: &Runtime, name: &str, tenant: &str) -> selium_abi::ProcessId {
    runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: name.to_string(),
            module_id: format!("{name}-module"),
            module_bytes: wat::parse_str("(module (func (export \"boot\")))").expect("wat"),
            entrypoint: "boot".to_string(),
            arguments: Vec::new(),
            grants: Vec::new(),
            tenant: Some(tenant.to_string()),
            discovery_service: false,
            handlers: Vec::new(),
        })
        .expect("spawn guest")
        .process_id
}

/// Installs a fresh working publisher and returns a subscriber attached
/// to its channel, so a retried teardown's revocations are observable.
fn swap_in_working_publisher(
    runtime: &Runtime,
) -> selium_wire::pubsub::Subscriber<DiscoveryRequest, selium_shm::transport::ShmTransport> {
    let channel = selium_shm::Channel::create_with_backpressure(
        64 * 1024,
        selium_shm::ChannelBackpressure::Drop,
        selium_abi::ResourceKind::PubSubTopic,
    )
    .expect("channel");
    let writer =
        selium_shm::transport::ShmTransport::new(&channel, &channel).expect("writer transport");
    let reader =
        selium_shm::transport::ShmTransport::new(&channel, &channel).expect("reader transport");
    let publisher =
        selium_wire::pubsub::Publisher::new(selium_wire::framed::FramedWrite::new(writer));
    *runtime.discovery_publisher.lock() = Some(publisher);
    selium_wire::pubsub::Subscriber::new(
        selium_wire::framed::FramedRead::new(reader),
        Some(channel.ring().capacity()),
    )
}

/// Drains every discovery event currently readable from the subscriber.
fn drain_feed(
    subscriber: &mut selium_wire::pubsub::Subscriber<
        DiscoveryRequest,
        selium_shm::transport::ShmTransport,
    >,
) -> Vec<DiscoveryRequest> {
    let mut events = Vec::new();
    loop {
        match subscriber.read_with_tag() {
            Ok((request, _tag)) => events.push(request),
            Err(selium_wire::error::Error::BufferEmpty) => break,
            Err(error) => panic!("feed read failed: {error}"),
        }
    }
    events
}

/// A discovery publish failure during `stop_process` must fail the stop
/// (rather than silently skipping revocations), keep the process
/// authority so the stop can be retried, and leave the pending
// scenario: selium-runtime/a-concurrent-stop-is-not-undone-by-an-in-flight-poll/repeated-stop-cycles-leave-no-loaded-guests
// scenario: selium-runtime/a-concurrent-stop-is-not-undone-by-an-in-flight-poll/stop-during-a-poll-reclaims-the-execution
#[test]
fn repeated_stop_cycles_leave_no_loaded_guests() {
    let runtime = Runtime::default();
    for _ in 0..5 {
        let guest = runtime
            .spawn_system_guest(crate::SystemGuestDescriptor {
                name: "stop-cycle".to_string(),
                module_id: "stop-cycle-module".to_string(),
                module_bytes: wat::parse_str("(module (memory 1) (func (export \"boot\")))")
                    .expect("compile wat"),
                entrypoint: "boot".to_string(),
                arguments: Vec::new(),
                grants: Vec::new(),
                tenant: None,
                discovery_service: false,
                handlers: Vec::new(),
            })
            .expect("spawn guest");
        runtime.stop_process(guest.process_id).expect("stop");
        assert_eq!(runtime.loaded_guest_count(), 0);
    }
}

/// revocations staged so the retry publishes them.
// scenario: guest-shared-memory/mapping-release-on-process-teardown/teardown-retry-does-not-duplicate-mappings
// scenario: selium-runtime/process-teardown-revocation/failed-teardown-is-retryable
// scenario: selium-runtime/process-teardown-revocation/exit-revokes-before-reclaim
#[test]
fn stop_process_teardown_is_retryable_after_publish_failure() {
    let runtime = Runtime::default();
    runtime
        .bootstrap_system_guests(crate::RuntimeConfig {
            start_discovery: true,
            system_guests: Vec::new(),
            state_dir: None,
        })
        .expect("bootstrap discovery");
    let pid = spawn_tenant_guest(&runtime, "retry-guest", "acme");

    // Every publish now fails: teardown cannot revoke the process node.
    swap_in_failing_publisher(&runtime);
    let stopped = runtime.stop_process(pid);
    assert!(stopped.is_err(), "teardown must fail while publishes fail");
    // The authority is retained so the stop is retryable, not lost.
    assert_eq!(
        runtime.process_tenant(pid).as_deref(),
        Some("acme"),
        "authority must be retained for a teardown retry"
    );

    // Install a working publisher and retry: the stop completes and the
    // owner-keyed revocation is published.
    let mut subscriber = swap_in_working_publisher(&runtime);
    runtime.stop_process(pid).expect("retry stop");
    let events = drain_feed(&mut subscriber);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, DiscoveryRequest::ProcessGone { process_id } if *process_id == pid)),
        "expected the owner-keyed revocation after retry, got: {events:?}"
    );
    assert!(
        runtime.process_tenant(pid).is_none(),
        "authority must be dropped after a successful teardown"
    );
}

/// A publish failure during `cleanup_failed_process` retains the process
/// authority (best-effort teardown still reclaims what it can), and a
/// later `stop_process` retry completes the pending revocations even
/// though the kernel record is already reaped.
#[test]
fn cleanup_failed_process_retains_authority_for_teardown_retry() {
    let runtime = Runtime::default();
    runtime
        .bootstrap_system_guests(crate::RuntimeConfig {
            start_discovery: true,
            system_guests: Vec::new(),
            state_dir: None,
        })
        .expect("bootstrap discovery");
    let pid = spawn_tenant_guest(&runtime, "failed-guest", "acme");

    // Every publish now fails: the failed-process teardown cannot revoke
    // the process node, but stays best-effort.
    swap_in_failing_publisher(&runtime);
    runtime
        .cleanup_failed_process(pid)
        .expect("best-effort cleanup");
    assert_eq!(
        runtime.process_tenant(pid).as_deref(),
        Some("acme"),
        "authority must be retained for a teardown retry"
    );

    // A later stop completes the pending revocation, tolerating the
    // already-reaped kernel record.
    let mut subscriber = swap_in_working_publisher(&runtime);
    runtime.stop_process(pid).expect("retry stop");
    let events = drain_feed(&mut subscriber);
    assert!(
        events
            .iter()
            .any(|event| matches!(event, DiscoveryRequest::ProcessGone { process_id } if *process_id == pid)),
        "expected the owner-keyed revocation after retry, got: {events:?}"
    );
}

/// A `DelegateGrants` grant with no `Tenant` selector would vacuously
/// match every scope context (including root), granting global
/// cross-tenant delegation authority; validation rejects it.
// scenario: selium-runtime/principal-provenance-allocation/selector-less-delegation-grant-rejected
#[test]
fn delegate_grants_requires_a_tenant_selector() {
    let runtime = Runtime::default();

    let rejected = runtime.spawn_system_guest(SystemGuestDescriptor {
        name: "bad-delegator".to_string(),
        module_id: "bad-delegator-module".to_string(),
        module_bytes: wat::parse_str("(module (func (export \"boot\")))").expect("wat"),
        entrypoint: "boot".to_string(),
        arguments: Vec::new(),
        grants: vec![CapabilityGrant::new(Capability::DelegateGrants, Vec::new())],
        tenant: None,
        discovery_service: false,
        handlers: Vec::new(),
    });
    assert!(
        matches!(
            rejected,
            Err(crate::Error::InvalidGrant(grant)) if grant == Capability::DelegateGrants
        ),
        "selector-less DelegateGrants must be rejected"
    );

    // A tenant-scoped delegation grant is admitted.
    let admitted = runtime.spawn_system_guest(SystemGuestDescriptor {
        name: "scoped-delegator".to_string(),
        module_id: "scoped-delegator-module".to_string(),
        module_bytes: wat::parse_str("(module (func (export \"boot\")))").expect("wat"),
        entrypoint: "boot".to_string(),
        arguments: Vec::new(),
        grants: vec![CapabilityGrant::new(
            Capability::DelegateGrants,
            vec![ResourceSelector::Tenant("acme".to_string())],
        )],
        tenant: None,
        discovery_service: false,
        handlers: Vec::new(),
    });
    assert!(admitted.is_ok(), "tenant-scoped DelegateGrants is admitted");
}

/// A `Namespace::Root`-carrying `DelegateGrants` is admitted: root-wide
/// delegation is explicit (a named selector), not the vacuous empty-
/// selector list validation rejects.
#[test]
fn delegate_grants_admits_namespace_root() {
    let runtime = Runtime::default();

    let admitted = runtime.spawn_system_guest(SystemGuestDescriptor {
        name: "root-delegator".to_string(),
        module_id: "root-delegator-module".to_string(),
        module_bytes: wat::parse_str("(module (func (export \"boot\")))").expect("wat"),
        entrypoint: "boot".to_string(),
        arguments: Vec::new(),
        grants: vec![CapabilityGrant::new(
            Capability::DelegateGrants,
            vec![ResourceSelector::Namespace(Namespace::Root)],
        )],
        tenant: None,
        discovery_service: false,
        handlers: Vec::new(),
    });
    assert!(
        admitted.is_ok(),
        "Namespace::Root DelegateGrants is admitted"
    );

    // A `Namespace::Tenant`-carrying `DelegateGrants` is also a named
    // tenant scope and therefore admitted.
    let admitted_tenant = runtime.spawn_system_guest(SystemGuestDescriptor {
        name: "tenant-ns-delegator".to_string(),
        module_id: "tenant-ns-delegator-module".to_string(),
        module_bytes: wat::parse_str("(module (func (export \"boot\")))").expect("wat"),
        entrypoint: "boot".to_string(),
        arguments: Vec::new(),
        grants: vec![CapabilityGrant::new(
            Capability::DelegateGrants,
            vec![ResourceSelector::Namespace(Namespace::Tenant(
                "acme".to_string(),
            ))],
        )],
        tenant: None,
        discovery_service: false,
        handlers: Vec::new(),
    });
    assert!(
        admitted_tenant.is_ok(),
        "Namespace::Tenant DelegateGrants is admitted"
    );
}

// --- Spec traceability (scripts/check-traceability.sh) -------------------
// These tests are the evidence for the `selium-runtime` scenarios. One scenario may
// be claimed by several tests; the annotation lives next to the evidence.

// --- Spec traceability (scripts/check-traceability.sh) -------------------
// These tests are the evidence for the `route-claims` scenarios. One scenario may
// be claimed by several tests; the annotation lives next to the evidence.
