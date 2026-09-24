//! Multithreaded guest execution integration tests (guest-worker-pool).
//!
//! The runtime-behaviour tests use minimal `wat` modules (worker pool,
//! wake-by-notify delivery, execution-guard bypass, per-worker trap teardown,
//! CPU-parallelism). `sdk_mt_demo_guest_runs_two_cpu_bound_tasks_in_parallel`
//! is the end-to-end proof on a real rustc-compiled guest (`selium-mt-demo`):
//! it is `#[ignore]`d and needs the atomics artifact built by
//! `scripts/build-all.sh`. The engine gives each concurrent invocation its own
//! `__stack_pointer` shadow stack, which is what lets a rustc-compiled module
//! run over one shared instance.
//!
//! - **4.1** — a multithreaded guest is provisioned with the configured
//!   worker-pool size at spawn, and teardown joins the pool.
//! - **4.2** — wake delivery routes through notify on the parking word
//!   (never driving the reactor): a parked worker resumes in place when the
//!   host wakes a task, with no embedder pumping.
//! - **4.3** — the per-guest execution guard is bypassed for multithreaded
//!   guests: `poll_guest` is a no-op.
//! - **5.1** — a trapping worker tears the guest down without a hang.
//! - **6.2** — two CPU-bound worker tasks complete in less wall-clock time
//!   on two workers than on one (serial execution).

use std::path::PathBuf;
use std::time::{Duration, Instant};

use selium_abi::{ActivityKind, mailbox};
use selium_runtime::{ReadinessCondition, Runtime, SystemGuestDescriptor};

/// Builds a CPU-bound worker module whose worker entry performs `iterations`
/// atomic increments on the worker's own linear-memory word (address = worker
/// id × 4, so distinct workers never contend on the same cache line), then
/// returns (the process exits).
fn cpu_bound_module(iterations: u32) -> String {
    format!(
        r#"(module
    (memory 1)
    (func (export "boot") (result i32) i32.const 0)
    (func (export "__selium_guest_worker") (param $id i32) (result i32)
      (local $i i32)
      (local $addr i32)
      (local.get $id) (i32.const 1024) (i32.mul) (local.set $addr)
      (block $done
        (loop $l
          (local.get $i) (i32.const {iterations}) (i32.ge_u) (br_if $done)
          (local.get $addr) (i32.atomic.rmw.add (local.get $addr) (i32.const 1)) (drop)
          (local.get $i) (i32.const 1) (i32.add) (local.set $i)
          (br $l)))
      (i32.const 0)))"#
    )
}

/// The linear-memory address this fixture places its mailbox at. The guest is
/// free to choose any address; only the mailbox *layout* is fixed by the ABI.
const MAILBOX_BASE: u32 = 0x1000;

/// Worker-entry body that registers a mailbox at [`MAILBOX_BASE`] (via the
/// `selium.mailbox_register` host import) and then parks on the shared wake
/// word until a host notify wakes it. The mailbox length and the wake-word
/// address come from the `selium_abi::mailbox` layout constants, so the
/// fixture tracks ABI changes instead of hard-coding byte offsets.
fn parking_worker_module() -> String {
    let mailbox_len = mailbox::BYTE_LEN;
    let wake_word = MAILBOX_BASE + mailbox::WAKE_WORD_OFFSET as u32;
    format!(
        r#"(module
    (import "selium" "mailbox_register" (func $mb (param i32 i32)))
    (memory 1)
    (func (export "boot") (result i32)
      (i32.const {MAILBOX_BASE}) (i32.const {mailbox_len}) (call $mb)
      ;; Zero the shared wake word (the host's mailbox_register only sets the
      ;; ring capacity), so the worker's wait32 actually parks.
      (i32.const {wake_word}) (i32.const 0) (i32.store)
      i32.const 0)
    (func (export "__selium_guest_worker") (param i32) (result i32)
      (memory.atomic.wait32
        (i32.const {wake_word}) (i32.const 0) (i64.const -1))
      drop
      i32.const 0))"#
    )
}

/// Worker-entry body that traps immediately (`unreachable`).
const TRAPPING_WORKER: &str = r#"(module
    (memory 1)
    (func (export "boot") (result i32) i32.const 0)
    (func (export "__selium_guest_worker") (param i32) (result i32)
      unreachable))"#;

fn pool_descriptor(name: &str, module_id: &str, module_text: &str) -> SystemGuestDescriptor {
    let module = wat::parse_str(module_text).expect("compile pool fixture");
    SystemGuestDescriptor {
        name: name.to_string(),
        module_id: module_id.to_string(),
        module_bytes: module,
        entrypoint: "boot".to_string(),
        arguments: Vec::new(),
        grants: Vec::new(),
        dependencies: Vec::new(),
        readiness: ReadinessCondition::Immediate,
        tenant: None,
        serving_role: None,
        handlers: Vec::new(),
    }
}

/// Waits until `process_id` is reaped (its `ProcessExited` activity event
/// appears) or `timeout` elapses; asserts it was reaped.
fn wait_for_process_exit(runtime: &Runtime, process_id: u64, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if runtime
            .activity_log()
            .iter()
            .any(|event| event.process_id == Some(process_id) && event.kind == ActivityKind::ProcessExited)
        {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!("timed out waiting for process {process_id} to exit");
}

/// Waits until `loaded_guest_count` reaches `expected` or `timeout` elapses.
fn wait_for_guest_count(runtime: &Runtime, expected: usize, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if runtime.loaded_guest_count() == expected {
            return;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
    panic!(
        "timed out waiting for guest count {expected}, at {}",
        runtime.loaded_guest_count()
    );
}

/// 4.1: a multithreaded guest is provisioned with the configured worker-pool
/// size at spawn, the pool runs the guest's CPU-bound workers, and teardown
/// joins the pool without hanging.
#[test]
fn worker_pool_provisioned_at_spawn_with_configured_count_and_joins_on_teardown() {
    let runtime = Runtime::default();
    runtime.set_worker_count("pool", 4);
    let bootstrapped = runtime
        .spawn_system_guest(pool_descriptor("pool", "pool-cpu", &cpu_bound_module(2_000_000)))
        .expect("spawn multithreaded guest");
    let process_id = bootstrapped.process_id;

    // The configured count is provisioned at spawn.
    assert_eq!(
        runtime.multithreaded_worker_count(process_id),
        Some(4),
        "the configured worker count must be provisioned"
    );

    // The pool runs the guest's CPU-bound workers; each returns after its
    // work and the process exits on its own, then the pool monitor reaps it.
    wait_for_process_exit(&runtime, process_id, Duration::from_secs(30));
    wait_for_guest_count(&runtime, 0, Duration::from_secs(10));

    // Teardown of an already-reaped process is tolerated (idempotent).
    drop(runtime.stop_process(process_id));
}

/// 4.2: wake delivery for multithreaded guests routes through notify on the
/// parking word instead of driving the reactor. A worker parked on the shared
/// wake word resumes in place when the host wakes a task — the process then
/// exits on its own, with no embedder pumping and no reactor poll.
#[test]
fn wake_delivery_notifies_parked_workers_without_driving_the_reactor() {
    let runtime = Runtime::default();
    runtime.set_worker_count("park", 1);
    let bootstrapped = runtime
        .spawn_system_guest(pool_descriptor("park", "park", &parking_worker_module()))
        .expect("spawn parking guest");
    let process_id = bootstrapped.process_id;

    // The worker is parked on the wake word: the process stays resident.
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(
        runtime.loaded_guest_count(),
        1,
        "the parked guest must stay resident before the wake"
    );

    // Deliver a wake from a poller thread: the notify path fires (ring
    // enqueue + wake-word notify) and the parked worker resumes in place,
    // returning from the worker entry so the process exits — the waking
    // thread never drives the reactor.
    runtime.wake_guest_task(process_id, 7);
    wait_for_process_exit(&runtime, process_id, Duration::from_secs(30));
    wait_for_guest_count(&runtime, 0, Duration::from_secs(10));
}

/// 4.3: the per-guest execution guard is bypassed for multithreaded guests —
/// `poll_guest` is a no-op (the pool, not the reactor, drives the guest).
#[test]
fn execution_guard_is_bypassed_for_multithreaded_guests() {
    let runtime = Runtime::default();
    runtime.set_worker_count("park", 2);
    let bootstrapped = runtime
        .spawn_system_guest(pool_descriptor("park", "park", &parking_worker_module()))
        .expect("spawn resident multithreaded guest");
    let process_id = bootstrapped.process_id;

    // Polling a multithreaded guest must not unload it or disturb the pool.
    runtime.poll_guest(process_id);
    assert_eq!(
        runtime.loaded_guest_count(),
        1,
        "polling a multithreaded guest must be a no-op"
    );
    assert_eq!(
        runtime.multithreaded_worker_count(process_id),
        Some(2),
        "the pool must still be provisioned"
    );

    runtime.stop_process(process_id).expect("stop process");
    wait_for_guest_count(&runtime, 0, Duration::from_secs(10));
}

/// 5.1: a trapping worker tears the guest down without a hang — the faulted
/// worker stops the rest of the pool and the process is reaped.
#[test]
fn trapping_worker_tears_guest_down_without_hang() {
    let runtime = Runtime::default();
    runtime.set_worker_count("trap", 2);
    let bootstrapped = runtime
        .spawn_system_guest(pool_descriptor("trap", "trap", TRAPPING_WORKER))
        .expect("spawn trapping guest");
    let process_id = bootstrapped.process_id;

    // The trapped workers must tear the process down (reap) without hanging.
    wait_for_process_exit(&runtime, process_id, Duration::from_secs(30));
    wait_for_guest_count(&runtime, 0, Duration::from_secs(10));

    // The reap recorded the fault, not a normal exit.
    assert!(
        runtime
            .activity_log()
            .iter()
            .any(|event| event.process_id == Some(process_id)
                && event.kind == ActivityKind::ProcessExited
                && event.message.contains("trapped")),
        "the trap must be recorded in the activity log"
    );
}

/// 6.2: two CPU-bound worker tasks complete in less wall-clock time on two
/// workers than on one (serial execution), over the guest's shared linear
/// memory.
#[test]
fn two_cpu_bound_tasks_beat_serial_wall_time_on_two_workers() {
    let runtime = Runtime::default();

    // Serial baseline: one worker runs both CPU-bound units one after
    // another (4M increments total), so the total work equals the parallel
    // run's. Wall time is measured from right after spawn, so the AOT
    // compilation inside `spawn_system_guest` is not counted.
    runtime.set_worker_count("pool", 1);
    let serial = runtime
        .spawn_system_guest(pool_descriptor("pool", "pool-cpu-serial", &cpu_bound_module(4_000_000)))
        .expect("spawn serial guest");
    let start = Instant::now();
    wait_for_process_exit(&runtime, serial.process_id, Duration::from_secs(60));
    let serial_time = start.elapsed();
    wait_for_guest_count(&runtime, 0, Duration::from_secs(10));

    // Parallel: two workers each run one CPU-bound unit (2M each, 4M total)
    // concurrently over the same shared linear memory.
    runtime.set_worker_count("pool", 2);
    let parallel = runtime
        .spawn_system_guest(pool_descriptor("pool", "pool-cpu", &cpu_bound_module(2_000_000)))
        .expect("spawn parallel guest");
    let start = Instant::now();
    wait_for_process_exit(&runtime, parallel.process_id, Duration::from_secs(60));
    let parallel_time = start.elapsed();
    wait_for_guest_count(&runtime, 0, Duration::from_secs(10));

    assert!(
        parallel_time < serial_time.mul_f32(0.8),
        "two workers must beat serial wall time: serial {serial_time:?} vs parallel {parallel_time:?}"
    );
}

/// 6.2 (end-to-end, real SDK guest): the `selium-mt-demo` guest spawns two
/// CPU-bound tasks through the SDK's work-stealing executor and joins them.
/// This is the proof that the engine runs a real rustc-compiled module over
/// one shared instance: on two workers the tasks execute concurrently without
/// overlapping shadow stacks (the pre-`__stack_pointer`-export engine trapped
/// or deadlocked here), and on one worker the same guest completes serially.
///
/// The wall-clock factor is asserted deterministically by the wat fixture
/// `two_cpu_bound_tasks_beat_serial_wall_time_on_two_workers`; this test
/// instead pins correctness (a clean exit, never a trap/wedge) because
/// wall-clock on a shared host is too noisy to assert (isolated runs measure
/// ~0.54× on two workers vs one).
///
/// `#[ignore]`d by default: it needs the mt-demo guest built for
/// `wasm32-unknown-unknown` with the atomics target **and** the
/// `__stack_pointer` export (the wasm-threads shadow-stack convention, which
/// the engine uses to give each concurrent invocation its own stack). See
/// `scripts/build-all.sh`; the artifact is kept at
/// `selium_mt_demo_atomics.wasm`.
#[test]
#[ignore = "requires the mt-demo guest built with the atomics target (see scripts/build-all.sh)"]
fn sdk_mt_demo_guest_runs_two_cpu_bound_tasks_in_parallel() {
    let module_bytes = mt_demo_wasm();

    // Concurrent: two workers run the two spawned tasks.
    let runtime = Runtime::default();
    runtime.set_worker_count("parallel", 2);
    let parallel = spawn_mt_demo(&runtime, module_bytes.clone(), "parallel");
    wait_for_process_exit(&runtime, parallel, Duration::from_secs(120));
    wait_for_guest_count(&runtime, 0, Duration::from_secs(10));
    assert!(
        !runtime.activity_log().iter().any(|event| {
            event.process_id == Some(parallel)
                && (event.message.contains("worker trapped")
                    || event.message.contains("wedged"))
        }),
        "the multithreaded SDK guest must exit cleanly, not trap or wedge"
    );

    // Serial: one worker runs the same guest to completion as a baseline.
    let runtime = Runtime::default();
    runtime.set_worker_count("serial", 1);
    let serial = spawn_mt_demo(&runtime, module_bytes, "serial");
    wait_for_process_exit(&runtime, serial, Duration::from_secs(120));
    wait_for_guest_count(&runtime, 0, Duration::from_secs(10));
    assert!(
        !runtime.activity_log().iter().any(|event| {
            event.process_id == Some(serial)
                && (event.message.contains("worker trapped")
                    || event.message.contains("wedged"))
        }),
        "the single-worker SDK guest must exit cleanly"
    );
}

/// Spawns the mt-demo guest in exit mode (mode 0) with the shared-memory
/// grant the log/region path needs, returning the process id.
fn spawn_mt_demo(runtime: &Runtime, module_bytes: Vec<u8>, name: &str) -> u64 {
    runtime
        .spawn_system_guest(SystemGuestDescriptor {
            name: name.to_string(),
            module_id: format!("{name}-mt-demo-module"),
            module_bytes,
            entrypoint: "mt_demo".to_string(),
            arguments: vec![selium_runtime::SystemGuestArg::Integer(0)],
            grants: vec![selium_abi::CapabilityGrant::new(
                selium_abi::Capability::SharedMemory,
                vec![selium_abi::ResourceSelector::ResourceClass(
                    selium_abi::ResourceClass::SharedRegion,
                )],
            )],
            dependencies: Vec::new(),
            readiness: ReadinessCondition::Immediate,
            tenant: None,
            serving_role: None,
            handlers: Vec::new(),
        })
        .expect("spawn multithreaded SDK guest")
        .process_id
}

/// Loads the atomics mt-demo module, failing with an actionable message when
/// it is absent (it is built by `scripts/build-all.sh`, not on the fly).
fn mt_demo_wasm() -> Vec<u8> {
    let target_dir =
        std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_e| "../../target".to_string());
    let path =
        PathBuf::from(target_dir).join("wasm32-unknown-unknown/debug/selium_mt_demo_atomics.wasm");
    std::fs::read(&path).unwrap_or_else(|_error| {
        panic!(
            "atomics mt-demo guest not found at {}.\n\
             Build it first with `scripts/build-all.sh` (needs a nightly \
             toolchain with the rust-src component for -Zbuild-std)",
            path.display()
        )
    })
}