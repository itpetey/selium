use std::{fs::File, io::Read};

use anyhow::Result;
use selium_abi::{Capability, CapabilityGrant, ResourceClass, ResourceSelector};
use selium_encoding::FlatMsg;
use selium_kernel::Kernel;
use selium_runtime::{ReadinessCondition, Runtime, RuntimeConfig, SystemGuestDescriptor};

fn main() -> Result<()> {
    let kernel = Kernel::default();
    let runtime = Runtime::new(kernel);

    // Bootstrap system guests
    let mut config = RuntimeConfig::default();
    config.start_discovery = false;
    config.system_guests = vec![spine_demo()?];
    let report = runtime.bootstrap_system_guests(config)?;

    let process_id = report
        .guests
        .first()
        .expect("expected at least one bootstrapped guest")
        .process_id;

    // Print activity log events
    let activity = runtime.activity_log();
    println!("=== Activity Log ===");
    for event in &activity {
        println!("  [{:?}] {}: {}", event.kind, event.process_id.unwrap_or(0), event.message);
    }

    // Drain and print guest log messages
    println!("\n=== Guest Logs ===");
    let frames = runtime
        .kernel()
        .drain_log_channel(process_id)
        .expect("drain log channel");
    for frame in &frames {
        let record = selium_encoding::log::LogRecord::decode(frame)
            .expect("decode log record");
        println!("  {}", record.message);
    }

    // Stop the process and clean up
    println!("\n--- stopping process ---");
    runtime.stop_process(process_id)?;
    assert_eq!(runtime.loaded_guest_count(), 0);

    Ok(())
}

fn spine_demo() -> Result<SystemGuestDescriptor> {
    let mut fh = File::open("target/wasm32-unknown-unknown/debug/selium_spine_demo.wasm")?;
    let mut module_bytes = Vec::new();
    fh.read_to_end(&mut module_bytes)?;

    let grants = vec![CapabilityGrant::new(
        Capability::SharedMemory,
        vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
    )];

    Ok(SystemGuestDescriptor {
        name: "spine-demo".into(),
        module_id: "spine-demo".into(),
        module_bytes,
        entrypoint: "spine_demo".into(),
        arguments: Vec::new(),
        grants,
        dependencies: Vec::new(),
        readiness: ReadinessCondition::Immediate,
    })
}
