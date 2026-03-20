//! Selium Host - Main entry point.
//!
//! The host is a minimal runtime that:
//! - Spawns the init guest
//! - Drives the guest executor
//! - Exits when the init guest terminates

use anyhow::Context;
use std::path::PathBuf;
use tracing::info;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use selium_host::{guest, Guest, GuestExitStatus, Kernel};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    info!("Selium Host starting...");

    // Parse command line arguments
    let args = Args::parse();

    // Create the kernel with built-in capabilities
    let kernel = Kernel::new();

    // Register built-in capabilities
    kernel.register(selium_host::TimeSource::new());
    kernel.register(selium_host::HostcallDispatcher::new());
    kernel.register(selium_host::metering::UsageMeter::new());

    // Load the init module
    let init_module_path = args
        .init_module
        .unwrap_or_else(|| PathBuf::from("init.wasm"));
    let engine = wasmtime::Engine::default();
    let module = wasmtime::Module::from_file(&engine, &init_module_path)
        .with_context(|| format!("Failed to load init module: {}", init_module_path.display()))?;

    info!("Loaded init module: {}", init_module_path.display());

    // Spawn the init guest
    let init_guest = Guest::spawn(&engine, &module, guest::next_guest_id())
        .context("Failed to spawn init guest")?;

    info!("Spawned init guest: {:?}", init_guest.id());

    // Run the init guest to completion
    let exit_status = run_guest(init_guest).await;

    info!("Init guest exited with: {:?}", exit_status);

    // Propagate exit status to host
    let code: i32 = exit_status.into();
    std::process::exit(code);
}

/// Run a guest until it exits.
async fn run_guest(mut guest: Guest) -> GuestExitStatus {
    loop {
        // Poll the guest executor
        guest.poll();

        // Check if the guest has exited
        if let Some(status) = guest.exit_status() {
            return status;
        }

        // Yield to the scheduler
        tokio::task::yield_now().await;
    }
}

/// Command line arguments.
struct Args {
    init_module: Option<PathBuf>,
    #[allow(dead_code)]
    config: Option<PathBuf>,
}

impl Args {
    fn parse() -> Self {
        let mut args = std::env::args_os().skip(1);

        let mut init_module = None;
        let mut config = None;

        while let Some(arg) = args.next() {
            match arg.to_str() {
                Some("--init") => {
                    init_module = args.next().map(PathBuf::from);
                }
                Some("--config") => {
                    config = args.next().map(PathBuf::from);
                }
                _ => {
                    // Assume it's the init module path
                    if init_module.is_none() {
                        init_module = Some(PathBuf::from(&arg));
                    }
                }
            }
        }

        Self {
            init_module,
            config,
        }
    }
}
