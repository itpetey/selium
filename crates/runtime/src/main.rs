mod certs;
mod config;
mod control_plane;
mod daemon;
mod kernel;
mod modules;
mod providers;
mod wasmtime;

use std::env;

use anyhow::{Context, Result};
use selium_kernel::registry::Registry;
use tracing_subscriber::{EnvFilter, fmt::time::SystemTime};

use crate::config::{LogFormat, ServerCommand, load_server_options};

fn initialise_tracing(format: LogFormat) -> Result<()> {
    let filter = EnvFilter::try_from_default_env()
        .or_else(|_| EnvFilter::try_new(env::var("RUST_LOG").unwrap_or_else(|_| "info".into())))?;

    match format {
        LogFormat::Text => {
            tracing_subscriber::fmt()
                .with_env_filter(filter.clone())
                .with_target(false)
                .with_timer(SystemTime)
                .init();
        }
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .with_target(false)
                .with_current_span(true)
                .with_span_list(true)
                .init();
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = load_server_options()?;
    initialise_tracing(args.log_format)?;

    if let Some(ServerCommand::GenerateCerts(cert_args)) = &args.command {
        certs::generate_certificates(
            &cert_args.output_dir,
            &cert_args.ca_common_name,
            &cert_args.server_name,
            &cert_args.client_name,
        )?;
        return Ok(());
    }

    let (kernel, shutdown) = kernel::build(&args.work_dir).context("build runtime kernel")?;
    let registry = Registry::new();

    if let Some(ServerCommand::Daemon(daemon_args)) = args.command {
        return daemon::run_daemon(kernel, registry, shutdown, args.work_dir, daemon_args).await;
    }

    daemon::run(
        kernel,
        registry,
        shutdown,
        &args.work_dir,
        args.module.as_ref(),
    )
    .await
}
