use std::env;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use selium_kernel::registry::Registry;
use selium_runtime_network::{NetworkEgressProfile, NetworkIngressBinding, NetworkService};
use selium_runtime_storage::{StorageBlobStoreDefinition, StorageLogDefinition, StorageService};
use tracing_subscriber::{EnvFilter, fmt::time::SystemTime};

use crate::config::{
    EgressProfileConfig, IngressBindingConfig, LogFormat, RuntimeNetworkConfig,
    RuntimeStorageConfig, ServerCommand, StorageBlobConfig, StorageLogConfig, load_server_options,
};

mod certs;
mod config;
mod daemon;
mod kernel;
mod modules;
mod providers;
mod wasmtime;

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
    configure_network(&kernel, &args.work_dir, &args.network)
        .await
        .context("configure runtime network")?;
    configure_storage(&kernel, &args.work_dir, &args.storage)
        .await
        .context("configure runtime storage")?;
    let registry = Registry::new();

    if let Some(ServerCommand::Daemon(daemon_args)) = args.command {
        return daemon::run_daemon(kernel, registry, shutdown, args.work_dir, *daemon_args).await;
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

async fn configure_network(
    kernel: &selium_kernel::Kernel,
    work_dir: &Path,
    config: &RuntimeNetworkConfig,
) -> Result<()> {
    let network = kernel
        .get::<NetworkService>()
        .ok_or_else(|| anyhow::anyhow!("missing NetworkService in kernel"))?;

    for profile in &config.egress_profiles {
        network
            .register_egress_profile(resolve_egress_profile(work_dir, profile))
            .await;
    }

    for binding in &config.ingress_bindings {
        network
            .register_ingress_binding(resolve_ingress_binding(work_dir, binding))
            .await;
    }

    Ok(())
}

fn resolve_egress_profile(work_dir: &Path, config: &EgressProfileConfig) -> NetworkEgressProfile {
    NetworkEgressProfile {
        name: config.name.clone(),
        protocol: config.protocol.into(),
        interactions: config
            .interactions
            .iter()
            .copied()
            .map(Into::into)
            .collect(),
        allowed_authorities: config.allowed_authorities.clone(),
        ca_cert_path: make_abs(work_dir, &config.ca_cert),
        client_cert_path: config
            .client_cert
            .as_ref()
            .map(|path| make_abs(work_dir, path)),
        client_key_path: config
            .client_key
            .as_ref()
            .map(|path| make_abs(work_dir, path)),
    }
}

fn resolve_ingress_binding(
    work_dir: &Path,
    config: &IngressBindingConfig,
) -> NetworkIngressBinding {
    NetworkIngressBinding {
        name: config.name.clone(),
        protocol: config.protocol.into(),
        interactions: config
            .interactions
            .iter()
            .copied()
            .map(Into::into)
            .collect(),
        listen_addr: config.listen.clone(),
        cert_path: make_abs(work_dir, &config.cert),
        key_path: make_abs(work_dir, &config.key),
    }
}

fn make_abs(work_dir: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        work_dir.join(path)
    }
}

async fn configure_storage(
    kernel: &selium_kernel::Kernel,
    work_dir: &Path,
    config: &RuntimeStorageConfig,
) -> Result<()> {
    let storage = kernel
        .get::<StorageService>()
        .ok_or_else(|| anyhow::anyhow!("missing StorageService in kernel"))?;

    for log in &config.logs {
        storage
            .register_log(resolve_storage_log(work_dir, log))
            .await;
    }
    for blob in &config.blobs {
        storage
            .register_blob_store(resolve_storage_blob(work_dir, blob))
            .await;
    }

    Ok(())
}

fn resolve_storage_log(work_dir: &Path, config: &StorageLogConfig) -> StorageLogDefinition {
    StorageLogDefinition {
        name: config.name.clone(),
        path: make_abs(work_dir, &config.path),
        retention: selium_io_durability::RetentionPolicy {
            max_entries: config.max_entries,
        },
    }
}

fn resolve_storage_blob(work_dir: &Path, config: &StorageBlobConfig) -> StorageBlobStoreDefinition {
    StorageBlobStoreDefinition {
        name: config.name.clone(),
        path: make_abs(work_dir, &config.path),
    }
}
