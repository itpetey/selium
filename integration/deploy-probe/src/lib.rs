//! Durable-slice deploy probe test fixture guest.
//!
//! Receives a child module as a bootstrap pointer argument. On first boot it
//! uploads the module to the store's blob store and records a manifest; on a
//! later boot the manifest is already durable, so it resolves the module's
//! durable slice and spawns the child from it — module bytes never round-trip
//! through the guest.

use anyhow::Result;
use selium_guest::{Context, ModuleSource, Process, entrypoint, info};

/// Blob store holding the uploaded child module.
pub const DEPLOY_BLOB_STORE: &str = "selium.deploy-probe.modules";
/// Manifest naming the uploaded child module.
pub const DEPLOY_MANIFEST: &str = "child";

#[entrypoint]
async fn deploy_probe(mut ctx: Context, child_module: (u64, u64)) -> Result<()> {
    drop(selium_guest::log::init());
    info!(guest = "deploy-probe", "booting");

    // SAFETY: the runtime wrote the child module bytes into this guest's
    // linear memory for this entrypoint invocation.
    let child_module =
        unsafe { selium_guest::args::bytes(child_module.0, child_module.1) }.to_vec();

    let blobs = ctx.open_blob_store(DEPLOY_BLOB_STORE).await?;
    let slice = match blobs.resolve_slice(DEPLOY_MANIFEST).await? {
        Some(slice) => {
            info!("deploy-probe: reusing persisted manifest {DEPLOY_MANIFEST}");
            slice
        }
        None => {
            let blob_id = blobs.put(child_module).await?;
            blobs.set_manifest(DEPLOY_MANIFEST, blob_id).await?;
            info!("deploy-probe: uploaded module and recorded manifest {DEPLOY_MANIFEST}");
            blobs
                .resolve_slice(DEPLOY_MANIFEST)
                .await?
                .ok_or_else(|| anyhow::anyhow!("deploy-probe: manifest missing after set"))?
        }
    };

    let child = Process::start_for_tenant(
        ModuleSource::Slice {
            volume: slice.volume.clone(),
            offset: slice.offset,
            len: slice.len,
        },
        "child",
        Vec::new(),
        Vec::new(),
        None,
    )?;
    info!(
        "deploy-probe: spawned {} from durable slice {}",
        child.descriptor().local_id,
        slice.volume
    );

    info!("guest ready");
    Ok(())
}
