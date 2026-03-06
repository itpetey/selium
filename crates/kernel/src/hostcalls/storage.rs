//! Storage hostcall drivers.

use std::{convert::TryFrom, sync::Arc};

use selium_abi::{
    StorageBlobGet, StorageBlobGetResult, StorageBlobPut, StorageBlobPutResult,
    StorageBlobStoreDescriptor, StorageCheckpointResult, StorageClose, StorageLogAppend,
    StorageLogAppendResult, StorageLogBounds, StorageLogBoundsResult, StorageLogCheckpoint,
    StorageLogCheckpointGet, StorageLogDescriptor, StorageLogReplay, StorageLogReplayResult,
    StorageManifestGet, StorageManifestGetResult, StorageManifestSet, StorageOpenBlobStore,
    StorageOpenLog, StorageStatus,
};

use crate::{
    guest_error::{GuestError, GuestResult},
    registry::ResourceType,
    spi::storage::{BlobStoreHandle, LogHandle, StorageCapability, StorageProcessPolicy},
};

use super::{Contract, HostcallContext, Operation};

type StorageOps<C> = (
    Arc<Operation<StorageOpenLogDriver<C>>>,
    Arc<Operation<StorageOpenBlobStoreDriver<C>>>,
    Arc<Operation<StorageCloseDriver<C>>>,
    Arc<Operation<StorageLogAppendDriver<C>>>,
    Arc<Operation<StorageLogCheckpointDriver<C>>>,
    Arc<Operation<StorageLogReplayDriver<C>>>,
    Arc<Operation<StorageLogCheckpointGetDriver<C>>>,
    Arc<Operation<StorageLogBoundsDriver<C>>>,
    Arc<Operation<StorageBlobPutDriver<C>>>,
    Arc<Operation<StorageManifestSetDriver<C>>>,
    Arc<Operation<StorageBlobGetDriver<C>>>,
    Arc<Operation<StorageManifestGetDriver<C>>>,
);

pub struct StorageOpenLogDriver<C>(C);
pub struct StorageOpenBlobStoreDriver<C>(C);
pub struct StorageCloseDriver<C>(C);
pub struct StorageLogAppendDriver<C>(C);
pub struct StorageLogCheckpointDriver<C>(C);
pub struct StorageLogReplayDriver<C>(C);
pub struct StorageLogCheckpointGetDriver<C>(C);
pub struct StorageLogBoundsDriver<C>(C);
pub struct StorageBlobPutDriver<C>(C);
pub struct StorageManifestSetDriver<C>(C);
pub struct StorageBlobGetDriver<C>(C);
pub struct StorageManifestGetDriver<C>(C);

impl<C> Contract for StorageOpenLogDriver<C>
where
    C: StorageCapability + Clone + Send + Sync + 'static,
{
    type Input = StorageOpenLog;
    type Output = StorageLogDescriptor;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let registrar = context.registry().registrar();
        let policy = context
            .registry()
            .extension::<StorageProcessPolicy>()
            .unwrap_or_else(|| Arc::new(StorageProcessPolicy::default()));

        async move {
            let log = inner.open_log(policy, input).await.map_err(Into::into)?;
            let slot = registrar
                .insert(log.clone(), None, ResourceType::StorageLog)
                .map_err(GuestError::from)?;
            let resource_id = u32::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;
            Ok(StorageLogDescriptor { resource_id })
        }
    }
}

impl<C> Contract for StorageOpenBlobStoreDriver<C>
where
    C: StorageCapability + Clone + Send + Sync + 'static,
{
    type Input = StorageOpenBlobStore;
    type Output = StorageBlobStoreDescriptor;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let registrar = context.registry().registrar();
        let policy = context
            .registry()
            .extension::<StorageProcessPolicy>()
            .unwrap_or_else(|| Arc::new(StorageProcessPolicy::default()));

        async move {
            let store = inner
                .open_blob_store(policy, input)
                .await
                .map_err(Into::into)?;
            let slot = registrar
                .insert(store.clone(), None, ResourceType::StorageBlobStore)
                .map_err(GuestError::from)?;
            let resource_id = u32::try_from(slot).map_err(|_| GuestError::InvalidArgument)?;
            Ok(StorageBlobStoreDescriptor { resource_id })
        }
    }
}

impl<C> Contract for StorageLogAppendDriver<C>
where
    C: StorageCapability + Clone + Send + Sync + 'static,
{
    type Input = StorageLogAppend;
    type Output = StorageLogAppendResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let log = context
            .registry()
            .with::<LogHandle<C::Log>, _>(
                usize::try_from(input.log_id).unwrap_or(usize::MAX),
                |log| log.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let log = log?;
            inner
                .log_append(&log.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for StorageLogCheckpointDriver<C>
where
    C: StorageCapability + Clone + Send + Sync + 'static,
{
    type Input = StorageLogCheckpoint;
    type Output = StorageStatus;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let log = context
            .registry()
            .with::<LogHandle<C::Log>, _>(
                usize::try_from(input.log_id).unwrap_or(usize::MAX),
                |log| log.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let log = log?;
            inner
                .log_checkpoint(&log.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for StorageLogReplayDriver<C>
where
    C: StorageCapability + Clone + Send + Sync + 'static,
{
    type Input = StorageLogReplay;
    type Output = StorageLogReplayResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let log = context
            .registry()
            .with::<LogHandle<C::Log>, _>(
                usize::try_from(input.log_id).unwrap_or(usize::MAX),
                |log| log.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let log = log?;
            inner
                .log_replay(&log.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for StorageLogCheckpointGetDriver<C>
where
    C: StorageCapability + Clone + Send + Sync + 'static,
{
    type Input = StorageLogCheckpointGet;
    type Output = StorageCheckpointResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let log = context
            .registry()
            .with::<LogHandle<C::Log>, _>(
                usize::try_from(input.log_id).unwrap_or(usize::MAX),
                |log| log.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let log = log?;
            inner
                .log_checkpoint_get(&log.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for StorageLogBoundsDriver<C>
where
    C: StorageCapability + Clone + Send + Sync + 'static,
{
    type Input = StorageLogBounds;
    type Output = StorageLogBoundsResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let log = context
            .registry()
            .with::<LogHandle<C::Log>, _>(
                usize::try_from(input.log_id).unwrap_or(usize::MAX),
                |log| log.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let log = log?;
            inner
                .log_bounds(&log.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for StorageBlobPutDriver<C>
where
    C: StorageCapability + Clone + Send + Sync + 'static,
{
    type Input = StorageBlobPut;
    type Output = StorageBlobPutResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let store = context
            .registry()
            .with::<BlobStoreHandle<C::BlobStore>, _>(
                usize::try_from(input.store_id).unwrap_or(usize::MAX),
                |store| store.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let store = store?;
            inner
                .blob_put(&store.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for StorageManifestSetDriver<C>
where
    C: StorageCapability + Clone + Send + Sync + 'static,
{
    type Input = StorageManifestSet;
    type Output = StorageStatus;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let store = context
            .registry()
            .with::<BlobStoreHandle<C::BlobStore>, _>(
                usize::try_from(input.store_id).unwrap_or(usize::MAX),
                |store| store.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let store = store?;
            inner
                .manifest_set(&store.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for StorageBlobGetDriver<C>
where
    C: StorageCapability + Clone + Send + Sync + 'static,
{
    type Input = StorageBlobGet;
    type Output = StorageBlobGetResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let store = context
            .registry()
            .with::<BlobStoreHandle<C::BlobStore>, _>(
                usize::try_from(input.store_id).unwrap_or(usize::MAX),
                |store| store.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let store = store?;
            inner
                .blob_get(&store.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for StorageManifestGetDriver<C>
where
    C: StorageCapability + Clone + Send + Sync + 'static,
{
    type Input = StorageManifestGet;
    type Output = StorageManifestGetResult;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let store = context
            .registry()
            .with::<BlobStoreHandle<C::BlobStore>, _>(
                usize::try_from(input.store_id).unwrap_or(usize::MAX),
                |store| store.clone(),
            )
            .ok_or(GuestError::NotFound);

        async move {
            let store = store?;
            inner
                .manifest_get(&store.inner, input)
                .await
                .map_err(Into::into)
        }
    }
}

impl<C> Contract for StorageCloseDriver<C>
where
    C: StorageCapability + Clone + Send + Sync + 'static,
{
    type Input = StorageClose;
    type Output = StorageStatus;

    fn to_future<Ctx>(
        &self,
        context: &mut Ctx,
        input: Self::Input,
    ) -> impl std::future::Future<Output = GuestResult<Self::Output>> + Send + 'static
    where
        Ctx: HostcallContext,
    {
        let inner = self.0.clone();
        let slot = usize::try_from(input.resource_id).map_err(|_| GuestError::InvalidArgument);
        let meta = slot
            .as_ref()
            .ok()
            .and_then(|slot| context.registry().entry(*slot))
            .and_then(|id| context.registry().registry().metadata(id))
            .ok_or(GuestError::NotFound);

        let future = match (slot, meta) {
            (Ok(slot), Ok(meta)) => match meta.kind {
                ResourceType::StorageLog => {
                    let log = context
                        .registry_mut()
                        .remove::<LogHandle<C::Log>>(slot)
                        .ok_or(GuestError::NotFound);
                    EitherClose::<C>::Log(log)
                }
                ResourceType::StorageBlobStore => {
                    let store = context
                        .registry_mut()
                        .remove::<BlobStoreHandle<C::BlobStore>>(slot)
                        .ok_or(GuestError::NotFound);
                    EitherClose::<C>::BlobStore(store)
                }
                _ => EitherClose::<C>::Error(Err(GuestError::InvalidArgument)),
            },
            (Err(err), _) => EitherClose::<C>::Error(Err(err)),
            (_, Err(err)) => EitherClose::<C>::Error(Err(err)),
        };

        async move {
            match future {
                EitherClose::Log(log) => inner.close_log(log?).await.map_err(Into::into),
                EitherClose::BlobStore(store) => {
                    inner.close_blob_store(store?).await.map_err(Into::into)
                }
                EitherClose::Error(result) => result,
            }
        }
    }
}

enum EitherClose<C>
where
    C: StorageCapability,
{
    Log(GuestResult<LogHandle<C::Log>>),
    BlobStore(GuestResult<BlobStoreHandle<C::BlobStore>>),
    Error(GuestResult<StorageStatus>),
}

pub fn operations<C>(cap: C) -> StorageOps<C>
where
    C: StorageCapability + Clone + Send + Sync + 'static,
{
    (
        Operation::from_hostcall(
            StorageOpenLogDriver(cap.clone()),
            selium_abi::hostcall_contract!(STORAGE_OPEN_LOG),
        ),
        Operation::from_hostcall(
            StorageOpenBlobStoreDriver(cap.clone()),
            selium_abi::hostcall_contract!(STORAGE_OPEN_BLOB_STORE),
        ),
        Operation::from_hostcall(
            StorageCloseDriver(cap.clone()),
            selium_abi::hostcall_contract!(STORAGE_CLOSE),
        ),
        Operation::from_hostcall(
            StorageLogAppendDriver(cap.clone()),
            selium_abi::hostcall_contract!(STORAGE_LOG_APPEND),
        ),
        Operation::from_hostcall(
            StorageLogCheckpointDriver(cap.clone()),
            selium_abi::hostcall_contract!(STORAGE_LOG_CHECKPOINT),
        ),
        Operation::from_hostcall(
            StorageLogReplayDriver(cap.clone()),
            selium_abi::hostcall_contract!(STORAGE_LOG_REPLAY),
        ),
        Operation::from_hostcall(
            StorageLogCheckpointGetDriver(cap.clone()),
            selium_abi::hostcall_contract!(STORAGE_LOG_CHECKPOINT_GET),
        ),
        Operation::from_hostcall(
            StorageLogBoundsDriver(cap.clone()),
            selium_abi::hostcall_contract!(STORAGE_LOG_BOUNDS),
        ),
        Operation::from_hostcall(
            StorageBlobPutDriver(cap.clone()),
            selium_abi::hostcall_contract!(STORAGE_BLOB_PUT),
        ),
        Operation::from_hostcall(
            StorageManifestSetDriver(cap.clone()),
            selium_abi::hostcall_contract!(STORAGE_MANIFEST_SET),
        ),
        Operation::from_hostcall(
            StorageBlobGetDriver(cap.clone()),
            selium_abi::hostcall_contract!(STORAGE_BLOB_GET),
        ),
        Operation::from_hostcall(
            StorageManifestGetDriver(cap),
            selium_abi::hostcall_contract!(STORAGE_MANIFEST_GET),
        ),
    )
}
