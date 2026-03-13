//! Storage hostcall drivers.

use std::{convert::TryFrom, sync::Arc};

use selium_abi::{
    Capability, StorageBlobGet, StorageBlobGetResult, StorageBlobPut, StorageBlobPutResult,
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
        let authorisation =
            super::ensure_capability_authorised(context.registry(), Capability::StorageLifecycle);
        let policy = context
            .registry()
            .extension::<StorageProcessPolicy>()
            .unwrap_or_else(|| Arc::new(StorageProcessPolicy::default()));

        async move {
            authorisation?;
            let log = inner.open_log(policy, input).await.map_err(Into::into)?;
            let slot = registrar
                .insert(log.clone(), None, ResourceType::StorageLog)
                .map_err(GuestError::from)?;
            let resource_id = registrar.entry(slot).ok_or(GuestError::NotFound)?;
            registrar
                .grant_root_session_resources(
                    &[
                        Capability::StorageLifecycle,
                        Capability::StorageLogRead,
                        Capability::StorageLogWrite,
                    ],
                    resource_id,
                )
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
        let authorisation =
            super::ensure_capability_authorised(context.registry(), Capability::StorageLifecycle);
        let policy = context
            .registry()
            .extension::<StorageProcessPolicy>()
            .unwrap_or_else(|| Arc::new(StorageProcessPolicy::default()));

        async move {
            authorisation?;
            let store = inner
                .open_blob_store(policy, input)
                .await
                .map_err(Into::into)?;
            let slot = registrar
                .insert(store.clone(), None, ResourceType::StorageBlobStore)
                .map_err(GuestError::from)?;
            let resource_id = registrar.entry(slot).ok_or(GuestError::NotFound)?;
            registrar
                .grant_root_session_resources(
                    &[
                        Capability::StorageLifecycle,
                        Capability::StorageBlobRead,
                        Capability::StorageBlobWrite,
                    ],
                    resource_id,
                )
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
        let log = (|| -> GuestResult<LogHandle<C::Log>> {
            let slot = usize::try_from(input.log_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::StorageLogWrite, slot)?;
            context
                .registry()
                .with::<LogHandle<C::Log>, _>(slot, |log| log.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let log = (|| -> GuestResult<LogHandle<C::Log>> {
            let slot = usize::try_from(input.log_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::StorageLogWrite, slot)?;
            context
                .registry()
                .with::<LogHandle<C::Log>, _>(slot, |log| log.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let log = (|| -> GuestResult<LogHandle<C::Log>> {
            let slot = usize::try_from(input.log_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::StorageLogRead, slot)?;
            context
                .registry()
                .with::<LogHandle<C::Log>, _>(slot, |log| log.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let log = (|| -> GuestResult<LogHandle<C::Log>> {
            let slot = usize::try_from(input.log_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::StorageLogRead, slot)?;
            context
                .registry()
                .with::<LogHandle<C::Log>, _>(slot, |log| log.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let log = (|| -> GuestResult<LogHandle<C::Log>> {
            let slot = usize::try_from(input.log_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::StorageLogRead, slot)?;
            context
                .registry()
                .with::<LogHandle<C::Log>, _>(slot, |log| log.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let store = (|| -> GuestResult<BlobStoreHandle<C::BlobStore>> {
            let slot = usize::try_from(input.store_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::StorageBlobWrite, slot)?;
            context
                .registry()
                .with::<BlobStoreHandle<C::BlobStore>, _>(slot, |store| store.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let store = (|| -> GuestResult<BlobStoreHandle<C::BlobStore>> {
            let slot = usize::try_from(input.store_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::StorageBlobWrite, slot)?;
            context
                .registry()
                .with::<BlobStoreHandle<C::BlobStore>, _>(slot, |store| store.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let store = (|| -> GuestResult<BlobStoreHandle<C::BlobStore>> {
            let slot = usize::try_from(input.store_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::StorageBlobRead, slot)?;
            context
                .registry()
                .with::<BlobStoreHandle<C::BlobStore>, _>(slot, |store| store.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let store = (|| -> GuestResult<BlobStoreHandle<C::BlobStore>> {
            let slot = usize::try_from(input.store_id).map_err(|_| GuestError::InvalidArgument)?;
            super::ensure_slot_authorised(context.registry(), Capability::StorageBlobRead, slot)?;
            context
                .registry()
                .with::<BlobStoreHandle<C::BlobStore>, _>(slot, |store| store.clone())
                .ok_or(GuestError::NotFound)
        })();

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
        let future = match slot {
            Ok(slot) => {
                let meta = context
                    .registry()
                    .entry(slot)
                    .and_then(|id| context.registry().registry().metadata(id))
                    .ok_or(GuestError::NotFound);

                match meta {
                    Ok(meta) => {
                        if let Err(err) =
                            authorise_storage_close(context.registry(), slot, meta.kind)
                        {
                            EitherClose::<C>::Error(Err(err))
                        } else {
                            match meta.kind {
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
                            }
                        }
                    }
                    Err(err) => EitherClose::<C>::Error(Err(err)),
                }
            }
            Err(err) => EitherClose::<C>::Error(Err(err)),
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

fn authorise_storage_close(
    registry: &crate::registry::InstanceRegistry,
    slot: usize,
    kind: ResourceType,
) -> GuestResult<()> {
    let capabilities = match kind {
        ResourceType::StorageLog => &[
            Capability::StorageLifecycle,
            Capability::StorageLogRead,
            Capability::StorageLogWrite,
        ][..],
        ResourceType::StorageBlobStore => &[
            Capability::StorageLifecycle,
            Capability::StorageBlobRead,
            Capability::StorageBlobWrite,
        ][..],
        _ => return Err(GuestError::InvalidArgument),
    };
    super::ensure_slot_authorised_any(registry, capabilities, slot).map(|_| ())
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

#[cfg(test)]
mod tests {
    use super::*;

    use std::{collections::HashMap, sync::Mutex};

    use selium_abi::{StorageBlobPut, StorageOpenBlobStore, StorageOpenLog, StorageStatusCode};

    use crate::{
        registry::Registry,
        services::session_service::{ResourceScope, RootSession, Session, SessionAuthnMethod},
        spi::storage::StorageFuture,
    };

    struct TestStorageCapability {
        log_appends: Arc<Mutex<Vec<Vec<u8>>>>,
        blob_puts: Arc<Mutex<Vec<Vec<u8>>>>,
    }

    impl Default for TestStorageCapability {
        fn default() -> Self {
            Self {
                log_appends: Arc::new(Mutex::new(Vec::new())),
                blob_puts: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl Clone for TestStorageCapability {
        fn clone(&self) -> Self {
            Self {
                log_appends: Arc::clone(&self.log_appends),
                blob_puts: Arc::clone(&self.blob_puts),
            }
        }
    }

    impl StorageCapability for TestStorageCapability {
        type Error = GuestError;
        type Log = &'static str;
        type BlobStore = &'static str;

        fn open_log(
            &self,
            _policy: Arc<StorageProcessPolicy>,
            _input: StorageOpenLog,
        ) -> StorageFuture<LogHandle<Self::Log>, Self::Error> {
            Box::pin(async move { Ok(LogHandle { inner: "log" }) })
        }

        fn open_blob_store(
            &self,
            _policy: Arc<StorageProcessPolicy>,
            _input: StorageOpenBlobStore,
        ) -> StorageFuture<BlobStoreHandle<Self::BlobStore>, Self::Error> {
            Box::pin(async move {
                Ok(BlobStoreHandle {
                    inner: "blob-store",
                })
            })
        }

        fn log_append(
            &self,
            _log: &Self::Log,
            input: StorageLogAppend,
        ) -> StorageFuture<StorageLogAppendResult, Self::Error> {
            self.log_appends
                .lock()
                .expect("log append lock")
                .push(input.payload);
            Box::pin(async move {
                Ok(StorageLogAppendResult {
                    code: StorageStatusCode::Ok,
                    sequence: Some(1),
                })
            })
        }

        fn log_replay(
            &self,
            _log: &Self::Log,
            _input: StorageLogReplay,
        ) -> StorageFuture<StorageLogReplayResult, Self::Error> {
            panic!("log_replay not used in test")
        }

        fn log_checkpoint(
            &self,
            _log: &Self::Log,
            _input: StorageLogCheckpoint,
        ) -> StorageFuture<StorageStatus, Self::Error> {
            panic!("log_checkpoint not used in test")
        }

        fn log_checkpoint_get(
            &self,
            _log: &Self::Log,
            _input: StorageLogCheckpointGet,
        ) -> StorageFuture<StorageCheckpointResult, Self::Error> {
            panic!("log_checkpoint_get not used in test")
        }

        fn log_bounds(
            &self,
            _log: &Self::Log,
            _input: StorageLogBounds,
        ) -> StorageFuture<StorageLogBoundsResult, Self::Error> {
            panic!("log_bounds not used in test")
        }

        fn blob_put(
            &self,
            _store: &Self::BlobStore,
            input: StorageBlobPut,
        ) -> StorageFuture<StorageBlobPutResult, Self::Error> {
            self.blob_puts
                .lock()
                .expect("blob put lock")
                .push(input.bytes);
            Box::pin(async move {
                Ok(StorageBlobPutResult {
                    code: StorageStatusCode::Ok,
                    blob_id: Some("blob-1".to_string()),
                })
            })
        }

        fn blob_get(
            &self,
            _store: &Self::BlobStore,
            _input: StorageBlobGet,
        ) -> StorageFuture<StorageBlobGetResult, Self::Error> {
            panic!("blob_get not used in test")
        }

        fn manifest_set(
            &self,
            _store: &Self::BlobStore,
            _input: StorageManifestSet,
        ) -> StorageFuture<StorageStatus, Self::Error> {
            panic!("manifest_set not used in test")
        }

        fn manifest_get(
            &self,
            _store: &Self::BlobStore,
            _input: StorageManifestGet,
        ) -> StorageFuture<StorageManifestGetResult, Self::Error> {
            panic!("manifest_get not used in test")
        }

        fn close_log(
            &self,
            _log: LogHandle<Self::Log>,
        ) -> StorageFuture<StorageStatus, Self::Error> {
            Box::pin(async move {
                Ok(StorageStatus {
                    code: StorageStatusCode::Ok,
                })
            })
        }

        fn close_blob_store(
            &self,
            _store: BlobStoreHandle<Self::BlobStore>,
        ) -> StorageFuture<StorageStatus, Self::Error> {
            Box::pin(async move {
                Ok(StorageStatus {
                    code: StorageStatusCode::Ok,
                })
            })
        }
    }

    struct TestContext {
        registry: crate::registry::InstanceRegistry,
    }

    impl HostcallContext for TestContext {
        fn registry(&self) -> &crate::registry::InstanceRegistry {
            &self.registry
        }

        fn registry_mut(&mut self) -> &mut crate::registry::InstanceRegistry {
            &mut self.registry
        }

        fn mailbox_base(&mut self) -> Option<usize> {
            None
        }
    }

    fn registry_with_capabilities(
        capabilities: Vec<Capability>,
    ) -> crate::registry::InstanceRegistry {
        let registry = Registry::new();
        let mut instance = registry.instance().expect("instance");
        instance
            .insert_extension(RootSession(Session::bootstrap(capabilities, [0; 32])))
            .expect("root session");
        instance
    }

    fn registry_with_scoped_entitlements(
        entitlements: HashMap<Capability, ResourceScope>,
    ) -> crate::registry::InstanceRegistry {
        let registry = Registry::new();
        let mut instance = registry.instance().expect("instance");
        instance
            .insert_extension(RootSession(Session::bootstrap_with_scoped_principal(
                entitlements,
                [0; 32],
                selium_abi::PrincipalRef::new(selium_abi::PrincipalKind::Internal, "runtime"),
                SessionAuthnMethod::Delegated,
            )))
            .expect("root session");
        instance
    }

    #[test]
    fn storage_close_allows_log_readers_to_close_logs() {
        let instance = registry_with_capabilities(vec![Capability::StorageLogRead]);
        let slot = instance
            .registrar()
            .insert((), None, ResourceType::StorageLog)
            .expect("insert log");

        authorise_storage_close(&instance, slot, ResourceType::StorageLog)
            .expect("log close should be authorised");
    }

    #[test]
    fn storage_close_allows_blob_writers_to_close_blob_stores() {
        let instance = registry_with_capabilities(vec![Capability::StorageBlobWrite]);
        let slot = instance
            .registrar()
            .insert((), None, ResourceType::StorageBlobStore)
            .expect("insert blob store");

        authorise_storage_close(&instance, slot, ResourceType::StorageBlobStore)
            .expect("blob close should be authorised");
    }

    #[test]
    fn storage_close_rejects_unrelated_capabilities() {
        let instance = registry_with_capabilities(vec![Capability::StorageBlobRead]);
        let slot = instance
            .registrar()
            .insert((), None, ResourceType::StorageLog)
            .expect("insert log");

        let err = authorise_storage_close(&instance, slot, ResourceType::StorageLog)
            .expect_err("log close should be denied");
        assert!(matches!(err, GuestError::PermissionDenied));
    }

    #[tokio::test]
    async fn storage_open_log_grants_scoped_log_write_access() {
        let capability = TestStorageCapability::default();
        let mut ctx = TestContext {
            registry: registry_with_scoped_entitlements(HashMap::from([
                (Capability::StorageLifecycle, ResourceScope::Any),
                (Capability::StorageLogWrite, ResourceScope::None),
            ])),
        };
        let open = StorageOpenLogDriver(capability.clone());
        let append = StorageLogAppendDriver(capability.clone());

        let log = open
            .to_future(
                &mut ctx,
                StorageOpenLog {
                    name: "audit".to_string(),
                },
            )
            .await
            .expect("open log");
        let result = append
            .to_future(
                &mut ctx,
                StorageLogAppend {
                    log_id: log.resource_id,
                    timestamp_ms: 1,
                    headers: Default::default(),
                    payload: b"entry".to_vec(),
                },
            )
            .await
            .expect("append log");

        assert_eq!(result.code, StorageStatusCode::Ok);
        assert_eq!(
            capability
                .log_appends
                .lock()
                .expect("log append lock")
                .as_slice(),
            &[b"entry".to_vec()]
        );
    }

    #[tokio::test]
    async fn storage_open_blob_store_grants_scoped_blob_write_access() {
        let capability = TestStorageCapability::default();
        let mut ctx = TestContext {
            registry: registry_with_scoped_entitlements(HashMap::from([
                (Capability::StorageLifecycle, ResourceScope::Any),
                (Capability::StorageBlobWrite, ResourceScope::None),
            ])),
        };
        let open = StorageOpenBlobStoreDriver(capability.clone());
        let put = StorageBlobPutDriver(capability.clone());

        let store = open
            .to_future(
                &mut ctx,
                StorageOpenBlobStore {
                    name: "artifacts".to_string(),
                },
            )
            .await
            .expect("open blob store");
        let result = put
            .to_future(
                &mut ctx,
                StorageBlobPut {
                    store_id: store.resource_id,
                    bytes: b"blob".to_vec(),
                },
            )
            .await
            .expect("put blob");

        assert_eq!(result.code, StorageStatusCode::Ok);
        assert_eq!(
            capability
                .blob_puts
                .lock()
                .expect("blob put lock")
                .as_slice(),
            &[b"blob".to_vec()]
        );
    }
}
