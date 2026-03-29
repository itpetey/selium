//! Selium guest SDK.

use parking_lot::RwLock;
use rkyv::api::high::{HighDeserializer, HighValidator};
use rkyv::rancor::Error as RancorError;
use selium_abi::{
    AbiError, ActivityEvent, BlobStoreDescriptor, DurableLogDescriptor, GuestLogEntry,
    MeteringObservation, NetworkListenerDescriptor, NetworkSessionDescriptor,
    NetworkStreamDescriptor, ProcessDescriptor, ProcessId, SharedMappingDescriptor,
    SharedRegionDescriptor, SignalDescriptor, StorageRecord, decode_rkyv, deframe_bytes,
    encode_rkyv, frame_bytes,
};
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::{broadcast, mpsc};
use tracing::field::{Field, Visit};
use tracing::{Event, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::Context;
use tracing_subscriber::prelude::*;

pub use selium_abi::{
    Capability, CapabilityGrant, GuestHost, HostError, HostFuture, LocalityScope, ResourceClass,
    ResourceIdentity, ResourceSelector, ScopeContext,
};
pub use selium_abi::{EntrypointMetadata, InterfaceMetadata};
pub use selium_guest_macros::{entrypoint, pattern_interface};
pub use tracing::{debug, error, info, trace, warn};

pub mod native;

#[derive(Debug, Error)]
pub enum GuestError {
    #[error("host error: {0}")]
    Host(String),
    #[error("codec error: {0}")]
    Codec(#[from] selium_abi::RkyvError),
    #[error("permission denied for capability {0:?}")]
    PermissionDenied(Capability),
    #[error("pattern error: {0}")]
    Pattern(String),
}

pub type Result<T> = std::result::Result<T, GuestError>;

impl From<HostError> for GuestError {
    fn from(value: HostError) -> Self {
        match value {
            HostError::Host(message) => Self::Host(message),
            HostError::PermissionDenied(capability) => Self::PermissionDenied(capability),
        }
    }
}

#[derive(Clone)]
pub struct GuestContext {
    host: Arc<dyn GuestHost>,
    scope_context: ScopeContext,
}

impl GuestContext {
    pub fn new<H>(host: H) -> Self
    where
        H: GuestHost + 'static,
    {
        Self::from_shared_host(Arc::new(host))
    }

    pub fn from_shared_host(host: Arc<dyn GuestHost>) -> Self {
        Self {
            host,
            scope_context: ScopeContext {
                locality: LocalityScope::Cluster,
                ..ScopeContext::default()
            },
        }
    }

    pub fn with_scope_context(mut self, scope_context: ScopeContext) -> Self {
        self.scope_context = scope_context;
        self
    }

    pub(crate) fn host(&self) -> Arc<dyn GuestHost> {
        self.host.scoped(self.scope_context.clone())
    }

    fn require(
        &self,
        capability: Capability,
        resource_class: Option<ResourceClass>,
        resource_id: Option<ResourceIdentity>,
    ) -> Result<()> {
        let mut scope_context = self.scope_context.clone();
        scope_context.resource_class = resource_class;
        scope_context.resource_id = resource_id;

        let allowed = self.host().authorises(capability.clone(), &scope_context)?;
        if allowed {
            Ok(())
        } else {
            Err(GuestError::PermissionDenied(capability))
        }
    }
}

pub fn encode_typed<T>(value: &T) -> Result<Vec<u8>>
where
    T: selium_abi::RkyvEncode,
{
    frame_bytes(&encode_rkyv(value)?).map_err(abi_error_to_guest_error)
}

pub fn decode_typed<T>(bytes: &[u8]) -> Result<T>
where
    T: rkyv::Archive + Sized,
    for<'a> T::Archived: rkyv::Deserialize<T, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
{
    let payload = deframe_bytes(bytes).map_err(abi_error_to_guest_error)?;
    Ok(decode_rkyv(payload)?)
}

fn abi_error_to_guest_error(error: AbiError) -> GuestError {
    GuestError::Host(format!("{:?}: {}", error.code, error.message))
}

pub fn block_on_entrypoint<F>(future: F)
where
    F: Future<Output = ()>,
{
    futures::executor::block_on(future)
}

pub fn run_entrypoint_safely<F>(future: F)
where
    F: Future<Output = ()>,
{
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| block_on_entrypoint(future)));
    if result.is_err() {
        std::process::abort();
    }
}

pub struct SharedMemoryHandle {
    context: GuestContext,
    descriptor: SharedMappingDescriptor,
    owns_region: bool,
}

impl SharedMemoryHandle {
    pub fn allocate(context: GuestContext, size: u32, alignment: u32) -> Result<Self> {
        context.require(
            Capability::SharedMemory,
            Some(ResourceClass::SharedRegion),
            None,
        )?;
        let region = context.host().allocate_shared_region(size, alignment)?;
        Self::attach_internal(context, region, 0, region.len, true)
    }

    pub fn attach(
        context: GuestContext,
        region: SharedRegionDescriptor,
        offset: u32,
        len: u32,
    ) -> Result<Self> {
        Self::attach_internal(context, region, offset, len, false)
    }

    fn attach_internal(
        context: GuestContext,
        region: SharedRegionDescriptor,
        offset: u32,
        len: u32,
        owns_region: bool,
    ) -> Result<Self> {
        context.require(
            Capability::SharedMemory,
            Some(ResourceClass::SharedMapping),
            Some(ResourceIdentity::Shared(region.shared_id)),
        )?;
        let descriptor = context
            .host()
            .attach_shared_region(region.shared_id, offset, len)?;
        Ok(Self {
            context,
            descriptor,
            owns_region,
        })
    }

    pub fn descriptor(&self) -> SharedMappingDescriptor {
        self.descriptor
    }

    pub fn read(&self, offset: u32, len: usize) -> Result<Vec<u8>> {
        self.context.require(
            Capability::SharedMemory,
            Some(ResourceClass::SharedMapping),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        Ok(self
            .context
            .host()
            .read_shared_memory(self.descriptor.local_id, offset, len)?)
    }

    pub fn write(&self, offset: u32, bytes: &[u8]) -> Result<()> {
        self.context.require(
            Capability::SharedMemory,
            Some(ResourceClass::SharedMapping),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        Ok(self
            .context
            .host()
            .write_shared_memory(self.descriptor.local_id, offset, bytes)?)
    }
}

impl Drop for SharedMemoryHandle {
    fn drop(&mut self) {
        let _ = self
            .context
            .host()
            .detach_shared_region(self.descriptor.local_id);
    }
}

impl SharedMemoryHandle {
    pub fn destroy_region(self) -> Result<()> {
        if !self.owns_region {
            return Err(GuestError::Pattern(
                "only allocated shared memory can destroy its backing region".to_string(),
            ));
        }
        self.context.require(
            Capability::SharedMemory,
            Some(ResourceClass::SharedRegion),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        let local_id = self.descriptor.local_id;
        let shared_id = self.descriptor.shared_id;
        self.context.host().detach_shared_region(local_id)?;
        self.context.host().destroy_shared_region(shared_id)?;
        std::mem::forget(self);
        Ok(())
    }
}

#[derive(Clone)]
pub struct SignalHandle {
    context: GuestContext,
    descriptor: SignalDescriptor,
}

impl SignalHandle {
    pub fn create(context: GuestContext) -> Result<Self> {
        context.require(Capability::Signal, Some(ResourceClass::Signal), None)?;
        let descriptor = context.host().create_signal()?;
        Ok(Self {
            context,
            descriptor,
        })
    }

    pub fn attach(context: GuestContext, shared_id: u64) -> Result<Self> {
        context.require(
            Capability::Signal,
            Some(ResourceClass::Signal),
            Some(ResourceIdentity::Shared(shared_id)),
        )?;
        let descriptor = context.host().attach_signal(shared_id)?;
        Ok(Self {
            context,
            descriptor,
        })
    }

    pub fn shared_id(&self) -> u64 {
        self.descriptor.shared_id
    }

    pub fn notify(&self) -> Result<u64> {
        self.context.require(
            Capability::Signal,
            Some(ResourceClass::Signal),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        Ok(self
            .context
            .host()
            .notify_signal(self.descriptor.local_id)?)
    }

    pub async fn wait(&self, observed_generation: u64, timeout_ms: u64) -> Result<u64> {
        self.context.require(
            Capability::Signal,
            Some(ResourceClass::Signal),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        Ok(self
            .context
            .host()
            .wait_signal(self.descriptor.local_id, observed_generation, timeout_ms)
            .await?)
    }

    pub fn close(self) -> Result<()> {
        self.context.host().close_signal(self.descriptor.local_id)?;
        std::mem::forget(self);
        Ok(())
    }
}

impl Drop for SignalHandle {
    fn drop(&mut self) {
        let _ = self.context.host().close_signal(self.descriptor.local_id);
    }
}

#[derive(Clone)]
pub struct DurableLogHandle {
    context: GuestContext,
    descriptor: DurableLogDescriptor,
}

impl DurableLogHandle {
    pub fn open(context: GuestContext, name: impl Into<String>) -> Result<Self> {
        context.require(Capability::Storage, Some(ResourceClass::DurableLog), None)?;
        let descriptor = context.host().open_log(name.into())?;
        Ok(Self {
            context,
            descriptor,
        })
    }

    pub fn append(
        &self,
        timestamp_ms: u64,
        headers: Vec<(String, String)>,
        payload: Vec<u8>,
    ) -> Result<u64> {
        self.context.require(
            Capability::Storage,
            Some(ResourceClass::DurableLog),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        Ok(self.context.host().append_log(
            self.descriptor.local_id,
            timestamp_ms,
            headers,
            payload,
        )?)
    }

    pub fn replay(&self, from_sequence: Option<u64>, limit: usize) -> Result<Vec<StorageRecord>> {
        self.context.require(
            Capability::Storage,
            Some(ResourceClass::DurableLog),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        Ok(self
            .context
            .host()
            .replay_log(self.descriptor.local_id, from_sequence, limit)?)
    }

    pub fn checkpoint(&self, name: impl Into<String>, sequence: u64) -> Result<()> {
        self.context.require(
            Capability::Storage,
            Some(ResourceClass::DurableLog),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        Ok(self
            .context
            .host()
            .checkpoint_log(self.descriptor.local_id, name.into(), sequence)?)
    }

    pub fn checkpoint_sequence(&self, name: &str) -> Result<Option<u64>> {
        self.context.require(
            Capability::Storage,
            Some(ResourceClass::DurableLog),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        Ok(self
            .context
            .host()
            .checkpoint_sequence(self.descriptor.local_id, name)?)
    }

    pub fn close(self) -> Result<()> {
        self.context.host().close_log(self.descriptor.local_id)?;
        std::mem::forget(self);
        Ok(())
    }
}

impl Drop for DurableLogHandle {
    fn drop(&mut self) {
        let _ = self.context.host().close_log(self.descriptor.local_id);
    }
}

#[derive(Clone)]
pub struct BlobStoreHandle {
    context: GuestContext,
    descriptor: BlobStoreDescriptor,
}

impl BlobStoreHandle {
    pub fn open(context: GuestContext, name: impl Into<String>) -> Result<Self> {
        context.require(Capability::Storage, Some(ResourceClass::BlobStore), None)?;
        let descriptor = context.host().open_blob_store(name.into())?;
        Ok(Self {
            context,
            descriptor,
        })
    }

    pub fn put(&self, bytes: Vec<u8>) -> Result<String> {
        self.context.require(
            Capability::Storage,
            Some(ResourceClass::BlobStore),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        Ok(self
            .context
            .host()
            .put_blob(self.descriptor.local_id, bytes)?)
    }

    pub fn get(&self, blob_id: &str) -> Result<Option<Vec<u8>>> {
        self.context.require(
            Capability::Storage,
            Some(ResourceClass::BlobStore),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        Ok(self
            .context
            .host()
            .get_blob(self.descriptor.local_id, blob_id)?)
    }

    pub fn set_manifest(&self, name: impl Into<String>, blob_id: impl Into<String>) -> Result<()> {
        self.context.require(
            Capability::Storage,
            Some(ResourceClass::BlobStore),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        Ok(self.context.host().set_manifest(
            self.descriptor.local_id,
            name.into(),
            blob_id.into(),
        )?)
    }

    pub fn manifest(&self, name: &str) -> Result<Option<String>> {
        self.context.require(
            Capability::Storage,
            Some(ResourceClass::BlobStore),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        Ok(self
            .context
            .host()
            .get_manifest(self.descriptor.local_id, name)?)
    }

    pub fn close(self) -> Result<()> {
        self.context
            .host()
            .close_blob_store(self.descriptor.local_id)?;
        std::mem::forget(self);
        Ok(())
    }
}

impl Drop for BlobStoreHandle {
    fn drop(&mut self) {
        let _ = self
            .context
            .host()
            .close_blob_store(self.descriptor.local_id);
    }
}

#[derive(Clone)]
pub struct NetworkListenerHandle {
    context: GuestContext,
    descriptor: NetworkListenerDescriptor,
}

impl NetworkListenerHandle {
    pub fn listen(context: GuestContext, address: impl Into<String>) -> Result<Self> {
        context.require(Capability::Network, Some(ResourceClass::Listener), None)?;
        let descriptor = context.host().listen(address.into())?;
        Ok(Self {
            context,
            descriptor,
        })
    }

    pub fn descriptor(&self) -> &NetworkListenerDescriptor {
        &self.descriptor
    }

    pub fn address(&self) -> &str {
        let _ = &self.context;
        self.descriptor.address.as_str()
    }

    pub fn close(self) -> Result<()> {
        self.context
            .host()
            .close_listener(self.descriptor.local_id)?;
        std::mem::forget(self);
        Ok(())
    }
}

impl Drop for NetworkListenerHandle {
    fn drop(&mut self) {
        let _ = self.context.host().close_listener(self.descriptor.local_id);
    }
}

#[derive(Clone)]
pub struct NetworkSessionHandle {
    context: GuestContext,
    descriptor: NetworkSessionDescriptor,
}

impl NetworkSessionHandle {
    pub fn connect(context: GuestContext, authority: impl Into<String>) -> Result<Self> {
        context.require(Capability::Network, Some(ResourceClass::Session), None)?;
        let descriptor = context.host().connect(authority.into())?;
        Ok(Self {
            context,
            descriptor,
        })
    }

    pub fn open_stream(&self) -> Result<ByteStream> {
        self.context.require(
            Capability::Network,
            Some(ResourceClass::Session),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        let descriptor = self.context.host().open_stream(self.descriptor.local_id)?;
        Ok(ByteStream {
            context: self.context.clone(),
            descriptor,
            session_shared_id: self.descriptor.shared_id,
        })
    }

    pub async fn request_reply<TReq, TResp>(
        &self,
        method: &str,
        path: &str,
        request: &TReq,
        timeout_ms: u64,
    ) -> Result<TResp>
    where
        TReq: selium_abi::RkyvEncode,
        TResp: rkyv::Archive + Sized,
        for<'a> TResp::Archived: rkyv::Deserialize<TResp, HighDeserializer<RancorError>>
            + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
    {
        self.context.require(
            Capability::Network,
            Some(ResourceClass::RequestExchange),
            Some(ResourceIdentity::Shared(self.descriptor.shared_id)),
        )?;
        let exchange = self.context.host().send_request(
            self.descriptor.local_id,
            method.to_string(),
            path.to_string(),
            encode_typed(request)?,
        )?;
        let (_, body) = self
            .context
            .host()
            .wait_request_response(exchange, timeout_ms)
            .await?;
        decode_typed::<TResp>(&body)
    }

    pub fn close(self) -> Result<()> {
        self.context
            .host()
            .close_session(self.descriptor.local_id)?;
        std::mem::forget(self);
        Ok(())
    }
}

impl Drop for NetworkSessionHandle {
    fn drop(&mut self) {
        let _ = self.context.host().close_session(self.descriptor.local_id);
    }
}

#[derive(Clone)]
pub struct ByteStream {
    context: GuestContext,
    descriptor: NetworkStreamDescriptor,
    session_shared_id: u64,
}

impl ByteStream {
    pub fn send(&self, bytes: Vec<u8>) -> Result<()> {
        self.context.require(
            Capability::Network,
            Some(ResourceClass::Stream),
            Some(ResourceIdentity::Shared(self.session_shared_id)),
        )?;
        Ok(self
            .context
            .host()
            .send_stream_chunk(self.descriptor.local_id, bytes)?)
    }

    pub fn recv(&self) -> Result<Option<Vec<u8>>> {
        self.context.require(
            Capability::Network,
            Some(ResourceClass::Stream),
            Some(ResourceIdentity::Shared(self.session_shared_id)),
        )?;
        Ok(self
            .context
            .host()
            .recv_stream_chunk(self.descriptor.local_id)?)
    }

    pub fn close(self) -> Result<()> {
        self.context.host().close_stream(self.descriptor.local_id)?;
        std::mem::forget(self);
        Ok(())
    }
}

impl Drop for ByteStream {
    fn drop(&mut self) {
        let _ = self.context.host().close_stream(self.descriptor.local_id);
    }
}

#[derive(Clone)]
pub struct ProcessHandle {
    context: GuestContext,
    descriptor: ProcessDescriptor,
}

impl ProcessHandle {
    pub fn start(
        context: GuestContext,
        module_id: impl Into<String>,
        entrypoint: impl Into<String>,
        arguments: Vec<Vec<u8>>,
        grants: Vec<CapabilityGrant>,
    ) -> Result<Self> {
        context.require(
            Capability::ProcessLifecycle,
            Some(ResourceClass::Process),
            None,
        )?;
        if grants.iter().any(|grant| grant.selectors.is_empty()) {
            return Err(GuestError::Pattern(
                "process grants must include at least one selector".to_string(),
            ));
        }
        let descriptor =
            context
                .host()
                .start_process(module_id.into(), entrypoint.into(), arguments, grants)?;
        Ok(Self {
            context,
            descriptor,
        })
    }

    pub fn stop(&self) -> Result<()> {
        self.context.require(
            Capability::ProcessLifecycle,
            Some(ResourceClass::Process),
            Some(ResourceIdentity::Local(self.descriptor.local_id)),
        )?;
        Ok(self.context.host().stop_process(self.descriptor.local_id)?)
    }

    pub fn descriptor(&self) -> &ProcessDescriptor {
        &self.descriptor
    }

    pub fn metering(&self) -> Result<Option<MeteringObservation>> {
        self.context.require(
            Capability::MeteringRead,
            Some(ResourceClass::MeteringStream),
            Some(ResourceIdentity::Local(self.descriptor.local_id)),
        )?;
        Ok(self
            .context
            .host()
            .metering_observation(self.descriptor.local_id)?)
    }
}

#[derive(Clone)]
pub struct ActivityLogHandle {
    context: GuestContext,
}

impl ActivityLogHandle {
    pub fn open(context: GuestContext) -> Result<Self> {
        context.require(
            Capability::ActivityRead,
            Some(ResourceClass::ActivityLog),
            None,
        )?;
        Ok(Self { context })
    }

    pub fn read_from(&self, cursor: usize) -> Result<Vec<ActivityEvent>> {
        self.context.require(
            Capability::ActivityRead,
            Some(ResourceClass::ActivityLog),
            None,
        )?;
        Ok(self.context.host().read_activity_from(cursor)?)
    }
}

type BoxedPatternFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send>>;
type BoxedRequestHandler = Arc<dyn Fn(Vec<u8>) -> BoxedPatternFuture + Send + Sync>;

#[derive(Clone, Default)]
pub struct PatternFabric {
    inner: Arc<PatternFabricInner>,
}

#[derive(Default)]
struct PatternFabricInner {
    topics: RwLock<HashMap<String, broadcast::Sender<Vec<u8>>>>,
    request_handlers: RwLock<HashMap<String, BoxedRequestHandler>>,
    live_tables: RwLock<HashMap<String, BTreeMap<String, Vec<u8>>>>,
}

impl PatternFabric {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn publish<T>(&self, topic: &str, payload: &T) -> Result<usize>
    where
        T: selium_abi::RkyvEncode,
    {
        let sender = self.topic_sender(topic);
        sender
            .send(encode_typed(payload)?)
            .map_err(|error| GuestError::Pattern(error.to_string()))
    }

    pub fn fanout<T>(&self, topic: &str, payload: &T) -> Result<usize>
    where
        T: selium_abi::RkyvEncode,
    {
        self.publish(topic, payload)
    }

    pub fn subscribe<T>(&self, topic: &str) -> Subscription<T> {
        Subscription {
            receiver: self.topic_sender(topic).subscribe(),
            _marker: PhantomData,
        }
    }

    pub fn register_request_reply<TReq, TResp, F, Fut>(&self, name: &str, handler: F) -> Result<()>
    where
        TReq: rkyv::Archive + Send + Sized + 'static,
        TResp: selium_abi::RkyvEncode + Send + 'static,
        for<'a> TReq::Archived: rkyv::Deserialize<TReq, HighDeserializer<RancorError>>
            + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
        F: Fn(TReq) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<TResp>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        let wrapper: BoxedRequestHandler = Arc::new(move |bytes| {
            let handler = handler.clone();
            Box::pin(async move {
                let request = decode_typed::<TReq>(&bytes)?;
                let response = handler(request).await?;
                encode_typed(&response)
            })
        });
        let mut request_handlers = self.inner.request_handlers.write();
        if request_handlers.contains_key(name) {
            return Err(GuestError::Pattern(format!(
                "request/reply interface `{name}` already registered"
            )));
        }
        request_handlers.insert(name.to_string(), wrapper);
        Ok(())
    }

    pub async fn request_reply<TReq, TResp>(&self, name: &str, request: &TReq) -> Result<TResp>
    where
        TReq: selium_abi::RkyvEncode,
        TResp: rkyv::Archive + Sized,
        for<'a> TResp::Archived: rkyv::Deserialize<TResp, HighDeserializer<RancorError>>
            + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
    {
        let handler = self
            .inner
            .request_handlers
            .read()
            .get(name)
            .cloned()
            .ok_or_else(|| {
                GuestError::Pattern(format!("unknown request/reply interface `{name}`"))
            })?;
        let response = handler(encode_typed(request)?).await?;
        decode_typed::<TResp>(&response)
    }

    pub fn open_stream(&self, capacity: usize) -> (PatternStreamWriter, PatternStreamReader) {
        let (sender, receiver) = mpsc::channel(capacity.max(1));
        (
            PatternStreamWriter { sender },
            PatternStreamReader { receiver },
        )
    }

    pub fn live_table<T>(&self, name: &str) -> LiveTable<T> {
        self.inner
            .live_tables
            .write()
            .entry(name.to_string())
            .or_default();
        LiveTable {
            fabric: self.clone(),
            name: name.to_string(),
            _marker: PhantomData,
        }
    }

    fn topic_sender(&self, topic: &str) -> broadcast::Sender<Vec<u8>> {
        let mut topics = self.inner.topics.write();
        topics
            .entry(topic.to_string())
            .or_insert_with(|| broadcast::channel(64).0)
            .clone()
    }
}

pub struct Subscription<T> {
    receiver: broadcast::Receiver<Vec<u8>>,
    _marker: PhantomData<T>,
}

impl<T> Subscription<T> {
    pub async fn recv(&mut self) -> Result<T>
    where
        T: rkyv::Archive + Sized,
        for<'a> T::Archived: rkyv::Deserialize<T, HighDeserializer<RancorError>>
            + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
    {
        let payload = self
            .receiver
            .recv()
            .await
            .map_err(|error| GuestError::Pattern(error.to_string()))?;
        decode_typed::<T>(&payload)
    }
}

pub struct PatternStreamWriter {
    sender: mpsc::Sender<Vec<u8>>,
}

impl PatternStreamWriter {
    pub async fn send(&self, bytes: Vec<u8>) -> Result<()> {
        self.sender
            .send(bytes)
            .await
            .map_err(|error| GuestError::Pattern(error.to_string()))
    }
}

pub struct PatternStreamReader {
    receiver: mpsc::Receiver<Vec<u8>>,
}

impl PatternStreamReader {
    pub async fn recv(&mut self) -> Option<Vec<u8>> {
        self.receiver.recv().await
    }
}

pub struct LiveTable<T> {
    fabric: PatternFabric,
    name: String,
    _marker: PhantomData<T>,
}

impl<T> LiveTable<T> {
    pub fn insert(&self, key: impl Into<String>, value: &T) -> Result<()>
    where
        T: selium_abi::RkyvEncode,
    {
        self.fabric
            .inner
            .live_tables
            .write()
            .entry(self.name.clone())
            .or_default()
            .insert(key.into(), encode_typed(value)?);
        Ok(())
    }

    pub fn get(&self, key: &str) -> Result<Option<T>>
    where
        T: rkyv::Archive + Sized,
        for<'a> T::Archived: rkyv::Deserialize<T, HighDeserializer<RancorError>>
            + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
    {
        self.fabric
            .inner
            .live_tables
            .read()
            .get(&self.name)
            .and_then(|table| table.get(key))
            .map(|bytes| decode_typed::<T>(bytes))
            .transpose()
    }

    pub fn snapshot(&self) -> Result<Vec<(String, T)>>
    where
        T: rkyv::Archive + Sized,
        for<'a> T::Archived: rkyv::Deserialize<T, HighDeserializer<RancorError>>
            + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
    {
        self.fabric
            .inner
            .live_tables
            .read()
            .get(&self.name)
            .map(|table| {
                table
                    .iter()
                    .map(|(key, bytes)| Ok((key.clone(), decode_typed::<T>(bytes)?)))
                    .collect()
            })
            .unwrap_or_else(|| Ok(Vec::new()))
    }
}

pub type GuestLogRecord = GuestLogEntry;

#[derive(Clone)]
pub struct GuestLogResource {
    host: Arc<dyn GuestHost>,
    process_id: Option<ProcessId>,
    can_write: bool,
    can_read: bool,
}

impl GuestLogResource {
    pub fn new(context: &GuestContext, process_id: Option<ProcessId>) -> Result<Self> {
        let can_write = context
            .require(
                Capability::GuestLogWrite,
                Some(ResourceClass::GuestLog),
                process_id.map(ResourceIdentity::Local),
            )
            .is_ok();
        let can_read = context
            .require(
                Capability::GuestLogRead,
                Some(ResourceClass::GuestLog),
                process_id.map(ResourceIdentity::Local),
            )
            .is_ok();
        if !can_write && !can_read {
            return Err(GuestError::PermissionDenied(Capability::GuestLogRead));
        }
        Ok(Self {
            host: context.host(),
            process_id,
            can_write,
            can_read,
        })
    }

    pub fn records(&self) -> Result<Vec<GuestLogRecord>> {
        if !self.can_read {
            return Err(GuestError::PermissionDenied(Capability::GuestLogRead));
        }
        Ok(self
            .host
            .read_guest_logs_from(0, self.process_id)?
            .into_iter()
            .filter(|record| self.process_id.is_none() || record.process_id == self.process_id)
            .collect::<Vec<_>>())
    }

    pub fn install(&self) -> Result<tracing::subscriber::DefaultGuard> {
        if !self.can_write {
            return Err(GuestError::PermissionDenied(Capability::GuestLogWrite));
        }
        let subscriber = tracing_subscriber::registry().with(GuestLogLayer {
            resource: self.clone(),
        });
        Ok(tracing::subscriber::set_default(subscriber))
    }
}

struct GuestLogLayer {
    resource: GuestLogResource,
}

impl<S> Layer<S> for GuestLogLayer
where
    S: Subscriber,
{
    fn on_event(&self, event: &Event<'_>, _context: Context<'_, S>) {
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);
        let _ = self.resource.host.write_guest_log(GuestLogEntry {
            process_id: self.resource.process_id,
            level: event.metadata().level().to_string(),
            target: event.metadata().target().to_string(),
            message: visitor.message,
        });
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{value:?}").trim_matches('"').to_string();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::native::NativeHost;
    use selium_abi::ResourceSelector;
    use std::task::Poll;

    #[derive(Debug, Clone, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
    #[rkyv(bytecheck())]
    struct DemoPayload {
        message: String,
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pub_sub_and_request_reply_work_natively() {
        let fabric = PatternFabric::new();
        let mut subscription = fabric.subscribe::<DemoPayload>("updates");
        fabric
            .register_request_reply::<DemoPayload, DemoPayload, _, _>(
                "echo",
                |payload| async move { Ok(payload) },
            )
            .expect("register request reply");

        fabric
            .publish(
                "updates",
                &DemoPayload {
                    message: "hello".to_string(),
                },
            )
            .expect("publish payload");
        let event = subscription.recv().await.expect("receive payload");
        assert_eq!(event.message, "hello");

        let reply = fabric
            .request_reply::<DemoPayload, DemoPayload>(
                "echo",
                &DemoPayload {
                    message: "echo".to_string(),
                },
            )
            .await
            .expect("request reply");
        assert_eq!(reply.message, "echo");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn handles_and_native_pattern_fallbacks_work() {
        let context = GuestContext::new(NativeHost::with_grants(vec![
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(ResourceClass::SharedRegion)],
            ),
            CapabilityGrant::new(
                Capability::SharedMemory,
                vec![ResourceSelector::ResourceClass(
                    ResourceClass::SharedMapping,
                )],
            ),
            CapabilityGrant::new(
                Capability::Signal,
                vec![ResourceSelector::ResourceClass(ResourceClass::Signal)],
            ),
            CapabilityGrant::new(
                Capability::Storage,
                vec![ResourceSelector::ResourceClass(ResourceClass::DurableLog)],
            ),
            CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::Locality(LocalityScope::Cluster)],
            ),
            CapabilityGrant::new(
                Capability::Network,
                vec![ResourceSelector::ResourceClass(ResourceClass::Listener)],
            ),
            CapabilityGrant::new(
                Capability::MeteringRead,
                vec![ResourceSelector::ResourceClass(
                    ResourceClass::MeteringStream,
                )],
            ),
            CapabilityGrant::new(
                Capability::ActivityRead,
                vec![ResourceSelector::ResourceClass(ResourceClass::ActivityLog)],
            ),
        ]));
        let left =
            SharedMemoryHandle::allocate(context.clone(), 64, 8).expect("allocate shared memory");
        let right = SharedMemoryHandle::attach(
            context.clone(),
            SharedRegionDescriptor {
                shared_id: left.descriptor().shared_id,
                len: 64,
            },
            0,
            64,
        )
        .expect("attach shared memory");
        left.write(0, b"abc").expect("write memory");
        assert_eq!(right.read(0, 3).expect("read memory"), b"abc");

        let signal = SignalHandle::create(context.clone()).expect("create signal");
        let attached =
            SignalHandle::attach(context.clone(), signal.shared_id()).expect("attach signal");
        signal.notify().expect("notify signal");
        assert_eq!(attached.wait(0, 1_000).await.expect("wait signal"), 1);

        let log = DurableLogHandle::open(context.clone(), "audit").expect("open durable log");
        let sequence = log
            .append(42, Vec::new(), b"entry".to_vec())
            .expect("append durable log");
        log.checkpoint("boot", sequence).expect("checkpoint log");
        assert_eq!(
            log.checkpoint_sequence("boot")
                .expect("checkpoint sequence"),
            Some(sequence)
        );

        let listener = NetworkListenerHandle::listen(context.clone(), "127.0.0.1:9000")
            .expect("open listener");
        assert_eq!(listener.address(), "127.0.0.1:9000");

        let table = PatternFabric::new().live_table::<DemoPayload>("state");
        table
            .insert(
                "a",
                &DemoPayload {
                    message: "value".to_string(),
                },
            )
            .expect("insert table");
        assert_eq!(
            table.get("a").expect("get table").expect("entry").message,
            "value"
        );

        let process = ProcessHandle::start(
            context,
            "guest-module",
            "main",
            Vec::new(),
            vec![CapabilityGrant::new(
                Capability::ProcessLifecycle,
                vec![ResourceSelector::Locality(LocalityScope::Cluster)],
            )],
        )
        .expect("start process");
        assert_eq!(process.descriptor().module_id, "guest-module");
        assert!(process.metering().expect("read metering").is_none());
        process.stop().expect("stop process");

        let activity = ActivityLogHandle::open(GuestContext::new(NativeHost::with_grants(vec![
            CapabilityGrant::new(
                Capability::ActivityRead,
                vec![ResourceSelector::ResourceClass(ResourceClass::ActivityLog)],
            ),
        ])))
        .expect("open activity log");
        let _ = activity.read_from(0).expect("read activity log");
    }

    #[test]
    fn guest_tracing_layer_captures_logs() {
        let context = GuestContext::new(NativeHost::with_grants(vec![
            CapabilityGrant::new(
                Capability::GuestLogWrite,
                vec![ResourceSelector::ResourceClass(ResourceClass::GuestLog)],
            ),
            CapabilityGrant::new(
                Capability::GuestLogRead,
                vec![ResourceSelector::ResourceClass(ResourceClass::GuestLog)],
            ),
        ]));
        let logs = GuestLogResource::new(&context, None).expect("create guest log resource");
        let _guard = logs.install().expect("install log resource");

        tracing::info!(target: "selium_guest_test", "guest log message");

        let records = logs.records().expect("read guest logs");
        assert!(
            records
                .iter()
                .any(|record| record.message == "guest log message")
        );
    }

    #[test]
    fn missing_capability_is_rejected() {
        let context = GuestContext::new(NativeHost::with_grants(Vec::new()));
        let result = SharedMemoryHandle::allocate(context, 64, 8);

        assert!(matches!(
            result,
            Err(GuestError::PermissionDenied(Capability::SharedMemory))
        ));
    }

    #[test]
    fn block_on_entrypoint_handles_pending_wakeups() {
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let done_for_future = done.clone();
        let done_for_thread = done.clone();

        block_on_entrypoint(async move {
            std::future::poll_fn(|context| {
                if done_for_future.load(std::sync::atomic::Ordering::SeqCst) {
                    Poll::Ready(())
                } else {
                    let waker = context.waker().clone();
                    if !done_for_thread.swap(true, std::sync::atomic::Ordering::SeqCst) {
                        std::thread::spawn(move || waker.wake());
                    }
                    Poll::Pending
                }
            })
            .await;
        });

        assert!(done.load(std::sync::atomic::Ordering::SeqCst));
    }
}
