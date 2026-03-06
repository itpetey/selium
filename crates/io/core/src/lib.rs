//! Unified app-facing communication primitives backed by in-memory or kernel core-io transport.

use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    future::Future,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use parking_lot::{Mutex, RwLock};
use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::{
    QueueAttach, QueueCommit, QueueCreate, QueueDelivery, QueueOverflow, QueueReserve, QueueRole,
    QueueStatusCode, ShmAlloc, ShmRegion, decode_rkyv, encode_rkyv,
};
use selium_io_durability::{DurabilityError, DurableLogStore, ReplayStart, RetentionPolicy};
use selium_kernel::{
    guest_error::GuestError,
    services::{queue_service::QueueService, shared_memory_service::SharedMemoryDriver},
    spi::{queue::QueueCapability, shared_memory::SharedMemoryCapability},
};
use thiserror::Error;
use tokio::{sync::Notify, time::timeout};

const DEFAULT_QUEUE_CAPACITY_FRAMES: u32 = 1024;
const DEFAULT_QUEUE_MAX_FRAME_BYTES: u32 = 1024 * 1024;
const PUBLISH_TIMEOUT_MS: u32 = 5_000;
const WAIT_TIMEOUT_MS: u32 = 30_000;
const MAX_SUBSCRIBE_BACKLOG: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ChannelKind {
    Rpc,
    Event,
    Stream,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ChannelConfig {
    pub kind: ChannelKind,
    pub retention: RetentionPolicy,
}

impl Default for ChannelConfig {
    fn default() -> Self {
        Self {
            kind: ChannelKind::Event,
            retention: RetentionPolicy::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct Frame {
    pub channel: String,
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub headers: BTreeMap<String, String>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct PublishAck {
    pub sequence: u64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Error)]
pub enum DataPlaneError {
    #[error("channel `{0}` already exists")]
    ChannelExists(String),
    #[error("channel `{0}` does not exist")]
    UnknownChannel(String),
    #[error("channel `{0}` exists with a different kind")]
    ChannelKindMismatch(String),
    #[error("kernel transport error: {0}")]
    Kernel(String),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error(transparent)]
    Durability(#[from] DurabilityError),
}

#[derive(Clone)]
pub struct CoreIo {
    inner: Arc<CoreIoInner>,
}

struct CoreIoInner {
    transport: Transport,
    persistence_dir: Option<PathBuf>,
    channels: RwLock<HashMap<String, Arc<ChannelState>>>,
}

#[derive(Clone)]
enum Transport {
    InMemory,
    Kernel(KernelTransport),
}

#[derive(Clone)]
struct KernelTransport {
    queue: QueueService,
    shm: Arc<SharedMemoryDriver>,
    queue_capacity_frames: u32,
    queue_max_frame_bytes: u32,
}

struct ChannelState {
    config: ChannelConfig,
    durable: Mutex<DurableLogStore>,
    transport: ChannelTransport,
    notify: Option<Arc<Notify>>,
}

enum ChannelTransport {
    InMemory,
    Kernel(KernelChannel),
}

struct KernelChannel {
    queue: selium_kernel::services::queue_service::QueueState,
    writer: selium_kernel::services::queue_service::QueueEndpoint,
    queue_service: QueueService,
    shm: Arc<SharedMemoryDriver>,
}

pub struct Subscription {
    channel_name: String,
    backlog: VecDeque<Frame>,
    mode: SubscriptionMode,
}

enum SubscriptionMode {
    InMemory(InMemorySubscription),
    Kernel(KernelSubscription),
}

struct InMemorySubscription {
    channel: Arc<ChannelState>,
    next_sequence: u64,
}

struct KernelSubscription {
    reader: selium_kernel::services::queue_service::QueueEndpoint,
    queue_service: QueueService,
    shm: Arc<SharedMemoryDriver>,
    min_live_sequence: u64,
}

pub struct CoreIoBuilder {
    transport: BuilderTransport,
    persistence_dir: Option<PathBuf>,
    queue_capacity_frames: u32,
    queue_max_frame_bytes: u32,
}

enum BuilderTransport {
    InMemory,
    Kernel,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
struct WirePayload {
    headers: BTreeMap<String, String>,
    payload: Vec<u8>,
}

impl Default for CoreIo {
    fn default() -> Self {
        Self::new()
    }
}

impl CoreIo {
    pub fn builder() -> CoreIoBuilder {
        CoreIoBuilder {
            transport: BuilderTransport::InMemory,
            persistence_dir: None,
            queue_capacity_frames: DEFAULT_QUEUE_CAPACITY_FRAMES,
            queue_max_frame_bytes: DEFAULT_QUEUE_MAX_FRAME_BYTES,
        }
    }

    pub fn new() -> Self {
        Self::builder().build()
    }

    pub fn create_channel(
        &self,
        name: impl Into<String>,
        config: ChannelConfig,
    ) -> Result<(), DataPlaneError> {
        let name = name.into();
        let mut channels = self.inner.channels.write();
        if channels.contains_key(&name) {
            return Err(DataPlaneError::ChannelExists(name));
        }

        let durable = durable_store_for_channel(&self.inner.persistence_dir, &name, &config)?;

        let (transport, notify) = match &self.inner.transport {
            Transport::InMemory => (ChannelTransport::InMemory, Some(Arc::new(Notify::new()))),
            Transport::Kernel(kernel) => {
                let queue = kernel
                    .queue
                    .create(QueueCreate {
                        capacity_frames: kernel.queue_capacity_frames,
                        max_frame_bytes: kernel.queue_max_frame_bytes,
                        delivery: QueueDelivery::Lossless,
                        overflow: QueueOverflow::Block,
                    })
                    .map_err(map_guest_error)?;
                let writer = kernel
                    .queue
                    .attach(
                        &queue,
                        QueueAttach {
                            shared_id: 0,
                            role: QueueRole::Writer { writer_id: 1 },
                        },
                    )
                    .map_err(map_guest_error)?;
                (
                    ChannelTransport::Kernel(KernelChannel {
                        queue,
                        writer,
                        queue_service: kernel.queue,
                        shm: Arc::clone(&kernel.shm),
                    }),
                    None,
                )
            }
        };

        channels.insert(
            name,
            Arc::new(ChannelState {
                config,
                durable: Mutex::new(durable),
                transport,
                notify,
            }),
        );
        Ok(())
    }

    pub fn ensure_channel(
        &self,
        name: impl Into<String>,
        config: ChannelConfig,
    ) -> Result<(), DataPlaneError> {
        let name = name.into();
        if let Some(existing) = self.inner.channels.read().get(&name).cloned() {
            if existing.config.kind != config.kind {
                return Err(DataPlaneError::ChannelKindMismatch(name));
            }
            return Ok(());
        }

        self.create_channel(name, config)
    }

    pub fn publish(
        &self,
        channel: &str,
        headers: BTreeMap<String, String>,
        payload: Vec<u8>,
    ) -> Result<PublishAck, DataPlaneError> {
        let state = self.channel_state(channel)?;
        let timestamp_ms = unix_timestamp_ms();

        match &state.transport {
            ChannelTransport::InMemory => {
                let record = {
                    let mut durable = state.durable.lock();
                    durable.append(timestamp_ms, headers, payload)?
                };
                if let Some(notify) = &state.notify {
                    notify.notify_waiters();
                }
                Ok(PublishAck {
                    sequence: record.sequence,
                    timestamp_ms: record.timestamp_ms,
                })
            }
            ChannelTransport::Kernel(kernel) => {
                let wire = WirePayload {
                    headers: headers.clone(),
                    payload: payload.clone(),
                };
                let wire_bytes = encode_rkyv(&wire)
                    .map_err(|err| DataPlaneError::Serialization(err.to_string()))?;
                let wire_len = u32::try_from(wire_bytes.len())
                    .map_err(|_| DataPlaneError::Kernel("payload exceeds u32".to_string()))?;

                let reserve = block_on_future(kernel.queue_service.reserve(
                    &kernel.writer,
                    QueueReserve {
                        endpoint_id: 0,
                        len: wire_len,
                        timeout_ms: PUBLISH_TIMEOUT_MS,
                    },
                ))?
                .map_err(map_guest_error)?;
                if reserve.code != QueueStatusCode::Ok {
                    return Err(DataPlaneError::Kernel(format!(
                        "queue reserve failed with status {:?}",
                        reserve.code
                    )));
                }
                let reservation = reserve.reservation.ok_or_else(|| {
                    DataPlaneError::Kernel("queue reserve returned no reservation".to_string())
                })?;

                let region = kernel
                    .shm
                    .alloc(ShmAlloc {
                        size: wire_len,
                        align: 8,
                    })
                    .map_err(map_guest_error)?;
                kernel
                    .shm
                    .write(region, 0, &wire_bytes)
                    .map_err(map_guest_error)?;

                let commit = kernel
                    .queue_service
                    .commit(
                        &kernel.writer,
                        QueueCommit {
                            endpoint_id: 0,
                            reservation_id: reservation.reservation_id,
                            shm_shared_id: u64::from(region.offset),
                            offset: region.offset,
                            len: region.len,
                        },
                    )
                    .map_err(map_guest_error)?;
                if commit.code != QueueStatusCode::Ok {
                    return Err(DataPlaneError::Kernel(format!(
                        "queue commit failed with status {:?}",
                        commit.code
                    )));
                }

                let record = {
                    let mut durable = state.durable.lock();
                    durable.append(timestamp_ms, headers, payload)?
                };

                Ok(PublishAck {
                    sequence: record.sequence,
                    timestamp_ms: record.timestamp_ms,
                })
            }
        }
    }

    pub fn replay(
        &self,
        channel: &str,
        start: ReplayStart,
        limit: usize,
    ) -> Result<Vec<Frame>, DataPlaneError> {
        let state = self.channel_state(channel)?;
        let batch = state.durable.lock().replay(start, limit)?;
        Ok(batch
            .records
            .into_iter()
            .map(|record| Frame {
                channel: channel.to_string(),
                sequence: record.sequence,
                timestamp_ms: record.timestamp_ms,
                headers: record.headers,
                payload: record.payload,
            })
            .collect())
    }

    pub fn checkpoint(
        &self,
        channel: &str,
        name: impl Into<String>,
        sequence: u64,
    ) -> Result<(), DataPlaneError> {
        let state = self.channel_state(channel)?;
        state.durable.lock().checkpoint(name, sequence)?;
        Ok(())
    }

    pub fn subscribe(
        &self,
        channel: &str,
        start: ReplayStart,
    ) -> Result<Subscription, DataPlaneError> {
        let state = self.channel_state(channel)?;

        let backlog_records = state
            .durable
            .lock()
            .replay(start.clone(), MAX_SUBSCRIBE_BACKLOG)?
            .records;
        let mut backlog = backlog_records
            .into_iter()
            .map(|record| Frame {
                channel: channel.to_string(),
                sequence: record.sequence,
                timestamp_ms: record.timestamp_ms,
                headers: record.headers,
                payload: record.payload,
            })
            .collect::<VecDeque<_>>();

        let min_live_sequence = backlog.back().map_or_else(
            || min_sequence_for_start(&state, &start),
            |frame| Ok(frame.sequence.saturating_add(1)),
        )?;

        let mode = match &state.transport {
            ChannelTransport::InMemory => SubscriptionMode::InMemory(InMemorySubscription {
                channel: Arc::clone(&state),
                next_sequence: min_live_sequence,
            }),
            ChannelTransport::Kernel(kernel) => {
                let reader = kernel
                    .queue_service
                    .attach(
                        &kernel.queue,
                        QueueAttach {
                            shared_id: 0,
                            role: QueueRole::Reader,
                        },
                    )
                    .map_err(map_guest_error)?;

                SubscriptionMode::Kernel(KernelSubscription {
                    reader,
                    queue_service: kernel.queue_service,
                    shm: Arc::clone(&kernel.shm),
                    min_live_sequence,
                })
            }
        };

        Ok(Subscription {
            channel_name: channel.to_string(),
            backlog: std::mem::take(&mut backlog),
            mode,
        })
    }

    fn channel_state(&self, channel: &str) -> Result<Arc<ChannelState>, DataPlaneError> {
        self.inner
            .channels
            .read()
            .get(channel)
            .cloned()
            .ok_or_else(|| DataPlaneError::UnknownChannel(channel.to_string()))
    }
}

impl CoreIoBuilder {
    pub fn kernel_transport(mut self) -> Self {
        self.transport = BuilderTransport::Kernel;
        self
    }

    pub fn with_persistence_dir(mut self, path: impl AsRef<Path>) -> Self {
        self.persistence_dir = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn queue_capacity_frames(mut self, capacity_frames: u32) -> Self {
        self.queue_capacity_frames = capacity_frames.max(1);
        self
    }

    pub fn queue_max_frame_bytes(mut self, max_frame_bytes: u32) -> Self {
        self.queue_max_frame_bytes = max_frame_bytes.max(1);
        self
    }

    pub fn build(self) -> CoreIo {
        let transport = match self.transport {
            BuilderTransport::InMemory => Transport::InMemory,
            BuilderTransport::Kernel => Transport::Kernel(KernelTransport {
                queue: QueueService,
                shm: SharedMemoryDriver::new(),
                queue_capacity_frames: self.queue_capacity_frames,
                queue_max_frame_bytes: self.queue_max_frame_bytes,
            }),
        };

        CoreIo {
            inner: Arc::new(CoreIoInner {
                transport,
                persistence_dir: self.persistence_dir,
                channels: RwLock::new(HashMap::new()),
            }),
        }
    }
}

impl Subscription {
    pub async fn recv(&mut self) -> Result<Frame, DataPlaneError> {
        if let Some(frame) = self.backlog.pop_front() {
            return Ok(frame);
        }

        match &mut self.mode {
            SubscriptionMode::InMemory(mode) => loop {
                let notified = mode
                    .channel
                    .notify
                    .as_ref()
                    .map(Arc::clone)
                    .expect("in-memory channel has notify")
                    .notified_owned();

                if let Some(frame) = next_in_memory_frame(&self.channel_name, mode)? {
                    return Ok(frame);
                }
                notified.await;
            },
            SubscriptionMode::Kernel(mode) => loop {
                let wait = mode
                    .queue_service
                    .wait(&mode.reader, WAIT_TIMEOUT_MS)
                    .await
                    .map_err(map_guest_error)?;

                if wait.code == QueueStatusCode::Timeout {
                    continue;
                }
                if wait.code != QueueStatusCode::Ok {
                    return Err(DataPlaneError::Kernel(format!(
                        "queue wait failed with status {:?}",
                        wait.code
                    )));
                }

                let frame_ref = wait.frame.ok_or_else(|| {
                    DataPlaneError::Kernel("queue wait returned no frame".to_string())
                })?;
                let external_seq = frame_ref.seq.saturating_add(1);

                let ack = mode
                    .queue_service
                    .ack(
                        &mode.reader,
                        selium_abi::QueueAck {
                            endpoint_id: 0,
                            seq: frame_ref.seq,
                        },
                    )
                    .map_err(map_guest_error)?;
                if ack.code != QueueStatusCode::Ok {
                    return Err(DataPlaneError::Kernel(format!(
                        "queue ack failed with status {:?}",
                        ack.code
                    )));
                }

                if external_seq < mode.min_live_sequence {
                    continue;
                }

                let bytes = mode
                    .shm
                    .read(
                        ShmRegion {
                            offset: frame_ref.offset,
                            len: frame_ref.len,
                        },
                        0,
                        frame_ref.len,
                    )
                    .map_err(map_guest_error)?;
                let wire: WirePayload = decode_rkyv(&bytes)
                    .map_err(|err| DataPlaneError::Serialization(err.to_string()))?;

                mode.min_live_sequence = external_seq.saturating_add(1);
                return Ok(Frame {
                    channel: self.channel_name.clone(),
                    sequence: external_seq,
                    timestamp_ms: unix_timestamp_ms(),
                    headers: wire.headers,
                    payload: wire.payload,
                });
            },
        }
    }

    pub async fn recv_timeout(&mut self, wait: Duration) -> Result<Option<Frame>, DataPlaneError> {
        match timeout(wait, self.recv()).await {
            Ok(frame) => frame.map(Some),
            Err(_) => Ok(None),
        }
    }
}

fn min_sequence_for_start(
    state: &ChannelState,
    start: &ReplayStart,
) -> Result<u64, DataPlaneError> {
    let durable = state.durable.lock();
    let min_sequence = match start {
        ReplayStart::Earliest => durable.first_sequence().unwrap_or(durable.next_sequence()),
        ReplayStart::Latest => durable.next_sequence(),
        ReplayStart::Sequence(sequence) => *sequence,
        ReplayStart::Timestamp(timestamp) => {
            let first = durable
                .replay(ReplayStart::Timestamp(*timestamp), 1)?
                .records;
            first
                .first()
                .map(|record| record.sequence)
                .unwrap_or_else(|| durable.next_sequence())
        }
        ReplayStart::Checkpoint(name) => durable.checkpoint_sequence(name).ok_or_else(|| {
            DataPlaneError::Durability(DurabilityError::UnknownCheckpoint(name.clone()))
        })?,
    };

    Ok(min_sequence)
}

fn next_in_memory_frame(
    channel_name: &str,
    mode: &mut InMemorySubscription,
) -> Result<Option<Frame>, DataPlaneError> {
    let mut replay = mode
        .channel
        .durable
        .lock()
        .replay(ReplayStart::Sequence(mode.next_sequence), 1)?
        .records
        .into_iter();
    let Some(record) = replay.next() else {
        return Ok(None);
    };

    mode.next_sequence = record.sequence.saturating_add(1);
    Ok(Some(Frame {
        channel: channel_name.to_string(),
        sequence: record.sequence,
        timestamp_ms: record.timestamp_ms,
        headers: record.headers,
        payload: record.payload,
    }))
}

fn durable_store_for_channel(
    persistence_dir: &Option<PathBuf>,
    channel: &str,
    config: &ChannelConfig,
) -> Result<DurableLogStore, DataPlaneError> {
    match persistence_dir {
        Some(dir) => {
            let path = dir.join(format!("{}.json", sanitize_channel_name(channel)));
            Ok(DurableLogStore::file_backed(
                path,
                config.retention.clone(),
            )?)
        }
        None => Ok(DurableLogStore::in_memory(config.retention.clone())),
    }
}

fn sanitize_channel_name(channel: &str) -> String {
    let mut out = String::with_capacity(channel.len());
    for ch in channel.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "channel".to_string()
    } else {
        out
    }
}

fn map_guest_error(error: GuestError) -> DataPlaneError {
    DataPlaneError::Kernel(error.to_string())
}

fn block_on_future<F>(future: F) -> Result<F::Output, DataPlaneError>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        return std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .map_err(|err| DataPlaneError::Kernel(format!("create blocking runtime: {err}")))?;
            Ok(runtime.block_on(future))
        })
        .join()
        .map_err(|_| DataPlaneError::Kernel("join blocking runtime thread".to_string()))?;
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .map_err(|err| DataPlaneError::Kernel(format!("create blocking runtime: {err}")))?;
    Ok(runtime.block_on(future))
}

fn unix_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_publish_and_replay_round_trip() {
        let io = CoreIo::new();
        io.create_channel("camera.frames", ChannelConfig::default())
            .expect("create channel");

        let ack1 = io
            .publish("camera.frames", BTreeMap::new(), b"a".to_vec())
            .expect("publish");
        let ack2 = io
            .publish("camera.frames", BTreeMap::new(), b"b".to_vec())
            .expect("publish");

        assert_eq!(ack1.sequence, 1);
        assert_eq!(ack2.sequence, 2);

        let replay = io
            .replay("camera.frames", ReplayStart::Earliest, 10)
            .expect("replay");
        assert_eq!(replay.len(), 2);
        assert_eq!(replay[0].payload, b"a".to_vec());
        assert_eq!(replay[1].payload, b"b".to_vec());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn kernel_transport_publish_and_subscribe() {
        let io = CoreIo::builder().kernel_transport().build();
        io.create_channel("events", ChannelConfig::default())
            .expect("create channel");

        let mut subscriber = io
            .subscribe("events", ReplayStart::Latest)
            .expect("subscribe");
        io.publish("events", BTreeMap::new(), b"hello".to_vec())
            .expect("publish");

        let frame = subscriber.recv().await.expect("recv");
        assert_eq!(frame.payload, b"hello".to_vec());
    }

    #[test]
    fn persistence_survives_restart() {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("selium-core-io-{id}"));

        {
            let io = CoreIo::builder().with_persistence_dir(&dir).build();
            io.create_channel("persisted", ChannelConfig::default())
                .expect("create channel");
            io.publish("persisted", BTreeMap::new(), b"persist".to_vec())
                .expect("publish");
        }

        {
            let io = CoreIo::builder().with_persistence_dir(&dir).build();
            io.create_channel("persisted", ChannelConfig::default())
                .expect("create channel");
            let replay = io
                .replay("persisted", ReplayStart::Earliest, 10)
                .expect("replay");
            assert_eq!(replay.len(), 1);
            assert_eq!(replay[0].payload, b"persist".to_vec());
        }

        let _ = std::fs::remove_dir_all(dir);
    }
}
