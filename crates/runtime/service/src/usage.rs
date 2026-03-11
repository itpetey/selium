use std::{
    collections::BTreeMap,
    fmt,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::{
    RuntimeUsageAttribution, RuntimeUsageQuery, RuntimeUsageQueryResult, RuntimeUsageRecord,
    RuntimeUsageReplayStart, RuntimeUsageSample, RuntimeUsageSampleTrigger, decode_rkyv,
    encode_rkyv,
};
use selium_io_durability::{DurableLogStore, DurableRecord, ReplayStart, RetentionPolicy};
use selium_kernel::spi::usage::UsageRecorder;
use tokio::{
    sync::{Mutex, Notify},
    task::JoinHandle,
};

const USAGE_SAMPLES_PATH: &str = "usage/runtime-usage.rkyv";
const RECORD_KIND: &str = "runtime_usage_sample";
const RECORD_SCHEMA_V1: &str = "runtime_usage_sample.v1";
const RECORD_SCHEMA_V2: &str = "runtime_usage_sample.v2";

#[derive(Clone)]
pub struct RuntimeUsageCollector {
    inner: Arc<CollectorInner>,
}

struct CollectorInner {
    sample_period: Duration,
    store: Arc<Mutex<DurableLogStore>>,
}

pub struct ProcessUsageHandle {
    collector: RuntimeUsageCollector,
    workload_key: String,
    process_id: String,
    instance_id: Option<String>,
    attribution: RuntimeUsageAttribution,
    started_at_ms: u64,
    stop_signal: Notify,
    finished: AtomicBool,
    memory_high_watermark_bytes: AtomicU64,
    ingress_bytes: AtomicU64,
    egress_bytes: AtomicU64,
    storage_read_bytes: AtomicU64,
    storage_write_bytes: AtomicU64,
    emission: Mutex<EmissionState>,
    interval_task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ProcessUsageAttribution {
    pub instance_id: Option<String>,
    pub external_account_ref: Option<String>,
    pub module_id: String,
}

struct EmissionState {
    last_emitted_at_ms: u64,
    last_ingress_bytes: u64,
    last_egress_bytes: u64,
    last_storage_read_bytes: u64,
    last_storage_write_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
struct RuntimeUsageSampleV1 {
    workload_key: String,
    process_id: String,
    window_start_ms: u64,
    window_end_ms: u64,
    trigger: RuntimeUsageSampleTrigger,
    cpu_time_millis: u64,
    memory_high_watermark_bytes: u64,
    memory_byte_millis: u128,
    ingress_bytes: u64,
    egress_bytes: u64,
    storage_read_bytes: u64,
    storage_write_bytes: u64,
}

impl RuntimeUsageCollector {
    pub fn file_backed(work_dir: impl AsRef<Path>, sample_period: Duration) -> Result<Arc<Self>> {
        let path = work_dir.as_ref().join(USAGE_SAMPLES_PATH);
        let store = DurableLogStore::file_backed(path, RetentionPolicy::default())
            .context("open runtime usage store")?;
        Ok(Arc::new(Self {
            inner: Arc::new(CollectorInner {
                sample_period,
                store: Arc::new(Mutex::new(store)),
            }),
        }))
    }

    #[cfg(test)]
    pub(crate) fn in_memory(sample_period: Duration) -> Arc<Self> {
        Arc::new(Self {
            inner: Arc::new(CollectorInner {
                sample_period,
                store: Arc::new(Mutex::new(DurableLogStore::in_memory(
                    RetentionPolicy::default(),
                ))),
            }),
        })
    }

    pub async fn register_process(
        &self,
        workload_key: impl Into<String>,
        process_id: impl Into<String>,
        attribution: ProcessUsageAttribution,
    ) -> Result<Arc<ProcessUsageHandle>> {
        let workload_key = workload_key.into();
        let checkpoint_key = attribution.instance_id.as_deref().unwrap_or(&workload_key);
        let last_emitted_at_ms = self
            .recover_last_emitted_at_ms(checkpoint_key)
            .await?
            .unwrap_or_else(now_ms);
        let handle = Arc::new(ProcessUsageHandle {
            collector: self.clone(),
            workload_key,
            process_id: process_id.into(),
            instance_id: attribution.instance_id.clone(),
            attribution: RuntimeUsageAttribution {
                external_account_ref: attribution.external_account_ref,
                module_id: attribution.module_id,
            },
            started_at_ms: now_ms(),
            stop_signal: Notify::new(),
            finished: AtomicBool::new(false),
            memory_high_watermark_bytes: AtomicU64::new(0),
            ingress_bytes: AtomicU64::new(0),
            egress_bytes: AtomicU64::new(0),
            storage_read_bytes: AtomicU64::new(0),
            storage_write_bytes: AtomicU64::new(0),
            emission: Mutex::new(EmissionState {
                last_emitted_at_ms,
                last_ingress_bytes: 0,
                last_egress_bytes: 0,
                last_storage_read_bytes: 0,
                last_storage_write_bytes: 0,
            }),
            interval_task: Mutex::new(None),
        });
        handle.spawn_interval_task().await;
        Ok(handle)
    }

    pub async fn replay_usage(&self, query: &RuntimeUsageQuery) -> Result<RuntimeUsageQueryResult> {
        let replay_start = effective_replay_start(query);
        let store = self.inner.store.lock().await;
        let batch = store
            .replay(replay_start, if query.limit == 0 { 0 } else { usize::MAX })
            .context("replay runtime usage records")?;
        if query.limit == 0 {
            return Ok(RuntimeUsageQueryResult {
                records: Vec::new(),
                high_watermark: batch.high_watermark,
            });
        }

        let mut records = Vec::new();
        for record in batch.records {
            let sample = decode_record_sample(&record).context("decode runtime usage record")?;
            if !sample_matches_query(&sample, query) {
                continue;
            }

            records.push(RuntimeUsageRecord {
                sequence: record.sequence,
                timestamp_ms: record.timestamp_ms,
                headers: record.headers,
                sample,
            });
            if records.len() >= query.limit {
                break;
            }
        }

        Ok(RuntimeUsageQueryResult {
            records,
            high_watermark: batch.high_watermark,
        })
    }

    #[cfg(test)]
    async fn read_samples(&self) -> Vec<RuntimeUsageSample> {
        self.replay_usage(&RuntimeUsageQuery {
            start: RuntimeUsageReplayStart::Earliest,
            limit: usize::MAX,
            external_account_ref: None,
            workload_key: None,
            module_id: None,
            window_start_ms: None,
            window_end_ms: None,
        })
        .await
        .expect("replay usage samples")
        .records
        .into_iter()
        .map(|record| record.sample)
        .collect()
    }

    async fn recover_last_emitted_at_ms(&self, workload_key: &str) -> Result<Option<u64>> {
        let checkpoint_name = checkpoint_name(workload_key);
        let store = self.inner.store.lock().await;
        let Some(sequence) = store.checkpoint_sequence(&checkpoint_name) else {
            return Ok(None);
        };
        let batch = store
            .replay(ReplayStart::Sequence(sequence), 1)
            .context("replay runtime usage checkpoint")?;
        let Some(record) = batch
            .records
            .into_iter()
            .find(|record| record.sequence == sequence)
        else {
            return Ok(None);
        };
        let sample = decode_record_sample(&record).context("decode runtime usage sample")?;
        Ok(Some(sample.window_end_ms))
    }

    async fn emit_sample(
        &self,
        handle: &ProcessUsageHandle,
        trigger: RuntimeUsageSampleTrigger,
    ) -> Result<bool> {
        let now = now_ms();
        let mut emission = handle.emission.lock().await;
        let window_start_ms = emission.last_emitted_at_ms.max(handle.started_at_ms);
        if now <= window_start_ms {
            return Ok(false);
        }

        let ingress_bytes = handle.ingress_bytes.load(Ordering::Relaxed);
        let egress_bytes = handle.egress_bytes.load(Ordering::Relaxed);
        let storage_read_bytes = handle.storage_read_bytes.load(Ordering::Relaxed);
        let storage_write_bytes = handle.storage_write_bytes.load(Ordering::Relaxed);
        let memory_high_watermark_bytes =
            handle.memory_high_watermark_bytes.load(Ordering::Relaxed);
        let sample = RuntimeUsageSample {
            workload_key: handle.workload_key.clone(),
            process_id: handle.process_id.clone(),
            attribution: handle.attribution.clone(),
            window_start_ms,
            window_end_ms: now,
            trigger,
            cpu_time_millis: now.saturating_sub(window_start_ms),
            memory_high_watermark_bytes,
            memory_byte_millis: u128::from(memory_high_watermark_bytes)
                * u128::from(now.saturating_sub(window_start_ms)),
            ingress_bytes: ingress_bytes.saturating_sub(emission.last_ingress_bytes),
            egress_bytes: egress_bytes.saturating_sub(emission.last_egress_bytes),
            storage_read_bytes: storage_read_bytes.saturating_sub(emission.last_storage_read_bytes),
            storage_write_bytes: storage_write_bytes
                .saturating_sub(emission.last_storage_write_bytes),
        };

        let mut headers = BTreeMap::new();
        headers.insert("kind".to_string(), RECORD_KIND.to_string());
        headers.insert("schema".to_string(), RECORD_SCHEMA_V2.to_string());
        headers.insert("workload_key".to_string(), handle.workload_key.clone());
        headers.insert("process_id".to_string(), handle.process_id.clone());
        headers.insert(
            "module_id".to_string(),
            handle.attribution.module_id.clone(),
        );
        if let Some(instance_id) = &handle.instance_id {
            headers.insert("instance_id".to_string(), instance_id.clone());
        }
        if let Some(external_account_ref) = &handle.attribution.external_account_ref {
            headers.insert(
                "external_account_ref".to_string(),
                external_account_ref.clone(),
            );
        }
        headers.insert("trigger".to_string(), trigger_name(trigger).to_string());

        let payload = encode_rkyv(&sample).context("encode runtime usage sample")?;
        let mut store = self.inner.store.lock().await;
        let record = store
            .append(now, headers, payload)
            .context("append runtime usage sample")?;
        let checkpoint_key = handle
            .instance_id
            .as_deref()
            .unwrap_or(&handle.workload_key);
        store
            .checkpoint(checkpoint_name(checkpoint_key), record.sequence)
            .context("checkpoint runtime usage sample")?;

        emission.last_emitted_at_ms = now;
        emission.last_ingress_bytes = ingress_bytes;
        emission.last_egress_bytes = egress_bytes;
        emission.last_storage_read_bytes = storage_read_bytes;
        emission.last_storage_write_bytes = storage_write_bytes;
        Ok(true)
    }
}

impl ProcessUsageHandle {
    pub fn set_memory_high_watermark_bytes(&self, bytes: u64) {
        let _ = self.memory_high_watermark_bytes.fetch_update(
            Ordering::Relaxed,
            Ordering::Relaxed,
            |current| Some(current.max(bytes)),
        );
    }

    pub async fn finish(&self) -> Result<()> {
        if self.finished.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.stop_signal.notify_waiters();
        if let Some(task) = self.interval_task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        self.collector
            .emit_sample(self, RuntimeUsageSampleTrigger::Termination)
            .await?;
        Ok(())
    }

    async fn spawn_interval_task(self: &Arc<Self>) {
        let collector = self.collector.clone();
        let handle = Arc::clone(self);
        let sample_period = collector.inner.sample_period;
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(sample_period);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = handle.stop_signal.notified() => break,
                    _ = ticker.tick() => {
                        if handle.finished.load(Ordering::Acquire) {
                            break;
                        }
                        if let Err(err) = collector.emit_sample(&handle, RuntimeUsageSampleTrigger::Interval).await {
                            tracing::warn!(workload_key = %handle.workload_key, error = %err, "runtime usage interval emission failed");
                        }
                    }
                }
            }
        });
        *self.interval_task.lock().await = Some(task);
    }
}

impl UsageRecorder for ProcessUsageHandle {
    fn record_network_ingress(&self, bytes: u64) {
        self.ingress_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_network_egress(&self, bytes: u64) {
        self.egress_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_storage_read(&self, bytes: u64) {
        self.storage_read_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn record_storage_write(&self, bytes: u64) {
        self.storage_write_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

impl fmt::Debug for ProcessUsageHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProcessUsageHandle")
            .field("workload_key", &self.workload_key)
            .field("process_id", &self.process_id)
            .field("attribution", &self.attribution)
            .finish()
    }
}

impl RuntimeUsageSampleV1 {
    fn into_current(self, record: &DurableRecord) -> RuntimeUsageSample {
        RuntimeUsageSample {
            workload_key: self.workload_key,
            process_id: self.process_id,
            attribution: RuntimeUsageAttribution {
                external_account_ref: record.headers.get("external_account_ref").cloned(),
                module_id: record.headers.get("module_id").cloned().unwrap_or_default(),
            },
            window_start_ms: self.window_start_ms,
            window_end_ms: self.window_end_ms,
            trigger: self.trigger,
            cpu_time_millis: self.cpu_time_millis,
            memory_high_watermark_bytes: self.memory_high_watermark_bytes,
            memory_byte_millis: self.memory_byte_millis,
            ingress_bytes: self.ingress_bytes,
            egress_bytes: self.egress_bytes,
            storage_read_bytes: self.storage_read_bytes,
            storage_write_bytes: self.storage_write_bytes,
        }
    }
}

fn checkpoint_name(workload_key: &str) -> String {
    format!("usage:{workload_key}")
}

fn decode_record_sample(record: &DurableRecord) -> Result<RuntimeUsageSample> {
    match record.headers.get("schema").map(String::as_str) {
        Some(RECORD_SCHEMA_V2) => decode_rkyv::<RuntimeUsageSample>(&record.payload)
            .context("decode runtime usage v2 sample"),
        Some(RECORD_SCHEMA_V1) | None => decode_rkyv::<RuntimeUsageSampleV1>(&record.payload)
            .context("decode runtime usage v1 sample")
            .map(|sample| sample.into_current(record)),
        Some(schema) => Err(anyhow::anyhow!(
            "unsupported runtime usage schema `{schema}`"
        )),
    }
}

fn effective_replay_start(query: &RuntimeUsageQuery) -> ReplayStart {
    match query.start {
        RuntimeUsageReplayStart::Earliest => query
            .window_start_ms
            .map(ReplayStart::Timestamp)
            .unwrap_or(ReplayStart::Earliest),
        RuntimeUsageReplayStart::Latest => ReplayStart::Latest,
        RuntimeUsageReplayStart::Sequence(sequence) => ReplayStart::Sequence(sequence),
        RuntimeUsageReplayStart::Timestamp(timestamp_ms) => ReplayStart::Timestamp(timestamp_ms),
    }
}

fn sample_matches_query(sample: &RuntimeUsageSample, query: &RuntimeUsageQuery) -> bool {
    if let Some(external_account_ref) = query.external_account_ref.as_deref()
        && sample.attribution.external_account_ref.as_deref() != Some(external_account_ref)
    {
        return false;
    }
    if let Some(workload_key) = query.workload_key.as_deref()
        && sample.workload_key != workload_key
    {
        return false;
    }
    if let Some(module_id) = query.module_id.as_deref()
        && sample.attribution.module_id != module_id
    {
        return false;
    }
    if let Some(window_start_ms) = query.window_start_ms
        && sample.window_end_ms < window_start_ms
    {
        return false;
    }
    if let Some(window_end_ms) = query.window_end_ms
        && sample.window_start_ms >= window_end_ms
    {
        return false;
    }
    true
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}

fn trigger_name(trigger: RuntimeUsageSampleTrigger) -> &'static str {
    match trigger {
        RuntimeUsageSampleTrigger::Interval => "interval",
        RuntimeUsageSampleTrigger::Termination => "termination",
    }
}

#[cfg(test)]
mod tests;
