//! Retention, checkpoints, and replay primitives.

use std::{
    collections::{BTreeMap, VecDeque},
    fs, io,
    path::{Path, PathBuf},
};

use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::{decode_rkyv, encode_rkyv};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ReplayStart {
    Earliest,
    Latest,
    Sequence(u64),
    Timestamp(u64),
    Checkpoint(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize, Default)]
#[rkyv(bytecheck())]
pub struct RetentionPolicy {
    pub max_entries: Option<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DurableRecord {
    pub sequence: u64,
    pub timestamp_ms: u64,
    pub headers: BTreeMap<String, String>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ReplayBatch {
    pub records: Vec<DurableRecord>,
    pub high_watermark: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct Checkpoint {
    pub name: String,
    pub sequence: u64,
}

#[derive(Debug, Error)]
pub enum DurabilityError {
    #[error("checkpoint `{0}` does not exist")]
    UnknownCheckpoint(String),
    #[error("sequence {sequence} is outside retained range [{min:?}, {max:?}]")]
    SequenceOutOfRange {
        sequence: u64,
        min: Option<u64>,
        max: Option<u64>,
    },
    #[error("invalid checkpoint name")]
    InvalidCheckpointName,
    #[error("I/O error: {0}")]
    Io(String),
    #[error("serialization error: {0}")]
    Serialization(String),
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct DurableLog {
    retention: RetentionPolicy,
    records: VecDeque<DurableRecord>,
    checkpoints: BTreeMap<String, u64>,
    next_sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableLogStore {
    log: DurableLog,
    path: Option<PathBuf>,
}

impl DurableLog {
    pub fn new(retention: RetentionPolicy) -> Self {
        Self {
            retention,
            records: VecDeque::new(),
            checkpoints: BTreeMap::new(),
            next_sequence: 1,
        }
    }

    pub fn retention(&self) -> &RetentionPolicy {
        &self.retention
    }

    pub fn set_retention(&mut self, retention: RetentionPolicy) {
        self.retention = retention;
        self.apply_retention();
    }

    pub fn append(
        &mut self,
        timestamp_ms: u64,
        headers: BTreeMap<String, String>,
        payload: Vec<u8>,
    ) -> DurableRecord {
        let record = DurableRecord {
            sequence: self.next_sequence,
            timestamp_ms,
            headers,
            payload,
        };
        self.next_sequence += 1;
        self.records.push_back(record.clone());
        self.apply_retention();
        record
    }

    pub fn latest_sequence(&self) -> Option<u64> {
        self.records.back().map(|record| record.sequence)
    }

    pub fn first_sequence(&self) -> Option<u64> {
        self.records.front().map(|record| record.sequence)
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub fn checkpoint(
        &mut self,
        name: impl Into<String>,
        sequence: u64,
    ) -> Result<(), DurabilityError> {
        let name = name.into();
        if name.trim().is_empty() {
            return Err(DurabilityError::InvalidCheckpointName);
        }
        self.ensure_sequence_is_checkpointable(sequence)?;
        self.checkpoints.insert(name, sequence);
        Ok(())
    }

    pub fn checkpoint_sequence(&self, name: &str) -> Option<u64> {
        self.checkpoints.get(name).copied()
    }

    pub fn replay(&self, start: ReplayStart, limit: usize) -> Result<ReplayBatch, DurabilityError> {
        let high_watermark = self.latest_sequence();
        if self.records.is_empty() {
            if let ReplayStart::Checkpoint(name) = &start {
                let sequence = self
                    .checkpoints
                    .get(name)
                    .copied()
                    .ok_or_else(|| DurabilityError::UnknownCheckpoint(name.clone()))?;
                self.ensure_sequence_is_checkpointable(sequence)?;
            }
            return Ok(ReplayBatch {
                records: Vec::new(),
                high_watermark,
            });
        }

        let iter = match start {
            ReplayStart::Earliest => self.records.iter().collect::<Vec<_>>(),
            ReplayStart::Latest => self.records.back().into_iter().collect::<Vec<_>>(),
            ReplayStart::Sequence(sequence) => {
                let min = self.first_sequence().expect("records not empty");
                let max = self.latest_sequence().expect("records not empty");
                if sequence < min {
                    return Err(DurabilityError::SequenceOutOfRange {
                        sequence,
                        min: Some(min),
                        max: Some(max),
                    });
                }
                if sequence > max {
                    Vec::new()
                } else {
                    self.records
                        .iter()
                        .filter(|record| record.sequence >= sequence)
                        .collect::<Vec<_>>()
                }
            }
            ReplayStart::Timestamp(timestamp_ms) => self
                .records
                .iter()
                .filter(|record| record.timestamp_ms >= timestamp_ms)
                .collect::<Vec<_>>(),
            ReplayStart::Checkpoint(name) => {
                let sequence = self
                    .checkpoints
                    .get(&name)
                    .copied()
                    .ok_or(DurabilityError::UnknownCheckpoint(name))?;
                if sequence == self.next_sequence {
                    Vec::new()
                } else {
                    self.ensure_sequence_is_retained(sequence)?;
                    self.records
                        .iter()
                        .filter(|record| record.sequence >= sequence)
                        .collect::<Vec<_>>()
                }
            }
        };

        let records = if limit == 0 {
            Vec::new()
        } else {
            iter.into_iter().take(limit).cloned().collect()
        };

        Ok(ReplayBatch {
            records,
            high_watermark,
        })
    }

    fn ensure_sequence_is_checkpointable(&self, sequence: u64) -> Result<(), DurabilityError> {
        if sequence == self.next_sequence {
            return Ok(());
        }
        self.ensure_sequence_is_retained(sequence)
    }

    fn ensure_sequence_is_retained(&self, sequence: u64) -> Result<(), DurabilityError> {
        let min = self.first_sequence();
        let max = self.latest_sequence();

        let retained = min
            .zip(max)
            .map(|(min, max)| sequence >= min && sequence <= max)
            .unwrap_or(false);

        if retained {
            return Ok(());
        }

        Err(DurabilityError::SequenceOutOfRange { sequence, min, max })
    }

    fn apply_retention(&mut self) {
        let Some(limit) = self.retention.max_entries else {
            return;
        };

        while self.records.len() > limit {
            if let Some(evicted) = self.records.pop_front() {
                self.checkpoints
                    .retain(|_, sequence| *sequence > evicted.sequence);
            }
        }
    }
}

impl Default for DurableLog {
    fn default() -> Self {
        Self::new(RetentionPolicy::default())
    }
}

impl DurableLogStore {
    pub fn in_memory(retention: RetentionPolicy) -> Self {
        Self {
            log: DurableLog::new(retention),
            path: None,
        }
    }

    pub fn file_backed(
        path: impl AsRef<Path>,
        retention: RetentionPolicy,
    ) -> Result<Self, DurabilityError> {
        let path = path.as_ref().to_path_buf();
        let log = if path.exists() {
            let bytes = fs::read(&path).map_err(io_error)?;
            let mut loaded: DurableLog = decode_rkyv(&bytes)
                .map_err(|err| DurabilityError::Serialization(err.to_string()))?;
            loaded.set_retention(retention);
            loaded
        } else {
            DurableLog::new(retention)
        };

        Ok(Self {
            log,
            path: Some(path),
        })
    }

    pub fn append(
        &mut self,
        timestamp_ms: u64,
        headers: BTreeMap<String, String>,
        payload: Vec<u8>,
    ) -> Result<DurableRecord, DurabilityError> {
        let record = self.log.append(timestamp_ms, headers, payload);
        self.flush()?;
        Ok(record)
    }

    pub fn checkpoint(
        &mut self,
        name: impl Into<String>,
        sequence: u64,
    ) -> Result<(), DurabilityError> {
        self.log.checkpoint(name, sequence)?;
        self.flush()
    }

    pub fn replay(&self, start: ReplayStart, limit: usize) -> Result<ReplayBatch, DurabilityError> {
        self.log.replay(start, limit)
    }

    pub fn first_sequence(&self) -> Option<u64> {
        self.log.first_sequence()
    }

    pub fn next_sequence(&self) -> u64 {
        self.log.next_sequence()
    }

    pub fn checkpoint_sequence(&self, name: &str) -> Option<u64> {
        self.log.checkpoint_sequence(name)
    }

    pub fn latest_sequence(&self) -> Option<u64> {
        self.log.latest_sequence()
    }

    fn flush(&self) -> Result<(), DurabilityError> {
        let Some(path) = &self.path else {
            return Ok(());
        };

        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            fs::create_dir_all(parent).map_err(io_error)?;
        }

        let bytes = encode_rkyv(&self.log)
            .map_err(|err| DurabilityError::Serialization(err.to_string()))?;
        let tmp_path = path.with_extension("tmp");
        fs::write(&tmp_path, bytes).map_err(io_error)?;
        fs::rename(&tmp_path, path).map_err(io_error)?;
        Ok(())
    }
}

fn io_error(error: io::Error) -> DurabilityError {
    DurabilityError::Io(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn record_payload(value: u8) -> Vec<u8> {
        vec![value]
    }

    #[test]
    fn retention_evicts_old_records_and_checkpoints() {
        let mut log = DurableLog::new(RetentionPolicy {
            max_entries: Some(2),
        });

        let r1 = log.append(10, BTreeMap::new(), record_payload(1));
        log.checkpoint("cp", r1.sequence).expect("checkpoint");
        log.append(20, BTreeMap::new(), record_payload(2));
        log.append(30, BTreeMap::new(), record_payload(3));

        assert_eq!(log.first_sequence(), Some(2));
        assert!(log.checkpoint_sequence("cp").is_none());
    }

    #[test]
    fn replay_from_checkpoint_returns_expected_slice() {
        let mut log = DurableLog::default();
        log.append(10, BTreeMap::new(), record_payload(1));
        let second = log.append(20, BTreeMap::new(), record_payload(2));
        log.append(30, BTreeMap::new(), record_payload(3));
        log.checkpoint("detector", second.sequence)
            .expect("checkpoint");

        let batch = log
            .replay(ReplayStart::Checkpoint("detector".to_string()), 10)
            .expect("replay");
        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.records[0].sequence, second.sequence);
    }

    #[test]
    fn checkpoint_can_store_next_sequence_resume_cursor() {
        let mut log = DurableLog::default();
        log.append(10, BTreeMap::new(), record_payload(1));
        log.append(20, BTreeMap::new(), record_payload(2));
        let cursor = log.next_sequence();
        log.checkpoint("cursor", cursor).expect("checkpoint");

        let empty = log
            .replay(ReplayStart::Checkpoint("cursor".to_string()), 10)
            .expect("replay empty cursor");
        assert!(empty.records.is_empty());

        let appended = log.append(30, BTreeMap::new(), record_payload(3));
        let resumed = log
            .replay(ReplayStart::Checkpoint("cursor".to_string()), 10)
            .expect("replay resumed cursor");
        assert_eq!(resumed.records.len(), 1);
        assert_eq!(resumed.records[0].sequence, appended.sequence);
    }

    #[test]
    fn replay_sequence_out_of_retained_range_errors() {
        let mut log = DurableLog::new(RetentionPolicy {
            max_entries: Some(1),
        });
        log.append(10, BTreeMap::new(), record_payload(1));
        log.append(20, BTreeMap::new(), record_payload(2));

        let err = log
            .replay(ReplayStart::Sequence(1), 10)
            .expect_err("expected range error");
        assert!(matches!(err, DurabilityError::SequenceOutOfRange { .. }));
    }

    #[test]
    fn file_backed_store_persists_records() {
        let id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("selium-durable-log-{id}.rkyv"));

        {
            let mut store = DurableLogStore::file_backed(
                &path,
                RetentionPolicy {
                    max_entries: Some(16),
                },
            )
            .expect("create store");
            store
                .append(123, BTreeMap::new(), vec![1, 2, 3])
                .expect("append");
            store.checkpoint("cp", 1).expect("checkpoint");
        }

        let store =
            DurableLogStore::file_backed(&path, RetentionPolicy::default()).expect("reload store");
        let replay = store.replay(ReplayStart::Earliest, 10).expect("replay");
        assert_eq!(replay.records.len(), 1);
        assert_eq!(replay.records[0].payload, vec![1, 2, 3]);
        assert_eq!(store.checkpoint_sequence("cp"), Some(1));

        let _ = std::fs::remove_file(path);
    }
}
