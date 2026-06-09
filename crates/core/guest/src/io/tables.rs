use std::{cell::RefCell, collections::HashMap, hash::Hash};

use crate::{
    encoding::{FlatMsg, HasSchema, SchemaDescriptor},
    io::{
        error::{Error, Result},
        pubsub::{self, Publisher, Subscriber},
    },
    schema,
};

/// Wire type for live table mutations, backed by Flatbuffers.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = "schemas/live_table.fbs",
    ty = "selium.live_table.LiveTableMessage",
    binding = "crate::fbs::selium::live_table::LiveTableMessage"
)]
pub struct LiveTableMessageWire {
    /// Topic-wide mutation id used to acknowledge writes in stream order.
    pub mutation_id: u64,
    /// The entry key encoded as bytes via FlatMsg.
    pub key_bytes: Vec<u8>,
    /// The entry value encoded as bytes via FlatMsg (empty for tombstones).
    pub value_bytes: Vec<u8>,
    /// Optional version that must be current for this mutation to apply (0 = none).
    pub expected_version: u64,
}

/// A table mutation published over a pub/sub topic.
///
/// Every `set` publishes one of these messages. All processes attached to
/// the same topic replay the stream to build their local materialised view.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveTableMessage<K, V> {
    /// Topic-wide mutation id used to acknowledge writes in stream order.
    pub mutation_id: u64,
    /// The entry key.
    pub key: K,
    /// The entry value, or `None` for deletes.
    pub value: Option<V>,
    /// Optional version that must be current for this mutation to apply.
    pub expected_version: Option<u64>,
}

impl<K: FlatMsg, V: FlatMsg> FlatMsg for LiveTableMessage<K, V> {
    fn encode(value: &Self) -> Vec<u8> {
        let key_bytes = FlatMsg::encode(&value.key);
        let value_bytes = match &value.value {
            Some(v) => FlatMsg::encode(v),
            None => Vec::new(),
        };
        let expected_version = value.expected_version.unwrap_or(0);

        let wire = LiveTableMessageWire::new(
            value.mutation_id,
            key_bytes,
            value_bytes,
            expected_version,
        );
        FlatMsg::encode(&wire)
    }

    fn decode(bytes: &[u8]) -> std::result::Result<Self, flatbuffers::InvalidFlatbuffer> {
        let wire: LiveTableMessageWire = FlatMsg::decode(bytes)?;
        let key: K = FlatMsg::decode(&wire.key_bytes)?;
        let value: Option<V> = if wire.value_bytes.is_empty() {
            None
        } else {
            Some(FlatMsg::decode(&wire.value_bytes)?)
        };
        let expected_version = if wire.expected_version == 0 {
            None
        } else {
            Some(wire.expected_version)
        };

        Ok(Self {
            mutation_id: wire.mutation_id,
            key,
            value,
            expected_version,
        })
    }
}

impl<K: FlatMsg, V: FlatMsg> HasSchema for LiveTableMessage<K, V> {
    const SCHEMA: SchemaDescriptor = LiveTableMessageWireSchema;
}

/// A materialised live table record.
///
/// A `None` value represents a tombstone — the key was deleted but its
/// version is still tracked for CAS consistency.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTableRecord<V> {
    /// The entry value, or `None` for deleted entries (tombstones).
    pub value: Option<V>,
    /// Monotonic version assigned by replay order.
    pub version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApplyOutcome {
    Applied(u64),
    Deleted(u64),
    Conflict { actual: Option<u64> },
}

/// A live table projected from a pub/sub topic stream.
///
/// # How it works
///
/// Writes are published as `LiveTableMessage`s to the underlying topic.
/// Reads are served from a local materialised `HashMap<K, V>`. Remote
/// writes from other processes attached to the same topic are picked up
/// by calling [`sync`](Self::sync).
///
/// On [`attach`](Self::attach), the subscriber starts at position 0 of
/// the ring buffer and replays every retained message to build the local
/// view before returning. If the topic has already overwritten its prefix,
/// attach fails with [`Error::ReaderBehind`]; callers that need indefinite
/// attachability must retain a large enough topic or restore from an
/// application snapshot before following the live stream.
pub struct LiveTable<K, V> {
    publisher: RefCell<Publisher<LiveTableMessage<K, V>>>,
    subscriber: RefCell<Subscriber<LiveTableMessage<K, V>>>,
    local: RefCell<HashMap<K, LiveTableRecord<V>>>,
}

impl<K, V> LiveTable<K, V>
where
    K: FlatMsg + Clone + Eq + Hash,
    V: FlatMsg + Clone,
{
    /// Creates a new live table with its own pub/sub topic.
    pub fn create(capacity: u64) -> Result<Self> {
        let (publisher, subscriber) = pubsub::create_pair(capacity)?;

        Ok(Self {
            publisher: RefCell::new(publisher),
            subscriber: RefCell::new(subscriber),
            local: RefCell::new(HashMap::new()),
        })
    }

    /// Attaches to an existing live table topic.
    ///
    /// Replays all existing messages to build the local materialised view
    /// before returning. Returns [`Error::ReaderBehind`] if the ring no longer
    /// retains the full mutation history required to rebuild the table.
    pub fn attach(shared_id: u64, capacity: u64) -> Result<Self>
    where
        K: Eq + Hash,
    {
        let (publisher, subscriber) = pubsub::attach_pair(shared_id, capacity)?;
        let table = Self {
            publisher: RefCell::new(publisher),
            subscriber: RefCell::new(subscriber),
            local: RefCell::new(HashMap::new()),
        };
        table.sync()?;
        Ok(table)
    }

    /// Inserts or updates a value, publishing the change to the topic.
    pub fn set(&self, key: K, value: V) -> Result<()>
    where
        K: Clone + Eq + Hash,
        V: Clone,
    {
        let mut publisher = self.publisher.borrow_mut();
        let mutation_id = publisher.allocate_mutation_id()?;
        let msg = LiveTableMessage {
            mutation_id,
            key,
            value: Some(value),
            expected_version: None,
        };
        publisher.publish(&msg)?;
        drop(publisher);
        self.sync_until_own_mutation(mutation_id)?;
        Ok(())
    }

    /// Inserts or updates a value only when the current version matches `expected_version`.
    pub fn compare_and_set(&self, key: K, expected_version: u64, value: V) -> Result<u64>
    where
        K: Clone + Eq + Hash,
        V: Clone,
    {
        self.sync()?;
        let actual = self.local.borrow().get(&key).map(|record| record.version);
        if actual.unwrap_or(0) != expected_version {
            return Err(Error::CasConflict {
                expected: expected_version,
                actual,
            });
        }

        let mut publisher = self.publisher.borrow_mut();
        let mutation_id = publisher.allocate_mutation_id()?;
        let msg = LiveTableMessage {
            mutation_id,
            key,
            value: Some(value),
            expected_version: Some(expected_version),
        };
        publisher.publish(&msg)?;
        drop(publisher);
        match self.sync_until_own_mutation(mutation_id)? {
            ApplyOutcome::Applied(version) => Ok(version),
            ApplyOutcome::Conflict { actual } => Err(Error::CasConflict {
                expected: expected_version,
                actual,
            }),
            ApplyOutcome::Deleted(version) => Ok(version),
        }
    }

    /// Deletes a value, publishing the deletion to the topic.
    pub fn delete(&self, key: K) -> Result<()>
    where
        K: Clone + Eq + Hash,
        V: Clone,
    {
        let mut publisher = self.publisher.borrow_mut();
        let mutation_id = publisher.allocate_mutation_id()?;
        let msg = LiveTableMessage {
            mutation_id,
            key,
            value: None,
            expected_version: None,
        };
        publisher.publish(&msg)?;
        drop(publisher);
        self.sync_until_own_mutation(mutation_id)?;
        Ok(())
    }

    /// Returns the value for a key from the local materialised view.
    ///
    /// This is instant — no I/O. Call [`sync`](Self::sync) first if you
    /// want the latest cross-process state.
    pub fn get(&self, key: &K) -> Result<Option<V>>
    where
        K: Eq + Hash,
        V: Clone,
    {
        Ok(self
            .local
            .borrow()
            .get(key)
            .and_then(|record| record.value.clone()))
    }

    /// Returns the record for a key, including its version.
    ///
    /// Returns the record even for tombstones (deleted keys). Check
    /// `record.value` to determine whether the entry is live.
    pub fn get_record(&self, key: &K) -> Result<Option<LiveTableRecord<V>>>
    where
        K: Eq + Hash,
        V: Clone,
    {
        Ok(self.local.borrow().get(key).cloned())
    }

    /// Returns the current version for a key, including tombstones from deletes.
    pub fn get_version(&self, key: &K) -> Result<Option<u64>>
    where
        K: Eq + Hash,
    {
        Ok(self.local.borrow().get(key).map(|entry| entry.version))
    }

    /// Returns up to `limit` records from the local materialised view.
    pub fn scan(&self, limit: usize) -> Result<Vec<(K, LiveTableRecord<V>)>>
    where
        K: Clone + Eq + Hash,
        V: Clone,
    {
        Ok(scan_entries(&self.local.borrow(), limit))
    }

    /// Drains the subscriber to pick up remote writes.
    ///
    /// Call this before `get` to synchronise with other processes
    /// attached to the same topic.
    pub fn sync(&self) -> Result<()>
    where
        K: Eq + Hash,
    {
        let mut subscriber = self.subscriber.borrow_mut();
        let mut local = self.local.borrow_mut();
        loop {
            match subscriber.read_with_writer_id() {
                Ok((msg, _writer_id)) => {
                    apply_message_to(&mut local, msg);
                }
                Err(Error::BufferEmpty) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    fn sync_until_own_mutation(&self, mutation_id: u64) -> Result<ApplyOutcome>
    where
        K: Eq + Hash,
    {
        let own_writer_id = self.publisher.borrow().writer_id();
        let mut subscriber = self.subscriber.borrow_mut();
        let mut local = self.local.borrow_mut();
        loop {
            match subscriber.read_with_writer_id() {
                Ok((msg, writer_id)) => {
                    let is_own_mutation =
                        writer_id == own_writer_id && msg.mutation_id == mutation_id;
                    let outcome = apply_message_to(&mut local, msg);
                    if is_own_mutation {
                        return Ok(outcome);
                    }
                }
                Err(Error::BufferEmpty) => return Err(Error::BufferEmpty),
                Err(e) => return Err(e),
            }
        }
    }
}

fn apply_message_to<K, V>(
    local: &mut HashMap<K, LiveTableRecord<V>>,
    msg: LiveTableMessage<K, V>,
) -> ApplyOutcome
where
    K: Eq + Hash,
{
    let actual = local
        .get(&msg.key)
        .map(|record| record.version)
        .unwrap_or(0);
    if let Some(expected) = msg.expected_version
        && actual != expected
    {
        return ApplyOutcome::Conflict {
            actual: local.get(&msg.key).map(|record| record.version),
        };
    }

    let version = actual.saturating_add(1);
    match msg.value {
        Some(value) => {
            local.insert(
                msg.key,
                LiveTableRecord {
                    value: Some(value),
                    version,
                },
            );
            ApplyOutcome::Applied(version)
        }
        None => {
            local.insert(
                msg.key,
                LiveTableRecord {
                    value: None,
                    version,
                },
            );
            ApplyOutcome::Deleted(version)
        }
    }
}

fn scan_entries<K, V>(
    local: &HashMap<K, LiveTableRecord<V>>,
    limit: usize,
) -> Vec<(K, LiveTableRecord<V>)>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    local
        .iter()
        .filter(|(_, record)| record.value.is_some())
        .map(|(key, record)| (key.clone(), record.clone()))
        .take(limit)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_message_versions_records() {
        let mut local = HashMap::new();

        let version = apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 1,
                key: "alpha".to_string(),
                value: Some(10u64),
                expected_version: None,
            },
        );
        assert_eq!(version, ApplyOutcome::Applied(1));

        let version = apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 2,
                key: "alpha".to_string(),
                value: Some(20u64),
                expected_version: Some(1),
            },
        );
        assert_eq!(version, ApplyOutcome::Applied(2));
        assert_eq!(
            local.get("alpha").map(|record| record.version),
            Some(2)
        );
        assert_eq!(
            local.get("alpha").and_then(|record| record.value),
            Some(20u64)
        );
    }

    #[test]
    fn apply_message_rejects_stale_cas() {
        let mut local = HashMap::from([(
            "alpha".to_string(),
            LiveTableRecord {
                value: Some(10u64),
                version: 2,
            },
        )]);

        let version = apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 1,
                key: "alpha".to_string(),
                value: Some(20u64),
                expected_version: Some(1),
            },
        );
        assert_eq!(version, ApplyOutcome::Conflict { actual: Some(2) });
        assert_eq!(
            local.get("alpha").and_then(|record| record.value),
            Some(10u64)
        );
    }

    #[test]
    fn apply_message_deletes_records() {
        let mut local = HashMap::from([(
            "alpha".to_string(),
            LiveTableRecord {
                value: Some(10u64),
                version: 1,
            },
        )]);

        apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 1,
                key: "alpha".to_string(),
                value: None,
                expected_version: None,
            },
        );
        assert_eq!(
            local.get("alpha").and_then(|record| record.value),
            None
        );
        assert_eq!(
            local.get("alpha").map(|record| record.version),
            Some(2)
        );
    }

    #[test]
    fn apply_message_recreates_only_with_tombstone_version() {
        let mut local = HashMap::from([(
            "alpha".to_string(),
            LiveTableRecord {
                value: None,
                version: 2,
            },
        )]);

        let stale = apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 1,
                key: "alpha".to_string(),
                value: Some(10u64),
                expected_version: Some(0),
            },
        );
        assert_eq!(stale, ApplyOutcome::Conflict { actual: Some(2) });

        let recreated = apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 2,
                key: "alpha".to_string(),
                value: Some(20u64),
                expected_version: Some(2),
            },
        );
        assert_eq!(recreated, ApplyOutcome::Applied(3));
        assert_eq!(
            local.get("alpha").and_then(|record| record.value),
            Some(20u64)
        );
    }

    #[test]
    fn scan_limit_counts_live_records_only() {
        let mut local = HashMap::from([(
            "deleted".to_string(),
            LiveTableRecord {
                value: None,
                version: 2,
            },
        )]);
        apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 1,
                key: "first".to_string(),
                value: Some(1u64),
                expected_version: None,
            },
        );
        apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 2,
                key: "second".to_string(),
                value: Some(2u64),
                expected_version: None,
            },
        );

        let live = scan_entries(&local, 2);
        assert_eq!(live.len(), 2);
    }
}
