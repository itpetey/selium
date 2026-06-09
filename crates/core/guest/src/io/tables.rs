use std::{
    cell::RefCell,
    collections::HashMap,
    hash::Hash,
};

use rkyv::{
    api::high::{HighDeserializer, HighValidator},
    rancor::Error as RancorError,
};
use selium_abi::RkyvEncode;

use crate::io::{
    error::{Error, Result},
    pubsub::{self, Publisher, Subscriber},
};

/// A table mutation published over a pub/sub topic.
///
/// Every `set` publishes one of these messages. All processes attached to
/// the same topic replay the stream to build their local materialised view.
#[derive(Debug, Clone, PartialEq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
#[rkyv(bytecheck())]
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

/// A materialised live table record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveTableRecord<V> {
    /// The entry value.
    pub value: V,
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
    local: RefCell<HashMap<K, LiveTableEntry<V>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LiveTableEntry<V> {
    value: Option<V>,
    version: u64,
}

impl<K, V> LiveTable<K, V>
where
    K: rkyv::Archive + Clone + Eq + Hash,
    V: rkyv::Archive + Clone,
    for<'a> K::Archived: rkyv::Deserialize<K, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
    for<'a> V::Archived: rkyv::Deserialize<V, HighDeserializer<RancorError>>
        + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
    for<'a> <LiveTableMessage<K, V> as rkyv::Archive>::Archived: rkyv::Deserialize<
            LiveTableMessage<K, V>,
            HighDeserializer<RancorError>,
        > + rkyv::bytecheck::CheckBytes<HighValidator<'a, RancorError>>,
    LiveTableMessage<K, V>: RkyvEncode,
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
    pub fn get_record(&self, key: &K) -> Result<Option<LiveTableRecord<V>>>
    where
        K: Eq + Hash,
        V: Clone,
    {
        Ok(self.local.borrow().get(key).and_then(|entry| {
            entry.value.clone().map(|value| LiveTableRecord {
                value,
                version: entry.version,
            })
        }))
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
    local: &mut HashMap<K, LiveTableEntry<V>>,
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
                LiveTableEntry {
                    value: Some(value),
                    version,
                },
            );
            ApplyOutcome::Applied(version)
        }
        None => {
            local.insert(
                msg.key,
                LiveTableEntry {
                    value: None,
                    version,
                },
            );
            ApplyOutcome::Deleted(version)
        }
    }
}

fn scan_entries<K, V>(
    local: &HashMap<K, LiveTableEntry<V>>,
    limit: usize,
) -> Vec<(K, LiveTableRecord<V>)>
where
    K: Clone + Eq + Hash,
    V: Clone,
{
    local
        .iter()
        .filter_map(|(key, entry)| {
            entry.value.clone().map(|value| {
                (
                    key.clone(),
                    LiveTableRecord {
                        value,
                        version: entry.version,
                    },
                )
            })
        })
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
                key: "alpha",
                value: Some(10),
                expected_version: None,
            },
        );
        assert_eq!(version, ApplyOutcome::Applied(1));

        let version = apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 2,
                key: "alpha",
                value: Some(20),
                expected_version: Some(1),
            },
        );
        assert_eq!(version, ApplyOutcome::Applied(2));
        assert_eq!(local.get("alpha").map(|record| record.version), Some(2));
        assert_eq!(local.get("alpha").and_then(|record| record.value), Some(20));
    }

    #[test]
    fn apply_message_rejects_stale_cas() {
        let mut local = HashMap::from([(
            "alpha",
            LiveTableEntry {
                value: Some(10),
                version: 2,
            },
        )]);

        let version = apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 1,
                key: "alpha",
                value: Some(20),
                expected_version: Some(1),
            },
        );
        assert_eq!(version, ApplyOutcome::Conflict { actual: Some(2) });
        assert_eq!(local.get("alpha").and_then(|record| record.value), Some(10));
    }

    #[test]
    fn apply_message_deletes_records() {
        let mut local = HashMap::from([(
            "alpha",
            LiveTableEntry {
                value: Some(10),
                version: 1,
            },
        )]);

        apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 1,
                key: "alpha",
                value: None,
                expected_version: None,
            },
        );
        assert_eq!(local.get("alpha").and_then(|record| record.value), None);
        assert_eq!(local.get("alpha").map(|record| record.version), Some(2));
    }

    #[test]
    fn apply_message_recreates_only_with_tombstone_version() {
        let mut local = HashMap::from([(
            "alpha",
            LiveTableEntry {
                value: None,
                version: 2,
            },
        )]);

        let stale = apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 1,
                key: "alpha",
                value: Some(10),
                expected_version: Some(0),
            },
        );
        assert_eq!(stale, ApplyOutcome::Conflict { actual: Some(2) });

        let recreated = apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 2,
                key: "alpha",
                value: Some(20),
                expected_version: Some(2),
            },
        );
        assert_eq!(recreated, ApplyOutcome::Applied(3));
        assert_eq!(local.get("alpha").and_then(|record| record.value), Some(20));
    }

    #[test]
    fn scan_limit_counts_live_records_only() {
        let mut local = HashMap::from([(
            "deleted",
            LiveTableEntry {
                value: None,
                version: 2,
            },
        )]);
        apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 1,
                key: "first",
                value: Some(1),
                expected_version: None,
            },
        );
        apply_message_to(
            &mut local,
            LiveTableMessage {
                mutation_id: 2,
                key: "second",
                value: Some(2),
                expected_version: None,
            },
        );

        let live = scan_entries(&local, 2);
        assert_eq!(live.len(), 2);
    }
}
