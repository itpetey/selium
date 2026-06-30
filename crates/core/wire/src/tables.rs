//! Transport-agnostic live table projected from a pub/sub stream.

use std::{cell::RefCell, collections::HashMap, hash::Hash};

use selium_encoding::FlatMsg;
use selium_guest_macros::schema;

use crate::{
    MessageTransport,
    error::{Error, Result},
    pubsub::{Publisher, Subscriber},
};

/// A table mutation published over a pub/sub topic.
#[derive(Debug, Clone, PartialEq)]
#[schema(
    path = concat!(env!("CARGO_MANIFEST_DIR"), "/../encoding/schemas/live_table.fbs"),
    ty = "selium.live_table.LiveTableMessage",
    binding = "selium_encoding::fbs::selium::live_table::LiveTableMessage",
    wire = LiveTableMessageWire
)]
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
/// Writes are published as `LiveTableMessage`s to the underlying topic.
/// Reads are served from a local materialised `HashMap<K, V>`. Remote
/// writes from other processes attached to the same topic are picked up
/// by calling [`sync`](Self::sync).
pub struct LiveTable<K, V, M> {
    publisher: RefCell<Publisher<LiveTableMessage<K, V>, M>>,
    subscriber: RefCell<Subscriber<LiveTableMessage<K, V>, M>>,
    local: RefCell<HashMap<K, LiveTableRecord<V>>>,
}

impl<K, V, M> LiveTable<K, V, M>
where
    K: FlatMsg + Clone + Eq + Hash,
    V: FlatMsg + Clone,
    M: MessageTransport,
{
    /// Creates a live table from an existing publisher/subscriber pair.
    pub fn new(
        publisher: Publisher<LiveTableMessage<K, V>, M>,
        subscriber: Subscriber<LiveTableMessage<K, V>, M>,
    ) -> Result<Self> {
        let table = Self {
            publisher: RefCell::new(publisher),
            subscriber: RefCell::new(subscriber),
            local: RefCell::new(HashMap::new()),
        };
        table.sync()?;
        Ok(table)
    }

    /// Inserts or updates a value, publishing the change to the topic.
    pub fn set(&self, key: K, value: V) -> Result<()> {
        let mut publisher = self.publisher.borrow_mut();
        let mutation_id = publisher.allocate_mutation_id();
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
    pub fn compare_and_set(&self, key: K, expected_version: u64, value: V) -> Result<u64> {
        self.sync()?;
        let actual = self.local.borrow().get(&key).map(|record| record.version);
        if actual.unwrap_or(0) != expected_version {
            return Err(Error::CasConflict {
                expected: expected_version,
                actual,
            });
        }

        let mut publisher = self.publisher.borrow_mut();
        let mutation_id = publisher.allocate_mutation_id();
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
    pub fn delete(&self, key: K) -> Result<()> {
        let mut publisher = self.publisher.borrow_mut();
        let mutation_id = publisher.allocate_mutation_id();
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
    pub fn get(&self, key: &K) -> Result<Option<V>>
    where
        K: Eq + Hash,
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
    {
        Ok(scan_entries(&self.local.borrow(), limit))
    }

    /// Drains the subscriber to pick up remote writes.
    pub fn sync(&self) -> Result<()> {
        let mut subscriber = self.subscriber.borrow_mut();
        let mut local = self.local.borrow_mut();
        loop {
            match subscriber.read_with_tag() {
                Ok((msg, _writer_id)) => {
                    apply_message_to(&mut local, msg);
                }
                Err(Error::BufferEmpty) => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    fn sync_until_own_mutation(&self, mutation_id: u64) -> Result<ApplyOutcome> {
        let own_writer_id = self.publisher.borrow().writer_id();
        let mut subscriber = self.subscriber.borrow_mut();
        let mut local = self.local.borrow_mut();
        loop {
            match subscriber.read_with_tag() {
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
        assert_eq!(local.get("alpha").map(|record| record.version), Some(2));
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
        assert_eq!(local.get("alpha").and_then(|record| record.value), None);
        assert_eq!(local.get("alpha").map(|record| record.version), Some(2));
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
