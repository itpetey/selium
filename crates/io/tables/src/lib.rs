//! Host-managed tables and materialized views.

use std::collections::BTreeMap;

use rkyv::{Archive, Deserialize, Serialize};
use selium_abi::DataValue;
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct TableRecord {
    pub value: DataValue,
    pub version: u64,
    pub updated_index: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub struct ViewDefinition {
    pub source_table: String,
    pub kind: ViewKind,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum ViewKind {
    RowCount,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize, Default)]
#[rkyv(bytecheck())]
pub struct TableStore {
    tables: BTreeMap<String, BTreeMap<String, TableRecord>>,
    views: BTreeMap<String, ViewDefinition>,
    view_state: BTreeMap<String, DataValue>,
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum TableCommand {
    Put {
        table: String,
        key: String,
        value: DataValue,
    },
    Delete {
        table: String,
        key: String,
    },
    Cas {
        table: String,
        key: String,
        expected_version: u64,
        value: DataValue,
    },
    CreateView {
        name: String,
        source_table: String,
        kind: ViewKind,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Archive, Serialize, Deserialize)]
#[rkyv(bytecheck())]
pub enum TableApplyResult {
    Updated {
        table: String,
        key: String,
        version: u64,
    },
    Deleted {
        table: String,
        key: String,
    },
    ViewCreated {
        name: String,
    },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum TableError {
    #[error(
        "compare-and-set failed for `{table}/{key}` expected version {expected} got {actual:?}"
    )]
    CasConflict {
        table: String,
        key: String,
        expected: u64,
        actual: Option<u64>,
    },
    #[error("view `{0}` already exists")]
    ViewExists(String),
}

impl TableStore {
    pub fn apply(
        &mut self,
        command: TableCommand,
        commit_index: u64,
    ) -> Result<TableApplyResult, TableError> {
        match command {
            TableCommand::Put { table, key, value } => {
                let bucket = self.tables.entry(table.clone()).or_default();
                let next_version = bucket
                    .get(&key)
                    .map(|record| record.version + 1)
                    .unwrap_or(1);
                bucket.insert(
                    key.clone(),
                    TableRecord {
                        value,
                        version: next_version,
                        updated_index: commit_index,
                    },
                );
                self.refresh_views_for_table(&table);
                Ok(TableApplyResult::Updated {
                    table,
                    key,
                    version: next_version,
                })
            }
            TableCommand::Delete { table, key } => {
                if let Some(bucket) = self.tables.get_mut(&table) {
                    bucket.remove(&key);
                }
                self.refresh_views_for_table(&table);
                Ok(TableApplyResult::Deleted { table, key })
            }
            TableCommand::Cas {
                table,
                key,
                expected_version,
                value,
            } => {
                let bucket = self.tables.entry(table.clone()).or_default();
                let current_version = bucket.get(&key).map(|record| record.version);
                if current_version.unwrap_or(0) != expected_version {
                    return Err(TableError::CasConflict {
                        table,
                        key,
                        expected: expected_version,
                        actual: current_version,
                    });
                }

                let next_version = expected_version + 1;
                bucket.insert(
                    key.clone(),
                    TableRecord {
                        value,
                        version: next_version,
                        updated_index: commit_index,
                    },
                );
                self.refresh_views_for_table(&table);
                Ok(TableApplyResult::Updated {
                    table,
                    key,
                    version: next_version,
                })
            }
            TableCommand::CreateView {
                name,
                source_table,
                kind,
            } => {
                if self.views.contains_key(&name) {
                    return Err(TableError::ViewExists(name));
                }
                self.views.insert(
                    name.clone(),
                    ViewDefinition {
                        source_table: source_table.clone(),
                        kind,
                    },
                );
                self.refresh_views_for_table(&source_table);
                Ok(TableApplyResult::ViewCreated { name })
            }
        }
    }

    pub fn get(&self, table: &str, key: &str) -> Option<&TableRecord> {
        self.tables.get(table)?.get(key)
    }

    pub fn scan(&self, table: &str, limit: usize) -> Vec<(String, TableRecord)> {
        self.tables
            .get(table)
            .map(|bucket| {
                bucket
                    .iter()
                    .take(limit)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub fn view(&self, name: &str) -> Option<&DataValue> {
        self.view_state.get(name)
    }

    pub fn tables(&self) -> &BTreeMap<String, BTreeMap<String, TableRecord>> {
        &self.tables
    }

    fn refresh_views_for_table(&mut self, table: &str) {
        let affected = self
            .views
            .iter()
            .filter(|(_, view)| view.source_table == table)
            .map(|(name, view)| (name.clone(), view.kind.clone()))
            .collect::<Vec<_>>();

        for (name, kind) in affected {
            let value = match kind {
                ViewKind::RowCount => {
                    let rows = self.tables.get(table).map(|rows| rows.len()).unwrap_or(0);
                    DataValue::Map(BTreeMap::from([(
                        "rows".to_string(),
                        DataValue::from(rows),
                    )]))
                }
            };
            self.view_state.insert(name, value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_and_cas_advances_versions() {
        let mut store = TableStore::default();
        let result = store
            .apply(
                TableCommand::Put {
                    table: "apps".to_string(),
                    key: "echo".to_string(),
                    value: DataValue::Map(BTreeMap::from([(
                        "replicas".to_string(),
                        DataValue::from(1u64),
                    )])),
                },
                1,
            )
            .expect("put");
        assert!(matches!(
            result,
            TableApplyResult::Updated { version: 1, .. }
        ));

        let result = store
            .apply(
                TableCommand::Cas {
                    table: "apps".to_string(),
                    key: "echo".to_string(),
                    expected_version: 1,
                    value: DataValue::Map(BTreeMap::from([(
                        "replicas".to_string(),
                        DataValue::from(2u64),
                    )])),
                },
                2,
            )
            .expect("cas");
        assert!(matches!(
            result,
            TableApplyResult::Updated { version: 2, .. }
        ));

        let record = store.get("apps", "echo").expect("record");
        assert_eq!(record.version, 2);
    }

    #[test]
    fn create_row_count_view_updates_on_writes() {
        let mut store = TableStore::default();
        store
            .apply(
                TableCommand::CreateView {
                    name: "apps.count".to_string(),
                    source_table: "apps".to_string(),
                    kind: ViewKind::RowCount,
                },
                1,
            )
            .expect("create view");

        store
            .apply(
                TableCommand::Put {
                    table: "apps".to_string(),
                    key: "a".to_string(),
                    value: DataValue::Map(BTreeMap::from([(
                        "ok".to_string(),
                        DataValue::from(true),
                    )])),
                },
                2,
            )
            .expect("put");
        store
            .apply(
                TableCommand::Put {
                    table: "apps".to_string(),
                    key: "b".to_string(),
                    value: DataValue::Map(BTreeMap::from([(
                        "ok".to_string(),
                        DataValue::from(true),
                    )])),
                },
                3,
            )
            .expect("put");

        let view = store.view("apps.count").expect("view");
        assert_eq!(view.get("rows").and_then(DataValue::as_u64), Some(2));
    }
}
