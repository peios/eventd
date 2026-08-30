//! Reconstructible daemon-wide metadata database.

use core::fmt;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};

use crate::Guid;

const CREATE_SCHEMA: &str = r"
CREATE TABLE index_counters (
    field_path TEXT PRIMARY KEY,
    query_count INTEGER NOT NULL,
    window_start INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE desired_indexes (
    field_path TEXT PRIMARY KEY,
    priority INTEGER NOT NULL,
    is_expression INTEGER NOT NULL CHECK (is_expression IN (0, 1))
) WITHOUT ROWID;
CREATE TABLE sequence_checkpoints (
    boot_id BLOB NOT NULL,
    cpu_id INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY (boot_id, cpu_id)
) WITHOUT ROWID;
CREATE TABLE meta (
    key TEXT PRIMARY KEY,
    value BLOB NOT NULL
) WITHOUT ROWID;
INSERT INTO meta(key, value) VALUES ('schema_version', CAST('1' AS BLOB));
INSERT INTO meta(key, value)
VALUES ('created_at', CAST(strftime('%Y-%m-%dT%H:%M:%SZ', 'now') AS BLOB));
";

/// The policy thread's sole read-write metadata connection.
pub struct MetaStore {
    connection: Connection,
    path: PathBuf,
    checkpoint_pages: u32,
    page_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One persisted adaptive-index frequency counter.
pub struct IndexCounter {
    /// Query-language field or flattened payload path.
    pub field_path: String,
    /// Queries referencing the field in the current window.
    pub query_count: u64,
    /// Current window start in realtime nanoseconds.
    pub window_start: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// One member of the global desired secondary-index set.
pub struct DesiredIndex {
    /// Query-language field or flattened payload path.
    pub field_path: String,
    /// Lower values are converged first and shed last.
    pub priority: u64,
    /// Whether this is a payload expression rather than a header column.
    pub is_expression: bool,
}

impl MetaStore {
    /// Apply the next passive-checkpoint threshold at a policy boundary.
    pub const fn set_checkpoint_pages(&mut self, pages: u32) {
        self.checkpoint_pages = pages;
    }

    /// Open the reconstructible database, replacing malformed state with defaults.
    pub fn open(path: impl AsRef<Path>, checkpoint_pages: u32) -> Result<Self, MetaStoreError> {
        let path = path.as_ref();
        let existed = path.try_exists().map_err(MetaStoreError::Io)?;
        let mut connection = open_connection(path)?;
        if !existed {
            connection.execute_batch(CREATE_SCHEMA)?;
        } else if validate_schema(&connection).is_err() {
            drop(connection);
            remove_database(path)?;
            connection = open_connection(path)?;
            connection.execute_batch(CREATE_SCHEMA)?;
        }
        validate_schema(&connection)?;
        let page_size = connection.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        Ok(Self {
            connection,
            path: path.to_owned(),
            checkpoint_pages,
            page_size,
        })
    }

    /// Persist diagnostic sequence coverage after all event writers have flushed.
    pub fn write_sequence_checkpoints(
        &mut self,
        boot_id: &Guid,
        sequences: &[(u16, u64)],
        updated_at: u64,
    ) -> Result<(), MetaStoreError> {
        let updated_at = sqlite_integer(updated_at)?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        {
            let mut upsert = transaction.prepare_cached(
                "INSERT INTO sequence_checkpoints \
                 (boot_id, cpu_id, sequence, updated_at) VALUES (?1, ?2, ?3, ?4) \
                 ON CONFLICT(boot_id, cpu_id) DO UPDATE SET \
                 sequence = excluded.sequence, updated_at = excluded.updated_at",
            )?;
            for &(cpu_id, sequence) in sequences {
                upsert.execute(params![
                    &boot_id[..],
                    i64::from(cpu_id),
                    sqlite_integer(sequence)?,
                    updated_at,
                ])?;
            }
        }
        transaction.commit()?;
        self.checkpoint_if_needed()
    }

    /// Load reconstructible adaptive-index state at startup.
    pub fn load_index_state(
        &self,
    ) -> Result<(Vec<IndexCounter>, Vec<DesiredIndex>), MetaStoreError> {
        let mut counters_statement = self
            .connection
            .prepare("SELECT field_path, query_count, window_start FROM index_counters")?;
        let counters = counters_statement
            .query_map([], |row| {
                Ok(IndexCounter {
                    field_path: row.get(0)?,
                    query_count: row.get(1)?,
                    window_start: row.get(2)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut desired_statement = self.connection.prepare(
            "SELECT field_path, priority, is_expression FROM desired_indexes \
             ORDER BY priority, field_path",
        )?;
        let desired = desired_statement
            .query_map([], |row| {
                Ok(DesiredIndex {
                    field_path: row.get(0)?,
                    priority: row.get(1)?,
                    is_expression: row.get::<_, i64>(2)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok((counters, desired))
    }

    /// Atomically replace the current adaptive-index counters and desired set.
    pub fn write_index_state(
        &mut self,
        counters: &[IndexCounter],
        desired: &[DesiredIndex],
    ) -> Result<(), MetaStoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute("DELETE FROM index_counters", [])?;
        transaction.execute("DELETE FROM desired_indexes", [])?;
        {
            let mut insert_counter = transaction.prepare_cached(
                "INSERT INTO index_counters(field_path, query_count, window_start) \
                 VALUES (?1, ?2, ?3)",
            )?;
            for counter in counters {
                insert_counter.execute(params![
                    counter.field_path,
                    sqlite_integer(counter.query_count)?,
                    sqlite_integer(counter.window_start)?,
                ])?;
            }
            let mut insert_desired = transaction.prepare_cached(
                "INSERT INTO desired_indexes(field_path, priority, is_expression) \
                 VALUES (?1, ?2, ?3)",
            )?;
            for index in desired {
                insert_desired.execute(params![
                    index.field_path,
                    sqlite_integer(index.priority)?,
                    i64::from(index.is_expression),
                ])?;
            }
        }
        transaction.commit()?;
        self.checkpoint_if_needed()
    }

    fn checkpoint_if_needed(&self) -> Result<(), MetaStoreError> {
        let bytes = sidecar_size(&self.path, "-wal")?;
        if bytes / self.page_size.max(1) >= u64::from(self.checkpoint_pages) {
            self.connection
                .execute_batch("PRAGMA wal_checkpoint(PASSIVE)")?;
        }
        Ok(())
    }
}

fn open_connection(path: &Path) -> Result<Connection, MetaStoreError> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(std::time::Duration::ZERO)?;
    connection.execute_batch(
        "PRAGMA journal_mode=WAL;\
         PRAGMA synchronous=NORMAL;\
         PRAGMA wal_autocheckpoint=0;\
         PRAGMA temp_store=MEMORY;",
    )?;
    Ok(connection)
}

fn validate_schema(connection: &Connection) -> Result<(), MetaStoreError> {
    let version: Vec<u8> = connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => {
                MetaStoreError::InvalidSchema("schema_version is missing")
            }
            other => MetaStoreError::Sql(other),
        })?;
    if version != b"1" {
        return Err(MetaStoreError::UnknownVersion(version));
    }
    let created: Vec<u8> = connection
        .query_row(
            "SELECT value FROM meta WHERE key = 'created_at'",
            [],
            |row| row.get(0),
        )
        .map_err(|_| MetaStoreError::InvalidSchema("created_at is missing"))?;
    if created.is_empty() || std::str::from_utf8(&created).is_err() {
        return Err(MetaStoreError::InvalidSchema("created_at is malformed"));
    }
    for name in [
        "index_counters",
        "desired_indexes",
        "sequence_checkpoints",
        "meta",
    ] {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?1)",
            [name],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(MetaStoreError::InvalidSchema(
                "required metadata table is missing",
            ));
        }
    }
    Ok(())
}

fn remove_database(path: &Path) -> Result<(), MetaStoreError> {
    remove_if_present(path)?;
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        remove_if_present(Path::new(&sidecar))?;
    }
    Ok(())
}

fn remove_if_present(path: &Path) -> Result<(), MetaStoreError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(MetaStoreError::Io(error)),
    }
}

fn sidecar_size(path: &Path, suffix: &str) -> Result<u64, MetaStoreError> {
    let mut sidecar = path.as_os_str().to_owned();
    sidecar.push(suffix);
    match std::fs::metadata(PathBuf::from(sidecar)) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(MetaStoreError::Io(error)),
    }
}

fn sqlite_integer(value: u64) -> Result<i64, MetaStoreError> {
    i64::try_from(value).map_err(|_| MetaStoreError::IntegerRange)
}

/// Metadata-store failure.
#[derive(Debug)]
pub enum MetaStoreError {
    /// `SQLite` failure.
    Sql(rusqlite::Error),
    /// Filesystem failure.
    Io(std::io::Error),
    /// Required schema state is malformed.
    InvalidSchema(&'static str),
    /// Unsupported schema version bytes.
    UnknownVersion(Vec<u8>),
    /// A u64 cannot be represented by `SQLite`'s signed integer.
    IntegerRange,
}

impl fmt::Display for MetaStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sql(error) => write!(formatter, "metadata SQLite error: {error}"),
            Self::Io(error) => write!(formatter, "metadata filesystem error: {error}"),
            Self::InvalidSchema(reason) => write!(formatter, "invalid metadata schema: {reason}"),
            Self::UnknownVersion(version) => write!(
                formatter,
                "unsupported metadata schema {}",
                String::from_utf8_lossy(version)
            ),
            Self::IntegerRange => formatter.write_str("metadata integer exceeds SQLite range"),
        }
    }
}

impl std::error::Error for MetaStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sql(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::InvalidSchema(_) | Self::UnknownVersion(_) | Self::IntegerRange => None,
        }
    }
}

impl From<rusqlite::Error> for MetaStoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recreates_malformed_metadata_and_writes_checkpoints() {
        let directory = std::env::temp_dir().join(format!(
            "eventd-meta-test-{}-{}",
            std::process::id(),
            std::thread::current().name().unwrap_or("unnamed")
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("eventd-meta.db");
        Connection::open(&path)
            .unwrap()
            .execute_batch("CREATE TABLE broken(value INTEGER);")
            .unwrap();
        let mut store = MetaStore::open(&path, 1_000).unwrap();
        store
            .write_sequence_checkpoints(&[1; 16], &[(2, 9)], 10)
            .unwrap();
        let row: (i64, i64) = store
            .connection
            .query_row(
                "SELECT sequence, updated_at FROM sequence_checkpoints WHERE cpu_id = 2",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(row, (9, 10));
        let counters = [IndexCounter {
            field_path: "event_type".into(),
            query_count: 12,
            window_start: 34,
        }];
        let desired = [DesiredIndex {
            field_path: "event_type".into(),
            priority: 0,
            is_expression: false,
        }];
        store.write_index_state(&counters, &desired).unwrap();
        assert_eq!(
            store.load_index_state().unwrap(),
            (counters.into(), desired.into())
        );
        drop(store);
        let _ = std::fs::remove_dir_all(directory);
    }
}
