//! Single-owner `SQLite` log store.

use core::fmt;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};

use crate::Guid;
use crate::schema::Contents;

const CREATE_SCHEMA: &str = r"
BEGIN IMMEDIATE;
CREATE TABLE logs (
    id INTEGER PRIMARY KEY,
    boot_id BLOB NOT NULL,
    timestamp INTEGER NOT NULL,
    origin TEXT NOT NULL,
    is_error INTEGER NOT NULL CHECK (is_error IN (0, 1)),
    message TEXT NOT NULL,
    job_id BLOB
);
CREATE TABLE log_origins (
    origin TEXT PRIMARY KEY
) WITHOUT ROWID;
CREATE TABLE metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
CREATE INDEX idx_logs_timestamp ON logs(timestamp);
CREATE INDEX idx_logs_origin ON logs(origin);
CREATE INDEX idx_logs_job_id ON logs(job_id) WHERE job_id IS NOT NULL;
INSERT INTO metadata(key, value) VALUES ('schema_version', '1');
INSERT INTO metadata(key, value)
VALUES ('created_at', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));
COMMIT;
";

/// One validated log record ready for storage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    /// Kernel boot at receipt.
    pub boot_id: Guid,
    /// Producer timestamp or eventd receipt time.
    pub timestamp: i64,
    /// Producer-declared origin.
    pub origin: Box<str>,
    /// Standard-error marker.
    pub is_error: bool,
    /// Verbatim UTF-8 log text.
    pub message: Box<str>,
    /// Optional execution correlation GUID.
    pub job_id: Option<Guid>,
}

/// The log thread's sole read-write connection.
pub struct LogStore {
    connection: Connection,
    path: PathBuf,
    known_origins: HashSet<Box<str>>,
    checkpoint_pages: u32,
    page_size: u64,
}

impl LogStore {
    /// Apply the next passive-checkpoint threshold at a transaction boundary.
    pub const fn set_checkpoint_pages(&mut self, pages: u32) {
        self.checkpoint_pages = pages;
    }

    /// Open the required log store, quarantining only reported corruption.
    pub fn open_recovering(
        path: impl AsRef<Path>,
        checkpoint_pages: u32,
    ) -> Result<(Self, Option<String>), LogStoreError> {
        let path = path.as_ref();
        match Self::open(path, checkpoint_pages) {
            Ok(store) => Ok((store, None)),
            Err(error) if error.is_corruption() => {
                let reason = error.to_string();
                crate::quarantine::database(path).map_err(LogStoreError::Io)?;
                Ok((Self::open(path, checkpoint_pages)?, Some(reason)))
            }
            Err(error) => Err(error),
        }
    }

    /// Open or create `logs.db` and verify schema version one.
    pub fn open(path: impl AsRef<Path>, checkpoint_pages: u32) -> Result<Self, LogStoreError> {
        let path = path.as_ref();
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
        match crate::schema::contents(&connection)? {
            Contents::Empty => connection.execute_batch(CREATE_SCHEMA)?,
            Contents::Unrecognised => return Err(LogStoreError::UnrecognisedContents),
            Contents::Store => {}
        }
        validate_schema(&connection)?;
        let known_origins = {
            let mut statement = connection.prepare("SELECT origin FROM log_origins")?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<Result<HashSet<_>, _>>()?
                .into_iter()
                .map(String::into_boxed_str)
                .collect()
        };
        let page_size = connection.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        Ok(Self {
            connection,
            path: path.to_owned(),
            known_origins,
            checkpoint_pages,
            page_size,
        })
    }

    /// Commit a non-empty adaptive batch.
    pub fn commit(&mut self, records: &[LogRecord]) -> Result<(), LogStoreError> {
        if records.is_empty() {
            return Ok(());
        }
        let mut pending = HashSet::<Box<str>>::new();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        {
            let mut insert_log = transaction.prepare_cached(
                "INSERT INTO logs \
                 (boot_id, timestamp, origin, is_error, message, job_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;
            let mut insert_origin = transaction
                .prepare_cached("INSERT OR IGNORE INTO log_origins(origin) VALUES (?1)")?;
            for record in records {
                if !self.known_origins.contains(record.origin.as_ref())
                    && pending.insert(record.origin.clone())
                {
                    insert_origin.execute([record.origin.as_ref()])?;
                }
                insert_log.execute(params![
                    &record.boot_id[..],
                    record.timestamp,
                    record.origin.as_ref(),
                    record.is_error,
                    record.message.as_ref(),
                    record.job_id.as_ref().map(|id| &id[..]),
                ])?;
            }
        }
        transaction.commit()?;
        self.known_origins.extend(pending);
        match self.checkpoint_if_needed() {
            Err(error) if error.is_capacity() => Ok(()),
            result => result,
        }
    }

    /// Replace this store after `SQLite` reports corruption during a write.
    pub fn replace_corrupt(&mut self) -> Result<(), LogStoreError> {
        let path = self.path.clone();
        let checkpoint_pages = self.checkpoint_pages;
        let placeholder = Connection::open_in_memory()?;
        let connection = std::mem::replace(&mut self.connection, placeholder);
        drop(connection);
        crate::quarantine::database(&path).map_err(LogStoreError::Io)?;
        *self = Self::open(path, checkpoint_pages)?;
        Ok(())
    }

    /// Delete at most `limit` oldest rows, optionally constrained by age.
    pub fn retain_oldest(
        &mut self,
        older_than: Option<i64>,
        limit: usize,
    ) -> Result<usize, LogStoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let limit = i64::try_from(limit).map_err(|_| LogStoreError::IntegerRange)?;
        let deleted = if let Some(cutoff) = older_than {
            transaction.execute(
                "DELETE FROM logs WHERE id IN (SELECT id FROM logs WHERE timestamp < ?1 \
                 ORDER BY timestamp, id LIMIT ?2)",
                params![cutoff, limit],
            )?
        } else {
            transaction.execute(
                "DELETE FROM logs WHERE id IN (SELECT id FROM logs \
                 ORDER BY timestamp, id LIMIT ?1)",
                [limit],
            )?
        };
        transaction.commit()?;
        self.checkpoint_if_needed()?;
        Ok(deleted)
    }

    /// Ask the sole writer connection to perform a passive checkpoint.
    pub fn passive_checkpoint(&self) -> Result<(), LogStoreError> {
        self.connection
            .execute_batch("PRAGMA wal_checkpoint(PASSIVE)")?;
        Ok(())
    }

    fn checkpoint_if_needed(&self) -> Result<(), LogStoreError> {
        let mut wal_name = self.path.as_os_str().to_owned();
        wal_name.push("-wal");
        let bytes = match std::fs::metadata(PathBuf::from(wal_name)) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(LogStoreError::Io(error)),
        };
        if bytes / self.page_size.max(1) >= u64::from(self.checkpoint_pages) {
            self.connection
                .execute_batch("PRAGMA wal_checkpoint(PASSIVE)")?;
        }
        Ok(())
    }
}

fn validate_schema(connection: &Connection) -> Result<(), LogStoreError> {
    let version: String = connection
        .query_row(
            "SELECT value FROM metadata WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => {
                LogStoreError::InvalidSchema("schema_version is missing")
            }
            other => LogStoreError::Sql(other),
        })?;
    if version != "1" {
        return Err(LogStoreError::UnknownVersion(version));
    }
    for (kind, name) in [
        ("table", "logs"),
        ("table", "log_origins"),
        ("table", "metadata"),
        ("index", "idx_logs_timestamp"),
        ("index", "idx_logs_origin"),
        ("index", "idx_logs_job_id"),
    ] {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = ?1 AND name = ?2)",
            params![kind, name],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(LogStoreError::InvalidSchema(
                "required schema object is missing",
            ));
        }
    }
    Ok(())
}

/// Log-store failure.
#[derive(Debug)]
pub enum LogStoreError {
    /// `SQLite` failure.
    Sql(rusqlite::Error),
    /// Filesystem failure.
    Io(std::io::Error),
    /// Required schema content is invalid.
    InvalidSchema(&'static str),
    /// Unsupported schema version.
    UnknownVersion(String),
    /// Schema objects without the metadata entries of a log store.
    UnrecognisedContents,
    /// Retention batch size exceeds `SQLite`'s integer range.
    IntegerRange,
}

impl LogStoreError {
    /// Whether retrying after retention may make this operation succeed.
    #[must_use]
    pub fn is_capacity(&self) -> bool {
        matches!(
            self,
            Self::Sql(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::DiskFull
        )
    }

    /// Whether `SQLite` has declared the database image corrupt, or it holds
    /// contents that are not a log store at all.
    #[must_use]
    pub const fn is_corruption(&self) -> bool {
        matches!(
            self,
            Self::Sql(rusqlite::Error::SqliteFailure(error, _))
                if matches!(
                    error.code,
                    rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase
                )
        ) || matches!(self, Self::UnrecognisedContents)
    }
}

impl fmt::Display for LogStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sql(error) => write!(formatter, "log-store SQLite error: {error}"),
            Self::Io(error) => write!(formatter, "log-store filesystem error: {error}"),
            Self::InvalidSchema(reason) => write!(formatter, "invalid log-store schema: {reason}"),
            Self::UnknownVersion(version) => {
                write!(formatter, "unsupported log-store schema {version}")
            }
            Self::UnrecognisedContents => {
                formatter.write_str("unrecognised log-store contents: no metadata entries")
            }
            Self::IntegerRange => {
                formatter.write_str("log retention batch size exceeds SQLite range")
            }
        }
    }
}

impl std::error::Error for LogStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sql(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::InvalidSchema(_)
            | Self::UnknownVersion(_)
            | Self::UnrecognisedContents
            | Self::IntegerRange => None,
        }
    }
}

impl From<rusqlite::Error> for LogStoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commits_rows_and_origin_catalogue() {
        let directory = temporary_directory();
        let path = directory.join("logs.db");
        let mut store = LogStore::open(path, 1_000).unwrap();
        let record = LogRecord {
            boot_id: [1; 16],
            timestamp: 10,
            origin: "test.origin".into(),
            is_error: false,
            message: "hello".into(),
            job_id: Some([2; 16]),
        };
        store.commit(&[record.clone(), record]).unwrap();
        let counts: (u32, u32) = store
            .connection
            .query_row(
                "SELECT (SELECT count(*) FROM logs), (SELECT count(*) FROM log_origins)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(counts, (2, 1));
        assert_eq!(store.retain_oldest(Some(11), 1).unwrap(), 1);
        assert_eq!(store.retain_oldest(Some(11), 10).unwrap(), 1);
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn the_origin_insert_runs_once_per_new_origin_per_batch() {
        let directory = temporary_directory();
        let path = directory.join("logs.db");
        let mut store = LogStore::open(path, 1_000).unwrap();
        // A BEFORE trigger fires for every row an INSERT OR IGNORE attempts,
        // including the ones the conflict then discards, so it counts
        // executions of the catalogue insert rather than rows it added.
        store
            .connection
            .execute_batch(
                "CREATE TEMP TABLE origin_inserts (origin TEXT NOT NULL);\
                 CREATE TEMP TRIGGER count_origin_inserts BEFORE INSERT ON main.log_origins \
                 BEGIN INSERT INTO origin_inserts(origin) VALUES (NEW.origin); END;",
            )
            .unwrap();
        let record = |origin: &str, timestamp| LogRecord {
            boot_id: [1; 16],
            timestamp,
            origin: origin.into(),
            is_error: false,
            message: "hello".into(),
            job_id: None,
        };
        let batch: Vec<_> = (0..5)
            .map(|timestamp| record("test.first", timestamp))
            .chain((5..8).map(|timestamp| record("test.second", timestamp)))
            .collect();
        store.commit(&batch).unwrap();
        let attempts: Vec<(String, u32)> = {
            let mut statement = store
                .connection
                .prepare(
                    "SELECT origin, count(*) FROM origin_inserts GROUP BY origin ORDER BY origin",
                )
                .unwrap();
            statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap()
        };
        assert_eq!(
            attempts,
            [("test.first".to_owned(), 1), ("test.second".to_owned(), 1)]
        );
        let logs: u32 = store
            .connection
            .query_row("SELECT count(*) FROM logs", [], |row| row.get(0))
            .unwrap();
        assert_eq!(logs, 8);
        // The counter does see an attempt the conflict discards.
        store
            .connection
            .execute(
                "INSERT OR IGNORE INTO log_origins(origin) VALUES ('test.first')",
                [],
            )
            .unwrap();
        let first_attempts: u32 = store
            .connection
            .query_row(
                "SELECT count(*) FROM origin_inserts WHERE origin = 'test.first'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(first_attempts, 2);
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// What a power cut leaves of a store whose creating transaction never
    /// reached the disk: a WAL-mode header page and no schema.
    fn schemaless(path: &Path) {
        Connection::open(path)
            .unwrap()
            .execute_batch("PRAGMA journal_mode=WAL;")
            .unwrap();
        assert_eq!(std::fs::metadata(path).unwrap().len(), 4096);
    }

    fn quarantined(directory: &Path) -> Vec<String> {
        std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".corrupt."))
            .collect()
    }

    #[test]
    fn a_store_whose_creation_was_lost_is_created_afresh() {
        let directory = temporary_directory();
        let path = directory.join("logs.db");
        schemaless(&path);
        let (mut store, recovery) = LogStore::open_recovering(&path, 1_000).unwrap();
        assert_eq!(recovery, None);
        store
            .commit(&[LogRecord {
                boot_id: [1; 16],
                timestamp: 10,
                origin: "test.origin".into(),
                is_error: false,
                message: "after the cut".into(),
                job_id: None,
            }])
            .unwrap();
        assert!(quarantined(&directory).is_empty());
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_partly_created_store_is_quarantined_but_a_missing_schema_version_fails() {
        let directory = temporary_directory();
        let path = directory.join("logs.db");
        // Prefixes of a statement-at-a-time creation: tables without the
        // metadata table, and the metadata table without its entries.
        for partial in [
            "CREATE TABLE logs (id INTEGER PRIMARY KEY);",
            "CREATE TABLE logs (id INTEGER PRIMARY KEY);\
             CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;",
        ] {
            Connection::open(&path)
                .unwrap()
                .execute_batch(partial)
                .unwrap();
            let before = quarantined(&directory).len();
            let (store, recovery) = LogStore::open_recovering(&path, 1_000).unwrap();
            assert!(recovery.is_some(), "{partial}");
            assert_eq!(quarantined(&directory).len(), before + 1, "{partial}");
            drop(store);
            std::fs::remove_file(&path).unwrap();
        }
        // A store whose metadata survives but lacks schema_version is a
        // store with a bad schema: startup fails, nothing is quarantined.
        drop(LogStore::open(&path, 1_000).unwrap());
        Connection::open(&path)
            .unwrap()
            .execute_batch("DELETE FROM metadata WHERE key = 'schema_version';")
            .unwrap();
        let before = quarantined(&directory).len();
        assert!(matches!(
            LogStore::open_recovering(&path, 1_000),
            Err(LogStoreError::InvalidSchema(_))
        ));
        assert_eq!(quarantined(&directory).len(), before);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn classifies_sqlite_full_as_capacity_failure() {
        let error = LogStoreError::Sql(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
            None,
        ));
        assert!(error.is_capacity());
    }

    fn temporary_directory() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "eventd-log-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        path
    }
}
