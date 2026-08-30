//! Single-owner `SQLite` log store.

use core::fmt;
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};

use crate::Guid;

const CREATE_SCHEMA: &str = r"
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
    /// Open or create `logs.db` and verify schema version one.
    pub fn open(path: impl AsRef<Path>, checkpoint_pages: u32) -> Result<Self, LogStoreError> {
        let path = path.as_ref();
        let existed = path.try_exists().map_err(LogStoreError::Io)?;
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
        if !existed {
            connection.execute_batch(CREATE_SCHEMA)?;
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
        self.checkpoint_if_needed()
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
        }
    }
}

impl std::error::Error for LogStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sql(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::InvalidSchema(_) | Self::UnknownVersion(_) => None,
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
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
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
