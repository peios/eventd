//! Single-owner metric store with bounded LRU series resolution.

use core::fmt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use hashlink::LinkedHashMap;
use rusqlite::{Connection, OpenFlags, Transaction, TransactionBehavior, params};

use crate::Guid;

const CREATE_SCHEMA: &str = r"
CREATE TABLE series (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    labels TEXT NOT NULL,
    type INTEGER NOT NULL CHECK (type IN (0, 1, 2)),
    label_hash INTEGER NOT NULL,
    boundaries_hash INTEGER,
    boundaries BLOB,
    UNIQUE(name, labels, boundaries_hash)
);
CREATE TABLE samples (
    id INTEGER PRIMARY KEY,
    series_id INTEGER NOT NULL REFERENCES series(id),
    boot_id BLOB NOT NULL,
    timestamp INTEGER NOT NULL,
    value REAL NOT NULL,
    histogram_data BLOB
);
CREATE TABLE metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
CREATE INDEX idx_samples_series_timestamp ON samples(series_id, timestamp, id);
CREATE INDEX idx_series_name ON series(name);
CREATE INDEX idx_series_label_hash ON series(label_hash);
INSERT INTO metadata(key, value) VALUES ('schema_version', '1');
INSERT INTO metadata(key, value)
VALUES ('created_at', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));
";

/// Metric series type stored as a stable integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum MetricType {
    /// Monotonic cumulative value.
    Counter = 0,
    /// Value that may move in either direction.
    Gauge = 1,
    /// Cumulative bucket distribution.
    Histogram = 2,
}

/// A validated histogram sample.
#[derive(Debug, Clone, PartialEq)]
pub struct Histogram {
    /// Strictly increasing finite bucket boundaries.
    pub boundaries: Box<[f64]>,
    /// Non-decreasing cumulative bucket counts.
    pub counts: Box<[u64]>,
    /// Total observation count.
    pub total_count: u64,
    /// Finite sum of observations.
    pub sum: f64,
}

/// Validated metric value.
#[derive(Debug, Clone, PartialEq)]
pub enum MetricValue {
    /// Counter or gauge binary64 value.
    Number(f64),
    /// Histogram sample and identity boundaries.
    Histogram(Histogram),
}

/// One validated sample ready for series resolution.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricRecord {
    /// Kernel boot at receipt.
    pub boot_id: Guid,
    /// Producer timestamp or eventd receipt time.
    pub timestamp: i64,
    /// Metric name.
    pub name: Box<str>,
    /// Canonical sorted `key=value` label string.
    pub labels: Box<str>,
    /// Declared series type.
    pub metric_type: MetricType,
    /// Measurement.
    pub value: MetricValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SeriesKey {
    name: Box<str>,
    labels: Box<str>,
    boundaries: Option<Box<[u8]>>,
}

#[derive(Debug, Clone, Copy)]
struct SeriesInfo {
    id: i64,
    metric_type: MetricType,
}

/// The metric thread's sole read-write connection and resolution cache.
pub struct MetricStore {
    connection: Connection,
    path: PathBuf,
    cache: LinkedHashMap<SeriesKey, SeriesInfo>,
    cache_capacity: usize,
    checkpoint_pages: u32,
    page_size: u64,
}

impl MetricStore {
    /// Current number of entries in the bounded series-resolution cache.
    #[must_use]
    pub fn cache_len(&self) -> usize {
        self.cache.len()
    }

    /// Open the required metric store, quarantining only reported corruption.
    pub fn open_recovering(
        path: impl AsRef<Path>,
        checkpoint_pages: u32,
        cache_capacity: usize,
    ) -> Result<(Self, Option<String>), MetricStoreError> {
        let path = path.as_ref();
        match Self::open(path, checkpoint_pages, cache_capacity) {
            Ok(store) => Ok((store, None)),
            Err(error) if error.is_corruption() => {
                let reason = error.to_string();
                crate::quarantine::database(path).map_err(MetricStoreError::Io)?;
                Ok((
                    Self::open(path, checkpoint_pages, cache_capacity)?,
                    Some(reason),
                ))
            }
            Err(error) => Err(error),
        }
    }

    /// Open or create `metrics.db` and start with an empty series cache.
    pub fn open(
        path: impl AsRef<Path>,
        checkpoint_pages: u32,
        cache_capacity: usize,
    ) -> Result<Self, MetricStoreError> {
        let path = path.as_ref();
        let existed = path.try_exists().map_err(MetricStoreError::Io)?;
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
             PRAGMA foreign_keys=ON;\
             PRAGMA temp_store=MEMORY;",
        )?;
        if !existed {
            connection.execute_batch(CREATE_SCHEMA)?;
        }
        validate_schema(&connection)?;
        let page_size = connection.query_row("PRAGMA page_size", [], |row| row.get(0))?;
        Ok(Self {
            connection,
            path: path.to_owned(),
            cache: LinkedHashMap::new(),
            cache_capacity,
            checkpoint_pages,
            page_size,
        })
    }

    /// Resolve series and commit one adaptive batch.
    pub fn commit(
        &mut self,
        records: &[MetricRecord],
    ) -> Result<MetricCommitStats, MetricStoreError> {
        if records.is_empty() {
            return Ok(MetricCommitStats::default());
        }
        let mut accepted = 0;
        let mut pending = HashMap::<SeriesKey, SeriesInfo>::new();
        let mut cache_after_commit = Vec::new();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        {
            let mut insert_sample = transaction.prepare_cached(
                "INSERT INTO samples \
                 (series_id, boot_id, timestamp, value, histogram_data) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for record in records {
                let key = series_key(record);
                let info = if let Some(info) = pending.get(&key).copied() {
                    info
                } else if let Some(info) = self.cache.to_back(&key).copied() {
                    info
                } else {
                    let info = resolve_or_insert(&transaction, record, &key)?;
                    pending.insert(key.clone(), info);
                    cache_after_commit.push((key, info));
                    info
                };
                if info.metric_type != record.metric_type {
                    continue;
                }
                let (number, histogram_data) = match &record.value {
                    MetricValue::Number(value) => (*value, None),
                    MetricValue::Histogram(histogram) => (0.0, Some(encode_histogram(histogram))),
                };
                insert_sample.execute(params![
                    info.id,
                    &record.boot_id[..],
                    record.timestamp,
                    number,
                    histogram_data,
                ])?;
                accepted += 1;
            }
        }
        transaction.commit()?;
        for (key, info) in cache_after_commit {
            self.cache.insert(key, info);
            while self.cache.len() > self.cache_capacity {
                self.cache.pop_front();
            }
        }
        if let Err(error) = self.checkpoint_if_needed()
            && !error.is_capacity()
        {
            return Err(error);
        }
        Ok(MetricCommitStats {
            accepted,
            type_mismatches: records.len() - accepted,
        })
    }

    /// Replace this store after `SQLite` reports corruption during a write.
    pub fn replace_corrupt(&mut self) -> Result<(), MetricStoreError> {
        let path = self.path.clone();
        let checkpoint_pages = self.checkpoint_pages;
        let cache_capacity = self.cache_capacity;
        let placeholder = Connection::open_in_memory()?;
        let connection = std::mem::replace(&mut self.connection, placeholder);
        drop(connection);
        crate::quarantine::database(&path).map_err(MetricStoreError::Io)?;
        *self = Self::open(path, checkpoint_pages, cache_capacity)?;
        Ok(())
    }

    /// Delete at most `limit` oldest samples and prune now-empty series.
    pub fn retain_oldest(
        &mut self,
        older_than: Option<i64>,
        limit: usize,
    ) -> Result<usize, MetricStoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        let limit = i64::try_from(limit).map_err(|_| MetricStoreError::IntegerRange)?;
        let deleted = if let Some(cutoff) = older_than {
            transaction.execute(
                "DELETE FROM samples WHERE id IN (SELECT id FROM samples WHERE timestamp < ?1 \
                 ORDER BY timestamp, id LIMIT ?2)",
                params![cutoff, limit],
            )?
        } else {
            transaction.execute(
                "DELETE FROM samples WHERE id IN (SELECT id FROM samples \
                 ORDER BY timestamp, id LIMIT ?1)",
                [limit],
            )?
        };
        transaction.execute(
            "DELETE FROM series WHERE id IN (SELECT series.id FROM series \
             WHERE NOT EXISTS (SELECT 1 FROM samples WHERE samples.series_id = series.id) \
             LIMIT ?1)",
            [limit],
        )?;
        transaction.commit()?;
        if deleted != 0 {
            self.cache.clear();
        }
        self.checkpoint_if_needed()?;
        Ok(deleted)
    }

    /// Ask the sole writer connection to perform a passive checkpoint.
    pub fn passive_checkpoint(&self) -> Result<(), MetricStoreError> {
        self.connection
            .execute_batch("PRAGMA wal_checkpoint(PASSIVE)")?;
        Ok(())
    }

    fn checkpoint_if_needed(&self) -> Result<(), MetricStoreError> {
        let mut wal_name = self.path.as_os_str().to_owned();
        wal_name.push("-wal");
        let bytes = match std::fs::metadata(PathBuf::from(wal_name)) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(MetricStoreError::Io(error)),
        };
        if bytes / self.page_size.max(1) >= u64::from(self.checkpoint_pages) {
            self.connection
                .execute_batch("PRAGMA wal_checkpoint(PASSIVE)")?;
        }
        Ok(())
    }
}

fn resolve_or_insert(
    transaction: &Transaction<'_>,
    record: &MetricRecord,
    key: &SeriesKey,
) -> Result<SeriesInfo, MetricStoreError> {
    let label_hash = hash_for_sql(record.labels.as_bytes());
    let boundary_hash = key.boundaries.as_deref().map(hash_for_sql);
    let mut statement = transaction.prepare_cached(
        "SELECT id, labels, type, boundaries FROM series \
         WHERE name = ?1 AND label_hash = ?2 \
         AND ((?3 IS NULL AND boundaries_hash IS NULL) OR boundaries_hash = ?3)",
    )?;
    let mut rows = statement.query(params![record.name.as_ref(), label_hash, boundary_hash])?;
    while let Some(row) = rows.next()? {
        let labels: String = row.get(1)?;
        let boundaries: Option<Vec<u8>> = row.get(3)?;
        if labels == record.labels.as_ref() && boundaries.as_deref() == key.boundaries.as_deref() {
            return Ok(SeriesInfo {
                id: row.get(0)?,
                metric_type: metric_type_from_sql(row.get(2)?)?,
            });
        }
    }
    drop(rows);
    drop(statement);
    transaction.execute(
        "INSERT INTO series \
         (name, labels, type, label_hash, boundaries_hash, boundaries) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        params![
            record.name.as_ref(),
            record.labels.as_ref(),
            i64::from(record.metric_type as u8),
            label_hash,
            boundary_hash,
            key.boundaries.as_deref(),
        ],
    )?;
    Ok(SeriesInfo {
        id: transaction.last_insert_rowid(),
        metric_type: record.metric_type,
    })
}

fn series_key(record: &MetricRecord) -> SeriesKey {
    let boundaries = match &record.value {
        MetricValue::Number(_) => None,
        MetricValue::Histogram(histogram) => Some(boundary_blob(&histogram.boundaries)),
    };
    SeriesKey {
        name: record.name.clone(),
        labels: record.labels.clone(),
        boundaries,
    }
}

fn boundary_blob(boundaries: &[f64]) -> Box<[u8]> {
    let mut output = Vec::with_capacity(4 + boundaries.len() * 8);
    output.extend_from_slice(
        &u32::try_from(boundaries.len())
            .expect("validated datagram bounds boundary count to u32")
            .to_le_bytes(),
    );
    for boundary in boundaries {
        output.extend_from_slice(&boundary.to_le_bytes());
    }
    output.into_boxed_slice()
}

fn hash_for_sql(bytes: &[u8]) -> i64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    i64::try_from(hash & 0x7fff_ffff_ffff_ffff).expect("high bit cleared")
}

fn encode_histogram(histogram: &Histogram) -> Vec<u8> {
    let mut output =
        Vec::with_capacity(64 + histogram.boundaries.len() * 9 + histogram.counts.len() * 9);
    output.push(0x84); // fixmap, canonical key order follows encoded key length
    pack_str(&mut output, "sum");
    pack_float(&mut output, histogram.sum);
    pack_str(&mut output, "counts");
    pack_array(&mut output, histogram.counts.len());
    for count in &histogram.counts {
        pack_uint(&mut output, *count);
    }
    pack_str(&mut output, "boundaries");
    pack_array(&mut output, histogram.boundaries.len());
    for boundary in &histogram.boundaries {
        pack_float(&mut output, *boundary);
    }
    pack_str(&mut output, "total_count");
    pack_uint(&mut output, histogram.total_count);
    output
}

fn pack_str(output: &mut Vec<u8>, value: &str) {
    output.push(0xa0 | u8::try_from(value.len()).expect("histogram keys are fixstr"));
    output.extend_from_slice(value.as_bytes());
}

fn pack_array(output: &mut Vec<u8>, length: usize) {
    if length <= 15 {
        output.push(0x90 | u8::try_from(length).expect("fixarray length"));
    } else if let Ok(length) = u16::try_from(length) {
        output.push(0xdc);
        output.extend_from_slice(&length.to_be_bytes());
    } else {
        output.push(0xdd);
        output.extend_from_slice(
            &u32::try_from(length)
                .expect("validated datagram bounds array to u32")
                .to_be_bytes(),
        );
    }
}

fn pack_float(output: &mut Vec<u8>, value: f64) {
    output.push(0xcb);
    output.extend_from_slice(&value.to_be_bytes());
}

fn pack_uint(output: &mut Vec<u8>, value: u64) {
    match value {
        0..=0x7f => output.push(u8::try_from(value).expect("positive fixint range")),
        0x80..=0xff => {
            output.extend_from_slice(&[0xcc, u8::try_from(value).expect("uint8 range")]);
        }
        0x100..=0xffff => {
            output.push(0xcd);
            output.extend_from_slice(&u16::try_from(value).expect("uint16 range").to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            output.push(0xce);
            output.extend_from_slice(&u32::try_from(value).expect("uint32 range").to_be_bytes());
        }
        _ => {
            output.push(0xcf);
            output.extend_from_slice(&value.to_be_bytes());
        }
    }
}

const fn metric_type_from_sql(value: i64) -> Result<MetricType, MetricStoreError> {
    match value {
        0 => Ok(MetricType::Counter),
        1 => Ok(MetricType::Gauge),
        2 => Ok(MetricType::Histogram),
        _ => Err(MetricStoreError::InvalidSchema("series type is invalid")),
    }
}

fn validate_schema(connection: &Connection) -> Result<(), MetricStoreError> {
    let version: String = connection
        .query_row(
            "SELECT value FROM metadata WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => {
                MetricStoreError::InvalidSchema("schema_version is missing")
            }
            other => MetricStoreError::Sql(other),
        })?;
    if version != "1" {
        return Err(MetricStoreError::UnknownVersion(version));
    }
    for (kind, name) in [
        ("table", "series"),
        ("table", "samples"),
        ("table", "metadata"),
        ("index", "idx_samples_series_timestamp"),
        ("index", "idx_series_name"),
        ("index", "idx_series_label_hash"),
    ] {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = ?1 AND name = ?2)",
            params![kind, name],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(MetricStoreError::InvalidSchema(
                "required schema object is missing",
            ));
        }
    }
    Ok(())
}

/// Outcome of one metric transaction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MetricCommitStats {
    /// Samples inserted.
    pub accepted: usize,
    /// Samples silently discarded for immutable-type disagreement.
    pub type_mismatches: usize,
}

/// Metric-store failure.
#[derive(Debug)]
pub enum MetricStoreError {
    /// `SQLite` failure.
    Sql(rusqlite::Error),
    /// Filesystem failure.
    Io(std::io::Error),
    /// Required schema content is invalid.
    InvalidSchema(&'static str),
    /// Unsupported schema version.
    UnknownVersion(String),
    /// Retention batch size exceeds `SQLite`'s integer range.
    IntegerRange,
}

impl MetricStoreError {
    /// Whether retrying after retention may make this operation succeed.
    #[must_use]
    pub fn is_capacity(&self) -> bool {
        matches!(
            self,
            Self::Sql(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::DiskFull
        )
    }

    /// Whether `SQLite` has declared the database image corrupt.
    #[must_use]
    pub const fn is_corruption(&self) -> bool {
        matches!(
            self,
            Self::Sql(rusqlite::Error::SqliteFailure(error, _))
                if matches!(
                    error.code,
                    rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase
                )
        )
    }
}

impl fmt::Display for MetricStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sql(error) => write!(formatter, "metric-store SQLite error: {error}"),
            Self::Io(error) => write!(formatter, "metric-store filesystem error: {error}"),
            Self::InvalidSchema(reason) => {
                write!(formatter, "invalid metric-store schema: {reason}")
            }
            Self::UnknownVersion(version) => {
                write!(formatter, "unsupported metric-store schema {version}")
            }
            Self::IntegerRange => {
                formatter.write_str("metric retention batch size exceeds SQLite range")
            }
        }
    }
}

impl std::error::Error for MetricStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sql(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::InvalidSchema(_) | Self::UnknownVersion(_) | Self::IntegerRange => None,
        }
    }
}

impl From<rusqlite::Error> for MetricStoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(metric_type: MetricType, value: MetricValue) -> MetricRecord {
        MetricRecord {
            boot_id: [1; 16],
            timestamp: 10,
            name: "test.metric".into(),
            labels: "core=0".into(),
            metric_type,
            value,
        }
    }

    #[test]
    fn resolves_one_series_and_drops_later_type_mismatch() {
        let directory = temporary_directory();
        let path = directory.join("metrics.db");
        let mut store = MetricStore::open(path, 1_000, 10).unwrap();
        let stats = store
            .commit(&[
                record(MetricType::Gauge, MetricValue::Number(1.0)),
                record(MetricType::Gauge, MetricValue::Number(2.0)),
                record(MetricType::Counter, MetricValue::Number(3.0)),
            ])
            .unwrap();
        assert_eq!(stats.accepted, 2);
        assert_eq!(stats.type_mismatches, 1);
        let counts: (u32, u32) = store
            .connection
            .query_row(
                "SELECT (SELECT count(*) FROM series), (SELECT count(*) FROM samples)",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(counts, (1, 2));
        assert_eq!(store.retain_oldest(Some(11), 10).unwrap(), 2);
        let series: u32 = store
            .connection
            .query_row("SELECT count(*) FROM series", [], |row| row.get(0))
            .unwrap();
        assert_eq!(series, 0);
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn classifies_sqlite_full_as_capacity_failure() {
        let error = MetricStoreError::Sql(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
            None,
        ));
        assert!(error.is_capacity());
    }

    #[test]
    fn changed_histogram_boundaries_create_a_new_series() {
        let directory = temporary_directory();
        let mut store = MetricStore::open(directory.join("metrics.db"), 1_000, 10).unwrap();
        for boundaries in [vec![1.0, 2.0], vec![1.0, 3.0]] {
            store
                .commit(&[record(
                    MetricType::Histogram,
                    MetricValue::Histogram(Histogram {
                        boundaries: boundaries.into_boxed_slice(),
                        counts: vec![1, 2].into_boxed_slice(),
                        total_count: 2,
                        sum: 2.0,
                    }),
                )])
                .unwrap();
        }
        let count: u32 = store
            .connection
            .query_row("SELECT count(*) FROM series", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    fn temporary_directory() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "eventd-metric-test-{}-{}",
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
