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
CREATE TABLE rollups (
    series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,
    window_start INTEGER NOT NULL,
    window_width INTEGER NOT NULL CHECK (window_width > 0),
    transform INTEGER NOT NULL CHECK (transform IN (0, 1, 2, 50, 95, 99)),
    function INTEGER NOT NULL CHECK (function BETWEEN 0 AND 3),
    value REAL,
    overflow INTEGER NOT NULL CHECK (overflow IN (0, 1)),
    source_max_sample_id INTEGER NOT NULL CHECK (source_max_sample_id >= 0),
    source_baseline_sample_id INTEGER CHECK (source_baseline_sample_id > 0),
    CHECK (overflow = 0 OR value IS NULL),
    PRIMARY KEY (series_id, window_start, window_width, transform, function)
) WITHOUT ROWID;
CREATE INDEX idx_rollups_window ON rollups(window_start);
INSERT INTO metadata(key, value) VALUES ('schema_version', '2');
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

impl MetricType {
    /// Stable PSPU wire name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
            Self::Histogram => "histogram",
        }
    }
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

/// One query-computed, writer-committed metric window cache row.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricRollup {
    /// Owning metric series.
    pub series_id: i64,
    /// Epoch-aligned inclusive window start in nanoseconds.
    pub window_start: i64,
    /// Positive window width in nanoseconds.
    pub window_width: i64,
    /// Stable query-transform discriminator.
    pub transform: i64,
    /// Stable terminal-aggregation discriminator.
    pub function: i64,
    /// Finite result, or absent for an empty or overflowing window.
    pub value: Option<f64>,
    /// Whether the absent value represents percentile overflow.
    pub overflow: bool,
    /// Greatest in-window raw sample identifier observed by the query.
    pub source_max_sample_id: i64,
    /// Immediately preceding sample for pair transforms, with absence explicit.
    pub source_baseline_sample_id: Option<i64>,
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

    /// Apply live writer-maintenance bounds at a transaction boundary.
    pub fn configure(&mut self, checkpoint_pages: u32, cache_capacity: usize) {
        self.checkpoint_pages = checkpoint_pages;
        self.cache_capacity = cache_capacity;
        while self.cache.len() > self.cache_capacity {
            self.cache.pop_front();
        }
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
        let mut type_mismatches = 0;
        let mut last_type_mismatch = None;
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
                    type_mismatches += 1;
                    last_type_mismatch = Some((record, info.metric_type));
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
            type_mismatches,
            last_type_mismatch: last_type_mismatch.map(|(record, expected)| MetricTypeMismatch {
                name: record.name.clone(),
                expected,
                received: record.metric_type,
            }),
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
        let selection = if older_than.is_some() {
            "SELECT id FROM samples WHERE timestamp < ?1 ORDER BY timestamp, id LIMIT ?2"
        } else {
            "SELECT id FROM samples ORDER BY timestamp, id LIMIT ?1"
        };
        if let Some(cutoff) = older_than {
            transaction.execute(
                &format!(
                    "DELETE FROM rollups WHERE series_id IN (SELECT DISTINCT series_id FROM samples WHERE id IN ({selection}))"
                ),
                params![cutoff, limit],
            )?;
        } else {
            transaction.execute(
                &format!(
                    "DELETE FROM rollups WHERE series_id IN (SELECT DISTINCT series_id FROM samples WHERE id IN ({selection}))"
                ),
                [limit],
            )?;
        }
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

    /// Commit a bounded set of query-computed rollups and prune the oldest cache rows.
    pub fn commit_rollups(
        &mut self,
        rows: &[MetricRollup],
        max_rows: usize,
    ) -> Result<usize, MetricStoreError> {
        if rows.is_empty() {
            return Ok(0);
        }
        if max_rows == 0 {
            self.prune_rollups(0)?;
            return Ok(0);
        }
        for row in rows {
            validate_rollup(row)?;
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        {
            let mut upsert = transaction.prepare_cached(
                "INSERT INTO rollups (series_id, window_start, window_width, transform, function, value, overflow, source_max_sample_id, source_baseline_sample_id) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9) \
                 ON CONFLICT(series_id, window_start, window_width, transform, function) DO UPDATE SET \
                 value=excluded.value, overflow=excluded.overflow, source_max_sample_id=excluded.source_max_sample_id, source_baseline_sample_id=excluded.source_baseline_sample_id",
            )?;
            for row in rows {
                upsert.execute(params![
                    row.series_id,
                    row.window_start,
                    row.window_width,
                    row.transform,
                    row.function,
                    row.value,
                    row.overflow,
                    row.source_max_sample_id,
                    row.source_baseline_sample_id,
                ])?;
            }
        }
        prune_rollups(&transaction, max_rows)?;
        transaction.commit()?;
        Ok(rows.len())
    }

    /// Apply a live cache bound without adding or recomputing any rollup.
    pub fn prune_rollups(&mut self, max_rows: usize) -> Result<(), MetricStoreError> {
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        prune_rollups(&transaction, max_rows)?;
        transaction.commit()?;
        Ok(())
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
    let mut version: String = connection
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
    if version == "1" {
        connection.execute_batch(
            "BEGIN IMMEDIATE;\
             CREATE TABLE rollups (\
                 series_id INTEGER NOT NULL REFERENCES series(id) ON DELETE CASCADE,\
                 window_start INTEGER NOT NULL,\
                 window_width INTEGER NOT NULL CHECK (window_width > 0),\
                 transform INTEGER NOT NULL CHECK (transform IN (0, 1, 2, 50, 95, 99)),\
                 function INTEGER NOT NULL CHECK (function BETWEEN 0 AND 3),\
                 value REAL,\
                 overflow INTEGER NOT NULL CHECK (overflow IN (0, 1)),\
                 source_max_sample_id INTEGER NOT NULL CHECK (source_max_sample_id >= 0),\
                 source_baseline_sample_id INTEGER CHECK (source_baseline_sample_id > 0),\
                 CHECK (overflow = 0 OR value IS NULL),\
                 PRIMARY KEY (series_id, window_start, window_width, transform, function)\
             ) WITHOUT ROWID;\
             CREATE INDEX idx_rollups_window ON rollups(window_start);\
             UPDATE metadata SET value = '2' WHERE key = 'schema_version';\
             COMMIT;",
        )?;
        "2".clone_into(&mut version);
    }
    if version != "2" {
        return Err(MetricStoreError::UnknownVersion(version));
    }
    for (kind, name) in [
        ("table", "series"),
        ("table", "samples"),
        ("table", "metadata"),
        ("index", "idx_samples_series_timestamp"),
        ("index", "idx_series_name"),
        ("index", "idx_series_label_hash"),
        ("table", "rollups"),
        ("index", "idx_rollups_window"),
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

fn validate_rollup(row: &MetricRollup) -> Result<(), MetricStoreError> {
    if row.series_id <= 0
        || row.window_width <= 0
        || row.window_start.rem_euclid(row.window_width) != 0
        || !matches!(row.transform, 0 | 1 | 2 | 50 | 95 | 99)
        || !(0..=3).contains(&row.function)
        || row.source_max_sample_id < 0
        || row.source_baseline_sample_id.is_some_and(|id| id <= 0)
        || row.value.is_some_and(|value| !value.is_finite())
        || (row.overflow && row.value.is_some())
        || (row.overflow && !matches!(row.transform, 50 | 95 | 99))
        || (!matches!(row.transform, 1 | 2) && row.source_baseline_sample_id.is_some())
    {
        return Err(MetricStoreError::InvalidRollup);
    }
    Ok(())
}

fn prune_rollups(transaction: &Transaction<'_>, max_rows: usize) -> Result<(), MetricStoreError> {
    let max_rows = i64::try_from(max_rows).map_err(|_| MetricStoreError::IntegerRange)?;
    if max_rows == 0 {
        transaction.execute("DELETE FROM rollups", [])?;
        return Ok(());
    }
    let count: i64 = transaction.query_row("SELECT COUNT(*) FROM rollups", [], |row| row.get(0))?;
    let excess = count.saturating_sub(max_rows);
    if excess != 0 {
        transaction.execute(
            "DELETE FROM rollups WHERE (series_id, window_start, window_width, transform, function) IN \
             (SELECT series_id, window_start, window_width, transform, function FROM rollups ORDER BY window_start LIMIT ?1)",
            [excess],
        )?;
    }
    Ok(())
}

/// Outcome of one metric transaction.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricCommitStats {
    /// Samples inserted.
    pub accepted: usize,
    /// Samples silently discarded for immutable-type disagreement.
    pub type_mismatches: usize,
    /// Most recently encountered disagreement in this transaction.
    pub last_type_mismatch: Option<MetricTypeMismatch>,
}

/// Actionable detail for one immutable-series-type disagreement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricTypeMismatch {
    /// Concrete metric name.
    pub name: Box<str>,
    /// Type fixed when the series was created.
    pub expected: MetricType,
    /// Type declared by the discarded sample.
    pub received: MetricType,
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
    /// A query submitted an internally inconsistent adaptive-rollup row.
    InvalidRollup,
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
            Self::InvalidRollup => formatter.write_str("query submitted an invalid metric rollup"),
        }
    }
}

impl std::error::Error for MetricStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sql(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::InvalidSchema(_)
            | Self::UnknownVersion(_)
            | Self::IntegerRange
            | Self::InvalidRollup => None,
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
        assert_eq!(
            stats.last_type_mismatch,
            Some(MetricTypeMismatch {
                name: "test.metric".into(),
                expected: MetricType::Gauge,
                received: MetricType::Counter,
            })
        );
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

    #[test]
    fn rollups_are_bounded_and_retention_invalidates_their_series() {
        let directory = temporary_directory();
        let mut store = MetricStore::open(directory.join("metrics.db"), 1_000, 10).unwrap();
        store
            .commit(&[record(MetricType::Gauge, MetricValue::Number(1.0))])
            .unwrap();
        let rollup = |window_start, value| MetricRollup {
            series_id: 1,
            window_start,
            window_width: 10,
            transform: 0,
            function: 0,
            value: Some(value),
            overflow: false,
            source_max_sample_id: 1,
            source_baseline_sample_id: None,
        };
        store
            .commit_rollups(&[rollup(0, 1.0), rollup(10, 2.0)], 1)
            .unwrap();
        let retained: (u32, i64) = store
            .connection
            .query_row(
                "SELECT COUNT(*), MIN(window_start) FROM rollups",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(retained, (1, 10));

        assert_eq!(store.retain_oldest(Some(11), 10).unwrap(), 1);
        let remaining: u32 = store
            .connection
            .query_row("SELECT COUNT(*) FROM rollups", [], |row| row.get(0))
            .unwrap();
        assert_eq!(remaining, 0);
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_invalid_rollup_state_before_writing() {
        let directory = temporary_directory();
        let mut store = MetricStore::open(directory.join("metrics.db"), 1_000, 10).unwrap();
        store
            .commit(&[record(MetricType::Gauge, MetricValue::Number(1.0))])
            .unwrap();
        let invalid = MetricRollup {
            series_id: 1,
            window_start: 1,
            window_width: 10,
            transform: 0,
            function: 0,
            value: Some(f64::INFINITY),
            overflow: true,
            source_max_sample_id: 1,
            source_baseline_sample_id: Some(1),
        };
        assert!(matches!(
            store.commit_rollups(&[invalid], 10),
            Err(MetricStoreError::InvalidRollup)
        ));
        let remaining: u32 = store
            .connection
            .query_row("SELECT COUNT(*) FROM rollups", [], |row| row.get(0))
            .unwrap();
        assert_eq!(remaining, 0);
        drop(store);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn opens_and_migrates_a_version_one_store() {
        let directory = temporary_directory();
        let path = directory.join("metrics.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE series(id INTEGER PRIMARY KEY, name TEXT, labels TEXT, type INTEGER, label_hash INTEGER, boundaries_hash INTEGER, boundaries BLOB);\
                 CREATE TABLE samples(id INTEGER PRIMARY KEY, series_id INTEGER, boot_id BLOB, timestamp INTEGER, value REAL, histogram_data BLOB);\
                 CREATE TABLE metadata(key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;\
                 CREATE INDEX idx_samples_series_timestamp ON samples(series_id, timestamp, id);\
                 CREATE INDEX idx_series_name ON series(name);\
                 CREATE INDEX idx_series_label_hash ON series(label_hash);\
                 INSERT INTO metadata VALUES ('schema_version', '1');",
            )
            .unwrap();
        drop(connection);

        let store = MetricStore::open(&path, 1_000, 10).unwrap();
        let version: String = store
            .connection
            .query_row(
                "SELECT value FROM metadata WHERE key='schema_version'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(version, "2");
        let exists: bool = store
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name='rollups')",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert!(exists);
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
