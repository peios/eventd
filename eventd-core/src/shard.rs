//! Single-owner `SQLite` event-shard writer.

use core::fmt;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};

use crate::{Guid, IngestItem, Interval, SyntheticEvent};

const SCHEMA_VERSION: &str = "1";

const CREATE_SCHEMA: &str = r"
CREATE TABLE events (
    id INTEGER PRIMARY KEY,
    boot_id BLOB NOT NULL,
    timestamp INTEGER NOT NULL,
    cpu_id INTEGER,
    sequence INTEGER,
    origin_class INTEGER,
    event_type TEXT NOT NULL,
    effective_token_guid BLOB,
    true_token_guid BLOB,
    process_guid BLOB,
    payload BLOB
);
CREATE TABLE event_types (
    event_type TEXT PRIMARY KEY
) WITHOUT ROWID;
CREATE TABLE receipt_ranges (
    boot_id BLOB NOT NULL,
    cpu_id INTEGER NOT NULL,
    first_sequence INTEGER NOT NULL CHECK (first_sequence > 0),
    last_sequence INTEGER NOT NULL CHECK (last_sequence >= first_sequence),
    PRIMARY KEY (boot_id, cpu_id, first_sequence, last_sequence)
) WITHOUT ROWID;
CREATE TABLE metadata (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
CREATE INDEX idx_events_timestamp ON events(timestamp);
INSERT INTO metadata(key, value) VALUES ('schema_version', '1');
INSERT INTO metadata(key, value)
VALUES ('created_at', strftime('%Y-%m-%dT%H:%M:%SZ', 'now'));
";

/// The only read-write connection to one event shard.
pub struct Shard {
    connection: Connection,
    path: PathBuf,
    known_types: HashSet<Box<str>>,
    checkpoint_pages: u32,
    page_size: u64,
}

impl Shard {
    /// Open or create one active shard and verify its required schema.
    pub fn open(path: impl AsRef<Path>, checkpoint_pages: u32) -> Result<Self, ShardError> {
        let path = path.as_ref();
        let existed = path.try_exists().map_err(ShardError::Io)?;
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(std::time::Duration::ZERO)?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;\
             PRAGMA synchronous=FULL;\
             PRAGMA wal_autocheckpoint=0;\
             PRAGMA foreign_keys=ON;\
             PRAGMA temp_store=MEMORY;",
        )?;
        if !existed {
            connection.execute_batch(CREATE_SCHEMA)?;
        }
        validate_schema(&connection)?;

        let known_types = {
            let mut statement = connection.prepare("SELECT event_type FROM event_types")?;
            let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
            rows.collect::<Result<HashSet<_>, _>>()?
                .into_iter()
                .map(String::into_boxed_str)
                .collect()
        };
        let page_size = connection.query_row("PRAGMA page_size", [], |row| row.get::<_, u64>(0))?;
        Ok(Self {
            connection,
            path: path.to_owned(),
            known_types,
            checkpoint_pages,
            page_size,
        })
    }

    /// Commit a non-empty batch, including catalogue rows and receipt ranges.
    pub fn commit(&mut self, items: &[IngestItem]) -> Result<CommitStats, ShardError> {
        if items.is_empty() {
            return Ok(CommitStats::default());
        }

        let mut pending_types: HashSet<Box<str>> = HashSet::new();
        let mut receipts: HashMap<(Guid, u16), Vec<Interval>> = HashMap::new();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        {
            let mut insert_event = transaction.prepare_cached(
                "INSERT INTO events (boot_id, timestamp, cpu_id, sequence, origin_class, \
                 event_type, effective_token_guid, true_token_guid, process_guid, payload) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            )?;
            let mut insert_gap = transaction.prepare_cached(
                "INSERT INTO events (boot_id, timestamp, cpu_id, event_type, payload) \
                 VALUES (?1, ?2, ?3, 'synthetic.gap', ?4)",
            )?;
            let mut insert_type = transaction
                .prepare_cached("INSERT OR IGNORE INTO event_types(event_type) VALUES (?1)")?;

            for item in items {
                let event = &item.event;
                for gap in &item.gaps {
                    if !self.known_types.contains("synthetic.gap")
                        && pending_types.insert("synthetic.gap".into())
                    {
                        insert_type.execute(["synthetic.gap"])?;
                    }
                    let payload = encode_gap_payload(event.cpu_id, *gap);
                    insert_gap.execute(params![
                        &event.boot_id[..],
                        sqlite_integer(gap.timestamp, "gap timestamp")?,
                        i64::from(event.cpu_id),
                        payload,
                    ])?;
                }
                if item.store_event {
                    if !self.known_types.contains(event.event_type.as_ref())
                        && pending_types.insert(event.event_type.clone())
                    {
                        insert_type.execute([event.event_type.as_ref()])?;
                    }
                    insert_event.execute(params![
                        &event.boot_id[..],
                        sqlite_integer(event.timestamp, "event timestamp")?,
                        i64::from(event.cpu_id),
                        sqlite_integer(event.sequence, "event sequence")?,
                        i64::from(event.origin_class),
                        event.event_type.as_ref(),
                        &event.effective_token_guid[..],
                        &event.true_token_guid[..],
                        &event.process_guid[..],
                        event.payload.as_ref(),
                    ])?;
                }

                let stream_receipts = receipts.entry((event.boot_id, event.cpu_id)).or_default();
                stream_receipts.extend(item.gaps.iter().map(|gap| Interval {
                    first: gap.first_sequence,
                    last: gap.last_sequence,
                }));
                if item.store_event {
                    stream_receipts.push(Interval {
                        first: event.sequence,
                        last: event.sequence,
                    });
                }
            }
            drop(insert_type);
            drop(insert_gap);
            drop(insert_event);

            let mut insert_receipt = transaction.prepare_cached(
                "INSERT OR IGNORE INTO receipt_ranges \
                 (boot_id, cpu_id, first_sequence, last_sequence) VALUES (?1, ?2, ?3, ?4)",
            )?;
            for ((boot_id, cpu_id), intervals) in &mut receipts {
                merge_intervals(intervals);
                for interval in intervals {
                    insert_receipt.execute(params![
                        &boot_id[..],
                        i64::from(*cpu_id),
                        sqlite_integer(interval.first, "receipt first sequence")?,
                        sqlite_integer(interval.last, "receipt last sequence")?,
                    ])?;
                }
            }
        }
        transaction.commit()?;
        self.known_types.extend(pending_types);

        self.checkpoint_if_needed()?;
        Ok(CommitStats {
            items: items.len(),
            event_rows: items.iter().filter(|item| item.store_event).count()
                + items.iter().map(|item| item.gaps.len()).sum::<usize>(),
            receipt_rows: receipts.values().map(Vec::len).sum(),
        })
    }

    /// Commit one daemon-generated event in its own durability transaction.
    pub fn commit_synthetic(&mut self, event: &SyntheticEvent) -> Result<(), ShardError> {
        if !event.event_type.starts_with("synthetic.") {
            return Err(ShardError::InvalidSyntheticType);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        transaction.execute(
            "INSERT OR IGNORE INTO event_types(event_type) VALUES (?1)",
            [event.event_type.as_ref()],
        )?;
        transaction.execute(
            "INSERT INTO events (boot_id, timestamp, event_type, payload) \
             VALUES (?1, ?2, ?3, ?4)",
            params![
                &event.boot_id[..],
                sqlite_integer(event.timestamp, "synthetic timestamp")?,
                event.event_type.as_ref(),
                event.payload.as_ref(),
            ],
        )?;
        transaction.commit()?;
        self.known_types.insert(event.event_type.clone());
        self.checkpoint_if_needed()
    }

    /// Read all receipt rows from this shard for startup reconciliation.
    pub fn receipts(&self) -> Result<Vec<(Guid, u16, Interval)>, ShardError> {
        read_receipts(&self.connection)
    }

    /// Verify and read receipts through a read-only historical-shard handle.
    pub fn historical_receipts(
        path: impl AsRef<Path>,
    ) -> Result<Vec<(Guid, u16, Interval)>, ShardError> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        validate_schema(&connection)?;
        read_receipts(&connection)
    }

    /// Whether this shard has committed evidence for `boot_id`.
    pub fn contains_boot(&self, boot_id: &Guid) -> Result<bool, ShardError> {
        self.connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM receipt_ranges WHERE boot_id = ?1 \
                 UNION ALL SELECT 1 FROM events WHERE boot_id = ?1 LIMIT 1)",
                [&boot_id[..]],
                |row| row.get(0),
            )
            .map_err(ShardError::Sql)
    }

    /// Whether a read-only shard has committed evidence for `boot_id`.
    pub fn historical_contains_boot(
        path: impl AsRef<Path>,
        boot_id: &Guid,
    ) -> Result<bool, ShardError> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        validate_schema(&connection)?;
        connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM receipt_ranges WHERE boot_id = ?1 \
                 UNION ALL SELECT 1 FROM events WHERE boot_id = ?1 LIMIT 1)",
                [&boot_id[..]],
                |row| row.get(0),
            )
            .map_err(ShardError::Sql)
    }

    /// Filesystem path used for diagnostics.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn checkpoint_if_needed(&self) -> Result<(), ShardError> {
        let mut wal_name = self.path.as_os_str().to_owned();
        wal_name.push("-wal");
        let wal_bytes = match std::fs::metadata(PathBuf::from(wal_name)) {
            Ok(metadata) => metadata.len(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => 0,
            Err(error) => return Err(ShardError::Io(error)),
        };
        let pages = wal_bytes / self.page_size.max(1);
        if pages < u64::from(self.checkpoint_pages) {
            return Ok(());
        }
        self.connection
            .execute_batch("PRAGMA wal_checkpoint(PASSIVE)")?;
        Ok(())
    }
}

fn read_receipts(connection: &Connection) -> Result<Vec<(Guid, u16, Interval)>, ShardError> {
    let mut statement = connection
        .prepare("SELECT boot_id, cpu_id, first_sequence, last_sequence FROM receipt_ranges")?;
    let rows = statement.query_map([], |row| {
        let boot: Vec<u8> = row.get(0)?;
        let cpu: u16 = row.get(1)?;
        let first: u64 = row.get(2)?;
        let last: u64 = row.get(3)?;
        Ok((boot, cpu, first, last))
    })?;
    let mut receipts = Vec::new();
    for row in rows {
        let (boot, cpu, first, last) = row?;
        let boot_id: Guid = boot
            .try_into()
            .map_err(|_| ShardError::InvalidSchema("receipt boot_id is not 16 bytes"))?;
        let interval = Interval::new(first, last)
            .ok_or(ShardError::InvalidSchema("receipt interval is invalid"))?;
        receipts.push((boot_id, cpu, interval));
    }
    Ok(receipts)
}

fn validate_schema(connection: &Connection) -> Result<(), ShardError> {
    let version: String = connection
        .query_row(
            "SELECT value FROM metadata WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .map_err(|error| match error {
            rusqlite::Error::QueryReturnedNoRows => {
                ShardError::InvalidSchema("schema_version is missing")
            }
            other => ShardError::Sql(other),
        })?;
    if version != SCHEMA_VERSION {
        return Err(ShardError::UnknownVersion(version));
    }
    let created_at: String = connection
        .query_row(
            "SELECT value FROM metadata WHERE key = 'created_at'",
            [],
            |row| row.get(0),
        )
        .map_err(|_| ShardError::InvalidSchema("created_at is missing"))?;
    if created_at.len() != 20 || !created_at.ends_with('Z') {
        return Err(ShardError::InvalidSchema("created_at is malformed"));
    }
    for (kind, name) in [
        ("table", "events"),
        ("table", "event_types"),
        ("table", "receipt_ranges"),
        ("table", "metadata"),
        ("index", "idx_events_timestamp"),
    ] {
        let exists: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = ?1 AND name = ?2)",
            params![kind, name],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(ShardError::InvalidSchema(
                "required schema object is missing",
            ));
        }
    }
    Ok(())
}

fn merge_intervals(intervals: &mut Vec<Interval>) {
    intervals.sort_unstable_by_key(|interval| interval.first);
    let mut output = 0;
    for input in 0..intervals.len() {
        let interval = intervals[input];
        if output > 0 && interval.first <= intervals[output - 1].last.saturating_add(1) {
            intervals[output - 1].last = intervals[output - 1].last.max(interval.last);
        } else {
            intervals[output] = interval;
            output += 1;
        }
    }
    intervals.truncate(output);
}

fn sqlite_integer(value: u64, field: &'static str) -> Result<i64, ShardError> {
    i64::try_from(value).map_err(|_| ShardError::IntegerRange(field))
}

fn encode_gap_payload(cpu_id: u16, gap: crate::Gap) -> Vec<u8> {
    let mut output = Vec::with_capacity(144);
    output.push(0x86); // fixmap, six entries
    pack_str(&mut output, "cpu_id");
    pack_u64(&mut output, u64::from(cpu_id));
    pack_str(&mut output, "first_sequence");
    pack_u64(&mut output, gap.first_sequence);
    pack_str(&mut output, "last_sequence");
    pack_u64(&mut output, gap.last_sequence);
    pack_str(&mut output, "count");
    pack_u64(&mut output, gap.count());
    pack_str(&mut output, "last_seen_timestamp");
    if let Some(timestamp) = gap.preceding_timestamp {
        pack_u64(&mut output, timestamp);
    } else {
        output.push(0xc0);
    }
    pack_str(&mut output, "revealing_timestamp");
    pack_u64(&mut output, gap.revealing_timestamp);
    output
}

fn pack_str(output: &mut Vec<u8>, value: &str) {
    if value.len() <= 31 {
        output.push(0xa0 | u8::try_from(value.len()).expect("fixstr length"));
    } else {
        output.push(0xd9);
        output.push(u8::try_from(value.len()).expect("str8 length"));
    }
    output.extend_from_slice(value.as_bytes());
}

fn pack_u64(output: &mut Vec<u8>, value: u64) {
    match value {
        0..=0x7f => output.push(u8::try_from(value).expect("positive fixint range")),
        0x80..=0xff => {
            output
                .extend_from_slice(&[0xcc, u8::try_from(value).expect("MessagePack uint8 range")]);
        }
        0x100..=0xffff => {
            output.push(0xcd);
            output.extend_from_slice(
                &u16::try_from(value)
                    .expect("MessagePack uint16 range")
                    .to_be_bytes(),
            );
        }
        0x1_0000..=0xffff_ffff => {
            output.push(0xce);
            output.extend_from_slice(
                &u32::try_from(value)
                    .expect("MessagePack uint32 range")
                    .to_be_bytes(),
            );
        }
        _ => {
            output.push(0xcf);
            output.extend_from_slice(&value.to_be_bytes());
        }
    }
}

/// Rows produced by one successful commit.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitStats {
    /// Queue items consumed.
    pub items: usize,
    /// Real plus synthetic event rows inserted.
    pub event_rows: usize,
    /// Merged receipt rows inserted or already present.
    pub receipt_rows: usize,
}

/// Event-shard failure.
#[derive(Debug)]
pub enum ShardError {
    /// `SQLite` failure.
    Sql(rusqlite::Error),
    /// Filesystem inspection failure.
    Io(std::io::Error),
    /// Required schema content is invalid.
    InvalidSchema(&'static str),
    /// The shard uses an unsupported schema version.
    UnknownVersion(String),
    /// An unsigned kernel value cannot fit `SQLite`'s signed `INTEGER`.
    IntegerRange(&'static str),
    /// The direct-write API was given a non-synthetic event type.
    InvalidSyntheticType,
}

impl fmt::Display for ShardError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sql(error) => write!(formatter, "SQLite error: {error}"),
            Self::Io(error) => write!(formatter, "filesystem error: {error}"),
            Self::InvalidSchema(reason) => {
                write!(formatter, "invalid event shard schema: {reason}")
            }
            Self::UnknownVersion(version) => {
                write!(formatter, "unsupported event shard schema {version}")
            }
            Self::IntegerRange(field) => write!(formatter, "{field} exceeds SQLite INTEGER range"),
            Self::InvalidSyntheticType => {
                formatter.write_str("direct event type does not begin with synthetic.")
            }
        }
    }
}

impl std::error::Error for ShardError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sql(error) => Some(error),
            Self::Io(error) => Some(error),
            Self::InvalidSchema(_)
            | Self::UnknownVersion(_)
            | Self::IntegerRange(_)
            | Self::InvalidSyntheticType => None,
        }
    }
}

impl From<rusqlite::Error> for ShardError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Gap, RealEvent};

    fn event(sequence: u64, event_type: &str) -> IngestItem {
        IngestItem {
            gaps: Vec::new(),
            store_event: true,
            event: RealEvent {
                boot_id: [1; 16],
                timestamp: sequence,
                cpu_id: 2,
                sequence,
                origin_class: 3,
                effective_token_guid: [2; 16],
                true_token_guid: [3; 16],
                process_guid: [4; 16],
                event_type: event_type.into(),
                payload: [0x80].into(),
            },
        }
    }

    #[test]
    fn commits_events_catalogue_and_merged_receipt_atomically() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = Shard::open(&path, 1_000).unwrap();
        let mut first = event(4, "example.test");
        first.gaps.push(Gap {
            timestamp: 40,
            first_sequence: 1,
            last_sequence: 3,
            preceding_timestamp: None,
            revealing_timestamp: 4,
        });
        let stats = shard.commit(&[first, event(5, "example.test")]).unwrap();
        assert_eq!(stats.event_rows, 3);
        assert_eq!(stats.receipt_rows, 1);
        let receipts = shard.receipts().unwrap();
        assert_eq!(receipts[0].2, Interval { first: 1, last: 5 });
        let type_count: u32 = shard
            .connection
            .query_row("SELECT count(*) FROM event_types", [], |row| row.get(0))
            .unwrap();
        assert_eq!(type_count, 2);
        assert!(shard.contains_boot(&[1; 16]).unwrap());
        assert!(!shard.contains_boot(&[9; 16]).unwrap());
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn rejects_unrecognised_schema_version() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let shard = Shard::open(&path, 1_000).unwrap();
        shard
            .connection
            .execute(
                "UPDATE metadata SET value = '99' WHERE key = 'schema_version'",
                [],
            )
            .unwrap();
        drop(shard);
        assert!(matches!(
            Shard::open(&path, 1_000),
            Err(ShardError::UnknownVersion(version)) if version == "99"
        ));
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn commits_synthetic_event_directly() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = Shard::open(&path, 1_000).unwrap();
        shard
            .commit_synthetic(&SyntheticEvent {
                boot_id: [1; 16],
                timestamp: 42,
                event_type: "synthetic.startup".into(),
                payload: [0x80].into(),
            })
            .unwrap();
        let count: u32 = shard
            .connection
            .query_row(
                "SELECT count(*) FROM events WHERE event_type = 'synthetic.startup'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    fn temporary_directory() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "eventd-core-test-{}-{}",
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
