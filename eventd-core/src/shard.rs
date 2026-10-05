//! Single-owner `SQLite` event-shard writer.

use core::fmt;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use rusqlite::config::DbConfig;
use rusqlite::{Connection, OpenFlags, TransactionBehavior, params};

use crate::schema::Contents;
use crate::{DesiredIndex, Gap, Guid, IngestItem, Interval, SyntheticEvent};

const SCHEMA_VERSION: &str = "1";

const CREATE_SCHEMA: &str = r"
BEGIN IMMEDIATE;
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
COMMIT;
";

/// The only read-write connection to one event shard.
pub struct Shard {
    connection: Connection,
    path: PathBuf,
    known_types: HashSet<Box<str>>,
    /// Types a committed retention delete touched, awaiting an orphan check.
    orphan_candidates: HashSet<Box<str>>,
    checkpoint_pages: u32,
    page_size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
/// Result of one quiet-period index convergence step.
pub enum IndexAction {
    /// A secondary index was materialized.
    Created(String),
    /// A secondary index was removed.
    Dropped(String),
    /// The shard is converged or no supported action is available.
    Unchanged,
    /// Index creation yielded immediately to newly queued ingestion.
    Cancelled,
}

impl Shard {
    /// Apply the next passive-checkpoint threshold at a transaction boundary.
    pub const fn set_checkpoint_pages(&mut self, pages: u32) {
        self.checkpoint_pages = pages;
    }

    /// Open a required shard, quarantining only SQLite-reported corruption.
    pub fn open_recovering(
        path: impl AsRef<Path>,
        checkpoint_pages: u32,
    ) -> Result<(Self, Option<String>), ShardError> {
        let path = path.as_ref();
        let preserved = crate::quarantine::Preserved::take(path);
        match Self::open(path, checkpoint_pages) {
            Ok(shard) => Ok((shard, None)),
            Err(error) if error.is_corruption() => {
                let note = preserved.quarantine().map_err(ShardError::Io)?;
                let reason = crate::quarantine::reason(&error, note);
                Ok((Self::open(path, checkpoint_pages)?, Some(reason)))
            }
            Err(error) => Err(error),
        }
    }

    /// Open or create one active shard and verify its required schema.
    pub fn open(path: impl AsRef<Path>, checkpoint_pages: u32) -> Result<Self, ShardError> {
        let path = path.as_ref();
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        crate::payload_index::register(&connection)?;
        // Until the shard is verified, closing must not checkpoint the WAL
        // into a database that may be quarantined (§3.3).
        connection.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true)?;
        crate::writer_lock::configure(&connection)?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;\
             PRAGMA synchronous=FULL;\
             PRAGMA wal_autocheckpoint=0;\
             PRAGMA journal_size_limit=0;\
             PRAGMA foreign_keys=ON;\
             PRAGMA temp_store=MEMORY;",
        )?;
        match crate::schema::contents(&connection)? {
            Contents::Empty => connection.execute_batch(CREATE_SCHEMA)?,
            Contents::Unrecognised => return Err(ShardError::UnrecognisedContents),
            Contents::Store => {}
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
        connection.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, false)?;
        Ok(Self {
            connection,
            path: path.to_owned(),
            known_types,
            orphan_candidates: HashSet::new(),
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

        if let Err(error) = self.checkpoint_if_needed()
            && !error.is_capacity()
        {
            return Err(error);
        }
        Ok(CommitStats {
            items: items.len(),
            event_rows: items.iter().filter(|item| item.store_event).count()
                + items.iter().map(|item| item.gaps.len()).sum::<usize>(),
            receipt_rows: receipts.values().map(Vec::len).sum(),
        })
    }

    /// Durably describe event ranges consumed while storage was unavailable.
    ///
    /// Gap rows and their receipt ranges share one transaction so restart
    /// reconciliation never mistakes a deliberately reported loss for an
    /// unreported one.
    pub fn commit_gaps(
        &mut self,
        boot_id: &Guid,
        gaps: &[(u16, Gap)],
    ) -> Result<CommitStats, ShardError> {
        if gaps.is_empty() {
            return Ok(CommitStats::default());
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !self.known_types.contains("synthetic.gap") {
            transaction.execute(
                "INSERT OR IGNORE INTO event_types(event_type) VALUES ('synthetic.gap')",
                [],
            )?;
        }
        let receipt_rows;
        {
            let mut insert_gap = transaction.prepare_cached(
                "INSERT INTO events (boot_id, timestamp, cpu_id, event_type, payload) \
                 VALUES (?1, ?2, ?3, 'synthetic.gap', ?4)",
            )?;
            let mut receipts: HashMap<u16, Vec<Interval>> = HashMap::new();
            for (cpu_id, gap) in gaps {
                insert_gap.execute(params![
                    &boot_id[..],
                    sqlite_integer(gap.timestamp, "gap timestamp")?,
                    i64::from(*cpu_id),
                    encode_gap_payload(*cpu_id, *gap),
                ])?;
                receipts.entry(*cpu_id).or_default().push(Interval {
                    first: gap.first_sequence,
                    last: gap.last_sequence,
                });
            }
            drop(insert_gap);

            let mut insert_receipt = transaction.prepare_cached(
                "INSERT OR IGNORE INTO receipt_ranges \
                 (boot_id, cpu_id, first_sequence, last_sequence) VALUES (?1, ?2, ?3, ?4)",
            )?;
            for (cpu_id, intervals) in &mut receipts {
                merge_intervals(intervals);
                for interval in intervals.iter() {
                    insert_receipt.execute(params![
                        &boot_id[..],
                        i64::from(*cpu_id),
                        sqlite_integer(interval.first, "receipt first sequence")?,
                        sqlite_integer(interval.last, "receipt last sequence")?,
                    ])?;
                }
            }
            receipt_rows = receipts.values().map(Vec::len).sum();
        }
        transaction.commit()?;
        self.known_types.insert("synthetic.gap".into());
        if let Err(error) = self.checkpoint_if_needed()
            && !error.is_capacity()
        {
            return Err(error);
        }
        Ok(CommitStats {
            items: gaps.len(),
            event_rows: gaps.len(),
            receipt_rows,
        })
    }

    /// Replace this shard after `SQLite` reports corruption during a write.
    pub fn replace_corrupt(&mut self) -> Result<(), ShardError> {
        let path = self.path.clone();
        let checkpoint_pages = self.checkpoint_pages;
        let placeholder = Connection::open_in_memory()?;
        let connection = std::mem::replace(&mut self.connection, placeholder);
        // Closing would otherwise checkpoint the WAL into the corrupt
        // database and delete it, leaving quarantine nothing to move.
        let _ = connection.set_db_config(DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE, true);
        drop(connection);
        crate::quarantine::database(&path).map_err(ShardError::Io)?;
        *self = Self::open(path, checkpoint_pages)?;
        Ok(())
    }

    /// Commit one daemon-generated event in its own durability transaction.
    pub fn commit_synthetic(&mut self, event: &SyntheticEvent) -> Result<(), ShardError> {
        if !event.event_type.starts_with("synthetic.") {
            return Err(ShardError::InvalidSyntheticType);
        }
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        if !self.known_types.contains(event.event_type.as_ref()) {
            transaction.execute(
                "INSERT OR IGNORE INTO event_types(event_type) VALUES (?1)",
                [event.event_type.as_ref()],
            )?;
        }
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

    /// Delete at most `limit` event rows older than `cutoff`.
    pub fn retain_before(&mut self, cutoff: i64, limit: usize) -> Result<usize, ShardError> {
        self.delete_events(
            "DELETE FROM events WHERE id IN (SELECT id FROM events \
             WHERE timestamp < ?1 ORDER BY timestamp, id LIMIT ?2) RETURNING event_type",
            params![cutoff, sqlite_limit(limit)?],
        )
    }

    /// Delete at most `limit` rows belonging to one complete boot.
    pub fn retain_boot(&mut self, boot_id: &Guid, limit: usize) -> Result<usize, ShardError> {
        self.delete_events(
            "DELETE FROM events WHERE id IN (SELECT id FROM events \
             WHERE boot_id = ?1 ORDER BY timestamp, id LIMIT ?2) RETURNING event_type",
            params![&boot_id[..], sqlite_limit(limit)?],
        )
    }

    /// Remove catalogued types that retention's deletes left with no event.
    ///
    /// The candidates are the distinct types the committed delete batches
    /// touched. Each is rechecked with `NOT EXISTS` inside the deletion
    /// transaction, and uninterned only once that commits (§3.1). A stale
    /// catalogue row is safe, so the check is skipped, keeping its
    /// candidates for a later offer, when `idx_events_event_type` is not
    /// material (the recheck would scan the events table) or when `cancel`
    /// reports work waiting: cleanup never delays ingestion. Returns the
    /// number of types removed.
    pub fn remove_orphan_types<F>(&mut self, mut cancel: F) -> Result<usize, ShardError>
    where
        F: FnMut() -> bool + Send + 'static,
    {
        if self.orphan_candidates.is_empty()
            || cancel()
            || !self
                .material_indexes()?
                .iter()
                .any(|name| name == "idx_events_event_type")
        {
            return Ok(0);
        }
        let candidates: Vec<Box<str>> = self.orphan_candidates.iter().cloned().collect();
        self.connection.progress_handler(1_000, Some(cancel));
        let result = delete_orphans(&mut self.connection, &candidates);
        self.connection.progress_handler(0, None::<fn() -> bool>);
        let removed = match result {
            Ok(removed) => removed,
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::OperationInterrupted =>
            {
                return Ok(0);
            }
            Err(error) => return Err(error.into()),
        };
        // Every candidate was rechecked: the rest still have events.
        self.orphan_candidates.clear();
        for event_type in &removed {
            self.known_types.remove(event_type);
        }
        self.checkpoint_if_needed()?;
        Ok(removed.len())
    }

    /// Ask the sole writer connection to perform a passive checkpoint.
    pub fn passive_checkpoint(&self) -> Result<(), ShardError> {
        self.connection
            .execute_batch("PRAGMA wal_checkpoint(PASSIVE)")?;
        Ok(())
    }

    /// Move one step toward the global desired secondary-index set.
    pub fn converge_indexes<F>(
        &mut self,
        desired: &[DesiredIndex],
        cancel: F,
    ) -> Result<IndexAction, ShardError>
    where
        F: FnMut() -> bool + Send + 'static,
    {
        let material = self.material_indexes()?;
        self.converge_from_material(desired, &material, cancel)
    }

    /// Take one convergence step from a material-index set read earlier.
    fn converge_from_material<F>(
        &self,
        desired: &[DesiredIndex],
        material: &[String],
        cancel: F,
    ) -> Result<IndexAction, ShardError>
    where
        F: FnMut() -> bool + Send + 'static,
    {
        let wanted: Vec<_> = desired.iter().filter_map(adaptive_index).collect();
        let wanted_names: Vec<_> = wanted.iter().map(|(name, _)| name.clone()).collect();
        if let Some(name) = material
            .iter()
            .rev()
            .find(|name| !wanted_names.contains(name))
        {
            self.connection
                .execute_batch(&format!("DROP INDEX IF EXISTS {name}"))?;
            return Ok(IndexAction::Dropped(name.clone()));
        }
        for (name, expression) in wanted {
            if material.contains(&name) {
                continue;
            }
            self.connection.progress_handler(1_000, Some(cancel));
            let result = self.connection.execute_batch(&format!(
                "CREATE INDEX IF NOT EXISTS {name} ON events({expression})"
            ));
            self.connection.progress_handler(0, None::<fn() -> bool>);
            match result {
                Ok(()) => return Ok(IndexAction::Created(name)),
                Err(rusqlite::Error::SqliteFailure(error, _))
                    if error.code == rusqlite::ErrorCode::OperationInterrupted =>
                {
                    return Ok(IndexAction::Cancelled);
                }
                Err(error) => return Err(error.into()),
            }
        }
        Ok(IndexAction::Unchanged)
    }

    /// Drop every adaptive secondary index, retaining the timestamp index.
    pub fn shed_all_indexes(&mut self) -> Result<usize, ShardError> {
        let material = self.material_indexes()?;
        for name in &material {
            self.connection
                .execute_batch(&format!("DROP INDEX IF EXISTS {name}"))?;
        }
        Ok(material.len())
    }

    /// Drop the lowest-priority currently materialized desired index.
    pub fn shed_lowest_index(
        &mut self,
        desired: &[DesiredIndex],
    ) -> Result<Option<String>, ShardError> {
        let material = self.material_indexes()?;
        let candidate = desired.iter().rev().find_map(|index| {
            adaptive_index(index)
                .map(|(name, _)| name)
                .filter(|name| material.contains(name))
        });
        let Some(name) = candidate else {
            return Ok(None);
        };
        self.connection
            .execute_batch(&format!("DROP INDEX IF EXISTS {name}"))?;
        Ok(Some(name))
    }

    fn material_indexes(&self) -> Result<Vec<String>, ShardError> {
        let mut statement = self.connection.prepare(
            "SELECT name FROM sqlite_master WHERE type = 'index' \
             AND name LIKE 'idx_events_%' AND name <> 'idx_events_timestamp' ORDER BY name",
        )?;
        statement
            .query_map([], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(ShardError::Sql)
    }

    /// Run one bounded `DELETE … RETURNING event_type`, recording the
    /// distinct types it touched as orphan candidates once it commits.
    fn delete_events(
        &mut self,
        sql: &str,
        parameters: impl rusqlite::Params,
    ) -> Result<usize, ShardError> {
        let mut deleted = 0;
        let mut touched = HashSet::<Box<str>>::new();
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        {
            let mut statement = transaction.prepare(sql)?;
            let mut rows = statement.query(parameters)?;
            while let Some(row) = rows.next()? {
                deleted += 1;
                let event_type = row
                    .get_ref(0)?
                    .as_str()
                    .map_err(|_| ShardError::InvalidSchema("event_type is not text"))?;
                if !touched.contains(event_type) {
                    touched.insert(event_type.into());
                }
            }
        }
        transaction.commit()?;
        self.orphan_candidates.extend(touched);
        self.checkpoint_if_needed()?;
        Ok(deleted)
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
        crate::payload_index::register(&connection)?;
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
        crate::payload_index::register(&connection)?;
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

    /// Cap this connection's database at its current page count, so that a
    /// write needing a new page fails as a full disk does (`SQLITE_FULL`).
    /// A test seam for the capacity paths above the shard; eventd never
    /// calls it.
    #[doc(hidden)]
    pub fn cap_pages_for_test(&self) -> Result<(), ShardError> {
        // SQLite clamps the maximum to the current size.
        self.connection
            .query_row("PRAGMA max_page_count = 1", [], |row| row.get::<_, i64>(0))?;
        Ok(())
    }

    /// The `-wal` file's size measures the log because `journal_size_limit=0`
    /// truncates it when a commit restarts the log after a checkpoint;
    /// otherwise `SQLite` reuses the file at its high-water size.
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

fn header_index_name(field: &str) -> Option<&'static str> {
    match field {
        "event_type" => Some("idx_events_event_type"),
        "origin_class" => Some("idx_events_origin_class"),
        "cpu_id" => Some("idx_events_cpu_id"),
        "effective_token_guid" => Some("idx_events_effective_token_guid"),
        "true_token_guid" => Some("idx_events_true_token_guid"),
        "process_guid" => Some("idx_events_process_guid"),
        "boot_id" => Some("idx_events_boot_id"),
        _ => None,
    }
}

fn adaptive_index(index: &DesiredIndex) -> Option<(String, String)> {
    if index.is_expression {
        Some((
            crate::payload_index_name(&index.field_path)?,
            crate::payload_index::expression(&index.field_path)?,
        ))
    } else {
        let name = header_index_name(&index.field_path)?;
        let column = name.strip_prefix("idx_events_")?;
        let collation = if column == "event_type" {
            " COLLATE NOCASE"
        } else {
            ""
        };
        Some((name.to_owned(), format!("{column}{collation}")))
    }
}

/// Delete each candidate type still without an event, in one transaction.
/// `COLLATE NOCASE` lets the recheck use `idx_events_event_type`; a type
/// that differs from a stored one only in case is kept, which is safe.
fn delete_orphans(
    connection: &mut Connection,
    candidates: &[Box<str>],
) -> rusqlite::Result<Vec<Box<str>>> {
    let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let mut removed = Vec::new();
    {
        let mut delete = transaction.prepare(ORPHAN_DELETE)?;
        for candidate in candidates {
            if delete.execute([candidate.as_ref()])? != 0 {
                removed.push(candidate.clone());
            }
        }
    }
    transaction.commit()?;
    Ok(removed)
}

const ORPHAN_DELETE: &str = "DELETE FROM event_types WHERE event_type = ?1 \
     AND NOT EXISTS (SELECT 1 FROM events WHERE event_type = ?1 COLLATE NOCASE)";

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

fn sqlite_limit(limit: usize) -> Result<i64, ShardError> {
    i64::try_from(limit).map_err(|_| ShardError::IntegerRange("retention batch size"))
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
    /// Schema objects without the metadata entries of an event shard.
    UnrecognisedContents,
    /// An unsigned kernel value cannot fit `SQLite`'s signed `INTEGER`.
    IntegerRange(&'static str),
    /// The direct-write API was given a non-synthetic event type.
    InvalidSyntheticType,
}

impl ShardError {
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
    /// contents that are not an event shard at all.
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
            Self::UnrecognisedContents => {
                formatter.write_str("unrecognised event shard contents: no metadata entries")
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
            | Self::UnrecognisedContents
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

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
        assert_eq!(shard.retain_before(5, 1).unwrap(), 1);
        assert_eq!(
            shard.receipts().unwrap()[0].2,
            Interval { first: 1, last: 5 }
        );
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn converges_one_header_index_step_at_a_time() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = Shard::open(&path, 1_000).unwrap();
        let desired = [DesiredIndex {
            field_path: "event_type".into(),
            priority: 0,
            is_expression: false,
        }];
        assert_eq!(
            shard.converge_indexes(&desired, || false).unwrap(),
            IndexAction::Created("idx_events_event_type".into())
        );
        assert_eq!(
            shard.converge_indexes(&desired, || false).unwrap(),
            IndexAction::Unchanged
        );
        assert_eq!(
            shard.converge_indexes(&[], || false).unwrap(),
            IndexAction::Dropped("idx_events_event_type".into())
        );
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn converges_and_maintains_payload_expression_index() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = Shard::open(&path, 1_000).unwrap();
        let mut first = event(1, "example.payload");
        first.event.payload = [
            0x81, 0xa6, b's', b'o', b'u', b'r', b'c', b'e', 0x81, 0xa4, b'n', b'a', b'm', b'e',
            0xa5, b'A', b'l', b'p', b'h', b'a',
        ]
        .into();
        shard.commit(&[first]).unwrap();
        let desired = [DesiredIndex {
            field_path: "source.name".into(),
            priority: 0,
            is_expression: true,
        }];
        let index_name = crate::payload_index_name("source.name").unwrap();
        assert_eq!(
            shard.converge_indexes(&desired, || false).unwrap(),
            IndexAction::Created(index_name.clone())
        );

        let mut second = event(2, "example.payload");
        second.event.payload = [
            0x81, 0xa6, b's', b'o', b'u', b'r', b'c', b'e', 0x81, 0xa4, b'n', b'a', b'm', b'e',
            0xa4, b'b', b'e', b't', b'a',
        ]
        .into();
        shard.commit(&[second]).unwrap();
        let key = crate::payload_query_key(crate::PayloadIndexValue::String("alpha"));
        let count: u32 = shard
            .connection
            .query_row(
                "SELECT count(*) FROM events \
                 WHERE eventd_payload_key(payload, 'source.name') = ?1",
                [&key],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(shard.shed_lowest_index(&desired).unwrap(), Some(index_name));
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

    #[test]
    fn synthetic_and_gap_commits_catalogue_a_type_only_while_it_is_unknown() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = Shard::open(&path, 1_000).unwrap();
        // A BEFORE trigger fires for every row an INSERT OR IGNORE attempts,
        // so it counts catalogue statements, not rows added.
        shard
            .connection
            .execute_batch(
                "CREATE TEMP TABLE type_inserts (event_type TEXT NOT NULL);\
                 CREATE TEMP TRIGGER count_type_inserts BEFORE INSERT ON main.event_types \
                 BEGIN INSERT INTO type_inserts(event_type) VALUES (NEW.event_type); END;",
            )
            .unwrap();
        let startup = SyntheticEvent {
            boot_id: [1; 16],
            timestamp: 42,
            event_type: "synthetic.startup".into(),
            payload: [0x80].into(),
        };
        shard.commit_synthetic(&startup).unwrap();
        shard.commit_synthetic(&startup).unwrap();
        let gap = |first_sequence| Gap {
            timestamp: 40,
            first_sequence,
            last_sequence: first_sequence,
            preceding_timestamp: None,
            revealing_timestamp: 60,
        };
        shard.commit_gaps(&[1; 16], &[(2, gap(4))]).unwrap();
        shard.commit_gaps(&[1; 16], &[(2, gap(5))]).unwrap();
        let attempts: Vec<(String, u32)> = {
            let mut statement = shard
                .connection
                .prepare(
                    "SELECT event_type, count(*) FROM type_inserts \
                     GROUP BY event_type ORDER BY event_type",
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
            [
                ("synthetic.gap".to_owned(), 1),
                ("synthetic.startup".to_owned(), 1)
            ]
        );
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    fn catalogued(shard: &Shard, event_type: &str) -> bool {
        shard
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM event_types WHERE event_type = ?1)",
                [event_type],
                |row| row.get(0),
            )
            .unwrap()
    }

    /// Events at timestamp 1 of `old.type` and of `orphans` more types, and
    /// of `kept.type` at timestamps 1 and 100000, in that id order.
    fn shard_with_orphans_to_be(path: &Path, orphans: u64) -> Shard {
        let mut shard = Shard::open(path, 1_000).unwrap();
        let mut items: Vec<_> = core::iter::once("old.type".to_owned())
            .chain((0..orphans).map(|index| format!("old.type{index}")))
            .chain(["kept.type".to_owned()])
            .enumerate()
            .map(|(index, event_type)| {
                let mut item = event(index as u64 + 1, &event_type);
                item.event.timestamp = 1;
                item
            })
            .collect();
        items.push(event(100_000, "kept.type"));
        shard.commit(&items).unwrap();
        shard
    }

    #[test]
    fn retention_removes_a_type_its_deletes_orphaned_and_uninterns_it_after_commit() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = shard_with_orphans_to_be(&path, 0);
        shard
            .converge_indexes(&event_type_index(), || false)
            .unwrap();
        // The recheck is answered from the index, not by scanning events.
        let plan = {
            let mut statement = shard
                .connection
                .prepare(&format!("EXPLAIN QUERY PLAN {ORPHAN_DELETE}"))
                .unwrap();
            statement
                .query_map(["old.type"], |row| row.get::<_, String>(3))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
                .join("; ")
        };
        assert!(plan.contains("idx_events_event_type"), "{plan}");

        assert_eq!(shard.retain_before(10, 100).unwrap(), 2);
        assert_eq!(shard.remove_orphan_types(|| false).unwrap(), 1);
        assert!(!catalogued(&shard, "old.type"));
        assert!(
            catalogued(&shard, "kept.type"),
            "a type with an event left stays"
        );
        // Uninterned: its next event catalogues it again.
        shard.commit(&[event(200, "old.type")]).unwrap();
        assert!(catalogued(&shard, "old.type"));
        // The candidates were all checked; nothing is left to offer.
        assert_eq!(shard.remove_orphan_types(|| false).unwrap(), 0);
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn an_unindexed_or_interrupted_orphan_check_is_skipped_and_offered_again() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        // Enough candidates for the deletion transaction to reach the
        // progress handler.
        let mut shard = shard_with_orphans_to_be(&path, 300);
        assert_eq!(shard.retain_boot(&[1; 16], 302).unwrap(), 302);
        // No idx_events_event_type: skipped.
        assert_eq!(shard.remove_orphan_types(|| false).unwrap(), 0);
        assert!(catalogued(&shard, "old.type"));
        shard
            .converge_indexes(&event_type_index(), || false)
            .unwrap();
        // Work waiting: skipped.
        assert_eq!(shard.remove_orphan_types(|| true).unwrap(), 0);
        assert!(catalogued(&shard, "old.type"));
        // Interrupted part-way through the deletion transaction: rolled back.
        let checks = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&checks);
        assert_eq!(
            shard
                .remove_orphan_types(move || counter.fetch_add(1, Ordering::Relaxed) > 0)
                .unwrap(),
            0
        );
        assert!(
            checks.load(Ordering::Relaxed) > 1,
            "the progress handler ran"
        );
        assert!(catalogued(&shard, "old.type"));
        assert!(shard.connection.is_autocommit());
        // Still candidates, so the next offer removes them all.
        assert_eq!(shard.remove_orphan_types(|| false).unwrap(), 301);
        assert!(!catalogued(&shard, "old.type"));
        assert!(catalogued(&shard, "kept.type"));
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn commits_recovery_gaps_and_receipts_atomically() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = Shard::open(&path, 1_000).unwrap();
        let gaps = [
            (
                2,
                Gap {
                    timestamp: 40,
                    first_sequence: 4,
                    last_sequence: 5,
                    preceding_timestamp: Some(30),
                    revealing_timestamp: 60,
                },
            ),
            (
                2,
                Gap {
                    timestamp: 60,
                    first_sequence: 6,
                    last_sequence: 6,
                    preceding_timestamp: None,
                    revealing_timestamp: 60,
                },
            ),
        ];
        let stats = shard.commit_gaps(&[1; 16], &gaps).unwrap();
        assert_eq!(stats.event_rows, 2);
        assert_eq!(stats.receipt_rows, 1);
        assert_eq!(
            shard.receipts().unwrap()[0].2,
            Interval { first: 4, last: 6 }
        );
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn classifies_sqlite_full_as_capacity_failure() {
        let error = ShardError::Sql(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_FULL),
            None,
        ));
        assert!(error.is_capacity());
    }

    #[test]
    fn quarantines_and_replaces_a_corrupt_required_shard() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        std::fs::write(&path, b"not a sqlite database").unwrap();
        let (mut shard, recovery) = Shard::open_recovering(&path, 1_000).unwrap();
        assert!(recovery.is_some());
        shard.commit(&[event(1, "test.after-recovery")]).unwrap();
        assert!(std::fs::read_dir(&directory).unwrap().any(|entry| {
            entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("shard-0000.db.corrupt.")
        }));
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// The WAL header's salt, which changes whenever a writer restarts the
    /// log after a checkpoint.
    fn wal_salt(path: &Path) -> Vec<u8> {
        let mut wal = path.as_os_str().to_owned();
        wal.push("-wal");
        std::fs::read(PathBuf::from(wal)).unwrap()[16..24].to_vec()
    }

    #[test]
    fn a_wal_below_the_threshold_is_not_checkpointed_after_the_first_crossing() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = Shard::open(&path, 10).unwrap();
        let mut sequence = 0;
        let mut batch = |count: u64| -> Vec<IngestItem> {
            (0..count)
                .map(|_| {
                    sequence += 1;
                    let mut item = event(sequence, "example.test");
                    item.event.payload = [0xc4, 200].into_iter().chain([0; 200]).collect();
                    item
                })
                .collect()
        };
        let first = wal_salt(&path);
        while wal_salt(&path) == first {
            shard.commit(&batch(100)).unwrap();
        }
        shard.commit(&batch(1)).unwrap();
        let restarted = wal_salt(&path);
        shard.commit(&batch(1)).unwrap();
        shard
            .commit_synthetic(&SyntheticEvent {
                boot_id: [1; 16],
                timestamp: 1,
                event_type: "synthetic.startup".into(),
                payload: [0x80].into(),
            })
            .unwrap();
        assert_eq!(wal_salt(&path), restarted);
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    fn quarantined(directory: &Path) -> Vec<String> {
        std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains(".corrupt."))
            .collect()
    }

    #[test]
    fn quarantine_keeps_the_wal_and_shm_byte_for_byte_under_the_databases_suffix() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let originals = [
            ("shard-0000.db", "garbage database ".repeat(500)),
            ("shard-0000.db-wal", "garbage wal ".repeat(300)),
            ("shard-0000.db-shm", "garbage shm ".repeat(3000)),
        ];
        for (name, body) in &originals {
            std::fs::write(directory.join(name), body).unwrap();
        }
        let (shard, recovery) = Shard::open_recovering(&path, 1_000).unwrap();
        assert!(recovery.is_some());
        drop(shard);
        let quarantined = quarantined(&directory);
        let suffix = quarantined
            .iter()
            .find_map(|name| name.strip_prefix("shard-0000.db.corrupt"))
            .unwrap();
        for (name, body) in &originals {
            assert_eq!(
                std::fs::read_to_string(directory.join(format!("{name}.corrupt{suffix}"))).unwrap(),
                *body,
                "{name} in {quarantined:?}"
            );
        }
        let leftovers: Vec<_> = std::fs::read_dir(&directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains("preserved"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_shard_whose_creation_was_lost_is_created_afresh() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        Connection::open(&path)
            .unwrap()
            .execute_batch("PRAGMA journal_mode=WAL;")
            .unwrap();
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 4096);
        let (mut shard, recovery) = Shard::open_recovering(&path, 1_000).unwrap();
        assert_eq!(recovery, None);
        shard.commit(&[event(1, "example.test")]).unwrap();
        assert!(quarantined(&directory).is_empty());
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_partly_created_shard_is_quarantined_but_a_missing_schema_version_fails() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        for partial in [
            "CREATE TABLE events (id INTEGER PRIMARY KEY);",
            "CREATE TABLE events (id INTEGER PRIMARY KEY);\
             CREATE TABLE metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID;",
        ] {
            Connection::open(&path)
                .unwrap()
                .execute_batch(partial)
                .unwrap();
            let before = quarantined(&directory).len();
            let (shard, recovery) = Shard::open_recovering(&path, 1_000).unwrap();
            assert!(recovery.is_some(), "{partial}");
            assert_eq!(quarantined(&directory).len(), before + 1, "{partial}");
            drop(shard);
            std::fs::remove_file(&path).unwrap();
        }
        drop(Shard::open(&path, 1_000).unwrap());
        Connection::open(&path)
            .unwrap()
            .execute_batch("DELETE FROM metadata WHERE key = 'schema_version';")
            .unwrap();
        let before = quarantined(&directory).len();
        assert!(matches!(
            Shard::open_recovering(&path, 1_000),
            Err(ShardError::InvalidSchema(_))
        ));
        assert_eq!(quarantined(&directory).len(), before);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_new_shard_is_created_with_synchronous_full() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        assert!(!path.exists());
        let shard = Shard::open(&path, 1_000).unwrap();
        assert_eq!(synchronous(&shard), FULL);
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn an_active_shard_is_opened_with_synchronous_full() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = Shard::open(&path, 1_000).unwrap();
        shard.commit(&[event(1, "example.test")]).unwrap();
        drop(shard);
        assert!(path.exists());
        let shard = Shard::open(&path, 1_000).unwrap();
        assert_eq!(synchronous(&shard), FULL);
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_real_event_without_identity_stores_the_null_guid_not_null() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = Shard::open(&path, 1_000).unwrap();
        let mut anonymous = event(1, "example.anonymous");
        anonymous.event.effective_token_guid = [0; 16];
        shard.commit(&[anonymous]).unwrap();
        let (kind, value): (String, Vec<u8>) = shard
            .connection
            .query_row(
                "SELECT typeof(effective_token_guid), effective_token_guid FROM events \
                 WHERE event_type = 'example.anonymous'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .unwrap();
        assert_eq!(kind, "blob");
        assert_eq!(value, [0; 16]);
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_cancelled_index_build_leaves_no_index_and_the_writer_takes_the_next_batch() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = populated(&path, 2_000);
        let checks = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&checks);
        let action = shard
            .converge_indexes(&event_type_index(), move || {
                counter.fetch_add(1, Ordering::Relaxed);
                true
            })
            .unwrap();
        assert_eq!(action, IndexAction::Cancelled);
        assert_eq!(
            checks.load(Ordering::Relaxed),
            1,
            "cancelled at the first check"
        );
        assert!(!index_exists(&shard, "idx_events_event_type"));
        assert!(
            shard.connection.is_autocommit(),
            "the build was rolled back"
        );
        let integrity: String = shard
            .connection
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap();
        assert_eq!(integrity, "ok");
        let stats = shard.commit(&[event(2_001, "example.after")]).unwrap();
        assert_eq!(stats.event_rows, 1);
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_cancelled_index_build_is_created_when_retried() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = populated(&path, 2_000);
        assert_eq!(
            shard
                .converge_indexes(&event_type_index(), || true)
                .unwrap(),
            IndexAction::Cancelled
        );
        assert_eq!(
            shard
                .converge_indexes(&event_type_index(), || false)
                .unwrap(),
            IndexAction::Created("idx_events_event_type".into())
        );
        assert!(index_exists(&shard, "idx_events_event_type"));
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn an_index_build_checks_for_cancellation_every_thousand_opcodes() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = populated(&path, 10_000);
        let checks = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&checks);
        let (name, expression) = adaptive_index(&event_type_index()[0]).unwrap();
        assert_eq!(
            shard
                .converge_indexes(&event_type_index(), move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                    false
                })
                .unwrap(),
            IndexAction::Created(name.clone())
        );
        let checks = checks.load(Ordering::Relaxed);

        // Build the same index over the same rows again, unobserved, and
        // read how many VM opcodes that statement executes.
        shard
            .connection
            .execute_batch(&format!("DROP INDEX {name}"))
            .unwrap();
        let mut statement = shard
            .connection
            .prepare(&format!(
                "CREATE INDEX IF NOT EXISTS {name} ON events({expression})"
            ))
            .unwrap();
        statement.raw_execute().unwrap();
        let opcodes =
            usize::try_from(statement.get_status(rusqlite::StatementStatus::VmStep)).unwrap();
        drop(statement);
        assert!(
            opcodes > 10_000,
            "a build long enough to measure: {opcodes}"
        );
        assert!(
            (opcodes / 1_000..=opcodes / 1_000 + 1).contains(&checks),
            "{checks} cancellation checks for {opcodes} opcodes"
        );
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn index_convergence_tolerates_an_index_created_or_dropped_since_its_material_read() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let shard = populated(&path, 100);
        let (name, expression) = adaptive_index(&event_type_index()[0]).unwrap();
        let other = Connection::open(&path).unwrap();

        // The writer reads its material set, then another connection creates
        // the index under the same name before the writer's create runs.
        let material = shard.material_indexes().unwrap();
        assert!(material.is_empty());
        other
            .execute_batch(&format!("CREATE INDEX {name} ON events({expression})"))
            .unwrap();
        assert_eq!(
            shard
                .converge_from_material(&event_type_index(), &material, || false)
                .unwrap(),
            IndexAction::Created(name.clone())
        );
        assert!(index_exists(&shard, &name));

        // Likewise the index disappears between the read and the drop.
        let material = shard.material_indexes().unwrap();
        assert_eq!(material, core::slice::from_ref(&name));
        other.execute_batch(&format!("DROP INDEX {name}")).unwrap();
        assert_eq!(
            shard
                .converge_from_material(&[], &material, || false)
                .unwrap(),
            IndexAction::Dropped(name.clone())
        );
        assert!(!index_exists(&shard, &name));
        drop(other);
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_commit_waits_out_a_write_lock_held_for_a_moment() {
        // A query connection that catches the wal-index header mid-update
        // takes the write lock to read it again (PEI-1359). Another
        // connection's IMMEDIATE transaction holds the same lock here, and
        // releases it only once the writer is about to commit, so the commit
        // usually begins while the lock is held.
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = Shard::open(&path, 1_000).unwrap();
        for sequence in 1..=20 {
            let (held_sender, held) = std::sync::mpsc::channel();
            let (release, release_receiver) = std::sync::mpsc::channel();
            let path = &path;
            std::thread::scope(|scope| {
                scope.spawn(move || {
                    let holder = Connection::open(path).unwrap();
                    holder.execute_batch("BEGIN IMMEDIATE").unwrap();
                    held_sender.send(()).unwrap();
                    release_receiver.recv().unwrap();
                    holder.execute_batch("ROLLBACK").unwrap();
                });
                held.recv().unwrap();
                release.send(()).unwrap();
                shard.commit(&[event(sequence, "example.kind")]).unwrap();
            });
        }
        let committed: i64 = shard
            .connection
            .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(committed, 20);
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_commit_fails_once_the_write_lock_is_held_past_its_bounded_wait() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let mut shard = Shard::open(&path, 1_000).unwrap();
        let holder = Connection::open(&path).unwrap();
        holder.execute_batch("BEGIN IMMEDIATE").unwrap();

        let started = std::time::Instant::now();
        let error = shard.commit(&[event(1, "example.kind")]).unwrap_err();
        assert!(
            matches!(&error, ShardError::Sql(cause)
                if cause.sqlite_error_code() == Some(rusqlite::ErrorCode::DatabaseBusy)),
            "{error}"
        );
        assert!(started.elapsed() >= crate::writer_lock::WRITER_LOCK_WAIT);

        holder.execute_batch("ROLLBACK").unwrap();
        shard.commit(&[event(1, "example.kind")]).unwrap();
        drop(holder);
        drop(shard);
        std::fs::remove_dir_all(directory).unwrap();
    }

    const FULL: i64 = 2;

    fn event_type_index() -> [DesiredIndex; 1] {
        [DesiredIndex {
            field_path: "event_type".into(),
            priority: 0,
            is_expression: false,
        }]
    }

    fn synchronous(shard: &Shard) -> i64 {
        shard
            .connection
            .query_row("PRAGMA synchronous", [], |row| row.get(0))
            .unwrap()
    }

    fn populated(path: &Path, rows: u64) -> Shard {
        let mut shard = Shard::open(path, 1_000).unwrap();
        let batch: Vec<_> = (1..=rows)
            .map(|sequence| event(sequence, &format!("example.kind{}", sequence % 7)))
            .collect();
        shard.commit(&batch).unwrap();
        shard
    }

    fn index_exists(shard: &Shard, name: &str) -> bool {
        shard
            .connection
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE name = ?1)",
                [name],
                |row| row.get(0),
            )
            .unwrap()
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
