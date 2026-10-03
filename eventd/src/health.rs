//! eventd's own health, sampled into its metric store as `eventd.*`
//! (TRM §5.7). Only conditions eventd observes about itself are counted
//! here: rejected ingestion input stays in `diagnostics`, because counting
//! it where a query client can see it is forbidden (PSPU §3.4, §3.12).

use core::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use eventd_core::{MetricRecord, MetricType, MetricValue};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Store {
    Events,
    Logs,
    Metrics,
    Metadata,
}

impl Store {
    const fn label(self) -> &'static str {
        match self {
            Self::Events => "store=events",
            Self::Logs => "store=logs",
            Self::Metrics => "store=metrics",
            Self::Metadata => "store=metadata",
        }
    }
}

const STORES: [Store; 4] = [Store::Events, Store::Logs, Store::Metrics, Store::Metadata];

#[derive(Debug, Clone, Copy)]
pub enum Shed {
    Pressure,
    Emergency,
}

#[derive(Debug, Clone, Copy)]
pub enum Refusal {
    Machine,
    Streaming,
    User,
}

#[derive(Debug, Clone, Copy)]
pub enum Failure {
    Timeout,
    HeldBytes,
}

static LOGS_STORED: AtomicU64 = AtomicU64::new(0);
static METRICS_STORED: AtomicU64 = AtomicU64::new(0);
static SHEDS: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];
static WRITE_ERRORS: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
static RETENTION_DELETED: [AtomicU64; 4] = [const { AtomicU64::new(0) }; 4];
static REFUSED: [AtomicU64; 3] = [const { AtomicU64::new(0) }; 3];
static FAILED: [AtomicU64; 2] = [const { AtomicU64::new(0) }; 2];

/// What is known only once the daemon has attached and bound: its shards,
/// its CPUs, the drains' ring pressure, the query slots and the files
/// each store occupies.
pub struct Layout {
    pub shards: usize,
    /// Logical CPU IDs, in ring-pressure slot order.
    pub cpus: Vec<u16>,
    pub ring_pressure: Arc<[AtomicU8]>,
    pub active_queries: Arc<AtomicUsize>,
    pub streaming_queries: Arc<AtomicUsize>,
    pub event_paths: Vec<PathBuf>,
    pub log_path: PathBuf,
    pub metric_path: PathBuf,
    pub metadata_path: PathBuf,
}

struct Started {
    layout: Layout,
    events_stored: Box<[AtomicU64]>,
    events_lost: Box<[AtomicU64]>,
}

static STARTED: OnceLock<Started> = OnceLock::new();

/// Record the daemon's layout. Counts that need it are dropped until then.
pub fn start(layout: Layout) {
    let _ = STARTED.set(Started {
        events_stored: (0..layout.shards).map(|_| AtomicU64::new(0)).collect(),
        events_lost: layout.cpus.iter().map(|_| AtomicU64::new(0)).collect(),
        layout,
    });
}

fn add(counter: &AtomicU64, count: usize) {
    counter.fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
}

pub fn events_stored(shard: usize, count: usize) {
    if let Some(counter) = STARTED
        .get()
        .and_then(|started| started.events_stored.get(shard))
    {
        add(counter, count);
    }
}

/// Sequences recorded as lost on one CPU: a KMES overrun, or a batch the
/// store could not take (§2.5, §9.1).
pub fn events_lost(cpu_id: u16, sequences: u64) {
    let Some(started) = STARTED.get() else {
        return;
    };
    if let Some(slot) = started.layout.cpus.iter().position(|&cpu| cpu == cpu_id) {
        started.events_lost[slot].fetch_add(sequences, Ordering::Relaxed);
    }
}

pub fn index_shed(reason: Shed, indexes: usize) {
    add(&SHEDS[reason as usize], indexes);
}

pub fn logs_stored(count: usize) {
    add(&LOGS_STORED, count);
}

pub fn metrics_stored(count: usize) {
    add(&METRICS_STORED, count);
}

pub fn write_error(store: Store) {
    add(&WRITE_ERRORS[store as usize], 1);
}

pub fn retention_deleted(store: Store, rows: usize) {
    add(&RETENTION_DELETED[store as usize], rows);
}

pub fn query_refused(reason: Refusal) {
    add(&REFUSED[reason as usize], 1);
}

pub fn query_failed(reason: Failure) {
    add(&FAILED[reason as usize], 1);
}

/// One sample of every health series, stamped `timestamp`.
pub fn sample(boot_id: [u8; 16], timestamp: i64, series_cached: usize) -> Vec<MetricRecord> {
    let mut samples = Samples {
        boot_id,
        timestamp,
        records: Vec::with_capacity(64),
    };
    if let Some(started) = STARTED.get() {
        samples.layout(started);
    }
    samples.counts(series_cached);
    samples.records
}

struct Samples {
    boot_id: [u8; 16],
    timestamp: i64,
    records: Vec<MetricRecord>,
}

impl Samples {
    fn push(&mut self, name: &str, labels: &str, metric_type: MetricType, value: u64) {
        #[allow(
            clippy::cast_precision_loss,
            reason = "a binary64 metric value is the interface's only numeric form (PSPU §3.10)"
        )]
        let value = value as f64;
        self.records.push(MetricRecord {
            boot_id: self.boot_id,
            timestamp: self.timestamp,
            name: name.into(),
            labels: labels.into(),
            metric_type,
            value: MetricValue::Number(value),
        });
    }

    fn counter(&mut self, name: &str, labels: &str, value: &AtomicU64) {
        self.push(
            name,
            labels,
            MetricType::Counter,
            value.load(Ordering::Relaxed),
        );
    }

    fn gauge(&mut self, name: &str, labels: &str, value: u64) {
        self.push(name, labels, MetricType::Gauge, value);
    }

    fn layout(&mut self, started: &Started) {
        let layout = &started.layout;
        for (shard, stored) in started.events_stored.iter().enumerate() {
            self.counter("eventd.events.stored", &format!("shard={shard}"), stored);
        }
        for (slot, cpu) in layout.cpus.iter().enumerate() {
            let labels = format!("cpu={cpu}");
            self.counter("eventd.events.lost", &labels, &started.events_lost[slot]);
            let fill = layout
                .ring_pressure
                .get(slot)
                .map_or(0, |pressure| pressure.load(Ordering::Acquire));
            self.gauge("eventd.kmes.ring.fill.percent", &labels, u64::from(fill));
        }
        for (name, running) in [
            ("eventd.queries.active", &layout.active_queries),
            ("eventd.queries.streaming", &layout.streaming_queries),
        ] {
            self.gauge(name, "", size(running.load(Ordering::Acquire)));
        }
        let files = [
            (Store::Events, layout.event_paths.as_slice()),
            (Store::Logs, core::slice::from_ref(&layout.log_path)),
            (Store::Metrics, core::slice::from_ref(&layout.metric_path)),
            (
                Store::Metadata,
                core::slice::from_ref(&layout.metadata_path),
            ),
        ];
        for (store, paths) in files {
            let bytes = paths.iter().map(|path| bytes_on_disk(path)).sum();
            self.gauge("eventd.store.bytes", store.label(), bytes);
        }
    }

    fn counts(&mut self, series_cached: usize) {
        for (reason, sheds) in ["reason=pressure", "reason=emergency"]
            .into_iter()
            .zip(&SHEDS)
        {
            self.counter("eventd.events.index.sheds", reason, sheds);
        }
        self.counter("eventd.logs.stored", "", &LOGS_STORED);
        self.counter("eventd.metrics.stored", "", &METRICS_STORED);
        self.gauge("eventd.metrics.series.cached", "", size(series_cached));
        for store in STORES {
            self.counter(
                "eventd.store.write.errors",
                store.label(),
                &WRITE_ERRORS[store as usize],
            );
        }
        // Retention never touches the metadata store (§3.5).
        for store in [Store::Events, Store::Logs, Store::Metrics] {
            self.counter(
                "eventd.retention.deleted",
                store.label(),
                &RETENTION_DELETED[store as usize],
            );
        }
        for (reason, refused) in ["reason=machine", "reason=streaming", "reason=user"]
            .into_iter()
            .zip(&REFUSED)
        {
            self.counter("eventd.queries.refused", reason, refused);
        }
        for (reason, failed) in ["reason=timeout", "reason=held_bytes"]
            .into_iter()
            .zip(&FAILED)
        {
            self.counter("eventd.queries.failed", reason, failed);
        }
    }
}

fn size(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// A database's bytes on disk, with its write-ahead log. A file that is
/// not there counts nothing.
fn bytes_on_disk(path: &Path) -> u64 {
    let mut wal = path.as_os_str().to_owned();
    wal.push("-wal");
    [path, Path::new(&wal)]
        .iter()
        .filter_map(|file| std::fs::metadata(file).ok())
        .map(|metadata| metadata.len())
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sample_names_every_series_once_and_validly() {
        let records = sample([7; 16], 1_000, 3);
        let mut seen = std::collections::HashSet::new();
        for record in &records {
            assert!(record.name.starts_with("eventd."), "{}", record.name);
            assert!(
                seen.insert((record.name.clone(), record.labels.clone())),
                "{} {{{}}} twice",
                record.name,
                record.labels
            );
            assert_eq!(record.timestamp, 1_000);
            let MetricValue::Number(value) = record.value else {
                panic!("health values are numbers");
            };
            assert!(value.is_finite() && value >= 0.0);
        }
        let cached = records
            .iter()
            .find(|record| record.name.as_ref() == "eventd.metrics.series.cached")
            .expect("series cache gauge");
        assert_eq!(cached.value, MetricValue::Number(3.0));
        assert!(
            records
                .iter()
                .any(|record| record.name.as_ref() == "eventd.queries.refused"
                    && record.labels.as_ref() == "reason=user")
        );
    }

    #[test]
    fn counts_accumulate() {
        let before = REFUSED[Refusal::Streaming as usize].load(Ordering::Relaxed);
        query_refused(Refusal::Streaming);
        query_refused(Refusal::Streaming);
        assert_eq!(
            REFUSED[Refusal::Streaming as usize].load(Ordering::Relaxed),
            before + 2
        );
    }

    #[test]
    fn bytes_on_disk_counts_the_write_ahead_log() {
        let directory = std::env::temp_dir().join(format!("eventd-health-{}", std::process::id()));
        std::fs::create_dir_all(&directory).expect("directory");
        let database = directory.join("logs.db");
        std::fs::write(&database, [0; 100]).expect("database");
        std::fs::write(directory.join("logs.db-wal"), [0; 20]).expect("wal");
        assert_eq!(bytes_on_disk(&database), 120);
        assert_eq!(bytes_on_disk(&directory.join("absent.db")), 0);
        std::fs::remove_dir_all(&directory).expect("clean up");
    }
}
