//! Bounded low-priority retention coordinator.

use core::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{Sender, sync_channel};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eventd_core::{BoundedQueue, Shard};
use rusqlite::{Connection, OpenFlags};

use crate::config::{Config, SharedConfig};
use crate::health::{self, Store};
use crate::log_ingest::LogMaintenance;
use crate::metric_ingest::MetricMaintenance;
use crate::writer::{EventMaintenance, WriterMessage};

#[derive(Debug, Clone, Copy)]
pub struct RetentionConfig {
    pub event_age: Duration,
    pub event_max_bytes: u64,
    pub log_age: Duration,
    pub log_max_bytes: u64,
    pub metric_age: Duration,
    pub metric_max_bytes: u64,
    pub batch_rows: usize,
    pub checkpoint_pages: u32,
}

pub struct Stores {
    pub event_paths: Vec<PathBuf>,
    pub historical_event_paths: Vec<PathBuf>,
    pub log_path: PathBuf,
    pub metric_path: PathBuf,
}

#[allow(
    clippy::needless_pass_by_value,
    clippy::too_many_arguments,
    reason = "the retention thread deliberately owns every lifetime dependency"
)]
pub fn run(
    runtime: SharedConfig,
    mut stores: Stores,
    event_queues: Arc<[BoundedQueue<WriterMessage>]>,
    log_sender: Sender<LogMaintenance>,
    metric_sender: Sender<MetricMaintenance>,
    boot_id: [u8; 16],
    stopping: Arc<AtomicBool>,
    requested: Arc<AtomicBool>,
) -> Result<(), String> {
    let initial = Config::read(&runtime, retention_config);
    let mut historical_shards = open_historical(
        &mut stores.historical_event_paths,
        initial.checkpoint_pages,
    );
    let mut backoff = Duration::ZERO;
    while wait_interval(
        &stopping,
        &requested,
        Config::read(&runtime, |config| config.retention_interval),
        backoff,
    ) {
        let config = Config::read(&runtime, retention_config);
        for shard in &mut historical_shards {
            shard.set_checkpoint_pages(config.checkpoint_pages);
        }
        match pass(
            &|| Config::read(&runtime, retention_config),
            &stores,
            &event_queues,
            &mut historical_shards,
            &log_sender,
            &metric_sender,
            &boot_id,
            &stopping,
        ) {
            Ok(()) => backoff = Duration::ZERO,
            Err(error) => {
                backoff = next_backoff(backoff);
                eprintln!(
                    "eventd: retention pass failed and will be retried in {}s: {error}",
                    backoff.as_secs()
                );
            }
        }
    }
    Ok(())
}

const FIRST_RETRY: Duration = Duration::from_secs(1);
const LONGEST_RETRY: Duration = Duration::from_mins(1);

/// How long to hold off requested passes after another failed one. A pass
/// that fails for want of space asks for another itself (the writers'
/// capacity errors request retention), so without this it would retry
/// every 100 ms for as long as the disk stayed full.
fn next_backoff(previous: Duration) -> Duration {
    if previous.is_zero() {
        FIRST_RETRY
    } else {
        previous.saturating_mul(2).min(LONGEST_RETRY)
    }
}

/// Open each historical shard read-write, as its one writer. A shard that
/// will not open is left out of retention (and of `paths`, which retention
/// measures) rather than failing the thread: a bad historical shard does not
/// stop eventd (§3.3).
fn open_historical(paths: &mut Vec<PathBuf>, checkpoint_pages: u32) -> Vec<Shard> {
    let mut shards = Vec::with_capacity(paths.len());
    paths.retain(|path| match Shard::open(path, checkpoint_pages) {
        Ok(shard) => {
            shards.push(shard);
            true
        }
        Err(error) => {
            eprintln!(
                "eventd: excluding historical shard {} from retention: {error}",
                path.display()
            );
            false
        }
    });
    shards
}

const fn retention_config(config: &Config) -> RetentionConfig {
    RetentionConfig {
        event_age: config.event_retention,
        event_max_bytes: config.event_retention_max_bytes,
        log_age: config.log_retention,
        log_max_bytes: config.log_retention_max_bytes,
        metric_age: config.metric_retention,
        metric_max_bytes: config.metric_retention_max_bytes,
        batch_rows: config.retention_delete_batch_rows,
        checkpoint_pages: config.wal_checkpoint_pages,
    }
}

/// Wait for the next pass: `interval`, or sooner on request, but no request
/// is taken until `hold` has passed. A request made during the hold is kept
/// and runs the pass when the hold ends.
fn wait_interval(
    stopping: &AtomicBool,
    requested: &AtomicBool,
    interval: Duration,
    hold: Duration,
) -> bool {
    let total = interval.max(hold);
    let mut waited = Duration::ZERO;
    while !stopping.load(Ordering::Acquire) {
        if waited >= total || (waited >= hold && requested.swap(false, Ordering::AcqRel)) {
            return true;
        }
        let until = if waited < hold { hold } else { total };
        let sleep = until.saturating_sub(waited).min(Duration::from_millis(100));
        std::thread::sleep(sleep);
        waited = waited.saturating_add(sleep);
    }
    false
}

#[allow(
    clippy::too_many_arguments,
    reason = "the coordinator processes three independent stores in a fixed order"
)]
fn pass(
    current: Current<'_>,
    stores: &Stores,
    event_queues: &[BoundedQueue<WriterMessage>],
    historical_shards: &mut [Shard],
    log_sender: &Sender<LogMaintenance>,
    metric_sender: &Sender<MetricMaintenance>,
    boot_id: &[u8; 16],
    stopping: &AtomicBool,
) -> Result<(), String> {
    let now = realtime_nanoseconds()?;
    // Each store is retained independently: one that fails (a full disk
    // under the event store, say) must not keep retention, the only lever
    // that frees space, from the others (§9.2).
    let failures: Vec<_> = [
        (
            "event",
            retain_events(
                current,
                stores,
                event_queues,
                historical_shards,
                boot_id,
                stopping,
                now,
            ),
        ),
        (
            "log",
            retain_logs(current, &stores.log_path, log_sender, stopping, now),
        ),
        (
            "metric",
            retain_metrics(current, &stores.metric_path, metric_sender, stopping, now),
        ),
    ]
    .into_iter()
    .filter_map(|(store, result)| result.err().map(|error| format!("{store} store: {error}")))
    .collect();
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

/// The retention configuration as it is now. A pass reads it again before
/// every bounded batch, so a change applied while a pass runs (a lifted
/// size limit, a longer retention period) governs the rest of that pass,
/// not only the next one.
type Current<'a> = &'a dyn Fn() -> RetentionConfig;

/// Whether a store of `size` bytes exceeds `maximum`, zero meaning no limit.
const fn over(size: u64, maximum: u64) -> bool {
    maximum != 0 && size > maximum
}

fn retain_events(
    current: Current<'_>,
    stores: &Stores,
    event_queues: &[BoundedQueue<WriterMessage>],
    historical_shards: &mut [Shard],
    boot_id: &[u8; 16],
    stopping: &AtomicBool,
    now: i64,
) -> Result<(), String> {
    retain_event_age(current, event_queues, now, stopping)?;
    for shard in historical_shards.iter_mut() {
        while !stopping.load(Ordering::Acquire) {
            let config = current();
            let deleted = shard
                .retain_before(cutoff(now, config.event_age)?, config.batch_rows)
                .map_err(|error| error.to_string())?;
            health::retention_deleted(Store::Events, deleted);
            if deleted != config.batch_rows {
                break;
            }
        }
    }
    checkpoint_events(event_queues, historical_shards)?;
    if current().event_max_bytes != 0 {
        let all_event_paths = stores
            .event_paths
            .iter()
            .chain(&stores.historical_event_paths)
            .cloned()
            .collect::<Vec<_>>();
        retain_event_size(
            current,
            event_queues,
            historical_shards,
            &all_event_paths,
            boot_id,
            stopping,
        )?;
    }
    Ok(())
}

fn retain_logs(
    current: Current<'_>,
    path: &Path,
    log_sender: &Sender<LogMaintenance>,
    stopping: &AtomicBool,
    now: i64,
) -> Result<(), String> {
    while !stopping.load(Ordering::Acquire) {
        let config = current();
        let limit = config.batch_rows;
        if log_delete(log_sender, Some(cutoff(now, config.log_age)?), limit)? != limit {
            break;
        }
    }
    checkpoint_log(log_sender)?;
    while current().log_max_bytes != 0 && !stopping.load(Ordering::Acquire) {
        if !over(logical_live_size(path)?, current().log_max_bytes)
            || log_delete(log_sender, None, current().batch_rows)? == 0
        {
            break;
        }
        checkpoint_log(log_sender)?;
    }
    Ok(())
}

fn retain_metrics(
    current: Current<'_>,
    path: &Path,
    metric_sender: &Sender<MetricMaintenance>,
    stopping: &AtomicBool,
    now: i64,
) -> Result<(), String> {
    while !stopping.load(Ordering::Acquire) {
        let config = current();
        let limit = config.batch_rows;
        if metric_delete(metric_sender, Some(cutoff(now, config.metric_age)?), limit)? != limit {
            break;
        }
    }
    checkpoint_metric(metric_sender)?;
    while current().metric_max_bytes != 0 && !stopping.load(Ordering::Acquire) {
        if !over(logical_live_size(path)?, current().metric_max_bytes)
            || metric_delete(metric_sender, None, current().batch_rows)? == 0
        {
            break;
        }
        checkpoint_metric(metric_sender)?;
    }
    Ok(())
}

fn retain_event_age(
    current: Current<'_>,
    queues: &[BoundedQueue<WriterMessage>],
    now: i64,
    stopping: &AtomicBool,
) -> Result<(), String> {
    for queue in queues {
        while !stopping.load(Ordering::Acquire) {
            let config = current();
            let limit = config.batch_rows;
            let cutoff = cutoff(now, config.event_age)?;
            let deleted = event_command(queue, EventMaintenance::DeleteBefore { cutoff, limit })?;
            health::retention_deleted(Store::Events, deleted);
            if deleted < limit {
                break;
            }
        }
    }
    Ok(())
}

fn retain_event_size(
    current: Current<'_>,
    queues: &[BoundedQueue<WriterMessage>],
    historical_shards: &mut [Shard],
    paths: &[PathBuf],
    current_boot: &[u8; 16],
    stopping: &AtomicBool,
) -> Result<(), String> {
    let mut boots: Vec<_> = boot_inventory(paths)?
        .into_iter()
        .filter(|(boot, _)| boot != current_boot)
        .collect();
    boots.sort_by_key(|(_, newest)| *newest);
    for (boot_id, _) in boots {
        delete_boot(current, queues, historical_shards, &boot_id, stopping)?;
        checkpoint_events(queues, historical_shards)?;
        if !over(total_live_size(paths)?, current().event_max_bytes) {
            return Ok(());
        }
    }
    while !stopping.load(Ordering::Acquire)
        && over(total_live_size(paths)?, current().event_max_bytes)
    {
        let deleted =
            delete_boot_once(queues, historical_shards, current_boot, current().batch_rows)?;
        if deleted == 0 {
            break;
        }
        checkpoint_events(queues, historical_shards)?;
    }
    Ok(())
}

/// Delete one whole non-current boot, a batch at a time, unless the size
/// limit is lifted while it runs.
fn delete_boot(
    current: Current<'_>,
    queues: &[BoundedQueue<WriterMessage>],
    historical_shards: &mut [Shard],
    boot_id: &[u8; 16],
    stopping: &AtomicBool,
) -> Result<(), String> {
    while !stopping.load(Ordering::Acquire)
        && current().event_max_bytes != 0
        && delete_boot_once(queues, historical_shards, boot_id, current().batch_rows)? != 0
    {}
    Ok(())
}

fn delete_boot_once(
    queues: &[BoundedQueue<WriterMessage>],
    historical_shards: &mut [Shard],
    boot_id: &[u8; 16],
    limit: usize,
) -> Result<usize, String> {
    let mut deleted = 0;
    for queue in queues {
        deleted += event_command(
            queue,
            EventMaintenance::DeleteBoot {
                boot_id: *boot_id,
                limit,
            },
        )?;
    }
    for shard in historical_shards {
        deleted += shard
            .retain_boot(boot_id, limit)
            .map_err(|error| error.to_string())?;
    }
    health::retention_deleted(Store::Events, deleted);
    Ok(deleted)
}

fn checkpoint_events(
    queues: &[BoundedQueue<WriterMessage>],
    historical_shards: &[Shard],
) -> Result<(), String> {
    for queue in queues {
        event_command(queue, EventMaintenance::Checkpoint)?;
    }
    for shard in historical_shards {
        shard
            .passive_checkpoint()
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn event_command(
    queue: &BoundedQueue<WriterMessage>,
    command: EventMaintenance,
) -> Result<usize, String> {
    let (sender, receiver) = sync_channel(1);
    queue
        .reserve(core::mem::size_of::<WriterMessage>())
        .map_err(|error| error.to_string())?
        .publish(WriterMessage::Maintenance(command, sender));
    receiver
        .recv()
        .map_err(|_| "event writer stopped during retention".to_owned())?
}

fn log_delete(
    sender: &Sender<LogMaintenance>,
    older_than: Option<i64>,
    limit: usize,
) -> Result<usize, String> {
    let (response, receiver) = sync_channel(1);
    sender
        .send(LogMaintenance::DeleteOldest {
            older_than,
            limit,
            response,
        })
        .map_err(|_| "log writer stopped during retention".to_owned())?;
    let deleted = receiver
        .recv()
        .map_err(|_| "log writer stopped during retention".to_owned())??;
    health::retention_deleted(Store::Logs, deleted);
    Ok(deleted)
}

fn checkpoint_log(sender: &Sender<LogMaintenance>) -> Result<(), String> {
    let (response, receiver) = sync_channel(1);
    sender
        .send(LogMaintenance::Checkpoint(response))
        .map_err(|_| "log writer stopped during checkpoint".to_owned())?;
    receiver
        .recv()
        .map_err(|_| "log writer stopped during checkpoint".to_owned())??;
    Ok(())
}

fn metric_delete(
    sender: &Sender<MetricMaintenance>,
    older_than: Option<i64>,
    limit: usize,
) -> Result<usize, String> {
    let (response, receiver) = sync_channel(1);
    sender
        .send(MetricMaintenance::DeleteOldest {
            older_than,
            limit,
            response,
        })
        .map_err(|_| "metric writer stopped during retention".to_owned())?;
    let deleted = receiver
        .recv()
        .map_err(|_| "metric writer stopped during retention".to_owned())??;
    health::retention_deleted(Store::Metrics, deleted);
    Ok(deleted)
}

fn checkpoint_metric(sender: &Sender<MetricMaintenance>) -> Result<(), String> {
    let (response, receiver) = sync_channel(1);
    sender
        .send(MetricMaintenance::Checkpoint(response))
        .map_err(|_| "metric writer stopped during checkpoint".to_owned())?;
    receiver
        .recv()
        .map_err(|_| "metric writer stopped during checkpoint".to_owned())??;
    Ok(())
}

fn total_live_size(paths: &[PathBuf]) -> Result<u64, String> {
    paths.iter().try_fold(0_u64, |total, path| {
        total
            .checked_add(logical_live_size(path)?)
            .ok_or_else(|| "event logical size overflow".to_owned())
    })
}

fn logical_live_size(path: &Path) -> Result<u64, String> {
    let connection = open_read_only(path)?;
    let page_count: u64 = connection
        .query_row("PRAGMA page_count", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    let free_count: u64 = connection
        .query_row("PRAGMA freelist_count", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    let page_size: u64 = connection
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .map_err(|error| error.to_string())?;
    page_count
        .saturating_sub(free_count)
        .checked_mul(page_size)
        .ok_or_else(|| "logical store size overflow".to_owned())
}

fn boot_inventory(paths: &[PathBuf]) -> Result<HashMap<[u8; 16], i64>, String> {
    let mut boots: HashMap<[u8; 16], i64> = HashMap::new();
    for path in paths {
        let connection = open_read_only(path)?;
        let mut statement = connection
            .prepare("SELECT boot_id, MAX(timestamp) FROM events GROUP BY boot_id")
            .map_err(|error| error.to_string())?;
        let mut rows = statement.query([]).map_err(|error| error.to_string())?;
        while let Some(row) = rows.next().map_err(|error| error.to_string())? {
            let bytes: Vec<u8> = row.get(0).map_err(|error| error.to_string())?;
            let Ok(boot_id) = <[u8; 16]>::try_from(bytes) else {
                continue;
            };
            let newest: i64 = row.get(1).map_err(|error| error.to_string())?;
            boots
                .entry(boot_id)
                .and_modify(|known| *known = (*known).max(newest))
                .or_insert(newest);
        }
    }
    Ok(boots)
}

fn open_read_only(path: &Path) -> Result<Connection, String> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| error.to_string())?;
    eventd_core::payload_index::register(&connection).map_err(|error| error.to_string())?;
    Ok(connection)
}

fn realtime_nanoseconds() -> Result<i64, String> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "wall clock precedes Unix epoch".to_owned())?;
    i64::try_from(elapsed.as_nanos()).map_err(|_| "wall clock exceeds timestamp range".to_owned())
}

fn cutoff(now: i64, age: Duration) -> Result<i64, String> {
    let age = i64::try_from(age.as_nanos()).map_err(|_| "retention age overflow".to_owned())?;
    now.checked_sub(age)
        .ok_or_else(|| "retention cutoff underflow".to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::mpsc::channel;

    use eventd_core::Pop;

    use super::*;

    fn temporary_directory() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "eventd-retention-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        path
    }

    fn count(path: &Path, table: &str) -> i64 {
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)
            .unwrap()
            .query_row(&format!("SELECT count(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    /// Stops and closes everything the test started when dropped, so a
    /// failing test still lets its scope join the threads.
    struct Shutdown<'a> {
        stopping: &'a AtomicBool,
        queue: &'a BoundedQueue<WriterMessage>,
    }

    impl Drop for Shutdown<'_> {
        fn drop(&mut self) {
            self.stopping.store(true, Ordering::Release);
            self.queue.close();
        }
    }

    // TRM §8.1: KACS is needed to serve queries and not to ingest; the
    // drain's handoff, the writers and retention never call it. A host has
    // no KACS, so each path below working here is the claim.
    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "the writer, log and metric threads and retention run together as eventd wires them"
    )]
    fn the_write_and_retention_paths_run_without_kacs() {
        use crate::commit_signal::CommitSignal;
        use crate::datagram::IngestionSocket;
        use core::sync::atomic::AtomicU8;
        use eventd_core::{IngestItem, LogStore, MetricStore, RealEvent};
        use peios::msgpack::Writer;

        let directory = temporary_directory();
        let shard_path = directory.join("shard-0000.db");
        let log_path = directory.join("logs.db");
        let metric_path = directory.join("metrics.db");
        let socket_path = directory.join("log.sock");
        let metric_socket_path = directory.join("metric.sock");
        let mut config = Config::test_defaults();
        config.max_batch_latency = Duration::from_millis(10);
        config.health_metric_interval = Duration::ZERO;
        let runtime = config.shared();
        let shard = Shard::open(&shard_path, 1_000).unwrap();
        let log_store = LogStore::open(&log_path, 1_000).unwrap();
        let metric_store = MetricStore::open(&metric_path, 1_000, 1_000).unwrap();
        let ceiling = crate::config::PORTABLE_INGEST_DATAGRAM_BYTES as usize;
        let log_socket = IngestionSocket::unprotected(&socket_path, ceiling).unwrap();
        let metric_socket = IngestionSocket::unprotected(&metric_socket_path, ceiling).unwrap();
        let queue: BoundedQueue<WriterMessage> = BoundedQueue::new(1_024, 1 << 24).unwrap();
        let stopping = Arc::new(AtomicBool::new(false));
        let event_commits = Arc::new(CommitSignal::new());
        let log_commits = Arc::new(CommitSignal::new());
        let ring_pressure: Arc<[AtomicU8]> = Arc::from([AtomicU8::new(0)]);
        let retention_requested = Arc::new(AtomicBool::new(false));
        let (log_sender, log_commands) = channel();
        let (metric_sender, metric_commands) = channel();
        let (_rollup_sender, rollups) = std::sync::mpsc::sync_channel(1);

        std::thread::scope(|scope| {
            let _shutdown = Shutdown {
                stopping: &stopping,
                queue: &queue,
            };
            let writer = scope.spawn(|| {
                crate::writer::run(
                    shard,
                    0,
                    [1; 16],
                    &queue,
                    &runtime,
                    &stopping,
                    &event_commits,
                    &ring_pressure,
                    &retention_requested,
                )
            });
            let (queue_ref, runtime_ref, stopping_ref, requested_ref) =
                (&queue, &runtime, &stopping, &retention_requested);
            let log_commits = &log_commits;
            let log = scope.spawn(move || {
                crate::log_ingest::run(
                    &log_socket,
                    log_store,
                    [1; 16],
                    queue_ref,
                    runtime_ref,
                    stopping_ref,
                    log_commits,
                    &log_commands,
                    requested_ref,
                )
            });
            let metric = scope.spawn(move || {
                crate::metric_ingest::run(
                    &metric_socket,
                    metric_store,
                    [1; 16],
                    queue_ref,
                    runtime_ref,
                    stopping_ref,
                    &metric_commands,
                    &rollups,
                    requested_ref,
                    Arc::new(crate::query::DescriptorCache::new()),
                )
            });

            // Hand the writer what a drain hands it: old events, then the
            // barrier a drain's recovery marker is.
            for sequence in 1..=5 {
                queue
                    .reserve(core::mem::size_of::<WriterMessage>())
                    .unwrap()
                    .publish(WriterMessage::Event(IngestItem {
                        gaps: Vec::new(),
                        store_event: true,
                        event: RealEvent {
                            boot_id: [1; 16],
                            timestamp: sequence,
                            cpu_id: 0,
                            sequence,
                            origin_class: 0,
                            effective_token_guid: [2; 16],
                            true_token_guid: [3; 16],
                            process_guid: [4; 16],
                            event_type: "test.event".into(),
                            payload: [0x80].into(),
                        },
                    }));
            }
            let (barrier, committed) = std::sync::mpsc::sync_channel(1);
            queue
                .reserve(core::mem::size_of::<WriterMessage>())
                .unwrap()
                .publish(WriterMessage::Barrier(barrier));
            committed.recv().unwrap().unwrap();
            assert_eq!(count(&shard_path, "events"), 5, "the events committed");

            // Three old log records through the log socket.
            let mut records = Writer::new();
            records.write_array(3);
            for _ in 0..3 {
                records
                    .write_map(4)
                    .write_str("origin")
                    .write_str("svc.test")
                    .write_str("is_error")
                    .write_bool(false)
                    .write_str("message")
                    .write_str("old")
                    .write_str("timestamp")
                    .write_uint(1);
            }
            std::os::unix::net::UnixDatagram::unbound()
                .unwrap()
                .send_to(&records.to_bytes().unwrap(), &socket_path)
                .unwrap();
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while count(&log_path, "logs") != 3 {
                assert!(
                    std::time::Instant::now() < deadline,
                    "the log records commit"
                );
                std::thread::sleep(Duration::from_millis(10));
            }

            let retention = RetentionConfig {
                event_age: Duration::from_hours(24),
                event_max_bytes: 0,
                log_age: Duration::from_hours(24),
                log_max_bytes: 0,
                metric_age: Duration::from_hours(24),
                metric_max_bytes: 0,
                batch_rows: 2,
                checkpoint_pages: 1_000,
            };
            let stores = Stores {
                event_paths: vec![shard_path.clone()],
                historical_event_paths: Vec::new(),
                log_path: log_path.clone(),
                metric_path: metric_path.clone(),
            };
            pass(
                &|| retention,
                &stores,
                core::slice::from_ref(&queue),
                &mut [],
                &log_sender,
                &metric_sender,
                &[1; 16],
                &AtomicBool::new(false),
            )
            .unwrap();
            assert_eq!(count(&shard_path, "events"), 0, "event retention deleted");
            assert_eq!(count(&log_path, "logs"), 0, "log retention deleted");

            stopping.store(true, Ordering::Release);
            queue.close();
            writer.join().unwrap().unwrap();
            log.join().unwrap().unwrap();
            metric.join().unwrap().unwrap();
        });
        std::fs::remove_dir_all(directory).unwrap();
    }

    // PEI-1297 item 5: a historical shard the pipeline admitted read-only
    // can still fail Shard::open's read-write open. Retention returning Err
    // for it would end the worker, and supervise() would stop eventd.
    #[test]
    fn a_historical_shard_that_will_not_open_read_write_does_not_stop_retention() {
        let directory = temporary_directory();
        let bad = directory.join("shard-0007.db");
        std::fs::write(&bad, b"not a sqlite database").unwrap();
        let (log_sender, _log_commands) = channel();
        let (metric_sender, _metric_commands) = channel();
        let queues: Arc<[BoundedQueue<WriterMessage>]> = Arc::from(Vec::new());
        let result = run(
            Config::test_defaults().shared(),
            Stores {
                event_paths: Vec::new(),
                historical_event_paths: vec![bad],
                log_path: directory.join("logs.db"),
                metric_path: directory.join("metrics.db"),
            },
            queues,
            log_sender,
            metric_sender,
            [1; 16],
            // Already stopping: run returns once it has opened its shards.
            Arc::new(AtomicBool::new(true)),
            Arc::new(AtomicBool::new(false)),
        );
        assert_eq!(result, Ok(()));

        // It is also left out of what retention measures.
        let good = directory.join("shard-0008.db");
        drop(Shard::open(&good, 1_000).unwrap());
        let mut paths = vec![directory.join("shard-0007.db"), good.clone()];
        let shards = open_historical(&mut paths, 1_000);
        assert_eq!(shards.len(), 1);
        assert_eq!(paths, [good]);
        drop(shards);
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// A runtime configuration retaining every store by a day's age alone.
    fn age_only(batch_rows: usize) -> SharedConfig {
        let mut config = Config::test_defaults();
        config.event_retention = Duration::from_hours(24);
        config.event_retention_max_bytes = 0;
        config.log_retention = Duration::from_hours(24);
        config.log_retention_max_bytes = 0;
        config.metric_retention = Duration::from_hours(24);
        config.metric_retention_max_bytes = 0;
        config.retention_delete_batch_rows = batch_rows;
        config.shared()
    }

    /// Run one pass, configured by `runtime`, against two stub event
    /// writers and stub log and metric writers. Each stub records what
    /// reached it, in arrival order (as "events 0: DeleteBefore",
    /// "logs: Checkpoint", "logs: DeleteOldest by size", …), and answers
    /// with `answer` for that arrival.
    fn pass_against_stubs(
        runtime: &SharedConfig,
        log_path: &Path,
        answer: impl Fn(&str) -> Result<usize, String> + Sync,
    ) -> (Result<(), String>, Vec<String>) {
        let stores = Stores {
            event_paths: Vec::new(),
            historical_event_paths: Vec::new(),
            log_path: log_path.to_owned(),
            metric_path: PathBuf::from("/nonexistent/metrics.db"),
        };
        let queues: Vec<BoundedQueue<WriterMessage>> = (0..2)
            .map(|_| BoundedQueue::new(16, 1 << 20).unwrap())
            .collect();
        let arrivals = Mutex::new(Vec::new());
        let arrived = |what: String| {
            let result = answer(&what);
            arrivals.lock().unwrap().push(what);
            result
        };
        let result = std::thread::scope(|scope| {
            for (index, queue) in queues.iter().enumerate() {
                let arrived = &arrived;
                scope.spawn(move || {
                    while let Pop::Item(message) = queue.pop_wait() {
                        let WriterMessage::Maintenance(command, response) = message else {
                            panic!("retention sent an event writer something else");
                        };
                        let kind = match command {
                            EventMaintenance::DeleteBefore { .. } => "DeleteBefore",
                            EventMaintenance::DeleteBoot { .. } => "DeleteBoot",
                            EventMaintenance::Checkpoint => "Checkpoint",
                        };
                        response
                            .send(arrived(format!("events {index}: {kind}")))
                            .unwrap();
                    }
                });
            }
            let (log_sender, log_commands) = channel();
            scope.spawn(|| {
                for command in log_commands {
                    match command {
                        LogMaintenance::DeleteOldest {
                            older_than,
                            response,
                            ..
                        } => {
                            let arrival = if older_than.is_some() {
                                "logs: DeleteOldest"
                            } else {
                                "logs: DeleteOldest by size"
                            };
                            response.send(arrived(arrival.to_owned())).unwrap();
                        }
                        LogMaintenance::Checkpoint(response) => {
                            response
                                .send(arrived("logs: Checkpoint".to_owned()))
                                .unwrap();
                        }
                    }
                }
            });
            let (metric_sender, metric_commands) = channel();
            scope.spawn(|| {
                for command in metric_commands {
                    match command {
                        MetricMaintenance::DeleteOldest { response, .. } => {
                            response
                                .send(arrived("metrics: DeleteOldest".to_owned()))
                                .unwrap();
                        }
                        MetricMaintenance::Checkpoint(response) => {
                            response
                                .send(arrived("metrics: Checkpoint".to_owned()))
                                .unwrap();
                        }
                    }
                }
            });
            let result = pass(
                &|| Config::read(runtime, retention_config),
                &stores,
                &queues,
                &mut [],
                &log_sender,
                &metric_sender,
                &[1; 16],
                &AtomicBool::new(false),
            );
            for queue in &queues {
                queue.close();
            }
            drop(log_sender);
            drop(metric_sender);
            result
        });
        (result, arrivals.into_inner().unwrap())
    }

    // PEI-1289: a full event store must not keep retention from the log
    // and metric stores, the one lever that frees space elsewhere.
    #[test]
    fn a_failing_store_does_not_stop_the_pass_reaching_the_others() {
        let nowhere = Path::new("/nonexistent/logs.db");
        let (result, arrivals) = pass_against_stubs(&age_only(100), nowhere, |arrival| {
            if arrival.starts_with("events") {
                Err("database or disk is full".to_owned())
            } else {
                Ok(0)
            }
        });
        let error = result.expect_err("the event store's failure is reported");
        assert!(error.contains("disk is full"), "{error}");
        for reached in [
            "logs: DeleteOldest",
            "logs: Checkpoint",
            "metrics: DeleteOldest",
            "metrics: Checkpoint",
        ] {
            assert!(
                arrivals.iter().any(|arrival| arrival == reached),
                "{reached} in {arrivals:?}"
            );
        }
    }

    // PEI-1297 item 4: a size limit lifted while a pass runs stops that
    // pass's size deletion at its next batch.
    #[test]
    fn a_running_pass_follows_a_size_limit_lifted_during_it() {
        let directory = temporary_directory();
        let log_path = directory.join("logs.db");
        let connection = Connection::open(&log_path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE logs(message TEXT);\
                 INSERT INTO logs VALUES (zeroblob(65536));",
            )
            .unwrap();
        drop(connection);
        let runtime = age_only(100);
        runtime.write().unwrap().log_retention_max_bytes = 1;
        let size_deletes = std::sync::atomic::AtomicUsize::new(0);
        let (result, _) = pass_against_stubs(&runtime, &log_path, |arrival| {
            if arrival != "logs: DeleteOldest by size" {
                return Ok(0);
            }
            // The stub deletes nothing, so the store stays over any limit;
            // the operator lifts the limit as the first batch runs.
            runtime.write().unwrap().log_retention_max_bytes = 0;
            let calls = size_deletes.fetch_add(1, Ordering::SeqCst) + 1;
            Ok(usize::from(calls < 5))
        });
        result.unwrap();
        assert_eq!(
            size_deletes.load(Ordering::SeqCst),
            1,
            "no batch after the one during which the limit was lifted"
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    // PEI-1289: after a failed pass a request waits out a growing hold
    // instead of running the next pass at once.
    #[test]
    fn a_request_after_a_failed_pass_waits_out_the_backoff() {
        let stopping = AtomicBool::new(false);
        let requested = AtomicBool::new(true);
        let started = std::time::Instant::now();
        assert!(wait_interval(
            &stopping,
            &requested,
            Duration::from_hours(1),
            Duration::ZERO
        ));
        assert!(
            started.elapsed() < Duration::from_millis(100),
            "without a hold a request runs at once"
        );

        requested.store(true, Ordering::Release);
        let started = std::time::Instant::now();
        assert!(wait_interval(
            &stopping,
            &requested,
            Duration::from_hours(1),
            Duration::from_millis(300)
        ));
        let waited = started.elapsed();
        assert!(
            waited >= Duration::from_millis(300) && waited < Duration::from_secs(1),
            "the held request ran when the hold ended: {waited:?}"
        );
        assert!(!requested.load(Ordering::Acquire), "and was taken");

        let mut backoff = Duration::ZERO;
        let mut seen = Vec::new();
        for _ in 0..8 {
            backoff = next_backoff(backoff);
            seen.push(backoff.as_secs());
        }
        assert_eq!(seen, [1, 2, 4, 8, 16, 32, 60, 60]);
    }

    #[test]
    fn a_pass_processes_events_then_logs_then_metrics() {
        let nowhere = Path::new("/nonexistent/logs.db");
        let (result, arrivals) = pass_against_stubs(&age_only(100), nowhere, |_| Ok(0));
        result.unwrap();
        assert_eq!(
            arrivals,
            [
                "events 0: DeleteBefore",
                "events 1: DeleteBefore",
                "events 0: Checkpoint",
                "events 1: Checkpoint",
                "logs: DeleteOldest",
                "logs: Checkpoint",
                "metrics: DeleteOldest",
                "metrics: Checkpoint",
            ]
        );
    }
}
