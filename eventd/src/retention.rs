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
    stores: Stores,
    event_queues: Arc<[BoundedQueue<WriterMessage>]>,
    log_sender: Sender<LogMaintenance>,
    metric_sender: Sender<MetricMaintenance>,
    boot_id: [u8; 16],
    stopping: Arc<AtomicBool>,
    requested: Arc<AtomicBool>,
) -> Result<(), String> {
    let initial = Config::read(&runtime, retention_config);
    let mut historical_shards = stores
        .historical_event_paths
        .iter()
        .map(|path| Shard::open(path, initial.checkpoint_pages).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    while wait_interval(
        &stopping,
        &requested,
        Config::read(&runtime, |config| config.retention_interval),
    ) {
        let config = Config::read(&runtime, retention_config);
        for shard in &mut historical_shards {
            shard.set_checkpoint_pages(config.checkpoint_pages);
        }
        if let Err(error) = pass(
            &config,
            &stores,
            &event_queues,
            &mut historical_shards,
            &log_sender,
            &metric_sender,
            &boot_id,
            &stopping,
        ) {
            eprintln!("eventd: retention pass failed and will be retried: {error}");
        }
    }
    Ok(())
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

fn wait_interval(stopping: &AtomicBool, requested: &AtomicBool, interval: Duration) -> bool {
    let mut remaining = interval;
    while !stopping.load(Ordering::Acquire) {
        if requested.swap(false, Ordering::AcqRel) || remaining.is_zero() {
            return true;
        }
        let sleep = remaining.min(Duration::from_millis(100));
        std::thread::sleep(sleep);
        remaining = remaining.saturating_sub(sleep);
    }
    false
}

#[allow(
    clippy::too_many_arguments,
    reason = "the coordinator processes three independent stores in a fixed order"
)]
fn pass(
    config: &RetentionConfig,
    stores: &Stores,
    event_queues: &[BoundedQueue<WriterMessage>],
    historical_shards: &mut [Shard],
    log_sender: &Sender<LogMaintenance>,
    metric_sender: &Sender<MetricMaintenance>,
    boot_id: &[u8; 16],
    stopping: &AtomicBool,
) -> Result<(), String> {
    let now = realtime_nanoseconds()?;
    retain_event_age(
        event_queues,
        cutoff(now, config.event_age)?,
        config.batch_rows,
        stopping,
    )?;
    for shard in historical_shards.iter_mut() {
        while !stopping.load(Ordering::Acquire) {
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
    if config.event_max_bytes != 0 {
        let all_event_paths = stores
            .event_paths
            .iter()
            .chain(&stores.historical_event_paths)
            .cloned()
            .collect::<Vec<_>>();
        retain_event_size(
            event_queues,
            historical_shards,
            &all_event_paths,
            config.event_max_bytes,
            config.batch_rows,
            boot_id,
            stopping,
        )?;
    }

    retain_log_age(
        log_sender,
        cutoff(now, config.log_age)?,
        config.batch_rows,
        stopping,
    )?;
    checkpoint_log(log_sender)?;
    if config.log_max_bytes != 0 {
        while logical_live_size(&stores.log_path)? > config.log_max_bytes
            && !stopping.load(Ordering::Acquire)
        {
            if log_delete(log_sender, None, config.batch_rows)? == 0 {
                break;
            }
            checkpoint_log(log_sender)?;
        }
    }

    retain_metric_age(
        metric_sender,
        cutoff(now, config.metric_age)?,
        config.batch_rows,
        stopping,
    )?;
    checkpoint_metric(metric_sender)?;
    if config.metric_max_bytes != 0 {
        while logical_live_size(&stores.metric_path)? > config.metric_max_bytes
            && !stopping.load(Ordering::Acquire)
        {
            if metric_delete(metric_sender, None, config.batch_rows)? == 0 {
                break;
            }
            checkpoint_metric(metric_sender)?;
        }
    }
    Ok(())
}

fn retain_event_age(
    queues: &[BoundedQueue<WriterMessage>],
    cutoff: i64,
    limit: usize,
    stopping: &AtomicBool,
) -> Result<(), String> {
    for queue in queues {
        while !stopping.load(Ordering::Acquire) {
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
    queues: &[BoundedQueue<WriterMessage>],
    historical_shards: &mut [Shard],
    paths: &[PathBuf],
    maximum: u64,
    limit: usize,
    current_boot: &[u8; 16],
    stopping: &AtomicBool,
) -> Result<(), String> {
    let mut boots: Vec<_> = boot_inventory(paths)?
        .into_iter()
        .filter(|(boot, _)| boot != current_boot)
        .collect();
    boots.sort_by_key(|(_, newest)| *newest);
    for (boot_id, _) in boots {
        delete_boot(queues, historical_shards, &boot_id, limit, stopping)?;
        checkpoint_events(queues, historical_shards)?;
        if total_live_size(paths)? <= maximum {
            return Ok(());
        }
    }
    while total_live_size(paths)? > maximum && !stopping.load(Ordering::Acquire) {
        let deleted = delete_boot_once(queues, historical_shards, current_boot, limit)?;
        if deleted == 0 {
            break;
        }
        checkpoint_events(queues, historical_shards)?;
    }
    Ok(())
}

fn delete_boot(
    queues: &[BoundedQueue<WriterMessage>],
    historical_shards: &mut [Shard],
    boot_id: &[u8; 16],
    limit: usize,
    stopping: &AtomicBool,
) -> Result<(), String> {
    while !stopping.load(Ordering::Acquire)
        && delete_boot_once(queues, historical_shards, boot_id, limit)? != 0
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
    historical_shards: &mut [Shard],
) -> Result<(), String> {
    for queue in queues {
        event_command(queue, EventMaintenance::Checkpoint)?;
    }
    for shard in historical_shards {
        // A historical shard takes no ingestion to yield to.
        shard
            .remove_orphan_types(|| false)
            .and_then(|_| shard.passive_checkpoint())
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

fn retain_log_age(
    sender: &Sender<LogMaintenance>,
    cutoff: i64,
    limit: usize,
    stopping: &AtomicBool,
) -> Result<(), String> {
    while !stopping.load(Ordering::Acquire) && log_delete(sender, Some(cutoff), limit)? == limit {}
    Ok(())
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

fn retain_metric_age(
    sender: &Sender<MetricMaintenance>,
    cutoff: i64,
    limit: usize,
    stopping: &AtomicBool,
) -> Result<(), String> {
    while !stopping.load(Ordering::Acquire) && metric_delete(sender, Some(cutoff), limit)? == limit
    {
    }
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
                &retention,
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

    #[test]
    fn a_pass_processes_events_then_logs_then_metrics() {
        let config = RetentionConfig {
            event_age: Duration::from_hours(24),
            event_max_bytes: 0,
            log_age: Duration::from_hours(24),
            log_max_bytes: 0,
            metric_age: Duration::from_hours(24),
            metric_max_bytes: 0,
            batch_rows: 100,
            checkpoint_pages: 1_000,
        };
        let stores = Stores {
            event_paths: Vec::new(),
            historical_event_paths: Vec::new(),
            log_path: PathBuf::from("/nonexistent/logs.db"),
            metric_path: PathBuf::from("/nonexistent/metrics.db"),
        };
        let queues: Vec<BoundedQueue<WriterMessage>> = (0..2)
            .map(|_| BoundedQueue::new(16, 1 << 20).unwrap())
            .collect();
        // Stub writers: each records what reached it, in arrival order, and
        // answers that nothing was deleted.
        let arrivals = Mutex::new(Vec::new());
        let arrived = |what: String| arrivals.lock().unwrap().push(what);
        std::thread::scope(|scope| {
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
                        arrived(format!("events {index}: {kind}"));
                        response.send(Ok(0)).unwrap();
                    }
                });
            }
            let (log_sender, log_commands) = channel();
            scope.spawn(|| {
                for command in log_commands {
                    match command {
                        LogMaintenance::DeleteOldest { response, .. } => {
                            arrived("logs: DeleteOldest".to_owned());
                            response.send(Ok(0)).unwrap();
                        }
                        LogMaintenance::Checkpoint(response) => {
                            arrived("logs: Checkpoint".to_owned());
                            response.send(Ok(0)).unwrap();
                        }
                    }
                }
            });
            let (metric_sender, metric_commands) = channel();
            scope.spawn(|| {
                for command in metric_commands {
                    match command {
                        MetricMaintenance::DeleteOldest { response, .. } => {
                            arrived("metrics: DeleteOldest".to_owned());
                            response.send(Ok(0)).unwrap();
                        }
                        MetricMaintenance::Checkpoint(response) => {
                            arrived("metrics: Checkpoint".to_owned());
                            response.send(Ok(0)).unwrap();
                        }
                    }
                }
            });
            pass(
                &config,
                &stores,
                &queues,
                &mut [],
                &log_sender,
                &metric_sender,
                &[1; 16],
                &AtomicBool::new(false),
            )
            .unwrap();
            for queue in &queues {
                queue.close();
            }
            drop(log_sender);
            drop(metric_sender);
        });
        assert_eq!(
            arrivals.into_inner().unwrap(),
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
