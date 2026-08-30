//! Startup and supervision of the event-ingestion vertical slice.

use core::sync::atomic::{AtomicBool, Ordering};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::sync_channel;
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use eventd_core::{
    BoundedQueue, Coverage, LogStore, MetricStore, Shard, StripeRouter, assigned_shards,
};

use crate::commit_signal::CommitSignal;
use crate::config::{Config, HANDOFF_BYTES, HANDOFF_SLOTS, STRIPE_LENGTH};
use crate::datagram::IngestionSocket;
use crate::directory::StoreDirectory;
use crate::kmes::{self, DrainContext};
use crate::query::{QueryServer, ServerConfig};
use crate::writer::WriterMessage;

static SIGNAL_STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn stop_signal(_signal: libc::c_int) {
    SIGNAL_STOP.store(true, Ordering::Release);
}

#[allow(
    clippy::too_many_lines,
    reason = "keeping the ordered bootstrap phases linear makes partial startup auditable"
)]
pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;
    validate_distinct_paths(&config)?;
    let event_directory = StoreDirectory::open(&config.event_store_path)?;
    let log_directory = StoreDirectory::open(&config.log_store_path)?;
    let metric_directory = StoreDirectory::open(&config.metric_store_path)?;
    let attachments = kmes::attach_all()?;
    let cpu_count = attachments.len();
    let shard_count = if config.storage_shards == 0 {
        cpu_count
    } else {
        config.storage_shards
    };

    let boot = eventd_core::BootId::read_kernel()?;
    let canonical_boot_id = boot.canonical();
    let boot_id = boot.into_bytes();
    let historical_paths = discover_historical(&event_directory, shard_count)?;
    let mut shards = Vec::with_capacity(shard_count);
    let mut active_paths = Vec::with_capacity(shard_count);
    let mut receipts = Vec::new();
    let mut restart = false;
    for index in 0..shard_count {
        let path = event_directory.child(&format!("shard-{index:04}.db"));
        let shard = Shard::open(&path, config.wal_checkpoint_pages)?;
        restart |= shard.contains_boot(&boot_id)?;
        receipts.extend(shard.receipts()?);
        shards.push(shard);
        active_paths.push(path);
    }
    for path in &historical_paths {
        match Shard::historical_receipts(path) {
            Ok(rows) => receipts.extend(rows),
            Err(error) => {
                eprintln!(
                    "eventd: excluding historical shard {}: {error}",
                    path.display()
                );
                continue;
            }
        }
        restart |= Shard::historical_contains_boot(path, &boot_id).unwrap_or(false);
    }
    eprintln!(
        "eventd: {} with {cpu_count} KMES buffer(s), {shard_count} active shard(s); sockets {}, {}, {}",
        if restart { "restarting" } else { "starting" },
        config.query_socket_path.display(),
        config.log_socket_path.display(),
        config.metric_socket_path.display(),
    );
    let coverage = Arc::new(Coverage::from_receipts(receipts));
    let log_path = log_directory.child("logs.db");
    let log_store = LogStore::open(&log_path, config.wal_checkpoint_pages)?;
    let log_socket = IngestionSocket::bind(&config.log_socket_path, config.max_log_datagram_bytes)?;
    let metric_path = metric_directory.child("metrics.db");
    let metric_store = MetricStore::open(
        &metric_path,
        config.wal_checkpoint_pages,
        config.metric_series_cache_size,
    )?;
    let metric_socket =
        IngestionSocket::bind(&config.metric_socket_path, config.max_metric_datagram_bytes)?;
    let query_server = QueryServer::bind(&config.query_socket_path)?;

    let queues: Arc<[BoundedQueue<WriterMessage>]> = (0..shard_count)
        .map(|_| BoundedQueue::new(HANDOFF_SLOTS, HANDOFF_BYTES))
        .collect::<Result<Vec<_>, _>>()?
        .into();
    let stopping = Arc::new(AtomicBool::new(false));
    let event_commits = Arc::new(CommitSignal::new());
    let log_commits = Arc::new(CommitSignal::new());
    install_signal_handlers()?;

    let mut writers = Vec::with_capacity(shard_count);
    for (index, shard) in shards.into_iter().enumerate() {
        let queue = queues[index].clone();
        let writer_stopping = Arc::clone(&stopping);
        let max_batch_size = config.max_batch_size;
        let max_batch_latency = config.max_batch_latency;
        let writer_commits = Arc::clone(&event_commits);
        writers.push(
            std::thread::Builder::new()
                .name(format!("eventd-writer-{index:04}"))
                .spawn(move || {
                    crate::writer::run(
                        shard,
                        &queue,
                        max_batch_size,
                        max_batch_latency,
                        &writer_stopping,
                        &writer_commits,
                    )
                    .map_err(|error| error.to_string())
                })?,
        );
    }
    let log_stopping = Arc::clone(&stopping);
    let log_batch_size = config.log_max_batch_size;
    let log_batch_latency = config.log_max_batch_latency;
    let log_datagram_ceiling = config.max_log_datagram_bytes;
    let log_writer_commits = Arc::clone(&log_commits);
    writers.push(
        std::thread::Builder::new()
            .name("eventd-log".to_owned())
            .spawn(move || {
                crate::log_ingest::run(
                    &log_socket,
                    log_store,
                    boot_id,
                    log_datagram_ceiling,
                    log_batch_size,
                    log_batch_latency,
                    &log_stopping,
                    &log_writer_commits,
                )
                .map_err(|error| error.to_string())
            })?,
    );
    let metric_stopping = Arc::clone(&stopping);
    let metric_batch_size = config.metric_max_batch_size;
    let metric_batch_latency = config.metric_max_batch_latency;
    let metric_datagram_ceiling = config.max_metric_datagram_bytes;
    writers.push(
        std::thread::Builder::new()
            .name("eventd-metric".to_owned())
            .spawn(move || {
                crate::metric_ingest::run(
                    &metric_socket,
                    metric_store,
                    boot_id,
                    metric_datagram_ceiling,
                    metric_batch_size,
                    metric_batch_latency,
                    &metric_stopping,
                )
                .map_err(|error| error.to_string())
            })?,
    );

    let (startup_sender, startup_receiver) = sync_channel(cpu_count);
    let mut drains = Vec::with_capacity(cpu_count);
    for (ordinal, attachment) in attachments.into_iter().enumerate() {
        let cpu_id = attachment.cpu_id;
        let context = DrainContext {
            boot_id,
            queues: Arc::clone(&queues),
            router: StripeRouter::new(
                assigned_shards(ordinal, cpu_count, shard_count),
                STRIPE_LENGTH,
            ),
            coverage: Arc::clone(&coverage),
            stopping: Arc::clone(&stopping),
            startup: startup_sender.clone(),
        };
        drains.push(
            std::thread::Builder::new()
                .name(format!("eventd-drain-{cpu_id}"))
                .spawn(move || {
                    kmes::drain(attachment, context).map_err(|error| error.to_string())
                })?,
        );
    }
    drop(startup_sender);
    let mut cpu_ids = Vec::with_capacity(cpu_count);
    for _ in 0..cpu_count {
        let cpu_id = startup_receiver
            .recv()
            .map_err(|_| "event drain stopped before recovery committed")??;
        cpu_ids.push(cpu_id);
    }
    cpu_ids.sort_unstable();
    let committed_coverage = load_coverage(&active_paths, &historical_paths)?;
    let resume_points: Vec<_> = cpu_ids
        .into_iter()
        .map(|cpu_id| {
            (
                cpu_id,
                committed_coverage.highest_contiguous(&boot_id, cpu_id),
            )
        })
        .collect();
    commit_synthetic(
        &queues[0],
        crate::synthetic::startup(
            boot_id,
            &canonical_boot_id,
            restart,
            shard_count,
            &resume_points,
            realtime_nanoseconds()?,
        ),
    )?;

    let query_stores = Arc::new(crate::query::Stores {
        event_paths: active_paths
            .iter()
            .chain(&historical_paths)
            .cloned()
            .collect(),
        log_path,
        metric_path,
    });
    let query_config = Arc::new(ServerConfig {
        max_request_bytes: config.max_query_request_bytes,
        response_target_bytes: config.query_response_target_bytes,
        max_concurrent: config.max_concurrent_queries,
        max_streaming: config.max_streaming_queries,
        max_distinct_stream_values: config.max_distinct_stream_values,
        timeout: config.query_timeout,
    });
    let query_stopping = Arc::clone(&stopping);
    writers.push(
        std::thread::Builder::new()
            .name("eventd-query-listener".to_owned())
            .spawn(move || {
                query_server
                    .run(
                        &query_stores,
                        &query_config,
                        &query_stopping,
                        &event_commits,
                        &log_commits,
                    )
                    .map_err(|error| error.to_string())
            })?,
    );
    notify_ready()?;

    supervise(drains, writers, &queues, &stopping)
}

fn validate_distinct_paths(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    let paths = [
        &config.query_socket_path,
        &config.log_socket_path,
        &config.metric_socket_path,
    ];
    if paths[0] == paths[1] || paths[0] == paths[2] || paths[1] == paths[2] {
        return Err("query, log and metric socket paths must be distinct".into());
    }
    Ok(())
}

fn notify_ready() -> Result<(), Box<dyn std::error::Error>> {
    let Some(path) = std::env::var_os("NOTIFY_SOCKET") else {
        return Ok(());
    };
    let socket = std::os::unix::net::UnixDatagram::unbound()?;
    socket.send_to(b"READY=1", path)?;
    Ok(())
}

fn load_coverage(
    active_paths: &[PathBuf],
    historical_paths: &[PathBuf],
) -> Result<Coverage, Box<dyn std::error::Error>> {
    let mut receipts = Vec::new();
    for path in active_paths.iter().chain(historical_paths) {
        receipts.extend(Shard::historical_receipts(path)?);
    }
    Ok(Coverage::from_receipts(receipts))
}

fn commit_synthetic(
    queue: &BoundedQueue<WriterMessage>,
    event: eventd_core::SyntheticEvent,
) -> Result<(), Box<dyn std::error::Error>> {
    let (sender, receiver) = sync_channel(1);
    queue
        .reserve(core::mem::size_of::<WriterMessage>())?
        .publish(WriterMessage::Synthetic(event, sender));
    receiver
        .recv()
        .map_err(|_| "event writer stopped before synthetic event commit")??;
    Ok(())
}

fn realtime_nanoseconds() -> Result<u64, Box<dyn std::error::Error>> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(u64::try_from(elapsed.as_nanos())?)
}

fn discover_historical(
    directory: &StoreDirectory,
    active_count: usize,
) -> Result<Vec<PathBuf>, std::io::Error> {
    let mut historical = Vec::new();
    for entry in std::fs::read_dir(directory.anchored_path())? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        let Some(index) = shard_index(name) else {
            continue;
        };
        if index >= active_count {
            historical.push(directory.child(name));
        }
    }
    historical.sort_unstable();
    Ok(historical)
}

#[allow(clippy::case_sensitive_file_extension_comparisons)]
fn shard_index(name: &str) -> Option<usize> {
    if name.len() != 13 || !name.starts_with("shard-") || !name.ends_with(".db") {
        return None;
    }
    name[6..10].parse().ok()
}

fn install_signal_handlers() -> Result<(), std::io::Error> {
    // SAFETY: the handler performs only a lock-free atomic store, which is
    // async-signal-safe. SIGKILL remains unhandled by definition.
    let handler = stop_signal as *const () as libc::sighandler_t;
    if unsafe { libc::signal(libc::SIGTERM, handler) } == libc::SIG_ERR
        || unsafe { libc::signal(libc::SIGINT, handler) } == libc::SIG_ERR
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

fn supervise(
    drains: Vec<JoinHandle<Result<(), String>>>,
    writers: Vec<JoinHandle<Result<(), String>>>,
    queues: &[BoundedQueue<WriterMessage>],
    stopping: &Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    while !SIGNAL_STOP.load(Ordering::Acquire)
        && !drains.iter().any(JoinHandle::is_finished)
        && !writers.iter().any(JoinHandle::is_finished)
    {
        std::thread::sleep(Duration::from_millis(50));
    }
    stopping.store(true, Ordering::Release);
    for queue in queues {
        queue.close();
    }

    let mut first_error = None;
    for handle in drains.into_iter().chain(writers) {
        match handle.join() {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(_) => {
                first_error.get_or_insert_with(|| "eventd worker panicked".to_owned());
            }
        }
    }
    first_error.map_or_else(|| Ok(()), |error| Err(error.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shard_names_are_exact() {
        assert_eq!(shard_index("shard-0000.db"), Some(0));
        assert_eq!(shard_index("shard-0255.db"), Some(255));
        assert_eq!(shard_index("shard-1.db"), None);
        assert_eq!(shard_index("eventd-meta.db"), None);
        assert_eq!(shard_index("shard-0000.db-wal"), None);
    }
}
