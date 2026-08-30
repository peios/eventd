//! Startup and supervision of the event-ingestion vertical slice.

use core::sync::atomic::{AtomicBool, Ordering};
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::Duration;

use eventd_core::{
    BoundedQueue, Coverage, IngestItem, LogStore, MetricStore, Shard, StripeRouter, assigned_shards,
};

use crate::config::{Config, HANDOFF_BYTES, HANDOFF_SLOTS, STRIPE_LENGTH};
use crate::datagram::IngestionSocket;
use crate::directory::StoreDirectory;
use crate::kmes::{self, DrainContext};

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

    let boot_id = eventd_core::BootId::read_kernel()?.into_bytes();
    let historical_paths = discover_historical(&event_directory, shard_count)?;
    let mut shards = Vec::with_capacity(shard_count);
    let mut receipts = Vec::new();
    let mut restart = false;
    for index in 0..shard_count {
        let path = event_directory.child(&format!("shard-{index:04}.db"));
        let shard = Shard::open(path, config.wal_checkpoint_pages)?;
        restart |= shard.contains_boot(&boot_id)?;
        receipts.extend(shard.receipts()?);
        shards.push(shard);
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
    let log_store = LogStore::open(log_directory.child("logs.db"), config.wal_checkpoint_pages)?;
    let log_socket = IngestionSocket::bind(&config.log_socket_path, config.max_log_datagram_bytes)?;
    let metric_store = MetricStore::open(
        metric_directory.child("metrics.db"),
        config.wal_checkpoint_pages,
        config.metric_series_cache_size,
    )?;
    let metric_socket =
        IngestionSocket::bind(&config.metric_socket_path, config.max_metric_datagram_bytes)?;

    let queues: Arc<[BoundedQueue<IngestItem>]> = (0..shard_count)
        .map(|_| BoundedQueue::new(HANDOFF_SLOTS, HANDOFF_BYTES))
        .collect::<Result<Vec<_>, _>>()?
        .into();
    let stopping = Arc::new(AtomicBool::new(false));
    install_signal_handlers()?;

    let mut writers = Vec::with_capacity(shard_count);
    for (index, shard) in shards.into_iter().enumerate() {
        let queue = queues[index].clone();
        let writer_stopping = Arc::clone(&stopping);
        let max_batch_size = config.max_batch_size;
        let max_batch_latency = config.max_batch_latency;
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
                    )
                    .map_err(|error| error.to_string())
                })?,
        );
    }
    let log_stopping = Arc::clone(&stopping);
    let log_batch_size = config.log_max_batch_size;
    let log_batch_latency = config.log_max_batch_latency;
    let log_datagram_ceiling = config.max_log_datagram_bytes;
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
        };
        drains.push(
            std::thread::Builder::new()
                .name(format!("eventd-drain-{cpu_id}"))
                .spawn(move || {
                    kmes::drain(attachment, context).map_err(|error| error.to_string())
                })?,
        );
    }

    supervise(drains, writers, &queues, &stopping)
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
    queues: &[BoundedQueue<IngestItem>],
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
