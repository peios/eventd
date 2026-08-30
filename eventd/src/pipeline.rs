//! Startup and supervision of the event-ingestion vertical slice.

use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::path::PathBuf;
use std::sync::mpsc::{channel, sync_channel};
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;
use std::time::{SystemTime, UNIX_EPOCH};

use eventd_core::{
    BoundedQueue, Coverage, LogStore, MetaStore, MetricStore, Shard, StripeRouter, assigned_shards,
};

use crate::commit_signal::CommitSignal;
use crate::config::{Config, HANDOFF_BYTES, HANDOFF_SLOTS, STRIPE_LENGTH};
use crate::datagram::IngestionSocket;
use crate::directory::StoreDirectory;
use crate::indexing::{PolicyConfig, PolicyMessage, Tracker};
use crate::kmes::{self, DrainContext};
use crate::query::{DescriptorCache, QueryServer, ServerConfig};
use crate::writer::{SheddingConfig, WriterMessage};

type ReceiptRow = ([u8; 16], u16, eventd_core::Interval);

static SIGNAL_STOP: AtomicBool = AtomicBool::new(false);
static SIGNAL_QUIT: AtomicBool = AtomicBool::new(false);

extern "C" fn stop_signal(signal: libc::c_int) {
    if signal == libc::SIGQUIT {
        SIGNAL_QUIT.store(true, Ordering::Release);
    }
    SIGNAL_STOP.store(true, Ordering::Release);
}

#[allow(
    clippy::too_many_lines,
    reason = "keeping the ordered bootstrap phases linear makes partial startup auditable"
)]
pub fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = Config::load()?;
    crate::query::provision_security_defaults()?;
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
    let meta_store = MetaStore::open(
        event_directory.child("eventd-meta.db"),
        config.wal_checkpoint_pages,
    )?;
    let (persisted_counters, persisted_desired) = meta_store.load_index_state()?;
    let index_tracker = Arc::new(Tracker::from_persisted(
        persisted_counters,
        config.adaptive_index_window,
        config.adaptive_index_create_threshold,
    ));
    let desired_indexes = Arc::new(RwLock::new(persisted_desired));
    let (index_policy_sender, index_policy_receiver) = sync_channel(1);
    let discovered_historical = discover_historical(&event_directory, shard_count)?;
    let mut historical_paths = Vec::new();
    let mut shards = Vec::with_capacity(shard_count);
    let mut active_paths = Vec::with_capacity(shard_count);
    let mut receipts = Vec::new();
    let mut startup_storage_errors = Vec::new();
    let mut restart = false;
    for index in 0..shard_count {
        let path = event_directory.child(&format!("shard-{index:04}.db"));
        let (shard, recovery) = Shard::open_recovering(&path, config.wal_checkpoint_pages)?;
        if let Some(error) = recovery {
            eprintln!("eventd: quarantined corrupt event shard {index}: {error}");
            startup_storage_errors.push(("event", Some(index), error));
        }
        restart |= shard.contains_boot(&boot_id)?;
        receipts.extend(shard.receipts()?);
        shards.push(shard);
        active_paths.push(path);
    }
    for path in &discovered_historical {
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
        historical_paths.push(path.clone());
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
    let (log_store, log_recovery) =
        LogStore::open_recovering(&log_path, config.wal_checkpoint_pages)?;
    if let Some(error) = log_recovery {
        eprintln!("eventd: quarantined corrupt log store: {error}");
        startup_storage_errors.push(("log", None, error));
    }
    let log_socket = Arc::new(IngestionSocket::bind(
        &config.log_socket_path,
        config.max_log_datagram_bytes,
    )?);
    let metric_path = metric_directory.child("metrics.db");
    let (metric_store, metric_recovery) = MetricStore::open_recovering(
        &metric_path,
        config.wal_checkpoint_pages,
        config.metric_series_cache_size,
    )?;
    if let Some(error) = metric_recovery {
        eprintln!("eventd: quarantined corrupt metric store: {error}");
        startup_storage_errors.push(("metric", None, error));
    }
    let metric_socket = Arc::new(IngestionSocket::bind(
        &config.metric_socket_path,
        config.max_metric_datagram_bytes,
    )?);
    let query_server = Arc::new(QueryServer::bind(&config.query_socket_path)?);

    for (store, shard_index, error) in startup_storage_errors {
        shards[0].commit_synthetic(&crate::synthetic::storage_error(
            boot_id,
            store,
            shard_index,
            &error,
            realtime_nanoseconds()?,
        ))?;
    }

    let queues: Arc<[BoundedQueue<WriterMessage>]> = (0..shard_count)
        .map(|_| BoundedQueue::new(HANDOFF_SLOTS, HANDOFF_BYTES))
        .collect::<Result<Vec<_>, _>>()?
        .into();
    let stopping = Arc::new(AtomicBool::new(false));
    let descriptors = Arc::new(DescriptorCache::new());
    let descriptor_thread_cache = Arc::clone(&descriptors);
    let descriptor_thread_stopping = Arc::clone(&stopping);
    let descriptor_handle = std::thread::Builder::new()
        .name("eventd-security-watch".to_owned())
        .spawn(move || {
            crate::query::watch_security_descriptors(
                &descriptor_thread_cache,
                &descriptor_thread_stopping,
            );
            Ok(())
        })?;
    let ring_pressure: Arc<[AtomicU8]> = (0..cpu_count)
        .map(|_| AtomicU8::new(0))
        .collect::<Vec<_>>()
        .into();
    let event_commits = Arc::new(CommitSignal::new());
    let log_commits = Arc::new(CommitSignal::new());
    let retention_requested = Arc::new(AtomicBool::new(false));
    install_signal_handlers()?;

    let mut event_writers = Vec::with_capacity(shard_count);
    for (index, shard) in shards.into_iter().enumerate() {
        let queue = queues[index].clone();
        let writer_stopping = Arc::clone(&stopping);
        let max_batch_size = config.max_batch_size;
        let max_batch_latency = config.max_batch_latency;
        let writer_commits = Arc::clone(&event_commits);
        let writer_ring_pressure = Arc::clone(&ring_pressure);
        let writer_retention_requested = Arc::clone(&retention_requested);
        let shedding = SheddingConfig {
            window: config.shedding_window,
            batch_percent: config.shedding_batch_percent,
            emergency_buffer_percent: config.emergency_shedding_buffer_percent,
        };
        event_writers.push(
            std::thread::Builder::new()
                .name(format!("eventd-writer-{index:04}"))
                .spawn(move || {
                    crate::writer::run(
                        shard,
                        index,
                        boot_id,
                        &queue,
                        max_batch_size,
                        max_batch_latency,
                        &writer_stopping,
                        &writer_commits,
                        shedding,
                        &writer_ring_pressure,
                        &writer_retention_requested,
                    )
                    .map_err(|error| error.to_string())
                })?,
        );
    }
    let index_policy_config = PolicyConfig {
        interval: config.adaptive_index_policy_interval,
        create_threshold: config.adaptive_index_create_threshold,
        drop_threshold: config.adaptive_index_drop_threshold,
    };
    let index_thread_tracker = Arc::clone(&index_tracker);
    let index_thread_desired = Arc::clone(&desired_indexes);
    let index_thread_queues = Arc::clone(&queues);
    let index_handle = std::thread::Builder::new()
        .name("eventd-index-policy".to_owned())
        .spawn(move || {
            crate::indexing::run(
                meta_store,
                &index_thread_tracker,
                &index_thread_desired,
                &index_thread_queues,
                index_policy_config,
                &index_policy_receiver,
            )
            .map_err(|error| error.to_string())
        })?;
    let log_stopping = Arc::clone(&stopping);
    let log_batch_size = config.log_max_batch_size;
    let log_batch_latency = config.log_max_batch_latency;
    let log_datagram_ceiling = config.max_log_datagram_bytes;
    let log_writer_commits = Arc::clone(&log_commits);
    let log_retention_requested = Arc::clone(&retention_requested);
    let (log_maintenance_sender, log_maintenance_receiver) = channel();
    let log_thread_socket = Arc::clone(&log_socket);
    let log_error_events = queues[0].clone();
    let log_handle = std::thread::Builder::new()
        .name("eventd-log".to_owned())
        .spawn(move || {
            crate::log_ingest::run(
                &log_thread_socket,
                log_store,
                boot_id,
                &log_error_events,
                log_datagram_ceiling,
                log_batch_size,
                log_batch_latency,
                &log_stopping,
                &log_writer_commits,
                &log_maintenance_receiver,
                &log_retention_requested,
            )
            .map_err(|error| error.to_string())
        })?;
    let metric_stopping = Arc::clone(&stopping);
    let metric_batch_size = config.metric_max_batch_size;
    let metric_batch_latency = config.metric_max_batch_latency;
    let metric_datagram_ceiling = config.max_metric_datagram_bytes;
    let metric_retention_requested = Arc::clone(&retention_requested);
    let (metric_maintenance_sender, metric_maintenance_receiver) = channel();
    let metric_thread_socket = Arc::clone(&metric_socket);
    let metric_error_events = queues[0].clone();
    let metric_handle = std::thread::Builder::new()
        .name("eventd-metric".to_owned())
        .spawn(move || {
            crate::metric_ingest::run(
                &metric_thread_socket,
                metric_store,
                boot_id,
                &metric_error_events,
                metric_datagram_ceiling,
                metric_batch_size,
                metric_batch_latency,
                &metric_stopping,
                &metric_maintenance_receiver,
                &metric_retention_requested,
            )
            .map_err(|error| error.to_string())
        })?;

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
            ring_pressure: Arc::clone(&ring_pressure),
            pressure_slot: ordinal,
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
        .iter()
        .map(|&cpu_id| {
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
        log_path: log_path.clone(),
        metric_path: metric_path.clone(),
    });
    let query_config = Arc::new(ServerConfig {
        max_request_bytes: config.max_query_request_bytes,
        response_target_bytes: config.query_response_target_bytes,
        max_concurrent: config.max_concurrent_queries,
        max_streaming: config.max_streaming_queries,
        max_distinct_stream_values: config.max_distinct_stream_values,
        timeout: config.query_timeout,
        cross_type_window: config.cross_type_window,
        cross_type_max_lookback: config.cross_type_max_lookback,
        index_tracker,
        index_policy: index_policy_sender.clone(),
        descriptors,
    });
    let query_stopping = Arc::clone(&stopping);
    let query_thread_server = Arc::clone(&query_server);
    let query_handle = std::thread::Builder::new()
        .name("eventd-query-listener".to_owned())
        .spawn(move || {
            query_thread_server
                .run(
                    &query_stores,
                    &query_config,
                    &query_stopping,
                    &event_commits,
                    &log_commits,
                )
                .map_err(|error| error.to_string())
        })?;
    let retention_config = crate::retention::RetentionConfig {
        event_age: config.event_retention,
        event_max_bytes: config.event_retention_max_bytes,
        log_age: config.log_retention,
        log_max_bytes: config.log_retention_max_bytes,
        metric_age: config.metric_retention,
        metric_max_bytes: config.metric_retention_max_bytes,
        interval: config.retention_interval,
        batch_rows: config.retention_delete_batch_rows,
        checkpoint_pages: config.wal_checkpoint_pages,
    };
    let retention_stores = crate::retention::Stores {
        event_paths: active_paths.clone(),
        historical_event_paths: historical_paths.clone(),
        log_path,
        metric_path,
    };
    let retention_queues = Arc::clone(&queues);
    let retention_stopping = Arc::clone(&stopping);
    let retention_requested = Arc::clone(&retention_requested);
    let retention_handle = std::thread::Builder::new()
        .name("eventd-retention".to_owned())
        .spawn(move || {
            crate::retention::run(
                retention_config,
                retention_stores,
                retention_queues,
                log_maintenance_sender,
                metric_maintenance_sender,
                boot_id,
                retention_stopping,
                retention_requested,
            )
        })?;
    notify_ready()?;

    supervise(
        drains,
        event_writers,
        log_handle,
        metric_handle,
        query_handle,
        retention_handle,
        index_handle,
        descriptor_handle,
        &queues,
        &stopping,
        query_server,
        log_socket,
        metric_socket,
        &active_paths,
        &historical_paths,
        boot_id,
        &canonical_boot_id,
        &cpu_ids,
        &index_policy_sender,
    )
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
    Ok(Coverage::from_receipts(load_receipts(
        active_paths,
        historical_paths,
    )?))
}

fn load_receipts(
    active_paths: &[PathBuf],
    historical_paths: &[PathBuf],
) -> Result<Vec<ReceiptRow>, Box<dyn std::error::Error>> {
    let mut receipts = Vec::new();
    for path in active_paths.iter().chain(historical_paths) {
        receipts.extend(Shard::historical_receipts(path)?);
    }
    Ok(receipts)
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
        || unsafe { libc::signal(libc::SIGQUIT, handler) } == libc::SIG_ERR
    {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "shutdown owns each resource explicitly in its mandated close order"
)]
fn supervise(
    drains: Vec<JoinHandle<Result<kmes::Attachment, String>>>,
    event_writers: Vec<JoinHandle<Result<(), String>>>,
    log_handle: JoinHandle<Result<(), String>>,
    metric_handle: JoinHandle<Result<(), String>>,
    query_handle: JoinHandle<Result<(), String>>,
    retention_handle: JoinHandle<Result<(), String>>,
    index_handle: JoinHandle<Result<(), String>>,
    descriptor_handle: JoinHandle<Result<(), String>>,
    queues: &[BoundedQueue<WriterMessage>],
    stopping: &Arc<AtomicBool>,
    query_server: Arc<QueryServer>,
    log_socket: Arc<IngestionSocket>,
    metric_socket: Arc<IngestionSocket>,
    active_paths: &[PathBuf],
    historical_paths: &[PathBuf],
    boot_id: [u8; 16],
    canonical_boot_id: &str,
    cpu_ids: &[u16],
    index_policy: &std::sync::mpsc::SyncSender<PolicyMessage>,
) -> Result<(), Box<dyn std::error::Error>> {
    while !SIGNAL_STOP.load(Ordering::Acquire)
        && !drains.iter().any(JoinHandle::is_finished)
        && !event_writers.iter().any(JoinHandle::is_finished)
        && !log_handle.is_finished()
        && !metric_handle.is_finished()
        && !query_handle.is_finished()
        && !retention_handle.is_finished()
        && !index_handle.is_finished()
        && !descriptor_handle.is_finished()
    {
        std::thread::sleep(Duration::from_millis(50));
    }

    if SIGNAL_QUIT.swap(false, Ordering::AcqRel) {
        diagnostic_dump(
            canonical_boot_id,
            active_paths,
            historical_paths,
            boot_id,
            cpu_ids,
            &query_server,
        );
    }

    query_server.unlink();
    log_socket.unlink();
    metric_socket.unlink();
    stopping.store(true, Ordering::Release);

    let mut first_error = None;
    join_worker(retention_handle, &mut first_error);
    join_worker(query_handle, &mut first_error);
    join_worker(descriptor_handle, &mut first_error);
    join_worker(log_handle, &mut first_error);
    join_worker(metric_handle, &mut first_error);
    drop(query_server);
    drop(log_socket);
    drop(metric_socket);
    let mut mapped_rings = Vec::with_capacity(drains.len());
    for drain in drains {
        match drain.join() {
            Ok(Ok(attachment)) => mapped_rings.push(attachment),
            Ok(Err(error)) => {
                first_error.get_or_insert(error);
            }
            Err(_) => {
                first_error.get_or_insert_with(|| "eventd drain panicked".to_owned());
            }
        }
    }

    if let Err(error) = flush_event_queues(queues) {
        first_error.get_or_insert(error);
    }

    let mut sequences = cpu_ids
        .iter()
        .copied()
        .map(|cpu_id| (cpu_id, 0))
        .collect::<Vec<_>>();
    match load_coverage(active_paths, historical_paths) {
        Ok(coverage) => {
            for (cpu_id, sequence) in &mut sequences {
                *sequence = coverage.highest_contiguous(&boot_id, *cpu_id);
            }
        }
        Err(error) => {
            first_error.get_or_insert_with(|| error.to_string());
        }
    }
    match realtime_nanoseconds() {
        Ok(timestamp) => {
            let (sender, receiver) = sync_channel(1);
            if index_policy
                .send(PolicyMessage::Checkpoint {
                    boot_id,
                    sequences: sequences.clone(),
                    updated_at: timestamp,
                    reply: sender,
                })
                .is_err()
            {
                first_error.get_or_insert_with(|| "index policy thread stopped".to_owned());
            } else if let Ok(Err(error)) = receiver.recv() {
                first_error.get_or_insert(error);
            }
            let shutdown = crate::synthetic::shutdown(boot_id, &sequences, timestamp);
            if let Err(error) = commit_synthetic_fallback(queues, &shutdown) {
                eprintln!("eventd: cannot persist synthetic.shutdown: {error}");
                first_error.get_or_insert(error);
            }
        }
        Err(error) => {
            first_error.get_or_insert_with(|| error.to_string());
        }
    }

    for queue in queues {
        queue.close();
    }
    for writer in event_writers {
        join_worker(writer, &mut first_error);
    }
    let (sender, receiver) = sync_channel(1);
    if index_policy.send(PolicyMessage::Stop(sender)).is_ok()
        && let Ok(Err(error)) = receiver.recv()
    {
        first_error.get_or_insert(error);
    }
    join_worker(index_handle, &mut first_error);
    drop(mapped_rings);
    first_error.map_or_else(|| Ok(()), |error| Err(error.into()))
}

fn diagnostic_dump(
    canonical_boot_id: &str,
    active_paths: &[PathBuf],
    historical_paths: &[PathBuf],
    boot_id: [u8; 16],
    cpu_ids: &[u16],
    query_server: &QueryServer,
) {
    let (active_queries, streaming_queries) = query_server.counts();
    let (metric_series, errors) = crate::diagnostics::snapshot();
    eprintln!("eventd diagnostic dump:");
    eprintln!("  boot_id: {canonical_boot_id}");
    eprintln!(
        "  shards: active={} historical_readable={}",
        active_paths.len(),
        historical_paths.len()
    );
    eprintln!("  queries: active={active_queries} streaming={streaming_queries}");
    eprintln!("  metric_series_cache: {metric_series}");
    match load_receipts(active_paths, historical_paths) {
        Ok(receipts) => {
            let mut range_counts = std::collections::HashMap::<u16, usize>::new();
            for (receipt_boot, cpu_id, _) in &receipts {
                if receipt_boot == &boot_id {
                    *range_counts.entry(*cpu_id).or_default() += 1;
                }
            }
            let coverage = Coverage::from_receipts(receipts);
            for cpu_id in cpu_ids {
                eprintln!(
                    "  cpu[{cpu_id}]: receipt_ranges={} highest_contiguous={}",
                    range_counts.get(cpu_id).copied().unwrap_or(0),
                    coverage.highest_contiguous(&boot_id, *cpu_id)
                );
            }
        }
        Err(error) => eprintln!("  receipt_coverage: unavailable: {error}"),
    }
    eprintln!(
        "  last_write_errors: event={:?} log={:?} metric={:?} metadata={:?}",
        errors.event, errors.log, errors.metric, errors.metadata
    );
}

fn flush_event_queues(queues: &[BoundedQueue<WriterMessage>]) -> Result<(), String> {
    let mut receivers = Vec::with_capacity(queues.len());
    for queue in queues {
        let (sender, receiver) = sync_channel(1);
        queue
            .reserve(core::mem::size_of::<WriterMessage>())
            .map_err(|error| error.to_string())?
            .publish(WriterMessage::Barrier(sender));
        receivers.push(receiver);
    }
    for receiver in receivers {
        receiver
            .recv()
            .map_err(|_| "event writer stopped before final commit".to_owned())??;
    }
    Ok(())
}

fn commit_synthetic_fallback(
    queues: &[BoundedQueue<WriterMessage>],
    event: &eventd_core::SyntheticEvent,
) -> Result<(), String> {
    let mut last_error = "no active event shard is writable".to_owned();
    for queue in queues {
        let (sender, receiver) = sync_channel(1);
        let permit = match queue.reserve(core::mem::size_of::<WriterMessage>()) {
            Ok(permit) => permit,
            Err(error) => {
                last_error = error.to_string();
                continue;
            }
        };
        permit.publish(WriterMessage::Synthetic(event.clone(), sender));
        match receiver.recv() {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => last_error = error,
            Err(_) => {
                "event writer stopped before synthetic commit".clone_into(&mut last_error);
            }
        }
    }
    Err(last_error)
}

fn join_worker(handle: JoinHandle<Result<(), String>>, first_error: &mut Option<String>) {
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
