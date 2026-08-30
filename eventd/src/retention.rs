//! Bounded low-priority retention coordinator.

use core::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{Sender, sync_channel};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eventd_core::{BoundedQueue, Shard};
use rusqlite::{Connection, OpenFlags};

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
    pub interval: Duration,
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
    reason = "the retention thread deliberately owns every lifetime dependency"
)]
pub fn run(
    config: RetentionConfig,
    stores: Stores,
    event_queues: Arc<[BoundedQueue<WriterMessage>]>,
    log_sender: Sender<LogMaintenance>,
    metric_sender: Sender<MetricMaintenance>,
    boot_id: [u8; 16],
    stopping: Arc<AtomicBool>,
) -> Result<(), String> {
    let mut historical_shards = stores
        .historical_event_paths
        .iter()
        .map(|path| Shard::open(path, config.checkpoint_pages).map_err(|error| error.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    while wait_interval(&stopping, config.interval) {
        pass(
            &config,
            &stores,
            &event_queues,
            &mut historical_shards,
            &log_sender,
            &metric_sender,
            &boot_id,
            &stopping,
        )?;
    }
    Ok(())
}

fn wait_interval(stopping: &AtomicBool, interval: Duration) -> bool {
    let mut remaining = interval;
    while !stopping.load(Ordering::Acquire) && !remaining.is_zero() {
        let sleep = remaining.min(Duration::from_secs(1));
        std::thread::sleep(sleep);
        remaining = remaining.saturating_sub(sleep);
    }
    !stopping.load(Ordering::Acquire)
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
        while !stopping.load(Ordering::Acquire)
            && shard
                .retain_before(cutoff(now, config.event_age)?, config.batch_rows)
                .map_err(|error| error.to_string())?
                == config.batch_rows
        {}
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
    receiver
        .recv()
        .map_err(|_| "log writer stopped during retention".to_owned())?
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
    receiver
        .recv()
        .map_err(|_| "metric writer stopped during retention".to_owned())?
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
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|error| error.to_string())
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
