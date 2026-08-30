//! Adaptive single-owner event-shard writer loop.

use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::mpsc::SyncSender;
use std::time::{Duration, Instant};

use eventd_core::{BoundedQueue, DesiredIndex, IngestItem, Pop, Shard, ShardError, SyntheticEvent};

use crate::commit_signal::CommitSignal;

/// Ordered data and control messages consumed by a shard's sole owner.
pub enum WriterMessage {
    /// One KMES handoff from a drain thread.
    Event(IngestItem),
    /// Commit everything before this marker, then acknowledge it.
    Barrier(SyncSender<Result<(), String>>),
    /// Commit a daemon-generated event immediately, then acknowledge it.
    Synthetic(SyntheticEvent, SyncSender<Result<(), String>>),
    /// One bounded low-priority retention or checkpoint operation.
    Maintenance(EventMaintenance, SyncSender<Result<usize, String>>),
    /// Reconsider one secondary index at a quiet point.
    IndexPolicy(Arc<[DesiredIndex]>),
}

#[derive(Debug, Clone)]
pub enum EventMaintenance {
    DeleteBefore { cutoff: i64, limit: usize },
    DeleteBoot { boot_id: [u8; 16], limit: usize },
    Checkpoint,
}

#[derive(Debug, Clone, Copy)]
pub struct SheddingConfig {
    pub window: Duration,
    pub batch_percent: u32,
    pub emergency_buffer_percent: u8,
}

#[allow(
    clippy::too_many_arguments,
    reason = "the shard owner receives immutable batching and pressure policy explicitly"
)]
pub fn run(
    mut shard: Shard,
    queue: &BoundedQueue<WriterMessage>,
    max_batch_size: usize,
    max_batch_latency: Duration,
    stopping: &Arc<AtomicBool>,
    commits: &Arc<CommitSignal>,
    shedding: SheddingConfig,
    ring_pressure: &Arc<[AtomicU8]>,
) -> Result<(), ShardError> {
    let mut batch = Vec::with_capacity(max_batch_size);
    let mut desired: Arc<[DesiredIndex]> = Arc::from([]);
    let mut batch_history = VecDeque::new();
    loop {
        let first = queue.pop_wait();
        match first {
            Pop::Item(WriterMessage::Event(item)) => batch.push(item),
            Pop::Item(control) => {
                if let Err(error) =
                    handle_control(&mut shard, control, commits, queue, &mut desired)
                {
                    stopping.store(true, Ordering::Release);
                    queue.close();
                    return Err(error);
                }
                continue;
            }
            Pop::Closed => return Ok(()),
            Pop::Empty => unreachable!("pop_wait never returns Empty"),
        }
        let started = Instant::now();
        let mut closed = false;
        while batch.len() < max_batch_size && started.elapsed() < max_batch_latency {
            match queue.pop() {
                Pop::Item(WriterMessage::Event(item)) => batch.push(item),
                Pop::Item(control) => {
                    if let Err(error) = commit_batch(
                        &mut shard,
                        &batch,
                        commits,
                        max_batch_size,
                        shedding,
                        ring_pressure,
                        &desired,
                        &mut batch_history,
                    ) {
                        fail_control(control, &error);
                        stopping.store(true, Ordering::Release);
                        queue.close();
                        return Err(error);
                    }
                    batch.clear();
                    if let Err(error) =
                        handle_control(&mut shard, control, commits, queue, &mut desired)
                    {
                        stopping.store(true, Ordering::Release);
                        queue.close();
                        return Err(error);
                    }
                    break;
                }
                Pop::Empty => break,
                Pop::Closed => {
                    closed = true;
                    break;
                }
            }
        }
        if let Err(error) = commit_batch(
            &mut shard,
            &batch,
            commits,
            max_batch_size,
            shedding,
            ring_pressure,
            &desired,
            &mut batch_history,
        ) {
            stopping.store(true, Ordering::Release);
            queue.close();
            return Err(error);
        }
        batch.clear();
        if closed {
            return Ok(());
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the commit boundary owns both durability notification and pressure shedding"
)]
fn commit_batch(
    shard: &mut Shard,
    batch: &[IngestItem],
    commits: &CommitSignal,
    max_batch_size: usize,
    shedding: SheddingConfig,
    ring_pressure: &[AtomicU8],
    desired: &[DesiredIndex],
    history: &mut VecDeque<(Instant, bool)>,
) -> Result<(), ShardError> {
    shard.commit(batch)?;
    if batch.is_empty() {
        return Ok(());
    }
    commits.committed();
    let now = Instant::now();
    history.push_back((
        now,
        batch.len().saturating_mul(4) > max_batch_size.saturating_mul(3),
    ));
    while history
        .front()
        .is_some_and(|(at, _)| now.duration_since(*at) > shedding.window)
    {
        history.pop_front();
    }
    let emergency = batch.len() == max_batch_size
        && ring_pressure
            .iter()
            .any(|pressure| pressure.load(Ordering::Acquire) >= shedding.emergency_buffer_percent);
    if emergency {
        shard.shed_all_indexes()?;
        return Ok(());
    }
    let overloaded = history.iter().filter(|(_, large)| *large).count();
    if overloaded.saturating_mul(100)
        > history
            .len()
            .saturating_mul(shedding.batch_percent as usize)
    {
        let _ = shard.shed_lowest_index(desired)?;
    }
    Ok(())
}

fn handle_control(
    shard: &mut Shard,
    message: WriterMessage,
    commits: &CommitSignal,
    queue: &BoundedQueue<WriterMessage>,
    current_desired: &mut Arc<[DesiredIndex]>,
) -> Result<(), ShardError> {
    match message {
        WriterMessage::Event(_) => unreachable!("events are handled by the batch loop"),
        WriterMessage::Barrier(sender) => {
            let _ = sender.send(Ok(()));
            Ok(())
        }
        WriterMessage::Synthetic(event, sender) => match shard.commit_synthetic(&event) {
            Ok(()) => {
                commits.committed();
                let _ = sender.send(Ok(()));
                Ok(())
            }
            Err(error) => {
                let _ = sender.send(Err(error.to_string()));
                Err(error)
            }
        },
        WriterMessage::Maintenance(command, sender) => {
            let result = match command {
                EventMaintenance::DeleteBefore { cutoff, limit } => {
                    shard.retain_before(cutoff, limit)
                }
                EventMaintenance::DeleteBoot { boot_id, limit } => {
                    shard.retain_boot(&boot_id, limit)
                }
                EventMaintenance::Checkpoint => shard.passive_checkpoint().map(|()| 0),
            };
            match result {
                Ok(deleted) => {
                    let _ = sender.send(Ok(deleted));
                    Ok(())
                }
                Err(error) => {
                    let _ = sender.send(Err(error.to_string()));
                    Err(error)
                }
            }
        }
        WriterMessage::IndexPolicy(desired) => {
            if queue.is_empty()
                && let Err(error) = shard.converge_indexes(&desired, {
                    let queue = queue.clone();
                    move || !queue.is_empty()
                })
            {
                eprintln!("eventd: adaptive index convergence failed: {error}");
            }
            *current_desired = desired;
            Ok(())
        }
    }
}

fn fail_control(message: WriterMessage, error: &ShardError) {
    match message {
        WriterMessage::Event(_) | WriterMessage::IndexPolicy(_) => {}
        WriterMessage::Barrier(sender) | WriterMessage::Synthetic(_, sender) => {
            let _ = sender.send(Err(error.to_string()));
        }
        WriterMessage::Maintenance(_, sender) => {
            let _ = sender.send(Err(error.to_string()));
        }
    }
}
