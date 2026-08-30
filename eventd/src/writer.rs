//! Adaptive single-owner event-shard writer loop.

use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::mpsc::SyncSender;
use std::time::{Duration, Instant};

use eventd_core::{BoundedQueue, IngestItem, Pop, Shard, ShardError, SyntheticEvent};

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
}

#[derive(Debug, Clone)]
pub enum EventMaintenance {
    DeleteBefore { cutoff: i64, limit: usize },
    DeleteBoot { boot_id: [u8; 16], limit: usize },
    Checkpoint,
}

pub fn run(
    mut shard: Shard,
    queue: &BoundedQueue<WriterMessage>,
    max_batch_size: usize,
    max_batch_latency: Duration,
    stopping: &Arc<AtomicBool>,
    commits: &Arc<CommitSignal>,
) -> Result<(), ShardError> {
    let mut batch = Vec::with_capacity(max_batch_size);
    loop {
        let first = queue.pop_wait();
        match first {
            Pop::Item(WriterMessage::Event(item)) => batch.push(item),
            Pop::Item(control) => {
                if let Err(error) = handle_control(&mut shard, control, commits) {
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
                    if let Err(error) = shard.commit(&batch) {
                        fail_control(control, &error);
                        stopping.store(true, Ordering::Release);
                        queue.close();
                        return Err(error);
                    }
                    if !batch.is_empty() {
                        commits.committed();
                    }
                    batch.clear();
                    if let Err(error) = handle_control(&mut shard, control, commits) {
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
        if let Err(error) = shard.commit(&batch) {
            stopping.store(true, Ordering::Release);
            queue.close();
            return Err(error);
        }
        if !batch.is_empty() {
            commits.committed();
        }
        batch.clear();
        if closed {
            return Ok(());
        }
    }
}

fn handle_control(
    shard: &mut Shard,
    message: WriterMessage,
    commits: &CommitSignal,
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
    }
}

fn fail_control(message: WriterMessage, error: &ShardError) {
    match message {
        WriterMessage::Event(_) => {}
        WriterMessage::Barrier(sender) | WriterMessage::Synthetic(_, sender) => {
            let _ = sender.send(Err(error.to_string()));
        }
        WriterMessage::Maintenance(_, sender) => {
            let _ = sender.send(Err(error.to_string()));
        }
    }
}
