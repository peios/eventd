//! Adaptive single-owner event-shard writer loop.

use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::mpsc::SyncSender;
use std::time::{Duration, Instant};

use eventd_core::{BoundedQueue, IngestItem, Pop, Shard, ShardError, SyntheticEvent};

/// Ordered data and control messages consumed by a shard's sole owner.
pub enum WriterMessage {
    /// One KMES handoff from a drain thread.
    Event(IngestItem),
    /// Commit everything before this marker, then acknowledge it.
    Barrier(SyncSender<Result<(), String>>),
    /// Commit a daemon-generated event immediately, then acknowledge it.
    Synthetic(SyntheticEvent, SyncSender<Result<(), String>>),
}

pub fn run(
    mut shard: Shard,
    queue: &BoundedQueue<WriterMessage>,
    max_batch_size: usize,
    max_batch_latency: Duration,
    stopping: &Arc<AtomicBool>,
) -> Result<(), ShardError> {
    let mut batch = Vec::with_capacity(max_batch_size);
    loop {
        let first = queue.pop_wait();
        match first {
            Pop::Item(WriterMessage::Event(item)) => batch.push(item),
            Pop::Item(control) => {
                if let Err(error) = handle_control(&mut shard, control) {
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
                    batch.clear();
                    if let Err(error) = handle_control(&mut shard, control) {
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
        batch.clear();
        if closed {
            return Ok(());
        }
    }
}

fn handle_control(shard: &mut Shard, message: WriterMessage) -> Result<(), ShardError> {
    match message {
        WriterMessage::Event(_) => unreachable!("events are handled by the batch loop"),
        WriterMessage::Barrier(sender) => {
            let _ = sender.send(Ok(()));
            Ok(())
        }
        WriterMessage::Synthetic(event, sender) => match shard.commit_synthetic(&event) {
            Ok(()) => {
                let _ = sender.send(Ok(()));
                Ok(())
            }
            Err(error) => {
                let _ = sender.send(Err(error.to_string()));
                Err(error)
            }
        },
    }
}

fn fail_control(message: WriterMessage, error: &ShardError) {
    match message {
        WriterMessage::Event(_) => {}
        WriterMessage::Barrier(sender) | WriterMessage::Synthetic(_, sender) => {
            let _ = sender.send(Err(error.to_string()));
        }
    }
}
