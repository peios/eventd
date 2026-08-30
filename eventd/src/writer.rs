//! Adaptive single-owner event-shard writer loop.

use core::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use eventd_core::{BoundedQueue, IngestItem, Pop, Shard, ShardError};

pub fn run(
    mut shard: Shard,
    queue: &BoundedQueue<IngestItem>,
    max_batch_size: usize,
    max_batch_latency: Duration,
    stopping: &Arc<AtomicBool>,
) -> Result<(), ShardError> {
    let mut batch = Vec::with_capacity(max_batch_size);
    loop {
        let first = queue.pop_wait();
        match first {
            Pop::Item(item) => batch.push(item),
            Pop::Closed => return Ok(()),
            Pop::Empty => unreachable!("pop_wait never returns Empty"),
        }
        let started = Instant::now();
        let mut closed = false;
        while batch.len() < max_batch_size && started.elapsed() < max_batch_latency {
            match queue.pop() {
                Pop::Item(item) => batch.push(item),
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
