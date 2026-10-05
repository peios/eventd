//! Adaptive single-owner event-shard writer loop.

use core::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::mpsc::SyncSender;
use std::time::{Duration, Instant};

use eventd_core::{
    BoundedQueue, DesiredIndex, Gap, Guid, IndexAction, IngestItem, Pop, Shard, ShardError,
    SyntheticEvent,
};

use crate::commit_signal::CommitSignal;
use crate::config::{Config, SharedConfig};
use crate::health::Shed;

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
    clippy::too_many_lines,
    reason = "the shard owner receives immutable batching and pressure policy explicitly"
)]
pub fn run(
    mut shard: Shard,
    shard_index: usize,
    boot_id: Guid,
    queue: &BoundedQueue<WriterMessage>,
    runtime: &SharedConfig,
    stopping: &Arc<AtomicBool>,
    commits: &Arc<CommitSignal>,
    ring_pressure: &Arc<[AtomicU8]>,
    retention_requested: &Arc<AtomicBool>,
) -> Result<(), ShardError> {
    let mut batch = Vec::with_capacity(Config::read(runtime, |config| config.max_batch_size));
    let mut pending_gaps = Vec::new();
    let mut desired: Arc<[DesiredIndex]> = Arc::from([]);
    let mut batch_history = VecDeque::new();
    // Whether the material indexes may still differ from `desired`.
    let mut unconverged = false;
    loop {
        let (max_batch_size, max_batch_latency, checkpoint_pages, shedding) =
            Config::read(runtime, |config| {
                (
                    config.max_batch_size,
                    config.max_batch_latency,
                    config.wal_checkpoint_pages,
                    SheddingConfig {
                        window: config.shedding_window,
                        batch_percent: config.shedding_batch_percent,
                        emergency_buffer_percent: config.emergency_shedding_buffer_percent,
                    },
                )
            });
        shard.set_checkpoint_pages(checkpoint_pages);
        let context = ControlContext {
            commits,
            queue,
            retention_requested,
            shard_index,
            boot_id,
        };
        let first = if unconverged && queue.is_empty() {
            // TRM §3.4: a quiet writer short of the desired set takes one
            // convergence action, then rechecks pressure. Quiet means no
            // pending events and no large batch within the shedding window,
            // the measure that shed the indexes in the first place.
            let pressure = until_quiet(&batch_history, shedding.window);
            if pressure.is_zero() {
                match converge_once(&mut shard, &desired, &context) {
                    Ok(more) => unconverged = more,
                    Err(error) => {
                        crate::diagnostics::event_error(&error);
                        stopping.store(true, Ordering::Release);
                        queue.close();
                        return Err(error);
                    }
                }
                continue;
            }
            match queue.pop_wait_timeout(pressure) {
                Pop::Empty => continue,
                popped => popped,
            }
        } else {
            queue.pop_wait()
        };
        match first {
            Pop::Item(WriterMessage::Event(item)) => batch.push(item),
            Pop::Item(control) => {
                unconverged |= matches!(control, WriterMessage::IndexPolicy(_));
                if let Err(error) = handle_control(&mut shard, control, &mut desired, &context) {
                    crate::diagnostics::event_error(&error);
                    stopping.store(true, Ordering::Release);
                    queue.close();
                    return Err(error);
                }
                continue;
            }
            Pop::Closed => return Ok(()),
            Pop::Empty => unreachable!("only a timed wait returns Empty, and it is taken above"),
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
                        shard_index,
                        boot_id,
                        &mut pending_gaps,
                        retention_requested,
                        &desired,
                        &mut batch_history,
                        &mut unconverged,
                    ) {
                        crate::diagnostics::event_error(&error);
                        fail_control(control, &error);
                        stopping.store(true, Ordering::Release);
                        queue.close();
                        return Err(error);
                    }
                    batch.clear();
                    unconverged |= matches!(control, WriterMessage::IndexPolicy(_));
                    if let Err(error) = handle_control(&mut shard, control, &mut desired, &context)
                    {
                        crate::diagnostics::event_error(&error);
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
            shard_index,
            boot_id,
            &mut pending_gaps,
            retention_requested,
            &desired,
            &mut batch_history,
            &mut unconverged,
        ) {
            crate::diagnostics::event_error(&error);
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
    shard_index: usize,
    boot_id: Guid,
    pending_gaps: &mut Vec<(u16, Gap)>,
    retention_requested: &AtomicBool,
    desired: &[DesiredIndex],
    history: &mut VecDeque<(Instant, bool)>,
    unconverged: &mut bool,
) -> Result<(), ShardError> {
    if batch.is_empty() {
        return Ok(());
    }
    if !pending_gaps.is_empty() {
        match shard.commit_gaps(&boot_id, pending_gaps) {
            Ok(_) => {
                count_lost(pending_gaps.iter().copied());
                pending_gaps.clear();
                commits.committed();
            }
            Err(error) if error.is_capacity() => {
                batch_refused(batch, pending_gaps, retention_requested, &error);
                return Ok(());
            }
            Err(error) if error.is_corruption() => {
                recover_corruption(
                    shard,
                    shard_index,
                    boot_id,
                    commits,
                    retention_requested,
                    &error,
                )?;
                match shard.commit_gaps(&boot_id, pending_gaps) {
                    Ok(_) => {
                        count_lost(pending_gaps.iter().copied());
                        pending_gaps.clear();
                        commits.committed();
                    }
                    Err(retry) if retry.is_capacity() => {
                        batch_refused(batch, pending_gaps, retention_requested, &retry);
                        return Ok(());
                    }
                    Err(retry) => return Err(retry),
                }
            }
            Err(error) => return Err(error),
        }
    }
    match shard.commit(batch) {
        Ok(_) => count_committed(shard_index, batch),
        Err(error) if error.is_capacity() => {
            batch_refused(batch, pending_gaps, retention_requested, &error);
            return Ok(());
        }
        Err(error) if error.is_corruption() => {
            record_lost(batch, pending_gaps);
            recover_corruption(
                shard,
                shard_index,
                boot_id,
                commits,
                retention_requested,
                &error,
            )?;
            return Ok(());
        }
        Err(error) => return Err(error),
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
        match shard.shed_all_indexes() {
            Ok(shed) => {
                *unconverged |= shed != 0;
                crate::health::index_shed(Shed::Emergency, shed);
            }
            Err(error) => eprintln!("eventd: adaptive event-index shedding failed: {error}"),
        }
        return Ok(());
    }
    let overloaded = history.iter().filter(|(_, large)| *large).count();
    if overloaded.saturating_mul(100)
        > history
            .len()
            .saturating_mul(shedding.batch_percent as usize)
    {
        match shard.shed_lowest_index(desired) {
            Ok(shed) => {
                *unconverged |= shed.is_some();
                crate::health::index_shed(Shed::Pressure, usize::from(shed.is_some()));
            }
            Err(error) => eprintln!("eventd: adaptive event-index shedding failed: {error}"),
        }
    }
    Ok(())
}

/// Count a committed batch's events, and the sequences its gaps name.
fn count_committed(shard_index: usize, batch: &[IngestItem]) {
    crate::health::events_stored(
        shard_index,
        batch.iter().filter(|item| item.store_event).count(),
    );
    count_lost(
        batch
            .iter()
            .flat_map(|item| item.gaps.iter().map(|gap| (item.event.cpu_id, *gap))),
    );
}

/// Count the sequences that committed gap records name (§2.5).
fn count_lost(gaps: impl Iterator<Item = (u16, Gap)>) {
    for (cpu_id, gap) in gaps {
        crate::health::events_lost(
            cpu_id,
            gap.last_sequence
                .saturating_sub(gap.first_sequence)
                .saturating_add(1),
        );
    }
}

fn record_lost(batch: &[IngestItem], pending: &mut Vec<(u16, Gap)>) {
    for item in batch {
        pending.extend(item.gaps.iter().map(|gap| (item.event.cpu_id, *gap)));
        if item.store_event {
            pending.push((
                item.event.cpu_id,
                Gap {
                    timestamp: item.event.timestamp,
                    first_sequence: item.event.sequence,
                    last_sequence: item.event.sequence,
                    preceding_timestamp: None,
                    revealing_timestamp: item.event.timestamp,
                },
            ));
        }
    }
    pending.sort_unstable_by_key(|(cpu_id, gap)| (*cpu_id, gap.first_sequence));
    let mut output = 0;
    for input in 0..pending.len() {
        let (cpu_id, gap) = pending[input];
        if output > 0
            && pending[output - 1].0 == cpu_id
            && gap.first_sequence <= pending[output - 1].1.last_sequence.saturating_add(1)
        {
            let previous = &mut pending[output - 1].1;
            previous.last_sequence = previous.last_sequence.max(gap.last_sequence);
            previous.timestamp = previous.timestamp.max(gap.timestamp);
            previous.revealing_timestamp =
                previous.revealing_timestamp.max(gap.revealing_timestamp);
        } else {
            pending[output] = (cpu_id, gap);
            output += 1;
        }
    }
    pending.truncate(output);
}

/// A batch refused for want of space: hold its ranges in the lost-batch
/// list, request retention, and say on stderr which CPUs and sequences the
/// list now holds unrecorded (TRM §9.2) — the only visibility there is while
/// the disk stays full.
fn batch_refused(
    batch: &[IngestItem],
    pending: &mut Vec<(u16, Gap)>,
    requested: &AtomicBool,
    error: &ShardError,
) {
    record_lost(batch, pending);
    crate::diagnostics::event_error(error);
    requested.store(true, Ordering::Release);
    eprintln!(
        "eventd: event store is full; batch discarded and retention requested; \
         lost and not yet recorded: {}: {error}",
        describe_lost(pending)
    );
}

/// Render lost-batch ranges as `cpu 0 sequences 4-9, cpu 2 sequences 7-7`.
fn describe_lost(pending: &[(u16, Gap)]) -> String {
    if pending.is_empty() {
        return "no sequences".to_owned();
    }
    pending
        .iter()
        .map(|(cpu_id, gap)| {
            format!(
                "cpu {cpu_id} sequences {}-{}",
                gap.first_sequence, gap.last_sequence
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn request_retention(requested: &AtomicBool, store: &str, error: &ShardError) {
    crate::diagnostics::event_error(error);
    requested.store(true, Ordering::Release);
    eprintln!("eventd: {store} store is full; batch discarded and retention requested: {error}");
}

fn recover_corruption(
    shard: &mut Shard,
    shard_index: usize,
    boot_id: Guid,
    commits: &CommitSignal,
    retention_requested: &AtomicBool,
    error: &ShardError,
) -> Result<(), ShardError> {
    let description = error.to_string();
    crate::diagnostics::event_error(error);
    eprintln!("eventd: quarantining corrupt event shard {shard_index}: {description}");
    shard.replace_corrupt()?;
    let event = crate::synthetic::storage_error(
        boot_id,
        "event",
        Some(shard_index),
        &description,
        realtime_nanoseconds()?,
    );
    match shard.commit_synthetic(&event) {
        Ok(()) => commits.committed(),
        Err(write_error) if write_error.is_capacity() => {
            request_retention(retention_requested, "event", &write_error);
        }
        Err(write_error) => return Err(write_error),
    }
    Ok(())
}

fn realtime_nanoseconds() -> Result<u64, ShardError> {
    let elapsed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| ShardError::Io(std::io::Error::other(error)))?;
    u64::try_from(elapsed.as_nanos()).map_err(|_| ShardError::IntegerRange("realtime timestamp"))
}

struct ControlContext<'a> {
    commits: &'a CommitSignal,
    queue: &'a BoundedQueue<WriterMessage>,
    retention_requested: &'a AtomicBool,
    shard_index: usize,
    boot_id: Guid,
}

fn handle_control(
    shard: &mut Shard,
    message: WriterMessage,
    current_desired: &mut Arc<[DesiredIndex]>,
    context: &ControlContext<'_>,
) -> Result<(), ShardError> {
    match message {
        WriterMessage::Event(_) => unreachable!("events are handled by the batch loop"),
        WriterMessage::Barrier(sender) => {
            let _ = sender.send(Ok(()));
            Ok(())
        }
        WriterMessage::Synthetic(event, sender) => {
            handle_synthetic(shard, &event, &sender, context)
        }
        WriterMessage::Maintenance(command, sender) => {
            handle_maintenance(shard, &command, &sender, context)
        }
        WriterMessage::IndexPolicy(desired) => {
            let mut retry = !context.queue.is_empty();
            while !retry && converge_once(shard, &desired, context)? {
                retry = !context.queue.is_empty();
            }
            *current_desired = Arc::clone(&desired);
            if retry
                && let Ok(permit) = context
                    .queue
                    .try_reserve(core::mem::size_of::<WriterMessage>())
            {
                permit.publish(WriterMessage::IndexPolicy(desired));
            }
            Ok(())
        }
    }
}

/// Take one convergence action toward `desired`, yielding to any event
/// that arrives meanwhile. `Ok(true)` while the shard may still differ.
fn converge_once(
    shard: &mut Shard,
    desired: &[DesiredIndex],
    context: &ControlContext<'_>,
) -> Result<bool, ShardError> {
    let action = shard.converge_indexes(desired, {
        let queue = context.queue.clone();
        move || !queue.is_empty()
    });
    match action {
        Ok(IndexAction::Created(_) | IndexAction::Dropped(_) | IndexAction::Cancelled) => Ok(true),
        Ok(IndexAction::Unchanged) => Ok(false),
        Err(error) if error.is_corruption() => {
            recover_corruption(
                shard,
                context.shard_index,
                context.boot_id,
                context.commits,
                context.retention_requested,
                &error,
            )?;
            Ok(true)
        }
        Err(error) => {
            eprintln!("eventd: adaptive index convergence failed: {error}");
            Ok(false)
        }
    }
}

/// How long until no large batch lies within the shedding window: zero once
/// the pressure that sheds indexes has subsided.
fn until_quiet(history: &VecDeque<(Instant, bool)>, window: Duration) -> Duration {
    history
        .iter()
        .rev()
        .find(|(_, large)| *large)
        .map_or(Duration::ZERO, |(at, _)| {
            window.saturating_sub(at.elapsed())
        })
}

fn handle_synthetic(
    shard: &mut Shard,
    event: &SyntheticEvent,
    sender: &SyncSender<Result<(), String>>,
    context: &ControlContext<'_>,
) -> Result<(), ShardError> {
    match shard.commit_synthetic(event) {
        Ok(()) => {
            context.commits.committed();
            let _ = sender.send(Ok(()));
            Ok(())
        }
        Err(error) if error.is_capacity() => {
            // Not stored: say so, and the sender tries the next shard
            // (TRM §2.6). The writer itself carries on.
            request_retention(context.retention_requested, "event", &error);
            let _ = sender.send(Err(error.to_string()));
            Ok(())
        }
        Err(error) if error.is_corruption() => {
            recover_corruption(
                shard,
                context.shard_index,
                context.boot_id,
                context.commits,
                context.retention_requested,
                &error,
            )?;
            match shard.commit_synthetic(event) {
                Ok(()) => {
                    context.commits.committed();
                    let _ = sender.send(Ok(()));
                    Ok(())
                }
                Err(retry) if retry.is_capacity() => {
                    request_retention(context.retention_requested, "event", &retry);
                    let _ = sender.send(Err(retry.to_string()));
                    Ok(())
                }
                Err(retry) => {
                    let _ = sender.send(Err(retry.to_string()));
                    Err(retry)
                }
            }
        }
        Err(error) => {
            let _ = sender.send(Err(error.to_string()));
            Err(error)
        }
    }
}

fn handle_maintenance(
    shard: &mut Shard,
    command: &EventMaintenance,
    sender: &SyncSender<Result<usize, String>>,
    context: &ControlContext<'_>,
) -> Result<(), ShardError> {
    let result = match command {
        EventMaintenance::DeleteBefore { cutoff, limit } => shard.retain_before(*cutoff, *limit),
        EventMaintenance::DeleteBoot { boot_id, limit } => shard.retain_boot(boot_id, *limit),
        EventMaintenance::Checkpoint => shard.passive_checkpoint().map(|()| 0),
    };
    match result {
        Ok(deleted) => {
            let _ = sender.send(Ok(deleted));
            Ok(())
        }
        Err(error) if error.is_capacity() => {
            let _ = sender.send(Err(error.to_string()));
            request_retention(context.retention_requested, "event", &error);
            Ok(())
        }
        Err(error) if error.is_corruption() => {
            let _ = sender.send(Err(error.to_string()));
            recover_corruption(
                shard,
                context.shard_index,
                context.boot_id,
                context.commits,
                context.retention_requested,
                &error,
            )
        }
        Err(error) => {
            let _ = sender.send(Err(error.to_string()));
            Err(error)
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

#[cfg(test)]
mod tests {
    use super::*;
    use eventd_core::RealEvent;
    use std::sync::mpsc::sync_channel;

    /// The shared state `run` borrows, as the pipeline gives each writer.
    struct Harness {
        stopping: Arc<AtomicBool>,
        commits: Arc<CommitSignal>,
        ring_pressure: Arc<[AtomicU8]>,
        retention_requested: Arc<AtomicBool>,
    }

    impl Harness {
        fn new() -> Self {
            Self {
                stopping: Arc::new(AtomicBool::new(false)),
                commits: Arc::new(CommitSignal::new()),
                ring_pressure: Arc::from([]),
                retention_requested: Arc::new(AtomicBool::new(false)),
            }
        }

        fn run(
            &self,
            shard: Shard,
            queue: &BoundedQueue<WriterMessage>,
            runtime: &SharedConfig,
        ) -> Result<(), ShardError> {
            run(
                shard,
                0,
                [1; 16],
                queue,
                runtime,
                &self.stopping,
                &self.commits,
                &self.ring_pressure,
                &self.retention_requested,
            )
        }
    }

    /// Closes the handoff when dropped, so a failing test still ends the
    /// writer and its scope can join it.
    struct CloseOnDrop<'a>(&'a BoundedQueue<WriterMessage>);

    impl Drop for CloseOnDrop<'_> {
        fn drop(&mut self) {
            self.0.close();
        }
    }

    fn real_event(sequence: u64) -> WriterMessage {
        WriterMessage::Event(IngestItem {
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
        })
    }

    fn publish(queue: &BoundedQueue<WriterMessage>, message: WriterMessage) {
        queue
            .reserve(core::mem::size_of::<WriterMessage>())
            .unwrap()
            .publish(message);
    }

    fn delete_before(
        queue: &BoundedQueue<WriterMessage>,
        cutoff: i64,
        limit: usize,
        response: SyncSender<Result<usize, String>>,
    ) {
        publish(
            queue,
            WriterMessage::Maintenance(EventMaintenance::DeleteBefore { cutoff, limit }, response),
        );
    }

    fn handoff() -> BoundedQueue<WriterMessage> {
        BoundedQueue::new(1_024, 16 * 1024 * 1024).unwrap()
    }

    /// A configuration whose batches are bounded by size alone.
    fn batched_by_size(max_batch_size: usize) -> SharedConfig {
        let mut config = Config::test_defaults();
        config.max_batch_size = max_batch_size;
        config.max_batch_latency = Duration::from_mins(1);
        config.shared()
    }

    fn stored_events(path: &std::path::Path) -> i64 {
        rusqlite::Connection::open(path)
            .unwrap()
            .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
            .unwrap()
    }

    fn temporary_directory() -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "eventd-writer-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        path
    }

    #[test]
    fn a_max_batch_size_change_between_batches_bounds_the_next_batch() {
        let directory = temporary_directory();
        let shard = Shard::open(directory.join("shard-0000.db"), 1_000).unwrap();
        let queue = handoff();
        let runtime = batched_by_size(100);
        let harness = Harness::new();
        let (settled, settled_signal) = sync_channel(1);
        // A rendezvous: the writer cannot go on until the test takes it.
        let (pause, paused) = sync_channel(0);
        let (done, done_signal) = sync_channel(1);
        let mut sequence = 0;
        for _ in 0..150 {
            sequence += 1;
            publish(&queue, real_event(sequence));
        }
        publish(&queue, WriterMessage::Barrier(settled));
        publish(&queue, WriterMessage::Barrier(pause));
        for _ in 0..300 {
            sequence += 1;
            publish(&queue, real_event(sequence));
        }
        publish(&queue, WriterMessage::Barrier(done));

        std::thread::scope(|scope| {
            let writer = scope.spawn(|| harness.run(shard, &queue, &runtime));
            let _closing = CloseOnDrop(&queue);
            let paused = paused;
            settled_signal.recv().unwrap().unwrap();
            assert_eq!(
                harness.commits.generation(),
                2,
                "150 events under MaxBatchSize 100 commit as 100 then 50"
            );
            // The writer is now between batches, held at the pause.
            runtime.write().unwrap().max_batch_size = 150;
            paused.recv().unwrap().unwrap();
            done_signal.recv().unwrap().unwrap();
            assert_eq!(
                harness.commits.generation() - 2,
                2,
                "the next 300 events commit as two batches of the new 150, not three of 100"
            );
            queue.close();
            writer.join().unwrap().unwrap();
        });
        assert_eq!(stored_events(&directory.join("shard-0000.db")), 450);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn maintenance_runs_after_the_batch_before_it_commits_and_deletes_at_most_its_limit() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let shard = Shard::open(&path, 1_000).unwrap();
        let queue = handoff();
        let runtime = batched_by_size(10_000);
        let harness = Harness::new();
        let (first, first_result) = sync_channel(1);
        let (second, second_result) = sync_channel(1);
        let (rest, rest_result) = sync_channel(1);
        // Ten events, every one older than the cutoff, then the commands
        // behind them in the same handoff.
        for sequence in 1..=10 {
            publish(&queue, real_event(sequence));
        }
        delete_before(&queue, 1_000, 3, first);
        delete_before(&queue, 1_000, 3, second);
        delete_before(&queue, 1_000, 100, rest);

        std::thread::scope(|scope| {
            let writer = scope.spawn(|| harness.run(shard, &queue, &runtime));
            let _closing = CloseOnDrop(&queue);
            // Had the command run inside or before the open batch, there
            // would have been nothing committed for it to delete.
            assert_eq!(first_result.recv().unwrap(), Ok(3));
            assert_eq!(second_result.recv().unwrap(), Ok(3));
            assert_eq!(rest_result.recv().unwrap(), Ok(4));
            assert_eq!(
                harness.commits.generation(),
                1,
                "the batch was committed once, as its own transaction"
            );
            queue.close();
            writer.join().unwrap().unwrap();
        });
        assert_eq!(stored_events(&path), 0);
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn events_handed_off_while_a_command_runs_commit_before_the_next_command() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let shard = Shard::open(&path, 1_000).unwrap();
        let queue = handoff();
        let runtime = batched_by_size(10_000);
        let harness = Harness::new();
        // The retention coordinator sends one command and waits for its
        // answer before sending the next; a rendezvous holds the writer at
        // the end of the first while the drains hand off more events.
        let (first, first_result) = sync_channel(0);
        let (next, next_result) = sync_channel(1);
        delete_before(&queue, 1_000, 100, first);

        std::thread::scope(|scope| {
            let writer = scope.spawn(|| harness.run(shard, &queue, &runtime));
            let _closing = CloseOnDrop(&queue);
            let first_result = first_result;
            for sequence in 1..=5 {
                publish(&queue, real_event(sequence));
            }
            assert_eq!(first_result.recv().unwrap(), Ok(0));
            delete_before(&queue, 1_000, 100, next);
            assert_eq!(
                next_result.recv().unwrap(),
                Ok(5),
                "the five events were committed before the next command ran"
            );
            assert_eq!(harness.commits.generation(), 1);
            queue.close();
            writer.join().unwrap().unwrap();
        });
        assert_eq!(stored_events(&path), 0);
        std::fs::remove_dir_all(directory).unwrap();
    }

    /// A shard with no room left for another row.
    fn full_shard(path: &std::path::Path) -> Shard {
        let mut shard = Shard::open(path, 1_000).unwrap();
        shard.cap_pages_for_test().unwrap();
        let filler = crate::synthetic::shutdown([1; 16], &[(0, 1)], 1);
        for _ in 0..100_000 {
            match shard.commit_synthetic(&filler) {
                Ok(()) => {}
                Err(error) if error.is_capacity() => return shard,
                Err(error) => panic!("filling the shard: {error}"),
            }
        }
        panic!("the shard did not fill");
    }

    // PEI-1292: a daemon-wide record the shard has no room for is reported
    // to its sender, which then tries the next shard, rather than being
    // acknowledged as stored and dropped. The writer carries on.
    #[test]
    fn a_synthetic_event_refused_for_space_is_reported_to_its_sender() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let shard = full_shard(&path);
        let queue = handoff();
        let runtime = batched_by_size(100);
        let harness = Harness::new();
        let (sent, result) = sync_channel(1);
        let (after, after_result) = sync_channel(1);
        publish(
            &queue,
            WriterMessage::Synthetic(crate::synthetic::shutdown([1; 16], &[], 2), sent),
        );
        publish(&queue, WriterMessage::Barrier(after));

        std::thread::scope(|scope| {
            let writer = scope.spawn(|| harness.run(shard, &queue, &runtime));
            let _closing = CloseOnDrop(&queue);
            let reply = result.recv().unwrap();
            assert!(
                reply.as_ref().is_err_and(|error| error.contains("full")),
                "the sender learns the record was not stored: {reply:?}"
            );
            assert_eq!(
                after_result.recv().unwrap(),
                Ok(()),
                "the writer carries on"
            );
            assert!(harness.retention_requested.load(Ordering::Acquire));
            queue.close();
            writer.join().unwrap().unwrap();
        });
        std::fs::remove_dir_all(directory).unwrap();
    }

    // PEI-1292 (TRM §2.6): daemon-wide events go to shard 0, else to the
    // lowest-numbered writable active shard.
    #[test]
    fn a_daemon_wide_record_goes_to_the_next_writable_shard_when_shard_0_is_full() {
        let directory = temporary_directory();
        let paths = [
            directory.join("shard-0000.db"),
            directory.join("shard-0001.db"),
        ];
        let shards = [
            full_shard(&paths[0]),
            Shard::open(&paths[1], 1_000).unwrap(),
        ];
        let queues = [handoff(), handoff()];
        let runtime = batched_by_size(100);
        let harnesses = [Harness::new(), Harness::new()];
        let change = crate::config::AppliedChange {
            key: "LogRetentionDays",
            old_value_type: "REG_DWORD",
            old_value: Some("14".into()),
            new_value_type: "REG_DWORD",
            new_value: Some("9".into()),
        };
        let event = crate::synthetic::config_change([1; 16], &change, 5);

        std::thread::scope(|scope| {
            let writers: Vec<_> = shards
                .into_iter()
                .zip(&queues)
                .zip(&harnesses)
                .map(|((shard, queue), harness)| {
                    let runtime = &runtime;
                    scope.spawn(move || harness.run(shard, queue, runtime))
                })
                .collect();
            let _closing = (CloseOnDrop(&queues[0]), CloseOnDrop(&queues[1]));
            assert_eq!(
                crate::pipeline::commit_synthetic_fallback(&queues, &event),
                Ok(())
            );
            for queue in &queues {
                queue.close();
            }
            for writer in writers {
                writer.join().unwrap().unwrap();
            }
        });
        let config_changes = |path: &std::path::Path| -> i64 {
            rusqlite::Connection::open(path)
                .unwrap()
                .query_row(
                    "SELECT count(*) FROM events WHERE event_type = 'synthetic.config_change'",
                    [],
                    |row| row.get(0),
                )
                .unwrap()
        };
        assert_eq!(config_changes(&paths[0]), 0, "not in the full shard 0");
        assert_eq!(config_changes(&paths[1]), 1, "but in shard 1");
        std::fs::remove_dir_all(directory).unwrap();
    }

    // PEI-1296 item 5 (TRM §3.4): an idle writer converges on its own once
    // pressure subsides; it does not wait for the next policy broadcast.
    #[test]
    fn an_idle_writer_rebuilds_what_pressure_shed_once_the_window_is_quiet() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let shard = Shard::open(&path, 1_000).unwrap();
        let queue = handoff();
        let mut config = Config::test_defaults();
        config.max_batch_size = 10;
        config.max_batch_latency = Duration::from_mins(1);
        config.shedding_window = Duration::from_millis(300);
        config.shedding_batch_percent = 50;
        let runtime = config.shared();
        let harness = Harness::new();
        let index_present = || -> bool {
            rusqlite::Connection::open(&path)
                .unwrap()
                .query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master \
                     WHERE type = 'index' AND name = 'idx_events_process_guid')",
                    [],
                    |row| row.get(0),
                )
                .unwrap()
        };
        let (shed, shed_signal) = sync_channel(1);
        publish(
            &queue,
            WriterMessage::IndexPolicy(Arc::from([DesiredIndex {
                field_path: "process_guid".into(),
                priority: 0,
                is_expression: false,
            }])),
        );

        std::thread::scope(|scope| {
            let writer = scope.spawn(|| harness.run(shard, &queue, &runtime));
            let _closing = CloseOnDrop(&queue);
            let deadline = Instant::now() + Duration::from_secs(10);
            while !index_present() {
                assert!(Instant::now() < deadline, "the policy's index was built");
                std::thread::sleep(Duration::from_millis(10));
            }
            // One full batch: every batch in the window was large, so the
            // graduated check sheds the index.
            for sequence in 1..=10 {
                publish(&queue, real_event(sequence));
            }
            publish(&queue, WriterMessage::Barrier(shed));
            shed_signal.recv().unwrap().unwrap();
            let shed_at = Instant::now();
            assert!(!index_present(), "pressure shed it");
            let deadline = shed_at + Duration::from_secs(10);
            while !index_present() {
                assert!(
                    Instant::now() < deadline,
                    "the idle writer rebuilt the shed index"
                );
                std::thread::sleep(Duration::from_millis(10));
            }
            assert!(
                shed_at.elapsed() >= Duration::from_millis(200),
                "but not before the shedding window had passed: {:?}",
                shed_at.elapsed()
            );
            queue.close();
            writer.join().unwrap().unwrap();
        });
        std::fs::remove_dir_all(directory).unwrap();
    }

    // PEI-1296 item 8: the disk-full line names the CPUs and sequence
    // ranges the lost-batch list holds.
    #[test]
    fn the_lost_batch_list_is_described_by_cpu_and_sequence_range() {
        let batch: Vec<_> = (3..=5)
            .map(|sequence| match real_event(sequence) {
                WriterMessage::Event(item) => item,
                _ => unreachable!(),
            })
            .collect();
        let mut pending = vec![(
            2,
            Gap {
                timestamp: 1,
                first_sequence: 7,
                last_sequence: 7,
                preceding_timestamp: None,
                revealing_timestamp: 1,
            },
        )];
        assert_eq!(describe_lost(&[]), "no sequences");
        record_lost(&batch, &mut pending);
        assert_eq!(
            describe_lost(&pending),
            "cpu 0 sequences 3-5, cpu 2 sequences 7-7"
        );
    }

    #[test]
    fn lost_batches_become_merged_per_cpu_gap_ranges() {
        let item = IngestItem {
            gaps: vec![Gap {
                timestamp: 20,
                first_sequence: 2,
                last_sequence: 3,
                preceding_timestamp: Some(10),
                revealing_timestamp: 40,
            }],
            store_event: true,
            event: RealEvent {
                boot_id: [1; 16],
                timestamp: 40,
                cpu_id: 7,
                sequence: 4,
                origin_class: 0,
                effective_token_guid: [0; 16],
                true_token_guid: [0; 16],
                process_guid: [0; 16],
                event_type: "test.event".into(),
                payload: [0x80].into(),
            },
        };
        let mut pending = vec![(
            7,
            Gap {
                timestamp: 10,
                first_sequence: 1,
                last_sequence: 1,
                preceding_timestamp: None,
                revealing_timestamp: 10,
            },
        )];
        record_lost(&[item], &mut pending);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].0, 7);
        assert_eq!(pending[0].1.first_sequence, 1);
        assert_eq!(pending[0].1.last_sequence, 4);
        assert_eq!(pending[0].1.revealing_timestamp, 40);
    }
}
