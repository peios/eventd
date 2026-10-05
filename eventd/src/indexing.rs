//! Lock-decoupled adaptive-index accounting and policy.

use core::fmt;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{Receiver, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use eventd_core::{BoundedQueue, DesiredIndex, IndexCounter, MetaStore};

use crate::config::{Config, SharedConfig};
use crate::query_language::{CrossFilter, Query, Source};
use crate::writer::WriterMessage;

const HEADER_FIELDS: [&str; 7] = [
    "event_type",
    "origin_class",
    "cpu_id",
    "effective_token_guid",
    "true_token_guid",
    "process_guid",
    "boot_id",
];

#[derive(Debug, Clone, Copy)]
pub struct PolicyConfig {
    pub interval: Duration,
    pub create_threshold: u64,
    pub drop_threshold: u64,
}

#[derive(Debug, Clone)]
struct Counter {
    count: u64,
    window_start: u64,
}

pub struct Tracker {
    counters: Mutex<HashMap<String, Counter>>,
    runtime: SharedConfig,
}

impl Tracker {
    pub fn from_persisted(counters: Vec<IndexCounter>, runtime: SharedConfig) -> Self {
        Self {
            counters: Mutex::new(
                counters
                    .into_iter()
                    .map(|counter| {
                        (
                            counter.field_path,
                            Counter {
                                count: counter.query_count,
                                window_start: counter.window_start,
                            },
                        )
                    })
                    .collect(),
            ),
            runtime,
        }
    }

    pub fn record_query(&self, query: &Query) {
        if !matches!(query.source, Source::Events { .. }) || query.index.is_some() {
            return;
        }
        let mut fields = Vec::new();
        for predicate in &query.predicates {
            predicate.fields(&mut fields);
        }
        for filter in &query.cross_filters {
            match filter {
                CrossFilter::Metric { labels, .. } => {
                    fields.push("value".to_owned());
                    if let Some(labels) = labels {
                        for label in labels {
                            label.fields(&mut fields);
                        }
                    }
                }
                CrossFilter::EventExists { .. } => fields.push("event_type".to_owned()),
                CrossFilter::LogExists { containing, .. } => {
                    fields.push("origin".to_owned());
                    if containing.is_some() {
                        fields.push("message".to_owned());
                    }
                }
            }
        }
        self.record_fields(fields, 1);
    }

    pub fn prioritize(&self, field: &str) {
        let create_threshold = Config::read(&self.runtime, |config| {
            config.adaptive_index_create_threshold
        });
        self.record_fields([field.to_owned()], create_threshold);
    }

    fn record_fields(&self, fields: impl IntoIterator<Item = String>, increment: u64) {
        let now = realtime_ns().unwrap_or(0);
        let window_ns = duration_ns(Config::read(&self.runtime, |config| {
            config.adaptive_index_window
        }));
        let mut counters = self
            .counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for field in fields {
            if field == "timestamp" || field == "payload" {
                continue;
            }
            let counter = counters.entry(field).or_insert(Counter {
                count: 0,
                window_start: now,
            });
            rotate(counter, now, window_ns);
            counter.count = counter.count.saturating_add(increment);
        }
        drop(counters);
    }

    fn snapshot(&self) -> Vec<IndexCounter> {
        let now = realtime_ns().unwrap_or(0);
        let window_ns = duration_ns(Config::read(&self.runtime, |config| {
            config.adaptive_index_window
        }));
        let mut counters = self
            .counters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let snapshot = counters
            .iter_mut()
            .map(|(field_path, counter)| {
                rotate(counter, now, window_ns);
                IndexCounter {
                    field_path: field_path.clone(),
                    query_count: counter.count,
                    window_start: counter.window_start,
                }
            })
            .collect();
        drop(counters);
        snapshot
    }
}

const fn rotate(counter: &mut Counter, now: u64, window_ns: u64) {
    if now.saturating_sub(counter.window_start) >= window_ns {
        counter.count = 0;
        counter.window_start = now;
    }
}

pub enum PolicyMessage {
    Recompute,
    /// Graceful shutdown: the final policy run, then — policy activity
    /// having stopped (TRM §3.5) — the sequence checkpoints, then exit.
    Stop {
        checkpoint: Option<SequenceCheckpoint>,
        reply: SyncSender<Result<(), String>>,
    },
}

/// Each CPU's highest contiguously covered sequence, for diagnostics.
pub struct SequenceCheckpoint {
    pub boot_id: [u8; 16],
    pub sequences: Vec<(u16, u64)>,
    pub updated_at: u64,
}

pub fn run(
    mut store: MetaStore,
    tracker: &Arc<Tracker>,
    desired: &Arc<RwLock<Vec<DesiredIndex>>>,
    queues: &Arc<[BoundedQueue<WriterMessage>]>,
    runtime: &SharedConfig,
    receiver: &Receiver<PolicyMessage>,
) -> Result<(), IndexError> {
    broadcast_desired(
        &desired
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner),
        queues,
    );
    loop {
        let (config, checkpoint_pages) = Config::read(runtime, |live| {
            (
                PolicyConfig {
                    interval: live.adaptive_index_policy_interval,
                    create_threshold: live.adaptive_index_create_threshold,
                    drop_threshold: live.adaptive_index_drop_threshold,
                },
                live.wal_checkpoint_pages,
            )
        });
        store.set_checkpoint_pages(checkpoint_pages);
        match receiver.recv_timeout(config.interval) {
            Ok(PolicyMessage::Recompute) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                if let Err(error) = recompute(&mut store, tracker, desired, queues, config) {
                    crate::diagnostics::metadata_error(&error);
                    return Err(error);
                }
            }
            Ok(PolicyMessage::Stop { checkpoint, reply }) => {
                // No broadcast: the writers are stopping, and the next
                // startup broadcasts the persisted set.
                let flushed = recompute(&mut store, tracker, desired, &[], config)
                    .map_err(|error| format!("final policy flush failed: {error}"));
                let checkpointed = checkpoint.map_or(Ok(()), |checkpoint| {
                    store
                        .write_sequence_checkpoints(
                            &checkpoint.boot_id,
                            &checkpoint.sequences,
                            checkpoint.updated_at,
                        )
                        .map_err(|error| format!("checkpoint write failed: {error}"))
                });
                let result = flushed.and(checkpointed);
                if let Err(error) = &result {
                    let error = IndexError::Metadata(error.clone());
                    crate::diagnostics::metadata_error(&error);
                    let _ = reply.send(result);
                    return Err(error);
                }
                let _ = reply.send(result);
                return Ok(());
            }
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        }
    }
}

fn recompute(
    store: &mut MetaStore,
    tracker: &Tracker,
    desired: &RwLock<Vec<DesiredIndex>>,
    queues: &[BoundedQueue<WriterMessage>],
    config: PolicyConfig,
) -> Result<(), IndexError> {
    let counters = tracker.snapshot();
    let existing: HashSet<_> = desired
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .iter()
        .map(|index| index.field_path.clone())
        .collect();
    let mut wanted: Vec<_> = counters
        .iter()
        .filter(|counter| {
            if existing.contains(&counter.field_path) {
                counter.query_count >= config.drop_threshold
            } else {
                counter.query_count >= config.create_threshold
            }
        })
        .collect();
    wanted.sort_unstable_by(|left, right| {
        right
            .query_count
            .cmp(&left.query_count)
            .then_with(|| left.field_path.cmp(&right.field_path))
    });
    let indexes: Vec<_> = wanted
        .into_iter()
        .enumerate()
        .map(|(priority, counter)| DesiredIndex {
            field_path: counter.field_path.clone(),
            priority: u64::try_from(priority).unwrap_or(u64::MAX),
            is_expression: !HEADER_FIELDS.contains(&counter.field_path.as_str()),
        })
        .collect();
    store.write_index_state(&counters, &indexes)?;
    desired
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone_from(&indexes);
    broadcast_desired(&indexes, queues);
    Ok(())
}

fn broadcast_desired(indexes: &[DesiredIndex], queues: &[BoundedQueue<WriterMessage>]) {
    let snapshot: Arc<[DesiredIndex]> = indexes.into();
    for queue in queues {
        if let Ok(permit) = queue.try_reserve(core::mem::size_of::<WriterMessage>()) {
            permit.publish(WriterMessage::IndexPolicy(Arc::clone(&snapshot)));
        }
    }
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn realtime_ns() -> Result<u64, std::time::SystemTimeError> {
    Ok(u64::try_from(SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()).unwrap_or(u64::MAX))
}

#[derive(Debug)]
pub enum IndexError {
    Metadata(String),
    Store(eventd_core::MetaStoreError),
}

impl fmt::Display for IndexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Metadata(error) => formatter.write_str(error),
            Self::Store(error) => write!(formatter, "adaptive-index metadata failure: {error}"),
        }
    }
}

impl std::error::Error for IndexError {}

impl From<eventd_core::MetaStoreError> for IndexError {
    fn from(error: eventd_core::MetaStoreError) -> Self {
        Self::Store(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // PEI-1297 item 8 (TRM §3.5): graceful shutdown writes the sequence
    // checkpoints after policy activity has stopped, so the final policy
    // run comes first, in the same Stop, and broadcasts nothing to writers
    // that are stopping.
    #[test]
    fn stop_runs_the_final_policy_then_writes_the_sequence_checkpoints() {
        use eventd_core::Pop;

        let mut directory = std::env::temp_dir();
        directory.push(format!(
            "eventd-indexing-test-{}-{}",
            std::process::id(),
            realtime_ns().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("eventd-meta.db");
        let runtime = Config::test_defaults().shared();
        let tracker = Arc::new(Tracker::from_persisted(Vec::new(), Arc::clone(&runtime)));
        tracker.prioritize("process_guid");
        let desired = Arc::new(RwLock::new(Vec::new()));
        let queues: Arc<[BoundedQueue<WriterMessage>]> =
            Arc::from(vec![BoundedQueue::new(16, 1 << 20).unwrap()]);
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let (reply, replied) = std::sync::mpsc::sync_channel(1);
        sender
            .send(PolicyMessage::Stop {
                checkpoint: Some(SequenceCheckpoint {
                    boot_id: [1; 16],
                    sequences: vec![(0, 42)],
                    updated_at: 7,
                }),
                reply,
            })
            .unwrap();
        run(
            MetaStore::open(&path, 1_000).unwrap(),
            &tracker,
            &desired,
            &queues,
            &runtime,
            &receiver,
        )
        .unwrap();
        assert_eq!(replied.recv().unwrap(), Ok(()));
        assert!(
            matches!(queues[0].pop(), Pop::Item(WriterMessage::IndexPolicy(set)) if set.is_empty()),
            "the startup broadcast"
        );
        assert!(
            matches!(queues[0].pop(), Pop::Empty),
            "and none from the final run"
        );

        let (_, persisted) = MetaStore::open(&path, 1_000)
            .unwrap()
            .load_index_state()
            .unwrap();
        assert_eq!(
            persisted
                .iter()
                .map(|index| index.field_path.as_str())
                .collect::<Vec<_>>(),
            ["process_guid"],
            "the final policy run was persisted"
        );
        let sequence: i64 = rusqlite::Connection::open(&path)
            .unwrap()
            .query_row(
                "SELECT sequence FROM sequence_checkpoints WHERE cpu_id = 0",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(sequence, 42, "and the checkpoints after it");
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn event_queries_account_once_per_referenced_predicate_field() {
        let mut config = Config::test_defaults();
        config.adaptive_index_window = Duration::from_mins(1);
        config.adaptive_index_create_threshold = 10;
        let tracker = Tracker::from_persisted(Vec::new(), config.shared());
        let query = crate::query_language::parse(
            "EVENTS SINCE 1h ago WHERE event_type == kacs.denied \
             WHERE payload.subject == alice",
        )
        .unwrap();
        tracker.record_query(&query);
        tracker.prioritize("process_guid");
        let counters: HashMap<_, _> = tracker
            .snapshot()
            .into_iter()
            .map(|counter| (counter.field_path, counter.query_count))
            .collect();
        assert_eq!(counters["event_type"], 1);
        assert_eq!(counters["payload.subject"], 1);
        assert_eq!(counters["process_guid"], 10);
    }
}
