//! Framed query socket, access control and read-only execution.

mod executor;
mod security;
mod value;

use core::fmt;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc::SyncSender;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use peios::file::SecInfo;
use peios::msgpack::{Reader, Type, Writer};
use peios::security::{Sid, sddl};

use crate::commit_signal::CommitSignal;
use crate::config::{Config, SharedConfig};
use crate::health::{self, Failure, Refusal};
use crate::indexing::{PolicyMessage, Tracker};
use crate::metric_ingest::RollupMaintenance;
use crate::query_language::{RecordAggregate, Source};

pub use executor::{Limits, Stores};
pub use security::DescriptorCache;

pub fn watch_security_descriptors(cache: &Arc<DescriptorCache>, stopping: &AtomicBool) {
    security::watch_descriptors(cache, stopping);
}

pub fn provision_security_defaults() -> Result<(), security::SecurityError> {
    security::provision_defaults()
}

pub struct ServerConfig {
    pub runtime: SharedConfig,
    pub index_tracker: Arc<Tracker>,
    pub index_policy: SyncSender<PolicyMessage>,
    pub rollups: SyncSender<RollupMaintenance>,
    pub descriptors: Arc<DescriptorCache>,
}

struct QueryTuning {
    max_request_bytes: usize,
    response_target_bytes: usize,
    max_streaming: usize,
    max_per_user: usize,
    max_distinct_stream_values: usize,
    timeout: Duration,
    cross_type_window: Duration,
    cross_type_max_lookback: Duration,
    adaptive_rollup_min_samples: usize,
    adaptive_rollup_batch_rows: usize,
    adaptive_rollup_max_rows: usize,
    max_held_bytes: usize,
}

impl From<&Config> for QueryTuning {
    fn from(config: &Config) -> Self {
        Self {
            max_request_bytes: config.max_query_request_bytes,
            response_target_bytes: config.query_response_target_bytes,
            max_streaming: config.max_streaming_queries,
            max_per_user: config.max_queries_per_user,
            max_distinct_stream_values: config.max_distinct_stream_values,
            timeout: config.query_timeout,
            cross_type_window: config.cross_type_window,
            cross_type_max_lookback: config.cross_type_max_lookback,
            adaptive_rollup_min_samples: config.adaptive_rollup_min_samples,
            adaptive_rollup_batch_rows: config.adaptive_rollup_batch_rows,
            adaptive_rollup_max_rows: config.adaptive_rollup_max_rows,
            max_held_bytes: config.max_query_held_bytes,
        }
    }
}

/// Who may connect to the query socket: every signed-in caller. What each
/// may read is decided per identifier against eventd's own descriptors
/// (TRM §7), not by the socket, so the socket admits anyone a descriptor
/// could grant. The directory it is in stays closed (`directory.rs`).
const QUERY_SOCKET_SDDL: &str = "D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)(A;;FW;;;AU)";

/// The user SID of `SYSTEM`, whose queries `MaxQueriesPerUser` does not
/// count: SYSTEM is the machine, not one caller among others.
const SYSTEM_SID: &str = "S-1-5-18";

/// Queries running, by the caller's user SID.
type PerUser = Arc<Mutex<HashMap<Sid, usize>>>;

pub struct QueryServer {
    listener: UnixListener,
    path: PathBuf,
    identity: (u64, u64),
    active: Arc<AtomicUsize>,
    streaming: Arc<AtomicUsize>,
    per_user: PerUser,
    /// Bytes the running queries hold between them (`MaxQueryHeldBytes`).
    held: Arc<AtomicUsize>,
}

impl QueryServer {
    pub fn bind(path: &Path) -> Result<Self, QuerySocketError> {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_socket() => {
                std::fs::remove_file(path).map_err(QuerySocketError::Io)?;
            }
            Ok(_) => return Err(QuerySocketError::Occupied(path.to_owned())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(QuerySocketError::Io(error)),
        }
        let listener = UnixListener::bind(path).map_err(QuerySocketError::Io)?;
        listener
            .set_nonblocking(true)
            .map_err(QuerySocketError::Io)?;
        let metadata = std::fs::symlink_metadata(path).map_err(QuerySocketError::Io)?;

        let descriptor = sddl::parse(QUERY_SOCKET_SDDL).map_err(QuerySocketError::Peios)?;
        peios::file::set_sd(
            None,
            path,
            SecInfo::DACL,
            &descriptor,
            libc::AT_SYMLINK_NOFOLLOW,
        )
        .map_err(QuerySocketError::Peios)?;
        let actual = peios::file::get_sd(None, path, SecInfo::DACL, libc::AT_SYMLINK_NOFOLLOW)
            .map_err(QuerySocketError::Peios)?;
        let actual = sddl::format(actual.as_bytes()).map_err(QuerySocketError::Peios)?;
        let expected = sddl::format(descriptor.as_bytes()).map_err(QuerySocketError::Peios)?;
        if actual != expected {
            return Err(QuerySocketError::Protection(path.to_owned()));
        }
        Ok(Self {
            listener,
            path: path.to_owned(),
            identity: (metadata.dev(), metadata.ino()),
            active: Arc::new(AtomicUsize::new(0)),
            streaming: Arc::new(AtomicUsize::new(0)),
            per_user: PerUser::default(),
            held: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// The running and streaming query counts, for eventd's health.
    pub fn gauges(&self) -> (Arc<AtomicUsize>, Arc<AtomicUsize>) {
        (Arc::clone(&self.active), Arc::clone(&self.streaming))
    }

    pub fn counts(&self) -> (usize, usize) {
        (
            self.active.load(Ordering::Acquire),
            self.streaming.load(Ordering::Acquire),
        )
    }

    pub fn run(
        &self,
        stores: &Arc<Stores>,
        config: &Arc<ServerConfig>,
        stopping: &Arc<AtomicBool>,
        event_commits: &Arc<CommitSignal>,
        log_commits: &Arc<CommitSignal>,
    ) -> Result<(), QuerySocketError> {
        while !stopping.load(Ordering::Acquire) {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    let (max_concurrent, tuning) = Config::read(&config.runtime, |live| {
                        (live.max_concurrent_queries, QueryTuning::from(live))
                    });
                    if !try_acquire(&self.active, max_concurrent) {
                        health::query_refused(Refusal::Machine);
                        let _ = send_error(stream, "too many concurrent queries");
                        continue;
                    }
                    let worker_active = Arc::clone(&self.active);
                    let streaming = Arc::clone(&self.streaming);
                    let per_user = Arc::clone(&self.per_user);
                    let held = Arc::clone(&self.held);
                    let stores = Arc::clone(stores);
                    let config = Arc::clone(config);
                    let stopping = Arc::clone(stopping);
                    let event_commits = Arc::clone(event_commits);
                    let log_commits = Arc::clone(log_commits);
                    let spawned = std::thread::Builder::new()
                        .name("eventd-query".to_owned())
                        .spawn(move || {
                            let _guard = CounterGuard(worker_active);
                            if let Err(error) = handle(
                                stream,
                                &stores,
                                &config,
                                &tuning,
                                &streaming,
                                &per_user,
                                &held,
                                &stopping,
                                &event_commits,
                                &log_commits,
                            ) {
                                eprintln!("eventd: query failed: {error}");
                            }
                        });
                    if let Err(error) = spawned {
                        self.active.fetch_sub(1, Ordering::Release);
                        return Err(QuerySocketError::Io(error));
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                Err(error) => return Err(QuerySocketError::Io(error)),
            }
        }
        while self.active.load(Ordering::Acquire) != 0 {
            std::thread::sleep(Duration::from_millis(10));
        }
        Ok(())
    }

    pub fn unlink(&self) {
        unlink_if_owned(&self.path, self.identity);
    }
}

impl Drop for QueryServer {
    fn drop(&mut self) {
        self.unlink();
    }
}

fn unlink_if_owned(path: &Path, identity: (u64, u64)) {
    if std::fs::symlink_metadata(path).is_ok_and(|metadata| {
        metadata.file_type().is_socket() && (metadata.dev(), metadata.ino()) == identity
    }) {
        let _ = std::fs::remove_file(path);
    }
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "connection ownership and its protocol state remain linear and auditable"
)]
fn handle(
    mut stream: UnixStream,
    stores: &Stores,
    config: &ServerConfig,
    tuning: &QueryTuning,
    streaming_count: &Arc<AtomicUsize>,
    per_user: &PerUser,
    held: &Arc<AtomicUsize>,
    stopping: &AtomicBool,
    event_commits: &CommitSignal,
    log_commits: &CommitSignal,
) -> Result<(), QuerySocketError> {
    stream
        .set_read_timeout(Some(tuning.timeout))
        .map_err(QuerySocketError::Io)?;
    stream
        .set_write_timeout(Some(tuning.timeout))
        .map_err(QuerySocketError::Io)?;
    let authorizer =
        security::Authorizer::from_peer(stream.as_fd(), Arc::clone(&config.descriptors))
            .map_err(QuerySocketError::Security)?;
    // One caller cannot take every query the machine allows. The slot is
    // taken before the request is read, so a caller holding idle
    // connections holds slots.
    let user = authorizer.user().map_err(QuerySocketError::Security)?;
    let Some(_user_slot) = UserSlot::take(per_user, user, tuning.max_per_user) else {
        health::query_refused(Refusal::User);
        send_error(&mut stream, "too many concurrent queries from this user")?;
        return Ok(());
    };
    let query_text = match read_request(&mut stream, tuning.max_request_bytes) {
        Ok(query) => query,
        Err(error) => {
            send_error(&mut stream, &error.to_string())?;
            return Ok(());
        }
    };
    let deadline = Instant::now() + tuning.timeout;
    let query = match crate::query_language::parse(&query_text) {
        Ok(query) => query,
        Err(error) => {
            send_error(&mut stream, &error.to_string())?;
            return Ok(());
        }
    };
    if Instant::now() >= deadline {
        health::query_failed(Failure::Timeout);
        send_error(&mut stream, "query timed out")?;
        return Ok(());
    }
    if let Some(field) = query.index.as_deref() {
        match authorizer.administer() {
            Ok(true) => {}
            Ok(false) => {
                send_error(&mut stream, "INDEX requires EVENTD_ADMINISTER")?;
                return Ok(());
            }
            Err(error) => {
                send_error(&mut stream, &error.to_string())?;
                return Ok(());
            }
        }
        config.index_tracker.prioritize(field);
        let _ = config.index_policy.try_send(PolicyMessage::Recompute);
        send_status(&mut stream, "end", Some(deadline))?;
        return Ok(());
    }
    config.index_tracker.record_query(&query);
    let stream_guard = if query.stream {
        if !try_acquire(streaming_count, tuning.max_streaming) {
            health::query_refused(Refusal::Streaming);
            send_error(&mut stream, "too many concurrent streaming queries")?;
            return Ok(());
        }
        Some(CounterGuard(Arc::clone(streaming_count)))
    } else {
        None
    };
    let signal = match query.source {
        Source::Logs { .. } => log_commits,
        Source::Events { .. } | Source::Metric { .. } => event_commits,
    };
    let mut observed_generation = signal.generation();
    let limits = Limits {
        deadline,
        cross_type_window: tuning.cross_type_window,
        cross_type_max_lookback: tuning.cross_type_max_lookback,
        rollups: Some(config.rollups.clone()),
        adaptive_rollup_min_samples: tuning.adaptive_rollup_min_samples,
        adaptive_rollup_batch_rows: tuning.adaptive_rollup_batch_rows,
        adaptive_rollup_max_rows: tuning.adaptive_rollup_max_rows,
        held: executor::HeldBudget {
            used: Arc::clone(held),
            limit: tuning.max_held_bytes,
        },
    };
    // The initial result set goes out as it is produced. If the query then
    // fails, the error that follows tells the client to discard every "ok"
    // it has had (PSPU §3.16).
    let mut sender = Sender::new(
        &mut stream,
        tuning.response_target_bytes,
        deadline,
        if query.stream {
            distinct_field(&query).map(|field| (field, tuning.max_distinct_stream_values))
        } else {
            None
        },
    );
    let outcome = if query.stream {
        executor::start_stream(&query, stores, &authorizer, &limits, &mut |record| {
            sender.push(&record)
        })
        .map(Some)
    } else {
        executor::execute(&query, stores, &authorizer, &limits, &mut |record| {
            sender.push(&record)
        })
        .map(|()| None)
    };
    let (mut stream_state, mut seen) = match outcome {
        Ok(state) => (state, sender.finish()?),
        // The socket itself failed: there is nobody left to tell.
        Err(executor::QueryError::Delivery(error)) => return Err(*error),
        Err(error) => {
            drop(sender);
            count_failure(&error);
            send_error(&mut stream, &error.to_string())?;
            return Ok(());
        }
    };
    if query.stream {
        send_status(&mut stream, "watch", Some(deadline))?;
        stream.set_nonblocking(true).map_err(QuerySocketError::Io)?;
        while !stopping.load(Ordering::Acquire) {
            match peer_state(&stream) {
                Ok(PeerState::Closed) => break,
                Ok(PeerState::Data) => {
                    return Err(QuerySocketError::Protocol("unexpected client data".into()));
                }
                Ok(PeerState::Idle) => {
                    let generation =
                        signal.wait_for_change(observed_generation, Duration::from_millis(100));
                    if generation == observed_generation {
                        continue;
                    }
                    observed_generation = generation;
                    let Some(state) = stream_state.as_mut() else {
                        unreachable!("stream query has stream state")
                    };
                    let mut records =
                        match executor::stream_next(state, &query, stores, &authorizer) {
                            Ok(records) => records,
                            Err(error) => {
                                count_failure(&error);
                                let _ = send_error(&mut stream, &error.to_string());
                                return Ok(());
                            }
                        };
                    if let (Some(values), Some(field)) = (seen.as_mut(), distinct_field(&query)) {
                        records.retain(|record| {
                            let value = record.get(field).cloned().unwrap_or(value::Value::Null);
                            if values.iter().any(|seen| seen.language_equal(&value)) {
                                false
                            } else {
                                values.push(value);
                                true
                            }
                        });
                        if values.len() > tuning.max_distinct_stream_values {
                            let _ = send_error(
                                &mut stream,
                                "DISTINCT stream exceeds its seen-value limit",
                            );
                            return Ok(());
                        }
                    }
                    if !records.is_empty() {
                        send_records(&mut stream, &records, tuning.response_target_bytes, None)?;
                    }
                }
                Err(error) => return Err(QuerySocketError::Io(error)),
            }
        }
        let _ = send_error(&mut stream, "eventd is shutting down");
    } else {
        send_status(&mut stream, "end", Some(deadline))?;
    }
    drop(stream_guard);
    Ok(())
}

/// A query that ran into its time or memory limit, for eventd's health.
fn count_failure(error: &executor::QueryError) {
    match error {
        executor::QueryError::Timeout => health::query_failed(Failure::Timeout),
        executor::QueryError::HeldLimit => health::query_failed(Failure::HeldBytes),
        _ => {}
    }
}

fn distinct_field(query: &crate::query_language::Query) -> Option<&str> {
    match &query.aggregate {
        Some(RecordAggregate::Distinct(field)) => Some(field),
        _ => None,
    }
}

/// Sends an initial result set as it is produced, in `ok` messages of
/// whole records grouped toward the response target (PSPU §3.15–§3.16).
struct Sender<'a> {
    stream: &'a mut UnixStream,
    target: usize,
    deadline: Instant,
    chunk: Vec<Vec<u8>>,
    size: usize,
    sent: bool,
    /// For a DISTINCT stream: its field, the values sent so far, which
    /// seed the watch phase's seen set, and the bound on them.
    distinct: Option<(&'a str, Vec<value::Value>, usize)>,
}

/// What an `ok` message costs beyond its records.
const OK_OVERHEAD: usize = 32;

impl<'a> Sender<'a> {
    const fn new(
        stream: &'a mut UnixStream,
        target: usize,
        deadline: Instant,
        distinct: Option<(&'a str, usize)>,
    ) -> Self {
        Self {
            stream,
            target,
            deadline,
            chunk: Vec::new(),
            size: OK_OVERHEAD,
            sent: false,
            distinct: match distinct {
                Some((field, bound)) => Some((field, Vec::new(), bound)),
                None => None,
            },
        }
    }

    fn push(&mut self, record: &value::Record) -> Result<(), executor::QueryError> {
        if let Some((field, seen, bound)) = self.distinct.as_mut() {
            seen.push(record.get(*field).cloned().unwrap_or(value::Value::Null));
            if seen.len() > *bound {
                return Err(executor::QueryError::DistinctStreamLimit);
            }
        }
        let encoded = encode_record(record).map_err(executor::QueryError::delivery)?;
        // A record never shares a message it would push over the target,
        // and one larger than the target travels alone.
        if !self.chunk.is_empty() && self.size.saturating_add(encoded.len()) > self.target {
            self.flush().map_err(executor::QueryError::delivery)?;
        }
        self.size = self.size.saturating_add(encoded.len());
        self.chunk.push(encoded);
        Ok(())
    }

    fn flush(&mut self) -> Result<(), QuerySocketError> {
        send_ok(self.stream, &self.chunk, Some(self.deadline))?;
        self.chunk.clear();
        self.size = OK_OVERHEAD;
        self.sent = true;
        Ok(())
    }

    /// Sends what is left, or the one empty `ok` that says nothing
    /// matched, and gives back a DISTINCT stream's seen values.
    fn finish(mut self) -> Result<Option<Vec<value::Value>>, QuerySocketError> {
        if !self.chunk.is_empty() || !self.sent {
            self.flush()?;
        }
        Ok(self.distinct.map(|(_, seen, _)| seen))
    }
}

enum PeerState {
    Closed,
    Data,
    Idle,
}

fn peer_state(stream: &UnixStream) -> Result<PeerState, std::io::Error> {
    let mut byte = 0_u8;
    // SAFETY: byte is a writable one-byte buffer and the stream fd is live.
    let result = unsafe {
        libc::recv(
            stream.as_raw_fd(),
            (&raw mut byte).cast(),
            1,
            libc::MSG_PEEK | libc::MSG_DONTWAIT,
        )
    };
    match result.cmp(&0) {
        core::cmp::Ordering::Equal => Ok(PeerState::Closed),
        core::cmp::Ordering::Greater => Ok(PeerState::Data),
        core::cmp::Ordering::Less => {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::WouldBlock {
                Ok(PeerState::Idle)
            } else {
                Err(error)
            }
        }
    }
}

fn read_request(stream: &mut UnixStream, ceiling: usize) -> Result<String, QuerySocketError> {
    let mut prefix = [0_u8; 4];
    stream
        .read_exact(&mut prefix)
        .map_err(QuerySocketError::Io)?;
    let length = u32::from_le_bytes(prefix) as usize;
    if length > ceiling {
        return Err(QuerySocketError::Protocol(format!(
            "query request exceeds {ceiling} bytes"
        )));
    }
    let mut payload = vec![0_u8; length];
    stream
        .read_exact(&mut payload)
        .map_err(QuerySocketError::Io)?;
    peios::msgpack::validate(&payload, peios::msgpack::DEFAULT_MAX_DEPTH)
        .map_err(QuerySocketError::Peios)?;
    let mut reader = Reader::new(&payload);
    if reader.peek() != Some(Type::Map) {
        return Err(QuerySocketError::Protocol(
            "query request is not a map".into(),
        ));
    }
    let count = reader.read_map().map_err(QuerySocketError::Peios)?;
    let mut seen = std::collections::HashSet::with_capacity(count);
    let mut query = None;
    for _ in 0..count {
        if reader.peek() != Some(Type::Str) {
            return Err(QuerySocketError::Protocol(
                "query request key is not a string".into(),
            ));
        }
        let key = reader.read_str().map_err(QuerySocketError::Peios)?;
        if !seen.insert(key) {
            return Err(QuerySocketError::Protocol(
                "duplicate query request key".into(),
            ));
        }
        if key == "query" {
            if reader.peek() != Some(Type::Str) {
                return Err(QuerySocketError::Protocol(
                    "query field is not a string".into(),
                ));
            }
            query = Some(
                reader
                    .read_str()
                    .map_err(QuerySocketError::Peios)?
                    .to_owned(),
            );
        } else {
            reader.skip().map_err(QuerySocketError::Peios)?;
        }
    }
    query.ok_or_else(|| QuerySocketError::Protocol("query field is missing".into()))
}

fn send_records(
    stream: &mut UnixStream,
    records: &[value::Record],
    target: usize,
    deadline: Option<Instant>,
) -> Result<(), QuerySocketError> {
    if records.is_empty() {
        return send_ok(stream, &[], deadline);
    }
    let encoded: Vec<Vec<u8>> = records
        .iter()
        .map(encode_record)
        .collect::<Result<_, _>>()?;
    let mut start = 0;
    while start < encoded.len() {
        let mut end = start;
        let mut size = 32_usize;
        while end < encoded.len()
            && (end == start || size.saturating_add(encoded[end].len()) <= target)
        {
            size = size.saturating_add(encoded[end].len());
            end += 1;
        }
        send_ok(stream, &encoded[start..end], deadline)?;
        start = end;
    }
    Ok(())
}

fn encode_record(record: &value::Record) -> Result<Vec<u8>, QuerySocketError> {
    let mut writer = Writer::new();
    writer.write_map(u32::try_from(record.len()).map_err(|_| QuerySocketError::FrameTooLarge)?);
    for (field, value) in record {
        writer.write_str(field);
        value.write(&mut writer);
    }
    writer.to_bytes().map_err(QuerySocketError::Peios)
}

fn send_ok(
    stream: &mut UnixStream,
    records: &[Vec<u8>],
    deadline: Option<Instant>,
) -> Result<(), QuerySocketError> {
    let mut writer = Writer::new();
    writer
        .write_map(2)
        .write_str("status")
        .write_str("ok")
        .write_str("records")
        .write_array(u32::try_from(records.len()).map_err(|_| QuerySocketError::FrameTooLarge)?);
    for record in records {
        writer.write_raw(record);
    }
    send_frame(
        stream,
        &writer.to_bytes().map_err(QuerySocketError::Peios)?,
        deadline,
    )
}

fn send_status(
    stream: &mut UnixStream,
    status: &str,
    deadline: Option<Instant>,
) -> Result<(), QuerySocketError> {
    let mut writer = Writer::new();
    writer.write_map(1).write_str("status").write_str(status);
    send_frame(
        stream,
        &writer.to_bytes().map_err(QuerySocketError::Peios)?,
        deadline,
    )
}

fn send_error(mut stream: impl Write, error: &str) -> Result<(), QuerySocketError> {
    let mut writer = Writer::new();
    writer
        .write_map(2)
        .write_str("status")
        .write_str("error")
        .write_str("error")
        .write_str(error);
    let payload = writer.to_bytes().map_err(QuerySocketError::Peios)?;
    let length = u32::try_from(payload.len()).map_err(|_| QuerySocketError::FrameTooLarge)?;
    stream
        .write_all(&length.to_le_bytes())
        .map_err(QuerySocketError::Io)?;
    stream.write_all(&payload).map_err(QuerySocketError::Io)
}

fn send_frame(
    stream: &mut UnixStream,
    payload: &[u8],
    deadline: Option<Instant>,
) -> Result<(), QuerySocketError> {
    if let Some(deadline) = deadline {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .ok_or(QuerySocketError::Timeout)?;
        stream
            .set_write_timeout(Some(remaining))
            .map_err(QuerySocketError::Io)?;
    }
    let length = u32::try_from(payload.len()).map_err(|_| QuerySocketError::FrameTooLarge)?;
    stream
        .write_all(&length.to_le_bytes())
        .map_err(QuerySocketError::Io)?;
    stream.write_all(payload).map_err(QuerySocketError::Io)
}

fn try_acquire(counter: &AtomicUsize, limit: usize) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            (value < limit).then_some(value + 1)
        })
        .is_ok()
}

struct CounterGuard(Arc<AtomicUsize>);

impl Drop for CounterGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

/// One of a user's `MaxQueriesPerUser` queries, given back when dropped.
/// SYSTEM's are not counted, and hold nothing.
struct UserSlot {
    per_user: PerUser,
    user: Option<Sid>,
}

impl UserSlot {
    fn take(per_user: &PerUser, user: Sid, limit: usize) -> Option<Self> {
        if user.to_string() == SYSTEM_SID {
            return Some(Self {
                per_user: Arc::clone(per_user),
                user: None,
            });
        }
        let mut counts = per_user.lock().unwrap_or_else(PoisonError::into_inner);
        let count = counts.get(&user).copied().unwrap_or(0);
        if count >= limit {
            return None;
        }
        counts.insert(user, count + 1);
        drop(counts);
        Some(Self {
            per_user: Arc::clone(per_user),
            user: Some(user),
        })
    }
}

impl Drop for UserSlot {
    fn drop(&mut self) {
        let Some(user) = self.user else { return };
        let mut counts = self.per_user.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(count) = counts.get_mut(&user) {
            *count -= 1;
            if *count == 0 {
                counts.remove(&user);
            }
        }
    }
}

#[derive(Debug)]
pub enum QuerySocketError {
    Io(std::io::Error),
    Peios(peios::Error),
    Security(security::SecurityError),
    Protocol(String),
    Occupied(PathBuf),
    /// The socket did not take the descriptor it was given.
    Protection(PathBuf),
    FrameTooLarge,
    Timeout,
}

impl fmt::Display for QuerySocketError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "query socket failure: {error}"),
            Self::Peios(error) => write!(formatter, "query protocol failure: {error}"),
            Self::Security(error) => write!(formatter, "{error}"),
            Self::Protocol(error) => formatter.write_str(error),
            Self::Occupied(path) => write!(
                formatter,
                "configured query path {} is not a socket",
                path.display()
            ),
            Self::Protection(path) => write!(
                formatter,
                "query socket {} did not take its security descriptor",
                path.display()
            ),
            Self::FrameTooLarge => {
                formatter.write_str("query response exceeds the u32 framing bound")
            }
            Self::Timeout => formatter.write_str("query timed out"),
        }
    }
}

impl std::error::Error for QuerySocketError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Peios(error) => Some(error),
            Self::Security(error) => Some(error),
            Self::Protocol(_)
            | Self::Occupied(_)
            | Self::Protection(_)
            | Self::FrameTooLarge
            | Self::Timeout => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_user_holds_at_most_their_share_and_system_is_not_counted() {
        let per_user = PerUser::default();
        let alice = Sid::build(5, &[21, 1, 2, 3, 1001]).expect("alice");
        let dana = Sid::build(5, &[21, 1, 2, 3, 1002]).expect("dana");
        let system = Sid::build(5, &[18]).expect("SYSTEM");
        let first = UserSlot::take(&per_user, alice, 2).expect("first");
        let second = UserSlot::take(&per_user, alice, 2).expect("second");
        assert!(UserSlot::take(&per_user, alice, 2).is_none());
        // Someone else is not held up by alice.
        let other = UserSlot::take(&per_user, dana, 2).expect("dana's");
        let machine: Vec<UserSlot> = (0..5)
            .map(|_| UserSlot::take(&per_user, system, 2).expect("SYSTEM's"))
            .collect();
        // A query that ends gives its slot back.
        drop(first);
        assert!(UserSlot::take(&per_user, alice, 2).is_some());
        drop((second, other, machine));
        assert!(
            per_user
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .is_empty()
        );
    }

    #[test]
    fn the_query_socket_admits_authenticated_users_to_connect() {
        let descriptor = sddl::parse(QUERY_SOCKET_SDDL).expect("valid descriptor");
        let text = sddl::format(descriptor.as_bytes()).expect("formats");
        assert!(text.contains(";AU)"), "{text}");
    }
}
