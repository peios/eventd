//! Framed query socket, access control and read-only execution.

mod executor;
mod security;
mod value;

use core::fmt;
use core::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::io::{Read, Write};
use std::os::fd::{AsFd, AsRawFd, FromRawFd};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::SyncSender;
use std::time::{Duration, Instant};

use peios::file::{File, SecInfo};
use peios::msgpack::{Reader, Type, Writer};

use crate::commit_signal::CommitSignal;
use crate::indexing::{PolicyMessage, Tracker};
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
    pub max_request_bytes: usize,
    pub response_target_bytes: usize,
    pub max_concurrent: usize,
    pub max_streaming: usize,
    pub max_distinct_stream_values: usize,
    pub timeout: Duration,
    pub cross_type_window: Duration,
    pub cross_type_max_lookback: Duration,
    pub index_tracker: Arc<Tracker>,
    pub index_policy: SyncSender<PolicyMessage>,
    pub descriptors: Arc<DescriptorCache>,
}

pub struct QueryServer {
    listener: UnixListener,
    path: PathBuf,
    identity: (u64, u64),
    active: Arc<AtomicUsize>,
    streaming: Arc<AtomicUsize>,
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

        let duplicate =
            unsafe { libc::fcntl(listener.as_fd().as_raw_fd(), libc::F_DUPFD_CLOEXEC, 0) };
        if duplicate < 0 {
            return Err(QuerySocketError::Io(std::io::Error::last_os_error()));
        }
        // SAFETY: fcntl returned a fresh owned descriptor.
        let file = File::from(unsafe { std::os::fd::OwnedFd::from_raw_fd(duplicate) });
        let secinfo = SecInfo::OWNER | SecInfo::GROUP | SecInfo::DACL | SecInfo::LABEL;
        let descriptor = file.fd_get_sd(secinfo).map_err(QuerySocketError::Peios)?;
        file.fd_set_sd(secinfo, &descriptor)
            .map_err(QuerySocketError::Peios)?;
        Ok(Self {
            listener,
            path: path.to_owned(),
            identity: (metadata.dev(), metadata.ino()),
            active: Arc::new(AtomicUsize::new(0)),
            streaming: Arc::new(AtomicUsize::new(0)),
        })
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
                    if !try_acquire(&self.active, config.max_concurrent) {
                        let _ = send_error(stream, "too many concurrent queries");
                        continue;
                    }
                    let worker_active = Arc::clone(&self.active);
                    let streaming = Arc::clone(&self.streaming);
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
                                &streaming,
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
    clippy::too_many_lines,
    reason = "connection ownership and its protocol state remain linear and auditable"
)]
fn handle(
    mut stream: UnixStream,
    stores: &Stores,
    config: &ServerConfig,
    streaming_count: &Arc<AtomicUsize>,
    stopping: &AtomicBool,
    event_commits: &CommitSignal,
    log_commits: &CommitSignal,
) -> Result<(), QuerySocketError> {
    stream
        .set_read_timeout(Some(config.timeout))
        .map_err(QuerySocketError::Io)?;
    stream
        .set_write_timeout(Some(config.timeout))
        .map_err(QuerySocketError::Io)?;
    let authorizer =
        security::Authorizer::from_peer(stream.as_fd(), Arc::clone(&config.descriptors))
            .map_err(QuerySocketError::Security)?;
    let query_text = match read_request(&mut stream, config.max_request_bytes) {
        Ok(query) => query,
        Err(error) => {
            send_error(&mut stream, &error.to_string())?;
            return Ok(());
        }
    };
    let deadline = Instant::now() + config.timeout;
    let query = match crate::query_language::parse(&query_text) {
        Ok(query) => query,
        Err(error) => {
            send_error(&mut stream, &error.to_string())?;
            return Ok(());
        }
    };
    if Instant::now() >= deadline {
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
        if !try_acquire(streaming_count, config.max_streaming) {
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
    let (records, mut stream_state) = match if query.stream {
        executor::start_stream(
            &query,
            stores,
            &authorizer,
            &Limits {
                deadline,
                cross_type_window: config.cross_type_window,
                cross_type_max_lookback: config.cross_type_max_lookback,
            },
        )
        .map(|(records, state)| (records, Some(state)))
    } else {
        executor::execute(
            &query,
            stores,
            &authorizer,
            &Limits {
                deadline,
                cross_type_window: config.cross_type_window,
                cross_type_max_lookback: config.cross_type_max_lookback,
            },
        )
        .map(|records| (records, None))
    } {
        Ok(result) => result,
        Err(error) => {
            send_error(&mut stream, &error.to_string())?;
            return Ok(());
        }
    };
    if query.stream
        && matches!(
            query.aggregate,
            Some(crate::query_language::RecordAggregate::Distinct(_))
        )
        && records.len() > config.max_distinct_stream_values
    {
        send_error(&mut stream, "DISTINCT stream exceeds its seen-value limit")?;
        return Ok(());
    }
    send_records(
        &mut stream,
        &records,
        config.response_target_bytes,
        Some(deadline),
    )?;
    if query.stream {
        let mut seen = distinct_values(&query, &records);
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
                        if values.len() > config.max_distinct_stream_values {
                            let _ = send_error(
                                &mut stream,
                                "DISTINCT stream exceeds its seen-value limit",
                            );
                            return Ok(());
                        }
                    }
                    if !records.is_empty() {
                        send_records(&mut stream, &records, config.response_target_bytes, None)?;
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

fn distinct_field(query: &crate::query_language::Query) -> Option<&str> {
    match &query.aggregate {
        Some(RecordAggregate::Distinct(field)) => Some(field),
        _ => None,
    }
}

fn distinct_values(
    query: &crate::query_language::Query,
    records: &[value::Record],
) -> Option<Vec<value::Value>> {
    let field = distinct_field(query)?;
    Some(
        records
            .iter()
            .map(|record| record.get(field).cloned().unwrap_or(value::Value::Null))
            .collect(),
    )
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

#[derive(Debug)]
pub enum QuerySocketError {
    Io(std::io::Error),
    Peios(peios::Error),
    Security(security::SecurityError),
    Protocol(String),
    Occupied(PathBuf),
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
            Self::Protocol(_) | Self::Occupied(_) | Self::FrameTooLarge | Self::Timeout => None,
        }
    }
}
