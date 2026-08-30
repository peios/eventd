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
use std::time::Duration;

use peios::file::{File, SecInfo};
use peios::msgpack::{Reader, Type, Writer};

pub use executor::{Limits, Stores};

pub struct ServerConfig {
    pub max_request_bytes: usize,
    pub response_target_bytes: usize,
    pub max_concurrent: usize,
    pub max_streaming: usize,
    pub max_distinct_stream_values: usize,
    pub timeout: Duration,
}

pub struct QueryServer {
    listener: UnixListener,
    path: PathBuf,
    identity: (u64, u64),
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
        })
    }

    pub fn run(
        self,
        stores: &Arc<Stores>,
        config: &Arc<ServerConfig>,
        stopping: &Arc<AtomicBool>,
    ) -> Result<(), QuerySocketError> {
        let active = Arc::new(AtomicUsize::new(0));
        let streaming = Arc::new(AtomicUsize::new(0));
        while !stopping.load(Ordering::Acquire) {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    if !try_acquire(&active, config.max_concurrent) {
                        let _ = send_error(stream, "too many concurrent queries");
                        continue;
                    }
                    let worker_active = Arc::clone(&active);
                    let streaming = Arc::clone(&streaming);
                    let stores = Arc::clone(stores);
                    let config = Arc::clone(config);
                    let stopping = Arc::clone(stopping);
                    let spawned = std::thread::Builder::new()
                        .name("eventd-query".to_owned())
                        .spawn(move || {
                            let _guard = CounterGuard(worker_active);
                            if let Err(error) =
                                handle(stream, &stores, &config, &streaming, &stopping)
                            {
                                eprintln!("eventd: query failed: {error}");
                            }
                        });
                    if let Err(error) = spawned {
                        active.fetch_sub(1, Ordering::Release);
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
        Ok(())
    }
}

impl Drop for QueryServer {
    fn drop(&mut self) {
        if std::fs::symlink_metadata(&self.path).is_ok_and(|metadata| {
            metadata.file_type().is_socket() && (metadata.dev(), metadata.ino()) == self.identity
        }) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

fn handle(
    mut stream: UnixStream,
    stores: &Stores,
    config: &ServerConfig,
    streaming_count: &Arc<AtomicUsize>,
    stopping: &AtomicBool,
) -> Result<(), QuerySocketError> {
    stream
        .set_read_timeout(Some(config.timeout))
        .map_err(QuerySocketError::Io)?;
    stream
        .set_write_timeout(Some(config.timeout))
        .map_err(QuerySocketError::Io)?;
    let authorizer =
        security::Authorizer::from_peer(stream.as_fd()).map_err(QuerySocketError::Security)?;
    let query_text = match read_request(&mut stream, config.max_request_bytes) {
        Ok(query) => query,
        Err(error) => {
            send_error(&mut stream, &error.to_string())?;
            return Ok(());
        }
    };
    let query = match crate::query_language::parse(&query_text) {
        Ok(query) => query,
        Err(error) => {
            send_error(&mut stream, &error.to_string())?;
            return Ok(());
        }
    };
    let stream_guard = if query.stream {
        if !try_acquire(streaming_count, config.max_streaming) {
            send_error(&mut stream, "too many concurrent streaming queries")?;
            return Ok(());
        }
        Some(CounterGuard(Arc::clone(streaming_count)))
    } else {
        None
    };
    let records = match executor::execute(
        &query,
        stores,
        &authorizer,
        &Limits {
            timeout: config.timeout,
        },
    ) {
        Ok(records) => records,
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
    send_records(&mut stream, &records, config.response_target_bytes)?;
    if query.stream {
        send_status(&mut stream, "watch")?;
        stream
            .set_write_timeout(Some(Duration::ZERO))
            .map_err(QuerySocketError::Io)?;
        while !stopping.load(Ordering::Acquire) {
            match peer_state(&stream) {
                Ok(PeerState::Closed) => break,
                Ok(PeerState::Data) => {
                    return Err(QuerySocketError::Protocol("unexpected client data".into()));
                }
                Ok(PeerState::Idle) => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                Err(error) => return Err(QuerySocketError::Io(error)),
            }
        }
    } else {
        send_status(&mut stream, "end")?;
    }
    drop(stream_guard);
    Ok(())
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
) -> Result<(), QuerySocketError> {
    if records.is_empty() {
        return send_ok(stream, &[]);
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
        send_ok(stream, &encoded[start..end])?;
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

fn send_ok(stream: &mut UnixStream, records: &[Vec<u8>]) -> Result<(), QuerySocketError> {
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
    send_frame(stream, &writer.to_bytes().map_err(QuerySocketError::Peios)?)
}

fn send_status(stream: &mut UnixStream, status: &str) -> Result<(), QuerySocketError> {
    let mut writer = Writer::new();
    writer.write_map(1).write_str("status").write_str(status);
    send_frame(stream, &writer.to_bytes().map_err(QuerySocketError::Peios)?)
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

fn send_frame(stream: &mut UnixStream, payload: &[u8]) -> Result<(), QuerySocketError> {
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
        }
    }
}

impl std::error::Error for QuerySocketError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Peios(error) => Some(error),
            Self::Security(error) => Some(error),
            Self::Protocol(_) | Self::Occupied(_) | Self::FrameTooLarge => None,
        }
    }
}
