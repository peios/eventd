//! Zero-copy validation of log datagrams before owned storage records are built.

use core::fmt;
use core::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use eventd_core::{BoundedQueue, LogRecord, LogStore, LogStoreError};
use peios::msgpack::{Reader, Type};

use crate::commit_signal::CommitSignal;
use crate::datagram::{IngestionSocket, Receive, SocketError};
use crate::writer::WriterMessage;

pub enum LogMaintenance {
    DeleteOldest {
        older_than: Option<i64>,
        limit: usize,
        response: SyncSender<Result<usize, String>>,
    },
    Checkpoint(SyncSender<Result<usize, String>>),
}

#[allow(
    clippy::too_many_arguments,
    reason = "the sole log owner receives its fixed batching and wake dependencies explicitly"
)]
pub fn run(
    socket: &IngestionSocket,
    mut store: LogStore,
    boot_id: [u8; 16],
    error_events: &BoundedQueue<WriterMessage>,
    datagram_ceiling: usize,
    max_batch_size: usize,
    max_batch_latency: Duration,
    stopping: &Arc<AtomicBool>,
    commits: &Arc<CommitSignal>,
    maintenance: &Receiver<LogMaintenance>,
    retention_requested: &Arc<AtomicBool>,
) -> Result<(), LogIngestError> {
    let mut buffer = vec![0_u8; datagram_ceiling];
    let mut batch = Vec::with_capacity(max_batch_size);
    let mut started = None;
    while !stopping.load(Ordering::Acquire) {
        match socket.receive(&mut buffer)? {
            Receive::Datagram(length) => {
                let receipt_timestamp = realtime_nanoseconds()?;
                let Some(records) = parse_datagram(&buffer[..length], boot_id, receipt_timestamp)
                else {
                    continue;
                };
                for record in records {
                    started.get_or_insert_with(Instant::now);
                    batch.push(record);
                    if batch.len() == max_batch_size
                        || started.is_some_and(|time| time.elapsed() >= max_batch_latency)
                    {
                        commit_batch(
                            &mut store,
                            &batch,
                            commits,
                            retention_requested,
                            boot_id,
                            error_events,
                        )?;
                        batch.clear();
                        started = None;
                    }
                }
            }
            Receive::Truncated => {}
            Receive::Empty if batch.is_empty() => socket.wait_readable(1_000)?,
            Receive::Empty => {
                commit_batch(
                    &mut store,
                    &batch,
                    commits,
                    retention_requested,
                    boot_id,
                    error_events,
                )?;
                batch.clear();
                started = None;
            }
        }
        process_maintenance(
            &mut store,
            maintenance,
            retention_requested,
            boot_id,
            error_events,
        )?;
    }
    loop {
        match socket.receive(&mut buffer)? {
            Receive::Datagram(length) => {
                let receipt_timestamp = realtime_nanoseconds()?;
                let Some(records) = parse_datagram(&buffer[..length], boot_id, receipt_timestamp)
                else {
                    continue;
                };
                for record in records {
                    batch.push(record);
                    if batch.len() == max_batch_size {
                        commit_batch(
                            &mut store,
                            &batch,
                            commits,
                            retention_requested,
                            boot_id,
                            error_events,
                        )?;
                        batch.clear();
                    }
                }
            }
            Receive::Truncated => {}
            Receive::Empty => break,
        }
    }
    if !batch.is_empty() {
        commit_batch(
            &mut store,
            &batch,
            commits,
            retention_requested,
            boot_id,
            error_events,
        )?;
    }
    Ok(())
}

fn process_maintenance(
    store: &mut LogStore,
    receiver: &Receiver<LogMaintenance>,
    retention_requested: &AtomicBool,
    boot_id: [u8; 16],
    error_events: &BoundedQueue<WriterMessage>,
) -> Result<(), LogIngestError> {
    let command = match receiver.try_recv() {
        Ok(command) => command,
        Err(TryRecvError::Empty | TryRecvError::Disconnected) => return Ok(()),
    };
    let (result, response) = match command {
        LogMaintenance::DeleteOldest {
            older_than,
            limit,
            response,
        } => (store.retain_oldest(older_than, limit), response),
        LogMaintenance::Checkpoint(response) => (store.passive_checkpoint().map(|()| 0), response),
    };
    match result {
        Ok(deleted) => {
            let _ = response.send(Ok(deleted));
            Ok(())
        }
        Err(error) => {
            let _ = response.send(Err(error.to_string()));
            if error.is_capacity() {
                request_retention(retention_requested, &error);
                Ok(())
            } else if error.is_corruption() {
                recover_corruption(store, boot_id, error_events, &error)
            } else {
                Err(error.into())
            }
        }
    }
}

fn commit_batch(
    store: &mut LogStore,
    batch: &[LogRecord],
    commits: &CommitSignal,
    retention_requested: &AtomicBool,
    boot_id: [u8; 16],
    error_events: &BoundedQueue<WriterMessage>,
) -> Result<(), LogIngestError> {
    match store.commit(batch) {
        Ok(()) => {
            if !batch.is_empty() {
                commits.committed();
            }
            Ok(())
        }
        Err(error) if error.is_capacity() => {
            request_retention(retention_requested, &error);
            Ok(())
        }
        Err(error) if error.is_corruption() => {
            recover_corruption(store, boot_id, error_events, &error)
        }
        Err(error) => Err(error.into()),
    }
}

fn request_retention(requested: &AtomicBool, error: &LogStoreError) {
    requested.store(true, Ordering::Release);
    eprintln!("eventd: log store is full; batch discarded and retention requested: {error}");
}

fn recover_corruption(
    store: &mut LogStore,
    boot_id: [u8; 16],
    error_events: &BoundedQueue<WriterMessage>,
    error: &LogStoreError,
) -> Result<(), LogIngestError> {
    let description = error.to_string();
    eprintln!("eventd: quarantining corrupt log store: {description}");
    store.replace_corrupt()?;
    emit_storage_error(error_events, boot_id, "log", &description);
    Ok(())
}

fn emit_storage_error(
    queue: &BoundedQueue<WriterMessage>,
    boot_id: [u8; 16],
    store: &str,
    error: &str,
) {
    let Ok(timestamp) = realtime_nanoseconds()
        .and_then(|value| u64::try_from(value).map_err(|_| LogIngestError::Clock))
    else {
        eprintln!("eventd: cannot timestamp {store} storage error");
        return;
    };
    let (sender, _receiver) = std::sync::mpsc::sync_channel(1);
    match queue.reserve(core::mem::size_of::<WriterMessage>()) {
        Ok(permit) => permit.publish(WriterMessage::Synthetic(
            crate::synthetic::storage_error(boot_id, store, None, error, timestamp),
            sender,
        )),
        Err(queue_error) => {
            eprintln!("eventd: cannot enqueue {store} storage error: {queue_error}");
        }
    }
}

fn realtime_nanoseconds() -> Result<i64, LogIngestError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| LogIngestError::Clock)?;
    i64::try_from(elapsed.as_nanos()).map_err(|_| LogIngestError::Clock)
}

pub fn parse_datagram(
    bytes: &[u8],
    boot_id: [u8; 16],
    receipt_timestamp: i64,
) -> Option<Vec<LogRecord>> {
    peios::msgpack::validate(bytes, peios::msgpack::DEFAULT_MAX_DEPTH).ok()?;
    let mut reader = Reader::new(bytes);
    let records = match reader.peek()? {
        Type::Map => parse_record(&mut reader, boot_id, receipt_timestamp)
            .into_iter()
            .collect(),
        Type::Array => {
            let count = reader.read_array().ok()?;
            let mut records = Vec::new();
            for _ in 0..count {
                if reader.peek() != Some(Type::Map) {
                    return None;
                }
                if let Some(record) = parse_record(&mut reader, boot_id, receipt_timestamp) {
                    records.push(record);
                }
            }
            records
        }
        _ => return None,
    };
    (reader.remaining() == 0).then_some(records)
}

fn parse_record(
    reader: &mut Reader<'_>,
    boot_id: [u8; 16],
    receipt_timestamp: i64,
) -> Option<LogRecord> {
    let field_count = reader.read_map().ok()?;
    let mut seen = HashSet::with_capacity(field_count.min(16));
    let mut origin = None;
    let mut is_error = None;
    let mut message = None;
    let mut timestamp = None;
    let mut job_id = None;
    let mut valid = true;

    for _ in 0..field_count {
        if reader.peek() != Some(Type::Str) {
            reader.skip().ok()?;
            reader.skip().ok()?;
            valid = false;
            continue;
        }
        let key = reader.read_str().ok()?;
        if !seen.insert(key) {
            valid = false;
        }
        match key {
            "origin" if reader.peek() == Some(Type::Str) => {
                origin = Some(reader.read_str().ok()?);
            }
            "is_error" if reader.peek() == Some(Type::Bool) => {
                is_error = Some(reader.read_bool().ok()?);
            }
            "message" if reader.peek() == Some(Type::Str) => {
                message = Some(reader.read_str().ok()?);
            }
            "timestamp" if reader.peek() == Some(Type::Int) => {
                timestamp = read_timestamp(reader);
            }
            "job_id" if reader.peek() == Some(Type::Bin) => {
                let bytes = reader.read_bin().ok()?;
                job_id = bytes.try_into().ok();
            }
            _ => {
                reader.skip().ok()?;
            }
        }
    }

    let origin = origin.filter(|value| valid_identifier(value))?;
    let is_error = is_error?;
    let message = message?;
    valid.then(|| LogRecord {
        boot_id,
        timestamp: timestamp.unwrap_or(receipt_timestamp),
        origin: origin.into(),
        is_error,
        message: message.into(),
        job_id,
    })
}

fn read_timestamp(reader: &mut Reader<'_>) -> Option<i64> {
    if let Ok(value) = reader.read_int() {
        return (value >= 0).then_some(value);
    }
    reader
        .read_uint()
        .ok()
        .and_then(|value| i64::try_from(value).ok())
}

fn valid_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

#[derive(Debug)]
pub enum LogIngestError {
    Socket(SocketError),
    Store(LogStoreError),
    Clock,
}

impl fmt::Display for LogIngestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Socket(error) => write!(formatter, "{error}"),
            Self::Store(error) => write!(formatter, "{error}"),
            Self::Clock => formatter.write_str("realtime clock is outside the timestamp domain"),
        }
    }
}

impl std::error::Error for LogIngestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Socket(error) => Some(error),
            Self::Store(error) => Some(error),
            Self::Clock => None,
        }
    }
}

impl From<SocketError> for LogIngestError {
    fn from(error: SocketError) -> Self {
        Self::Socket(error)
    }
}

impl From<LogStoreError> for LogIngestError {
    fn from(error: LogStoreError) -> Self {
        Self::Store(error)
    }
}

#[cfg(test)]
mod tests {
    use peios::msgpack::Writer;

    use super::*;

    #[test]
    fn parses_one_valid_record() {
        let mut writer = Writer::new();
        writer
            .write_map(3)
            .write_str("origin")
            .write_str("svc.test")
            .write_str("is_error")
            .write_bool(true)
            .write_str("message")
            .write_str("hello");
        let records = parse_datagram(&writer.to_bytes().unwrap(), [1; 16], 99).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].timestamp, 99);
        assert_eq!(records[0].origin.as_ref(), "svc.test");
    }

    #[test]
    fn duplicate_key_discards_only_that_batch_record() {
        let mut writer = Writer::new();
        writer
            .write_array(2)
            .write_map(4)
            .write_str("origin")
            .write_str("bad")
            .write_str("origin")
            .write_str("also_bad")
            .write_str("is_error")
            .write_bool(false)
            .write_str("message")
            .write_str("discard")
            .write_map(3)
            .write_str("origin")
            .write_str("good")
            .write_str("is_error")
            .write_bool(false)
            .write_str("message")
            .write_str("keep");
        let records = parse_datagram(&writer.to_bytes().unwrap(), [1; 16], 99).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].origin.as_ref(), "good");
    }

    #[test]
    fn malformed_optional_fields_are_ignored() {
        let mut writer = Writer::new();
        writer
            .write_map(5)
            .write_str("origin")
            .write_str("svc")
            .write_str("is_error")
            .write_bool(false)
            .write_str("message")
            .write_str("line")
            .write_str("timestamp")
            .write_int(-1)
            .write_str("job_id")
            .write_bin(&[2; 15]);
        let records = parse_datagram(&writer.to_bytes().unwrap(), [1; 16], 123).unwrap();
        assert_eq!(records[0].timestamp, 123);
        assert_eq!(records[0].job_id, None);
    }
}
