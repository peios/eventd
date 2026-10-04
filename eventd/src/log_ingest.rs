//! Zero-copy validation of log datagrams before owned storage records are built.

use core::fmt;
use core::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, SyncSender, TryRecvError};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use eventd_core::{BoundedQueue, LogRecord, LogStore, LogStoreError};
use peios::msgpack::{Reader, Type};

use crate::commit_signal::CommitSignal;
use crate::config::{Config, SharedConfig};
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
    clippy::too_many_lines,
    reason = "the sole log owner receives its fixed batching and wake dependencies explicitly"
)]
pub fn run(
    socket: &IngestionSocket,
    mut store: LogStore,
    boot_id: [u8; 16],
    error_events: &BoundedQueue<WriterMessage>,
    runtime: &SharedConfig,
    stopping: &Arc<AtomicBool>,
    commits: &Arc<CommitSignal>,
    maintenance: &Receiver<LogMaintenance>,
    retention_requested: &Arc<AtomicBool>,
) -> Result<(), LogIngestError> {
    let (initial_batch_size, mut datagram_ceiling) = Config::read(runtime, |config| {
        (config.log_max_batch_size, config.max_log_datagram_bytes)
    });
    let mut buffer = vec![0_u8; datagram_ceiling];
    let mut batch = Vec::with_capacity(initial_batch_size);
    let mut started = None;
    while !stopping.load(Ordering::Acquire) {
        let (max_batch_size, max_batch_latency, checkpoint_pages, next_ceiling) =
            Config::read(runtime, |config| {
                (
                    config.log_max_batch_size,
                    config.log_max_batch_latency,
                    config.wal_checkpoint_pages,
                    config.max_log_datagram_bytes,
                )
            });
        store.set_checkpoint_pages(checkpoint_pages);
        if datagram_ceiling != next_ceiling {
            datagram_ceiling = next_ceiling;
            buffer.resize(datagram_ceiling, 0);
            socket.configure_receive_buffer(datagram_ceiling)?;
        }
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
        let max_batch_size = Config::read(runtime, |config| config.log_max_batch_size);
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
                crate::diagnostics::log_error(&error);
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
                crate::health::logs_stored(batch.len());
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
        Err(error) => {
            crate::diagnostics::log_error(&error);
            Err(error.into())
        }
    }
}

fn request_retention(requested: &AtomicBool, error: &LogStoreError) {
    crate::diagnostics::log_error(error);
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
    crate::diagnostics::log_error(error);
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

    let origin = origin?;
    if !valid_origin(origin) {
        report_rejected_origin(origin);
        return None;
    }
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

/// The origin grammar, which is the vocabulary the service manager
/// produces (peinit TRM §11.1):
///
/// ```text
/// origin    := component | component "/" producer
/// producer  := component | component "[" [0-9]+ "]"
/// component := [A-Za-z0-9_] [A-Za-z0-9_.-]*
/// ```
///
/// A main process is its service name; a hook is
/// `<service>/ExecStartPre[0]`, a reload `<service>/ExecReload`, a health
/// check `<service>/HealthCheck`, and a submitted job `jobs/<guid>`.
/// At most one slash, and the bracketed index only after one — the
/// grammar admits what the broker sends and nothing wider. `*`, `\`,
/// whitespace and quoting characters stay excluded, so an origin can
/// still neither impersonate a wildcard pattern nor escape the registry
/// path its descriptor is stored under (§7.2).
fn valid_origin(value: &str) -> bool {
    match value.split_once('/') {
        None => valid_component(value),
        Some((service, producer)) => valid_component(service) && valid_producer(producer),
    }
}

fn valid_producer(value: &str) -> bool {
    let Some(open) = value.find('[') else {
        return valid_component(value);
    };
    let Some(index) = value[open..]
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
    else {
        return false;
    };
    valid_component(&value[..open])
        && !index.is_empty()
        && index.bytes().all(|byte| byte.is_ascii_digit())
}

fn valid_component(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphanumeric() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

/// Count the rejection and, at most once a minute, say so on standard
/// error — which peinit captures, so it reaches the log store by the
/// ordinary path. A discarded record is otherwise invisible, and a
/// producer whose vocabulary has drifted from the collector's discards
/// everything it sends.
fn report_rejected_origin(origin: &str) {
    if !crate::diagnostics::log_rejected_origin(origin) {
        return;
    }
    eprintln!(
        "eventd: discarding log records whose origin is not of the accepted grammar; \
         most recent: \"{}\"",
        crate::diagnostics::displayable_origin(origin)
    );
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
    use std::path::PathBuf;
    use std::sync::mpsc::{Sender, channel, sync_channel};
    use std::time::Duration;

    use peios::msgpack::Writer;

    use super::*;

    /// One log thread over a host socket and a temporary store, with the
    /// handles the pipeline would hold.
    struct LogThread {
        directory: PathBuf,
        socket_path: PathBuf,
        runtime: SharedConfig,
        stopping: Arc<AtomicBool>,
        commits: Arc<CommitSignal>,
        retention_requested: Arc<AtomicBool>,
        error_events: BoundedQueue<WriterMessage>,
    }

    impl LogThread {
        fn new(max_batch_size: usize) -> Self {
            let mut directory = std::env::temp_dir();
            directory.push(format!(
                "eventd-log-test-{}-{}",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir(&directory).unwrap();
            let mut config = Config::test_defaults();
            config.log_max_batch_size = max_batch_size;
            config.log_max_batch_latency = Duration::from_mins(1);
            Self {
                socket_path: directory.join("log.sock"),
                directory,
                runtime: config.shared(),
                stopping: Arc::new(AtomicBool::new(false)),
                commits: Arc::new(CommitSignal::new()),
                retention_requested: Arc::new(AtomicBool::new(false)),
                error_events: BoundedQueue::new(16, 1 << 20).unwrap(),
            }
        }

        fn store_path(&self) -> PathBuf {
            self.directory.join("logs.db")
        }

        fn bind(&self) -> IngestionSocket {
            IngestionSocket::unprotected(&self.socket_path, PORTABLE_CEILING).unwrap()
        }

        fn open_store(&self) -> LogStore {
            LogStore::open(self.store_path(), 1_000).unwrap()
        }

        fn run(
            &self,
            socket: &IngestionSocket,
            store: LogStore,
            maintenance: &Receiver<LogMaintenance>,
        ) -> Result<(), LogIngestError> {
            run(
                socket,
                store,
                [1; 16],
                &self.error_events,
                &self.runtime,
                &self.stopping,
                &self.commits,
                maintenance,
                &self.retention_requested,
            )
        }

        fn send(&self, records: usize, message: &str) {
            let mut writer = Writer::new();
            writer.write_array(u32::try_from(records).unwrap());
            for _ in 0..records {
                writer
                    .write_map(3)
                    .write_str("origin")
                    .write_str("svc.test")
                    .write_str("is_error")
                    .write_bool(false)
                    .write_str("message")
                    .write_str(message);
            }
            std::os::unix::net::UnixDatagram::unbound()
                .unwrap()
                .send_to(&writer.to_bytes().unwrap(), &self.socket_path)
                .unwrap();
        }

        fn stored(&self) -> i64 {
            rusqlite::Connection::open_with_flags(
                self.store_path(),
                rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE,
            )
            .unwrap()
            .query_row("SELECT count(*) FROM logs", [], |row| row.get(0))
            .unwrap()
        }

        fn wait_for_stored(&self, rows: i64) {
            let deadline = Instant::now() + Duration::from_secs(10);
            while self.stored() != rows {
                assert!(
                    Instant::now() < deadline,
                    "{} of {rows} rows stored",
                    self.stored()
                );
                std::thread::sleep(Duration::from_millis(10));
            }
        }

        fn wait_for_generation(&self, generation: u64) {
            let deadline = Instant::now() + Duration::from_secs(10);
            while self.commits.generation() != generation {
                assert!(Instant::now() < deadline, "commit generation {generation}");
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }

    impl Drop for LogThread {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.directory);
        }
    }

    /// Stops the log thread when dropped, so a failing test still ends it
    /// and its scope can join it.
    struct StopOnDrop<'a>(&'a AtomicBool);

    impl Drop for StopOnDrop<'_> {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }

    const PORTABLE_CEILING: usize = crate::config::PORTABLE_INGEST_DATAGRAM_BYTES as usize;

    /// Round-trip a maintenance command. The log thread takes one after
    /// each receive, so its answer means every commit that receive led to
    /// has been signalled.
    fn settle(maintenance: &Sender<LogMaintenance>) {
        let (response, answer) = sync_channel(1);
        maintenance
            .send(LogMaintenance::Checkpoint(response))
            .unwrap();
        answer.recv().unwrap().unwrap();
    }

    #[test]
    fn without_kacs_log_ingestion_commits_and_a_failed_metric_check_stops_nothing() {
        // A host has no KACS: no token, no access check, no descriptor.
        let log = LogThread::new(5_000);
        let socket = log.bind();
        let store = log.open_store();
        let (_maintenance, commands) = channel();
        std::thread::scope(|scope| {
            let (log, socket) = (&log, &socket);
            let thread = scope.spawn(move || log.run(socket, store, &commands));
            let _stopping = StopOnDrop(&log.stopping);
            log.send(3, "without KACS");
            log.send(2, "still without KACS");
            log.wait_for_stored(5);
            log.stopping.store(true, Ordering::Release);
            thread.join().unwrap().unwrap();
        });
        assert!(log.commits.generation() >= 1);

        // The metric thread's publication check is a KACS call; failing, it
        // costs the datagram and a count, never the thread.
        let mut last_error = None;
        assert!(!crate::metric_ingest::admit_authorized(
            Err(crate::write_security::MetricPublishError::Token(
                peios::Error::from_raw_os_error(libc::ENOSYS)
            )),
            &mut last_error,
        ));
        assert!(crate::metric_ingest::admit_authorized(
            Ok(0),
            &mut last_error
        ));
    }

    #[test]
    #[ignore = "PEI-1315: log_ingest::run rereads LogMaxBatchSize for every datagram, so a change \
                rebounds the transaction already open"]
    fn a_log_batch_size_change_applies_only_to_later_transactions() {
        let log = LogThread::new(100);
        let socket = log.bind();
        let (maintenance, commands) = channel();
        // 130 records: the first transaction commits at 100, and the next
        // opens with 30 under the same setting.
        log.send(130, "first");
        log.send(60, "second");
        // The writer answers this after taking the first datagram, and
        // cannot go on until the test has taken the answer.
        let (pause, paused) = sync_channel(0);
        maintenance.send(LogMaintenance::Checkpoint(pause)).unwrap();
        let store = log.open_store();
        std::thread::scope(|scope| {
            let (log, socket) = (&log, &socket);
            let thread = scope.spawn(move || log.run(socket, store, &commands));
            let _stopping = StopOnDrop(&log.stopping);
            let paused = paused;
            log.wait_for_stored(100);
            log.wait_for_generation(1);
            // The second transaction is open with 30 records.
            log.runtime.write().unwrap().log_max_batch_size = 50;
            paused.recv().unwrap().unwrap();
            // Its 30 and the next datagram's 60 make 90: under the 100 it
            // opened with, so it commits whole when the queue empties.
            log.wait_for_stored(190);
            settle(&maintenance);
            assert_eq!(
                log.commits.generation(),
                2,
                "the open transaction kept the threshold it began with"
            );
            // A transaction begun after the change is bounded by it.
            log.send(60, "third");
            log.wait_for_stored(250);
            settle(&maintenance);
            assert_eq!(
                log.commits.generation(),
                4,
                "60 records under 50: 50, then 10"
            );
            log.stopping.store(true, Ordering::Release);
            thread.join().unwrap().unwrap();
        });
    }

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

    fn origin_of(origin: &str) -> Option<LogRecord> {
        let mut writer = Writer::new();
        writer
            .write_map(3)
            .write_str("origin")
            .write_str(origin)
            .write_str("is_error")
            .write_bool(false)
            .write_str("message")
            .write_str("line");
        parse_datagram(&writer.to_bytes().unwrap(), [1; 16], 99)
            .unwrap()
            .pop()
    }

    #[test]
    fn accepts_the_origins_the_service_manager_produces() {
        for origin in [
            "jellyfin",
            "jellyfin/ExecStartPre[0]",
            "jellyfin/ExecStartPost[11]",
            "jellyfin/ExecReload",
            "jellyfin/HealthCheck",
            "jobs/0f8fad5b-d9cb-469f-a165-70867728950e",
            "7zip-daemon",
            "loregd.watcher",
        ] {
            let record = origin_of(origin).unwrap_or_else(|| panic!("{origin} is accepted"));
            assert_eq!(record.origin.as_ref(), origin);
        }
    }

    #[test]
    fn rejects_origins_outside_the_grammar() {
        for origin in [
            "",
            "svc/",
            "/svc",
            "svc//hook",
            "svc/hooks/0",
            "svc/ExecStartPre[0]extra",
            "svc/ExecStartPre[]",
            "svc/ExecStartPre[a]",
            "svc/ExecStartPre[0",
            "svc[0]",
            "svc*",
            r"svc\hook",
            "svc hook",
            "svc\"hook\"",
            "s\u{e9}rvice",
        ] {
            assert!(
                origin_of(origin).is_none(),
                "{origin:?} is outside the origin grammar"
            );
        }
    }

    #[test]
    fn a_rejected_origin_is_counted() {
        // The counters are process-wide and every test in this binary
        // shares them, so this asserts movement rather than a total.
        let before = crate::diagnostics::snapshot().1.rejected_origins;
        assert!(origin_of("svc/hooks/0").is_none());
        let after = crate::diagnostics::snapshot().1;
        assert!(
            after.rejected_origins > before,
            "a discarded origin is counted: {before} -> {}",
            after.rejected_origins
        );
        assert!(after.last_rejected_origin.is_some());
    }

    #[test]
    fn a_rejected_origin_is_escaped_and_truncated_for_display() {
        assert_eq!(
            crate::diagnostics::displayable_origin("svc\n\"x\""),
            "svc\\n\\\"x\\\""
        );
        let long = "x".repeat(200);
        let shown = crate::diagnostics::displayable_origin(&long);
        assert_eq!(shown.chars().count(), 65);
        assert!(shown.ends_with('…'));
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
