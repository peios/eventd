//! A client for eventd's query channel: what a program needs to ask eventd
//! for events, logs and metrics, follow them live, and tell its user what
//! they may read.
//!
//! The channel is PSPU book 3. One connection carries one query, which is
//! text in the query language (§3.18); eventd answers with result records
//! and one terminal message.
//!
//! - [`query`] runs a query and gives its whole result, or nothing: an
//!   error before the end voids what came before it (§3.16).
//! - [`Tail`] follows a `STREAM` query on a thread of its own, so the
//!   socket is read as fast as eventd writes it, and hands over the initial
//!   result once it is complete, then each live batch.
//! - [`text`] writes values into query text safely.
//! - [`access`] holds eventd's rights and the descriptors that grant them,
//!   so a program can say what its user may read: eventd itself leaves out
//!   what the caller may not read without saying so (§3.28).
//! - [`wire`] is the framing underneath, for a client that needs it.

pub mod access;
pub mod text;
pub mod wire;

use std::io;
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};

pub use wire::{Connection, Record, Response, Value};

/// Where eventd's query socket is in the standard deployment.
pub const DEFAULT_SOCKET: &str = "/run/eventd/query.sock";

/// Connects to the query socket at `socket`.
pub fn connect(socket: &Path) -> Result<Connection<UnixStream>, Error> {
    UnixStream::connect(socket)
        .map(Connection::new)
        .map_err(|source| Error::Connect {
            path: socket.to_owned(),
            source,
        })
}

/// Runs `text`, a query that does not stream, and gives its whole result.
///
/// It waits for eventd's terminal message, which may take up to eventd's
/// query timeout, so a program with a window to keep alive runs it on
/// another thread.
pub fn query(socket: &Path, text: &str) -> Result<Vec<Record>, Error> {
    let mut connection = connect(socket)?;
    connection.send_query(text).map_err(Error::Channel)?;
    let mut records = Vec::new();
    loop {
        match connection.next_response().map_err(Error::Channel)? {
            Response::Records(chunk) => records.extend(chunk),
            Response::End => return Ok(records),
            Response::Error(message) => return Err(Error::Refused(message)),
            Response::Watch => return Err(Error::Channel(wire::Error::UnexpectedStatus)),
        }
    }
}

/// What a [`Tail`] reports, in this order: the initial result once, live
/// batches, and finally why it ended.
#[derive(Debug)]
pub enum Tailed {
    /// The initial result, complete. If the query fails before this, it is
    /// never sent: [`Tailed::Ended`] comes alone.
    Initial(Vec<Record>),
    /// Records committed since, in the order eventd sent them.
    Live(Vec<Record>),
    /// The tail is over. Records already reported remain valid (§3.16).
    Ended(Error),
}

/// A `STREAM` query followed on a thread of its own.
///
/// The thread reads the socket as fast as eventd writes it and passes
/// what it reads through a channel of `capacity` reports. A program that
/// stops taking them fills the channel, the thread stops reading, and
/// eventd, which never waits for a slow reader, ends the query (§3.27):
/// the next report is [`Tailed::Ended`].
///
/// Dropping the `Tail` ends the query.
pub struct Tail {
    updates: Receiver<Tailed>,
    socket: UnixStream,
}

impl Tail {
    /// Sends `text`, which should carry `STREAM`, and starts following it.
    pub fn start(socket: &Path, text: &str, capacity: usize) -> Result<Self, Error> {
        let mut connection = connect(socket)?;
        connection.send_query(text).map_err(Error::Channel)?;
        let handle = connection
            .stream()
            .try_clone()
            .map_err(|source| Error::Channel(wire::Error::Io(source)))?;
        let (sender, updates) = sync_channel(capacity.max(1));
        std::thread::Builder::new()
            .name("eventd-tail".to_owned())
            .spawn(move || follow(connection, &sender))
            .map_err(|source| Error::Channel(wire::Error::Io(source)))?;
        Ok(Self {
            updates,
            socket: handle,
        })
    }

    /// The reports, to wait on or poll.
    #[must_use]
    pub const fn updates(&self) -> &Receiver<Tailed> {
        &self.updates
    }
}

impl Drop for Tail {
    fn drop(&mut self) {
        // The thread may be waiting to hand over a report; it finds the
        // channel gone once this `Tail` is, and the socket closed.
        let _ = self.socket.shutdown(std::net::Shutdown::Both);
    }
}

fn follow(mut connection: Connection<UnixStream>, sender: &SyncSender<Tailed>) {
    let mut initial = Vec::new();
    let mut live = false;
    let ended = loop {
        let report = match connection.next_response() {
            Ok(Response::Records(records)) if live => Tailed::Live(records),
            Ok(Response::Records(records)) => {
                initial.extend(records);
                continue;
            }
            Ok(Response::Watch) if !live => {
                live = true;
                Tailed::Initial(std::mem::take(&mut initial))
            }
            Ok(Response::Error(message)) => break Error::Refused(message),
            Ok(Response::End | Response::Watch) => {
                break Error::Channel(wire::Error::UnexpectedStatus);
            }
            Err(error) => break Error::Channel(error),
        };
        if sender.send(report).is_err() {
            return;
        }
    };
    let _ = sender.send(Tailed::Ended(ended));
}

/// Why a query gave no result, or a tail ended.
#[derive(Debug)]
pub enum Error {
    /// The query socket could not be reached.
    Connect {
        /// Where it was looked for.
        path: PathBuf,
        /// What connecting said.
        source: io::Error,
    },
    /// The connection failed, or eventd sent something that is not the
    /// protocol.
    Channel(wire::Error),
    /// eventd refused or failed the query, in its own words. Show them to
    /// whoever wrote the query; never parse them (§3.16).
    Refused(String),
}

impl core::fmt::Display for Error {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Connect { path, source } => {
                write!(formatter, "cannot connect to {}: {source}", path.display())
            }
            Self::Channel(error) => write!(formatter, "query protocol failure: {error}"),
            Self::Refused(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Connect { source, .. } => Some(source),
            Self::Channel(error) => Some(error),
            Self::Refused(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use peios::msgpack::Writer;
    use std::io::{Read, Write};
    use std::os::unix::net::UnixListener;
    use std::time::Duration;

    fn status(status: &str) -> Vec<u8> {
        let mut writer = Writer::new();
        writer.write_map(1).write_str("status").write_str(status);
        frame(&writer.to_bytes().unwrap())
    }

    fn records(values: &[i64]) -> Vec<u8> {
        let mut writer = Writer::new();
        writer
            .write_map(2)
            .write_str("status")
            .write_str("ok")
            .write_str("records")
            .write_array(u32::try_from(values.len()).unwrap());
        for value in values {
            writer.write_map(1).write_str("n").write_int(*value);
        }
        frame(&writer.to_bytes().unwrap())
    }

    fn error(message: &str) -> Vec<u8> {
        let mut writer = Writer::new();
        writer
            .write_map(2)
            .write_str("status")
            .write_str("error")
            .write_str("error")
            .write_str(message);
        frame(&writer.to_bytes().unwrap())
    }

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut frame = Vec::from(u32::try_from(payload.len()).unwrap().to_le_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    /// A query socket that reads one request and answers with `messages`.
    fn eventd(messages: Vec<Vec<u8>>) -> (PathBuf, std::thread::JoinHandle<()>) {
        static NEXT: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "eventd-client-{}-{}.sock",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path).unwrap();
        let served = path.clone();
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut prefix = [0_u8; 4];
            stream.read_exact(&mut prefix).unwrap();
            let mut request = vec![0; u32::from_le_bytes(prefix) as usize];
            stream.read_exact(&mut request).unwrap();
            for message in messages {
                if stream.write_all(&message).is_err() {
                    break;
                }
            }
            let _ = std::fs::remove_file(&served);
        });
        (path, thread)
    }

    fn numbers(records: &[Record]) -> Vec<i64> {
        records
            .iter()
            .map(|record| match record.get("n") {
                Some(Value::Signed(value)) => *value,
                other => panic!("unexpected {other:?}"),
            })
            .collect()
    }

    #[test]
    fn a_query_gives_every_chunk_once_it_ends() {
        let (path, eventd) = eventd(vec![records(&[1, 2]), records(&[3]), status("end")]);
        assert_eq!(numbers(&query(&path, "LOGS").unwrap()), [1, 2, 3]);
        eventd.join().unwrap();
    }

    #[test]
    fn a_query_that_fails_part_way_gives_nothing() {
        let (path, eventd) = eventd(vec![records(&[1]), error("query timed out")]);
        let failed = query(&path, "LOGS").unwrap_err();
        assert!(matches!(failed, Error::Refused(ref message) if message == "query timed out"));
        eventd.join().unwrap();
    }

    #[test]
    fn a_tail_reports_its_initial_result_whole_then_live_batches() {
        let (path, eventd) = eventd(vec![
            records(&[3]),
            records(&[2, 1]),
            status("watch"),
            records(&[4]),
            records(&[5]),
            error("eventd is shutting down"),
        ]);
        let tail = Tail::start(&path, "LOGS STREAM", 8).unwrap();
        let wait = |tail: &Tail| tail.updates().recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            matches!(wait(&tail), Tailed::Initial(ref records) if numbers(records) == [3, 2, 1])
        );
        assert!(matches!(wait(&tail), Tailed::Live(ref records) if numbers(records) == [4]));
        assert!(matches!(wait(&tail), Tailed::Live(ref records) if numbers(records) == [5]));
        assert!(matches!(wait(&tail), Tailed::Ended(Error::Refused(_))));
        eventd.join().unwrap();
    }

    #[test]
    fn a_tail_that_fails_before_it_is_live_reports_no_records() {
        let (path, eventd) = eventd(vec![
            records(&[1]),
            error("too many concurrent streaming queries"),
        ]);
        let tail = Tail::start(&path, "LOGS STREAM", 8).unwrap();
        assert!(matches!(
            tail.updates().recv_timeout(Duration::from_secs(5)).unwrap(),
            Tailed::Ended(Error::Refused(_))
        ));
        eventd.join().unwrap();
    }

    #[test]
    fn nothing_listening_is_a_connect_error() {
        let path = std::env::temp_dir().join("eventd-client-nobody-here.sock");
        assert!(matches!(query(&path, "LOGS"), Err(Error::Connect { .. })));
    }
}
