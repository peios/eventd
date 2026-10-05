//! Bounded nonblocking Unix datagram ingestion socket.

use core::fmt;
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::OpenOptionsExt;
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, SendError, Sender, channel};

use peios::file::SecInfo;
use peios::security::{SecurityDescriptor, sddl};
use peios::token::Token;

/// The service manager is SYSTEM without the Service-logon group. Every
/// phase-2 service, including a SYSTEM service, carries `SU` (S-1-5-6). The
/// explicit deny therefore keeps services from forging peinit's log origins,
/// while SYSTEM can still deliver logs and the socket owner can manage it.
/// Preserve the virtual service owner: changing it to SYSTEM would require a
/// privilege the long-running daemon deliberately does not hold.
const LOG_BROKER_SDDL: &str = "D:P(D;;0x2;;;SU)(A;;GA;;;SY)(A;;GA;;;OW)";

/// Every authenticated caller may send to the metric socket. Who may publish
/// what is decided per metric name, against the token each datagram carries
/// (`EVENTD_PUBLISH`, TRM §7.6), not by the socket. An operator who wants it
/// narrower sets another descriptor after eventd starts; eventd sets this one
/// again each time it starts.
const METRIC_PUBLISHERS_SDDL: &str = "D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)(A;;FW;;;AU)";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protection {
    PeinitLogBroker,
    MetricPublishers,
}

impl Protection {
    const fn sddl(self) -> &'static str {
        match self {
            Self::PeinitLogBroker => LOG_BROKER_SDDL,
            Self::MetricPublishers => METRIC_PUBLISHERS_SDDL,
        }
    }
}

pub struct IngestionSocket {
    socket: UnixDatagram,
    path: PathBuf,
    identity: (u64, u64),
    waker: Waker,
}

/// Rouses the thread waiting in [`IngestionSocket::wait_readable`] for
/// something other than a datagram: a maintenance command, say, which would
/// otherwise wait out the poll's timeout behind an idle socket (PEI-1315).
///
/// An eventfd in semaphore mode: each wake adds one and each wait the
/// eventfd ends takes one away. The thread tries for a command on every
/// pass of its loop, so the count never falls below the commands still
/// waiting; a pass that finds none was woken early, which costs a pass.
#[derive(Clone)]
pub struct Waker(Arc<OwnedFd>);

impl Waker {
    pub fn new() -> std::io::Result<Self> {
        // SAFETY: eventfd takes no pointers.
        let descriptor = unsafe {
            libc::eventfd(
                0,
                libc::EFD_CLOEXEC | libc::EFD_NONBLOCK | libc::EFD_SEMAPHORE,
            )
        };
        if descriptor < 0 {
            return Err(std::io::Error::last_os_error());
        }
        // SAFETY: `descriptor` is a fresh descriptor this call owns.
        Ok(Self(Arc::new(unsafe { OwnedFd::from_raw_fd(descriptor) })))
    }

    pub fn wake(&self) {
        let one = 1_u64;
        // SAFETY: the write reads eight bytes from a live u64. It fails only
        // with the count at its ceiling, when the eventfd is readable anyway.
        let _ = unsafe {
            libc::write(
                self.0.as_raw_fd(),
                (&raw const one).cast(),
                core::mem::size_of::<u64>(),
            )
        };
    }

    /// Take one wake, if there is one.
    fn take(&self) {
        let mut count = 0_u64;
        // SAFETY: the read writes at most eight bytes to a live u64. With no
        // wake to take it fails with EAGAIN, which leaves nothing to undo.
        let _ = unsafe {
            libc::read(
                self.0.as_raw_fd(),
                (&raw mut count).cast(),
                core::mem::size_of::<u64>(),
            )
        };
    }
}

/// A channel to an ingest thread whose every send wakes the thread.
pub struct WakingSender<T> {
    sender: Sender<T>,
    waker: Waker,
}

impl<T> WakingSender<T> {
    pub fn send(&self, value: T) -> Result<(), SendError<T>> {
        self.sender.send(value)?;
        self.waker.wake();
        Ok(())
    }
}

/// A channel whose sends wake the thread `waker` belongs to.
pub fn waking_channel<T>(waker: Waker) -> (WakingSender<T>, Receiver<T>) {
    let (sender, receiver) = channel();
    (WakingSender { sender, waker }, receiver)
}

impl IngestionSocket {
    pub fn bind(
        path: &Path,
        datagram_ceiling: usize,
        protection: Protection,
    ) -> Result<Self, SocketError> {
        Self::bind_with(path, datagram_ceiling, protection, establish_protection)
    }

    /// A socket for host tests, where no descriptor can be set: bound as
    /// `bind` binds, with the protection step left out.
    #[cfg(test)]
    pub fn unprotected(path: &Path, datagram_ceiling: usize) -> Result<Self, SocketError> {
        Self::bind_with(
            path,
            datagram_ceiling,
            Protection::PeinitLogBroker,
            |_, _| Ok(()),
        )
    }

    /// Bind, establishing the socket's descriptor through `establish` before
    /// the socket is returned to anything that could receive on it.
    fn bind_with(
        path: &Path,
        datagram_ceiling: usize,
        protection: Protection,
        establish: impl Fn(&Path, Protection) -> Result<(), SocketError>,
    ) -> Result<Self, SocketError> {
        match occupant(path).map_err(SocketError::Io)? {
            Some(metadata) if metadata.file_type().is_socket() => {
                remove_stale(path, protection, &establish)?;
            }
            Some(_) => return Err(SocketError::Occupied(path.to_owned())),
            None => {}
        }
        let socket = UnixDatagram::bind(path).map_err(SocketError::Io)?;
        let metadata = occupant(path)
            .map_err(SocketError::Io)?
            .ok_or_else(|| SocketError::Io(std::io::ErrorKind::NotFound.into()))?;
        socket.set_nonblocking(true).map_err(SocketError::Io)?;
        set_receive_buffer(&socket, datagram_ceiling)?;

        establish(path, protection)?;
        Ok(Self {
            socket,
            path: path.to_owned(),
            identity: (metadata.dev(), metadata.ino()),
            waker: Waker::new().map_err(SocketError::Io)?,
        })
    }

    /// The waker that ends this socket's [`wait_readable`](Self::wait_readable).
    pub fn waker(&self) -> Waker {
        self.waker.clone()
    }

    pub fn receive(&self, buffer: &mut [u8]) -> Result<Receive, SocketError> {
        let mut iovec = libc::iovec {
            iov_base: buffer.as_mut_ptr().cast(),
            iov_len: buffer.len(),
        };
        // SAFETY: zero is a valid empty msghdr, completed with one live iovec.
        let mut message: libc::msghdr = unsafe { core::mem::zeroed() };
        message.msg_iov = &raw mut iovec;
        message.msg_iovlen = 1;
        // SAFETY: the socket and iovec are live for the call. MSG_TRUNC makes
        // the return value the original datagram length when the buffer is short.
        let received = unsafe {
            libc::recvmsg(
                self.socket.as_raw_fd(),
                &raw mut message,
                libc::MSG_DONTWAIT | libc::MSG_TRUNC,
            )
        };
        if received < 0 {
            let error = std::io::Error::last_os_error();
            return if error.kind() == std::io::ErrorKind::WouldBlock {
                Ok(Receive::Empty)
            } else {
                Err(SocketError::Io(error))
            };
        }
        let length = usize::try_from(received).map_err(|_| SocketError::Length)?;
        if length > buffer.len() || message.msg_flags & libc::MSG_TRUNC != 0 {
            Ok(Receive::Truncated)
        } else {
            Ok(Receive::Datagram(length))
        }
    }

    /// Receive one datagram and the KACS identity conveyed with it. The SDK
    /// reserves room for exactly one token and no ordinary descriptors; it
    /// closes anything the caller did not request. Its token-only path uses a
    /// fixed stack control buffer and performs no allocation.
    pub fn receive_token(&self, buffer: &mut [u8]) -> Result<TokenReceive, SocketError> {
        match peios::socket::recv_message(
            self.socket.as_fd(),
            buffer,
            0,
            libc::MSG_DONTWAIT | libc::MSG_TRUNC,
        ) {
            Ok(message) if message.truncated || message.control_truncated => {
                Ok(TokenReceive::Truncated)
            }
            Ok(message) => Ok(TokenReceive::Datagram {
                length: message.len,
                token: message.token,
            }),
            Err(error) if error.raw_os_error() == Some(libc::EAGAIN) => Ok(TokenReceive::Empty),
            Err(error) => Err(SocketError::Security(error)),
        }
    }

    pub fn configure_receive_buffer(&self, datagram_ceiling: usize) -> Result<(), SocketError> {
        set_receive_buffer(&self.socket, datagram_ceiling)
    }

    /// Wait until a datagram arrives, the socket's [`Waker`] is rung, or
    /// the timeout passes.
    pub fn wait_readable(&self, timeout_milliseconds: i32) -> Result<(), SocketError> {
        let mut descriptors = [
            libc::pollfd {
                fd: self.socket.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
            libc::pollfd {
                fd: self.waker.0.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            },
        ];
        // SAFETY: `descriptors` names two writable pollfds for the call.
        let result = unsafe { libc::poll(descriptors.as_mut_ptr(), 2, timeout_milliseconds) };
        if result < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                return Err(SocketError::Io(error));
            }
        } else if descriptors[1].revents & libc::POLLIN != 0 {
            self.waker.take();
        }
        Ok(())
    }

    /// Refuse every datagram from here on: a send to the socket now fails
    /// with `EPIPE`, telling its sender the datagram was not taken, where
    /// one queued now would be freed unread when the descriptor closes.
    /// What is already queued stays readable.
    pub fn shut_for_reading(&self) -> Result<(), SocketError> {
        self.socket
            .shutdown(std::net::Shutdown::Read)
            .map_err(SocketError::Io)
    }

    /// Remove the bound pathname without closing the queued datagram descriptor.
    pub fn unlink(&self) {
        unlink_if_owned(&self.path, self.identity);
    }
}

fn establish_protection(path: &Path, protection: Protection) -> Result<(), SocketError> {
    establish_protection_with(
        path,
        protection,
        |path, descriptor| {
            peios::file::set_sd(
                None,
                path,
                SecInfo::DACL,
                descriptor,
                libc::AT_SYMLINK_NOFOLLOW,
            )
        },
        |path| peios::file::get_sd(None, path, SecInfo::DACL, libc::AT_SYMLINK_NOFOLLOW),
    )
}

/// Set the protection's descriptor through `set`, read it back through
/// `get`, and refuse the socket unless the two agree.
fn establish_protection_with(
    path: &Path,
    protection: Protection,
    set: impl FnOnce(&Path, &SecurityDescriptor) -> peios::Result<()>,
    get: impl FnOnce(&Path) -> peios::Result<SecurityDescriptor>,
) -> Result<(), SocketError> {
    let descriptor = sddl::parse(protection.sddl()).map_err(SocketError::Security)?;
    set(path, &descriptor).map_err(SocketError::Security)?;
    let actual = get(path).map_err(SocketError::Security)?;
    let actual = sddl::format(actual.as_bytes()).map_err(SocketError::Security)?;
    let expected = sddl::format(descriptor.as_bytes()).map_err(SocketError::Security)?;
    if actual != expected {
        return Err(SocketError::Protection(path.to_owned()));
    }
    Ok(())
}

impl Drop for IngestionSocket {
    fn drop(&mut self) {
        self.unlink();
    }
}

/// What occupies `path`, if anything, read through an `O_PATH` descriptor
/// opened without following a symlink. `fstat` on such a descriptor needs
/// no right on the object, where `lstat` needs `FILE_READ_ATTRIBUTES` (KACS
/// TRM, FACS). An operator may narrow a socket's descriptor until eventd,
/// its owner, holds only the owner's implicit `READ_CONTROL | WRITE_DAC`;
/// `lstat` then fails, and a socket eventd left behind could be neither
/// recognised as stale nor unlinked.
fn occupant(path: &Path) -> std::io::Result<Option<std::fs::Metadata>> {
    match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_PATH | libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => file.metadata().map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

/// Unlink the stale socket at `path`. Where its descriptor no longer grants
/// eventd `DELETE` and the directory does not grant `FILE_DELETE_CHILD`,
/// eventd's own descriptor is put back first — the owner's implicit
/// `WRITE_DAC` allows that, and the descriptor grants the owner `DELETE` —
/// and the unlink is tried once more.
fn remove_stale(
    path: &Path,
    protection: Protection,
    establish: &impl Fn(&Path, Protection) -> Result<(), SocketError>,
) -> Result<(), SocketError> {
    let unlink = || match std::fs::remove_file(path) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        result => result,
    };
    match unlink() {
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
            if establish(path, protection).is_err() {
                return Err(SocketError::Io(error));
            }
            unlink().map_err(SocketError::Io)
        }
        result => result.map_err(SocketError::Io),
    }
}

fn unlink_if_owned(path: &Path, identity: (u64, u64)) {
    let owned = match occupant(path) {
        Ok(Some(metadata)) => {
            metadata.file_type().is_socket() && (metadata.dev(), metadata.ino()) == identity
        }
        Ok(None) => false,
        Err(error) => {
            eprintln!("eventd: cannot inspect socket {}: {error}", path.display());
            false
        }
    };
    if owned
        && let Err(error) = std::fs::remove_file(path)
        && error.kind() != std::io::ErrorKind::NotFound
    {
        eprintln!("eventd: cannot unlink socket {}: {error}", path.display());
    }
}

fn set_receive_buffer(socket: &UnixDatagram, ceiling: usize) -> Result<(), SocketError> {
    // Linux doubles SO_RCVBUF for bookkeeping. Requesting twice the ceiling
    // produces an effective queue no larger than the permitted four times.
    // For a Unix datagram socket that is a ceiling on paper only: the kernel
    // charges a queued datagram to its sender's send buffer and bounds the
    // queue by net.unix.max_dgram_qlen datagrams, so SO_RCVBUF bounds nothing.
    let requested = i32::try_from(ceiling.saturating_mul(2)).map_err(|_| SocketError::Length)?;
    let length = libc::socklen_t::try_from(core::mem::size_of_val(&requested))
        .map_err(|_| SocketError::Length)?;
    // SAFETY: the option points to one live i32 for `length` bytes.
    let result = unsafe {
        libc::setsockopt(
            socket.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&raw const requested).cast(),
            length,
        )
    };
    if result == 0 {
        Ok(())
    } else {
        Err(SocketError::Io(std::io::Error::last_os_error()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Receive {
    Empty,
    Truncated,
    Datagram(usize),
}

#[derive(Debug)]
pub enum TokenReceive {
    Empty,
    Truncated,
    Datagram { length: usize, token: Option<Token> },
}

#[derive(Debug)]
pub enum SocketError {
    Io(std::io::Error),
    Security(peios::Error),
    Occupied(PathBuf),
    Protection(PathBuf),
    Length,
}

impl fmt::Display for SocketError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "ingestion socket error: {error}"),
            Self::Security(error) => {
                write!(formatter, "cannot establish socket protection: {error}")
            }
            Self::Occupied(path) => write!(
                formatter,
                "configured socket path {} is not a socket",
                path.display()
            ),
            Self::Protection(path) => write!(
                formatter,
                "log ingestion socket {} does not have the required peinit-only descriptor",
                path.display()
            ),
            Self::Length => formatter.write_str("ingestion socket size exceeds the platform ABI"),
        }
    }
}

impl std::error::Error for SocketError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Security(error) => Some(error),
            Self::Occupied(_) | Self::Protection(_) | Self::Length => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn log_broker_descriptor_denies_services_before_allowing_system() {
        let descriptor = sddl::parse(LOG_BROKER_SDDL).expect("valid log broker descriptor");
        assert_eq!(
            sddl::format(descriptor.as_bytes()).expect("format descriptor"),
            LOG_BROKER_SDDL
        );
    }

    fn socket_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "eventd-datagram-test-{}-{}-{name}.sock",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        path
    }

    /// Establish protection against a stand-in filesystem whose read-back
    /// returns `stored`, whatever was set.
    fn establish_reading_back(
        stored: &str,
    ) -> impl Fn(&Path, Protection) -> Result<(), SocketError> {
        move |path, protection| {
            establish_protection_with(path, protection, |_, _| Ok(()), |_| sddl::parse(stored))
        }
    }

    #[test]
    fn a_socket_whose_descriptor_reads_back_differently_is_refused_before_it_can_receive() {
        for protection in [Protection::PeinitLogBroker, Protection::MetricPublishers] {
            let path = socket_path("refused");
            // The read-back is a descriptor that admits everyone.
            let result = IngestionSocket::bind_with(
                &path,
                4_096,
                protection,
                establish_reading_back("D:(A;;GA;;;WD)"),
            );
            assert!(
                matches!(&result, Err(SocketError::Protection(refused)) if refused == &path),
                "{protection:?}: {:?}",
                result.as_ref().err()
            );
            // No socket was handed out, and nothing is receiving at the path.
            let client = UnixDatagram::unbound().unwrap();
            assert!(client.send_to(b"probe", &path).is_err());
            let _ = std::fs::remove_file(&path);
        }
    }

    #[test]
    fn a_socket_whose_descriptor_reads_back_as_set_is_bound() {
        for protection in [Protection::PeinitLogBroker, Protection::MetricPublishers] {
            let path = socket_path("bound");
            let socket = IngestionSocket::bind_with(
                &path,
                4_096,
                protection,
                establish_reading_back(protection.sddl()),
            )
            .unwrap();
            let client = UnixDatagram::unbound().unwrap();
            client.send_to(b"probe", &path).unwrap();
            let mut buffer = [0_u8; 16];
            assert_eq!(socket.receive(&mut buffer).unwrap(), Receive::Datagram(5));
        }
    }

    #[test]
    fn the_occupant_of_a_socket_path_is_read_without_following_a_symlink() {
        let path = socket_path("occupant");
        assert!(occupant(&path).unwrap().is_none(), "nothing there yet");
        let socket = IngestionSocket::unprotected(&path, 4_096).unwrap();
        let found = occupant(&path).unwrap().unwrap();
        assert!(found.file_type().is_socket());
        assert_eq!((found.dev(), found.ino()), socket.identity);
        // A symlink to the socket is a symlink, not the socket.
        let link = socket_path("occupant-link");
        std::os::unix::fs::symlink(&path, &link).unwrap();
        assert!(occupant(&link).unwrap().unwrap().file_type().is_symlink());
        assert!(matches!(
            IngestionSocket::unprotected(&link, 4_096),
            Err(SocketError::Occupied(occupied)) if occupied == link
        ));
        std::fs::remove_file(&link).unwrap();
        // The socket eventd bound is unlinked as it goes.
        drop(socket);
        assert!(occupant(&path).unwrap().is_none());
    }

    #[test]
    fn a_stale_socket_is_replaced_and_a_file_at_a_socket_path_is_refused() {
        let path = socket_path("stale");
        drop(UnixDatagram::bind(&path).unwrap());
        let socket = IngestionSocket::unprotected(&path, 4_096).unwrap();
        let client = UnixDatagram::unbound().unwrap();
        client.send_to(b"probe", &path).unwrap();
        let mut buffer = [0_u8; 16];
        assert_eq!(socket.receive(&mut buffer).unwrap(), Receive::Datagram(5));
        drop(socket);

        std::fs::write(&path, b"not a socket").unwrap();
        assert!(matches!(
            IngestionSocket::unprotected(&path, 4_096),
            Err(SocketError::Occupied(occupied)) if occupied == path
        ));
        assert_eq!(std::fs::read(&path).unwrap(), b"not a socket");
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn a_stale_socket_eventd_may_not_delete_is_reclaimed_and_replaced() {
        use std::os::unix::fs::PermissionsExt;
        let directory = socket_path("reclaim-dir");
        std::fs::create_dir(&directory).unwrap();
        let path = directory.join("stale.sock");
        drop(UnixDatagram::bind(&path).unwrap());
        let set_mode = |mode| {
            std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(mode)).unwrap();
        };
        // The directory refuses the unlink, as a socket whose descriptor an
        // operator narrowed past eventd's DELETE does where the directory
        // does not grant FILE_DELETE_CHILD. Putting eventd's own descriptor
        // back is what returns the right; the stand-in returns it here.
        set_mode(0o555);
        let result =
            IngestionSocket::bind_with(&path, 4_096, Protection::MetricPublishers, |_, _| {
                set_mode(0o755);
                Ok(())
            });
        set_mode(0o755);
        let socket = result.unwrap();
        let client = UnixDatagram::unbound().unwrap();
        client.send_to(b"probe", &path).unwrap();
        let mut buffer = [0_u8; 16];
        assert_eq!(socket.receive(&mut buffer).unwrap(), Receive::Datagram(5));
        drop(socket);
        std::fs::remove_dir_all(&directory).unwrap();
    }

    #[test]
    fn each_wake_ends_one_wait_and_no_more() {
        let path = socket_path("waker");
        let socket = IngestionSocket::unprotected(&path, 4_096).unwrap();
        let (sender, commands) = waking_channel(socket.waker());
        sender.send(1).unwrap();
        sender.send(2).unwrap();
        let waited = |socket: &IngestionSocket| {
            let started = std::time::Instant::now();
            socket.wait_readable(200).unwrap();
            started.elapsed()
        };
        let short = std::time::Duration::from_millis(100);
        // Two commands queued, two waits ended at once: the second command
        // is not left behind the first's wake.
        assert!(waited(&socket) < short);
        assert!(waited(&socket) < short);
        assert_eq!(commands.try_iter().collect::<Vec<_>>(), [1, 2]);
        // With the wakes taken, an idle socket waits out its timeout.
        assert!(waited(&socket) >= short);
    }

    #[test]
    fn every_authenticated_caller_may_send_to_the_metric_socket() {
        let descriptor = sddl::parse(METRIC_PUBLISHERS_SDDL).expect("valid publishers descriptor");
        let text = sddl::format(descriptor.as_bytes()).expect("format descriptor");
        assert!(text.contains(";AU)"), "{text}");
    }
}
