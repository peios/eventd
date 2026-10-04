//! Bounded nonblocking Unix datagram ingestion socket.

use core::fmt;
use std::os::fd::{AsFd, AsRawFd};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};

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
        establish: impl FnOnce(&Path, Protection) -> Result<(), SocketError>,
    ) -> Result<Self, SocketError> {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) if metadata.file_type().is_socket() => {
                std::fs::remove_file(path).map_err(SocketError::Io)?;
            }
            Ok(_) => return Err(SocketError::Occupied(path.to_owned())),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(SocketError::Io(error)),
        }
        let socket = UnixDatagram::bind(path).map_err(SocketError::Io)?;
        let metadata = std::fs::symlink_metadata(path).map_err(SocketError::Io)?;
        socket.set_nonblocking(true).map_err(SocketError::Io)?;
        set_receive_buffer(&socket, datagram_ceiling)?;

        establish(path, protection)?;
        Ok(Self {
            socket,
            path: path.to_owned(),
            identity: (metadata.dev(), metadata.ino()),
        })
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

    pub fn wait_readable(&self, timeout_milliseconds: i32) -> Result<(), SocketError> {
        let mut descriptor = libc::pollfd {
            fd: self.socket.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: `descriptor` names one writable pollfd for the call.
        let result = unsafe { libc::poll(&raw mut descriptor, 1, timeout_milliseconds) };
        if result < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::Interrupted {
                return Err(SocketError::Io(error));
            }
        }
        Ok(())
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

fn unlink_if_owned(path: &Path, identity: (u64, u64)) {
    if std::fs::symlink_metadata(path).is_ok_and(|metadata| {
        metadata.file_type().is_socket() && (metadata.dev(), metadata.ino()) == identity
    }) {
        let _ = std::fs::remove_file(path);
    }
}

fn set_receive_buffer(socket: &UnixDatagram, ceiling: usize) -> Result<(), SocketError> {
    // Linux doubles SO_RCVBUF for bookkeeping. Requesting twice the ceiling
    // produces an effective queue no larger than the permitted four times.
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
    ) -> impl FnOnce(&Path, Protection) -> Result<(), SocketError> {
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
    fn every_authenticated_caller_may_send_to_the_metric_socket() {
        let descriptor = sddl::parse(METRIC_PUBLISHERS_SDDL).expect("valid publishers descriptor");
        let text = sddl::format(descriptor.as_bytes()).expect("format descriptor");
        assert!(text.contains(";AU)"), "{text}");
    }
}
