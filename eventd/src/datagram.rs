//! Bounded nonblocking Unix datagram ingestion socket.

use core::fmt;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::FileTypeExt;
use std::os::unix::fs::MetadataExt;
use std::os::unix::net::UnixDatagram;
use std::path::{Path, PathBuf};

use peios::file::{File, SecInfo};

pub struct IngestionSocket {
    socket: UnixDatagram,
    path: PathBuf,
    identity: (u64, u64),
}

impl IngestionSocket {
    pub fn bind(path: &Path, datagram_ceiling: usize) -> Result<Self, SocketError> {
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

        // Binding applies the parent directory's inheritable descriptor. Read it
        // back and establish that complete descriptor explicitly before the
        // first receive, so a directory without usable inheritance fails here.
        let duplicate = duplicate_fd(socket.as_raw_fd())?;
        let file = File::from(duplicate);
        let secinfo = SecInfo::OWNER | SecInfo::GROUP | SecInfo::DACL | SecInfo::LABEL;
        let descriptor = file.fd_get_sd(secinfo).map_err(SocketError::Security)?;
        file.fd_set_sd(secinfo, &descriptor)
            .map_err(SocketError::Security)?;
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

fn duplicate_fd(raw: i32) -> Result<OwnedFd, SocketError> {
    // SAFETY: F_DUPFD_CLOEXEC returns a fresh descriptor or -1.
    let duplicate = unsafe { libc::fcntl(raw, libc::F_DUPFD_CLOEXEC, 0) };
    if duplicate < 0 {
        Err(SocketError::Io(std::io::Error::last_os_error()))
    } else {
        // SAFETY: `duplicate` is a fresh owned descriptor.
        Ok(unsafe { OwnedFd::from_raw_fd(duplicate) })
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
pub enum SocketError {
    Io(std::io::Error),
    Security(peios::Error),
    Occupied(PathBuf),
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
            Self::Length => formatter.write_str("ingestion socket size exceeds the platform ABI"),
        }
    }
}

impl std::error::Error for SocketError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Security(error) => Some(error),
            Self::Occupied(_) | Self::Length => None,
        }
    }
}
