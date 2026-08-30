//! Race-free opening of provisioned state directories.

use core::fmt;
use std::ffi::CString;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};

use peios::file::{File, SecInfo};
use peios::security::sddl;

const REQUIRED_SDDL: &str = "O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)";

pub struct StoreDirectory {
    handle: File,
}

impl StoreDirectory {
    pub fn open(path: &Path) -> Result<Self, DirectoryError> {
        let handle = open_components(path)?;
        let file = File::from(handle);
        let actual = file
            .fd_get_sd(SecInfo::OWNER | SecInfo::GROUP | SecInfo::DACL)
            .map_err(DirectoryError::Security)?;
        let required = sddl::parse(REQUIRED_SDDL).map_err(DirectoryError::Security)?;
        let actual_sddl = sddl::format(actual.as_bytes()).map_err(DirectoryError::Security)?;
        let required_sddl = sddl::format(required.as_bytes()).map_err(DirectoryError::Security)?;
        if actual_sddl != required_sddl {
            return Err(DirectoryError::Protection(path.to_owned()));
        }
        Ok(Self { handle: file })
    }

    /// Return an fd-anchored child pathname for `SQLite`.
    pub fn child(&self, name: &str) -> PathBuf {
        self.anchored_path().join(name)
    }

    pub fn anchored_path(&self) -> PathBuf {
        PathBuf::from(format!("/proc/self/fd/{}", self.handle.as_raw_fd()))
    }
}

fn open_components(path: &Path) -> Result<OwnedFd, DirectoryError> {
    if !path.is_absolute() {
        return Err(DirectoryError::Invalid(path.to_owned()));
    }
    // SAFETY: the static path is NUL terminated and flags have no mode argument.
    let root = unsafe {
        libc::open(
            c"/".as_ptr(),
            libc::O_PATH | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if root < 0 {
        return Err(DirectoryError::Io(std::io::Error::last_os_error()));
    }
    // SAFETY: `root` is a fresh owned descriptor after successful open.
    let mut current = unsafe { OwnedFd::from_raw_fd(root) };
    for component in path.components() {
        let Component::Normal(name) = component else {
            if matches!(component, Component::RootDir) {
                continue;
            }
            return Err(DirectoryError::Invalid(path.to_owned()));
        };
        let name =
            CString::new(name.as_bytes()).map_err(|_| DirectoryError::Invalid(path.into()))?;
        // SAFETY: `current` and `name` are live. O_NOFOLLOW rejects a symlink at
        // this component; O_DIRECTORY rejects non-directories.
        let next = unsafe {
            libc::openat(
                current.as_raw_fd(),
                name.as_ptr(),
                libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        if next < 0 {
            return Err(DirectoryError::Io(std::io::Error::last_os_error()));
        }
        // SAFETY: `next` is a fresh owned descriptor after successful openat.
        current = unsafe { OwnedFd::from_raw_fd(next) };
    }
    Ok(current)
}

#[derive(Debug)]
pub enum DirectoryError {
    Invalid(PathBuf),
    Io(std::io::Error),
    Security(peios::Error),
    Protection(PathBuf),
}

impl fmt::Display for DirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(path) => {
                write!(formatter, "invalid state-directory path {}", path.display())
            }
            Self::Io(error) => write!(formatter, "cannot open state directory safely: {error}"),
            Self::Security(error) => write!(
                formatter,
                "cannot inspect state-directory protection: {error}"
            ),
            Self::Protection(path) => write!(
                formatter,
                "state directory {} does not have the required protected descriptor",
                path.display()
            ),
        }
    }
}

impl std::error::Error for DirectoryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Security(error) => Some(error),
            Self::Invalid(_) | Self::Protection(_) => None,
        }
    }
}
