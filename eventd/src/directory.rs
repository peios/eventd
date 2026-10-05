//! Race-free opening of provisioned state directories.

use core::fmt;
use std::ffi::CString;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Component, Path, PathBuf};

use peios::file::{File, SecInfo};
use peios::security::{AccessMask, Control, SdView, sddl};

const REQUIRED_SDDL: &str = "O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)(A;OICI;GA;;;S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124)";

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
        if !has_required_protection(actual.as_bytes()).map_err(DirectoryError::Security)? {
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

/// Whether `actual` grants exactly what [`REQUIRED_SDDL`] does.
///
/// Compared as access, not as text: a descriptor applied with `GA` may be
/// stored with the generic bits already mapped through the file generic
/// mapping (`FA`), which is the same grant (PEI-1316). Everything else —
/// owner, group, the DACL's protection and inheritance, and each ACE's type,
/// flags and trustee — must match exactly.
fn has_required_protection(actual: &[u8]) -> Result<bool, peios::Error> {
    let required = sddl::parse(REQUIRED_SDDL)?;
    let required = SdView::parse(required.as_bytes())?;
    let actual = SdView::parse(actual)?;
    let dacl_control =
        Control::DACL_PRESENT | Control::DACL_PROTECTED | Control::DACL_AUTO_INHERITED;
    if actual.owner() != required.owner()
        || actual.group() != required.group()
        || actual.control() & dacl_control != required.control() & dacl_control
    {
        return Ok(false);
    }
    let (Some(actual), Some(required)) = (actual.dacl(), required.dacl()) else {
        return Ok(false);
    };
    let mapping = File::generic_mapping();
    let mapped = |mask: u32| AccessMask::from_bits_retain(mask).resolve_generic(&mapping);
    Ok(actual.len() == required.len()
        && actual.iter().zip(required.iter()).all(|(have, want)| {
            have.ace_type() == want.ace_type()
                && have.flags() == want.flags()
                && have.sid() == want.sid()
                && have.object_type() == want.object_type()
                && have.inherited_object_type() == want.inherited_object_type()
                && have.app_data() == want.app_data()
                && mapped(have.mask()) == mapped(want.mask())
        }))
}

fn open_components(path: &Path) -> Result<OwnedFd, DirectoryError> {
    if !path.is_absolute() {
        return Err(DirectoryError::Invalid(path.to_owned()));
    }
    // SAFETY: the static path is NUL terminated and flags have no mode argument.
    let root = unsafe {
        libc::open(
            c"/".as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if root < 0 {
        return Err(DirectoryError::Io(std::io::Error::last_os_error()));
    }
    // SAFETY: `root` is a fresh owned descriptor after successful open.
    let mut current = unsafe { OwnedFd::from_raw_fd(root) };
    let components: Vec<_> = path.components().collect();
    for (index, component) in components.iter().enumerate() {
        let Component::Normal(name) = component else {
            if matches!(component, Component::RootDir) {
                continue;
            }
            return Err(DirectoryError::Invalid(path.to_owned()));
        };
        let name =
            CString::new(name.as_bytes()).map_err(|_| DirectoryError::Invalid(path.into()))?;
        // SAFETY: `current` and `name` are live. O_NOFOLLOW rejects a symlink at
        // this component; O_DIRECTORY rejects non-directories. Intermediate
        // O_PATH handles preserve anchoring without asking to list private
        // ancestors such as /var/state. Only the store itself needs reading.
        let next = unsafe {
            libc::openat(
                current.as_raw_fd(),
                name.as_ptr(),
                (if index + 1 == components.len() {
                    libc::O_RDONLY
                } else {
                    libc::O_PATH
                }) | libc::O_DIRECTORY
                    | libc::O_NOFOLLOW
                    | libc::O_CLOEXEC,
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

#[cfg(test)]
mod tests {
    use super::*;

    const SERVICE_SID: &str = "S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124";

    fn protected(sddl: &str) -> bool {
        let descriptor = sddl::parse(sddl).expect("valid descriptor");
        has_required_protection(descriptor.as_bytes()).expect("comparable descriptor")
    }

    #[test]
    fn the_required_descriptor_is_accepted() {
        assert!(protected(REQUIRED_SDDL));
    }

    // PEI-1316: path provisioning creates the store directory from the
    // GENERIC_ALL blob, and KACS stores the grant mapped through the file
    // generic mapping. It is the same grant.
    #[test]
    fn a_descriptor_whose_generic_grants_were_mapped_to_file_rights_is_accepted() {
        assert!(protected(&format!(
            "O:SYG:SYD:P(A;CIOI;FA;;;SY)(A;CIOI;FA;;;BA)(A;CIOI;FA;;;{SERVICE_SID})"
        )));
    }

    #[test]
    fn a_descriptor_that_grants_or_protects_differently_is_refused() {
        for sddl in [
            // Not protected from inheritance.
            format!("O:SYG:SYD:(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)(A;OICI;GA;;;{SERVICE_SID})"),
            // Read where all is required, generic or mapped.
            format!("O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GR;;;BA)(A;OICI;GA;;;{SERVICE_SID})"),
            format!("O:SYG:SYD:P(A;OICI;FA;;;SY)(A;OICI;FR;;;BA)(A;OICI;FA;;;{SERVICE_SID})"),
            // A further trustee.
            format!(
                "O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)(A;OICI;GA;;;{SERVICE_SID})(A;;FR;;;WD)"
            ),
            // Not inherited by children.
            format!("O:SYG:SYD:P(A;;FA;;;SY)(A;;FA;;;BA)(A;;FA;;;{SERVICE_SID})"),
            // Another owner.
            format!("O:BAG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)(A;OICI;GA;;;{SERVICE_SID})"),
        ] {
            assert!(!protected(&sddl), "{sddl} is refused");
        }
    }
}
