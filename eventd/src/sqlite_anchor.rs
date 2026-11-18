//! Keep validated directory handles intact through SQLite's Unix VFS.
//!
//! Unix `xFullPathname` resolves procfs magic links and inspects every ancestor.
//! That both discards our directory anchor and requires metadata rights on
//! ancestors the service may only traverse. All other Unix VFS operations,
//! including locking, synchronization and final-component `O_NOFOLLOW`, stay intact.

use std::ffi::{CStr, c_char, c_int};
use std::sync::OnceLock;

use rusqlite::ffi;

const NAME: &CStr = c"eventd-anchored-unix";
const PREFIX: &[u8] = b"/proc/self/fd/";

/// Install before opening stores or starting any SQLite worker.
pub fn install() -> rusqlite::Result<()> {
    static RESULT: OnceLock<c_int> = OnceLock::new();
    let result = *RESULT.get_or_init(|| {
        // SAFETY: SQLite owns this process-lifetime VFS. Finding it initializes
        // SQLite; installation occurs once and never modifies the original.
        let parent = unsafe { ffi::sqlite3_vfs_find(c"unix".as_ptr()) };
        if parent.is_null() {
            return ffi::SQLITE_NOTFOUND;
        }
        // SAFETY: the non-null registered parent is a live sqlite3_vfs.
        let mut wrapper = Box::new(unsafe { *parent });
        wrapper.zName = NAME.as_ptr();
        wrapper.pNext = std::ptr::null_mut();
        wrapper.xFullPathname = Some(full_pathname);
        // SAFETY: callbacks and name have static lifetime. The Unix callbacks
        // retain the original pAppData; SQLite fills the new registration link.
        let result = unsafe { ffi::sqlite3_vfs_register(&raw mut *wrapper, 1) };
        if result == ffi::SQLITE_OK {
            // SQLite keeps the registration pointer until process exit.
            let _ = Box::leak(wrapper);
        }
        result
    });
    if result == ffi::SQLITE_OK {
        Ok(())
    } else {
        Err(rusqlite::Error::SqliteFailure(
            ffi::Error::new(result),
            None,
        ))
    }
}

fn anchored_child(path: &[u8]) -> bool {
    let Some(tail) = path.strip_prefix(PREFIX) else {
        return false;
    };
    let Some(slash) = tail.iter().position(|byte| *byte == b'/') else {
        return false;
    };
    let (descriptor, child) = (&tail[..slash], &tail[slash + 1..]);
    !descriptor.is_empty()
        && descriptor.iter().all(u8::is_ascii_digit)
        && std::str::from_utf8(descriptor)
            .ok()
            .and_then(|text| text.parse::<c_int>().ok())
            .is_some()
        && !child.is_empty()
        && child != b"."
        && child != b".."
        && !child.contains(&b'/')
}

unsafe extern "C" fn full_pathname(
    _vfs: *mut ffi::sqlite3_vfs,
    input: *const c_char,
    capacity: c_int,
    output: *mut c_char,
) -> c_int {
    // SAFETY: SQLite supplies a NUL-terminated input and writable output of
    // capacity bytes under the xFullPathname contract.
    let path = unsafe { CStr::from_ptr(input) };
    let bytes = path.to_bytes();
    if bytes.starts_with(PREFIX) {
        if !anchored_child(bytes) || usize::try_from(capacity).unwrap_or(0) <= bytes.len() {
            return ffi::SQLITE_CANTOPEN;
        }
        // SAFETY: input and output are disjoint SQLite buffers; the capacity
        // check includes the terminating NUL. No path or symlink is resolved.
        unsafe { std::ptr::copy_nonoverlapping(input, output, bytes.len() + 1) };
        return ffi::SQLITE_OK;
    }
    // Ordinary paths keep the bundled Unix VFS's original normalization.
    // SAFETY: this built-in registration has process lifetime and is unmodified.
    let parent = unsafe { ffi::sqlite3_vfs_find(c"unix".as_ptr()) };
    if parent.is_null() {
        return ffi::SQLITE_CANTOPEN;
    }
    // SAFETY: parent is a valid VFS; arguments retain SQLite's original contract.
    unsafe {
        (*parent)
            .xFullPathname
            .map_or(ffi::SQLITE_CANTOPEN, |callback| {
                callback(parent, input, capacity, output)
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::fd::AsRawFd;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[test]
    fn only_one_child_of_a_numeric_descriptor_is_accepted() {
        assert!(anchored_child(b"/proc/self/fd/4/shard-0000.db"));
        for path in [
            b"/proc/self/fd/4/..".as_slice(),
            b"/proc/self/fd/4/.",
            b"/proc/self/fd/4/",
            b"/proc/self/fd/4/a/b",
            b"/proc/self/fd/-1/a",
            b"/proc/self/fd/2147483648/a",
            b"/proc/self/fd//a",
            b"/var/state/eventd/events/a",
        ] {
            assert!(!anchored_child(path));
        }
    }

    #[test]
    fn pathname_copy_is_bounded_and_rejects_parent_escape() {
        let mut output = [0_i8; 128];
        // SAFETY: static C strings and writable output satisfy callback contract.
        unsafe {
            assert_eq!(
                full_pathname(
                    std::ptr::null_mut(),
                    c"/proc/self/fd/4/a".as_ptr(),
                    2,
                    output.as_mut_ptr()
                ),
                ffi::SQLITE_CANTOPEN
            );
            assert_eq!(
                full_pathname(
                    std::ptr::null_mut(),
                    c"/proc/self/fd/4/../a".as_ptr(),
                    128,
                    output.as_mut_ptr()
                ),
                ffi::SQLITE_CANTOPEN
            );
        }
    }

    #[test]
    fn wal_and_readers_stay_on_the_open_directory_after_path_replacement() {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "eventd-anchor-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let original = root.join("store");
        let moved = root.join("moved");
        std::fs::create_dir_all(&original).unwrap();
        let directory = std::fs::File::open(&original).unwrap();
        let anchored = PathBuf::from(format!("/proc/self/fd/{}/store.db", directory.as_raw_fd()));
        install().unwrap();
        let writer = rusqlite::Connection::open_with_flags_and_vfs(
            &anchored,
            rusqlite::OpenFlags::default(),
            NAME.to_str().unwrap(),
        )
        .unwrap();
        writer.execute_batch("PRAGMA journal_mode=WAL; CREATE TABLE data(n INTEGER); INSERT INTO data VALUES(1);").unwrap();
        assert_eq!(writer.path(), anchored.to_str());
        std::fs::rename(&original, &moved).unwrap();
        std::fs::create_dir(&original).unwrap();
        writer.execute_batch("INSERT INTO data VALUES(2);").unwrap();
        let reader = rusqlite::Connection::open_with_flags_and_vfs(
            &anchored,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
            NAME.to_str().unwrap(),
        )
        .unwrap();
        assert_eq!(
            reader
                .query_row("SELECT SUM(n) FROM data", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            3
        );
        assert!(!original.join("store.db").exists());
        assert!(moved.join("store.db-wal").exists());
        // A final-component symlink must not redirect an anchored database.
        let link = moved.join("link.db");
        std::os::unix::fs::symlink("store.db", &link).unwrap();
        assert!(
            rusqlite::Connection::open_with_flags_and_vfs(
                anchored.with_file_name("link.db"),
                rusqlite::OpenFlags::default(),
                NAME.to_str().unwrap()
            )
            .is_err()
        );
        drop(reader);
        drop(writer);
        drop(directory);
        std::fs::remove_dir_all(root).unwrap();
    }
}
