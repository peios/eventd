//! Collision-safe quarantine of a `SQLite` database and its sidecars.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// Rename a database, WAL and shared-memory file under one unique suffix.
pub fn database(path: &Path) -> Result<PathBuf, std::io::Error> {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(std::io::Error::other)?
        .as_nanos();
    database_at(path, timestamp)
}

/// Quarantine under the suffix for `timestamp`, nanoseconds since the epoch.
fn database_at(path: &Path, timestamp: u128) -> Result<PathBuf, std::io::Error> {
    let sources = [
        path.to_owned(),
        sidecar(path, "-wal"),
        sidecar(path, "-shm"),
    ];
    let present = sources
        .into_iter()
        .filter(|source| source.try_exists().unwrap_or(true))
        .collect::<Vec<_>>();
    if present.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("database {} disappeared before quarantine", path.display()),
        ));
    }

    let mut collision = 0_u32;
    let (suffix, targets) = loop {
        let suffix = if collision == 0 {
            format!(".corrupt.{timestamp}")
        } else {
            format!(".corrupt.{timestamp}.{collision}")
        };
        let targets = present
            .iter()
            .map(|source| append(source, &suffix))
            .collect::<Vec<_>>();
        if targets
            .iter()
            .all(|target| !target.try_exists().unwrap_or(true))
        {
            break (suffix, targets);
        }
        collision = collision.checked_add(1).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "quarantine suffix space exhausted",
            )
        })?;
    };

    let mut renamed = Vec::with_capacity(present.len());
    for (source, target) in present.iter().zip(&targets) {
        if let Err(error) = std::fs::rename(source, target) {
            for (original, quarantined) in renamed.iter().rev() {
                let _ = std::fs::rename(quarantined, original);
            }
            return Err(error);
        }
        renamed.push((source, target));
    }
    Ok(append(path, &suffix))
}

/// Byte-for-byte copies of a database's `-wal` and `-shm`, taken before any
/// connection opens it.
///
/// Opening runs `SQLite` against the sidecars: it rebuilds the `-shm`, and
/// when the failed connection closes it deletes both. Quarantine after a
/// failed open therefore moves these copies, not whatever the attempt left.
/// Dropping the value discards the copies.
pub struct Preserved {
    path: PathBuf,
    sidecars: Vec<(PathBuf, Saved)>,
    failure: Option<String>,
}

enum Saved {
    /// The sidecar did not exist before the open.
    Absent,
    /// The sidecar's contents as they were before the open.
    Taken(PathBuf),
    /// Copying failed; the sidecar is quarantined as the open left it.
    Failed,
}

impl Preserved {
    /// Copy whichever sidecars exist. Best effort: a copy that fails, on a
    /// full disk say, must not itself stop the store opening, so it is
    /// recorded and reported only if the store turns out to need quarantine.
    pub fn take(path: &Path) -> Self {
        let mut failure = None;
        let sidecars = ["-wal", "-shm"]
            .into_iter()
            .map(|suffix| {
                let source = sidecar(path, suffix);
                let copy = append(&source, ".preserved");
                let state = match std::fs::copy(&source, &copy) {
                    Ok(_) => Saved::Taken(copy),
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => Saved::Absent,
                    Err(error) => {
                        let _ = std::fs::remove_file(&copy);
                        failure = Some(format!(
                            "{} could not be preserved before opening: {error}",
                            source.display()
                        ));
                        Saved::Failed
                    }
                };
                (source, state)
            })
            .collect();
        Self {
            path: path.to_owned(),
            sidecars,
            failure,
        }
    }

    /// Put the sidecars back as they were before the open, then quarantine
    /// all three. Returns a note when a sidecar could not be preserved.
    pub fn quarantine(mut self) -> Result<Option<String>, std::io::Error> {
        for (source, state) in &mut self.sidecars {
            match std::mem::replace(state, Saved::Absent) {
                Saved::Taken(copy) => std::fs::rename(copy, &*source)?,
                // Anything there now was made by the failed open.
                Saved::Absent => match std::fs::remove_file(&*source) {
                    Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                        return Err(error);
                    }
                    _ => {}
                },
                Saved::Failed => {}
            }
        }
        database(&self.path)?;
        Ok(self.failure.take())
    }
}

/// The recovery reason for a quarantine, with any preservation note.
pub fn reason(error: &dyn std::fmt::Display, note: Option<String>) -> String {
    note.map_or_else(|| error.to_string(), |note| format!("{error} ({note})"))
}

impl Drop for Preserved {
    fn drop(&mut self) {
        for (_, state) in &self.sidecars {
            if let Saved::Taken(copy) = state {
                let _ = std::fs::remove_file(copy);
            }
        }
    }
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    append(path, suffix)
}

fn append(path: &Path, suffix: &str) -> PathBuf {
    let mut name: OsString = path.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn moves_database_and_sidecars_under_one_suffix() {
        let directory = temporary_directory();
        let path = directory.join("events.db");
        std::fs::write(&path, b"db").unwrap();
        std::fs::write(sidecar(&path, "-wal"), b"wal").unwrap();
        std::fs::write(sidecar(&path, "-shm"), b"shm").unwrap();
        let quarantined = database(&path).unwrap();
        let suffix = quarantined
            .as_os_str()
            .to_string_lossy()
            .strip_prefix(path.as_os_str().to_string_lossy().as_ref())
            .unwrap()
            .to_owned();
        assert!(!path.exists());
        assert_eq!(std::fs::read(quarantined).unwrap(), b"db");
        assert_eq!(
            std::fs::read(append(&sidecar(&path, "-wal"), &suffix)).unwrap(),
            b"wal"
        );
        assert_eq!(
            std::fs::read(append(&sidecar(&path, "-shm"), &suffix)).unwrap(),
            b"shm"
        );
        std::fs::remove_dir_all(directory).unwrap();
    }

    #[test]
    fn a_taken_quarantine_name_gets_the_lowest_free_positive_suffix() {
        let directory = temporary_directory();
        let path = directory.join("shard-0000.db");
        let timestamp = 1_700_000_000_123_456_789_u128;
        let base = format!(".corrupt.{timestamp}");
        std::fs::write(&path, b"db").unwrap();
        std::fs::write(sidecar(&path, "-wal"), b"wal").unwrap();
        // The bare name and .1 are taken; .3 is taken too, so the lowest
        // free suffix is .2, not one past the highest.
        for taken in [base.clone(), format!("{base}.1"), format!("{base}.3")] {
            std::fs::write(append(&path, &taken), b"earlier").unwrap();
        }

        let quarantined = database_at(&path, timestamp).unwrap();
        let expected = format!("{base}.2");
        assert_eq!(quarantined, append(&path, &expected));
        assert_eq!(std::fs::read(&quarantined).unwrap(), b"db");
        assert_eq!(
            std::fs::read(append(&sidecar(&path, "-wal"), &expected)).unwrap(),
            b"wal"
        );
        for taken in [base.clone(), format!("{base}.1"), format!("{base}.3")] {
            assert_eq!(std::fs::read(append(&path, &taken)).unwrap(), b"earlier");
        }

        // A sidecar's target being taken moves all three, not only it.
        std::fs::write(&path, b"db2").unwrap();
        std::fs::write(sidecar(&path, "-wal"), b"wal2").unwrap();
        let later = timestamp + 1;
        let later_base = format!(".corrupt.{later}");
        std::fs::write(append(&sidecar(&path, "-wal"), &later_base), b"earlier").unwrap();
        let quarantined = database_at(&path, later).unwrap();
        let expected = format!("{later_base}.1");
        assert_eq!(quarantined, append(&path, &expected));
        assert_eq!(
            std::fs::read(append(&sidecar(&path, "-wal"), &expected)).unwrap(),
            b"wal2"
        );
        assert!(!append(&path, &later_base).exists());
        std::fs::remove_dir_all(directory).unwrap();
    }

    fn temporary_directory() -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "eventd-quarantine-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        path
    }
}
