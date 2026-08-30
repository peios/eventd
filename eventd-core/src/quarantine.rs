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
