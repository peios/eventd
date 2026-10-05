//! How long a store's writer connection waits for `SQLite`'s write lock.

use rusqlite::Connection;
use std::time::Duration;

/// The longest a writer waits for the write lock before its transaction
/// fails with `SQLITE_BUSY`.
///
/// eventd runs one writer per store, and read-only query connections in WAL
/// mode do not contend for the lock with it. The exception is `SQLite`'s
/// own: a reader that catches the wal-index header mid-update takes the
/// write lock for the moment it needs to read the header again
/// (`walIndexReadHdr`). A writer that begins a transaction in that moment
/// waits it out rather than failing a commit, which would stop eventd.
/// 25 milliseconds covers a reader descheduled while holding the lock on a
/// loaded host. The writer cannot wait longer, so a query still never blocks
/// it beyond this bound. A passive checkpoint does not wait at all: `SQLite`
/// runs it without the busy handler.
pub const WRITER_LOCK_WAIT: Duration = Duration::from_millis(25);

/// Give a store's writer connection its bounded wait for the write lock.
pub fn configure(connection: &Connection) -> rusqlite::Result<()> {
    connection.busy_timeout(WRITER_LOCK_WAIT)
}
