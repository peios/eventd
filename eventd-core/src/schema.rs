//! What an existing store database holds, before its schema is verified.

use rusqlite::Connection;

/// The shape of a log, metric or shard database at open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Contents {
    /// No schema object at all: a new database, or one whose creating
    /// transaction a power cut took before it reached the disk.
    Empty,
    /// Schema objects, but no `metadata` table holding any entry. eventd
    /// creates its schema in one transaction, so it never leaves this; an
    /// interrupted creation by an earlier eventd, which created the schema a
    /// statement at a time, or a foreign database can.
    Unrecognised,
    /// A `metadata` table with entries: a store, to be verified.
    Store,
}

/// Classify a database's contents by its schema, not by whether its file
/// existed before it was opened.
pub fn contents(connection: &Connection) -> rusqlite::Result<Contents> {
    let objects: i64 =
        connection.query_row("SELECT count(*) FROM sqlite_master", [], |row| row.get(0))?;
    if objects == 0 {
        return Ok(Contents::Empty);
    }
    let metadata: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'metadata')",
        [],
        |row| row.get(0),
    )?;
    if !metadata {
        return Ok(Contents::Unrecognised);
    }
    let entries: bool =
        connection.query_row("SELECT EXISTS(SELECT 1 FROM metadata)", [], |row| {
            row.get(0)
        })?;
    Ok(if entries {
        Contents::Store
    } else {
        Contents::Unrecognised
    })
}
