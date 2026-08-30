//! Deterministic payload extraction for adaptive `SQLite` expression indexes.

use rusqlite::functions::FunctionFlags;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, Result as SqlResult};

const FUNCTION: &str = "eventd_payload_key";
const MAX_DEPTH: usize = 32;
const RESERVED: [&str; 9] = [
    "timestamp",
    "cpu_id",
    "sequence",
    "origin_class",
    "event_type",
    "effective_token_guid",
    "true_token_guid",
    "process_guid",
    "boot_id",
];

/// A query literal that can safely narrow through the payload index.
#[derive(Clone, Copy)]
pub enum PayloadIndexValue<'a> {
    /// Boolean value.
    Bool(bool),
    /// UTF-8 string, compared with query-language ASCII folding.
    String(&'a str),
    /// Binary value.
    Binary(&'a [u8]),
}

/// Encode a query literal using the expression index's private key format.
#[must_use]
pub fn query_key(value: PayloadIndexValue<'_>) -> Vec<u8> {
    match value {
        PayloadIndexValue::Bool(value) => vec![1, u8::from(value)],
        PayloadIndexValue::String(value) => {
            let mut output = Vec::with_capacity(value.len() + 1);
            output.push(2);
            output.extend(value.bytes().map(fold_ascii));
            output
        }
        PayloadIndexValue::Binary(value) => {
            let mut output = Vec::with_capacity(value.len() + 1);
            output.push(3);
            output.extend_from_slice(value);
            output
        }
    }
}

/// Register the deterministic extractor required by payload index schemas.
pub fn register(connection: &Connection) -> SqlResult<()> {
    connection.create_scalar_function(
        FUNCTION,
        2,
        FunctionFlags::SQLITE_UTF8
            | FunctionFlags::SQLITE_DETERMINISTIC
            | FunctionFlags::SQLITE_INNOCUOUS,
        |context| {
            let ValueRef::Blob(payload) = context.get_raw(0) else {
                return Ok(None::<Vec<u8>>);
            };
            let Ok(field) = context.get_raw(1).as_str() else {
                return Ok(None);
            };
            Ok(extract(payload, field))
        },
    )
}

/// SQL expression used by both index creation and query planning.
#[must_use]
pub fn expression(field: &str) -> Option<String> {
    if !crate::valid_field_path(field) {
        return None;
    }
    let root = field.split('.').next()?;
    if RESERVED.contains(&root) {
        return None;
    }
    Some(format!("{FUNCTION}(payload, '{field}')"))
}

fn extract(payload: &[u8], field: &str) -> Option<Vec<u8>> {
    let segments: Vec<_> = field.split('.').collect();
    if segments.is_empty() || !crate::valid_field_path(field) || RESERVED.contains(&segments[0]) {
        return None;
    }
    let mut cursor = Cursor::new(payload);
    let found = scan_map(&mut cursor, &segments, 0, 0).ok().flatten()?;
    if cursor.position > payload.len() {
        return None;
    }
    found.0
}

struct Found(Option<Vec<u8>>);

fn scan_map(
    cursor: &mut Cursor<'_>,
    segments: &[&str],
    segment: usize,
    depth: usize,
) -> Result<Option<Found>, ()> {
    if depth >= MAX_DEPTH {
        return Err(());
    }
    let count = cursor.read_map_len()?;
    for _ in 0..count {
        let key = cursor.read_string()?;
        let matches = key.is_some_and(|key| key == segments[segment]);
        if !matches {
            cursor.skip_value(depth + 1)?;
            continue;
        }
        let is_map = cursor.peek().is_some_and(is_map_marker);
        if segment + 1 < segments.len() {
            if is_map {
                if let Some(found) = scan_map(cursor, segments, segment + 1, depth + 1)? {
                    return Ok(Some(found));
                }
            } else {
                cursor.skip_value(depth + 1)?;
            }
            continue;
        }
        if is_map {
            cursor.skip_value(depth + 1)?;
            continue;
        }
        return cursor.read_leaf_key(depth + 1).map(|key| Some(Found(key)));
    }
    Ok(None)
}

const fn fold_ascii(byte: u8) -> u8 {
    if byte.is_ascii_uppercase() {
        byte + (b'a' - b'A')
    } else {
        byte
    }
}

const fn is_map_marker(marker: u8) -> bool {
    matches!(marker, 0x80..=0x8f | 0xde | 0xdf)
}

struct Cursor<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.position).copied()
    }

    fn byte(&mut self) -> Result<u8, ()> {
        let byte = *self.bytes.get(self.position).ok_or(())?;
        self.position += 1;
        Ok(byte)
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ()> {
        let end = self.position.checked_add(length).ok_or(())?;
        let value = self.bytes.get(self.position..end).ok_or(())?;
        self.position = end;
        Ok(value)
    }

    fn length(&mut self, bytes: usize) -> Result<usize, ()> {
        let value = match bytes {
            1 => u64::from(self.byte()?),
            2 => u64::from(u16::from_be_bytes(
                self.take(2)?.try_into().map_err(|_| ())?,
            )),
            4 => u64::from(u32::from_be_bytes(
                self.take(4)?.try_into().map_err(|_| ())?,
            )),
            _ => return Err(()),
        };
        usize::try_from(value).map_err(|_| ())
    }

    fn read_map_len(&mut self) -> Result<usize, ()> {
        match self.byte()? {
            marker @ 0x80..=0x8f => Ok(usize::from(marker & 0x0f)),
            0xde => self.length(2),
            0xdf => self.length(4),
            _ => Err(()),
        }
    }

    fn read_string(&mut self) -> Result<Option<&'a str>, ()> {
        let length = match self.byte()? {
            marker @ 0xa0..=0xbf => usize::from(marker & 0x1f),
            0xd9 => self.length(1)?,
            0xda => self.length(2)?,
            0xdb => self.length(4)?,
            marker => {
                self.skip_after_marker(marker, 0)?;
                return Ok(None);
            }
        };
        Ok(core::str::from_utf8(self.take(length)?).ok())
    }

    fn read_leaf_key(&mut self, depth: usize) -> Result<Option<Vec<u8>>, ()> {
        let marker = self.byte()?;
        match marker {
            0xc0 => Ok(None),
            0xc2 => Ok(Some(query_key(PayloadIndexValue::Bool(false)))),
            0xc3 => Ok(Some(query_key(PayloadIndexValue::Bool(true)))),
            marker @ 0xa0..=0xbf => {
                let value = core::str::from_utf8(self.take(usize::from(marker & 0x1f))?).ok();
                Ok(value.map(|value| query_key(PayloadIndexValue::String(value))))
            }
            0xd9..=0xdb => {
                let length_bytes = match marker {
                    0xd9 => 1,
                    0xda => 2,
                    _ => 4,
                };
                let length = self.length(length_bytes)?;
                let value = core::str::from_utf8(self.take(length)?).ok();
                Ok(value.map(|value| query_key(PayloadIndexValue::String(value))))
            }
            0xc4..=0xc6 => {
                let length_bytes = match marker {
                    0xc4 => 1,
                    0xc5 => 2,
                    _ => 4,
                };
                let length = self.length(length_bytes)?;
                Ok(Some(query_key(PayloadIndexValue::Binary(
                    self.take(length)?,
                ))))
            }
            0xc7..=0xc9 => {
                let length_bytes = match marker {
                    0xc7 => 1,
                    0xc8 => 2,
                    _ => 4,
                };
                let length = self.length(length_bytes)?;
                self.byte()?;
                Ok(Some(query_key(PayloadIndexValue::Binary(
                    self.take(length)?,
                ))))
            }
            0xd4..=0xd8 => {
                let length = 1_usize << usize::from(marker - 0xd4);
                self.byte()?;
                Ok(Some(query_key(PayloadIndexValue::Binary(
                    self.take(length)?,
                ))))
            }
            marker => {
                self.skip_after_marker(marker, depth)?;
                Ok(None)
            }
        }
    }

    fn skip_value(&mut self, depth: usize) -> Result<(), ()> {
        if depth >= MAX_DEPTH {
            return Err(());
        }
        let marker = self.byte()?;
        self.skip_after_marker(marker, depth)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the MessagePack marker table is intentionally explicit"
    )]
    fn skip_after_marker(&mut self, marker: u8, depth: usize) -> Result<(), ()> {
        match marker {
            0x00..=0x7f | 0xc0 | 0xc2 | 0xc3 | 0xe0..=0xff => Ok(()),
            marker @ 0x80..=0x8f => self.skip_items(usize::from(marker & 0x0f) * 2, depth),
            marker @ 0x90..=0x9f => self.skip_items(usize::from(marker & 0x0f), depth),
            marker @ 0xa0..=0xbf => self.take(usize::from(marker & 0x1f)).map(|_| ()),
            0xc4 | 0xd9 => self.skip_sized(1),
            0xc5 | 0xda => self.skip_sized(2),
            0xc6 | 0xdb => self.skip_sized(4),
            0xc7 => self.skip_ext(1),
            0xc8 => self.skip_ext(2),
            0xc9 => self.skip_ext(4),
            0xca => self.take(4).map(|_| ()),
            0xcb => self.take(8).map(|_| ()),
            0xcc | 0xd0 => self.take(1).map(|_| ()),
            0xcd | 0xd1 => self.take(2).map(|_| ()),
            0xce | 0xd2 => self.take(4).map(|_| ()),
            0xcf | 0xd3 => self.take(8).map(|_| ()),
            0xd4 => self.take(2).map(|_| ()),
            0xd5 => self.take(3).map(|_| ()),
            0xd6 => self.take(5).map(|_| ()),
            0xd7 => self.take(9).map(|_| ()),
            0xd8 => self.take(17).map(|_| ()),
            0xdc => {
                let count = self.length(2)?;
                self.skip_items(count, depth)
            }
            0xdd => {
                let count = self.length(4)?;
                self.skip_items(count, depth)
            }
            0xde => {
                let count = self.length(2)?.checked_mul(2).ok_or(())?;
                self.skip_items(count, depth)
            }
            0xdf => {
                let count = self.length(4)?.checked_mul(2).ok_or(())?;
                self.skip_items(count, depth)
            }
            0xc1 => Err(()),
        }
    }

    fn skip_sized(&mut self, length_bytes: usize) -> Result<(), ()> {
        let length = self.length(length_bytes)?;
        self.take(length).map(|_| ())
    }

    fn skip_ext(&mut self, length_bytes: usize) -> Result<(), ()> {
        let length = self.length(length_bytes)?.checked_add(1).ok_or(())?;
        self.take(length).map(|_| ())
    }

    fn skip_items(&mut self, count: usize, depth: usize) -> Result<(), ()> {
        for _ in 0..count {
            self.skip_value(depth + 1)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_nested_values_with_flattening_duplicate_rules() {
        let payload = [
            0x83, 0xa6, b's', b'o', b'u', b'r', b'c', b'e', 0x81, 0xa4, b'n', b'a', b'm', b'e',
            0xa5, b'A', b'l', b'p', b'h', b'a', 0xa6, b's', b'o', b'u', b'r', b'c', b'e', 0x81,
            0xa4, b'n', b'a', b'm', b'e', 0xa4, b'b', b'e', b't', b'a', 0xa9, b't', b'i', b'm',
            b'e', b's', b't', b'a', b'm', b'p', 0xa3, b'b', b'a', b'd',
        ];
        assert_eq!(
            extract(&payload, "source.name"),
            Some(query_key(PayloadIndexValue::String("alpha")))
        );
        assert_eq!(extract(&payload, "timestamp"), None);
    }

    #[test]
    fn registered_function_drives_an_expression_index() {
        let connection = Connection::open_in_memory().unwrap();
        register(&connection).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE events(payload BLOB);\
                 CREATE INDEX payload_name ON events(eventd_payload_key(payload, 'name'));",
            )
            .unwrap();
        connection
            .execute(
                "INSERT INTO events VALUES (?1)",
                [[0x81, 0xa4, b'n', b'a', b'm', b'e', 0xa1, b'X']],
            )
            .unwrap();
        let key = query_key(PayloadIndexValue::String("x"));
        let count: u32 = connection
            .query_row(
                "SELECT count(*) FROM events WHERE eventd_payload_key(payload, 'name') = ?1",
                [&key],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }
}
