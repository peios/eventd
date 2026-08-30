//! Loss-aware rendering with transactional initial-result publication.

use std::ffi::CString;
use std::fs::File;
use std::io::{self, Seek, SeekFrom, Write};
use std::os::fd::{FromRawFd, RawFd};
use std::path::Path;

use peios::msgpack::Writer;

use crate::protocol::{Record, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Pretty,
    JsonLines,
    MessagePack,
}

impl Format {
    pub fn parse(value: &std::ffi::OsStr) -> Option<Self> {
        match value.to_str()? {
            "pretty" => Some(Self::Pretty),
            "jsonl" => Some(Self::JsonLines),
            "msgpack" => Some(Self::MessagePack),
            _ => None,
        }
    }
}

pub struct TransactionalOutput<W> {
    destination: W,
    spool: File,
    format: Format,
    terminal: bool,
    committed: bool,
}

impl<W: Write> TransactionalOutput<W> {
    pub fn new(destination: W, format: Format, terminal: bool) -> io::Result<Self> {
        Ok(Self {
            destination,
            spool: anonymous_spool(&std::env::temp_dir())?,
            format,
            terminal,
            committed: false,
        })
    }

    pub fn write_record(&mut self, record: &Record) -> io::Result<()> {
        let format = self.format;
        let terminal = self.terminal;
        if self.committed {
            write_record(&mut self.destination, record, format, terminal)
        } else {
            write_record(&mut self.spool, record, format, terminal)
        }
    }

    pub fn commit(&mut self) -> io::Result<()> {
        if self.committed {
            return Ok(());
        }
        self.spool.seek(SeekFrom::Start(0))?;
        io::copy(&mut self.spool, &mut self.destination)?;
        self.destination.flush()?;
        self.committed = true;
        Ok(())
    }
}

fn anonymous_spool(directory: &Path) -> io::Result<File> {
    let directory = CString::new(directory.as_os_str().as_encoded_bytes())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "temporary path contains NUL"))?;
    // SAFETY: directory is a live NUL-terminated path. O_TMPFILE returns a new
    // anonymous inode or an error; a successful fd is owned exactly once below.
    let descriptor = unsafe {
        libc::open(
            directory.as_ptr(),
            libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC,
            0o600,
        )
    };
    if descriptor < 0 {
        Err(io::Error::last_os_error())
    } else {
        // SAFETY: open returned a fresh owned descriptor.
        Ok(unsafe { File::from_raw_fd(descriptor as RawFd) })
    }
}

fn write_record(
    output: &mut impl Write,
    record: &Record,
    format: Format,
    terminal: bool,
) -> io::Result<()> {
    match format {
        Format::Pretty => write_pretty(output, record, terminal),
        Format::JsonLines => {
            write_json_record(output, record)?;
            output.write_all(b"\n")
        }
        Format::MessagePack => write_message_pack(output, record),
    }
}

fn write_pretty(output: &mut impl Write, record: &Record, terminal: bool) -> io::Result<()> {
    for (index, (field, value)) in record.iter().enumerate() {
        if index != 0 {
            output.write_all(b"  ")?;
        }
        if terminal {
            output.write_all(b"\x1b[1m")?;
        }
        output.write_all(field.as_bytes())?;
        if terminal {
            output.write_all(b"\x1b[0m")?;
        }
        output.write_all(b"=")?;
        write_json_value(output, value)?;
    }
    output.write_all(b"\n")
}

fn write_json_record(output: &mut impl Write, record: &Record) -> io::Result<()> {
    output.write_all(b"{")?;
    for (index, (field, value)) in record.iter().enumerate() {
        if index != 0 {
            output.write_all(b",")?;
        }
        write_json_string(output, field)?;
        output.write_all(b":")?;
        write_json_value(output, value)?;
    }
    output.write_all(b"}")
}

#[allow(
    clippy::too_many_lines,
    reason = "the exhaustive value rendering keeps JSON's tagged-lossless rules together"
)]
fn write_json_value(output: &mut impl Write, value: &Value) -> io::Result<()> {
    match value {
        Value::Null => output.write_all(b"null"),
        Value::Bool(value) => write!(output, "{value}"),
        Value::Signed(value) => write!(output, "{value}"),
        Value::Unsigned(value) => write!(output, "{value}"),
        Value::Float(value) if value.is_finite() => write!(output, "{value}"),
        Value::Float(value) => {
            output.write_all(b"{\"$float\":")?;
            let name = if value.is_nan() {
                "nan"
            } else if value.is_sign_positive() {
                "+inf"
            } else {
                "-inf"
            };
            write_json_string(output, name)?;
            output.write_all(b"}")
        }
        Value::String(value) => write_json_string(output, value),
        Value::Binary(value) => {
            output.write_all(b"{\"$binary\":\"")?;
            write_hex(output, value)?;
            output.write_all(b"\"}")
        }
        Value::Array(values) => {
            output.write_all(b"[")?;
            for (index, value) in values.iter().enumerate() {
                if index != 0 {
                    output.write_all(b",")?;
                }
                write_json_value(output, value)?;
            }
            output.write_all(b"]")
        }
        Value::Map(entries) if string_map(entries) => {
            output.write_all(b"{")?;
            for (index, (key, value)) in entries.iter().enumerate() {
                if index != 0 {
                    output.write_all(b",")?;
                }
                let Value::String(key) = key else {
                    unreachable!("string_map checked every key")
                };
                write_json_string(output, key)?;
                output.write_all(b":")?;
                write_json_value(output, value)?;
            }
            output.write_all(b"}")
        }
        Value::Map(entries) => {
            output.write_all(b"{\"$map\":[")?;
            for (index, (key, value)) in entries.iter().enumerate() {
                if index != 0 {
                    output.write_all(b",")?;
                }
                output.write_all(b"[")?;
                write_json_value(output, key)?;
                output.write_all(b",")?;
                write_json_value(output, value)?;
                output.write_all(b"]")?;
            }
            output.write_all(b"]}")
        }
        Value::Extension(kind, bytes) => {
            write!(output, "{{\"$extension\":{{\"type\":{kind},\"data\":\"")?;
            write_hex(output, bytes)?;
            output.write_all(b"\"}}}")
        }
    }
}

fn string_map(entries: &[(Value, Value)]) -> bool {
    let mut keys = std::collections::HashSet::with_capacity(entries.len());
    entries.iter().all(|(key, _)| {
        let Value::String(key) = key else {
            return false;
        };
        keys.insert(key)
    })
}

fn write_json_string(output: &mut impl Write, value: &str) -> io::Result<()> {
    output.write_all(b"\"")?;
    for character in value.chars() {
        match character {
            '"' => output.write_all(b"\\\"")?,
            '\\' => output.write_all(b"\\\\")?,
            '\u{08}' => output.write_all(b"\\b")?,
            '\u{0c}' => output.write_all(b"\\f")?,
            '\n' => output.write_all(b"\\n")?,
            '\r' => output.write_all(b"\\r")?,
            '\t' => output.write_all(b"\\t")?,
            character if character <= '\u{1f}' => write!(output, "\\u{:04x}", character as u32)?,
            character => {
                let mut bytes = [0_u8; 4];
                output.write_all(character.encode_utf8(&mut bytes).as_bytes())?;
            }
        }
    }
    output.write_all(b"\"")
}

fn write_hex(output: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        output.write_all(&[HEX[usize::from(byte >> 4)], HEX[usize::from(byte & 0x0f)]])?;
    }
    Ok(())
}

fn write_message_pack(output: &mut impl Write, record: &Record) -> io::Result<()> {
    let mut writer = Writer::new();
    writer.write_map(
        u32::try_from(record.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "record has too many fields")
        })?,
    );
    for (field, value) in record {
        writer.write_str(field);
        value
            .write_message_pack(&mut writer)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    }
    let bytes = writer
        .to_bytes()
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    let length = u32::try_from(bytes.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "record exceeds u32 framing"))?;
    output.write_all(&length.to_le_bytes())?;
    output.write_all(&bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record() -> Record {
        [
            ("binary".into(), Value::Binary(vec![0, 255])),
            ("line".into(), Value::String("a\nb".into())),
            ("wide".into(), Value::Unsigned(u64::MAX)),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn json_lines_is_lossless_for_non_json_values() {
        let mut output = Vec::new();
        write_record(&mut output, &record(), Format::JsonLines, false).unwrap();
        assert_eq!(
            String::from_utf8(output).unwrap(),
            "{\"binary\":{\"$binary\":\"00ff\"},\"line\":\"a\\nb\",\"wide\":18446744073709551615}\n"
        );
    }

    #[test]
    fn initial_output_is_invisible_until_committed() {
        let mut destination = Vec::new();
        {
            let mut output =
                TransactionalOutput::new(&mut destination, Format::JsonLines, false).unwrap();
            output.write_record(&record()).unwrap();
            assert!(output.destination.is_empty());
            output.commit().unwrap();
        }
        assert!(!destination.is_empty());
    }

    #[test]
    fn dropping_uncommitted_output_discards_it() {
        let mut destination = Vec::new();
        {
            let mut output =
                TransactionalOutput::new(&mut destination, Format::JsonLines, false).unwrap();
            output.write_record(&record()).unwrap();
        }
        assert!(destination.is_empty());
    }

    #[test]
    fn message_pack_records_are_individually_framed() {
        let mut output = Vec::new();
        write_record(&mut output, &record(), Format::MessagePack, false).unwrap();
        let length = u32::from_le_bytes(output[..4].try_into().unwrap()) as usize;
        assert_eq!(length, output.len() - 4);
        peios::msgpack::validate(&output[4..], peios::msgpack::DEFAULT_MAX_DEPTH).unwrap();
    }
}
