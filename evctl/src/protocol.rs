//! PSPU query framing and response decoding.

use std::collections::{BTreeMap, HashSet};
use std::io::{self, Read, Write};

use peios::msgpack::{Reader, Type, Writer};

const RESPONSE_MAX_DEPTH: u32 = peios::msgpack::DEFAULT_MAX_DEPTH.saturating_add(4);

pub type Record = BTreeMap<String, Value>;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Signed(i64),
    Unsigned(u64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
    Array(Vec<Self>),
    Map(Vec<(Self, Self)>),
    Extension(i8, Vec<u8>),
}

impl Value {
    pub fn write_message_pack(&self, writer: &mut Writer) -> Result<(), Error> {
        match self {
            Self::Null => {
                writer.write_nil();
            }
            Self::Bool(value) => {
                writer.write_bool(*value);
            }
            Self::Signed(value) => {
                writer.write_int(*value);
            }
            Self::Unsigned(value) => {
                writer.write_uint(*value);
            }
            Self::Float(value) => {
                writer.write_float(*value);
            }
            Self::String(value) => {
                writer.write_str(value);
            }
            Self::Binary(value) => {
                writer.write_bin(value);
            }
            Self::Array(values) => {
                writer.write_array(count(values.len())?);
                for value in values {
                    value.write_message_pack(writer)?;
                }
            }
            Self::Map(entries) => {
                writer.write_map(count(entries.len())?);
                for (key, value) in entries {
                    key.write_message_pack(writer)?;
                    value.write_message_pack(writer)?;
                }
            }
            Self::Extension(kind, bytes) => {
                writer.write_ext(*kind, bytes);
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Response {
    Records(Vec<Record>),
    End,
    Watch,
    Error(String),
}

pub struct Connection<S> {
    stream: S,
    frame: Vec<u8>,
}

impl<S: Read + Write> Connection<S> {
    pub const fn new(stream: S) -> Self {
        Self {
            stream,
            frame: Vec::new(),
        }
    }

    pub fn send_query(&mut self, query: &str) -> Result<(), Error> {
        let mut writer = Writer::new();
        writer.write_map(1).write_str("query").write_str(query);
        let payload = writer.to_bytes().map_err(Error::Codec)?;
        let length = count(payload.len())?;
        self.stream
            .write_all(&length.to_le_bytes())
            .map_err(Error::Io)?;
        self.stream.write_all(&payload).map_err(Error::Io)?;
        self.stream.flush().map_err(Error::Io)
    }

    pub fn next_response(&mut self) -> Result<Response, Error> {
        let mut prefix = [0_u8; 4];
        self.stream.read_exact(&mut prefix).map_err(Error::Io)?;
        let length = u32::from_le_bytes(prefix) as usize;
        self.frame.clear();
        self.frame
            .try_reserve_exact(length)
            .map_err(|_| Error::Allocation(length))?;
        self.frame.resize(length, 0);
        self.stream.read_exact(&mut self.frame).map_err(Error::Io)?;
        decode_response(&self.frame)
    }
}

fn decode_response(payload: &[u8]) -> Result<Response, Error> {
    // The response envelope adds a map, records array and record map around a
    // value that was already accepted at the platform MessagePack depth.
    peios::msgpack::validate(payload, RESPONSE_MAX_DEPTH).map_err(Error::Codec)?;
    let mut reader = Reader::new(payload);
    expect(&reader, Type::Map, "response is not a map")?;
    let field_count = reader.read_map().map_err(Error::Codec)?;
    let mut seen = HashSet::with_capacity(field_count);
    let mut status = None;
    let mut records = None;
    let mut message = None;

    for _ in 0..field_count {
        expect(&reader, Type::Str, "response key is not a string")?;
        let key = reader.read_str().map_err(Error::Codec)?;
        if !seen.insert(key) {
            return Err(Error::Malformed("duplicate response key"));
        }
        match key {
            "status" => {
                expect(&reader, Type::Str, "response status is not a string")?;
                status = Some(reader.read_str().map_err(Error::Codec)?.to_owned());
            }
            "records" => records = Some(read_records(&mut reader)?),
            "error" => {
                expect(&reader, Type::Str, "response error is not a string")?;
                message = Some(reader.read_str().map_err(Error::Codec)?.to_owned());
            }
            _ => reader.skip().map_err(Error::Codec)?,
        }
    }
    if reader.remaining() != 0 {
        return Err(Error::Malformed("trailing response data"));
    }
    match status.as_deref() {
        Some("ok") => records
            .map(Response::Records)
            .ok_or(Error::Malformed("ok response has no records")),
        Some("end") => Ok(Response::End),
        Some("watch") => Ok(Response::Watch),
        Some("error") => message
            .map(Response::Error)
            .ok_or(Error::Malformed("error response has no message")),
        Some(_) => Err(Error::Malformed("unknown response status")),
        None => Err(Error::Malformed("response status is missing")),
    }
}

fn read_records(reader: &mut Reader<'_>) -> Result<Vec<Record>, Error> {
    expect(reader, Type::Array, "response records is not an array")?;
    let count = reader.read_array().map_err(Error::Codec)?;
    let mut records = Vec::new();
    records
        .try_reserve_exact(count)
        .map_err(|_| Error::Allocation(count))?;
    for _ in 0..count {
        records.push(read_record(reader)?);
    }
    Ok(records)
}

fn read_record(reader: &mut Reader<'_>) -> Result<Record, Error> {
    expect(reader, Type::Map, "result record is not a map")?;
    let count = reader.read_map().map_err(Error::Codec)?;
    let mut record = Record::new();
    for _ in 0..count {
        expect(reader, Type::Str, "result field name is not a string")?;
        let field = reader.read_str().map_err(Error::Codec)?.to_owned();
        let value = read_value(reader)?;
        if record.insert(field, value).is_some() {
            return Err(Error::Malformed("duplicate result field"));
        }
    }
    Ok(record)
}

fn read_value(reader: &mut Reader<'_>) -> Result<Value, Error> {
    match reader.peek() {
        Some(Type::Nil) => {
            reader.read_nil().map_err(Error::Codec)?;
            Ok(Value::Null)
        }
        Some(Type::Bool) => reader.read_bool().map(Value::Bool).map_err(Error::Codec),
        Some(Type::Int) => reader.read_int().map_or_else(
            |_| {
                reader
                    .read_uint()
                    .map(Value::Unsigned)
                    .map_err(Error::Codec)
            },
            |value| Ok(Value::Signed(value)),
        ),
        Some(Type::Float) => reader.read_float().map(Value::Float).map_err(Error::Codec),
        Some(Type::Str) => reader
            .read_str()
            .map(|value| Value::String(value.to_owned()))
            .map_err(Error::Codec),
        Some(Type::Bin) => reader
            .read_bin()
            .map(|value| Value::Binary(value.to_vec()))
            .map_err(Error::Codec),
        Some(Type::Array) => {
            let count = reader.read_array().map_err(Error::Codec)?;
            let mut values = Vec::new();
            values
                .try_reserve_exact(count)
                .map_err(|_| Error::Allocation(count))?;
            for _ in 0..count {
                values.push(read_value(reader)?);
            }
            Ok(Value::Array(values))
        }
        Some(Type::Map) => {
            let count = reader.read_map().map_err(Error::Codec)?;
            let mut entries = Vec::new();
            entries
                .try_reserve_exact(count)
                .map_err(|_| Error::Allocation(count))?;
            for _ in 0..count {
                entries.push((read_value(reader)?, read_value(reader)?));
            }
            Ok(Value::Map(entries))
        }
        Some(Type::Ext) => reader
            .read_ext()
            .map(|(kind, bytes)| Value::Extension(kind, bytes.to_vec()))
            .map_err(Error::Codec),
        None => Err(Error::Malformed("truncated result value")),
    }
}

fn expect(reader: &Reader<'_>, expected: Type, message: &'static str) -> Result<(), Error> {
    if reader.peek() == Some(expected) {
        Ok(())
    } else {
        Err(Error::Malformed(message))
    }
}

fn count(length: usize) -> Result<u32, Error> {
    u32::try_from(length).map_err(|_| Error::FrameTooLarge)
}

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Codec(peios::Error),
    Malformed(&'static str),
    Allocation(usize),
    FrameTooLarge,
    UnexpectedStatus,
}

impl core::fmt::Display for Error {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Io(error) => error.fmt(formatter),
            Self::Codec(error) => error.fmt(formatter),
            Self::Malformed(message) => formatter.write_str(message),
            Self::Allocation(amount) => {
                write!(
                    formatter,
                    "cannot allocate response storage for {amount} items"
                )
            }
            Self::FrameTooLarge => formatter.write_str("query frame exceeds the u32 bound"),
            Self::UnexpectedStatus => formatter.write_str("unexpected query terminal status"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Codec(error) => Some(error),
            Self::Malformed(_)
            | Self::Allocation(_)
            | Self::FrameTooLarge
            | Self::UnexpectedStatus => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut frame = Vec::from(u32::try_from(payload.len()).unwrap().to_le_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    #[test]
    fn sends_the_normative_query_request() {
        let mut connection = Connection::new(Cursor::new(Vec::new()));
        connection.send_query("EVENTS TAKE 1").unwrap();
        let bytes = connection.stream.into_inner();
        let length = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
        let mut reader = Reader::new(&bytes[4..4 + length]);
        assert_eq!(reader.read_map().unwrap(), 1);
        assert_eq!(reader.read_str().unwrap(), "query");
        assert_eq!(reader.read_str().unwrap(), "EVENTS TAKE 1");
        assert_eq!(reader.remaining(), 0);
    }

    #[test]
    fn decodes_records_without_assuming_a_uniform_schema() {
        let mut writer = Writer::new();
        writer
            .write_map(2)
            .write_str("status")
            .write_str("ok")
            .write_str("records")
            .write_array(2)
            .write_map(1)
            .write_str("a")
            .write_uint(u64::MAX)
            .write_map(1)
            .write_str("b")
            .write_bin(&[1, 2]);
        let response = decode_response(&writer.to_bytes().unwrap()).unwrap();
        let Response::Records(records) = response else {
            panic!("expected records")
        };
        assert_eq!(records[0].get("a"), Some(&Value::Unsigned(u64::MAX)));
        assert_eq!(records[1].get("b"), Some(&Value::Binary(vec![1, 2])));
    }

    #[test]
    fn rejects_duplicate_response_keys() {
        let mut writer = Writer::new();
        writer
            .write_map(2)
            .write_str("status")
            .write_str("end")
            .write_str("status")
            .write_str("end");
        assert!(matches!(
            decode_response(&writer.to_bytes().unwrap()),
            Err(Error::Malformed("duplicate response key"))
        ));
    }

    #[test]
    fn reads_one_frame_at_a_time() {
        let mut end = Writer::new();
        end.write_map(1).write_str("status").write_str("end");
        let bytes = frame(&end.to_bytes().unwrap());
        let mut connection = Connection::new(Cursor::new(bytes));
        assert_eq!(connection.next_response().unwrap(), Response::End);
    }
}
