//! Validation, canonicalisation and adaptive ingestion of metric datagrams.

use core::fmt;
use core::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use eventd_core::{
    Histogram, MetricRecord, MetricStore, MetricStoreError, MetricType, MetricValue,
};
use peios::msgpack::{Reader, Type};

use crate::datagram::{IngestionSocket, Receive, SocketError};

pub fn run(
    socket: &IngestionSocket,
    mut store: MetricStore,
    boot_id: [u8; 16],
    datagram_ceiling: usize,
    max_batch_size: usize,
    max_batch_latency: Duration,
    stopping: &Arc<AtomicBool>,
) -> Result<(), MetricIngestError> {
    let mut buffer = vec![0_u8; datagram_ceiling];
    let mut batch = Vec::with_capacity(max_batch_size);
    let mut started = None;
    while !stopping.load(Ordering::Acquire) {
        match socket.receive(&mut buffer)? {
            Receive::Datagram(length) => {
                let receipt_timestamp = realtime_nanoseconds()?;
                let Some(records) = parse_datagram(&buffer[..length], boot_id, receipt_timestamp)
                else {
                    continue;
                };
                for record in records {
                    started.get_or_insert_with(Instant::now);
                    batch.push(record);
                    if batch.len() == max_batch_size
                        || started.is_some_and(|time| time.elapsed() >= max_batch_latency)
                    {
                        store.commit(&batch)?;
                        batch.clear();
                        started = None;
                    }
                }
            }
            Receive::Truncated => {}
            Receive::Empty if batch.is_empty() => socket.wait_readable(1_000)?,
            Receive::Empty => {
                store.commit(&batch)?;
                batch.clear();
                started = None;
            }
        }
    }
    loop {
        match socket.receive(&mut buffer)? {
            Receive::Datagram(length) => {
                let receipt_timestamp = realtime_nanoseconds()?;
                let Some(records) = parse_datagram(&buffer[..length], boot_id, receipt_timestamp)
                else {
                    continue;
                };
                for record in records {
                    batch.push(record);
                    if batch.len() == max_batch_size {
                        store.commit(&batch)?;
                        batch.clear();
                    }
                }
            }
            Receive::Truncated => {}
            Receive::Empty => break,
        }
    }
    if !batch.is_empty() {
        store.commit(&batch)?;
    }
    Ok(())
}

pub fn parse_datagram(
    bytes: &[u8],
    boot_id: [u8; 16],
    receipt_timestamp: i64,
) -> Option<Vec<MetricRecord>> {
    peios::msgpack::validate(bytes, peios::msgpack::DEFAULT_MAX_DEPTH).ok()?;
    let mut reader = Reader::new(bytes);
    let records = match reader.peek()? {
        Type::Map => parse_record(&mut reader, boot_id, receipt_timestamp)
            .into_iter()
            .collect(),
        Type::Array => {
            let count = reader.read_array().ok()?;
            let mut records = Vec::new();
            for _ in 0..count {
                if reader.peek() != Some(Type::Map) {
                    return None;
                }
                if let Some(record) = parse_record(&mut reader, boot_id, receipt_timestamp) {
                    records.push(record);
                }
            }
            records
        }
        _ => return None,
    };
    (reader.remaining() == 0).then_some(records)
}

fn parse_record(
    reader: &mut Reader<'_>,
    boot_id: [u8; 16],
    receipt_timestamp: i64,
) -> Option<MetricRecord> {
    let field_count = reader.read_map().ok()?;
    let mut seen = HashSet::with_capacity(field_count.min(16));
    let mut name = None;
    let mut labels: Option<Box<str>> = None;
    let mut metric_type = None;
    let mut timestamp = None;
    let mut timestamp_present = false;
    let mut value = None;
    let mut valid = true;
    for _ in 0..field_count {
        if reader.peek() != Some(Type::Str) {
            reader.skip().ok()?;
            reader.skip().ok()?;
            valid = false;
            continue;
        }
        let key = reader.read_str().ok()?;
        if !seen.insert(key) {
            valid = false;
        }
        match key {
            "name" if reader.peek() == Some(Type::Str) => {
                name = Some(reader.read_str().ok()?);
            }
            "labels" if reader.peek() == Some(Type::Map) => {
                labels = parse_labels(reader);
                valid &= labels.is_some();
            }
            "type" if reader.peek() == Some(Type::Str) => {
                metric_type = match reader.read_str().ok()? {
                    "counter" => Some(MetricType::Counter),
                    "gauge" => Some(MetricType::Gauge),
                    "histogram" => Some(MetricType::Histogram),
                    _ => None,
                };
            }
            "timestamp" if reader.peek() == Some(Type::Int) => {
                timestamp_present = true;
                timestamp = read_timestamp(reader);
                valid &= timestamp.is_some();
            }
            "timestamp" => {
                timestamp_present = true;
                reader.skip().ok()?;
                valid = false;
            }
            "value" => {
                value = parse_wire_value(reader);
                valid &= value.is_some();
            }
            _ => {
                reader.skip().ok()?;
            }
        }
    }
    let name = name.filter(|value| valid_identifier(value))?;
    let metric_type = metric_type?;
    let value = value?;
    let value = match (metric_type, value) {
        (MetricType::Counter, WireValue::Number(number)) if number >= 0.0 => {
            MetricValue::Number(number)
        }
        (MetricType::Gauge, WireValue::Number(number)) => MetricValue::Number(number),
        (MetricType::Histogram, WireValue::Histogram(histogram)) => {
            MetricValue::Histogram(histogram)
        }
        _ => return None,
    };
    valid.then(|| MetricRecord {
        boot_id,
        timestamp: if timestamp_present {
            timestamp.expect("valid timestamp checked")
        } else {
            receipt_timestamp
        },
        name: name.into(),
        labels: labels.unwrap_or_default(),
        metric_type,
        value,
    })
}

fn parse_labels(reader: &mut Reader<'_>) -> Option<Box<str>> {
    let count = reader.read_map().ok()?;
    let mut labels = Vec::with_capacity(count);
    let mut keys = HashSet::with_capacity(count);
    let mut valid = true;
    for _ in 0..count {
        if reader.peek() != Some(Type::Str) {
            reader.skip().ok()?;
            reader.skip().ok()?;
            valid = false;
            continue;
        }
        let key = reader.read_str().ok()?;
        if reader.peek() != Some(Type::Str) {
            reader.skip().ok()?;
            valid = false;
            continue;
        }
        let value = reader.read_str().ok()?;
        valid &= keys.insert(key)
            && valid_label_key(key)
            && !value.is_empty()
            && !value.bytes().any(|byte| matches!(byte, b'=' | b','));
        labels.push((key, value));
    }
    if !valid {
        return None;
    }
    labels.sort_unstable_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));
    let capacity = labels
        .iter()
        .map(|(key, value)| key.len() + value.len() + 1)
        .sum::<usize>()
        + labels.len().saturating_sub(1);
    let mut canonical = String::with_capacity(capacity);
    for (index, (key, value)) in labels.into_iter().enumerate() {
        if index != 0 {
            canonical.push(',');
        }
        canonical.push_str(key);
        canonical.push('=');
        canonical.push_str(value);
    }
    Some(canonical.into_boxed_str())
}

enum WireValue {
    Number(f64),
    Histogram(Histogram),
}

fn parse_wire_value(reader: &mut Reader<'_>) -> Option<WireValue> {
    match reader.peek()? {
        Type::Int | Type::Float => read_number(reader).map(WireValue::Number),
        Type::Map => parse_histogram(reader).map(WireValue::Histogram),
        _ => {
            reader.skip().ok()?;
            None
        }
    }
}

fn parse_histogram(reader: &mut Reader<'_>) -> Option<Histogram> {
    let field_count = reader.read_map().ok()?;
    let mut seen = HashSet::with_capacity(field_count.min(8));
    let mut boundaries = None;
    let mut counts = None;
    let mut total_count = None;
    let mut sum = None;
    let mut valid = true;
    for _ in 0..field_count {
        if reader.peek() != Some(Type::Str) {
            reader.skip().ok()?;
            reader.skip().ok()?;
            valid = false;
            continue;
        }
        let key = reader.read_str().ok()?;
        valid &= seen.insert(key);
        match key {
            "boundaries" if reader.peek() == Some(Type::Array) => {
                boundaries = read_number_array(reader);
                valid &= boundaries.is_some();
            }
            "counts" if reader.peek() == Some(Type::Array) => {
                counts = read_count_array(reader);
                valid &= counts.is_some();
            }
            "total_count" if reader.peek() == Some(Type::Int) => {
                total_count = read_nonnegative_integer(reader);
                valid &= total_count.is_some();
            }
            "sum" if matches!(reader.peek(), Some(Type::Int | Type::Float)) => {
                sum = read_number(reader);
                valid &= sum.is_some();
            }
            _ => {
                reader.skip().ok()?;
            }
        }
    }
    let boundaries = boundaries?;
    let counts = counts?;
    let total_count = total_count?;
    let sum = sum?;
    valid &= !boundaries.is_empty()
        && boundaries.len() == counts.len()
        && boundaries.windows(2).all(|pair| pair[0] < pair[1])
        && counts.windows(2).all(|pair| pair[0] <= pair[1])
        && counts.iter().all(|count| *count <= total_count)
        && (total_count != 0 || (sum == 0.0 && counts.iter().all(|count| *count == 0)));
    valid.then(|| Histogram {
        boundaries: boundaries.into_boxed_slice(),
        counts: counts.into_boxed_slice(),
        total_count,
        sum,
    })
}

fn read_number_array(reader: &mut Reader<'_>) -> Option<Vec<f64>> {
    let count = reader.read_array().ok()?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(read_number(reader)?);
    }
    Some(values)
}

fn read_count_array(reader: &mut Reader<'_>) -> Option<Vec<u64>> {
    let count = reader.read_array().ok()?;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        values.push(read_nonnegative_integer(reader)?);
    }
    Some(values)
}

fn read_number(reader: &mut Reader<'_>) -> Option<f64> {
    let value = match reader.peek()? {
        Type::Float => reader.read_float().ok()?,
        Type::Int => {
            if let Ok(value) = reader.read_int() {
                #[allow(clippy::cast_precision_loss)]
                let converted = value as f64;
                converted
            } else {
                #[allow(clippy::cast_precision_loss)]
                let converted = reader.read_uint().ok()? as f64;
                converted
            }
        }
        _ => return None,
    };
    value.is_finite().then_some(value)
}

fn read_nonnegative_integer(reader: &mut Reader<'_>) -> Option<u64> {
    if let Ok(value) = reader.read_uint() {
        return Some(value);
    }
    reader
        .read_int()
        .ok()
        .and_then(|value| u64::try_from(value).ok())
}

fn read_timestamp(reader: &mut Reader<'_>) -> Option<i64> {
    if let Ok(value) = reader.read_int() {
        return (value >= 0).then_some(value);
    }
    reader
        .read_uint()
        .ok()
        .and_then(|value| i64::try_from(value).ok())
}

fn valid_identifier(value: &str) -> bool {
    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || first == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b'-'))
}

fn valid_label_key(value: &str) -> bool {
    valid_identifier(value) && !matches!(value, "timestamp" | "boot_id" | "name" | "type" | "value")
}

fn realtime_nanoseconds() -> Result<i64, MetricIngestError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| MetricIngestError::Clock)?;
    i64::try_from(elapsed.as_nanos()).map_err(|_| MetricIngestError::Clock)
}

#[derive(Debug)]
pub enum MetricIngestError {
    Socket(SocketError),
    Store(MetricStoreError),
    Clock,
}

impl fmt::Display for MetricIngestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Socket(error) => write!(formatter, "{error}"),
            Self::Store(error) => write!(formatter, "{error}"),
            Self::Clock => formatter.write_str("realtime clock is outside the timestamp domain"),
        }
    }
}

impl std::error::Error for MetricIngestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Socket(error) => Some(error),
            Self::Store(error) => Some(error),
            Self::Clock => None,
        }
    }
}

impl From<SocketError> for MetricIngestError {
    fn from(error: SocketError) -> Self {
        Self::Socket(error)
    }
}

impl From<MetricStoreError> for MetricIngestError {
    fn from(error: MetricStoreError) -> Self {
        Self::Store(error)
    }
}

#[cfg(test)]
mod tests {
    use peios::msgpack::Writer;

    use super::*;

    #[test]
    fn canonicalises_labels_independently_of_wire_order() {
        let mut writer = Writer::new();
        writer
            .write_map(4)
            .write_str("name")
            .write_str("cpu.usage")
            .write_str("labels")
            .write_map(2)
            .write_str("host")
            .write_str("server1")
            .write_str("core")
            .write_str("0")
            .write_str("type")
            .write_str("gauge")
            .write_str("value")
            .write_float(0.5);
        let records = parse_datagram(&writer.to_bytes().unwrap(), [1; 16], 7).unwrap();
        assert_eq!(records[0].labels.as_ref(), "core=0,host=server1");
        assert_eq!(records[0].timestamp, 7);
    }

    #[test]
    fn negative_counter_is_silently_discarded() {
        let mut writer = Writer::new();
        writer
            .write_map(3)
            .write_str("name")
            .write_str("requests.total")
            .write_str("type")
            .write_str("counter")
            .write_str("value")
            .write_float(-1.0);
        assert!(
            parse_datagram(&writer.to_bytes().unwrap(), [1; 16], 7)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn validates_a_histogram() {
        let mut writer = Writer::new();
        writer
            .write_map(3)
            .write_str("name")
            .write_str("request.duration")
            .write_str("type")
            .write_str("histogram")
            .write_str("value")
            .write_map(4)
            .write_str("boundaries")
            .write_array(2)
            .write_float(0.1)
            .write_float(1.0)
            .write_str("counts")
            .write_array(2)
            .write_uint(4)
            .write_uint(5)
            .write_str("total_count")
            .write_uint(6)
            .write_str("sum")
            .write_float(2.5);
        let records = parse_datagram(&writer.to_bytes().unwrap(), [1; 16], 7).unwrap();
        assert!(matches!(records[0].value, MetricValue::Histogram(_)));
    }
}
