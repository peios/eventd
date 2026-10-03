//! Query-language values, comparison, `MessagePack` encoding and payload flattening.

use core::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

use peios::msgpack::{Reader, Type, Writer};

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
}

pub type Record = BTreeMap<String, Value>;

impl Value {
    pub fn write(&self, writer: &mut Writer) {
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
                writer.write_array(u32::try_from(values.len()).expect("stored array fits u32"));
                for value in values {
                    value.write(writer);
                }
            }
            Self::Map(entries) => {
                writer.write_map(u32::try_from(entries.len()).expect("stored map fits u32"));
                for (key, value) in entries {
                    key.write(writer);
                    value.write(writer);
                }
            }
        }
    }

    pub fn language_equal(&self, other: &Self) -> bool {
        if let Some(ordering) = numeric_cmp(self, other) {
            return ordering == Ordering::Equal;
        }
        match (self, other) {
            (Self::Null, Self::Null) => true,
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::String(left), Self::String(right)) => ascii_equal(left, right),
            (Self::Binary(left), Self::Binary(right)) => left == right,
            (Self::Array(left), Self::Array(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(left, right)| left.language_equal(right))
            }
            (Self::Map(left), Self::Map(right)) => {
                left.len() == right.len()
                    && left.iter().zip(right).all(
                        |((left_key, left_value), (right_key, right_value))| {
                            left_key.language_equal(right_key)
                                && left_value.language_equal(right_value)
                        },
                    )
            }
            _ => false,
        }
    }
}

pub fn language_cmp(left: &Value, right: &Value) -> Ordering {
    if let Some(ordering) = numeric_cmp(left, right) {
        return ordering;
    }
    let left_kind = kind(left);
    let right_kind = kind(right);
    if left_kind != right_kind {
        return left_kind.cmp(&right_kind);
    }
    match (left, right) {
        (Value::Bool(left), Value::Bool(right)) => left.cmp(right),
        (Value::String(left), Value::String(right)) => ascii_cmp(left, right),
        (Value::Binary(left), Value::Binary(right)) => left.cmp(right),
        (Value::Array(left), Value::Array(right)) => compare_arrays(left, right),
        (Value::Map(left), Value::Map(right)) => format!("{left:?}").cmp(&format!("{right:?}")),
        _ => Ordering::Equal,
    }
}

const fn kind(value: &Value) -> u8 {
    match value {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Signed(_) | Value::Unsigned(_) | Value::Float(_) => 2,
        Value::String(_) => 3,
        Value::Binary(_) => 4,
        Value::Array(_) => 5,
        Value::Map(_) => 6,
    }
}

/// Whether `value` is a NaN, which a payload may carry (PSPU §3.5) and a
/// literal may not (§3.19).
pub const fn is_nan(value: &Value) -> bool {
    matches!(value, Value::Float(number) if number.is_nan())
}

const fn is_number(value: &Value) -> bool {
    matches!(
        value,
        Value::Signed(_) | Value::Unsigned(_) | Value::Float(_)
    )
}

fn numeric_cmp(left: &Value, right: &Value) -> Option<Ordering> {
    // Every NaN is one value, after every other number, the infinities
    // included (PSPU §3.21), so that ordering stays total and all NaNs
    // group together.
    if is_nan(left) || is_nan(right) {
        return (is_number(left) && is_number(right)).then(|| is_nan(left).cmp(&is_nan(right)));
    }
    match (left, right) {
        (Value::Signed(left), Value::Signed(right)) => Some(left.cmp(right)),
        (Value::Unsigned(left), Value::Unsigned(right)) => Some(left.cmp(right)),
        (Value::Signed(left), Value::Unsigned(right)) => Some(compare_i64_u64(*left, *right)),
        (Value::Unsigned(left), Value::Signed(right)) => {
            Some(compare_i64_u64(*right, *left).reverse())
        }
        (Value::Float(left), Value::Float(right)) => left.partial_cmp(right),
        (Value::Signed(left), Value::Float(right)) => Some(compare_i64_float(*left, *right)),
        (Value::Float(left), Value::Signed(right)) => {
            Some(compare_i64_float(*right, *left).reverse())
        }
        (Value::Unsigned(left), Value::Float(right)) => Some(compare_u64_float(*left, *right)),
        (Value::Float(left), Value::Unsigned(right)) => {
            Some(compare_u64_float(*right, *left).reverse())
        }
        _ => None,
    }
}

fn compare_i64_u64(signed: i64, unsigned: u64) -> Ordering {
    if signed < 0 {
        Ordering::Less
    } else {
        signed.cast_unsigned().cmp(&unsigned)
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    reason = "range checks make the truncation safe and the original integer is retained for exact comparison"
)]
fn compare_i64_float(integer: i64, float: f64) -> Ordering {
    if float < i64::MIN as f64 {
        return Ordering::Greater;
    }
    if float >= 9_223_372_036_854_775_808.0 {
        return Ordering::Less;
    }
    let truncated = float.trunc() as i64;
    match integer.cmp(&truncated) {
        Ordering::Equal => (integer as f64)
            .partial_cmp(&float)
            .unwrap_or(Ordering::Equal),
        ordering => ordering,
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    reason = "the nonnegative range check makes the truncation safe and comparison remains exact"
)]
fn compare_u64_float(integer: u64, float: f64) -> Ordering {
    if float < 0.0 {
        return Ordering::Greater;
    }
    if float >= 18_446_744_073_709_551_616.0 {
        return Ordering::Less;
    }
    let truncated = float.trunc() as u64;
    match integer.cmp(&truncated) {
        Ordering::Equal => (integer as f64)
            .partial_cmp(&float)
            .unwrap_or(Ordering::Equal),
        ordering => ordering,
    }
}

fn ascii_cmp(left: &str, right: &str) -> Ordering {
    let folded = left
        .bytes()
        .map(fold_ascii)
        .cmp(right.bytes().map(fold_ascii));
    folded.then_with(|| left.as_bytes().cmp(right.as_bytes()))
}

pub fn ascii_equal(left: &str, right: &str) -> bool {
    left.len() == right.len()
        && left
            .bytes()
            .zip(right.bytes())
            .all(|(left, right)| fold_ascii(left) == fold_ascii(right))
}

pub const fn fold_ascii(byte: u8) -> u8 {
    if byte.is_ascii_uppercase() {
        byte + (b'a' - b'A')
    } else {
        byte
    }
}

fn compare_arrays(left: &[Value], right: &[Value]) -> Ordering {
    for (left, right) in left.iter().zip(right) {
        let ordering = language_cmp(left, right);
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left.len().cmp(&right.len())
}

pub fn decode(bytes: &[u8]) -> Result<Value, peios::Error> {
    let mut reader = Reader::new(bytes);
    let value = read_value(&mut reader)?;
    if reader.remaining() == 0 {
        Ok(value)
    } else {
        Err(peios::Error::from_raw_os_error(libc::EINVAL))
    }
}

fn read_value(reader: &mut Reader<'_>) -> Result<Value, peios::Error> {
    match reader.peek() {
        Some(Type::Nil) => {
            reader.read_nil()?;
            Ok(Value::Null)
        }
        Some(Type::Bool) => reader.read_bool().map(Value::Bool),
        Some(Type::Int) => reader.read_int().map_or_else(
            |_| reader.read_uint().map(Value::Unsigned),
            |value| Ok(Value::Signed(value)),
        ),
        Some(Type::Float) => reader.read_float().map(Value::Float),
        Some(Type::Str) => reader
            .read_str()
            .map(|value| Value::String(value.to_owned())),
        Some(Type::Bin) => reader.read_bin().map(|value| Value::Binary(value.to_vec())),
        Some(Type::Array) => {
            let count = reader.read_array()?;
            let mut values = Vec::with_capacity(count);
            for _ in 0..count {
                values.push(read_value(reader)?);
            }
            Ok(Value::Array(values))
        }
        Some(Type::Map) => {
            let count = reader.read_map()?;
            let mut entries = Vec::with_capacity(count);
            for _ in 0..count {
                entries.push((read_value(reader)?, read_value(reader)?));
            }
            Ok(Value::Map(entries))
        }
        Some(Type::Ext) => {
            let (_, bytes) = reader.read_ext()?;
            Ok(Value::Binary(bytes.to_vec()))
        }
        None => Err(peios::Error::from_raw_os_error(libc::EINVAL)),
    }
}

pub fn flatten_event_payload(payload: &[u8], record: &mut Record) {
    let Ok(Value::Map(entries)) = decode(payload) else {
        return;
    };
    let reserved: HashSet<&str> = [
        "timestamp",
        "cpu_id",
        "sequence",
        "origin_class",
        "event_type",
        "effective_token_guid",
        "true_token_guid",
        "process_guid",
        "boot_id",
    ]
    .into_iter()
    .collect();
    flatten_entries(&entries, "", record, &reserved);
}

fn flatten_entries(
    entries: &[(Value, Value)],
    prefix: &str,
    record: &mut Record,
    reserved: &HashSet<&str>,
) {
    for (key, value) in entries {
        let Value::String(segment) = key else {
            continue;
        };
        if !valid_segment(segment) || (prefix.is_empty() && reserved.contains(segment.as_str())) {
            continue;
        }
        let path = if prefix.is_empty() {
            segment.clone()
        } else {
            format!("{prefix}.{segment}")
        };
        if record.contains_key(&path) {
            continue;
        }
        if let Value::Map(children) = value {
            flatten_entries(children, &path, record, reserved);
        } else {
            record.insert(path, value.clone());
        }
    }
}

fn valid_segment(value: &str) -> bool {
    let mut bytes = value.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

pub fn guid_string(bytes: &[u8]) -> Option<String> {
    let mut guid: [u8; 16] = bytes.try_into().ok()?;
    guid[0..4].reverse();
    guid[4..6].reverse();
    guid[6..8].reverse();
    Some(format!(
        "{{{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}}}",
        guid[0],
        guid[1],
        guid[2],
        guid[3],
        guid[4],
        guid[5],
        guid[6],
        guid[7],
        guid[8],
        guid[9],
        guid[10],
        guid[11],
        guid[12],
        guid[13],
        guid[14],
        guid[15]
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_comparison_does_not_round_large_integers() {
        assert_eq!(
            language_cmp(
                &Value::Unsigned(9_007_199_254_740_993),
                &Value::Float(9_007_199_254_740_992.0),
            ),
            Ordering::Greater
        );
    }

    #[test]
    fn nan_is_one_value_after_every_other_number() {
        let nan = Value::Float(f64::NAN);
        assert!(nan.language_equal(&Value::Float(-f64::NAN)));
        for number in [
            Value::Float(f64::INFINITY),
            Value::Float(f64::NEG_INFINITY),
            Value::Signed(0),
            Value::Signed(i64::MIN),
            Value::Unsigned(u64::MAX),
            Value::Float(-0.0),
        ] {
            assert_eq!(language_cmp(&nan, &number), Ordering::Greater, "{number:?}");
            assert_eq!(language_cmp(&number, &nan), Ordering::Less, "{number:?}");
            assert!(!nan.language_equal(&number));
        }
        // Still a number: after null and booleans, before strings.
        assert_eq!(language_cmp(&nan, &Value::Bool(true)), Ordering::Greater);
        assert_eq!(
            language_cmp(&nan, &Value::String(String::new())),
            Ordering::Less
        );
        // A total order, which sorting checks and panics without.
        let mut values = [
            Value::Float(1.0),
            nan,
            Value::Signed(5),
            Value::Float(f64::NAN),
            Value::Unsigned(2),
            Value::Float(-0.0),
            Value::Float(f64::INFINITY),
            Value::Signed(0),
        ];
        values.sort_by(language_cmp);
        assert!(values[..6].iter().all(|value| !is_nan(value)));
        assert!(values[6..].iter().all(is_nan));
    }

    #[test]
    fn string_equality_uses_ascii_folding() {
        assert!(Value::String("Alpha".into()).language_equal(&Value::String("alpha".into())));
        assert!(!Value::String("Alpha".into()).language_equal(&Value::String("alphas".into())));
    }

    #[test]
    fn payload_flattening_suppresses_headers_and_bad_paths() {
        let mut writer = Writer::new();
        writer
            .write_map(3)
            .write_str("source")
            .write_map(1)
            .write_str("name")
            .write_str("x")
            .write_str("timestamp")
            .write_uint(1)
            .write_str("bad.key")
            .write_uint(2);
        let mut record = Record::new();
        flatten_event_payload(&writer.to_bytes().unwrap(), &mut record);
        assert_eq!(record.get("source.name"), Some(&Value::String("x".into())));
        assert!(!record.contains_key("timestamp"));
        assert!(!record.contains_key("bad.key"));
    }
}
