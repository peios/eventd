//! The records eventd writes about itself straight into its event store.
//!
//! Each payload is a `MessagePack` map nested by field path (PGSS §6.4), with
//! the field names `eventd.evman` defines. A value with nothing to say is
//! left out, never written as nil (PGSS §6.5).

use eventd_core::{
    CONFIG_CHANGED, DAEMON_STARTED, DAEMON_STOPPED, Guid, STORE_QUARANTINED, SyntheticEvent,
};

use crate::config::{AppliedChange, AppliedValue, ROOT_KEY};

/// Whether a KMES event claims a type eventd writes itself (TRM §2.6). No
/// such event is stored, so `event.type` alone tells a record eventd wrote
/// from one an emitter sent (TRM §3.1, PEI-1294).
pub fn is_reserved(event_type: &str) -> bool {
    eventd_core::is_store_written(event_type)
}

/// `eventd.daemon.started`: `{store: {restarted, shard-count, resume:
/// {cpus, sequences}}}`.
pub fn startup(
    boot_id: Guid,
    restart: bool,
    shard_count: usize,
    resume_points: &[(u16, u64)],
    timestamp: u64,
) -> SyntheticEvent {
    let mut payload = Vec::with_capacity(96 + resume_points.len() * 16);
    payload.push(0x81);
    pack_str(&mut payload, "store");
    payload.push(0x83);
    pack_str(&mut payload, "restarted");
    payload.push(if restart { 0xc3 } else { 0xc2 });
    pack_str(&mut payload, "shard-count");
    pack_u64(
        &mut payload,
        u64::try_from(shard_count).expect("shard count fits u64"),
    );
    pack_str(&mut payload, "resume");
    pack_parallel_arrays(&mut payload, resume_points);
    SyntheticEvent {
        boot_id,
        timestamp,
        event_type: DAEMON_STARTED.into(),
        payload: payload.into_boxed_slice(),
    }
}

/// `eventd.daemon.stopped`: `{store: {committed: {cpus, sequences}}}`, or
/// an empty map when eventd could not read what it had committed
/// (`committed` is `None`, PEI-1394). Zeroes would claim nothing was
/// committed.
pub fn shutdown(boot_id: Guid, committed: Option<&[(u16, u64)]>, timestamp: u64) -> SyntheticEvent {
    let mut payload = Vec::with_capacity(48 + committed.map_or(0, <[_]>::len) * 16);
    if let Some(committed) = committed {
        payload.push(0x81);
        pack_str(&mut payload, "store");
        payload.push(0x81);
        pack_str(&mut payload, "committed");
        pack_parallel_arrays(&mut payload, committed);
    } else {
        payload.push(0x80);
    }
    SyntheticEvent {
        boot_id,
        timestamp,
        event_type: DAEMON_STOPPED.into(),
        payload: payload.into_boxed_slice(),
    }
}

/// `eventd.store.quarantined`: `{store: {kind, shard?}, outcome:
/// {detail}}`. `shard_index` is given only for the event store, the one
/// store that is sharded.
pub fn storage_error(
    boot_id: Guid,
    store: &str,
    shard_index: Option<usize>,
    error: &str,
    timestamp: u64,
) -> SyntheticEvent {
    let mut payload = Vec::with_capacity(64 + error.len());
    payload.push(0x82);
    pack_str(&mut payload, "store");
    payload.push(if shard_index.is_some() { 0x82 } else { 0x81 });
    pack_str(&mut payload, "kind");
    pack_str(&mut payload, store);
    if let Some(index) = shard_index {
        pack_str(&mut payload, "shard");
        pack_u64(
            &mut payload,
            u64::try_from(index).expect("shard index fits u64"),
        );
    }
    pack_str(&mut payload, "outcome");
    payload.push(0x81);
    pack_str(&mut payload, "detail");
    pack_str(&mut payload, error);
    SyntheticEvent {
        boot_id,
        timestamp,
        event_type: STORE_QUARANTINED.into(),
        payload: payload.into_boxed_slice(),
    }
}

/// `eventd.config.changed`: `{config: {key: {path}, name, type?,
/// type-previous?, value?, value-previous?}}`. A side with no value stored
/// is left out entirely.
pub fn config_change(boot_id: Guid, change: &AppliedChange, timestamp: u64) -> SyntheticEvent {
    let mut payload = Vec::with_capacity(160);
    let sides = [
        (change.current, "type", "value"),
        (change.previous, "type-previous", "value-previous"),
    ];
    let present = sides.iter().filter(|(side, _, _)| side.is_some()).count();
    payload.push(0x81);
    pack_str(&mut payload, "config");
    payload.push(0x82 + 2 * u8::try_from(present).expect("at most two sides"));
    pack_str(&mut payload, "key");
    payload.push(0x81);
    pack_str(&mut payload, "path");
    pack_str(&mut payload, ROOT_KEY);
    pack_str(&mut payload, "name");
    pack_str(&mut payload, change.key);
    for (side, type_name, value_name) in sides {
        if let Some(AppliedValue {
            registry_type,
            value,
        }) = side
        {
            pack_str(&mut payload, type_name);
            pack_u64(&mut payload, u64::from(registry_type.0));
            pack_str(&mut payload, value_name);
            pack_u64(&mut payload, value);
        }
    }
    SyntheticEvent {
        boot_id,
        timestamp,
        event_type: CONFIG_CHANGED.into(),
        payload: payload.into_boxed_slice(),
    }
}

/// `{cpus: [...], sequences: [...]}`: one entry per CPU in each, in the
/// order given, so entry `i` of `sequences` belongs to entry `i` of `cpus`.
fn pack_parallel_arrays(output: &mut Vec<u8>, points: &[(u16, u64)]) {
    output.push(0x82);
    pack_str(output, "cpus");
    pack_array_len(output, points.len());
    for &(cpu_id, _) in points {
        pack_u64(output, u64::from(cpu_id));
    }
    pack_str(output, "sequences");
    pack_array_len(output, points.len());
    for &(_, sequence) in points {
        pack_u64(output, sequence);
    }
}

fn pack_array_len(output: &mut Vec<u8>, length: usize) {
    if length <= 15 {
        output.push(0x90 | u8::try_from(length).expect("fixarray length"));
    } else {
        output.push(0xdc);
        output.extend_from_slice(&u16::try_from(length).expect("array16 length").to_be_bytes());
    }
}

fn pack_str(output: &mut Vec<u8>, value: &str) {
    match value.len() {
        0..=31 => output.push(0xa0 | u8::try_from(value.len()).expect("fixstr length")),
        32..=255 => {
            output.push(0xd9);
            output.push(u8::try_from(value.len()).expect("str8 length"));
        }
        _ => {
            output.push(0xda);
            output.extend_from_slice(
                &u16::try_from(value.len())
                    .expect("str16 length")
                    .to_be_bytes(),
            );
        }
    }
    output.extend_from_slice(value.as_bytes());
}

fn pack_u64(output: &mut Vec<u8>, value: u64) {
    match value {
        0..=0x7f => output.push(u8::try_from(value).expect("positive fixint range")),
        0x80..=0xff => {
            output.push(0xcc);
            output.push(u8::try_from(value).expect("uint8 range"));
        }
        0x100..=0xffff => {
            output.push(0xcd);
            output.extend_from_slice(&u16::try_from(value).expect("uint16 range").to_be_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            output.push(0xce);
            output.extend_from_slice(&u32::try_from(value).expect("uint32 range").to_be_bytes());
        }
        _ => {
            output.push(0xcf);
            output.extend_from_slice(&value.to_be_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use peios::msgpack::{Reader, Type};
    use peios::registry::ValueType;

    use super::*;

    /// A decoded payload value, enough to compare whole payloads.
    #[derive(Debug, PartialEq)]
    enum Value {
        Bool(bool),
        Uint(u64),
        Str(String),
        Array(Vec<Self>),
        Map(Vec<(String, Self)>),
    }

    fn read(reader: &mut Reader<'_>) -> Value {
        match reader.peek().expect("a value") {
            Type::Bool => Value::Bool(reader.read_bool().unwrap()),
            Type::Int => Value::Uint(reader.read_uint().unwrap()),
            Type::Str => Value::Str(reader.read_str().unwrap().to_owned()),
            Type::Array => {
                let length = reader.read_array().unwrap();
                Value::Array((0..length).map(|_| read(reader)).collect())
            }
            Type::Map => {
                let length = reader.read_map().unwrap();
                Value::Map(
                    (0..length)
                        .map(|_| (reader.read_str().unwrap().to_owned(), read(reader)))
                        .collect(),
                )
            }
            other => panic!("no {other:?} belongs in these payloads"),
        }
    }

    fn decode(event: &SyntheticEvent) -> Value {
        let mut reader = Reader::new(&event.payload);
        let value = read(&mut reader);
        assert_eq!(reader.remaining(), 0, "one value, nothing after it");
        value
    }

    fn map(entries: Vec<(&str, Value)>) -> Value {
        Value::Map(
            entries
                .into_iter()
                .map(|(key, value)| (key.to_owned(), value))
                .collect(),
        )
    }

    fn uints(values: &[u64]) -> Value {
        Value::Array(values.iter().copied().map(Value::Uint).collect())
    }

    #[test]
    fn startup_nests_the_store_fields_and_carries_no_boot_id() {
        let event = startup([1; 16], true, 4, &[(0, 7), (1, 0)], 9);
        assert_eq!(event.event_type.as_ref(), "eventd.daemon.started");
        assert_eq!(event.timestamp, 9);
        assert_eq!(
            decode(&event),
            map(vec![(
                "store",
                map(vec![
                    ("restarted", Value::Bool(true)),
                    ("shard-count", Value::Uint(4)),
                    (
                        "resume",
                        map(vec![
                            ("cpus", uints(&[0, 1])),
                            ("sequences", uints(&[7, 0]))
                        ])
                    ),
                ])
            )])
        );
    }

    #[test]
    fn shutdown_carries_what_was_committed() {
        let event = shutdown([1; 16], Some(&[(0, 9), (2, 11)]), 7);
        assert_eq!(event.event_type.as_ref(), "eventd.daemon.stopped");
        assert_eq!(
            decode(&event),
            map(vec![(
                "store",
                map(vec![(
                    "committed",
                    map(vec![
                        ("cpus", uints(&[0, 2])),
                        ("sequences", uints(&[9, 11]))
                    ])
                )])
            )])
        );
    }

    // PEI-1394: unreadable coverage leaves the arrays out, not zeroed.
    #[test]
    fn shutdown_without_readable_coverage_is_an_empty_map() {
        let event = shutdown([1; 16], None, 7);
        assert_eq!(event.event_type.as_ref(), "eventd.daemon.stopped");
        assert_eq!(decode(&event), map(vec![]));
    }

    #[test]
    fn a_quarantined_event_shard_names_its_shard() {
        let event = storage_error([1; 16], "event", Some(3), "corrupt", 7);
        assert_eq!(event.event_type.as_ref(), "eventd.store.quarantined");
        assert_eq!(
            decode(&event),
            map(vec![
                (
                    "store",
                    map(vec![
                        ("kind", Value::Str("event".into())),
                        ("shard", Value::Uint(3)),
                    ])
                ),
                (
                    "outcome",
                    map(vec![("detail", Value::Str("corrupt".into()))])
                ),
            ])
        );
    }

    // PEI-1394: the log and metric stores are not sharded, so no shard key.
    #[test]
    fn a_quarantined_unsharded_store_has_no_shard_key() {
        let event = storage_error([1; 16], "log", None, "corrupt", 7);
        assert_eq!(
            decode(&event),
            map(vec![
                ("store", map(vec![("kind", Value::Str("log".into()))])),
                (
                    "outcome",
                    map(vec![("detail", Value::Str("corrupt".into()))])
                ),
            ])
        );
    }

    const fn side(registry_type: ValueType, value: u64) -> AppliedValue {
        AppliedValue {
            registry_type,
            value,
        }
    }

    #[test]
    fn a_config_change_carries_both_sides_as_registry_type_numbers_and_integers() {
        let event = config_change(
            [1; 16],
            &AppliedChange {
                key: "EventRetentionMaxBytes",
                previous: Some(side(ValueType::QWORD, 10_000)),
                current: Some(side(ValueType::QWORD, 20_000)),
            },
            7,
        );
        assert_eq!(event.event_type.as_ref(), "eventd.config.changed");
        assert_eq!(
            decode(&event),
            map(vec![(
                "config",
                map(vec![
                    (
                        "key",
                        map(vec![("path", Value::Str(r"Machine\System\eventd".into()))])
                    ),
                    ("name", Value::Str("EventRetentionMaxBytes".into())),
                    ("type", Value::Uint(11)),
                    ("value", Value::Uint(20_000)),
                    ("type-previous", Value::Uint(11)),
                    ("value-previous", Value::Uint(10_000)),
                ])
            )])
        );
    }

    #[test]
    fn a_config_change_leaves_out_a_side_with_no_value_stored() {
        let set = config_change(
            [1; 16],
            &AppliedChange {
                key: "MaxBatchSize",
                previous: None,
                current: Some(side(ValueType::DWORD, 20_000)),
            },
            7,
        );
        let removed = config_change(
            [1; 16],
            &AppliedChange {
                key: "MaxBatchSize",
                previous: Some(side(ValueType::DWORD, 20_000)),
                current: None,
            },
            7,
        );
        let path = || map(vec![("path", Value::Str(r"Machine\System\eventd".into()))]);
        assert_eq!(
            decode(&set),
            map(vec![(
                "config",
                map(vec![
                    ("key", path()),
                    ("name", Value::Str("MaxBatchSize".into())),
                    ("type", Value::Uint(4)),
                    ("value", Value::Uint(20_000)),
                ])
            )])
        );
        assert_eq!(
            decode(&removed),
            map(vec![(
                "config",
                map(vec![
                    ("key", path()),
                    ("name", Value::Str("MaxBatchSize".into())),
                    ("type-previous", Value::Uint(4)),
                    ("value-previous", Value::Uint(20_000)),
                ])
            )])
        );
    }

    // PEI-1294: exactly the five types, never a prefix.
    #[test]
    fn only_the_five_store_written_types_are_reserved() {
        for event_type in [
            "eventd.daemon.started",
            "eventd.daemon.stopped",
            "eventd.events.lost",
            "eventd.store.quarantined",
            "eventd.config.changed",
        ] {
            assert!(is_reserved(event_type), "{event_type}");
        }
        for event_type in [
            "eventd.daemon.restarted",
            "eventd.daemon",
            "eventd.events.lost.more",
            "eventd.query.refused",
            "synthetic.startup",
            "synthetic.gap",
        ] {
            assert!(!is_reserved(event_type), "{event_type}");
        }
    }
}
