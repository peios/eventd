//! Stable `MessagePack` payloads for daemon-generated events.

use eventd_core::{Guid, SyntheticEvent};

use crate::config::AppliedChange;

pub fn startup(
    boot_id: Guid,
    canonical_boot_id: &str,
    restart: bool,
    shard_count: usize,
    resume_points: &[(u16, u64)],
    timestamp: u64,
) -> SyntheticEvent {
    let mut payload = Vec::with_capacity(128 + resume_points.len() * 32);
    payload.push(0x84);
    pack_str(&mut payload, "boot_id");
    pack_str(&mut payload, canonical_boot_id);
    pack_str(&mut payload, "restart");
    payload.push(if restart { 0xc3 } else { 0xc2 });
    pack_str(&mut payload, "shard_count");
    pack_u64(
        &mut payload,
        u64::try_from(shard_count).expect("shard count fits u64"),
    );
    pack_str(&mut payload, "resume_points");
    pack_array_len(&mut payload, resume_points.len());
    for &(cpu_id, sequence) in resume_points {
        payload.push(0x82);
        pack_str(&mut payload, "cpu_id");
        pack_u64(&mut payload, u64::from(cpu_id));
        pack_str(&mut payload, "sequence");
        pack_u64(&mut payload, sequence);
    }
    SyntheticEvent {
        boot_id,
        timestamp,
        event_type: "synthetic.startup".into(),
        payload: payload.into_boxed_slice(),
    }
}

pub fn shutdown(boot_id: Guid, last_sequences: &[(u16, u64)], timestamp: u64) -> SyntheticEvent {
    let mut payload = Vec::with_capacity(32 + last_sequences.len() * 32);
    payload.push(0x81);
    pack_str(&mut payload, "last_sequences");
    pack_array_len(&mut payload, last_sequences.len());
    for &(cpu_id, sequence) in last_sequences {
        payload.push(0x82);
        pack_str(&mut payload, "cpu_id");
        pack_u64(&mut payload, u64::from(cpu_id));
        pack_str(&mut payload, "sequence");
        pack_u64(&mut payload, sequence);
    }
    SyntheticEvent {
        boot_id,
        timestamp,
        event_type: "synthetic.shutdown".into(),
        payload: payload.into_boxed_slice(),
    }
}

pub fn storage_error(
    boot_id: Guid,
    store: &str,
    shard_index: Option<usize>,
    error: &str,
    timestamp: u64,
) -> SyntheticEvent {
    let mut payload = Vec::with_capacity(64 + error.len());
    payload.push(0x83);
    pack_str(&mut payload, "store");
    pack_str(&mut payload, store);
    pack_str(&mut payload, "shard_index");
    if let Some(index) = shard_index {
        pack_u64(
            &mut payload,
            u64::try_from(index).expect("shard index fits u64"),
        );
    } else {
        payload.push(0xc0);
    }
    pack_str(&mut payload, "error");
    pack_str(&mut payload, error);
    SyntheticEvent {
        boot_id,
        timestamp,
        event_type: "synthetic.storage_error".into(),
        payload: payload.into_boxed_slice(),
    }
}

pub fn config_change(boot_id: Guid, change: &AppliedChange, timestamp: u64) -> SyntheticEvent {
    let mut payload = Vec::with_capacity(
        96 + change.old_value.as_ref().map_or(0, String::len)
            + change.new_value.as_ref().map_or(0, String::len),
    );
    payload.push(0x85);
    pack_str(&mut payload, "key");
    pack_str(&mut payload, change.key);
    pack_str(&mut payload, "old_value_type");
    pack_str(&mut payload, change.old_value_type);
    pack_str(&mut payload, "old_value");
    if let Some(value) = &change.old_value {
        pack_str(&mut payload, value);
    } else {
        payload.push(0xc0);
    }
    pack_str(&mut payload, "new_value_type");
    pack_str(&mut payload, change.new_value_type);
    pack_str(&mut payload, "new_value");
    if let Some(value) = &change.new_value {
        pack_str(&mut payload, value);
    } else {
        payload.push(0xc0);
    }
    SyntheticEvent {
        boot_id,
        timestamp,
        event_type: "synthetic.config_change".into(),
        payload: payload.into_boxed_slice(),
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
    use super::*;

    #[test]
    fn startup_payload_has_stable_top_level_shape() {
        let event = startup(
            [1; 16],
            "00112233-4455-6677-8899-aabbccddeeff",
            true,
            4,
            &[(1, 9)],
            7,
        );
        assert_eq!(event.event_type.as_ref(), "synthetic.startup");
        assert_eq!(event.payload[0], 0x84);
    }

    #[test]
    fn shutdown_payload_has_stable_top_level_shape() {
        let event = shutdown([1; 16], &[(0, 9), (2, 11)], 7);
        assert_eq!(event.event_type.as_ref(), "synthetic.shutdown");
        assert_eq!(event.payload[0], 0x81);
    }

    #[test]
    fn storage_error_payload_has_stable_top_level_shape() {
        let event = storage_error([1; 16], "event", Some(3), "corrupt", 7);
        assert_eq!(event.event_type.as_ref(), "synthetic.storage_error");
        assert_eq!(event.payload[0], 0x83);
    }

    #[test]
    fn config_change_payload_has_stable_top_level_shape() {
        let event = config_change(
            [1; 16],
            &AppliedChange {
                key: "MaxBatchSize",
                old_value_type: "REG_DWORD",
                old_value: Some("10000".into()),
                new_value_type: "REG_DWORD",
                new_value: Some("20000".into()),
            },
            7,
        );
        assert_eq!(event.event_type.as_ref(), "synthetic.config_change");
        assert_eq!(event.payload[0], 0x85);
    }
}
