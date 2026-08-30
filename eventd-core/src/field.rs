//! Stable event-field identities and adaptive-index names.

const EVENTD_FIELD_NAMESPACE: [u8; 16] = [
    0xe7, 0xd3, 0xa1, 0xb0, 0x5c, 0x2f, 0x4e, 0x8a, 0x9b, 0x1d, 0x0a, 0x6f, 0x3c, 0x8e, 0x2d, 0x4b,
];

/// Return the UUID-v5 field identity in PCDS GUID byte order.
#[must_use]
pub fn field_guid(field: &str) -> [u8; 16] {
    let mut rfc = field_uuid_rfc(field);
    rfc[0..4].reverse();
    rfc[4..6].reverse();
    rfc[6..8].reverse();
    rfc
}

/// Return the safe `SQLite` name for a flattened payload-field index.
#[must_use]
pub fn payload_index_name(field: &str) -> Option<String> {
    if !valid_field_path(field) {
        return None;
    }
    let guid = field_uuid_rfc(field);
    let mut name = String::with_capacity("idx_events_payload_".len() + 32);
    name.push_str("idx_events_payload_");
    for byte in guid {
        use core::fmt::Write as _;
        write!(name, "{byte:02x}").expect("writing to String cannot fail");
    }
    Some(name)
}

/// Whether a name can be produced by event payload flattening.
#[must_use]
pub fn valid_field_path(field: &str) -> bool {
    !field.is_empty() && field.split('.').all(valid_segment)
}

fn valid_segment(segment: &str) -> bool {
    let mut bytes = segment.bytes();
    bytes
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
}

fn field_uuid_rfc(field: &str) -> [u8; 16] {
    let mut sha1 = Sha1::new();
    sha1.update(&EVENTD_FIELD_NAMESPACE);
    sha1.update(field.as_bytes());
    let digest = sha1.finish();
    let mut uuid: [u8; 16] = digest[..16].try_into().expect("SHA-1 prefix length");
    uuid[6] = (uuid[6] & 0x0f) | 0x50;
    uuid[8] = (uuid[8] & 0x3f) | 0x80;
    uuid
}

struct Sha1 {
    state: [u32; 5],
    bytes: u64,
    block: [u8; 64],
    used: usize,
}

impl Sha1 {
    const fn new() -> Self {
        Self {
            state: [
                0x6745_2301,
                0xefcd_ab89,
                0x98ba_dcfe,
                0x1032_5476,
                0xc3d2_e1f0,
            ],
            bytes: 0,
            block: [0; 64],
            used: 0,
        }
    }

    fn update(&mut self, mut bytes: &[u8]) {
        self.bytes = self.bytes.wrapping_add(bytes.len() as u64);
        while !bytes.is_empty() {
            let copied = (64 - self.used).min(bytes.len());
            self.block[self.used..self.used + copied].copy_from_slice(&bytes[..copied]);
            self.used += copied;
            bytes = &bytes[copied..];
            if self.used == 64 {
                compress(&mut self.state, &self.block);
                self.used = 0;
            }
        }
    }

    fn finish(mut self) -> [u8; 20] {
        let bit_length = self.bytes.wrapping_mul(8);
        self.block[self.used] = 0x80;
        self.used += 1;
        if self.used > 56 {
            self.block[self.used..].fill(0);
            compress(&mut self.state, &self.block);
            self.block = [0; 64];
        } else {
            self.block[self.used..56].fill(0);
        }
        self.block[56..].copy_from_slice(&bit_length.to_be_bytes());
        compress(&mut self.state, &self.block);
        let mut output = [0_u8; 20];
        for (chunk, word) in output.chunks_exact_mut(4).zip(self.state) {
            chunk.copy_from_slice(&word.to_be_bytes());
        }
        output
    }
}

#[allow(
    clippy::many_single_char_names,
    reason = "SHA-1's five working words use the names from its standard algorithm"
)]
fn compress(state: &mut [u32; 5], block: &[u8; 64]) {
    let mut words = [0_u32; 80];
    for (index, chunk) in block.chunks_exact(4).enumerate() {
        words[index] = u32::from_be_bytes(chunk.try_into().expect("word size"));
    }
    for index in 16..80 {
        words[index] =
            (words[index - 3] ^ words[index - 8] ^ words[index - 14] ^ words[index - 16])
                .rotate_left(1);
    }
    let [mut a, mut b, mut c, mut d, mut e] = *state;
    for (index, word) in words.into_iter().enumerate() {
        let (function, constant) = match index {
            0..=19 => ((b & c) | ((!b) & d), 0x5a82_7999),
            20..=39 => (b ^ c ^ d, 0x6ed9_eba1),
            40..=59 => ((b & c) | (b & d) | (c & d), 0x8f1b_bcdc),
            _ => (b ^ c ^ d, 0xca62_c1d6),
        };
        let temporary = a
            .rotate_left(5)
            .wrapping_add(function)
            .wrapping_add(e)
            .wrapping_add(constant)
            .wrapping_add(word);
        e = d;
        d = c;
        c = b.rotate_left(30);
        b = a;
        a = temporary;
    }
    state[0] = state[0].wrapping_add(a);
    state[1] = state[1].wrapping_add(b);
    state[2] = state[2].wrapping_add(c);
    state[3] = state[3].wrapping_add(d);
    state[4] = state[4].wrapping_add(e);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_v5_and_index_name_match_stable_vectors() {
        assert_eq!(
            payload_index_name("source.name").as_deref(),
            Some("idx_events_payload_2602646b558759b4af6e77009f3ebc3d")
        );
        assert_eq!(
            field_guid("timestamp"),
            [
                0x67, 0x22, 0x1d, 0x34, 0xdb, 0xb9, 0x6b, 0x53, 0xb3, 0x6c, 0x94, 0xab, 0x6c, 0xd4,
                0x7e, 0x4c,
            ]
        );
        assert!(!valid_field_path("source..name"));
        assert!(!valid_field_path("source.1name"));
    }

    #[test]
    fn sha1_known_vector() {
        let mut hash = Sha1::new();
        hash.update(b"abc");
        assert_eq!(
            hash.finish(),
            [
                0xa9, 0x99, 0x3e, 0x36, 0x47, 0x06, 0x81, 0x6a, 0xba, 0x3e, 0x25, 0x71, 0x78, 0x50,
                0xc2, 0x6c, 0x9c, 0xd0, 0xd8, 0x9d,
            ]
        );
    }
}
