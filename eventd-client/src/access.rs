//! What a caller may read from eventd, asked of the system rather than
//! guessed.
//!
//! eventd leaves out what a caller may not read and says nothing about it
//! (PSPU §3.28), so a program that wants to tell its user "you can't read
//! events here" has to work it out itself, from the same descriptors eventd
//! checks (eventd TRM §7). This module reads them and checks them the way
//! eventd does: the caller's own token, eventd's generic mapping, and an
//! object type list with the data type's root and each field.
//!
//! A record is visible only to a caller granted `EVENTD_READ` on the root;
//! a field ACE then decides which fields it carries.

use peios::access::AccessCheck;
use peios::registry::{Key, KeyAccess, OpenFlags, ValueType};
use peios::security::{AccessMask, GenericMapping, SecurityDescriptor};

/// The registry key holding eventd's read policy.
pub const SECURITY_ROOT: &str = r"Machine\System\eventd\Security";
/// The key whose descriptor governs `EVENTD_ADMINISTER` (TRM §7.2).
pub const ADMIN_KEY: &str = r"Machine\System\eventd\Security\Admin";

/// Read records matching a pattern.
pub const EVENTD_READ: u32 = 0x0001;
/// Delete records matching a pattern. Reserved: nothing uses it yet.
pub const EVENTD_CLEAR: u32 = 0x0002;
/// Change eventd's own policy, which is the `INDEX` command.
pub const EVENTD_ADMINISTER: u32 = 0x0004;
/// Publish metric records under a matching name.
pub const EVENTD_PUBLISH: u32 = 0x0008;

/// One of eventd's rights, as a permissions editor names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Right {
    /// The right's bit.
    pub mask: u32,
    /// Its name.
    pub name: &'static str,
    /// What it allows, in words.
    pub meaning: &'static str,
}

/// eventd's rights (TRM §B).
pub const RIGHTS: [Right; 4] = [
    Right {
        mask: EVENTD_READ,
        name: "Read",
        meaning: "Read records matching the pattern",
    },
    Right {
        mask: EVENTD_CLEAR,
        name: "Clear",
        meaning: "Delete records matching the pattern (reserved; nothing uses it yet)",
    },
    Right {
        mask: EVENTD_ADMINISTER,
        name: "Administer",
        meaning: "Change eventd's own policy",
    },
    Right {
        mask: EVENTD_PUBLISH,
        name: "Publish",
        meaning: "Publish metric records under the matching name",
    },
];

/// What `GENERIC_READ` means to eventd: `EVENTD_READ | READ_CONTROL`.
pub const GENERIC_READ: u32 = 0x0002_0001;
/// What `GENERIC_WRITE` means: clear, administer, publish and
/// `READ_CONTROL`.
pub const GENERIC_WRITE: u32 = 0x0002_000e;
/// What `GENERIC_EXECUTE` means: the same as `GENERIC_READ`.
pub const GENERIC_EXECUTE: u32 = 0x0002_0001;
/// What `GENERIC_ALL` means: every eventd right and the standard ones.
pub const GENERIC_ALL: u32 = 0x000f_000f;

/// eventd's generic mapping, for an access check.
#[must_use]
pub fn generic_mapping() -> GenericMapping {
    GenericMapping::new(GENERIC_READ, GENERIC_WRITE, GENERIC_EXECUTE, GENERIC_ALL)
}

/// The three kinds of data, each with its own patterns (TRM §7.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Namespace {
    /// Events, by event type.
    Events,
    /// Logs, by origin.
    Logs,
    /// Metrics, by name.
    Metrics,
}

impl Namespace {
    /// All three.
    pub const ALL: [Self; 3] = [Self::Events, Self::Logs, Self::Metrics];

    /// The name of its key under [`SECURITY_ROOT`].
    #[must_use]
    pub const fn registry_name(self) -> &'static str {
        match self {
            Self::Events => "Events",
            Self::Logs => "Logs",
            Self::Metrics => "Metrics",
        }
    }

    /// The GUID at level 0 of its object type list, in PCDS byte order.
    #[must_use]
    pub const fn root_guid(self) -> [u8; 16] {
        let last = match self {
            Self::Events => 1,
            Self::Logs => 2,
            Self::Metrics => 3,
        };
        [
            0xd4, 0xc3, 0xb2, 0xa1, 0x01, 0x00, 0x00, 0x40, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, last,
        ]
    }
}

/// The registry key holding `pattern`'s descriptor, as its default value.
#[must_use]
pub fn descriptor_path(namespace: Namespace, pattern: &str) -> String {
    format!("{SECURITY_ROOT}\\{}\\{pattern}", namespace.registry_name())
}

/// The patterns that could hold the descriptor for `identifier`.
///
/// Most specific first: the identifier, then each shorter dotted prefix,
/// then `*` (TRM §7.2). An origin's producer, from a slash on, is not part
/// of any pattern: `jobs/<guid>` starts at `jobs`.
#[must_use]
pub fn candidates(identifier: &str) -> Vec<&str> {
    let mut pattern = identifier.split('/').next().unwrap_or(identifier);
    let mut candidates = vec![pattern];
    while let Some(index) = pattern.rfind('.') {
        pattern = &pattern[..index];
        candidates.push(pattern);
    }
    candidates.push("*");
    candidates
}

/// The descriptor written for `pattern`, if there is one.
pub fn descriptor(
    namespace: Namespace,
    pattern: &str,
) -> Result<Option<SecurityDescriptor>, peios::Error> {
    load(&descriptor_path(namespace, pattern))
}

/// The descriptor eventd checks `identifier` against, and the pattern it
/// was written for; `None` when there is none, which denies.
pub fn resolve(
    namespace: Namespace,
    identifier: &str,
) -> Result<Option<(String, SecurityDescriptor)>, peios::Error> {
    for pattern in candidates(identifier) {
        if let Some(descriptor) = descriptor(namespace, pattern)? {
            return Ok(Some((pattern.to_owned(), descriptor)));
        }
    }
    Ok(None)
}

/// Every pattern with a key under `namespace`, `*` included.
pub fn patterns(namespace: Namespace) -> Result<Vec<String>, peios::Error> {
    let path = format!("{SECURITY_ROOT}\\{}", namespace.registry_name());
    let key = Key::open(
        None,
        &path,
        KeyAccess::ENUMERATE_SUB_KEYS,
        OpenFlags::default(),
    )?;
    key.subkeys(None)
        .map(|subkey| subkey.map(|subkey| String::from_utf8_lossy(&subkey.name).into_owned()))
        .collect()
}

/// What the caller may read under one descriptor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Access {
    /// Whether records under it are visible at all.
    pub records: bool,
    /// Of the fields asked about, those the caller may read. Meaningful
    /// only when `records` is.
    pub fields: Vec<String>,
}

/// What this process's token may read under `descriptor`, asking about
/// `fields` as well as the records.
pub fn access(
    descriptor: &SecurityDescriptor,
    namespace: Namespace,
    fields: &[&str],
) -> Result<Access, peios::Error> {
    let mut tree = Vec::with_capacity(fields.len() + 1);
    tree.push(peios_sys::kacs_object_type_entry {
        level: 0,
        _reserved: 0,
        guid: namespace.root_guid(),
    });
    for field in fields {
        tree.push(peios_sys::kacs_object_type_entry {
            level: 1,
            _reserved: 0,
            guid: field_guid(field),
        });
    }
    let results = AccessCheck::new(
        descriptor,
        AccessMask::from_bits_retain(EVENTD_READ),
        generic_mapping(),
    )
    .check_list(&tree)?;
    let reads = |result: &peios_sys::kacs_node_result| {
        result.status == 0 && result.granted & EVENTD_READ != 0
    };
    Ok(Access {
        records: reads(&results[0]),
        fields: fields
            .iter()
            .zip(&results[1..])
            .filter(|(_, result)| reads(result))
            .map(|(field, _)| (*field).to_owned())
            .collect(),
    })
}

/// How much of one kind of data the caller may read, from every pattern
/// written for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Readable {
    /// Every pattern lets the caller read records.
    Everything,
    /// Some do and some don't: the patterns that don't.
    Some {
        /// Patterns whose records the caller may not read.
        hidden: Vec<String>,
    },
    /// No pattern lets the caller read records.
    Nothing,
    /// The policy could not be read, so nobody can say; why, in words.
    Unknown(String),
}

/// How much of `namespace` this process may read.
///
/// A more specific pattern may grant what `*` denies, and the other way
/// round, so every pattern counts. Patterns that match nothing stored
/// still count: this says what the policy allows, not what is there.
#[must_use]
pub fn readable(namespace: Namespace) -> Readable {
    let patterns = match patterns(namespace) {
        Ok(patterns) => patterns,
        Err(error) => return Readable::Unknown(format!("its policy can't be read ({error})")),
    };
    let mut hidden = Vec::new();
    let mut visible = 0;
    for pattern in patterns {
        let granted = match descriptor(namespace, &pattern) {
            Ok(Some(descriptor)) => {
                access(&descriptor, namespace, &[]).map(|access| access.records)
            }
            Ok(None) => Ok(false),
            Err(error) => Err(error),
        };
        match granted {
            Ok(true) => visible += 1,
            Ok(false) => hidden.push(pattern),
            Err(error) => {
                return Readable::Unknown(format!(
                    "the policy for {pattern} can't be checked ({error})"
                ));
            }
        }
    }
    match (visible, hidden.is_empty()) {
        (0, _) => Readable::Nothing,
        (_, true) => Readable::Everything,
        (_, false) => Readable::Some { hidden },
    }
}

/// Whether this process may change eventd's own policy (`INDEX`): `None`
/// when the descriptor can't be read or checked.
#[must_use]
pub fn may_administer() -> Option<bool> {
    let descriptor = load(ADMIN_KEY).ok()?;
    let Some(descriptor) = descriptor else {
        return Some(false);
    };
    AccessCheck::new(
        &descriptor,
        AccessMask::from_bits_retain(EVENTD_ADMINISTER),
        generic_mapping(),
    )
    .check()
    .ok()
    .map(|decision| decision.allowed)
}

fn load(path: &str) -> Result<Option<SecurityDescriptor>, peios::Error> {
    let key = match Key::open(None, path, KeyAccess::QUERY_VALUE, OpenFlags::default()) {
        Ok(key) => key,
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
        Err(error) => return Err(error),
    };
    let value = match key.query_value(b"", None) {
        Ok(value) => value,
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
        Err(error) => return Err(error),
    };
    if value.ty != ValueType::BINARY {
        return Err(peios::Error::from_raw_os_error(libc::EINVAL));
    }
    SecurityDescriptor::from_validated_bytes(value.data).map(Some)
}

/// The GUID a descriptor's object ACEs name `field` by (TRM §7.3).
///
/// It is UUID v5 of the field's query-language name in eventd's
/// namespace, in PCDS byte order: computed, never registered, so that a
/// new event type with a new field needs nothing recorded anywhere first.
#[must_use]
pub fn field_guid(field: &str) -> [u8; 16] {
    const NAMESPACE: [u8; 16] = [
        0xe7, 0xd3, 0xa1, 0xb0, 0x5c, 0x2f, 0x4e, 0x8a, 0x9b, 0x1d, 0x0a, 0x6f, 0x3c, 0x8e, 0x2d,
        0x4b,
    ];
    let mut sha1 = Sha1::new();
    sha1.update(&NAMESPACE);
    sha1.update(field.as_bytes());
    let digest = sha1.finish();
    let mut guid: [u8; 16] = digest[..16].try_into().expect("SHA-1 prefix length");
    guid[6] = (guid[6] & 0x0f) | 0x50;
    guid[8] = (guid[8] & 0x3f) | 0x80;
    // RFC 4122 byte order to PCDS's: the first three fields little-endian.
    guid[0..4].reverse();
    guid[4..6].reverse();
    guid[6..8].reverse();
    guid
}

// SHA-1, for UUID v5 only. eventd-core has the same, and both are pinned
// to the same vectors; this crate does not depend on eventd-core, which
// brings SQLite.
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
        for (chunk, word) in output.as_chunks_mut::<4>().0.iter_mut().zip(self.state) {
            *chunk = word.to_be_bytes();
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
    for (index, chunk) in block.as_chunks::<4>().0.iter().enumerate() {
        words[index] = u32::from_be_bytes(*chunk);
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
    fn field_guids_match_eventds_stable_vector() {
        assert_eq!(
            field_guid("timestamp"),
            [
                0x67, 0x22, 0x1d, 0x34, 0xdb, 0xb9, 0x6b, 0x53, 0xb3, 0x6c, 0x94, 0xab, 0x6c, 0xd4,
                0x7e, 0x4c,
            ]
        );
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

    #[test]
    fn candidates_walk_from_the_identifier_to_the_wildcard() {
        assert_eq!(
            candidates("kacs.token.denied"),
            ["kacs.token.denied", "kacs.token", "kacs", "*"]
        );
        assert_eq!(candidates("jellyfin/ExecStartPre[0]"), ["jellyfin", "*"]);
        assert_eq!(candidates("jobs/00000000-4518"), ["jobs", "*"]);
        assert_eq!(candidates("loregd"), ["loregd", "*"]);
    }

    #[test]
    fn descriptor_paths_sit_under_the_namespace_key() {
        assert_eq!(
            descriptor_path(Namespace::Logs, "*"),
            r"Machine\System\eventd\Security\Logs\*"
        );
        assert_eq!(
            descriptor_path(Namespace::Events, "kacs"),
            r"Machine\System\eventd\Security\Events\kacs"
        );
    }

    #[test]
    fn the_generic_mapping_keeps_administer_and_publish_out_of_read() {
        assert_eq!(GENERIC_READ & (EVENTD_ADMINISTER | EVENTD_PUBLISH), 0);
        assert_eq!(GENERIC_EXECUTE & (EVENTD_ADMINISTER | EVENTD_PUBLISH), 0);
        assert_eq!(
            GENERIC_ALL & 0xf,
            RIGHTS.iter().fold(0, |all, right| all | right.mask)
        );
    }
}
