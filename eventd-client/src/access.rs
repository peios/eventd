//! What a caller may read from eventd, asked of the system rather than
//! guessed.
//!
//! eventd leaves out what a caller may not read and says nothing about it
//! (PSPU §3.28), so a program that wants to tell its user "you can't read
//! events here" has to work it out itself, from the same descriptors eventd
//! checks (eventd TRM §7). This module reads them and checks them the way
//! eventd does: the caller's own token, eventd's generic mapping, and an
//! object type list with the data type's root, each field, and each prefix
//! of a field's dotted path between them ([`field_tree`]).
//!
//! A record is visible only to a caller granted `EVENTD_READ` on the root;
//! a field ACE then decides which fields it carries.

use std::collections::HashMap;

use peios::access::AccessCheck;
use peios::registry::{Key, KeyAccess, OpenFlags, ValueType};
use peios::security::{AccessMask, AceType, GenericMapping, SecurityDescriptor};

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
    /// Whether records under it are visible at all: the record as a whole,
    /// or some field of it, may be read.
    pub records: bool,
    /// Of the fields asked about, those the caller may read. Meaningful
    /// only when `records` is.
    pub fields: Vec<String>,
}

/// What this process's token may read under `descriptor`, asking about
/// `fields` as well as the records.
///
/// Records are visible to a caller who may read the record as a whole or
/// any field of it (§7.3): a grant of some fields shows the records with
/// those fields. Which fields a descriptor grants by name is in its object
/// ACEs (`field_grants`), so those are asked about too.
pub fn access(
    descriptor: &SecurityDescriptor,
    namespace: Namespace,
    fields: &[&str],
) -> Result<Access, peios::Error> {
    let check = |tree: &[peios_sys::kacs_object_type_entry]| {
        AccessCheck::new(
            descriptor,
            AccessMask::from_bits_retain(EVENTD_READ),
            generic_mapping(),
        )
        .check_list(tree)
        .map(|results| {
            results
                .iter()
                .map(|result| result.status == 0 && result.granted & EVENTD_READ != 0)
                .collect::<Vec<bool>>()
        })
    };
    let reads = field_reads(namespace.root_guid(), fields, check)?;
    let granted: Vec<String> = fields
        .iter()
        .zip(&reads.fields)
        .filter(|(_, reads)| **reads)
        .map(|(field, _)| (*field).to_owned())
        .collect();
    let records = reads.record || !granted.is_empty() || {
        let named = field_grants(descriptor);
        !named.is_empty()
            && check(&flat_tree(namespace.root_guid(), &named))?
                .into_iter()
                .any(|reads| reads)
    };
    Ok(Access {
        records,
        fields: granted,
    })
}

/// The most nodes KACS accepts in one object type list
/// (`KACS_ACCESS_CHECK_MAX_OBJECT_TYPE_COUNT`). It refuses a longer list
/// with `EINVAL`. KACS sets no limit on a list's depth.
pub const MAX_OBJECT_TYPES: usize = 1024;

/// The object type list for `fields` of a record whose data type's root
/// GUID is `root` (TRM §7.3), and where each field's node is in it.
///
/// A field's dotted name is a path. Every prefix of it that ends before a
/// `.` is a node, with the prefix's [`field_guid`], one level below the
/// prefix before it, and the field is the node at the end: `subject` at
/// level 1, `subject.token` at 2, `subject.token.sid` at 3. Each node is
/// listed once however many fields share it, and the list is in preorder,
/// so a node's subtree follows it directly, as KACS requires. A name with
/// no `.` is at level 1. There is no level limit: the list is as deep as
/// the deepest name.
#[must_use]
pub fn field_tree<S: AsRef<str>>(root: [u8; 16], fields: &[S]) -> FieldTree {
    /// One node: its path, and its children in the order first named.
    struct Node<'a> {
        path: &'a str,
        children: Vec<usize>,
    }
    let mut nodes = vec![Node {
        path: "",
        children: Vec::new(),
    }];
    let mut found: HashMap<&str, usize> = HashMap::new();
    let mut ends = Vec::with_capacity(fields.len());
    for field in fields {
        let field = field.as_ref();
        let mut parent = 0;
        let prefixes = field
            .match_indices('.')
            .map(|(index, _)| &field[..index])
            .chain([field]);
        for path in prefixes {
            parent = if let Some(&node) = found.get(path) {
                node
            } else {
                nodes.push(Node {
                    path,
                    children: Vec::new(),
                });
                let node = nodes.len() - 1;
                nodes[parent].children.push(node);
                found.insert(path, node);
                node
            };
        }
        ends.push(parent);
    }

    // Preorder: each node, then its children's subtrees in turn.
    let mut entries = Vec::with_capacity(nodes.len());
    let mut position = vec![0; nodes.len()];
    let mut stack = vec![(0_usize, 0_usize)];
    while let Some((node, depth)) = stack.pop() {
        position[node] = entries.len();
        entries.push(peios_sys::kacs_object_type_entry {
            // A level past u16 needs that many ancestors, which is far
            // past MAX_OBJECT_TYPES, so such a list is refused whole.
            level: u16::try_from(depth).unwrap_or(u16::MAX),
            _reserved: 0,
            guid: if node == 0 {
                root
            } else {
                field_guid(nodes[node].path)
            },
        });
        for &child in nodes[node].children.iter().rev() {
            stack.push((child, depth + 1));
        }
    }
    FieldTree {
        fields: ends.into_iter().map(|node| position[node]).collect(),
        entries,
    }
}

/// An object type list built by [`field_tree`].
#[derive(Debug, Clone)]
pub struct FieldTree {
    /// The nodes, in preorder, the data type's root first.
    pub entries: Vec<peios_sys::kacs_object_type_entry>,
    /// For each field asked about, in order, its node's index in `entries`.
    pub fields: Vec<usize>,
}

impl FieldTree {
    /// Whether the node at `index` has nodes beneath it.
    #[must_use]
    pub fn has_children(&self, index: usize) -> bool {
        self.entries
            .get(index + 1)
            .is_some_and(|next| next.level > self.entries[index].level)
    }
}

/// The root at level 0 and each of `guids` at level 1 beneath it.
///
/// This is the list that asks whether a descriptor grants a caller any of
/// the fields it names (`field_grants`). A GUID does not tell its field's
/// path, and so not its place in a tree.
#[must_use]
pub fn flat_tree(root: [u8; 16], guids: &[[u8; 16]]) -> Vec<peios_sys::kacs_object_type_entry> {
    let mut tree = Vec::with_capacity(guids.len() + 1);
    tree.push(peios_sys::kacs_object_type_entry {
        level: 0,
        _reserved: 0,
        guid: root,
    });
    tree.extend(guids.iter().filter(|guid| **guid != root).map(|guid| {
        peios_sys::kacs_object_type_entry {
            level: 1,
            _reserved: 0,
            guid: *guid,
        }
    }));
    tree
}

/// What an access check over [`field_tree`] decided.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldReads {
    /// The verdict on the root: the record as a whole.
    pub record: bool,
    /// The verdict on each field asked about, in order.
    pub fields: Vec<bool>,
}

/// Whether the record and each of `fields` may be read, by `check`, which
/// makes the access check over an object type list and answers per node.
///
/// A field is read when the first ACE to decide `EVENTD_READ` for it,
/// in DACL order, allows it, of those that name its GUID, the GUID of any
/// prefix of its path, the data type's root, or no object type at all.
/// KACS makes that the verdict on a node that has nothing beneath it: an
/// object ACE covers its node's subtree, and only decisions that
/// propagate up from beneath reach a node otherwise. So a field that is
/// also a prefix of another field asked about, such as a payload value at
/// `emitter` beside the header's `emitter.class`, is checked again on its
/// own, with only its ancestors in the list: as a parent, a grant on all
/// of its children, or a deny on one, would decide it too.
pub fn field_reads<S: AsRef<str>, E>(
    root: [u8; 16],
    fields: &[S],
    mut check: impl FnMut(&[peios_sys::kacs_object_type_entry]) -> Result<Vec<bool>, E>,
) -> Result<FieldReads, E> {
    let tree = field_tree(root, fields);
    let results = check(&tree.entries)?;
    let mut reads = Vec::with_capacity(fields.len());
    for (field, &node) in fields.iter().zip(&tree.fields) {
        reads.push(if tree.has_children(node) {
            let own = field_tree(root, &[field.as_ref()]);
            check(&own.entries)?[own.fields[0]]
        } else {
            results[node]
        });
    }
    Ok(FieldReads {
        record: results[0],
        fields: reads,
    })
}

/// The fields `descriptor` grants by name: the object GUIDs of its
/// allowing object ACEs, each a field's (§7.3), once each. Whether they
/// grant anything to a given caller is the access check's to say.
///
/// A data type's root GUID (§B) is the list's level-0 node and names no
/// field, so an ACE naming one is not a field grant: listed again at level
/// 1 it would put the root in the list twice, which KACS refuses.
/// Every root is left out, not only the namespace's own, since no field's
/// GUID is a root's: field GUIDs are UUID v5 and the roots are not.
#[must_use]
pub fn field_grants(descriptor: &SecurityDescriptor) -> Vec<[u8; 16]> {
    /// `ACCESS_ALLOWED_OBJECT` and `ACCESS_ALLOWED_CALLBACK_OBJECT` (MS-DTYP
    /// 2.4.4.1), which the SDK leaves as raw types.
    const ALLOWING_OBJECT: [u8; 2] = [0x05, 0x0b];
    let Ok(view) = descriptor.view() else {
        return Vec::new();
    };
    let Some(dacl) = view.dacl() else {
        return Vec::new();
    };
    let mut guids: Vec<[u8; 16]> = dacl
        .iter()
        .filter(
            |ace| matches!(ace.ace_type(), AceType::Other(raw) if ALLOWING_OBJECT.contains(&raw)),
        )
        .filter_map(|ace| ace.object_type().copied())
        .filter(|guid| {
            !Namespace::ALL
                .iter()
                .any(|namespace| namespace.root_guid() == *guid)
        })
        .collect();
    guids.sort_unstable();
    guids.dedup();
    guids
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
    readable_from(patterns(namespace), |pattern| {
        match descriptor(namespace, pattern) {
            Ok(Some(descriptor)) => {
                access(&descriptor, namespace, &[]).map(|access| access.records)
            }
            Ok(None) => Ok(false),
            Err(error) => Err(error),
        }
    })
}

/// [`readable`]'s verdict from the patterns the policy lists, or why they
/// could not be listed, and whether the caller may read records under each.
fn readable_from(
    patterns: Result<Vec<String>, peios::Error>,
    mut granted: impl FnMut(&str) -> Result<bool, peios::Error>,
) -> Readable {
    let patterns = match patterns {
        Ok(patterns) => patterns,
        Err(error) => return Readable::Unknown(format!("its policy can't be read ({error})")),
    };
    let mut hidden = Vec::new();
    let mut visible = 0;
    for pattern in patterns {
        match granted(&pattern) {
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
    fn a_descriptor_grants_by_name_the_fields_its_allowing_object_aces_name() {
        let descriptor = peios::security::sddl::parse(
            "O:SYG:SYD:P(A;;0x1;;;SY)\
             (OA;;0x1;fe639b5f-4f7f-54c5-9702-f20fac24fa0e;;BA)\
             (OA;;0x1;341d2267-b9db-536b-b36c-94ab6cd47e4c;;BA)\
             (OA;;0x1;341d2267-b9db-536b-b36c-94ab6cd47e4c;;AU)\
             (OD;;0x1;1f638b88-4da0-5dd1-ace6-05204455e880;;AU)",
        )
        .expect("descriptor");
        let mut expected = vec![field_guid("message"), field_guid("timestamp")];
        expected.sort_unstable();
        assert_eq!(field_grants(&descriptor), expected);
        let plain = peios::security::sddl::parse("O:SYG:SYD:P(A;;0x1;;;SY)").expect("descriptor");
        assert!(field_grants(&plain).is_empty());
    }

    // PEI-1288: an object ACE naming a data type's root GUID grants the
    // level-0 node, and with it the record; it is no field's grant. Listed
    // as a field, the root would be in the object type list twice.
    #[test]
    fn an_ace_naming_a_data_type_root_is_not_a_field_grant() {
        let descriptor = peios::security::sddl::parse(
            "O:SYG:SYD:P(A;;0x1;;;SY)\
             (OA;;0x1;a1b2c3d4-0001-4000-8000-000000000001;;BA)\
             (OA;;0x1;a1b2c3d4-0001-4000-8000-000000000002;;BA)\
             (OA;;0x1;a1b2c3d4-0001-4000-8000-000000000003;;AU)\
             (OA;;0x1;341d2267-b9db-536b-b36c-94ab6cd47e4c;;AU)",
        )
        .expect("descriptor");
        assert_eq!(field_grants(&descriptor), [field_guid("timestamp")]);
        for namespace in Namespace::ALL {
            assert!(!field_grants(&descriptor).contains(&namespace.root_guid()));
        }
    }

    #[test]
    fn the_root_guids_are_the_ones_the_book_gives_in_pcds_byte_order() {
        for (namespace, sddl) in [
            (Namespace::Events, "a1b2c3d4-0001-4000-8000-000000000001"),
            (Namespace::Logs, "a1b2c3d4-0001-4000-8000-000000000002"),
            (Namespace::Metrics, "a1b2c3d4-0001-4000-8000-000000000003"),
        ] {
            let descriptor =
                peios::security::sddl::parse(&format!("O:SYG:SYD:P(OA;;0x1;{sddl};;AU)"))
                    .expect("descriptor");
            let view = descriptor.view().expect("view");
            let named = view
                .dacl()
                .expect("dacl")
                .iter()
                .find_map(|ace| ace.object_type().copied());
            assert_eq!(named, Some(namespace.root_guid()), "{sddl}");
        }
    }

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

    /// `readable` over `patterns`, the caller reading records under those
    /// in `granting` and no others, and the patterns it asked about.
    fn readable_over(patterns: &[&str], granting: &[&str]) -> (Readable, Vec<String>) {
        let mut asked = Vec::new();
        let verdict = readable_from(
            Ok(patterns
                .iter()
                .map(|pattern| (*pattern).to_owned())
                .collect()),
            |pattern| {
                asked.push(pattern.to_owned());
                Ok(granting.contains(&pattern))
            },
        );
        (verdict, asked)
    }

    #[test]
    fn readable_weighs_every_pattern_not_only_the_wildcard() {
        // Logs\x grants what Logs\* denies: some, not nothing.
        let (verdict, asked) = readable_over(&["*", "x"], &["x"]);
        assert_eq!(
            verdict,
            Readable::Some {
                hidden: vec!["*".to_owned()]
            }
        );
        assert_eq!(asked, ["*", "x"]);
        // And the other way round: x denies what * grants.
        let (verdict, asked) = readable_over(&["*", "x"], &["*"]);
        assert_eq!(
            verdict,
            Readable::Some {
                hidden: vec!["x".to_owned()]
            }
        );
        assert_eq!(asked, ["*", "x"]);
        assert_eq!(
            readable_over(&["*", "x"], &["*", "x"]).0,
            Readable::Everything
        );
        assert_eq!(readable_over(&["*", "x"], &[]).0, Readable::Nothing);
    }

    #[test]
    fn a_policy_that_cannot_be_read_is_unknown_with_its_reason_never_nothing() {
        // Denied ENUMERATE_SUB_KEYS on Security\Logs: the patterns can't
        // be listed.
        let verdict = readable_from(Err(peios::Error::from_raw_os_error(libc::EACCES)), |_| {
            panic!("no pattern can be asked about when none could be listed")
        });
        let Readable::Unknown(reason) = verdict else {
            panic!("expected Unknown, got {verdict:?}")
        };
        assert!(
            reason.contains(&peios::Error::from_raw_os_error(libc::EACCES).to_string()),
            "the reason says why: {reason}"
        );
        // A pattern whose descriptor can't be read or checked is the same.
        let verdict = readable_from(Ok(vec!["*".to_owned(), "x".to_owned()]), |pattern| {
            if pattern == "x" {
                Err(peios::Error::from_raw_os_error(libc::EACCES))
            } else {
                Ok(false)
            }
        });
        assert!(
            matches!(verdict, Readable::Unknown(ref reason) if reason.contains("for x ")),
            "expected Unknown naming the pattern, got {verdict:?}"
        );
    }

    #[test]
    fn eventd_clear_is_bit_1_value_0x0002() {
        assert_eq!(EVENTD_CLEAR, 0x0002);
        assert_eq!(EVENTD_CLEAR, 1 << 1);
        assert_eq!(
            RIGHTS
                .iter()
                .find(|right| right.mask == EVENTD_CLEAR)
                .map(|right| right.name),
            Some("Clear")
        );
    }

    #[test]
    fn generic_write_already_grants_eventd_clear() {
        assert_eq!(GENERIC_WRITE & EVENTD_CLEAR, EVENTD_CLEAR);
        assert_eq!(
            generic_mapping(),
            GenericMapping::new(GENERIC_READ, GENERIC_WRITE, GENERIC_EXECUTE, GENERIC_ALL),
            "and GENERIC_WRITE is what the mapping eventd checks with maps it to"
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
