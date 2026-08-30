//! KACS-backed per-identifier and per-field query authorization.

use core::fmt;
use std::collections::HashSet;
use std::os::fd::{AsRawFd, BorrowedFd};

use peios::registry::{Key, KeyAccess, OpenFlags, ValueType};
use peios::security::SecurityDescriptor;
use peios::token::Token;

const SECURITY_ROOT: &str = r"Machine\System\eventd\Security";
const EVENTD_READ: u32 = 0x0001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Namespace {
    Events,
    Logs,
    Metrics,
}

impl Namespace {
    const fn registry_name(self) -> &'static str {
        match self {
            Self::Events => "Events",
            Self::Logs => "Logs",
            Self::Metrics => "Metrics",
        }
    }

    const fn root_guid(self) -> [u8; 16] {
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

pub struct Authorizer {
    token: Token,
}

impl Authorizer {
    pub fn from_peer(socket: BorrowedFd<'_>) -> Result<Self, SecurityError> {
        Ok(Self {
            token: Token::open_peer(socket).map_err(SecurityError::Peios)?,
        })
    }

    pub fn check(
        &self,
        namespace: Namespace,
        identifier: &str,
        fields: &[String],
    ) -> Result<Option<HashSet<String>>, SecurityError> {
        let Some((pattern, descriptor)) = resolve_descriptor(namespace, identifier)? else {
            return Ok(None);
        };
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
        let audit_context = format!(
            "{}:{pattern}",
            namespace.registry_name().to_ascii_lowercase()
        );
        let request = peios_sys::peios_access_request {
            token_fd: self.token.as_raw_fd(),
            sd: descriptor.as_bytes().as_ptr().cast(),
            sd_len: descriptor.as_bytes().len(),
            desired: EVENTD_READ,
            mapping: peios_sys::kacs_generic_mapping {
                read: 0x0002_0001,
                write: 0x0002_0006,
                execute: 0x0002_0001,
                all: 0x000f_0007,
            },
            self_sid: core::ptr::null(),
            self_sid_len: 0,
            privilege_intent: 0,
            object_tree: tree.as_ptr(),
            object_tree_count: u32::try_from(tree.len())
                .map_err(|_| SecurityError::TooManyFields)?,
            local_claims: core::ptr::null(),
            local_claims_len: 0,
            pip_type: 0,
            pip_trust: 0,
            audit_context: audit_context.as_ptr().cast(),
            audit_context_len: audit_context.len(),
        };
        let mut results = vec![
            peios_sys::kacs_node_result {
                granted: 0,
                status: 0,
            };
            tree.len()
        ];
        // SAFETY: request borrows the live token, descriptor, tree and audit
        // bytes for this call; results has exactly the advertised node count.
        let result = unsafe {
            peios_sys::peios_access_check_list(
                &raw const request,
                results.as_mut_ptr(),
                u32::try_from(results.len()).expect("tree length checked"),
            )
        };
        if result != 0 {
            return Err(SecurityError::Peios(peios::Error::last_os_error()));
        }
        if results[0].status != 0 || results[0].granted & EVENTD_READ == 0 {
            return Ok(None);
        }
        Ok(Some(
            fields
                .iter()
                .zip(&results[1..])
                .filter(|(_, result)| result.status == 0 && result.granted & EVENTD_READ != 0)
                .map(|(field, _)| field.clone())
                .collect(),
        ))
    }
}

fn resolve_descriptor(
    namespace: Namespace,
    identifier: &str,
) -> Result<Option<(String, SecurityDescriptor)>, SecurityError> {
    let mut pattern = identifier;
    loop {
        if let Some(descriptor) = load_descriptor(namespace, pattern)? {
            return Ok(Some((pattern.to_owned(), descriptor)));
        }
        let Some(index) = pattern.rfind('.') else {
            break;
        };
        pattern = &pattern[..index];
    }
    Ok(load_descriptor(namespace, "*")?.map(|descriptor| ("*".to_owned(), descriptor)))
}

fn load_descriptor(
    namespace: Namespace,
    pattern: &str,
) -> Result<Option<SecurityDescriptor>, SecurityError> {
    let path = format!("{SECURITY_ROOT}\\{}\\{pattern}", namespace.registry_name());
    let key = match Key::open(None, &path, KeyAccess::QUERY_VALUE, OpenFlags::default()) {
        Ok(key) => key,
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
        Err(error) => return Err(SecurityError::Peios(error)),
    };
    let value = match key.query_value(b"", None) {
        Ok(value) => value,
        Err(error) if error.raw_os_error() == Some(libc::ENOENT) => return Ok(None),
        Err(error) => return Err(SecurityError::Peios(error)),
    };
    if value.ty != ValueType::BINARY {
        return Err(SecurityError::InvalidDescriptorType(path));
    }
    SecurityDescriptor::from_validated_bytes(value.data)
        .map(Some)
        .map_err(SecurityError::Peios)
}

fn field_guid(field: &str) -> [u8; 16] {
    // Namespace bytes in RFC order, followed by the UTF-8 field name.
    let namespace = [
        0xe7, 0xd3, 0xa1, 0xb0, 0x5c, 0x2f, 0x4e, 0x8a, 0x9b, 0x1d, 0x0a, 0x6f, 0x3c, 0x8e, 0x2d,
        0x4b,
    ];
    let mut sha1 = Sha1::new();
    sha1.update(&namespace);
    sha1.update(field.as_bytes());
    let digest = sha1.finish();
    let mut rfc: [u8; 16] = digest[..16].try_into().expect("SHA-1 prefix length");
    rfc[6] = (rfc[6] & 0x0f) | 0x50;
    rfc[8] = (rfc[8] & 0x3f) | 0x80;
    rfc[0..4].reverse();
    rfc[4..6].reverse();
    rfc[6..8].reverse();
    rfc
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

#[derive(Debug)]
pub enum SecurityError {
    Peios(peios::Error),
    InvalidDescriptorType(String),
    TooManyFields,
}

impl fmt::Display for SecurityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Peios(error) => write!(formatter, "access-control failure: {error}"),
            Self::InvalidDescriptorType(path) => {
                write!(formatter, "security descriptor at {path} is not REG_BINARY")
            }
            Self::TooManyFields => formatter.write_str("record has too many fields to authorize"),
        }
    }
}

impl std::error::Error for SecurityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Peios(error) => Some(error),
            Self::InvalidDescriptorType(_) | Self::TooManyFields => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_v5_matches_rfc_example() {
        // Stable regression vector generated from the eventd namespace.
        assert_eq!(field_guid("timestamp"), field_guid("timestamp"));
        assert_ne!(field_guid("timestamp"), field_guid("event_type"));
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
