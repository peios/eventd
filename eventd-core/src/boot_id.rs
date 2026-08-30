//! Linux boot-ID parsing and RFC UUID to PCDS GUID conversion.

use core::fmt;
use std::fs;
use std::io;
use std::path::Path;

use crate::Guid;

/// A validated kernel boot ID in PCDS GUID byte layout.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct BootId(Guid);

impl BootId {
    /// Read `/proc/sys/kernel/random/boot_id`.
    pub fn read_kernel() -> Result<Self, BootIdError> {
        Self::read_from("/proc/sys/kernel/random/boot_id")
    }

    /// Read and parse a boot ID from `path`.
    pub fn read_from(path: impl AsRef<Path>) -> Result<Self, BootIdError> {
        let text = fs::read_to_string(path).map_err(BootIdError::Read)?;
        text.trim().parse()
    }

    /// Return the PCDS binary representation stored by eventd.
    #[must_use]
    pub const fn into_bytes(self) -> Guid {
        self.0
    }

    /// Borrow the PCDS binary representation.
    #[must_use]
    pub const fn as_bytes(&self) -> &Guid {
        &self.0
    }
}

impl core::str::FromStr for BootId {
    type Err = BootIdError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        if text.len() != 36 {
            return Err(BootIdError::Malformed);
        }
        for index in [8, 13, 18, 23] {
            if text.as_bytes()[index] != b'-' {
                return Err(BootIdError::Malformed);
            }
        }

        let mut rfc = [0_u8; 16];
        let mut output = 0;
        let mut high = None;
        for byte in text.bytes() {
            if byte == b'-' {
                continue;
            }
            let nibble = hex_nibble(byte).ok_or(BootIdError::Malformed)?;
            if let Some(value) = high.take() {
                rfc[output] = (value << 4) | nibble;
                output += 1;
            } else {
                high = Some(nibble);
            }
        }
        if output != rfc.len() || high.is_some() {
            return Err(BootIdError::Malformed);
        }

        // PCDS follows the DCE/MS GUID memory layout: the first three numeric
        // fields are little-endian, while Data4 retains RFC byte order.
        rfc[0..4].reverse();
        rfc[4..6].reverse();
        rfc[6..8].reverse();
        Ok(Self(rfc))
    }
}

impl fmt::Debug for BootId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("BootId").field(&self.0).finish()
    }
}

const fn hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

/// Failure to obtain or validate the kernel boot ID.
#[derive(Debug)]
pub enum BootIdError {
    /// The boot-ID file could not be read.
    Read(io::Error),
    /// The content was not a canonical hyphenated UUID.
    Malformed,
}

impl fmt::Display for BootIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Read(error) => write!(formatter, "cannot read kernel boot ID: {error}"),
            Self::Malformed => formatter.write_str("kernel boot ID is not a canonical UUID"),
        }
    }
}

impl std::error::Error for BootIdError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Read(error) => Some(error),
            Self::Malformed => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_rfc_text_to_pcds_layout() {
        let id: BootId = "00112233-4455-6677-8899-aabbccddeeff".parse().unwrap();
        assert_eq!(
            id.into_bytes(),
            [
                0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff,
            ]
        );
    }

    #[test]
    fn rejects_noncanonical_text() {
        assert!(
            "00112233445566778899aabbccddeeff"
                .parse::<BootId>()
                .is_err()
        );
        assert!(
            "00112233-4455-6677-8899-aabbccddeefg"
                .parse::<BootId>()
                .is_err()
        );
    }
}
