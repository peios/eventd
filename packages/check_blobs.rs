//! Verify the uncompressed SHA-256 identity of every blob in a store.

use std::{fs, io::Read, path::PathBuf};

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};

const SHA256_HEX_LENGTH: usize = 64;
// Amortize decoder reads without buffering a complete package blob.
const DECODE_BUFFER_SIZE_BYTES: usize = 64 * 1024;

fn main() -> Result<()> {
    let directory = PathBuf::from(
        std::env::args_os()
            .nth(1)
            .context("expected blob directory")?,
    );
    let mut count = 0;
    for item in fs::read_dir(directory)? {
        let item = item?;
        let name = item
            .file_name()
            .into_string()
            .map_err(|_| anyhow::anyhow!("non-UTF8 blob name"))?;
        let expected = name
            .strip_suffix(".zst")
            .context("unexpected blob filename")?;
        if expected.len() != SHA256_HEX_LENGTH
            || !expected
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            bail!("invalid blob filename: {name}");
        }
        let mut decoder = zstd::stream::read::Decoder::new(fs::File::open(item.path())?)?;
        let mut hash = Sha256::new();
        let mut buffer = [0u8; DECODE_BUFFER_SIZE_BYTES];
        loop {
            let length = decoder
                .read(&mut buffer)
                .with_context(|| format!("decoding {name}"))?;
            if length == 0 {
                break;
            }
            hash.update(&buffer[..length]);
        }
        if format!("{:x}", hash.finalize()) != expected {
            bail!("blob hash mismatch: {name}");
        }
        count += 1;
    }
    println!("verified {count} content-addressed blobs");
    Ok(())
}
