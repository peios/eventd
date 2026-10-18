use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "ppk", version, about = "Peios package tool")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Commit a directory as a package, writing content-addressed blobs and a
    /// manifest.
    ///
    /// Prints the manifest hash, which is the package's identity.
    Commit {
        /// Package name.
        name: String,
        /// Package version.
        version: String,
        /// Architecture the content is built for.
        arch: String,
        /// Directory whose contents become the package's files.
        #[arg(short, long)]
        content: PathBuf,
        /// Where to write blobs/ and manifests/.
        #[arg(short, long, default_value = "dist")]
        output: PathBuf,
    },
}

fn main() -> Result<()> {
    match Cli::parse().command {
        Command::Commit {
            name,
            version,
            arch,
            content,
            output,
        } => {
            let pkg = ppk::Package {
                name: &name,
                version: &version,
                arch: &arch,
            };
            let commit = ppk::commit(&pkg, &content, &output)?;
            eprintln!(
                "committed {} entries ({} new blobs) to {}",
                commit.entries,
                commit.blobs_written,
                commit.manifest_path.display()
            );
            println!("{}", commit.manifest_hash);
        }
    }
    Ok(())
}
