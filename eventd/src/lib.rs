//! KMES attachment and eventd process lifecycle.

use std::error::Error;

mod commit_signal;
mod config;
mod datagram;
mod diagnostics;
mod directory;
mod indexing;
mod kmes;
mod log_ingest;
mod metric_ingest;
mod pipeline;
mod query;
mod query_language;
mod retention;
mod synthetic;
mod write_security;
mod writer;

/// Start eventd.
///
/// The live process wiring is deliberately kept out of `main` so lifecycle
/// behavior is testable as it is added.
pub fn run() -> Result<(), Box<dyn Error>> {
    pipeline::run()
}

/// Initialize registry query policy from a short privileged pre-start hook.
pub fn prepare_security() -> Result<(), Box<dyn Error>> {
    query::provision_security_defaults().map_err(Into::into)
}
