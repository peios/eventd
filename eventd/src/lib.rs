//! KMES attachment and eventd process lifecycle.

use std::error::Error;

mod commit_signal;
mod config;
mod datagram;
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
mod writer;

/// Start eventd.
///
/// The live process wiring is deliberately kept out of `main` so lifecycle
/// behavior is testable as it is added.
pub fn run() -> Result<(), Box<dyn Error>> {
    pipeline::run()
}
