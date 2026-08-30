//! Process-wide diagnostic counters and last-error snapshots.

use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{OnceLock, RwLock};

#[derive(Debug, Clone, Default)]
pub struct WriteErrors {
    pub event: Option<String>,
    pub log: Option<String>,
    pub metric: Option<String>,
    pub metadata: Option<String>,
}

struct State {
    errors: RwLock<WriteErrors>,
    metric_series: AtomicUsize,
}

static STATE: OnceLock<State> = OnceLock::new();

fn state() -> &'static State {
    STATE.get_or_init(|| State {
        errors: RwLock::new(WriteErrors::default()),
        metric_series: AtomicUsize::new(0),
    })
}

pub fn event_error(error: &impl std::fmt::Display) {
    state()
        .errors
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .event = Some(error.to_string());
}

pub fn log_error(error: &impl std::fmt::Display) {
    state()
        .errors
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .log = Some(error.to_string());
}

pub fn metric_error(error: &impl std::fmt::Display) {
    state()
        .errors
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .metric = Some(error.to_string());
}

pub fn metadata_error(error: &impl std::fmt::Display) {
    state()
        .errors
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .metadata = Some(error.to_string());
}

pub fn metric_series(occupancy: usize) {
    state().metric_series.store(occupancy, Ordering::Release);
}

pub fn snapshot() -> (usize, WriteErrors) {
    (
        state().metric_series.load(Ordering::Acquire),
        state()
            .errors
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
}
