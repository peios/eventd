//! Process-wide diagnostic counters and last-error snapshots.

use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{OnceLock, RwLock};
use std::time::{Duration, Instant};

use eventd_core::MetricTypeMismatch;

/// How often a rejected log origin may be reported on standard error.
/// A producer using an unaccepted origin usually uses it for every line
/// it sends, so the count is the interesting quantity and the text is
/// there to name the vocabulary that drifted.
const ORIGIN_REPORT_INTERVAL: Duration = Duration::from_secs(60);

/// The longest rejected origin reproduced in a report or a dump.
const ORIGIN_REPORT_LIMIT: usize = 64;

#[derive(Debug, Clone, Default)]
pub struct WriteErrors {
    pub event: Option<String>,
    pub log: Option<String>,
    pub metric: Option<String>,
    pub metadata: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LogIngress {
    pub rejected_origins: u64,
    pub last_rejected_origin: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricIngress {
    pub missing_identity: u64,
    pub truncated: u64,
    pub unauthorized_records: u64,
    pub authorization_errors: u64,
    pub type_mismatches: u64,
    pub last_type_mismatch: Option<MetricTypeMismatch>,
}

#[derive(Default)]
struct MetricTypeDiagnostics {
    total: u64,
    last: Option<MetricTypeMismatch>,
}

struct State {
    errors: RwLock<WriteErrors>,
    started: Instant,
    log_rejected_origins: AtomicU64,
    log_last_rejected_origin: RwLock<Option<String>>,
    log_origin_report_at: AtomicU64,
    metric_series: AtomicUsize,
    metric_missing_identity: AtomicU64,
    metric_truncated: AtomicU64,
    metric_unauthorized_records: AtomicU64,
    metric_authorization_errors: AtomicU64,
    metric_type_mismatches: RwLock<MetricTypeDiagnostics>,
}

static STATE: OnceLock<State> = OnceLock::new();

fn state() -> &'static State {
    STATE.get_or_init(|| State {
        errors: RwLock::new(WriteErrors::default()),
        started: Instant::now(),
        log_rejected_origins: AtomicU64::new(0),
        log_last_rejected_origin: RwLock::new(None),
        log_origin_report_at: AtomicU64::new(0),
        metric_series: AtomicUsize::new(0),
        metric_missing_identity: AtomicU64::new(0),
        metric_truncated: AtomicU64::new(0),
        metric_unauthorized_records: AtomicU64::new(0),
        metric_authorization_errors: AtomicU64::new(0),
        metric_type_mismatches: RwLock::new(MetricTypeDiagnostics::default()),
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

/// Record a log record discarded for its origin, and answer whether the
/// caller should also report it on standard error. The first rejection
/// reports, and thereafter one per `ORIGIN_REPORT_INTERVAL`; the count
/// in the diagnostic dump (§8.5) carries the rest.
pub fn log_rejected_origin(origin: &str) -> bool {
    let state = state();
    state.log_rejected_origins.fetch_add(1, Ordering::Relaxed);
    *state
        .log_last_rejected_origin
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(displayable_origin(origin));
    let now = u64::try_from(state.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let due = state.log_origin_report_at.load(Ordering::Relaxed);
    if now < due {
        return false;
    }
    let next = now.saturating_add(
        u64::try_from(ORIGIN_REPORT_INTERVAL.as_nanos()).unwrap_or(u64::MAX),
    );
    state
        .log_origin_report_at
        .compare_exchange(due, next, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

/// A rejected origin is unvalidated producer text, so it is escaped
/// before it reaches a terminal and truncated before it reaches a log
/// line of its own.
pub fn displayable_origin(origin: &str) -> String {
    let mut escaped: String = origin.escape_debug().take(ORIGIN_REPORT_LIMIT).collect();
    if origin.escape_debug().nth(ORIGIN_REPORT_LIMIT).is_some() {
        escaped.push('…');
    }
    escaped
}

pub fn metric_series(occupancy: usize) {
    state().metric_series.store(occupancy, Ordering::Release);
}

pub fn metric_missing_identity() {
    state()
        .metric_missing_identity
        .fetch_add(1, Ordering::Relaxed);
}

pub fn metric_truncated() {
    state().metric_truncated.fetch_add(1, Ordering::Relaxed);
}

pub fn metric_unauthorized(count: usize) {
    if count == 0 {
        return;
    }
    state()
        .metric_unauthorized_records
        .fetch_add(u64::try_from(count).unwrap_or(u64::MAX), Ordering::Relaxed);
}

pub fn metric_authorization_error() {
    state()
        .metric_authorization_errors
        .fetch_add(1, Ordering::Relaxed);
}

pub fn metric_type_mismatches(count: usize, last: Option<&MetricTypeMismatch>) {
    if count == 0 {
        return;
    }
    let mut diagnostics = state()
        .metric_type_mismatches
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    diagnostics.total = diagnostics
        .total
        .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
    if let Some(last) = last {
        diagnostics.last = Some(last.clone());
    }
}

pub fn snapshot() -> (usize, LogIngress, MetricIngress, WriteErrors) {
    let state = state();
    let (type_mismatches, last_type_mismatch) = {
        let diagnostics = state
            .metric_type_mismatches
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (diagnostics.total, diagnostics.last.clone())
    };
    (
        state.metric_series.load(Ordering::Acquire),
        LogIngress {
            rejected_origins: state.log_rejected_origins.load(Ordering::Relaxed),
            last_rejected_origin: state
                .log_last_rejected_origin
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        },
        MetricIngress {
            missing_identity: state.metric_missing_identity.load(Ordering::Relaxed),
            truncated: state.metric_truncated.load(Ordering::Relaxed),
            unauthorized_records: state.metric_unauthorized_records.load(Ordering::Relaxed),
            authorization_errors: state.metric_authorization_errors.load(Ordering::Relaxed),
            type_mismatches,
            last_type_mismatch,
        },
        state
            .errors
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone(),
    )
}
