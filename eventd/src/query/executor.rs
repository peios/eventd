//! Read-only query execution over event, log and metric stores.

mod time;

use self::time::time_range;

use core::cmp::Ordering;
use core::fmt;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use eventd_core::MetricRollup;
use rusqlite::{Connection, OpenFlags, params};

use super::security::{Authorizer, Namespace};
use super::value::{
    Record, Value, ascii_equal, flatten_event_payload, guid_string, is_nan, language_cmp,
};
use crate::metric_ingest::{RollupMaintenance, RollupSender};
use crate::query_language::{
    AggregateFunction, CrossFilter, Expr, GroupFunction, Literal, MetricAggregate, Operator, Query,
    RecordAggregate, Source, Transform,
};

pub struct Stores {
    pub event_paths: Vec<PathBuf>,
    pub log_path: PathBuf,
    pub metric_path: PathBuf,
}

pub struct Limits {
    pub deadline: Instant,
    pub cross_type_window: Duration,
    pub cross_type_max_lookback: Duration,
    pub rollups: Option<RollupSender>,
    pub adaptive_rollup_min_samples: usize,
    pub adaptive_rollup_batch_rows: usize,
    pub adaptive_rollup_max_rows: usize,
    pub held: HeldBudget,
}

/// The memory every running query together may hold to answer
/// (`MaxQueryHeldBytes`), and how much of it is taken. One budget for all
/// of them, like the concurrency limits, because eventd may not be killed:
/// what bounds it must hold however many queries run (TRM §6.5).
#[derive(Clone)]
pub struct HeldBudget {
    pub used: Arc<AtomicUsize>,
    pub limit: usize,
}

pub struct StreamState {
    evaluation_time: i64,
    cursor: StoreCursor,
    cross_type_window: Duration,
    cross_type_max_lookback: Duration,
    authorization: AuthorizationCache,
    held: HeldBudget,
}

#[derive(Clone)]
struct StoreCursor {
    event_ids: Vec<i64>,
    log_id: i64,
}

/// Where a query's result records go, one at a time and in result order.
/// A query in the default order hands each record over as it is read, so
/// nothing but the merge frontier is held (TRM §6.4).
pub type Emit<'a> = dyn FnMut(Record) -> Result<(), QueryError> + 'a;

pub fn execute(
    query: &Query,
    stores: &Stores,
    authorizer: &Authorizer,
    limits: &Limits,
    emit: &mut Emit<'_>,
) -> Result<(), QueryError> {
    let evaluation_time = realtime_nanoseconds()?;
    let mut authorization = AuthorizationCache::new(authorizer);
    execute_at(
        query,
        stores,
        authorizer,
        evaluation_time,
        Some(limits.deadline),
        &StoreCursor {
            event_ids: vec![i64::MAX; stores.event_paths.len()],
            log_id: i64::MAX,
        },
        &StoreCursor {
            event_ids: vec![0; stores.event_paths.len()],
            log_id: 0,
        },
        false,
        limits.cross_type_window,
        limits.cross_type_max_lookback,
        &mut authorization,
        Some(limits),
        &limits.held,
        emit,
    )
}

pub fn start_stream(
    query: &Query,
    stores: &Stores,
    authorizer: &Authorizer,
    limits: &Limits,
    emit: &mut Emit<'_>,
) -> Result<StreamState, QueryError> {
    let evaluation_time = realtime_nanoseconds()?;
    let mut authorization = AuthorizationCache::new(authorizer);
    let cursor = capture_cursor(stores, &query.source, Some(limits.deadline))?;
    let lower = StoreCursor {
        event_ids: vec![0; stores.event_paths.len()],
        log_id: 0,
    };
    execute_at(
        query,
        stores,
        authorizer,
        evaluation_time,
        Some(limits.deadline),
        &cursor,
        &lower,
        false,
        limits.cross_type_window,
        limits.cross_type_max_lookback,
        &mut authorization,
        Some(limits),
        &limits.held,
        emit,
    )?;
    Ok(StreamState {
        evaluation_time,
        cursor,
        cross_type_window: limits.cross_type_window,
        cross_type_max_lookback: limits.cross_type_max_lookback,
        authorization,
        held: limits.held.clone(),
    })
}

pub fn stream_next(
    state: &mut StreamState,
    query: &Query,
    stores: &Stores,
    authorizer: &Authorizer,
) -> Result<Vec<Record>, QueryError> {
    let upper = capture_cursor(stores, &query.source, None)?;
    if upper.event_ids == state.cursor.event_ids && upper.log_id == state.cursor.log_id {
        return Ok(Vec::new());
    }
    let mut watch_query = query.clone();
    watch_query.sort.clear();
    watch_query.take = None;
    watch_query.skip = 0;
    // The records committed between the two captured cursors.
    let mut records = Vec::new();
    execute_at(
        &watch_query,
        stores,
        authorizer,
        state.evaluation_time,
        None,
        &upper,
        &state.cursor,
        true,
        state.cross_type_window,
        state.cross_type_max_lookback,
        &mut state.authorization,
        None,
        &state.held,
        &mut |record| {
            records.push(record);
            Ok(())
        },
    )?;
    state.cursor = upper;
    Ok(records)
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the shared query pipeline keeps filtering and output stages in their mandated order"
)]
fn execute_at(
    query: &Query,
    stores: &Stores,
    authorizer: &Authorizer,
    evaluation_time: i64,
    deadline: Option<Instant>,
    upper: &StoreCursor,
    lower: &StoreCursor,
    watch: bool,
    cross_type_window: Duration,
    cross_type_max_lookback: Duration,
    authorization: &mut AuthorizationCache,
    rollup_limits: Option<&Limits>,
    budget: &HeldBudget,
    emit: &mut Emit<'_>,
) -> Result<(), QueryError> {
    let (since, mut until) = time_range(query, evaluation_time)?;
    if watch {
        until = i64::MAX;
    }
    if since >= until {
        return Ok(());
    }
    let historical_ranges = if watch || query.cross_filters.is_empty() {
        None
    } else {
        let maximum = duration_nanoseconds(cross_type_max_lookback);
        if until.saturating_sub(since) > maximum {
            return Err(QueryError::CrossTypeRangeTooLarge);
        }
        Some(cross_type_ranges(
            &query.cross_filters,
            stores,
            authorizer,
            since,
            until,
            duration_nanoseconds(cross_type_window),
            deadline,
        )?)
    };
    let referenced = referenced_fields(query);
    let (namespace, allowed) = match &query.source {
        Source::Events { pattern } => {
            let identifiers =
                discover_event_identifiers(&stores.event_paths, pattern.as_deref(), deadline)?;
            let allowed = authorize_identifiers(
                authorizer,
                Namespace::Events,
                identifiers,
                &referenced,
                authorization,
            )?;
            (Namespace::Events, allowed)
        }
        Source::Logs { origins, .. } => {
            let identifiers = discover_log_identifiers(&stores.log_path, origins, deadline)?;
            let allowed = authorize_identifiers(
                authorizer,
                Namespace::Logs,
                identifiers,
                &referenced,
                authorization,
            )?;
            (Namespace::Logs, allowed)
        }
        Source::Metric { name, labels } => {
            return execute_metric(
                query,
                &stores.metric_path,
                name,
                labels.as_deref(),
                since,
                until,
                authorizer,
                deadline,
                historical_ranges.as_deref(),
                authorization,
                rollup_limits,
                budget,
                emit,
            );
        }
    };

    // A query in the default order holds nothing but the merge frontier:
    // every store is read newest first, and each record goes to `emit` as
    // soon as it has passed access control and every predicate, until TAKE
    // is met (TRM §6.4). Anything else needs the whole visible set first.
    let newest_first = !watch && query.aggregate.is_none() && query.sort.is_empty();
    if newest_first && query.take == Some(0) {
        return Ok(());
    }
    let order = if newest_first {
        ScanOrder::Newest
    } else {
        ScanOrder::Stored
    };
    let mut skip = query.skip;
    let mut remaining = query.take;
    // What the other queries hold: an aggregation folds its groups as rows
    // pass, and a sorted one keeps its rows, or with TAKE only its best
    // SKIP + TAKE. A watch batch is gathered whole, because its cross-type
    // conditions are judged on the batch (PSPU §3.27); several commits
    // may have become visible since the previous cursor.
    let mut held = Held::new(budget);
    let mut gathered = match &query.aggregate {
        Some(aggregate) if !watch => Gathered::Groups(Groups::new(aggregate)),
        _ => Gathered::Rows(Vec::new()),
    };
    let keep = if watch {
        None
    } else {
        query
            .take
            .and_then(|take| usize::try_from(take.saturating_add(query.skip)).ok())
    };
    let predicates = if matches!(query.source, Source::Logs { .. }) {
        log_predicates(&query.predicates)
    } else {
        query.predicates.clone()
    };
    let mut visit = |mut row: Row| -> Result<Flow, QueryError> {
        if historical_ranges
            .as_deref()
            .is_some_and(|ranges| !range_contains(ranges, timestamp(&row)))
        {
            return Ok(Flow::More);
        }
        if !authorize_row(authorizer, namespace, &mut row, &referenced, authorization)?
            || !predicates
                .iter()
                .all(|predicate| evaluate(namespace, predicate, &row.record))
        {
            return Ok(Flow::More);
        }
        if !newest_first {
            gathered.add(row, query, keep, &mut held)?;
            return Ok(Flow::More);
        }
        if skip > 0 {
            skip -= 1;
            return Ok(Flow::More);
        }
        emit(project(row.record, &query.select))?;
        let Some(left) = remaining.as_mut() else {
            return Ok(Flow::More);
        };
        *left -= 1;
        Ok(if *left == 0 { Flow::Enough } else { Flow::More })
    };
    let time_field = match query.source {
        Source::Events { .. } => "event.time",
        Source::Logs { .. } | Source::Metric { .. } => "timestamp",
    };
    let (scan_since, scan_until) = narrow_by_timestamp(&query.predicates, time_field, since, until);
    match &query.source {
        Source::Events { .. } => scan_events(
            &stores.event_paths,
            &allowed,
            &query.predicates,
            scan_since,
            scan_until,
            deadline,
            &lower.event_ids,
            &upper.event_ids,
            order,
            &mut visit,
        )?,
        Source::Logs {
            error_only,
            containing,
            ..
        } => scan_logs(
            &stores.log_path,
            &allowed,
            *error_only,
            containing.as_deref(),
            scan_since,
            scan_until,
            deadline,
            lower.log_id,
            upper.log_id,
            order,
            &mut visit,
        )?,
        Source::Metric { .. } => unreachable!("metric queries returned above"),
    }
    if newest_first {
        return Ok(());
    }
    let mut rows = match gathered {
        Gathered::Groups(groups) => return emit_aggregate(groups.finish()?, query, emit),
        Gathered::Rows(rows) => rows,
    };

    if watch && !query.cross_filters.is_empty() {
        apply_watch_cross_filters(
            &mut rows,
            &query.cross_filters,
            stores,
            authorizer,
            duration_nanoseconds(cross_type_window),
            deadline,
        )?;
    }

    if watch {
        order_result_rows(&mut rows, query, true);
        return rows.into_iter().try_for_each(|row| {
            let record = if let Some(RecordAggregate::Distinct(field)) = &query.aggregate {
                // The stream handler owns the cross-batch seen set. Give it
                // values in record order, retaining the first representation.
                let value = row.record.get(field).cloned().unwrap_or(Value::Null);
                BTreeMap::from([(field.clone(), value)])
            } else {
                project(row.record, &query.select)
            };
            emit(record)
        });
    }
    if let Some(aggregate) = &query.aggregate {
        let mut groups = Groups::new(aggregate);
        for row in &rows {
            groups.add(row, &mut held)?;
        }
        return emit_aggregate(groups.finish()?, query, emit);
    }
    order_result_rows(&mut rows, query, false);
    rows.into_iter()
        .try_for_each(|row| emit(project(row.record, &query.select)))
}

fn order_result_rows(rows: &mut Vec<Row>, query: &Query, watch: bool) {
    if watch {
        // Shards commit independently. Preserve insertion order within each
        // shard, with a deterministic shard order inside an observed batch.
        rows.sort_unstable_by_key(|row| row.tie);
    } else {
        sort_rows(rows, query);
        apply_pagination(rows, query);
    }
}

fn emit_aggregate(
    mut records: Vec<Row>,
    query: &Query,
    emit: &mut Emit<'_>,
) -> Result<(), QueryError> {
    apply_record_sort(&mut records, query);
    apply_pagination(&mut records, query);
    records.into_iter().try_for_each(|row| emit(row.record))
}

/// The visible rows a query that is not in the default order gathers.
enum Gathered<'q> {
    Rows(Vec<Row>),
    Groups(Groups<'q>),
}

/// Rows a sorted query gathers before it trims to its best SKIP + TAKE.
const TRIM_AT: usize = 1_024;

impl Gathered<'_> {
    fn add(
        &mut self,
        row: Row,
        query: &Query,
        keep: Option<usize>,
        held: &mut Held<'_>,
    ) -> Result<(), QueryError> {
        match self {
            Self::Groups(groups) => groups.add(&row, held),
            Self::Rows(rows) => {
                held.add(row_size(&row))?;
                rows.push(row);
                if let Some(keep) = keep
                    && rows.len() >= keep.saturating_mul(2).max(TRIM_AT)
                {
                    sort_rows(rows, query);
                    rows.truncate(keep);
                    held.set(rows.iter().map(row_size).sum())?;
                }
                Ok(())
            }
        }
    }
}

/// What one query holds against the budget every running query shares.
/// It is reserved in granules, so that the shared counter is touched only
/// now and then, and given back when the query is done.
struct Held<'a> {
    budget: &'a HeldBudget,
    reserved: usize,
    holding: usize,
}

const HELD_GRANULE: usize = 64 * 1_024;

impl<'a> Held<'a> {
    const fn new(budget: &'a HeldBudget) -> Self {
        Self {
            budget,
            reserved: 0,
            holding: 0,
        }
    }

    fn add(&mut self, bytes: usize) -> Result<(), QueryError> {
        self.set(self.holding.saturating_add(bytes))
    }

    /// Give back what is no longer held. Shrinking cannot fail.
    fn release(&mut self, bytes: usize) {
        let _ = self.set(self.holding.saturating_sub(bytes));
    }

    fn set(&mut self, bytes: usize) -> Result<(), QueryError> {
        self.holding = bytes;
        let wanted = bytes.div_ceil(HELD_GRANULE).saturating_mul(HELD_GRANULE);
        if wanted > self.reserved {
            let more = wanted - self.reserved;
            let limit = self.budget.limit;
            self.budget
                .used
                .fetch_update(AtomicOrdering::AcqRel, AtomicOrdering::Acquire, |used| {
                    used.checked_add(more).filter(|total| *total <= limit)
                })
                .map_err(|_| QueryError::HeldLimit)?;
        } else if wanted < self.reserved {
            self.budget
                .used
                .fetch_sub(self.reserved - wanted, AtomicOrdering::AcqRel);
        }
        self.reserved = wanted;
        Ok(())
    }
}

impl Drop for Held<'_> {
    fn drop(&mut self) {
        self.budget
            .used
            .fetch_sub(self.reserved, AtomicOrdering::AcqRel);
    }
}

/// What the allocator takes for `len` bytes: rounded up to 16, plus a
/// header.
const fn heap(len: usize) -> usize {
    if len == 0 {
        0
    } else {
        len.div_ceil(16) * 16 + 16
    }
}

/// What a value owns beyond its own slot.
fn value_heap(value: &Value) -> usize {
    match value {
        Value::String(text) => heap(text.len()),
        Value::Binary(bytes) => heap(bytes.len()),
        Value::Array(values) => {
            heap(values.len() * size_of::<Value>()) + values.iter().map(value_heap).sum::<usize>()
        }
        Value::Map(entries) => {
            heap(entries.len() * 2 * size_of::<Value>())
                + entries
                    .iter()
                    .map(|(key, value)| value_heap(key) + value_heap(value))
                    .sum::<usize>()
        }
        Value::Null | Value::Bool(_) | Value::Signed(_) | Value::Unsigned(_) | Value::Float(_) => 0,
    }
}

/// Roughly what a value costs held in memory.
fn value_size(value: &Value) -> usize {
    size_of::<Value>() + value_heap(value)
}

/// One record field's share of its map's B-tree: a name slot and a value
/// slot, in nodes that run about half to two thirds full, with the
/// nodes' own links and lengths.
const FIELD_SLOT: usize = (size_of::<String>() + size_of::<Value>()) * 2;

/// Roughly what a row costs held in memory: its fields' slots, and what
/// the allocator takes for each name and each text or binary value. On
/// the dev VM, a sort gathering event rows until the budget refused it
/// grew eventd by 1.07 to 1.12 times the budget.
fn row_size(row: &Row) -> usize {
    size_of::<Row>()
        + heap(row.identifier.len())
        + heap(row.metric_labels.len())
        + record_size(&row.record)
}

fn record_size(record: &Record) -> usize {
    record
        .iter()
        .map(|(field, value)| FIELD_SLOT + heap(field.len()) + value_heap(value))
        .sum::<usize>()
}

/// The time range the stores are read over, narrowed by every top-level
/// comparison of the record's time, `time_field`, with an integer. The
/// predicate is still evaluated against each row, so this narrows and
/// never decides (TRM §6.3); what it buys is that a page of older records,
/// `WHERE event.time <= T`, starts reading at T instead of at the newest
/// record.
fn narrow_by_timestamp(
    predicates: &[Expr],
    time_field: &str,
    mut since: i64,
    mut until: i64,
) -> (i64, i64) {
    for predicate in predicates {
        let Expr::Compare {
            field,
            operator,
            value,
        } = predicate
        else {
            continue;
        };
        if field != time_field {
            continue;
        }
        let bound = match value {
            Literal::Signed(value) => i128::from(*value),
            Literal::Unsigned(value) => i128::from(*value),
            _ => continue,
        };
        // Out of the i64 domain on the far side, a bound excludes nothing;
        // on the near side, everything.
        let clamp = |value: i128| {
            i64::try_from(value).unwrap_or(if value < 0 { i64::MIN } else { i64::MAX })
        };
        let (lower, upper) = match operator {
            Operator::Less => (None, Some(bound)),
            Operator::LessEqual => (None, Some(bound + 1)),
            Operator::Greater => (Some(bound + 1), None),
            Operator::GreaterEqual => (Some(bound), None),
            Operator::Equal => (Some(bound), Some(bound + 1)),
            _ => (None, None),
        };
        if let Some(lower) = lower {
            since = since.max(clamp(lower));
        }
        if let Some(upper) = upper.filter(|upper| *upper <= i128::from(i64::MAX)) {
            until = until.min(clamp(upper));
        }
    }
    (since, until)
}

/// A result record narrowed to SELECT's fields, which is applied last.
fn project(mut record: Record, select: &[String]) -> Record {
    if !select.is_empty() {
        record.retain(|field, _| select.contains(field));
    }
    record
}

fn capture_cursor(
    stores: &Stores,
    source: &Source,
    deadline: Option<Instant>,
) -> Result<StoreCursor, QueryError> {
    let mut cursor = StoreCursor {
        event_ids: vec![0; stores.event_paths.len()],
        log_id: 0,
    };
    match source {
        Source::Events { .. } => {
            for (index, path) in stores.event_paths.iter().enumerate() {
                cursor.event_ids[index] = open_read_only(path, deadline)?.query_row(
                    "SELECT COALESCE(MAX(id), 0) FROM events",
                    [],
                    |row| row.get(0),
                )?;
            }
        }
        Source::Logs { .. } => {
            cursor.log_id = open_read_only(&stores.log_path, deadline)?.query_row(
                "SELECT COALESCE(MAX(id), 0) FROM logs",
                [],
                |row| row.get(0),
            )?;
        }
        Source::Metric { .. } => unreachable!("metric streams are rejected by the parser"),
    }
    Ok(cursor)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimeRange {
    start: i64,
    end: i64,
}

fn duration_nanoseconds(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX)
}

fn discover_event_identifiers(
    paths: &[PathBuf],
    pattern: Option<&str>,
    deadline: Option<Instant>,
) -> Result<HashSet<String>, QueryError> {
    let mut identifiers = HashSet::new();
    for path in paths {
        check_deadline(deadline)?;
        let connection = open_read_only(path, deadline)?;
        let mut statement = connection.prepare("SELECT event_type FROM event_types")?;
        let mut rows = statement.query([])?;
        while let Some(row) = rows.next()? {
            if identifiers.len().is_multiple_of(1_024) {
                check_deadline(deadline)?;
            }
            let identifier: String = row.get(0)?;
            if pattern.is_none_or(|pattern| glob_matches(pattern, &identifier)) {
                identifiers.insert(identifier);
            }
        }
    }
    Ok(identifiers)
}

fn discover_log_identifiers(
    path: &Path,
    origins: &[String],
    deadline: Option<Instant>,
) -> Result<HashSet<String>, QueryError> {
    check_deadline(deadline)?;
    let connection = open_read_only(path, deadline)?;
    let mut statement = connection.prepare("SELECT origin FROM log_origins")?;
    let mut rows = statement.query([])?;
    let mut identifiers = HashSet::new();
    while let Some(row) = rows.next()? {
        if identifiers.len().is_multiple_of(1_024) {
            check_deadline(deadline)?;
        }
        let identifier: String = row.get(0)?;
        if origins.is_empty()
            || origins
                .iter()
                .any(|origin| ascii_equal(origin, &identifier))
        {
            identifiers.insert(identifier);
        }
    }
    Ok(identifiers)
}

fn discover_metric_identifiers(
    connection: &Connection,
    pattern: &str,
    deadline: Option<Instant>,
) -> Result<HashSet<String>, QueryError> {
    let mut statement = connection.prepare("SELECT DISTINCT name FROM series")?;
    let mut rows = statement.query([])?;
    let mut identifiers = HashSet::new();
    while let Some(row) = rows.next()? {
        if identifiers.len().is_multiple_of(1_024) {
            check_deadline(deadline)?;
        }
        let identifier: String = row.get(0)?;
        if glob_matches(pattern, &identifier) {
            identifiers.insert(identifier);
        }
    }
    Ok(identifiers)
}

fn authorize_identifiers(
    authorizer: &Authorizer,
    namespace: Namespace,
    identifiers: HashSet<String>,
    fields: &[String],
    cache: &mut AuthorizationCache,
) -> Result<HashSet<String>, QueryError> {
    let mut identifiers: Vec<_> = identifiers.into_iter().collect();
    identifiers.sort_unstable();
    loop {
        cache.refresh(authorizer);
        let generation = authorizer.descriptor_generation();
        let mut allowed = HashSet::with_capacity(identifiers.len());
        for identifier in &identifiers {
            if cache.may_read(authorizer, namespace, identifier, fields)? {
                allowed.insert(identifier.clone());
            }
        }
        if authorizer.descriptor_generation() == generation {
            return Ok(allowed);
        }
    }
}

fn cross_type_ranges(
    filters: &[CrossFilter],
    stores: &Stores,
    authorizer: &Authorizer,
    since: i64,
    until: i64,
    window: i64,
    deadline: Option<Instant>,
) -> Result<Vec<TimeRange>, QueryError> {
    let mut combined = vec![TimeRange {
        start: since,
        end: until,
    }];
    for filter in filters {
        check_deadline(deadline)?;
        let ranges =
            cross_filter_ranges(filter, stores, authorizer, since, until, window, deadline)?;
        combined = intersect_ranges(&combined, &ranges);
        if combined.is_empty() {
            break;
        }
    }
    Ok(combined)
}

fn cross_filter_ranges(
    filter: &CrossFilter,
    stores: &Stores,
    authorizer: &Authorizer,
    since: i64,
    until: i64,
    window: i64,
    deadline: Option<Instant>,
) -> Result<Vec<TimeRange>, QueryError> {
    match filter {
        CrossFilter::Metric {
            name,
            labels,
            operator,
            value,
        } => metric_true_ranges(
            &stores.metric_path,
            name,
            labels.as_deref(),
            *operator,
            value,
            authorizer,
            since,
            until,
            deadline,
        ),
        CrossFilter::EventExists { pattern } => {
            let (lower, upper) = split_window(window);
            let timestamps = cross_event_timestamps(
                &stores.event_paths,
                pattern,
                since.saturating_sub(upper),
                until.saturating_add(lower),
                authorizer,
                deadline,
            )?;
            Ok(existence_ranges(&timestamps, since, until, lower, upper))
        }
        CrossFilter::LogExists { origin, containing } => {
            let (lower, upper) = split_window(window);
            let timestamps = cross_log_timestamps(
                &stores.log_path,
                origin,
                containing.as_deref(),
                since.saturating_sub(upper),
                until.saturating_add(lower),
                authorizer,
                deadline,
            )?;
            Ok(existence_ranges(&timestamps, since, until, lower, upper))
        }
    }
}

fn apply_watch_cross_filters(
    rows: &mut Vec<Row>,
    filters: &[CrossFilter],
    stores: &Stores,
    authorizer: &Authorizer,
    window: i64,
    deadline: Option<Instant>,
) -> Result<(), QueryError> {
    if rows.is_empty() {
        return Ok(());
    }
    let first = rows.iter().map(timestamp).min().expect("rows is non-empty");
    let last = rows.iter().map(timestamp).max().expect("rows is non-empty");
    for filter in filters {
        check_deadline(deadline)?;
        match filter {
            CrossFilter::Metric {
                name,
                labels,
                operator,
                value,
            } => {
                if !metric_condition_at(
                    &stores.metric_path,
                    name,
                    labels.as_deref(),
                    *operator,
                    value,
                    authorizer,
                    last,
                    deadline,
                )? {
                    rows.clear();
                    return Ok(());
                }
            }
            CrossFilter::EventExists { .. } | CrossFilter::LogExists { .. } => {
                let ranges = cross_filter_ranges(
                    filter,
                    stores,
                    authorizer,
                    first,
                    last.saturating_add(1),
                    window,
                    deadline,
                )?;
                rows.retain(|row| range_contains(&ranges, timestamp(row)));
                if rows.is_empty() {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

const fn split_window(window: i64) -> (i64, i64) {
    let lower = window / 2;
    (lower, window - lower)
}

fn existence_ranges(
    timestamps: &[i64],
    since: i64,
    until: i64,
    lower: i64,
    upper: i64,
) -> Vec<TimeRange> {
    let ranges = timestamps.iter().filter_map(|timestamp| {
        let start = timestamp.saturating_sub(lower).max(since);
        let end = timestamp.saturating_add(upper).min(until);
        (start < end).then_some(TimeRange { start, end })
    });
    merge_ranges(ranges)
}

fn merge_ranges(ranges: impl IntoIterator<Item = TimeRange>) -> Vec<TimeRange> {
    let mut ranges: Vec<_> = ranges.into_iter().collect();
    ranges.sort_unstable_by_key(|range| (range.start, range.end));
    let mut merged: Vec<TimeRange> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if let Some(previous) = merged.last_mut()
            && range.start <= previous.end
        {
            previous.end = previous.end.max(range.end);
        } else {
            merged.push(range);
        }
    }
    merged
}

fn intersect_ranges(left: &[TimeRange], right: &[TimeRange]) -> Vec<TimeRange> {
    let (mut left_index, mut right_index) = (0, 0);
    let mut output = Vec::new();
    while left_index < left.len() && right_index < right.len() {
        let start = left[left_index].start.max(right[right_index].start);
        let end = left[left_index].end.min(right[right_index].end);
        if start < end {
            output.push(TimeRange { start, end });
        }
        if left[left_index].end <= right[right_index].end {
            left_index += 1;
        } else {
            right_index += 1;
        }
    }
    output
}

fn range_contains(ranges: &[TimeRange], timestamp: i64) -> bool {
    let index = ranges.partition_point(|range| range.start <= timestamp);
    index != 0 && timestamp < ranges[index - 1].end
}

fn cross_event_timestamps(
    paths: &[PathBuf],
    pattern: &str,
    since: i64,
    until: i64,
    authorizer: &Authorizer,
    deadline: Option<Instant>,
) -> Result<Vec<i64>, QueryError> {
    let fields = vec!["event.time".to_owned()];
    let identifiers = discover_event_identifiers(paths, Some(pattern), deadline)?;
    let mut authorization = AuthorizationCache::new(authorizer);
    let allowed = authorize_identifiers(
        authorizer,
        Namespace::Events,
        identifiers,
        &fields,
        &mut authorization,
    )?;
    if allowed.is_empty() {
        return Ok(Vec::new());
    }
    let mut output = Vec::new();
    let mut scanned = 0_usize;
    for path in paths {
        check_deadline(deadline)?;
        let connection = open_read_only(path, deadline)?;
        let mut statement = connection.prepare(
            "SELECT event_type, timestamp FROM events \
             WHERE timestamp >= ?1 AND timestamp < ?2 ORDER BY timestamp ASC, id ASC",
        )?;
        let mut rows = statement.query(params![since, until])?;
        while let Some(row) = rows.next()? {
            scanned += 1;
            if scanned.is_multiple_of(1_024) {
                check_deadline(deadline)?;
            }
            let identifier: String = row.get(0)?;
            if allowed.contains(&identifier) {
                output.push(row.get(1)?);
            }
        }
    }
    output.sort_unstable();
    Ok(output)
}

fn cross_log_timestamps(
    path: &Path,
    origin: &str,
    containing: Option<&str>,
    since: i64,
    until: i64,
    authorizer: &Authorizer,
    deadline: Option<Instant>,
) -> Result<Vec<i64>, QueryError> {
    let mut fields = vec!["timestamp".to_owned()];
    if containing.is_some() {
        fields.push("message".to_owned());
    }
    let identifiers = discover_log_identifiers(path, &[origin.to_owned()], deadline)?;
    let mut authorization = AuthorizationCache::new(authorizer);
    let allowed = authorize_identifiers(
        authorizer,
        Namespace::Logs,
        identifiers,
        &fields,
        &mut authorization,
    )?;
    if allowed.is_empty() {
        return Ok(Vec::new());
    }
    let connection = open_read_only(path, deadline)?;
    let mut statement = connection.prepare(
        "SELECT origin, timestamp, message FROM logs \
         WHERE timestamp >= ?1 AND timestamp < ?2 ORDER BY timestamp ASC, id ASC",
    )?;
    let mut rows = statement.query(params![since, until])?;
    let mut output = Vec::new();
    let mut scanned = 0_usize;
    while let Some(row) = rows.next()? {
        scanned += 1;
        if scanned.is_multiple_of(1_024) {
            check_deadline(deadline)?;
        }
        let identifier: String = row.get(0)?;
        if !allowed.contains(&identifier) {
            continue;
        }
        let message: String = row.get(2)?;
        if containing.is_some_and(|needle| !ascii_contains(&message, needle)) {
            continue;
        }
        output.push(row.get(1)?);
    }
    Ok(output)
}

struct CrossMetricSeries {
    id: i64,
}

fn resolve_cross_metric(
    connection: &Connection,
    name_pattern: &str,
    labels: Option<&[Expr]>,
    authorizer: &Authorizer,
    deadline: Option<Instant>,
) -> Result<Option<CrossMetricSeries>, QueryError> {
    let mut required = vec![
        "timestamp".to_owned(),
        "value".to_owned(),
        "type".to_owned(),
    ];
    if let Some(labels) = labels {
        for expression in labels {
            expression.fields(&mut required);
        }
    }
    required.sort_unstable();
    required.dedup();
    let identifiers = discover_metric_identifiers(connection, name_pattern, deadline)?;
    let mut authorization = AuthorizationCache::new(authorizer);
    let allowed = authorize_identifiers(
        authorizer,
        Namespace::Metrics,
        identifiers,
        &required,
        &mut authorization,
    )?;
    if allowed.is_empty() {
        return Ok(None);
    }
    let mut statement = connection.prepare("SELECT id, name, labels, type FROM series")?;
    let mut rows = statement.query([])?;
    let mut resolved = None;
    while let Some(row) = rows.next()? {
        check_deadline(deadline)?;
        let id: i64 = row.get(0)?;
        let name: String = row.get(1)?;
        if !allowed.contains(&name) {
            continue;
        }
        let canonical_labels: String = row.get(2)?;
        let metric_type: i64 = row.get(3)?;
        let mut selector = parse_labels(&canonical_labels);
        selector.insert("name".into(), Value::String(name.clone()));
        selector.insert(
            "type".into(),
            Value::String(metric_type_name(metric_type)?.into()),
        );
        if labels.is_some_and(|items| {
            !items
                .iter()
                .all(|item| evaluate(Namespace::Metrics, item, &selector))
        }) {
            continue;
        }
        if metric_type == 2 {
            return Err(QueryError::CrossMetricHistogram);
        }
        if !matches!(metric_type, 0 | 1) {
            return Err(QueryError::InvalidMetricType);
        }
        if resolved.is_some() {
            return Err(QueryError::CrossMetricNeedsSelector);
        }
        resolved = Some(CrossMetricSeries { id });
    }
    Ok(resolved)
}

#[allow(
    clippy::too_many_arguments,
    reason = "cross-metric semantics require explicit bounds"
)]
fn metric_true_ranges(
    path: &Path,
    name_pattern: &str,
    labels: Option<&[Expr]>,
    operator: Operator,
    value: &Literal,
    authorizer: &Authorizer,
    since: i64,
    until: i64,
    deadline: Option<Instant>,
) -> Result<Vec<TimeRange>, QueryError> {
    let connection = open_read_only(path, deadline)?;
    let Some(series) =
        resolve_cross_metric(&connection, name_pattern, labels, authorizer, deadline)?
    else {
        return Ok(Vec::new());
    };
    let mut samples = Vec::<(i64, i64, f64)>::new();
    if since > i64::MIN {
        let preceding = connection.query_row(
            "SELECT id, timestamp, value FROM samples WHERE series_id = ?1 AND timestamp < ?2 \
             ORDER BY timestamp DESC, id DESC LIMIT 1",
            params![series.id, since],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );
        match preceding {
            Ok(sample) => samples.push(sample),
            Err(rusqlite::Error::QueryReturnedNoRows) => {}
            Err(error) => return Err(error.into()),
        }
    }
    let mut statement = connection.prepare(
        "SELECT id, timestamp, value FROM samples \
         WHERE series_id = ?1 AND timestamp >= ?2 AND timestamp < ?3 \
         ORDER BY timestamp ASC, id ASC",
    )?;
    let mut rows = statement.query(params![series.id, since, until])?;
    while let Some(row) = rows.next()? {
        if samples.len().is_multiple_of(1_024) {
            check_deadline(deadline)?;
        }
        samples.push((row.get(0)?, row.get(1)?, row.get(2)?));
    }
    let mut true_ranges = Vec::new();
    for (index, (_, timestamp, number)) in samples.iter().enumerate() {
        if !number.is_finite() {
            return Err(QueryError::NonFinite);
        }
        let start = (*timestamp).max(since);
        let end = samples
            .get(index + 1)
            .map_or(until, |(_, timestamp, _)| *timestamp)
            .min(until);
        if start < end && metric_comparison(*number, operator, value) {
            true_ranges.push(TimeRange { start, end });
        }
    }
    Ok(merge_ranges(true_ranges))
}

#[allow(
    clippy::too_many_arguments,
    reason = "watch metric lookup is one bounded index seek"
)]
fn metric_condition_at(
    path: &Path,
    name_pattern: &str,
    labels: Option<&[Expr]>,
    operator: Operator,
    value: &Literal,
    authorizer: &Authorizer,
    timestamp: i64,
    deadline: Option<Instant>,
) -> Result<bool, QueryError> {
    check_deadline(deadline)?;
    let connection = open_read_only(path, deadline)?;
    let Some(series) =
        resolve_cross_metric(&connection, name_pattern, labels, authorizer, deadline)?
    else {
        return Ok(false);
    };
    let sample = connection.query_row(
        "SELECT value FROM samples WHERE series_id = ?1 AND timestamp <= ?2 \
         ORDER BY timestamp DESC, id DESC LIMIT 1",
        params![series.id, timestamp],
        |row| row.get::<_, f64>(0),
    );
    match sample {
        Ok(number) if number.is_finite() => Ok(metric_comparison(number, operator, value)),
        Ok(_) => Err(QueryError::NonFinite),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn metric_comparison(number: f64, operator: Operator, value: &Literal) -> bool {
    let mut record = Record::new();
    record.insert("value".into(), Value::Float(number));
    evaluate(
        Namespace::Metrics,
        &Expr::Compare {
            field: "value".into(),
            operator,
            value: value.clone(),
        },
        &record,
    )
}

#[derive(Debug, Clone)]
struct Row {
    record: Record,
    identifier: String,
    /// The record's timestamp, which orders and windows it whether or not
    /// the caller may read its time field (`event.time`, or a log's or
    /// sample's `timestamp`): an internal key with no
    /// access-control identity, like `tie` (TRM §6.2).
    at: i64,
    /// Canonical metric labels before field filtering. This private key
    /// preserves the TRM §6.2 tiebreak even when label fields are denied.
    /// Empty for event/log rows and aggregates that do not retain labels.
    metric_labels: Box<[u8]>,
    tie: Tie,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Tie {
    Event { shard: usize, id: i64 },
    Single(i64),
}

/// The order a scan hands rows over in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanOrder {
    /// Each database in whatever order it yields, one after another, for a
    /// query that will order or aggregate the whole visible set itself.
    Stored,
    /// The default result order, newest first with the tiebreakers of TRM
    /// §6.2, merged across every database as it is read.
    Newest,
}

/// Whether a scan should go on reading after a row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Flow {
    More,
    Enough,
}

type Visit<'a> = dyn FnMut(Row) -> Result<Flow, QueryError> + 'a;

/// Rows read between deadline checks while nothing is being accepted.
const DEADLINE_CHECK_ROWS: usize = 1_024;

#[allow(
    clippy::too_many_arguments,
    reason = "event shards use independent selector, predicate, time and stream-cursor bounds"
)]
fn scan_events(
    paths: &[PathBuf],
    allowed: &HashSet<String>,
    predicates: &[Expr],
    since: i64,
    until: i64,
    deadline: Option<Instant>,
    lower_ids: &[i64],
    upper_ids: &[i64],
    order: ScanOrder,
    visit: &mut Visit<'_>,
) -> Result<(), QueryError> {
    if allowed.is_empty() {
        return Ok(());
    }
    let header_constraint = predicates.iter().find_map(sql_header_constraint);
    let mut read = 0;
    let select = |connection: &Connection, shard: usize| {
        let constraint = if let Some(constraint) = header_constraint.clone() {
            Some(constraint)
        } else {
            first_payload_constraint(predicates, connection)?
        };
        let mut sql = String::from(
            "SELECT id, boot_id, timestamp, cpu_id, sequence, origin_class, event_type, \
             effective_token_guid, true_token_guid, process_guid, payload \
             FROM events WHERE timestamp >= ?1 AND timestamp < ?2 AND id > ?3 AND id <= ?4",
        );
        let mut values = vec![
            rusqlite::types::Value::Integer(since),
            rusqlite::types::Value::Integer(until),
            rusqlite::types::Value::Integer(lower_ids[shard]),
            rusqlite::types::Value::Integer(upper_ids[shard]),
        ];
        if let Some(constraint) = constraint {
            sql.push_str(" AND ");
            sql.push_str(&constraint.sql);
            if let Some(value) = constraint.value {
                values.push(value);
            }
        }
        if order == ScanOrder::Newest {
            sql.push_str(" ORDER BY timestamp DESC, id DESC");
        }
        Ok::<_, QueryError>((sql, values))
    };

    if order == ScanOrder::Stored {
        for (shard, path) in paths.iter().enumerate() {
            check_deadline(deadline)?;
            let connection = open_read_only(path, deadline)?;
            let (sql, values) = select(&connection, shard)?;
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(rusqlite::params_from_iter(values))?;
            while let Some(row) = next_event(&mut rows, shard, allowed, deadline, &mut read)? {
                if visit(row)? == Flow::Enough {
                    return Ok(());
                }
            }
        }
        return Ok(());
    }

    // Every shard at once, each newest first, and always the newest head
    // next: the frontier is one row per shard, whatever the result's size.
    let mut connections = Vec::with_capacity(paths.len());
    for path in paths {
        check_deadline(deadline)?;
        connections.push(open_read_only(path, deadline)?);
    }
    let mut statements = Vec::with_capacity(connections.len());
    for (shard, connection) in connections.iter().enumerate() {
        let (sql, values) = select(connection, shard)?;
        statements.push((connection.prepare(&sql)?, values));
    }
    let mut cursors = Vec::with_capacity(statements.len());
    for (statement, values) in &mut statements {
        cursors.push(statement.query(rusqlite::params_from_iter(values.iter()))?);
    }
    let mut heads = Vec::with_capacity(cursors.len());
    for (shard, rows) in cursors.iter_mut().enumerate() {
        heads.push(next_event(rows, shard, allowed, deadline, &mut read)?);
    }
    while let Some(shard) = newest_head(&heads) {
        let row = heads[shard].take().expect("the newest head is present");
        heads[shard] = next_event(&mut cursors[shard], shard, allowed, deadline, &mut read)?;
        if visit(row)? == Flow::Enough {
            return Ok(());
        }
    }
    Ok(())
}

/// The shard whose head comes first in the default order: the newest, and
/// of equally new ones the lowest shard (TRM §6.2). Each shard's own rows
/// already come id descending.
fn newest_head(heads: &[Option<Row>]) -> Option<usize> {
    heads
        .iter()
        .enumerate()
        .filter_map(|(shard, head)| head.as_ref().map(|row| (shard, timestamp(row))))
        .max_by(|(left_shard, left), (right_shard, right)| {
            left.cmp(right).then_with(|| right_shard.cmp(left_shard))
        })
        .map(|(shard, _)| shard)
}

/// The next event in `rows` whose type may be read, decoded. Denied types
/// are skipped before their payloads are decoded.
fn next_event(
    rows: &mut rusqlite::Rows<'_>,
    shard: usize,
    allowed: &HashSet<String>,
    deadline: Option<Instant>,
    read: &mut usize,
) -> Result<Option<Row>, QueryError> {
    while let Some(row) = rows.next()? {
        *read += 1;
        if read.is_multiple_of(DEADLINE_CHECK_ROWS) {
            check_deadline(deadline)?;
        }
        let identifier: String = row.get(6)?;
        if !allowed.contains(&identifier) {
            continue;
        }
        let id: i64 = row.get(0)?;
        let boot: Vec<u8> = row.get(1)?;
        let timestamp: i64 = row.get(2)?;
        let cpu_id: Option<u64> = row.get(3)?;
        let sequence: Option<u64> = row.get(4)?;
        let origin_class: Option<u64> = row.get(5)?;
        let effective: Option<Vec<u8>> = row.get(7)?;
        let true_token: Option<Vec<u8>> = row.get(8)?;
        let process: Option<Vec<u8>> = row.get(9)?;
        let payload: Option<Vec<u8>> = row.get(10)?;
        let mut record = Record::new();
        record.insert("event.time".into(), Value::Signed(timestamp));
        record.insert("event.cpu".into(), option_unsigned(cpu_id));
        record.insert("event.sequence".into(), option_unsigned(sequence));
        record.insert("emitter.class".into(), option_unsigned(origin_class));
        record.insert("event.type".into(), Value::String(identifier.clone()));
        record.insert(
            "emitter.token.guid".into(),
            option_guid(effective.as_deref()),
        );
        record.insert(
            "emitter.true-token.guid".into(),
            option_guid(true_token.as_deref()),
        );
        record.insert(
            "emitter.process.guid".into(),
            option_guid(process.as_deref()),
        );
        record.insert("event.boot.guid".into(), option_guid(Some(&boot)));
        if let Some(payload) = payload {
            flatten_event_payload(&payload, &mut record);
        }
        return Ok(Some(Row {
            record,
            identifier,
            at: timestamp,
            metric_labels: Box::default(),
            tie: Tie::Event { shard, id },
        }));
    }
    Ok(None)
}

#[derive(Clone)]
struct SqlConstraint {
    sql: String,
    value: Option<rusqlite::types::Value>,
}

fn sql_header_constraint(expression: &Expr) -> Option<SqlConstraint> {
    let Expr::Compare {
        field,
        operator,
        value,
    } = expression
    else {
        return None;
    };
    if field == "event.type" && *operator == Operator::Equal {
        let Literal::String(value) = value else {
            return None;
        };
        return Some(SqlConstraint {
            sql: "event_type = ?5 COLLATE NOCASE".into(),
            value: Some(rusqlite::types::Value::Text(value.clone())),
        });
    }
    if !matches!(field.as_str(), "event.cpu" | "emitter.class") {
        return None;
    }
    let value = match literal_value(Namespace::Events, field, value) {
        Value::Signed(value) => value,
        Value::Unsigned(value) => i64::try_from(value).ok()?,
        _ => return None,
    };
    let sql = match (field.as_str(), operator) {
        ("event.cpu", Operator::Equal) => "cpu_id = ?5",
        ("event.cpu", Operator::NotEqual) => "cpu_id <> ?5",
        ("event.cpu", Operator::Greater) => "cpu_id > ?5",
        ("event.cpu", Operator::GreaterEqual) => "cpu_id >= ?5",
        ("event.cpu", Operator::Less) => "cpu_id < ?5",
        ("event.cpu", Operator::LessEqual) => "cpu_id <= ?5",
        ("emitter.class", Operator::Equal) => "origin_class = ?5",
        ("emitter.class", Operator::NotEqual) => "origin_class <> ?5",
        ("emitter.class", Operator::Greater) => "origin_class > ?5",
        ("emitter.class", Operator::GreaterEqual) => "origin_class >= ?5",
        ("emitter.class", Operator::Less) => "origin_class < ?5",
        ("emitter.class", Operator::LessEqual) => "origin_class <= ?5",
        _ => return None,
    };
    Some(SqlConstraint {
        sql: sql.into(),
        value: Some(rusqlite::types::Value::Integer(value)),
    })
}

fn sql_payload_constraint(
    expression: &Expr,
    connection: &Connection,
) -> Result<Option<SqlConstraint>, QueryError> {
    if let Expr::And(left, right) = expression {
        return sql_payload_constraint(left, connection)?.map_or_else(
            || sql_payload_constraint(right, connection),
            |constraint| Ok(Some(constraint)),
        );
    }
    let (field, key) = match expression {
        Expr::Compare {
            field,
            operator: Operator::Equal,
            value,
        } => {
            let key = match value {
                Literal::Bool(value) => {
                    eventd_core::payload_query_key(eventd_core::PayloadIndexValue::Bool(*value))
                }
                Literal::String(value) => {
                    eventd_core::payload_query_key(eventd_core::PayloadIndexValue::String(value))
                }
                Literal::Binary(value) => {
                    eventd_core::payload_query_key(eventd_core::PayloadIndexValue::Binary(value))
                }
                Literal::Signed(_) | Literal::Unsigned(_) | Literal::Float(_) => return Ok(None),
            };
            (field, Some(rusqlite::types::Value::Blob(key)))
        }
        Expr::Null {
            field,
            negated: false,
        } => (field, None),
        _ => return Ok(None),
    };
    let Some(name) = eventd_core::payload_index_name(field) else {
        return Ok(None);
    };
    let Some(index_expression) = eventd_core::payload_index::expression(field) else {
        return Ok(None);
    };
    let material: bool = connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = ?1)",
        [&name],
        |row| row.get(0),
    )?;
    if !material {
        return Ok(None);
    }
    Ok(Some(SqlConstraint {
        sql: if key.is_some() {
            format!("{index_expression} = ?5")
        } else {
            format!("{index_expression} IS NULL")
        },
        value: key,
    }))
}

fn first_payload_constraint(
    predicates: &[Expr],
    connection: &Connection,
) -> Result<Option<SqlConstraint>, QueryError> {
    for predicate in predicates {
        if let Some(constraint) = sql_payload_constraint(predicate, connection)? {
            return Ok(Some(constraint));
        }
    }
    Ok(None)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the fixed log selectors are independent SQL narrowing inputs"
)]
fn scan_logs(
    path: &Path,
    allowed: &HashSet<String>,
    error_only: bool,
    containing: Option<&str>,
    since: i64,
    until: i64,
    deadline: Option<Instant>,
    lower_id: i64,
    upper_id: i64,
    order: ScanOrder,
    visit: &mut Visit<'_>,
) -> Result<(), QueryError> {
    if allowed.is_empty() {
        return Ok(());
    }
    let connection = open_read_only(path, deadline)?;
    let mut statement = connection.prepare(match order {
        ScanOrder::Stored => {
            "SELECT id, boot_id, timestamp, origin, is_error, message, job_id \
             FROM logs WHERE timestamp >= ?1 AND timestamp < ?2 AND id > ?3 AND id <= ?4"
        }
        ScanOrder::Newest => {
            "SELECT id, boot_id, timestamp, origin, is_error, message, job_id \
             FROM logs WHERE timestamp >= ?1 AND timestamp < ?2 AND id > ?3 AND id <= ?4 \
             ORDER BY timestamp DESC, id DESC"
        }
    })?;
    let mut rows = statement.query(params![since, until, lower_id, upper_id])?;
    let mut read = 0_usize;
    while let Some(row) = rows.next()? {
        read += 1;
        if read.is_multiple_of(DEADLINE_CHECK_ROWS) {
            check_deadline(deadline)?;
        }
        let identifier: String = row.get(3)?;
        if !allowed.contains(&identifier) {
            continue;
        }
        let is_error: bool = row.get(4)?;
        if error_only && !is_error {
            continue;
        }
        let message: String = row.get(5)?;
        if containing.is_some_and(|needle| !ascii_contains(&message, needle)) {
            continue;
        }
        let id: i64 = row.get(0)?;
        let boot: Vec<u8> = row.get(1)?;
        let timestamp: i64 = row.get(2)?;
        let job_id: Option<Vec<u8>> = row.get(6)?;
        let record = BTreeMap::from([
            ("timestamp".into(), Value::Signed(timestamp)),
            ("origin".into(), Value::String(identifier.clone())),
            ("is_error".into(), Value::Bool(is_error)),
            ("message".into(), Value::String(message)),
            ("boot_id".into(), option_guid(Some(&boot))),
            ("job_id".into(), option_guid(job_id.as_deref())),
        ]);
        let row = Row {
            record,
            identifier,
            at: timestamp,
            metric_labels: Box::default(),
            tie: Tie::Single(id),
        };
        if visit(row)? == Flow::Enough {
            return Ok(());
        }
    }
    Ok(())
}

fn authorize_row(
    authorizer: &Authorizer,
    namespace: Namespace,
    row: &mut Row,
    referenced: &[String],
    cache: &mut AuthorizationCache,
) -> Result<bool, QueryError> {
    let mut fields: Vec<_> = row.record.keys().cloned().collect();
    fields.extend(referenced.iter().cloned());
    fields.sort_unstable();
    fields.dedup();
    let allowed = cache.check(authorizer, namespace, &row.identifier, &fields)?;
    let Some(allowed) = allowed else {
        return Ok(false);
    };
    if referenced.iter().any(|field| !allowed.contains(field)) {
        return Ok(false);
    }
    row.record.retain(|field, _| allowed.contains(field));
    Ok(true)
}

type AccessEntries = HashMap<(String, Vec<String>), Option<std::collections::HashSet<String>>>;

struct AuthorizationCache {
    generation: u64,
    entries: AccessEntries,
    /// Whether each identifier's records may be visible with the fields
    /// asked about, before any is read (`Authorizer::may_read`).
    identifiers: HashMap<(String, Vec<String>), bool>,
}

impl AuthorizationCache {
    fn new(authorizer: &Authorizer) -> Self {
        Self {
            generation: authorizer.descriptor_generation(),
            entries: HashMap::new(),
            identifiers: HashMap::new(),
        }
    }

    fn refresh(&mut self, authorizer: &Authorizer) {
        let generation = authorizer.descriptor_generation();
        if self.generation != generation {
            self.generation = generation;
            self.entries.clear();
            self.identifiers.clear();
        }
    }

    fn may_read(
        &mut self,
        authorizer: &Authorizer,
        namespace: Namespace,
        identifier: &str,
        fields: &[String],
    ) -> Result<bool, QueryError> {
        self.refresh(authorizer);
        let key = (identifier.to_owned(), fields.to_vec());
        if let Some(visible) = self.identifiers.get(&key) {
            return Ok(*visible);
        }
        let visible = authorizer.may_read(namespace, identifier, fields)?;
        self.identifiers.insert(key, visible);
        Ok(visible)
    }

    fn check(
        &mut self,
        authorizer: &Authorizer,
        namespace: Namespace,
        identifier: &str,
        fields: &[String],
    ) -> Result<Option<HashSet<String>>, QueryError> {
        self.refresh(authorizer);
        let key = (identifier.to_owned(), fields.to_vec());
        if let Some(allowed) = self.entries.get(&key) {
            return Ok(allowed.clone());
        }
        let allowed = authorizer.check(namespace, identifier, fields)?;
        self.entries.insert(key, allowed.clone());
        Ok(allowed)
    }
}

fn referenced_fields(query: &Query) -> Vec<String> {
    let mut fields = Vec::new();
    match &query.source {
        Source::Logs {
            error_only,
            containing,
            ..
        } => {
            if *error_only {
                fields.push("is_error".into());
            }
            if containing.is_some() {
                fields.push("message".into());
            }
        }
        Source::Metric {
            labels: Some(labels),
            ..
        } => {
            for label in labels {
                label.fields(&mut fields);
            }
        }
        Source::Events { .. } | Source::Metric { labels: None, .. } => {}
    }
    for predicate in &query.predicates {
        predicate.fields(&mut fields);
    }
    for sort in &query.sort {
        if !fields.contains(&sort.field) {
            fields.push(sort.field.clone());
        }
    }
    if let Some(aggregate) = &query.aggregate {
        match aggregate {
            RecordAggregate::CountBy(field)
            | RecordAggregate::Distinct(field)
            | RecordAggregate::TopBy { field, .. } => fields.push(field.clone()),
            RecordAggregate::Group {
                fields: groups,
                function,
            } => {
                fields.extend(groups.iter().cloned());
                if let GroupFunction::Sum(field)
                | GroupFunction::Avg(field)
                | GroupFunction::Min(field)
                | GroupFunction::Max(field) = function
                {
                    fields.push(field.clone());
                }
            }
        }
    }
    if query.transform.is_some() || query.metric_aggregate.is_some() {
        fields.push("value".into());
    }
    fields.sort_unstable();
    fields.dedup();
    fields
}

fn evaluate(namespace: Namespace, expression: &Expr, record: &Record) -> bool {
    match expression {
        Expr::And(left, right) => {
            evaluate(namespace, left, record) && evaluate(namespace, right, record)
        }
        Expr::Or(left, right) => {
            evaluate(namespace, left, record) || evaluate(namespace, right, record)
        }
        Expr::Null { field, negated } => {
            let is_null = record
                .get(field)
                .is_none_or(|value| matches!(value, Value::Null));
            is_null != *negated
        }
        Expr::In {
            field,
            negated,
            values,
        } => {
            let Some(actual) = record
                .get(field)
                .filter(|value| !matches!(value, Value::Null))
            else {
                return false;
            };
            let found = values
                .iter()
                .any(|value| actual.language_equal(&literal_value(namespace, field, value)));
            found != *negated
        }
        Expr::Compare {
            field,
            operator,
            value,
        } => {
            let Some(actual) = record
                .get(field)
                .filter(|value| !matches!(value, Value::Null))
            else {
                return false;
            };
            let expected = literal_value(namespace, field, value);
            match operator {
                Operator::Equal => actual.language_equal(&expected),
                Operator::NotEqual => !actual.language_equal(&expected),
                Operator::Greater => numeric_order(actual, &expected, Ordering::Greater),
                Operator::GreaterEqual => {
                    numeric_order(actual, &expected, Ordering::Greater)
                        || numeric_order(actual, &expected, Ordering::Equal)
                }
                Operator::Less => numeric_order(actual, &expected, Ordering::Less),
                Operator::LessEqual => {
                    numeric_order(actual, &expected, Ordering::Less)
                        || numeric_order(actual, &expected, Ordering::Equal)
                }
                Operator::StartsWith => string_operation(actual, &expected, |value, needle| {
                    ascii_starts_with(value, needle)
                }),
                Operator::EndsWith => string_operation(actual, &expected, |value, needle| {
                    ascii_ends_with(value, needle)
                }),
                Operator::Contains => string_operation(actual, &expected, ascii_contains),
                Operator::Has => array_contains(actual, &expected),
            }
        }
    }
}

fn literal_value(namespace: Namespace, field: &str, literal: &Literal) -> Value {
    match literal {
        Literal::Signed(value) => Value::Signed(*value),
        Literal::Unsigned(value) => Value::Unsigned(*value),
        Literal::Float(value) => Value::Float(*value),
        Literal::String(value) if namespace == Namespace::Events && field == "emitter.class" => {
            match value.to_ascii_lowercase().as_str() {
                "userspace" => Value::Unsigned(0),
                "kmes" => Value::Unsigned(1),
                "kacs" => Value::Unsigned(2),
                "lcs" => Value::Unsigned(3),
                _ => Value::String(value.clone()),
            }
        }
        Literal::String(value)
            if matches!(
                (namespace, field),
                (
                    Namespace::Events,
                    "emitter.token.guid"
                        | "emitter.true-token.guid"
                        | "emitter.process.guid"
                        | "event.boot.guid"
                ) | (Namespace::Logs, "boot_id" | "job_id")
                    | (Namespace::Metrics, "boot_id")
            ) =>
        {
            canonical_guid_literal(value)
                .map_or_else(|| Value::String(value.clone()), Value::String)
        }
        Literal::String(value) => Value::String(value.clone()),
        Literal::Binary(value) => Value::Binary(value.clone()),
        Literal::Bool(value) => Value::Bool(*value),
    }
}

/// A log query's predicates, with each integer `1` or `0` that `is_error`
/// is compared against read as `true` or `false`. PSPU §3.22 makes the
/// two forms one for that field alone; everywhere else types do not
/// coerce (§3.20), so an event payload's `is_error` is left as it is.
fn log_predicates(predicates: &[Expr]) -> Vec<Expr> {
    fn boolean(literal: &Literal) -> Literal {
        match literal {
            Literal::Signed(1) | Literal::Unsigned(1) => Literal::Bool(true),
            Literal::Signed(0) | Literal::Unsigned(0) => Literal::Bool(false),
            other => other.clone(),
        }
    }
    fn rewrite(expression: &Expr) -> Expr {
        match expression {
            Expr::Compare {
                field,
                operator,
                value,
            } if field == "is_error" => Expr::Compare {
                field: field.clone(),
                operator: *operator,
                value: boolean(value),
            },
            Expr::In {
                field,
                negated,
                values,
            } if field == "is_error" => Expr::In {
                field: field.clone(),
                negated: *negated,
                values: values.iter().map(boolean).collect(),
            },
            Expr::And(left, right) => Expr::And(Box::new(rewrite(left)), Box::new(rewrite(right))),
            Expr::Or(left, right) => Expr::Or(Box::new(rewrite(left)), Box::new(rewrite(right))),
            other => other.clone(),
        }
    }
    predicates.iter().map(rewrite).collect()
}

fn canonical_guid_literal(value: &str) -> Option<String> {
    let body = value
        .strip_prefix('{')
        .and_then(|value| value.strip_suffix('}'))
        .unwrap_or(value);
    if body.len() != 36
        || [8, 13, 18, 23]
            .into_iter()
            .any(|index| body.as_bytes()[index] != b'-')
        || body
            .bytes()
            .enumerate()
            .any(|(index, byte)| ![8, 13, 18, 23].contains(&index) && !byte.is_ascii_hexdigit())
    {
        return None;
    }
    Some(format!("{{{}}}", body.to_ascii_lowercase()))
}

/// An ordering predicate. A NaN satisfies none, though it sorts after
/// every number (PSPU §3.20–§3.21).
fn numeric_order(left: &Value, right: &Value, wanted: Ordering) -> bool {
    matches!(
        left,
        Value::Signed(_) | Value::Unsigned(_) | Value::Float(_)
    ) && matches!(
        right,
        Value::Signed(_) | Value::Unsigned(_) | Value::Float(_)
    ) && !is_nan(left)
        && !is_nan(right)
        && language_cmp(left, right) == wanted
}

fn string_operation(left: &Value, right: &Value, operation: impl Fn(&str, &str) -> bool) -> bool {
    matches!((left, right), (Value::String(left), Value::String(right)) if operation(left, right))
}

/// `HAS`: true when the field holds an array one of whose elements
/// equals the value under the language's own equality — so a binary SID
/// matches a binary element and a string matches under ASCII folding,
/// exactly as `==` would against a scalar field. Elements are compared
/// one level deep: an array element that is itself an array is compared
/// as a whole, not searched. A field that is not an array is false
/// rather than an error, which is how every other type mismatch in a
/// predicate behaves.
fn array_contains(left: &Value, right: &Value) -> bool {
    matches!(left, Value::Array(elements) if elements.iter().any(|element| element.language_equal(right)))
}

fn sort_rows(rows: &mut [Row], query: &Query) {
    rows.sort_by(|left, right| {
        for key in &query.sort {
            let ordering = language_cmp(
                left.record.get(&key.field).unwrap_or(&Value::Null),
                right.record.get(&key.field).unwrap_or(&Value::Null),
            );
            let ordering = if key.descending {
                ordering.reverse()
            } else {
                ordering
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        // The tiebreakers follow the SORT keys as they follow the default
        // order: timestamp descending first (TRM §6.2).
        let ordering = timestamp(right).cmp(&timestamp(left));
        if ordering != Ordering::Equal {
            return ordering;
        }
        match (left.tie, right.tie) {
            (
                Tie::Event {
                    shard: left_shard,
                    id: left_id,
                },
                Tie::Event {
                    shard: right_shard,
                    id: right_id,
                },
            ) => left_shard
                .cmp(&right_shard)
                .then_with(|| right_id.cmp(&left_id)),
            (Tie::Single(left), Tie::Single(right)) => right.cmp(&left),
            _ => left.tie.cmp(&right.tie),
        }
    });
}

const fn timestamp(row: &Row) -> i64 {
    row.at
}

/// An aggregation's groups, folded as rows pass, so what is held is the
/// groups and never the rows (TRM §6.4). Groups are found by a hash of a
/// key that language-equal values always share, then compared under the
/// language's own equality against each group's representative in the
/// order the groups appeared, so that the answer is the one comparing
/// every group in turn would give.
struct Groups<'q> {
    aggregate: &'q RecordAggregate,
    index: HashMap<Vec<GroupKey>, Vec<usize>>,
    /// Every group so far, in the order they appeared.
    seen: Vec<Group>,
}

struct Group {
    /// The smallest member of each grouping field (TRM §6.2).
    keys: Vec<Value>,
    count: u64,
    numeric: Numeric,
}

/// What a new group costs beyond its keys.
const GROUP_OVERHEAD: usize = 128;

impl<'q> Groups<'q> {
    fn new(aggregate: &'q RecordAggregate) -> Self {
        Self {
            aggregate,
            index: HashMap::new(),
            seen: Vec::new(),
        }
    }

    fn fields(&self) -> &'q [String] {
        match self.aggregate {
            RecordAggregate::CountBy(field)
            | RecordAggregate::Distinct(field)
            | RecordAggregate::TopBy { field, .. } => std::slice::from_ref(field),
            RecordAggregate::Group { fields, .. } => fields,
        }
    }

    fn add(&mut self, row: &Row, held: &mut Held<'_>) -> Result<(), QueryError> {
        let keys: Vec<_> = self
            .fields()
            .iter()
            .map(|field| row.record.get(field).cloned().unwrap_or(Value::Null))
            .collect();
        let bucket = self
            .index
            .entry(keys.iter().map(GroupKey::of).collect())
            .or_default();
        let found = bucket.iter().copied().find(|&index| {
            self.seen[index]
                .keys
                .iter()
                .zip(&keys)
                .all(|(representative, value)| representative.language_equal(value))
        });
        let index = if let Some(index) = found {
            for (representative, value) in self.seen[index].keys.iter_mut().zip(keys) {
                if language_cmp(&value, representative) == Ordering::Less {
                    *representative = value;
                }
            }
            index
        } else {
            held.add(GROUP_OVERHEAD + keys.iter().map(value_size).sum::<usize>())?;
            bucket.push(self.seen.len());
            self.seen.push(Group {
                keys,
                count: 0,
                numeric: Numeric::new(),
            });
            self.seen.len() - 1
        };
        let group = &mut self.seen[index];
        group.count += 1;
        if let RecordAggregate::Group {
            function:
                GroupFunction::Sum(field)
                | GroupFunction::Avg(field)
                | GroupFunction::Min(field)
                | GroupFunction::Max(field),
            ..
        } = self.aggregate
            && let Some(value) = row.record.get(field)
        {
            group.numeric.push(value);
        }
        Ok(())
    }

    /// The result rows: `COUNT BY`, `TOP N BY` and `GROUP … COUNT` by
    /// count descending, `DISTINCT` by value, numeric `GROUP` by keys.
    fn finish(mut self) -> Result<Vec<Row>, QueryError> {
        let records = match self.aggregate {
            RecordAggregate::CountBy(field) => counted(field, self.seen, None),
            RecordAggregate::TopBy { count, field } => counted(field, self.seen, Some(*count)),
            RecordAggregate::Distinct(field) => {
                let mut values: Vec<_> = self
                    .seen
                    .into_iter()
                    .filter_map(|group| group.keys.into_iter().next())
                    .collect();
                values.sort_by(language_cmp);
                values
                    .into_iter()
                    .map(|value| BTreeMap::from([(field.clone(), value)]))
                    .collect()
            }
            RecordAggregate::Group { fields, function } => {
                // COUNT sorts by count descending; numeric groups sort by
                // their keys. Both use keys in GROUP order to break ties
                // before pagination (PSPU §3.21). Distinct groups differ in
                // some key, so the order is total.
                self.seen.sort_by(|left, right| {
                    let count_order = if matches!(function, GroupFunction::Count) {
                        right.count.cmp(&left.count)
                    } else {
                        Ordering::Equal
                    };
                    count_order.then_with(|| {
                        left.keys
                            .iter()
                            .zip(&right.keys)
                            .map(|(left, right)| language_cmp(left, right))
                            .find(|ordering| ordering.is_ne())
                            .unwrap_or(Ordering::Equal)
                    })
                });
                let mut records = Vec::with_capacity(self.seen.len());
                for group in self.seen {
                    let (name, value) = match function {
                        GroupFunction::Count => ("count", Value::Unsigned(group.count)),
                        GroupFunction::Sum(_) => {
                            ("sum", group.numeric.finish(AggregateFunction::Sum)?)
                        }
                        GroupFunction::Avg(_) => {
                            ("avg", group.numeric.finish(AggregateFunction::Avg)?)
                        }
                        GroupFunction::Min(_) => {
                            ("min", group.numeric.finish(AggregateFunction::Min)?)
                        }
                        GroupFunction::Max(_) => {
                            ("max", group.numeric.finish(AggregateFunction::Max)?)
                        }
                    };
                    let mut record: Record = fields.iter().cloned().zip(group.keys).collect();
                    record.insert(name.into(), value);
                    records.push(record);
                }
                records
            }
        };
        Ok(records
            .into_iter()
            .enumerate()
            .map(|(index, record)| Row {
                record,
                identifier: String::new(),
                // A group is no record and has no time of its own.
                at: 0,
                metric_labels: Box::default(),
                tie: Tie::Single(i64::try_from(index).unwrap_or(i64::MAX)),
            })
            .collect())
    }
}

/// `COUNT BY` and `TOP N BY` records: count descending, then value, with
/// groups that tie on both in the order they appeared.
fn counted(field: &str, groups: Vec<Group>, take: Option<u64>) -> Vec<Record> {
    let mut groups: Vec<_> = groups
        .into_iter()
        .filter_map(|group| Some((group.keys.into_iter().next()?, group.count)))
        .collect();
    groups.sort_by(|left, right| {
        right
            .1
            .cmp(&left.1)
            .then_with(|| language_cmp(&left.0, &right.0))
    });
    if let Some(take) = take.and_then(|value| usize::try_from(value).ok()) {
        groups.truncate(take);
    }
    groups
        .into_iter()
        .map(|(value, count)| {
            BTreeMap::from([
                (field.to_owned(), value),
                ("count".into(), Value::Unsigned(count)),
            ])
        })
        .collect()
}

/// A hash key that every pair of language-equal values shares (PSPU
/// §3.20–§3.21): numbers by their nearest binary64, with the zeros as
/// one, text with ASCII case folded, containers by their members. Values
/// that share a key may still differ; only equality decides.
#[derive(Debug, PartialEq, Eq, Hash)]
enum GroupKey {
    Null,
    Bool(bool),
    Number(u64),
    Text(Vec<u8>),
    Binary(Vec<u8>),
    Array(Vec<Self>),
    Map(Vec<Self>),
}

impl GroupKey {
    #[allow(
        clippy::cast_precision_loss,
        reason = "an integer and a float are equal only when the float is that integer, which then converts exactly; integers that round together merely share a bucket"
    )]
    fn of(value: &Value) -> Self {
        let number = |number: f64| Self::Number(if number == 0.0 { 0 } else { number.to_bits() });
        match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Bool(*value),
            Value::Signed(value) => number(*value as f64),
            Value::Unsigned(value) => number(*value as f64),
            Value::Float(value) => number(*value),
            Value::String(text) => Self::Text(text.bytes().map(super::value::fold_ascii).collect()),
            Value::Binary(bytes) => Self::Binary(bytes.clone()),
            Value::Array(values) => Self::Array(values.iter().map(Self::of).collect()),
            Value::Map(entries) => Self::Map(
                entries
                    .iter()
                    .flat_map(|(key, value)| [Self::of(key), Self::of(value)])
                    .collect(),
            ),
        }
    }
}

/// `SUM`, `AVG`, `MIN` and `MAX` over a group's numeric values, folded in
/// the order the values come. Non-numeric values are not counted.
struct Numeric {
    count: usize,
    /// The binary64 sum, from the same neutral element and in the same
    /// order as summing every value at the end would use.
    total: f64,
    /// The exact sum while every value is an integer and it fits.
    exact: Option<i128>,
    float: bool,
    /// The first of the smallest values, and the last of the largest.
    least: Option<Value>,
    most: Option<Value>,
}

impl Numeric {
    fn new() -> Self {
        Self {
            count: 0,
            total: std::iter::empty::<f64>().sum(),
            exact: Some(0),
            float: false,
            least: None,
            most: None,
        }
    }

    fn push(&mut self, value: &Value) {
        match value {
            Value::Signed(number) => {
                self.exact = self
                    .exact
                    .and_then(|sum| sum.checked_add(i128::from(*number)));
            }
            Value::Unsigned(number) => {
                self.exact = self
                    .exact
                    .and_then(|sum| sum.checked_add(i128::from(*number)));
            }
            Value::Float(_) => self.float = true,
            _ => return,
        }
        self.count += 1;
        self.total += as_f64(value);
        if self
            .least
            .as_ref()
            .is_none_or(|least| language_cmp(value, least) == Ordering::Less)
        {
            self.least = Some(value.clone());
        }
        if self
            .most
            .as_ref()
            .is_none_or(|most| language_cmp(value, most) != Ordering::Less)
        {
            self.most = Some(value.clone());
        }
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "PSPU defines AVG and overflowing integer SUM results as binary64"
    )]
    fn finish(&self, function: AggregateFunction) -> Result<Value, QueryError> {
        if self.count == 0 {
            return Ok(Value::Null);
        }
        let result = match function {
            AggregateFunction::Min => return Ok(self.least.clone().unwrap_or(Value::Null)),
            AggregateFunction::Max => return Ok(self.most.clone().unwrap_or(Value::Null)),
            AggregateFunction::Sum => {
                if !self.float
                    && let Some(exact) = self.exact
                {
                    if let Ok(value) = i64::try_from(exact) {
                        return Ok(Value::Signed(value));
                    }
                    if let Ok(value) = u64::try_from(exact) {
                        return Ok(Value::Unsigned(value));
                    }
                }
                self.total
            }
            AggregateFunction::Avg => self.total / self.count as f64,
        };
        if result.is_finite() {
            Ok(Value::Float(result))
        } else {
            Err(QueryError::NonFinite)
        }
    }
}

#[cfg(test)]
fn numeric_aggregate(
    rows: &[Row],
    field: &str,
    function: AggregateFunction,
) -> Result<Value, QueryError> {
    let mut numeric = Numeric::new();
    for value in rows.iter().filter_map(|row| row.record.get(field)) {
        numeric.push(value);
    }
    numeric.finish(function)
}

#[allow(
    clippy::cast_precision_loss,
    reason = "the query language explicitly converts numeric aggregate inputs to binary64"
)]
const fn as_f64(value: &Value) -> f64 {
    match value {
        Value::Signed(value) => *value as f64,
        Value::Unsigned(value) => *value as f64,
        Value::Float(value) => *value,
        _ => 0.0,
    }
}

fn apply_record_sort(rows: &mut [Row], query: &Query) {
    if query.sort.is_empty() {
        return;
    }
    rows.sort_by(|left, right| {
        sort_key_order(left, right, query).then_with(|| left.tie.cmp(&right.tie))
    });
}

/// How the query's SORT keys order two rows, `Equal` when they tie on
/// every key.
fn sort_key_order(left: &Row, right: &Row, query: &Query) -> Ordering {
    query
        .sort
        .iter()
        .find_map(|key| {
            let ordering = language_cmp(
                left.record.get(&key.field).unwrap_or(&Value::Null),
                right.record.get(&key.field).unwrap_or(&Value::Null),
            );
            let ordering = if key.descending {
                ordering.reverse()
            } else {
                ordering
            };
            (ordering != Ordering::Equal).then_some(ordering)
        })
        .unwrap_or(Ordering::Equal)
}

fn apply_pagination<T>(rows: &mut Vec<T>, query: &Query) {
    let skip = usize::try_from(query.skip)
        .unwrap_or(usize::MAX)
        .min(rows.len());
    rows.drain(..skip);
    if let Some(take) = query.take.and_then(|value| usize::try_from(value).ok()) {
        rows.truncate(take);
    }
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the metric pipeline keeps series resolution, authorization and transforms in their specified order"
)]
fn execute_metric(
    query: &Query,
    path: &Path,
    name_pattern: &str,
    labels: Option<&[Expr]>,
    since: i64,
    until: i64,
    authorizer: &Authorizer,
    deadline: Option<Instant>,
    cross_ranges: Option<&[TimeRange]>,
    authorization: &mut AuthorizationCache,
    rollup_limits: Option<&Limits>,
    budget: &HeldBudget,
    emit: &mut Emit<'_>,
) -> Result<(), QueryError> {
    let connection = open_read_only(path, deadline)?;
    let referenced = referenced_fields(query);
    let identifiers = discover_metric_identifiers(&connection, name_pattern, deadline)?;
    let allowed = authorize_identifiers(
        authorizer,
        Namespace::Metrics,
        identifiers,
        &referenced,
        authorization,
    )?;
    if allowed.is_empty() {
        return Ok(());
    }
    // What this query holds: the series it matched, then only what its
    // result needs, with every window folded as samples are read (TRM
    // §6.5).
    let mut held = Held::new(budget);
    let mut series_statement = connection.prepare("SELECT id, name, labels, type FROM series")?;
    let mut series_rows = series_statement.query([])?;
    let mut series = Vec::new();
    while let Some(row) = series_rows.next()? {
        let id: i64 = row.get(0)?;
        let name: String = row.get(1)?;
        if !allowed.contains(&name) {
            continue;
        }
        let canonical_labels: String = row.get(2)?;
        let metric_type: i64 = row.get(3)?;
        let label_map = parse_labels(&canonical_labels);
        let mut selector_record = label_map.clone();
        selector_record.insert("name".into(), Value::String(name.clone()));
        selector_record.insert(
            "type".into(),
            Value::String(metric_type_name(metric_type)?.into()),
        );
        if labels.is_some_and(|items| {
            !items
                .iter()
                .all(|item| evaluate(Namespace::Metrics, item, &selector_record))
        }) {
            continue;
        }
        held.add(SERIES_OVERHEAD + heap(name.len()) + record_size(&label_map))?;
        series.push((id, name, metric_type, label_map));
    }
    if series.is_empty() {
        return Ok(());
    }
    let series_count = series.len();
    let first_type = series[0].2;
    if series.iter().any(|item| item.2 != first_type) {
        return Err(QueryError::MixedMetricTypes);
    }
    validate_metric_pipeline(first_type, query.transform)?;
    let bracketed = labels.is_some();
    if !bracketed
        && query.since.is_some()
        && series_count > 1
        && !matches!(query.metric_aggregate, Some(MetricAggregate::Window(_, _)))
    {
        return Err(QueryError::MetricNeedsWindow);
    }
    let rollup = execute_rollup_window_query(
        &connection,
        &series,
        query,
        name_pattern,
        first_type,
        bracketed,
        since,
        until,
        authorizer,
        &referenced,
        authorization,
        rollup_limits,
        &mut held,
    )?;
    let mut output = if let Some(output) = rollup {
        output
    } else {
        let mut sink = MetricSink::new(query, name_pattern, first_type, bracketed, series_count)?;
        for (series_id, name, metric_type, label_map) in &series {
            check_deadline(deadline)?;
            let mut transformer = Transformer::new(query.transform, since);
            read_series(
                &connection,
                &SeriesRef {
                    id: *series_id,
                    name,
                    metric_type: *metric_type,
                    labels: label_map,
                },
                since,
                until,
                query.transform,
                deadline,
                &mut |row| {
                    if cross_ranges.is_some_and(|ranges| !range_contains(ranges, timestamp(row))) {
                        return Ok(false);
                    }
                    Ok(authorize_row(
                        authorizer,
                        Namespace::Metrics,
                        row,
                        &referenced,
                        authorization,
                    )? && query
                        .predicates
                        .iter()
                        .all(|item| evaluate(Namespace::Metrics, item, &row.record)))
                },
                &mut |input| {
                    transformer
                        .push(input)?
                        .map_or(Ok(()), |point| sink.push(point, &mut held))
                },
            )?;
            sink.end_series(*series_id, &mut held)?;
        }
        sink.finish(&mut held)?
    };
    sort_metric_rows(&mut output, query);
    apply_pagination(&mut output, query);
    output.into_iter().try_for_each(|row| emit(row.record))
}

/// What one matched series costs held beside its name and labels.
const SERIES_OVERHEAD: usize = size_of::<(i64, String, i64, Record)>();

/// One matched metric series, as its samples are read.
struct SeriesRef<'a> {
    id: i64,
    name: &'a str,
    metric_type: i64,
    labels: &'a Record,
}

/// How often a series read looks at the query's deadline.
const DEADLINE_STRIDE: usize = 4_096;

/// Hand each sample of one series in `[since, until)` that `visible`
/// passes to `visit`, in ascending order, holding none of them. A pair
/// transform gets the sample before `since` first, as its first pair's
/// earlier half (PSPU §3.25).
#[allow(
    clippy::too_many_arguments,
    reason = "a series read carries the authorized query context explicitly"
)]
fn read_series(
    connection: &Connection,
    series: &SeriesRef<'_>,
    since: i64,
    until: i64,
    transform: Option<Transform>,
    deadline: Option<Instant>,
    visible: &mut dyn FnMut(&mut Row) -> Result<bool, QueryError>,
    visit: &mut dyn FnMut(MetricInput) -> Result<(), QueryError>,
) -> Result<(), QueryError> {
    let mut offer = |mut input: MetricInput| -> Result<(), QueryError> {
        if visible(&mut input.row)? {
            visit(input)?;
        }
        Ok(())
    };
    if matches!(transform, Some(Transform::Rate | Transform::Delta)) && since > i64::MIN {
        let mut preceding = connection.prepare(
            "SELECT id, boot_id, timestamp, value, histogram_data FROM samples \
             WHERE series_id = ?1 AND timestamp < ?2 \
             ORDER BY timestamp DESC, id DESC LIMIT 1",
        )?;
        let mut rows = preceding.query(params![series.id, since])?;
        if let Some(sample) = rows.next()? {
            offer(read_metric_input(
                sample,
                series.name,
                series.metric_type,
                series.labels,
            )?)?;
        }
    }
    let mut statement = connection.prepare(
        "SELECT id, boot_id, timestamp, value, histogram_data FROM samples \
         WHERE series_id = ?1 AND timestamp >= ?2 AND timestamp < ?3 \
         ORDER BY timestamp ASC, id ASC",
    )?;
    let mut samples = statement.query(params![series.id, since, until])?;
    let mut read = 0_usize;
    while let Some(sample) = samples.next()? {
        read += 1;
        if read.is_multiple_of(DEADLINE_STRIDE) {
            check_deadline(deadline)?;
        }
        offer(read_metric_input(
            sample,
            series.name,
            series.metric_type,
            series.labels,
        )?)?;
    }
    Ok(())
}

/// The transform stage for one series, holding at most the sample before
/// (PSPU §3.25).
struct Transformer {
    transform: Option<Transform>,
    since: i64,
    previous: Option<(i64, f64)>,
}

impl Transformer {
    const fn new(transform: Option<Transform>, since: i64) -> Self {
        Self {
            transform,
            since,
            previous: None,
        }
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "RATE is specified as a finite binary64 ratio over nanoseconds"
    )]
    fn push(&mut self, mut input: MetricInput) -> Result<Option<MetricPoint>, QueryError> {
        let at = timestamp(&input.row);
        let point = |row| MetricPoint {
            row,
            adjusted_delta: None,
            elapsed_ns: None,
        };
        match self.transform {
            None => Ok((at >= self.since).then(|| point(input.row))),
            Some(Transform::Percentile(percentile)) => {
                if at < self.since {
                    return Ok(None);
                }
                let value = histogram_percentile(
                    input
                        .histogram
                        .as_deref()
                        .ok_or(QueryError::InvalidHistogram)?,
                    percentile,
                )?;
                match value {
                    PercentileValue::Empty => return Ok(None),
                    PercentileValue::Finite(value) => {
                        set_metric_result(&mut input.row.record, finite_value(value)?, false);
                    }
                    PercentileValue::Overflow => {
                        set_metric_result(&mut input.row.record, Value::Null, true);
                    }
                }
                Ok(Some(point(input.row)))
            }
            Some(transform @ (Transform::Rate | Transform::Delta)) => {
                let Some((earlier_at, earlier)) = self.previous.replace((at, input.number)) else {
                    return Ok(None);
                };
                let elapsed_ns = at.saturating_sub(earlier_at);
                if at < self.since || elapsed_ns <= 0 {
                    return Ok(None);
                }
                let adjusted_delta = if input.number >= earlier {
                    input.number - earlier
                } else {
                    input.number
                };
                let value = if transform == Transform::Rate {
                    adjusted_delta * 1_000_000_000.0 / elapsed_ns as f64
                } else {
                    adjusted_delta
                };
                input
                    .row
                    .record
                    .insert("value".into(), finite_value(value)?);
                Ok(Some(MetricPoint {
                    row: input.row,
                    adjusted_delta: Some(adjusted_delta),
                    elapsed_ns: Some(elapsed_ns),
                }))
            }
        }
    }
}

/// One window's, or one series', aggregation, folded as points pass: the
/// first point's row for the result's labels, and the values in the order
/// they came, so the result is the one aggregating them all at the end
/// would give.
struct Fold {
    template: Row,
    numeric: Numeric,
    overflow: bool,
    delta: f64,
    elapsed: Option<i64>,
    latest: i64,
}

impl Fold {
    fn new(point: &MetricPoint) -> Self {
        let mut fold = Self {
            template: point.row.clone(),
            numeric: Numeric::new(),
            overflow: false,
            delta: std::iter::empty::<f64>().sum(),
            elapsed: Some(0),
            latest: i64::MIN,
        };
        fold.push(point);
        fold
    }

    fn push(&mut self, point: &MetricPoint) {
        // A result may combine identifiers with different field grants.
        // Keep a header only if every contributing source exposed it.
        for field in ["timestamp", "name", "type"] {
            if !point.row.record.contains_key(field) {
                self.template.record.remove(field);
            }
        }

        if matches!(point.row.record.get("overflow"), Some(Value::Bool(true))) {
            self.overflow = true;
        }
        if let Some(value) = point.row.record.get("value") {
            self.numeric.push(value);
        }
        if let Some(delta) = point.adjusted_delta {
            self.delta += delta;
        }
        if let Some(elapsed) = point.elapsed_ns {
            self.elapsed = self.elapsed.and_then(|total| total.checked_add(elapsed));
        }
        self.latest = self.latest.max(timestamp(&point.row));
    }

    /// A window of a `RATE` or `DELTA` is the sum of its pairs' deltas,
    /// over the time they cover for `RATE`; anything else is `function`
    /// over the values, or the overflow result if any was one.
    #[allow(
        clippy::cast_precision_loss,
        reason = "RATE is specified as a finite binary64 ratio over nanoseconds"
    )]
    fn value(
        &self,
        function: AggregateFunction,
        pairs: Option<Transform>,
    ) -> Result<Value, QueryError> {
        match pairs {
            Some(Transform::Rate) => {
                let elapsed = self.elapsed.ok_or(QueryError::InvalidTime)?;
                finite_value(self.delta * 1_000_000_000.0 / elapsed as f64)
            }
            Some(Transform::Delta) => finite_value(self.delta),
            _ if self.overflow => Ok(Value::Null),
            _ => self.numeric.finish(function),
        }
    }
}

/// What a fold costs held beside its template row.
const FOLD_OVERHEAD: usize = size_of::<Fold>() + size_of::<i64>() * 4;

/// Points folded into epoch-aligned windows of one width (PSPU §3.25).
struct Windows {
    width: i64,
    folds: BTreeMap<i64, Fold>,
    cost: usize,
}

impl Windows {
    const fn new(width: i64) -> Self {
        Self {
            width,
            folds: BTreeMap::new(),
            cost: 0,
        }
    }

    fn push(&mut self, point: &MetricPoint, held: &mut Held<'_>) -> Result<(), QueryError> {
        let start = timestamp(&point.row).div_euclid(self.width) * self.width;
        if let Some(fold) = self.folds.get_mut(&start) {
            fold.push(point);
            return Ok(());
        }
        let cost = FOLD_OVERHEAD + row_size(&point.row);
        held.add(cost)?;
        self.cost += cost;
        self.folds.insert(start, Fold::new(point));
        Ok(())
    }

    /// One row per window, starting at the window, with `tie` or else
    /// the window start as its tiebreaker. What the folds held is given
    /// back and the rows are held instead.
    #[allow(
        clippy::too_many_arguments,
        reason = "metric output metadata is explicit"
    )]
    fn rows(
        self,
        function: AggregateFunction,
        pairs: Option<Transform>,
        retain_labels: bool,
        output_name: Option<&str>,
        metric_type: i64,
        tie: Option<i64>,
        held: &mut Held<'_>,
    ) -> Result<Vec<Row>, QueryError> {
        let mut rows = Vec::with_capacity(self.folds.len());
        for (start, fold) in self.folds {
            let name = output_name.unwrap_or(&fold.template.identifier);
            let row = metric_aggregate_row(
                &fold.template,
                fold.value(function, pairs)?,
                start,
                retain_labels,
                name,
                metric_type,
                tie.unwrap_or(start),
            )?;
            held.add(row_size(&row))?;
            rows.push(row);
        }
        held.release(self.cost);
        Ok(rows)
    }
}

/// Where a metric query's transformed points go, chosen once from its
/// shape (PSPU §3.25), each keeping no more than its result needs.
struct MetricSink<'q> {
    output_name: &'q str,
    metric_type: i64,
    /// An unbracketed result keeps labels only when one series matched.
    single: bool,
    transform: Option<Transform>,
    mode: SinkMode,
}

enum SinkMode {
    /// Every point is a result: held whole.
    Points(Vec<Row>),
    /// Each series' latest point, as is when `rows`, otherwise reduced
    /// across series by `function`.
    Latest {
        current: Option<MetricPoint>,
        points: Vec<MetricPoint>,
        rows: bool,
        function: AggregateFunction,
    },
    /// `function` over each series' whole range.
    PerSeries {
        current: Option<Fold>,
        rows: Vec<Row>,
        function: AggregateFunction,
    },
    /// Each series' windows, kept per series.
    SeriesWindows {
        current: Windows,
        rows: Vec<Row>,
        function: AggregateFunction,
    },
    /// Each series' `RATE` or `DELTA` windows, combined across series by
    /// `function`.
    Combined {
        current: Windows,
        combined: Windows,
        function: AggregateFunction,
    },
    /// Windows over every series' points together.
    Shared {
        windows: Windows,
        function: AggregateFunction,
    },
}

impl<'q> MetricSink<'q> {
    fn new(
        query: &Query,
        output_name: &'q str,
        metric_type: i64,
        bracketed: bool,
        series_count: usize,
    ) -> Result<Self, QueryError> {
        let pairs = matches!(query.transform, Some(Transform::Rate | Transform::Delta));
        let latest = |rows, function| SinkMode::Latest {
            current: None,
            points: Vec::new(),
            rows,
            function,
        };
        let mode = match query.metric_aggregate {
            Some(MetricAggregate::Scalar(function)) if bracketed || query.since.is_some() => {
                SinkMode::PerSeries {
                    current: None,
                    rows: Vec::new(),
                    function,
                }
            }
            Some(MetricAggregate::Scalar(function)) => latest(false, function),
            Some(MetricAggregate::Window(function, width)) => {
                let width = i64::try_from(width).map_err(|_| QueryError::InvalidTime)?;
                if bracketed {
                    SinkMode::SeriesWindows {
                        current: Windows::new(width),
                        rows: Vec::new(),
                        function,
                    }
                } else if pairs {
                    SinkMode::Combined {
                        current: Windows::new(width),
                        combined: Windows::new(width),
                        function,
                    }
                } else {
                    SinkMode::Shared {
                        windows: Windows::new(width),
                        function,
                    }
                }
            }
            None if query.since.is_none() => latest(bracketed, AggregateFunction::Avg),
            None => SinkMode::Points(Vec::new()),
        };
        Ok(Self {
            output_name,
            metric_type,
            single: series_count == 1,
            transform: query.transform,
            mode,
        })
    }

    /// The transform whose pairs a window sums, if any.
    fn pairs(&self) -> Option<Transform> {
        self.transform
            .filter(|transform| matches!(transform, Transform::Rate | Transform::Delta))
    }

    fn push(&mut self, point: MetricPoint, held: &mut Held<'_>) -> Result<(), QueryError> {
        match &mut self.mode {
            SinkMode::Points(rows) => {
                held.add(row_size(&point.row))?;
                rows.push(point.row);
            }
            SinkMode::Latest { current, .. } => *current = Some(point),
            SinkMode::PerSeries { current, .. } => match current {
                Some(fold) => fold.push(&point),
                None => *current = Some(Fold::new(&point)),
            },
            SinkMode::SeriesWindows { current, .. } | SinkMode::Combined { current, .. } => {
                current.push(&point, held)?;
            }
            SinkMode::Shared { windows, .. } => windows.push(&point, held)?,
        }
        Ok(())
    }

    fn end_series(&mut self, series_id: i64, held: &mut Held<'_>) -> Result<(), QueryError> {
        let pairs = self.pairs();
        let single = self.single;
        let output_name = self.output_name;
        let metric_type = self.metric_type;
        match &mut self.mode {
            SinkMode::Points(_) | SinkMode::Shared { .. } => {}
            SinkMode::Latest {
                current, points, ..
            } => {
                if let Some(point) = current.take() {
                    held.add(row_size(&point.row))?;
                    points.push(point);
                }
            }
            SinkMode::PerSeries {
                current,
                rows,
                function,
            } => {
                if let Some(fold) = current.take() {
                    let name = fold.template.identifier.clone();
                    let row = metric_aggregate_row(
                        &fold.template,
                        fold.value(*function, None)?,
                        fold.latest,
                        true,
                        &name,
                        metric_type,
                        series_id,
                    )?;
                    held.add(row_size(&row))?;
                    rows.push(row);
                }
            }
            SinkMode::SeriesWindows {
                current,
                rows,
                function,
            } => {
                let width = current.width;
                let windows = core::mem::replace(current, Windows::new(width));
                rows.extend(windows.rows(
                    *function,
                    pairs,
                    true,
                    None,
                    metric_type,
                    Some(series_id),
                    held,
                )?);
            }
            SinkMode::Combined {
                current,
                combined,
                function,
            } => {
                let width = current.width;
                let windows = core::mem::replace(current, Windows::new(width));
                let rows = windows.rows(
                    *function,
                    pairs,
                    single,
                    Some(output_name),
                    metric_type,
                    Some(series_id),
                    held,
                )?;
                for row in rows {
                    held.release(row_size(&row));
                    combined.push(
                        &MetricPoint {
                            row,
                            adjusted_delta: None,
                            elapsed_ns: None,
                        },
                        held,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn finish(self, held: &mut Held<'_>) -> Result<Vec<Row>, QueryError> {
        let pairs = self.pairs();
        match self.mode {
            SinkMode::Points(rows)
            | SinkMode::PerSeries { rows, .. }
            | SinkMode::SeriesWindows { rows, .. } => Ok(rows),
            SinkMode::Latest {
                points, rows: true, ..
            } => Ok(points.into_iter().map(|point| point.row).collect()),
            SinkMode::Latest {
                points, function, ..
            } => {
                let Some((first, rest)) = points.split_first() else {
                    return Ok(Vec::new());
                };
                let mut fold = Fold::new(first);
                for point in rest {
                    fold.push(point);
                }
                Ok(vec![metric_aggregate_row(
                    &fold.template,
                    fold.value(function, None)?,
                    fold.latest,
                    self.single,
                    self.output_name,
                    self.metric_type,
                    0,
                )?])
            }
            SinkMode::Combined {
                combined, function, ..
            } => combined.rows(
                function,
                None,
                self.single,
                Some(self.output_name),
                self.metric_type,
                None,
                held,
            ),
            SinkMode::Shared { windows, function } => windows.rows(
                function,
                pairs,
                self.single,
                Some(self.output_name),
                self.metric_type,
                Some(0),
                held,
            ),
        }
    }
}

#[allow(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "the rollup fast path keeps query, authorization and cache policy explicit"
)]
fn execute_rollup_window_query(
    connection: &Connection,
    series: &[(i64, String, i64, Record)],
    query: &Query,
    output_name: &str,
    metric_type: i64,
    bracketed: bool,
    since: i64,
    until: i64,
    authorizer: &Authorizer,
    referenced: &[String],
    authorization: &mut AuthorizationCache,
    limits: Option<&Limits>,
    held: &mut Held<'_>,
) -> Result<Option<Vec<Row>>, QueryError> {
    let Some(limits) = limits else {
        return Ok(None);
    };
    let Some(MetricAggregate::Window(function, width)) = query.metric_aggregate else {
        return Ok(None);
    };
    if limits.adaptive_rollup_max_rows == 0
        || query.since.is_none()
        || !query.predicates.is_empty()
        || !query.cross_filters.is_empty()
        || (!bracketed && series.len() != 1)
    {
        return Ok(None);
    }
    let width = i64::try_from(width).map_err(|_| QueryError::InvalidTime)?;
    let first_full = if since.rem_euclid(width) == 0 {
        since
    } else {
        since
            .div_euclid(width)
            .checked_add(1)
            .and_then(|value| value.checked_mul(width))
            .ok_or(QueryError::InvalidTime)?
    };
    let full_end = until
        .div_euclid(width)
        .checked_mul(width)
        .ok_or(QueryError::InvalidTime)?;
    if first_full >= full_end {
        return Ok(None);
    }
    let transform = transform_code(query.transform);
    let function_code = aggregate_code(function);
    let expected = usize::try_from(
        full_end
            .checked_sub(first_full)
            .ok_or(QueryError::InvalidTime)?
            / width,
    )
    .map_err(|_| QueryError::InvalidTime)?;
    let mut output = Vec::new();
    let mut pending = Vec::new();
    for (series_id, name, _, labels) in series {
        check_deadline(Some(limits.deadline))?;
        let cached = read_valid_rollups(
            connection,
            *series_id,
            first_full,
            full_end,
            width,
            transform,
            function_code,
        )?;
        let complete = cached.len() == expected;
        let mut template = metric_template(name, metric_type, labels)?;
        if !authorize_row(
            authorizer,
            Namespace::Metrics,
            &mut template,
            referenced,
            authorization,
        )? {
            continue;
        }
        let series = SeriesRef {
            id: *series_id,
            name,
            metric_type,
            labels,
        };
        let mut windows =
            |lower: i64, upper: i64, sources: Option<&mut RollupSources>, held: &mut Held<'_>| {
                series_windows(
                    connection,
                    &series,
                    lower,
                    upper,
                    query.transform,
                    function,
                    width,
                    if bracketed { name } else { output_name },
                    limits.deadline,
                    &mut |row| {
                        authorize_row(
                            authorizer,
                            Namespace::Metrics,
                            row,
                            referenced,
                            authorization,
                        )
                    },
                    sources,
                    held,
                )
            };
        if complete {
            for cached in cached.values() {
                let value = match cached.value {
                    Some(value) => Value::Float(value),
                    None if cached.overflow => Value::Null,
                    None => continue,
                };
                let row = metric_aggregate_row(
                    &template,
                    value,
                    cached.window_start,
                    true,
                    if bracketed { name } else { output_name },
                    metric_type,
                    *series_id,
                )?;
                held.add(row_size(&row))?;
                output.push(row);
            }
            for (lower, upper) in [(since, first_full), (full_end, until)] {
                if lower < upper {
                    output.extend(windows(lower, upper, None, held)?);
                }
            }
            continue;
        }

        let mut sources = RollupSources::new(width);
        let rows = windows(since, until, Some(&mut sources), held)?;
        if sources.count >= limits.adaptive_rollup_min_samples {
            let by_start: HashMap<_, _> = rows.iter().map(|row| (timestamp(row), row)).collect();
            let mut start = first_full;
            while start < full_end && pending.len() < limits.adaptive_rollup_batch_rows {
                let (source_max_sample_id, baseline_id) = sources.proof(start);
                if !cached.contains_key(&start) {
                    let source_baseline_sample_id =
                        if matches!(query.transform, Some(Transform::Rate | Transform::Delta)) {
                            baseline_id
                        } else {
                            None
                        };
                    let row = by_start.get(&start).copied();
                    let overflow = row.is_some_and(|row| {
                        matches!(row.record.get("overflow"), Some(Value::Bool(true)))
                    });
                    let value = row.and_then(|row| match row.record.get("value") {
                        Some(Value::Float(value)) => Some(*value),
                        _ => None,
                    });
                    pending.push(MetricRollup {
                        series_id: *series_id,
                        window_start: start,
                        window_width: width,
                        transform,
                        function: function_code,
                        value,
                        overflow,
                        source_max_sample_id,
                        source_baseline_sample_id,
                    });
                }
                start = start.checked_add(width).ok_or(QueryError::InvalidTime)?;
            }
        }
        held.release(sources.cost);
        output.extend(rows);
    }
    if !pending.is_empty()
        && let Some(sender) = &limits.rollups
    {
        // Rollups are a disposable cache. A busy or stopped maintenance worker must
        // not delay delivery of the result already calculated from authoritative data.
        let _ = sender.try_send(RollupMaintenance { rows: pending });
    }
    Ok(Some(output))
}

/// Per window, the largest id of the visible samples in it and the id of
/// its last in order: what proves a cached rollup still fresh, kept
/// without keeping the samples (TRM §5.6).
struct RollupSources {
    width: i64,
    windows: BTreeMap<i64, (i64, i64)>,
    count: usize,
    cost: usize,
}

/// What one window's proof costs held.
const SOURCE_COST: usize = size_of::<(i64, (i64, i64))>() * 3;

impl RollupSources {
    const fn new(width: i64) -> Self {
        Self {
            width,
            windows: BTreeMap::new(),
            count: 0,
            cost: 0,
        }
    }

    fn push(&mut self, at: i64, id: i64, held: &mut Held<'_>) -> Result<(), QueryError> {
        self.count += 1;
        let start = at.div_euclid(self.width) * self.width;
        if let Some((largest, last)) = self.windows.get_mut(&start) {
            *largest = (*largest).max(id);
            *last = id;
            return Ok(());
        }
        held.add(SOURCE_COST)?;
        self.cost += SOURCE_COST;
        self.windows.insert(start, (id, id));
        Ok(())
    }

    /// The largest sample id in the window at `start`, 0 when it has
    /// none, and the id of the last sample before it.
    fn proof(&self, start: i64) -> (i64, Option<i64>) {
        (
            self.windows.get(&start).map_or(0, |(largest, _)| *largest),
            self.windows
                .range(..start)
                .next_back()
                .map(|(_, (_, last))| *last),
        )
    }
}

/// One series' windows over `[since, until)`, folded as its samples are
/// read, with each window's proof noted in `sources` when given.
#[allow(
    clippy::too_many_arguments,
    reason = "a series read carries the authorized query context explicitly"
)]
fn series_windows(
    connection: &Connection,
    series: &SeriesRef<'_>,
    since: i64,
    until: i64,
    transform: Option<Transform>,
    function: AggregateFunction,
    width: i64,
    output_name: &str,
    deadline: Instant,
    visible: &mut dyn FnMut(&mut Row) -> Result<bool, QueryError>,
    mut sources: Option<&mut RollupSources>,
    held: &mut Held<'_>,
) -> Result<Vec<Row>, QueryError> {
    let mut windows = Windows::new(width);
    let mut transformer = Transformer::new(transform, since);
    read_series(
        connection,
        series,
        since,
        until,
        transform,
        Some(deadline),
        visible,
        &mut |input| {
            if let Some(sources) = sources.as_deref_mut() {
                sources.push(timestamp(&input.row), row_id(&input.row), held)?;
            }
            transformer
                .push(input)?
                .map_or(Ok(()), |point| windows.push(&point, held))
        },
    )?;
    windows.rows(
        function,
        transform.filter(|transform| matches!(transform, Transform::Rate | Transform::Delta)),
        true,
        Some(output_name),
        series.metric_type,
        Some(series.id),
        held,
    )
}

#[derive(Debug)]
struct CachedRollup {
    window_start: i64,
    value: Option<f64>,
    overflow: bool,
}

fn read_valid_rollups(
    connection: &Connection,
    series_id: i64,
    first: i64,
    end: i64,
    width: i64,
    transform: i64,
    function: i64,
) -> Result<HashMap<i64, CachedRollup>, QueryError> {
    let pair = matches!(transform, 1 | 2);
    let mut statement = connection.prepare(
        "SELECT r.window_start, r.value, r.overflow FROM rollups r \
         WHERE r.series_id=?1 AND r.window_start>=?2 AND r.window_start<?3 \
         AND r.window_width=?4 AND r.transform=?5 AND r.function=?6 \
         AND NOT EXISTS (SELECT 1 FROM samples s WHERE s.series_id=r.series_id \
             AND s.timestamp>=r.window_start AND s.timestamp<r.window_start+r.window_width \
             AND s.id>r.source_max_sample_id) \
         AND (?7=0 OR r.source_baseline_sample_id IS \
             (SELECT s.id FROM samples s WHERE s.series_id=r.series_id \
              AND s.timestamp<r.window_start ORDER BY s.timestamp DESC, s.id DESC LIMIT 1))",
    )?;
    let mut rows = statement.query(params![
        series_id, first, end, width, transform, function, pair
    ])?;
    let mut output = HashMap::new();
    while let Some(row) = rows.next()? {
        let cached = CachedRollup {
            window_start: row.get(0)?,
            value: row.get(1)?,
            overflow: row.get(2)?,
        };
        if cached.value.is_some_and(|value| !value.is_finite()) {
            continue;
        }
        output.insert(cached.window_start, cached);
    }
    Ok(output)
}

fn metric_template(name: &str, metric_type: i64, labels: &Record) -> Result<Row, QueryError> {
    let mut record = labels.clone();
    record.insert("timestamp".into(), Value::Signed(0));
    record.insert("boot_id".into(), Value::Null);
    record.insert("name".into(), Value::String(name.to_owned()));
    record.insert(
        "type".into(),
        Value::String(metric_type_name(metric_type)?.into()),
    );
    record.insert("value".into(), Value::Null);
    Ok(Row {
        record,
        identifier: name.to_owned(),
        at: 0,
        metric_labels: canonical_labels(labels).collect(),
        tie: Tie::Single(0),
    })
}

const fn transform_code(transform: Option<Transform>) -> i64 {
    match transform {
        None => 0,
        Some(Transform::Rate) => 1,
        Some(Transform::Delta) => 2,
        Some(Transform::Percentile(value)) => value as i64,
    }
}

const fn aggregate_code(function: AggregateFunction) -> i64 {
    match function {
        AggregateFunction::Avg => 0,
        AggregateFunction::Min => 1,
        AggregateFunction::Max => 2,
        AggregateFunction::Sum => 3,
    }
}

const fn row_id(row: &Row) -> i64 {
    match row.tie {
        Tie::Single(id) => id,
        Tie::Event { .. } => unreachable!(),
    }
}

#[derive(Debug)]
struct MetricInput {
    row: Row,
    number: f64,
    histogram: Option<Vec<u8>>,
}

#[derive(Debug)]
struct MetricPoint {
    row: Row,
    adjusted_delta: Option<f64>,
    elapsed_ns: Option<i64>,
}

fn read_metric_input(
    sample: &rusqlite::Row<'_>,
    name: &str,
    metric_type: i64,
    labels: &Record,
) -> Result<MetricInput, QueryError> {
    let id: i64 = sample.get(0)?;
    let boot: Vec<u8> = sample.get(1)?;
    let timestamp: i64 = sample.get(2)?;
    let number: f64 = sample.get(3)?;
    if !number.is_finite() {
        return Err(QueryError::NonFinite);
    }
    let histogram: Option<Vec<u8>> = sample.get(4)?;
    let mut record = labels.clone();
    record.insert("timestamp".into(), Value::Signed(timestamp));
    record.insert("boot_id".into(), option_guid(Some(&boot)));
    record.insert("name".into(), Value::String(name.to_owned()));
    record.insert(
        "type".into(),
        Value::String(metric_type_name(metric_type)?.into()),
    );
    record.insert(
        "value".into(),
        if metric_type == 2 {
            Value::Null
        } else {
            Value::Float(number)
        },
    );
    Ok(MetricInput {
        row: Row {
            record,
            identifier: name.to_owned(),
            at: timestamp,
            metric_labels: canonical_labels(labels).collect(),
            tie: Tie::Single(id),
        },
        number,
        histogram,
    })
}

const fn validate_metric_pipeline(
    metric_type: i64,
    transform: Option<Transform>,
) -> Result<(), QueryError> {
    match (metric_type, transform) {
        (0 | 1, None)
        | (0, Some(Transform::Rate | Transform::Delta))
        | (2, Some(Transform::Percentile(_))) => Ok(()),
        (2, _) => Err(QueryError::HistogramNeedsPercentile),
        (0 | 1, Some(Transform::Percentile(_))) => Err(QueryError::PercentileNeedsHistogram),
        (1, Some(Transform::Rate | Transform::Delta)) => Err(QueryError::RateNeedsCounter),
        _ => Err(QueryError::InvalidMetricType),
    }
}

// The metric pipeline as it was before windows folded, gathering every
// sample first: kept as the reference the fold is tested against.

#[cfg(test)]
#[allow(
    clippy::cast_precision_loss,
    reason = "RATE is specified as a finite binary64 ratio over nanoseconds"
)]
fn transform_metric_inputs(
    inputs: Vec<MetricInput>,
    transform: Option<Transform>,
    since: i64,
) -> Result<Vec<MetricPoint>, QueryError> {
    match transform {
        None => Ok(inputs
            .into_iter()
            .filter(|input| timestamp(&input.row) >= since)
            .map(|input| MetricPoint {
                row: input.row,
                adjusted_delta: None,
                elapsed_ns: None,
            })
            .collect()),
        Some(Transform::Percentile(percentile)) => {
            let mut output = Vec::new();
            for mut input in inputs {
                if timestamp(&input.row) < since {
                    continue;
                }
                let value = histogram_percentile(
                    input
                        .histogram
                        .as_deref()
                        .ok_or(QueryError::InvalidHistogram)?,
                    percentile,
                )?;
                match value {
                    PercentileValue::Empty => continue,
                    PercentileValue::Finite(value) => {
                        set_metric_result(&mut input.row.record, finite_value(value)?, false);
                    }
                    PercentileValue::Overflow => {
                        set_metric_result(&mut input.row.record, Value::Null, true);
                    }
                }
                output.push(MetricPoint {
                    row: input.row,
                    adjusted_delta: None,
                    elapsed_ns: None,
                });
            }
            Ok(output)
        }
        Some(transform @ (Transform::Rate | Transform::Delta)) => {
            let mut output = Vec::new();
            for pair in inputs.windows(2) {
                let earlier = &pair[0];
                let later = &pair[1];
                let later_timestamp = timestamp(&later.row);
                let elapsed_ns = later_timestamp.saturating_sub(timestamp(&earlier.row));
                if later_timestamp < since || elapsed_ns <= 0 {
                    continue;
                }
                let adjusted_delta = if later.number >= earlier.number {
                    later.number - earlier.number
                } else {
                    later.number
                };
                let value = if transform == Transform::Rate {
                    adjusted_delta * 1_000_000_000.0 / elapsed_ns as f64
                } else {
                    adjusted_delta
                };
                let mut row = Row {
                    record: later.row.record.clone(),
                    identifier: later.row.identifier.clone(),
                    at: later.row.at,
                    metric_labels: later.row.metric_labels.clone(),
                    tie: later.row.tie,
                };
                row.record.insert("value".into(), finite_value(value)?);
                output.push(MetricPoint {
                    row,
                    adjusted_delta: Some(adjusted_delta),
                    elapsed_ns: Some(elapsed_ns),
                });
            }
            Ok(output)
        }
    }
}

const fn finite_value(value: f64) -> Result<Value, QueryError> {
    if value.is_finite() {
        Ok(Value::Float(value))
    } else {
        Err(QueryError::NonFinite)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum PercentileValue {
    Empty,
    Finite(f64),
    Overflow,
}

fn histogram_percentile(bytes: &[u8], percentile: u8) -> Result<PercentileValue, QueryError> {
    let Value::Map(entries) = super::value::decode(bytes).map_err(QueryError::Peios)? else {
        return Err(QueryError::InvalidHistogram);
    };
    let get = |name: &str| {
        entries.iter().find_map(|(key, value)| {
            matches!(key, Value::String(key) if key == name).then_some(value)
        })
    };
    let total = match get("total_count") {
        Some(Value::Unsigned(value)) => *value,
        Some(Value::Signed(value)) => {
            u64::try_from(*value).map_err(|_| QueryError::InvalidHistogram)?
        }
        _ => return Err(QueryError::InvalidHistogram),
    };
    if total == 0 {
        return Ok(PercentileValue::Empty);
    }
    let rank = total
        .checked_mul(u64::from(percentile))
        .and_then(|value| value.checked_add(99))
        .map(|value| value / 100)
        .ok_or(QueryError::InvalidHistogram)?;
    let (Some(Value::Array(boundaries)), Some(Value::Array(counts))) =
        (get("boundaries"), get("counts"))
    else {
        return Err(QueryError::InvalidHistogram);
    };
    for (boundary, count) in boundaries.iter().zip(counts) {
        let count = match count {
            Value::Unsigned(value) => *value,
            Value::Signed(value) => {
                u64::try_from(*value).map_err(|_| QueryError::InvalidHistogram)?
            }
            _ => return Err(QueryError::InvalidHistogram),
        };
        if count >= rank {
            return match boundary {
                Value::Float(value) => Ok(PercentileValue::Finite(*value)),
                _ => Err(QueryError::InvalidHistogram),
            };
        }
    }
    Ok(PercentileValue::Overflow)
}

#[cfg(test)]
type ResolvedMetricSeries = (i64, String, Vec<MetricPoint>);

/// The reference for `MetricSink`: every series' points, gathered whole.
#[cfg(test)]
#[allow(
    clippy::too_many_lines,
    reason = "the mutually exclusive metric result modes are kept together"
)]
fn finish_metric_query(
    mut series: Vec<ResolvedMetricSeries>,
    query: &Query,
    output_name: &str,
    metric_type: i64,
    bracketed: bool,
    series_count: usize,
) -> Result<Vec<Row>, QueryError> {
    match query.metric_aggregate {
        Some(MetricAggregate::Scalar(function)) if bracketed || query.since.is_some() => {
            let mut output = Vec::new();
            for (series_id, _, points) in series {
                if let Some(row) = aggregate_points(
                    &points,
                    function,
                    points
                        .iter()
                        .map(|point| timestamp(&point.row))
                        .max()
                        .unwrap_or(0),
                    true,
                    None,
                    metric_type,
                    series_id,
                )? {
                    output.push(row);
                }
            }
            Ok(output)
        }
        Some(MetricAggregate::Scalar(function)) => {
            let latest = take_latest_per_series(&mut series);
            aggregate_points(
                &latest,
                function,
                latest
                    .iter()
                    .map(|point| timestamp(&point.row))
                    .max()
                    .unwrap_or(0),
                series_count == 1,
                Some(output_name),
                metric_type,
                0,
            )
            .map(Option::into_iter)
            .map(Iterator::collect)
        }
        Some(MetricAggregate::Window(function, width)) => {
            if bracketed {
                let mut output = Vec::new();
                for (series_id, _, points) in series {
                    output.extend(window_points(
                        points,
                        function,
                        width,
                        query.transform,
                        true,
                        None,
                        metric_type,
                        series_id,
                    )?);
                }
                Ok(output)
            } else if matches!(query.transform, Some(Transform::Rate | Transform::Delta)) {
                let retain_labels = series_count == 1;
                let mut per_series = Vec::new();
                for (series_id, _, points) in series {
                    per_series.extend(window_points(
                        points,
                        function,
                        width,
                        query.transform,
                        retain_labels,
                        Some(output_name),
                        metric_type,
                        series_id,
                    )?);
                }
                combine_window_rows(
                    per_series,
                    function,
                    retain_labels,
                    output_name,
                    metric_type,
                )
            } else {
                let points = series
                    .into_iter()
                    .flat_map(|(_, _, points)| points)
                    .collect();
                window_points(
                    points,
                    function,
                    width,
                    query.transform,
                    series_count == 1,
                    Some(output_name),
                    metric_type,
                    0,
                )
            }
        }
        None if bracketed => {
            if query.since.is_none() {
                Ok(take_latest_per_series(&mut series)
                    .into_iter()
                    .map(|point| point.row)
                    .collect())
            } else {
                Ok(series
                    .into_iter()
                    .flat_map(|(_, _, points)| points)
                    .map(|point| point.row)
                    .collect())
            }
        }
        None if query.since.is_none() => {
            let latest = take_latest_per_series(&mut series);
            aggregate_points(
                &latest,
                AggregateFunction::Avg,
                latest
                    .iter()
                    .map(|point| timestamp(&point.row))
                    .max()
                    .unwrap_or(0),
                series_count == 1,
                Some(output_name),
                metric_type,
                0,
            )
            .map(Option::into_iter)
            .map(Iterator::collect)
        }
        None => Ok(series
            .into_iter()
            .flat_map(|(_, _, points)| points)
            .map(|point| point.row)
            .collect()),
    }
}

#[cfg(test)]
fn take_latest_per_series(series: &mut [ResolvedMetricSeries]) -> Vec<MetricPoint> {
    series
        .iter_mut()
        .filter_map(|(_, _, points)| points.pop())
        .collect()
}

#[cfg(test)]
#[allow(
    clippy::too_many_arguments,
    reason = "metric output metadata is explicit"
)]
fn aggregate_points(
    points: &[MetricPoint],
    function: AggregateFunction,
    output_timestamp: i64,
    retain_labels: bool,
    output_name: Option<&str>,
    metric_type: i64,
    tie: i64,
) -> Result<Option<Row>, QueryError> {
    if points.is_empty() {
        return Ok(None);
    }
    let rows: Vec<_> = points.iter().map(|point| point.row.clone()).collect();
    let value = metric_numeric_aggregate(&rows, function)?;
    let name = output_name.unwrap_or(&points[0].row.identifier);
    Ok(Some(metric_aggregate_row(
        &points[0].row,
        value,
        output_timestamp,
        retain_labels,
        name,
        metric_type,
        tie,
    )?))
}

#[allow(
    clippy::too_many_arguments,
    reason = "metric output metadata is explicit"
)]
fn metric_aggregate_row(
    template: &Row,
    value: Value,
    output_timestamp: i64,
    retain_labels: bool,
    output_name: &str,
    metric_type: i64,
    tie: i64,
) -> Result<Row, QueryError> {
    let mut record = if retain_labels {
        template.record.clone()
    } else {
        Record::new()
    };
    record.remove("boot_id");
    for (field, value) in [
        ("timestamp", Value::Signed(output_timestamp)),
        ("name", Value::String(output_name.to_owned())),
        ("type", Value::String(metric_type_name(metric_type)?.into())),
    ] {
        if template.record.contains_key(field) {
            record.insert(field.into(), value);
        }
    }
    let overflow = matches!(value, Value::Null);
    set_metric_result(&mut record, value, overflow);
    Ok(Row {
        record,
        identifier: output_name.to_owned(),
        at: output_timestamp,
        metric_labels: if retain_labels {
            template.metric_labels.clone()
        } else {
            Box::default()
        },
        tie: Tie::Single(tie),
    })
}

#[cfg(test)]
#[allow(
    clippy::cast_precision_loss,
    clippy::too_many_arguments,
    reason = "metric output metadata is explicit and RATE is binary64"
)]
fn window_points(
    points: Vec<MetricPoint>,
    function: AggregateFunction,
    width: u64,
    transform: Option<Transform>,
    retain_labels: bool,
    output_name: Option<&str>,
    metric_type: i64,
    tie: i64,
) -> Result<Vec<Row>, QueryError> {
    let width = i64::try_from(width).map_err(|_| QueryError::InvalidTime)?;
    let mut windows: BTreeMap<i64, Vec<MetricPoint>> = BTreeMap::new();
    for point in points {
        let start = timestamp(&point.row).div_euclid(width) * width;
        windows.entry(start).or_default().push(point);
    }
    let pair_transform = matches!(transform, Some(Transform::Rate | Transform::Delta));
    let mut output = Vec::with_capacity(windows.len());
    for (start, points) in windows {
        let value = if pair_transform {
            let delta = points
                .iter()
                .map(|point| point.adjusted_delta.expect("pair transform delta"))
                .sum::<f64>();
            if transform == Some(Transform::Rate) {
                let elapsed = points
                    .iter()
                    .map(|point| point.elapsed_ns.expect("pair transform elapsed"))
                    .try_fold(0_i64, i64::checked_add)
                    .ok_or(QueryError::InvalidTime)?;
                finite_value(delta * 1_000_000_000.0 / elapsed as f64)?
            } else {
                finite_value(delta)?
            }
        } else {
            let rows: Vec<_> = points.iter().map(|point| point.row.clone()).collect();
            metric_numeric_aggregate(&rows, function)?
        };
        let name = output_name.unwrap_or(&points[0].row.identifier);
        output.push(metric_aggregate_row(
            &points[0].row,
            value,
            start,
            retain_labels,
            name,
            metric_type,
            tie,
        )?);
    }
    Ok(output)
}

#[cfg(test)]
fn combine_window_rows(
    rows: Vec<Row>,
    function: AggregateFunction,
    retain_labels: bool,
    output_name: &str,
    metric_type: i64,
) -> Result<Vec<Row>, QueryError> {
    let mut windows: BTreeMap<i64, Vec<Row>> = BTreeMap::new();
    for row in rows {
        windows.entry(timestamp(&row)).or_default().push(row);
    }
    let mut output = Vec::with_capacity(windows.len());
    for (start, rows) in windows {
        let value = metric_numeric_aggregate(&rows, function)?;
        output.push(metric_aggregate_row(
            &rows[0],
            value,
            start,
            retain_labels,
            output_name,
            metric_type,
            start,
        )?);
    }
    Ok(output)
}

#[cfg(test)]
fn metric_numeric_aggregate(
    rows: &[Row],
    function: AggregateFunction,
) -> Result<Value, QueryError> {
    if rows
        .iter()
        .any(|row| matches!(row.record.get("overflow"), Some(Value::Bool(true))))
    {
        Ok(Value::Null)
    } else {
        numeric_aggregate(rows, "value", function)
    }
}

fn set_metric_result(record: &mut Record, value: Value, overflow: bool) {
    record.insert("value".into(), value);
    if overflow {
        record.insert("overflow".into(), Value::Bool(true));
    } else {
        record.remove("overflow");
    }
}

/// The SORT keys, then the metric tiebreakers of TRM §6.2: timestamp,
/// name, canonical labels and sample id, all ascending.
fn sort_metric_rows(rows: &mut [Row], query: &Query) {
    rows.sort_by(|left, right| {
        sort_key_order(left, right, query)
            .then_with(|| timestamp(left).cmp(&timestamp(right)))
            .then_with(|| left.identifier.cmp(&right.identifier))
            .then_with(|| left.metric_labels.cmp(&right.metric_labels))
            .then_with(|| left.tie.cmp(&right.tie))
    });
}

/// The fields of a metric record that are not labels (PSPU §3.22).
const METRIC_FIELDS: [&str; 6] = ["timestamp", "boot_id", "name", "type", "value", "overflow"];

/// Capture canonical labels before access filtering: sorted by key,
/// each `key=value`, joined with commas (PSPU §3.13). The record's map
/// is already in key byte order.
fn canonical_labels(record: &Record) -> impl Iterator<Item = u8> + '_ {
    record
        .iter()
        .filter(|(key, _)| !METRIC_FIELDS.contains(&key.as_str()))
        .enumerate()
        .flat_map(|(index, (key, value))| {
            let value: &[u8] = match value {
                Value::String(value) => value.as_bytes(),
                _ => &[],
            };
            (index != 0)
                .then_some(b',')
                .into_iter()
                .chain(key.bytes())
                .chain(core::iter::once(b'='))
                .chain(value.iter().copied())
        })
}

fn parse_labels(canonical: &str) -> Record {
    if canonical.is_empty() {
        return Record::new();
    }
    canonical
        .split(',')
        .filter_map(|pair| pair.split_once('='))
        .map(|(key, value)| (key.to_owned(), Value::String(value.to_owned())))
        .collect()
}

const fn metric_type_name(metric_type: i64) -> Result<&'static str, QueryError> {
    match metric_type {
        0 => Ok("counter"),
        1 => Ok("gauge"),
        2 => Ok("histogram"),
        _ => Err(QueryError::InvalidMetricType),
    }
}

fn open_read_only(path: &Path, deadline: Option<Instant>) -> Result<Connection, QueryError> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(QueryError::Sql)?;
    eventd_core::payload_index::register(&connection)?;
    if let Some(deadline) = deadline {
        connection.progress_handler(1_000, Some(move || Instant::now() >= deadline));
    }
    Ok(connection)
}

fn option_unsigned(value: Option<u64>) -> Value {
    value.map_or(Value::Null, Value::Unsigned)
}

fn option_guid(value: Option<&[u8]>) -> Value {
    value
        .and_then(guid_string)
        .map_or(Value::Null, Value::String)
}

fn realtime_nanoseconds() -> Result<i64, QueryError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| QueryError::InvalidTime)?;
    i64::try_from(elapsed.as_nanos()).map_err(|_| QueryError::InvalidTime)
}

fn check_deadline(deadline: Option<Instant>) -> Result<(), QueryError> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        Err(QueryError::Timeout)
    } else {
        Ok(())
    }
}

fn ascii_contains(value: &str, needle: &str) -> bool {
    let value: Vec<_> = value.bytes().map(super::value::fold_ascii).collect();
    let needle: Vec<_> = needle.bytes().map(super::value::fold_ascii).collect();
    needle.is_empty() || value.windows(needle.len()).any(|window| window == needle)
}

fn ascii_starts_with(value: &str, needle: &str) -> bool {
    value.len() >= needle.len() && ascii_equal(&value[..needle.len()], needle)
}

fn ascii_ends_with(value: &str, needle: &str) -> bool {
    value.len() >= needle.len() && ascii_equal(&value[value.len() - needle.len()..], needle)
}

fn glob_matches(pattern: &str, value: &str) -> bool {
    let pattern: Vec<_> = pattern.bytes().map(super::value::fold_ascii).collect();
    let value: Vec<_> = value.bytes().map(super::value::fold_ascii).collect();
    let (mut pattern_index, mut value_index, mut star, mut retry) = (0, 0, None, 0);
    while value_index < value.len() {
        if pattern.get(pattern_index) == value.get(value_index) {
            pattern_index += 1;
            value_index += 1;
        } else if pattern.get(pattern_index) == Some(&b'*') {
            star = Some(pattern_index);
            pattern_index += 1;
            retry = value_index;
        } else if let Some(star_index) = star {
            pattern_index = star_index + 1;
            retry += 1;
            value_index = retry;
        } else {
            return false;
        }
    }
    pattern[pattern_index..].iter().all(|byte| *byte == b'*')
}

#[derive(Debug)]
pub enum QueryError {
    Sql(rusqlite::Error),
    Peios(peios::Error),
    Security(super::security::SecurityError),
    Timeout,
    InvalidTime,
    NonFinite,
    MixedMetricTypes,
    InvalidMetricType,
    InvalidHistogram,
    HistogramNeedsPercentile,
    PercentileNeedsHistogram,
    RateNeedsCounter,
    MetricNeedsWindow,
    CrossTypeRangeTooLarge,
    CrossMetricNeedsSelector,
    CrossMetricHistogram,
    DistinctStreamLimit,
    HeldLimit,
    /// The result could not be sent: the client is gone or too slow, and
    /// there is nobody left to send an error to.
    Delivery(Box<super::QuerySocketError>),
}

impl QueryError {
    pub fn delivery(error: super::QuerySocketError) -> Self {
        Self::Delivery(Box::new(error))
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sql(error) => write!(formatter, "query storage failure: {error}"),
            Self::Peios(error) => write!(formatter, "query decoding failure: {error}"),
            Self::Security(error) => write!(formatter, "{error}"),
            Self::Timeout => formatter.write_str("query timed out"),
            Self::InvalidTime => formatter.write_str("query time is outside the timestamp domain"),
            Self::NonFinite => formatter.write_str("aggregation produced a non-finite value"),
            Self::MixedMetricTypes => {
                formatter.write_str("metric selector spans more than one type")
            }
            Self::InvalidMetricType => {
                formatter.write_str("metric store contains an invalid series type")
            }
            Self::InvalidHistogram => {
                formatter.write_str("metric store contains an invalid histogram")
            }
            Self::HistogramNeedsPercentile => {
                formatter.write_str("histogram query requires P50, P95 or P99")
            }
            Self::PercentileNeedsHistogram => {
                formatter.write_str("percentile transform requires a histogram")
            }
            Self::RateNeedsCounter => {
                formatter.write_str("RATE and DELTA require a counter series")
            }
            Self::MetricNeedsWindow => {
                formatter.write_str("multiple metric series over time require a window aggregation")
            }
            Self::CrossTypeRangeTooLarge => formatter.write_str(
                "cross-type query range is too large; narrow it with SINCE or UNTIL",
            ),
            Self::CrossMetricNeedsSelector => formatter.write_str(
                "cross-type metric selector matches multiple series; use a bracketed or narrower selector",
            ),
            Self::CrossMetricHistogram => {
                formatter.write_str("cross-type metric conditions require a counter or gauge series")
            }
            Self::DistinctStreamLimit => {
                formatter.write_str("DISTINCT stream exceeds its seen-value limit")
            }
            Self::HeldLimit => formatter.write_str(
                "query needs more memory than eventd lets running queries hold \
                 (MaxQueryHeldBytes): narrow it, add TAKE, or try again later",
            ),
            Self::Delivery(error) => write!(formatter, "query results could not be sent: {error}"),
        }
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sql(error) => Some(error),
            Self::Peios(error) => Some(error),
            Self::Security(error) => Some(error),
            Self::Delivery(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for QueryError {
    fn from(error: rusqlite::Error) -> Self {
        if matches!(
            error.sqlite_error_code(),
            Some(rusqlite::ErrorCode::OperationInterrupted)
        ) {
            Self::Timeout
        } else {
            Self::Sql(error)
        }
    }
}

impl From<super::security::SecurityError> for QueryError {
    fn from(error: super::security::SecurityError) -> Self {
        Self::Security(error)
    }
}

#[cfg(test)]
mod tests;
