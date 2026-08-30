//! Read-only query execution over event, log and metric stores.

use core::cmp::Ordering;
use core::fmt;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags, params};

use super::security::{Authorizer, Namespace};
use super::value::{Record, Value, ascii_equal, flatten_event_payload, guid_string, language_cmp};
use crate::query_language::{
    AggregateFunction, Expr, GroupFunction, Literal, MetricAggregate, Operator, Query,
    RecordAggregate, Source, TimeExpr, Transform,
};

pub struct Stores {
    pub event_paths: Vec<PathBuf>,
    pub log_path: PathBuf,
    pub metric_path: PathBuf,
}

pub struct Limits {
    pub timeout: Duration,
}

pub fn execute(
    query: &Query,
    stores: &Stores,
    authorizer: &Authorizer,
    limits: &Limits,
) -> Result<Vec<Record>, QueryError> {
    let evaluation_time = realtime_nanoseconds()?;
    let deadline = Instant::now() + limits.timeout;
    let (since, until) = time_range(query, evaluation_time)?;
    if since >= until {
        return Ok(Vec::new());
    }
    let rows = match &query.source {
        Source::Events { pattern } => read_events(
            &stores.event_paths,
            pattern.as_deref(),
            since,
            until,
            deadline,
        )?,
        Source::Logs {
            origins,
            error_only,
            containing,
        } => read_logs(
            &stores.log_path,
            origins,
            *error_only,
            containing.as_deref(),
            since,
            until,
            deadline,
        )?,
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
            );
        }
    };

    let namespace = match query.source {
        Source::Events { .. } => Namespace::Events,
        Source::Logs { .. } => Namespace::Logs,
        Source::Metric { .. } => unreachable!(),
    };
    let referenced = referenced_fields(query);
    let mut cache = HashMap::new();
    let mut visible = Vec::with_capacity(rows.len());
    for mut row in rows {
        check_deadline(deadline)?;
        if authorize_row(authorizer, namespace, &mut row, &referenced, &mut cache)?
            && query
                .predicates
                .iter()
                .all(|predicate| evaluate(predicate, &row.record))
        {
            visible.push(row);
        }
    }
    let mut rows = visible;

    if let Some(aggregate) = &query.aggregate {
        let mut records = aggregate_records(rows, aggregate)?;
        apply_record_sort(&mut records, query);
        apply_pagination(&mut records, query);
        return Ok(records.into_iter().map(|row| row.record).collect());
    }
    sort_rows(&mut rows, query);
    apply_pagination(&mut rows, query);
    let mut records: Vec<_> = rows.into_iter().map(|row| row.record).collect();
    if !query.select.is_empty() {
        for record in &mut records {
            record.retain(|field, _| query.select.contains(field));
        }
    }
    Ok(records)
}

#[derive(Debug, Clone)]
struct Row {
    record: Record,
    identifier: String,
    tie: Tie,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Tie {
    Event { shard: usize, id: i64 },
    Single(i64),
}

fn read_events(
    paths: &[PathBuf],
    pattern: Option<&str>,
    since: i64,
    until: i64,
    deadline: Instant,
) -> Result<Vec<Row>, QueryError> {
    let mut output = Vec::new();
    for (shard, path) in paths.iter().enumerate() {
        check_deadline(deadline)?;
        let connection = open_read_only(path)?;
        let mut statement = connection.prepare(
            "SELECT id, boot_id, timestamp, cpu_id, sequence, origin_class, event_type, \
             effective_token_guid, true_token_guid, process_guid, payload \
             FROM events WHERE timestamp >= ?1 AND timestamp < ?2",
        )?;
        let mut rows = statement.query(params![since, until])?;
        while let Some(row) = rows.next()? {
            if output.len().is_multiple_of(1_024) {
                check_deadline(deadline)?;
            }
            let identifier: String = row.get(6)?;
            if pattern.is_some_and(|pattern| !glob_matches(pattern, &identifier)) {
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
            record.insert("timestamp".into(), Value::Signed(timestamp));
            record.insert("cpu_id".into(), option_unsigned(cpu_id));
            record.insert("sequence".into(), option_unsigned(sequence));
            record.insert("origin_class".into(), option_unsigned(origin_class));
            record.insert("event_type".into(), Value::String(identifier.clone()));
            record.insert(
                "effective_token_guid".into(),
                option_guid(effective.as_deref()),
            );
            record.insert("true_token_guid".into(), option_guid(true_token.as_deref()));
            record.insert("process_guid".into(), option_guid(process.as_deref()));
            record.insert("boot_id".into(), option_guid(Some(&boot)));
            if let Some(payload) = payload {
                flatten_event_payload(&payload, &mut record);
            }
            output.push(Row {
                record,
                identifier,
                tie: Tie::Event { shard, id },
            });
        }
    }
    Ok(output)
}

#[allow(
    clippy::too_many_arguments,
    reason = "the fixed log selectors are independent SQL narrowing inputs"
)]
fn read_logs(
    path: &Path,
    origins: &[String],
    error_only: bool,
    containing: Option<&str>,
    since: i64,
    until: i64,
    deadline: Instant,
) -> Result<Vec<Row>, QueryError> {
    let connection = open_read_only(path)?;
    let mut statement = connection.prepare(
        "SELECT id, boot_id, timestamp, origin, is_error, message, job_id \
         FROM logs WHERE timestamp >= ?1 AND timestamp < ?2",
    )?;
    let mut rows = statement.query(params![since, until])?;
    let mut output = Vec::new();
    while let Some(row) = rows.next()? {
        if output.len().is_multiple_of(1_024) {
            check_deadline(deadline)?;
        }
        let identifier: String = row.get(3)?;
        if !origins.is_empty()
            && !origins
                .iter()
                .any(|origin| ascii_equal(origin, &identifier))
        {
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
        output.push(Row {
            record,
            identifier,
            tie: Tie::Single(id),
        });
    }
    Ok(output)
}

fn authorize_row(
    authorizer: &Authorizer,
    namespace: Namespace,
    row: &mut Row,
    referenced: &[String],
    cache: &mut HashMap<(String, Vec<String>), Option<std::collections::HashSet<String>>>,
) -> Result<bool, QueryError> {
    let mut fields: Vec<_> = row.record.keys().cloned().collect();
    fields.extend(referenced.iter().cloned());
    fields.sort_unstable();
    let key = (row.identifier.clone(), fields.clone());
    let allowed = if let Some(allowed) = cache.get(&key) {
        allowed.clone()
    } else {
        let allowed = authorizer.check(namespace, &row.identifier, &fields)?;
        cache.insert(key, allowed.clone());
        allowed
    };
    let Some(allowed) = allowed else {
        return Ok(false);
    };
    if referenced.iter().any(|field| !allowed.contains(field)) {
        return Ok(false);
    }
    row.record.retain(|field, _| allowed.contains(field));
    Ok(true)
}

fn referenced_fields(query: &Query) -> Vec<String> {
    let mut fields = Vec::new();
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
    fields.sort_unstable();
    fields.dedup();
    fields
}

fn evaluate(expression: &Expr, record: &Record) -> bool {
    match expression {
        Expr::And(left, right) => evaluate(left, record) && evaluate(right, record),
        Expr::Or(left, right) => evaluate(left, record) || evaluate(right, record),
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
                .any(|value| actual.language_equal(&literal_value(field, value)));
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
            let expected = literal_value(field, value);
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
            }
        }
    }
}

fn literal_value(field: &str, literal: &Literal) -> Value {
    match literal {
        Literal::Signed(value) => Value::Signed(*value),
        Literal::Unsigned(value) => Value::Unsigned(*value),
        Literal::Float(value) => Value::Float(*value),
        Literal::String(value) if field == "origin_class" => {
            match value.to_ascii_lowercase().as_str() {
                "userspace" => Value::Unsigned(0),
                "kmes" => Value::Unsigned(1),
                "kacs" => Value::Unsigned(2),
                "lcs" => Value::Unsigned(3),
                _ => Value::String(value.clone()),
            }
        }
        Literal::String(value) => Value::String(value.clone()),
        Literal::Binary(value) => Value::Binary(value.clone()),
        Literal::Bool(value) => Value::Bool(*value),
    }
}

fn numeric_order(left: &Value, right: &Value, wanted: Ordering) -> bool {
    matches!(
        left,
        Value::Signed(_) | Value::Unsigned(_) | Value::Float(_)
    ) && matches!(
        right,
        Value::Signed(_) | Value::Unsigned(_) | Value::Float(_)
    ) && language_cmp(left, right) == wanted
}

fn string_operation(left: &Value, right: &Value, operation: impl Fn(&str, &str) -> bool) -> bool {
    matches!((left, right), (Value::String(left), Value::String(right)) if operation(left, right))
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
        if query.sort.is_empty() {
            let ordering = timestamp(right).cmp(&timestamp(left));
            if ordering != Ordering::Equal {
                return ordering;
            }
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

fn timestamp(row: &Row) -> i64 {
    match row.record.get("timestamp") {
        Some(Value::Signed(value)) => *value,
        _ => 0,
    }
}

fn aggregate_records(rows: Vec<Row>, aggregate: &RecordAggregate) -> Result<Vec<Row>, QueryError> {
    match aggregate {
        RecordAggregate::CountBy(field) => Ok(count_by(&rows, field, None)),
        RecordAggregate::TopBy { count, field } => Ok(count_by(&rows, field, Some(*count))),
        RecordAggregate::Distinct(field) => Ok(distinct(&rows, field)),
        RecordAggregate::Group { fields, function } => group(rows, fields, function),
    }
}

fn count_by(rows: &[Row], field: &str, take: Option<u64>) -> Vec<Row> {
    let mut groups: Vec<(Value, u64)> = Vec::new();
    for row in rows {
        let value = row.record.get(field).cloned().unwrap_or(Value::Null);
        if let Some((representative, count)) = groups
            .iter_mut()
            .find(|(representative, _)| representative.language_equal(&value))
        {
            if language_cmp(&value, representative) == Ordering::Less {
                *representative = value;
            }
            *count += 1;
        } else {
            groups.push((value, 1));
        }
    }
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
        .enumerate()
        .map(|(index, (value, count))| Row {
            record: BTreeMap::from([
                (field.to_owned(), value),
                ("count".into(), Value::Unsigned(count)),
            ]),
            identifier: String::new(),
            tie: Tie::Single(i64::try_from(index).unwrap_or(i64::MAX)),
        })
        .collect()
}

fn distinct(rows: &[Row], field: &str) -> Vec<Row> {
    let mut values = Vec::<Value>::new();
    for row in rows {
        let value = row.record.get(field).cloned().unwrap_or(Value::Null);
        if let Some(representative) = values.iter_mut().find(|item| item.language_equal(&value)) {
            if language_cmp(&value, representative) == Ordering::Less {
                *representative = value;
            }
        } else {
            values.push(value);
        }
    }
    values.sort_by(language_cmp);
    values
        .into_iter()
        .enumerate()
        .map(|(index, value)| Row {
            record: BTreeMap::from([(field.to_owned(), value)]),
            identifier: String::new(),
            tie: Tie::Single(i64::try_from(index).unwrap_or(i64::MAX)),
        })
        .collect()
}

fn group(
    rows: Vec<Row>,
    fields: &[String],
    function: &GroupFunction,
) -> Result<Vec<Row>, QueryError> {
    let mut groups: Vec<(Vec<Value>, Vec<Row>)> = Vec::new();
    for row in rows {
        let keys: Vec<_> = fields
            .iter()
            .map(|field| row.record.get(field).cloned().unwrap_or(Value::Null))
            .collect();
        if let Some((representatives, members)) = groups.iter_mut().find(|(representatives, _)| {
            representatives
                .iter()
                .zip(&keys)
                .all(|(left, right)| left.language_equal(right))
        }) {
            for (representative, value) in representatives.iter_mut().zip(keys) {
                if language_cmp(&value, representative) == Ordering::Less {
                    *representative = value;
                }
            }
            members.push(row);
        } else {
            groups.push((keys, vec![row]));
        }
    }
    let mut output = Vec::with_capacity(groups.len());
    for (index, (keys, members)) in groups.into_iter().enumerate() {
        let mut record: Record = fields.iter().cloned().zip(keys).collect();
        match function {
            GroupFunction::Count => {
                record.insert("count".into(), Value::Unsigned(members.len() as u64));
            }
            GroupFunction::Sum(field) => {
                record.insert(
                    "sum".into(),
                    numeric_aggregate(&members, field, AggregateFunction::Sum)?,
                );
            }
            GroupFunction::Avg(field) => {
                record.insert(
                    "avg".into(),
                    numeric_aggregate(&members, field, AggregateFunction::Avg)?,
                );
            }
            GroupFunction::Min(field) => {
                record.insert(
                    "min".into(),
                    numeric_aggregate(&members, field, AggregateFunction::Min)?,
                );
            }
            GroupFunction::Max(field) => {
                record.insert(
                    "max".into(),
                    numeric_aggregate(&members, field, AggregateFunction::Max)?,
                );
            }
        }
        output.push(Row {
            record,
            identifier: String::new(),
            tie: Tie::Single(i64::try_from(index).unwrap_or(i64::MAX)),
        });
    }
    Ok(output)
}

#[allow(
    clippy::cast_precision_loss,
    reason = "PSPU defines AVG and overflowing integer SUM results as binary64"
)]
fn numeric_aggregate(
    rows: &[Row],
    field: &str,
    function: AggregateFunction,
) -> Result<Value, QueryError> {
    let values: Vec<_> = rows
        .iter()
        .filter_map(|row| row.record.get(field))
        .filter(|value| {
            matches!(
                value,
                Value::Signed(_) | Value::Unsigned(_) | Value::Float(_)
            )
        })
        .collect();
    if values.is_empty() {
        return Ok(Value::Null);
    }
    match function {
        AggregateFunction::Min => Ok((*values
            .into_iter()
            .min_by(|left, right| language_cmp(left, right))
            .expect("nonempty"))
        .clone()),
        AggregateFunction::Max => Ok((*values
            .into_iter()
            .max_by(|left, right| language_cmp(left, right))
            .expect("nonempty"))
        .clone()),
        AggregateFunction::Sum if values.iter().all(|value| !matches!(value, Value::Float(_))) => {
            let exact = values.iter().try_fold(0_i128, |sum, value| {
                let value = match value {
                    Value::Signed(value) => i128::from(*value),
                    Value::Unsigned(value) => i128::from(*value),
                    _ => unreachable!("integer-only aggregate"),
                };
                sum.checked_add(value)
            });
            if let Some(exact) = exact {
                if let Ok(value) = i64::try_from(exact) {
                    return Ok(Value::Signed(value));
                }
                if let Ok(value) = u64::try_from(exact) {
                    return Ok(Value::Unsigned(value));
                }
            }
            let result = values.iter().map(|value| as_f64(value)).sum::<f64>();
            if result.is_finite() {
                Ok(Value::Float(result))
            } else {
                Err(QueryError::NonFinite)
            }
        }
        AggregateFunction::Avg | AggregateFunction::Sum => {
            let count = values.len();
            let sum = values.iter().map(|value| as_f64(value)).sum::<f64>();
            let result = if function == AggregateFunction::Avg {
                sum / count as f64
            } else {
                sum
            };
            if result.is_finite() {
                Ok(Value::Float(result))
            } else {
                Err(QueryError::NonFinite)
            }
        }
    }
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
            .unwrap_or_else(|| left.tie.cmp(&right.tie))
    });
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
    deadline: Instant,
) -> Result<Vec<Record>, QueryError> {
    let connection = open_read_only(path)?;
    let mut series_statement = connection.prepare("SELECT id, name, labels, type FROM series")?;
    let mut series_rows = series_statement.query([])?;
    let mut series = Vec::new();
    while let Some(row) = series_rows.next()? {
        let id: i64 = row.get(0)?;
        let name: String = row.get(1)?;
        if !glob_matches(name_pattern, &name) {
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
        if labels.is_some_and(|items| !items.iter().all(|item| evaluate(item, &selector_record))) {
            continue;
        }
        series.push((id, name, metric_type, label_map));
    }
    if series.is_empty() {
        return Ok(Vec::new());
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
    let referenced = referenced_fields(query);
    let mut authorization_cache = HashMap::new();
    let mut resolved = Vec::with_capacity(series_count);
    for (series_id, name, metric_type, label_map) in series {
        check_deadline(deadline)?;
        let pair_transform = matches!(query.transform, Some(Transform::Rate | Transform::Delta));
        let mut inputs = Vec::new();
        if pair_transform && since > i64::MIN {
            let mut preceding = connection.prepare(
                "SELECT id, boot_id, timestamp, value, histogram_data FROM samples \
                 WHERE series_id = ?1 AND timestamp < ?2 \
                 ORDER BY timestamp DESC, id DESC LIMIT 1",
            )?;
            let mut rows = preceding.query(params![series_id, since])?;
            if let Some(sample) = rows.next()? {
                inputs.push(read_metric_input(sample, &name, metric_type, &label_map)?);
            }
        }
        let mut statement = connection.prepare(
            "SELECT id, boot_id, timestamp, value, histogram_data FROM samples \
             WHERE series_id = ?1 AND timestamp >= ?2 AND timestamp < ?3 \
             ORDER BY timestamp ASC, id ASC",
        )?;
        let mut samples = statement.query(params![series_id, since, until])?;
        while let Some(sample) = samples.next()? {
            inputs.push(read_metric_input(sample, &name, metric_type, &label_map)?);
        }
        let mut visible = Vec::with_capacity(inputs.len());
        for mut input in inputs {
            if authorize_row(
                authorizer,
                Namespace::Metrics,
                &mut input.row,
                &referenced,
                &mut authorization_cache,
            )? && query
                .predicates
                .iter()
                .all(|item| evaluate(item, &input.row.record))
            {
                visible.push(input);
            }
        }
        let points = transform_metric_inputs(visible, query.transform, since)?;
        resolved.push((series_id, name, points));
    }
    let mut output = finish_metric_query(
        resolved,
        query,
        name_pattern,
        first_type,
        bracketed,
        series_count,
    )?;
    sort_metric_rows(&mut output, query);
    apply_pagination(&mut output, query);
    Ok(output.into_iter().map(|row| row.record).collect())
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
                if let Some(value) = value {
                    input
                        .row
                        .record
                        .insert("value".into(), finite_value(value)?);
                    output.push(MetricPoint {
                        row: input.row,
                        adjusted_delta: None,
                        elapsed_ns: None,
                    });
                }
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

fn histogram_percentile(bytes: &[u8], percentile: u8) -> Result<Option<f64>, QueryError> {
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
        return Ok(None);
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
                Value::Float(value) => Ok(Some(*value)),
                _ => Err(QueryError::InvalidHistogram),
            };
        }
    }
    Ok(None)
}

type ResolvedMetricSeries = (i64, String, Vec<MetricPoint>);

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

fn take_latest_per_series(series: &mut [ResolvedMetricSeries]) -> Vec<MetricPoint> {
    series
        .iter_mut()
        .filter_map(|(_, _, points)| points.pop())
        .collect()
}

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
    let value = numeric_aggregate(&rows, "value", function)?;
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
    record.insert("timestamp".into(), Value::Signed(output_timestamp));
    record.insert("name".into(), Value::String(output_name.to_owned()));
    record.insert(
        "type".into(),
        Value::String(metric_type_name(metric_type)?.into()),
    );
    record.insert("value".into(), value);
    Ok(Row {
        record,
        identifier: output_name.to_owned(),
        tie: Tie::Single(tie),
    })
}

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
            numeric_aggregate(&rows, "value", function)?
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
        let value = numeric_aggregate(&rows, "value", function)?;
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

fn sort_metric_rows(rows: &mut [Row], query: &Query) {
    if query.sort.is_empty() {
        rows.sort_by(|left, right| {
            timestamp(left)
                .cmp(&timestamp(right))
                .then_with(|| left.identifier.cmp(&right.identifier))
                .then_with(|| left.tie.cmp(&right.tie))
        });
    } else {
        apply_record_sort(rows, query);
    }
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

fn time_range(query: &Query, evaluation: i64) -> Result<(i64, i64), QueryError> {
    let since = query
        .since
        .as_ref()
        .map_or(Ok(0), |time| resolve_time(time, evaluation))?;
    let until = query
        .until
        .as_ref()
        .map_or(Ok(evaluation), |time| resolve_time(time, evaluation))?;
    Ok((since, until))
}

fn resolve_time(time: &TimeExpr, evaluation: i64) -> Result<i64, QueryError> {
    match time {
        TimeExpr::Relative {
            nanoseconds,
            future,
        } => {
            let delta = i64::try_from(*nanoseconds).map_err(|_| QueryError::InvalidTime)?;
            if *future {
                evaluation.checked_add(delta)
            } else {
                evaluation.checked_sub(delta)
            }
            .ok_or(QueryError::InvalidTime)
        }
        TimeExpr::Today => Ok(evaluation.div_euclid(86_400_000_000_000) * 86_400_000_000_000),
        TimeExpr::Yesterday => {
            Ok(evaluation.div_euclid(86_400_000_000_000) * 86_400_000_000_000 - 86_400_000_000_000)
        }
        TimeExpr::Absolute(value) => parse_absolute_time(value),
    }
}

fn parse_absolute_time(value: &str) -> Result<i64, QueryError> {
    let year: i64 = value[0..4].parse().map_err(|_| QueryError::InvalidTime)?;
    let month: i64 = value[5..7].parse().map_err(|_| QueryError::InvalidTime)?;
    let day: i64 = value[8..10].parse().map_err(|_| QueryError::InvalidTime)?;
    if !(1..=12).contains(&month) || !(1..=days_in_month(year, month)).contains(&day) {
        return Err(QueryError::InvalidTime);
    }
    let (hour, minute, second) = if value.len() == 19 {
        (
            value[11..13].parse().map_err(|_| QueryError::InvalidTime)?,
            value[14..16].parse().map_err(|_| QueryError::InvalidTime)?,
            value[17..19].parse().map_err(|_| QueryError::InvalidTime)?,
        )
    } else {
        (0_i64, 0_i64, 0_i64)
    };
    if hour > 23 || minute > 59 || second > 59 {
        return Err(QueryError::InvalidTime);
    }
    let days = days_from_civil(year, month, day);
    days.checked_mul(86_400)
        .and_then(|value| value.checked_add(hour * 3_600 + minute * 60 + second))
        .and_then(|value| value.checked_mul(1_000_000_000))
        .ok_or(QueryError::InvalidTime)
}

const fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

fn days_from_civil(mut year: i64, month: i64, day: i64) -> i64 {
    year -= i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    era * 146_097 + day_of_era - 719_468
}

fn open_read_only(path: &Path) -> Result<Connection, QueryError> {
    Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(QueryError::Sql)
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

fn check_deadline(deadline: Instant) -> Result<(), QueryError> {
    if Instant::now() >= deadline {
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
        }
    }
}

impl std::error::Error for QueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sql(error) => Some(error),
            Self::Peios(error) => Some(error),
            Self::Security(error) => Some(error),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for QueryError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Sql(error)
    }
}

impl From<super::security::SecurityError> for QueryError {
    fn from(error: super::security::SecurityError) -> Self {
        Self::Security(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_is_ascii_folded_and_star_only() {
        assert!(glob_matches("KACS.*.denied", "kacs.token.denied"));
        assert!(!glob_matches("kacs.?", "kacs.x"));
    }

    #[test]
    fn civil_date_conversion_has_unix_epoch() {
        assert_eq!(parse_absolute_time("1970-01-01").unwrap(), 0);
        assert!(parse_absolute_time("2025-02-29").is_err());
    }

    #[test]
    fn integer_sum_stays_exact_until_it_leaves_the_integer_domain() {
        let rows = vec![
            test_row(0, Value::Unsigned(u64::MAX)),
            test_row(1, Value::Signed(-1)),
        ];
        assert!(matches!(
            numeric_aggregate(&rows, "value", AggregateFunction::Sum).unwrap(),
            Value::Unsigned(value) if value == u64::MAX - 1
        ));
        let rows = vec![
            test_row(0, Value::Unsigned(u64::MAX)),
            test_row(1, Value::Unsigned(1)),
        ];
        assert!(matches!(
            numeric_aggregate(&rows, "value", AggregateFunction::Sum).unwrap(),
            Value::Float(value) if value.is_finite()
        ));
    }

    #[test]
    fn counter_transform_uses_preceding_sample_and_adjusts_resets() {
        let inputs = vec![
            test_metric_input(5, 90.0),
            test_metric_input(10, 100.0),
            test_metric_input(20, 4.0),
        ];
        let points = transform_metric_inputs(inputs, Some(Transform::Delta), 10).unwrap();
        assert_eq!(points.len(), 2);
        assert!(matches!(
            points[0].row.record.get("value"),
            Some(Value::Float(10.0))
        ));
        assert!(matches!(
            points[1].row.record.get("value"),
            Some(Value::Float(4.0))
        ));
    }

    #[test]
    fn rate_window_divides_total_delta_by_covered_time() {
        let inputs = vec![
            test_metric_input(0, 0.0),
            test_metric_input(1_000_000_000, 10.0),
            test_metric_input(3_000_000_000, 30.0),
        ];
        let points = transform_metric_inputs(inputs, Some(Transform::Rate), 0).unwrap();
        let rows = window_points(
            points,
            AggregateFunction::Avg,
            5_000_000_000,
            Some(Transform::Rate),
            false,
            Some("requests"),
            0,
            0,
        )
        .unwrap();
        assert!(matches!(
            rows[0].record.get("value"),
            Some(Value::Float(value)) if (*value - 10.0).abs() < f64::EPSILON
        ));
    }

    fn test_row(timestamp: i64, value: Value) -> Row {
        Row {
            record: BTreeMap::from([
                ("timestamp".into(), Value::Signed(timestamp)),
                ("value".into(), value),
            ]),
            identifier: "test".into(),
            tie: Tie::Single(timestamp),
        }
    }

    fn test_metric_input(timestamp: i64, number: f64) -> MetricInput {
        MetricInput {
            row: test_row(timestamp, Value::Float(number)),
            number,
            histogram: None,
        }
    }
}
