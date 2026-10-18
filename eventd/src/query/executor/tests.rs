//! Executor regression and reference-model tests.

use super::*;
use peios::msgpack::Writer;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

static TEST_DATABASE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct TestDatabase(PathBuf);

impl TestDatabase {
    fn create(schema: &str) -> Self {
        let sequence = TEST_DATABASE_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("eventd-query-{}-{sequence}.db", std::process::id()));
        let connection = Connection::open(&path).unwrap();
        connection.execute_batch(schema).unwrap();
        drop(connection);
        Self(path)
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
        let _ = std::fs::remove_file(self.0.with_extension("db-shm"));
        let _ = std::fs::remove_file(self.0.with_extension("db-wal"));
    }
}

#[test]
fn glob_is_ascii_folded_and_star_only() {
    assert!(glob_matches("KACS.*.denied", "kacs.token.denied"));
    assert!(!glob_matches("kacs.?", "kacs.x"));
}

#[test]
fn watch_batches_follow_store_ids_not_timestamps_or_initial_pagination() {
    let query = crate::query_language::parse("EVENTS SORT n DESC SKIP 1 TAKE 1 STREAM").unwrap();
    let ties = [
        Tie::Event { shard: 1, id: 1 },
        Tie::Event { shard: 0, id: 3 },
        Tie::Event { shard: 0, id: 1 },
        Tie::Event { shard: 0, id: 2 },
    ];
    let mut rows: Vec<_> = ties
        .into_iter()
        .enumerate()
        .map(|(i, tie)| Row {
            record: BTreeMap::from([("n".into(), Value::Signed(i64::try_from(i).unwrap()))]),
            identifier: "t".into(),
            at: i64::try_from(i).unwrap(),
            metric_labels: Box::default(),
            tie,
        })
        .collect();
    let mut initial = rows.clone();
    order_result_rows(&mut initial, &query, false);
    assert_eq!(initial.len(), 1);
    assert_eq!(initial[0].tie, ties[2]);

    order_result_rows(&mut rows, &query, true);
    assert_eq!(
        rows.iter().map(|r| r.tie).collect::<Vec<_>>(),
        vec![ties[2], ties[3], ties[1], ties[0]]
    );

    let query = crate::query_language::parse("LOGS STREAM").unwrap();
    for (row, id) in rows.iter_mut().zip([4, 1, 3, 2]) {
        row.tie = Tie::Single(id);
    }
    order_result_rows(&mut rows, &query, true);
    assert_eq!(
        rows.iter().map(|r| r.tie).collect::<Vec<_>>(),
        (1..=4).map(Tie::Single).collect::<Vec<_>>()
    );
}

#[test]
fn planning_discovers_identifiers_from_compact_catalogues() {
    let events = TestDatabase::create(
        "CREATE TABLE event_types(event_type TEXT PRIMARY KEY) WITHOUT ROWID;\
         INSERT INTO event_types VALUES ('kacs.denied'), ('service.started');",
    );
    let discovered =
        discover_event_identifiers(std::slice::from_ref(&events.0), Some("KACS.*"), None).unwrap();
    assert_eq!(discovered, HashSet::from(["kacs.denied".to_owned()]));

    let logs = TestDatabase::create(
        "CREATE TABLE log_origins(origin TEXT PRIMARY KEY) WITHOUT ROWID;\
         INSERT INTO log_origins VALUES ('loregd'), ('peinit');",
    );
    let discovered = discover_log_identifiers(&logs.0, &["PEINIT".to_owned()], None).unwrap();
    assert_eq!(discovered, HashSet::from(["peinit".to_owned()]));

    let metrics = Connection::open_in_memory().unwrap();
    metrics
        .execute_batch(
            "CREATE TABLE series(name TEXT NOT NULL);\
             INSERT INTO series VALUES ('cpu.time'), ('disk.bytes'), ('cpu.load');",
        )
        .unwrap();
    let discovered = discover_metric_identifiers(&metrics, "CPU.*", None).unwrap();
    assert_eq!(
        discovered,
        HashSet::from(["cpu.time".to_owned(), "cpu.load".to_owned()])
    );
}

#[test]
fn denied_event_types_are_skipped_before_payload_decoding() {
    let events = TestDatabase::create(
        "CREATE TABLE events(\
             id INTEGER PRIMARY KEY, boot_id BLOB NOT NULL, timestamp INTEGER NOT NULL,\
             cpu_id INTEGER, sequence INTEGER, origin_class INTEGER, event_type TEXT NOT NULL,\
             effective_token_guid BLOB, true_token_guid BLOB, process_guid BLOB, payload BLOB\
         );\
         INSERT INTO events VALUES\
             (1, zeroblob(16), 1, NULL, NULL, NULL, 'denied.type', NULL, NULL, NULL, X'C1'),\
             (2, zeroblob(16), 2, NULL, NULL, NULL, 'allowed.type', NULL, NULL, NULL, NULL);",
    );
    let mut rows = Vec::new();
    scan_events(
        std::slice::from_ref(&events.0),
        &HashSet::from(["allowed.type".to_owned()]),
        &[],
        i64::MIN,
        i64::MAX,
        None,
        &[0],
        &[i64::MAX],
        ScanOrder::Stored,
        &mut |row| {
            rows.push(row);
            Ok(Flow::More)
        },
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].identifier, "allowed.type");
}

/// An event shard holding `(id, timestamp)` events of type `t`.
fn event_shard(events: &[(i64, i64)]) -> TestDatabase {
    let values: Vec<_> = events
        .iter()
        .map(|(id, timestamp)| {
            format!(
                "({id}, zeroblob(16), {timestamp}, NULL, NULL, NULL, 't', NULL, NULL, NULL, NULL)"
            )
        })
        .collect();
    TestDatabase::create(&format!(
        "CREATE TABLE events(\
             id INTEGER PRIMARY KEY, boot_id BLOB NOT NULL, timestamp INTEGER NOT NULL,\
             cpu_id INTEGER, sequence INTEGER, origin_class INTEGER, event_type TEXT NOT NULL,\
             effective_token_guid BLOB, true_token_guid BLOB, process_guid BLOB, payload BLOB\
         );\
         CREATE INDEX idx_events_timestamp ON events(timestamp);\
         INSERT INTO events VALUES {};",
        values.join(", ")
    ))
}

/// The events of `shards` as `(timestamp, shard, id)`, as a scan in
/// `order` hands them over, stopping after `take`.
fn scanned(shards: &[TestDatabase], order: ScanOrder, take: usize) -> Vec<(i64, usize, i64)> {
    let paths: Vec<_> = shards.iter().map(|shard| shard.0.clone()).collect();
    let mut seen = Vec::new();
    scan_events(
        &paths,
        &HashSet::from(["t".to_owned()]),
        &[],
        i64::MIN,
        i64::MAX,
        None,
        &vec![0; paths.len()],
        &vec![i64::MAX; paths.len()],
        order,
        &mut |row| {
            let Tie::Event { shard, id } = row.tie else {
                panic!("an event row has an event tie")
            };
            seen.push((timestamp(&row), shard, id));
            Ok(if seen.len() == take {
                Flow::Enough
            } else {
                Flow::More
            })
        },
    )
    .unwrap();
    seen
}

#[test]
fn the_newest_first_merge_is_the_default_result_order() {
    // Equal timestamps across and within shards, and ids out of time
    // order, so every tiebreaker of TRM §6.2 decides something.
    let shards = [
        event_shard(&[(1, 30), (2, 10), (3, 20), (4, 20), (5, 40)]),
        event_shard(&[(1, 20), (2, 40), (3, 5), (4, 20)]),
        event_shard(&[(1, 20), (2, 50)]),
    ];
    let merged = scanned(&shards, ScanOrder::Newest, usize::MAX);
    // What sorting the whole set in the default order gives.
    let paths: Vec<_> = shards.iter().map(|shard| shard.0.clone()).collect();
    let mut rows = Vec::new();
    scan_events(
        &paths,
        &HashSet::from(["t".to_owned()]),
        &[],
        i64::MIN,
        i64::MAX,
        None,
        &[0; 3],
        &[i64::MAX; 3],
        ScanOrder::Stored,
        &mut |row| {
            rows.push(row);
            Ok(Flow::More)
        },
    )
    .unwrap();
    sort_rows(&mut rows, &crate::query_language::parse("EVENTS").unwrap());
    let sorted: Vec<_> = rows
        .iter()
        .map(|row| match row.tie {
            Tie::Event { shard, id } => (timestamp(row), shard, id),
            Tie::Single(_) => unreachable!(),
        })
        .collect();
    assert_eq!(merged, sorted);
    assert_eq!(merged.len(), 11);
    assert_eq!(&merged[..3], &[(50, 2, 2), (40, 0, 5), (40, 1, 2)]);
    assert_eq!(
        &merged[4..8],
        &[(20, 0, 4), (20, 0, 3), (20, 1, 4), (20, 1, 1)]
    );
}

// PEI-1295, TRM §6.2: records an explicit SORT leaves tied are ordered
// by the mode's tiebreakers in full, timestamp descending first.
#[test]
fn records_tied_on_the_sort_keys_come_newest_first_before_shard_and_id() {
    let event = |timestamp: i64, shard: usize, id: i64| Row {
        record: BTreeMap::from([
            ("timestamp".into(), Value::Signed(timestamp)),
            ("k".into(), Value::Signed(1)),
        ]),
        identifier: "t".into(),
        at: timestamp,
        metric_labels: Box::default(),
        tie: Tie::Event { shard, id },
    };
    // The older event in the lower shard, the newer in the higher, and
    // a newer one still with the lower row id of the higher shard.
    let mut rows = vec![
        event(10, 0, 9),
        event(20, 1, 1),
        event(20, 1, 2),
        event(30, 1, 0),
    ];
    sort_rows(
        &mut rows,
        &crate::query_language::parse("EVENTS SORT k").unwrap(),
    );
    let order: Vec<_> = rows.iter().map(|row| (timestamp(row), row.tie)).collect();
    assert_eq!(
        order,
        [
            (30, Tie::Event { shard: 1, id: 0 }),
            (20, Tie::Event { shard: 1, id: 2 }),
            (20, Tie::Event { shard: 1, id: 1 }),
            (10, Tie::Event { shard: 0, id: 9 }),
        ]
    );

    let line = |timestamp: i64, id: i64| Row {
        record: BTreeMap::from([
            ("timestamp".into(), Value::Signed(timestamp)),
            ("is_error".into(), Value::Bool(false)),
        ]),
        identifier: "o".into(),
        at: timestamp,
        metric_labels: Box::default(),
        tie: Tie::Single(id),
    };
    // The newer line inserted first: row id and time disagree.
    let mut rows = vec![line(20, 1), line(10, 2)];
    sort_rows(
        &mut rows,
        &crate::query_language::parse("LOGS SORT is_error").unwrap(),
    );
    assert_eq!(
        rows.iter().map(|row| row.tie).collect::<Vec<_>>(),
        [Tie::Single(1), Tie::Single(2)]
    );
}

#[test]
fn a_newest_first_scan_stops_when_told_enough() {
    let shards = [
        event_shard(&[(1, 1), (2, 2), (3, 3)]),
        event_shard(&[(1, 4), (2, 5)]),
    ];
    assert_eq!(
        scanned(&shards, ScanOrder::Newest, 2),
        [(5, 1, 2), (4, 1, 1)]
    );
    assert_eq!(scanned(&shards, ScanOrder::Stored, 1).len(), 1);
}

#[test]
fn logs_are_scanned_newest_first_with_id_breaking_ties() {
    let logs = TestDatabase::create(
        "CREATE TABLE logs(\
             id INTEGER PRIMARY KEY, boot_id BLOB NOT NULL, timestamp INTEGER NOT NULL,\
             origin TEXT NOT NULL, is_error INTEGER NOT NULL, message TEXT NOT NULL, job_id BLOB\
         );\
         INSERT INTO logs VALUES\
             (1, zeroblob(16), 10, 'a', 0, 'one', NULL),\
             (2, zeroblob(16), 30, 'a', 1, 'two', NULL),\
             (3, zeroblob(16), 10, 'a', 0, 'three', NULL),\
             (4, zeroblob(16), 20, 'b', 0, 'four', NULL);",
    );
    let mut seen = Vec::new();
    scan_logs(
        &logs.0,
        &HashSet::from(["a".to_owned()]),
        false,
        None,
        i64::MIN,
        i64::MAX,
        None,
        0,
        i64::MAX,
        ScanOrder::Newest,
        &mut |row| {
            seen.push(row.tie);
            Ok(Flow::More)
        },
    )
    .unwrap();
    assert_eq!(seen, [Tie::Single(2), Tie::Single(3), Tie::Single(1)]);
}

#[test]
fn source_filters_and_metric_computation_are_authorized_fields() {
    let logs =
        crate::query_language::parse("LOGS FROM loregd ERROR ONLY CONTAINING \"failure\"").unwrap();
    assert_eq!(
        referenced_fields(&logs),
        ["is_error".to_owned(), "message".to_owned()]
    );

    let metric =
        crate::query_language::parse("METRIC cpu.usage[core=\"0\"] RATE WHERE boot_id IS NOT NULL")
            .unwrap();
    assert_eq!(
        referenced_fields(&metric),
        ["boot_id".to_owned(), "core".to_owned(), "value".to_owned()]
    );
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

#[test]
fn rollup_validation_detects_new_inputs_and_changed_baselines() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE samples(id INTEGER PRIMARY KEY, series_id INTEGER, timestamp INTEGER);\
             CREATE TABLE rollups(\
                series_id INTEGER, window_start INTEGER, window_width INTEGER,\
                transform INTEGER, function INTEGER, value REAL, overflow INTEGER,\
                source_max_sample_id INTEGER, source_baseline_sample_id INTEGER);\
             INSERT INTO samples VALUES (1, 1, 5), (2, 1, 12);\
             INSERT INTO rollups VALUES (1, 10, 10, 0, 0, 4.0, 0, 2, NULL);\
             INSERT INTO rollups VALUES (1, 10, 10, 1, 0, 2.0, 0, 2, 1);",
        )
        .unwrap();
    assert_eq!(
        read_valid_rollups(&connection, 1, 10, 20, 10, 0, 0)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        read_valid_rollups(&connection, 1, 10, 20, 10, 1, 0)
            .unwrap()
            .len(),
        1
    );

    connection
        .execute("INSERT INTO samples VALUES (3, 1, 7)", [])
        .unwrap();
    assert!(
        read_valid_rollups(&connection, 1, 10, 20, 10, 1, 0)
            .unwrap()
            .is_empty()
    );
    connection
        .execute("INSERT INTO samples VALUES (4, 1, 15)", [])
        .unwrap();
    assert!(
        read_valid_rollups(&connection, 1, 10, 20, 10, 0, 0)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn rollup_source_proofs_keep_the_largest_id_and_the_pre_window_baseline() {
    let budget = budget(1 << 20);
    let mut held = Held::new(&budget);
    let mut sources = RollupSources::new(10);
    for (at, id) in [(5, 8), (10, 10), (10, 11), (18, 9), (25, 12)] {
        sources.push(at, id, &mut held).unwrap();
    }
    assert_eq!(sources.count, 5);
    assert_eq!(sources.proof(10), (11, Some(8)));
    assert_eq!(sources.proof(20), (12, Some(9)));
    assert_eq!(sources.proof(30), (0, Some(12)));
    assert_eq!(sources.proof(0), (8, None));
}

/// Samples of `series` series named `name`, with equal timestamps,
/// counter resets, uneven spacing, values that do not add exactly in
/// binary64, and some before the range.
fn awkward_inputs(name: &str, series: i64, histogram: bool) -> Vec<Vec<MetricInput>> {
    const SECOND: i64 = 1_000_000_000;
    (0..series)
        .map(|core| {
            let mut value = 0.1 * f64::from(u8::try_from(core).unwrap());
            let mut at = 0;
            (0..90_i64)
                .map(|index| {
                    at += match (index + core) % 5 {
                        0 => 0,
                        1 => SECOND / 3,
                        2 => SECOND,
                        3 => 2 * SECOND + 7,
                        _ => 3 * SECOND,
                    };
                    value = if index % 23 == 22 { 0.3 } else { value + 0.1 };
                    let mut record = Record::from([
                        ("core".into(), Value::String(core.to_string())),
                        ("timestamp".into(), Value::Signed(at)),
                        ("boot_id".into(), Value::Null),
                        ("name".into(), Value::String(name.into())),
                    ]);
                    let (number, histogram) = if histogram {
                        record.insert("type".into(), Value::String("histogram".into()));
                        record.insert("value".into(), Value::Null);
                        let total = u64::try_from(index % 7).unwrap() * 10;
                        let located = if index % 11 == 0 { total / 2 } else { total };
                        (0.0, Some(test_histogram(total, located)))
                    } else {
                        record.insert("type".into(), Value::String("counter".into()));
                        record.insert("value".into(), Value::Float(value));
                        (value, None)
                    };
                    MetricInput {
                        row: Row {
                            metric_labels: canonical_labels(&record).collect(),
                            record,
                            identifier: name.into(),
                            at,
                            tie: Tie::Single(core * 1_000 + index),
                        },
                        number,
                        histogram,
                    }
                })
                .collect()
        })
        .collect()
}

/// The fold gives exactly the rows the gathering pipeline gave, bit
/// for bit, for every result shape.
#[test]
fn the_metric_fold_matches_gathering_every_sample() {
    let since = 20 * 1_000_000_000;
    let shapes = [
        ("m", false, "METRIC m[]"),
        ("m", false, "METRIC m"),
        ("m", false, "METRIC m MAX"),
        ("m", false, "METRIC m RATE"),
        ("m", false, "METRIC m[] RATE"),
        ("m", false, "METRIC m RATE SUM"),
        ("m", false, "METRIC m[] SINCE 1h ago"),
        ("m", false, "METRIC m SINCE 1h ago"),
        ("m", false, "METRIC m[] SINCE 1h ago DELTA"),
        ("m", false, "METRIC m[] SINCE 1h ago AVG"),
        ("m", false, "METRIC m[] SINCE 1h ago RATE MIN"),
        ("m", false, "METRIC m SINCE 1h ago AVG_OVER 7s"),
        ("m", false, "METRIC m SINCE 1h ago MIN_OVER 7s"),
        ("m", false, "METRIC m[] SINCE 1h ago MAX_OVER 7s"),
        ("m", false, "METRIC m SINCE 1h ago RATE SUM_OVER 7s"),
        ("m", false, "METRIC m SINCE 1h ago RATE AVG_OVER 7s"),
        ("m", false, "METRIC m[] SINCE 1h ago DELTA SUM_OVER 7s"),
        ("m", false, "METRIC m[] SINCE 1h ago RATE AVG_OVER 7s"),
        ("h", true, "METRIC h P95"),
        ("h", true, "METRIC h[] P99"),
        ("h", true, "METRIC h[] P99 SINCE 1h ago"),
        ("h", true, "METRIC h[] P50 SINCE 1h ago MAX"),
        ("h", true, "METRIC h P99 SINCE 1h ago AVG_OVER 7s"),
        ("h", true, "METRIC h[] P99 SINCE 1h ago MAX_OVER 7s"),
    ];
    for (name, histogram, text) in shapes {
        let query = crate::query_language::parse(text).unwrap();
        let bracketed = matches!(
            &query.source,
            Source::Metric {
                labels: Some(_),
                ..
            }
        );
        let metric_type = if histogram { 2 } else { 0 };
        for count in [1, 3] {
            let mut resolved = Vec::new();
            for (index, inputs) in awkward_inputs(name, count, histogram)
                .into_iter()
                .enumerate()
            {
                let points = transform_metric_inputs(inputs, query.transform, since).unwrap();
                resolved.push((i64::try_from(index).unwrap(), name.to_owned(), points));
            }
            let mut expected = finish_metric_query(
                resolved,
                &query,
                name,
                metric_type,
                bracketed,
                count_of(count),
            )
            .unwrap();

            let budget = budget(1 << 30);
            let mut held = Held::new(&budget);
            let mut sink =
                MetricSink::new(&query, name, metric_type, bracketed, count_of(count)).unwrap();
            for (index, inputs) in awkward_inputs(name, count, histogram)
                .into_iter()
                .enumerate()
            {
                let mut transformer = Transformer::new(query.transform, since);
                for input in inputs {
                    if let Some(point) = transformer.push(input).unwrap() {
                        sink.push(point, &mut held).unwrap();
                    }
                }
                sink.end_series(i64::try_from(index).unwrap(), &mut held)
                    .unwrap();
            }
            let mut folded = sink.finish(&mut held).unwrap();
            sort_metric_rows(&mut expected, &query);
            sort_metric_rows(&mut folded, &query);
            assert!(
                !expected.is_empty(),
                "{text} over {count}: nothing to compare"
            );
            assert_eq!(
                format!("{folded:?}"),
                format!("{expected:?}"),
                "{text} over {count} series"
            );
        }
    }
}

fn count_of(series: i64) -> usize {
    usize::try_from(series).unwrap()
}

#[test]
fn raw_metric_points_count_against_the_held_budget_and_windows_do_not_grow() {
    let since = 0;
    let points = |query: &Query, held: &mut Held<'_>| {
        let mut sink = MetricSink::new(query, "m", 0, true, 1)?;
        let mut transformer = Transformer::new(query.transform, since);
        for round in 0..200 {
            for input in awkward_inputs("m", 1, false).remove(0) {
                let mut input = input;
                let at = timestamp(&input.row) + round * 1_000_000_000_000;
                input
                    .row
                    .record
                    .insert("timestamp".into(), Value::Signed(at));
                input.row.at = at;
                if let Some(point) = transformer.push(input)? {
                    sink.push(point, held)?;
                }
            }
        }
        sink.end_series(0, held)?;
        sink.finish(held).map(|rows| rows.len())
    };
    let budget = budget(1 << 20);

    let raw = crate::query_language::parse("METRIC m[] SINCE 1h ago").unwrap();
    let mut held = Held::new(&budget);
    assert!(matches!(
        points(&raw, &mut held),
        Err(QueryError::HeldLimit)
    ));
    drop(held);
    assert_eq!(budget.used.load(AtomicOrdering::Acquire), 0);

    let windowed = crate::query_language::parse("METRIC m[] SINCE 1h ago AVG_OVER 1h").unwrap();
    let mut held = Held::new(&budget);
    assert_eq!(points(&windowed, &mut held).unwrap(), 56);
}

#[test]
fn rollup_validation_preserves_empty_and_overflow_windows_but_rejects_nonfinite_values() {
    let connection = Connection::open_in_memory().unwrap();
    connection
        .execute_batch(
            "CREATE TABLE samples(id INTEGER PRIMARY KEY, series_id INTEGER, timestamp INTEGER);\
             CREATE TABLE rollups(\
                series_id INTEGER, window_start INTEGER, window_width INTEGER,\
                transform INTEGER, function INTEGER, value REAL, overflow INTEGER,\
                source_max_sample_id INTEGER, source_baseline_sample_id INTEGER);\
             INSERT INTO rollups VALUES (1, 0, 10, 0, 0, NULL, 0, 0, NULL);\
             INSERT INTO rollups VALUES (1, 10, 10, 99, 0, NULL, 1, 0, NULL);\
             INSERT INTO rollups VALUES (1, 20, 10, 0, 0, 1e999, 0, 0, NULL);",
        )
        .unwrap();

    let ordinary = read_valid_rollups(&connection, 1, 0, 30, 10, 0, 0).unwrap();
    assert_eq!(ordinary.len(), 1);
    assert!(ordinary.contains_key(&0));
    let percentile = read_valid_rollups(&connection, 1, 0, 30, 10, 99, 0).unwrap();
    assert_eq!(percentile.len(), 1);
    assert!(percentile.get(&10).is_some_and(|row| row.overflow));
}

#[test]
fn histogram_percentile_distinguishes_empty_finite_and_overflow() {
    assert_eq!(
        histogram_percentile(&test_histogram(0, 0), 99).unwrap(),
        PercentileValue::Empty
    );
    assert_eq!(
        histogram_percentile(&test_histogram(100, 100), 99).unwrap(),
        PercentileValue::Finite(1.0)
    );
    assert_eq!(
        histogram_percentile(&test_histogram(100, 98), 99).unwrap(),
        PercentileValue::Overflow
    );
}

#[test]
fn percentile_overflow_emits_an_explicit_result() {
    let points = transform_metric_inputs(
        vec![
            test_histogram_input(10, 100, 100),
            test_histogram_input(20, 100, 98),
        ],
        Some(Transform::Percentile(99)),
        0,
    )
    .unwrap();
    assert_eq!(points.len(), 2);
    assert!(matches!(
        points[0].row.record.get("value"),
        Some(Value::Float(1.0))
    ));
    assert!(!points[0].row.record.contains_key("overflow"));
    assert!(matches!(
        points[1].row.record.get("value"),
        Some(Value::Null)
    ));
    assert!(matches!(
        points[1].row.record.get("overflow"),
        Some(Value::Bool(true))
    ));

    let empty = transform_metric_inputs(
        vec![test_histogram_input(10, 0, 0)],
        Some(Transform::Percentile(99)),
        0,
    )
    .unwrap();
    assert!(empty.is_empty());
}

#[test]
fn percentile_overflow_propagates_through_aggregations() {
    let points = transform_metric_inputs(
        vec![
            test_histogram_input(10, 100, 100),
            test_histogram_input(20, 100, 98),
        ],
        Some(Transform::Percentile(99)),
        0,
    )
    .unwrap();
    let scalar = aggregate_points(&points, AggregateFunction::Avg, 20, true, None, 2, 0)
        .unwrap()
        .unwrap();
    assert!(matches!(scalar.record.get("value"), Some(Value::Null)));
    assert!(matches!(
        scalar.record.get("overflow"),
        Some(Value::Bool(true))
    ));

    let windows = window_points(
        points,
        AggregateFunction::Avg,
        100,
        Some(Transform::Percentile(99)),
        true,
        None,
        2,
        0,
    )
    .unwrap();
    assert_eq!(windows.len(), 1);
    assert!(matches!(windows[0].record.get("value"), Some(Value::Null)));
    assert!(matches!(
        windows[0].record.get("overflow"),
        Some(Value::Bool(true))
    ));
}

#[test]
fn existence_ranges_are_half_open_merged_and_intersectable() {
    let ranges = existence_ranges(&[10, 15, 30], 0, 40, 5, 5);
    assert_eq!(
        ranges,
        [
            TimeRange { start: 5, end: 20 },
            TimeRange { start: 25, end: 35 }
        ]
    );
    assert!(range_contains(&ranges, 5));
    assert!(range_contains(&ranges, 19));
    assert!(!range_contains(&ranges, 20));
    assert_eq!(
        intersect_ranges(&ranges, &[TimeRange { start: 18, end: 28 }]),
        [
            TimeRange { start: 18, end: 20 },
            TimeRange { start: 25, end: 28 }
        ]
    );
}

#[test]
fn header_constraints_preserve_query_language_comparison_semantics() {
    let event_type = Expr::Compare {
        field: "event.type".into(),
        operator: Operator::Equal,
        value: Literal::String("KACS.Denied".into()),
    };
    let constraint = sql_header_constraint(&event_type).unwrap();
    assert_eq!(constraint.sql, "event_type = ?5 COLLATE NOCASE");
    assert_eq!(
        constraint.value,
        Some(rusqlite::types::Value::Text("KACS.Denied".into()))
    );
    let origin = Expr::Compare {
        field: "emitter.class".into(),
        operator: Operator::Equal,
        value: Literal::String("kacs".into()),
    };
    assert_eq!(
        sql_header_constraint(&origin).unwrap().value,
        Some(rusqlite::types::Value::Integer(2))
    );
    let cpu = Expr::Compare {
        field: "event.cpu".into(),
        operator: Operator::GreaterEqual,
        value: Literal::Unsigned(3),
    };
    assert_eq!(sql_header_constraint(&cpu).unwrap().sql, "cpu_id >= ?5");
    // The columns' names are not fields: a payload field spelled like
    // one is left to the payload.
    for column in ["event_type", "origin_class", "cpu_id"] {
        assert!(
            sql_header_constraint(&Expr::Compare {
                field: column.into(),
                operator: Operator::Equal,
                value: Literal::Unsigned(2),
            })
            .is_none(),
            "{column}"
        );
    }
}

#[test]
fn payload_constraint_requires_a_material_compatible_index() {
    let connection = Connection::open_in_memory().unwrap();
    eventd_core::payload_index::register(&connection).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE events(payload BLOB);\
             CREATE INDEX idx_events_payload_2602646b558759b4af6e77009f3ebc3d \
             ON events(eventd_payload_key(payload, 'source.name'));",
        )
        .unwrap();
    let predicate = Expr::Compare {
        field: "source.name".into(),
        operator: Operator::Equal,
        value: Literal::String("Alpha".into()),
    };
    let constraint = sql_payload_constraint(&predicate, &connection)
        .unwrap()
        .unwrap();
    assert_eq!(
        constraint.sql,
        "eventd_payload_key(payload, 'source.name') = ?5"
    );
    assert_eq!(
        constraint.value,
        Some(rusqlite::types::Value::Blob(
            eventd_core::payload_query_key(eventd_core::PayloadIndexValue::String("alpha"))
        ))
    );

    let absent = Expr::Compare {
        field: "source.other".into(),
        operator: Operator::Equal,
        value: Literal::String("Alpha".into()),
    };
    assert!(
        sql_payload_constraint(&absent, &connection)
            .unwrap()
            .is_none()
    );
}

#[test]
fn has_matches_an_element_of_an_array_field() {
    // The real shape: a KACS event whose payload carries the token's
    // group SIDs as an array of binary values. `==` against one SID
    // is an array-versus-binary mismatch and matches nothing, which
    // is why the operator exists.
    let administrators = [1_u8, 2, 0, 0, 0, 0, 0, 5, 32, 0, 0, 0, 32, 2, 0, 0];
    let users = [1_u8, 2, 0, 0, 0, 0, 0, 5, 32, 0, 0, 0, 33, 2, 0, 0];
    let absent = [1_u8, 2, 0, 0, 0, 0, 0, 5, 32, 0, 0, 0, 34, 2, 0, 0];
    let mut writer = Writer::new();
    writer
        .write_map(1)
        .write_str("subject")
        .write_map(1)
        .write_str("token")
        .write_map(1)
        .write_str("groups")
        .write_array(2)
        .write_bin(&administrators)
        .write_bin(&users);
    let mut record = Record::new();
    crate::query::value::flatten_event_payload(&writer.to_bytes().unwrap(), &mut record);
    assert!(matches!(
        record.get("subject.token.groups"),
        Some(Value::Array(elements)) if elements.len() == 2
    ));

    let has = |sid: &[u8]| {
        evaluate(
            Namespace::Events,
            &Expr::Compare {
                field: "subject.token.groups".into(),
                operator: Operator::Has,
                value: Literal::Binary(sid.to_vec()),
            },
            &record,
        )
    };
    assert!(has(&administrators), "HAS matches the first element");
    assert!(has(&users), "HAS matches a later element");
    assert!(!has(&absent), "HAS does not match a SID that is not there");

    // A field that is not an array, and a missing field, are false
    // rather than an error — as every other type mismatch is.
    for field in ["event.type", "subject.token.nothing"] {
        assert!(!evaluate(
            Namespace::Events,
            &Expr::Compare {
                field: field.into(),
                operator: Operator::Has,
                value: Literal::Binary(administrators.to_vec()),
            },
            &record,
        ));
    }
}

#[test]
fn equality_against_an_array_field_is_unchanged_by_has() {
    // `==` stays whole-value equality: an element does not match the
    // array, and the array matches only an equal array.
    let first = [1_u8, 2, 3];
    let second = [4_u8, 5, 6];
    let mut record = Record::new();
    record.insert(
        "groups".into(),
        Value::Array(vec![
            Value::Binary(first.to_vec()),
            Value::Binary(second.to_vec()),
        ]),
    );
    let equals = |value: Literal| {
        evaluate(
            Namespace::Events,
            &Expr::Compare {
                field: "groups".into(),
                operator: Operator::Equal,
                value,
            },
            &record,
        )
    };
    assert!(!equals(Literal::Binary(first.to_vec())));
    assert!(
        record
            .get("groups")
            .unwrap()
            .language_equal(&Value::Array(vec![
                Value::Binary(first.to_vec()),
                Value::Binary(second.to_vec()),
            ]))
    );
}

#[test]
fn metric_label_literals_are_not_event_header_aliases() {
    let guid = "550E8400-E29B-41D4-A716-446655440000";
    for (field, value) in [
        ("emitter.class", "kacs"),
        ("emitter.token.guid", guid),
        ("event.boot.guid", guid),
        ("job_id", guid),
    ] {
        let record = BTreeMap::from([(field.into(), Value::String(value.into()))]);
        for predicate in [
            format!("{field} == \"{value}\""),
            format!("{field} IN (\"absent\", \"{value}\")"),
            format!("{field} NOT_IN (\"absent\") AND {field} == \"{value}\""),
            format!("{field} == \"absent\" OR {field} == \"{value}\""),
        ] {
            let query =
                crate::query_language::parse(&format!("METRIC cpu.usage WHERE {predicate}"))
                    .unwrap();
            assert!(
                evaluate(Namespace::Metrics, &query.predicates[0], &record),
                "{predicate}"
            );
        }
        let query = crate::query_language::parse(&format!("METRIC cpu.usage[{field}=\"{value}\"]"))
            .unwrap();
        let Source::Metric {
            labels: Some(labels),
            ..
        } = query.source
        else {
            panic!("metric selector expected");
        };
        assert!(
            evaluate(Namespace::Metrics, &labels[0], &record),
            "selector for {field}"
        );
    }
}

#[test]
fn guid_payload_fields_do_not_borrow_another_namespaces_header_type() {
    let guid = "550E8400-E29B-41D4-A716-446655440000";
    for field in ["boot_id", "job_id"] {
        let record = BTreeMap::from([(field.into(), Value::String(guid.into()))]);
        let query =
            crate::query_language::parse(&format!("EVENTS WHERE {field} == \"{guid}\"")).unwrap();
        assert!(
            evaluate(Namespace::Events, &query.predicates[0], &record),
            "payload {field}"
        );
    }
}

#[test]
fn header_literals_keep_their_namespace_specific_conversions() {
    let unbraced = "550E8400-E29B-41D4-A716-446655440000";
    let braced = "{550e8400-e29b-41d4-a716-446655440000}";
    for (namespace, field) in [
        (Namespace::Events, "emitter.token.guid"),
        (Namespace::Events, "emitter.true-token.guid"),
        (Namespace::Events, "emitter.process.guid"),
        (Namespace::Events, "event.boot.guid"),
        (Namespace::Logs, "boot_id"),
        (Namespace::Logs, "job_id"),
        (Namespace::Metrics, "boot_id"),
    ] {
        let record = BTreeMap::from([(field.into(), Value::String(braced.into()))]);
        for expression in [
            Expr::Compare {
                field: field.into(),
                operator: Operator::Equal,
                value: Literal::String(unbraced.into()),
            },
            Expr::In {
                field: field.into(),
                negated: false,
                values: vec![Literal::String(unbraced.into())],
            },
        ] {
            assert!(
                evaluate(namespace, &expression, &record),
                "{namespace:?} {field}"
            );
        }
    }
    for (name, number) in [("userspace", 0), ("KMES", 1), ("kacs", 2), ("lcs", 3)] {
        let record = BTreeMap::from([("emitter.class".into(), Value::Unsigned(number))]);
        let expression = Expr::In {
            field: "emitter.class".into(),
            negated: false,
            values: vec![Literal::String(name.into())],
        };
        assert!(evaluate(Namespace::Events, &expression, &record));
    }
}

#[test]
fn guid_literals_accept_documented_brace_free_form() {
    assert_eq!(
        canonical_guid_literal("550E8400-E29B-41D4-A716-446655440000").as_deref(),
        Some("{550e8400-e29b-41d4-a716-446655440000}")
    );
    assert_eq!(
        canonical_guid_literal("{550e8400-e29b-41d4-a716-446655440000}").as_deref(),
        Some("{550e8400-e29b-41d4-a716-446655440000}")
    );
    assert!(canonical_guid_literal("not-a-guid").is_none());
}

#[test]
fn sqlite_progress_interrupt_is_reported_as_query_timeout() {
    let connection = Connection::open_in_memory().unwrap();
    connection.progress_handler(1, Some(|| true));
    let error = connection
        .query_row(
            "WITH RECURSIVE numbers(value) AS (\
             VALUES(1) UNION ALL SELECT value + 1 FROM numbers WHERE value < 1000000\
             ) SELECT SUM(value) FROM numbers",
            [],
            |row| row.get::<_, i64>(0),
        )
        .unwrap_err();
    assert!(matches!(QueryError::from(error), QueryError::Timeout));
}

fn test_row(timestamp: i64, value: Value) -> Row {
    Row {
        record: BTreeMap::from([
            ("timestamp".into(), Value::Signed(timestamp)),
            ("value".into(), value),
        ]),
        identifier: "test".into(),
        at: timestamp,
        metric_labels: Box::default(),
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

fn test_histogram_input(timestamp: i64, total_count: u64, final_count: u64) -> MetricInput {
    MetricInput {
        row: test_row(timestamp, Value::Null),
        number: 0.0,
        histogram: Some(test_histogram(total_count, final_count)),
    }
}

// PEI-1296, TRM §7.3: a sample whose timestamp field the caller may
// not read is still placed by its time, as range and window membership
// need, and its result still lacks the field.
#[test]
fn a_sample_with_its_timestamp_hidden_is_still_placed_by_its_time() {
    let since = 1_000;
    // What authorize_row leaves of a sample when timestamp is denied.
    let hidden = |at| {
        let mut input = test_metric_input(at, 2.0);
        input.row.record.remove("timestamp");
        input
    };
    let point = Transformer::new(None, since)
        .push(hidden(5_000))
        .unwrap()
        .expect("the sample is in range");
    assert_eq!(point.row.record.get("timestamp"), None, "and stays hidden");
    assert!(
        Transformer::new(None, since)
            .push(hidden(500))
            .unwrap()
            .is_none(),
        "one before SINCE is out of range still"
    );
    let budget = budget(usize::MAX);
    let mut held = Held::new(&budget);
    let mut windows = Windows::new(1_000);
    windows.push(&point, &mut held).unwrap();
    assert_eq!(
        windows.folds.keys().copied().collect::<Vec<_>>(),
        [5_000],
        "a window takes it by its time"
    );
}

// The rows reaching aggregation have already passed field authorization.
// Derived metadata must not reconstruct a source header removed there.
#[test]
fn metric_aggregates_keep_each_denied_source_header_absent() {
    for denied in ["timestamp", "name", "type"] {
        for retain_labels in [false, true] {
            let mut template = metric_template("test", 1, &Record::new()).unwrap();
            template.record.remove(denied);
            let row = metric_aggregate_row(
                &template,
                Value::Float(12.0),
                5_000,
                retain_labels,
                "test",
                1,
                7,
            )
            .unwrap();
            assert!(!row.record.contains_key(denied), "{denied} was denied");
            for visible in ["timestamp", "name", "type"] {
                if visible != denied {
                    assert!(row.record.contains_key(visible), "{visible} is authorized");
                }
            }
            assert_eq!(row.record.get("value"), Some(&Value::Float(12.0)));
            assert_eq!(timestamp(&row), 5_000, "internal ordering time is retained");
        }
    }
}

#[test]
fn metric_windows_only_expose_headers_visible_in_every_contributing_source() {
    for hidden_first in [false, true] {
        let budget = budget(usize::MAX);
        let mut held = Held::new(&budget);
        let mut windows = Windows::new(1_000);
        for (index, hidden) in [hidden_first, !hidden_first].into_iter().enumerate() {
            let mut row =
                metric_template(if hidden { "hidden" } else { "visible" }, 1, &Record::new())
                    .unwrap();
            row.at = 5_100 + i64::try_from(index).unwrap();
            row.record.insert("timestamp".into(), Value::Signed(row.at));
            row.record.insert("value".into(), Value::Signed(6));
            if hidden {
                for field in ["timestamp", "name", "type"] {
                    row.record.remove(field);
                }
            }
            windows
                .push(
                    &MetricPoint {
                        row,
                        adjusted_delta: None,
                        elapsed_ns: None,
                    },
                    &mut held,
                )
                .unwrap();
        }
        let rows = windows
            .rows(
                AggregateFunction::Sum,
                None,
                false,
                Some("*"),
                1,
                None,
                &mut held,
            )
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].record.get("value"), Some(&Value::Signed(12)));
        assert_eq!(
            timestamp(&rows[0]),
            5_000,
            "window placement uses the private timestamp"
        );
        for field in ["timestamp", "name", "type"] {
            assert!(
                !rows[0].record.contains_key(field),
                "{field} cannot be restored from the first contributor"
            );
        }
    }
}

// PEI-1295, TRM §6.2: metric rows tie-break on timestamp, name,
// canonical labels and sample id, all ascending, after any SORT keys.
#[test]
fn metric_ties_are_broken_by_canonical_labels_before_sample_id() {
    let sample = |name: &str, labels: &[(&str, &str)], at: i64, id: i64| {
        let mut row = test_row(at, Value::Float(1.0));
        row.identifier = name.into();
        row.record.insert("name".into(), Value::String(name.into()));
        for (key, value) in labels {
            row.record
                .insert((*key).into(), Value::String((*value).into()));
        }
        row.metric_labels = canonical_labels(&row.record).collect();
        row.tie = Tie::Single(id);
        row
    };
    let order = |query: &str, mut rows: Vec<Row>| {
        sort_metric_rows(&mut rows, &crate::query_language::parse(query).unwrap());
        rows.iter().map(row_id).collect::<Vec<_>>()
    };
    // k=b was written first, so its sample id is the lower.
    let tied = || {
        vec![
            sample("m", &[("k", "b")], 5, 1),
            sample("m", &[("k", "a")], 5, 2),
        ]
    };
    assert_eq!(order("METRIC m[]", tied()), [2, 1], "k=a before k=b");
    assert_eq!(
        order("METRIC m[] SORT value", tied()),
        [2, 1],
        "under a SORT too"
    );
    // Canonical labels compare as the joined string, where `,` (0x2c)
    // sorts after `+` (0x2b): "k=a+" < "k=a,l=x".
    let joined = vec![
        sample("m", &[("k", "a"), ("l", "x")], 5, 1),
        sample("m", &[("k", "a+")], 5, 2),
    ];
    assert_eq!(order("METRIC m[]", joined), [2, 1]);
    // Timestamp, then name, come first, and the sample id last.
    let rows = vec![
        sample("n", &[("k", "a")], 5, 1),
        sample("m", &[("k", "b")], 5, 2),
        sample("m", &[("k", "c")], 4, 3),
        sample("m", &[("k", "b")], 5, 0),
    ];
    assert_eq!(order("METRIC m[] SORT value", rows), [3, 0, 2, 1]);
}

#[test]
fn hidden_labels_preserve_metric_tiebreaks_through_per_series_aggregation() {
    let sample = |label: &str, id| {
        let labels = BTreeMap::from([("k".into(), Value::String(label.into()))]);
        let mut row = metric_template("m", 1, &labels).unwrap();
        row.tie = Tie::Single(id);
        // What authorize_row leaves after denying k.
        row.record.remove("k");
        row
    };
    let raw = vec![sample("b", 1), sample("a", 2)];
    let aggregate = |retain_labels| {
        raw.iter()
            .map(|row| {
                metric_aggregate_row(
                    row,
                    Value::Float(1.0),
                    0,
                    retain_labels,
                    "m",
                    1,
                    row_id(row),
                )
                .unwrap()
            })
            .collect::<Vec<_>>()
    };
    for mut rows in [raw.clone(), aggregate(true)] {
        sort_metric_rows(
            &mut rows,
            &crate::query_language::parse("METRIC m[]").unwrap(),
        );
        assert_eq!(rows.iter().map(row_id).collect::<Vec<_>>(), [2, 1]);
        assert!(rows.iter().all(|row| !row.record.contains_key("k")));
    }
    assert!(
        aggregate(false)
            .iter()
            .all(|row| row.metric_labels.is_empty())
    );
}

fn test_histogram(total_count: u64, final_count: u64) -> Vec<u8> {
    let mut writer = Writer::new();
    writer
        .write_map(4)
        .write_str("boundaries")
        .write_array(1)
        .write_float(1.0)
        .write_str("counts")
        .write_array(1)
        .write_uint(final_count)
        .write_str("total_count")
        .write_uint(total_count)
        .write_str("sum")
        .write_float(0.0);
    writer.to_bytes().unwrap()
}

/// A budget nobody else is using.
fn budget(limit: usize) -> HeldBudget {
    HeldBudget {
        used: Arc::new(AtomicUsize::new(0)),
        limit,
    }
}

/// Rows whose `k` and `n` fields come from a small, awkward domain:
/// text that differs only in ASCII case, integers equal to floats in
/// every representation, both zeros, NaN, null and a missing field.
fn awkward_rows() -> Vec<Row> {
    let keys = [
        Value::String("b".into()),
        Value::String("B".into()),
        Value::String("a".into()),
        Value::Signed(1),
        Value::Unsigned(1),
        Value::Float(1.0),
        Value::Float(-0.0),
        Value::Float(0.0),
        Value::Signed(0),
        Value::Float(f64::NAN),
        Value::Null,
        Value::Signed(-3),
        Value::Unsigned(u64::MAX),
        Value::Float(18_446_744_073_709_551_616.0),
    ];
    let numbers = [
        Value::Signed(5),
        Value::Unsigned(u64::MAX),
        Value::Float(2.5),
        Value::Signed(-7),
        Value::String("x".into()),
        Value::Float(-0.0),
        Value::Signed(5),
    ];
    // A fixed linear congruential sequence, so the run is repeatable.
    let mut state = 0x2545_f491_u64;
    let mut next = move |bound: usize| {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1);
        usize::try_from(state >> 33).unwrap() % bound
    };
    (0..600)
        .map(|index| {
            let mut record = Record::new();
            if index % 17 != 0 {
                record.insert("k".into(), keys[next(keys.len())].clone());
            }
            record.insert("j".into(), keys[next(4)].clone());
            record.insert("n".into(), numbers[next(numbers.len())].clone());
            Row {
                record,
                identifier: "t".into(),
                at: 0,
                metric_labels: Box::default(),
                tie: Tie::Single(index),
            }
        })
        .collect()
}

fn fold(rows: &[Row], aggregate: &RecordAggregate) -> String {
    let budget = budget(usize::MAX);
    let mut held = Held::new(&budget);
    let mut groups = Groups::new(aggregate);
    for row in rows {
        groups.add(row, &mut held).unwrap();
    }
    format!("{:?}", groups.finish().unwrap())
}

// What eventd did before it folded: every group compared in turn, and
// a GROUP's rows kept until the end. Normalise its output to the current
// default order; the fold must still agree on every group and value.
#[allow(
    clippy::too_many_lines,
    clippy::cast_precision_loss,
    clippy::option_if_let_else,
    reason = "the earlier implementation, kept whole and in its own shape as the reference"
)]
fn reference(rows: &[Row], aggregate: &RecordAggregate) -> String {
    fn find(groups: &[(Vec<Value>, Vec<&Row>)], keys: &[Value]) -> Option<usize> {
        groups.iter().position(|(representatives, _)| {
            representatives
                .iter()
                .zip(keys)
                .all(|(left, right)| left.language_equal(right))
        })
    }
    let fields: Vec<String> = match aggregate {
        RecordAggregate::CountBy(field)
        | RecordAggregate::Distinct(field)
        | RecordAggregate::TopBy { field, .. } => vec![field.clone()],
        RecordAggregate::Group { fields, .. } => fields.clone(),
    };
    let mut groups: Vec<(Vec<Value>, Vec<&Row>)> = Vec::new();
    for row in rows {
        let keys: Vec<_> = fields
            .iter()
            .map(|field| row.record.get(field).cloned().unwrap_or(Value::Null))
            .collect();
        if let Some(index) = find(&groups, &keys) {
            for (representative, value) in groups[index].0.iter_mut().zip(keys) {
                if language_cmp(&value, representative) == Ordering::Less {
                    *representative = value;
                }
            }
            groups[index].1.push(row);
        } else {
            groups.push((keys, vec![row]));
        }
    }
    if let RecordAggregate::Group { function, .. } = aggregate {
        groups.sort_by(|left, right| {
            for (left, right) in left.0.iter().zip(&right.0) {
                let order = language_cmp(left, right);
                if order.is_ne() {
                    return order;
                }
            }
            Ordering::Equal
        });
        if matches!(function, GroupFunction::Count) {
            // Stable sorting retains key order for equal counts.
            groups.sort_by_key(|group| std::cmp::Reverse(group.1.len()));
        }
    }
    let records: Vec<Record> = match aggregate {
        RecordAggregate::CountBy(field) | RecordAggregate::TopBy { field, .. } => {
            let mut counted: Vec<_> = groups
                .into_iter()
                .map(|(keys, members)| (keys[0].clone(), members.len() as u64))
                .collect();
            counted.sort_by(|left, right| {
                right
                    .1
                    .cmp(&left.1)
                    .then_with(|| language_cmp(&left.0, &right.0))
            });
            if let RecordAggregate::TopBy { count, .. } = aggregate {
                counted.truncate(usize::try_from(*count).unwrap());
            }
            counted
                .into_iter()
                .map(|(value, count)| {
                    BTreeMap::from([
                        (field.clone(), value),
                        ("count".into(), Value::Unsigned(count)),
                    ])
                })
                .collect()
        }
        RecordAggregate::Distinct(field) => {
            let mut values: Vec<_> = groups
                .into_iter()
                .map(|(keys, _)| keys[0].clone())
                .collect();
            values.sort_by(language_cmp);
            values
                .into_iter()
                .map(|value| BTreeMap::from([(field.clone(), value)]))
                .collect()
        }
        RecordAggregate::Group { fields, function } => groups
            .into_iter()
            .map(|(keys, members)| {
                let values: Vec<&Value> = members
                    .iter()
                    .filter_map(|row| match function {
                        GroupFunction::Count => None,
                        GroupFunction::Sum(field)
                        | GroupFunction::Avg(field)
                        | GroupFunction::Min(field)
                        | GroupFunction::Max(field) => row.record.get(field),
                    })
                    .filter(|value| {
                        matches!(
                            value,
                            Value::Signed(_) | Value::Unsigned(_) | Value::Float(_)
                        )
                    })
                    .collect();
                let sum = values.iter().map(|value| as_f64(value)).sum::<f64>();
                let exact = values.iter().try_fold(0_i128, |sum, value| match value {
                    Value::Signed(value) => sum.checked_add(i128::from(*value)),
                    Value::Unsigned(value) => sum.checked_add(i128::from(*value)),
                    _ => None,
                });
                let (name, value) = match function {
                    GroupFunction::Count => ("count", Value::Unsigned(members.len() as u64)),
                    _ if values.is_empty() => match function {
                        GroupFunction::Sum(_) => ("sum", Value::Null),
                        GroupFunction::Avg(_) => ("avg", Value::Null),
                        GroupFunction::Min(_) => ("min", Value::Null),
                        _ => ("max", Value::Null),
                    },
                    GroupFunction::Min(_) => (
                        "min",
                        (*values
                            .iter()
                            .copied()
                            .min_by(|left, right| language_cmp(left, right))
                            .unwrap())
                        .clone(),
                    ),
                    GroupFunction::Max(_) => (
                        "max",
                        (*values
                            .iter()
                            .copied()
                            .max_by(|left, right| language_cmp(left, right))
                            .unwrap())
                        .clone(),
                    ),
                    GroupFunction::Avg(_) => ("avg", Value::Float(sum / values.len() as f64)),
                    GroupFunction::Sum(_) => (
                        "sum",
                        if let Some(value) = exact.and_then(|exact| i64::try_from(exact).ok()) {
                            Value::Signed(value)
                        } else if let Some(value) =
                            exact.and_then(|exact| u64::try_from(exact).ok())
                        {
                            Value::Unsigned(value)
                        } else {
                            Value::Float(sum)
                        },
                    ),
                };
                let mut record: Record = fields.iter().cloned().zip(keys).collect();
                record.insert(name.into(), value);
                record
            })
            .collect(),
    };
    let rows: Vec<Row> = records
        .into_iter()
        .enumerate()
        .map(|(index, record)| Row {
            record,
            identifier: String::new(),
            at: 0,
            metric_labels: Box::default(),
            tie: Tie::Single(i64::try_from(index).unwrap()),
        })
        .collect();
    format!("{rows:?}")
}

#[test]
fn folded_aggregates_agree_with_comparing_every_group() {
    let rows = awkward_rows();
    let aggregates = [
        RecordAggregate::CountBy("k".into()),
        RecordAggregate::TopBy {
            count: 4,
            field: "k".into(),
        },
        RecordAggregate::Distinct("k".into()),
        RecordAggregate::Group {
            fields: vec!["k".into(), "j".into()],
            function: GroupFunction::Count,
        },
        RecordAggregate::Group {
            fields: vec!["j".into()],
            function: GroupFunction::Sum("n".into()),
        },
        RecordAggregate::Group {
            fields: vec!["k".into()],
            function: GroupFunction::Avg("n".into()),
        },
        RecordAggregate::Group {
            fields: vec!["j".into()],
            function: GroupFunction::Min("n".into()),
        },
        RecordAggregate::Group {
            fields: vec!["j".into()],
            function: GroupFunction::Max("n".into()),
        },
    ];
    for aggregate in &aggregates {
        assert_eq!(
            fold(&rows, aggregate),
            reference(&rows, aggregate),
            "{aggregate:?}"
        );
    }
    // Every NaN is one value, so they are one group (PSPU §3.21).
    let distinct = fold(&rows, &RecordAggregate::Distinct("k".into()));
    assert_eq!(distinct.matches("NaN").count(), 1);
}

// PEI-1296, PSPU §3.22: a log's is_error, served as a boolean, compares
// against 1 and 0 as against true and false.
#[test]
fn a_log_query_compares_is_error_against_1_and_0_as_true_and_false() {
    let matches = |text: &str, is_error: bool| {
        let query = crate::query_language::parse(&format!("LOGS WHERE {text}")).unwrap();
        let record = BTreeMap::from([("is_error".into(), Value::Bool(is_error))]);
        log_predicates(&query.predicates)
            .iter()
            .all(|predicate| evaluate(Namespace::Logs, predicate, &record))
    };
    for (text, error, normal) in [
        ("is_error == 1", true, false),
        ("is_error == 0", false, true),
        ("is_error == true", true, false),
        ("is_error != 1", false, true),
        ("is_error IN (1)", true, false),
        ("is_error NOT_IN (0)", true, false),
        ("is_error == 1 AND is_error != 0", true, false),
        ("is_error == 0 OR is_error == 1", true, true),
        // Only 1 and 0 are its integers; 2 is no boolean.
        ("is_error == 2", false, false),
    ] {
        assert_eq!(matches(text, true), error, "{text} of an error line");
        assert_eq!(matches(text, false), normal, "{text} of a normal one");
    }
    // An event's is_error is a payload field like any other, and types
    // do not coerce there (§3.20).
    let query = crate::query_language::parse("EVENTS WHERE is_error == 1").unwrap();
    let record = BTreeMap::from([("is_error".into(), Value::Bool(true))]);
    assert!(!evaluate(Namespace::Events, &query.predicates[0], &record));
}

#[test]
fn timestamp_comparisons_narrow_the_range_read() {
    let narrowed = |text: &str| {
        let query = crate::query_language::parse(&format!("EVENTS WHERE {text}")).unwrap();
        narrow_by_timestamp(&query.predicates, "event.time", 0, 1_000)
    };
    assert_eq!(narrowed("event.time <= 500"), (0, 501));
    assert_eq!(narrowed("event.time < 500"), (0, 500));
    assert_eq!(
        narrowed("event.time > 100 AND event.type == \"x\""),
        (0, 1_000)
    );
    assert_eq!(narrowed("event.time > 100"), (101, 1_000));
    assert_eq!(narrowed("event.time >= 100"), (100, 1_000));
    assert_eq!(narrowed("event.time == 7"), (7, 8));
    // Only a predicate that must hold narrows: one under OR does not.
    assert_eq!(narrowed("event.time < 5 OR event.cpu == 1"), (0, 1_000));
    // Bounds beyond the domain.
    assert_eq!(narrowed("event.time <= 9223372036854775807"), (0, 1_000));
    assert_eq!(narrowed("event.time < -5"), (0, -5));
    assert_eq!(
        narrowed("event.time > 18446744073709551615"),
        (i64::MAX, 1_000)
    );
    // A float bound narrows nothing; the predicate still applies.
    assert_eq!(narrowed("event.time < 5.5"), (0, 1_000));
    // An event's `timestamp` is a payload field, which narrows nothing.
    assert_eq!(narrowed("timestamp <= 500"), (0, 1_000));
    // A log record's time is still `timestamp`.
    let query = crate::query_language::parse("LOGS WHERE timestamp <= 500").unwrap();
    assert_eq!(
        narrow_by_timestamp(&query.predicates, "timestamp", 0, 1_000),
        (0, 501)
    );
}

#[test]
fn a_nan_satisfies_no_ordering_predicate() {
    let record = BTreeMap::from([("x".into(), Value::Float(f64::NAN))]);
    for text in [
        "x < 5", "x <= 5", "x > 5", "x >= 5", "x == 0", "x > -1.5", "x < 1.5",
    ] {
        let query = crate::query_language::parse(&format!("EVENTS WHERE {text}")).unwrap();
        assert!(
            !evaluate(Namespace::Events, &query.predicates[0], &record),
            "{text}"
        );
    }
    let query = crate::query_language::parse("EVENTS WHERE x != 5").unwrap();
    assert!(evaluate(Namespace::Events, &query.predicates[0], &record));
}

#[test]
fn language_equal_values_share_a_group_key() {
    let equal = [
        (Value::Signed(1), Value::Float(1.0)),
        (Value::Unsigned(1), Value::Signed(1)),
        (Value::Float(-0.0), Value::Signed(0)),
        (Value::String("KACS".into()), Value::String("kacs".into())),
        (
            Value::Array(vec![Value::Signed(2), Value::String("A".into())]),
            Value::Array(vec![Value::Float(2.0), Value::String("a".into())]),
        ),
    ];
    for (left, right) in &equal {
        assert!(left.language_equal(right), "{left:?} {right:?}");
        assert_eq!(
            GroupKey::of(left),
            GroupKey::of(right),
            "{left:?} {right:?}"
        );
    }
}

#[test]
fn a_sorted_query_with_take_keeps_only_its_best_rows() {
    let query = crate::query_language::parse("EVENTS SORT n DESC SKIP 3 TAKE 5").unwrap();
    let rows: Vec<Row> = (0..5_000)
        .map(|index| Row {
            record: BTreeMap::from([("n".into(), Value::Signed((index * 7_919) % 5_003))]),
            identifier: "t".into(),
            at: 0,
            metric_labels: Box::default(),
            tie: Tie::Single(index),
        })
        .collect();
    let budget = budget(usize::MAX);
    let mut held = Held::new(&budget);
    let mut gathered = Gathered::Rows(Vec::new());
    for row in rows.clone() {
        gathered.add(row, &query, Some(8), &mut held).unwrap();
    }
    let Gathered::Rows(mut kept) = gathered else {
        unreachable!()
    };
    assert!(kept.len() < 2 * TRIM_AT);
    sort_rows(&mut kept, &query);
    apply_pagination(&mut kept, &query);
    let mut all = rows;
    sort_rows(&mut all, &query);
    apply_pagination(&mut all, &query);
    assert_eq!(format!("{kept:?}"), format!("{all:?}"));
    assert_eq!(kept.len(), 5);
}

#[test]
fn what_queries_hold_is_shared_bounded_and_given_back() {
    let budget = budget(4 * HELD_GRANULE);
    let mut first = Held::new(&budget);
    first.add(1).unwrap();
    assert_eq!(budget.used.load(AtomicOrdering::Acquire), HELD_GRANULE);
    first.add(2 * HELD_GRANULE).unwrap();
    assert_eq!(budget.used.load(AtomicOrdering::Acquire), 3 * HELD_GRANULE);
    // A second query finds only what the first left.
    let mut second = Held::new(&budget);
    second.add(HELD_GRANULE).unwrap();
    assert!(matches!(second.add(1), Err(QueryError::HeldLimit)));
    drop(second);
    assert_eq!(budget.used.load(AtomicOrdering::Acquire), 3 * HELD_GRANULE);
    // Trimming gives back what is no longer held.
    first.set(10).unwrap();
    assert_eq!(budget.used.load(AtomicOrdering::Acquire), HELD_GRANULE);
    drop(first);
    assert_eq!(budget.used.load(AtomicOrdering::Acquire), 0);
}

// PEI-1295, TRM §6.4: GROUP … COUNT's groups are sorted by count
// descending and then taken, whatever order they were read in; ties
// go by the group keys under the value ordering (PSPU §3.21).
#[test]
fn grouped_counts_come_by_count_descending_then_key_whatever_the_read_order() {
    let grouped = |query: &str, values: &[(&str, i64)]| {
        let query = crate::query_language::parse(query).unwrap();
        let budget = budget(usize::MAX);
        let mut held = Held::new(&budget);
        let mut groups = Groups::new(query.aggregate.as_ref().unwrap());
        for (index, (v, w)) in values.iter().enumerate() {
            let row = Row {
                record: BTreeMap::from([
                    ("v".into(), Value::String((*v).into())),
                    ("w".into(), Value::Signed(*w)),
                ]),
                identifier: "t".into(),
                at: 0,
                metric_labels: Box::default(),
                tie: Tie::Single(i64::try_from(index).unwrap()),
            };
            groups.add(&row, &mut held).unwrap();
        }
        let mut rows = groups.finish().unwrap();
        apply_record_sort(&mut rows, &query);
        apply_pagination(&mut rows, &query);
        rows.iter()
            .map(|row| {
                let Some(Value::String(v)) = row.record.get("v") else {
                    panic!("a group has its key")
                };
                let w = match row.record.get("w") {
                    Some(Value::Signed(w)) => format!("{w}"),
                    _ => String::new(),
                };
                format!("{v}{w}")
            })
            .collect::<Vec<_>>()
    };
    // a once, b three times, c twice, d once; a and d tie, d read first.
    let read = [
        ("d", 0),
        ("a", 0),
        ("b", 0),
        ("c", 0),
        ("b", 0),
        ("c", 0),
        ("b", 0),
    ];
    assert_eq!(grouped("EVENTS GROUP v COUNT", &read), ["b", "c", "a", "d"]);
    let mut reversed = read;
    reversed.reverse();
    assert_eq!(
        grouped("EVENTS GROUP v COUNT", &reversed),
        ["b", "c", "a", "d"]
    );
    assert_eq!(grouped("EVENTS GROUP v COUNT TAKE 2", &read), ["b", "c"]);
    // Two keys: ties go by the first, then the second.
    let read = [("b", 2), ("a", 2), ("b", 1), ("a", 1), ("a", 1)];
    assert_eq!(
        grouped("EVENTS GROUP v, w COUNT", &read),
        ["a1", "a2", "b1", "b2"]
    );
}

#[test]
fn numeric_groups_sort_by_keys_before_pagination_regardless_of_input_order() {
    let values = [("z", 2), ("a", 2), ("z", 1), ("a", 1)];
    for function in ["SUM", "AVG", "MIN", "MAX"] {
        for offset in 0..values.len() {
            for (suffix, expected) in [
                ("", vec![("a", 1), ("a", 2), ("z", 1), ("z", 2)]),
                (" SKIP 1 TAKE 2", vec![("a", 2), ("z", 1)]),
                (" SORT {aggregate} SKIP 1 TAKE 2", vec![("a", 2), ("z", 1)]),
                (
                    " SORT v DESC, w",
                    vec![("z", 1), ("z", 2), ("a", 1), ("a", 2)],
                ),
            ] {
                let suffix = suffix.replace("{aggregate}", &function.to_ascii_lowercase());
                let query = crate::query_language::parse(&format!(
                    "EVENTS GROUP v, w {function} n{suffix}"
                ))
                .unwrap();
                let budget = budget(usize::MAX);
                let mut held = Held::new(&budget);
                let mut groups = Groups::new(query.aggregate.as_ref().unwrap());
                for index in 0..values.len() {
                    let (v, w) = values[(index + offset) % values.len()];
                    groups
                        .add(
                            &Row {
                                record: BTreeMap::from([
                                    ("v".into(), Value::String(v.into())),
                                    ("w".into(), Value::Signed(w)),
                                    ("n".into(), Value::Signed(10)),
                                ]),
                                identifier: "t".into(),
                                at: 0,
                                metric_labels: Box::default(),
                                tie: Tie::Single(0),
                            },
                            &mut held,
                        )
                        .unwrap();
                }
                let mut rows = groups.finish().unwrap();
                apply_record_sort(&mut rows, &query);
                apply_pagination(&mut rows, &query);
                let got: Vec<_> = rows
                    .iter()
                    .map(|row| (row.record["v"].clone(), row.record["w"].clone()))
                    .collect();
                let expected: Vec<_> = expected
                    .into_iter()
                    .map(|(v, w)| (Value::String(v.into()), Value::Signed(w)))
                    .collect();
                assert_eq!(got, expected, "{function}{suffix}, rotation {offset}");
            }
        }
    }
}

#[test]
fn numeric_groups_use_language_order_and_declared_key_priority() {
    // Type order, mathematical numeric order, and ASCII-folded text
    // order all differ from sorting encoded values or arrival order.
    let values = [
        Value::Null,
        Value::Bool(false),
        Value::Bool(true),
        Value::Signed(-2),
        Value::Float(0.5),
        Value::Unsigned(3),
        Value::String("a".into()),
        Value::String("Z".into()),
        Value::Binary(vec![0]),
    ];
    for function in ["SUM", "AVG", "MIN", "MAX"] {
        for fields in ["z, a", "a, z"] {
            let query =
                crate::query_language::parse(&format!("EVENTS GROUP {fields} {function} n"))
                    .unwrap();
            let budget = budget(usize::MAX);
            let mut held = Held::new(&budget);
            let mut groups = Groups::new(query.aggregate.as_ref().unwrap());
            for (index, value) in values.iter().enumerate().rev() {
                groups
                    .add(
                        &Row {
                            record: BTreeMap::from([
                                ("z".into(), value.clone()),
                                (
                                    "a".into(),
                                    Value::Unsigned(100 - u64::try_from(index).unwrap()),
                                ),
                                ("n".into(), Value::Signed(10)),
                            ]),
                            identifier: "t".into(),
                            at: 0,
                            metric_labels: Box::default(),
                            tie: Tie::Single(0),
                        },
                        &mut held,
                    )
                    .unwrap();
            }
            let rows = groups.finish().unwrap();
            let got: Vec<_> = rows.iter().map(|row| row.record["z"].clone()).collect();
            let mut expected = values.to_vec();
            if fields == "a, z" {
                expected.reverse();
            }
            assert_eq!(got, expected, "{fields} {function}");
        }
    }
}

#[test]
fn an_aggregation_past_the_budget_fails_rather_than_grows() {
    let budget = budget(HELD_GRANULE);
    let mut held = Held::new(&budget);
    let aggregate = RecordAggregate::Distinct("k".into());
    let mut groups = Groups::new(&aggregate);
    let failed = (0..10_000).find_map(|index| {
        let row = Row {
            record: BTreeMap::from([("k".into(), Value::String(format!("value {index}")))]),
            identifier: "t".into(),
            at: 0,
            metric_labels: Box::default(),
            tie: Tie::Single(index),
        };
        groups.add(&row, &mut held).err()
    });
    assert!(matches!(failed, Some(QueryError::HeldLimit)));
}

/// An authorizer whose every access check fails, as each does once KACS
/// is gone: its token is `/dev/null`, which is no token.
fn authorizer_without_kacs(
    descriptors: Arc<super::super::security::DescriptorCache>,
) -> Authorizer {
    let token = std::os::fd::OwnedFd::from(std::fs::File::open("/dev/null").unwrap());
    Authorizer::for_token(peios::token::Token::from(token), descriptors)
}

fn readable_by_everyone() -> peios::security::SecurityDescriptor {
    peios::security::sddl::parse("O:SYG:SYD:P(A;;0x00000001;;;WD)").unwrap()
}

fn query_limits() -> Limits {
    Limits {
        deadline: Instant::now() + Duration::from_mins(1),
        cross_type_window: Duration::from_secs(1),
        cross_type_max_lookback: Duration::from_hours(1),
        rollups: None,
        adaptive_rollup_min_samples: 0,
        adaptive_rollup_batch_rows: 0,
        adaptive_rollup_max_rows: 0,
        held: budget(1 << 20),
    }
}

/// A real event shard, as the writer keeps it, holding events of type
/// `t` with sequences and timestamps `sequences`.
fn writer_shard(path: &Path, sequences: core::ops::RangeInclusive<u64>) -> eventd_core::Shard {
    let mut shard = eventd_core::Shard::open(path, 1_000).unwrap();
    shard.commit(&ingest_items(sequences)).unwrap();
    shard
}

fn ingest_items(sequences: core::ops::RangeInclusive<u64>) -> Vec<eventd_core::IngestItem> {
    sequences
        .map(|sequence| eventd_core::IngestItem {
            gaps: Vec::new(),
            store_event: true,
            event: eventd_core::RealEvent {
                boot_id: [1; 16],
                timestamp: sequence,
                cpu_id: 0,
                sequence,
                origin_class: 0,
                effective_token_guid: [2; 16],
                true_token_guid: [3; 16],
                process_guid: [4; 16],
                event_type: "t".into(),
                payload: [0x80].into(),
            },
        })
        .collect()
}

fn store_directory() -> PathBuf {
    let sequence = TEST_DATABASE_SEQUENCE.fetch_add(1, AtomicOrdering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "eventd-query-store-{}-{sequence}",
        std::process::id()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

// TRM §9.3: when KACS fails, a query that needs a fresh access check is
// denied.
#[test]
fn a_query_whose_fresh_access_check_fails_is_denied_not_allowed() {
    let directory = store_directory();
    let shard_path = directory.join("shard-0000.db");
    let shard = writer_shard(&shard_path, 1..=3);
    let stores = Stores {
        event_paths: vec![shard_path],
        log_path: directory.join("logs.db"),
        metric_path: directory.join("metrics.db"),
    };
    // The descriptor is already resolved, so the registry plays no part:
    // the one thing the query still needs is the check itself.
    let descriptors = Arc::new(super::super::security::DescriptorCache::new());
    descriptors.resolve_as(Namespace::Events, "t", "*", &readable_by_everyone());
    let authorizer = authorizer_without_kacs(descriptors);

    let mut emitted = Vec::new();
    let outcome = execute(
        &crate::query_language::parse("EVENTS").unwrap(),
        &stores,
        &authorizer,
        &query_limits(),
        &mut |record| {
            emitted.push(record);
            Ok(())
        },
    );
    assert!(
        matches!(outcome, Err(QueryError::Security(_))),
        "the query is denied: {outcome:?}"
    );
    assert!(emitted.is_empty(), "and none of its records were allowed");
    assert!(
        authorizer
            .check(Namespace::Events, "t", &["event.type".to_owned()])
            .is_err(),
        "the identifier is denied rather than allowed"
    );
    drop(shard);
    std::fs::remove_dir_all(directory).unwrap();
}

// TRM §9.3: cached check results stay valid for the duration of the
// query that obtained them.
#[test]
fn a_verdict_is_reused_for_the_rest_of_its_query_without_a_second_check() {
    let descriptors = Arc::new(super::super::security::DescriptorCache::new());
    descriptors.resolve_as(Namespace::Events, "t", "*", &readable_by_everyone());
    let authorizer = authorizer_without_kacs(descriptors);
    let fields = vec!["event.type".to_owned(), "event.time".to_owned()];
    let granted = HashSet::from(["event.type".to_owned(), "event.time".to_owned()]);
    let mut cache = AuthorizationCache::new(&authorizer);
    // The verdicts this query obtained while KACS still answered.
    cache
        .identifiers
        .insert(("t".to_owned(), fields.clone()), true);
    cache
        .entries
        .insert(("t".to_owned(), fields.clone()), Some(granted.clone()));

    // KACS now fails every check, and the query goes on from its verdicts.
    for _ in 0..3 {
        assert!(
            cache
                .may_read(&authorizer, Namespace::Events, "t", &fields)
                .unwrap()
        );
        assert_eq!(
            cache
                .check(&authorizer, Namespace::Events, "t", &fields)
                .unwrap(),
            Some(granted.clone())
        );
    }
    // What it has not yet been told needs a check, which fails.
    assert!(
        cache
            .check(&authorizer, Namespace::Events, "t", &["message".to_owned()])
            .is_err()
    );
}

// TRM §9.4: where eventd cannot allocate what an admitted query needs,
// it fails that query rather than blocking a writer.
#[test]
fn a_query_whose_store_cannot_be_opened_fails_without_blocking_a_writer() {
    let directory = store_directory();
    let shard_path = directory.join("shard-0000.db");
    let mut shard = writer_shard(&shard_path, 1..=1);
    // A shard the query cannot open, as when it has no descriptor left
    // for the connection: SQLite reports both as CANTOPEN.
    let stores = Stores {
        event_paths: vec![shard_path.clone(), directory.join("absent/shard-0001.db")],
        log_path: directory.join("logs.db"),
        metric_path: directory.join("metrics.db"),
    };
    let authorizer =
        authorizer_without_kacs(Arc::new(super::super::security::DescriptorCache::new()));
    let query = crate::query_language::parse("EVENTS").unwrap();

    // The writer commits throughout. Its connection waits for the write
    // lock only for the moment a reader re-reading the wal-index header
    // holds it, so a lock the query held any longer would fail a commit.
    let writer = std::thread::spawn(move || {
        for sequence in 2..=500 {
            shard.commit(&ingest_items(sequence..=sequence)).unwrap();
        }
        shard
    });
    let mut failed = 0;
    while !writer.is_finished() || failed == 0 {
        let outcome = execute(&query, &stores, &authorizer, &query_limits(), &mut |_| {
            panic!("a query that failed sends no records")
        });
        let Err(error) = outcome else {
            panic!("the query cannot succeed without its store")
        };
        assert!(
            matches!(&error, QueryError::Sql(cause)
                if cause.sqlite_error_code() == Some(rusqlite::ErrorCode::CannotOpen)),
            "{error}"
        );
        failed += 1;
    }
    let shard = writer.join().expect("every commit succeeded");
    let committed: i64 = Connection::open(&shard_path)
        .unwrap()
        .query_row("SELECT count(*) FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(committed, 500);
    drop(shard);
    std::fs::remove_dir_all(directory).unwrap();
}
