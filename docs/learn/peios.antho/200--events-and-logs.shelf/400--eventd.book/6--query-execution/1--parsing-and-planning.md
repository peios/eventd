---
title: Parsing and Planning
description: The four phases between a query string and an answer, what can
  only fail after planning, and when payloads are decoded.
---

A query arrives as one string (PSPU §3.15). Turning it into an answer
has four phases before any data is read: parse, plan, authorize,
execute. [*plan.a-query-runs-parse-then-plan-then-authorize-then-execute]

## Parsing

The string is parsed into a syntax tree. The parser:

1. Identifies the mode from the first token — `EVENTS`, `LOGS`, or
   `METRIC`. [*plan.the-mode-is-identified-from-the-first-token]
2. Extracts the primary selector: a type pattern, a `FROM` list, or a
   metric name with an optional label selector.
   [*plan.the-parser-extracts-the-primary-selector]
3. Collects every clause, in whatever order they appear.
   [*plan.clauses-are-collected-in-whatever-order-they-appear]
4. Validates that the clauses suit the mode — `CONTAINING` only in log
   mode, `RATE` only in metric mode, `SELECT` only where a result schema
   is not fixed.
   [*plan.clause-and-mode-compatibility-is-checked-at-parse-time]

Parse errors are returned immediately, before anything is opened, read
or authorized.
[*plan.parse-errors-return-before-anything-is-opened-read-or-authorized]
A malformed query costs a decode and a parse.

## Expression depth

The parser enforces two independent limits before query execution:

- Grouping parentheses can nest at most 128 pairs deep.
- Each boolean expression tree can contain at most 128 nodes on a path
  from its root to a leaf.

A comparison, membership predicate (`IN` or `NOT_IN`), or null predicate
(`IS NULL` or `IS NOT NULL`) is a leaf with depth 1. An `AND` or `OR` node
has depth 1 plus its deeper child's depth. Both operators share the same
tree limit. Grouping parentheses do not add nodes to the stored tree.

A flat chain of 128 predicates fits the tree limit; a chain of 129 does
not. Parentheses around an `IN` list are list delimiters, not recursive
expression grouping. Each `WHERE` clause is checked separately.

Exceeding either limit returns the parse error
`expression depth exceeds 128 levels`. The caller can simplify the
expression or remove redundant grouping and retry. These limits are
independent of the query request's byte limit and prevent deeply nested
parser and expression-tree traversal from exhausting the worker stack.

## What can only fail later

Some failures need data. The parser cannot know whether a metric name
resolves to a counter or a histogram, how many series a selector
matches, or whether the effective range exceeds the cross-type lookback
limit — those depend on the store, so they surface at planning or
execution time (PSPU §3.B).
[*plan.store-dependent-failures-surface-at-planning-or-execution-not-parse]

The practical consequence is that the same query string can parse
everywhere and fail on one machine: a metric selector matching one
series on a two-core box matches two on a four-core one.

## Planning

Planning resolves what the query will actually touch:

- which concrete identifiers the data could carry — event types, log
  origins, metric names — because access control resolves per identifier
  and a broad selector authorizes nothing by itself (§7.4)
  [*plan.planning-resolves-the-concrete-identifiers-the-data-could-carry]
- which series a metric selector matches, and whether they are
  type-homogeneous
[*plan.planning-resolves-matched-series-and-checks-they-are-type-homogeneous]
- which fields the query references, for both authorization and
  frequency accounting (§6.5)
  [*plan.planning-collects-referenced-fields-for-authorization-and-accounting]
- which stores are involved, including any cross-type source
[*plan.planning-identifies-every-store-involved-including-cross-type-sources]

Identifier discovery reads compact catalogs: the union of every
shard's `event_types`, the log store's `log_origins`, and the metric
store's `series` rows (§3.1, §4.2, §5.2).
<!-- lint-disable-next-line line-length spelling -->
[*plan.identifier-discovery-reads-the-event-types-log-origins-and-series-catalogues]
The primary selector filters that set before access checks.
<!-- lint-disable-next-line line-length -->
[*plan.the-primary-selector-filters-discovered-identifiers-before-access-checks]
Discovery therefore never runs a `DISTINCT` scan over a hot records
table and does not depend on an adaptive event index being present.
[*plan.discovery-never-scans-a-records-table-or-needs-an-adaptive-index]

Catalogs can conservatively contain a name whose final retained row was
<!-- lint-disable-next-line spelling words -->
deleted. [*plan.a-catalogue-may-keep-a-name-whose-rows-were-all-deleted]
That safe superset can cause an extra access check but cannot
expose a row or omit a concrete identifier that committed successfully.
<!-- lint-disable-next-line line-length spelling -->
[*plan.a-stale-catalogue-name-never-exposes-a-row-or-omits-a-committed-identifier]
Low-priority cleanup is optional and is never query-critical.

## Header literal types

[*plan.header-literal-coercions-are-scoped-to-the-query-namespace]

Literal conversion uses the namespace of the record being compared.
`emitter.class` aliases apply only to event headers. GUID forms apply
to the event GUID headers, log `boot_id` and `job_id`, and metric
`boot_id`. The same spelling in an ordinary payload field or metric
label remains a string; it does not borrow another namespace's header
type (PSPU §3.22).

The rule holds for comparisons, membership and compound predicates,
metric bracket selectors, and metric selectors inside historical or
streaming cross-type conditions. A metric condition uses the metric
namespace even when the query's primary source is events or logs.

## When payloads are decoded

Event payloads are stored as opaque MessagePack and are never decoded on
the write path (§3.1). Decoding happens here, on the read path, and only
where a query needs it: to evaluate a payload predicate, to build a flat
result record, or to compute a payload expression index's key on insert.
[*plan.payloads-are-decoded-only-where-a-query-needs-them]

At high result counts this dominates the query path. Constructing flat
maps from thousands of events means decoding thousands of payloads and
applying the flattening rules of PSPU §3.22 to each. Partial extraction
is the lever: with a `SELECT` present, only the named paths need
decoding, and without one a streaming decoder that emits flattened pairs
avoids materializing the payload at all (§C).

## Read connections

Execution uses read-only SQLite connections, which in WAL mode do not
block writer threads. [*plan.queries-execute-on-read-only-sqlite-connections]
The one exception is SQLite's own. A reader that catches the wal-index
header while a writer is updating it takes the write lock for the moment
it needs to read the header again. A writer therefore waits up to 25
milliseconds for the write lock rather than failing a commit at once,
and fails only if the lock is held longer.

An event query opens one connection per shard database in the directory
(§6.4). A log or metric query opens one.
[*plan.a-log-or-metric-query-opens-a-single-connection] eventd supports
concurrent queries up to its admission limit (§6.5), subject to the
operating system actually having the descriptors and memory; where it
cannot allocate what an admitted query needs, it fails that query rather
than blocking a writer or exceeding the limit.
<!-- lint-disable-next-line line-length -->
[*plan.an-admitted-query-that-cannot-get-resources-fails-rather-than-blocking-a-writer]

## Timestamp fields

ISO timestamp hour, minute, and second fields must be nonnegative and in
range. A minus sign in any of those fields is rejected as a parse error;
it does not subtract time from the date.
