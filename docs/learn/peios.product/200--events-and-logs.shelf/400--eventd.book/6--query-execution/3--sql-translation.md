---
title: SQL Translation
description: What the query language translates to directly, what does not
  translate, where access control sits, and how aggregation is handled.
---

Events and logs are translated to SQL. Metrics are translated to SQL
against `series` and `samples`. Clients never see any of it; the
translation is entirely internal and carries no guarantees.
[*sql.clients-never-see-the-generated-sql]

## What translates directly

**Event header fields** are columns, so a predicate on one can become a
SQL `WHERE` comparison over an indexable column (§3.1). Today one
predicate does per shard: the first of `event.type ==` a string, which
becomes a comparison of the `event_type` column, or a comparison of
`event.cpu` or `emitter.class` with an integer, which compares
`cpu_id` or `origin_class`, falling back to an indexed payload field
(below). Other header predicates, `emitter.process.guid` among them,
are applied after loading. A column's own name in a predicate is a
payload field, not the column.
[*sql.an-event-header-predicate-becomes-a-sql-where-comparison]

**Timestamps** bound every read already, through `SINCE` and `UNTIL`. A
top-level comparison of the record's time with an integer; `event.time`
for events, `timestamp` for logs; narrows that range further, so a page
of older records; `WHERE event.time <= T`; starts reading at `T`
rather than at the newest record. One inside an `OR` does not narrow,
since it need not hold.
[*sql.a-top-level-timestamp-comparison-narrows-the-range-read]

**Log fields** are all columns; log mode has no payload and its field
set is closed (§4.2). [*sql.every-log-field-is-a-column]

**Metric selection** resolves names and labels through `series`; from
the in-memory cache where possible; and reads `samples` for the range,
ordered by the composite index that already provides `(timestamp, id)`
(§5.2).
<!-- lint-disable-next-line line-length -->
[*sql.metric-selection-resolves-through-series-and-reads-samples-in-index-order]

## What does not

**Event payload predicates** have no column. They become eventd-internal
payload extraction predicates, and can use an adaptive payload
expression index to narrow candidates (§3.4).
[*sql.a-payload-predicate-becomes-an-internal-extraction-predicate]

**`HAS`**, array containment, narrows nothing. The payload expression
index stores one key per field, and a field holding an array of group
SIDs has no single key to store, so a `HAS` predicate is answered by
decoding each candidate row and testing its array in full.
[*sql.has-uses-no-index-and-tests-each-candidates-array-in-full] The operator
is correct on every row and costs a scan of whatever the rest of the
query left; a query using it wants a time range or another predicate
beside it.

The rule governing every such translation is that **SQL narrows, the
query language decides**. Where a SQL construct cannot reproduce a
predicate's comparison semantics exactly, eventd uses it only to reduce
the candidate set and then applies the real predicate after loading the
row.
<!-- lint-disable-next-line line-length -->
[*sql.sql-only-narrows-candidates-and-the-real-predicate-is-applied-after-loading]

SQLite's native dynamic-type equality and ordering never substitute for
the query language's ASCII case folding, exact numeric comparison,
binary comparison, array comparison, or null and missing-field handling
(§6.2). An index that returns a smaller set faster is useful; one that
returns a *different* set is a wrong answer arriving quickly.

## Where access control sits

Access filtering is part of the logical execution, not a filter over the
output (PSPU §3.18, §3.28). eventd can push authorization predicates
down into SQL when the concrete identifier set is known at planning
time, or read candidate rows and discard them before aggregating.

Which it does is a performance decision. What is fixed is that the
externally visible result is identical to the one filtering-first would
produce; aggregates, ordering and pagination included, since all three
would otherwise leak the existence of rows the caller cannot read.
<!-- lint-disable-next-line line-length -->
[*sql.results-equal-filtering-first-including-aggregates-ordering-and-pagination]

## Aggregation

Aggregation is never pushed into SQL. SQL reads the candidate rows, and
eventd folds them into groups as they are read (§6.4), under the
language's own equality (§6.2) and after access filtering has discarded
what the caller cannot read; grouping over columns included, not only
grouping over payload paths whose comparison semantics SQL cannot
reproduce.
[*sql.aggregation-is-never-pushed-into-sql-and-rows-fold-in-eventd]

For an event query this means every shard yields its matching rows,
so the work is proportional to the row count, while what the
aggregation holds is proportional to the group cardinality (§6.4).
