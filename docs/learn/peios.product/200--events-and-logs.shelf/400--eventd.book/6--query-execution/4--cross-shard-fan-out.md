---
title: Cross-Shard Fan-Out
description: Event queries run against every database in the store, active and
  historical — how results merge, and the unbounded case.
---

Event queries execute against **every** database in the event store
directory — active shards and historical ones alike (§3.3).
[*fanout.an-event-query-runs-against-every-active-and-historical-shard]
Log and metric queries touch one database each and need none of this.

Shards carry no meaning for the query path (§2.3). A shard holds
whatever CPUs routed to it during whatever lifetimes wrote it, so there
is no shard a query can skip on the basis of its contents, and a
predicate on `event.cpu` scans all of them.
[*fanout.no-shard-is-skipped-even-for-a-cpu-id-predicate]

## Merging

How results combine depends on the query.

**Queries in the default order.** A non-aggregating query with no
`SORT` reads every shard at once, each newest first through
`idx_events_timestamp`, and the coordinator takes whichever shard's
next row comes first under the tiebreakers of §6.2: an N-way merge of
the sorted streams.
[*fanout.non-aggregating-results-are-an-n-way-merge-of-sorted-shard-streams]
Each row that passes access control and every predicate is sent as soon
as it is taken.
[*fanout.merged-results-are-streamed-incrementally-not-materialised]
Once `SKIP + TAKE` rows have passed, the merge stops.
[*fanout.with-take-the-merge-stops-once-skip-plus-take-rows-have-passed]
What the query holds is one row per shard, the merge frontier, whatever
the size of its result.
[*fanout.a-default-order-query-holds-one-row-per-shard]

**Sorted queries.** A `SORT` on any other field reads the shards in
turn and gathers the visible rows. With `TAKE` it keeps only the best
`SKIP + TAKE` of them, trimming to that whenever it holds twice as many
or a thousand, whichever is more.
[*fanout.a-sorted-query-with-take-keeps-only-its-best-skip-plus-take-rows]
Without `TAKE` it holds every visible row. Either way it holds them
within `MaxQueryHeldBytes` (§6.5).

**Aggregating queries.** Rows from every shard fold into one set of
groups as they are read, and no row is kept.
[*fanout.aggregating-queries-fold-rows-into-groups-as-they-are-read]
Each group keeps:

| Query | Each group keeps |
|---|---|
| `COUNT BY`, `TOP N BY`, `GROUP … COUNT` | a count; at the end the groups are sorted by count descending and `TAKE` applies |
| `GROUP … SUM` | the exact integer sum while every value is an integer and it fits, and the binary64 sum |
| `GROUP … AVG` | the binary64 **sum and count**, divided once at the end |
| `GROUP … MIN` / `MAX` | the extreme so far |
| `DISTINCT` | the value |

[*fanout.grouped-counts-are-sorted-descending-then-taken]
[*fanout.group-sum-keeps-the-exact-and-binary64-sums]
[*fanout.group-avg-divides-the-whole-sum-by-the-whole-count]
[*fanout.group-min-and-max-keep-the-extreme-so-far]
[*fanout.distinct-keeps-each-value]

`GROUP … SUM`, `AVG`, `MIN` and `MAX` groups are sorted by their keys
ascending, in the order the fields are named, before `SKIP` and `TAKE`
apply. An explicit `SORT` overrides that order, with group-key order
breaking its ties (PSPU §3.21).
[*fanout.numeric-groups-are-sorted-by-keys-before-pagination]

`AVG` is the one that cannot be composed from its own output: averaging
partial averages weights each part equally, regardless of how many rows
it held, so the sum and the count are kept and divided once.

A group is found by hashing a key that every pair of language-equal
values shares — numbers by their nearest binary64, text with ASCII case
folded — and then by the language's own equality against each group's
representative in the order the groups appeared (§6.2). The hash only
narrows the search; equality decides.
[*fanout.groups-are-found-by-a-hash-that-equal-values-share]

What an aggregation holds is bounded by the number of its groups rather
than by its rows, within `MaxQueryHeldBytes` (§6.5).
[*fanout.aggregation-memory-is-bounded-by-group-cardinality]

## The unbounded case

A non-aggregating query without `TAKE` has no implicit row limit.
[*fanout.a-non-aggregating-query-without-take-has-no-implicit-row-limit]
`EVENTS SINCE 7d ago` can match millions of rows, all of which pass
through the merge.

In the default order that costs time and no memory, so the query
timeout is the only backstop (§6.5).
[*fanout.the-query-timeout-is-the-only-backstop-for-an-unbounded-query]
The timeout covers sending as well as reading, so a client that reads
slowly can run out of time on a result the merge would have finished.

## Descriptors

An event query opens a read-only connection per database, and each
SQLite connection holds one or two descriptors for the database and its
write-ahead log.
[*fanout.an-event-query-opens-a-read-only-connection-per-database]
With many historical shards this adds up quickly across concurrent
queries.

Active shard writer connections stay open for the process lifetime and
are not negotiable.
[*fanout.active-shard-writer-connections-stay-open-for-the-process-lifetime]
Historical shard read connections are the pool worth
bounding — opened when a query touches them, closed after a period of
inactivity (§C).
