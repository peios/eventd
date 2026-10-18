---
title: Accounting and Limits
description: What every query records for the adaptive indexer, and the
  concurrency, memory and timeout limits it runs under.
---

## Recording what was asked

Every event query is recorded by the adaptive indexing system (§3.4).
[*account.every-event-query-is-recorded-by-the-adaptive-indexer]
For each `WHERE` predicate:

- a header field reference increments the frequency counter of that
  field's column, kept under the field's path (`event.type`,
  `emitter.process.guid`); `event.time`, whose column is always
  indexed, is not counted
  [*account.a-header-column-predicate-increments-that-columns-counter]
- a payload field reference increments that path's counter
  [*account.a-payload-field-predicate-increments-that-paths-counter]

Cross-type `WHERE` predicates are counted like any other, since they
narrow the same data by the same fields.
[*account.cross-type-where-predicates-are-counted-like-any-other]

This applies to **event queries only**. The log and metric stores have
fixed write-time indexes and no candidate space for a policy to explore
(§4.2, §5.2), so their queries increment nothing.
[*account.log-and-metric-queries-increment-no-counters]

Counters are in-memory and are flushed to the metadata database at each
policy interval (§3.5).
[*account.counters-are-flushed-to-the-metadata-database-each-policy-interval]
Query handlers never write to that database directly, which is what
keeps the once-per-query update off any lock a writer thread contends
for. [*account.query-handlers-never-write-the-metadata-database-directly]

## Concurrency

eventd bounds concurrent queries — streaming and non-streaming together
— at `MaxConcurrentQueries` (§A).
<!-- lint-disable-next-line line-length -->
[*account.max-concurrent-queries-bounds-streaming-and-non-streaming-queries-together]
Beyond it a query is rejected with an error rather than queued.
[*account.a-query-over-the-concurrency-limit-is-rejected-not-queued]

The per-query cost that limit is protecting is real: read-only SQLite
connections, one per shard for an event query (§6.4), memory for the
merge, and CPU for execution and payload decoding.

`MaxStreamingQueries` is enforced separately and is lower.
[*account.max-streaming-queries-is-a-separate-lower-limit] A streaming
query holds its resources for as long as its client stays connected,
where an ordinary one holds them for at most a timeout, so the two
populations need different bounds.

Both are global. Every authenticated caller can connect (§7.1), so
eventd also bounds the queries one caller can have running at once, at
`MaxQueriesPerUser`.
[*account.max-queries-per-user-bounds-one-callers-running-queries]
The caller is the user SID of the token read at connect.
[*account.the-per-user-limit-counts-by-the-user-sid-of-the-peer-token]
The slot is taken as soon as the token is read, before the request, so
connections held open without a query count too.
[*account.the-per-user-slot-is-taken-before-the-request-is-read]
SYSTEM's queries are not counted: SYSTEM is the machine, not one caller
among others. [*account.system-queries-are-not-counted-per-user] Over
the limit, the query is rejected with an error, as over the global ones.
[*account.a-query-over-the-per-user-limit-is-rejected-with-an-error]

The per-user limit counts users, not processes or sessions: two windows
of the same person share one budget, and two people never share one.

Queries and ingestion are separate channels, so exhausting the query
side cannot exhaust ingestion (PSPU §3.3).
[*account.exhausting-the-query-slots-cannot-exhaust-ingestion]

## Memory

eventd can not be killed: it runs with `oom_score_adj` −1000, so if its
memory alone fills the machine, the kernel has nothing left to reclaim
and panics. What bounds a query's memory therefore has to hold however
many queries run at once.

A query in the default order holds one row per shard and nothing more
(§6.4), and is never refused for the size of its result (PSPU §3.16).
[*account.a-default-order-query-is-never-refused-for-its-size]

Everything else a query must hold — a sorted query's rows, an
aggregation's groups, a watch batch — counts against
`MaxQueryHeldBytes` (§A), **one budget for every running query
together**.
[*account.max-query-held-bytes-bounds-what-all-running-queries-hold-together]
A query takes from it in 64 KiB granules as it grows, gives back what
it does not need when a sorted query trims, and gives back all of it
when it ends.
[*account.held-memory-is-reserved-in-granules-and-given-back]
A query that would take more than is left fails with an error, rather
than being truncated or answered from part of the data.
[*account.a-query-past-the-held-budget-fails-with-an-error]

What a row or group costs is estimated from its fields and values, not
measured from the allocator, so the budget is approximate. It is still a
bound: with the default of 256 MiB, no number of concurrent queries
holds much more than that.

A metric query reads each series' samples in order and holds none of
them: a transform keeps only the sample before, and an aggregation folds
each sample into its window, or its series, as it is read.
[*account.a-metric-query-folds-samples-as-it-reads-them]
What it holds is what its result needs, and that counts against the
same budget: the matched series, one fold per window or per series, the
latest point of each series, and the result rows. A query whose result
is the samples themselves — a range without an aggregation — holds them
all, and is refused when they pass the budget like a sorted query.
[*account.a-metric-querys-held-series-folds-and-rows-count-against-the-budget]
A window aggregation over the same range holds one fold per window,
however many samples fall in it.

## Request and response sizes

eventd rejects an inbound frame above `MaxQueryRequestBytes` before
allocating or reading its payload.
<!-- lint-disable-next-line line-length -->
[*account.a-request-frame-over-max-query-request-bytes-is-rejected-before-reading]
Outbound result records are grouped at record boundaries toward
`QueryResponseTargetBytes` (§A).
<!-- lint-disable-next-line line-length -->
[*account.result-records-are-grouped-at-record-boundaries-toward-the-response-target]
The target is not a limit: a complete record that exceeds it is sent
alone, never split, truncated, skipped or failed merely for exceeding
the target (PSPU §3.15–§3.16).
[*account.a-record-over-the-response-target-is-sent-alone-and-whole]
Only the protocol's `u32` frame length is a hard outbound bound.
[*account.the-u32-frame-length-is-the-only-hard-outbound-bound]

## Timeouts

Every query has a maximum execution time, `QueryTimeoutMs` (§A).
[*account.every-query-is-bounded-by-query-timeout-ms]

The clock starts once the request has been decoded and the caller's
token obtained, and covers everything after: parsing, planning, access
checks, cross-type pre-computation, SQL execution, merging, aggregation,
pagination, projection, and transmitting the initial result set.
<!-- lint-disable-next-line line-length -->
[*account.the-timeout-starts-after-decoding-and-token-acquisition-and-covers-the-rest]

It bounds the **initial result set only**. A non-streaming query sends
`end` before it expires; a streaming query sends `watch`.
[*account.the-timeout-bounds-only-the-initial-result-set] Past `watch`
the stream is not time-limited, and what bounds it instead is
`MaxStreamingQueries`, `MaxDistinctStreamValues` and backpressure
(§6.6). [*account.a-stream-past-watch-is-not-time-limited]

On expiry eventd cancels the query and sends an error.
<!-- lint-disable-next-line spelling -->
[*account.on-expiry-the-query-is-cancelled-and-an-error-is-sent] Any result
messages already sent are discarded by the client, since no terminal
message arrived (PSPU §3.16).

Cancellation has to reach two kinds of work. SQLite work is interrupted
through `sqlite3_interrupt` or an equivalent progress-handler check —
the same responsiveness problem as canceling an index build (§3.4).
[*account.cancellation-interrupts-in-progress-sqlite-work]
Non-SQL work — MessagePack flattening, the cross-shard merge — checks
the same deadline periodically, because a query can spend most of its
time in neither the database nor the kernel.
[*account.non-sql-query-work-checks-the-deadline-periodically]

Large scans over unindexed fields are the main timeout risk, and the
adaptive indexing system reduces it over time by indexing whatever keeps
being filtered on — which is also why a timeout is a signal worth
watching rather than merely an error to retry.
