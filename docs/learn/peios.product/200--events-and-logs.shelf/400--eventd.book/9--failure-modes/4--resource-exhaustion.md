---
title: Resource Exhaustion
description: Memory, query slots, descriptors, and writer stalls — how each
  runs out, and what eventd does when it does.
---

## Memory

If the out-of-memory killer takes eventd, it is treated exactly as a
crash (§8.5): the databases are consistent, the ring buffers are
untouched, peinit restarts the daemon, and the gap is recorded.
[*exhaust.an-oom-kill-is-recovered-exactly-like-a-crash]

The principal long-lived memory consumers are:

| Consumer | Proportional to | Bounded by |
|---|---|---|
| Series cache | distinct metric series held in memory | `MetricSeriesCacheSize` (§5.3) |
| Event-type and log-origin intern sets | distinct committed identifiers not yet cleaned from active catalogs | catalog cardinality; no independent memory setting (§3.1, §4.2) |
| Adaptive index counters | distinct event fields queried | the event query surface |
| SQLite page caches | connections and active indexes | per-connection configuration (§C) |
| Event handoff channels | occupied slots and copied event bytes | startup-fixed internal slot and byte limits (§2.3) |

[*exhaust.series-cache-memory-is-bounded-by-metricseriescachesize]
<!-- lint-disable-next-line line-length spelling -->
[*exhaust.intern-set-memory-has-no-independent-bound-beyond-catalogue-cardinality]
[*exhaust.adaptive-index-counter-memory-is-bounded-by-the-event-query-surface]
[*exhaust.sqlite-page-cache-memory-is-bounded-per-connection]
<!-- lint-disable-next-line line-length -->
[*exhaust.handoff-channel-memory-is-bounded-by-startup-fixed-slot-and-byte-limits]

The handoff limits do not follow live `MaxBatchSize` changes.
[*exhaust.handoff-limits-do-not-follow-live-maxbatchsize-changes] The
channels are small scheduling handoffs; KMES remains the real event
backlog.

High-cardinality producers are the likely surprise. The series cache
cannot grow past its limit, but the `series` table can grow and make it
thrash on the thread that also drains the metric socket (§5.3).
Identifier intern sets intentionally trade memory for zero catalog work
on known-name ingestion. The default 1 GiB metric retention ceiling and
low-priority orphan cleanup bound persistent growth indirectly, but do
not bound the number of active series between retention passes.

## Query slots

Reaching `MaxConcurrentQueries`, `MaxStreamingQueries`, or
`MaxQueriesPerUser` rejects new queries with an error rather than
queueing them (§6.5).
[*exhaust.a-query-beyond-the-concurrency-limits-is-rejected-not-queued]
The per-user bound keeps one caller from occupying every slot; several
callers together, or SYSTEM, still can.
[*exhaust.one-user-cannot-occupy-every-query-slot]

Ingestion is unaffected, because queries and ingestion are separate
channels and separate threads.
<!-- lint-disable-next-line line-length -->
[*exhaust.query-slot-exhaustion-does-not-affect-ingestion] That separation is what makes query-side
exhaustion an inconvenience rather than data loss.

## Descriptors

An event query opens a read-only connection per shard database, and each
connection holds one or two file descriptors (§6.4). Many historical
shards multiplied by many concurrent queries reaches a process limit
faster than anything else eventd does.

Where eventd cannot allocate what an admitted query needs, it fails that
query rather than blocking a writer or exceeding its own limit (§6.1).
<!-- lint-disable-next-line line-length -->
[*exhaust.a-query-whose-resources-cannot-be-allocated-fails-without-blocking-a-writer]

## Writer stalls

Two things stall a writer thread briefly, and both are bounded by
design.

**An index build in progress.** Drain threads detect rising write
pressure and signal cancellation; the writer aborts the `CREATE INDEX`,
SQLite rolls back the partial index, and event writing resumes
immediately (§3.4).
<!-- lint-disable-next-line line-length -->
[*exhaust.rising-write-pressure-cancels-an-index-build-and-event-writing-resumes]

**A writer-owned maintenance command.** Retention and adaptive-index
maintenance execute only as bounded, low-priority commands at
transaction boundaries.
<!-- lint-disable-next-line line-length -->
[*exhaust.maintenance-runs-only-as-bounded-low-priority-commands-at-transaction-boundaries]
Ingestion wins before the next command; there
is no competing read-write connection or writer mutex (§3.6).
[*exhaust.ingestion-takes-priority-over-the-next-maintenance-command]

Neither stall loses anything by itself. Both contribute to ring buffer
pressure, and prolonged enough, both end at §9.1.
