---
title: Health Metrics
description: The eventd.* metrics eventd records about itself — what each
  counts, how they are written, and what is deliberately left out.
---

eventd records its own health as metrics named `eventd.*`. They answer
the questions an operator asks of a running collector — is it keeping
up, is it losing anything, how big are the stores, are queries being
turned away — through the ordinary query channel, so they can be charted
and compared over time like any other metric.

## How they are written

Every `HealthMetricIntervalSeconds` (§A), the metric thread takes one
sample of every series below and commits it to the metric store.
[*health.the-metric-thread-samples-every-health-series-each-interval]
The samples do not travel through the metric socket: there is no
datagram, no KACS token, and no `EVENTD_PUBLISH` check (§7.6), in the
same way synthetic events never touch KMES (§2.6).
[*health.health-samples-bypass-the-metric-socket-and-publication-check]
An interval of 0 turns them off.
[*health.a-zero-interval-turns-health-metrics-off]

Health samples obey `MetricMaxBatchSize` (§5.1). They fill any pending
batch up to the size cap captured when it opened; further transactions
use the current configured cap (§8.3). The final partial batch is
committed immediately. Splitting an interval's samples does not drop or
reorder them.
[*health.health-samples-respect-the-metric-transaction-size-cap]

Each sample carries eventd's clock and the current boot ID, like a
metric record that omits its timestamp (PSPU §3.11).

The counters count from eventd's start, and so restart from zero
whenever eventd does. A reader asks for their `RATE` or `DELTA`, which
read a decrease as a restart (PSPU §3.10, §3.25).
[*health.health-counters-restart-from-zero-with-eventd]

## The series

| Name | Type | Labels | What it is |
|---|---|---|---|
| `eventd.events.stored` | counter | `shard` | Events committed to each active shard. |
| `eventd.events.lost` | counter | `cpu` | Sequences recorded as lost on each CPU, whether KMES overwrote them or a full store refused their batch (§2.5, §9.1). The sum of the ranges in the `eventd.events.lost` records eventd committed; the metric and the event type share the name. |
| `eventd.kmes.ring.fill.percent` | gauge | `cpu` | How full each CPU's KMES ring buffer is, as its drain last saw it. |
| `eventd.events.index.sheds` | counter | `reason` | Event indexes dropped under write pressure: `pressure` one at a time, `emergency` all at once (§3.4). |
| `eventd.logs.stored` | counter | None | Log records committed. |
| `eventd.metrics.stored` | counter | None | Metric samples committed, these included. |
| `eventd.metrics.series.cached` | gauge | None | Entries in the series cache (§5.3). |
| `eventd.store.bytes` | gauge | `store` | Bytes each store occupies on disk, its write-ahead logs included: `events` (every active and historical shard), `logs`, `metrics` and `metadata`. |
| `eventd.store.write.errors` | counter | `store` | Failed writes to each store, the ones the diagnostic dump names the latest of (§8.5). |
| `eventd.retention.deleted` | counter | `store` | Rows retention removed from each store (§3.6, §4.4, §5.5). |
| `eventd.queries.active` | gauge | None | Queries running, streaming or not. |
| `eventd.queries.streaming` | gauge | None | Streaming queries running. |
| `eventd.queries.refused` | counter | `reason` | Queries turned away by `MaxConcurrentQueries` (`machine`), `MaxStreamingQueries` (`streaming`) or `MaxQueriesPerUser` (`user`) (§6.5). |
| `eventd.queries.failed` | counter | `reason` | Queries ended by `QueryTimeoutMs` (`timeout`) or `MaxQueryHeldBytes` (`held_bytes`) (§6.5). |

[*health.events-stored-counts-committed-events-per-shard]
[*health.events-lost-counts-the-sequences-committed-gap-records-name]
[*health.ring-fill-is-each-drains-last-observed-ring-occupancy]
[*health.index-sheds-count-indexes-dropped-by-reason]
[*health.store-bytes-is-bytes-on-disk-with-write-ahead-logs]
[*health.queries-refused-counts-each-slot-limit-separately]

`eventd.store.bytes` is the space on disk, which is what fills a volume.
Size retention compares a store's logical live size instead, which
leaves out free pages and the write-ahead log (§3.6), so the two can
differ.

## Who can read and publish them

They are read like any metric, through the `Metrics` descriptors (§7.2).
eventd creates `Metrics\eventd` on first boot, granting `EVENTD_READ` to
SYSTEM, Administrators and Authenticated Users and `EVENTD_PUBLISH` to
nobody.
[*health.the-default-eventd-metrics-descriptor-grants-read-and-no-publish]
Without it the wildcard's grant would let an administrator's
process send samples under `eventd.*` through the socket and pass them
off as eventd's. An administrator who wants them narrower edits that
descriptor.

## What is left out

Rejected ingestion input is not counted here: log records with an
invalid origin, and metric datagrams without a token, truncated, denied
by name policy or carrying the wrong type. Counting that input where a
query client can see it is exactly what PSPU §3.4 and §3.12 forbid. Those
counts stay in memory and reach only the diagnostic dump (§8.5).
[*health.rejected-ingestion-input-is-not-among-the-health-metrics]

Neither are datagrams the kernel refuses at a full socket queue. eventd
is never told about them (§9.1), so it has nothing to count.
[*health.kernel-socket-drops-are-not-among-the-health-metrics]
