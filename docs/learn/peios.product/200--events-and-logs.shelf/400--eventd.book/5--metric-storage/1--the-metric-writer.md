---
title: The Metric Writer
description: One thread reads the metric socket and writes samples —
  processing a record, out-of-order samples, batching, and durability.
---

One thread reads datagrams from the metric socket and writes samples to
the metric store, independent of both the event and log paths.
<!-- lint-disable-next-line line-length -->
[*metricwriter.one-thread-reads-the-metric-socket-and-writes-samples-independently] It has
the same single-thread shape as the log writer, with the same
consequence during a commit (§4.1).

The wire contract is PSPU §3.9 to §3.13. The same thread also writes
eventd's own health samples, which skip step 1 below (§5.7).

## Processing a record

For each valid record:

1. **Authorize the name.** The datagram's KACS token must hold
   `EVENTD_PUBLISH` on the resolved metric-name descriptor (§7.6).
<!-- lint-disable-next-line line-length -->
   [*metricwriter.the-token-must-hold-eventd-publish-on-the-metric-name-descriptor]
2. **Resolve the series** from name and labels — and, for a histogram,
   bucket boundaries — through the in-memory series cache (§5.3).
<!-- lint-disable-next-line line-length -->
   [*metricwriter.the-series-is-resolved-from-name-labels-and-histogram-boundaries] A
   series that does not exist is inserted into `series` with the
   record's type, and the cache is updated.
[*metricwriter.a-missing-series-is-inserted-with-the-records-type-and-cached]
3. **Check the type.** If the record's type differs from the resolved
   series' type, the record is dropped without replying to the producer.
   [*metricwriter.a-type-mismatch-drops-the-record-without-replying]
   The type is set at creation and is immutable; a series never changes
   type. [*metricwriter.a-series-type-is-fixed-at-creation-and-never-changes]
4. **Insert the sample** into `samples` with the resolved `series_id`,
   the timestamp and the value.
[*metricwriter.the-sample-is-inserted-with-its-series-id-timestamp-and-value]
   SQLite assigns `samples.id`, which is
   the deterministic tiebreaker among samples sharing a timestamp
   (§5.2).
   [*metricwriter.sqlite-assigns-the-sample-id-that-breaks-timestamp-ties] A
   histogram's data is encoded as the canonical MessagePack
   sample map and stored in `histogram_data`.
[*metricwriter.histogram-data-is-stored-as-a-canonical-messagepack-sample-map]

The transaction result counts step-3 failures and carries the most recent
conflict's metric name and expected and received types.
<!-- lint-disable-next-line line-length -->
[*metricwriter.the-transaction-result-counts-type-mismatches-and-carries-the-latest-conflict]
The metric thread
adds the count to an in-memory diagnostic total, retains that conflict,
and emits a coalesced standard-error warning at most once per minute.
<!-- lint-disable-next-line line-length -->
[*metricwriter.mismatches-accumulate-in-a-diagnostic-total-and-warn-at-most-once-per-minute]
`SIGQUIT` includes both the total and latest conflict (§8.5).
[*metricwriter.sigquit-reports-the-mismatch-total-and-latest-conflict]

There is still no response to the producer and no event per failure.
[*metricwriter.a-type-mismatch-produces-no-response-and-no-event]
Authentication identifies the producer; it does not make reactions
proportional to its bad input safe. The one-minute global bound is what
keeps the warning from becoming the amplification vector PSPU §3.4
forbids. A valid transaction takes only the existing zero-mismatch branch
and performs no diagnostic allocation or synchronization.
<!-- lint-disable-next-line line-length -->
[*metricwriter.a-mismatch-free-transaction-does-no-diagnostic-allocation-or-synchronization]

## Out-of-order samples

[*metricwriter.a-sample-older-than-stored-samples-is-accepted]

The writer stores a valid sample whose timestamp precedes samples
already held for that series.

Producers batch, clocks step, and sweeps get retried. Refusing late
samples would convert any of those into silent loss, so eventd accepts
them and defines every ordering it performs over `(timestamp, id)`
rather than over insertion order. That makes raw `RATE` and `DELTA`
evaluation deterministic regardless of arrival (§6.2).

## Batching

The same adaptive algorithm as the event and log writers (§2.4), with
the socket receive queue as input.
<!-- lint-disable-next-line line-length -->
[*metricwriter.batching-uses-the-shared-adaptive-algorithm-over-the-socket-receive-queue]
A transaction opens at the first
valid sample and commits when any of these holds:

- no further datagram is immediately available
  [*metricwriter.a-batch-commits-when-no-datagram-is-immediately-available]
- the batch holds `MetricMaxBatchSize` samples
  [*metricwriter.a-batch-commits-at-metricmaxbatchsize-samples]
- `MetricMaxBatchLatencyMs` has elapsed since the first sample entered
  it
<!-- lint-disable-next-line line-length -->
  [*metricwriter.a-batch-commits-when-metricmaxbatchlatencyms-has-elapsed-since-its-first-sample]

A datagram yielding more samples than fit is split across transactions,
the writer committing before continuing with the same datagram.
[*metricwriter.a-datagram-that-overflows-a-batch-is-split-across-transactions]
Neither
cap is ever exceeded. [*metricwriter.neither-batch-cap-is-ever-exceeded]

The defaults (§A) are 5000 samples and 1000 milliseconds — the longest
latency of the three writers.
[*metricwriter.the-batch-defaults-are-5000-samples-and-1000-milliseconds]
Metrics are typically sampled every
fifteen seconds, so a one-second commit window accumulates a whole
sweep's worth without any latency that a dashboard could notice. Under a
burst, where a collection agent submits every core, disk and interface
at once, the size cap is what forces a timely commit.

## Durability

WAL mode with `synchronous=NORMAL`, as the log store (§4.1).
[*metricwriter.metric-writes-use-wal-with-synchronous-normal] Metric loss
on power failure is acceptable, so per-transaction fsync buys nothing.
