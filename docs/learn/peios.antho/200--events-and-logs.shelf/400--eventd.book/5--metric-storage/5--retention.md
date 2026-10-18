---
title: Retention
description: The metric retention pass, its long default, rollup invalidation,
  and how space is reclaimed.
---

Metric retention runs on the same background thread as the other two,
after the log store (§3.6, §4.4).
<!-- lint-disable-next-line line-length -->
[*metricretain.metric-retention-runs-on-the-shared-retention-thread-after-the-log-store]
Both limits are enforced and the more
aggressive wins.
[*metricretain.both-limits-are-enforced-and-the-more-aggressive-wins]

## The pass

1. Delete rows from `samples` older than `MetricRetentionDays` (§A)
   until none remain.
   [*metricretain.samples-older-than-metricretentiondays-are-deleted]
2. If `MetricRetentionMaxBytes` is non-zero and the store's logical live
   size exceeds it, delete the oldest samples by timestamp until it is
   within the limit.
<!-- lint-disable-next-line line-length -->
   [*metricretain.over-the-byte-limit-the-oldest-samples-are-deleted-until-within-it]
   The mainline default is 1073741824 bytes (1 GiB);
   zero is an explicit opt-out rather than the default.
   [*metricretain.the-byte-limit-defaults-to-1-gib-and-zero-disables-it]
3. Track every `series_id` whose samples were deleted by either step.
   [*metricretain.every-series-that-lost-samples-is-tracked]
4. Before deleting a sample batch, delete all adaptive rollups belonging to
  any
   affected series (§5.6).
<!-- lint-disable-next-line line-length -->
   [*metricretain.an-affected-series-loses-its-rollups-before-its-samples-are-deleted]
5. Delete every `series` row with no remaining samples.
   [*metricretain.series-with-no-remaining-samples-are-deleted]

Logical live size is the same measure as §3.6.
[*metricretain.the-byte-limit-measures-logical-live-size-as-for-events]

Step 5 removes the definitions of series nobody produces any more, which
is the only mechanism that ever removes a `series` row.
[*metricretain.retention-is-the-only-mechanism-that-removes-a-series-row]
A series that
stopped receiving samples persists until its last sample is removed.
[*metricretain.an-idle-series-persists-until-its-last-sample-is-removed]
In
the absence of size pressure that is ninety days by default; the byte
ceiling can remove it earlier.

## The longest default

Ninety days, against fourteen for logs and thirty for events (§A).
[*metricretain.metric-retention-defaults-to-ninety-days]

A metric sample is small and its value grows with age: a year of CPU
utilization is a capacity-planning input in a way that a year of log
lines is not. A thousand series sampled every fifteen seconds produce
about 5.7 million samples a day, which is a few hundred megabytes in
SQLite.

The long age limit therefore does not make storage unbounded. The 1 GiB
size default is the safety boundary for a publisher that continually
creates new label combinations: eventd still accepts new series, as
PSPU requires, but retires the oldest metric data rather than allowing
one metric namespace to consume the shared eventd volume.
<!-- lint-disable-next-line line-length -->
[*metricretain.size-pressure-retires-old-samples-rather-than-refusing-new-series]
Installations
with a dedicated, externally bounded metric volume can set zero
deliberately.

The size decision is made by the retention coordinator, never while a
datagram is parsed or a sample is committed.
[*metricretain.the-size-decision-is-made-only-by-the-retention-coordinator]
Normal metric persistence
therefore pays no size-check cost.

## Rollups are not downsampling

Retention deletes raw samples; it does not replace them with lower-resolution
data. [*metricretain.retention-deletes-samples-without-downsampling]
Adaptive rollups accelerate repeated queries while their raw sources
remain present and valid. They are invalidated before retention removes any
source from their series, so they cannot extend the apparent lifetime of data
or answer a query after its authoritative samples have expired (§5.6).
[*metricretain.rollups-never-answer-for-expired-samples]

A future retention engine can aggregate high-resolution data as it ages —
per-second samples becoming five-minute aggregates after a week, hourly
aggregates after a month — but that would require a separate versioned storage
and query contract.

## Batching and reclamation

The retention coordinator plans with a read-only connection and submits
low-priority delete commands to the metric writer.
<!-- lint-disable-next-line line-length -->
[*metricretain.retention-plans-read-only-and-submits-low-priority-deletes-to-the-writer]
Each writer-owned
transaction deletes at most `RetentionDeleteBatchRows`, and ingestion is
rechecked before the next command.
[*metricretain.each-delete-transaction-is-capped-at-retentiondeletebatchrows]
Under urgent size pressure the writer
can append one bounded retention delete to a transaction it already has
open; retention never takes a writer mutex or opens another read-write
connection (§3.6).
<!-- lint-disable-next-line line-length words -->
[*metricretain.urgent-size-pressure-may-add-one-bounded-delete-to-an-open-transaction]

`VACUUM` is never run automatically.
[*metricretain.vacuum-is-never-run-automatically]
Freed pages are recycled and are
excluded from the size measure.
[*metricretain.freed-pages-are-excluded-from-the-size-measure]
