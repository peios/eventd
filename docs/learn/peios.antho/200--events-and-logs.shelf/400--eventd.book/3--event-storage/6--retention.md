---
title: Retention
description: Bounding disk growth on two axes, age and size, with both
  enforced — how the pass runs and how space is reclaimed.
---

Retention bounds disk growth. eventd deletes on two axes, age and size,
and enforces both — an event goes when it exceeds either threshold.
<!-- lint-disable-next-line line-length -->
[*eventretain.an-event-is-deleted-when-it-exceeds-either-the-age-or-size-threshold]

The retention model is deliberately minimal, and a later one is expected to
support rules resembling queries: retain KACS events for ninety days,
synthetic events for seven, userspace-origin events for fourteen; and to
prune during ingestion rather than only in arrears. What is here is the
least that prevents unbounded growth.

## Age

Rows are deleted from `events` where `timestamp` is older than
`EventRetentionDays` (§A) from the current wall clock, until none
remain.
<!-- lint-disable-next-line line-length -->
[*eventretain.events-older-than-eventretentiondays-by-the-wall-clock-are-deleted]
Each shard is processed independently, and the rule covers KMES
events, synthetic events and gap records alike.
<!-- lint-disable-next-line line-length -->
[*eventretain.age-retention-runs-per-shard-and-covers-kmes-synthetic-and-gap-records-alike]

## Size

Size is measured as **logical live size**, not file size:

```text
logical_live_bytes = (page_count - freelist_count) * page_size
```

from `PRAGMA page_count`, `PRAGMA freelist_count` and
`PRAGMA page_size`, taken after attempting a passive WAL checkpoint.
<!-- lint-disable-next-line line-length -->
[*eventretain.size-is-logical-live-size-measured-after-a-passive-checkpoint-attempt]
The event store's total is the sum across every shard.
[*eventretain.the-event-store-size-is-the-sum-across-every-shard]

Pages freed by retention do not count, because they are reusable by
future inserts.
[*eventretain.pages-freed-by-retention-do-not-count-toward-size]
Counting them would make retention chase its own tail:
each deletion would free pages that still counted against the limit,
prompting more deletion.

When `EventRetentionMaxBytes` is non-zero and the total exceeds it:
<!-- lint-disable-next-line line-length -->
[*eventretain.size-retention-runs-only-when-eventretentionmaxbytes-is-non-zero-and-exceeded]

1. Identify every non-current boot ID present in the shards.
<!-- lint-disable-next-line line-length -->
   [*eventretain.size-retention-considers-every-non-current-boot-id-in-the-shards]
2. Order them by their newest event timestamp, oldest boot first.
<!-- lint-disable-next-line line-length -->
   [*eventretain.non-current-boots-are-ordered-by-newest-event-timestamp-oldest-first]
3. Delete each of those boots entirely, across all shards, one boot at a
   time, until the total is within the limit or no non-current boots
   remain.
<!-- lint-disable-next-line line-length -->
   [*eventretain.whole-non-current-boots-are-deleted-one-at-a-time-across-all-shards]
4. If still over, delete the oldest events of the **current** boot by
   timestamp, across all shards, until within the limit.
   [*eventretain.only-then-are-the-current-boots-oldest-events-deleted]

Size pressure prefers boot boundaries. Deleting a whole old boot removes
a self-contained unit — its sequence numbers, its gap records and its
startup event go together — and it preserves recent events across the
boundary, which is what an operator investigating a reboot needs. Only
when whole boots are exhausted does eventd start on the current one.

## Running it

A background retention coordinator processes the event store first,
then the log store (§4.4), then the metric store (§5.5).
[*eventretain.the-coordinator-processes-events-then-logs-then-metrics]
Each store is processed whether or not the one before it failed, and the
pass reports every store that did.
[*eventretain.a-store-whose-retention-fails-does-not-stop-the-others]
The interval is
`RetentionCheckIntervalMinutes` (§A); an applied configuration change
(§8.3) and a write refused for want of space (§9.2) each request a pass
at once.
[*eventretain.the-coordinator-runs-every-retentioncheckintervalminutes]
After a failed pass, requests are held for one second, doubling to a
minute while passes keep failing, so a full disk does not drive
retention into a tight retry loop.
[*eventretain.after-a-failed-pass-requested-passes-back-off]
It uses read-only connections to
measure and plan. The only read-write connections it owns are to
historical shards, which have no ingestion writer: it opens each one
read-write for the life of the process and deletes from it and
checkpoints it directly, as that shard's one writer.
<!-- lint-disable-next-line line-length -->
[*eventretain.the-coordinator-measures-read-only-and-owns-read-write-connections-only-to-historical-shards]

Every other database has exactly one read-write connection, owned by its
ingestion writer.
<!-- lint-disable-next-line line-length -->
[*eventretain.every-database-but-a-historical-shard-has-one-read-write-connection-owned-by-its-ingestion-writer]
The coordinator submits low-priority maintenance
commands to that owner.
<!-- lint-disable-next-line line-length -->
[*eventretain.the-coordinator-submits-low-priority-commands-to-the-owning-writer]
A command runs only at a transaction boundary,
deletes at most `RetentionDeleteBatchRows`, and yields to ingestion
before another command runs.
<!-- lint-disable-next-line line-length -->
[*eventretain.a-command-runs-at-a-transaction-boundary-deletes-at-most-retentiondeletebatchrows-and-yields]
Under urgent size pressure the owner can
append one bounded deletion to an ingestion transaction it already has
open, avoiding an additional commit and fsync.
<!-- lint-disable-next-line line-length words -->
[*eventretain.under-urgent-size-pressure-one-bounded-delete-may-join-an-open-ingestion-transaction]

The passive checkpoint attempted before a size measurement is also a
writer-owned command.
[*eventretain.the-pre-measurement-checkpoint-is-a-writer-owned-command]
The coordinator requests it, then reads the page
and freelist counts through its read-only connection.
<!-- lint-disable-next-line line-length -->
[*eventretain.the-coordinator-requests-the-checkpoint-then-reads-page-counts-read-only]
It never tries to
checkpoint through that connection or temporarily promotes it.
<!-- lint-disable-next-line line-length -->
[*eventretain.the-coordinator-never-checkpoints-through-or-promotes-its-read-only-connection]

There is no retention writer mutex and no second SQLite writer.
[*eventretain.there-is-no-retention-writer-mutex-and-no-second-writer]
A single unbatched `DELETE` over a month of events could otherwise hold a
write transaction for as long as it took, blocking the writer and,
behind it, the drain thread and KMES ring.

## Reclamation

Deleting rows does not shrink a SQLite file. Freed pages are reused by
later inserts, and reclaiming filesystem space needs `VACUUM`, which
rewrites the whole database.

eventd never runs `VACUUM` automatically.
[*eventretain.eventd-never-runs-vacuum-automatically]
Reclamation is an explicit
administrative operation.
[*eventretain.reclamation-is-an-explicit-administrative-operation]

In steady state it is not needed. Where ingestion and retention run at
comparable rates, the file settles at roughly the high-water mark of
retained data and the freed pages are recycled without further growth —
which is also why logical live size, rather than file size, is the right
thing to measure.
