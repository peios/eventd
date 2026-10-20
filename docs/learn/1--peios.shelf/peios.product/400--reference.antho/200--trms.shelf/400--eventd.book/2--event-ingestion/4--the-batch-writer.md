---
title: The Batch Writer
description: How each writer thread commits — explicit transactions, adaptive
  batch sizing, WAL checkpointing, and prepared statements.
---

## Transactions

Each writer thread writes to its shard with explicit transactions: a
`BEGIN`, one `INSERT` per event, a `COMMIT`.
[*batch.a-batch-is-an-explicit-transaction-with-one-insert-per-event]
The commit is the durability boundary.
[*batch.the-commit-is-the-durability-boundary]

For every maximal contiguous sequence run represented by the event rows
and `eventd.events.lost` rows in that transaction, the writer also inserts a
`receipt_ranges` row for `(boot_id, cpu_id, first_sequence,
last_sequence)` (§3.1).
<!-- lint-disable-next-line line-length -->
[*batch.each-maximal-contiguous-sequence-run-in-a-transaction-gets-a-receipt-row]
Events, gaps, and their receipts commit together: there is no second
durability operation and no global receipt writer.
[*batch.events-gaps-and-their-receipts-commit-in-the-same-transaction]

The database runs in WAL mode with `synchronous = FULL`, so every commit
fsyncs the write-ahead log.
<!-- lint-disable-next-line line-length -->
[*batch.shards-run-in-wal-mode-with-synchronous-full] This is the strictest of the three stores'
settings, and the only one where per-transaction durability is bought at
per-transaction cost — because an event can be an audit record and
losing the last second of them to a power cut is a real loss.

## Adaptive batch sizing

The writer sizes each batch to balance throughput against how much sits
uncommitted at any moment.

**Throughput is always the priority.** If eventd falls behind the
emission rate, ring buffers fill and events are overwritten, which is
irrecoverable; a shorter power-loss window is not worth that trade. The
algorithm maximizes resilience *within* the constraint that throughput
is maintained, never against it.

1. When the first event is available, the writer opens a transaction and
   records the start time.
<!-- lint-disable-next-line line-length -->
   [*batch.the-first-available-event-opens-a-transaction-and-records-the-start-time]
2. It reads available events from its drain threads and inserts them.
3. After each group of inserts it commits if any of these holds:
   - no assigned drain thread  has an event available
[*batch.a-batch-commits-when-no-assigned-drain-thread-has-an-event-available]
   - the batch holds `MaxBatchSize` events
     [*batch.a-batch-commits-when-it-holds-max-batch-size-events]
   - `MaxBatchLatencyMs` has elapsed since the first event entered it
<!-- lint-disable-next-line line-length -->
     [*batch.a-batch-commits-once-max-batch-latency-ms-has-elapsed-since-its-first-event]
4. Otherwise it keeps reading and inserting.
5. With nothing available and an empty batch, it sleeps until a producer
   wakes it. [*batch.an-idle-writer-sleeps-until-a-producer-wakes-it]

The first condition is what makes the algorithm adaptive. Under light
load the input drains immediately, so a batch of three events commits at
once and the exposure window is microseconds. Under sustained load
batches grow until they hit the size cap or the latency cap, whichever
comes first, and the per-commit fsync is amortized across thousands of
rows.

The writer chooses its own insert-group size, subject to a group never
letting a batch exceed the size cap or stay open past the latency cap.
[*batch.an-insert-group-never-takes-a-batch-past-either-cap]

Both bounds are configuration (§A). The defaults are 10000 events and
100 milliseconds — the tightest latency of the three stores, for the
same reason the durability setting is the strictest.
[*batch.the-default-caps-are-10000-events-and-100-milliseconds]

## WAL checkpointing

WAL mode accumulates log data until a checkpoint copies it back into the
main database file. Under sustained writes the log grows.

Each writer triggers a checkpoint when its write-ahead log reaches
`WalCheckpointPages` (§A), in `SQLITE_CHECKPOINT_PASSIVE` mode —
checkpointing as much as it can without blocking readers.
<!-- lint-disable-next-line line-length -->
[*batch.a-passive-checkpoint-is-triggered-when-the-wal-reaches-wal-checkpoint-pages]
If a passive checkpoint cannot make progress because readers hold pages,
the writer does not block: it keeps writing and retries after a later
commit.
<!-- lint-disable-next-line line-length -->
[*batch.a-stalled-checkpoint-never-blocks-the-writer-and-is-retried-after-a-later-commit]

Checkpointing runs on the writer thread and briefly serializes with
insert work, which is inherent to SQLite rather than a choice — a
database cannot be checkpointed and written concurrently.
[*batch.checkpointing-runs-on-the-writer-thread] Passive mode
is the lightest option available, yielding immediately when readers hold
pages, and the per-checkpoint cost is bounded by the threshold.

## Prepared statements

Each writer prepares its `INSERT` once at startup and reuses it for
every row, which keeps SQL parsing and planning off the hot path
entirely. [*batch.the-event-insert-is-prepared-once-at-startup-and-reused]

The receipt insert and new-event-type catalog insert are prepared and
reused too.
<!-- lint-disable-next-line spelling -->
[*batch.the-receipt-and-catalogue-inserts-are-prepared-and-reused] A
known event type causes no catalog SQL operation; the writer's
in-memory intern table identifies only new names, which are
inserted in the same transaction as their first event (§3.1).
<!-- lint-disable-next-line line-length spelling -->
[*batch.only-a-new-event-type-is-catalogued-in-the-transaction-of-its-first-event]

## Maintenance commands

The writer owns the shard's only read-write connection.
[*batch.the-writer-owns-the-shards-only-read-write-connection]
Retention, receipt compaction and index policy code can inspect through
read-only connections, but they submit bounded maintenance commands to
this writer rather than opening another writer or taking a shard mutex.
[*batch.maintenance-reaches-a-shard-only-as-bounded-commands-to-its-writer]

The writer considers low-priority maintenance only at transaction
boundaries and rechecks ingestion pressure after one bounded action.
<!-- lint-disable-next-line line-length -->
[*batch.low-priority-maintenance-runs-at-transaction-boundaries-one-bounded-action-at-a-time]
A size-urgent retention delete can be included in an existing ingestion
transaction so it adds no second commit.
<!-- lint-disable-next-line spelling -->
[*batch.a-size-urgent-retention-delete-may-join-an-ingestion-transaction]
Index creation remains cancellable through SQLite's progress handler
(§3.4). Under sustained pressure maintenance can lag indefinitely;
accepting incoming events is the higher priority.
[*batch.ingestion-takes-priority-over-maintenance-under-sustained-pressure]
