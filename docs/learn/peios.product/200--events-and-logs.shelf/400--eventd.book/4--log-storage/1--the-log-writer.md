---
title: The Log Writer
description: One thread reads the log socket and writes the store, independent
  of the event path — batching, durability and what it adds to a record.
---

One thread reads datagrams from the log socket and writes log records to
the log store. It is independent of the event drain and writer threads,
so log ingestion never contends with event ingestion.
<!-- lint-disable-next-line line-length -->
[*logwriter.the-log-thread-is-independent-of-the-event-drain-and-writer-threads]

The wire contract — socket type, datagram ceiling, record format, and
exactly which malformations cost what — is PSPU §3.6 to §3.8. What
follows is what eventd does with a record once it has one.

## One thread does both jobs

[*logwriter.one-thread-does-both-the-socket-reads-and-the-sqlite-writes]

The log thread performs both the socket reads and the SQLite writes.

The consequence is direct: **during a batch commit the socket is not
being drained**, and datagrams arriving in that window occupy the
receive queue until it fills.
[*logwriter.the-socket-is-not-drained-during-a-batch-commit]
A Unix datagram socket's receive queue is bounded in datagrams, by the
kernel's `net.unix.max_dgram_qlen`, and it is the whole cushion. Once it
is full the kernel refuses further sends rather than accepting and
discarding them: a non-blocking sender receives `EAGAIN`, and a blocking
one waits until the queue drains.
<!-- lint-disable-next-line line-length -->
[*logwriter.a-full-receive-queue-of-max-dgram-qlen-datagrams-refuses-further-sends]
eventd sets `SO_RCVBUF` to four times the datagram ceiling,
[*logwriter.so-rcvbuf-is-sized-at-four-times-the-datagram-ceiling]
but for this socket type the kernel charges a queued datagram to its
sender's send buffer, and the receive buffer bounds nothing.

> [!NOTE]
> Linux's default `net.unix.max_dgram_qlen` is ten, and the kernel
> refuses a send only once more than that many are queued, so the queue
> holds eleven datagrams. A batch commit takes one to ten milliseconds,
> and those eleven are the only thing absorbing arrivals in the window. A
service dumping a stack trace
> through non-blocking sends will have datagrams refused, which is the
> intended degradation for a loss-tolerant path (PSPU §3.4): whatever is
> lost is lost in the sender, and eventd neither buffers beyond the
> queue nor learns of the refusal.

Splitting into a reader and a writer with a bounded handoff — the shape
the event path uses (§2.3) — would decouple them. It is not done, and
the reasoning is that log loss is tolerable by design (PSPU §3.4), log
volume is normally well below event volume, and the single-thread model
avoids a handoff channel and its backpressure semantics entirely.

Where log throughput does become the constraint, sharding the log store
the way the event store is sharded is the larger lever; splitting the
thread only moves the stall.

## Batching

The writer batches on the same adaptive principle as the event writer
(§2.4), with the socket receive queue as its input. A transaction opens
when the first valid record is available and commits when any of these
holds:
<!-- lint-disable-next-line line-length -->
[*logwriter.a-transaction-opens-at-the-first-valid-record-and-commits-on-any-of-three-conditions]

- no further datagram is immediately available in the receive queue
[*logwriter.a-batch-commits-when-no-further-datagram-is-immediately-available]
- the batch holds `LogMaxBatchSize` records
  [*logwriter.a-batch-commits-when-it-holds-logmaxbatchsize-records]
- `LogMaxBatchLatencyMs` has elapsed since the first record entered it
<!-- lint-disable-next-line line-length -->
  [*logwriter.a-batch-commits-when-logmaxbatchlatencyms-has-elapsed-since-its-first-record]

If a datagram yields more valid records than fit in the remaining space,
the writer commits, then continues with the same datagram in a new
transaction.
<!-- lint-disable-next-line line-length -->
[*logwriter.a-datagram-that-overflows-the-batch-continues-in-a-new-transaction]
A transaction never exceeds the size cap and never stays
open past the latency cap — a batched datagram cannot smuggle a larger
transaction past either.
<!-- lint-disable-next-line line-length -->
[*logwriter.a-transaction-never-exceeds-the-size-cap-or-outlives-the-latency-cap]

The defaults (§A) are 5000 records and 500 milliseconds.
[*logwriter.the-batch-defaults-are-5000-records-and-500-milliseconds]
The latency is
five times the event writer's, because log loss on power failure is
acceptable where event loss is not, and larger, less frequent
transactions are more efficient at the moderate volumes logs normally
run at.

The writer keeps the known origin names in memory.
[*logwriter.the-writer-keeps-the-known-origin-names-in-memory]
Before inserting the
first log row for an origin not in that set, it executes
`INSERT OR IGNORE` into `log_origins` in the same transaction (§4.2).
<!-- lint-disable-next-line line-length -->
[*logwriter.a-new-origin-is-inserted-into-log-origins-in-the-same-transaction-as-its-first-row]

A transaction-local pending set ensures that insert runs only once per
new origin in a batch.
[*logwriter.the-origin-insert-runs-once-per-new-origin-per-batch]
The main in-memory set is updated only after
commit and the pending set is discarded on rollback.
<!-- lint-disable-next-line line-length -->
[*logwriter.the-origin-cache-is-updated-only-after-commit-and-pending-origins-are-discarded-on-rollback]
Thus discovery
never needs to scan the hot `logs` table, and a rolled-back first row
cannot make the cache claim that its catalog entry exists.

## Durability

The log store runs in WAL mode with `synchronous=NORMAL`, not FULL.
[*logwriter.the-log-store-runs-in-wal-mode-with-synchronous-normal]

NORMAL syncs at checkpoint time rather than at every commit. It is
durable against process crashes — the write-ahead log survives — but not
against power loss, where commits since the last checkpoint can be gone.
<!-- lint-disable-next-line line-length -->
[*logwriter.log-commits-survive-a-process-crash-but-not-necessarily-a-power-loss]

This is a deliberate divergence from the event store, and it is the
single clearest expression of the hierarchy the whole daemon is
organized around: events are sacred, logs are not. Paying an fsync per
transaction to protect data whose loss is defined as acceptable would be
paying for nothing.

## Adding to the record

eventd supplies the `boot_id` and, where the producer omitted
`timestamp`, its own clock at receipt.
<!-- lint-disable-next-line line-length -->
[*logwriter.eventd-supplies-the-boot-id-and-a-receipt-timestamp-when-none-was-given]
Everything else is stored as
given — `message` byte for byte (PSPU §3.8).
[*logwriter.everything-else-is-stored-as-given-with-message-byte-for-byte]

## Rejected origins are counted

A record whose `origin` is outside the origin grammar (§4.2) is
discarded where it is parsed, before it can join a batch.
<!-- lint-disable-next-line line-length -->
[*logwriter.a-record-with-an-invalid-origin-is-discarded-before-it-joins-a-batch]
eventd counts
every such discard and keeps the most recent origin, escaped and
truncated to 64 characters; both appear in the diagnostic dump (§8.5) as
`log_ingress` and `last_rejected_log_origin`.
<!-- lint-disable-next-line line-length -->
[*logwriter.discards-are-counted-and-the-last-rejected-origin-is-kept-escaped-and-truncated-to-64-characters]

It also writes one line to standard error on the first discard and at
most one a minute after that.
<!-- lint-disable-next-line line-length -->
[*logwriter.a-discard-is-reported-on-stderr-at-the-first-occurrence-then-at-most-once-a-minute]
peinit captures eventd's own standard
error, so that line is stored as an ordinary log record under the origin
`eventd` and is queryable with everything else — and it still reaches
the console directly when the log store is the broken thing.
<!-- lint-disable-next-line line-length -->
[*logwriter.the-stderr-report-is-stored-as-a-log-record-under-the-origin-eventd]

The rate limit is what makes the report usable. A producer whose
vocabulary has drifted from this one usually uses the same origin for
every line it sends, so an unlimited report would turn a silent total
loss into a flood; the count is the quantity that matters and the text
is there to name the vocabulary that drifted.
