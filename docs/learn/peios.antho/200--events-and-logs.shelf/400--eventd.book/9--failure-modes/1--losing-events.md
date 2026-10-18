---
title: Losing Events
description: The one failure that costs something unrecoverable, and why the
  whole ingestion pipeline is shaped around delaying it.
---

Every other failure in this chapter costs something recoverable. This
one does not, which is why the whole ingestion pipeline is shaped around
delaying it.

## Ring buffer overrun

When events are emitted faster than eventd drains them, the per-CPU ring
buffers fill and KMES overwrites its oldest entries.

- eventd sees it as a sequence gap on the affected CPU (§2.5).
  [*lostevents.an-overrun-is-detected-as-a-sequence-gap-on-the-affected-cpu]
- An `eventd.events.lost` record is written, naming the missing range.
<!-- lint-disable-next-line line-length -->
  [*lostevents.an-overrun-writes-an-eventd-events-lost-record-naming-the-missing-range]
- The range is added to that CPU's `eventd.events.lost` (§5.7).
- Draining resumes from the oldest survivor at `tail_pos`.
<!-- lint-disable-next-line line-length -->
  [*lostevents.after-an-overrun-draining-resumes-from-the-oldest-survivor-at-tail-pos]

The events are gone. No other copy exists, and a gap record is a
tombstone rather than a recovery — it records what was lost and when,
which is the most that can be offered.

Four mechanisms delay it:

| Mechanism | Effect |
|---|---|
| Adaptive batch sizing (§2.4) | Commits as often as throughput allows, so the writer stays close to the drain rate. |
| Index shedding (§3.4) | Drops per-insert index cost under pressure, including all of it at once in the emergency case. |
| Sharding (§2.3) | Scales write throughput with the shard count. |
| Ring buffer capacity | The absorption window, sized by an administrator. |

The first three are eventd's and operate automatically. The fourth is
KMES's and is the one an operator can enlarge for a workload that bursts
predictably.

## Query timeouts

A query exceeding `QueryTimeoutMs` is canceled and the client receives
an error (§6.5).
<!-- lint-disable-next-line spelling -->
[*lostevents.a-query-exceeding-querytimeoutms-is-cancelled-with-an-error]
Read-only connections are released; nothing is lost,
and the query did not finish.
[*lostevents.a-timed-out-query-releases-its-read-only-connections]

Streaming queries are bounded only up to `watch`; past that the watch
phase is not time-limited.
[*lostevents.the-watch-phase-of-a-streaming-query-is-not-time-limited]

The main risk is a large scan over a field with no index, which adaptive
indexing reduces over time by indexing whatever keeps being filtered on
(§3.4). A timeout is therefore worth reading as a signal about the index
set rather than only as an error to retry.

## Ingestion backpressure

When a log or metric socket's receive queue is full, the kernel refuses
the send: a non-blocking sender receives `EAGAIN`, and a blocking one
waits (§4.1). eventd is not notified and does not count the refusal, so
no health metric shows it (§5.7).
[*lostevents.a-datagram-dropped-on-a-full-receive-queue-is-not-counted]

This is by design and is not a failure to be tuned away (PSPU §3.4). The
one operational note is that the queue is not being drained *while a
batch is committing*, because the same thread does both jobs (§4.1) —
so a burst arriving during a commit is the common case for log loss.
[*lostevents.a-socket-queue-is-not-drained-while-its-batch-commits]
