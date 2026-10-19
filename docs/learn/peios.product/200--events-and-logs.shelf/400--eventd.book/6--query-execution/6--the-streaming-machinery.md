---
title: The Streaming Machinery
description: How a live watch is implemented — commit generations, latency,
  the DISTINCT seen set, cross-type re-evaluation and backpressure.
---

The externally visible behavior of a streaming query — what can be
streamed, what applies during the watch phase, how `DISTINCT` streams
behave, and when a slow client is dropped — is PSPU §3.27. This is the
machinery underneath.

## Commit generations

eventd keeps a monotonic `u64` commit generation counter for each
streamable store: one for the event store **as a whole**, and one for
the log store.
<!-- lint-disable-next-line line-length -->
[*stream.there-is-one-commit-generation-counter-for-the-event-store-and-one-for-the-log-store]
Metric queries do not stream, so the metric store has none.
[*stream.the-metric-store-has-no-commit-generation-counter]

After a writer commits a batch it increments the counter for its store
and wakes the streaming handlers waiting on it.
<!-- lint-disable-next-line line-length -->
[*stream.a-writer-increments-its-stores-generation-and-wakes-handlers-after-each-commit]
A handler records the last generation it processed and waits until the
counter exceeds it.
[*stream.a-handler-waits-until-the-generation-exceeds-its-last-processed-one]

The event counter covers the whole store rather than one per shard.
Several writer threads increment it, so a wake is "something committed
somewhere" and a handler re-examines every shard it cares about — which
is what it would have to do anyway, since a shard means nothing to the
query path (§6.4).
[*stream.an-event-store-wake-makes-a-handler-re-examine-every-shard]

The counter is process-local and never persisted; it has no meaning
across a restart, and a streaming query does not survive one.
<!-- lint-disable-next-line line-length -->
[*stream.the-generation-is-never-persisted-and-a-stream-does-not-survive-a-restart]
On wraparound the next increment is treated as a wake for every handler
and operation continues.
[*stream.generation-wraparound-is-treated-as-a-wake-for-every-handler]
At any commit rate a machine can sustain,
wraparound of a 64-bit counter is not reachable.

## Record order within a watch batch

The initial result set uses the query's usual sorting and pagination.
During the watch phase, newly visible event records are delivered by
shard ordinal ascending, then database record ID ascending; log records
are delivered by database record ID ascending. `SORT`, `SKIP`, and `TAKE`
do not reorder or trim these watch records.
[*stream.watch-records-follow-store-order]

`DISTINCT` watches consider values in this same order. The first unseen
value is emitted using its first record's representation; equivalent
later values are suppressed by the stream's seen set. The initial
distinct result retains its usual value ordering.

Record IDs preserve insertion order within each store. Event shards
commit independently, so there is no durable total chronological commit
order across shards. Shard ordinal supplies a deterministic order within
each observed batch; successive batches follow the stream cursor. A batch
can include several commits observed together. Producer timestamps do
not change delivery order, even when they arrive out of order.

## Latency

Delivery latency is bounded below by the commit interval of the store
concerned, because a record is not streamable until it is committed.
[*stream.a-record-is-not-streamed-until-it-is-committed]

| Store | Approximate floor | From |
|---|---|---|
| Events | `MaxBatchLatencyMs`, default 100 ms | §2.4 |
| Logs | `LogMaxBatchLatencyMs`, default 500 ms | §4.1 |

[*stream.the-event-stream-latency-floor-is-max-batch-latency-ms]
[*stream.the-log-stream-latency-floor-is-log-max-batch-latency-ms]

Under light load the actual latency is lower, because the adaptive
batcher commits as soon as its input drains rather than waiting out the
cap (§2.4). Under sustained load it converges on the cap.

A consumer needing better than this is not served by eventd at all: the
KMES ring buffers are the low-latency path, they are specified in PSPK,
and attaching to them directly costs the per-event access control that
eventd exists to apply (§7).

## The DISTINCT seen set

A `DISTINCT` stream holds a per-query set of the values it has already
emitted, initialized from the initial result set and added to as new
values appear (PSPU §3.27).
[*stream.a-distinct-streams-seen-set-starts-from-the-initial-result-set]

It is bounded by `MaxDistinctStreamValues` (§A), and exceeding the bound
terminates the query with an error rather than evicting.
<!-- lint-disable-next-line line-length -->
[*stream.exceeding-max-distinct-stream-values-terminates-the-query-without-eviction]
Eviction would
make the output wrong rather than merely truncated: a forgotten value
would be re-emitted as newly seen, and "newly seen" is the entire
meaning of the result.

The set is per query and in memory, which is why the bound exists and
why it is separate from the general query concurrency limit — sixty-four
streams each holding a hundred thousand values is a different memory
profile from sixty-four ordinary queries.

## Cross-type re-evaluation

Pre-computed cross-type ranges describe the past and are discarded when
the watch phase begins (PSPU §3.27).
[*stream.precomputed-cross-type-ranges-are-discarded-when-the-watch-begins]

A **metric** condition costs one index seek per batch: the selector has
already been constrained to exactly one series, so finding the active
sample at the batch's latest candidate timestamp is a single lookup on
`idx_samples_series_timestamp` (§5.2).
<!-- lint-disable-next-line line-length -->
[*stream.a-metric-condition-is-evaluated-once-per-batch-at-its-latest-candidate-timestamp]

An **existence** condition is evaluated per candidate record rather than
per batch, because the centered window is relative to each record's own
timestamp and a matching record can be near some of a batch and not the
rest. [*stream.an-existence-condition-is-evaluated-per-candidate-record]

The per-batch metric evaluation is an approximation, and the reason it
is acceptable is the ratio between the two intervals: a commit batch
spans a fraction of a second and a metric sample fifteen, so every
record in a batch normally maps to the same sample. At sub-second metric
resolution it filters more coarsely, and records near a threshold
crossing are included or excluded as a group.
[*stream.records-in-one-batch-pass-or-fail-a-metric-condition-together]

## Backpressure

Backpressure is detected on the socket send buffer.
[*stream.backpressure-is-detected-on-the-socket-send-buffer] When a
result message cannot be sent because the buffer is full, the query is
terminated immediately; eventd never blocks on the send.
[*stream.a-full-send-buffer-terminates-the-query-and-eventd-never-blocks]

Blocking would put a slow reader in the path of eventd's own work, and
the write path is what would suffer. A streaming client is the
lowest-priority consumer of eventd's time, and dropping it is the same
principle as dropping an ingestion datagram (PSPU §3.4), applied on the
way out.

## Commit notifications

The commit notification counter changes while holding the same mutex used
by a waiting query. This prevents a commit from falling between a query's
last counter check and its condition-variable wait. Queries still inspect
committed storage after waking; a notification does not carry a result.
