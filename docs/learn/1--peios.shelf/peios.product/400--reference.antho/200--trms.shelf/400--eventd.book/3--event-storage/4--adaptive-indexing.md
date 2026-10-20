---
title: Adaptive Indexing
description: Which secondary indexes are worth their write cost depends on
  what a deployment queries — how eventd decides, converges, and sheds.
---

Secondary indexes make queries fast and writes slow. Which indexes are
worth that trade depends on what a particular deployment actually
queries, which varies between systems and over time, and which nobody
wants to tune by hand.

eventd observes the queries and maintains the indexes they imply —
subject throughout to the rule that **throughput outranks query
latency**.

## Three decoupled parts

**Query frequency counters.** Query handlers increment a per-field
counter when a field appears in a `WHERE` predicate. This is the
write-heavy path — once per query, per predicate.
<!-- lint-disable-next-line line-length -->
[*index.a-field-in-a-where-predicate-increments-its-counter-once-per-query-per-predicate]
Counters live in
memory and are flushed periodically to the metadata database (§3.5),
never to a shard, because they are global state that must survive shard
reconfiguration.
<!-- lint-disable-next-line line-length -->
[*index.counters-are-flushed-periodically-to-the-metadata-database-never-a-shard]

**Index policy.** A periodic process reads the counters, applies the
creation and removal thresholds, and computes the **desired index set**
— an ordered list of fields, highest priority first.
<!-- lint-disable-next-line line-length -->
[*index.the-policy-computes-the-desired-set-as-fields-in-priority-order] It runs every
`AdaptiveIndexPolicyIntervalMinutes` (§A), and also at once on every
applied configuration change (§8.3) and every `INDEX` command, and once
more at shutdown; it is the only writer to the desired set.
<!-- lint-disable-next-line line-length -->
[*index.the-policy-runs-every-policy-interval-and-is-the-only-desired-set-writer]

Each run reads the current thresholds and WAL checkpoint setting after
its wait ends, including the final run on shutdown. An applied
threshold change is therefore judged using the new configuration;
it does not require a second change to take effect. Pending wakes can
coalesce, so this is not a promise of one run per registry update.
[*index.a-policy-run-reads-current-configuration-after-waking]

**Shard convergence.** Writer threads read the desired set and move
their material indexes toward it.
[*index.writer-threads-move-their-material-indexes-toward-the-desired-set]
They never read the counters and never
write the desired set.
[*index.writer-threads-never-read-counters-or-write-the-desired-set]

The separation exists so that the high-frequency counter updates never
contend with the writer threads. The policy is the bridge between them
and runs on the order of once an hour, so it is never a contention
point.

The desired set is **global** — one list applying to every shard.
[*index.the-desired-set-is-one-global-list-for-every-shard]
Individual shards do not make independent decisions; they differ only in
how far they have got.

## Convergence

A shard converges when it is quiet: its writer thread has no pending
events, and no batch within the last `SheddingWindowSeconds` exceeded
75% of `MaxBatchSize`, the same measure that sheds indexes (below).
<!-- lint-disable-next-line line-length -->
[*index.a-shard-is-quiet-with-no-pending-events-and-no-large-batch-in-the-shedding-window]
When it is quiet and its material indexes do not match the desired set,
it takes **one** convergence action — creating the highest-priority
missing index, or dropping the lowest-priority material index outside the
desired set — then rechecks write pressure before considering another. It
does so without waiting for the next policy run.
[*index.an-idle-writer-takes-one-convergence-action-then-rechecks-pressure]

Creation uses `CREATE INDEX IF NOT EXISTS`; removal uses
`DROP INDEX IF EXISTS`.
<!-- lint-disable-next-line line-length -->
[*index.indexes-are-created-if-not-exists-and-dropped-if-exists] Both run on the shard's writer thread, which is
the only thread permitted to write that database (§2.3).
[*index.index-creation-and-removal-run-on-the-shards-writer-thread]

**Index creation is cancellable.** If drain threads detect rising write
pressure during a build, they signal the writer to abort; the writer
cancels the `CREATE INDEX`, SQLite rolls back the partial index cleanly,
and the writer returns to event batches immediately.
<!-- lint-disable-next-line line-length -->
[*index.rising-write-pressure-cancels-an-index-build-and-the-writer-returns-to-batches]
The abandoned build
is retried at the next quiet period.
<!-- lint-disable-next-line spelling -->
[*index.a-cancelled-index-build-is-retried-at-the-next-quiet-period]

Cancellation responsiveness matters more than it looks. `sqlite3_interrupt`
sets a flag checked at SQL VM opcode boundaries, and during B-tree
construction for a large index the gap between checks can be tens of
milliseconds — long enough to overrun a ring buffer at a high event
rate. `sqlite3_progress_handler()`, registering a callback invoked every
thousand opcodes that checks a cancellation flag and returns non-zero to
abort, gives cancellation that tracks the pressure signal rather than
lagging it.
[*index.a-progress-handler-checks-for-cancellation-every-thousand-opcodes]

Shards converge at their own pace. One under sustained pressure can lag
the desired set indefinitely, and that is the correct outcome: it is
prioritizing throughput.
<!-- lint-disable-next-line spelling -->
[*index.a-shard-under-sustained-pressure-may-lag-the-desired-set-indefinitely]

## Shedding

Under sustained pressure a shard drops indexes to cut per-insert cost.

**Graduated shedding.** If more than `SheddingBatchPercent` of a shard's
batches within a `SheddingWindowSeconds` sliding window exceeded 75% of
`MaxBatchSize`, the shard drops its lowest-priority secondary index —
the one whose column has the lowest query frequency in the desired set.
[*index.too-many-large-batches-in-the-window-sheds-the-lowest-priority-index]
If pressure persists, the next-lowest goes, and so on.
<!-- lint-disable-next-line line-length -->
[*index.persistent-pressure-sheds-the-next-lowest-index-in-turn] The check runs
once per batch commit.
[*index.the-graduated-shedding-check-runs-once-per-batch-commit]

**Emergency shedding.** If a shard is at maximum batch size and its
drain thread signals rising ring buffer pressure, it drops **all**
secondary indexes at once.
<!-- lint-disable-next-line line-length -->
[*index.emergency-shedding-drops-every-secondary-index-at-once] `DROP INDEX` is a metadata operation
measured in milliseconds, so it is safe to do under pressure in a way
that creation is not.

The pressure signal comes from the drain thread watching the gap between
`write_pos` and its own `read_pos`. Exceeding
`EmergencySheddingBufferPercent` of ring buffer capacity raises it.
<!-- lint-disable-next-line line-length -->
[*index.ring-fill-above-emergency-shedding-buffer-percent-raises-the-pressure-signal]
It
is a distinct signal from the index-build cancellation one: this
triggers shedding whether or not a build is in progress.
[*index.emergency-shedding-triggers-whether-or-not-a-build-is-in-progress]

`idx_events_timestamp` is **exempt**. It is never shed at any pressure,
because time-range queries are the access pattern everything else is
built on and the store is unusable without it.
[*index.the-timestamp-index-is-never-shed]

When pressure subsides, shedding reverses: the shard rebuilds toward the
desired set under the same quiet-period scheduling and the same
cancellability, highest priority first.
<!-- lint-disable-next-line line-length -->
[*index.shed-indexes-are-rebuilt-highest-priority-first-once-pressure-subsides]

## Candidates

Any field that can appear in a `WHERE` predicate is a candidate.
[*index.any-field-usable-in-a-where-predicate-is-a-candidate]

**Header columns.** The columns of `event.type`, `emitter.class`,
`event.cpu`, `emitter.token.guid`, `emitter.true-token.guid`,
`emitter.process.guid`, and `event.boot.guid` (§3.1). The counters and
the desired set name each by its field path.
[*index.the-candidate-header-columns]
`timestamp`, the column of `event.time`, is always indexed and is not
adaptively managed; a predicate on `event.time` is not counted.
[*index.the-timestamp-column-is-not-adaptively-managed]

**Payload fields.** Any queryable flattened path that appears in a
predicate is a candidate for an expression index.
[*index.a-queryable-flattened-payload-path-is-an-expression-index-candidate]
A path suppressed by
the flattening rules of PSPU §3.22 — a header field's path or one
beneath it, a key that is not a valid segment, a duplicate path —
never receives one, because it is not a query-language field at all.
[*index.a-suppressed-payload-path-never-receives-an-index]
A path beside a header field's, such as `emitter.process.pid` beside
`emitter.process.guid`, is an ordinary candidate.

Counters and a desired set written by an earlier eventd, which named
header fields by their columns (`event_type`, `process_guid`), are read
back at startup under the fields' paths. Such a name could only have
meant the header field then, because a payload key spelled like a
column was reserved, so the renaming loses no index and builds no
spurious one.

The raw `payload` column never receives a plain column index.
[*index.the-raw-payload-column-never-receives-a-column-index] Indexing
an opaque blob accelerates nothing.

## Payload indexes are an optimization, not an authority

An expression index extracts a field from the payload on every insert
and indexes a deterministic private key for it.
<!-- lint-disable-next-line line-length -->
[*index.a-payload-index-keys-each-row-on-a-deterministic-extraction-of-the-field]
The exact key bytes are
internal to eventd and are not part of the storage contract.

eventd can implement these as SQLite expression indexes with
deterministic extraction functions, as generated columns, or by any
equivalent SQLite-backed means. What every mechanism has in common is
that it implements the same field resolution and flattening as
PSPU §3.22 — and that where the index cannot reproduce the query
language's comparison semantics exactly, it is used only to **narrow
candidate rows**, with the real predicate applied after the row is
loaded (§6.3).
<!-- lint-disable-next-line line-length -->
[*index.payload-indexes-resolve-fields-as-the-query-language-does-and-otherwise-only-narrow-candidates]

SQLite's native dynamic-type equality and ordering never substitute for
the query language's string case folding, numeric comparison, binary
comparison, array comparison, or null and missing-field handling.
[*index.sqlite-comparison-never-substitutes-for-query-language-comparison]
Getting a smaller answer faster is worthless if it is a different
answer.

Rows where the field is absent, unqueryable or suppressed index as null,
or are otherwise excluded in a way that preserves those semantics.
<!-- lint-disable-next-line line-length -->
[*index.rows-lacking-a-queryable-field-index-as-null-or-are-excluded-preserving-semantics]

Payload indexes are otherwise ordinary members of the desired set, with
the same priority ordering, shedding and convergence.
[*index.payload-indexes-are-ordinary-desired-set-members]

## Naming

Header column indexes are `idx_events_<column>`, named for the column
rather than the field — `idx_events_event_type` for `event.type`,
`idx_events_process_guid` for `emitter.process.guid`.
[*index.header-column-indexes-are-named-idx-events-column]

Payload expression indexes are named from the field GUID (§7.3), to
avoid both collisions and characters SQLite will not accept in an
identifier:

```text
idx_events_payload_<field_guid_hex>
```

where `field_guid_hex` is the UUID v5 field GUID for the flattened path,
as 32 lowercase hexadecimal digits with braces and hyphens stripped.
<!-- lint-disable-next-line line-length -->
[*index.payload-indexes-are-named-by-the-32-lowercase-hex-digit-field-guid] The
path `source.name` yields
`idx_events_payload_` followed by the 32 hex digits of
`uuid_v5(EVENTD_FIELD_NAMESPACE, "source.name")`.

Deriving the name from the GUID rather than from the path means the same
path always produces the same index name, and no path — however it is
spelled — can produce a name that collides with another's or that
SQLite rejects.
