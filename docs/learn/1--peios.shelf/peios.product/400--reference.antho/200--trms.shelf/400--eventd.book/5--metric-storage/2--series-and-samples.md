---
title: Series and Samples
description: The metric store is organized around series rather than records —
  the series table, the canonical label string and the boundaries blob.
---

The metric store is a single SQLite database, and unlike the event and
log stores it is organized around **series** rather than records.
<!-- lint-disable-next-line spelling -->
[*series.the-metric-store-is-one-sqlite-database-organised-around-series]
Individual samples are appended to a series that already has an
identity.

## The series table

| Column | Type | Contents |
|---|---|---|
| `id` | INTEGER PRIMARY KEY | Series identifier; the foreign key `samples` uses. |
| `name` | TEXT NOT NULL | The metric name. |
| `labels` | TEXT NOT NULL | Canonical label representation. Empty string for no labels. |
| `type` | INTEGER NOT NULL | 0 counter, 1 gauge, 2 histogram. |
| `label_hash` | INTEGER NOT NULL | Hash of the canonical label string. |
| `boundaries_hash` | INTEGER | Hash of the canonical boundary blob. Null for counters and gauges. |
| `boundaries` | BLOB | Canonical boundary blob. Null for counters and gauges. |

[*series.series-id-is-the-integer-primary-key-samples-reference]
[*series.series-name-is-the-metric-name]
[*series.series-labels-is-the-canonical-label-string-empty-for-no-labels]
[*series.series-type-is-0-counter-1-gauge-2-histogram]
[*series.series-label-hash-hashes-the-canonical-label-string]
<!-- lint-disable-next-line line-length -->
[*series.series-boundaries-hash-hashes-the-boundary-blob-and-is-null-for-counters-and-gauges]
<!-- lint-disable-next-line line-length -->
[*series.series-boundaries-holds-the-boundary-blob-and-is-null-for-counters-and-gauges]

### The canonical label string

Labels are sorted by key in unsigned UTF-8 byte order, each pair written
`key=value`, and the pairs joined with commas: `core=0,host=server1`.
<!-- lint-disable-next-line line-length -->
[*series.canonical-labels-are-key-sorted-by-utf-8-bytes-and-comma-joined-as-key-value-pairs]
The empty label set is the empty string.
[*series.the-empty-label-set-is-the-empty-string]

No escaping is performed and none is needed, because ingestion rejects
`=` and `,` inside a key or a value (PSPU §3.10).
[*series.the-canonical-label-string-is-not-escaped] That prohibition
exists precisely to make this encoding unambiguous, and it is the reason
the constraint binds the producer rather than being handled internally.

### The boundaries blob

A fixed binary encoding, not MessagePack:

1. `boundary_count`, `u32` little-endian
   [*series.the-boundary-blob-starts-with-a-u32-little-endian-count]
2. that many IEEE-754 `f64` values, each little-endian, in the validated
   order the producer sent
<!-- lint-disable-next-line line-length -->
   [*series.the-boundary-blob-then-holds-little-endian-f64-values-in-producer-order]

It exists only to identify histogram series and to resolve hash
collisions, and is never returned in a query result.
[*series.the-boundary-blob-is-never-returned-in-a-query-result]

### Hashes narrow, they do not decide

`label_hash` and `boundaries_hash` are 64-bit FNV-1a over the exact
bytes of the canonical string or blob, with offset basis
`0xCBF29CE484222325` and prime `0x100000001B3`.
[*series.series-hashes-are-64-bit-fnv-1a-over-the-canonical-bytes]
The high bit is cleared
before storage, `hash & 0x7FFF_FFFF_FFFF_FFFF`, so the value always fits
SQLite's signed `INTEGER`.
[*series.the-hash-high-bit-is-cleared-before-storage]

A lookup always verifies the full `labels` string, and for a histogram
the full `boundaries` blob, after narrowing by hash.
<!-- lint-disable-next-line line-length -->
[*series.a-lookup-verifies-the-full-labels-and-boundaries-after-narrowing-by-hash]
A hash is an index
key, never an identity: two label sets that collide are still two
series. [*series.colliding-label-sets-are-still-distinct-series]

### Uniqueness

The table carries `UNIQUE(name, labels, boundaries)`, on the full
boundary blob. `boundaries_hash` is a lookup key with its own index, and
two histograms whose boundaries collide on it are two series.
[*series.the-series-table-is-unique-on-name-labels-and-boundaries]

For counters and gauges `boundaries` is null, and SQLite treats
nulls as distinct in a unique constraint — so the constraint does not
enforce uniqueness for them. What does is the single-writer resolution
logic (§5.3), which checks before inserting.
<!-- lint-disable-next-line line-length -->
[*series.counter-and-gauge-uniqueness-rests-on-single-writer-resolution-not-the-constraint]
The constraint is a
defensive backstop against a future change that introduces a second
write path, not the primary mechanism.

`type` is **not** part of the identity.
[*series.type-is-not-part-of-series-identity] A record resolving to an
existing series with a different type resolves successfully and is then
dropped for the mismatch (§5.1).

## The samples table

| Column | Type | Contents |
|---|---|---|
| `id` | INTEGER PRIMARY KEY | Internal row identifier; the tiebreaker for samples sharing a series and timestamp. |
| `series_id` | INTEGER NOT NULL | References `series(id)`. |
| `boot_id` | BLOB NOT NULL | 16-byte boot ID GUID. |
| `timestamp` | INTEGER NOT NULL | Nanoseconds since the Unix epoch. |
| `value` | REAL NOT NULL | The raw value for counters and gauges. Stores 0 for histograms. |
| `histogram_data` | BLOB | Canonical MessagePack histogram sample map. Null for counters and gauges. |
[*series.samples-id-breaks-ties-between-samples-sharing-a-series-and-timestamp]
[*series.samples-series-id-references-series-id]
[*series.samples-boot-id-is-a-16-byte-boot-id-guid]
[*series.samples-timestamp-is-nanoseconds-since-the-unix-epoch]
[*series.samples-value-is-the-raw-value-or-0-for-histograms]
[*series.samples-histogram-data-is-the-sample-map-and-null-for-counters-and-gauges]

For a histogram, `histogram_data` is a canonical MessagePack map
(PSPU §3.5) with exactly four keys: `boundaries`, an array of `float64`
in the producer's order; `counts`, an array of unsigned integers;
`total_count`; and `sum`, a finite `float64`.
<!-- lint-disable-next-line line-length -->
[*series.the-histogram-sample-map-has-exactly-boundaries-counts-total-count-and-sum]

`value` is a placeholder for histogram rows and is never returned as a
metric query value.
[*series.a-histogram-rows-value-is-never-returned-as-a-query-value]
Storing 0 rather than null keeps the column
`NOT NULL` and keeps the row layout uniform.

Canonical encoding is required here because a stored sample map must be
byte-stable: two equal histograms encode identically, which is what
makes them comparable without decoding.
[*series.equal-histogram-samples-encode-to-identical-bytes]

`boot_id` is per sample and is not part of the series identity, so a
series stays continuous across a reboot (§3.7).
[*series.boot-id-is-per-sample-so-a-series-continues-across-reboots]

## Ordering

Query execution order within a series is always `(timestamp, id)`
ascending, never insertion order alone.
[*series.samples-in-a-series-are-ordered-by-timestamp-then-id-ascending]
Duplicate timestamps are
permitted and `id` gives them a stable order.
[*series.duplicate-timestamps-are-permitted]

`id` is internal. It is never exposed as a query field, a result field,
an access-control field, or a reserved label key (PSPU §3.28).
<!-- lint-disable-next-line line-length -->
[*series.the-sample-id-is-never-exposed-as-a-query-result-access-control-or-label-field]

## Write-time indexes

- `idx_samples_series_timestamp` on `samples(series_id, timestamp, id)`
  — the dominant pattern is "samples for series X over range Y in
  deterministic order", and this one composite index serves the series
  lookup, the range filter and the `(timestamp, id)` ordering in a
  single scan.
  [*series.idx-samples-series-timestamp-indexes-series-id-timestamp-and-id]
- `idx_series_name` on `series(name)` — name lookups.
  [*series.idx-series-name-indexes-series-by-name]
- `idx_series_label_hash` on `series(label_hash)` — series resolution on
  the ingestion path.
  [*series.idx-series-label-hash-indexes-series-by-label-hash]
- `idx_series_boundaries_hash` on `series(boundaries_hash)` — narrowing
  a histogram's lookup by its boundary hash.
  [*series.idx-series-boundaries-hash-indexes-series-by-boundaries-hash]

## Schema version

A `metadata` table with the same structure as the other stores' (§3.1).
[*series.the-metric-store-has-a-metadata-table-shaped-like-the-other-stores]
Version 1 comprises `series`, `samples`, and `metadata`.
[*series.schema-version-1-comprises-series-samples-and-metadata]
Version 2 adds the
disposable `rollups` query cache (§5.6); it does not change raw series or
sample
storage. [*series.schema-version-2-adds-only-the-rollups-cache]
Version 3 changes only the series table's uniqueness, from the boundary
hash to the full boundary blob, and adds `idx_series_boundaries_hash`.
[*series.schema-version-3-makes-series-unique-on-the-full-boundary-blob]
The current value is in §B. eventd checks it at startup and applies the
lifecycle and migration rules of §5.4.
