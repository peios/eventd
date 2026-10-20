---
title: Series Resolution
description: Turning each arriving sample into a series_id once, on the single
  ingestion thread — the cache, and how to size it.
---

Every arriving sample must be turned into a `series_id` before it can be
inserted. [*resolve.every-sample-is-resolved-to-a-series-id-before-insertion]
This happens once per sample on the single metric ingestion
thread, so it is the hottest lookup in the daemon and the reason a cache
exists at all.
[*resolve.resolution-runs-once-per-sample-on-the-metric-ingestion-thread]

## Resolving

1. Compute the canonical label string: sort by key in unsigned UTF-8
   byte order, encode each pair `key=value`, join with commas. The empty
   label set encodes as the empty string. No escaping — ingestion has
   already rejected the delimiters (§5.2).
   [*resolve.first-the-canonical-label-string-is-computed]
2. Hash it. [*resolve.then-the-canonical-label-string-is-hashed]
3. For a histogram, compute the canonical boundary blob and its hash
   from the **validated, producer-supplied order**.
[*resolve.a-histograms-boundary-blob-and-hash-use-the-producer-supplied-order]
   eventd never sorts
   boundaries. [*resolve.eventd-never-sorts-histogram-boundaries]
   For counters and gauges both are null.
   [*resolve.counters-and-gauges-have-a-null-boundary-blob-and-hash]
4. Look up `series` by `name`, `label_hash`, and for histograms
   `boundaries_hash`.
   [*resolve.lookup-is-by-name-label-hash-and-for-histograms-boundaries-hash]
5. On a match, verify the full `labels` string, and for histograms the
   full boundary blob.
[*resolve.a-hash-match-is-verified-against-the-full-labels-and-boundary-blob]
   If the record's type differs from the existing
   series' type, drop the record (§5.1).
   [*resolve.a-type-mismatch-with-the-matched-series-drops-the-record]
   Otherwise use the existing
   `series_id`. [*resolve.a-verified-match-reuses-the-existing-series-id]
6. On no match, insert a new `series` row and use the new identifier.
   [*resolve.no-match-inserts-a-new-series-row]

A histogram whose boundaries changed takes step 6: it is a new series
(PSPU §3.13).
[*resolve.a-histogram-with-changed-boundaries-is-a-new-series]
The old one keeps its historical samples and the new one
starts accumulating.
[*resolve.the-old-histogram-series-keeps-its-historical-samples]

## The cache

Resolution runs through a bounded in-memory cache mapping
`(name, canonical labels, boundaries hash, boundaries blob)` to
`series_id`. For counters and gauges the boundary components are absent.
[*resolve.the-series-cache-maps-name-labels-and-boundaries-to-series-id]

A hit is a hash table lookup with no SQLite involvement.
[*resolve.a-cache-hit-does-not-touch-sqlite] A miss costs
one `SELECT` on `name` and `label_hash`, after which the result is
<!-- lint-disable-next-line spelling -->
inserted, evicting the least-recently-used entry if the cache is full.
[*resolve.a-cache-miss-costs-one-select-and-evicts-the-lru-entry-when-full]

The bound is `MetricSeriesCacheSize` (§A), default 50000, with LRU
eviction.
[*resolve.the-cache-is-bounded-by-metricseriescachesize-default-50000]
It bounds **memory**, not the number of series: the `series`
table is uncapped and a new series is always created in the database.
[*resolve.a-full-cache-never-prevents-series-creation]
A system with a million series and a 50000-entry cache uses memory
proportional to the cache, and at roughly 200 to 300 bytes an entry the
default costs 10 to 15 MB.

The cache starts empty after a restart and is warmed on demand — within
one collection cycle, typically fifteen seconds, every active series is
cached. [*resolve.the-cache-starts-empty-and-warms-on-demand]
There is no pre-warming pass, because reading a million-row
`series` table at startup to populate a 50000-entry cache would be work
spent to discard most of its result.
[*resolve.there-is-no-cache-pre-warming-pass]

## Sizing it

The cache is sized for the set of actively reporting series, and
behaves badly below it.

Below that, LRU does not help, because every series is equally hot: each
collection cycle evicts the overflow and reloads it, producing a fixed
number of `SELECT`s every cycle, permanently. A system with 55000 active
series and a 50000-entry cache incurs about 5000 cache misses every
fifteen seconds, indefinitely.
[*resolve.an-undersized-cache-reloads-the-overflow-every-collection-cycle]

This interacts badly with label cardinality (PSPU §3.10). Labels with
unbounded values — request identifiers, user-supplied strings — grow the
series table without limit, and once the active set exceeds the cache
every cycle pays the eviction cost on the one thread that also drains
the metric socket. The failure presents as metric loss, because the
thread stops reading while it queries.

eventd does not cap series creation: at the interface, a producer
creating a new series is indistinguishable from one creating
garbage, and rejecting either would break a correct producer to
inconvenience an incorrect one (PSPU §3.13).
[*resolve.eventd-does-not-cap-series-creation] The default 1 GiB metric
retention ceiling bounds the resulting disk growth (§5.5), but it does
not prevent a hot working set from exceeding this cache and thrashing
the ingestion thread before retention runs.
