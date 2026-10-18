---
title: Performance Review
description: The implementation mechanisms and measurements used when
  reviewing eventd throughput, latency, and bounded resource use.
---

Performance changes must preserve receipt accounting, authorization, and
bounded memory use. A faster path that loses either property changes the
service contract rather than optimizing its implementation.

## Implemented mechanisms

The implementation uses these mechanisms:

- Bounded handoff reservations before copying kernel events (§2.3).
- Independent writer threads and receipt commits per shard (§2.4).
- Cached SQLite statements and event-type identifiers (§2.4).
- Pressure-sensitive payload indexes (§3.4).
- Bounded metric-series and authorization caches (§5.3 and §7.5).
- Query-driven metric rollups checked against raw samples (§5.6).

The host hot-path benchmark exercises queue handoff and SQLite ingestion.
It does not measure kernel ring attachment, native access checks, or a
complete production workload. Build and benchmark commands belong in the
repository's contributor guide.

## Measurement boundaries

Record the hardware, storage, build profile, data shape, shard count, and
configuration with each result. Compare throughput together with commit
latency, queue occupancy, memory use, and loss reports.

Increasing concurrency can increase memory, thread, and descriptor use.
Increasing batch sizes can amortize commits while increasing the amount
of uncommitted work. Evaluate those costs together rather than treating a
single throughput number as the result.

## Changes requiring evidence

Allocation pools, thread affinity, batched datagram reads, and connection
pools need workload evidence before adoption. A pool must have an explicit
bound and account for every retained object. No performance multiplier is
guaranteed by selecting one of these techniques.
