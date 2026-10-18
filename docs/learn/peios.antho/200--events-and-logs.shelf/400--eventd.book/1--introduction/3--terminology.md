---
title: Terminology
description: Terms this manual borrows unchanged from elsewhere in the corpus,
  and the ones it introduces.
---

Terms defined elsewhere are used with the same meaning and are not
redefined here: event, header, payload, stamp, ring buffer, consumer,
origin class, and sequence number from the KMES chapters of the Peios
Kernel TRM and PSPK; token, GUID, SID, Security Descriptor, ACL, ACE, and
privilege from the Peios Kernel TRM and PCDS; registry, hive, key, value,
and layer from the Peios Kernel TRM; producer, client, log record,
metric sample, time series, and concrete identifier from PSPU §3.2.

Syscall numbers and signatures for the kernel interfaces eventd calls —
`kmes_attach`, the `SOL_KACS` peer-token option, `kacs_access_check` and
`kacs_access_check_list` — are in the Peios Kernel TRM's generated ABI
appendices, §2.A and §3.A. This manual names them and does not repeat
their numbers.

The following are specific to eventd.

**Drain thread**: one of the threads that reads from a per-CPU KMES ring
buffer. There is exactly one per CPU (§2.2).
[*term.there-is-exactly-one-drain-thread-per-cpu]

**Writer thread**: the sole writer to one event shard. There is exactly
one per shard, and no other thread writes to that database (§2.3).
[*term.each-shard-has-exactly-one-writer-thread-and-no-other-writer]

**Shard**: one of the independent SQLite databases the event store is
split across, each with its own file, write-ahead log and writer thread.
A shard is a write-path construct only; the query path treats the whole
directory as one store (§2.3).
[*term.the-query-path-treats-every-shard-as-one-store]

**Active shard**: a shard in the current configuration's numbering.
**Historical shard**: a shard database left behind by a previous
configuration. It takes no new events and is still queried, through
read-only connections; its one writer is the retention coordinator
(§3.3, §3.6).
<!-- lint-disable-next-line line-length -->
[*term.a-historical-shard-takes-no-new-events-is-still-queried-and-is-written-only-by-retention]

**Handoff channel**: the bounded queue between drain threads and a
writer thread. It is a small startup-fixed scheduling handoff, bounded
by slots and bytes independently of transaction batch size (§2.3).
<!-- lint-disable-next-line line-length -->
[*term.the-handoff-channel-is-bounded-by-slots-and-bytes-independently-of-batch-size]

**Synthetic event**: a record eventd generates itself and writes
directly to a shard, bypassing KMES. Synthetic events carry no identity
stamps and no sequence numbers, and are distinguished by their type,
one of exactly five eventd reserves: `eventd.daemon.started`,
`eventd.daemon.stopped`, `eventd.events.lost`, `eventd.store.quarantined`,
and `eventd.config.changed` (§2.6).
[*term.synthetic-events-bypass-kmes-and-carry-no-stamps-or-sequence-numbers]

**Gap record**: the `eventd.events.lost` synthetic event recording that
events were lost on one CPU, and which sequence numbers went missing
(§2.5).
[*term.a-gap-record-names-the-cpu-and-the-missing-sequence-numbers]

**Event store directory**: the directory holding every shard database
and the metadata database. **Metadata database**: `eventd-meta.db`, the
one database that is not a shard and survives shard reconfiguration
(§3.5).
<!-- lint-disable-next-line line-length -->
[*term.the-metadata-database-is-not-a-shard-and-survives-shard-reconfiguration]

**Desired index set**: the global, priority-ordered list of fields eventd
aims to have indexed across all shards.
[*term.the-desired-index-set-is-one-global-priority-ordered-list]

**Material indexes**: the indexes a given shard actually has, which
converge toward the desired set when the shard is quiet and diverge from
it under pressure (§3.4).
[*term.material-indexes-converge-when-quiet-and-diverge-under-pressure]

**Shedding**: dropping secondary indexes to protect write throughput
(§3.4). [*term.shedding-drops-secondary-indexes]

**Rollup**: a disposable, pre-computed metric-window result whose freshness is
proved against authoritative raw samples before reuse (§5.6).
<!-- lint-disable-next-line line-length -->
[*term.a-rollup-is-reused-only-after-its-freshness-is-proved-against-raw-samples]

**Series cache**: the bounded in-memory map from series identity to
series row, which keeps metric ingestion off SQLite in the common case
(§5.3).
<!-- lint-disable-next-line line-length -->
[*term.the-series-cache-is-a-bounded-in-memory-map-from-series-identity-to-row]

**Logical live size**: `(page_count - freelist_count) × page_size` for a
SQLite database — the space actually holding data, excluding pages freed
by deletion and available for reuse. Retention is enforced against this
rather than against the file size (§3.6).
[*term.retention-is-enforced-against-logical-live-size-not-file-size]

**Quarantine**: renaming a database SQLite has reported corrupt, aside
from the path eventd uses, and creating an empty one in its place
(§3.3).
[*term.quarantine-renames-a-corrupt-database-aside-and-creates-an-empty-one]
