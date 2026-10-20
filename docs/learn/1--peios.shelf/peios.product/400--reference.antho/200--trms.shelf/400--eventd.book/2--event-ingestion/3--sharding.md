---
title: Sharding
description: Event writes distributed across independent databases — the
  count, descriptor budget, assignment, writer threads, and handoff.
---

## The shard count

Event writes are distributed across independent SQLite databases.
An explicit `StorageShards` count (§A) is between one and 256.
[*shard.there-are-between-1-and-256-event-shards] Zero means "as many
shards as there are successfully attached KMES buffers", and is the
default; that automatic count is not capped at 256.
<!-- lint-disable-next-line line-length -->
[*shard.storage-shards-zero-is-the-default-and-means-one-shard-per-attached-buffer]

Every shard costs file descriptors. An active shard's writer connection
holds three for eventd's life — the database, its write-ahead log and
its shared-memory index — as does the retention coordinator's connection
to each historical shard (§3.6), and every event query that reads every
shard holds about two more per shard while it runs (§6.4). eventd
neither raises its own descriptor limit nor caps the shard count to fit
it.
[*shard.eventd-neither-raises-its-descriptor-limit-nor-caps-the-shard-count]
The packaged `eventd-service.reg` sets `LimitNOFILE` to **262144**.
peinit applies it as both the soft and hard `RLIMIT_NOFILE` before
launch; eventd does not raise the limit itself. Under an inherited
1024-descriptor soft limit, 256 shards do not start, and counts well
below that leave little room for queries. Operators can override
`LimitNOFILE` (DWORD) at `Machine\System\Services\eventd` and restart
the service to apply it (peinit TRM §3.2).
[*shard.a-high-storage-shards-needs-a-raised-descriptor-limit]

The packaged value uses this conservative planning allowance:

```text
4 × Q × max(E, 1) + 4 × Q + 3 × (E + 3) + 2 × C + 256
```

Here `Q` is the concurrent query count, `E` the total active and
historical event databases visible to queries, and `C` the attached
KMES buffer count. The terms allow four descriptors per query database
(database, WAL, shared-memory index, and temporary-file headroom), four
per query for its socket, peer token and transient handles, three per
persistent database connection (including log, metric, and metadata
stores), two per KMES buffer during replacement, and 256 for fixed
handles and maintenance. Historical shards have persistent retention
connections as well as query readers. Streaming queries are already
included in `Q`; their separate limit is not added again.

At the default `MaxConcurrentQueries=128`, with at most 256 query-visible
event databases and 256 attached KMES buffers, this gives **133129**;
262144 rounds up with additional headroom. This is a sizing allowance,
not a strict SQLite descriptor bound or a guarantee that the workload
fits memory and thread limits. The limit does not preallocate handles.

Neither historical shard discovery nor the automatic shard count is
globally capped by the explicit `StorageShards` range. More historical
shards, more attached buffers or higher query concurrency require
operator sizing and, where needed, a larger service limit. The host's
`fs.nr_open` ceiling must also permit the chosen limit; setting the
service value does not raise that system ceiling.

Two properties make a count perform well, and neither is enforced.
[*shard.neither-a-power-of-two-nor-a-multiple-of-the-buffer-count-is-enforced]
A power of two lets routing use a bitwise AND rather than a modulo. A
multiple of the attached-buffer count distributes shards evenly across
drain threads. The default satisfies the second by construction.

## Assignment

Shard-to-CPU assignment is computed once at startup and is fixed for the
process lifetime.
<!-- lint-disable-next-line line-length -->
[*shard.assignment-is-computed-once-at-startup-and-fixed-for-the-process-lifetime]

The successfully attached logical CPUs are assigned dense ordinals
`c = 0..attached_count-1` in logical-ID order (§2.2). For each ordinal
`c`, eventd assigns every shard `j` where
`j % attached_count == c`.
<!-- lint-disable-next-line line-length -->
[*shard.ordinal-c-is-assigned-every-shard-j-where-j-mod-attached-count-equals-c]
If that produces nothing — which happens when there are fewer shards
than attached buffers — ordinal `c` is instead assigned
`c % shard_count`.
[*shard.an-ordinal-with-no-shard-is-assigned-c-mod-shard-count] Every
drain thread ends with at least one write path.
[*shard.every-drain-thread-has-at-least-one-shard] Logical `cpu_id`, not
the ordinal, is stored. [*shard.the-logical-cpu-id-not-the-ordinal-is-stored]

The three cases behave differently:

| Relation | Result |
|---|---|
| shards == CPUs | one shard per CPU, the 1:1 case |
| shards < CPUs | several CPUs share a shard |
| shards > CPUs | a CPU owns several shards |

[*shard.equal-counts-give-one-shard-per-cpu]
[*shard.fewer-shards-than-cpus-means-cpus-share-shards]
[*shard.more-shards-than-cpus-means-a-cpu-owns-several]

A drain thread owning several shards distributes **contiguous sequence
stripes**, not individual events round-robin. It sends a fixed positive
number of successive events to one shard, then moves to the next shard
it owns.
<!-- lint-disable-next-line line-length -->
[*shard.a-multi-shard-drain-thread-sends-fixed-length-contiguous-stripes-to-its-shards-in-turn]
The stripe length is selected once at startup and is not
persisted; committed receipt ranges, rather than recomputing the routing
choice, are the recovery authority (§2.2, §3.1).
[*shard.the-stripe-length-is-chosen-at-startup-and-not-persisted]

Keeping a contiguous run together makes one receipt describe the run
compactly. It preserves parallelism across CPUs and across successive
stripes without requiring any cross-shard commit coordinator.

When the counts do not divide evenly, some CPUs carry one shard more
than others, or some shards receive from one CPU more than others.
[*shard.uneven-counts-leave-an-imbalance-of-at-most-one-shard-or-cpu] The
resulting imbalance is one shard's worth of throughput, which is
negligible against the whole.

## Shards are not a query-path concept

[*shard.the-query-path-assumes-no-relationship-between-a-shard-and-a-cpu]

Assignment is not persisted. [*shard.assignment-is-not-persisted] A
shard database accumulates events from
whatever CPUs routed to it during whatever eventd lifetimes wrote it, so
a single shard file can hold events from a different set of CPUs in
different regions of its history.

The query path therefore assumes nothing: it reads every database in the
directory, and a query filtering on `event.cpu` scans all of them (§6.4).
Sharding is a write-path optimization that the read path pays a fan-out
for.

## Writer threads

Each shard has exactly one writer thread, and that thread is the only
writer to that database.
[*shard.each-shard-has-exactly-one-writer-thread-and-no-other-writer] No
other thread and no other connection writes to it, which is what makes
the single-writer assumptions in §3.3 and §3.4 safe.

Drain threads never write to SQLite.
[*shard.drain-threads-never-write-to-sqlite] When several drain threads
share a shard they hand off concurrently, so the handoff channel is
multi-producer and single-consumer.
[*shard.the-handoff-channel-is-multi-producer-single-consumer]

## The handoff channel

Each writer thread has one bounded channel through which drain threads
submit events. [*shard.each-writer-thread-has-one-bounded-handoff-channel]
Its capacity is fixed at startup and independent of `MaxBatchSize`: the
channel is bounded both by occupied slots and by the total bytes held by
those slots.
<!-- lint-disable-next-line line-length -->
[*shard.channel-capacity-is-fixed-at-startup-in-slots-and-bytes-independent-of-max-batch-size]
Both are internal implementation limits selected by performance testing,
not live registry settings.
[*shard.the-channel-bounds-are-not-registry-settings]

When either bound would be exceeded the drain thread **stops reading
from the ring buffer** and waits.
<!-- lint-disable-next-line line-length -->
[*shard.a-drain-thread-stops-reading-when-either-channel-bound-would-be-exceeded]
It reserves capacity before copying or advancing `read_pos` (§2.2), does
not drop events to relieve pressure, and does not grow the channel.
[*shard.channel-pressure-never-drops-events-or-grows-the-channel] Events
accumulate in the ring buffer instead, which is the designed path
(§2.1):

```text
writer slow → channel fills → drain pauses → ring buffer absorbs
            → KMES overwrites oldest if full → gap detected on resume
```

When the writer consumes slots and the channel has room, the drain thread
resumes immediately.
[*shard.the-drain-thread-resumes-as-soon-as-the-channel-has-room]

Publication wakes a writer waiting on an empty channel; releasing capacity
wakes producers waiting for room. The change itself supplies the wakeup,
even when it races with a thread going to sleep. Neither side needs a later
event or a timeout to notice the available work or capacity.

The channel is a small scheduling handoff, not storage for a complete
transaction and not a second backlog buffer. A writer can consume and
insert one slot while producers refill it, so a transaction can contain
many more events than the channel can hold simultaneously.
<!-- lint-disable-next-line spelling -->
[*shard.a-transaction-may-hold-more-events-than-the-channel-can-at-once]
Changing `MaxBatchSize` changes only the writer's next commit threshold
and never resizes or replaces a live channel (§8.3).
[*shard.changing-max-batch-size-never-resizes-or-replaces-a-live-channel]

## Lifecycle and reconfiguration

Shard databases are created in the event store directory on first use.
[*shard.shard-databases-are-created-on-first-use] eventd never deletes
or overwrites one left by a previous configuration: starting with fewer
shards than exist leaves the excess in place, and the query path
continues to read them (§3.3).
[*shard.shards-left-by-a-previous-configuration-are-kept-and-still-read]

Changing `StorageShards` takes effect at the next restart. The
configuration watch notices the change and eventd defers it rather than
reassigning CPUs or creating shards while running (§8.3).
[*shard.a-storage-shards-change-is-deferred-to-the-next-restart]

Shard count changes are expected to be rare — set once from the hardware
profile, one for a small board and a multiple of the CPU count for a
server, and then left alone. Live migration would mean rebalancing
writer threads and channels while events are in flight, for a
configuration change that happens once in a machine's life.

## Queue ownership

The Rust queue constructor returns a clonable producer handle and one
`QueueReceiver`. Consumer operations require mutable access to that
receiver, so safe Rust cannot create concurrent consumers for a shard.
Dropping the receiver closes the queue and wakes blocked producers.

Capacity must be a power of two and at least 2 slots. The sequence-number
ring algorithm cannot distinguish a published slot from a free slot when
the capacity is 1. Byte reservations remain independent of the slot count.
