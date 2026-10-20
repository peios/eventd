---
title: The Pipeline
description: The four stages from shared memory to a committed row, why the
  ring buffers are the only buffer, and how sharding scales writes.
---

eventd is the primary consumer of the KMES ring buffers. Events travel
from shared memory to a committed database row in four stages.
<!-- lint-disable-next-line line-length -->
[*pipeline.events-travel-from-the-ring-buffers-to-a-committed-row-in-four-stages]

1. **Drain.** One thread per CPU reads events from that CPU's ring
   buffer, following the lock-free read protocol PSPK specifies (§2.2).
2. **Detect.** The drain thread compares each event's sequence number
   against the last it saw for that CPU. A jump means events were lost,
   and the loss becomes a gap record (§2.5).
3. **Hand off.** The drain thread copies the event out of the mapped
   region and passes it to the writer thread that owns the shard it
   routes to (§2.3).
4. **Write.** The writer thread accumulates events into a transaction
   and commits, sizing the batch to whatever throughput allows (§2.4).

Two principles govern the whole pipeline, and most of its behavior
follows from them rather than from anything specific to a stage.

## The ring buffers are the only buffer

[*pipeline.the-ring-buffers-are-the-only-buffer]

eventd holds no large intermediate queue between KMES and SQLite. The
handoff channel has startup-fixed slot and byte bounds chosen by
performance testing, independently of the writer's transaction batch
size, and nothing else accumulates (§2.3).

When the writer falls behind, the channel fills; when the channel is
full, the drain thread stops reading; when the drain thread stops
reading, events accumulate in the ring buffer, which is exactly what a
ring buffer is for. Backpressure propagates all the way back to the
kernel, and the absorption capacity is the ring buffer's, which an
administrator already sizes.
<!-- lint-disable-next-line line-length -->
[*pipeline.backpressure-propagates-from-the-writer-through-the-channel-to-the-ring-buffer]

If the ring buffer also fills, KMES overwrites its oldest events, the
drain thread notices the sequence jump when it resumes, and the loss is
recorded (§2.5). That is the designed worst case: **eventd loses events
visibly rather than buffering without bound and dying**.
<!-- lint-disable-next-line line-length -->
[*pipeline.a-full-ring-buffer-loses-events-visibly-rather-than-buffering-without-bound]

The alternative — a large in-process queue — would move the same
capacity into a place where losing it is invisible, where it competes
with the page cache for memory, and where an out-of-memory kill takes
the whole queue with no record that it existed.

## Independent shard writes

Each shard is a self-contained SQLite database with its own file, its
own write-ahead log and its own writer thread.
[*pipeline.each-shard-has-its-own-file-wal-and-writer-thread] Each shard
commits its rows and receipts independently (§2.3). The writers share a
commit notification for queries, but no cross-shard transaction coordinator.
[*pipeline.shards-share-no-write-path-state-or-coordination-point]

The consequence for the query path is that a shard means nothing to it.
A shard database holds whatever CPUs happened to route to it in whatever
eventd lifetime wrote it, so a query filtering by CPU reads every shard,
and the query path never assumes a relationship between a shard and a
CPU (§6.4).
