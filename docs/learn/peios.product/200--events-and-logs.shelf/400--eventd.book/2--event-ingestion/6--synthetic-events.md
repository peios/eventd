---
title: Synthetic Events
description: Records eventd generates about itself, written straight to a
  shard and never through KMES — when, where and in what order.
---

Synthetic events are records eventd generates about itself. They are
written straight to a shard database and never touch KMES.
<!-- lint-disable-next-line line-length -->
[*synthetic.synthetic-events-are-written-straight-to-a-shard-and-never-touch-kmes]

They carry no KMES header: no identity stamps, no sequence number, no
origin class. [*synthetic.synthetic-events-carry-no-kmes-header] What
they have is a timestamp and one of exactly five event types, all under
eventd's own platform root `eventd` (PGSS §6.A), and the type is what
distinguishes them in the `events` table — no separate record-type
column exists (§3.1).
[*synthetic.a-synthetic-event-has-a-timestamp-and-one-of-five-eventd-types]
The timestamp is eventd's clock when it generated the record, except for
a gap record, whose timestamp is that of the event that revealed the gap
(§2.5).

## When they are generated

| Condition | Type |
|---|---|
| Lost events detected on a CPU | `eventd.events.lost` (§2.5) |
| eventd started and attached to KMES | `eventd.daemon.started` |
| Graceful shutdown, once everything read is committed | `eventd.daemon.stopped` |
| A store was found corrupt and quarantined, at startup or at write time (§9.2) | `eventd.store.quarantined` |
| A configuration value changed at runtime | `eventd.config.changed` |

[*synthetic.lost-events-on-a-cpu-emit-eventd-events-lost]
[*synthetic.starting-and-attaching-to-kmes-emits-eventd-daemon-started]
[*synthetic.graceful-shutdown-emits-eventd-daemon-stopped]
<!-- lint-disable-next-line line-length -->
[*synthetic.a-store-found-corrupt-and-quarantined-emits-eventd-store-quarantined]
[*synthetic.a-runtime-configuration-change-emits-eventd-config-changed]

Payload schemas for all five are in §3.2.

All five are tier `essential` (PGSS §6.8). eventd is their emitter as well
as their store, and the emission policy never switches an essential type
off (PGSS §6.9), so eventd writes each whenever its condition occurs and
consults no policy for them.
[*synthetic.all-five-types-are-essential-and-no-emission-policy-applies]
Their fields are those of eventd's catalog fragment, installed as
`/usr/share/evman/eventd.evman` (PGSS §6.10).

Note what is absent: there is no synthetic event for malformed
ingestion input.
[*synthetic.malformed-ingestion-input-emits-no-synthetic-event]
Authentication does not make admitted input trustworthy,
and emitting a durable record per bad datagram would hand a producer an
amplification primitive (PSPU §3.4). These five are conditions eventd
observed about itself, not reactions to what it was sent.

## Which shard

**CPU-specific** synthetic events — gap records — go to the shard
assigned to the CPU that generated them, handed to that writer thread
alongside that CPU's ordinary events.
[*synthetic.a-gap-record-goes-to-a-shard-of-the-cpu-that-generated-it] A
gap record travels with the events it describes.

**Daemon-wide** ones — start, stop, configuration changes, quarantines —
go to shard 0 when shard 0 is writable, and otherwise to the
lowest-numbered writable active shard.
<!-- lint-disable-next-line line-length -->
[*synthetic.daemon-wide-events-go-to-shard-0-else-the-lowest-numbered-writable-active-shard]
If no shard is writable at all the event is skipped, with the failure
logged to standard error.
<!-- lint-disable-next-line line-length -->
[*synthetic.with-no-writable-shard-a-daemon-wide-event-is-skipped-and-logged-to-stderr]

A quarantine is the case that needs the fallback. It describes a
failure on one particular shard but is itself a daemon-wide
notification, so it is not written to the failing shard unless that
shard has since been replaced and is writable again (§9.2) — writing the
record of a shard's failure into that shard would lose it exactly when
it matters.
<!-- lint-disable-next-line line-length -->
[*synthetic.a-storage-error-skips-the-failing-shard-unless-it-is-writable-again]

These events are infrequent enough that concentrating them on shard 0
costs nothing measurable in balance.

## Storage and ordering

Synthetic events live in the same shard databases as KMES events and
participate in the same batching, the same retention and the same
queries.
<!-- lint-disable-next-line line-length -->
[*synthetic.synthetic-events-share-the-shards-batching-retention-and-queries-of-kmes-events]
Access control treats their types like any other (§7.2), so a
descriptor at `Machine\System\eventd\Security\Events\eventd` governs all
five, and one at `…\Events\eventd.daemon` only the start and stop
records.
[*synthetic.access-to-synthetic-events-is-governed-by-the-eventd-events-keys]

They are ordered by their timestamp and take no part in per-CPU sequence
numbering.
<!-- lint-disable-next-line line-length -->
[*synthetic.synthetic-events-are-ordered-by-timestamp-and-take-no-sequence-number]

For the four daemon-wide types that timestamp is when eventd
**noticed**, not when the condition occurred.
[*synthetic.a-daemon-wide-records-timestamp-is-when-eventd-noticed]
A gap record is different: its timestamp is the revealing event's
(§2.5), so it sorts beside that event, and with `loss.preceding-time`
it bounds when the lost events were written. eventd can detect a gap
long after the events were overwritten — after a restart, the first
thing it writes in a boot can be the record of events lost before — and
the record's time says when they were lost, not when eventd found out.
<!-- lint-disable-next-line line-length -->
[*synthetic.a-gap-records-timestamp-is-the-revealing-events-not-the-detection-time]

## The reserved types

A KMES event whose type is one of the five is not stored: eventd counts
it and drops it, and still receipts its sequence (§3.1). Exactly those
five names are reserved, not the `eventd` root or any prefix of it, so
eventd remains free to emit other `eventd.*` types through KMES.
[*synthetic.exactly-the-five-types-are-reserved-not-the-eventd-root]
