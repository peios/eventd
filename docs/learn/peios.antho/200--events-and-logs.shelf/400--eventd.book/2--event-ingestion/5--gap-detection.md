---
title: Gap Detection
description: Per-CPU sequence numbers are the whole mechanism — what causes a
  gap, what a gap record holds, and how lapping is detected.
---

Every event carries a per-CPU, per-boot sequence number, and a drain
thread knows what it last saw. That is the whole mechanism: an event
whose sequence number exceeds the expected next one means the numbers in
between belonged to events eventd never received.
[*gap.a-sequence-beyond-the-expected-next-one-reveals-a-gap]

During restart reconciliation, "expected" is the union of committed
receipt coverage and surviving ring records rather than one last-seen
number. A sequence already covered by either source is not a gap (§2.2).

## Causes

- **Ring buffer overrun.** KMES overwrote events before eventd read
  them. The most serious case: it means audit events were lost
  irrecoverably. [*gap.a-ring-buffer-overrun-is-recorded-as-a-gap]
- **Structural drops.** KMES declined to write an event that exceeded
  its size limits. [*gap.a-structural-drop-is-recorded-as-a-gap]
- **Downtime.** Events emitted while eventd was not running.
  [*gap.events-lost-while-eventd-was-not-running-are-recorded-as-a-gap]

All three appear identically at the consumer, which is why the record
says what was lost rather than why.
[*gap.a-gap-record-says-what-was-lost-not-why]

## Gap records

On detecting a jump, the drain thread generates a gap record, an
`eventd.events.lost` event, carrying:

- the CPU identifier [*gap.a-gap-record-carries-the-cpu-identifier]
- the first missing sequence number, the last seen plus one
  [*gap.the-first-missing-sequence-is-the-last-seen-plus-one]
- the last missing sequence number, the revealing event's minus one
  [*gap.the-last-missing-sequence-is-the-revealing-events-minus-one]
- the count of missing events
  [*gap.a-gap-record-carries-the-count-of-missing-events]
- the timestamp of the last event successfully processed on this CPU,
  where one is known
  [*gap.a-gap-record-carries-the-last-processed-timestamp-where-known]

"Known" means the drain thread saw that event in this run. A gap found
during restart reconciliation whose preceding events are accounted only
by a committed receipt range carries no preceding timestamp, even though
those events are stored: a receipt proves the sequence was accounted
for, and eventd does not read the stored event back to recover its time.

Its own timestamp, `event.time`, is the timestamp of the event that
revealed the gap, not the moment eventd noticed. The lost events were
therefore written between the last processed timestamp and the record's
own, and the record sorts beside the event that revealed it.
[*gap.a-gap-records-time-is-the-revealing-events-timestamp]

The record is written into the shard database through the normal write
path — handed to the same writer thread, batched with ordinary events,
committed in the same transaction.
[*gap.a-gap-record-is-committed-through-the-normal-write-path] That
transaction's receipt range covers the missing sequence interval as well
as any real event rows, so a restart never emits the same gap twice
(§3.1).
<!-- lint-disable-next-line line-length -->
[*gap.a-gaps-receipt-range-covers-the-missing-interval-so-a-restart-never-repeats-it]
It is not emitted through KMES, which would be circular: the mechanism
for recording that the event transport lost something cannot depend on
that transport. [*gap.a-gap-record-is-never-emitted-through-kmes]

The gap details are stored as a MessagePack map in the `payload` column
(§3.2), under the catalog's `buffer.cpu` and `loss.*` fields, and gap
records are queryable exactly like any other event.
<!-- lint-disable-next-line line-length -->
[*gap.gap-details-are-a-messagepack-payload-and-gaps-are-queryable-like-any-event]

## The CPU column

A gap record populates `cpu_id` and leaves the other KMES header columns
— `sequence`, `origin_class`, the identity GUIDs — null (§3.1).
[*gap.a-gap-record-sets-cpu-id-and-leaves-the-other-header-columns-null]

`cpu_id` is populated deliberately, and it is the one place a synthetic
event carries a header field.
[*gap.gap-records-are-the-only-synthetic-events-with-a-header-field]
Without it, `EVENTS WHERE event.cpu == 3`
would return every event from CPU 3 *except* the record saying that
events from CPU 3 went missing — which is the one record such a query
most needs to return.

`sequence` stays null because a gap record has no place in the sequence:
it describes numbers that were skipped, and giving it one of them would
make it indistinguishable from the event that was lost. It is also what
keeps gap records distinct from real events; `receipt_ranges` records
the accounted sequence interval separately (§3.1).

## Lapping

If `read_pos` falls behind `tail_pos`, the consumer has been lapped and
the PSPK read protocol advances it to `tail_pos`.
[*gap.a-lapped-reader-is-advanced-to-tail-pos]

Lapping needs no special handling here. The next event read is the
oldest survivor, its sequence number is far beyond what the thread
expected, and ordinary gap detection records the difference.
[*gap.lapping-is-recorded-by-ordinary-gap-detection] The
lapping case and the restart case produce the same record by the same
path.
