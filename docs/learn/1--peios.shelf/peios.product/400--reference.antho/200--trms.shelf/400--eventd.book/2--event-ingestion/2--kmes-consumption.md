---
title: KMES Consumption
description: Discovering CPUs and attaching to their rings, the drain threads,
  copying, generation changes and sequence tracking.
---

## Attachment

At startup eventd calls `kmes_attach(KMES_ATTACH_QUERY_SLOTS, …)` to
obtain the logical CPU slot count. It then walks every slot from zero to
one below that count.
[*kmes.startup-queries-the-slot-count-and-tries-every-slot-below-it] A
successful call returns one descriptor for that logical CPU's ring
buffer; `EINVAL` means the slot is a hole and eventd continues rather
than treating it as the end of enumeration.
[*kmes.an-einval-slot-is-a-hole-and-enumeration-continues] Each
successful descriptor is mapped at the size derived from the returned
`capacity`, exactly as PSPK §2.3 defines.
[*kmes.each-descriptor-is-mapped-at-the-size-derived-from-its-capacity]

The attached logical CPU IDs can therefore be sparse. eventd assigns
the successful attachments dense internal ordinals in ascending logical
CPU-ID order for shard routing (§2.3), while preserving the logical
`cpu_id` everywhere externally visible: event rows, receipt ranges, gap
records, and diagnostics.
<!-- lint-disable-next-line line-length -->
[*kmes.sparse-cpus-get-dense-ordinals-internally-but-keep-their-logical-cpu-id-externally]

Attachment requires SeSecurityPrivilege in the effective token, which is
the privilege that grants an unfiltered view of every event on the
system. [*kmes.attachment-requires-sesecurityprivilege] eventd holds it
because it is the party that then applies per-event access control on
everything it stores (§7).

Discovering zero CPUs is a startup failure (§8.2).
[*kmes.discovering-zero-cpus-is-a-startup-failure] There is no
configuration for the CPU count and no way to attach to a subset.
[*kmes.every-attachable-cpu-is-attached-with-no-way-to-choose-a-subset]

## Drain threads

There is one drain thread per CPU, and each reads exactly one ring
buffer. [*kmes.one-drain-thread-per-cpu-reads-exactly-one-ring-buffer]
A drain thread never reads another CPU's buffer, which is what
makes per-CPU sequence tracking a thread-local variable rather than
shared state.

Each thread follows the read protocol PSPK specifies:

- `read_pos` starts at `tail_pos` on first attachment, so eventd begins
  at the oldest surviving event rather than at the newest.
  [*kmes.first-attachment-starts-reading-at-the-oldest-surviving-event]
- The drain loop loads `write_pos` with acquire ordering, checks
  `tail_pos` for lapping, validates the event's structural integrity,
  and advances `read_pos` by `event_size`.
<!-- lint-disable-next-line line-length -->
  [*kmes.the-drain-loop-checks-lapping-validates-each-event-and-advances-by-event-size]
- After reading an event it re-reads `tail_pos` — the torn-read check —
  to detect that KMES overwrote the event while it was being copied.
  [*kmes.tail-pos-is-re-read-after-each-event-to-detect-a-torn-read]
- With nothing to read it uses the notification protocol, setting
  `need_wake` and waiting on the futex, rather than spinning.
  [*kmes.an-empty-buffer-is-waited-on-through-the-futex-not-by-spinning]

## Copying

A drain thread reserves both one slot and the event's byte length in its
shard handoff before copying the event — header and payload — into
process-local memory and advancing `read_pos`.
[*kmes.a-slot-and-the-events-bytes-are-reserved-before-copying-and-advancing]
If either reservation is unavailable, it leaves the event in KMES and
waits; it does not copy or advance first (§2.3).
[*kmes.without-both-reservations-the-event-stays-in-kmes-and-the-drain-waits]

Nothing derived from the mapped region ever reaches a writer thread.
[*kmes.nothing-derived-from-the-mapped-region-reaches-a-writer-thread] The
region is producer-owned and KMES can overwrite any part of it the
moment `read_pos` moves past, so a pointer handed across the channel
would be a pointer into memory that another CPU is entitled to rewrite
before the writer gets to it.

The copy is bounded by the event's own `event_size`, and the drain
thread never reads beyond it.
[*kmes.the-copy-is-bounded-by-the-events-own-event-size]

## Generation changes
<!-- lint-disable-next-line line-length -->
[*kmes.a-drain-thread-checks-the-generation-after-each-cycle-and-follows-a-change]

An administrator changing the ring buffer capacity causes KMES to
replace the buffers, which it signals by changing the `generation`
field. A drain thread checks it after each drain cycle, and on a change:

1. Records the sequence number of the last event it has successfully
   handed off.
   [*kmes.generation-change-records-the-last-handed-off-sequence]
2. Finishes draining the old buffer to its now-frozen `write_pos`,
   updating that sequence number as it goes.
   [*kmes.generation-change-drains-the-old-buffer-to-its-frozen-write-pos]
3. Calls `kmes_attach` again for the same logical CPU, obtains the
   replacement descriptor, and maps it while the old mapping remains
   valid.
<!-- lint-disable-next-line line-length -->
   [*kmes.generation-change-reattaches-and-maps-the-replacement-while-the-old-mapping-is-valid]
4. Scans the new buffer from `tail_pos` for the first event whose
   sequence is greater than the final sequence drained from the old
   buffer, and sets the new `read_pos` there.
[*kmes.generation-change-resumes-at-the-first-sequence-after-the-last-drained]
5. Closes and unmaps the old buffer only after that scan succeeds.
[*kmes.generation-change-unmaps-the-old-buffer-only-after-the-scan-succeeds]
6. Resumes draining from the new buffer.
   [*kmes.generation-change-resumes-draining-from-the-new-buffer]

Each drain thread handles this independently.
[*kmes.each-drain-thread-handles-a-generation-change-independently]
There is no barrier, no
coordination and no shared state, because each attaches only to its own
CPU's buffer — so a resize is a per-CPU event that happens to occur on
every CPU at roughly the same time.

Draining the frozen old buffer before switching prevents loss at the
back of the old generation. The sequence scan prevents duplicates from
the events KMES copied into the replacement. A shrink can still discard
the oldest survivors; that loss appears as an ordinary sequence gap.
[*kmes.survivors-discarded-by-a-shrink-appear-as-an-ordinary-gap]

## Sequence tracking

During an uninterrupted run, each drain thread holds the last sequence
number it saw for its logical CPU.
[*kmes.each-drain-thread-holds-the-last-sequence-seen-for-its-cpu] It
serves gap detection (§2.5) and a generation change.

After a restart, there is deliberately no single resume number. Different
shard transactions can have committed out of sequence, so taking
`MAX(sequence)` could skip an earlier stripe that never committed. The
authority is the union of committed `receipt_ranges` from every readable
active and historical shard (§3.1).
<!-- lint-disable-next-line line-length -->
[*kmes.restart-resumes-from-the-union-of-committed-receipt-ranges-not-one-number]

Startup merges those ranges per `(boot_id, cpu_id)`, scans the surviving
ring records, skips survivors already covered by a receipt, and
re-ingests uncovered survivors.
[*kmes.restart-skips-covered-survivors-and-re-ingests-uncovered-ones] It
emits a gap only for a sequence that is covered by neither a committed
receipt nor a surviving ring record (§8.5).
[*kmes.restart-emits-a-gap-only-for-sequences-neither-receipted-nor-surviving]
With no receipt for a CPU, coverage begins before sequence 1, so
an overwritten prefix is detected correctly.
[*kmes.with-no-receipt-for-a-cpu-coverage-begins-before-sequence-1]

The metadata database's sequence checkpoints and the shutdown event are
diagnostic only (§3.5, §8.4). Neither participates in recovery.
[*kmes.sequence-checkpoints-and-the-shutdown-event-take-no-part-in-recovery]

eventd depends on KMES sequence numbering remaining continuous while the
kernel boot ID is unchanged, and so on KMES being initialized once for
the whole boot and never unloaded or reloaded. A sequence regression
under the same boot ID, other than duplicates deliberately encountered
during replacement scanning, is a fatal incompatibility: eventd stops
rather than confuse two KMES stream lifetimes under one receipt key.
[*kmes.a-sequence-regression-under-the-same-boot-id-stops-eventd]
