---
title: Boot Partitioning
description: Every stored record carries a boot_id — what it is for, where it
  is stored, how uniqueness is assured, and how a boundary is detected.
---

Every record eventd stores — every event, every log line, every raw
metric sample — carries a `boot_id`: a 16-byte GUID identifying the boot
that produced it.
<!-- lint-disable-next-line line-length -->
[*bootpart.every-stored-event-log-line-and-raw-metric-sample-carries-a-boot-id]
Linux assigns it at boot; eventd reads
`/proc/sys/kernel/random/boot_id` at startup, validates the UUID text,
and stores the corresponding PCDS-layout GUID.
<!-- lint-disable-next-line line-length -->
[*bootpart.the-boot-id-is-read-from-proc-at-startup-validated-and-stored-as-a-pcds-guid]

## What it is for

**Disambiguation.** KMES per-CPU sequence numbers restart at zero each
boot. Without a boot ID, sequence 42 from one boot is indistinguishable
from sequence 42 from the next, and every gap calculation across a
reboot would be wrong.

**Lifecycle.** Retention can delete a whole boot as a unit rather than
scanning by timestamp, and a boot is the natural unit to delete: its
events, its gaps and its startup record go together (§3.6).

## Where it is stored

| Store | Column |
|---|---|
| Event | `events.boot_id`, every row |
| Log | `logs.boot_id`, every row |
| Metric | `samples.boot_id`, every row |

[*bootpart.every-events-row-carries-a-boot-id]
[*bootpart.every-logs-row-carries-a-boot-id]
[*bootpart.every-samples-row-carries-a-boot-id]

The log store records it because log output can mean different things
across boots — a service configured differently at boot time says
different things.

The metric store records it **per sample** but does not make it part of
series identity (§5.2).
<!-- lint-disable-next-line line-length -->
[*bootpart.the-boot-id-is-recorded-per-sample-but-is-not-part-of-series-identity]
A time series stays continuous across a reboot,
which is what a chart of CPU usage across a restart should show, and a
query that wants one boot's worth filters for it explicitly
(PSPU §3.25).

## Uniqueness

Within one boot an event is uniquely identified by
`(cpu_id, sequence)`.
[*bootpart.cpu-id-and-sequence-identify-an-event-within-one-boot]
Across boots, `boot_id` supplies the
disambiguating dimension, and the triple
`(boot_id, cpu_id, sequence)` is globally unique.
[*bootpart.the-boot-id-cpu-id-and-sequence-triple-is-globally-unique]

## Detecting the boundary

At startup eventd reads the kernel boot ID, then searches every readable
event shard database — historical shards included — for committed rows
or receipt ranges carrying it.
<!-- lint-disable-next-line line-length -->
[*bootpart.startup-searches-every-readable-shard-including-historical-ones-for-the-current-boot-id]

**No committed rows or receipts for this boot.** This is the boot's
first successful eventd start.
[*bootpart.no-committed-rows-or-receipts-for-the-boot-means-a-first-start]
Coverage for every CPU begins before
sequence 1, the new boot ID is used for subsequent writes to all three
stores, and `eventd.daemon.started` carries `store.restarted` false.
<!-- lint-disable-next-line line-length -->
[*bootpart.a-first-start-covers-from-before-sequence-1-writes-the-new-boot-id-and-emits-restart-false]

**Committed rows or receipts exist.** eventd was previously ready in the
same boot.
[*bootpart.committed-rows-or-receipts-for-the-boot-mean-a-restart]
It merges receipt coverage per `(boot_id, cpu_id)`, reconciles
that coverage with surviving ring records (§2.2), continues under the
same boot ID, and emits `eventd.daemon.started` with `store.restarted`
true.
<!-- lint-disable-next-line line-length -->
[*bootpart.a-restart-merges-and-reconciles-coverage-keeps-the-boot-id-and-emits-restart-true]

The two cases are distinguished by the data itself rather than by any
flag eventd persisted.
<!-- lint-disable-next-line line-length -->
[*bootpart.first-start-and-restart-are-distinguished-by-stored-data-not-a-persisted-flag]
That is the point: a flag would have to be
written at a moment eventd might not reach, and a crash is precisely the
case where it did not.

Committed receipt ranges are the sequence authority.
[*bootpart.committed-receipt-ranges-are-the-sequence-authority]
Committed event
rows still establish that eventd previously ran when no receipt was yet
needed.
[*bootpart.committed-event-rows-alone-establish-a-previous-run]
The metadata database's sequence checkpoints and the previous
`eventd.daemon.stopped` payload are diagnostic — neither is consulted for
recovery (§2.2, §3.5).
<!-- lint-disable-next-line line-length -->
[*bootpart.sequence-checkpoints-and-the-previous-shutdown-payload-are-not-consulted-for-recovery]

The receipt key is sound only while KMES sequence numbers do not restart
under one kernel boot ID, so eventd depends on KMES remaining
initialized for the whole boot. A future KMES ABI can add a stream
generation GUID to make module reloads distinguishable without relying
on that lifecycle invariant.
