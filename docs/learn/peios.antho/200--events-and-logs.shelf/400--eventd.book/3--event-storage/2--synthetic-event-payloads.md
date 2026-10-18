---
title: Synthetic Event Payloads
description: The MessagePack payload of each of the five event types eventd
  writes about itself, field by field, in the catalog's names.
---

Each of the five synthetic event types (§2.6) carries a MessagePack map
in `payload`, with the schema below.
[*payload.every-synthetic-event-carries-a-messagepack-map] The fields
are catalog fields, defined in eventd's fragment
`/usr/share/evman/eventd.evman` or in the kernel's (PGSS §6.10), and each
is stored as nested maps, one per segment of its path: `store.shard-count`
is the value at `{store: {shard-count: …}}`. A query names a field by its
dotted path, and reads an array field whole.
[*payload.fields-are-nested-by-path-and-queried-by-their-dotted-names]

No field is ever nil. A field with no value is left out of the map
(PGSS §6.5), and the table below says when each one can be.
[*payload.no-field-is-ever-nil-an-absent-value-is-left-out]

None of them is at a header field's path, so none is suppressed. The
boot and the CPU are read from the columns as `event.boot.guid` and
`event.cpu` (§3.1), and no payload repeats the boot.

## `eventd.daemon.started` [*payload.the-eventd-daemon-started-payload-schema]

| Field | Type | Contents |
|---|---|---|
| `store.restarted` | bool | True when committed rows or receipts for this boot already existed at startup; false on the boot's first successful eventd start. |
| `store.shard-count` | unsigned integer | Active shard count after resolving `StorageShards`. |
| `store.resume.cpus` | array of unsigned integer | One entry per logical CPU, in ascending order. |
| `store.resume.sequences` | array of unsigned integer | Parallel to `store.resume.cpus`: for the CPU at the same index, the highest sequence contiguously accounted for after receipt/ring reconciliation, or 0. |
[*payload.startup-restart-is-true-when-the-boot-already-had-committed-rows-or-receipts]
[*payload.startup-shard-count-is-the-resolved-active-shard-count]
[*payload.startup-resume-cpus-and-sequences-give-each-cpus-highest-contiguous-sequence-in-cpu-order]

There is no boot ID in the payload: the record's own `event.boot.guid`
is the boot it describes.
[*payload.daemon-started-carries-no-boot-id-the-boot-is-event-boot-guid]

`store.restarted` is the boot-boundary decision of §3.7 recorded as
data, which makes "did eventd crash during this boot, and how often"
answerable by query rather than by inference from gaps.

## `eventd.daemon.stopped` [*payload.the-eventd-daemon-stopped-payload-schema]

| Field | Type | Contents |
|---|---|---|
| `store.committed.cpus` | array of unsigned integer | One entry per logical CPU, in ascending order. |
| `store.committed.sequences` | array of unsigned integer | Parallel to `store.committed.cpus`: the highest sequence contiguously covered by committed receipts for that CPU this boot, or 0 if none was. |
[*payload.shutdown-committed-cpus-and-sequences-give-each-cpus-highest-contiguous-receipted-sequence]

When eventd cannot read its committed receipts at shutdown, both arrays
are left out and the payload is an empty map. It never writes zeroes in
their place, so a 0 is always a real value.
[*payload.shutdown-leaves-both-arrays-out-when-coverage-cannot-be-read]

Diagnostic only. Startup derives recovery coverage from committed
receipt ranges, never from this payload (§2.2).
[*payload.startup-never-derives-recovery-coverage-from-the-shutdown-payload]

## `eventd.events.lost` [*payload.the-eventd-events-lost-payload-schema]

| Field | Type | Contents |
|---|---|---|
| `buffer.cpu` | unsigned integer | The CPU whose ring the gap was found on. |
| `loss.sequence` | unsigned integer | First missing sequence number. |
| `loss.sequence-last` | unsigned integer | Last missing sequence number. |
| `loss.count` | unsigned integer | How many are missing. |
| `loss.preceding-time` | timestamp | The last event successfully processed before the gap. Absent when none is known. |

[*payload.gap-buffer-cpu-is-the-ring-the-gap-was-found-on]
[*payload.gap-loss-sequence-is-the-first-missing-sequence]
[*payload.gap-loss-sequence-last-is-the-last-missing-sequence]
[*payload.gap-loss-count-is-how-many-sequences-are-missing]
[*payload.gap-loss-preceding-time-is-the-last-event-before-the-gap-or-absent]

The time of the event that revealed the gap is not a payload field: it
is the record's own `event.time` (§2.5).
[*payload.gap-event-time-is-the-revealing-events-timestamp]

The CPU appears both here, as `buffer.cpu`, and in the `cpu_id` column
(§2.5).
[*payload.a-gap-carries-its-cpu-as-buffer-cpu-and-in-the-column] The
column is what a `WHERE event.cpu == N` predicate matches and what a
query presents as `event.cpu`; the payload field is what
`WHERE buffer.cpu == N` matches, and what a reader of the stored payload
sees without joining anything.
<!-- lint-disable-next-line line-length -->
[*payload.an-event-cpu-predicate-matches-the-column-and-buffer-cpu-the-payload-field]

## `eventd.config.changed` [*payload.the-eventd-config-changed-payload-schema]

| Field | Type | Contents |
|---|---|---|
| `config.key.path` | string | Always `Machine\System\eventd`. |
| `config.name` | string | The value's name under `config.key.path`. |
| `config.type-previous` | unsigned integer | The registry type the value had before, as its `REG_*` number: 4 for `REG_DWORD`, 11 for `REG_QWORD`. Absent when no value was stored. |
| `config.value-previous` | unsigned integer | The value before. Present exactly when `config.type-previous` is. |
| `config.type` | unsigned integer | The registry type the value has now, numbered the same way. Absent when the change removed the value. |
| `config.value` | unsigned integer | The value now. Present exactly when `config.type` is. |

[*payload.config-change-key-path-is-always-the-eventd-configuration-key]
[*payload.config-change-name-is-the-value-name-under-the-key-path]
[*payload.config-change-type-previous-is-the-reg-type-number-or-absent]
[*payload.config-change-value-previous-is-absent-when-no-value-was-stored]
[*payload.config-change-type-is-the-reg-type-number-or-absent]
[*payload.config-change-value-is-absent-when-the-change-removed-the-value]

Every value eventd reloads at runtime is a `REG_DWORD` or a `REG_QWORD`
(§A), so both values are integers, and a query can compare them as
numbers.
[*payload.config-values-are-integers]

A side is absent when no value is stored, even though eventd's compiled
default then applies. A stored value of the wrong type is reported as
the type eventd expects, holding the integer it actually uses.
<!-- lint-disable-next-line line-length -->
[*payload.a-wrong-typed-value-reports-the-expected-type-and-the-integer-in-force]

## `eventd.store.quarantined`

[*payload.the-eventd-store-quarantined-payload-schema]

| Field | Type | Contents |
|---|---|---|
| `store.kind` | string | `event`, `log`, or `metric`. |
| `store.shard` | unsigned integer | The shard, for the event store. Absent for the log and metric stores, which are not sharded. |
| `outcome.detail` | string | Human-readable description of what eventd found. |

[*payload.quarantine-store-kind-is-event-log-or-metric]
[*payload.quarantine-store-shard-is-present-only-for-the-event-store]
[*payload.quarantine-outcome-detail-is-a-human-readable-description]

`outcome.detail` is diagnostic text and its wording is not stable.
`store.kind` and `store.shard` are the fields worth alerting on. The
metadata database is never reported here: a damaged one is rebuilt
(§3.5) and writes no record.
