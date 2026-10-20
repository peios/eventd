---
title: Storage Failure
description: What a full disk does to each of the three stores, and how
  corruption is detected and contained.
---

## Disk full

When the filesystem holding a store reaches capacity, SQLite writes
fail.

After any write failure consistent with disk-full or quota exhaustion,
eventd schedules an immediate retention run across every store whose
retention is enabled, under the ordinary bounded-batch rules (§3.6,
§4.4, §5.5).
<!-- lint-disable-next-line line-length -->
[*storagefail.a-disk-full-or-quota-write-failure-triggers-an-immediate-retention-run-on-every-enabled-store]
Retention is the only lever eventd has that frees space,
and waiting up to an hour for the next scheduled pass would waste the
window in which recovery is still cheap.

A write refused for want of space emits no `eventd.store.quarantined`;
that event reports only a store found corrupt and quarantined (§2.6).
[*storagefail.a-write-refused-for-want-of-space-emits-no-storage-error]

Capacity diagnostics name the operation that failed. A synthetic-event,
quarantine-report, maintenance, or rollup-maintenance failure is not by
itself a report of a discarded ingestion batch. Actual failed batches
are reported as discarded, with event-batch loss ranges as described below.

### The event store

A failed `INSERT` or `COMMIT` does not crash the writer thread.
<!-- lint-disable-next-line line-length -->
[*storagefail.a-failed-event-insert-or-commit-does-not-crash-the-writer-thread]

The batch is lost, and it is not recoverable: those events were already
consumed from the ring buffer, so KMES does not have them.
[*storagefail.a-failed-event-batch-is-lost] The writer
records the per-CPU sequence ranges of the failed batch in an in-memory
**lost-batch list**, and on its next successful commit emits
`eventd.events.lost` records for every accumulated range before writing
new events. Each record's timestamp is that of the last lost event in
its range, since no later event revealed it (§2.5).
[*storagefail.failed-batch-ranges-are-held-in-an-in-memory-lost-batch-list]
<!-- lint-disable-next-line line-length -->
[*storagefail.lost-batch-gap-records-are-written-before-new-events-on-the-next-successful-commit]

That ordering matters. Emitting the gap records first means the store
never contains events written after a loss without the record of the
loss preceding them.

The writer also logs the failure to standard error immediately —
including the CPU identifiers and sequence ranges — which peinit
captures.
<!-- lint-disable-next-line line-length -->
[*storagefail.a-failed-event-batch-is-logged-to-stderr-with-its-cpus-and-sequence-ranges]
That is the only visibility available while the disk is still
full and the gap record cannot yet be written.

If eventd crashes before the disk recovers, the in-memory list dies with
it. Nothing is silently lost even so: on restart, committed receipt
ranges are reconciled with the current ring survivors. Uncovered
survivors are ingested again, and sequences present in neither source
become ordinary restart gaps (§2.2, §3.7). The record can be coarser —
one gap rather than several — but the loss is still recorded.
<!-- lint-disable-next-line line-length -->
[*storagefail.a-crash-before-disk-recovery-still-records-the-loss-through-restart-reconciliation]

Meanwhile events accumulate in the ring buffers. If the disk stays full
long enough, they overrun and additional loss occurs, detected by the
same mechanism (§9.1).

### The log and metric stores

A failed commit loses the batch. The writer retries on the next one.
<!-- lint-disable-next-line line-length -->
[*storagefail.a-failed-log-or-metric-commit-loses-the-batch-and-the-writer-carries-on]
Acceptable under the loss model, and no lost-batch accounting exists for
either — there is nothing to reconcile against, since neither has
sequence numbers.
[*storagefail.the-log-and-metric-stores-have-no-lost-batch-accounting]

## Corruption

Corruption from a hardware error, a filesystem bug, or an incomplete
write during a kernel crash.

**Detection at startup** is structural: eventd verifies that the
required tables and indexes exist.
<!-- lint-disable-next-line line-length -->
[*storagefail.startup-corruption-detection-checks-that-required-tables-and-indexes-exist]
It does **not** run
`PRAGMA integrity_check`, which scans the entire database and costs time
proportional to its size — unacceptable for a large event store on every
boot. [*storagefail.startup-does-not-run-integrity-check] Corruption that
leaves the schema intact, such as a single bad
page, is found later, at query or write time, when SQLite touches it.

**At startup**, when SQLite reports corruption in a required active
store, eventd quarantines the files, creates a fresh empty database at
the original path, and continues (§3.3).
<!-- lint-disable-next-line line-length -->
[*storagefail.a-corrupt-required-store-at-startup-is-quarantined-and-replaced-with-an-empty-database]
It logs the corruption and
emits `eventd.store.quarantined` once a shard is available to hold it.
<!-- lint-disable-next-line line-length -->
[*storagefail.startup-corruption-is-logged-and-reported-by-a-storage-error-event]

**At write time**, eventd stops writing to the affected database, emits
`eventd.store.quarantined` if it can, quarantines and replaces the
database, and resumes writes to the replacement.
<!-- lint-disable-next-line line-length -->
[*storagefail.write-time-corruption-quarantines-and-replaces-the-database-and-writes-resume]

**At query time**, a handler encountering corruption fails the affected
query with an error.
<!-- lint-disable-next-line line-length -->
[*storagefail.query-time-corruption-fails-the-affected-query] It does **not** return the rows it managed to read:
partial data from a database SQLite has declared corrupt is
indistinguishable from complete data, and silently under-reporting an
audit query is worse than failing it.
[*storagefail.a-query-failed-by-corruption-returns-no-partial-rows]

A missing or unrecognized **schema version** is not corruption.
<!-- lint-disable-next-line line-length -->
[*storagefail.a-missing-or-unrecognised-schema-version-is-not-treated-as-corruption]
It is
not repaired and not migrated: a required store with one fails startup,
and a historical shard with one is excluded from the query path
(§3.3).
[*storagefail.a-required-store-with-a-bad-schema-version-fails-startup]
<!-- lint-disable-next-line line-length -->
[*storagefail.a-historical-shard-with-a-bad-schema-version-is-excluded-from-queries]

Recovering data from a quarantined file is an administrative operation.
eventd never attempts automatic repair, and the quarantined file is the
only copy of whatever it held.
[*storagefail.eventd-never-attempts-automatic-repair-of-a-quarantined-file]
