---
title: Database Lifecycle
description: The metric store file — its path, creation, opening, concurrency,
  and checkpointing.
---

## Path

`MetricStorePath` (§A) names a provisioned directory, conventionally
`/var/state/eventd/metrics`.
[*metricdb.metricstorepath-names-the-metric-store-directory]
The database itself has the fixed name
`metrics.db` inside it. [*metricdb.the-database-file-is-named-metrics-db]
A missing, invalid, or unsafe directory is a
startup failure.
[*metricdb.a-missing-invalid-or-unsafe-directory-is-a-startup-failure]

The directory has the provisioning, descriptor, and path-resolution
requirements defined for the event store (§3.3).
[*metricdb.the-directory-has-the-event-store-directory-requirements]
eventd does not create
it or any parent directory, never follows a symbolic-link component,
and creates or opens the database relative to an already validated
directory handle.
<!-- lint-disable-next-line line-length -->
[*metricdb.the-directory-is-never-created-or-traversed-through-symlinks-and-the-database-opens-relative-to-its-handle]

## Creation

A metric store that does not exist, or whose database holds no schema at
all (what a power cut leaves when it takes the uncheckpointed creating
transaction), is created, in one transaction, with:
[*metricdb.a-metric-store-with-no-schema-is-created-as-new]

1. WAL mode. [*metricdb.a-new-store-is-created-in-wal-mode]
2. `PRAGMA synchronous=NORMAL` — the log store's reasoning, for the same
   reason: metric loss on power failure is acceptable (§4.1).
   [*metricdb.a-new-store-sets-synchronous-normal]
3. The `series`, `samples`, `rollups`, and `metadata` tables (§5.2, §5.6).
[*metricdb.a-new-store-creates-the-series-samples-rollups-and-metadata-tables]
4. Every write-time index.
   [*metricdb.a-new-store-creates-every-write-time-index]
5. The `schema_version` and `created_at` entries.
   [*metricdb.a-new-store-records-schema-version-and-created-at]

## Opening

1. Open in WAL mode with synchronous NORMAL.
   [*metricdb.an-existing-store-opens-in-wal-mode-with-synchronous-normal]
2. Verify the schema version.
   [*metricdb.opening-verifies-the-schema-version]
   Version 1 is migrated transactionally to version
   2 by adding the adaptive-rollup cache.
   [*metricdb.a-version-1-store-is-migrated-transactionally-to-version-2]
   Version 2 is migrated transactionally to version 3 by rebuilding the
   series table under its new uniqueness constraint, foreign keys off for
   the rebuild and every identifier kept; a version 2 store missing a
   required table or index fails startup instead.
   [*metricdb.a-version-2-store-is-migrated-transactionally-to-version-3]
   Missing or unrecognized versions are
   a **startup failure**.
   [*metricdb.a-missing-or-unrecognised-schema-version-is-a-startup-failure]
3. Verify structural integrity — required tables and indexes present.
   [*metricdb.opening-verifies-the-required-tables-and-indexes-are-present]
   Failing this, with SQLite reporting no corruption, is a **startup
   failure**.
<!-- lint-disable-next-line line-length -->
   [*metricdb.a-structural-failure-without-reported-corruption-is-a-startup-failure]
4. On SQLite reporting corruption, quarantine and replace, exactly as
   for a shard (§3.3): matching `-wal` and `-shm` files renamed with the
   same `.corrupt.<timestamp_ns>` suffix, `.N` appended if the name is
   taken, and a fresh empty store created at the configured path.
<!-- lint-disable-next-line line-length -->
   [*metricdb.a-corrupt-store-is-quarantined-and-replaced-with-a-fresh-empty-store]
   A database holding schema objects but no `metadata` table with
   entries is not a store eventd could have written; it is quarantined
   and replaced like a corrupt one.
   [*metricdb.unrecognised-contents-are-quarantined]

The metric store is a required store; there is no degraded mode without
one (§8.2).
[*metricdb.the-metric-store-is-required-and-has-no-degraded-mode]

After opening or creation the series cache is empty and fills on demand
(§5.3).

## Concurrency

Exactly one read-write connection, owned by the metric writer thread,
and any number of read-only query and maintenance connections.
[*metricdb.exactly-one-read-write-connection-owned-by-the-metric-writer]
WAL mode
permits readers alongside the writer. Retention and adaptive rollups plan or
compute with read-only connections and submit bounded commands to the writer;
they never open another read-write connection (§3.6, §5.6).
<!-- lint-disable-next-line line-length -->
[*metricdb.retention-and-rollups-use-read-only-connections-and-submit-commands-to-the-writer]

The single-writer property is load-bearing here in a way it is not for
the other stores: series resolution checks for an existing row and then
inserts, without a transaction spanning both, and only one writer makes
that safe (§5.3).

## Checkpointing

The metric writer checkpoints at `WalCheckpointPages` (§A) in passive
mode, and does not block when readers hold pages.
<!-- lint-disable-next-line line-length -->
[*metricdb.the-writer-checkpoints-passively-at-walcheckpointpages-without-blocking]

As with the log store, the checkpoint is the durability boundary under
`synchronous=NORMAL`, not merely space reclamation (§9.5).
[*metricdb.the-checkpoint-is-the-durability-boundary]
