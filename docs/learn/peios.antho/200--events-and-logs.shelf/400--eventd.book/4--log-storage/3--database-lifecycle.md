---
title: Database Lifecycle
description: The log store directory and fixed database file — its creation,
  opening, concurrency, and checkpointing.
---

## Path

`LogStorePath` (§A) names a provisioned directory, conventionally
`/var/state/eventd/logs`.
[*logdb.logstorepath-names-a-provisioned-directory]
The database itself has the fixed name
`logs.db` inside it.
[*logdb.the-log-database-file-is-named-logs-db]
A missing, invalid, or unsafe directory is a startup
failure.
[*logdb.a-missing-invalid-or-unsafe-log-store-directory-fails-startup]

The directory has the provisioning, descriptor, and path-resolution
requirements defined for the event store (§3.3).
[*logdb.the-log-store-directory-has-the-event-store-directory-requirements]
eventd does not create
it or any parent directory, never follows a symbolic-link component,
and creates or opens the database relative to an already validated
directory handle.
<!-- lint-disable-next-line line-length -->
[*logdb.the-directory-is-never-created-symlinks-are-never-followed-and-the-database-is-opened-handle-relative]

## Creation [*logdb.a-log-store-that-does-not-exist-is-created]

A log store that does not exist, or whose database holds no schema at
all (what a power cut leaves when it takes the uncheckpointed creating
transaction), is created, in one transaction, with:
[*logdb.a-log-store-with-no-schema-is-created-as-new]

1. WAL mode, `PRAGMA journal_mode=WAL`
   [*logdb.creation-sets-wal-journal-mode]
2. `PRAGMA synchronous=NORMAL`
   [*logdb.creation-sets-synchronous-normal]
3. the `logs`, `log_origins`, and `metadata` tables (§4.2)
   [*logdb.creation-creates-the-logs-log-origins-and-metadata-tables]
4. the `idx_logs_timestamp`, `idx_logs_origin`, and `idx_logs_job_id`
   indexes
   [*logdb.creation-creates-the-three-write-time-indexes]
5. the `schema_version` and `created_at` entries
   [*logdb.creation-writes-the-schema-version-and-created-at-entries]

## Opening

1. Open in WAL mode.
   [*logdb.opening-uses-wal-mode]
2. Set synchronous to NORMAL.
   [*logdb.opening-sets-synchronous-normal]
3. Verify the schema version. Missing or unrecognized is a **startup
   failure**; no migration is attempted.
<!-- lint-disable-next-line line-length -->
   [*logdb.a-missing-or-unrecognised-schema-version-fails-startup-without-migration]
4. Verify structural integrity — required tables and indexes present.
   Failing this, with SQLite reporting no corruption, is a **startup
   failure**.
   [*logdb.missing-tables-or-indexes-without-reported-corruption-fail-startup]
5. On SQLite reporting corruption, quarantine and replace.
   [*logdb.a-corrupt-log-store-is-quarantined-and-replaced]
   A database holding schema objects but no `metadata` table with
   entries is not a store eventd could have written; it is quarantined
   and replaced like a corrupt one.
   [*logdb.unrecognised-contents-are-quarantined]

Quarantine works exactly as for a shard (§3.3): the database, `-wal` and
`-shm` files are renamed with a shared `.corrupt.<timestamp_ns>` suffix,
`.N` appended with the lowest positive integer if the name is taken, and
a fresh empty log store is created at the configured path.
<!-- lint-disable-next-line line-length -->
[*logdb.quarantine-renames-all-three-files-with-a-shared-corrupt-suffix-and-creates-a-fresh-store]

The log store is a **required** store. There is no degraded mode in
which eventd runs without one (§8.2), which is why steps 3 and 4 fail
startup rather than proceeding without logs.
[*logdb.the-log-store-is-required-and-eventd-never-runs-without-one]

## Concurrency

Exactly one read-write connection, owned by the log writer thread, and
any number of read-only query and maintenance connections.
<!-- lint-disable-next-line line-length -->
[*logdb.one-read-write-connection-owned-by-the-log-writer-and-any-number-of-read-only-ones]
WAL mode
lets readers run concurrently. Retention and catalog cleanup submit
bounded commands to the writer and never open another read-write
connection (§3.6).
<!-- lint-disable-next-line line-length spelling -->
[*logdb.retention-and-catalogue-cleanup-go-through-the-writer-and-never-open-a-read-write-connection]

## Checkpointing

The log writer checkpoints when its write-ahead log reaches
`WalCheckpointPages` (§A), in passive mode, and does not block if
readers hold pages — it keeps writing and retries after a later commit.
<!-- lint-disable-next-line line-length -->
[*logdb.the-log-writer-checkpoints-passively-at-walcheckpointpages-and-retries-after-a-later-commit]

Checkpointing matters more here than in the event store, because
`synchronous=NORMAL` makes the checkpoint the durability boundary rather
than merely a space-reclamation event: data committed since the last
checkpoint is what a power cut takes (§9.5).
