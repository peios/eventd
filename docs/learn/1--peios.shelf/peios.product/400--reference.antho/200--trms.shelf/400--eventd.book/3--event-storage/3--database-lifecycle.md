---
title: Database Lifecycle
description: The event store directory — naming, creation, opening an active
  shard, quarantine and historical shards.
---

## The event store directory

Every shard database and the metadata database live in one directory,
named by `EventStorePath` (§A).
<!-- lint-disable-next-line line-length -->
[*eventdb.shards-and-the-metadata-database-live-in-the-eventstorepath-directory]
There is no compiled-in default: a
missing or invalid value is a startup failure, and eventd writes event
databases nowhere else.
<!-- lint-disable-next-line line-length -->
[*eventdb.eventstorepath-has-no-default-and-a-missing-or-invalid-value-fails-startup]

The standard path is `/var/state/eventd/events/`.
<!-- lint-disable-next-line line-length -->
[*eventdb.the-standard-event-store-path-is-var-state-eventd-events] The eventd package
ships `/var/state/eventd/` and its `events/`, `logs/`, and `metrics/`
children.
<!-- lint-disable-next-line line-length -->
[*eventdb.the-package-ships-the-state-directory-and-its-three-store-directories]
The three store directories are also declared as required peinit
provisioned directories, so peinit establishes their descriptor before
eventd starts; `/var/state/eventd/` itself is not a provisioned
directory.
<!-- lint-disable-next-line line-length -->
[*eventdb.the-three-store-directories-are-required-peinit-provisioned-directories]
Each of the four carries an
explicit protected, inheritable descriptor granting full control only
to SYSTEM, Administrators, and the service SID of eventd's own virtual
Service identity, which is what the daemon runs as:
<!-- lint-disable-next-line line-length -->
[*eventdb.store-directories-grant-full-control-only-to-system-administrators-and-eventds-service-sid]

```text
O:SYG:SYD:P(A;OICI;GA;;;SY)(A;OICI;GA;;;BA)(A;OICI;GA;;;S-1-5-80-1963885778-1835409261-1671587836-2279113866-1994761124)
```

A deployment choosing another configured path provisions it with this
descriptor before eventd starts.

eventd does not create store directories.
<!-- lint-disable-next-line line-length -->
[*eventdb.eventd-never-creates-store-directories] It opens every path component
without following symbolic links, retains the resulting directory
descriptor, and opens, creates, renames and quarantines database, WAL
and shared-memory files relative to that descriptor.
<!-- lint-disable-next-line line-length -->
[*eventdb.store-files-are-handled-relative-to-a-directory-descriptor-opened-without-following-symlinks]
A missing path, a
non-directory component, a symbolic-link component, or a store
directory whose owner, group and DACL are not exactly the descriptor
above — which is what keeps an untrusted principal from replacing
children — is a startup failure.
<!-- lint-disable-next-line line-length -->
[*eventdb.a-missing-non-directory-symlinked-or-weakly-protected-store-path-fails-startup]
SQLite's database, `-wal`, and `-shm` files inherit the directory's
protection.
[*eventdb.database-wal-and-shm-files-inherit-the-directory-protection]

## Naming

Active shards are `shard-NNNN.db`, with the shard index zero-padded to
four digits — the index assigned at startup, which is not a CPU number
(§2.3).
[*eventdb.active-shards-are-named-by-a-four-digit-zero-padded-shard-index]

Starting with more shards than exist creates the new ones.
<!-- lint-disable-next-line line-length -->
[*eventdb.starting-with-more-shards-than-exist-creates-the-new-ones] Starting with
fewer leaves the excess in place: they become historical shards, are
never deleted, and remain available to the query path.
<!-- lint-disable-next-line line-length -->
[*eventdb.starting-with-fewer-shards-keeps-the-excess-as-queryable-historical-shards]

## Creation

A shard database that does not exist, or that holds no schema at all
(what a power cut leaves when it takes the uncheckpointed creating
transaction), is created, in one transaction, with:
[*eventdb.a-shard-with-no-schema-is-created-as-new]

1. WAL mode, `PRAGMA journal_mode=WAL`
   [*eventdb.a-new-shard-is-created-in-wal-mode]
2. `PRAGMA synchronous=FULL`
   [*eventdb.a-new-shard-is-created-with-synchronous-full]
3. the `events`, `event_types`, `receipt_ranges`, and `metadata` tables
   (§3.1) [*eventdb.a-new-shard-is-created-with-all-four-tables]
4. the `idx_events_timestamp` index
   [*eventdb.a-new-shard-is-created-with-the-timestamp-index]
5. the `schema_version` and `created_at` entries
[*eventdb.a-new-shard-is-created-with-schema-version-and-created-at-entries]

## Opening an active shard

1. Open in WAL mode. [*eventdb.an-active-shard-is-opened-in-wal-mode]
2. Set synchronous to FULL.
   [*eventdb.an-active-shard-is-opened-with-synchronous-full]
3. Read and verify `schema_version`. Missing or unrecognized is a
   **startup failure**. No migration is attempted.
[*eventdb.a-missing-or-unrecognised-active-shard-schema-version-fails-startup]
4. Verify structural integrity — the required tables and write-time
   indexes exist. Failing this, with SQLite reporting no corruption, is
   a **startup failure**.
   [*eventdb.an-active-shard-missing-required-tables-or-indexes-fails-startup]
5. If SQLite reports corruption while opening or verifying, quarantine
   and replace (below).
<!-- lint-disable-next-line line-length -->
   [*eventdb.corruption-reported-while-opening-an-active-shard-quarantines-and-replaces-it]
   A database holding schema objects but no `metadata` table with
   entries is not a shard eventd could have written; it is quarantined
   and replaced like a corrupt one.
   [*eventdb.unrecognised-contents-are-quarantined]

Steps 3 and 4 fail rather than repair because an active shard is
required: eventd has no degraded mode that runs without one (§8.2).

## Quarantine

When SQLite reports corruption in a required store, eventd renames the
database aside and starts a fresh one at the original path.
[*eventdb.a-corrupt-required-store-is-renamed-aside-and-replaced-at-its-path]
The main
database file and any matching `-wal` and `-shm` files are renamed with
the suffix `.corrupt.<timestamp_ns>`, all three using the same suffix
from one operation, and a new empty `shard-NNNN.db` is created.
<!-- lint-disable-next-line line-length -->
[*eventdb.quarantine-gives-the-database-wal-and-shm-one-shared-corrupt-timestamp-suffix]

If a target name is taken, eventd appends `.N` with the lowest positive
integer that makes it unique — which happens when two quarantines land
in the same nanosecond, and when a previous quarantine already used the
name.
<!-- lint-disable-next-line line-length -->
[*eventdb.a-taken-quarantine-name-gets-the-lowest-free-positive-integer-suffix]

Quarantining rather than deleting is the point: the corrupt file is the
only copy of whatever it held, recovering data from it is an
administrative operation, and eventd attempts no automatic repair.
[*eventdb.a-corrupt-store-is-never-deleted-or-automatically-repaired]

The corruption is logged and an `eventd.store.quarantined` event is
emitted once a shard is available to write it to (§9.2).
<!-- lint-disable-next-line line-length -->
[*eventdb.quarantine-is-logged-and-emits-a-storage-error-event-once-a-shard-is-writable]

## Historical shards

A historical shard is never required for startup.
<!-- lint-disable-next-line line-length -->
[*eventdb.a-historical-shard-is-never-required-for-startup] If one has a missing
or unrecognized schema version, fails structural verification, or cannot
be opened read-only, eventd logs the error and **excludes it from the
query path for this run** — it does not fail startup and does not
quarantine it.
<!-- lint-disable-next-line line-length -->
[*eventdb.a-bad-historical-shard-is-logged-and-excluded-without-failing-startup-or-quarantine]

The asymmetry is deliberate. An active shard that will not open means
eventd cannot do its job; a historical one that will not open means some
old data is unreadable, which is a smaller problem than refusing to boot
the audit daemon over it.

## Query path discovery

The query path opens every file in the directory matching
`shard-NNNN.db` that has a recognized schema and passes structural
verification — active and historical alike. It assumes no particular
number of them.
[*eventdb.the-query-path-opens-every-valid-shard-file-whatever-their-number]

It explicitly does **not** treat every `.db` file in the directory as a
shard: `eventd-meta.db` is excluded by the naming pattern, along with
anything else that happens to be there.
[*eventdb.the-query-path-ignores-files-not-matching-the-shard-name-pattern]

Each is opened with a read-only connection.
<!-- lint-disable-next-line line-length -->
[*eventdb.the-query-path-opens-shards-with-read-only-connections] Read-only connections in WAL
mode do not contend with the writer's connection.

## Concurrency

Each shard has exactly one read-write connection, owned by its writer
thread — for a historical shard, the retention coordinator (§3.6) — and
any number of read-only connections owned by query handlers.
<!-- lint-disable-next-line line-length -->
[*eventdb.each-shard-has-exactly-one-read-write-connection-owned-by-its-writer]
WAL mode permits concurrent readers alongside one writer without
blocking either.

Writer threads never share connections.
<!-- lint-disable-next-line line-length -->
[*eventdb.writer-threads-never-share-connections] Each creates and owns its own
for the process lifetime, which is what makes the prepared statement in
§2.4 a per-thread object with no locking around it.
[*eventdb.a-writer-creates-and-owns-its-connection-for-the-process-lifetime]
