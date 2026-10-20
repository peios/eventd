---
title: The Metadata Database
description: The one database in the store that is not a shard — its tables,
  its concurrency, and why recovering it is cheap.
---

One database in the event store directory is not a shard:
`eventd-meta.db`.
<!-- lint-disable-next-line line-length -->
[*meta.eventd-meta-db-is-the-one-non-shard-database-in-the-event-store-directory]
It holds the state that is global to eventd rather
than to any shard — adaptive index state and diagnostic sequence
checkpoints — and it is the one database that survives shard
reconfiguration untouched.
[*meta.the-metadata-database-survives-shard-reconfiguration-untouched]

It is created on first startup if absent or holding no schema, opened in WAL
mode with
`synchronous=NORMAL`.
[*meta.created-if-absent-and-opened-in-wal-mode-with-synchronous-normal]
It is written at each run of the index policy (§3.4) — every policy
interval, on every applied configuration change and every `INDEX`
command, and once more at shutdown — and read at startup, so per-transaction
durability buys
nothing: losing the counters since the last run costs some adaptation,
not any data.
<!-- lint-disable-next-line line-length -->
[*meta.written-at-each-policy-run-including-config-changes-and-shutdown-and-read-at-startup]

The query path excludes it explicitly, since it is in the same directory
as the shards (§3.3).
[*meta.the-query-path-excludes-the-metadata-database]

## Tables

**`index_counters`** — query frequency per field (§3.4).
[*meta.the-index-counters-table-holds-query-frequency-per-field]

| Column | Type | Contents |
|---|---|---|
| `field_path` | TEXT PRIMARY KEY | Header field or payload path: `event.type`, `access.granted`, `source.name`. A header field's column name written by an earlier eventd is read back as its path (§3.4). |
| `query_count` | INTEGER NOT NULL | Queries filtering on it within the current window. |
| `window_start` | INTEGER NOT NULL | When the window started, nanoseconds since the epoch. |
[*meta.index-counters-field-path-is-a-text-primary-key-naming-a-field-or-payload-path]
[*meta.index-counters-query-count-is-queries-filtering-on-the-field-in-the-current-window]
[*meta.index-counters-window-start-is-nanoseconds-since-the-epoch]

**`desired_indexes`** — the computed desired index set.
[*meta.the-desired-indexes-table-holds-the-computed-desired-index-set]

| Column | Type | Contents |
|---|---|---|
| `field_path` | TEXT PRIMARY KEY | Header field or payload path, read back like a counter's. |
| `priority` | INTEGER NOT NULL | Rank; lower is higher priority. |
| `is_expression` | INTEGER NOT NULL | 1 for a payload expression index, 0 for a column index. |

[*meta.desired-indexes-field-path-is-a-text-primary-key]
[*meta.desired-indexes-priority-ranks-lower-values-as-higher-priority]
<!-- lint-disable-next-line line-length -->
[*meta.desired-indexes-is-expression-is-1-for-a-payload-expression-index-and-0-for-a-column-index]

**`sequence_checkpoints`** — diagnostic only.
[*meta.the-sequence-checkpoints-table-is-diagnostic-only]

| Column | Type | Contents |
|---|---|---|
| `boot_id` | BLOB NOT NULL | The boot the checkpoint applies to. |
| `cpu_id` | INTEGER NOT NULL | CPU identifier. |
| `sequence` | INTEGER NOT NULL | Last committed sequence for that pair when written. |
| `updated_at` | INTEGER NOT NULL | When it was written. |

[*meta.sequence-checkpoints-boot-id-is-the-boot-the-checkpoint-applies-to]
[*meta.sequence-checkpoints-cpu-id-is-the-cpu-identifier]
<!-- lint-disable-next-line line-length -->
[*meta.sequence-checkpoints-sequence-is-the-last-committed-sequence-when-written]
[*meta.sequence-checkpoints-updated-at-is-when-the-checkpoint-was-written]

Primary key `(boot_id, cpu_id)`.
[*meta.sequence-checkpoints-primary-key-is-boot-id-and-cpu-id]
Startup recovery uses committed receipt
ranges and never this table (§2.2).
<!-- lint-disable-next-line line-length -->
[*meta.startup-recovery-never-uses-sequence-checkpoints] The table exists so that an operator
can see what eventd believed at shutdown and compare it with receipt
coverage — the two disagreeing is itself diagnostic.

**`meta`** — key-value. [*meta.the-meta-table-is-a-key-value-store]

| Column | Type | Contents |
|---|---|---|
| `key` | TEXT PRIMARY KEY | Metadata key. |
| `value` | BLOB NOT NULL | Strings as UTF-8 bytes, binary as raw bytes. |

[*meta.meta-key-is-a-text-primary-key]
[*meta.meta-values-store-strings-as-utf-8-and-binary-as-raw-bytes]

with two required entries: `schema_version` (§B) and `created_at` as a
UTC `YYYY-MM-DDTHH:MM:SSZ` string.
[*meta.meta-requires-schema-version-and-a-utc-created-at-string]
The administrative descriptor lives
with every other eventd descriptor at
`Machine\System\eventd\Security\Admin` (§7.2); no access-control state is
stored in this reconstructible database.
[*meta.no-access-control-state-is-stored-in-the-metadata-database]

## Concurrency

One writer connection, owned by the index policy thread. No
other thread opens the database read-write.
[*meta.only-the-index-policy-thread-opens-the-database-read-write]

Query handlers write only to the in-memory counters; the policy thread
flushes them at each interval.
<!-- lint-disable-next-line line-length -->
[*meta.query-handlers-update-in-memory-counters-and-the-policy-thread-flushes-them-each-interval]
Writer threads and query handlers read
the desired sets from memory, never from the database.
<!-- lint-disable-next-line line-length -->
[*meta.writers-and-query-handlers-read-desired-sets-from-memory-not-the-database]

Graceful shutdown
writes `sequence_checkpoints` through the same connection, after policy
activity has stopped (§8.4).
<!-- lint-disable-next-line line-length -->
[*meta.graceful-shutdown-writes-sequence-checkpoints-after-policy-activity-stops]

With a single writer and no other database-level access, SQLite's WAL
mode is the whole of the concurrency control needed.

The policy thread checkpoints the write-ahead log at
`WalCheckpointPages` in passive mode, and does not block when readers
hold pages — the same rule as every other store (§2.4).
<!-- lint-disable-next-line line-length -->
[*meta.the-policy-thread-checkpoints-passively-at-walcheckpointpages-without-blocking]

## Recovery is cheap

If SQLite cannot read the file, or the schema version is missing or
unrecognized, or any required table
or `meta` entry is missing or malformed, eventd logs an error and
**recreates the database from defaults**.
[*meta.an-invalid-metadata-database-is-logged-and-recreated-from-defaults]

This is the opposite of the rule for a shard, which fails startup
(§3.3), and the difference is what is at stake. A shard holds the only
copy of audit data. This database holds an optimization policy and some
diagnostics — all of it reconstructible, none of it irreplaceable.
Losing it costs the adaptation eventd had accumulated, and the counters
begin refilling immediately. Security policy is unaffected because it
lives in the registry.

## Startup

1. Open `eventd-meta.db`, creating it if absent.
   [*meta.startup-opens-eventd-meta-db-creating-it-if-absent]
2. Verify the schema version and required `meta` entries; recreate from
   defaults on failure.
   [*meta.startup-verifies-the-schema-version-and-required-meta-entries]
3. Load `index_counters` into memory.
   [*meta.startup-loads-index-counters-into-memory]
4. Load `desired_indexes` into memory.
   [*meta.startup-loads-desired-indexes-into-memory]
5. Load `sequence_checkpoints`, for diagnostics only.
   [*meta.startup-loads-sequence-checkpoints-for-diagnostics-only]
6. Discover the material indexes in each shard from its schema and
   compare against the desired set.
<!-- lint-disable-next-line line-length -->
   [*meta.startup-discovers-each-shards-material-indexes-and-compares-them-with-the-desired-set]

eventd resumes convergence from wherever each shard happens to be.
[*meta.index-convergence-resumes-from-each-shards-current-state] It
neither drops nor rebuilds indexes at startup: a shard's material set is
a fact to be observed, not a state to be restored.
[*meta.no-index-is-dropped-or-rebuilt-at-startup]
