---
title: The Logs Table
description: A single database rather than a directory of shards, its schema,
  its write-time indexes, and why there is no adaptive indexing here.
---

The log store is a single SQLite database — not a directory of shards.
[*logs.the-log-store-is-a-single-unsharded-database]
There is no sharding here: one ingestion thread produces the writes, so
splitting the target would give a single writer several files to switch
between rather than several writers working in parallel.

It holds the `logs`, `log_origins`, and `metadata` tables.
[*logs.the-log-store-holds-the-logs-log-origins-and-metadata-tables]

| Column | Type | Contents |
|---|---|---|
| `id` | INTEGER PRIMARY KEY | SQLite rowid, monotonic. |
| `boot_id` | BLOB NOT NULL | 16-byte boot ID GUID in PCDS binary layout. |
| `timestamp` | INTEGER NOT NULL | Nanoseconds since the Unix epoch — the producer's value if it supplied one, otherwise eventd's clock at receipt. |
| `origin` | TEXT NOT NULL | The producer peinit associated with the output pipe — a service, or a hook, health check, reload command or job within one. |
| `is_error` | INTEGER NOT NULL | 1 for standard error or an explicitly marked error, 0 otherwise. |
| `message` | TEXT NOT NULL | The log text. |
| `job_id` | BLOB | 16-byte correlation GUID when the producer supplied one; null otherwise. |

[*logs.id-is-a-monotonic-rowid-primary-key]
[*logs.boot-id-is-a-16-byte-guid-in-pcds-binary-layout]
<!-- lint-disable-next-line line-length -->
[*logs.timestamp-is-epoch-nanoseconds-from-the-producer-or-eventds-receipt-clock]
[*logs.origin-is-the-producer-peinit-associated-with-the-output-pipe]
[*logs.is-error-is-1-for-standard-error-or-an-explicitly-marked-error]
[*logs.message-is-the-non-null-log-text]
[*logs.job-id-is-a-16-byte-correlation-guid-or-null]

The schema is deliberately narrow. A log record is text with light
metadata: what produced it, whether it was an error, when, and
optionally which execution it belongs to. There is no payload blob and
no origin class.
[*logs.a-log-record-has-no-payload-blob-and-no-origin-class]
`origin` is broker-attested rather than a service's
self-assertion: the log socket admits peinit and excludes service-logon
tokens (§7.6).
<!-- lint-disable-next-line line-length -->
[*logs.origin-is-broker-attested-because-the-log-socket-admits-peinit-and-excludes-service-logon-tokens]
The optional correlation key is not an identity.

## What an origin looks like [*logs.the-origin-grammar]

An origin is one or two components (PSPU §3.7):

```text
origin    := component | component "/" producer
producer  := component | component "[" [0-9]+ "]"
component := [A-Za-z0-9_][A-Za-z0-9_.-]*
```

One component is a service — `loregd`, `jellyfin` — and is what a main
process's output carries.
[*logs.a-one-component-origin-names-a-service-and-its-main-process]
Two name a producer **within** a service:
peinit tags a pre-start hook `jellyfin/ExecStartPre[0]`, a reload
command `jellyfin/ExecReload`, a health check `jellyfin/HealthCheck`,
and a submitted job `jobs/<guid>` (peinit TRM §11.1).
[*logs.a-two-component-origin-names-a-producer-within-a-service]
The bracketed
index appears only after a slash, and there is never more than one
slash.
<!-- lint-disable-next-line line-length -->
[*logs.a-bracketed-index-appears-only-after-the-slash-and-there-is-at-most-one-slash]

Ingestion discards a record whose origin is anything else, before the
record reaches a batch (§4.1).
[*logs.a-record-whose-origin-is-outside-the-grammar-is-discarded]
Nothing wider is accepted: `*` would
impersonate the access-control wildcard, and a backslash, a quote or
whitespace would reach into the registry path an origin's descriptor
lives at (§7.2).
[*logs.an-origin-never-accepts-a-wildcard-backslash-quote-or-whitespace]

A program needing more structure than this emits events.

## The origin catalog

`log_origins` has one column, `origin TEXT PRIMARY KEY`.
[*logs.log-origins-has-a-single-origin-text-primary-key-column]
The log writer
inserts a previously unseen origin in the same transaction as its first
log row (§4.1), so every committed row is discoverable immediately.
[*logs.every-committed-log-row-is-discoverable-immediately]

Query planning enumerates origins from this compact catalog rather than
running `DISTINCT origin` over the retained log rows (§6.1).
<!-- lint-disable-next-line spelling -->
[*logs.query-planning-enumerates-origins-from-the-catalogue-not-the-log-rows]

Catalog entries deliberately survive ordinary retention.
<!-- lint-disable-next-line spelling -->
[*logs.catalogue-entries-survive-ordinary-retention]
An origin can
therefore remain discoverable after its final row expires; this safe
superset avoids write work on the ingestion path.

Low-priority maintenance can remove an entry after proving through a
read-only scan that no row refers to it, with the deletion submitted to
the log writer.
<!-- lint-disable-next-line spelling -->
[*logs.maintenance-may-remove-an-unreferenced-origin-through-the-log-writer]
The writer rechecks `NOT EXISTS` in that transaction and removes the
origin from its in-memory set only after commit, so a log arriving
between planning and execution cannot become undiscoverable.
<!-- lint-disable-next-line line-length -->
[*logs.origin-removal-rechecks-not-exists-and-updates-the-cache-only-after-commit]

Catalog pages count toward logical live size.
<!-- lint-disable-next-line spelling -->
[*logs.catalogue-pages-count-toward-logical-live-size]

## `is_error` is an integer here and a boolean there

The column stores 0 or 1; the query language exposes a boolean, and
accepts either `WHERE is_error == true` or `WHERE is_error == 1`
(PSPU §3.22).
[*logs.is-error-is-queried-as-a-boolean-accepting-true-or-1]
`ERROR ONLY` is sugar for the first.
[*logs.error-only-is-sugar-for-is-error-equals-true]

## Write-time indexes

Three indexes are created with the table:
[*logs.three-write-time-indexes-are-created-with-the-table]

- `idx_logs_timestamp` on `logs(timestamp)` — time-range filtering, as
  everywhere.
  [*logs.idx-logs-timestamp-indexes-the-timestamp]
- `idx_logs_origin` on `logs(origin)` — "show me logs from X", which is
  the dominant log query.
  [*logs.idx-logs-origin-indexes-the-origin]
- `idx_logs_job_id` on `logs(job_id) WHERE job_id IS NOT NULL` — a
  partial index for "show me logs for job X".
  [*logs.idx-logs-job-id-is-a-partial-index-on-non-null-job-ids]
  Partial because ordinary
  service output can carry no correlation key, so only correlated lines
  are worth indexing.

The origin index costs write amplification beyond the timestamp index,
and the cost is modest in practice: `origin` has low cardinality, tens
of distinct names on a normal system, so its index pages stay in
SQLite's page cache and insertion stays cheap. The trade is accepted
deliberately — the two dominant log queries must not become full table
scans.

## No adaptive indexing

[*logs.the-log-store-does-not-participate-in-adaptive-indexing]

The log store does not participate in adaptive indexing (§3.4). Its
field set is closed and small, and the three write-time indexes already
cover the access patterns; there is no space of candidate fields for a
policy to discover.

The same follows for query frequency counters: log queries do not
increment them (§6.5).
[*logs.log-queries-do-not-increment-query-frequency-counters]

## Schema version

The log store holds a `metadata` table with the same two-column
structure as a shard's (§3.1).
[*logs.the-log-store-metadata-table-has-a-shards-two-column-structure]
Schema version 1 comprises `logs`,
`log_origins`, and `metadata`; its version number is in §B.
[*logs.log-store-schema-version-1-comprises-logs-log-origins-and-metadata]

eventd checks it at startup and applies the lifecycle rules of §4.3,
and does not migrate.
[*logs.the-log-store-schema-is-checked-at-startup-and-never-migrated]
