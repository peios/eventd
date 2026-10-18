---
title: The Event Shard Schema
description: Event rows, committed sequence receipts, the event-type catalog,
  and the write-time index in every shard.
---

Every shard database holds one `events` table.
[*events.every-shard-database-holds-one-events-table]

| Column | Type | Contents |
|---|---|---|
| `id` | INTEGER PRIMARY KEY | SQLite rowid, monotonic within the shard. |
| `boot_id` | BLOB NOT NULL | 16-byte boot ID GUID in PCDS binary layout. |
| `timestamp` | INTEGER NOT NULL | Nanoseconds since the Unix epoch. From the KMES header for a real event; eventd's clock at generation for a daemon-wide synthetic one; the revealing event's timestamp for a gap record (§2.5). |
| `cpu_id` | INTEGER | From the KMES header. Null for daemon-wide synthetic events; populated for gap records (§2.5). |
| `sequence` | INTEGER | Per-CPU, per-boot sequence from the KMES header. Null for every synthetic event. |
| `origin_class` | INTEGER | 0 userspace, 1 KMES, 2 KACS, 3 LCS, 4 NTFE. From the header, as written. Null for synthetic events. |
| `event_type` | TEXT NOT NULL | From the header; or one of the five types eventd writes itself (§2.6). |
| `effective_token_guid` | BLOB | 16-byte GUID for the effective token at emission. Null for synthetic events; the null GUID when identity was unavailable at emission. |
| `true_token_guid` | BLOB | 16-byte GUID for the process's primary token. Null for synthetic events. |
| `process_guid` | BLOB | 16-byte GUID for the emitting process. Null for synthetic events. |
| `payload` | BLOB | MessagePack. For a KMES event, the raw payload bytes exactly as received. For a synthetic event, a map (§3.2). Null when the event carries none. |

[*events.id-is-a-rowid-monotonic-within-the-shard]
[*events.boot-id-is-a-16-byte-guid-in-pcds-binary-layout]
[*events.timestamp-is-epoch-nanoseconds-from-the-header-or-eventds-clock]
<!-- lint-disable-next-line line-length -->
[*events.cpu-id-is-null-for-daemon-wide-synthetic-events-but-set-for-gap-records]
<!-- lint-disable-next-line line-length -->
[*events.sequence-is-the-per-cpu-per-boot-header-sequence-and-null-for-synthetics]
<!-- lint-disable-next-line line-length -->
[*events.origin-class-is-0-userspace-1-kmes-2-kacs-3-lcs-and-null-for-synthetics]
[*events.event-type-is-the-header-type-or-one-of-the-five-eventd-types]
[*events.effective-token-guid-is-the-effective-token-at-emission]
[*events.true-token-guid-is-the-primary-token-and-null-for-synthetics]
[*events.process-guid-is-the-emitting-process-and-null-for-synthetics]
[*events.payload-is-the-raw-kmes-bytes-a-synthetic-map-or-null]

## Header fields are columns

Every KMES header field is extracted into its own column rather than
left inside the payload blob.
<!-- lint-disable-next-line line-length -->
[*events.every-kmes-header-field-is-extracted-into-its-own-column] That is what lets a predicate on
`emitter.process.guid` or `event.type` become a SQL comparison rather
than a decode of every candidate row, and it is what makes those fields
indexable by ordinary column indexes (§3.4).

The column names are internal to the store. A query names a header
field by its path from PSPU §3.22, and eventd reads it from its column:

| Field | Column |
|---|---|
| `event.time` | `timestamp` |
| `event.sequence` | `sequence` |
| `event.cpu` | `cpu_id` |
| `event.type` | `event_type` |
| `event.boot.guid` | `boot_id` |
| `emitter.class` | `origin_class` |
| `emitter.token.guid` | `effective_token_guid` |
| `emitter.true-token.guid` | `true_token_guid` |
| `emitter.process.guid` | `process_guid` |

A column's name is not a field: `cpu_id` in a query names a payload
field, as any other name outside these nine does.

`event_type` is the sole discriminator between real and synthetic
records. [*events.event-type-alone-distinguishes-real-from-synthetic-records]
No record-type column exists, because the five types eventd writes
itself already partition the type namespace and a second column would
be a second thing to keep consistent.

The partition holds because eventd reserves those five names, and only
those (§2.6). A KMES event whose type is one of them is not stored:
eventd counts it and receipts its sequence, so it is neither a row nor a
gap.
<!-- lint-disable-next-line line-length -->
[*events.a-kmes-event-typed-as-one-of-the-five-eventd-types-is-counted-and-not-stored]
Any other type, including any other under `eventd.`, is stored as
usual. The count appears in the diagnostic dump (§8.5).

## The payload is not touched

For a KMES event the payload column holds the bytes KMES delivered,
unmodified. [*events.a-kmes-payload-is-stored-exactly-as-delivered]
eventd does not decode them on the write path, does not
re-encode them, and does not validate them beyond what the ring-buffer
protocol already checked.
[*events.the-write-path-never-decodes-re-encodes-or-validates-a-payload]

The payload is a MessagePack value whose schema belongs to the emitting
subsystem, and eventd has no catalog of those schemas. It decodes on
the *read* path when a query needs a payload field (§6.1), which is also
the only point at which the flattening rules of PSPU §3.22 apply.
[*events.payloads-are-decoded-and-flattened-only-on-the-read-path]

Storing the bytes verbatim is also what keeps a payload value at a
header field's path recoverable: the value, or the map and everything
beneath it, is suppressed from the query surface but remains in the
blob.
<!-- lint-disable-next-line line-length -->
[*events.a-payload-field-colliding-with-a-header-name-is-suppressed-but-kept-in-the-blob]

## The event-type catalog

Every shard also holds `event_types`:

| Column | Type | Contents |
|---|---|---|
| `event_type` | TEXT PRIMARY KEY | A concrete event type that has been committed to this shard. |

[*events.event-types-lists-each-concrete-type-committed-to-the-shard]

The writer loads this small set into an in-memory intern table at
<!-- lint-disable-next-line spelling -->
startup. [*events.the-writer-loads-the-type-catalogue-into-memory-at-startup]
A known type adds no catalog statement to an event insert.
<!-- lint-disable-next-line spelling -->
[*events.a-known-type-adds-no-catalogue-statement-to-an-insert] A
 new type is inserted once, in the same transaction as its
first event, and enters the in-memory set only after that transaction
commits.
<!-- lint-disable-next-line line-length spelling -->
[*events.a-new-type-is-catalogued-with-its-first-event-and-interned-after-commit]
A transaction-local pending set prevents later events of that
type in the same batch from repeating the catalog statement; rollback
discards the pending set.
<!-- lint-disable-next-line line-length spelling -->
[*events.a-pending-set-stops-repeat-catalogue-inserts-in-a-batch-and-rollback-discards-it]

Query planning unions these catalogs across relevant active and
historical shards to discover concrete identifiers for access checks
(§6.1).
<!-- lint-disable-next-line spelling -->
[*events.planning-unions-type-catalogues-across-active-and-historical-shards]
It never needs a `DISTINCT event_type` scan of the events table,
so `idx_events_event_type` remains an adaptive index that can be shed
without breaking planning (§3.4).
[*events.planning-never-scans-the-events-table-for-distinct-types]

Retention records the distinct types touched by each delete batch and
offers orphan checks as low-priority maintenance.
[*events.retention-offers-orphan-type-checks-for-types-its-deletes-touched]
The writer rechecks
`NOT EXISTS` in the deletion transaction, and removes the type from its
in-memory set only after that deletion commits.
<!-- lint-disable-next-line line-length -->
[*events.an-orphan-type-is-rechecked-in-the-deletion-transaction-and-uninterned-after-commit]
A stale catalog row is
safe — it causes an unnecessary access check but cannot expose data — so
an unindexed or pressure-interrupted check is skipped and cleanup
never delays ingestion.
<!-- lint-disable-next-line line-length -->
[*events.an-unindexed-or-interrupted-orphan-check-is-skipped-rather-than-delaying-ingestion]
Catalog pages count toward the store's logical
live size (§3.6), preventing unique-name abuse from escaping the
store-wide size accounting.
<!-- lint-disable-next-line spelling -->
[*events.catalogue-pages-count-toward-the-logical-live-size]

## Committed receipt ranges

Every shard also holds `receipt_ranges`:

| Column | Type | Contents |
|---|---|---|
| `boot_id` | BLOB NOT NULL | 16-byte kernel boot ID. |
| `cpu_id` | INTEGER NOT NULL | Logical KMES CPU identifier. |
| `first_sequence` | INTEGER NOT NULL | First accounted sequence, inclusive. |
| `last_sequence` | INTEGER NOT NULL | Last accounted sequence, inclusive. |

[*events.a-receipt-range-records-the-16-byte-kernel-boot-id]
[*events.a-receipt-range-records-the-logical-kmes-cpu]
[*events.a-receipt-range-first-sequence-is-inclusive]
[*events.a-receipt-range-last-sequence-is-inclusive]

The primary key is `(boot_id, cpu_id, first_sequence, last_sequence)`,
and every row satisfies `first_sequence > 0` and
`last_sequence >= first_sequence`.
<!-- lint-disable-next-line line-length -->
[*events.receipt-ranges-are-keyed-on-all-four-columns-and-must-be-positive-and-ordered]

A receipt says that every sequence in its range was accounted for by
the same transaction: either the real event row was stored or an
`eventd.events.lost` row durably records why it was absent. The receipt is in
that transaction, so its presence proves commit without a global
checkpoint or another fsync.
<!-- lint-disable-next-line line-length -->
[*events.a-receipt-commits-in-the-transaction-that-stores-its-rows-or-gap-record]
Receipt rows survive event retention.
[*events.receipt-rows-survive-event-retention]

Startup unions and merges overlapping or adjacent ranges across every
readable shard (§2.2, §8.5).
<!-- lint-disable-next-line line-length -->
[*events.startup-merges-overlapping-and-adjacent-receipt-ranges-across-readable-shards]
A read-only background pass can prepare a
low-priority compaction plan while the store is quiet. Writer-owned
mutations first insert a merged range and only later delete ranges it
subsumes, including ranges in other shards.
<!-- lint-disable-next-line line-length -->
[*events.receipt-compaction-inserts-the-merged-range-before-deleting-what-it-subsumes]
Insert-before-delete makes
every intermediate state conservative and crash-safe.

Compaction mutations piggyback transactions a writer would commit
anyway; they never cause a standalone commit or fsync, acquire no
hot-path coordination primitive, and can lag indefinitely.
[*events.receipt-compaction-only-piggybacks-on-writer-commits]
Startup
merge correctness never depends on physical compaction.
[*events.startup-merge-correctness-never-depends-on-compaction]

## Identity can be absent two ways

`effective_token_guid`, which a query reads as `emitter.token.guid`,
distinguishes two cases that would otherwise
look alike. **Null** means the record is synthetic and never had an
identity. [*events.a-null-effective-token-guid-means-a-synthetic-record]
The **null GUID** — sixteen zero bytes — means the record is a
real KMES event whose identity was not available at emission time,
because it was emitted before or outside a context that had one.
<!-- lint-disable-next-line line-length -->
[*events.a-null-guid-effective-token-means-a-real-event-emitted-without-identity]

The distinction matters for audit: "eventd wrote this" and "the kernel
emitted this and could not attribute it" are different facts.

## Write-time indexes

One index is created with the table:

- `idx_events_timestamp` on `events(timestamp)`
  [*events.idx-events-timestamp-is-the-one-index-created-with-the-table]

Time-range filtering is the foundational access pattern — nearly every
query carries a `SINCE` — and it is the one index eventd never sheds,
whatever the write pressure (§3.4). Every other index is the adaptive
system's business.

## Schema version

Each shard holds `events`, `event_types`, `receipt_ranges`, and a
`metadata` table:

| Column | Type | Contents |
|---|---|---|
| `key` | TEXT PRIMARY KEY | Metadata key. |
| `value` | TEXT NOT NULL | Metadata value. |

[*events.shard-metadata-key-is-the-text-primary-key]
[*events.shard-metadata-value-is-non-null-text]

with two required entries: `schema_version`, and `created_at` as a UTC
timestamp formatted `YYYY-MM-DDTHH:MM:SSZ`.
<!-- lint-disable-next-line line-length -->
[*events.shard-metadata-requires-schema-version-and-a-utc-created-at] The current version is in
§B.

eventd checks the version at startup and applies the lifecycle rules of
§3.3. It does not migrate: an unrecognized version is a startup failure
for an active shard and an exclusion for a historical one.
[*events.a-shard-schema-is-never-migrated] Migration is
an administrative operation, deliberately not an automatic one — a
daemon that silently rewrote an audit store's schema on first start
after an upgrade would be doing the one thing an audit store must not do
unattended.
