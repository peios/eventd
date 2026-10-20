---
title: Caching
description: An access check is a syscall, and a large result cannot afford
  one per record — the record-level, field-level, and descriptor caches.
---

An access check is a syscall through the full AccessCheck pipeline. A
query returning ten thousand records cannot afford ten thousand of them,
so results are cached — at two levels, plus the descriptor resolution
underneath both.

## Record-level

Verdicts are cached for the life of one query — for a stream, the whole
stream — under the connection's token (§7.1), and keyed by concrete
identifier, not by pattern: two event types under one pattern are two
keys. When a descriptor contains **no object ACEs**, the check is a
plain grant or deny on the root, so every check of an identifier reaches
the same verdict.
<!-- lint-disable-next-line line-length -->
[*accesscache.verdicts-are-cached-per-query-and-keyed-by-identifier-not-pattern]

Each identifier costs one pre-check before any record is read, for the
fields the query references (§7.4 step 4), and one result check for each
distinct field set its records carry (§7.4 step 9). A query returning
ten thousand events across twenty event types, each type's records of
one shape, performs at most forty checks.
<!-- lint-disable-next-line line-length -->
[*accesscache.ten-thousand-events-of-twenty-uniform-types-take-at-most-forty-checks]

## Field-level

When the descriptor **does** contain object ACEs, the verdict depends on
which fields the record carries, since different payloads produce
different object type lists (§7.3). The result check is therefore cached
per `(identifier, field set)`, and is so whatever the descriptor holds.
[*accesscache.a-result-verdict-is-cached-per-identifier-and-field-set]

In practice events of one type carry the same fields, so this is
effectively one result check per event type. Log records have a fixed
field set, so a log query reaches two checks per origin: the pre-check
and one result check.
[*accesscache.a-log-query-takes-two-checks-per-origin] Metric
records vary by series label keys.

The pathological case is an event type whose payload fields differ from
record to record, which produces a distinct field set — and a distinct
cache entry, and a distinct syscall — for each shape encountered.
[*accesscache.each-distinct-field-set-costs-its-own-cache-entry-and-check]

## Descriptor resolution

Resolving a pattern to a descriptor is itself cached, across queries
rather than within one, since it costs a registry read and a hierarchy
walk (§7.2).
[*accesscache.descriptor-resolution-is-cached-across-queries]

eventd watches the security registry subtree and invalidates cached
resolutions and cached check results when a descriptor changes.
[*accesscache.a-descriptor-change-invalidates-cached-resolutions-and-verdicts]
That is
what makes a revocation take effect on the next query rather than at the
next restart. [*accesscache.a-revocation-takes-effect-on-the-next-query]

If the registry watch fails after startup, eventd **discards the
descriptor cache and operates fail-closed for new resolutions** until
the watch is re-established.
<!-- lint-disable-next-line line-length -->
[*accesscache.a-failed-registry-watch-discards-the-cache-and-fails-closed-for-new-resolutions]
A cache it cannot trust to be current is
worse than none: continuing to serve from stale entries would make a
revocation silently ineffective, and the failure would be invisible.
With the cache discarded every resolution is new, so the descriptors
resolved before the failure are not kept either. This is a degraded
state, not a failure — eventd keeps ingesting and keeps answering
queries, which see no records until the watch is re-established (§9.3).
<!-- lint-disable-next-line line-length -->
[*accesscache.a-failed-watch-degrades-ingestion-continues-and-queries-see-no-records-until-it-recovers]

## During a stream

Verdicts reached for a streaming query's initial result set are reused
through the watch phase, with two exceptions.
[*accesscache.initial-stream-verdicts-are-reused-through-the-watch-phase]

A **new concrete identifier** appearing in a streamed batch — an event
type or log origin not present in the initial results — is resolved and
checked before the record or its distinct value is used.
[*accesscache.a-new-identifier-in-a-stream-is-checked-before-it-is-used]
It has never
been authorized, and inheriting a verdict from a sibling pattern would
be a grant nobody made.

A **descriptor change** invalidates the cache as it does anywhere, and
subsequent batches are re-checked against the new one.
[*accesscache.after-a-descriptor-change-stream-batches-are-rechecked]

The **token** is not re-examined.
[*accesscache.a-streams-token-is-never-re-examined] It was captured at
connection (§7.1),
so a client whose memberships change mid-stream continues under what it
connected with, and a client whose access is revoked keeps receiving
records until it disconnects.
[*accesscache.a-stream-keeps-its-connection-time-identity-until-disconnect]
The bound on that exposure is the client's
own connection lifetime, which for a dashboard can be days.

> [!NOTE]
> Re-obtaining the peer token periodically would narrow the window, and
> would also mean a streaming query could change what it returns halfway
> through for reasons the client cannot see. The snapshot is the simpler
> contract and is what PSPU §3.14 states; the exposure is the cost.

## Metric publication

The metric ingestion thread owns a separate bounded cache for
`EVENTD_PUBLISH` checks (§7.6). It is keyed by the conveyed token's
`token_id` and `modified_id` plus concrete metric name. It caches denials
as well as grants, clears as a unit when full, and is invalidated by the
same descriptor generation as the read caches.

This cache is deliberately thread-local. A recurring metric performs a
borrowed string lookup without allocation, shared locking or AccessCheck;
the only per-datagram identity operation left is querying the fresh
token fd's stable statistics before that fd is closed.
[*accesscache.a-recurring-metric-needs-no-lock-and-only-stats-the-token-fd]
