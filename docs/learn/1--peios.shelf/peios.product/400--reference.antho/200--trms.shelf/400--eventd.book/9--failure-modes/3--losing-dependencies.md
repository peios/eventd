---
title: Losing Dependencies
description: Two of eventd's four dependencies can vanish after startup
  without stopping it — what survives, and what stops working.
---

Two of eventd's four dependencies can disappear after startup without
stopping it, and in both cases the ingestion path survives while the
query path does not. That asymmetry is deliberate: events that are not
collected are gone, and queries that cannot be answered can be asked
again.

## The registry becomes unavailable

If LCS or loregd goes away after eventd has started:

- eventd keeps its last known configuration. Changes are not applied
  until the registry returns.
  [*lostdeps.without-the-registry-the-last-known-configuration-is-kept]
- Descriptor lookups fall back to the cache.
  [*lostdeps.without-the-registry-descriptor-lookups-fall-back-to-the-cache]
  A pattern the cache does
  not hold is **denied**, fail-closed (§7.5).
  [*lostdeps.without-the-registry-an-uncached-descriptor-pattern-is-denied]
- eventd keeps ingesting and keeps serving queries for descriptors it
  already resolved, indefinitely.
<!-- lint-disable-next-line line-length -->
  [*lostdeps.without-the-registry-ingestion-and-queries-on-cached-descriptors-continue-indefinitely]
- When the registry returns, the watch fires and eventd re-reads.
  [*lostdeps.when-the-registry-returns-eventd-re-reads-its-configuration]

This is a degraded state, not a failure. eventd does not exit, and it
does not stop collecting.
[*lostdeps.registry-loss-does-not-make-eventd-exit-or-stop-collecting]

The related case is the **watch failing** while the registry is
otherwise reachable. eventd discards the descriptor cache and operates
fail-closed for new resolutions until the watch is re-established
(§7.5), because a cache it cannot trust to be current would make a
revocation silently ineffective.
<!-- lint-disable-next-line line-length -->
[*lostdeps.a-failed-watch-discards-the-descriptor-cache-and-fails-closed-until-re-established]
`SIGHUP` forces a configuration re-read
in the meantime (§8.3).

## KACS becomes unavailable

If KACS goes away after startup:

- reading the peer token (`KACS_SO_PEER_TOKEN`) fails on new query
  connections, so new queries
  are denied.
  [*lostdeps.without-kacs-new-query-connections-are-denied]
- `kacs_access_check` and `kacs_access_check_list` fail, so a query in
  progress that needs a fresh check is denied.
  [*lostdeps.without-kacs-a-query-needing-a-fresh-access-check-is-denied]
- Cached check results stay valid for the duration of the query that
  obtained them.
<!-- lint-disable-next-line line-length -->
  [*lostdeps.cached-access-check-results-stay-valid-for-the-query-that-obtained-them]
- **Event ingestion is unaffected.** Neither the drain nor the write
  path calls KACS (§8.1).
  [*lostdeps.without-kacs-event-ingestion-is-unaffected]
- Log ingestion is unaffected.
  [*lostdeps.without-kacs-log-ingestion-is-unaffected]
- Metric ingestion continues, but every record needs an `EVENTD_PUBLISH`
  check (§7.6). A record whose verdict is cached for the sender's token
  and name is stored as usual; one needing a fresh check is refused and
  counted as an authorization error, and the rest of its datagram is
  still taken.
<!-- lint-disable-next-line line-length -->
  [*lostdeps.without-kacs-a-metric-record-needing-a-fresh-publish-check-is-refused]

Established event ingestion continues. Queries or metric records that
need a fresh failing access check are rejected. Those operations resume
when KACS checks succeed. [*lostdeps.query-service-resumes-when-kacs-returns]

## KMES

There is no partial mode. eventd attaches to every per-CPU ring buffer
at startup and fails to start if it cannot (§8.2); there is no
subsequent state in which KMES is present but unusable, because the
mapping is established once and the read protocol has no call that can
fail afterward. A ring buffer resize is handled as a generation change,
not as a failure (§2.2).
[*lostdeps.a-ring-buffer-resize-is-a-generation-change-not-a-failure]

## peinit

peinit manages eventd's lifecycle but supplies no runtime service to it.
[*lostdeps.peinit-supplies-no-runtime-service-to-eventd]
The boot ID comes from `/proc/sys/kernel/random/boot_id` (§8.1). There is
therefore no separate peinit-loss mode once eventd is running — peinit
going away means the system is going away.
