---
title: Crash Recovery and Signals
description: What an ungraceful termination leaves behind, the signals eventd
  handles, and the diagnostic dump.
---

## After a crash

An ungraceful termination — a segmentation fault, a kill, an
out-of-memory kill — leaves four things true.

**The ring buffers are unaffected.** KMES writes regardless of consumer
state, and events emitted while eventd was down accumulate there.
[*crash.events-emitted-while-eventd-is-down-accumulate-in-the-ring-buffers]

**The databases are consistent.** WAL mode guarantees committed
transactions survive, and SQLite rolls back the in-flight batch on the
next open.
<!-- lint-disable-next-line line-length -->
[*crash.committed-transactions-survive-a-crash-and-the-in-flight-batch-rolls-back]

**There can be uncovered sequences.** Events in an uncommitted batch
were not persisted, but some can still survive in KMES. On restart
eventd merges committed receipt ranges from all shards, re-ingests each
uncovered survivor, and writes a gap only for a sequence present in
neither receipts nor the ring (§2.2, §2.5).
<!-- lint-disable-next-line line-length -->
[*crash.restart-re-ingests-uncovered-survivors-and-gaps-only-sequences-in-neither-source]

**Socket-buffered data is gone.** The kernel discards a socket receive
queue on process exit, taking whatever logs and metrics were waiting.
Acceptable by the loss model.
[*crash.socket-buffered-logs-and-metrics-are-lost-in-a-crash]

No manual recovery is needed and none is offered.
[*crash.no-manual-recovery-is-needed-or-offered] eventd restarts,
re-attaches, resumes draining, and records only what was truly missed.
The boot-boundary logic recognizes the restart from committed rows or
receipts rather than a flag written in advance (§3.7) — which is the
point, since a crash is precisely the case where nothing was written in
advance.
<!-- lint-disable-next-line line-length spelling -->
[*crash.a-restart-is-recognised-from-committed-rows-or-receipts-not-an-advance-flag]

## Signals

| Signal | Behavior |
|---|---|
| `SIGTERM` | Begin graceful shutdown (§8.4). |
| `SIGINT` | Begin graceful shutdown. |
| `SIGQUIT` | Write a diagnostic dump to standard error, then begin graceful shutdown. |
| `SIGHUP` | Re-read configuration from the registry (§8.3). |
| `SIGPIPE` | Ignored, so a write to a socket whose peer has gone — a query client that disconnects mid-response — fails with `EPIPE` on that connection instead of ending the daemon. |

[*crash.sigterm-begins-graceful-shutdown]
[*crash.sigint-begins-graceful-shutdown]
[*crash.sigquit-writes-a-diagnostic-dump-then-begins-graceful-shutdown]
[*crash.sighup-re-reads-configuration-from-the-registry]
[*crash.sigpipe-is-ignored]

Every other signal keeps its default behavior.
<!-- lint-disable-next-line spelling -->
[*crash.every-other-signal-keeps-its-default-behaviour]

## The diagnostic dump

`SIGQUIT` writes human-readable text to standard error before step 1 of
the shutdown sequence, so that it reflects the daemon's state while it
is still running rather than while it is tearing down.
[*crash.the-diagnostic-dump-is-written-to-stderr-before-shutdown-step-one]
It includes at
least:

- the current boot ID [*crash.the-dump-includes-the-current-boot-id]
- the active shard count and the readable historical shard count
  [*crash.the-dump-includes-the-active-and-readable-historical-shard-counts]
- the per-CPU committed receipt coverage and highest covered sequence
<!-- lint-disable-next-line line-length -->
  [*crash.the-dump-includes-per-cpu-receipt-coverage-and-highest-covered-sequence]
- the current non-streaming and streaming query counts
  [*crash.the-dump-includes-the-non-streaming-and-streaming-query-counts]
- the metric series cache occupancy
  [*crash.the-dump-includes-the-series-cache-occupancy]
- KMES events discarded for claiming one of the five types eventd
  writes itself (§3.1)
  [*crash.the-dump-includes-kmes-events-discarded-for-a-reserved-type]
- log records discarded for an origin outside the origin grammar, and
  the most recent such origin where one exists (§4.1)
<!-- lint-disable-next-line line-length -->
  [*crash.the-dump-includes-invalid-origin-log-discards-and-the-latest-such-origin]
- metric datagrams missing identity, truncated, denied by name policy or
  rejected by an authorization error
<!-- lint-disable-next-line line-length -->
  [*crash.the-dump-includes-metric-datagrams-rejected-for-identity-truncation-policy-or-authorization]
- the cumulative metric type-mismatch count and the latest conflicting
  metric name and expected and received types, where one exists
[*crash.the-dump-includes-the-metric-type-mismatch-count-and-latest-conflict]
- the last observed write error for each store, where one exists
  [*crash.the-dump-includes-the-last-write-error-per-store]

The format is not a stable machine interface and its wording can change.

Most of what an operator asks of a running eventd — whether the writers
keep up, whether the series cache is thrashing (§5.3), whether query
slots are exhausted (§6.5), whether a store has been failing writes —
is answered over time by its health metrics (§5.7). The dump is for
what those cannot carry: the counts of rejected ingestion input, which
no query client can see (PSPU §3.4), the latest error text and rejected
names, and the receipt coverage. It also still works when the metric
store is the thing that is broken. Standard error is the destination
because peinit captures it, so the dump reaches the log store by the
ordinary path — and reaches standard error directly when the log store
is broken.
