---
title: Power Loss
description: The failure the three stores' durability settings were chosen
  against, and what a restart does afterward.
---

Sudden power loss is the failure the three stores' durability settings
were chosen against, and it is where the hierarchy the whole daemon is
built on becomes visible as three different outcomes.

| Store | Setting | What survives |
|---|---|---|
| Event | `synchronous=FULL` | Every committed transaction. |
| Log | `synchronous=NORMAL` | Transactions up to the last checkpoint. |
| Metric | `synchronous=NORMAL` | Transactions up to the last checkpoint. |

[*powerloss.every-committed-event-transaction-survives-power-loss]
[*powerloss.log-transactions-survive-only-up-to-the-last-checkpoint]
[*powerloss.metric-transactions-survive-only-up-to-the-last-checkpoint]

**The event store** loses only the in-flight batch — the events
accumulated since the last commit, which the adaptive batcher keeps as
small as throughput allows (§2.4).
<!-- lint-disable-next-line line-length -->
[*powerloss.the-event-store-loses-only-the-in-flight-batch] Unlike a process crash, a power cycle
destroys the old KMES rings and changes the boot ID. eventd can neither
recover that batch nor determine its final sequence range, so it cannot
honestly manufacture an `eventd.events.lost` record for it.
[*powerloss.no-gap-record-is-written-for-a-batch-lost-to-power-loss]
The last receipt coverage
and the following boot's `eventd.daemon.started` bound the unknown loss.

**The log and metric stores** can lose everything committed since their
last write-ahead log checkpoint, which under `synchronous=NORMAL` is the
real durability boundary rather than the commit. How much that is
depends on `WalCheckpointPages` and the write rate, and it can be
considerably more than one batch.

The difference is bought and paid for deliberately. `FULL` costs an
fsync on every commit, which at ten thousand events a batch is amortized
and at three events a batch is not — and eventd commits small batches
constantly under light load. The event store pays it because an event
can be an audit record whose absence is itself the finding. The other
two do not, because their loss is defined as acceptable and paying for
durability they do not need would slow the paths that are most likely to
be bursty.

Events are sacred; logs and metrics are important but not fundamental.
Every durability decision in this manual is that sentence applied to a
particular store.

## What restart does

Nothing manual. eventd starts, finds its databases consistent — WAL
recovery is SQLite's, and an incomplete transaction is rolled back on
open — reads the new kernel boot ID and starts that boot's per-CPU
coverage before sequence 1 (§3.7).
[*powerloss.a-new-boots-per-cpu-coverage-starts-before-sequence-1]
It does not compare a new boot's ring
against the previous boot's receipts: those are different sequence
namespaces.
<!-- lint-disable-next-line line-length -->
[*powerloss.a-new-boots-ring-is-never-reconciled-against-the-previous-boots-receipts]

This is the irreducible difference from a process crash. In the latter,
KMES survives and receipt/ring reconciliation can distinguish committed,
recoverable and truly missing sequences (§8.5). Across power loss, the
old volatile source do not exists.
