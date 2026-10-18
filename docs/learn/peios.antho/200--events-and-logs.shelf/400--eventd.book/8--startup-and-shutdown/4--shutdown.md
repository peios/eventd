---
title: Shutdown
description: Persisting as much in-flight data as possible without blocking
  indefinitely — the sequence, and the timeout that bounds it.
---

When peinit signals a stop, eventd persists as much in-flight data as it
can without blocking indefinitely.

## The sequence

1. **Stop accepting.** Unlink all three socket paths so no new client
   can reach them, and stop accepting query connections.
<!-- lint-disable-next-line line-length -->
   [*shutdown.all-three-socket-paths-are-unlinked-and-query-connections-stop-being-accepted]
   Existing
   streaming queries are terminated with an error.
   [*shutdown.existing-streaming-queries-are-terminated-with-an-error]
   The log and metric
   socket descriptors stay **open**, and eventd goes on reading them
   until every query connection still being served has ended: a sender
   that reached either socket before its path was unlinked can still
   deliver to it.
   [*shutdown.the-log-and-metric-socket-descriptors-stay-open-after-unlinking]
2. **Drain ingestion.** Shut the log and metric sockets for reading, so
   that a later send fails with `EPIPE` and its sender keeps the
   datagram (peinit buffers it and replays it to the next eventd),
   rather than having it queued where nothing will read it.
<!-- lint-disable-next-line line-length -->
   [*shutdown.a-send-after-the-log-and-metric-sockets-are-shut-for-reading-fails-with-epipe]
   Then read and process the datagrams still in the receive queues, and
   close those descriptors.
<!-- lint-disable-next-line line-length -->
   [*shutdown.queued-log-and-metric-datagrams-are-processed-before-their-sockets-close]
   This is
   bounded by the receive queue, at most `net.unix.max_dgram_qlen`
   datagrams plus one (§4.1), so it completes quickly.
3. **Final event drain.** Each drain thread performs one last drain
   cycle from its ring buffer.
   [*shutdown.each-drain-thread-performs-one-final-drain-cycle]
4. **Final commit.** Every writer commits its current batch immediately,
   whatever its size. The log and metric writers do the same.
   [*shutdown.every-writer-commits-its-current-batch-whatever-its-size]
5. **Record sequence state.** Derive each logical CPU's highest
   contiguously covered sequence from committed receipt ranges and write
   it to `sequence_checkpoints` for diagnostics (§3.5).
<!-- lint-disable-next-line line-length -->
   [*shutdown.each-cpus-highest-contiguously-covered-sequence-is-written-to-sequence-checkpoints]
6. **Emit the shutdown event.** Write `eventd.daemon.stopped` with the
   per-CPU sequences, using the daemon-wide shard assignment rule
   (§2.6). If the receipt ranges could not be read in step 5, the
   record carries no sequences rather than zeroes (§3.2).
   [*shutdown.an-eventd-daemon-stopped-event-carries-the-per-cpu-sequences]
   If no shard is writable, the event is skipped and the failure
   logged to standard error.
<!-- lint-disable-next-line line-length -->
   [*shutdown.with-no-writable-shard-the-shutdown-event-is-skipped-and-the-failure-logged]
7. **Close databases.** Close every connection, writer, and reader.
   SQLite checkpoints the write-ahead log automatically on close.
   [*shutdown.every-database-connection-is-closed-checkpointing-its-wal]
8. **Unmap.** Unmap every ring buffer and close the per-CPU descriptors.
   [*shutdown.every-ring-buffer-is-unmapped-and-its-descriptor-closed]
9. **Exit.**

Steps 1 and 2 are deliberately split. Unlinking the pathnames stops new
senders finding the socket while the descriptors stay open, so whatever
is already queued is still readable — closing them at step 1 would
discard the queue, which is the data last produced and
therefore most likely to explain why the system is being stopped.
Shutting the sockets for reading before the drain is what makes the
drain final: nothing can join the queue behind it to be discarded when
the descriptors close.

The final checkpoint in step 7 matters most for the log and metric
stores, which run `synchronous=NORMAL` and whose durability boundary is
the checkpoint rather than the commit (§4.1).

## A failed event writer

When an event-shard writer exits, it closes its handoff queue and releases
the pending control requests. A caller waiting for a final-commit barrier,
synthetic-event commit or maintenance result then observes failure instead
of waiting for a reply from a writer that has stopped. These requests are
not acknowledged as committed. Producers waiting for queue capacity wake
with a closed-queue error; already admitted handoffs are released once
their producer publishes or cancels them. Healthy writers retain their
normal backpressure and commit acknowledgements.
[*shutdown.a-stopped-event-writer-releases-pending-control-requests]

A reservation racing with closure either joins the admitted handoffs or
fails with a closed-queue error. The final drained check cannot miss a
reservation that succeeds, including on processors with weaker memory
ordering. Closing also wakes threads that are entering a queue wait; it
does not depend on another publication to release them.

## The timeout

Shutdown is bounded by peinit's service stop timeout. If the sequence
has not finished, eventd aborts and exits immediately.
[*shutdown.an-unfinished-shutdown-aborts-at-the-peinit-stop-timeout]

What an aborted shutdown costs:

- **Uncommitted event batches** are lost. Those events remain in the
  KMES ring buffers and are available at the next start, provided they
  have not been overwritten by then.
<!-- lint-disable-next-line line-length -->
  [*shutdown.event-batches-lost-to-an-aborted-shutdown-are-recovered-from-kmes-at-the-next-start]
- **Uncommitted log and metric batches** are lost, which is acceptable
  by design.
  [*shutdown.an-aborted-shutdown-loses-uncommitted-log-and-metric-batches]
- **The diagnostic sequence metadata** can be stale. It does not matter:
  startup derives coverage from committed receipt ranges and reconciles
  it with the ring buffer (§2.2).
<!-- lint-disable-next-line line-length -->
  [*shutdown.stale-sequence-metadata-after-an-aborted-shutdown-does-not-affect-recovery]

Every consequence is one the restart path already handles, which is why
aborting is safe rather than merely tolerable.
