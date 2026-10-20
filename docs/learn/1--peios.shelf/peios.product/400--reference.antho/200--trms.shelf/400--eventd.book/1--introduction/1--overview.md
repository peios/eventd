---
title: Overview
description: eventd is the single persistent sink for everything a Peios
  system records about itself — the shape of the daemon, and what it is not.
---

eventd is the observability daemon: the single persistent sink for
everything a Peios system records about itself. Events, logs and metrics
all end in eventd, and every query for any of them is answered by it.
[*overview.eventd-stores-and-answers-queries-for-events-logs-and-metrics]

It is one of the platform daemons the service manager starts at boot,
configured as Critical. Production deployment requires signing at the
trusted computing base (TCB) level; a development build alone does not
establish that signing qualification.
<!-- lint-disable-next-line line-length -->
[*overview.eventd-is-a-boot-started-critical-platform-daemon-signed-at-tcb-level]

The three data types are different and eventd treats them
differently at every layer.

**Events** are structured, typed records carrying identity stamps the
kernel applied and an emitter could not influence. They arrive through
KMES, in per-CPU shared-memory ring buffers, and eventd is their primary
consumer. They are the audit and security telemetry, and losing one is a
real failure — so the event path is the one with sequence numbers, gap
detection, per-transaction durability, and an `eventd.events.lost`
record written for detected sequence loss when storage is writable.

**Logs** are text: a line a program wrote, with light metadata attached.
They arrive on a datagram socket, mostly from the service manager
forwarding what it read from a service's standard output and standard
error. The ingestion path is best effort and does not provide the event
store's receipt accounting.

**Metrics** are numeric measurements over time — dense series rather
than discrete occurrences. They arrive on a second datagram socket,
pushed by whatever is doing the measuring. eventd is a sink, not a
collector: it scrapes nothing and polls nothing.

## The shape of the daemon

Three ingestion paths, three storage engines, one query surface.

The event path runs one drain thread per CPU, each attached to one ring
buffer, handing events over a bounded channel to a writer thread that
batches them into a SQLite shard. Shards are independent — separate
files, separate write-ahead logs, and separate writer threads — so shards
can make independent write progress
(§2.3).

The log and metric paths each run a single thread that reads datagrams
and writes them to one database each. They do not share the event
writers' SQLite connections.

Underneath all three sits a decision that shapes the event pipeline
entirely: **the KMES ring buffers are the only buffer.** eventd holds no
large intermediate queue. Events move from the ring buffer through a
small bounded handoff straight into a transaction, and when the writer
falls behind, backpressure propagates backward until the ring buffer
absorbs it — and when the ring buffer cannot, the loss is detected and
recorded rather than hidden (§2.5).

Adaptive indexing watches which fields event queries filter on and
maintains indexes for them, shedding those indexes under write pressure
because ingestion throughput outranks query latency (§3.4). Repeated wide
metric-window queries can seed bounded adaptive rollups, but eventd proves
each cached result current against raw samples and never maintains it on the
ingestion path (§5.6).

Everything eventd holds is readable only through access checks that KACS
performs, per event type, per log origin, per metric name, and per field
within a record (§7).

## What eventd is not

**It is not a log framework.** eventd stores lines; it does not parse
them, does not understand severity beyond a single error flag, and does
not care whether the text happens to be JSON.
[*overview.eventd-stores-log-lines-without-parsing-them]

**It does not collect other services' measurements.** eventd does not
scrape endpoints or poll services for metrics. It reads `/proc` for its
boot identifier and records its own health (§5.7).
[*overview.eventd-reads-no-proc-scrapes-no-endpoint-and-polls-no-service]
External producers measure and push their own samples.

**It is not a tracing system.** Distributed tracing is out of scope
entirely. [*overview.eventd-implements-no-distributed-tracing]

**It exposes committed history.** A consumer that needs direct event
transport can attach to kernel messaging and event subsystem (KMES) rings.
eventd adds storage and query authorization; records become queryable
after their batch commits. That delay depends on the workload and storage.
[*overview.eventd-costs-a-batch-commit-interval-of-latency-over-kmes]

**It is not the only KMES consumer**, and holds no privileged position
among them.
[*overview.eventd-holds-no-privileged-position-among-kmes-consumers]
