---
title: What This Manual Is
description: The implementation scope, interface authorities, and limits of
  the eventd technical reference manual.
---

This manual describes the eventd implementation in this repository. It
covers ingestion, storage, queries, authorization, and failure handling.
The source code is authoritative when the description and implementation
disagree.

## Interface boundaries

The Peios userspace protocols specification (PSPU), section 3, defines
log ingestion, metric ingestion, and the query protocol. The Peios kernel
protocols specification (PSPK) defines the kernel event transport.
This manual explains how eventd implements its side of those interfaces.
It does not replace their contracts.

Command options belong to `eventd(8)` and `evctl(1)`. Registry values belong
to the `Machine\System\eventd` regman records. Synthetic event payloads
belong to the eventd evman catalog. The appendices summarize implementation
choices and point to those references.

## Reading the implementation

The workspace separates four responsibilities:

| Crate | Responsibility |
|---|---|
| `eventd-core` | Bounded queues, routing, receipts, and SQLite stores |
| `eventd` | Kernel attachment, ingestion, policy, lifecycle, and queries |
| `eventd-client` | Query wire format, client state, and shared access constants |
| `evctl` | Command-line input, output formatting, and result publication |

The source paths in the repository's documentation source map identify the
implementation areas used to review this manual. Tests establish specific
properties; passing host tests does not qualify kernel integration,
privilege provisioning, power-loss behavior, or deployment performance.

## Numbering and evidence

Chapter numbers and named statement anchors remain stable so that code,
tests, and other manuals can cite them. An anchor identifies a statement;
it is not itself evidence that a runtime test passed.

The manual describes mechanisms and limits without promising a particular
throughput or latency. Those results depend on the kernel, storage,
hardware, configuration, and workload.
