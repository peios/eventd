---
title: Component Boundaries
description: The responsibilities eventd owns and the interfaces it shares
  with kernel, service-management, and configuration components.
---

eventd owns persistence and authorized queries for events, logs, and
metrics. Event production, kernel transport, and service supervision remain
separate responsibilities. These boundaries determine where a failure
must be investigated.

The responsibilities are divided as follows:

| Concern | Owner |
|---|---|
| Event emission and ring-buffer transport | Kernel mediated event subsystem (KMES) |
| Event type names and payload meanings | The emitting component |
| Token and access-check primitives | Kernel access control subsystem (KACS) |
| Registry storage and access | Layered configuration subsystem (LCS) and loregd |
| Service lifecycle and output forwarding | peinit |
| Persistent stores and query execution | eventd |
| Shared userspace wire interfaces | Peios userspace protocols specification (PSPU) |

## Storage boundary

eventd uses SQLite databases for its stores.
[*priorart.eventd-stores-in-sqlite-not-a-proprietary-binary-format]
Clients use the query socket rather than opening those files. Opening a
store directly bypasses eventd's record and field authorization.

The database schemas describe eventd's implementation. They are not an
application interface or a substitute for the query protocol.

## Delivery boundary

Event receipts account for committed event sequences. Log and metric
sockets provide best-effort ingestion and do not acknowledge persistence
to the sender. A successful send therefore does not establish that a
record reached durable storage.

Kernel rings are finite and do not survive reboot. Stored history is
bounded by retention and available storage. Neither boundary provides an
unlimited audit archive.
