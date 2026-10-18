---
title: Stored Records
type: concept
description: The three kinds of eventd records and the limits of stored
  history.
---

eventd stores events, logs, and metrics so that you can inspect what a
Peios machine recorded. Each kind answers a different question and has a
different delivery guarantee.

The stores hold these records:

| Kind | Contents | Delivery |
|---|---|---|
| Events | Structured records from kernel rings | Sequence receipts and detected-loss records |
| Logs | Text with an origin and output-stream flag | Best effort through the trusted output broker |
| Metrics | Numeric samples identified by names and labels | Best effort with publication authorization |

A metric series combines a name with its labels. Samples from different
label sets belong to different series even when they share a name.

## Visibility

Queries run with the caller's socket identity. eventd checks access to
record names and individual fields before returning data. It omits denied
records and fields without announcing their existence. An empty result
therefore does not establish that nothing happened.

Use the query client rather than opening the databases directly. Direct
file access bypasses eventd's query authorization.

## Retention and loss

Stored history is finite. Retention removes old records and can remove
records to meet configured size limits. Kernel event rings can overwrite
records before eventd reads them; log and metric sockets can drop input.
A successful send is not a persistence acknowledgment.

The `eventd.events.lost` event describes detected missing event sequences.
It cannot reconstruct their payloads. `eventd.store.quarantined` describes
a corrupt database moved aside. Use the eventd evman records to interpret
the fields of either event.

See [Finding Missing Records](~peios/reading-records/finding-missing-records)
when a result differs from what you expect.
