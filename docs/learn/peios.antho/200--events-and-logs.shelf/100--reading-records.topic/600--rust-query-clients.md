---
title: Rust Query Clients
type: concept
description: Choosing finite, streaming, or wire-level eventd client APIs.
---

The `eventd-client` Rust crate provides query framing and client lifecycle
handling. It shares access-right constants with the daemon so that clients
can describe policy without implementing the authorization decision.

## Finite queries

`query` returns the complete result after the server's terminal response.
A server error discards the accumulated initial records. The call blocks,
so a graphical client must keep it off its interface thread.

The result is a vector in client memory. Server resource limits do not
make that client allocation a fixed-size buffer. Choose bounded queries
or use the wire interface when an application needs a different storage
strategy.

## Streaming queries

`Tail` reads a `STREAM` query on a worker thread. It reports the complete
initial result, live batches, and the reason the stream ended. Dropping the
handle shuts down the connection and releases the receiving channel.

The channel capacity bounds queued reports, not their total bytes.
A client that stops consuming reports eventually stops reading the socket,
and the server can disconnect it. A client must handle that end state;
reconnecting does not provide a resume cursor.

## Query text and policy

Use the crate's `text` helpers to quote values when constructing queries.
Do not concatenate untrusted strings as query syntax.

The `access` module can describe relevant registry policy to a person.
Only eventd, using kernel access checks and the socket peer token,
authorizes a query. A client's policy preview cannot replace that check.

See the manual's
[client library reference](~peios/eventd/evctl/eventd-client) for lifecycle
and wire details.
