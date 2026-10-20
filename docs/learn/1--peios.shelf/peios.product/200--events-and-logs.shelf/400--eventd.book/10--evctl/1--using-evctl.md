---
title: Using evctl
description: Run eventd queries from a terminal or pipeline, select a stable
  output encoding, and understand when results become visible.
---

`evctl` is eventd's native command-line client. Its normal operation has no
subcommand: the one positional argument is a PSPU observability query.
[*evctl.the-one-positional-argument-is-a-query-with-no-subcommand]

```sh
evctl 'EVENTS kacs.* SINCE 1h ago TAKE 50'
evctl 'LOGS FROM authd ERROR ONLY STREAM'
evctl 'METRIC cpu.usage SINCE 10m ago AVG'
```

Quote the complete query. Without the quotes, the shell can split string
literals or expand `*` patterns before `evctl` sees them. Query syntax and
semantics remain those of PSPU §3.18–§3.27; `evctl` does not maintain a second
flag-based query language.
[*evctl.evctl-uses-pspu-query-syntax-and-has-no-flag-based-query-language]

## Commands and queries

Exact standalone words such as `help` and `version` occupy the command
namespace.
[*evctl.exact-standalone-words-such-as-help-and-version-are-commands]
Anything whose first word is `EVENTS`, `LOGS` or `METRIC`, matched
without regard to ASCII case, is a query.
[*evctl.a-first-word-of-events-logs-or-metric-in-any-ascii-case-is-a-query]
This leaves room for administrative commands without requiring the common
query operation to be written as `evctl query ...`.

The query can instead be read from a UTF-8 file or standard input:
[*evctl.the-query-can-be-read-from-a-utf-8-file-or-standard-input]

```sh
evctl --file denied-events.evq
printf '%s\n' 'EVENTS eventd.* SINCE 1d ago' | evctl -
```

The default socket is `/run/eventd/query.sock`.
[*evctl.the-default-socket-is-run-eventd-query-sock]
`--socket PATH` selects a nonstandard configured socket.
[*evctl.socket-selects-a-nonstandard-query-socket] The server obtains the
caller from the Unix socket peer token and performs every identifier and field
access check; the client never opens an eventd database or substitutes its own
authorization decision.
[*evctl.evctl-never-opens-a-database-or-makes-its-own-authorization-decision]

## Output

Data is written to standard output. Diagnostics and query errors are written
to standard error, so redirecting or piping the result does not mix the two.
[*evctl.data-goes-to-standard-output-and-diagnostics-to-standard-error]

| Format | Selection | Use |
|---|---|---|
| Pretty | `--format pretty` (default) | One self-describing record per line, with terminal emphasis on field names. |
| JSON Lines | `--format jsonl` | One JSON object per record for text pipelines. Binary, extension, and non-finite floating-point values use tagged objects rather than being discarded. |
| MessagePack | `--format msgpack` | A lossless binary sequence. Each record is preceded by a four-byte little-endian payload length. |

[*evctl.pretty-is-the-default-format-with-one-record-per-line]
[*evctl.jsonl-writes-one-object-per-record-and-tags-values-json-cannot-carry]
[*evctl.msgpack-prefixes-each-record-with-a-four-byte-little-endian-length]

Records do not have a uniform schema. A field absent from one line is not
necessarily absent from later lines.
[*evctl.records-in-one-result-do-not-share-a-uniform-schema] Scripts should
select the fields they need in the query or tolerate missing object keys.

## Result commitment

An `ok` response is not, by itself, a completed result. A non-streaming query
only succeeds when eventd sends `end`; a streaming query's initial result only
succeeds when eventd sends `watch` (PSPU §3.16). If eventd reports an error
before that point, every preceding record belongs to an invalid partial result
and must be discarded.
<!-- lint-disable-next-line line-length -->
[*evctl.a-result-commits-only-on-end-or-watch-and-an-earlier-error-invalidates-it]

`evctl` therefore renders initial records into an anonymous temporary spool.
[*evctl.initial-records-are-rendered-into-an-anonymous-temporary-spool]
It publishes the spool to standard output only after `end` or `watch`.
[*evctl.the-spool-is-published-only-after-end-or-watch] The spool bounds heap
use independently of the result count and has no pathname to clean up after a
crash. [*evctl.the-spool-bounds-heap-use-and-has-no-pathname]

After `watch`, new records are written directly as they arrive.
[*evctl.after-watch-new-records-are-written-directly] Closing `evctl`,
including with Ctrl-C, closes the query connection and ends the watch.
[*evctl.closing-evctl-closes-the-query-connection-and-ends-the-watch]

As with every streaming client, a blocked output pipeline eventually applies
socket backpressure.
[*evctl.a-blocked-output-pipeline-applies-socket-backpressure] eventd can
terminate that stream rather than allowing a slow observer to interfere with
ingestion.
