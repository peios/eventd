---
title: Querying Records
type: how-to
description: Reading finite or streaming results with the evctl query client.
---

Use `evctl` to read events, logs, and metrics from a terminal. It sends one
query to eventd and writes only the records that the caller can read.

## Before you start

Before querying:

- Install the `evctl` client and start the configured eventd service.
- Use an identity permitted to read the requested records.
- Choose the record kind and time range needed for the investigation.

## Run a query

1. Select an `EVENTS`, `LOGS`, or `METRIC` query using the observability
   query language in the Peios userspace protocols specification (PSPU).
2. Pass the complete query as one quoted argument to `evctl`. Quoting
   prevents the shell from expanding patterns or splitting strings.
3. Read standard output for records and standard error for diagnostics.
   Check the exit status before treating a finite result as complete.

For exact flags, output formats, and exit statuses, read `evctl(1)`.
The default socket is `/run/eventd/query.sock`; `--socket` selects the
configured path when the deployment uses another location.

## Simplify an expression that exceeds the limit

eventd rejects expressions deeper than 128 levels with the query error
`expression depth exceeds 128 levels`. The limit applies separately to
nested grouping parentheses and to the structure built from `AND` and
`OR` predicates. A long flat chain can exceed the limit even without
parentheses.

Remove redundant grouping and simplify the predicates before retrying.
The rejection is a query error, not an empty successful result. Check
standard error and the exit status as you would for another query error.

## Check completion

For a finite query, `evctl` holds the initial records in a temporary spool
until eventd confirms completion. A query failure before that confirmation
publishes none of those records. Copying a completed spool to the output
can still fail and leave partial output.

A streaming query confirms its initial result before publishing it, then
prints live records as they arrive. A later failure does not invalidate
records already published. Restarting the query starts a new query; it
does not resume an acknowledged cursor.

An empty successful result means that no matching, readable records were
returned. It does not prove that every record remains available.
