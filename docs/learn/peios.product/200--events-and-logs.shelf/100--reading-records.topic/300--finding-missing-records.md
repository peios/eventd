---
title: Finding Missing Records
type: how-to
description: Checking scope, visibility, retention, and failures before
  interpreting an empty result.
---

Check query scope and collection health before concluding that a record
was never written. Keep the evidence before changing retention or removing
stored files.

## Inspect the result

1. Check the query's record kind, name filters, and time range. A relative
   time range changes each time the query runs.
2. Check the command's exit status and diagnostics. A connection or query
   error is not an empty successful result.
3. Confirm the caller's identity and intended read policy. Authorization
   can omit records or fields without disclosing them.
4. Inspect detected-loss and quarantine records when your identity can
   read them. Consult `eventd.events.lost` and `eventd.store.quarantined`
   in evman for their field meanings.
5. Check retention and storage diagnostics. Records removed by retention
   or held in an excluded historical shard do not return through queries.

## Interpret the evidence

The absence of a loss record does not prove complete collection. eventd
cannot count every datagram rejected by a full kernel socket queue, and a
storage failure can prevent the daemon from persisting its own report.

Logs reflect text received from the output broker. Whether a service sends
its output to that broker belongs to the service manager's configuration.
Do not change eventd's read policy to compensate for a producer that never
sent a record.

A logical retention limit is not a hard filesystem quota. SQLite can
retain free pages, and write-ahead logs occupy additional disk space.

## Preserve the store

Do not delete live database files to make a query succeed. Preserve the
query, diagnostics, timestamps, and relevant configuration, then follow
the deployment's recovery procedure. Replacing a corrupt store gives the
service an empty store; it does not recover lost history.
