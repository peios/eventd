---
title: Finding Missing Records
type: how-to
description: Checking scope, visibility, retention, and failures before
  interpreting an empty result.
---

Check query scope and collection health before concluding that a record
was never written. Keep the evidence before changing retention or removing
stored files.

## Before you start

Record the incident time, service or event name, and relevant job and boot
identifiers. A service restart creates another run; matching only its name
can mix unrelated attempts. Preserve available records before changing
retention or attempting storage recovery.

For Event Viewer controls and visibility notices, follow [Reading Events and
Logs](~peios/reading-records/reading-events-and-logs). The daemon checks
below apply regardless of the client.

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

## Retention and unavailable history

Read the effective settings and their defaults in eventd's registry manual.
Age and size limits are removal criteria, not a promise of minimum history.
Size pressure can remove records younger than the age limit. Event retention
can remove an earlier boot together with its loss records; log and metric
retention remove older entries or samples.

A query timeout means the query failed to finish. It is not itself evidence
that ingestion lost records. Save the error, then try a narrower time range
to investigate the failing query.

A running daemon can exclude an unreadable or unsupported historical shard
while serving its other stores. Inspect startup diagnostics as well as the
query result. Corruption encountered during a query fails that query;
quarantined stores are not silently repaired into complete history.

Current retention values cannot show which records an earlier configuration
removed. Likewise, a missing loss event cannot account for the final volatile
batch lost during power failure. Keep these limits with the investigation.

## Preserve the store

Do not delete live database files to make a query succeed. Preserve the
query, diagnostics, timestamps, and relevant configuration, then follow
the deployment's recovery procedure. Replacing a corrupt store gives the
service an empty store; it does not recover lost history.

Deleting records leaves pages for reuse and does not necessarily shrink a
database file. eventd does not automatically run `VACUUM`. Do not treat a
logical retention setting as a procedure for repairing or compacting a live
store. Keep quarantined files intact for the administrator handling recovery.

## Check the conclusion

Record which possibilities the evidence supports: query scope, authorization,
retention, collection loss, or unavailable storage. If the checks are
inconclusive, retain that uncertainty. Save a completed query with its
[completion context](~peios/reading-records/saving-evidence) instead of
labeling an empty display as proof that an event never happened.
