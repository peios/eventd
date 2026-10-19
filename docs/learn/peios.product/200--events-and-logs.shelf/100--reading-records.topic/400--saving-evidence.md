---
title: Saving Query Evidence
type: how-to
description: Preserving a finite query result together with completion and
  visibility context.
---

Keep a query result with its diagnostics and scope so that another reader
can distinguish a completed capture from an empty or failed attempt.

## Before you start

Before capturing records:

- Choose a private directory and unused output filenames.
- Select a finite query without `STREAM`.
- Record the identity, machine, query text, time zone, and client version.

## Capture the result

1. Write standard output directly to the result file and standard error
   to a separate diagnostic file. Avoid a pipeline that can close early.
2. Record the command's exit status immediately after it exits, before
   another command replaces the shell's status value.
3. Inspect the diagnostics and mark a nonzero or interrupted attempt as
   failed. A failed output copy can leave part of a completed result.
4. Keep the exact query, capture times, status, and diagnostics beside
   the result. Record any result-count limit and whether a relative time
   range was used.

`evctl` treats an output pipe closed by its reader as success. A pipeline
that selects only the first records therefore does not establish that the
whole query was captured. Filter a saved result after capture instead.

## Check the result

For a direct capture to a regular file, a zero status means the finite
query and output publication completed. It does not establish complete
system history: authorization, retention, and collection loss still bound
what the result contains.

Keep the original capture private and unchanged. Permission to read a
record does not automatically permit sharing it with another audience.
