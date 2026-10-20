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

The destination directory's native security descriptor and inheritance must
restrict new files to the intended readers. A shell `umask` or `chmod` is not
a substitute for that Peios access policy. Use an existing directory you
control; ask its administrator to establish the policy if needed.

Check that both output names are unused regular-file destinations, not
existing files, symlinks, or pipes. Shell redirection can truncate a file
before the query runs. Allow space for the output and the client's temporary
spool. The process's temporary directory must be writable, suitable for
sensitive data, and support anonymous temporary files (`O_TMPFILE`).

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

For example, after checking the private destination and unused names:

```sh
evctl --format jsonl 'EVENTS kacs.* SINCE 1h ago TAKE 50' > incident-events.jsonl 2> incident-events.err
capture_status=$?
printf 'evctl exit status: %s\n' "$capture_status"
```

This is at most 50 readable matching records since one hour ago, not all
records from that hour. The relative range moves on each attempt. Keep the
start and finish times, query, installed `evctl version`, and status beside
the capture. JSON Lines stores one JSON object per record.

If the shell cannot open a redirection target, the client might not run.
Preserve that error too. For a client failure, consult `evctl(1)` for exit
statuses and inspect both destination and temporary-storage errors. Use new
names for another attempt rather than overwriting its evidence.

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
