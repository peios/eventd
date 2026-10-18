---
title: The Rust Client Library
description: The Rust library a program uses to query eventd, follow it live,
  write query text safely, and tell its user what eventd will let them read.
---

`eventd-client` is a Rust library, in the eventd source tree, for
programs that query eventd: `evctl` itself, Event Viewer, and any
third-party tool. It speaks the query channel of PSPU §3.14–§3.17 and
adds what every client would otherwise have to get right on its own.
[*client.eventd-client-is-the-library-for-programs-that-query-eventd]

It depends on the `peios` crate and nothing of eventd's own, so a
program using it brings in neither SQLite nor eventd's storage.

## Whole results or none

PSPU §3.16 says an `error` that arrives before the terminal message
voids every result message before it. The library keeps that rule
itself, so a program cannot show part of a result as if it were all of
it.

- `query(socket, text)` runs a query that does not stream and returns
  every record once `end` has arrived, or the error and no records.
  [*client.query-returns-a-whole-result-or-none]
- `Tail::start(socket, text, capacity)` follows a `STREAM` query. It
  reports the initial result **once**, complete, when `watch` arrives;
  then each live batch; then why the tail ended. If the query fails
  before `watch`, the only report is that it ended.
  [*client.a-tail-reports-its-initial-result-once-complete]

A query blocks until eventd's terminal message, for as long as eventd's
query timeout (§6.5), so a program with a window to keep responsive runs
it on another thread.

## A tail that stops rather than grows

A tail reads the socket on a thread of its own, as fast as eventd writes,
and passes reports through a channel that holds at most `capacity` of
them. [*client.a-tail-reads-its-socket-on-its-own-thread]

If the program stops taking reports, the channel fills, the thread stops
reading, the socket's buffer fills, and eventd ends the query, since it
never waits for a slow reader (§6.6). The program's next report is that
the tail ended. Nothing grows without bound, in eventd or in the
program. [*client.a-tail-a-program-stops-reading-is-ended-by-eventd]

Dropping the `Tail` closes the connection, which eventd treats as
canceling the query (PSPU §3.14).

## Writing query text

The `text` module writes a value into query text so that it is always
exactly one value (PSPU §3.19).

- `string` writes a quoted string: the quote and backslash escaped, and
  every control character as `\n`, `\r`, `\t` or `\uXXXX`.
  [*client.text-string-escapes-quotes-backslashes-and-controls]
- `binary` writes `x"…"`; `duration` writes `90s`, `2h` and so on, in the
  largest unit that holds the duration exactly.

Every place the language takes an identifier or a pattern also takes a
quoted string, so text a person typed should always go through `string`.
Written bare, an event type called `STREAM` or `WHERE` would be read as
the clause, since the pattern after `EVENTS` is optional. After
`LOGS FROM`, which always takes an origin, the same word is read as the
origin, but quoting is never wrong there either.
[*client.text-a-person-typed-is-always-quoted]

eventd's own tests parse the library's quoted text back through eventd's
parser for strings chosen to break naive quoting, and require the value
to come back unchanged.

## What a caller can read

eventd removes what a caller can not read and says nothing about it
(PSPU §3.28). A program that wants to tell its user "you can't read
events here", rather than show an empty list, has to work it out from
the same descriptors eventd checks (§7). The `access` module does that.
[*client.access-checks-the-descriptors-eventd-checks]

- The rights, eventd's generic mapping, the three namespaces with their
  root GUIDs, and the walk from an identifier to its pattern come from
  this module, and **eventd uses the same definitions**, so a client and
  eventd cannot disagree on them.
  [*client.eventd-and-its-clients-share-the-rights-mapping-and-pattern-walk]
- `field_guid` derives a field's GUID (§7.3). eventd-core computes the
  same derivation for itself, and a test holds the two equal.
- `resolve` finds the descriptor eventd would check an identifier
  against: the identifier, each shorter dotted prefix, then `*`, with an
  origin's producer left off (§7.2).
- `access` checks a descriptor the way eventd does: the calling
  process's own token, eventd's mapping, and the object type list of the
  data type's root and the paths of any fields asked about (§7.3), then
  every field the descriptor grants by name. Records are visible with
  `EVENTD_READ` on the root or on any field, as eventd shows them
  (§7.4).
  [*client.records-are-visible-with-read-on-the-root-or-any-field]
- `field_tree` builds that list for a set of field names, and
  `field_reads` makes the check over it, answering per field; eventd
  uses both, so the tree a client asks about is the one eventd builds.
- `field_grants` lists the fields a descriptor grants by name: the
  object GUIDs of its allowing object ACEs. eventd uses it too, to find
  which identifiers a field-only grant makes visible.
  [*client.field-grants-are-the-object-guids-of-allowing-object-aces]
- `readable` says how much of events, logs or metrics the caller can
  read across **every** pattern written for them, since a more specific
  pattern can grant what `*` denies, and the other way round: everything,
  some (and which patterns it can not read), nothing, or unknown.
  [*client.readable-considers-every-pattern]

When the policy itself can't be read, `readable` says **unknown** and
why, never "nothing": a caller denied the registry keys can still be
granted the data. [*client.unreadable-policy-is-unknown-not-denied]
