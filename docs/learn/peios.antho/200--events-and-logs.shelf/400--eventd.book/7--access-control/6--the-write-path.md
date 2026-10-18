---
title: The Write Path
description: Broker-attested log origins and KACS-authorized metric names,
  including the cache that keeps AccessCheck off the steady-state hot path.
---

The three write paths establish provenance differently because their
inputs come from different places.

## Events

Emission is KMES's business. `kmes_emit` and `kmes_emit_batch` require
SeAuditPrivilege, and eventd is not in the admission path — it consumes
what KMES delivers (§2.2).
[*writepath.eventd-is-not-in-the-event-admission-path]

The identity stamps on an event are the kernel's, captured from kernel
state at the write. An emitting process cannot set, influence, or
suppress them.
<!-- lint-disable-next-line line-length -->
[*writepath.event-identity-stamps-are-the-kernels-and-the-emitter-cannot-alter-them]
That is what makes an event's `emitter.process.guid` evidence, and why
a payload value at that path never shadows it (PSPU §3.22).

## Logs use peinit as a broker

Service processes do not reach the log socket.
[*writepath.service-processes-cannot-reach-the-log-socket] eventd replaces the
inherited DACL on `LogSocketPath` with this protected DACL before
its first receive:
[*writepath.the-log-socket-gets-a-protected-dacl-before-the-first-receive]

```text
D:P(D;;0x2;;;SU)(A;;GA;;;SY)(A;;GA;;;OW)
```

It sets the DACL alone. The socket stays owned by the service SID of
eventd's own virtual Service identity, which created it, and the `OW`
allow lets that owner manage it; making SYSTEM the owner would need a
privilege the long-running daemon deliberately does not hold.
[*writepath.the-log-socket-stays-owned-by-eventds-service-sid]

`SU` is the Service logon group, S-1-5-6. Every phase-2 service token,
including a SYSTEM service token, carries it. peinit's bootstrap SYSTEM
token does not. The deny is evaluated before the SYSTEM allow, so a
service cannot write merely because its user SID is SYSTEM; peinit can.
[*writepath.a-system-service-cannot-write-to-the-log-socket-but-peinit-can]

peinit determines `origin` from the output pipe it is draining and can
batch records from several origins in one datagram.
<!-- lint-disable-next-line words -->
[*writepath.a-log-datagram-may-carry-records-from-several-origins] eventd
therefore
does not request or check a KACS token for each log datagram.
[*writepath.eventd-requests-and-checks-no-token-for-a-log-datagram] The socket
admission attests the broker and the broker attests the origin, without
token-fd churn or an AccessCheck on the log hot path.

A log origin proves which service context peinit associated with the
line. It does not prove that the line's message is truthful.

## Metrics carry the producer token

Every authenticated caller can send to the metric socket: eventd replaces
the descriptor it would inherit from its closed runtime directory with
`D:P(A;;GA;;;SY)(A;;GA;;;BA)(A;;GA;;;OW)(A;;FW;;;AU)`.
<!-- lint-disable-next-line words -->
[*writepath.every-authenticated-caller-may-send-to-the-metric-socket]
Who can publish what is decided below, per metric name, against the
token each datagram carries, so a socket that admitted fewer callers
would only leave a grant of `EVENTD_PUBLISH` to anyone else unusable.

The cost of an open socket is that anyone signed in can send datagrams,
and the socket is lossy (§9.1): a caller sending in bulk can crowd out
other producers' samples, though none of its own unauthorized records
is stored. An operator who wants the socket narrower sets another
descriptor on it after eventd starts, for example with `sd set`. eventd
sets its own again each time it starts.
[*writepath.eventd-sets-the-metric-socket-descriptor-again-at-each-start]

A metric producer enables `KACS_SO_PASS_TOKEN` once on its persistent
sending socket. KACS attaches the producer's effective identity to each
datagram as `KACS_SCM_TOKEN`. eventd uses `recvmsg` with room for exactly
one token and no ordinary file descriptors.
[*writepath.metric-recvmsg-has-room-for-one-token-and-no-ordinary-fds]

eventd discards the whole datagram before MessagePack parsing when:

- no token arrived
  [*writepath.a-metric-datagram-without-a-token-is-discarded]
- the data was truncated
  [*writepath.a-metric-datagram-with-truncated-data-is-discarded]
- the ancillary data was truncated
  [*writepath.a-metric-datagram-with-truncated-ancillary-data-is-discarded]
- querying the token or resolving policy failed
<!-- lint-disable-next-line line-length -->
  [*writepath.a-metric-datagram-is-discarded-when-token-query-or-policy-resolution-fails]

For a valid datagram, eventd resolves each record's metric name through
the ordinary hierarchical Metrics descriptor namespace (§7.2) and runs
AccessCheck against the conveyed token for `EVENTD_PUBLISH`.
<!-- lint-disable-next-line line-length -->
[*writepath.each-metric-record-is-checked-for-eventd-publish-against-the-conveyed-token]
A denied
record is discarded; authorized sibling records in the same datagram
continue.
<!-- lint-disable-next-line line-length -->
[*writepath.a-denied-metric-record-is-discarded-and-its-authorized-siblings-continue]

The wildcard Metrics descriptor grants `EVENTD_PUBLISH` to SYSTEM and
Administrators, not Authenticated Users. A package that owns a metric
prefix installs a more-specific descriptor granting its service SID
that right. Existing wildcard descriptors that exactly match eventd's
old read-only default are upgraded in place; administrator-modified
descriptors are never rewritten.

Authorization covers the metric name, not its labels or value. An
authorized producer can create arbitrarily many valid label sets under
that name (§5.3).
[*writepath.publication-authorizes-the-metric-name-not-its-labels-or-value]

## The publication cache

A full AccessCheck is not performed per sample or per datagram.
[*writepath.publication-is-not-checked-per-sample-or-per-datagram] The
metric ingestion thread owns a bounded verdict cache keyed by:
<!-- lint-disable-next-line line-length -->
[*writepath.the-publication-cache-is-keyed-by-token-id-modified-id-and-metric-name]

```text
(token_id, modified_id, concrete metric name)
```

KACS reuses the captured token object while a persistent sender socket
keeps the same effective identity, so ordinary traffic repeatedly hits
the same entry. A hit performs no allocation and no AccessCheck.
[*writepath.a-publication-cache-hit-performs-no-allocation-and-no-accesscheck]

`MetricAuthorizationCacheSize` bounds the total entry count.
<!-- lint-disable-next-line line-length -->
[*writepath.metricauthorizationcachesize-bounds-the-publication-cache-entry-count]
When full,
eventd clears the cache rather than maintaining an LRU list on the write
path. [*writepath.a-full-publication-cache-is-cleared-rather-than-evicted]
This makes churn expensive for the producer causing it without
adding pointer updates to every successful lookup.

The descriptor cache carries a generation. Any security-registry change
advances it and clears all local publication verdicts before they are
reused.
<!-- lint-disable-next-line line-length -->
[*writepath.a-security-registry-change-clears-publication-verdicts-before-reuse]
The cache stores denials as well as grants, so repeatedly sending
an unauthorized name does not repeatedly invoke KACS.
[*writepath.the-publication-cache-stores-denials-as-well-as-grants]

## Rejected input

Missing identity, truncation, denied records, and policy errors increment
in-memory diagnostic counters.
[*writepath.rejected-metric-input-increments-in-memory-diagnostic-counters]
Policy errors can produce rate-limited
standard-error text.
<!-- lint-disable-next-line words -->
[*writepath.policy-errors-may-produce-rate-limited-standard-error-text]
None produces a durable event or log record of eventd's own: doing
work proportional to hostile input would create an amplification path
(PSPU §3.4).
[*writepath.rejected-input-produces-no-durable-event-or-log-record]
The one record a denial can cause is KACS's: under a descriptor whose
SACL audits failure, as every default does (§7.2), the publication check
that denies a name writes a `kacs.audit.access.checked` record. Denials
are cached like grants, so that is one record per token and name until
the cache is cleared, not one per datagram.
