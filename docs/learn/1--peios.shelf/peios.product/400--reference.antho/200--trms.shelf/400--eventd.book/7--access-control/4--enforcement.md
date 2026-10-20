---
title: Enforcement
description: Access control is the third phase of query evaluation — which
  clauses count as referencing a field, and why denial is silent.
---

The order in which eventd evaluates a query is fixed (PSPU §3.18), and
access control is the third phase — before predicates, transforms,
grouping, aggregation, ordering and pagination.
[*enforce.access-control-is-the-third-query-phase-before-predicates] What
follows is the sequence within that phase.

1. **Obtain the caller's token** from the connection (§7.1). Failure
   denies the query.
   [*enforce.the-token-is-obtained-first-and-failure-denies-the-query]
2. **Parse** the query to establish its data sources and filters (§6.1).
   [*enforce.the-query-is-parsed-for-its-sources-and-filters]
3. **Discover the concrete identifiers** the query could touch — event
   type strings, log origin strings, metric name strings.
   [*enforce.the-concrete-identifiers-a-query-could-touch-are-discovered]
   A broad
   selector authorizes nothing by itself: `EVENTS`, `EVENTS kacs.*`,
   `LOGS` without `FROM`, and `METRIC cpu.*` are each resolved identifier
   by identifier.
   [*enforce.a-broad-selector-is-authorized-identifier-by-identifier]
4. **Resolve and check** each discovered identifier: find its descriptor
   by hierarchical matching (§7.2), and build the object type list for
   the fields the query references (§7.3). A query that references none
   asks instead about every field the descriptor's allowing object ACEs
   name, since those can make its records visible though the query names
   none of them; a GUID does not say its path, so each is a level-1 node
   of that list. Call
   `kacs_access_check_list`, and cache the verdict for this
   `(token, identifier, field set)` (§7.5).
   [*enforce.each-discovered-identifier-is-resolved-checked-and-cached]
[*enforce.the-identifier-check-adds-the-fields-the-descriptor-grants-by-name]
5. **Apply record verdicts.** An identifier is invisible when the caller
   can read neither its root nor any field: its records are excluded
   from the logical row set before aggregation, ordering, pagination and
   formatting.
[*enforce.a-root-denied-identifiers-records-are-excluded-before-aggregation]
   One whose root is denied but some field granted stays, for its
   records to be shaped to those fields (step 10).
   [*enforce.an-identifier-with-only-fields-granted-stays-visible]
   For a cross-type
   source, a denied identifier is treated as having no matching data.
   [*enforce.a-denied-cross-type-identifier-has-no-matching-data]
6. **Apply field verdicts to predicates.** Where the query references a
   field in a predicate or shaping clause and a matching identifier does
   not grant it, that identifier's records contribute nothing — exactly
   as if its root had been denied.
   [*enforce.a-denied-referenced-field-excludes-that-identifiers-records]
   The query is **not** rejected.
   [*enforce.a-denied-referenced-field-does-not-reject-the-query]
7. **Cross-type sources** get the same treatment. An invisible
   identifier, or a denied field needed to evaluate the condition, makes
   the condition
   evaluate as though no matching cross-source data existed.
[*enforce.a-denied-cross-source-root-or-field-evaluates-as-no-matching-data]
8. **Execute**, with root filtering already part of the logical row set.
   [*enforce.execution-runs-with-root-filtering-already-applied]
9. **Re-resolve per result identifier.** For each distinct concrete
   identifier in the resulting rows, resolve its descriptor, build the
   object type list with field GUIDs, call `kacs_access_check_list` with
   the token, the descriptor, `EVENTD_READ`, the list and an audit
   context naming the pattern the descriptor was resolved from (below),
   and cache the per-field results.
<!-- lint-disable-next-line line-length -->
   [*enforce.each-result-identifier-is-rechecked-for-eventd-read-with-field-guids]
10. **Shape each record.** Look up the cached results for its
    identifier and fields. Exclude the record entirely if neither the
    root nor any of its fields was granted; otherwise include it, with
    each field only if its node was granted.
    [*enforce.each-record-is-shaped-by-its-identifiers-cached-verdicts]
    A grant of the root alone grants every field, since an ACE with no
    object type applies to the whole tree (§7.3). A grant of some fields
    alone gives records holding exactly those fields, and a record
    carrying none of them is excluded.
    [*enforce.a-field-only-grant-gives-records-holding-exactly-those-fields]

## Which clauses count as referencing a field

Step 6 applies to every clause that reads a value rather than merely
displaying one:

- ordinary `WHERE` predicates
  [*enforce.a-where-predicate-references-its-fields]
- metric label filters in a primary selector
  [*enforce.a-metric-label-filter-in-a-primary-selector-references-the-label]
- `GROUP`, `COUNT BY`, `TOP N BY`, `SORT`, and `DISTINCT` fields
<!-- lint-disable-next-line line-length -->
  [*enforce.grouping-counting-ranking-sorting-and-distinct-fields-are-referenced]
- event and log aggregation arguments — `SUM`, `AVG`, `MIN`, `MAX`
  [*enforce.event-and-log-aggregation-arguments-are-referenced]
- metric transforms and terminal aggregations, all of which read the
  fixed `value` field: `RATE`, `DELTA`, `P50`, `P95`, `P99`, `AVG`,
  `MIN`, `MAX`, `SUM`, `AVG_OVER`, `MIN_OVER`, `MAX_OVER`, `SUM_OVER`
  [*enforce.metric-transforms-and-terminal-aggregations-reference-value]
- a metric boot filter, which reads `boot_id`; and an explicit metric
  type predicate, grouping, sort or distinct, which reads `type`
<!-- lint-disable-next-line line-length -->
  [*enforce.a-metric-boot-filter-references-boot-id-and-type-clauses-reference-type]

`SELECT` is not on this list.
[*enforce.select-does-not-count-as-referencing-a-field] It shapes output
and is applied last, so a
field it omits was still available to every earlier phase — and
conversely, selecting a field the caller can not read removes the field,
not the record.
[*enforce.selecting-an-unreadable-field-removes-the-field-not-the-record]

## Denial is silent, not fatal

Step 6 excludes rather than rejects, and this is the decision most worth
being explicit about, because rejecting is the more informative
behavior and that is exactly the objection to it.

A rejection would tell the caller that some identifier exists, matches
its query, and carries a field it can not read — three facts about data
it was not permitted to see, delivered by the mechanism meant to
withhold them, and enumerable by trying queries and watching which ones
fail. Excluding costs the caller a result narrower than it appears;
rejecting costs the model the property it rests on.

Cross-source fields already worked this way (step 7); primary-source
fields now match them.

## Field authorization does not depend on presence

A field's authorization is resolved from the name **as written**,
against each concrete identifier, whether or not any record of that
identifier actually carries it.
<!-- lint-disable-next-line line-length -->
[*enforce.field-authorization-is-resolved-from-the-written-name-regardless-of-presence]

Payload fields vary between records of the same type, so a rule turning
on presence would require the scan that authorization is meant to
precede.

## Internal values are not fields

Row identifiers, series identifiers, ordering tiebreakers, and series
type checks are not query-language source fields unless the mode exposes
them as fixed fields or the query names them (§6.2).
[*enforce.internal-values-are-not-source-fields-unless-exposed-or-named]
Errors raised by
internal checks never carry a denied field's value.
[*enforce.internal-check-errors-never-carry-a-denied-fields-value]

A metric result's `value` **is** a source field, because it is either a
raw sample or a scalar derived from raw samples.
[*enforce.a-metric-results-value-is-a-source-field]

## Filtering is part of the logical result

[*enforce.access-filtering-is-part-of-the-logical-result-not-presentation]

Access filtering is not a presentation step. Aggregating, ordering or
paginating over unreadable records would leak them through counts,
through ordering, and through the gaps in pagination.

eventd can push authorization predicates into SQL when the identifier
set is known, or read candidates and filter them before aggregating
(§6.3). The externally visible result is identical to filtering first,
and `COUNT`, `COUNT BY`, `TOP N BY` and every other aggregate reflect
only what the caller can see.
<!-- lint-disable-next-line spelling -->
[*enforce.every-aggregate-reflects-only-what-the-caller-may-see]

## The audit trail

Whether an access check produces a KACS audit event is decided by the
SACL audit walk in the AccessCheck pipeline, against the descriptor's
SACL. Every default descriptor eventd creates carries a failure-audit
ACE for Everyone at every eventd right (§7.2), so out of the box every
**denied** check produces a `kacs.audit.access.checked` record, and a
granted one produces none unless an administrator adds a success ACE.
<!-- lint-disable-next-line line-length -->
[*enforce.every-denied-check-under-a-default-descriptor-produces-a-kacs-audit-event]
KACS writes such a record from the AccessCheck syscall only when the
caller holds `SeAuditPrivilege` enabled. eventd's service definition
requires that privilege, and its token holds it enabled; without it the
check would be made and nothing recorded.
[*enforce.eventds-checks-are-audited-because-its-token-holds-seauditprivilege]

eventd passes each check an audit context naming the object it guards,
in the form PGSS §6.7 defines: a MessagePack map of the object's kind
and, under the kind's own name, the pattern whose descriptor was
checked.

| Check | Audit context |
|---|---|
| Reading events | `{kind: "event-namespace", event-namespace: {pattern: "kacs"}}` |
| Reading logs | `{kind: "log-namespace", log-namespace: {pattern: "loregd"}}` |
| Reading or publishing metrics | `{kind: "metric-namespace", metric-namespace: {pattern: "cpu"}}` |
| `INDEX` (§7.2) | `{kind: "eventd-admin"}` |

The pattern is the one the descriptor was found under (§7.2), not the
identifier asked about, and for a log origin it never holds the producer
after a `/`. KACS copies the map into its `kacs.audit.access.checked`
record as `object.kind` and `object.<kind>.pattern`, and marks the
record with `fields.attestation.userspace`, since those values are
eventd's claim. The audit trail therefore records exactly which
observability data was read, by whom — the client, as `subject.*` —
rather than merely that eventd performed a check. Reading and
publishing a metric name the same object; the record's
`access.requested` says which was checked.
[*enforce.the-audit-context-names-the-data-type-and-pattern-accessed]

Those audit events are themselves KMES events, which eventd consumes and
stores, and which are governed by the event patterns like any other,
under `kacs` (§7.2).
[*enforce.access-audit-events-are-stored-and-governed-like-any-other-event]
Reading the audit store is auditable.
