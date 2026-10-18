---
title: Per-Field Control
description: Granting some fields of a record and not others through object
  ACEs — the tree of field paths, granting a subtree by its prefix, and how
  field GUIDs are derived rather than registered.
---

A descriptor can grant read access to some fields of a record and not
others, using KACS **object ACEs** and object type lists.
[*fieldaccess.a-descriptor-can-grant-some-fields-of-a-record-and-not-others]
A caller
authorized for a pattern but not for a field receives the records with
that field absent — indistinguishable from a record that never carried
it (PSPU §3.28).
[*fieldaccess.an-unauthorized-field-is-absent-as-if-never-carried]

## Object type lists

For each access check eventd builds an object type list: a tree with
the data type's root at level 0 and, beneath it, the fields' dotted
paths, one node per segment. `subject` is at level 1, `subject.token`
at level 2, `subject.token.sid` at level 3.
<!-- lint-disable-next-line line-length -->
[*fieldaccess.the-object-type-list-is-the-root-and-the-tree-of-field-paths-one-node-per-segment]
A field's node is the one at the end of its path, and every node above
it is a prefix of the path, ending before a `.`, named as a field would
be (below): the node for `subject.token` has the GUID of the name
`subject.token`.
[*fieldaccess.a-prefix-node-has-the-guid-of-the-prefix-as-a-field-name]
A prefix that several fields share is one node, so the list names no
GUID twice, and the list is in preorder, each node's subtree directly
after it, as KACS requires.
[*fieldaccess.a-shared-prefix-is-one-node-and-the-list-is-in-preorder]

```text
Level 0: root GUID for the data type
  Level 1: event
    Level 2: event.time
    Level 2: event.type
    Level 2: event.cpu
  Level 1: emitter
    Level 2: emitter.class
    Level 2: emitter.token
      Level 3: emitter.token.guid
    Level 2: emitter.true-token
      Level 3: emitter.true-token.guid
    Level 2: emitter.process
      Level 3: emitter.process.guid
  Level 1: access
    Level 2: access.granted
  Level 1: subject
    Level 2: subject.token
      Level 3: subject.token.sid
  Level 1: source
    Level 2: source.name
```

The list is as deep as the deepest path, with no level limit.
[*fieldaccess.the-list-is-as-deep-as-the-deepest-path-with-no-level-limit]
MS-DTYP's object type lists stop at level 4; KACS sets no depth limit,
only one on the number of nodes, 1024 to a list
(`KACS_ACCESS_CHECK_MAX_OBJECT_TYPE_COUNT`). Every prefix counts toward
it. A record whose list would be longer cannot be checked, and a query
reaching one fails rather than showing it.

A name with no `.` — a log field, a fixed metric field, most metric
labels — is a node at level 1. A metric label key can contain `.`, and
is then a path like any other: a label `disk.read` is beneath the node
`disk`.

`kacs_access_check_list` returns a verdict per node, and eventd uses
the verdict on each field's node to include or exclude that field.
[*fieldaccess.each-field-is-included-or-excluded-by-its-node-verdict]
The verdicts on the prefix nodes are not used.

The three root GUIDs — one for events, one for logs, one for metrics —
are in §B.

## Field GUIDs are derived, not registered

[*fieldaccess.field-guids-are-derived-not-registered]

A field's GUID is computed deterministically with UUID v5 (RFC 4122):

```text
field_guid = uuid_v5(EVENTD_FIELD_NAMESPACE, field_name)
```

with the namespace UUID in §B, and `field_name` the field's
query-language name as UTF-8.
<!-- lint-disable-next-line line-length -->
[*fieldaccess.a-field-guid-is-uuid-v5-of-the-utf-8-field-name-in-the-eventd-namespace]

There is **no registry of field GUIDs and no allocation step**. The same
name always yields the same GUID, so an administrator writing a
descriptor computes the GUID from the field name with the same algorithm
that eventd will use when it builds the list.
<!-- lint-disable-next-line line-length -->
[*fieldaccess.the-same-field-name-always-yields-the-same-guid] This is the one part of
eventd's access control that a third party reproduces rather than merely
consumes.

Derivation rather than registration is what makes payload fields
tractable at all: event payload schemas belong to the emitting
subsystems, eventd has no catalog of them, and a new event type
carrying a new field needs no registration anywhere before a descriptor
can name it.

Which names are used:

| Data | `field_name` |
|---|---|
| Event header field | its field path (PSPU §3.22) — `event.time`, `event.type`, `event.cpu` — never its column's name |
| Event payload field | the flattened dot path — `access.granted`, `subject.token.sid`, `source.name` |
| Log field | the column name — `origin`, `message`, `is_error` |
| Fixed metric field | `timestamp`, `boot_id`, `name`, `type`, `value` |
| Metric label | the label key — `core`, `device` |

[*fieldaccess.an-event-header-field-is-named-by-its-field-path]
[*fieldaccess.an-event-payload-field-is-named-by-its-flattened-dot-path]
[*fieldaccess.a-log-field-is-named-by-its-column-name]
[*fieldaccess.the-fixed-metric-field-names]
[*fieldaccess.a-metric-label-is-named-by-its-label-key]

Payload fields suppressed by flattening or by a header collision — a
value or map at a header field's path — are not query-language fields,
so they get no GUID (PSPU §3.22).
[*fieldaccess.suppressed-payload-fields-get-no-guid] A GUID naming a
header field's path is the header field's, whatever the payload holds
there.

A descriptor written before header fields were named by path, granting
`timestamp` or `cpu_id` by GUID, now grants the payload field of that
name rather than the header field, because the GUID is the name's. Such
a grant must be rewritten with the path to keep its meaning. Metric label
keys cannot collide with the fixed metric fields, because ingestion
rejects records whose labels do.

## The GUID does not encode scope

A field GUID names a field and nothing else. `access.granted` produces
the same GUID whatever event type carries it.
[*fieldaccess.a-field-guid-does-not-depend-on-the-event-type-carrying-it]

Scoping comes from the descriptor hierarchy: an object ACE naming the
`access.granted` GUID inside the descriptor for pattern `kacs` means
"the `access.granted` field of KACS events". The same ACE in a different
pattern's descriptor means the same field of that pattern's records.
[*fieldaccess.a-field-ace-is-scoped-by-the-pattern-descriptor-holding-it]

## Granting a subtree

An object ACE applies to its node and to every node beneath it, so an
ACE naming a prefix applies to every field whose path it begins:
`subject` covers `subject.token.sid`, `subject.process.pid`, and any
`subject` field an emitter adds later.
[*fieldaccess.an-ace-naming-a-prefix-applies-to-every-field-beneath-it]
The header fields are paths like the rest: `event` covers every
`event.*` header field, and `emitter` every `emitter.*` header field
together with the payload fields beside them, such as
`emitter.process.pid`.
[*fieldaccess.a-grant-on-event-or-emitter-covers-the-header-fields-beneath-it]

KACS decides each node once, first ACE first. A field can therefore be
read when, of the ACEs that match the caller and name its GUID, the
GUID of any prefix of its path, the data type's root, or no object type
at all, the first in DACL order to decide `EVENTD_READ` allows it.
<!-- lint-disable-next-line line-length -->
[*fieldaccess.a-field-is-decided-by-the-first-ace-naming-it-a-prefix-of-it-or-the-root]
Neither the more specific nor the more general ACE wins as such:

| DACL, in order | `subject.token.sid` | `subject.process.pid` |
|---|---|---|
| deny `subject`, allow `subject.token.sid` | denied | denied |
| deny `subject.token.sid`, allow `subject` | denied | read |
| allow `subject.token.sid`, deny `subject` | read | denied |

A canonically ordered DACL (PCDS §5.5) puts every deny before every
allow, so there a deny anywhere on a field's path hides it.

A deny on a field does not reach its siblings: denying
`subject.token.sid` leaves `subject.token.auth-id` and
`subject.process.pid` as the rest of the DACL decides them, and denying
`subject.token` hides every token field and leaves `subject.process`.
[*fieldaccess.a-deny-on-a-field-leaves-its-siblings-untouched]
KACS does carry a deny up to the node's ancestors, and a grant up to a
node all of whose children are granted, but only onto the prefix nodes
and the root, never onto another field.

A field can also be a prefix of another in the same record, since a
payload is opaque: a payload value at `emitter` sits beside the header's
`emitter.class`. Its node in the record's list then has nodes beneath
it, whose grants and denies KACS carries up to it. eventd therefore
checks such a field again in a list of its own, holding only its
prefixes and itself, and uses that verdict, so that the same first-ACE
rule decides it as decides any other field.
[*fieldaccess.a-field-that-is-also-a-prefix-is-checked-with-only-its-prefixes]

A descriptor written before the list was a tree keeps its meaning for
every field it names by full path: that field's GUID, and so its node,
is unchanged. What is new is that an ACE naming a prefix, which named a
node in no list before, now covers the fields beneath it.

## Writing one

An object ACE with **no** object type GUID applies to the root and
therefore to every field.
[*fieldaccess.an-object-ace-without-a-guid-applies-to-every-field] One
**with** a field GUID applies to that field.
[*fieldaccess.an-object-ace-with-a-field-guid-applies-to-that-field]

To grant a security team full read access to KACS events, and a
monitoring team only the time, type and CPU:

- Allow SecurityAdmins, `EVENTD_READ`, no object GUID
- Allow MonitoringTeam, `EVENTD_READ`, object GUID = `event.time`
- Allow MonitoringTeam, `EVENTD_READ`, object GUID = `event.type`
- Allow MonitoringTeam, `EVENTD_READ`, object GUID = `event.cpu`

MonitoringTeam querying KACS events receives records containing exactly
those three keys. Payload fields, identity GUIDs, and the remaining
header fields are absent.
<!-- lint-disable-next-line line-length -->
[*fieldaccess.a-grant-of-three-field-guids-yields-records-with-exactly-those-three-keys]

To let an auditing team see who acted but not the token's groups:

- Deny AuditTeam, `EVENTD_READ`, object GUID = `subject.token.groups`
- Allow AuditTeam, `EVENTD_READ`, object GUID = `event`
- Allow AuditTeam, `EVENTD_READ`, object GUID = `subject`

AuditTeam receives every `event.*` header field and every `subject`
field except `subject.token.groups`.

## Building the list per record

The list is constructed from the fields actually present in the record
being checked: the root node, then the tree of their paths.
[*fieldaccess.the-list-is-built-from-the-fields-present-in-the-record]

**Event records** contribute every header field plus every non-suppressed
flattened payload field present in that particular payload.
<!-- lint-disable-next-line line-length -->
[*fieldaccess.an-event-list-has-every-header-field-and-every-present-unsuppressed-payload-field]
Different
event types produce different lists, because they carry different
payloads — and two events of the same type can too, since a payload is
opaque MessagePack and nothing requires two of a type to agree.
[*fieldaccess.two-events-of-the-same-type-can-produce-different-lists]

**Log records** have a fixed field set — `timestamp`, `origin`,
`is_error`, `message`, `job_id`, `boot_id` — so the list is the same for
every log record. [*fieldaccess.every-log-record-has-the-same-six-field-list]

**Metric records** contribute the five fixed fields plus the series'
label keys, which vary per series.
<!-- lint-disable-next-line line-length -->
[*fieldaccess.a-metric-list-has-the-five-fixed-fields-and-the-series-label-keys]

Derived aggregate outputs — `count`, `sum`, `avg`, `min`, `max` — are
omitted from the list entirely.
[*fieldaccess.aggregate-outputs-are-omitted-from-the-list] They are not
source fields, have no GUID
and no access identity of their own, and are visible when the caller is
authorized for the records and source fields they were computed from
(PSPU §3.28).
<!-- lint-disable-next-line line-length -->
[*fieldaccess.an-aggregate-is-visible-when-its-source-records-and-fields-are-authorized]


## Metric aggregate headers

A metric aggregate or window result does not restore `timestamp`, `name`
or `type` if source-field authorization removed that header. The private
time used for window placement and ordering is independent of the
returned record, so hiding `timestamp` does not change the numeric result
or move samples into another window.
[*fieldaccess.metric-aggregates-do-not-restore-denied-source-headers]

When an output combines several contributing series or metric identifiers,
each returned header must be visible in every contributing source. A
first contributor with broader access cannot restore a header denied by
another. Per-series output retains each series' own visibility. This also
applies to latest-value aggregates, transformed windows, and cached rollup
results: a cached value does not bypass the current source-field check.
Explicit references to a denied field still filter the source records as
specified by PSPU §3.28.
