---
title: Patterns and Descriptors
description: Access is defined on named patterns each carrying a descriptor —
  matching, resolution, storage, and the first-boot defaults.
---

Access is defined on **named patterns**, each standing for a category of
observability data and each carrying a descriptor. The three data types
have independent pattern namespaces:
[*pattern.each-data-type-has-an-independent-pattern-namespace]

| Namespace | Patterns match |
|---|---|
| Events | event type |
| Logs | log origin |
| Metrics | metric name |

[*pattern.event-patterns-match-the-event-type]
[*pattern.log-patterns-match-the-log-origin]
[*pattern.metric-patterns-match-the-metric-name]

## Matching

A pattern matches by **dot-delimited prefix**.
<!-- lint-disable-next-line line-length -->
[*pattern.a-pattern-matches-by-dot-delimited-prefix] The pattern `kacs` matches
the exact string `kacs` and any string beginning `kacs.` — and matches
neither `kacs_extended` nor `kacsfoo`, because the dot is the hierarchy
separator and not a mere character.
[*pattern.a-prefix-match-must-end-at-a-dot-or-the-end-of-the-string]

`*` is the wildcard default and matches everything.
[*pattern.the-wildcard-pattern-matches-everything]

Ingestion constrains metric names to the identifier grammar and origins
to that grammar plus a producer component (PSPU §3.7, §3.10), which is
what keeps a producer from choosing a name containing the wildcard or a
backslash — a name that would otherwise match a rule its producer was
never meant to satisfy, or store its descriptor somewhere other than
where the administrator who wrote it believes.

An origin naming a producer within a service — `jellyfin/HealthCheck`,
`jobs/<guid>` — resolves from the part **before** the slash, so the
matching above starts at `jellyfin` and at `jobs`.
[*pattern.an-origin-with-a-producer-resolves-from-the-part-before-the-slash]
A service's hooks,
reload commands and health checks therefore answer to the descriptor
written against the service itself, and every submitted job's output
answers to one descriptor at `Logs\jobs`.
<!-- lint-disable-next-line line-length -->
[*pattern.service-producers-use-the-service-descriptor-and-all-jobs-use-logs-jobs]
The slash never enters a
descriptor path, which is what keeps it out of the registry namespace it
would otherwise be a separator in.
[*pattern.a-slash-never-enters-a-descriptor-path]

## Resolution [*pattern.resolution-walks-up-the-hierarchy-to-the-wildcard]

For a concrete identifier, eventd resolves the applicable descriptor by
walking up the hierarchy:

1. Look for an exact match on the full identifier,
   `kacs.audit.access.checked`.
   [*pattern.resolution-tries-the-full-identifier-first]
2. Remove the last dot-separated component and look again,
   `kacs.audit.access`.
   [*pattern.resolution-then-drops-the-last-dot-separated-component]
3. Repeat: `kacs.audit`, then `kacs`.
4. Fall back to the wildcard, `*`.
   [*pattern.resolution-falls-back-to-the-wildcard-last]

The first match wins; a more specific pattern overrides a less specific
one. [*pattern.the-most-specific-matching-pattern-wins]

## Storage

[*pattern.descriptors-are-registry-values-under-the-eventd-security-subtree]

Descriptors are registry values under the eventd security subtree:

```text
Machine\System\eventd\Security\Events\*
Machine\System\eventd\Security\Events\kacs
Machine\System\eventd\Security\Events\kacs.audit.access.checked
Machine\System\eventd\Security\Logs\*
Machine\System\eventd\Security\Logs\loregd
Machine\System\eventd\Security\Metrics\*
Machine\System\eventd\Security\Metrics\cpu
Machine\System\eventd\Security\Admin
```

Each type's wildcard default is **load-bearing**. If a default is
missing, eventd denies access to all data of that type: resolution that
reaches the end of the hierarchy without a match is a denial, not a
grant (PSPU §3.28).
[*pattern.a-missing-wildcard-default-denies-all-data-of-that-type]

Storing them in the registry rather than in eventd's own databases means
the registry's access control protects them, and an administrator edits
them with the ordinary registry tools rather than through eventd.

### Who can change them

A descriptor is stored whole, as one value, SACL included. Whoever can
write that value can therefore replace the SACL as well as the DACL, and
removing a SACL this way needs no `SeSecurityPrivilege`: the registry
checks the right to set the value, not the right to change audit
policy.
<!-- lint-disable-next-line spelling -->
[*pattern.whoever-may-write-a-descriptor-value-may-also-drop-its-sacl]
Write access to the `Security` subtree is therefore audit-policy
authority, and only those trusted to change audit policy should hold it.
A tool that edits a descriptor's DACL must write the SACL back
unchanged.

eventd gives `Machine\System\eventd\Security` a protected DACL:
SYSTEM and Administrators full control, Authenticated Users read, which
is what eventd and its clients need to resolve descriptors. Protection
stops a broader grant on `Machine\System\eventd`, such as one made to
let someone tune eventd, from reaching the audit policy. eventd sets it
only while the key's DACL is still wholly inherited, so an
administrator's own choice there is never overwritten.
<!-- lint-disable-next-line line-length -->
[*pattern.the-security-root-gets-a-protected-dacl-while-its-dacl-is-wholly-inherited]

## Defaults on first boot [*pattern.missing-default-descriptors-are-created]

eventd creates the three wildcard keys, the descriptor for its own
health metrics and the administrative descriptor if they do not exist:

| Key | Default |
|---|---|
| `…\Security\Events\*` | SYSTEM and Administrators: `EVENTD_READ` on all fields. |
| `…\Security\Logs\*` | SYSTEM, Administrators, and Authenticated Users: `EVENTD_READ`. |
| `…\Security\Metrics\*` | SYSTEM and Administrators: `EVENTD_READ \| EVENTD_PUBLISH`; Authenticated Users: `EVENTD_READ`. |
| `…\Security\Metrics\eventd` | SYSTEM, Administrators, and Authenticated Users: `EVENTD_READ`; nobody `EVENTD_PUBLISH`, because eventd writes its own health without the socket (§5.7). |
| `…\Security\Admin` | SYSTEM and Administrators: `EVENTD_ADMINISTER`. |
[*pattern.the-default-events-descriptor-grants-read-to-system-and-administrators]
[*pattern.the-default-logs-descriptor-also-grants-read-to-authenticated-users]
[*pattern.the-default-metrics-descriptor-grants-publish-only-to-system-and-administrators]
[*pattern.the-default-eventd-metrics-descriptor-grants-publish-to-nobody]
[*pattern.the-default-admin-descriptor-grants-administer-to-system-and-administrators]

Every one of the five also carries a SACL with one failure-audit ACE
for Everyone, covering `EVENTD_READ`, `EVENTD_ADMINISTER` and
`EVENTD_PUBLISH`, and nothing else: a check under a default descriptor
that is denied any of them is audited, whoever made it, and a granted
one is not (§7.4).
[*pattern.every-default-descriptor-audits-every-failed-access-by-everyone]

The asymmetry reflects sensitivity. Events include security audit data
and are restricted to administrators; logs and metrics are operational
data and are readable by any authenticated user. Publication is
fail-closed for ordinary services until a more-specific pattern grants
their service SID `EVENTD_PUBLISH` (§7.6).
[*pattern.publication-is-fail-closed-for-services-without-a-specific-grant]
An administrator can tighten
either side independently.

On upgrade, eventd replaces a stored descriptor only when its binary
form exactly matches a default an earlier eventd shipped at that key:
the Metrics wildcard's former read-only default, and each key's former
default without a SACL. Those are replaced by the current default, SACL
and all.
[*pattern.a-former-default-is-replaced-only-on-an-exact-binary-match] A
value an administrator changed is never treated as a default and never
rewritten. [*pattern.an-administrator-changed-descriptor-is-never-rewritten]

## Conditional ACEs

Descriptors on eventd security objects can carry conditional ACEs, and
KACS evaluates them as it would anywhere.
[*pattern.conditional-aces-are-evaluated-by-kacs-as-anywhere]

eventd passes **no eventd-specific local claims** to AccessCheck:
`local_claims_ptr` is null and `local_claims_len` is zero.
[*pattern.no-local-claims-are-passed-to-accesscheck] Conditions
referencing token claims that KACS itself supplies still evaluate;
conditions referencing eventd-local claims observe them as absent.
[*pattern.conditions-on-eventd-local-claims-observe-them-as-absent]

A stable set of eventd-local claims — the event's type, the log's
origin, the time of day — is a plausible later addition, and defining
one is a commitment to keep those claim names meaningful thereafter.

## The administrative descriptor

`INDEX` (PSPU §3.23) is checked against
`Machine\System\eventd\Security\Admin`, with `EVENTD_ADMINISTER` as the
desired access.
[*pattern.index-is-checked-against-the-admin-descriptor-for-eventd-administer]
Its default grants SYSTEM and Administrators.

eventd refuses `INDEX` outright if no administrative descriptor exists,
by the same fail-closed rule as the read path.
[*pattern.index-is-refused-when-no-admin-descriptor-exists] Keeping it in the
registry means the registry's own descriptor protects the policy and a
recreated `eventd-meta.db` cannot reset it (§3.5).
[*pattern.recreating-eventd-meta-db-does-not-reset-the-admin-policy]
