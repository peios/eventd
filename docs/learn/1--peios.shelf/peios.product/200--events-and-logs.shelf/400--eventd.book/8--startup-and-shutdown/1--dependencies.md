---
title: Dependencies
description: The four subsystems eventd needs before it can do anything, the
  one that is not really a dependency, and where it sits in the boot.
---

eventd needs four subsystems before it can do anything.

| Subsystem | For |
|---|---|
| KMES | Event ingestion. Provided by the Peios kernel. |
| LCS and loregd | Configuration. eventd reads every setting from the registry. |
| KACS | Access control — the AccessCheck API for query authorization, and the peer-token socket option (`KACS_SO_PEER_TOKEN`) for caller identification. |
| peinit | Lifecycle management and pre-start state-directory provisioning. |

[*deps.events-are-ingested-from-kmes]
[*deps.eventd-reads-every-setting-from-the-registry]
[*deps.kacs-provides-query-authorization-and-peer-token-caller-identification]
<!-- lint-disable-next-line line-length -->
[*deps.peinit-manages-the-lifecycle-and-provisions-state-directories-before-start]

eventd is a peinit-managed service, started after loregd is available —
it cannot read a single configuration value without the registry, and it
has no compiled-in defaults for the six paths it needs (§A).
[*deps.eventd-cannot-start-without-the-registry]

The boot ID is not a service-manager API. eventd reads Linux's
`/proc/sys/kernel/random/boot_id`, validates its UUID text, and converts
it to PCDS GUID layout (§3.7).
[*deps.the-boot-id-is-read-from-proc-sys-kernel-random-boot-id]

## The dependency that is not one

KACS authorizes queries and metric publication. The established event
drain, write, and retention paths do not perform per-record KACS checks.
<!-- lint-disable-next-line line-length -->
[*deps.the-drain-write-and-retention-paths-never-call-kacs] That asymmetry is what lets eventd
keep draining attached event rings when fresh access checks fail (§9.3),
and it is the right way round: losing the ability to read the audit
store is recoverable, losing the events is not.

## Ordering in the boot

eventd is one of the platform daemons and is Critical: peinit reboots
the system rather than continuing without it.
[*deps.eventd-is-critical-and-peinit-reboots-rather-than-continue-without-it]
It comes up after loregd
and authd, and it stops before them on the way down — it is among the
last services shut down, because everything else's shutdown is worth
recording.
[*deps.eventd-starts-after-loregd-and-authd-and-stops-before-them]

The window before eventd exists is real and peinit covers it by
buffering service output until the log socket appears. Events emitted
during that window remain available only while they survive in the finite
KMES rings. The first drain after attachment reads survivors from
`tail_pos` (§2.2).
<!-- lint-disable-next-line line-length -->
[*deps.events-emitted-before-eventd-starts-are-read-by-its-first-drain-from-tail-pos]
