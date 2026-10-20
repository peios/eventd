---
title: Operating eventd
type: concept
description: Configuration ownership, service preparation, and safe
  interpretation of health signals.
---

eventd runs as a managed service with protected store directories and a
restricted service identity. Its package supplies registry seed files;
installing those files does not apply their contents to the registry.

## Startup configuration

The registry key `Machine\System\eventd` supplies six required paths:
three store directories and three socket paths. Missing or invalid paths
prevent startup. `eventd(8)` and the eventd regman records describe the
command and configuration values.

The service definition runs a short `--prepare-security` hook as SYSTEM
before starting the daemon. That hook prepares registry authorization
defaults. The long-running service does not acquire policy-editing
permissions merely because the hook needed them.

A deployment must provision the service identity, privileges, registry
configuration, and protected directories together. Copying the executable
alone is not a complete installation.

## Live changes

The daemon watches configuration and applies valid tuning changes.
Invalid live changes keep the last valid setting. Store paths, socket
paths, and shard count apply at restart. Read each regman record's
`applies` field before planning a change.

Read policy and metric publication policy use security descriptors under
`Machine\System\eventd\Security`. A read grant does not grant metric
publication. Broader grants can disclose records that were already stored.

## Health and storage

The daemon writes its own `eventd.*` health samples directly to the metric
store. These do not pass through the publication socket. Health counters
restart when eventd restarts, so compare changes with that lifecycle in
mind.

A store-size metric measures disk use, while retention limits logical live
data. A database can retain reusable free pages after deletion. Inspect
both filesystem capacity and retained data when diagnosing pressure.

For implementation details, read the
[eventd manual](~peios/eventd).
