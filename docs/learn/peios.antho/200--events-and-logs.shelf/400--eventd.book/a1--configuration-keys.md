---
title: Configuration Keys
description: Every configuration key under the eventd registry subtree, its
  type and default, and how an invalid value is treated.
---

Every key lives under `Machine\System\eventd\`.
[*config.every-key-lives-under-machine-system-eventd] eventd ignores unknown
keys in the subtree. [*config.unknown-keys-in-the-subtree-are-ignored] An
invalid value is ignored and the value already in use is retained, and eventd
emits an `eventd.config.changed` event for every change actually applied
(§8.3).
[*config.an-invalid-value-is-ignored-and-the-value-in-use-is-retained]

## Required

No compiled-in defaults. A missing or invalid value fails startup
(§8.2). [*config.a-missing-or-invalid-required-key-fails-startup]

| Key | Type | Description |
|---|---|---|
| `EventStorePath` | REG_SZ | Provisioned directory for event shards and `eventd-meta.db`; standard deployment `/var/state/eventd/events/`. |
| `LogStorePath` | REG_SZ | Provisioned directory for `logs.db`; standard deployment `/var/state/eventd/logs/`. |
| `MetricStorePath` | REG_SZ | Provisioned directory for `metrics.db`; standard deployment `/var/state/eventd/metrics/`. |
| `QuerySocketPath` | REG_SZ | Unix socket path for queries. |
| `LogSocketPath` | REG_SZ | Unix socket path for log ingestion. |
| `MetricSocketPath` | REG_SZ | Unix socket path for metric ingestion. |
[*config.event-store-path-is-the-directory-for-event-shards-and-eventd-meta-db]
[*config.log-store-path-is-the-directory-for-logs-db]
[*config.metric-store-path-is-the-directory-for-metrics-db]
[*config.query-socket-path-is-the-query-socket]
[*config.log-socket-path-is-the-log-ingestion-socket]
[*config.metric-socket-path-is-the-metric-ingestion-socket]

## SQLite storage

| Key | Type | Default | Range | Description |
|---|---|---|---|---|
| `WalCheckpointPages` | REG_DWORD | 1000 | 100–100000 | WAL page threshold triggering a passive checkpoint, on shard, log, metric and metadata databases alike. |

[*config.wal-checkpoint-pages-defaults-to-1000-on-every-database]

## Event ingestion

| Key | Type | Default | Range | Description |
|---|---|---|---|---|
| `StorageShards` | REG_DWORD | 0 | 0–256 | Number of event shards. 0 means the successfully attached KMES-buffer count. Each shard costs at least three file descriptors, plus about two for every running query that reads every shard; a high count needs eventd's `LimitNOFILE` raised (§2.3). |
| `MaxBatchSize` | REG_DWORD | 10000 | 100–100000 | Maximum events per writer transaction. |
| `MaxBatchLatencyMs` | REG_DWORD | 100 | 10–5000 | Maximum ms before an event batch commits. |

[*config.storage-shards-defaults-to-0-meaning-the-attached-kmes-buffer-count]
[*config.max-batch-size-defaults-to-10000-events-per-transaction]
[*config.max-batch-latency-defaults-to-100-ms]

## Log ingestion

| Key | Type | Default | Range | Description |
|---|---|---|---|---|
| `LogMaxBatchSize` | REG_DWORD | 5000 | 100–100000 | Maximum log records per transaction. |
| `LogMaxBatchLatencyMs` | REG_DWORD | 500 | 10–5000 | Maximum ms before a log batch commits. |
| `MaxLogDatagramBytes` | REG_DWORD | 262144 | 262144–1048576 | Maximum accepted log datagram size; the PSPU portable floor cannot be lowered. |

[*config.log-max-batch-size-defaults-to-5000-records-per-transaction]
[*config.log-max-batch-latency-defaults-to-500-ms]
<!-- lint-disable-next-line line-length -->
[*config.max-log-datagram-bytes-defaults-to-the-262144-floor-and-cannot-go-lower]

## Metric ingestion

| Key | Type | Default | Range | Description |
|---|---|---|---|---|
| `MetricMaxBatchSize` | REG_DWORD | 5000 | 100–100000 | Maximum metric samples per transaction. |
| `MetricMaxBatchLatencyMs` | REG_DWORD | 1000 | 10–5000 | Maximum ms before a metric batch commits. |
| `MaxMetricDatagramBytes` | REG_DWORD | 262144 | 262144–1048576 | Maximum accepted metric datagram size; the PSPU portable floor cannot be lowered. |
| `MetricSeriesCacheSize` | REG_DWORD | 50000 | 1000–1000000 | Entries in the LRU series resolution cache. |
| `MetricAuthorizationCacheSize` | REG_DWORD | 16384 | 256–1000000 | Cached KACS publication verdicts, invalidated by security-policy generation. |
| `HealthMetricIntervalSeconds` | REG_DWORD | 15 | 0–3600 | How often eventd records its own health as `eventd.*` metrics; 0 turns them off (§5.7). |

[*config.metric-max-batch-size-defaults-to-5000-samples-per-transaction]
[*config.metric-max-batch-latency-defaults-to-1000-ms]
<!-- lint-disable-next-line line-length -->
[*config.max-metric-datagram-bytes-defaults-to-the-262144-floor-and-cannot-go-lower]
[*config.metric-series-cache-size-defaults-to-50000-entries]
[*config.metric-authorization-cache-size-defaults-to-16384-verdicts]
[*config.health-metric-interval-defaults-to-15-seconds-and-zero-turns-it-off]

## Adaptive indexing

| Key | Type | Default | Range | Description |
|---|---|---|---|---|
| `AdaptiveIndexWindowHours` | REG_DWORD | 24 | 1–168 | Rolling window over which query frequency is measured. |
| `AdaptiveIndexPolicyIntervalMinutes` | REG_DWORD | 60 | 60–1440 | How often the desired index set is recomputed. The minimum of 60 prevents index churn. |
| `AdaptiveIndexCreateThreshold` | REG_DWORD | 100 | 10–10000 | Queries on a field within the window needed to add it. |
| `AdaptiveIndexDropThreshold` | REG_DWORD | 10 | 1–1000 | Queries below which it is removed. Less than the create threshold, which is what supplies the hysteresis. |

[*config.adaptive-index-window-defaults-to-24-hours]
<!-- lint-disable-next-line line-length -->
[*config.adaptive-index-policy-interval-defaults-to-and-cannot-go-below-60-minutes]
[*config.adaptive-index-create-threshold-defaults-to-100-queries]
[*config.adaptive-index-drop-threshold-defaults-to-10-queries]

## Index shedding

| Key | Type | Default | Range | Description |
|---|---|---|---|---|
| `SheddingWindowSeconds` | REG_DWORD | 30 | 10–300 | Sliding window for graduated shedding. |
| `SheddingBatchPercent` | REG_DWORD | 75 | 50–100 | Percentage of batches in the window exceeding 75% of `MaxBatchSize` that triggers graduated shedding. |
| `EmergencySheddingBufferPercent` | REG_DWORD | 75 | 50–95 | Ring buffer fill percentage triggering emergency shedding. |

[*config.shedding-window-defaults-to-30-seconds]
<!-- lint-disable-next-line line-length -->
[*config.graduated-shedding-defaults-to-75-percent-of-batches-over-75-percent-of-max-batch-size]
[*config.emergency-shedding-defaults-to-75-percent-ring-buffer-fill]

## Retention

| Key | Type | Default | Range | Description |
|---|---|---|---|---|
| `EventRetentionDays` | REG_DWORD | 30 | 1–3650 | Maximum age of events. |
| `EventRetentionMaxBytes` | REG_QWORD | 0 | 0–2^64−1 | Maximum total logical live size of the event shards. 0 means no limit. |
| `LogRetentionDays` | REG_DWORD | 14 | 1–3650 | Maximum age of log entries. |
| `LogRetentionMaxBytes` | REG_QWORD | 0 | 0–2^64−1 | Maximum logical live size of the log store. 0 means no limit. |
| `MetricRetentionDays` | REG_DWORD | 90 | 1–3650 | Maximum age of metric samples. |
| `MetricRetentionMaxBytes` | REG_QWORD | 1073741824 (1 GiB) | 0–2^64−1 | Maximum logical live size of the metric store. 0 explicitly disables the limit. |
| `RetentionCheckIntervalMinutes` | REG_DWORD | 60 | 1–1440 | How often the retention coordinator runs. |
| `RetentionDeleteBatchRows` | REG_DWORD | 10000 | 100–100000 | Maximum rows deleted in one retention transaction. |

[*config.event-retention-defaults-to-30-days]
[*config.event-retention-max-bytes-defaults-to-0-meaning-no-limit]
[*config.log-retention-defaults-to-14-days]
[*config.log-retention-max-bytes-defaults-to-0-meaning-no-limit]
[*config.metric-retention-defaults-to-90-days]
[*config.metric-retention-max-bytes-defaults-to-1-gib-and-0-disables-it]
[*config.retention-check-interval-defaults-to-60-minutes]
[*config.retention-delete-batch-defaults-to-10000-rows-per-transaction]

## Querying

| Key | Type | Default | Range | Description |
|---|---|---|---|---|
| `QueryTimeoutMs` | REG_DWORD | 30000 | 1000–300000 | Maximum query execution time. |
| `MaxConcurrentQueries` | REG_DWORD | 128 | 1–4096 | Concurrent finite and streaming queries globally. |
| `MaxStreamingQueries` | REG_DWORD | 64 | 1–1024 | Concurrent streaming queries globally. |
| `MaxQueriesPerUser` | REG_DWORD | 16 | 1–4096 | Concurrent queries, streaming or not, held by one caller's user SID; SYSTEM's are not counted (§6.5). |
| `MaxDistinctStreamValues` | REG_DWORD | 100000 | 1000–10000000 | Values tracked by one DISTINCT streaming query. |
| `MaxQueryRequestBytes` | REG_DWORD | 65536 | 1024–16777216 | Hard maximum query request payload. |
| `QueryResponseTargetBytes` | REG_DWORD | 65536 | 1024–16777216 | Soft response batching target; one complete record can exceed it (§6). |
| `MaxQueryHeldBytes` | REG_DWORD | 268435456 (256 MiB) | 16777216–4294967295 | What every running query together can hold to answer: sorted rows, aggregation groups, and watch batches. A query in the default order holds nothing against it (§6.5). |

[*config.query-timeout-defaults-to-30000-ms]
[*config.max-concurrent-queries-defaults-to-128-globally-including-streaming]
[*config.max-streaming-queries-defaults-to-64-globally]
[*config.max-queries-per-user-defaults-to-16-and-does-not-count-system]
[*config.max-distinct-stream-values-defaults-to-100000-per-query]
[*config.max-query-request-bytes-is-a-hard-limit-defaulting-to-65536]
[*config.query-response-target-bytes-is-a-soft-target-defaulting-to-65536]
[*config.max-query-held-bytes-defaults-to-256-mib-across-all-queries]

## Adaptive metric rollups

| Key | Type | Default | Range | Description |
|---|---|---|---|---|
| `AdaptiveRollupMinSamples` | REG_DWORD | 1000 | 100–1000000 | Minimum raw inputs an eligible window query must scan before it can seed rollups. |
| `AdaptiveRollupBatchRows` | REG_DWORD | 512 | 16–4096 | Maximum missing windows submitted in one bounded writer command. |
| `AdaptiveRollupMaxRows` | REG_DWORD | 100000 | 0–10000000 | Global rollup-cache row cap. 0 disables cache reads and writes without changing results. |

[*config.adaptive-rollup-min-samples-defaults-to-1000-raw-inputs]
[*config.adaptive-rollup-batch-rows-defaults-to-512-windows-per-command]
[*config.adaptive-rollup-max-rows-defaults-to-100000-and-0-disables-the-cache]

## Cross-type filtering

| Key | Type | Default | Range | Description |
|---|---|---|---|---|
| `CrossTypeWindowMs` | REG_DWORD | 15000 | 1000–300000 | Centered window for cross-type event and log existence checks. |
| `CrossTypeMaxLookbackSeconds` | REG_DWORD | 604800 | 3600–2592000 | Maximum range a cross-type filter can scan. |

<!-- lint-disable-next-line spelling -->
[*config.cross-type-window-defaults-to-a-centred-15000-ms]
[*config.cross-type-max-lookback-defaults-to-604800-seconds]

## The security subtree

[*config.read-path-descriptors-live-under-the-security-subtree]

Read-path descriptors live under `Machine\System\eventd\Security\` and
are not configuration in the sense above (§7.2):

```text
Machine\System\eventd\Security\Events\*
Machine\System\eventd\Security\Events\<pattern>
Machine\System\eventd\Security\Logs\*
Machine\System\eventd\Security\Logs\<pattern>
Machine\System\eventd\Security\Metrics\*
Machine\System\eventd\Security\Metrics\<pattern>
Machine\System\eventd\Security\Admin
```

`Security\Admin` governs `EVENTD_ADMINISTER`, especially `INDEX`, and
defaults to SYSTEM and Administrators.
<!-- lint-disable-next-line line-length -->
[*config.security-admin-governs-eventd-administer-and-defaults-to-system-and-administrators]
It is registry-protected policy, not data in `eventd-meta.db` (§3.5,
§7.2).
[*config.the-admin-descriptor-is-registry-policy-not-metadata-database-data]

## When a change takes effect

| Change | Effect |
|---|---|
| Every tuning parameter above | Applied immediately. |
| Socket paths | Restart. |
| Store paths | Restart. |
| `StorageShards` | Restart. |
| Security descriptors | Next query; the registry watch invalidates the cache. |

[*config.every-tuning-parameter-applies-immediately]
[*config.a-socket-path-change-waits-for-a-restart]
[*config.a-store-path-change-waits-for-a-restart]
[*config.a-storage-shards-change-waits-for-a-restart]
[*config.a-security-descriptor-change-applies-from-the-next-query]
