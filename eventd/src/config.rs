//! Registry-backed startup configuration.

use core::fmt;
use core::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashMap;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use eventd_core::BoundedQueue;
use peios::registry::{Key, KeyAccess, NotifyFilter, OpenFlags, ValueRecord, ValueType};

use crate::indexing::PolicyMessage;
use crate::writer::{WriterMessage, WriterMessage::Synthetic};

const ROOT_KEY: &str = r"Machine\System\eventd";

pub const HANDOFF_SLOTS: usize = 4_096;
pub const HANDOFF_BYTES: usize = 16 * 1024 * 1024;
pub const STRIPE_LENGTH: usize = 1_024;
pub const DEFAULT_METRIC_RETENTION_MAX_BYTES: u64 = 1 << 30;

pub type SharedConfig = Arc<RwLock<Config>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedChange {
    pub key: &'static str,
    pub old_value_type: &'static str,
    pub old_value: Option<String>,
    pub new_value_type: &'static str,
    pub new_value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub event_store_path: PathBuf,
    pub log_store_path: PathBuf,
    pub metric_store_path: PathBuf,
    pub query_socket_path: PathBuf,
    pub log_socket_path: PathBuf,
    pub metric_socket_path: PathBuf,
    pub storage_shards: usize,
    pub max_batch_size: usize,
    pub max_batch_latency: Duration,
    pub wal_checkpoint_pages: u32,
    pub log_max_batch_size: usize,
    pub log_max_batch_latency: Duration,
    pub max_log_datagram_bytes: usize,
    pub metric_max_batch_size: usize,
    pub metric_max_batch_latency: Duration,
    pub max_metric_datagram_bytes: usize,
    pub metric_series_cache_size: usize,
    pub metric_authorization_cache_size: usize,
    pub query_timeout: Duration,
    pub max_concurrent_queries: usize,
    pub max_streaming_queries: usize,
    pub max_distinct_stream_values: usize,
    pub max_query_request_bytes: usize,
    pub query_response_target_bytes: usize,
    pub cross_type_window: Duration,
    pub cross_type_max_lookback: Duration,
    pub adaptive_index_window: Duration,
    pub adaptive_index_policy_interval: Duration,
    pub adaptive_index_create_threshold: u64,
    pub adaptive_index_drop_threshold: u64,
    pub shedding_window: Duration,
    pub shedding_batch_percent: u32,
    pub emergency_shedding_buffer_percent: u8,
    pub event_retention: Duration,
    pub event_retention_max_bytes: u64,
    pub log_retention: Duration,
    pub log_retention_max_bytes: u64,
    pub metric_retention: Duration,
    pub metric_retention_max_bytes: u64,
    pub retention_interval: Duration,
    pub retention_delete_batch_rows: usize,
    raw_values: HashMap<Vec<u8>, ValueRecord>,
}

impl Config {
    pub fn shared(self) -> SharedConfig {
        Arc::new(RwLock::new(self))
    }

    pub fn snapshot(shared: &SharedConfig) -> Self {
        shared
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    pub fn read<T>(shared: &SharedConfig, read: impl FnOnce(&Self) -> T) -> T {
        read(
            &shared
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    #[cfg(test)]
    pub fn test_defaults() -> Self {
        let mut values = HashMap::new();
        for name in [
            b"EventStorePath".as_slice(),
            b"LogStorePath".as_slice(),
            b"MetricStorePath".as_slice(),
            b"QuerySocketPath".as_slice(),
            b"LogSocketPath".as_slice(),
            b"MetricSocketPath".as_slice(),
        ] {
            values.insert(name.to_vec(), path_record(name, Path::new("/test")));
        }
        Self::from_values(&values).expect("test defaults")
    }

    pub fn load() -> Result<Self, ConfigError> {
        Self::from_values(&read_values()?)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the registry schema remains visible as one configuration literal"
    )]
    fn from_values(values: &HashMap<Vec<u8>, ValueRecord>) -> Result<Self, ConfigError> {
        let adaptive_index_create_threshold = u64::from(dword(
            values,
            b"AdaptiveIndexCreateThreshold",
            100,
            10,
            10_000,
        ));
        let adaptive_index_drop_threshold =
            u64::from(dword(values, b"AdaptiveIndexDropThreshold", 10, 1, 1_000));
        if adaptive_index_drop_threshold >= adaptive_index_create_threshold {
            return Err(ConfigError::Invalid(b"AdaptiveIndexDropThreshold"));
        }

        Ok(Self {
            event_store_path: required_path(values, b"EventStorePath")?,
            log_store_path: required_path(values, b"LogStorePath")?,
            metric_store_path: required_path(values, b"MetricStorePath")?,
            query_socket_path: required_path(values, b"QuerySocketPath")?,
            log_socket_path: required_path(values, b"LogSocketPath")?,
            metric_socket_path: required_path(values, b"MetricSocketPath")?,
            storage_shards: usize::try_from(dword(values, b"StorageShards", 0, 0, 256))
                .expect("StorageShards fits usize"),
            max_batch_size: usize::try_from(dword(values, b"MaxBatchSize", 10_000, 100, 100_000))
                .expect("MaxBatchSize fits usize"),
            max_batch_latency: Duration::from_millis(u64::from(dword(
                values,
                b"MaxBatchLatencyMs",
                100,
                10,
                5_000,
            ))),
            wal_checkpoint_pages: dword(values, b"WalCheckpointPages", 1_000, 100, 100_000),
            log_max_batch_size: usize::try_from(dword(
                values,
                b"LogMaxBatchSize",
                5_000,
                100,
                100_000,
            ))
            .expect("LogMaxBatchSize fits usize"),
            log_max_batch_latency: Duration::from_millis(u64::from(dword(
                values,
                b"LogMaxBatchLatencyMs",
                500,
                10,
                5_000,
            ))),
            max_log_datagram_bytes: usize::try_from(dword(
                values,
                b"MaxLogDatagramBytes",
                262_144,
                4_096,
                1_048_576,
            ))
            .expect("MaxLogDatagramBytes fits usize"),
            metric_max_batch_size: usize::try_from(dword(
                values,
                b"MetricMaxBatchSize",
                5_000,
                100,
                100_000,
            ))
            .expect("MetricMaxBatchSize fits usize"),
            metric_max_batch_latency: Duration::from_millis(u64::from(dword(
                values,
                b"MetricMaxBatchLatencyMs",
                1_000,
                10,
                5_000,
            ))),
            max_metric_datagram_bytes: usize::try_from(dword(
                values,
                b"MaxMetricDatagramBytes",
                262_144,
                4_096,
                1_048_576,
            ))
            .expect("MaxMetricDatagramBytes fits usize"),
            metric_series_cache_size: usize::try_from(dword(
                values,
                b"MetricSeriesCacheSize",
                50_000,
                1_000,
                1_000_000,
            ))
            .expect("MetricSeriesCacheSize fits usize"),
            metric_authorization_cache_size: usize::try_from(dword(
                values,
                b"MetricAuthorizationCacheSize",
                16_384,
                256,
                1_000_000,
            ))
            .expect("MetricAuthorizationCacheSize fits usize"),
            query_timeout: Duration::from_millis(u64::from(dword(
                values,
                b"QueryTimeoutMs",
                30_000,
                1_000,
                300_000,
            ))),
            max_concurrent_queries: usize::try_from(dword(
                values,
                b"MaxConcurrentQueries",
                128,
                1,
                4_096,
            ))
            .expect("MaxConcurrentQueries fits usize"),
            max_streaming_queries: usize::try_from(dword(
                values,
                b"MaxStreamingQueries",
                64,
                1,
                1_024,
            ))
            .expect("MaxStreamingQueries fits usize"),
            max_distinct_stream_values: usize::try_from(dword(
                values,
                b"MaxDistinctStreamValues",
                100_000,
                1_000,
                10_000_000,
            ))
            .expect("MaxDistinctStreamValues fits usize"),
            max_query_request_bytes: usize::try_from(dword(
                values,
                b"MaxQueryRequestBytes",
                65_536,
                1_024,
                16_777_216,
            ))
            .expect("MaxQueryRequestBytes fits usize"),
            query_response_target_bytes: usize::try_from(dword(
                values,
                b"QueryResponseTargetBytes",
                65_536,
                1_024,
                16_777_216,
            ))
            .expect("QueryResponseTargetBytes fits usize"),
            cross_type_window: Duration::from_millis(u64::from(dword(
                values,
                b"CrossTypeWindowMs",
                15_000,
                1_000,
                300_000,
            ))),
            cross_type_max_lookback: Duration::from_secs(u64::from(dword(
                values,
                b"CrossTypeMaxLookbackSeconds",
                604_800,
                3_600,
                2_592_000,
            ))),
            adaptive_index_window: Duration::from_secs(
                u64::from(dword(values, b"AdaptiveIndexWindowHours", 24, 1, 168)) * 3_600,
            ),
            adaptive_index_policy_interval: Duration::from_secs(
                u64::from(dword(
                    values,
                    b"AdaptiveIndexPolicyIntervalMinutes",
                    60,
                    60,
                    1_440,
                )) * 60,
            ),
            adaptive_index_create_threshold,
            adaptive_index_drop_threshold,
            shedding_window: Duration::from_secs(u64::from(dword(
                values,
                b"SheddingWindowSeconds",
                30,
                10,
                300,
            ))),
            shedding_batch_percent: dword(values, b"SheddingBatchPercent", 75, 50, 100),
            emergency_shedding_buffer_percent: u8::try_from(dword(
                values,
                b"EmergencySheddingBufferPercent",
                75,
                50,
                95,
            ))
            .expect("emergency shedding percentage fits u8"),
            event_retention: Duration::from_secs(
                u64::from(dword(values, b"EventRetentionDays", 30, 1, 3_650)) * 86_400,
            ),
            event_retention_max_bytes: qword(values, b"EventRetentionMaxBytes", 0),
            log_retention: Duration::from_secs(
                u64::from(dword(values, b"LogRetentionDays", 14, 1, 3_650)) * 86_400,
            ),
            log_retention_max_bytes: qword(values, b"LogRetentionMaxBytes", 0),
            metric_retention: Duration::from_secs(
                u64::from(dword(values, b"MetricRetentionDays", 90, 1, 3_650)) * 86_400,
            ),
            metric_retention_max_bytes: qword(
                values,
                b"MetricRetentionMaxBytes",
                DEFAULT_METRIC_RETENTION_MAX_BYTES,
            ),
            retention_interval: Duration::from_secs(
                u64::from(dword(
                    values,
                    b"RetentionCheckIntervalMinutes",
                    60,
                    1,
                    1_440,
                )) * 60,
            ),
            retention_delete_batch_rows: usize::try_from(dword(
                values,
                b"RetentionDeleteBatchRows",
                10_000,
                100,
                100_000,
            ))
            .expect("RetentionDeleteBatchRows fits usize"),
            raw_values: values.clone(),
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "every independently reloadable registry key is validated explicitly"
    )]
    pub fn reload(&self) -> Result<Self, ConfigError> {
        let values = read_values()?;
        let mut adjusted = values.clone();
        for (name, path) in [
            (b"EventStorePath".as_slice(), &self.event_store_path),
            (b"LogStorePath".as_slice(), &self.log_store_path),
            (b"MetricStorePath".as_slice(), &self.metric_store_path),
            (b"QuerySocketPath".as_slice(), &self.query_socket_path),
            (b"LogSocketPath".as_slice(), &self.log_socket_path),
            (b"MetricSocketPath".as_slice(), &self.metric_socket_path),
        ] {
            if required_path(&adjusted, name).is_err() {
                adjusted.insert(name.to_vec(), path_record(name, path));
            }
        }
        let create_valid = valid_dword(&values, b"AdaptiveIndexCreateThreshold", 10, 10_000);
        let drop_valid = valid_dword(&values, b"AdaptiveIndexDropThreshold", 1, 1_000);
        let reloaded_create = configured_dword(&values, b"AdaptiveIndexCreateThreshold", 100)
            .map_or(self.adaptive_index_create_threshold, u64::from);
        let reloaded_drop = configured_dword(&values, b"AdaptiveIndexDropThreshold", 10)
            .map_or(self.adaptive_index_drop_threshold, u64::from);
        let threshold_invalid = !create_valid || !drop_valid || reloaded_drop >= reloaded_create;
        if threshold_invalid {
            adjusted.insert(
                b"AdaptiveIndexCreateThreshold".to_vec(),
                dword_record(
                    b"AdaptiveIndexCreateThreshold",
                    u32::try_from(self.adaptive_index_create_threshold)
                        .expect("validated threshold fits DWORD"),
                ),
            );
            adjusted.insert(
                b"AdaptiveIndexDropThreshold".to_vec(),
                dword_record(
                    b"AdaptiveIndexDropThreshold",
                    u32::try_from(self.adaptive_index_drop_threshold)
                        .expect("validated threshold fits DWORD"),
                ),
            );
        }
        let mut next = Self::from_values(&adjusted)?;
        macro_rules! retain_invalid_dword {
            ($field:ident, $name:literal, $minimum:expr, $maximum:expr) => {
                if !valid_dword(&values, $name, $minimum, $maximum) {
                    next.$field = self.$field;
                    copy_raw(&mut next.raw_values, &self.raw_values, $name);
                }
            };
        }
        retain_invalid_dword!(max_batch_size, b"MaxBatchSize", 100, 100_000);
        retain_invalid_dword!(max_batch_latency, b"MaxBatchLatencyMs", 10, 5_000);
        retain_invalid_dword!(wal_checkpoint_pages, b"WalCheckpointPages", 100, 100_000);
        retain_invalid_dword!(log_max_batch_size, b"LogMaxBatchSize", 100, 100_000);
        retain_invalid_dword!(log_max_batch_latency, b"LogMaxBatchLatencyMs", 10, 5_000);
        retain_invalid_dword!(
            max_log_datagram_bytes,
            b"MaxLogDatagramBytes",
            4_096,
            1_048_576
        );
        retain_invalid_dword!(metric_max_batch_size, b"MetricMaxBatchSize", 100, 100_000);
        retain_invalid_dword!(
            metric_max_batch_latency,
            b"MetricMaxBatchLatencyMs",
            10,
            5_000
        );
        retain_invalid_dword!(
            max_metric_datagram_bytes,
            b"MaxMetricDatagramBytes",
            4_096,
            1_048_576
        );
        retain_invalid_dword!(
            metric_series_cache_size,
            b"MetricSeriesCacheSize",
            1_000,
            1_000_000
        );
        retain_invalid_dword!(
            metric_authorization_cache_size,
            b"MetricAuthorizationCacheSize",
            256,
            1_000_000
        );
        retain_invalid_dword!(query_timeout, b"QueryTimeoutMs", 1_000, 300_000);
        retain_invalid_dword!(max_concurrent_queries, b"MaxConcurrentQueries", 1, 4_096);
        retain_invalid_dword!(max_streaming_queries, b"MaxStreamingQueries", 1, 1_024);
        retain_invalid_dword!(
            max_distinct_stream_values,
            b"MaxDistinctStreamValues",
            1_000,
            10_000_000
        );
        retain_invalid_dword!(
            max_query_request_bytes,
            b"MaxQueryRequestBytes",
            1_024,
            16_777_216
        );
        retain_invalid_dword!(
            query_response_target_bytes,
            b"QueryResponseTargetBytes",
            1_024,
            16_777_216
        );
        retain_invalid_dword!(cross_type_window, b"CrossTypeWindowMs", 1_000, 300_000);
        retain_invalid_dword!(
            cross_type_max_lookback,
            b"CrossTypeMaxLookbackSeconds",
            3_600,
            2_592_000
        );
        retain_invalid_dword!(adaptive_index_window, b"AdaptiveIndexWindowHours", 1, 168);
        retain_invalid_dword!(
            adaptive_index_policy_interval,
            b"AdaptiveIndexPolicyIntervalMinutes",
            60,
            1_440
        );
        retain_invalid_dword!(shedding_window, b"SheddingWindowSeconds", 10, 300);
        retain_invalid_dword!(shedding_batch_percent, b"SheddingBatchPercent", 50, 100);
        retain_invalid_dword!(
            emergency_shedding_buffer_percent,
            b"EmergencySheddingBufferPercent",
            50,
            95
        );
        retain_invalid_dword!(event_retention, b"EventRetentionDays", 1, 3_650);
        retain_invalid_dword!(log_retention, b"LogRetentionDays", 1, 3_650);
        retain_invalid_dword!(metric_retention, b"MetricRetentionDays", 1, 3_650);
        retain_invalid_dword!(
            retention_interval,
            b"RetentionCheckIntervalMinutes",
            1,
            1_440
        );
        retain_invalid_dword!(
            retention_delete_batch_rows,
            b"RetentionDeleteBatchRows",
            100,
            100_000
        );
        if threshold_invalid {
            next.adaptive_index_create_threshold = self.adaptive_index_create_threshold;
            next.adaptive_index_drop_threshold = self.adaptive_index_drop_threshold;
            copy_raw(
                &mut next.raw_values,
                &self.raw_values,
                b"AdaptiveIndexCreateThreshold",
            );
            copy_raw(
                &mut next.raw_values,
                &self.raw_values,
                b"AdaptiveIndexDropThreshold",
            );
        }
        if !valid_qword(&values, b"EventRetentionMaxBytes") {
            next.event_retention_max_bytes = self.event_retention_max_bytes;
            copy_raw(
                &mut next.raw_values,
                &self.raw_values,
                b"EventRetentionMaxBytes",
            );
        }
        if !valid_qword(&values, b"LogRetentionMaxBytes") {
            next.log_retention_max_bytes = self.log_retention_max_bytes;
            copy_raw(
                &mut next.raw_values,
                &self.raw_values,
                b"LogRetentionMaxBytes",
            );
        }
        if !valid_qword(&values, b"MetricRetentionMaxBytes") {
            next.metric_retention_max_bytes = self.metric_retention_max_bytes;
            copy_raw(
                &mut next.raw_values,
                &self.raw_values,
                b"MetricRetentionMaxBytes",
            );
        }
        for (key, changed) in [
            (
                "EventStorePath",
                next.event_store_path != self.event_store_path,
            ),
            ("LogStorePath", next.log_store_path != self.log_store_path),
            (
                "MetricStorePath",
                next.metric_store_path != self.metric_store_path,
            ),
            (
                "QuerySocketPath",
                next.query_socket_path != self.query_socket_path,
            ),
            (
                "LogSocketPath",
                next.log_socket_path != self.log_socket_path,
            ),
            (
                "MetricSocketPath",
                next.metric_socket_path != self.metric_socket_path,
            ),
            ("StorageShards", next.storage_shards != self.storage_shards),
        ] {
            if changed {
                eprintln!("eventd: configuration change to {key} is deferred until restart");
            }
        }
        next.event_store_path.clone_from(&self.event_store_path);
        next.log_store_path.clone_from(&self.log_store_path);
        next.metric_store_path.clone_from(&self.metric_store_path);
        next.query_socket_path.clone_from(&self.query_socket_path);
        next.log_socket_path.clone_from(&self.log_socket_path);
        next.metric_socket_path.clone_from(&self.metric_socket_path);
        next.storage_shards = self.storage_shards;
        for key in [
            b"EventStorePath".as_slice(),
            b"LogStorePath".as_slice(),
            b"MetricStorePath".as_slice(),
            b"QuerySocketPath".as_slice(),
            b"LogSocketPath".as_slice(),
            b"MetricSocketPath".as_slice(),
            b"StorageShards".as_slice(),
        ] {
            copy_raw(&mut next.raw_values, &self.raw_values, key);
        }
        Ok(next)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "every reloadable registry value has an explicit stable event rendering"
    )]
    pub fn applied_changes(&self, next: &Self) -> Vec<AppliedChange> {
        let mut changes = Vec::new();
        macro_rules! value {
            ($field:ident, $key:literal, $kind:literal) => {
                if self.$field != next.$field {
                    changes.push(applied_change(
                        self,
                        next,
                        $key,
                        $kind,
                        self.$field.to_string(),
                        next.$field.to_string(),
                    ));
                }
            };
        }
        macro_rules! milliseconds {
            ($field:ident, $key:literal) => {
                if self.$field != next.$field {
                    changes.push(applied_change(
                        self,
                        next,
                        $key,
                        "REG_DWORD",
                        self.$field.as_millis().to_string(),
                        next.$field.as_millis().to_string(),
                    ));
                }
            };
        }
        macro_rules! seconds {
            ($field:ident, $key:literal, $divisor:expr) => {
                if self.$field != next.$field {
                    changes.push(applied_change(
                        self,
                        next,
                        $key,
                        "REG_DWORD",
                        (self.$field.as_secs() / $divisor).to_string(),
                        (next.$field.as_secs() / $divisor).to_string(),
                    ));
                }
            };
        }
        value!(wal_checkpoint_pages, "WalCheckpointPages", "REG_DWORD");
        value!(max_batch_size, "MaxBatchSize", "REG_DWORD");
        milliseconds!(max_batch_latency, "MaxBatchLatencyMs");
        value!(log_max_batch_size, "LogMaxBatchSize", "REG_DWORD");
        milliseconds!(log_max_batch_latency, "LogMaxBatchLatencyMs");
        value!(max_log_datagram_bytes, "MaxLogDatagramBytes", "REG_DWORD");
        value!(metric_max_batch_size, "MetricMaxBatchSize", "REG_DWORD");
        milliseconds!(metric_max_batch_latency, "MetricMaxBatchLatencyMs");
        value!(
            max_metric_datagram_bytes,
            "MaxMetricDatagramBytes",
            "REG_DWORD"
        );
        value!(
            metric_series_cache_size,
            "MetricSeriesCacheSize",
            "REG_DWORD"
        );
        value!(
            metric_authorization_cache_size,
            "MetricAuthorizationCacheSize",
            "REG_DWORD"
        );
        seconds!(adaptive_index_window, "AdaptiveIndexWindowHours", 3_600);
        seconds!(
            adaptive_index_policy_interval,
            "AdaptiveIndexPolicyIntervalMinutes",
            60
        );
        value!(
            adaptive_index_create_threshold,
            "AdaptiveIndexCreateThreshold",
            "REG_DWORD"
        );
        value!(
            adaptive_index_drop_threshold,
            "AdaptiveIndexDropThreshold",
            "REG_DWORD"
        );
        seconds!(shedding_window, "SheddingWindowSeconds", 1);
        value!(shedding_batch_percent, "SheddingBatchPercent", "REG_DWORD");
        value!(
            emergency_shedding_buffer_percent,
            "EmergencySheddingBufferPercent",
            "REG_DWORD"
        );
        seconds!(event_retention, "EventRetentionDays", 86_400);
        value!(
            event_retention_max_bytes,
            "EventRetentionMaxBytes",
            "REG_QWORD"
        );
        seconds!(log_retention, "LogRetentionDays", 86_400);
        value!(log_retention_max_bytes, "LogRetentionMaxBytes", "REG_QWORD");
        seconds!(metric_retention, "MetricRetentionDays", 86_400);
        value!(
            metric_retention_max_bytes,
            "MetricRetentionMaxBytes",
            "REG_QWORD"
        );
        seconds!(retention_interval, "RetentionCheckIntervalMinutes", 60);
        value!(
            retention_delete_batch_rows,
            "RetentionDeleteBatchRows",
            "REG_DWORD"
        );
        milliseconds!(query_timeout, "QueryTimeoutMs");
        value!(max_concurrent_queries, "MaxConcurrentQueries", "REG_DWORD");
        value!(max_streaming_queries, "MaxStreamingQueries", "REG_DWORD");
        value!(
            max_distinct_stream_values,
            "MaxDistinctStreamValues",
            "REG_DWORD"
        );
        value!(max_query_request_bytes, "MaxQueryRequestBytes", "REG_DWORD");
        value!(
            query_response_target_bytes,
            "QueryResponseTargetBytes",
            "REG_DWORD"
        );
        milliseconds!(cross_type_window, "CrossTypeWindowMs");
        seconds!(cross_type_max_lookback, "CrossTypeMaxLookbackSeconds", 1);
        changes
    }
}

fn copy_raw(
    target: &mut HashMap<Vec<u8>, ValueRecord>,
    source: &HashMap<Vec<u8>, ValueRecord>,
    name: &[u8],
) {
    if let Some(record) = source.get(name) {
        target.insert(name.to_vec(), record.clone());
    } else {
        target.remove(name);
    }
}

fn applied_change(
    current: &Config,
    next: &Config,
    key: &'static str,
    expected_type: &'static str,
    old_fallback: String,
    new_fallback: String,
) -> AppliedChange {
    let (old_value_type, old_value) = render_applied(
        current.raw_values.get(key.as_bytes()),
        expected_type,
        old_fallback,
    );
    let (new_value_type, new_value) = render_applied(
        next.raw_values.get(key.as_bytes()),
        expected_type,
        new_fallback,
    );
    AppliedChange {
        key,
        old_value_type,
        old_value,
        new_value_type,
        new_value,
    }
}

fn render_applied(
    record: Option<&ValueRecord>,
    expected_type: &'static str,
    fallback: String,
) -> (&'static str, Option<String>) {
    let Some(record) = record else {
        return ("absent", None);
    };
    let rendered = match expected_type {
        "REG_DWORD" if record.ty == ValueType::DWORD && record.data.len() == 4 => {
            u32::from_le_bytes(record.data.as_slice().try_into().expect("length checked"))
                .to_string()
        }
        "REG_QWORD" if record.ty == ValueType::QWORD && record.data.len() == 8 => {
            u64::from_le_bytes(record.data.as_slice().try_into().expect("length checked"))
                .to_string()
        }
        _ => fallback,
    };
    (expected_type, Some(rendered))
}

pub struct ConfigWatch {
    key: Key,
}

impl ConfigWatch {
    pub fn arm() -> Result<Self, ConfigError> {
        let key = open_watch()?;
        Ok(Self { key })
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "the watch thread receives each independently owned wake and apply target"
    )]
    pub fn run(
        mut self,
        shared: &SharedConfig,
        stopping: &AtomicBool,
        forced: &AtomicBool,
        boot_id: [u8; 16],
        event_queue: &BoundedQueue<WriterMessage>,
        retention_requested: &AtomicBool,
        index_policy: &std::sync::mpsc::SyncSender<PolicyMessage>,
    ) {
        apply_reload(
            shared,
            boot_id,
            event_queue,
            retention_requested,
            index_policy,
        );
        let mut buffer = vec![0_u8; 64 * 1024];
        while !stopping.load(Ordering::Acquire) {
            let force = forced.swap(false, Ordering::AcqRel);
            match watch_ready(self.key.as_fd(), 100) {
                Ok(ready) if ready || force => {
                    if ready && let Err(error) = self.key.read_watch_events(&mut buffer) {
                        eprintln!("eventd: configuration watch read failed: {error}");
                    }
                    apply_reload(
                        shared,
                        boot_id,
                        event_queue,
                        retention_requested,
                        index_policy,
                    );
                }
                Ok(_) => {}
                Err(error) => {
                    eprintln!("eventd: configuration watch degraded: {error}");
                    while !stopping.load(Ordering::Acquire) {
                        match open_watch() {
                            Ok(key) => {
                                self.key = key;
                                apply_reload(
                                    shared,
                                    boot_id,
                                    event_queue,
                                    retention_requested,
                                    index_policy,
                                );
                                break;
                            }
                            Err(rearm_error) => {
                                if forced.swap(false, Ordering::AcqRel) {
                                    apply_reload(
                                        shared,
                                        boot_id,
                                        event_queue,
                                        retention_requested,
                                        index_policy,
                                    );
                                }
                                eprintln!(
                                    "eventd: configuration watch rearm failed: {rearm_error}"
                                );
                                std::thread::sleep(Duration::from_secs(1));
                            }
                        }
                    }
                }
            }
        }
    }
}

fn open_watch() -> Result<Key, ConfigError> {
    let key = Key::open(
        None,
        ROOT_KEY,
        KeyAccess::QUERY_VALUE | KeyAccess::NOTIFY,
        OpenFlags::default(),
    )
    .map_err(ConfigError::Registry)?;
    key.notify(NotifyFilter::VALUE, true)
        .map_err(ConfigError::Registry)?;
    key.set_nonblocking(true).map_err(ConfigError::Registry)?;
    Ok(key)
}

fn watch_ready(fd: BorrowedFd<'_>, timeout_ms: i32) -> Result<bool, std::io::Error> {
    let mut descriptor = libc::pollfd {
        fd: fd.as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    };
    // SAFETY: descriptor points to one initialized pollfd for the call.
    let result = unsafe { libc::poll(&raw mut descriptor, 1, timeout_ms) };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        return if error.kind() == std::io::ErrorKind::Interrupted {
            Ok(false)
        } else {
            Err(error)
        };
    }
    if descriptor.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "registry watch closed",
        ));
    }
    Ok(result != 0 && descriptor.revents & libc::POLLIN != 0)
}

fn apply_reload(
    shared: &SharedConfig,
    boot_id: [u8; 16],
    queue: &BoundedQueue<WriterMessage>,
    retention_requested: &AtomicBool,
    index_policy: &std::sync::mpsc::SyncSender<PolicyMessage>,
) {
    let previous = Config::snapshot(shared);
    let next = match previous.reload() {
        Ok(next) => next,
        Err(error) => {
            eprintln!("eventd: configuration reload ignored: {error}");
            return;
        }
    };
    let changes = previous.applied_changes(&next);
    if changes.is_empty() {
        return;
    }
    *shared
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner) = next;
    retention_requested.store(true, Ordering::Release);
    let _ = index_policy.try_send(PolicyMessage::Recompute);
    for change in changes {
        let event = crate::synthetic::config_change(boot_id, &change, realtime_nanoseconds());
        let (sender, receiver) = std::sync::mpsc::sync_channel(1);
        let result = queue
            .reserve(core::mem::size_of::<WriterMessage>())
            .map(|permit| permit.publish(Synthetic(event, sender)));
        if let Err(error) = result {
            eprintln!("eventd: cannot enqueue configuration change: {error}");
            continue;
        }
        if let Ok(Err(error)) = receiver.recv() {
            eprintln!("eventd: cannot persist configuration change: {error}");
        }
    }
}

fn realtime_nanoseconds() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()
        .and_then(|elapsed| u64::try_from(elapsed.as_nanos()).ok())
        .unwrap_or(0)
}

fn read_values() -> Result<HashMap<Vec<u8>, ValueRecord>, ConfigError> {
    let key = Key::open(
        None,
        ROOT_KEY,
        KeyAccess::QUERY_VALUE | KeyAccess::NOTIFY,
        OpenFlags::default(),
    )
    .map_err(ConfigError::Registry)?;
    Ok(key
        .query_values_batch(None)
        .map_err(ConfigError::Registry)?
        .into_iter()
        .map(|record| (record.name.clone(), record))
        .collect())
}

fn valid_dword(
    values: &HashMap<Vec<u8>, ValueRecord>,
    name: &[u8],
    minimum: u32,
    maximum: u32,
) -> bool {
    let Some(record) = values.get(name) else {
        return true;
    };
    record.ty == ValueType::DWORD
        && record.data.len() == 4
        && (minimum..=maximum).contains(&u32::from_le_bytes(
            record.data.as_slice().try_into().expect("length checked"),
        ))
}

fn valid_qword(values: &HashMap<Vec<u8>, ValueRecord>, name: &[u8]) -> bool {
    values
        .get(name)
        .is_none_or(|record| record.ty == ValueType::QWORD && record.data.len() == 8)
}

fn configured_dword(
    values: &HashMap<Vec<u8>, ValueRecord>,
    name: &[u8],
    default: u32,
) -> Option<u32> {
    let Some(record) = values.get(name) else {
        return Some(default);
    };
    (record.ty == ValueType::DWORD && record.data.len() == 4)
        .then(|| u32::from_le_bytes(record.data.as_slice().try_into().expect("length checked")))
}

fn dword_record(name: &[u8], value: u32) -> ValueRecord {
    ValueRecord {
        name: name.to_vec(),
        ty: ValueType::DWORD,
        data: value.to_le_bytes().to_vec(),
    }
}

fn path_record(name: &[u8], path: &Path) -> ValueRecord {
    let mut data = path.as_os_str().as_encoded_bytes().to_vec();
    data.push(0);
    ValueRecord {
        name: name.to_vec(),
        ty: ValueType::SZ,
        data,
    }
}

fn qword(values: &HashMap<Vec<u8>, ValueRecord>, name: &'static [u8], default: u64) -> u64 {
    let Some(record) = values.get(name) else {
        return default;
    };
    if record.ty != ValueType::QWORD || record.data.len() != 8 {
        return default;
    }
    u64::from_le_bytes(record.data.as_slice().try_into().expect("length checked"))
}

fn required_path(
    values: &HashMap<Vec<u8>, ValueRecord>,
    name: &'static [u8],
) -> Result<PathBuf, ConfigError> {
    let record = values.get(name).ok_or(ConfigError::Missing(name))?;
    if record.ty != ValueType::SZ {
        return Err(ConfigError::Invalid(name));
    }
    let end = record
        .data
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(record.data.len());
    let text = std::str::from_utf8(&record.data[..end]).map_err(|_| ConfigError::Invalid(name))?;
    let path = PathBuf::from(text);
    if text.is_empty() || !path.is_absolute() {
        return Err(ConfigError::Invalid(name));
    }
    Ok(path)
}

fn dword(
    values: &HashMap<Vec<u8>, ValueRecord>,
    name: &'static [u8],
    default: u32,
    minimum: u32,
    maximum: u32,
) -> u32 {
    let Some(record) = values.get(name) else {
        return default;
    };
    if record.ty != ValueType::DWORD || record.data.len() != 4 {
        return default;
    }
    let value = u32::from_le_bytes(record.data.as_slice().try_into().expect("length checked"));
    if !(minimum..=maximum).contains(&value) {
        return default;
    }
    value
}

#[derive(Debug)]
pub enum ConfigError {
    Registry(peios::Error),
    Missing(&'static [u8]),
    Invalid(&'static [u8]),
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Registry(error) => write!(formatter, "cannot read {ROOT_KEY}: {error}"),
            Self::Missing(name) => write!(
                formatter,
                "required registry value {} is missing",
                String::from_utf8_lossy(name)
            ),
            Self::Invalid(name) => write!(
                formatter,
                "registry value {} has an invalid type or value",
                String::from_utf8_lossy(name)
            ),
        }
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Registry(error) => Some(error),
            Self::Missing(_) | Self::Invalid(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(name: &[u8], ty: ValueType, data: &[u8]) -> ValueRecord {
        ValueRecord {
            name: name.to_vec(),
            ty,
            data: data.to_vec(),
        }
    }

    #[test]
    fn required_paths_are_absolute_utf8_sz_values() {
        let values = HashMap::from([(
            b"EventStorePath".to_vec(),
            record(
                b"EventStorePath",
                ValueType::SZ,
                b"/var/state/eventd/events/\0",
            ),
        )]);
        assert_eq!(
            required_path(&values, b"EventStorePath").unwrap(),
            PathBuf::from("/var/state/eventd/events/")
        );
    }

    #[test]
    fn dword_enforces_type_and_range() {
        let values = HashMap::from([(
            b"MaxBatchSize".to_vec(),
            record(b"MaxBatchSize", ValueType::DWORD, &10_000_u32.to_le_bytes()),
        )]);
        assert_eq!(dword(&values, b"MaxBatchSize", 1, 100, 100_000), 10_000);
        assert_eq!(dword(&values, b"MaxBatchSize", 1, 20_000, 100_000), 1);
    }

    #[test]
    fn metric_store_is_bounded_by_default_but_accepts_explicit_zero() {
        assert_eq!(
            Config::test_defaults().metric_retention_max_bytes,
            DEFAULT_METRIC_RETENTION_MAX_BYTES
        );
        let values = HashMap::from([(
            b"MetricRetentionMaxBytes".to_vec(),
            record(
                b"MetricRetentionMaxBytes",
                ValueType::QWORD,
                &0_u64.to_le_bytes(),
            ),
        )]);
        assert_eq!(
            qword(
                &values,
                b"MetricRetentionMaxBytes",
                DEFAULT_METRIC_RETENTION_MAX_BYTES
            ),
            0
        );
    }

    #[test]
    fn applied_changes_use_registry_units_and_types() {
        let mut current = Config::test_defaults();
        current.raw_values.insert(
            b"MaxBatchLatencyMs".to_vec(),
            dword_record(b"MaxBatchLatencyMs", 100),
        );
        current.raw_values.insert(
            b"EventRetentionMaxBytes".to_vec(),
            ValueRecord {
                name: b"EventRetentionMaxBytes".to_vec(),
                ty: ValueType::QWORD,
                data: 0_u64.to_le_bytes().to_vec(),
            },
        );
        let mut next = current.clone();
        next.max_batch_latency = Duration::from_millis(250);
        next.event_retention_max_bytes = 99;
        next.raw_values.insert(
            b"MaxBatchLatencyMs".to_vec(),
            dword_record(b"MaxBatchLatencyMs", 250),
        );
        next.raw_values.insert(
            b"EventRetentionMaxBytes".to_vec(),
            ValueRecord {
                name: b"EventRetentionMaxBytes".to_vec(),
                ty: ValueType::QWORD,
                data: 99_u64.to_le_bytes().to_vec(),
            },
        );
        assert_eq!(
            current.applied_changes(&next),
            vec![
                AppliedChange {
                    key: "MaxBatchLatencyMs",
                    old_value_type: "REG_DWORD",
                    old_value: Some("100".into()),
                    new_value_type: "REG_DWORD",
                    new_value: Some("250".into()),
                },
                AppliedChange {
                    key: "EventRetentionMaxBytes",
                    old_value_type: "REG_QWORD",
                    old_value: Some("0".into()),
                    new_value_type: "REG_QWORD",
                    new_value: Some("99".into()),
                },
            ]
        );
    }

    #[test]
    fn deleted_value_renders_as_absent_even_when_default_applies() {
        let mut current = Config::test_defaults();
        current.max_batch_size = 20_000;
        current.raw_values.insert(
            b"MaxBatchSize".to_vec(),
            dword_record(b"MaxBatchSize", 20_000),
        );
        let mut next = current.clone();
        next.max_batch_size = 10_000;
        next.raw_values.remove(b"MaxBatchSize".as_slice());
        assert_eq!(
            current.applied_changes(&next),
            vec![AppliedChange {
                key: "MaxBatchSize",
                old_value_type: "REG_DWORD",
                old_value: Some("20000".into()),
                new_value_type: "absent",
                new_value: None,
            }]
        );
    }
}
