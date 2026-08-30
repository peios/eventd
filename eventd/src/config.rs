//! Registry-backed startup configuration.

use core::fmt;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

use peios::registry::{Key, KeyAccess, OpenFlags, ValueRecord, ValueType};

const ROOT_KEY: &str = r"Machine\System\eventd";

pub const HANDOFF_SLOTS: usize = 4_096;
pub const HANDOFF_BYTES: usize = 16 * 1024 * 1024;
pub const STRIPE_LENGTH: usize = 1_024;

#[derive(Debug)]
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
    pub query_timeout: Duration,
    pub max_concurrent_queries: usize,
    pub max_streaming_queries: usize,
    pub max_distinct_stream_values: usize,
    pub max_query_request_bytes: usize,
    pub query_response_target_bytes: usize,
    pub cross_type_window: Duration,
    pub cross_type_max_lookback: Duration,
    pub event_retention: Duration,
    pub event_retention_max_bytes: u64,
    pub log_retention: Duration,
    pub log_retention_max_bytes: u64,
    pub metric_retention: Duration,
    pub metric_retention_max_bytes: u64,
    pub retention_interval: Duration,
    pub retention_delete_batch_rows: usize,
}

impl Config {
    #[allow(
        clippy::too_many_lines,
        reason = "the registry schema is intentionally visible as one literal configuration snapshot"
    )]
    pub fn load() -> Result<Self, ConfigError> {
        let key = Key::open(
            None,
            ROOT_KEY,
            KeyAccess::QUERY_VALUE | KeyAccess::NOTIFY,
            OpenFlags::default(),
        )
        .map_err(ConfigError::Registry)?;
        let values = key
            .query_values_batch(None)
            .map_err(ConfigError::Registry)?
            .into_iter()
            .map(|record| (record.name.clone(), record))
            .collect::<HashMap<_, _>>();

        Ok(Self {
            event_store_path: required_path(&values, b"EventStorePath")?,
            log_store_path: required_path(&values, b"LogStorePath")?,
            metric_store_path: required_path(&values, b"MetricStorePath")?,
            query_socket_path: required_path(&values, b"QuerySocketPath")?,
            log_socket_path: required_path(&values, b"LogSocketPath")?,
            metric_socket_path: required_path(&values, b"MetricSocketPath")?,
            storage_shards: usize::try_from(dword(&values, b"StorageShards", 0, 0, 256))
                .expect("StorageShards fits usize"),
            max_batch_size: usize::try_from(dword(&values, b"MaxBatchSize", 10_000, 100, 100_000))
                .expect("MaxBatchSize fits usize"),
            max_batch_latency: Duration::from_millis(u64::from(dword(
                &values,
                b"MaxBatchLatencyMs",
                100,
                10,
                5_000,
            ))),
            wal_checkpoint_pages: dword(&values, b"WalCheckpointPages", 1_000, 100, 100_000),
            log_max_batch_size: usize::try_from(dword(
                &values,
                b"LogMaxBatchSize",
                5_000,
                100,
                100_000,
            ))
            .expect("LogMaxBatchSize fits usize"),
            log_max_batch_latency: Duration::from_millis(u64::from(dword(
                &values,
                b"LogMaxBatchLatencyMs",
                500,
                10,
                5_000,
            ))),
            max_log_datagram_bytes: usize::try_from(dword(
                &values,
                b"MaxLogDatagramBytes",
                262_144,
                4_096,
                1_048_576,
            ))
            .expect("MaxLogDatagramBytes fits usize"),
            metric_max_batch_size: usize::try_from(dword(
                &values,
                b"MetricMaxBatchSize",
                5_000,
                100,
                100_000,
            ))
            .expect("MetricMaxBatchSize fits usize"),
            metric_max_batch_latency: Duration::from_millis(u64::from(dword(
                &values,
                b"MetricMaxBatchLatencyMs",
                1_000,
                10,
                5_000,
            ))),
            max_metric_datagram_bytes: usize::try_from(dword(
                &values,
                b"MaxMetricDatagramBytes",
                262_144,
                4_096,
                1_048_576,
            ))
            .expect("MaxMetricDatagramBytes fits usize"),
            metric_series_cache_size: usize::try_from(dword(
                &values,
                b"MetricSeriesCacheSize",
                50_000,
                1_000,
                1_000_000,
            ))
            .expect("MetricSeriesCacheSize fits usize"),
            query_timeout: Duration::from_millis(u64::from(dword(
                &values,
                b"QueryTimeoutMs",
                30_000,
                1_000,
                300_000,
            ))),
            max_concurrent_queries: usize::try_from(dword(
                &values,
                b"MaxConcurrentQueries",
                128,
                1,
                4_096,
            ))
            .expect("MaxConcurrentQueries fits usize"),
            max_streaming_queries: usize::try_from(dword(
                &values,
                b"MaxStreamingQueries",
                64,
                1,
                1_024,
            ))
            .expect("MaxStreamingQueries fits usize"),
            max_distinct_stream_values: usize::try_from(dword(
                &values,
                b"MaxDistinctStreamValues",
                100_000,
                1_000,
                10_000_000,
            ))
            .expect("MaxDistinctStreamValues fits usize"),
            max_query_request_bytes: usize::try_from(dword(
                &values,
                b"MaxQueryRequestBytes",
                65_536,
                1_024,
                16_777_216,
            ))
            .expect("MaxQueryRequestBytes fits usize"),
            query_response_target_bytes: usize::try_from(dword(
                &values,
                b"QueryResponseTargetBytes",
                65_536,
                1_024,
                16_777_216,
            ))
            .expect("QueryResponseTargetBytes fits usize"),
            cross_type_window: Duration::from_millis(u64::from(dword(
                &values,
                b"CrossTypeWindowMs",
                15_000,
                1_000,
                300_000,
            ))),
            cross_type_max_lookback: Duration::from_secs(u64::from(dword(
                &values,
                b"CrossTypeMaxLookbackSeconds",
                604_800,
                3_600,
                2_592_000,
            ))),
            event_retention: Duration::from_secs(
                u64::from(dword(&values, b"EventRetentionDays", 30, 1, 3_650)) * 86_400,
            ),
            event_retention_max_bytes: qword(&values, b"EventRetentionMaxBytes", 0),
            log_retention: Duration::from_secs(
                u64::from(dword(&values, b"LogRetentionDays", 14, 1, 3_650)) * 86_400,
            ),
            log_retention_max_bytes: qword(&values, b"LogRetentionMaxBytes", 0),
            metric_retention: Duration::from_secs(
                u64::from(dword(&values, b"MetricRetentionDays", 90, 1, 3_650)) * 86_400,
            ),
            metric_retention_max_bytes: qword(&values, b"MetricRetentionMaxBytes", 0),
            retention_interval: Duration::from_secs(
                u64::from(dword(
                    &values,
                    b"RetentionCheckIntervalMinutes",
                    60,
                    1,
                    1_440,
                )) * 60,
            ),
            retention_delete_batch_rows: usize::try_from(dword(
                &values,
                b"RetentionDeleteBatchRows",
                10_000,
                100,
                100_000,
            ))
            .expect("RetentionDeleteBatchRows fits usize"),
        })
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
}
