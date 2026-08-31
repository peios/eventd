//! The performance-critical, kernel-independent part of eventd.

pub mod boot_id;
pub mod field;
pub mod log_store;
pub mod meta_store;
pub mod metric_store;
pub mod model;
pub mod payload_index;
mod quarantine;
pub mod queue;
pub mod receipt;
pub mod reconcile;
pub mod routing;
pub mod shard;

pub use boot_id::{BootId, BootIdError};
pub use field::{field_guid, payload_index_name, valid_field_path};
pub use log_store::{LogRecord, LogStore, LogStoreError};
pub use meta_store::{DesiredIndex, IndexCounter, MetaStore, MetaStoreError};
pub use metric_store::{
    Histogram, MetricCommitStats, MetricRecord, MetricStore, MetricStoreError, MetricType,
    MetricTypeMismatch, MetricValue,
};
pub use model::{Gap, Guid, IngestItem, RealEvent, SyntheticEvent};
pub use payload_index::{PayloadIndexValue, query_key as payload_query_key};
pub use queue::{BoundedQueue, Pop, QueueConfigError, ReserveError};
pub use receipt::{Coverage, Interval};
pub use reconcile::{Observation, ReconcileError, Reconciler};
pub use routing::{StripeRouter, assigned_shards};
pub use shard::{CommitStats, IndexAction, Shard, ShardError};
