//! The performance-critical, kernel-independent part of eventd.

pub mod boot_id;
pub mod log_store;
pub mod meta_store;
pub mod metric_store;
pub mod model;
mod quarantine;
pub mod queue;
pub mod receipt;
pub mod reconcile;
pub mod routing;
pub mod shard;

pub use boot_id::{BootId, BootIdError};
pub use log_store::{LogRecord, LogStore, LogStoreError};
pub use meta_store::{DesiredIndex, IndexCounter, MetaStore, MetaStoreError};
pub use metric_store::{
    Histogram, MetricCommitStats, MetricRecord, MetricStore, MetricStoreError, MetricType,
    MetricValue,
};
pub use model::{Gap, Guid, IngestItem, RealEvent, SyntheticEvent};
pub use queue::{BoundedQueue, Pop, QueueConfigError, ReserveError};
pub use receipt::{Coverage, Interval};
pub use reconcile::{Observation, ReconcileError, Reconciler};
pub use routing::{StripeRouter, assigned_shards};
pub use shard::{CommitStats, IndexAction, Shard, ShardError};
