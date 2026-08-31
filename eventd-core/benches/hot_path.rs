//! Dependency-free microbenchmarks for the two write-path choke points.

use std::hint::black_box;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use eventd_core::{
    BoundedQueue, IngestItem, MetricRecord, MetricStore, MetricType, MetricValue, Pop, RealEvent,
    Shard,
};

const ITEMS: usize = 200_000;

fn main() {
    benchmark_queue();
    benchmark_sqlite();
    benchmark_metric_sqlite();
}

fn benchmark_queue() {
    let queue = BoundedQueue::new(4_096, 16 * 1024 * 1024).unwrap();
    let started = Instant::now();
    std::thread::scope(|scope| {
        let producer = queue.clone();
        scope.spawn(move || {
            for value in 0..ITEMS {
                producer.reserve(64).unwrap().publish(value);
            }
        });
        for _ in 0..ITEMS {
            match queue.pop_wait() {
                Pop::Item(value) => black_box(value),
                Pop::Empty | Pop::Closed => unreachable!(),
            };
        }
    });
    report("queue handoff", ITEMS, started.elapsed());
}

fn benchmark_sqlite() {
    let directory = temporary_directory("events");
    let mut shard = Shard::open(directory.join("shard-0000.db"), 1_000).unwrap();
    let event = RealEvent {
        boot_id: [1; 16],
        timestamp: 1,
        cpu_id: 0,
        sequence: 1,
        origin_class: 0,
        effective_token_guid: [2; 16],
        true_token_guid: [3; 16],
        process_guid: [4; 16],
        event_type: "benchmark.event".into(),
        payload: vec![0_u8; 96].into_boxed_slice(),
    };
    let mut batch = Vec::with_capacity(10_000);
    let started = Instant::now();
    for sequence in 1..=ITEMS as u64 {
        let mut event = event.clone();
        event.sequence = sequence;
        event.timestamp = sequence;
        batch.push(IngestItem {
            gaps: Vec::new(),
            store_event: true,
            event,
        });
        if batch.len() == batch.capacity() {
            shard.commit(&batch).unwrap();
            batch.clear();
        }
    }
    if !batch.is_empty() {
        shard.commit(&batch).unwrap();
    }
    report("SQLite FULL WAL", ITEMS, started.elapsed());
    drop(shard);
    std::fs::remove_dir_all(directory).unwrap();
}

fn benchmark_metric_sqlite() {
    let directory = temporary_directory("metrics");
    let mut store = MetricStore::open(directory.join("metrics.db"), 1_000, 50_000).unwrap();
    let sample = MetricRecord {
        boot_id: [1; 16],
        timestamp: 1,
        name: "benchmark.metric".into(),
        labels: "core=0".into(),
        metric_type: MetricType::Gauge,
        value: MetricValue::Number(1.0),
    };
    let mut batch = Vec::with_capacity(5_000);
    let started = Instant::now();
    for timestamp in 1..=i64::try_from(ITEMS).unwrap() {
        let mut sample = sample.clone();
        sample.timestamp = timestamp;
        batch.push(sample);
        if batch.len() == batch.capacity() {
            let stats = store.commit(&batch).unwrap();
            black_box(stats.accepted);
            batch.clear();
        }
    }
    if !batch.is_empty() {
        let stats = store.commit(&batch).unwrap();
        black_box(stats.accepted);
    }
    report("metric SQLite WAL", ITEMS, started.elapsed());
    drop(store);
    std::fs::remove_dir_all(directory).unwrap();
}

fn temporary_directory(label: &str) -> std::path::PathBuf {
    let mut directory = std::env::temp_dir();
    directory.push(format!(
        "eventd-bench-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    directory
}

fn report(label: &str, items: usize, elapsed: Duration) {
    let per_second = (items as u128 * 1_000_000_000) / elapsed.as_nanos();
    println!("{label:20} {per_second:>12} items/s  ({elapsed:.3?})");
}
