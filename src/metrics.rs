//! Process-wide counters for `serverStatus`.
//!
//! Each counter is an atomic. Network tasks and partition workers add
//! to them on the command path and do not take a lock to do it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// The seventeen counters, plus the moment the process started.
pub struct Metrics {
    started: Instant,
    queries: AtomicU64,
    inserts: AtomicU64,
    updates: AtomicU64,
    deletes: AtomicU64,
    connections_active: AtomicU64,
    connections_total: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    wal_writes: AtomicU64,
    buffer_pool_hits: AtomicU64,
    buffer_pool_misses: AtomicU64,
    buffer_pool_evictions: AtomicU64,
    index_lookups: AtomicU64,
    collection_scans: AtomicU64,
    transactions_started: AtomicU64,
    transactions_committed: AtomicU64,
    transactions_aborted: AtomicU64,
}

/// Shared counters. The first read starts the uptime clock.
pub static METRICS: std::sync::LazyLock<Metrics> = std::sync::LazyLock::new(Metrics::new);

/// One sample of every counter.
#[derive(Debug, Clone, Copy)]
pub struct Snapshot {
    pub uptime_secs: u64,
    pub queries: u64,
    pub inserts: u64,
    pub updates: u64,
    pub deletes: u64,
    pub connections_active: u64,
    pub connections_total: u64,
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub wal_writes: u64,
    pub buffer_pool_hits: u64,
    pub buffer_pool_misses: u64,
    pub buffer_pool_evictions: u64,
    pub index_lookups: u64,
    pub collection_scans: u64,
    pub transactions_started: u64,
    pub transactions_committed: u64,
    pub transactions_aborted: u64,
}

impl Metrics {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            queries: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            updates: AtomicU64::new(0),
            deletes: AtomicU64::new(0),
            connections_active: AtomicU64::new(0),
            connections_total: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            wal_writes: AtomicU64::new(0),
            buffer_pool_hits: AtomicU64::new(0),
            buffer_pool_misses: AtomicU64::new(0),
            buffer_pool_evictions: AtomicU64::new(0),
            index_lookups: AtomicU64::new(0),
            collection_scans: AtomicU64::new(0),
            transactions_started: AtomicU64::new(0),
            transactions_committed: AtomicU64::new(0),
            transactions_aborted: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> Snapshot {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        Snapshot {
            uptime_secs: self.started.elapsed().as_secs(),
            queries: load(&self.queries),
            inserts: load(&self.inserts),
            updates: load(&self.updates),
            deletes: load(&self.deletes),
            connections_active: load(&self.connections_active),
            connections_total: load(&self.connections_total),
            bytes_in: load(&self.bytes_in),
            bytes_out: load(&self.bytes_out),
            wal_writes: load(&self.wal_writes),
            buffer_pool_hits: load(&self.buffer_pool_hits),
            buffer_pool_misses: load(&self.buffer_pool_misses),
            buffer_pool_evictions: load(&self.buffer_pool_evictions),
            index_lookups: load(&self.index_lookups),
            collection_scans: load(&self.collection_scans),
            transactions_started: load(&self.transactions_started),
            transactions_committed: load(&self.transactions_committed),
            transactions_aborted: load(&self.transactions_aborted),
        }
    }

    pub fn query(&self) {
        self.queries.fetch_add(1, Ordering::Relaxed);
    }

    pub fn insert(&self) {
        self.inserts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn update(&self) {
        self.updates.fetch_add(1, Ordering::Relaxed);
    }

    pub fn delete(&self) {
        self.deletes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_opened(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
        self.connections_active.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_closed(&self) {
        self.connections_active
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(1))
            })
            .ok();
    }

    pub fn bytes_in(&self, n: u64) {
        self.bytes_in.fetch_add(n, Ordering::Relaxed);
    }

    pub fn bytes_out(&self, n: u64) {
        self.bytes_out.fetch_add(n, Ordering::Relaxed);
    }

    pub fn wal_write(&self) {
        self.wal_writes.fetch_add(1, Ordering::Relaxed);
    }

    pub fn buffer_hit(&self) {
        self.buffer_pool_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn buffer_miss(&self) {
        self.buffer_pool_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn buffer_evict(&self) {
        self.buffer_pool_evictions.fetch_add(1, Ordering::Relaxed);
    }

    pub fn index_lookup(&self) {
        self.index_lookups.fetch_add(1, Ordering::Relaxed);
    }

    pub fn collection_scan(&self) {
        self.collection_scans.fetch_add(1, Ordering::Relaxed);
    }

    pub fn transaction_started(&self) {
        self.transactions_started.fetch_add(1, Ordering::Relaxed);
    }

    pub fn transaction_committed(&self) {
        self.transactions_committed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn transaction_aborted(&self) {
        self.transactions_aborted.fetch_add(1, Ordering::Relaxed);
    }
}
