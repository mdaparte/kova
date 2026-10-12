# Kova Roadmap

> "This is the single source of truth for the project roadmap. Read it
> before opening any file."

## Vision (Why)

Unify transactional in-memory processing, MongoDB native interoperability,
full-text search ( Tantivy), and analytical SQL query optimization (DataFusion)
in a single, lock-free, **hybrid memory+disk** engine with durability,
replication, and multi-model indexing.

## Status

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Networking & Interception | ✅ Complete |
| 2 | Partition Isolation | ✅ Complete |
| 3 | Full-Text Search Indexing | ✅ Complete |
| 3.5 | Search Flexibility & Configuration | ✅ Complete |
| 4 | Analytical Query Optimization (DataFusion SQL) | 🔵 Current |
| 5 | Disk Storage Engine (WAL + Buffer Pool + Page Store) | ⚪ Planned |
| 6 | Transactional Storage (MVCC + ACID + Isolation) | ⚪ Planned |
| 7 | Indexing Subsystem (B-Tree, Hash, Unique, Sparse, Auto-Maintain) | ⚪ Planned |
| 8 | Replication & High Availability (Oplog + Replica Sets + Heartbeat) | ⚪ Planned |

## Phase 1 — Networking & Interception ✅

- TCP listener (default port 27018, overridable via `KOVA_PORT`).
- 16-byte Wire Protocol header parsed in **little-endian**.
- `OP_MSG (2013)` and legacy `OP_QUERY (2004)` handled; OP_QUERY answered with `OP_REPLY (1)`.
- Handshake answered at the protocol layer: `ping`, `hello`, `isMaster`/`ismaster`,
  `buildInfo`, `getParameter`, `endSessions`, `whatsmyuri`, `connectionStatus`,
  `hostInfo`, `atlasVersion`.
- OP_MSG decoder honors bit 0 for trailing CRC-32C checksum (Java driver sets it;
  Node driver does not).
- `messageLength` validated (16 B … 48 MB) before allocation.
- One `tokio::spawn` task per client; abrupt disconnects logged with peer address.
- SQL parsing via DataFusion: `db.runCommand({ sql: "..." })` accepted, returns
  the logical/physical plan or an error. (Phase 4 entry point.)

## Phase 2 — Partition Isolation ✅

- `PartitionCommand` enum: insert, find, update, delete, count, drop, create,
  dropDatabase, createIndexes, listIndexes, listCollections, listDatabases,
  collStats, dbStats, search.
- `crossbeam-channel` routing; single-threaded partition worker per core,
  no locks on the hot path.
- In-memory storage: hash map per collection, deterministic FNV-1a partition
  routing.
- **Database namespacing**: collections stored under `db.collection`.
- **Driver compatibility**: `createIndexes`/`listIndexes` implemented for
  Java/Spring Data clients; index declarations recorded as metadata.
- **Storage stats**: `collStats` (real serialized size, `avgObjSize` measured)
  and `dbStats` (broadcast + summed across partitions).
- Validated end-to-end with a real `mongosh`: insertMany, find (+filtered),
  count, updateOne (`n`/`nModified`), deleteOne, drop, listCollections.

## Phase 3 — Full-Text Search Indexing ✅

- `tantivy` integrated with a RAM directory per partition.
- Hot indexing: documents tokenized as they arrive (insert/update/delete/drop
  keep the index consistent).
- Relevance scoring (BM25) and fuzzy (distance-1) fallback via `$text`.
- Validated end-to-end with a real `mongosh` (8/8 assertions).

## Phase 3.5 — Search Flexibility & Configuration ✅

- Per-field indexes (one tantivy index per document field per partition).
- `$text.$boost`: per-field BM25 multipliers.
- Wildcards: Solr-style `dat*`/`d?ta` tokens expand to whole-term automaton.
- `$text.$decay`: exponential half-life recency decay over a BSON date field.
- Configurable analyzer: `[search]` section in `kova.toml` selects `default` or
  `en_stem` + custom stop words.
- Configuration file: `kova.toml` (port, log level, non-Mongo warning toggle,
  analyzer), overridable by environment variables.
- Non-Mongo warnings: Kova extensions (`$boost`, `$decay`) and unsupported
  operators (`$analyzer`, `$language`) log a WARN when enabled.

## Phase 4 — Analytical Query Optimization (DataFusion SQL) 🔵 Current

- Integrate `Apache DataFusion` (v45).
- SQL parsing and AST generation via `db.runCommand({ sql: "..." })`.
- Logical + physical plan extraction and description for debugging.
- **Next step (this phase):** integrate DataFusion with the storage layer so
  SQL queries actually execute against data. Start with a **MemoryTable**
  provider that exposes in-memory collections as DataFusion tables; later
  (Phase 5+) swap in the disk-backed provider without changing the query path.

### Phase 4 — Sub-tasks

- [x] Add `datafusion = "45"` dependency.
- [x] Create `src/query_engine/mod.rs` with `parse_sql()` and `plan_description()`.
- [x] Wire SQL command into `network/mod.rs` (`execute_command` branch on `"sql"` key).
- [x] Validate `SELECT 1 AS n` returns a plan (EmptyExec).
- [ ] Integrate DataFusion with in-memory storage (MemoryTable provider).
- [ ] Execute real SQL queries against stored documents and return result sets.
- [ ] Validate `SELECT ... FROM ... WHERE ...`, `COUNT(*)`, `GROUP BY`.
- [ ] Add unit tests for query_engine parsing + execution.

## Phase 5 — Disk Storage Engine (WAL + Buffer Pool + Page Store) ⚪ Planned

_Inspiration: GraniteDB (https://github.com/jmptrader/GraniteDB)_

### 5.1 Write-Ahead Log (WAL)

- Segmented WAL with CRC32 checksums per entry.
- LSN (Log Sequence Number) tracking for crash recovery.
- Automatic segment rolling when a segment reaches its size limit.
- `fsync` on commit (configurable: `wal_fsync = true/false` in `kova.toml`).
- Recovery on startup: replay WAL entries since the last checkpoint.

### 5.2 Page-Based Storage

- Fixed-size pages (default 16 KB, configurable via `page_size`).
- Pages stored on disk in a structured directory layout.
- Page integrity verification (checksum/header) on read.
- Overflow support for large documents (document spans multiple pages).
- Page manager handles allocation, deallocation, and reuse of pages.

### 5.3 Buffer Pool

- LRU-based page cache with pin/unpin eviction.
- Configurable capacity (default: 4096 pages, via `buffer_pool_pages`).
- Performance counters: hits, misses, evictions, pin count.
- Dirty page writeback: flush dirty pages to disk on checkpoint or eviction.

### 5.4 Hybrid Memory + Disk Architecture

- **Hot data in memory**: frequently accessed documents/collections stay in RAM
  (the existing in-memory hash maps).
- **Cold data on disk**: less-frequently accessed data is paged to disk.
- **Buffer pool as the bridge**: the buffer pool caches disk pages; the memory
  hash maps hold the working set.
- **Query execution**: DataFusion reads from the buffer pool (which reads from
  disk on miss); after Phase 6, MVCC reads from snapshots.

### 5.5 Durability Guarantees

- Every write goes through the WAL before reaching the in-memory state.
- On crash, the WAL is replayed to reconstruct the in-memory state up to the
  last committed LSN.
- Checkpoints periodically flush the in-memory state to disk and truncate the
  WAL.

### Phase 5 — Sub-tasks

- [ ] Design the on-disk layout: database directory, collection directories,
  page files, WAL segments.
- [ ] Implement `src/storage/wal.rs`: segmented WAL with CRC32 + LSN.
- [ ] Implement `src/storage/page.rs`: fixed-size page with header/checksum.
- [ ] Implement `src/storage/buffer_pool.rs`: LRU cache with pin/unpin.
- [ ] Implement `src/storage/disk.rs`: low-level disk I/O (read/write pages).
- [ ] Implement `src/storage/engine.rs`: unified storage (WAL + buffer pool +
  disk + in-memory hash maps).
- [ ] Integrate the disk storage engine with the existing partition workers.
- [ ] Add durability tests: crash recovery, WAL replay, checkpoint.
- [ ] Add configuration for `data_dir`, `page_size`, `buffer_pool_pages`,
  `wal_fsync` in `kova.toml`.

## Phase 6 — Transactional Storage (MVCC + ACID + Isolation) ⚪ Planned

_Inspiration: GraniteDB (MVCC, ACID transactions, multiple isolation levels)_

### 6.1 MVCC (Multi-Version Concurrency Control)

- Each document version gets a timestamp/LSN.
- Snapshot reads: a query sees the database as of a point in time.
- Garbage collection of old versions no longer needed by any active transaction.

### 6.2 ACID Transactions

- `beginTransaction`, `commit`, `abort` commands.
- Atomicity: all-or-nothing via the WAL + MVCC.
- Consistency: schema validation (optional, JSON-Schema-like) on insert/update.
- Isolation levels: Read Uncommitted, Read Committed, Repeatable Read, Snapshot,
  Serializable.
- Conflict detection: write-write conflicts abort the later transaction.

### 6.3 Transaction Manager

- Tracks active transactions, their isolation level, and their snapshot.
- On commit: flush to WAL, advance LSN, make the new version visible.
- On abort: discard the transaction's writes.
- Timeout enforcement: automatic abort on timeout.

### Phase 6 — Sub-tasks

- [ ] Design MVCC version storage: per-document version chain (LSN, document,
  deleted flag).
- [ ] Implement `src/storage/mvcc.rs`: version management + snapshot reads.
- [ ] Implement `src/storage/transaction.rs`: begin/commit/abort + conflict
  detection + timeout.
- [ ] Integrate MVCC with the buffer pool and WAL.
- [ ] Add transaction tests: ACID properties, isolation levels, conflict
  detection, timeout.

## Phase 7 — Indexing Subsystem (B-Tree, Hash, Unique, Sparse, Auto-Maintain) ⚪ Planned

_Inspiration: GraniteDB (B-Tree, Hash, Unique, Sparse, Index Manager)_

### 7.1 Index Types

- **B-Tree Indexes**: ordered indexes for range queries + composite key support.
- **Hash Indexes**: O(1) exact-match lookups.
- **Unique Constraints**: enforce uniqueness across indexed fields.
- **Sparse Indexes**: skip documents missing the indexed field.
- **Full-Text Indexes (Tantivy)**: already implemented in Phase 3/3.5; the index
  manager will create, drop, and auto-maintain them alongside other index types.

### 7.2 Index Manager

- Create index: `createIndexes` command (already in Phase 2, but now builds the
  actual index structure instead of just recording metadata).
- Drop index: `deleteIndexes` (or `dropIndexes`) command.
- Auto-maintain: indexes are updated on insert/update/delete (no manual
  reindexing needed).
- Index metadata stored in the disk storage (not just in-memory).

### 7.3 Query Planner Integration

- The query planner (Phase 4 + DataFusion) consults the index manager to choose
  the best strategy: ID lookup → index scan → collection scan.
- Explain plans: `db.runCommand({ explain: { find: "users", filter: { age: { $gt: 25 } } } })`
  shows the chosen strategy.

### Phase 7 — Sub-tasks

- [ ] Design index on-disk layout (B-Tree pages, hash buckets, full-text index
  files).
- [ ] Implement `src/index/btree.rs`: B-Tree index with range + composite keys.
- [ ] Implement `src/index/hash_index.rs`: Hash index for O(1) lookups.
- [ ] Implement `src/index/manager.rs`: create/drop/auto-maintain indexes.
- [ ] Integrate index manager with the storage engine (indexes read/write through
  the buffer pool).
- [ ] Integrate index manager with DataFusion query planner (index scan strategy).
- [ ] Add index tests: create, drop, auto-maintain, unique constraint, sparse
  index, query planner index usage.

## Phase 8 — Replication & High Availability (Oplog + Replica Sets + Heartbeat) ⚪ Planned

_Inspiration: GraniteDB (Oplog, Replica Sets, Heartbeat, Sharding)_

### 8.1 Oplog (Capped Operations Log)

- Every write operation is recorded in a capped operations log with a timestamp.
- The oplog is used for replication: secondaries replay the oplog to stay in sync
  with the primary.
- Timestamp-based sync queries: a secondary asks the primary for operations
  after a given timestamp.

### 8.2 Replica Sets

- Roles: Primary (read/write), Secondary (read-only, replicates from primary),
  Arbiter (votes, no data).
- Heartbeat monitoring: each member sends heartbeats to the others; if a member
  misses heartbeats, it is considered down.
- Election: if the primary is down, the secondaries hold an election to choose a
  new primary.
- Read preference: clients can read from primary, secondary, or nearest.

### 8.3 Sharding (future extension, optional)

- Consistent hashing with virtual nodes for even data distribution.
- Key range sharding: automatic routing based on shard key values.
- Shard router: routes queries to the correct shard.

### Phase 8 — Sub-tasks

- [ ] Design the oplog on-disk layout (capped collection of operations with
  timestamps).
- [ ] Implement `src/replication/oplog.rs`: capped operations log + timestamp
  queries.
- [ ] Implement `src/replication/replica.rs`: replica set management (add/remove
  members, election, state transitions).
- [ ] Implement heartbeat monitor: track member health, detect failures.
- [ ] Integrate oplog with the storage engine (every write appends to the oplog).
- [ ] Add replication tests: replica set setup, failover, oplog replay, sync.

## Architecture Principles

1. **Hybrid memory+disk**: hot data in RAM (in-memory hash maps + buffer pool
   cache), cold data on disk (page store). The buffer pool is the bridge.
2. **Durability first**: WAL before memory, checkpoint to truncate WAL, crash
   recovery on startup.
3. **ACID transactions**: MVCC for snapshot reads, conflict detection, multiple
   isolation levels.
4. **Native MongoDB**: port 27018, `OP_MSG` (+ legacy `OP_QUERY`), little-endian
   wire format, BSON parsing, `db.runCommand({ sql: "..." })` for SQL.
5. **Multi-model indexing**: B-Tree (range), Hash (O(1)), Unique, Sparse, and
   Full-Text (Tantivy) — all auto-maintained on write.
6. **Replication & HA**: oplog-based replication, replica sets with election,
   heartbeat monitoring.
7. **Analytical brain**: DataFusion as the internal optimizer, with SQL parsing
   and execution against the storage layer.

## Current Implementation Gaps (to address in Phase 5+)

| Gap | Phase | Notes |
|-----|-------|-------|
| No disk storage (all in-memory) | 5 | WAL + page store + buffer pool needed. |
| No durability (crash = data loss) | 5 | WAL + checkpoint fixes this. |
| No transactions | 6 | MVCC + transaction manager needed. |
| Indexes are metadata only | 7 | B-Tree + Hash + auto-maintain needed. |
| No replication | 8 | Oplog + replica sets needed. |
| SQL only returns plan, not results | 4 (current) | Integrate DataFusion with storage first. |
| DataFusion has no tables registered | 4 | MemoryTable provider for in-memory collections. |

## Architecture Diagram (updated)

```
┌─────────────────────────────────────────────────────────────┐
│                        Client / Driver                       │
│  (MongoDB Wire Protocol, db.runCommand({ sql: "..." }))     │
└─────────────────────────────┬───────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────┐
│                      Network Layer (Phase 1)                 │
│  TCP listener, OP_MSG/OP_QUERY, handshake, SQL command      │
│  routing to query_engine or storage/partition workers        │
└─────────────────────────────┬───────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              │                               │
┌─────────────▼─────────────┐ ┌──────────────▼──────────────┐
│     Query Engine (Phase 4)│ │   Aggregation Pipeline       │
│  DataFusion SQL parsing +  │ │  (Phase 5+) $match,$group,   │
│  execution against storage │ │  $project,$sort,$limit...    │
└─────────────┬──────────────┘ └──────────────┬──────────────┘
              │                               │
┌─────────────▼───────────────────────────────▼──────────────┐
│                   Storage Layer (Phase 5+)                   │
│  ┌────────────────────────────────────────────────────────┐  │
│  │               Unified Storage Engine                    │  │
│  │  ┌──────────┐  ┌───────────┐  ┌─────────────────────┐  │  │
│  │  │   WAL    │  │Buffer Pool│  │   Disk Page Store   │  │  │
│  │  │(CRC32+LSN)│  │  (LRU)    │  │ (fixed-size pages)  │  │  │
│  │  └──────────┘  └───────────┘  └─────────────────────┘  │  │
│  └────────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────────┐  │
│  │                MVCC + Transaction Manager               │  │
│  │  (Phase 6: begin/commit/abort, isolation levels)       │  │
│  └────────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────────┐  │
│  │                   Index Manager (Phase 7)               │  │
│  │  B-Tree │ Hash │ Unique │ Sparse │ Full-Text (Tantivy) │  │
│  └────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              │                               │
┌─────────────▼─────────────┐ ┌──────────────▼──────────────┐
│  Replication (Phase 8)    │ │   Sharding (optional)        │
│  Oplog + Replica Sets +   │ │  Consistent Hashing +        │
│  Heartbeat + Election     │ │  Key Range Sharding          │
└───────────────────────────┘ └──────────────────────────────┘
```

## Contact

Main entry point: `src/main.rs`.

---

*Updated to reflect the new direction: disk storage engine (WAL + buffer
pool + page store, inspired by GraniteDB), hybrid memory+disk architecture,
transactional storage (MVCC + ACID), indexing subsystem (B-Tree, Hash, Unique,
Sparse, Full-Text/Tantivy), and replication (oplog + replica sets). Phase 4
(DataFusion SQL) is current and will integrate with the storage layer as it
evolves from in-memory to hybrid.*
