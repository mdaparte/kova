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
| 4 | Analytical Query Optimization (DataFusion SQL) | ✅ Complete |
| 5 | Disk Storage Engine (WAL + Buffer Pool + Page Store) | ✅ Complete |
| 5b | Disk-Resident Collections (pages are the database, RAM is the cache) | 🔵 Current |
| 5.5 | MongoDB Aggregation Pipeline | ⚪ Planned |
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
- SQL via DataFusion: `db.runCommand({ sql: "..." })` executes against in-memory
  collections and returns a cursor. (Phase 4.)

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

## Phase 4 — Analytical Query Optimization (DataFusion SQL) ✅

- Integrate `Apache DataFusion` (v45).
- SQL via `db.runCommand({ sql: "..." })` executes against in-memory collections.
- `MemoryTable` exposes one collection snapshot as a DataFusion table. Phase 5b
  replaces that snapshot with a scan that reads pages through the buffer pool.
- `SELECT` without `FROM`, `WHERE`, `COUNT(*)`, and `GROUP BY` return a cursor.
- `parse_sql()` returns the logical plan for debugging.
- One `FROM` table per query. Joins are out of scope. The Mongo aggregation
  pipeline (`aggregate`, `$match`, `$group`, …) is Phase 5.5, after collections
  live on pages (Phase 5b).

### Phase 4 — Sub-tasks

- [x] Add `datafusion = "45"` dependency.
- [x] Create `src/query_engine/mod.rs` with `parse_sql()` and execution.
- [x] Wire SQL command into `network/mod.rs` (`execute_command` branch on `"sql"` key).
- [x] Validate `SELECT 1 AS n` returns a row when the query has no `FROM`.
- [x] Integrate DataFusion with in-memory storage (MemoryTable provider).
- [x] Execute real SQL queries against stored documents and return result sets.
- [x] Validate `SELECT ... FROM ... WHERE ...`, `COUNT(*)`, `GROUP BY`.
- [x] Add unit tests for query_engine parsing + execution.

## Phase 5 — Disk Storage Engine (WAL + Buffer Pool + Page Store) ✅

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

- **Hot data in memory**: the partition hash map is the working set. Finds,
  updates, and SQL read it.
- **Durability on disk**: every mutation is appended to that partition's WAL
  before the map changes. A checkpoint writes the map into fixed-size pages
  through the buffer pool, then deletes the WAL segments the snapshot covers.
- **Startup**: load the checkpoint pages (buffer pool on miss) and replay the
  WAL tail. The search index is rebuilt from the restored documents.
- **Layout**: `{data_dir}/partition-{id}/checkpoint.bin`, `wal/`, `pages/`.
- The page file is a snapshot of the whole map. Startup loads every document
  back into RAM. A database larger than memory does not fit. Phase 5b is the
  one that keeps documents on the pages and uses RAM only as a cache.

### 5.5 Durability Guarantees

- Every write goes through the WAL before reaching the in-memory state.
- On crash, the WAL is replayed to reconstruct the in-memory state up to the
  last committed LSN.
- Checkpoints periodically flush the in-memory state to disk and truncate the
  WAL.

### Phase 5 — Sub-tasks

- [x] Design the on-disk layout: database directory, collection directories,
  page files, WAL segments.
- [x] Implement `src/storage/wal.rs`: segmented WAL with CRC32 + LSN.
- [x] Implement `src/storage/page.rs`: fixed-size page with header/checksum.
- [x] Implement `src/storage/buffer_pool.rs`: LRU cache with pin/unpin.
- [x] Implement `src/storage/disk.rs`: low-level disk I/O (read/write pages).
- [x] Implement `src/storage/engine.rs`: unified storage (WAL + buffer pool +
  disk + in-memory hash maps).
- [x] Integrate the disk storage engine with the existing partition workers.
- [x] Add durability tests: crash recovery, WAL replay, checkpoint.
- [x] Add configuration for `data_dir`, `page_size`, `buffer_pool_pages`,
  `wal_fsync` in `kova.toml`.

## Phase 5b — Disk-Resident Collections 🔵 Current

Phase 5 made a crash survivable by copying the in-memory map to disk and
loading that copy back on startup. The map is still the database: `find`,
`update`, and SQL read it, so the collection has to fit in RAM.

Phase 5b turns that around. The page file is the database. RAM holds the
buffer pool (default 4096 pages × 16 KB, set by `buffer_pool_pages`) and
nothing else that grows with the collection. A query that misses the pool
reads the page from disk. When the pool is full, the least recently used
unpinned page is written back if it is dirty and then dropped.

The WAL from Phase 5 stays. It records the mutation before the page is
dirtied. It is not where the document lives.

### 5b.1 Documents live in pages

- A collection is a set of pages, not a `Vec` that must all be resident.
- Each document is BSON inside a page. The page header gains a slot
  directory (offset and length of each document).
- A document larger than the page payload spans overflow pages.
- Free space in a page is reused. Page ids are not recycled out from
  under a reader that still has the page pinned.
- `_id` to page id, and the list of pages in a collection, live in a
  catalog that is itself stored in pages and cached by the buffer pool.
  The catalog is not a second copy of every document.

### 5b.2 The buffer pool is the only working set

- `insert`, `find`, `update`, `delete`, and `count` pin pages, read or
  change the slots, then unpin.
- A miss reads the page from disk. Eviction writes a dirty page back
  before reuse.
- Startup opens the catalog and replays the WAL tail onto the pages it
  names. It does not materialize every document into a hash map.
- The partition `HashMap<String, Vec<Document>>` stops being the store.

### 5b.3 Checkpoint

- A checkpoint flushes dirty pages, records the last LSN those pages
  cover, and deletes the WAL segments behind that LSN.
- It does not serialize the whole collection into one BSON image.
- After a crash, flushed pages stay as they are. Recovery applies only
  the WAL records newer than the checkpoint.

### 5b.4 SQL and search

- DataFusion scans a collection by pulling pages through the buffer pool
  in batches. The command `db.runCommand({ sql: "..." })` does not change.
- Tantivy stays an index of tokens in RAM. It is updated on write and
  rebuilt by scanning pages on startup. It does not keep a second copy
  of the BSON.

### 5b.5 Tests

- Insert more documents than `buffer_pool_pages` can hold. A later `find`
  still returns a document that had to be read from disk.
- Restart, then `find` the same document. The process must not have
  loaded the whole collection into a `Vec` to answer.
- `update` and `delete` of a document that was not resident.
- A document larger than one page round-trips.
- `SELECT` over that collection returns the same rows as `find`.

### Phase 5b — Sub-tasks

- [ ] Add a slot directory and overflow pages in `src/storage/page.rs`.
- [ ] Store the collection catalog (page list, free space, `_id` to page
  id) in pages, cached by the buffer pool.
- [ ] Route insert, find, update, delete, and count through the buffer
  pool. Remove the resident `Vec<Document>` as the store.
- [ ] Change checkpoint to a dirty-page flush plus an LSN, not a full-map
  BSON image.
- [ ] Apply the WAL tail to pages on startup, without loading every
  document into RAM.
- [ ] Scan pages in batches for DataFusion SQL.
- [ ] Rebuild the Tantivy index by scanning pages.
- [ ] Tests: working set larger than the pool, restart, find, update,
  overflow document, SQL matches find.

## Phase 5.5 — MongoDB Aggregation Pipeline ⚪ Planned

Runs after Phase 5b. The `aggregate` command reads documents from pages
through the buffer pool, not from a second in-memory copy. mongosh and
Compass already send this command; today it is rejected as
`unsupported command aggregate`.

### 5.5.1 Command and cursor

- `aggregate` on a collection, with a `pipeline` array and a `cursor`.
- Reply is a cursor (`firstBatch`), same shape as `find`.
- An empty pipeline returns the collection.
- Unknown stages fail the command with the stage name, instead of dropping the filter.

### 5.5.2 Stages

- `$match` — same predicates as `find` (`$gt`, `$in`, `$and`, `$or`, …).
- `$project` — include, exclude, and rename fields.
- `$sort`, `$skip`, `$limit`.
- `$group` — `_id` plus `$sum`, `$avg`, `$min`, `$max`, `$first`, `$last`.
- `$count`.
- `$unwind` on one array field.

### 5.5.3 Integrated test

After Phase 5b, one mongosh session loads the Oracle `emp` sample and checks
the pipeline against the same question asked in SQL:

- `$match` on `sal` and `job` returns the same rows as `WHERE`.
- `$group` by `job` with `$sum` returns the same counts as `GROUP BY`.
- A crash-and-restart between the insert and the aggregate still sees the
  documents, read back from pages. The collection must not have been
  loaded entirely into RAM to answer.

### Phase 5.5 — Sub-tasks

- [ ] Accept `aggregate` in `command_from_document` and route it to the partition that owns the collection.
- [ ] Implement `src/aggregate/mod.rs`: run a pipeline over the storage engine's document scan.
- [ ] Implement `$match`, `$project`, `$sort`, `$skip`, `$limit`, `$count`, `$group`, `$unwind`.
- [ ] Return a cursor reply from the network layer.
- [ ] Integration test with mongosh against the Phase 5b engine: seed `emp`, aggregate, compare with SQL, restart, aggregate again.

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
| The whole collection is loaded into RAM | 5b | Pages are the database. The buffer pool caches hot pages. A miss reads disk. |
| `aggregate` is rejected | 5.5 | Pipeline after Phase 5b; mongosh test reads pages, including after a restart. |
| No transactions | 6 | MVCC + transaction manager needed. |
| Indexes are metadata only | 7 | B-Tree + Hash + auto-maintain needed. |
| No replication | 8 | Oplog + replica sets needed. |

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
│  DataFusion SQL parsing +  │ │  (Phase 5.5) $match,$group,  │
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

*Phase 5 (WAL, buffer pool, page store) is complete. The current phase is
5b: documents live on pages and RAM is only the buffer pool. After that,
the aggregation pipeline (Phase 5.5), transactional storage (MVCC + ACID),
the indexing subsystem, and replication.*
