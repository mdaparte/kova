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
| 4 | Search Flexibility & Configuration | ✅ Complete |
| 5 | Analytical Query Optimization (DataFusion SQL) | ✅ Complete |
| 6 | Disk Storage Engine (WAL + Buffer Pool + Page Store) | ✅ Complete |
| 7 | Disk-Resident Collections (pages are the database, RAM is the cache) | ✅ Complete |
| 8 | Query Language and Aggregation Pipeline | ✅ Complete |
| 9 | Transactional Storage (MVCC + ACID + Isolation) | ⚪ Planned |
| 10 | Indexing Subsystem (B-Tree, Hash, Unique, Sparse, Auto-Maintain) | ⚪ Planned |
| 11 | Replication & High Availability (Oplog + Replica Sets + Heartbeat) | ⚪ Planned |
| 12 | On-Disk Page Compression (zstd, WiredTiger-style) | ⚪ Planned |

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
  collections and returns a cursor. (Phase 5.)

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

## Phase 4 — Search Flexibility & Configuration ✅

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

## Phase 5 — Analytical Query Optimization (DataFusion SQL) ✅

- Integrate `Apache DataFusion` (v45).
- SQL via `db.runCommand({ sql: "..." })` executes against in-memory collections.
- `MemoryTable` exposes one collection snapshot as a DataFusion table. `find`
  walks pages one at a time and returns the matches; the store does not keep
  that `Vec`.
- `SELECT` without `FROM`, `WHERE`, `COUNT(*)`, and `GROUP BY` return a cursor.
- `parse_sql()` returns the logical plan for debugging.
- One `FROM` table per query. Joins are out of scope. The Mongo aggregation
  pipeline (`aggregate`, `$match`, `$group`, …) is Phase 8, after collections
  live on pages (Phase 7).

### Phase 5 — Sub-tasks

- [x] Add `datafusion = "45"` dependency.
- [x] Create `src/query_engine/mod.rs` with `parse_sql()` and execution.
- [x] Wire SQL command into `network/mod.rs` (`execute_command` branch on `"sql"` key).
- [x] Validate `SELECT 1 AS n` returns a row when the query has no `FROM`.
- [x] Integrate DataFusion with in-memory storage (MemoryTable provider).
- [x] Execute real SQL queries against stored documents and return result sets.
- [x] Validate `SELECT ... FROM ... WHERE ...`, `COUNT(*)`, `GROUP BY`.
- [x] Add unit tests for query_engine parsing + execution.

## Phase 6 — Disk Storage Engine (WAL + Buffer Pool + Page Store) ✅

_Inspiration: GraniteDB (https://github.com/jmptrader/GraniteDB)_

### 6.1 Write-Ahead Log (WAL)

- Segmented WAL with CRC32 checksums per entry.
- LSN (Log Sequence Number) tracking for crash recovery.
- Automatic segment rolling when a segment reaches its size limit.
- `fsync` on commit (configurable: `wal_fsync = true/false` in `kova.toml`).
- Recovery on startup: replay WAL entries since the last checkpoint.

### 6.2 Page-Based Storage

- Fixed-size pages (default 16 KB, configurable via `page_size`).
- Pages stored on disk in a structured directory layout.
- Page integrity verification (checksum/header) on read.
- Overflow support for large documents (document spans multiple pages).
- Page manager handles allocation, deallocation, and reuse of pages.

### 6.3 Buffer Pool

- LRU-based page cache with pin/unpin eviction.
- Configurable capacity (default: 4096 pages, via `buffer_pool_pages`).
- Performance counters: hits, misses, evictions, pin count.
- Dirty page writeback: flush dirty pages to disk on checkpoint or eviction.

### 6.4 Hybrid Memory + Disk Architecture

- **Hot data in memory**: the partition hash map is the working set. Finds,
  updates, and SQL read it.
- **Durability on disk**: every mutation is appended to that partition's WAL
  before the map changes. A checkpoint writes the map into fixed-size pages
  through the buffer pool, then deletes the WAL segments the snapshot covers.
- **Startup**: load the checkpoint pages (buffer pool on miss) and replay the
  WAL tail. The search index is rebuilt from the restored documents.
- **Layout**: `{data_dir}/partition-{id}/checkpoint.bin`, `wal/`, `pages/`.
- The page file in this phase was a snapshot of the whole map. Startup loaded
  every document back into RAM. Phase 7 replaced that: documents stay on the
  pages, and RAM is the buffer pool.

### 6.5 Durability Guarantees

- Every write goes through the WAL before reaching the in-memory state.
- On crash, the WAL is replayed to reconstruct the in-memory state up to the
  last committed LSN.
- Checkpoints periodically flush the in-memory state to disk and truncate the
  WAL.

### Phase 6 — Sub-tasks

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

## Phase 7 — Disk-Resident Collections ✅

Phase 6 made a crash survivable by copying the in-memory map to disk and
loading that copy back on startup. The map is still the database: `find`,
`update`, and SQL read it, so the collection has to fit in RAM.

Phase 7 turns that around. The page file is the database. RAM holds the
buffer pool (default 4096 pages × 16 KB, set by `buffer_pool_pages`) and
nothing else that grows with the collection. A query that misses the pool
reads the page from disk. When the pool is full, the least recently used
unpinned page is written back if it is dirty and then dropped.

The WAL from Phase 6 stays. It records the mutation before the page is
dirtied. It is not where the document lives.

### 7.1 Documents live in pages

- A collection is a set of pages, not a `Vec` that must all be resident.
- Each document is BSON inside a page. The page header gains a slot
  directory (offset and length of each document).
- A document larger than the page payload spans overflow pages.
- Free space in a page is reused. Page ids are not recycled out from
  under a reader that still has the page pinned.
- `_id` to page id, and the list of pages in a collection, live in a
  catalog that is itself stored in pages and cached by the buffer pool.
  The catalog is not a second copy of every document.

### 7.2 The buffer pool is the only working set

- `insert`, `find`, `update`, `delete`, and `count` pin pages, read or
  change the slots, then unpin.
- A miss reads the page from disk. Eviction writes a dirty page back
  before reuse.
- Startup opens the catalog and replays the WAL tail onto the pages it
  names. It does not materialize every document into a hash map.
- The partition `HashMap<String, Vec<Document>>` stops being the store.

### 7.3 Checkpoint

- A checkpoint flushes dirty pages, records the last LSN those pages
  cover, and deletes the WAL segments behind that LSN.
- It does not serialize the whole collection into one BSON image.
- After a crash, flushed pages stay as they are. Recovery applies only
  the WAL records newer than the checkpoint.

### 7.4 SQL and search

- DataFusion scans a collection by pulling pages through the buffer pool
  in batches. The command `db.runCommand({ sql: "..." })` does not change.
- Tantivy stays an index of tokens in RAM. It is updated on write and
  rebuilt by scanning pages on startup. It does not keep a second copy
  of the BSON.

### 7.5 Tests

- Insert more documents than `buffer_pool_pages` can hold. A later `find`
  still returns a document that had to be read from disk.
- Restart, then `find` the same document. The process must not have
  loaded the whole collection into a `Vec` to answer.
- `update` and `delete` of a document that was not resident.
- A document larger than one page round-trips.
- `SELECT` over that collection returns the same rows as `find`.

### Phase 7 — Sub-tasks

- [x] Slot directory and overflow chains in `src/storage/heap.rs`. The
  32-byte page envelope is unchanged.
- [x] Catalog page: applied LSN, next page id, index declarations, and
  empty collection names. Page membership is the collection name in
  each page header, found by scanning page files. There is no
  `_id` → page id map; a point lookup scans the collection's pages.
- [x] Route insert, find, update, delete, and count through the buffer
  pool. The resident `Vec<Document>` is no longer the store.
- [x] Checkpoint flushes dirty pages and records the LSN. It does not
  write a full-map BSON image.
- [x] Startup discovers pages and replays only the WAL tail past the
  catalog LSN. It does not load every document into a map.
- [x] `find` walks one page at a time. SQL still receives that result
  set; the store drops each page before reading the next.
- [x] Rebuild the Tantivy index by scanning pages on startup.
- [x] Tests: working set larger than the pool, restart, find, update
  and delete of an evicted document, overflow round-trip. SQL tests
  still read through `find`.

## Phase 8 — Query Language and Aggregation Pipeline ✅

`aggregate` reads documents from pages through the buffer pool, one page at
a time. A leading `$match` is applied during that scan. `find` and `$match`
share one matcher, and `update` uses the same operator document.

The pipeline is the BSON array on the command, the document mongosh already
sends. There is no second syntax.

### 8.1 Command and cursor

- `aggregate` on a collection, with a `pipeline` array and a `cursor`.
- Reply is a cursor (`firstBatch`), same shape as `find`.
- An empty pipeline returns the collection.
- An unknown stage or operator fails the command and names what it rejected.

### 8.2 Predicates

`find` already matches equality and these operators: `$eq`, `$ne`, `$gt`,
`$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`, `$and`, `$or`, `$nor`.
Numbers compare across Int32, Int64, and Double. Dates compare by instant.
This phase adds the rest, and `$match` uses that same function:

- `$not` — negate one operator document.
- `$regex` — match a string field.
- `$type` — match the BSON type of a field.
- `$elemMatch` — one array element must satisfy every predicate in the
  subdocument.

### 8.3 Projection, sort, skip, and limit

`find` today returns every field of every match. A find and a pipeline
share these:

- Projection: include or exclude fields. `_id` stays unless the projection
  removes it.
- Sort on one or more fields.
- Skip and limit, so a client can page through a result.

### 8.4 Update operators

Replacement and `$set` already work, and `_id` is kept. Add `$unset`,
`$inc`, `$push`, `$pull`, and `$rename`. An unknown operator is an error,
not a silent no-op.

### 8.5 Pipeline stages

Twelve stages, in any order the command sends:

- `$match`, `$project`, `$addFields`, `$replaceRoot`.
- `$group`, `$sort`, `$skip`, `$limit`, `$count`, `$unwind`.
- `$lookup` reads the foreign collection from the partition that owns it,
  through the buffer pool. Equality uses `localField` and `foreignField`.
  `let` plus a nested pipeline runs that pipeline for each local document,
  and `$match` inside it accepts `$expr`. The two collections are not
  copied into one resident vector.
- `$out` writes the result into a collection. That write goes through the
  WAL, like any other insert.

### 8.6 Group accumulators

`$group` accepts `$sum`, `$avg`, `$min`, `$max`, `$count`, `$push`,
`$addToSet`, `$first`, and `$last`.

### 8.7 Explain

`explain` on a find or an aggregate reports the filter or the pipeline and
says the read is a collection scan of pages. Choosing an id lookup or an
index scan is Phase 10, when those indexes exist. Phase 10 extends this
reply; it does not invent a second explain command.

### 8.8 Integrated test

One mongosh session loads the Oracle `emp` sample and checks the pipeline
against the same question asked in SQL:

- `$match` on `sal` and `job` returns the same rows as `WHERE`.
- `$group` by `job` with `$sum` returns the same counts as `GROUP BY`.
- A projection drops a field the client did not ask for.
- `$inc` changes `sal`, and a later `$match` sees the new value.
- A crash-and-restart between the insert and the aggregate still sees the
  documents, read back from pages. The collection must not have been
  loaded entirely into RAM to answer.

### Phase 8 — Sub-tasks

- [x] Accept `aggregate` in `command_from_document` and route it to the partition that owns the collection.
- [x] Add `$not`, `$regex`, `$type`, and `$elemMatch` to the shared matcher. `$match` calls that matcher.
- [x] Add projection, sort, skip, and limit to `find` and to the pipeline.
- [x] Add `$unset`, `$inc`, `$push`, `$pull`, and `$rename` to update. Reject an unknown operator.
- [x] Implement `src/storage/aggregate.rs` over the page scan: `$match`, `$project`, `$addFields`, `$replaceRoot`, `$group`, `$sort`, `$skip`, `$limit`, `$count`, `$unwind`, `$lookup`, `$out`.
- [x] Implement the nine `$group` accumulators.
- [x] Return a cursor reply from the network layer.
- [x] Answer `explain` with the pipeline and a collection scan.
- [x] Integration test: seed `emp`, compare `$match`/`$group` with SQL, project, `$inc`, `$lookup`, `$out`, restart, aggregate again.

## Phase 9 — Transactional Storage (MVCC + ACID + Isolation) ⚪ Planned

_Inspiration: GraniteDB (MVCC, ACID transactions, multiple isolation levels)_

### 9.1 MVCC (Multi-Version Concurrency Control)

- Each document version gets a timestamp/LSN.
- Snapshot reads: a query sees the database as of a point in time.
- Garbage collection of old versions no longer needed by any active transaction.

### 9.2 ACID Transactions

- `beginTransaction`, `commit`, `abort` commands.
- Atomicity: all-or-nothing via the WAL + MVCC.
- Consistency: schema validation (optional, JSON-Schema-like) on insert/update.
- Isolation levels: Read Uncommitted, Read Committed, Repeatable Read, Snapshot,
  Serializable.
- Conflict detection: write-write conflicts abort the later transaction.

### 9.3 Transaction Manager

- Tracks active transactions, their isolation level, and their snapshot.
- On commit: flush to WAL, advance LSN, make the new version visible.
- On abort: discard the transaction's writes.
- Timeout enforcement: automatic abort on timeout.

### Phase 9 — Sub-tasks

- [ ] Design MVCC version storage: per-document version chain (LSN, document,
  deleted flag).
- [ ] Implement `src/storage/mvcc.rs`: version management + snapshot reads.
- [ ] Implement `src/storage/transaction.rs`: begin/commit/abort + conflict
  detection + timeout.
- [ ] Integrate MVCC with the buffer pool and WAL.
- [ ] Add transaction tests: ACID properties, isolation levels, conflict
  detection, timeout.

## Phase 10 — Indexing Subsystem (B-Tree, Hash, Unique, Sparse, Auto-Maintain) ⚪ Planned

_Inspiration: GraniteDB (B-Tree, Hash, Unique, Sparse, Index Manager)_

### 10.1 Index Types

- **B-Tree Indexes**: ordered indexes for range queries + composite key support.
- **Hash Indexes**: O(1) exact-match lookups.
- **Unique Constraints**: enforce uniqueness across indexed fields.
- **Sparse Indexes**: skip documents missing the indexed field.
- **Full-Text Indexes (Tantivy)**: already implemented in Phase 3/4; the index
  manager will create, drop, and auto-maintain them alongside other index types.

### 10.2 Index Manager

- Create index: `createIndexes` command (already in Phase 2, but now builds the
  actual index structure instead of just recording metadata).
- Drop index: `deleteIndexes` (or `dropIndexes`) command.
- Auto-maintain: indexes are updated on insert/update/delete (no manual
  reindexing needed).
- Index metadata stored in the disk storage (not just in-memory).

### 10.3 Query Planner Integration

- The query planner consults the index manager to choose the strategy:
  id lookup, then an index scan, then a collection scan.
- `explain`, introduced in Phase 8 as a collection scan, reports which of
  those three strategies ran.

### Phase 10 — Sub-tasks

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

## Phase 11 — Replication & High Availability (Oplog + Replica Sets + Heartbeat) ⚪ Planned

_Inspiration: GraniteDB (Oplog, Replica Sets, Heartbeat, Sharding)_

### 11.1 Oplog (Capped Operations Log)

- Every write operation is recorded in a capped operations log with a timestamp.
- The oplog is used for replication: secondaries replay the oplog to stay in sync
  with the primary.
- Timestamp-based sync queries: a secondary asks the primary for operations
  after a given timestamp.

### 11.2 Replica Sets

- Roles: Primary (read/write), Secondary (read-only, replicates from primary),
  Arbiter (votes, no data).
- Heartbeat monitoring: each member sends heartbeats to the others; if a member
  misses heartbeats, it is considered down.
- Election: if the primary is down, the secondaries hold an election to choose a
  new primary.
- Read preference: clients can read from primary, secondary, or nearest.

### 11.3 Sharding (future extension, optional)

- Consistent hashing with virtual nodes for even data distribution.
- Key range sharding: automatic routing based on shard key values.
- Shard router: routes queries to the correct shard.

### Phase 11 — Sub-tasks

- [ ] Design the oplog on-disk layout (capped collection of operations with
  timestamps).
- [ ] Implement `src/replication/oplog.rs`: capped operations log + timestamp
  queries.
- [ ] Implement `src/replication/replica.rs`: replica set management (add/remove
  members, election, state transitions).
- [ ] Implement heartbeat monitor: track member health, detect failures.
- [ ] Integrate oplog with the storage engine (every write appends to the oplog).
- [ ] Add replication tests: replica set setup, failover, oplog replay, sync.

## Phase 12 — On-Disk Page Compression ⚪ Planned

WiredTiger compresses the pages of a collection (snappy or zstd). Kova pages store the BSON payload uncompressed inside the 16 KB envelope. This phase compresses that payload. The WAL stays the operation log: compression applies to the page file, which is where the documents live.

### 12.1 Codec

- Compress the payload before the page checksum is computed, and decompress after the checksum is verified.
- A flag in the existing page header records whether the payload is compressed, so a page written by Phase 7 still opens.
- The default codec is zstd. The choice lives in `kova.toml` and can be overridden by an environment variable.

### 12.2 What is compressed

- Document pages, overflow chunks, and the catalog page. Each page is compressed on its own.
- The buffer pool caches the decompressed page. A query does not see the compressed bytes.
- The WAL record stays uncompressed.

### 12.3 Tests

- A page of repetitive BSON occupies fewer bytes on disk than the same page written with compression off.
- A restart reads the compressed page back to the original documents.
- An uncompressed page from the current format still decodes.

### Phase 12 — Sub-tasks

- [ ] Add a compression flag to the page header and a zstd codec around the payload.
- [ ] Compress on flush and decompress on read in the page store.
- [ ] Keep Phase 7 pages readable when the flag is clear.
- [ ] Add `page_compression` to `kova.toml`, on by default for new pages.
- [ ] Tests: smaller file for repetitive documents, restart round-trip, legacy uncompressed page still opens.

## Architecture Principles

1. **Hybrid memory+disk**: documents live in pages. RAM holds the buffer
   pool. A miss reads disk; eviction writes a dirty page back.
2. **Durability first**: WAL before a page is dirtied, checkpoint flushes
   dirty pages and truncates the WAL, crash recovery replays the tail.
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

## Current Implementation Gaps

| Gap | Phase | Notes |
|-----|-------|-------|
| No transactions | 9 | MVCC + transaction manager needed. |
| Indexes are metadata only | 10 | B-Tree + Hash + auto-maintain needed. |
| No replication | 11 | Oplog + replica sets needed. |
| Pages are stored uncompressed | 12 | Compress the BSON payload in each page (zstd), the way WiredTiger compresses `.wt` pages. The WAL stays uncompressed. |

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
│     Query Engine (Phase 5)│ │   Aggregation Pipeline       │
│  DataFusion SQL parsing +  │ │  (Phase 8) $match,$group,  │
│  execution against storage │ │  $project,$sort,$limit...    │
└─────────────┬──────────────┘ └──────────────┬──────────────┘
              │                               │
┌─────────────▼───────────────────────────────▼──────────────┐
│                   Storage Layer (Phase 6+)                   │
│  ┌────────────────────────────────────────────────────────┐  │
│  │               Unified Storage Engine                    │  │
│  │  ┌──────────┐  ┌───────────┐  ┌─────────────────────┐  │  │
│  │  │   WAL    │  │Buffer Pool│  │   Disk Page Store   │  │  │
│  │  │(CRC32+LSN)│  │  (LRU)    │  │ (fixed-size pages)  │  │  │
│  │  └──────────┘  └───────────┘  └─────────────────────┘  │  │
│  └────────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────────┐  │
│  │                MVCC + Transaction Manager               │  │
│  │  (Phase 9: begin/commit/abort, isolation levels)       │  │
│  └────────────────────────────────────────────────────────┘  │
│  ┌────────────────────────────────────────────────────────┐  │
│  │                   Index Manager (Phase 10)               │  │
│  │  B-Tree │ Hash │ Unique │ Sparse │ Full-Text (Tantivy) │  │
│  └────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              │                               │
┌─────────────▼─────────────┐ ┌──────────────▼──────────────┐
│  Replication (Phase 11)    │ │   Sharding (optional)        │
│  Oplog + Replica Sets +   │ │  Consistent Hashing +        │
│  Heartbeat + Election     │ │  Key Range Sharding          │
└───────────────────────────┘ └──────────────────────────────┘
```

## Contact

Main entry point: `src/main.rs`.

---

*Phase 8 is complete: `aggregate` and the shared query language read
pages through the buffer pool. Next is transactional storage (MVCC +
ACID), the indexing subsystem, replication, and on-disk page compression
(Phase 12).*
