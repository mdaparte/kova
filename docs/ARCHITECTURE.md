# Kova Architecture Specification

## 1. Intent and Why

Kova is an ultra-high-performance, **hybrid memory+disk** database engine
in Rust. The goal is to unify multiple paradigms that traditionally live in
separate infrastructure stacks:

- **Transactional determinism** (NewSQL in-memory systems, VoltDB)
- **Document flexibility** (NoSQL, MongoDB native protocol)
- **Analytical search** (full-text relevance, Tantivy/Lucene)
- **SQL query optimization** (Apache DataFusion)
- **Durable disk storage** with Write-Ahead Logging, buffer pool, page-based
  storage (inspired by GraniteDB)
- **Multi-model indexing**: B-Tree (range), Hash (O(1)), Unique, Sparse, and
  Full-Text (Tantivy)
- **High availability** via oplog-based replication, replica sets, heartbeat

This single-engine approach avoids the synchronization cost between a
transactional DB, a search cluster, and a separate analytical engine,
making Kova suitable for edge IoT devices and as a showcase of low-level
Rust concurrency, memory management, and storage engine design.

## 2. Goals

- Accept native MongoDB clients on port 27018 without driver changes.
- Keep the working set in RAM for predictable, microsecond-latency
  transaction processing (in-memory hash maps + buffer pool cache).
- Persist data durably to disk via WAL with CRC32 checksums and LSN
  tracking (crash recovery on startup).
- Provide full-text search and relevance scoring over indexed fields
  (Tantivy).
- Parse, optimize, and execute SQL queries with DataFusion (return results
  as BSON via the MongoDB wire protocol).
- Support multi-model indexing: B-Tree (range), Hash (O(1)), Unique,
  Sparse, and Full-Text (Tantivy), all auto-maintained on write.
- Provide high availability via oplog-based replication and replica sets
  with heartbeat monitoring.
- Avoid heavy locking and allocator pauses via thread-per-core partitioning.

## 3. Non-Goals (Current Phase)

- Sharding across multiple nodes is out of scope until Phase 8 (optional).
- Full MongoDB command-level compatibility (only the wire protocol minimum
  required to speak with official drivers).
- Documents stay on pages. A query reads through the buffer pool. Startup
  opens those pages and replays the WAL tail. It does not load the
  collection into a map.

## 4. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Client / Driver                          │
│         (MongoDB Wire Protocol, db.runCommand({sql}))        │
└─────────────────────────────┬───────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────┐
│                   Network Layer (Phase 1)                    │
│   TCP listener, OP_MSG/OP_QUERY, handshake, SQL command      │
│   routing to query_engine or storage/partition workers        │
└─────────────────────────────┬───────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              │                               │
┌─────────────▼─────────────┐ ┌──────────────▼──────────────┐
│    Query Engine (Phase 4) │ │   Aggregation Pipeline       │
│   DataFusion SQL parsing + │ │   (Phase 5.5) $match,$group,  │
│   execution against storage│ │   $project,$sort,$limit...    │
└─────────────┬──────────────┘ └──────────────┬──────────────┘
              │                               │
┌─────────────▼───────────────────────────────▼──────────────┐
│                     Storage Layer                           │
│  ┌────────────────────────────────────────────────────────┐  │
│  │              Unified Storage Engine (engine.rs)         │  │
│  │                                                        │  │
│  │  ┌──────────┐  ┌───────────┐  ┌─────────────────────┐  │  │
│  │  │   WAL    │  │Buffer Pool│  │   Disk Page Store   │  │  │
│  │  │(CRC32+LSN)│  │  (LRU)    │  │ (fixed-size pages)  │  │  │
│  │  └──────────┘  └───────────┘  └─────────────────────┘  │  │
│  │                                                        │  │
│  │  ┌────────────────────────────────────────────────────┐ │  │
│  │  │              MVCC + Transaction Manager            │ │  │
│  │  │       (Phase 6: begin/commit/abort, isolation)     │ │  │
│  │  └────────────────────────────────────────────────────┘ │  │
│  │                                                        │  │
│  │  ┌────────────────────────────────────────────────────┐ │  │
│  │  │                Index Manager (Phase 7)             │ │  │
│  │  │  B-Tree │ Hash │ Unique │ Sparse │ Full-Text       │ │  │
│  │  │  (Tantivy already integrated in Phase 3/3.5)       │ │  │
│  │  └────────────────────────────────────────────────────┘ │  │
│  └────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              │                               │
┌─────────────▼─────────────┐ ┌──────────────▼──────────────┐
│   Replication (Phase 8)   │ │   Sharding (optional)        │
│   Oplog + Replica Sets +   │ │   Consistent Hashing +       │
│   Heartbeat + Election     │ │   Key Range Sharding         │
└───────────────────────────┘ └──────────────────────────────┘
```

### 4.1 Network Layer (Phase 1)

- TCP listener on port 27018 (configurable via `KOVA_PORT`).
- Accepts connections and spawns a `tokio::spawn` task per client.
- Reads 16-byte Wire Protocol header (little-endian): `messageLength`,
  `requestId`, `responseTo`, `opCode`.
- Handles `OP_MSG (2013)` and legacy `OP_QUERY (2004)`; `OP_QUERY` is
  answered with `OP_REPLY (1)`.
- Validates `messageLength` (16 B … 48 MB) before allocation.
- Routes commands to the appropriate handler:
  - Handshake commands (`ping`, `hello`, `isMaster`, `buildInfo`,
    `getParameter`, `endSessions`, `whatsmyuri`, `connectionStatus`,
    `hostInfo`, `atlasVersion`) are answered directly.
  - SQL commands (`db.runCommand({ sql: "..." })`) are routed to the
    query engine (`src/query_engine/mod.rs`).
  - Data commands (`insert`, `find`, `update`, `delete`, `count`, etc.)
    are routed to the storage layer via `PartitionCommand`.

### 4.2 Query Engine (Phase 4)

- **SQL Parsing:** Apache DataFusion parses SQL text and generates
  logical/physical plans.
- **Plan Extraction:** The plan is returned as a human-readable description
  (for debugging) or executed against the storage layer.
- **Execution:** SQL queries are executed against the storage layer
  (in-memory + disk via the buffer pool). Results are returned as BSON
  via the MongoDB wire protocol.
- **Entry Point:** `db.runCommand({ sql: "SELECT ... FROM ... WHERE ..." })`.

### 4.3 Storage Layer (Phase 5+)

#### 4.3.1 WAL (Write-Ahead Log)

- Segmented WAL with CRC32 checksums per entry.
- LSN (Log Sequence Number) tracking for crash recovery.
- Automatic segment rolling when a segment reaches its size limit.
- `fsync` on commit (configurable: `wal_fsync = true/false` in `kova.toml`).
- Recovery on startup: replay WAL entries since the last checkpoint.

#### 4.3.2 Buffer Pool

- LRU-based page cache with pin/unpin eviction.
- Configurable capacity (default: 4096 pages, via `buffer_pool_pages`).
- Performance counters: hits, misses, evictions, pin count.
- Dirty page writeback: flush dirty pages to disk on checkpoint or eviction.

#### 4.3.3 Disk Page Store

- Fixed-size pages (default 16 KB, configurable via `page_size`).
- Pages stored on disk in a structured directory layout.
- Page integrity verification (checksum/header) on read.
- Overflow support for large documents (document spans multiple pages).
- Page manager handles allocation, deallocation, and reuse of pages.

#### 4.3.4 Hybrid Memory + Disk Architecture

- **Hot data in memory:** the buffer pool. A miss reads the page from disk.
  Finds, updates, and SQL walk pages; they do not keep the collection.
- **Durability on disk:** mutations go to the WAL before a page is dirtied.
  A checkpoint flushes dirty pages, records the LSN, and truncates that WAL.
- **Startup:** discover the page files, overlay the catalog, and replay the
  WAL tail onto the pages. The search index is rebuilt by scanning pages.

#### 4.3.5 MVCC + Transactions (Phase 6)

- **MVCC:** Each document version gets a timestamp/LSN. Snapshot reads see
  the database as of a point in time. Garbage collection of old versions.
- **Transactions:** `beginTransaction`, `commit`, `abort` commands.
  Atomicity via WAL + MVCC. Consistency via optional schema validation.
  Isolation levels: Read Uncommitted, Read Committed, Repeatable Read,
  Snapshot, Serializable.
- **Conflict Detection:** Write-write conflicts abort the later transaction.
- **Timeout Enforcement:** Automatic transaction abort on timeout.

### 4.4 Indexing Subsystem (Phase 7)

- **B-Tree Indexes:** Ordered indexes for range queries + composite key support.
- **Hash Indexes:** O(1) exact-match lookups.
- **Unique Constraints:** Enforce uniqueness across indexed fields.
- **Sparse Indexes:** Skip documents missing the indexed field.
- **Full-Text Indexes (Tantivy):** Already integrated (Phase 3/3.5). The
  index manager creates, drops, and auto-maintains full-text indexes
  alongside other index types.
- **Index Manager:** Creates, drops, and auto-maintains indexes. Indexes are
  updated on insert/update/delete. Index metadata stored in disk storage.
- **Query Planner Integration:** The query planner (Phase 4 + DataFusion)
  consults the index manager to choose the best strategy: ID lookup → index
  scan → collection scan. `explain` plans show the chosen strategy.

### 4.5 Replication (Phase 8)

- **Oplog (Capped Operations Log):** Every write is recorded in a capped
  operations log with timestamps. Secondaries replay the oplog to stay in sync.
- **Replica Sets:** Primary/Secondary/Arbiter roles with heartbeat monitoring
  and automatic election.
- **Read Preference:** Clients can read from primary, secondary, or nearest.
- **Sharding (optional):** Consistent hashing with virtual nodes + key range
  sharding (future extension).

## 5. Data Flow

1. A client opens a TCP connection on port 27018.
2. The network task spawns a `tokio::spawn` task.
3. The network task reads exactly 16 bytes: `messageLength`, `requestId`,
   `responseTo`, `opCode`.
4. If `opCode == OP_MSG (2013)`, the payload is parsed as BSON.
5. The parsed command is dispatched:
   - Handshake commands → answered directly.
   - SQL commands → routed to the query engine (`src/query_engine/mod.rs`).
   - Data commands → routed to the storage layer via `PartitionCommand`.
6. The storage layer handles the command (insert, find, update, delete,
   search, SQL execution) inside its isolated memory region.
7. Results flow back through an `mpsc` channel and are serialized as a
   MongoDB response (OP_REPLY / OP_MSG) to the client.

## 6. Configuration

`kova.toml` (working directory, or the path in `KOVA_CONFIG`) holds the
engine configuration. Precedence: **environment variable > file > built-in
default**. A malformed file aborts startup; a malformed env value is warned
about and ignored.

| Key | Env override | Default | Meaning |
|---|---|---|---|
| `port` | `KOVA_PORT` | `27018` | TCP listener port |
| `log_level` | `KOVA_LOG_LEVEL` | `info` | Minimum emitted level |
| `warn_on_non_mongo_queries` | `KOVA_WARN_NON_MONGO_QUERIES` | `true` | WARN on non-Mongo operators |
| `search.analyzer` | — | `default` | `default` \| `en_stem` |
| `search.stopwords` | — | `[]` | Stop words removed at index/query time |
| `storage.data_dir` | `KOVA_DATA_DIR` | `data` | WAL, checkpoint, and page files |
| `storage.page_size` | `KOVA_PAGE_SIZE` | `16384` | Fixed page size in bytes |
| `storage.buffer_pool_pages` | `KOVA_BUFFER_POOL_PAGES` | `4096` | Buffer pool capacity in pages |
| `storage.wal_fsync` | `KOVA_WAL_FSYNC` | `true` | fsync each WAL append |

## 7. Phases

| Phase | Scope |
|-------|-------|
| 1 | TCP listener, header parsing, OP_MSG detection, concurrent tasks |
| 2 | Partition isolation, `PartitionCommand`, `crossbeam-channel` routing |
| 3 | Tantivy full-text indexing (per-partition, hot, `$text`) |
| 3.5 | Per-field boosts, wildcards, decay, analyzer config, `kova.toml` |
| 4 | DataFusion SQL parsing, `db.runCommand({ sql: "..." })`, plan extraction |
| 5 | Disk storage engine: WAL, buffer pool, page store, hybrid memory+disk |
| 5b | Disk-resident collections: pages are the database, RAM is the buffer pool |
| 5.5 | Aggregation pipeline, reading pages through the buffer pool |
| 6 | Transactional storage: MVCC, ACID, multiple isolation levels |
| 7 | Indexing subsystem: B-Tree, Hash, Unique, Sparse, auto-maintain |
| 8 | Replication & HA: Oplog, replica sets, heartbeat, election |

---

*Generated from the brainstorming session based on `draft.md` and inspired
by GraniteDB (https://github.com/jmptrader/GraniteDB) for the disk storage
engine, WAL, buffer pool, MVCC, and replication design.*
