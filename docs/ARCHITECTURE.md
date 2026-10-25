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

- A shard router is opt-in. With `[sharding]` absent, one process still stores every document. Phase 11 remains one replica set inside a process.
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
│    Query Engine (Phase 5) │ │   Aggregation Pipeline       │
│   DataFusion SQL parsing + │ │   (Phase 8) $match,$group,  │
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
│  │  │       (Phase 9: begin/commit/abort, isolation)     │ │  │
│  │  └────────────────────────────────────────────────────┘ │  │
│  │                                                        │  │
│  │  ┌────────────────────────────────────────────────────┐ │  │
│  │  │                Index Manager (Phase 10)             │ │  │
│  │  │  B-Tree │ Hash │ Unique │ Sparse │ Full-Text       │ │  │
│  │  │  (Tantivy already integrated in Phase 3/4)       │ │  │
│  │  └────────────────────────────────────────────────────┘ │  │
│  └────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              │                               │
┌─────────────▼─────────────┐ ┌──────────────▼──────────────┐
│   Replication (Phase 11)   │ │   Sharding (optional)        │
│   Oplog + Replica Sets +   │ │   Consistent Hashing +       │
│   Heartbeat + Election     │ │   Key Range Sharding         │
└───────────────────────────┘ └──────────────────────────────┘
```

### 4.1 Network Layer (Phase 1)

- TCP listener on port 27018 (configurable via `KOVA_PORT`).
- Accepts connections and spawns a `tokio::spawn` task per client.
  `max_connections` (default 128) caps how many stay open. The socket
  past the cap is answered with an error and closed, and it is not
  counted. `max_connection_idle_ms` of `0` leaves a quiet connection
  open; any other value closes it. Shutdown and a disabled user close
  the matching connections and free the slots.
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

### 4.2 Query Engine (Phase 5)

- **SQL Parsing:** Apache DataFusion parses SQL text and generates
  logical/physical plans.
- **Plan Extraction:** The plan is returned as a human-readable description
  (for debugging) or executed against the storage layer.
- **Execution:** SQL queries are executed against the storage layer
  (in-memory + disk via the buffer pool). Results are returned as BSON
  via the MongoDB wire protocol.
- **Entry Point:** `db.runCommand({ sql: "SELECT ... FROM ... WHERE ..." })`.
- **ODBC:** `libkova.so` is the unixODBC driver. `SERVER`, `PORT`, and
  `DATABASE` open the MongoDB port. `UID` and `PWD` send `authenticate`
  on that connection before the SQL.

### 4.3 Storage Layer (Phase 6+)

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
- **Startup:** read the `admin` catalog (databases, collections, fields,
  indexes, users, views) and replay the WAL tail. The search index opens
  from `text/`. Secondary indexes open from `indexes.bin`. A directory
  written before that catalog is scanned once.

#### 4.3.5 MVCC + Transactions (Phase 9)

- **MVCC:** The page holds the latest committed document. An in-memory
  chain keeps older images, each stamped with the LSN that replaced it.
  A snapshot reads the image that was current at its LSN. Garbage
  collection drops an image once every open snapshot is past it.
  Restart drops the chains: no snapshot is still open.
- **Transactions:** `beginTransaction`, `commit`, and `abort`, routed to
  the partition that owns the named collection. Writes stay in the
  transaction until commit, which appends one WAL record and then
  dirties pages. Abort discards the writes.
- **Isolation:** Read Uncommitted, Read Committed, Repeatable Read,
  Snapshot, and Serializable. Write-write conflicts abort the later
  transaction. Serializable also aborts when a document it read was
  committed by someone else.
- **Consistency:** `collMod` can attach a validator (`required` fields
  and `bsonType`). Insert and update reject a document that fails it,
  before the WAL record is written.
- **Timeout:** `timeoutMs` aborts the transaction on its next statement.
  `0` expires it immediately.

### 4.4 Indexing Subsystem (Phase 10)

Document pages stay the database. Each secondary index is an in-memory
B-tree or hash. A checkpoint writes the entries to `indexes.bin`. Startup
opens that file when its LSN matches the catalog. A missing snapshot is
rebuilt by scanning pages. Entries point at the document id. The fetch
reads pages and keeps the ids the index named, because an update can move
a document between pages.

- **B-Tree:** an equality prefix and one range on the next field. Keys
  are ascending. `10` and `10.0` share a key.
- **Hash:** `{ field: "hashed" }`, exact match on that one field.
- **Unique:** checked on a copy before the WAL append. A collision
  returns an error containing `duplicate` and does not write.
- **Sparse:** documents missing an indexed field are omitted from that
  index.
- **Full-text:** Tantivy, maintained on insert, update, and delete.
  A `"text"` key in `createIndexes` is rejected.
- **Planner:** `_id` equality is `IDHACK`, a covered predicate is
  `IXSCAN`, and the rest is `COLLSCAN`. `explain` reports that stage.
  Reads inside an open transaction stay on the snapshot. A SQL `WHERE`
  of one comparison uses the same choice before DataFusion runs.

### 4.5 Replication (Phase 11)

OpenRaft runs inside the `kova` process. One Raft group per process. The
Raft log is the oplog (`data/raft-{id}/oplog.json`); its index is the
timestamp. Heartbeats are AppendEntries. A majority of voters elects the
primary.

The network task proposes an auto-commit write with `client_write`. On
commit, the state machine applies that command through the partition
router, on the leader and on every follower. The worker does not propose
the same write again. Reads, including reads on a secondary, stay local.

`hello` carries `setName`, `isWritablePrimary`, `secondary`, `hosts`,
`primary`, and `me` when replication is enabled. A node that is not the
leader rejects writes with code 10107. `nearest` is chosen by the driver
from `hosts`. Without `[replication]`, `hello` stays a standalone primary.

An arbiter is a learner: it stores the oplog, applies no documents, and
does not vote. `begin`, staged statements, and `abort` stay on the
primary. `commit` proposes one `applyTransaction` entry carrying the
writes. Followers apply that entry as one WAL record and do not open a
transaction. Sharding is a router in front of these processes, not a second copy inside the replica set. A snapshot of the page files
goes out every 64 log entries. A follower whose log no longer overlaps
installs that snapshot and then applies the later oplog entries. An
arbiter does not install document pages.

### 4.6 Sharding (Phase 23)

`[sharding] role = "router"` hashes `_id` onto a ring of 128 virtual nodes per shard and forwards the OP_MSG command to `shards[].client`. The router does not store those documents. A find, count, update, or delete of one `_id` goes to one shard. The same command without that `_id` visits every shard and the replies are merged. `create`, `drop`, and `createIndexes` visit every shard. SQL loads each named collection from the shards and runs on the router. A shard is a normal Kova process and may enable `[replication]` on its own port. The router does not join that set. With the section absent, the process stores every document itself.

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
     When `[replication]` is on, an auto-commit write is proposed to
     Raft first and applied only after it commits.
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
| `max_connections` | `KOVA_MAX_CONNECTIONS` | `128` | Simultaneous clients. One more is refused |
| `max_connection_idle_ms` | `KOVA_MAX_CONNECTION_IDLE_MS` | `0` | Idle close. `0` disables it |
| `log_level` | `KOVA_LOG_LEVEL` | `info` | Minimum emitted level |
| `warn_on_non_mongo_queries` | `KOVA_WARN_NON_MONGO_QUERIES` | `true` | WARN on non-Mongo operators |
| `search.analyzer` | — | `default` | `default` \| `en_stem` |
| `search.stopwords` | — | `[]` | Stop words removed at index/query time |
| `storage.data_dir` | `KOVA_DATA_DIR` | `data` | WAL, checkpoint, and page files |
| `storage.page_size` | `KOVA_PAGE_SIZE` | `16384` | Fixed page size in bytes |
| `storage.buffer_pool_pages` | `KOVA_BUFFER_POOL_PAGES` | `4096` | Buffer pool capacity in pages |
| `storage.wal_fsync` | `KOVA_WAL_FSYNC` | `true` | fsync each WAL append |
| `replication.enabled` | — | `false` | Replica set. False is a single writable node |
| `replication.id` | — | `1` | This process's Raft id |
| `replication.bootstrap` | — | `false` | Commit the voter set and campaign |
| `replication.members` | — | `[]` | `client` (MongoDB) and `raft` (RPC) addresses |
| `sharding.enabled` | — | `false` | Hash `_id` across the `shards` list |
| `sharding.role` | — | `shard` | `router` forwards. `shard` stores |
| `sharding.shards` | — | `[]` | `id` and `client` (MongoDB port) of each shard |

## 7. Phases

| Phase | Scope |
|-------|-------|
| 1 | TCP listener, header parsing, OP_MSG detection, concurrent tasks |
| 2 | Partition isolation, `PartitionCommand`, `crossbeam-channel` routing |
| 3 | Tantivy full-text indexing (per-partition, hot, `$text`) |
| 4 | Per-field boosts, wildcards, decay, analyzer config, `kova.toml` |
| 5 | DataFusion SQL parsing, `db.runCommand({ sql: "..." })`, plan extraction |
| 6 | Disk storage engine: WAL, buffer pool, page store, hybrid memory+disk |
| 7 | Disk-resident collections: pages are the database, RAM is the buffer pool |
| 8 | Query language and aggregation pipeline, reading pages through the buffer pool |
| 9 | Transactional storage: MVCC, ACID, multiple isolation levels |
| 10 | Indexing subsystem: B-Tree, Hash, Unique, Sparse, auto-maintain |
| 11 | Replication & HA: OpenRaft oplog, election, heartbeats |
| 12 | On-disk page compression: zstd on the BSON payload; WAL stays uncompressed |
| 13 | Security: RBAC, Argon2id, AES-256-GCM at rest, user management |
| 14 | Monitoring: 17 atomic counters, `serverStatus` |
| 15 | Connection pool: cap, tracking, idle close |
| 16 | Lucene-like views and facet queries |
| 17 | Map projection: `mapReduce` returns `{ key, value }`. No `out` collection |
| 18 | Transactions in the oplog: a commit is one `applyTransaction` entry |
| 19 | Snapshot shipping: a lagged follower installs pages and replays the oplog |
| 20 | ODBC connector for the existing DataFusion SQL path |
| 21 | ODBC catalog for GUI clients: `SQLTables`, `SQLColumns`, `SQLGetInfo` |
| 22 | `kovadump`: backup, and restore from Kova, PostgreSQL, MongoDB, and MySQL dumps |
| 23 | Sharding: consistent hash of `_id`, router forwards to shard processes |
| 24 | Physical catalog: database `admin` holds databases, collections, fields, and indexes. Startup reads those rows |

---

*Generated from the brainstorming session based on `draft.md` and inspired
by GraniteDB (https://github.com/jmptrader/GraniteDB) for the disk storage
engine, WAL, buffer pool, MVCC, and replication design.*
