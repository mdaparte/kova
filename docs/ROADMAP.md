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
| 9 | Transactional Storage (MVCC + ACID + Isolation) | ✅ Complete |
| 10 | Indexing Subsystem (B-Tree, Hash, Unique, Sparse, Auto-Maintain) | ✅ Complete |
| 11 | Replication & High Availability (Oplog + Replica Sets + Heartbeat) | ✅ Complete |
| 12 | On-Disk Page Compression (zstd, WiredTiger-style) | ✅ Complete |
| 13 | Security (RBAC, Argon2id, AES-256-GCM, users) | ✅ Complete |
| 14 | Monitoring (17 atomic counters, `serverStatus`) | ✅ Complete |
| 15 | Connection Pool (cap, tracking, lifecycle) | ✅ Complete |
| 16 | Views (`viewOn` + pipeline, optional disk Tantivy) | ✅ Complete |
| 17 | Map projection (`mapReduce`, no `out`) | ✅ Complete |
| 18 | Transactions in the oplog | ✅ Complete |
| 19 | Snapshot shipping | ✅ Complete |
| 20 | ODBC connector | ✅ Complete |
| 21 | ODBC catalog for GUI clients | ✅ Complete |
| 22 | `kovadump` backup and restore | ✅ Complete |
| 23 | Sharding | ✅ Complete |
| 24 | Physical catalog (`admin`) | ✅ Complete |
| 25 | Autoincrement | ✅ Complete |
| 26 | Feed (Apache Iggy) | ⚪ Planned |
| 27 | User guide | ⚪ Planned |

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

`explain` on a find or an aggregate reports the filter or the pipeline.
The stage is `IDHACK`, `IXSCAN`, or `COLLSCAN`, chosen by the worker that
owns the collection's indexes.

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

## Phase 9 — Transactional Storage (MVCC + ACID + Isolation) ✅

_Inspiration: GraniteDB (MVCC, ACID transactions, multiple isolation levels)_

The page keeps the latest committed document. `src/storage/mvcc.rs` keeps
the older images an open snapshot still needs, and drops them when the
oldest snapshot moves past. A commit is one WAL record, so a crash
publishes every write in the transaction or none of them. Uncommitted
writes never reach a page.

`beginTransaction` names the collection whose partition owns the
transaction. Later `insert`, `find`, `update`, `delete`, `count`, and
`aggregate` commands pass `txnId`. A collection that hashes to another
partition does not see that transaction. `commit` and `abort` use the
same collection. `collMod` stores a small validator (`required`,
`properties.bsonType`) in the catalog.

`timeoutMs: 0` expires the transaction on its next statement. Serializable
aborts when a document that transaction read was committed by someone
else. A phantom insert into a range the transaction scanned is left for
Phase 10, when indexes can lock a range. `$out` is rejected inside a
transaction. `$lookup` of a collection on this partition uses the
transaction's snapshot; a collection on another partition is that
partition's committed pages.

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

- [x] Design MVCC version storage: per-document version chain (LSN, document,
  deleted flag).
- [x] Implement `src/storage/mvcc.rs`: version management + snapshot reads.
- [x] Implement `src/storage/transaction.rs`: begin/commit/abort + conflict
  detection + timeout.
- [x] Integrate MVCC with the buffer pool and WAL.
- [x] Add transaction tests: ACID properties, isolation levels, conflict
  detection, timeout.

## Phase 10 — Indexing Subsystem (B-Tree, Hash, Unique, Sparse, Auto-Maintain) ✅

_Inspiration: GraniteDB (B-Tree, Hash, Unique, Sparse, Index Manager)_

Document pages stay the database. An index is an in-memory B-tree or hash
rebuilt by scanning those pages through the buffer pool at startup, the
same way Tantivy is rebuilt. The catalog page remembers which indexes
exist. Index entries store the document id, not a page id: an update can
move a document to another page.

### 10.1 Index Types

- **B-Tree indexes**: equality prefix plus one range (`$gt`, `$gte`, `$lt`,
  `$lte`) on the next field. Keys are stored ascending. A declared direction
  of `-1` does not change which documents match.
- **Hash indexes**: `{ field: "hashed" }`, one field, exact match only.
- **Unique**: a second id on the same key is rejected before the WAL append.
  The error message contains `duplicate`.
- **Sparse**: a document missing any indexed field is left out of that index.
  A non-sparse index stores a missing field as null.
- **Full-text**: stays on Tantivy. `createIndexes` with `"text"` is rejected.
  Tantivy is still updated on every insert, update, and delete.

Numbers compare as ordered `f64` bits, so `10` and `10.0` share a key.
Array fields are not indexed element by element.

### 10.2 Index Manager

- `createIndexes` records the spec and builds the structure from the pages.
  A unique violation restores the previous spec and writes nothing to the WAL.
- `dropIndexes` and `deleteIndexes` drop one secondary index. `"*"` drops
  every secondary index. `_id_` cannot be dropped. The drop is a WAL record.
- Insert, update, and delete maintain the index after the page write.
  A unique check runs on a copy first, so a rejection does not leave a
  half-updated index or a WAL record.

### 10.3 Query Planner

The worker chooses the path in this order:

1. Equality on `_id` alone is `IDHACK`.
2. A covered secondary predicate is `IXSCAN`.
3. Anything else is `COLLSCAN`.

After an index returns ids, the fetch still reads pages and keeps those
ids, then applies the rest of the filter. An empty id list is an empty
answer. Find, count, and a leading aggregate `$match` use this path.
A read inside an open transaction stays a snapshot scan.

`explain` asks the worker and reports `IDHACK`, `IXSCAN`, or `COLLSCAN`,
plus `indexName` when an index was chosen. A direct explain with no
indexes still says `COLLSCAN`.

A SQL `WHERE` that is one comparison (`sal > 1000`, `job = 'CLERK'`) is
sent as that find, so the index narrows the pages before DataFusion
applies the predicate again. The SQL reply includes the winning stage.

### Phase 10 — Sub-tasks

- [x] Indexes are rebuilt from document pages. The catalog stores the spec.
  There is no second on-disk B-tree: a page pointer would go stale when a
  document moves.
- [x] Implement `src/index/btree.rs`: B-Tree index with range + composite keys.
- [x] Implement `src/index/hash_index.rs`: Hash index for exact-match lookups.
- [x] Implement `src/index/manager.rs`: create/drop/auto-maintain indexes.
- [x] Integrate the index manager with the storage engine. The fetch reads
  pages through the buffer pool and keeps the ids the index named.
- [x] A single-comparison SQL `WHERE` uses the same planner. DataFusion still
  applies the predicate.
- [x] Add index tests: range, hash, id lookup, unique, sparse, update,
  drop, and restart.

## Phase 11 — Replication & High Availability (Oplog + Replica Sets + Heartbeat) ✅

Election, heartbeats, and the oplog are OpenRaft 0.9 inside the `kova` process. There is no ZooKeeper and no second election protocol. With no `[replication]` section the process stays a single writable node.

### 11.1 Oplog

- The Raft log is the oplog. It is rewritten to `data/raft-{id}/oplog.json` before OpenRaft is told the append is durable. The log index is the timestamp.
- A committed auto-commit command is applied on the leader and on every follower through the partition workers. The network task proposes the command; the worker does not propose it again.
- `find` on `local.oplog.rs` returns entries after `ts` (`$gt` / `$gte`). `ts` is the log index.
- Inserts without `_id` receive one before the command is logged, so every replica stores the same id.
- `beginTransaction`, each staged statement, and `abort` stay on the primary. `commit` proposes one oplog entry, `applyTransaction`, that carries the writes. Followers apply that entry as one WAL record and do not open a transaction.
- Snapshot shipping is on. After 64 log entries the primary builds a snapshot of the page files. A follower whose log no longer overlaps installs that snapshot and applies only the later oplog entries. An arbiter stores the oplog and does not install document pages.

### 11.2 Replica Sets

- `[replication]` in `kova.toml` lists the members. `client` is the MongoDB port. `raft` is a second TCP port, one request per connection, length-prefixed JSON.
- The bootstrap process commits the voter set. The others wait for that log. The same membership on more than one bootstrap is safe; two different memberships split the set.
- Heartbeats and elections are Raft's. A majority of voting members elects the primary. A node that is not the leader rejects writes with `NotWritablePrimary` (code 10107). Secondaries serve reads.
- `hello` reports `setName`, `isWritablePrimary`, `secondary`, `hosts`, `primary`, and `me`. Read preference `nearest` is the driver's choice among `hosts`; this process does not proxy the read.
- An `arbiter` is a Raft learner. It keeps the oplog, stores no documents, and does not vote, so it cannot become primary. OpenRaft will elect any voter, and a data-less primary cannot serve the database.
- Sharding (consistent hashing, key ranges, a shard router) is not part of this phase.

### Phase 11 — Sub-tasks

- [x] Oplog on disk: the Raft log in `data/raft-{id}/oplog.json`, timestamped by log index.
- [x] `src/replication/oplog.rs`: durable Raft log and "after this timestamp" reads.
- [x] `src/replication/replica.rs`: membership, election, and heartbeats via OpenRaft.
- [x] Heartbeats are Raft AppendEntries. A missed election timeout starts a campaign.
- [x] Auto-commit writes are proposed from the network task and applied through the partition workers.
- [x] Tests: three voters replicate a write and elect a new leader; an arbiter keeps the oplog and stores no documents.
- A commit is one oplog entry (`applyTransaction`). `begin`, staged statements, and `abort` stay on the primary.
- Sharding remains a later extension.

## Phase 12 — On-Disk Page Compression ✅

WiredTiger compresses the pages of a collection (snappy or zstd). Kova compresses the BSON payload of every page with zstd. The WAL stays the operation log: compression applies to the page file, which is where the documents live.

### 12.1 Codec

- `Page::encode` compresses the payload, then computes the checksum over the header and the zstd frame. `Page::decode` verifies the checksum, then decompresses.
- The header stays 32 bytes. The length field is the compressed size. There is no compression flag: every page is a zstd frame.
- The on-disk file is the header plus that frame. It is not padded out to `page_size`, and it is never longer than `page_size`.
- The logical payload capacity is the largest size whose worst-case zstd frame (`compress_bound`) still fits in the envelope, so a full page of incompressible bytes still encodes.

### 12.2 What is compressed

- Document pages, overflow chunks, and the catalog page. Each page is compressed on its own.
- The buffer pool caches the decompressed page. A query does not see the compressed bytes.
- The WAL record stays uncompressed.

### 12.3 Tests

- A page of repetitive BSON occupies fewer bytes on disk than the raw payload.
- A page filled to the logical capacity still encodes inside `page_size`.
- A restart reads the compressed page back to the original documents.

### Phase 12 — Sub-tasks

- [x] Compress the payload with zstd in `Page::encode` and decompress it in `Page::decode`.
- [x] Size the logical payload so `compress_bound` fits in the fixed envelope.
- [x] Keep the buffer pool on the decompressed page. Leave the WAL uncompressed.
- [x] Tests: smaller file for repetitive documents, full-page encode, restart round-trip.

## Phase 13 — Security (RBAC, Argon2id, AES-256-GCM, users) ✅

With `[security] enabled = false` the process stays the open server it is today. Turning it on requires a user before any other command, checks that user's roles, and encrypts page files and WAL records.

### 13.1 Roles

Five built-in roles, plus custom roles. A custom role is a name and a subset of these eleven actions:

| Action | Commands it allows |
|--------|--------------------|
| `find` | find, count, aggregate, search, `sql` |
| `insert` | insert |
| `update` | update |
| `remove` | delete |
| `createCollection` | create |
| `dropCollection` | drop, dropDatabase |
| `createIndex` | createIndexes |
| `dropIndex` | dropIndexes, deleteIndexes |
| `viewStatus` | collStats, dbStats, listCollections, listIndexes, listDatabases, serverStatus |
| `createUser` | createUser, updateUser |
| `grantRole` | createRole, dropRole, grantRolesToUser, revokeRolesFromUser |

- `read` is `find`.
- `readWrite` adds `insert`, `update`, and `remove`.
- `dbAdmin` adds the collection, index, and `viewStatus` actions.
- `userAdmin` is `createUser` and `grantRole`.
- `root` is all eleven.
- A custom role cannot name an action outside this list. The command fails and names the rejected action.

### 13.2 Passwords

- `createUser` stores an Argon2id hash. The plaintext password is not written to the catalog or the WAL.
- Parameters: 19 MiB of memory, 2 iterations, parallelism 1.
- `authenticate` checks that hash and binds the connection to the user. A later command on that connection is checked against the user's roles.
- A disabled user fails `authenticate`. An unknown user fails the same way.

### 13.3 Users

Users live in the catalog, not in a second database of documents.

- `createUser` with a name, a password, and one or more roles.
- `updateUser` enables or disables the account. Disabling closes that user's open connections.
- `grantRolesToUser` and `revokeRolesFromUser` change the role list. `root` can be granted. The last `root` cannot be revoked or disabled.
- `createRole` and `dropRole` manage custom roles. A built-in role cannot be dropped.

### 13.4 Encryption at rest

- When `encryption_key` is set (32 bytes, or `KOVA_ENCRYPTION_KEY`), each page file and each WAL record is AES-256-GCM. The buffer pool and the query path still see plaintext.
- Compress with zstd first, then encrypt. Each write uses a fresh 96-bit nonce. The nonce and the tag sit in the page image next to the ciphertext. The checksum covers that image.
- Startup with encrypted files and no key fails. A data directory does not mix encrypted and plaintext pages.

### Phase 13 — Sub-tasks

- [x] Catalog records for users, built-in roles, and custom roles.
- [x] Argon2id on `createUser`. `authenticate` binds the connection.
- [x] Reject a command whose actions the connection's roles do not include.
- [x] AES-256-GCM around the compressed page and the WAL record, with a random nonce per write.
- [x] Tests: each built-in role, a custom role, a disabled user, a bad password, and a restart that reads an encrypted page only with the key.

## Phase 14 — Monitoring (17 atomic counters, `serverStatus`) ✅

Counters are atomics. A network task and a partition worker bump them without taking a lock on the command path. `serverStatus` returns the current values on the wire.

| Counter | What it counts |
|---------|----------------|
| `queries` | find, count, aggregate, search, and `sql` |
| `inserts` | insert commands |
| `updates` | update commands |
| `deletes` | delete commands |
| `connectionsActive` | sockets open now |
| `connectionsTotal` | sockets accepted since start |
| `bytesIn` | bytes read from clients |
| `bytesOut` | bytes written to clients |
| `walWrites` | WAL records appended |
| `bufferPoolHits` | page fetches served from the pool |
| `bufferPoolMisses` | page fetches read from disk |
| `bufferPoolEvictions` | pages dropped from the pool |
| `indexLookups` | `IDHACK` and `IXSCAN` plans |
| `collectionScans` | `COLLSCAN` plans |
| `transactionsStarted` | `beginTransaction` |
| `transactionsCommitted` | `commitTransaction` |
| `transactionsAborted` | `abortTransaction`, including timeout |

### Phase 14 — Sub-tasks

- [x] One atomic per counter, updated where the command or the pool already runs.
- [x] `serverStatus` returns the seventeen values and the uptime.
- [x] A test drives one insert, one indexed find, and one scan, then reads the counters back through `serverStatus`.

## Phase 15 — Connection Pool ✅

The listener caps how many clients can be connected. This is the server side of that cap, not a pool inside a driver.

- `max_connections` in `kova.toml`, overridable by `KOVA_MAX_CONNECTIONS`. Default 128.
- Past the cap the listener accepts the socket, answers with an error, and closes it. `connectionsActive` does not count that socket.
- Each accepted connection records peer address, start time, and the authenticated user once Phase 13 is on.
- `max_connection_idle_ms` closes a connection that has not sent a command in that long. `0` disables the idle close.
- Shutdown and a disabled user close the matching connections and free the slots.

### Phase 15 — Sub-tasks

- [x] Enforce `max_connections` in the accept loop.
- [x] Track peer, start, idle deadline, and user on each live connection.
- [x] Close idle connections and connections of a disabled user.
- [x] Test: the connection past the cap is refused, and a closed connection frees a slot.

## Phase 16 — Views and optional disk inverted index ✅

A view is a MongoDB view: a name, a source collection, and a pipeline. It is not a copy of the documents. `find`, `count`, `aggregate`, and `$text` on the view run that pipeline against the source. `listCollections` reports `type: "view"`. `drop` removes the view. Dropping the source is refused while a view still reads it.

Engine options live in the `kova` document on `create`. Keys added later stay in that document. The one key this phase reads is `inverted`.

- `inverted: true` builds a Tantivy index on disk at `data/partition-N/search/<view>/`, in segments. The index survives a restart. Inserts, updates, and deletes on the source keep it up to date.
- Field types are discovered from the documents that pass the pipeline. A string is text, an int is `i64`, a double is `f64`, a bool is bool, a date is a date. The first type seen for a field stays. A new field extends the schema and the index is rewritten.
- An inverted view accepts only `$match` and `$project`, and the project must keep `_id`. A `$group` view is still a view. `inverted: true` on it fails and names the stage.
- The existing `$text` index stays in RAM. `inverted` does not replace it.

```javascript
db.runCommand({
  create: "published_search",
  viewOn: "articles",
  pipeline: [{ $match: { published: true } }],
  kova: { inverted: true }
})
```

### Phase 16 — Sub-tasks

- [x] Persist the view (`name`, `viewOn`, `pipeline`, `kova`) in the catalog.
- [x] Route `find`, `count`, `aggregate`, and `$text` on a view to the source collection.
- [x] When `kova.inverted` is true, keep a Tantivy index on disk and discover field types.
- [x] Test: a `$match` view, a rejected `$group` inverted view, discovered types, and a `$text` hit after restart.

## Phase 17 — Map projection (`mapReduce`) ✅

`mapReduce` reads one collection and returns one `{ key, value }` document per match. It does not write a collection. It does not fan out across the replica set: every voter already stores every document, so splitting the scan would add network cost without reducing the work. The read stays on the node that received it, the same way `find` does.

- `mapReduce` names the collection. `query` is the find filter. Absent means every document.
- `map` is a document with `key` and `value`. A string that starts with `$` reads that field, including a dotted path. Any other value is a literal. A missing field is null.
- The reply is a cursor (`firstBatch`), the same shape as `find`.
- A JavaScript `map` string is rejected. The error names it.
- `reduce` and `out` are rejected. The error names the field. Nothing is written.
- A command carrying `txnId` is rejected.

### Phase 17 — Sub-tasks

- [x] Parse `mapReduce` into a collection, a query, and a map projection.
- [x] Return one `{ key, value }` per matching document. Write no collection.
- [x] Reject a JavaScript `map`, `reduce`, `out`, and `txnId`, naming what was refused.
- [x] Test: a query keeps the matches, the pairs are the projection, and `listCollections` still shows only the source.

## Phase 18 — Transactions in the oplog ✅

A commit's writes used to live only in the primary's worker. `beginTransaction`, staged statements, and `abort` are still refused as oplog entries. This phase logs the commit, not the open transaction.

- `beginTransaction` and each statement that only stages writes stay on the primary. They are not oplog entries.
- `commit` proposes one oplog entry, `applyTransaction`, that carries the writes that transaction made. Followers apply that entry through the partition workers as one WAL record. They do not open a transaction.
- `abort` writes nothing to the oplog. Uncommitted images never reach a follower.
- The entry is one Raft record, matching the one WAL record a commit already is, so a crash publishes every write in the transaction or none of them.
- A transaction still names one collection, and that collection's partition owns it. A collection on another partition stays outside the transaction.
- An arbiter stores the entry and applies no documents.
- `$out` inside a transaction stays rejected. `mapReduce` inside a transaction stays rejected (Phase 17).
- With no `[replication]` section the commit stays the local WAL record it is today.

### Phase 18 — Sub-tasks

- [x] Propose the committed writes as one oplog entry from `commit`.
- [x] Apply that entry on every follower without opening a transaction.
- [x] Leave `beginTransaction`, staged statements, and `abort` off the oplog.
- [x] Test: three voters commit a transaction on the primary, and a secondary `find` returns those documents. An abort leaves the secondary unchanged.

## Phase 19 — Snapshot shipping ✅

A follower that falls behind the retained oplog installs a snapshot of the pages, then applies the oplog from the index that snapshot covers. The primary builds that snapshot every 64 log entries, and when a snapshot is requested. With no `[replication]` section the process does not build or ship one.

- The snapshot is the page files, the catalog, and the view routes, at one applied oplog index. The WAL is not in it.
- A follower with an empty data directory installs that snapshot, then applies only the oplog entries after it.
- An arbiter stores the oplog and does not install document pages.

### Phase 19 — Sub-tasks

- [x] Build a snapshot of the pages on the primary and record the oplog index it covers.
- [x] Install that snapshot on a follower and replay the oplog after that index.
- [x] Leave an arbiter without document pages.
- [x] Test: a follower with an empty data directory catches up and a `find` returns the primary's documents.

## Phase 20 — ODBC connector ✅

SQL already runs inside the server: `db.runCommand({ sql: "..." })` on the MongoDB wire, executed by DataFusion. This phase adds a unixODBC driver on Linux, `libkova.so`, built with the server. The driver sends that same SQL string. The Windows driver manager stays out until it is named.

The connection string is `DRIVER=Kova;SERVER=host;PORT=27018;DATABASE=name`. `SERVER` is the host, including a machine on the network. `PORT` defaults to 27018. `DATABASE` is the `$db` of the command. `UID` and `PWD`, or the user and password of `SQLConnect`, send `{ authenticate, user, pwd }` on that socket before any SQL. A connection with neither value stays anonymous, which matches security left off. A connection with only one of the two is refused. A wrong password is refused with the server's `authentication failed`.

### Phase 20 — Sub-tasks

- [x] Accept an SQL string over ODBC and run it through the DataFusion path.
- [x] Return the rows to the ODBC client.
- [x] Pin unixODBC on Linux, and the connection string `SERVER`, `PORT`, `DATABASE`, `UID`, `PWD`.
- [x] Test: an ODBC client runs a `SELECT` and gets the same rows as `db.runCommand({ sql })`. A wrong password is refused and the right one runs the query.

## Phase 21 — ODBC catalog for GUI clients ✅

`SQLTables` returns `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `TABLE_TYPE`, `REMARKS` in that order. A call with an empty table name and table type `%` returns `TABLE` and `VIEW` in column 4 and does not ask the server. A GUI client fills the type filter from that column. A table pattern of `%` and a type list such as `TABLE,VIEW` returns the collections and views of the DSN database.

`SQLColumns` returns the ODBC column list. `COLUMN_NAME` is column 4 and `DATA_TYPE` is column 5. The types come from `admin.fields`. A collection the catalog does not know yet is sampled from its documents.

`SQLGetInfo` answers the values a GUI reads, including quoted identifier case (93), which is case-sensitive. An unknown info type returns a zero or an empty string. It does not return `HYC00`. A command-line client that lists tables and columns keeps the rows it already prints. The client maps `libkova.so` for the life of the process, so a rebuilt library loads only after that process quits.

Windows ODBC and `?` parameter binding stay out. A client that does not speak ODBC stays out. `SQLPrimaryKeys`, `SQLStatistics`, and `SQLGetTypeInfo` wait until a client error names the missing call.

### Phase 21 — Sub-tasks

- [x] `SQLTables` returns `TABLE_CAT`, `TABLE_SCHEM`, `TABLE_NAME`, `TABLE_TYPE`, `REMARKS` in that order. The type probe (`SQLTables` with table type `%`) returns `TABLE` and `VIEW` in column 4 and does not call the server.
- [x] `SQLColumns` returns the standard columns so a GUI reads `COLUMN_NAME` and `DATA_TYPE` by position.
- [x] `SQLGetInfo` answers the info types a GUI asks, including quoted identifier case (93). An unknown info type returns a zero or an empty string. It does not return `HYC00`.
- [x] Test: a GUI client's catalog sequence on DSN `Kova` returns `TABLE` and `VIEW`, lists `emp` and the views, and `SQLColumns` of `emp` returns `ename` as VARCHAR. A command-line client's table list and column list return those rows. Info type 93 returns success.

## Phase 22 — `kovadump` backup and restore ✅

`kovadump` is a second binary, `target/debug/kovadump`. It backs up a running Kova and restores into one. Backup and restore talk to the server over the MongoDB port. Restore writes through `insert` (and `create` / `createIndexes` / `create` of a view), so the WAL, the indexes, and replication see ordinary writes.

```text
kovadump backup  --db shop shop.kdmp
kovadump restore --db shop shop.kdmp
```

`--host` defaults to `127.0.0.1` and `--port` to `27018`. `--user` and `--password` are both set, or both omitted. Omit them while security is off.

Backup writes a Kova dump of one database: collections (documents and indexes, except the generated `_id_` index) and views. Restore drops a view that reads a collection being replaced, then drops and recreates each collection in the dump, then recreates the dumped views. A collection not named in the dump is left in place.

The four inputs restore accepts:

- **Kova dump 1.** Magic `KOVADMP1`, then BSON documents whose `kind` is `collection`, `document`, `index`, `counter`, or `view`. A `counter` record is one autoincrement field and its `last`.
- **`pg_dump` plain SQL** (not the custom archive). `CREATE TABLE`, text-format `COPY ... FROM stdin` ended by `\.`, and `INSERT INTO ... VALUES`. The schema is dropped (`public.emp` becomes `emp`). COPY values use the column type. A primary key or `UNIQUE` becomes a unique index. A foreign key becomes an index on the referencing columns. A `serial`, `bigserial`, `smallserial`, `AUTO_INCREMENT`, identity, or `DEFAULT nextval` column becomes a unique index when the primary key does not already cover it, and becomes an autoincrement field. `SELECT pg_catalog.setval(seq, n, called)` sets `last` to `n` when `called` is true and to one below `n` when it is false. The numbers stay as stored. `SET`, `OWNER`, and dollar-quoted function bodies are skipped. Binary `COPY` is refused.
- **`mysqldump` SQL.** Backtick identifiers, `CREATE TABLE`, and extended `INSERT` with backslash string escapes. `LOCK TABLES` is skipped.
- **`mongodump` directory or archive.** A directory is `<db>/<collection>.bson` plus optional `<collection>.metadata.json`. A dump root uses the subdirectory named by `--db`, or the only database subdirectory. An archive is format `0.1`: magic `0x8199e26d`, a prelude block, then namespace blocks ended by `0xffffffff`. The CRC is not checked. `timeseries` is refused. Names starting with `system.` are skipped.

PostgreSQL custom archives and MySQL binary logs stay out.

### Phase 22 — Sub-tasks

- [x] `kovadump` backup writes a Kova dump of a database on a running server.
- [x] Restore of that dump recreates the collections.
- [x] Restore a `pg_dump` plain-SQL file, a `mysqldump` SQL file, and a `mongodump` directory or archive.
- [x] Test: each of the four inputs restores, and a `find` returns the loaded documents.

## Phase 23 — Sharding ✅

A replica set is still a full copy of every document on each voter. Sharding splits one collection across Kova processes. The router hashes `_id` onto a ring of 128 virtual nodes per shard and forwards the command to the process that owns that arc. The router is a Kova process with `[sharding] role = "router"`. It does not store the documents. A shard is the Kova process at `shards[].client`. That process can also enable `[replication]`: the router speaks OP_MSG to the listed client address and does not join the replica set.

An insert with no `_id` receives one on the router, then goes to one shard. A find, count, update, or delete whose filter is one `_id` goes to that shard. The same commands without a single `_id` visit every shard, and the router merges the batches or sums `n`. A single-document update or delete that is not one `_id` stops at the first shard that changes a row. `create`, `drop`, `createIndexes`, and `dropDatabase` visit every shard, so the collection and its indexes exist wherever a document lands. `listCollections` and `listDatabases` merge the names. SQL on the router loads each named collection from every shard and runs DataFusion there. An aggregate stage other than match, project, or unwind is refused on the router, because a `$group` inside each shard is not the group of the whole collection. `mapReduce` visits every shard and the batches are concatenated. A transaction is refused on the router. With `[sharding]` absent the process is unchanged.

### Phase 23 — Sub-tasks

- [x] Consistent hashing of `_id`. The router is the process with `role = "router"`.
- [x] A command for one document is sent to the shard that owns it.
- [x] Test: two shards hold different documents of one collection, and a `find` on the router returns both.

## Phase 24 — Physical catalog (`admin`) ✅ Complete

A fresh process has database `admin`. `listDatabases` returns it before any user database exists. The catalog is a set of collections, not a second copy of the documents:

- `admin.databases`
- `admin.collections` — page lists, document count, and the `_id` set
- `admin.fields` — field name and type
- `admin.indexes`
- `admin.users`, `admin.roles`, `admin.views`

Applying a command updates that catalog in memory. The checkpoint writes the rows into `admin` and a version-2 root page, then truncates the WAL. A crash before the checkpoint replays the command. A directory written by an older process is scanned once; the next checkpoint writes the catalog.

Startup that finds the version-2 root reads those rows. It does not scan user pages to learn which collections exist, and it does not walk documents to rebuild the search index, the secondary indexes, or the `_id` set. A collection whose field list is empty is the exception: startup reads that collection and fills `admin.fields`. The search index is the Tantivy directory `text/`. Secondary indexes are `indexes.bin`, stamped with the catalog LSN. A `find` still reads the document from its page. A find on `admin.databases` keeps one row per database.

### Phase 24 — Sub-tasks

- [x] A fresh process has database `admin`. `listDatabases` returns it with no user data.
- [x] `admin.databases`, `admin.collections`, `admin.fields`, and `admin.indexes` store the catalog. A field row is the name and the type.
- [x] Creating a database, collection, index, or a new field updates that catalog as the command is applied. The checkpoint writes the rows before it truncates the WAL.
- [x] Users, roles, and views are read from `admin`.
- [x] Startup reads the catalog. It does not discover collections by scanning page files, and it does not walk every document to rebuild the search index, the secondary indexes, or the `_id` set.
- [x] Test: restart a process whose documents do not fit in the buffer pool. Startup does not scan those documents. `listDatabases`, `listCollections`, and the field list match the process that shut down. `find` still returns a document from its page.

## Phase 25 — Autoincrement ✅

The counter belongs to the collection. A SQL restore is one way to set it. Creating the collection in Kova is the other. `_id` stays a new ObjectId.

The last value used is a row in `admin.counters`, next to `admin.fields` and `admin.indexes`. One row per field:

```text
{ _id: "shop.orders.id", collection: "shop.orders", field: "id", last: 0 }
```

`last` is the number already handed out. The next insert that omits the field writes `last + 1` and stores that number back in `last`. A fresh field starts at `last: 0`, so the first insert stores 1. Changing the row by hand is the override: set `last` to 500 and the next insert stores 501. A value the client sends is kept, and `last` moves to it when the sent value is higher. The row is ordinary catalog data, so a checkpoint, a restart, and a Kova backup keep it, and a restore of that dump continues from the saved `last`.

`create` accepts `autoincrement` as a field name, or a list of field names:

```text
{ create: "orders", autoincrement: "id", $db: "shop" }
```

That writes the `admin.counters` row and adds a unique index when one does not already cover the field. The same insert path serves a driver, SQL, and a document loaded by restore. The unique index still refuses a duplicate.

A restored `serial`, `bigserial`, `smallserial`, `AUTO_INCREMENT`, `GENERATED ... AS IDENTITY`, or `DEFAULT nextval` column already gets a unique index, unless the primary key already covers that column. The integers in the dump stay in the field. Restore writes `last` from `setval`: when the third argument is true, `last` is the number in the call; when it is false, `last` is one below that number, so the next insert uses the number itself. A dump without `setval` and without a counter row sets `last` to the highest value already stored.

Each process keeps its own `admin.counters`. On a router, `create` starts every shard at `last: 0`, and an insert advances only the shard that stores the document. A find of `admin.counters` through the router keeps one row per `_id`.

### Phase 25 — Sub-tasks

- [x] `admin.counters` holds one row per autoincrement field: collection, field, and `last`. `create` with `autoincrement` writes `last: 0` and adds a unique index when that field is not already covered.
- [x] An update of that row changes `last`. The next insert that omits the field uses `last + 1`.
- [x] Restore writes `last` from `setval` (the call's number when the third argument is true, one below it when false). Without `setval` and without a counter row, `last` is the highest value already stored.
- [x] An insert that omits the column stores `last + 1` and writes that number into `last`. A client-supplied value that is higher moves `last` to it. The same path serves a collection created in Kova and a column restored from SQL.
- [x] The row survives restart, and a Kova backup restores it.
- [x] Test: `create` of `orders` with `autoincrement: "id"` leaves `admin.counters` at `last: 0`. An insert without `id` stores 1 and `last` becomes 1. The next stores 2. Setting `last` to 10 by hand makes the following insert store 11. After restart, the next insert stores 12.
- [x] Test: restore a table whose `setval` is 115 with the third argument true. `last` is 115. An insert without `id` stores 116. The next insert stores 117. After restart, the next insert stores 118.

## Phase 26 — Feed (Apache Iggy) ⚪ Planned

A program that wants to react to a write connects to a feed. It does not open a MongoDB driver, tail a collection, or read the oplog. The condition lives in that program. Kova publishes what happened.

The sidecar is Apache Iggy, a separate process. Kova is a client of it. Iggy stores the stream, the offset, and the consumers. A listener uses an Iggy client in its own language, or receives the same JSON over HTTP and applies its own condition. Kova does not embed the Iggy server.

`[feed]` is off unless `enabled` is true. With the section absent, or with `enabled` false, a write opens no connection and the insert path stays as it is.

```toml
[feed]
enabled = false
address = "127.0.0.1:8090"
username = "iggy"
password = "iggy"
stream = "kova"
topic = "changes"
```

After a committed insert, update, or delete, the process that applied the write publishes one JSON record to stream `kova`, topic `changes`:

```json
{ "op": "update", "ns": "db.C1", "id": "674a1c", "before": { "A": 1 }, "after": { "A": 2 }, "lsn": 120 }
```

`op` is `insert`, `update`, or `delete`. `ns` is `database.collection`. `before` is the document that was stored. `after` is the document that is stored. An insert leaves `before` empty. A delete leaves `after` empty. A listener that wants “field `A` on `C1` changed from 1 to 2” compares `before.A` and `after.A`. Another listener on the same feed can look at another collection. Kova does not store that rule.

The primary of a replica set publishes. A follower applies the same oplog entry and does not publish it again. A shard router forwards the command and does not publish it. The shard that stored the document publishes.

A committed transaction publishes one record per document after commit. An abort publishes nothing.

If Iggy is down, the database write still stands. The publisher retries. The same write can arrive twice, and the listener tells the repeat by `ns`, `id`, and `lsn`. The feed is not a second WAL and it does not roll a write back.

### Phase 26 — Sub-tasks

- [ ] `[feed]` absent or `enabled` false publishes nothing and opens no Iggy connection.
- [ ] A committed insert, update, or delete publishes one JSON record to stream `kova`, topic `changes`, from the process that applied the write. A follower and a shard router do not publish that write again.
- [ ] The record carries `op`, `ns`, `id`, `before`, `after`, and `lsn`. An insert has an empty `before`. A delete has an empty `after`. An update carries both documents.
- [ ] A committed transaction publishes one record per document after commit. An abort publishes nothing.
- [ ] Iggy being unreachable leaves the database write in place. A repeated delivery of the same write keeps the same `ns`, `id`, and `lsn`.
- [ ] Test: with the feed off, an insert does not connect. With the feed on, an update of field `A` from 1 to 2 is a record whose `before.A` is 1 and `after.A` is 2. An insert and a delete have the empty side. Stopping Iggy still lets the insert succeed.

## Phase 27 — User guide ⚪ Planned

The repository explains the engine to a contributor. `docs/ROADMAP.md`, `docs/ARCHITECTURE.md`, `docs/CONTRACT.md`, and `docs/CONTRIBUTING.md` stay that. A person who wants to run Kova has no guide. `make doc` stays the generated API reference.

This phase adds a README and a user guide in English, for what is already shipped. The README says what Kova is and how to build and run it: `make build`, `make run-debug`, port `27018`, `kova.toml`, and security off unless it is turned on. The commands match the Makefile.

The guide covers the rest of the running server:

- The MongoDB commands a client sends, and SQL through `db.runCommand({ sql })`.
- The ODBC connection keys `SERVER`, `PORT`, `DATABASE`, `UID`, and `PWD`.
- `kovadump` backup and restore, and the dump shapes it reads.
- The `admin` catalog, including `admin.counters` and `last`.
- A replica set and sharding: when each one is for, and that one process is not both the router and a voter.
- Autoincrement: `create` with `autoincrement`, and an update of the counter row.

The feed is a planned note that points at Phase 26. The guide describes it after that phase lands. The guide does not rename a client product, and it does not copy the roadmap phase by phase.

### Phase 27 — Sub-tasks

- [ ] A README at the repository root explains what Kova is, how to build it, how to run it, the default port, the config file, and that security is off unless enabled.
- [ ] A user guide in `docs/` covers the wire commands, SQL, the ODBC connection keys, `kovadump`, the `admin` catalog and counters, replica sets, and sharding.
- [ ] The text is English and describes what is shipped. The feed is a pointer to Phase 26 until that phase is done.
- [ ] `ROADMAP`, `ARCHITECTURE`, `CONTRACT`, and `CONTRIBUTING` stay the engineering documents. `make doc` stays the API reference.
- [ ] The README commands match the Makefile targets and port `27018`.

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
8. **Access control**: built-in and custom roles gate commands. Passwords are
   Argon2id. Page files and the WAL can be AES-256-GCM.
9. **Observable**: seventeen atomic counters, read back with `serverStatus`.

## Current Implementation Gaps

| Gap | Phase | Notes |
|-----|-------|-------|
| A committed write has no feed for other programs | 26 | `[feed]` will publish one JSON record per committed insert, update, or delete to Apache Iggy. The listener decides what to do with `before` and `after`. Today a program tails a collection or the oplog. |
| A person who wants to run Kova has no user guide | 27 | A README and a guide in `docs/` will cover the running server. The engineering documents stay as they are. |

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

*Phases 1–25 are complete. Still open: the feed (Phase 26) and the user guide (Phase 27).*
