# Kova: Ultra-High Performance Hybrid Memory+Disk Database Engine

**Kova** is a cutting-edge systems engineering project built from the ground up in **Rust**. Its vision is to unify multiple paradigms that traditionally operate separately in infrastructure stacks:

- The predictable speed and transactional determinism of *NewSQL* in-memory systems
- The flexibility of dynamic *NoSQL* document stores (MongoDB native wire protocol)
- The analytical power of a full-text relevance search engine (*Tantivy/Lucene*)
- The enterprise-grade query parsing and optimization of Apache DataFusion (SQL)
- **Durable disk storage** with Write-Ahead Logging (WAL), buffer pool, and page-based storage (inspired by GraniteDB)
- **Multi-model indexing**: B-Tree (range), Hash (O(1)), Unique, Sparse, and Full-Text (Tantivy)
- **High availability** via oplog-based replication, replica sets, and heartbeat monitoring

---

## 👁️ Project Vision & Purpose (The "Why")

In modern software development, engineering teams are often forced to duplicate their infrastructure. They rely on a transactional database for quick updates, a separate search cluster (Elasticsearch/Solr) for full-text search, and a separate analytical engine for SQL queries.

**Kova solves this by unifying these capabilities into a single hybrid memory+disk database engine:**

* Processes data at the physical speed of RAM for hot workloads (in-memory hash maps + buffer pool cache)
* Persists data durably to disk via Write-Ahead Logging (WAL) with CRC32 checksums and LSN tracking
* Speaks the native MongoDB wire protocol that developers already love and use (`OP_MSG`, `db.runCommand({ sql: "..." })`)
* Parses and structures internal query execution with Apache DataFusion (SQL parser + optimizer)
* Indexes string content in real-time for instant full-text analytics (Tantivy)
* Supports multi-model indexing: B-Tree (range), Hash (O(1)), Unique, Sparse, and Full-Text
* Provides high availability via oplog-based replication and replica sets

**Kova is inspired by:**
* **VoltDB** - lock-free, thread-per-core in-memory processing
* **GraniteDB** - WAL, buffer pool, page-based storage, MVCC, replication
* **MongoDB** - wire protocol, document model, aggregation pipeline
* **Apache DataFusion** - SQL parsing, optimization, and execution
* **Tantivy** - full-text search (Lucene equivalent in Rust)

This makes Kova an ideal solution for **Edge Computing/IoT environments** (where resources are limited and extreme local efficiency is mandatory) and as an **elite technical portfolio project** that showcases mastery over low-level Rust concurrency, memory management, and storage engine design.

---

## 🏗️ The Core Architectural Pillars

### 1. Hybrid Memory+Disk Engine (VoltDB + GraniteDB Inspiration)
* **In-Memory Hot Path:** Thread-per-core partitions, lock-free `crossbeam::channel` routing, in-memory hash maps for the working set.
* **Disk Durability:** Write-Ahead Logging (WAL) with CRC32 checksums and LSN tracking. Every write goes to the WAL before the memory state. Crash recovery replays the WAL on startup.
* **Page-Based Storage:** Fixed-size pages (16 KB default, configurable) with integrity verification and overflow support. Pages stored on disk in a structured directory layout.
* **Buffer Pool:** LRU-based page cache with pin/unpin eviction. The buffer pool bridges memory and disk: hot pages stay in RAM, cold pages are paged to disk.
* **MVCC + ACID Transactions:** Multi-Version Concurrency Control with snapshot reads, multiple isolation levels (Read Uncommitted, Read Committed, Repeatable Read, Snapshot, Serializable), conflict detection, and automatic transaction timeout.

### 2. Native MongoDB Interoperability (The Wire Protocol)
* **Drop-in Compatibility:** Kova exposes a server on port 27018 that speaks the native MongoDB wire protocol (`OP_MSG`, legacy `OP_QUERY`). Any application using official MongoDB drivers connects seamlessly.
* **SQL via MongoDB:** `db.runCommand({ sql: "SELECT ... FROM ... WHERE ..." })` is accepted and executed via DataFusion, returning result sets as BSON.

### 3. Integrated Advanced Search (Tantivy Equivalent)
* **Hot Indexing:** As soon as a document enters a partition, its text fields are processed through Tantivy inverted indexes. Full-text queries use `$text` with BM25 relevance, fuzzy fallback, wildcards, per-field boosts, and recency decay.

### 4. Enterprise-Grade Query Parsing and Optimization (Apache DataFusion)
* **SQL Parser + Optimizer:** Apache DataFusion parses SQL, generates AST/logical/physical plans, and optimizes execution over the storage layer.
* **Query Execution:** SQL queries are executed against the storage layer (in-memory + disk), returning results as BSON via the MongoDB wire protocol.

### 5. Multi-Model Indexing Subsystem
* **B-Tree Indexes:** Ordered indexes for range queries with composite key support.
* **Hash Indexes:** O(1) exact-match lookups.
* **Unique Constraints:** Enforce uniqueness across indexed fields.
* **Sparse Indexes:** Skip documents missing the indexed field.
* **Full-Text Indexes (Tantivy):** Already implemented (Phase 3/4). The index manager creates, drops, and auto-maintains full-text indexes alongside other index types.
* **Auto-Maintenance:** Indexes are updated on insert/update/delete - no manual reindexing.

### 6. Replication & High Availability
* **Oplog-Based Replication:** Every write is recorded in a capped operations log with timestamps. Secondaries replay the oplog to stay in sync.
* **Replica Sets:** Primary/Secondary/Arbiter roles with heartbeat monitoring and automatic election.
* **Read Preference:** Clients can read from primary, secondary, or nearest.

---

## 🛠️ Selected Technology Stack (Rust Ecosystem)

Kova's core is engineered by assembling highly optimized, production-proven crates from the Rust systems ecosystem:

* **`tokio` & `bytes`:** Asynchronous networking layer for thousands of concurrent TCP connections and raw binary streams via `BytesMut`.
* **`bson`:** Official MongoDB BSON serialization for parsing and manipulating dynamic document structures.
* **`crossbeam-channel`:** Ultra-fast MPSC channels for lock-free routing from network tasks to single-threaded storage workers.
* **`tantivy`:** The "Lucene of Rust" - blazing fast, memory-optimized full-text search engine.
* **`datafusion` (Apache):** Extensible SQL query planner and execution framework as the internal analytical brain.
* **`sled` or custom page store** (Phase 6+): For disk-based page storage, or a custom implementation inspired by GraniteDB.
* **`crc32fast`:** CRC32 checksums for WAL entries and page integrity verification.

---

## 📂 Codebase Structure

The local source code is modularized into dedicated files inside the `kova/src/` directory:

* `main.rs`: Application entry point, Tokio runtime bootstrap, engine orchestration.
* `network.rs`: Asynchronous TCP server and binary parser for the *MongoDB Wire Protocol* 16-byte header.
* `protocol/mod.rs`: Message types, opcodes, payload parsing, BSON enumerations, reply building.
* `types/mod.rs`: Partition command enum, document model, query model, shared types.
* `storage/mod.rs`: Partition worker, in-memory storage, `crossbeam-channel` routing.
* `storage/wal.rs` *(Phase 6)*: Write-Ahead Log with CRC32 checksums, LSN tracking, segment rolling.
* `storage/page.rs` *(Phase 6)*: Fixed-size page management with integrity verification.
* `storage/buffer_pool.rs` *(Phase 6)*: LRU page cache with pin/unpin eviction.
* `storage/disk.rs` *(Phase 6)*: Low-level disk I/O for page read/write.
* `storage/engine.rs` *(Phase 6)*: WAL, buffer pool, page store, and checkpoint.
* `storage/heap.rs` *(Phase 7)*: Document pages, overflow chains, and the catalog.
* `storage/aggregate.rs` *(Phase 8)*: Aggregation pipeline and the find cursor shape.
* `storage/crc.rs` *(Phase 6)*: CRC-32 used by the WAL and page checksums.
* `storage/mvcc.rs` *(Phase 9)*: Multi-Version Concurrency Control, snapshot reads.
* `storage/transaction.rs` *(Phase 9)*: Transaction manager (begin/commit/abort, conflict detection, timeout).
* `search/mod.rs`: Tantivy indexes, tokenizers, relevance queries.
* `index/btree.rs` *(Phase 10)*: B-Tree index for range queries + composite keys.
* `index/hash_index.rs` *(Phase 10)*: Hash index for O(1) lookups.
* `index/manager.rs` *(Phase 10)*: Index lifecycle management (create/drop/auto-maintain).
* `replication/oplog.rs` *(Phase 11)*: Capped operations log for replication.
* `replication/replica.rs` *(Phase 11)*: Replica set management, heartbeat, election.
* `query_engine/mod.rs`: DataFusion integration, SQL parser, query optimizer, execution.

---

## 📈 Roadmap for the AI Coding Agent (Phase-by-Phase)

1. **Phase 1 (Networking & Interception):** TCP socket, MongoDB client handshake, 16-byte binary header parsing. ✅ Complete.
2. **Phase 2 (Partition Isolation):** `crossbeam-channel` routing, single-threaded partition workers, `PartitionCommand` enum. ✅ Complete.
3. **Phase 3 (Full-Text Search):** `tantivy` integration, hot indexing, `$text` queries. ✅ Complete.
4. **Phase 4 (Search Flexibility):** Per-field indexes, `$text.$boost`, wildcards, `$text.$decay`, analyzer config, `kova.toml`. ✅ Complete.
5. **Phase 5 (DataFusion SQL):** SQL parsing, plan extraction, `db.runCommand({ sql: "..." })`. ✅ Complete.
6. **Phase 6 (Disk Storage Engine):** WAL, buffer pool, page store, checkpoint, crash recovery. ✅ Complete.
7. **Phase 7 (Disk-Resident Collections):** documents live in pages; RAM is the buffer pool. ✅ Complete.
8. **Phase 8 (Query Language and Aggregation):** remaining find predicates (`$not`, `$regex`, `$type`, `$elemMatch`), projection, sort, skip, limit, update operators (`$unset`, `$inc`, `$push`, `$pull`, `$rename`), and a twelve-stage pipeline (`$match` through `$lookup` and `$out`), reading pages. ✅ Complete.
9. **Phase 9 (Transactional Storage):** MVCC, ACID transactions, multiple isolation levels. ⚪ Planned.
10. **Phase 10 (Indexing Subsystem):** B-Tree, Hash, Unique, Sparse indexes, index manager, auto-maintain, query planner integration. ⚪ Planned.
11. **Phase 11 (Replication & HA):** Oplog, replica sets, heartbeat, election, read preference. ⚪ Planned.
12. **Phase 12 (On-Disk Page Compression):** zstd on the BSON payload of each page, WiredTiger-style. The WAL stays uncompressed. ⚪ Planned.
