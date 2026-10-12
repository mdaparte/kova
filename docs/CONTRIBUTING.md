# Kova — Contributing Guide

> "This is the single source of truth for how the code must be written
> in the Kova project. Read it before opening any file."

---

## 1. Project Purpose

Kova is an ultra-high-performance, **hybrid memory+disk** database engine
written in Rust. It presents a native MongoDB Wire Protocol interface on
port 27018, processes hot data in RAM (in-memory hash maps + buffer pool
cache), persists data durably to disk via Write-Ahead Logging (WAL) with
CRC32 checksums and LSN tracking, and unifies transactional writes (MVCC +
ACID), full-text search (Tantivy), multi-model indexing (B-Tree, Hash,
Unique, Sparse, Full-Text), analytical SQL query planning (Apache
DataFusion), and high availability (oplog-based replication, replica sets)
in a single process.

**Inspiration:** VoltDB (lock-free, thread-per-core), GraniteDB (WAL,
buffer pool, page-based storage, MVCC, replication), MongoDB (wire
protocol, document model, aggregation pipeline), Apache DataFusion (SQL
parser + optimizer), Tantivy (full-text search).

## 2. Repository Layout

| Path | Responsibility |
|---|---|
| `src/main.rs` | Application entry point, Tokio runtime bootstrap, engine orchestration |
| `src/network.rs` | TCP server, MongoDB Wire Protocol header parsing, client task spawning, SQL command routing |
| `src/protocol/mod.rs` | Message types, opcodes, payload parsing, BSON enumerations, reply building |
| `src/types/mod.rs` | `PartitionCommand`, document model, query model, shared types |
| `src/storage/mod.rs` | Partition worker, in-memory storage, `crossbeam-channel` routing |
| `src/storage/wal.rs` *(Phase 5)* | Write-Ahead Log with CRC32 checksums, LSN tracking, segment rolling |
| `src/storage/page.rs` *(Phase 5)* | Fixed-size page management with integrity verification |
| `src/storage/buffer_pool.rs` *(Phase 5)* | LRU page cache with pin/unpin eviction |
| `src/storage/disk.rs` *(Phase 5)* | Low-level disk I/O for page read/write |
| `src/storage/engine.rs` *(Phase 5)* | Unified storage (WAL + buffer pool + disk + memory) |
| `src/storage/mvcc.rs` *(Phase 6)* | Multi-Version Concurrency Control, snapshot reads |
| `src/storage/transaction.rs` *(Phase 6)* | Transaction manager (begin/commit/abort, conflict detection, timeout) |
| `src/search/mod.rs` | Tantivy indexes, tokenizers, relevance queries |
| `src/index/btree.rs` *(Phase 7)* | B-Tree index for range queries + composite keys |
| `src/index/hash_index.rs` *(Phase 7)* | Hash index for O(1) lookups |
| `src/index/manager.rs` *(Phase 7)* | Index lifecycle management (create/drop/auto-maintain) |
| `src/replication/oplog.rs` *(Phase 8)* | Capped operations log for replication |
| `src/replication/replica.rs` *(Phase 8)* | Replica set management, heartbeat, election |
| `src/query_engine/mod.rs` | DataFusion integration, SQL parser, query optimizer, execution |
| `kova.toml` | Engine configuration file (env vars override it) |
| `src/logger/` | Structured logging, log capture, external sinks |
| `docs/` | Design specs, architecture decision records, roadmap |
| `tests/unit/` *(novo)* | Unit tests for parsing, types, channel routing |
| `tests/integration/` *(novo)* | Integration tests for end-to-end flows |

## 3. Code Conventions

### 3.1 Language

- Rust 2021 edition.
- All source code, comments, documentation, and log messages MUST be in
  English. No Portuguese (or any other natural language) in code, comments,
  or strings.

### 3.2 Module Boundaries

- `network.rs` MUST NOT access shard state directly. It sends commands
  through `crossbeam::channel::Sender<PartitionCommand>`.
- `storage.rs` MUST NOT open TCP connections or parse binary protocols.
- `search.rs` MUST NOT read/write raw BSON documents. It consumes
  structured types from `types.rs` only.
- Mutating a partition MUST never panic. Errors are returned as `Result`
  and logged at the caller boundary.
- **New for Phase 5+:** `storage/wal.rs`, `storage/page.rs`,
  `storage/buffer_pool.rs`, `storage/disk.rs`, `storage/engine.rs` MUST
  NOT access the network layer directly. They are pure storage engine
  components.
- **New for Phase 6+:** `storage/mvcc.rs` and `storage/transaction.rs`
  MUST coordinate with the buffer pool and WAL, but MUST NOT access the
  network layer directly.
- **New for Phase 7+:** `index/btree.rs`, `index/hash_index.rs`,
  `index/manager.rs` MUST integrate with the storage engine (read/write
  through the buffer pool) and with the query planner (index scan strategy).
- **New for Phase 8+:** `replication/oplog.rs` and `replication/replica.rs`
  MUST integrate with the storage engine (oplog reads/writes through the
  buffer pool) and with the network layer (heartbeat, election messages).

### 3.3 Naming

| Category | Convention |
|---|---|
| Crate / package | `kova` |
| Types / structs | `PascalCase` (e.g., `PartitionWorker`, `WALSegment`) |
| Functions / methods | `snake_case` |
| Constants | `SCREAMING_SNAKE_CASE` |
| Private fields | `snake_case` with leading underscore if needed for clarity |
| Environment variable overrides | `SCREAMING_SNAKE_CASE` (e.g., `KOVA_PORT`) |

### 3.4 Error Handling

- Errors that are recoverable at the boundary MUST return `Result<T, E>`
  with a descriptive error type in the `kova::error` module.
- Connection-level failures (client abruptly closed) MUST be logged at
  `Error` level with the peer address.
- Unrecoverable engine failures MUST abort with a clear message on
  `stderr` and a non-zero exit code.
- **New for Phase 5+:** Storage engine errors (WAL corruption, page I/O
  failure, buffer pool exhaustion) MUST be logged at `Error` level with
  enough context to diagnose the issue (page number, LSN, etc.).

### 3.5 Logging

- Use `eprintln!` with the `[Kova]` prefix for all engine diagnostics:
  - `[Kova] Servidor de rede iniciado na porta 27018` →
    `[Kova] Network server started on port 27018`
  - `[Kova] Conexão recebida de 127.0.0.1:45678` →
    `[Kova] Connection received from 127.0.0.1:45678`
- Production logs MUST be written to `stderr` only. File/syslog logging
  will be added in a future phase.
- **New for Phase 5+:** WAL writes, page reads/writes, buffer pool hits/misses,
  checkpoint progress, MVCC version creation, transaction begin/commit/abort,
  replication heartbeats, and election events MUST be logged at appropriate
  levels (Debug for internals, Info for milestones, Warn for anomalies, Error
  for failures).

### 3.6 Documentation

- Every public item MUST have a doc comment explaining `what` and `why`.
- Every module MUST have a module-level doc comment explaining its
  responsibility and interactions with other modules.
- Unexposed (private) items SHOULD be documented only when they carry
  non-obvious invariants (e.g., byte layout, ownership rules, thread
  guarantees).

### 3.7 Performance

- Hot paths (header parsing, document serialization) MUST avoid heap
  allocations where possible.
- `BytesMut` buffers MUST be sized to the expected maximum message size.
- Partition reads MUST be lock-free via `crossbeam::channel`.
- **New for Phase 5+:** The buffer pool LRU eviction MUST be efficient
  (O(1) or O(log n) per operation). WAL writes MUST be batched where
  possible to reduce fsync overhead. Page reads/writes MUST use direct I/O
  or buffered I/O as appropriate for the workload.

## 4. Developer Workflow

1. Write code against a published design document in `docs/`.
2. Verify the change compiles (`cargo check --all-targets`).
3. Verify the change passes the test suite (`cargo test`).
4. Run formatting (`cargo fmt --check`) and linting (`cargo clippy`).
5. Update the design document and the roadmap when the change affects
   interfaces or the architecture.

## 5. Phasing

Phases are tracked in `docs/ROADMAP.md` and aligned with `draft.md`.

- **Phase 1 — Networking:** TCP listener, header parsing, `OP_MSG` detection,
  concurrent task spawning. ✅ Complete.
- **Phase 2 — Partitioning:** `PartitionCommand`, `crossbeam-channel` routing,
  single-threaded partition workers. ✅ Complete.
- **Phase 3 — Search indexing:** `tantivy` integration with hot indexing. ✅ Complete.
- **Phase 3.5 — Search flexibility:** Per-field indexes, `$text.$boost`, wildcards,
  `$text.$decay`, analyzer config, `kova.toml`. ✅ Complete.
- **Phase 4 — Analytical Query Optimization:** DataFusion SQL execution
  against in-memory collections, including `COUNT(*)` and `GROUP BY`. ✅ Complete.
- **Phase 5 — Disk Storage Engine:** WAL, buffer pool, page-based storage,
  hybrid memory+disk architecture. 🔵 Current.
- **Phase 6 — Transactional Storage:** MVCC, ACID transactions, multiple
  isolation levels. ⚪ Planned.
- **Phase 7 — Indexing Subsystem:** B-Tree, Hash, Unique, Sparse indexes,
  index manager, auto-maintain, query planner integration. ⚪ Planned.
- **Phase 8 — Replication & HA:** Oplog, replica sets, heartbeat, election,
  read preference. ⚪ Planned.

---

*Generated from the brainstorming session based on `draft.md`.*
