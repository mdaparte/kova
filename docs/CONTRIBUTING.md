# Kova — Contributing Guide

> "This is the single source of truth for how the code must be written
> in the Kova project. Read it before opening any file."

---

## 1. Project Purpose

Kova is an ultra-high-performance, **hybrid memory+disk** database engine
written in Rust. It presents a native MongoDB Wire Protocol interface on
port 27018. Documents live in pages. RAM holds the buffer pool and the
Tantivy token index. Mutations are recorded in a write-ahead log before a
page is dirtied. SQL goes through Apache DataFusion. Transactions (MVCC),
secondary indexes, and replication are later phases, tracked in
`docs/ROADMAP.md`.

**Inspiration:** VoltDB (lock-free, thread-per-core), GraniteDB (WAL,
buffer pool, page-based storage, MVCC, replication), MongoDB (wire
protocol, document model, aggregation pipeline), Apache DataFusion (SQL
parser + optimizer), Tantivy (full-text search).

## 2. Repository Layout

| Path | Responsibility |
|---|---|
| `src/main.rs` | Application entry point, Tokio runtime bootstrap, engine orchestration |
| `src/network/mod.rs` | TCP server, Wire Protocol framing, client tasks, SQL command routing |
| `src/protocol/mod.rs` | Message types, opcodes, payload parsing, BSON enumerations, reply building |
| `src/types/mod.rs` | `PartitionCommand`, document model, query model, shared types |
| `src/config/mod.rs` | `kova.toml` loading and environment overrides |
| `src/storage/mod.rs` | Partition worker and `crossbeam-channel` routing |
| `src/storage/wal.rs` | Write-ahead log with CRC32 checksums, LSN tracking, segment rolling |
| `src/storage/page.rs` | Fixed-size page envelope with integrity verification |
| `src/storage/buffer_pool.rs` | LRU page cache with pin/unpin eviction |
| `src/storage/disk.rs` | Low-level disk I/O for page read/write |
| `src/storage/crc.rs` | CRC-32 used by the WAL and page checksums |
| `src/storage/engine.rs` | WAL, buffer pool, page store, and checkpoint |
| `src/storage/heap.rs` | Document pages, overflow chains, and the catalog |
| `src/storage/aggregate.rs` | Aggregation pipeline and the find cursor shape |
| `src/storage/mvcc.rs` *(Phase 9)* | Multi-version concurrency control, snapshot reads |
| `src/storage/transaction.rs` *(Phase 9)* | Transaction manager (begin/commit/abort, conflict detection, timeout) |
| `src/search/mod.rs` | Tantivy indexes, tokenizers, relevance queries |
| `src/index/btree.rs` *(Phase 10)* | B-Tree index for range queries and composite keys |
| `src/index/hash_index.rs` *(Phase 10)* | Hash index for O(1) lookups |
| `src/index/manager.rs` *(Phase 10)* | Index lifecycle (create, drop, auto-maintain) |
| `src/replication/oplog.rs` *(Phase 11)* | Raft log used as the oplog, durable under `data/raft-{id}/` |
| `src/replication/replica.rs` *(Phase 11)* | OpenRaft membership, election, heartbeats, and apply |
| `src/query_engine/mod.rs` | DataFusion integration, SQL parser, query optimizer, execution |
| `kova.toml` | Engine configuration file (environment variables override it) |
| `src/logger/` | Structured logging, log capture, external sinks |
| `docs/` | Design specs, architecture, roadmap |
| module tests | `cargo test` runs the tests that live beside each module |

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
- `storage/wal.rs`, `storage/page.rs`, `storage/buffer_pool.rs`,
  `storage/disk.rs`, `storage/engine.rs`, and `storage/heap.rs` MUST NOT
  access the network layer directly. They are storage engine components.
- `storage/mvcc.rs` and `storage/transaction.rs` (Phase 9) MUST coordinate
  with the buffer pool and WAL, and MUST NOT access the network layer.
- `index/btree.rs`, `index/hash_index.rs`, and `index/manager.rs` (Phase 10)
  MUST read and write through the buffer pool and MUST be visible to the
  query planner.
- `replication/oplog.rs` and `replication/replica.rs` (Phase 11) MUST
  keep the oplog as the Raft log. A committed entry is applied through
  the partition router. Heartbeats and elections are OpenRaft's, not a
  second protocol. The standalone path MUST stay in place when
  `[replication]` is absent.

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
- Storage engine errors (WAL corruption, page I/O failure, buffer pool
  exhaustion) MUST be logged at `Error` level with enough context to
  diagnose the issue (page number, LSN, and so on).

### 3.5 Logging

- Use `eprintln!` with the `[Kova]` prefix for all engine diagnostics:
  - `[Kova] Servidor de rede iniciado na porta 27018` →
    `[Kova] Network server started on port 27018`
  - `[Kova] Conexão recebida de 127.0.0.1:45678` →
    `[Kova] Connection received from 127.0.0.1:45678`
- Production logs MUST be written to `stderr` only. File/syslog logging
  will be added in a future phase.
- WAL writes, page reads and writes, buffer pool hits and misses, and
  checkpoint progress MUST be logged at the appropriate level: Debug for
  internals, Info for milestones, Warn for anomalies, Error for failures.
  The same rule applies to MVCC, transactions, and replication when those
  phases land.

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
- Buffer pool LRU eviction MUST be O(1) per operation. WAL appends MUST
  stay sequential. Page reads and writes go through the page store.

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
- **Phase 4 — Search flexibility:** per-field indexes, `$text.$boost`, wildcards,
  `$text.$decay`, analyzer config, `kova.toml`. ✅ Complete.
- **Phase 5 — Analytical SQL:** DataFusion execution, including `COUNT(*)`
  and `GROUP BY`. ✅ Complete.
- **Phase 6 — Disk storage engine:** WAL, buffer pool, page store, checkpoint,
  and crash recovery. ✅ Complete.
- **Phase 7 — Disk-resident collections:** documents live in pages; the buffer
  pool is the only RAM that grows with the cache. ✅ Complete.
- **Phase 8 — Query language and aggregation:** the remaining find
  predicates, projection, sort, skip, limit, update operators, and the
  twelve-stage pipeline, reading pages. ✅ Complete.
- **Phase 9 — Transactional storage:** MVCC, ACID transactions, multiple
  isolation levels. ✅ Complete.
- **Phase 10 — Indexing subsystem:** B-Tree, hash, unique, and sparse indexes,
  rebuilt from document pages and maintained on every write. ✅ Complete.
- **Phase 11 — Replication and HA:** OpenRaft oplog, election, heartbeats,
  and replica fields on `hello`. ✅ Complete.
- **Phase 12 — On-disk page compression:** zstd on the BSON payload of each
  page, WiredTiger-style. The WAL stays uncompressed. ✅ Complete.
- **Phase 13 — Security:** five built-in roles, custom roles over eleven
  actions, Argon2id passwords, AES-256-GCM at rest, user management. ✅ Complete.
- **Phase 14 — Monitoring:** seventeen atomic counters and `serverStatus`. ✅ Complete.
- **Phase 15 — Connection pool:** `max_connections`, idle close, and
  shutdown or a disabled user free the slot. ✅ Complete.
- **Phase 16 — Views:** a MongoDB view (`viewOn` + pipeline). `kova.inverted`
  adds a Tantivy index on disk with discovered field types. ✅ Complete.
- **Phase 17 — Map projection:** `mapReduce` returns `{ key, value }` for each match. No `out`, no fan-out. ✅ Complete.
- **Phase 18 — Transactions in the oplog:** `commit` proposes one
  `applyTransaction` entry. Followers apply it as one WAL record.
  `begin`, staged statements, and `abort` stay on the primary. ✅ Complete.
- **Phase 19 — Snapshot shipping:** a follower outside the retained oplog
  installs a snapshot of the pages and replays from that index. An arbiter
  does not install document pages. ✅ Complete.
- **Phase 20 — ODBC connector:** unixODBC on Linux (`libkova.so`) runs the
  same SQL as `db.runCommand({ sql })`. `UID` and `PWD` call `authenticate`. ✅ Complete.
- **Phase 21 — ODBC catalog for GUI clients:** `SQLTables`, `SQLColumns`,
  and `SQLGetInfo` match the layout a database IDE reads by column position.
  LibreOffice Base lists the collections and opens the SQL window. ⚪ Planned.
- **Phase 22 — `kovadump`:** backup and restore. Restore reads a Kova dump,
  `pg_dump` plain SQL, `mongodump`, or `mysqldump` SQL. ⚪ Planned.
- **Phase 23 — Sharding:** documents split across shards. The routing
  scheme is chosen when the phase starts. ⚪ Planned.

---

*Generated from the brainstorming session based on `draft.md`.*
