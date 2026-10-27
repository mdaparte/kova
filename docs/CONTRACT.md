# Kova - Development Contract

This document defines the agreement between the engineer and the codebase:
how code must be written, structured, named, and verified.

---

## 1. Purpose

Establish consistent, reviewable conventions for all code changes. Any
engineer or AI contributor must be able to read this file and know exactly
how to produce a change that fits the project.

---

## 2. Language

- **All code, comments, documentation, log messages, and strings MUST be
  in English.**
- No Portuguese (or any natural language) is permitted inside source
  files, configuration, or generated documentation.

## 3. File Layout

| Path | Responsibility |
|---|---|
| `src/main.rs` | Entry point, Tokio runtime bootstrap, engine orchestration |
| `src/network/mod.rs` | TCP listener, Wire Protocol framing, client tasks, SQL command routing |
| `src/protocol/mod.rs` | Message types, opcodes, payload parsing, BSON, reply building |
| `src/types/mod.rs` | `PartitionCommand`, document model, query model, shared types |
| `src/storage/mod.rs` | Partition worker and channel routing |
| `src/storage/wal.rs` *(Phase 6)* | Write-Ahead Log with CRC32 checksums, LSN tracking, segment rolling |
| `src/storage/page.rs` *(Phase 6)* | Fixed-size page envelope with integrity verification |
| `src/storage/buffer_pool.rs` *(Phase 6)* | LRU page cache with pin/unpin eviction |
| `src/storage/disk.rs` *(Phase 6)* | Low-level disk I/O for page read/write |
| `src/storage/engine.rs` *(Phase 6)* | WAL, buffer pool, page store, and checkpoint |
| `src/storage/heap.rs` *(Phase 7)* | Document pages, overflow chains, and the catalog |
| `src/storage/aggregate.rs` *(Phase 8)* | Aggregation pipeline and the find cursor shape |
| `src/storage/crc.rs` *(Phase 6)* | CRC-32 used by the WAL and page checksums |
| `src/storage/mvcc.rs` *(Phase 9)* | Multi-Version Concurrency Control, snapshot reads |
| `src/storage/transaction.rs` *(Phase 9)* | Transaction manager (begin/commit/abort, conflict detection, timeout) |
| `src/search/mod.rs` | Tantivy per-field indexes, tokenizers, relevance queries |
| `src/index/btree.rs` *(Phase 10)* | B-Tree index for range queries + composite keys |
| `src/index/hash_index.rs` *(Phase 10)* | Hash index for O(1) lookups |
| `src/index/manager.rs` *(Phase 10)* | Index lifecycle management (create/drop/auto-maintain) |
| `src/replication/oplog.rs` *(Phase 11)* | Capped operations log for replication |
| `src/replication/replica.rs` *(Phase 11)* | Replica set management, heartbeat, election |
| `src/query_engine/mod.rs` | DataFusion integration, SQL parser, query optimizer, execution |
| `kova.toml` | Engine configuration file (env vars override it) |
| `src/logger/` | Structured logging, log capture, external sinks |
| `docs/` | Design specs, architecture decision records, roadmap |
| `docs/CONTRIBUTING.md` | How code in this repository is written |

## 4. Module Boundaries

- **network.rs** MUST NOT access shard state directly. It sends
  `PartitionCommand` through `crossbeam::channel::Sender`.
- **storage/mod.rs** MUST NOT open TCP connections or parse binary protocols.
- **search/mod.rs** MUST NOT read/write raw BSON. It consumes structured
  types from `types/mod.rs` only.
- Mutating a partition MUST never panic. Errors are returned as `Result`
  and logged at the caller boundary.
- **storage/wal.rs, storage/page.rs, storage/buffer_pool.rs,
  storage/disk.rs, storage/engine.rs** (Phase 6+) MUST NOT access the
  network layer directly. They are pure storage engine components.
- **storage/mvcc.rs and storage/transaction.rs** (Phase 9+) MUST coordinate
  with the buffer pool and WAL, but MUST NOT access the network layer directly.
- **index/btree.rs, index/hash_index.rs, index/manager.rs** (Phase 10+) MUST
  integrate with the storage engine (read/write through the buffer pool) and
  with the query planner (index scan strategy).
- **replication/oplog.rs and replication/replica.rs** (Phase 11+) MUST
  integrate with the storage engine (oplog reads/writes through the buffer
  pool) and with the network layer (heartbeat, election messages).

## 5. Naming

| Category | Convention |
|---|---|
| Crate / package | `kova` |
| Types / structs | `PascalCase` (e.g., `PartitionWorker`, `WALSegment`, `BufferPool`) |
| Functions / methods | `snake_case` |
| Constants | `SCREAMING_SNAKE_CASE` |
| Environment overrides | `SCREAMING_SNAKE_CASE` (e.g., `KOVA_PORT`, `KOVA_DATA_DIR`) |

## 6. Error Handling

- Recoverable errors at the boundary return `Result<T, E>`.
- Connection-level failures (abrupt socket close) log `Error` with the
  peer address.
- Unrecoverable engine failures abort with a message to `stderr` and a
  non-zero exit code.
- **Phase 6+:** Storage engine errors (WAL corruption, page I/O failure,
  buffer pool exhaustion) MUST be logged at `Error` level with enough
  context to diagnose the issue (page number, LSN, etc.) and MUST abort
  the operation (not panic) unless the error is unrecoverable (e.g., WAL
  corruption on startup).

## 7. Logging

- All logging MUST go through the `kova::logger` module.
- Every log entry MUST include a `level` (`Debug`, `Info`, `Warn`, `Error`)
  and a structured `key: value` payload where applicable.
- The default sink writes to `stderr` with the `[Kova]` prefix.
- The logger MUST be testable: capture logs in memory for assertion
  without writing to `stderr`.
- Production logs MUST NOT block the hot path. Use asynchronous
  dispatching (e.g., `crossbeam_channel` + background task) for high
  throughput.

### 7.1 Log Levels

| Level | When to use |
|---|---|
| `Debug` | Internal state, parsing details, hot-path diagnostics, WAL writes, page reads/writes, buffer pool hits/misses |
| `Info` | Startup, shutdown, connection events, partition routing, checkpoint progress, transaction begin/commit/abort, replication heartbeats, election events |
| `Warn` | Recoverable anomalies (duplicate key, invalid payload, WAL segment full, buffer pool eviction, MVCC conflict, replication lag) |
| `Error` | Unrecoverable failures (connection drop, storage crash, WAL corruption, page I/O failure, buffer pool exhaustion, replication failure) |

## 8. Documentation

- Every `pub` item MUST have a doc comment explaining `what` and `why`.
- Every module MUST have a module-level doc comment explaining its
  responsibility and interactions.
- Unexposed items are documented only when they carry non-obvious
  invariants (byte layout, ownership, thread guarantees, LSN ordering,
  page allocation rules).

## 9. Performance

- Hot paths (header parsing, serialization) MUST avoid heap allocations
  where possible.
- `BytesMut` buffers MUST be sized to the expected maximum message size.
- Partition reads MUST be lock-free via `crossbeam::channel`.
- **Phase 6+:** The buffer pool LRU eviction MUST be efficient (O(1) or
  O(log n) per operation). WAL writes MUST be batched where possible to
  reduce fsync overhead. Page reads/writes MUST use direct I/O or buffered
  I/O as appropriate for the workload. The page store MUST handle
  concurrent reads (multiple readers) and exclusive writes (one writer per
  page) without locking the entire store.

## 10. Developer Workflow

1. Write code against a published design document in `docs/`.
2. Verify compilation with `cargo check --all-targets`.
3. Verify the test suite with `cargo test`.
4. Run formatting (`cargo fmt --check`) and linting (`cargo clippy`).
5. Update the design document and roadmap when the change affects
   interfaces or the architecture.

## 11. Phasing

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
| 11 | Replication & HA: Oplog, replica sets, heartbeat, election |
| 12 | On-disk page compression: zstd on the BSON payload; WAL stays uncompressed |
| 13 | Security: RBAC, Argon2id, AES-256-GCM at rest, user management |
| 14 | Monitoring: 17 atomic counters, `serverStatus` |
| 15 | Connection pool: cap, tracking, idle close |
| 16 | Lucene-like views and facet queries |
| 17 | MapReduce across replica-set members |

## 12. Test Strategy

- **Unit tests** live next to the module they cover and test pure logic:
  header parsing, BSON payload, `PartitionCommand` routing, log capture,
  WAL entry encoding, page checksums, buffer pool eviction.
- **Integration tests** live in the same crate and test end-to-end flows:
  network handshake, storage reads/writes, search queries, SQL execution,
  WAL replay, and, in later phases, MVCC, indexes, and replication.
- **All tests must be deterministic** and MUST NOT depend on wall-clock
  time, random values, or external services.
- **No test may pass by accident** - failing assertions MUST fail the
  test suite.
- **All tests run with `cargo test`** (no separate test binary required).
- **Test coverage metric** tracked in CI: target is 80% for all modules.

## 13. Verification Checklist

- [ ] `cargo fmt --check` passes
- [ ] `cargo clippy --all-targets` reports no warnings
- [ ] `cargo test` passes
- [ ] Public API documented with doc comments
- [ ] No natural-language strings or comments in source code
- [ ] New modules have unit tests beside the code they cover
- [ ] New behaviours have tests that exercise the path a caller uses
- [ ] **(Phase 6+)** WAL replay tests verify crash recovery
- [ ] **(Phase 6+)** Page checksum verification tests
- [ ] **(Phase 6+)** Buffer pool LRU eviction tests
- [ ] **(Phase 9+)** MVCC snapshot read tests
- [ ] **(Phase 9+)** Transaction ACID tests
- [ ] **(Phase 10+)** Index creation/drop/query planner usage tests
- [ ] **(Phase 11+)** Oplog replay and replica set failover tests

---

*Generated from the brainstorming session based on `draft.md` and inspired
by GraniteDB (https://github.com/jmptrader/GraniteDB) for the disk storage
engine, WAL, buffer pool, MVCC, and replication design.*
