# Kova Roadmap

## Vision (Why)

Unify transactional in-memory processing, MongoDB native interoperability,
and analytical full-text search in a single, lock-free, in-memory engine.

## Status

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Networking & Interception | ✅ Complete |
| 2 | Partition Isolation | ✅ Complete |
| 3 | Full-Text Search Indexing | 🔵 Current |
| 4 | Analytical Query Optimization | ⬜ Pending |

## Phase 1 — Networking & Interception ✅

- TCP listener (default port 27017, overridable via `KOVA_PORT`).
- 16-byte Wire Protocol header parsed in **little-endian** (the spec's
  byte order; big-endian parsing was caught by validating against a
  real `mongosh`).
- `OP_MSG (2013)` and legacy `OP_QUERY (2004)` handled; OP_QUERY is
  answered with `OP_REPLY (1)`.
- Handshake answered at the protocol layer: `ping`, `hello`,
  `isMaster`/`ismaster` (advertises a MongoDB 7.x standalone),
  plus `buildInfo`, `getParameter`, `endSessions`.
- `messageLength` validated (16 B … 48 MB) before any allocation.
- One `tokio::spawn` task per client; abrupt disconnects logged with
  the peer address.

## Phase 2 — Partition Isolation ✅

- `PartitionCommand` enum (insert, find, update, delete, count, drop,
  listCollections, search).
- `crossbeam-channel` routing; single-threaded partition worker per
  core, no locks on the hot path.
- In-memory storage: hash map per collection, deterministic FNV-1a
  partition routing.
- MongoDB semantics validated end-to-end with a real `mongosh`
  client: `insertMany` (multi-document), `find` (+ filtered),
  `count`, `updateOne` (`n`/`nModified`), `deleteOne`, `drop`,
  `listCollections` (broadcast across partitions).

## Phase 3 — Full-Text Search Indexing 🔵 (current)

- Integrate `tantivy` (RAM directory, one index per partition).
- Hot indexing: tokenize text fields as documents arrive
  (insert/update/delete keep the index consistent).
- Inverted indexes per partition.
- Relevance scoring (BM25) and fuzzy matching, exposed through the
  native MongoDB `$text` query operator.

## Phase 4 — Analytical Query Optimization ⬜

- Integrate `Apache DataFusion`.
- SQL parsing and AST generation.
- Query planning over multi-partition datasets.
- Cost-based optimization.

## Architecture Principles

1. **Lock-free in-memory**: thread-per-core partitions, no `Mutex`/`RwLock`
   on hot paths.
2. **Native MongoDB**: port 27017, `OP_MSG` (+ legacy `OP_QUERY`),
   little-endian wire format, BSON parsing.
3. **Hot indexing**: tantivy integrated beside the transactional workers.
4. **Analytical brain**: DataFusion as the internal optimizer.

## Contact

Main entry point: `src/main.rs`.

---

*Updated after Phase 2 validation against a real MongoDB client (`mongosh`).*
