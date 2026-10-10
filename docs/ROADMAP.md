# Kova Roadmap

## Vision (Why)

Unify transactional in-memory processing, MongoDB native interoperability,
and analytical full-text search in a single, lock-free, in-memory engine.

## Phase 1 — Networking & Interception (current)

- TCP listener on port 27017 (overridable via `KOVA_PORT`).
- Read the 16-byte MongoDB Wire Protocol header
  (`messageLength`, `requestId`, `responseTo`, `opCode`).
- Detect `OP_MSG (2013)` and log `[Kova] Valid MongoDB message received`.
- Spawn a `tokio::spawn` task per client for concurrency.
- Basic error handling for abrupt socket closure.

## Phase 2 — Partition Isolation

- Define `PartitionCommand` enum (insert, find, update, delete, search).
- Bridge network tasks to storage workers via `crossbeam::channel`.
- Implement a single-threaded partition worker per core.
- Lock-free in-memory storage (hash maps / segmented vectors).

## Phase 3 — Full-Text Search Indexing

- Integrate `tantivy`.
- Hot indexing: tokenize text fields as documents arrive.
- Inverted indexes per partition.
- Relevance scoring (BM25) and fuzzy matching.

## Phase 4 — Analytical Query Optimization

- Integrate `Apache DataFusion`.
- SQL parsing and AST generation.
- Query planning over multi-partition datasets.
- Cost-based optimization.

## Architecture Principles

1. **Lock-free in-memory**: thread-per-core partitions, no `Mutex`/`RwLock`
   on hot paths.
2. **Native MongoDB**: port 27017, `OP_MSG`, BSON parsing.
3. **Hot indexing**: tantivy integrated beside the transactional workers.
4. **Analytical brain**: DataFusion as the internal optimizer.

## Contact

Main entry point: `src/main.rs`.

---

*Generated from the brainstorming session based on `draft.md`.*
