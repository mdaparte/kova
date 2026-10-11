# Kova Roadmap

## Vision (Why)

Unify transactional in-memory processing, MongoDB native interoperability,
and analytical full-text search in a single, lock-free, in-memory engine.

## Status

| Phase | Scope | Status |
|-------|-------|--------|
| 1 | Networking & Interception | ✅ Complete |
| 2 | Partition Isolation | ✅ Complete |
| 3 | Full-Text Search Indexing | ✅ Complete |
| 3.5 | Search Flexibility & Configuration | ✅ Complete |
| 4 | Analytical Query Optimization | 🔵 Current |

## Phase 1 — Networking & Interception ✅

- TCP listener (default port 27017, overridable via `KOVA_PORT`).
- 16-byte Wire Protocol header parsed in **little-endian** (the spec's
  byte order; big-endian parsing was caught by validating against a
  real `mongosh`).
- `OP_MSG (2013)` and legacy `OP_QUERY (2004)` handled; OP_QUERY is
  answered with `OP_REPLY (1)`.
- Handshake answered at the protocol layer: `ping`, `hello`,
  `isMaster`/`ismaster` (advertises a MongoDB 8.2 standalone,
  wire version 27),
  plus `buildInfo`, `getParameter`, `endSessions`, `whatsmyuri`.
  The OP_MSG decoder honors the spec's bit 0 for the trailing
  CRC-32C checksum (the Java driver sets it; the Node driver does
  not), so Java clients complete their handshake instead of hanging
  on an unanswered `whatsmyuri`.
- `messageLength` validated (16 B … 48 MB) before any allocation.
- One `tokio::spawn` task per client; abrupt disconnects logged with
  the peer address.

## Phase 2 — Partition Isolation ✅

- `PartitionCommand` enum (insert, find, update, delete, count, drop,
  create, dropDatabase, createIndexes, listIndexes, listCollections,
  listDatabases, collectionStats, databaseStats, search).
- `crossbeam-channel` routing; single-threaded partition worker per
  core, no locks on the hot path.
- In-memory storage: hash map per collection, deterministic FNV-1a
  partition routing.
- **Database namespacing**: collections are stored under `db.collection`,
  so `test.users` and `shop.users` are distinct — required by GUI
  clients (Compass, Studio 3T) whose create/list flows rely on
  `create`, `listDatabases` and `dropDatabase`.
- **Driver compatibility**: `createIndexes` and `listIndexes` are
  implemented for Java/Spring Data clients, which declare indexes at
  start-up and list them when a collection is browsed. Index
  declarations are recorded as metadata (the mandatory `_id_` plus
  whatever the client declares) and reported back by `listIndexes`;
  they are not yet built into query-accelerating structures, which
  arrives with the Phase 4 query engine.
- MongoDB semantics validated end-to-end with a real `mongosh`
  client: `insertMany` (multi-document), `find` (+ filtered),
  `count`, `updateOne` (`n`/`nModified`), `deleteOne`, `drop`,
  `listCollections` (broadcast across partitions).
- **Storage stats**: `collStats` and `dbStats` are implemented for
  GUI clients (Studio 3T storage panels), which query them when a
  collection or database is inspected. `collStats` reports the real
  serialized size of every document (`avgObjSize` is measured, not
  guessed) plus the index metadata recorded by `createIndexes`;
  `dbStats` is broadcast and summed across partitions. Sizes are
  document-serialized byte counts, not on-disk storage: Kova is
  RAM-only, so `storageSize` mirrors `size` by design.

## Phase 3 — Full-Text Search Indexing ✅

- `tantivy` integrated with a RAM directory per partition.
- Hot indexing: documents are tokenized as they arrive
  (insert/update/delete/drop keep the index consistent).
- Relevance scoring (BM25) and fuzzy (distance-1) fallback, exposed
  through the native MongoDB `$text` query operator.
- Validated end-to-end with a real `mongosh` (8/8 assertions,
  including typo tolerance and post-filter combinations).

## Phase 3.5 — Search Flexibility & Configuration ✅

- **Per-field indexes**: one tantivy index per document field per
  partition, enabling MongoDB-style field weights.
- **`$text.$boost`**: per-field BM25 multipliers
  (`{$text: {$search, $boost: {title: 10}}}`), summed at merge time
  like Elasticsearch combines multi-field matches.
- **Wildcards**: Solr-style `dat*` / `d?ta` tokens expand to
  whole-term automaton (regex) queries.
- **`$text.$decay`**: exponential half-life recency decay over a
  BSON date field, applied as a worker-side re-rank.
- **Configurable analyzer**: `[search]` section in `kova.toml`
  selects `default` or `en_stem` tokenization plus custom stop words
  (fixed at index creation, like a MongoDB text index).
- **Configuration file**: `kova.toml` (port, log level, non-Mongo
  warning toggle, analyzer), overridable by environment variables
  (`KOVA_PORT`, `KOVA_LOG_LEVEL`, `KOVA_WARN_NON_MONGO_QUERIES`,
  `KOVA_CONFIG`); env wins over file, file wins over defaults.
- **Non-Mongo warnings**: Kova extensions (`$boost`, `$decay`) and
  unsupported operators (`$analyzer`, `$language`) log a WARN when
  `warn_on_non_mongo_queries` is enabled.

## Phase 4 — Analytical Query Optimization 🔵 (current)

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

*Updated after Phase 3.5 (search flexibility + configuration file);
Phases 1–3 were validated against a real MongoDB client (`mongosh`).*
