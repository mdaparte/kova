# Kova Architecture Specification

## 1. Intent and Why

Kova is an ultra-high-performance, in-memory database engine in Rust.
The goal is to unify three paradigms that traditionally live in
separate infrastructure stacks:

- **Transactional determinism** (NewSQL in-memory systems)
- **Document flexibility** (NoSQL, MongoDB native protocol)
- **Analytical search** (full-text relevance, Lucene-like)

This single-engine approach avoids the synchronization cost between a
transactional DB and a separate search cluster, making Kova suitable for
edge IoT devices and as a showcase of low-level Rust concurrency and
memory management.

## 2. Goals

- Accept native MongoDB clients on port 27017 without driver changes.
- Keep the working set entirely in RAM for predictable, microsecond-latency
  transaction processing.
- Provide full-text search and relevance scoring over indexed fields.
- Parse, optimize, and execute analytical query plans with DataFusion.
- Avoid heavy locking and allocator pauses via thread-per-core partitioning.

## 3. Non-Goals (Phase 1)

- SQL query execution via DataFusion is out of scope for Phase 1.
- Persistent storage / WAL / crash recovery is not part of this phase.
- Sharding across multiple nodes.
- Full MongoDB command-level compatibility (only the wire protocol
  minimum required to speak with official drivers).

## 4. High-Level Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  Client TCP  │     │  Client TCP  │     │  Client TCP  │
│  (port 27017)│     │  (port 27017)│     │  (port 27017)│
└──────┬───────┘     └──────┬───────┘     └──────┬───────┘
       │                    │                    │
       └──────────┬─────────┴────────────────────┘
                  │  TCP accept loop
                  │  tokio::spawn per client
                  ▼
┌────────────────────────────────────────────────────────────┐
│  Network Layer (tokio::net::TcpListener)                    │
│  - Reads 16-byte Wire Protocol header                       │
│  - Parses messageLength, requestId, responseTo, opCode      │
│  - Dispatches opCode to the partition channel               │
└────────────────────────────────────────────────────────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│  Partition Workers (thread-per-core)                        │
│  - lock-free ring of in-memory containers                   │
│  - routes commands via crossbeam-channel                     │
│  - no Mutex/RwLock on hot data structures                   │
└────────────────────────────────────────────────────────────┘
                  │
        ┌─────────┼─────────┐
        ▼         ▼         ▼
   ┌──────────┐ ┌──────────┐ ┌──────────┐
   │ Partition│ │ Partition│ │ Partition│
   │  0       │ │  1       │ │  N-1     │
   │  Hash(key)│ │  Hash(key)│ │  Hash(key)│
   └──────────┘ └──────────┘ └──────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│  Search Subsystem (Tantivy)                                 │
│  - hot indexing: parse document text fields                 │
│  - inverted indexes per partition                           │
│  - relevance scoring (BM25)                                 │
└────────────────────────────────────────────────────────────┘
                  │
                  ▼
┌────────────────────────────────────────────────────────────┐
│  Query Engine (DataFusion, Phase 4)                         │
│  - SQL parser / AST                                         │
│  │                                                          │
└────────────────────────────────────────────────────────────┘
```

## 5. Data Flow

1. A client opens a TCP connection on port 27017.
2. The network task spawns a `tokio::spawn` task.
3. The network task reads exactly 16 bytes: `messageLength` (int32),
   `requestId` (int32), `responseTo` (int32), `opCode` (int32).
4. If `opCode == OP_MSG (2013)`, the payload is parsed as BSON.
5. The parsed command is dispatched to a partition worker through
   `crossbeam::channel::Sender<PartitionCommand>`.
6. The partition worker handles the command (insert, find, update, delete,
   search) inside its isolated memory region.
7. Results flow back through an `mpsc` channel and are serialized as a
   MongoDB response (OP_REPLY / OP_MSG) to the client.

## 6. Message Contract (Wire Protocol)

### 6.1 Header (16 bytes, little-endian)

| Offset | Size | Field            | Type  |
|--------|------|------------------|-------|
| 0      | 4    | messageLength    | int32 |
| 4      | 4    | requestId        | int32 |
| 8      | 4    | responseTo       | int32 |
| 12     | 4    | opCode           | int32 |

### 6.2 OpCodes

| Opcode | Name     | Constant       |
|--------|----------|----------------|
| 2013   | OP_MSG   | `wire::OP_MSG`   |
| 2004   | OP_QUERY | `wire::OP_QUERY` |
| 1      | OP_REPLY | `protocol::OP_REPLY` |

All integers in the wire protocol (header, `flagBits`, OP_REPLY fields)
are little-endian, matching the official MongoDB Wire Protocol
specification. OP_QUERY is used by drivers only for the initial
`isMaster`/`hello` handshake; it is answered with an OP_REPLY frame.
Every message length is validated against `MAX_MESSAGE_SIZE` (48 MB)
before any buffer is allocated.

## 7. Partitioning Strategy

- Each partition owns a disjoint set of collections, keyed by the
  `db.collection` namespace and hashed with FNV-1a. Two databases may
  hold a collection with the same name without colliding.
- Commands route to the owning partition deterministically; commands
  that span partitions (`listCollections`, `listDatabases`,
  `dropDatabase`, `dbStats`) are broadcast and merged in the network
  layer. Commands that name a single collection (`createIndexes`,
  `listIndexes`, `collStats`) are *not* broadcast: one partition
  answers alone.
- Partition workers are single-threaded and process commands sequentially.
- No cross-partition locks are required in the hot path.

## 8. Search Subsystem

Tantivy is Kova's embedded Lucene: the same inverted-index DNA
(BM25, FST term dictionaries, query automata), but in-process and
RAM-only.

- **Per-field indexes (Phase 3.5)**: each partition keeps one tantivy
  index per *document field name*, all in RAM directories and owned
  exclusively by the worker thread. This is what makes MongoDB-style
  field weights possible: every field has its own BM25 corpus and
  statistics, and the worker sums `bm25(field) * boost` per document
  when merging (fields default to weight 1, like a MongoDB text
  index). A cap of 8 field names per partition bounds memory (each
  tantivy writer owns a >= 15 MB arena); fields beyond it are skipped
  with a logged warning.
- **Analyzer**: the `[search]` section of `kova.toml` picks the token
  pipeline (`default` or `en_stem`) and optional stop words. It is
  registered at index creation and applied at index *and* query time,
  mirroring how a MongoDB text index fixes its tokenizer.
- **Query surface**: MongoDB's native `$text` operator, plus two Kova
  extensions (`$boost`, `$decay`) that log a WARN when
  `warn_on_non_mongo_queries` is enabled:
  `{ $text: { $search: "...", $boost: {title: 10},
              $decay: {field: "created_at", halfLifeSeconds: 86400} } }`.
  Extra predicates in the same filter are applied as an equality
  post-filter over the hits.
- **Wildcards**: tokens containing `*`/`?` expand to whole-term
  automaton queries (Solr semantics: `dat*` matches `database`, `*sta`
  does not match inside `rust`).
- **Fuzzy fallback**: when the exact query scores zero hits, the
  search retries with a distance-1 fuzzy query so a one-character
  typo still matches.
- **Decay re-rank**: `$decay` multiplies each hit's score by
  `2^(-age/half_life)` computed from a BSON date field in the worker;
  documents without a usable date keep their raw BM25 score.
- **BSON boundary**: `src/search/mod.rs` never reads raw BSON — the
  storage layer hands it `(field, text)` pairs and receives
  `(id, score)` hits, per `docs/CONTRACT.md` §4.
- **Consistency**: indexing is hot and incremental; insert, update,
  delete and drop keep the index consistent with the transactional
  map (searches commit and reload before querying).

## 9. Configuration

`kova.toml` (working directory, or the path in `KOVA_CONFIG`) holds
the engine configuration. Precedence: **environment variable > file >
built-in default**. A malformed file aborts startup; a malformed env
value is warned about and ignored.

| Key | Env override | Default | Meaning |
|---|---|---|---|
| `port` | `KOVA_PORT` | `27017` | TCP listener port |
| `log_level` | `KOVA_LOG_LEVEL` | `info` | Minimum emitted level |
| `warn_on_non_mongo_queries` | `KOVA_WARN_NON_MONGO_QUERIES` | `true` | WARN on non-Mongo operators |
| `search.analyzer` | — | `default` | `default` \| `en_stem` |
| `search.stopwords` | — | `[]` | Stop words removed at index/query time |

## 10. Phases

| Phase | Scope |
|-------|-------|
| 1 | TCP listener, header parsing, OP_MSG detection, concurrent tasks |
| 2 | Partition isolation, `PartitionCommand`, `crossbeam-channel` routing |
| 3 | Tantivy full-text indexing (per-partition, hot, `$text`) |
| 3.5 | Per-field boosts, wildcards, decay, analyzer config, `kova.toml` |
| 4 | DataFusion query optimization |

---

*Generated from the brainstorming session based on `draft.md`.*
