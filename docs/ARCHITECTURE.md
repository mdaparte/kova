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

- Each partition owns a disjoint set of keys (hashed by document ID).
- Commands route to the owning partition deterministically.
- Partition workers are single-threaded and process commands sequentially.
- No cross-partition locks are required in the hot path.

## 8. Search Subsystem

- Tantivy inverted indexes are built per partition, each in a RAM
  directory (no disk I/O) and owned exclusively by its worker thread.
- Text fields are tokenized into terms; BM25 relevance scoring is used.
- Indexing is a hot, incremental process: as a document arrives, its text
  fields are indexed immediately. Update, delete and drop keep the index
  consistent with the transactional map.
- The query surface is MongoDB's native `$text` operator
  (`{ $text: { $search: "..." } }`); extra predicates in the same
  filter are applied as an equality post-filter over the hits.
- When the exact query scores zero hits, the search retries with a
  fuzzy (distance-1) query so a one-character typo still matches.
- `src/search/mod.rs` never reads raw BSON: the storage layer hands it
  plain strings and receives document ids, per `docs/CONTRACT.md` §4.

## 9. Phases

| Phase | Scope |
|-------|-------|
| 1 | TCP listener, header parsing, OP_MSG detection, concurrent tasks |
| 2 | Partition isolation, `PartitionCommand`, `crossbeam-channel` routing |
| 3 | Tantivy full-text indexing |
| 4 | DataFusion query optimization |

---

*Generated from the brainstorming session based on `draft.md`.*
