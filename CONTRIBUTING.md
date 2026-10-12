# Kova — Contributing Guide

> "This is the single source of truth for how the code must be written
> in the Kova project. Read it before opening any file."

---

## 1. Project Purpose

Kova is an ultra-high-performance, in-memory database engine written in
Rust. It presents a native MongoDB Wire Protocol interface on port 27018,
processes data entirely in RAM, and unifies transactional writes, full-text
search (tantivy), and analytical query planning (Apache DataFusion) in a
single process.

## 2. Repository Layout

| Path | Responsibility |
|---|---|
| `src/main.rs` | Application entry point, Tokio runtime bootstrap, engine orchestration |
| `src/network.rs` | TCP server, MongoDB Wire Protocol header parsing, client task spawning |
| `src/protocol.rs` | Message types, opcodes, payload parsing, BSON enumerations |
| `src/types.rs` | Partition command enum, document model, query model, shared types |
| `src/storage.rs` | Partition worker, in-memory storage, `crossbeam-channel` routing |
| `src/search.rs` | Tantivy indexes, tokenizers, relevance queries |
| `src/query_engine.rs` | DataFusion integration, SQL parser, query optimizer |
| `docs/` | Design specs, architecture decision records, roadmap |
| `tests/` | Integration tests for network, storage, search |

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

### 3.3 Naming

| Category | Convention |
|---|---|
| Crate / package | `kova` |
| Types / structs | `PascalCase` (e.g., `PartitionWorker`, `MessageHeader`) |
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

### 3.5 Logging

- Use `eprintln!` with the `[Kova]` prefix for all engine diagnostics:
  - `[Kova] Servidor de rede iniciado na porta 27018` →
    `[Kova] Network server started on port 27018`
  - `[Kova] Conexão recebida de 127.0.0.1:45678` →
    `[Kova] Connection received from 127.0.0.1:45678`
- Production logs MUST be written to `stderr` only. File/syslog logging
  will be added in a future phase.

### 3.6 Documentation

- Every public item MUST have a doc comment explaining `what` and `why`.
- Every module MUST have a module-level doc comment explaining its
  responsibility and interactions with other modules.
- Unexposed (private) items SHOULD be documented only when they carry
  non-obvious invariants (e.g., byte layout, ownership rules).

### 3.7 Performance

- Hot paths (header parsing, document serialization) MUST avoid heap
  allocations where possible.
- `BytesMut` buffers MUST be sized to the expected maximum message size.
- Partition reads MUST be lock-free via `crossbeam::channel`.

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
  concurrent task spawning. (Current state)
- **Phase 2 — Partitioning:** `PartitionCommand`, `crossbeam-channel` routing,
  single-threaded partition workers.
- **Phase 3 — Search indexing:** `tantivy` integration with hot indexing.
- **Phase 4 — Query planning:** DataFusion integration.

---

*Generated from the brainstorming session based on `draft.md`.*
