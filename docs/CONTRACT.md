# Kova — Development Contract

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
| `src/network.rs` | TCP listener, Wire Protocol header parsing, client task spawning |
| `src/protocol.rs` | Message types, opcodes, payload parsing, BSON |
| `src/types.rs` | `PartitionCommand`, document model, query model, shared types |
| `src/storage.rs` | Partition worker, in-memory storage, channel routing |
| `src/search.rs` | Tantivy indexes, tokenizers, relevance queries |
| `src/query_engine.rs` | DataFusion integration, SQL parser, optimizer |
| `src/logger.rs` *(novo)* | Structured logging, log capture, external sinks |
| `tests/` | Integration tests for network, storage, search |
| `tests/unit/` *(novo)* | Unit tests for parsing, types, channel routing |
| `tests/integration/` *(novo)* | Integration tests for end-to-end flows |

## 4. Module Boundaries

- **network.rs** MUST NOT access shard state directly. It sends
  `PartitionCommand` through `crossbeam::channel::Sender`.
- **storage.rs** MUST NOT open TCP connections or parse binary protocols.
- **search.rs** MUST NOT read/write raw BSON. It consumes structured types
  from `types.rs` only.
- Mutating a partition MUST never panic. Errors are returned as
  `Result` and logged at the caller boundary.

## 5. Naming

| Category | Convention |
|---|---|
| Crate / package | `kova` |
| Types / structs | `PascalCase` |
| Functions / methods | `snake_case` |
| Constants | `SCREAMING_SNAKE_CASE` |
| Environment overrides | `SCREAMING_SNAKE_CASE` (e.g., `KOVA_PORT`) |

## 6. Error Handling

- Recoverable errors at the boundary return `Result<T, E>`.
- Connection-level failures (abrupt socket close) log `Error` with the
  peer address.
- Unrecoverable engine failures abort with a message to `stderr` and a
  non-zero exit code.

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
| `Debug` | Internal state, parsing details, hot-path diagnostics |
| `Info` | Startup, shutdown, connection events, partition routing |
| `Warn` | Recoverable anomalies (duplicate key, invalid payload) |
| `Error` | Unrecoverable failures (connection drop, storage crash) |

## 8. Documentation

- Every `pub` item MUST have a doc comment explaining `what` and `why`.
- Every module MUST have a module-level doc comment explaining its
  responsibility and interactions.
- Unexposed items are documented only when they carry non-obvious
  invariants (byte layout, ownership, thread guarantees).

## 9. Performance

- Hot paths (header parsing, serialization) MUST avoid heap allocations
  where possible.
- `BytesMut` buffers MUST be sized to the expected maximum message size.
- Partition reads MUST be lock-free via `crossbeam::channel`.

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
| 3 | Tantivy full-text indexing |
| 4 | DataFusion query optimization |

## 12. Test Strategy

- **Unit tests** belong in `tests/unit/` and test pure logic: header
  parsing, BSON payload, `PartitionCommand` routing, log capture.
- **Integration tests** belong in `tests/integration/` and test end-to-end
  flows: network handshake, storage reads/writes, search queries.
- **All tests must be deterministic** and MUST NOT depend on wall-clock
  time, random values, or external services.
- **No test may pass by accident** — failing assertions MUST fail the
  test suite.
- **All tests run with `cargo test`** (no separate test binary required).
- **Test coverage metric** tracked in CI: target is 80% for all modules.

## 13. Verification Checklist

- [ ] `cargo fmt --check` passes
- [ ] `cargo clippy --all-targets` reports no warnings
- [ ] `cargo test` passes
- [ ] Public API documented with doc comments
- [ ] No natural-language strings or comments in source code
- [ ] New modules have unit tests in `tests/unit/`
- [ ] New behaviours have integration tests in `tests/integration/`

---

*Generated from the brainstorming session based on `draft.md`.*
