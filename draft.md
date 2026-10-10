# Kova: Ultra-High Performance In-Memory Database Engine

**Kova** is a cutting-edge systems engineering project built from the ground up in **Rust**. Its vision is to unify three paradigms that traditionally operate separately in infrastructure stacks: the predictable speed and transactional determinism of *NewSQL* in-memory systems, the flexibility of dynamic *NoSQL* document stores (MongoDB), and the analytical power of a full-text relevance search engine (*Lucene*).

---

## 👁️ Project Vision & Purpose (The "Why")

In modern software development, engineering teams are often forced to duplicate their infrastructure. They rely on a transactional database (relational or document-based) for quick updates and maintain a completely separate cluster (such as Elasticsearch or Solr) for advanced text search. This architecture introduces complex data synchronization challenges, network latency, and high operational costs.

**Kova solves this by unifying these capabilities into a single database engine:**
* Processes data at the physical speed of RAM (inspired by VoltDB's architecture).
* Speaks the native wire protocol that developers already love and use (MongoDB).
* Parses and structures internal query execution with a production-grade query optimizer.
* Indexes string content in real-time for instant full-text analytics and relevance matching.

This makes Kova an ideal solution for **Edge Computing/IoT environments**—where resources are limited and extreme local efficiency is mandatory—as well as an **elite technical portfolio project** that showcases mastery over low-level Rust concurrency and memory management.

---

## 🏗️ The Four Core Architectural Pillars

### 1. Lock-Free In-Memory Engine (VoltDB Inspiration)
* **Thread-per-Core:** The database partitions its memory allocation strictly across CPU cores. Each data partition runs on its own isolated, native OS thread (`std::thread`).
* **Local Determinism:** Because a partition is confined to a single thread, it processes incoming transactions sequentially through fast asynchronous channels. This completely eliminates the need for heavy, contention-prone locks (`Mutex` or `RwLock`) on the main data structures. It guarantees microsecond-level latency and avoids Garbage Collection (GC) pauses entirely.

### 2. Native MongoDB Interoperability (The Wire Protocol)
* **Drop-in Compatibility:** Kova exposes a server listening on port `27017` that intercepts native binary messages from MongoDB clients (`OP_MSG`). Any application built using official MongoDB drivers (Node.js, Python, Go, Java, etc.) can connect seamlessly to Kova without altering a single line of business logic.

### 3. Integrated Advanced Search (Lucene Equivalent)
* **Hot Indexing:** As soon as a document enters a memory partition, its text fields are processed through an inverted index engine. This equips Kova with out-of-the-box analytical features like tokenization, relevance scoring, and fuzzy matching directly inside the transactional worker.

### 4. Enterprise-Grade Query Parsing and Optimization (Apache Ecosytem)
* While the external API speaks MongoDB, internal operations require an analytical query planner. Instead of reinventing the wheel by writing a logic optimizer from scratch, Kova integrates **Apache DataFusion** (the modern standard for analytical engines in Rust). This allows the system to parse, optimize, and execute logical query plans over memory partitions with maximum CPU efficiency.

---

## 🛠️ Selected Technology Stack (Rust Ecosystem)

Kova's core is engineered by assembling highly optimized, production-proven crates from the Rust systems ecosystem:

* **`tokio` & `bytes`:** Asynchronous networking layer built to handle thousands of concurrent TCP connections and manage raw binary streams via `BytesMut`.
* **`bson`:** The official MongoDB BSON serialization crate to parse and manipulate dynamic incoming document structures.
* **`crossbeam-channel`:** Ultra-fast multi-producer, single-consumer (MPSC) channels to route data from the network tasks to the single-threaded storage workers.
* **`tantivy`:** The "Lucene of Rust". A blazing fast, memory-optimized full-text search engine library that replaces Elasticsearch weight.
* **`datafusion` (Apache):** An extensible SQL query planner and execution framework used as the internal analytical brain of the database.

---

## 📂 Initial Codebase Structure

The local source code is modularized into dedicated files inside the `kova/src/` directory:

* `main.rs`: Application entry point, thread (worker) initialization, and global engine orchestration.
* `network.rs`: Asynchronous TCP server and binary parser for the *MongoDB Wire Protocol* 16-byte header.
* `storage.rs`: Implementation of the `PartitionWorker`, transactional execution loops, and isolated in-memory maps.
* `search.rs`: Tokenization configurations, inverted index schemas, and relevance queries managed by `tantivy`.
* `query_engine.rs`: Internal SQL parsing, optimization, and abstract syntax tree (AST) evaluation using Apache DataFusion.

---

## 📈 Roadmap for the AI Coding Agent (Phase-by-Phase)

1. **Phase 1 (Networking & Interception):** Spin up the TCP socket, intercept a MongoDB client handshake, and successfully parse the initial 16-byte binary header packet.
2. **Phase 2 (Partition Isolation):** Bridge asynchronous network tasks to the single-threaded native storage worker using a custom `enum PartitionCommand` for lock-free `Insert` and `Find` pipelines.
3. **Phase 3 (Lucene Indexing):** Embed `tantivy` inside the partition thread's lifecycle to automatically build inverted indexes alongside the main in-memory hash maps.
4. **Phase 4 (Apache Query Optimization):** Plug in the query compiler layer to optimize execution plans over multi-partition datasets.
