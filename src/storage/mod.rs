//! In-memory partition workers and command routing.
//!
//! This module implements the lock-free storage side of the engine.
//! Each partition is a **single-threaded** worker (`std::thread`) that
//! owns its own hash map of collections. Network tasks never touch
//! storage memory directly: they send a [`CommandEnvelope`] through a
//! `crossbeam::channel` and await the reply on a `oneshot`.
//!
//! Because a partition is confined to one thread, no `Mutex` or
//! `RwLock` is required on the data structures — this is the core of
//! the thread-per-core design described in `docs/ARCHITECTURE.md`.

use std::collections::HashMap;
use std::sync::Arc;
use std::thread::JoinHandle;

use bson::{doc, Document};
use crossbeam_channel::{unbounded, Receiver, Sender};
use tokio::sync::oneshot;

use crate::logger::LOGGER;
use crate::search::TextIndex;
use crate::types::{CommandEnvelope, PartitionCommand, PartitionResult};

/// A collection of documents stored by a single partition.
type Collection = Vec<Document>;

/// A single-threaded storage worker.
///
/// The worker runs on its own OS thread and processes commands
/// sequentially from its channel. It owns every collection in its
/// partition exclusively, so no synchronization primitives are needed.
pub struct PartitionWorker {
    id: usize,
    collections: HashMap<String, Collection>,
    receiver: Receiver<CommandEnvelope>,
    /// Full-text index mirroring this partition's documents.
    ///
    /// `None` only when tantivy fails to allocate its writer (e.g. out
    /// of memory): the partition keeps serving indexed lookups by
    /// equality while `$text` searches report an error instead of
    /// panicking the worker.
    index: Option<TextIndex>,
}

impl PartitionWorker {
    /// Creates a new worker with the given id and command receiver.
    fn new(id: usize, receiver: Receiver<CommandEnvelope>) -> Self {
        let index = match TextIndex::new() {
            Ok(index) => Some(index),
            Err(err) => {
                LOGGER.error(format!(
                    "Partition {}: full-text index unavailable: {}",
                    id, err
                ));
                None
            }
        };

        Self {
            id,
            collections: HashMap::new(),
            receiver,
            index,
        }
    }

    /// Builds the search index entry for one document and adds it.
    ///
    /// Indexing failures are logged, never propagated: a document that
    /// cannot be tokenized must still be stored and returned by normal
    /// queries.
    fn index_document(&mut self, collection: &str, document: &Document) {
        let id = document_id_string(document);
        let text = extract_text(document);
        if let Some(index) = self.index.as_mut() {
            if let Err(err) = index.add(collection, &id, &text) {
                LOGGER.warn(format!("Failed to index document {}: {}", id, err));
            }
        }
    }

    /// Removes one document from the search index (best effort).
    fn unindex_document(&mut self, collection: &str, document: &Document) {
        let id = document_id_string(document);
        if let Some(index) = self.index.as_mut() {
            if let Err(err) = index.remove(collection, &id) {
                LOGGER.warn(format!("Failed to unindex document {}: {}", id, err));
            }
        }
    }

    /// Runs the worker loop until every sender has been dropped.
    ///
    /// Returns when the channel is disconnected, which happens when
    /// the router is dropped during shutdown.
    fn run(&mut self) {
        while let Ok(envelope) = self.receiver.recv() {
            let result = self.execute(envelope.command);
            // The network task may have gone away (client disconnected);
            // that is not an error for the worker.
            let _ = envelope.respond_to.send(result);
        }

        LOGGER.info(format!(
            "Partition worker {} shutting down ({} collections)",
            self.id,
            self.collections.len()
        ));
    }

    /// Executes a command against this partition's data.
    fn execute(&mut self, command: PartitionCommand) -> PartitionResult {
        match command {
            PartitionCommand::Insert {
                collection,
                documents,
            } => {
                let inserted_count = documents.len() as u32;
                for document in &documents {
                    self.index_document(&collection, document);
                }
                let docs = self.collections.entry(collection).or_default();
                docs.extend(documents);
                PartitionResult::Inserted { inserted_count }
            }

            PartitionCommand::Find { collection, filter } => {
                let Some(docs) = self.collections.get(&collection) else {
                    return PartitionResult::Found {
                        documents: Vec::new(),
                    };
                };
                let matches = docs
                    .iter()
                    .filter(|doc| document_matches(doc, &filter))
                    .cloned()
                    .collect();
                PartitionResult::Found { documents: matches }
            }

            PartitionCommand::Count { collection, filter } => {
                let count = self
                    .collections
                    .get(&collection)
                    .map(|docs| {
                        docs.iter()
                            .filter(|doc| document_matches(doc, &filter))
                            .count()
                    })
                    .unwrap_or(0);
                PartitionResult::Counted {
                    count: count as u64,
                }
            }

            PartitionCommand::Update {
                collection,
                filter,
                update,
                multi,
            } => {
                let Some(docs) = self.collections.get_mut(&collection) else {
                    return PartitionResult::Modified {
                        matched_count: 0,
                        modified_count: 0,
                    };
                };

                let mut matched_count = 0u32;
                let mut modified_count = 0u32;
                // The index needs the pre-update id to remove the old
                // entry, so collect it while iterating.
                let mut updated_ids: Vec<String> = Vec::new();
                for doc in docs.iter_mut() {
                    if document_matches(doc, &filter) {
                        matched_count += 1;
                        let id = document_id_string(doc);
                        apply_update(doc, &update);
                        updated_ids.push(id);
                        modified_count += 1;
                        if !multi {
                            break;
                        }
                    }
                }
                drop(docs);

                // Re-index the modified documents so search reflects
                // the new text (`_id` survives updates). The documents
                // are cloned out of the map so the index methods can
                // borrow `self` mutably.
                let updated_docs: Vec<Document> = self
                    .collections
                    .get(&collection)
                    .map(|docs| {
                        updated_ids
                            .iter()
                            .filter_map(|id| {
                                docs.iter()
                                    .find(|doc| document_id_string(doc) == *id)
                                    .cloned()
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                for doc in &updated_docs {
                    self.unindex_document(&collection, doc);
                    self.index_document(&collection, doc);
                }

                PartitionResult::Modified {
                    matched_count,
                    modified_count,
                }
            }

            PartitionCommand::Drop { collection } => {
                self.collections.remove(&collection);
                if let Some(index) = self.index.as_mut() {
                    if let Err(err) = index.remove_collection(&collection) {
                        LOGGER.warn(format!(
                            "Failed to drop collection {} from index: {}",
                            collection, err
                        ));
                    }
                }
                PartitionResult::Dropped { collection }
            }

            PartitionCommand::ListCollections { filter } => {
                // One entry per collection this partition owns, shaped
                // like a real MongoDB listCollections document (the
                // server-generated `uuid` is omitted). The filter uses
                // the same equality matcher as every other command.
                let mut collections: Vec<Document> = self
                    .collections
                    .keys()
                    .map(|name| {
                        doc! {
                            "name": name,
                            "type": "collection",
                            "options": {},
                            "info": { "readOnly": false },
                            "idIndex": {
                                "v": 2i32,
                                "key": { "_id": 1i32 },
                                "name": "_id_",
                            },
                        }
                    })
                    .filter(|info| document_matches(info, &filter))
                    .collect();
                // Sorting keeps the merged reply stable regardless of
                // which worker answers first.
                collections.sort_by(|a, b| {
                    a.get_str("name")
                        .unwrap_or("")
                        .cmp(b.get_str("name").unwrap_or(""))
                });
                PartitionResult::ListedCollections { collections }
            }

            PartitionCommand::Search {
                collection,
                query,
                filter,
            } => {
                let Some(index) = self.index.as_mut() else {
                    return PartitionResult::Error {
                        message: "full-text search is unavailable".into(),
                    };
                };

                let hits = match index.search(&collection, &query) {
                    Ok(hits) => hits,
                    Err(err) => return PartitionResult::Error { message: err },
                };

                // The index stores ids, not documents: look each hit up
                // in the map and apply any non-`$text` predicates the
                // client also sent.
                let Some(docs) = self.collections.get(&collection) else {
                    return PartitionResult::Found {
                        documents: Vec::new(),
                    };
                };

                let mut documents = Vec::with_capacity(hits.len());
                for hit in hits {
                    if let Some(doc) = docs.iter().find(|doc| document_id_string(doc) == hit.id) {
                        if document_matches(doc, &filter) {
                            documents.push(doc.clone());
                        }
                    }
                }
                PartitionResult::Found { documents }
            }

            PartitionCommand::Delete {
                collection,
                filter,
                limit,
            } => {
                let Some(docs) = self.collections.get_mut(&collection) else {
                    return PartitionResult::Deleted { deleted_count: 0 };
                };

                // Snapshot the documents about to be removed so they
                // can be unindexed after the borrow ends.
                let mut removed_docs: Vec<Document> = Vec::new();
                let before = docs.len();
                if limit == 0 {
                    // deleteMany: drop every match.
                    docs.retain(|doc| {
                        let matches = document_matches(doc, &filter);
                        if matches {
                            removed_docs.push(doc.clone());
                        }
                        !matches
                    });
                } else {
                    // deleteOne (or a bounded delete): stop after `limit`.
                    let mut removed = 0u32;
                    docs.retain(|doc| {
                        let matches = document_matches(doc, &filter) && removed < limit;
                        if matches {
                            removed_docs.push(doc.clone());
                            removed += 1;
                        }
                        !matches
                    });
                }
                let deleted_count = (before - docs.len()) as u32;
                drop(docs);

                for doc in &removed_docs {
                    self.unindex_document(&collection, doc);
                }

                PartitionResult::Deleted { deleted_count }
            }
        }
    }
}

/// Returns the storage-side id of a document as a string.
///
/// Uses the `_id` field when present (MongoDB guarantees it is unique
/// within a collection). Documents inserted through internals that
/// skipped `_id` fall back to a content hash, which keeps the id
/// stable across index rebuilds.
fn document_id_string(doc: &Document) -> String {
    use bson::Bson;

    match doc.get("_id") {
        Some(Bson::String(s)) => s.clone(),
        Some(Bson::ObjectId(oid)) => oid.to_hex(),
        Some(Bson::Int32(n)) => n.to_string(),
        Some(Bson::Int64(n)) => n.to_string(),
        _ => {
            let bytes = bson::to_vec(doc).unwrap_or_default();
            format!("h{:016x}", fnv1a(&bytes))
        }
    }
}

/// Concatenates every string value in a document into one blob of text.
///
/// This is what the search index tokenizes: nested documents and arrays
/// are walked so fields at any depth become searchable. Non-string
/// values are skipped — BM25 over numbers adds noise, not signal.
fn extract_text(doc: &Document) -> String {
    fn walk(value: &bson::Bson, out: &mut String) {
        use bson::Bson;
        match value {
            Bson::String(s) => {
                if !out.is_empty() {
                    out.push(' ');
                }
                out.push_str(s);
            }
            Bson::Array(values) => {
                for value in values {
                    walk(value, out);
                }
            }
            Bson::Document(nested) => {
                for value in nested.values() {
                    walk(value, out);
                }
            }
            _ => {}
        }
    }

    let mut out = String::new();
    for value in doc.values() {
        walk(value, &mut out);
    }
    out
}

/// Returns `true` when every field in `filter` is present and equal in `doc`.
///
/// This is an equality-only matcher: it covers the subset of the MongoDB
/// query language needed to route and verify Phase 2 commands. Rich
/// operators (`$gt`, `$in`, ...) arrive with the query engine in Phase 4.
fn document_matches(doc: &Document, filter: &Document) -> bool {
    filter
        .iter()
        .all(|(key, expected)| doc.get(key) == Some(expected))
}

/// Applies an update document to `doc`.
///
/// Only the implicit replacement form and the `$set` operator are
/// supported in Phase 2. Unknown operators leave the document unchanged
/// and are reported by the caller as a no-op modification.
fn apply_update(doc: &mut Document, update: &Document) {
    if let Ok(set) = update.get_document("$set") {
        for (key, value) in set {
            doc.insert(key, value.clone());
        }
        return;
    }

    // Replacement form: the update document becomes the new document,
    // preserving the immutable `_id` field.
    let id = doc.get("_id").cloned();
    *doc = update.clone();
    if let Some(id) = id {
        doc.insert("_id", id);
    }
}

/// Routes commands to partition workers by hashing the collection name.
///
/// All commands for the same collection always reach the same worker,
/// which keeps reads and writes for a collection linearized without
/// cross-partition locks.
///
/// The router is cheap to clone: clones share the same senders through
/// an `Arc`, so every Tokio client task can hold its own handle while
/// the worker threads stay owned by the original instance.
#[derive(Clone)]
pub struct PartitionRouter {
    inner: Arc<RouterInner>,
}

struct RouterInner {
    senders: Vec<Sender<CommandEnvelope>>,
    handles: std::sync::Mutex<Vec<JoinHandle<()>>>,
}

impl PartitionRouter {
    /// Spawns `partition_count` workers on dedicated OS threads.
    pub fn new(partition_count: usize) -> Self {
        assert!(partition_count > 0, "at least one partition is required");

        let mut senders = Vec::with_capacity(partition_count);
        let mut handles = Vec::with_capacity(partition_count);

        for id in 0..partition_count {
            let (tx, rx) = unbounded();
            senders.push(tx);

            let handle = std::thread::Builder::new()
                .name(format!("kova-partition-{}", id))
                .spawn(move || {
                    let mut worker = PartitionWorker::new(id, rx);
                    worker.run();
                })
                .expect("failed to spawn partition worker thread");

            handles.push(handle);
        }

        LOGGER.info(format!(
            "Storage router started with {} partition worker(s)",
            partition_count
        ));

        Self {
            inner: Arc::new(RouterInner {
                senders,
                handles: std::sync::Mutex::new(handles),
            }),
        }
    }

    /// Sends a command to its owning partition and returns the reply channel.
    ///
    /// The caller awaits the returned receiver to get the
    /// [`PartitionResult`]. Returns `None` when the worker is gone,
    /// which only happens during shutdown.
    pub fn dispatch(
        &self,
        command: PartitionCommand,
    ) -> Option<oneshot::Receiver<PartitionResult>> {
        let (tx, rx) = oneshot::channel();
        let index = self.partition_index(command.collection());

        let envelope = CommandEnvelope {
            command,
            respond_to: tx,
        };

        match self.inner.senders[index].send(envelope) {
            Ok(()) => Some(rx),
            Err(_) => {
                LOGGER.error(format!("Partition worker {} is not reachable", index));
                None
            }
        }
    }

    /// Sends a command to **every** partition and returns one reply
    /// channel per worker.
    ///
    /// Used by commands that must see all shards of the data set
    /// (`listCollections`), since collection names hash to different
    /// workers. The caller awaits each receiver and merges the results.
    pub fn broadcast(&self, command: PartitionCommand) -> Vec<oneshot::Receiver<PartitionResult>> {
        self.inner
            .senders
            .iter()
            .map(|sender| {
                let (tx, rx) = oneshot::channel();
                let envelope = CommandEnvelope {
                    command: command.clone(),
                    respond_to: tx,
                };
                if sender.send(envelope).is_err() {
                    LOGGER.error("Partition worker is not reachable during broadcast");
                }
                rx
            })
            .collect()
    }

    /// Computes the partition index for a collection name.
    ///
    /// Uses a stable FNV-1a hash so routing is deterministic across
    /// runs (the standard `Hasher` is explicitly not guaranteed to be
    /// stable between processes or Rust versions).
    fn partition_index(&self, collection: &str) -> usize {
        let hash = fnv1a(collection.as_bytes());
        (hash % self.inner.senders.len() as u64) as usize
    }

    /// Waits for every worker thread to finish (used in tests/shutdown).
    ///
    /// No-op while other clones of this router still exist, because the
    /// shared `Arc` keeps the workers alive.
    pub fn join(&self) {
        if let Ok(mut handles) = self.inner.handles.lock() {
            for handle in handles.drain(..) {
                let _ = handle.join();
            }
        }
    }
}

impl Drop for RouterInner {
    fn drop(&mut self) {
        // Dropping the senders disconnects every worker, which makes
        // their `recv()` loops return and the threads exit cleanly.
        self.senders.clear();
        if let Ok(mut handles) = self.handles.lock() {
            for handle in handles.drain(..) {
                let _ = handle.join();
            }
        }
    }
}

/// FNV-1a 64-bit hash — stable, fast, and dependency-free.
fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = OFFSET_BASIS;
    for &byte in bytes {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn router() -> PartitionRouter {
        PartitionRouter::new(2)
    }

    #[tokio::test]
    async fn test_insert_then_find_returns_document() {
        let router = router();

        let rx = router
            .dispatch(PartitionCommand::Insert {
                collection: "users".into(),
                documents: vec![doc! { "name": "Ada", "age": 36 }],
            })
            .expect("worker reachable");
        let result = rx.await.expect("worker replied");
        assert!(matches!(
            result,
            PartitionResult::Inserted { inserted_count: 1 }
        ));

        let rx = router
            .dispatch(PartitionCommand::Find {
                collection: "users".into(),
                filter: doc! { "name": "Ada" },
            })
            .expect("worker reachable");
        let result = rx.await.expect("worker replied");

        match result {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 1);
                assert_eq!(documents[0].get_str("name").unwrap(), "Ada");
            }
            other => panic!("expected Found, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_find_with_non_matching_filter_returns_empty() {
        let router = router();

        let rx = router
            .dispatch(PartitionCommand::Find {
                collection: "empty".into(),
                filter: doc! { "name": "nobody" },
            })
            .expect("worker reachable");
        let result = rx.await.expect("worker replied");

        match result {
            PartitionResult::Found { documents } => assert!(documents.is_empty()),
            other => panic!("expected Found, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_update_set_modifies_document() {
        let router = router();

        router
            .dispatch(PartitionCommand::Insert {
                collection: "users".into(),
                documents: vec![doc! { "name": "Ada", "age": 36 }],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let rx = router
            .dispatch(PartitionCommand::Update {
                collection: "users".into(),
                filter: doc! { "name": "Ada" },
                update: doc! { "$set": { "age": 37 } },
                multi: false,
            })
            .expect("worker reachable");
        let result = rx.await.expect("worker replied");

        assert!(
            matches!(
                result,
                PartitionResult::Modified {
                    matched_count: 1,
                    modified_count: 1
                }
            ),
            "expected one modified document, got {:?}",
            result
        );

        let rx = router
            .dispatch(PartitionCommand::Find {
                collection: "users".into(),
                filter: doc! { "age": 37 },
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Found { documents } => assert_eq!(documents.len(), 1),
            other => panic!("expected Found, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_delete_removes_matching_documents() {
        let router = router();

        router
            .dispatch(PartitionCommand::Insert {
                collection: "todos".into(),
                documents: vec![doc! { "task": "write tests", "done": false }],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        router
            .dispatch(PartitionCommand::Insert {
                collection: "todos".into(),
                documents: vec![doc! { "task": "ship phase 2", "done": false }],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let rx = router
            .dispatch(PartitionCommand::Delete {
                collection: "todos".into(),
                filter: doc! { "task": "write tests" },
                limit: 0,
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Deleted { deleted_count } => assert_eq!(deleted_count, 1),
            other => panic!("expected Deleted, got {:?}", other),
        }

        let rx = router
            .dispatch(PartitionCommand::Count {
                collection: "todos".into(),
                filter: doc! {},
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Counted { count } => {
                assert_eq!(count, 1);
            }
            other => panic!("expected Counted, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_search_returns_documents_by_relevance() {
        let router = router();

        router
            .dispatch(PartitionCommand::Insert {
                collection: "articles".into(),
                documents: vec![
                    doc! { "_id": "a", "title": "Rust for database engines" },
                    doc! { "_id": "b", "title": "Cooking pasta" },
                ],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let rx = router
            .dispatch(PartitionCommand::Search {
                collection: "articles".into(),
                query: "database".into(),
                filter: doc! {},
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 1);
                assert_eq!(documents[0].get_str("_id").unwrap(), "a");
            }
            other => panic!("expected Found, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_search_applies_post_filter() {
        let router = router();
        router
            .dispatch(PartitionCommand::Insert {
                collection: "posts".into(),
                documents: vec![
                    doc! { "_id": 1, "tag": "rust", "body": "rust is fast" },
                    doc! { "_id": 2, "tag": "cooking", "body": "rust of the pan" },
                ],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let rx = router
            .dispatch(PartitionCommand::Search {
                collection: "posts".into(),
                query: "rust".into(),
                filter: doc! { "tag": "rust" },
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 1, "post filter must drop the pan");
                assert_eq!(documents[0].get_i32("_id").unwrap(), 1);
            }
            other => panic!("expected Found, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_search_reflects_update_and_delete() {
        let router = router();
        router
            .dispatch(PartitionCommand::Insert {
                collection: "notes".into(),
                documents: vec![doc! { "_id": "n1", "text": "alpha" }],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        // The word changes; the old one must stop matching.
        router
            .dispatch(PartitionCommand::Update {
                collection: "notes".into(),
                filter: doc! { "_id": "n1" },
                update: doc! { "$set": { "text": "omega" } },
                multi: false,
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let search = |query: String| {
            let router = router.clone();
            async move {
                let rx = router
                    .dispatch(PartitionCommand::Search {
                        collection: "notes".into(),
                        query,
                        filter: doc! {},
                    })
                    .expect("worker reachable");
                match rx.await.expect("worker replied") {
                    PartitionResult::Found { documents } => documents,
                    other => panic!("expected Found, got {:?}", other),
                }
            }
        };

        assert!(search("alpha".into()).await.is_empty(), "stale text");
        assert_eq!(search("omega".into()).await.len(), 1, "fresh text");

        // Deleting the document must hide it too.
        router
            .dispatch(PartitionCommand::Delete {
                collection: "notes".into(),
                filter: doc! { "_id": "n1" },
                limit: 0,
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        assert!(search("omega".into()).await.is_empty(), "deleted doc");
    }

    #[tokio::test]
    async fn test_drop_collection_clears_index() {
        let router = router();
        router
            .dispatch(PartitionCommand::Insert {
                collection: "tmp".into(),
                documents: vec![doc! { "_id": "t", "text": "vanish" }],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        router
            .dispatch(PartitionCommand::Drop {
                collection: "tmp".into(),
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        // Re-creating the collection must not resurrect old hits.
        let rx = router
            .dispatch(PartitionCommand::Search {
                collection: "tmp".into(),
                query: "vanish".into(),
                filter: doc! {},
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Found { documents } => {
                assert!(documents.is_empty(), "dropped doc leaked: {:?}", documents);
            }
            other => panic!("expected Found, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_list_collections_broadcast_merges_all_partitions() {
        let router = router();

        // "alpha" and "zeta" hash to different partitions, so only a
        // broadcast can see both.
        for name in ["alpha", "zeta"] {
            router
                .dispatch(PartitionCommand::Insert {
                    collection: name.into(),
                    documents: vec![doc! { "n": 1 }],
                })
                .expect("worker reachable")
                .await
                .expect("worker replied");
        }

        let mut names = Vec::new();
        for rx in router.broadcast(PartitionCommand::ListCollections { filter: doc! {} }) {
            match rx.await.expect("worker replied") {
                PartitionResult::ListedCollections { collections } => {
                    names.extend(
                        collections
                            .into_iter()
                            .filter_map(|info| info.get_str("name").ok().map(String::from)),
                    );
                }
                other => panic!("expected ListedCollections, got {:?}", other),
            }
        }

        names.sort();
        assert_eq!(names, vec!["alpha", "zeta"]);
    }

    #[tokio::test]
    async fn test_list_collections_honors_name_filter() {
        let router = router();
        router
            .dispatch(PartitionCommand::Insert {
                collection: "users".into(),
                documents: vec![doc! { "n": 1 }],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let mut matched = Vec::new();
        for rx in router.broadcast(PartitionCommand::ListCollections {
            filter: doc! { "name": "users" },
        }) {
            match rx.await.expect("worker replied") {
                PartitionResult::ListedCollections { collections } => {
                    matched.extend(collections);
                }
                other => panic!("expected ListedCollections, got {:?}", other),
            }
        }

        assert_eq!(matched.len(), 1, "filter must match exactly one");
        assert_eq!(matched[0].get_str("name").unwrap(), "users");
        assert_eq!(matched[0].get_str("type").unwrap(), "collection");
    }

    #[tokio::test]
    async fn test_same_collection_routes_to_same_partition() {
        let router = router();

        // Insert into "kova" several times; if routing were unstable the
        // documents would be scattered and Count would still find them
        // only on one partition, but a mismatch would surface as a
        // missing document after a Find from another partition.
        for i in 0..5 {
            router
                .dispatch(PartitionCommand::Insert {
                    collection: "kova".into(),
                    documents: vec![doc! { "seq": i }],
                })
                .expect("worker reachable")
                .await
                .expect("worker replied");
        }

        let rx = router
            .dispatch(PartitionCommand::Count {
                collection: "kova".into(),
                filter: doc! {},
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Counted { count } => {
                assert_eq!(count, 5);
            }
            other => panic!("expected Counted, got {:?}", other),
        }
    }

    #[test]
    fn test_partition_index_is_stable_for_same_collection() {
        let router = router();
        let first = router.partition_index("users");
        let second = router.partition_index("users");
        assert_eq!(first, second);
    }

    #[test]
    fn test_partition_index_within_bounds() {
        let router = PartitionRouter::new(4);
        for name in ["users", "orders", "products", "logs", ""] {
            let index = router.partition_index(name);
            assert!(index < 4, "index {} out of bounds for {}", index, name);
        }
    }

    #[test]
    fn test_fnv1a_known_values() {
        // FNV-1a 64 reference vectors.
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
    }

    #[test]
    fn test_document_matches_equality() {
        let doc = doc! { "name": "Ada", "age": 36 };
        assert!(document_matches(&doc, &doc! { "name": "Ada" }));
        assert!(document_matches(&doc, &doc! { "name": "Ada", "age": 36 }));
        assert!(!document_matches(&doc, &doc! { "name": "Grace" }));
        assert!(!document_matches(&doc, &doc! { "missing": 1 }));
        assert!(document_matches(&doc, &doc! {}));
    }

    #[test]
    fn test_apply_update_set_operator() {
        let mut doc = doc! { "name": "Ada", "age": 36 };
        apply_update(&mut doc, &doc! { "$set": { "age": 37 } });

        assert_eq!(doc.get_i32("age").unwrap(), 37);
        assert_eq!(doc.get_str("name").unwrap(), "Ada");
    }

    #[test]
    fn test_apply_update_replacement_preserves_id() {
        let mut doc = doc! { "_id": 1, "name": "Ada", "age": 36 };
        apply_update(&mut doc, &doc! { "name": "Grace" });

        assert_eq!(doc.get_i32("_id").unwrap(), 1);
        assert_eq!(doc.get_str("name").unwrap(), "Grace");
        assert!(doc.get("age").is_none(), "replacement drops old fields");
    }
}
