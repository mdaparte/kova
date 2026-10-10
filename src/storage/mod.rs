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
}

impl PartitionWorker {
    /// Creates a new worker with the given id and command receiver.
    fn new(id: usize, receiver: Receiver<CommandEnvelope>) -> Self {
        Self {
            id,
            collections: HashMap::new(),
            receiver,
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
                document,
            } => {
                let docs = self.collections.entry(collection).or_default();
                docs.push(document);
                PartitionResult::Inserted { inserted_count: 1 }
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
                // Count reuses Found so the caller gets a single shape.
                PartitionResult::Found {
                    documents: vec![doc! { "n": count as i64 }],
                }
            }

            PartitionCommand::Update {
                collection,
                filter,
                update,
            } => {
                let Some(docs) = self.collections.get_mut(&collection) else {
                    return PartitionResult::Modified { modified_count: 0 };
                };

                let mut modified = 0u32;
                for doc in docs.iter_mut() {
                    if document_matches(doc, &filter) {
                        apply_update(doc, &update);
                        modified += 1;
                    }
                }
                PartitionResult::Modified {
                    modified_count: modified,
                }
            }

            PartitionCommand::Delete { collection, filter } => {
                let Some(docs) = self.collections.get_mut(&collection) else {
                    return PartitionResult::Deleted { deleted_count: 0 };
                };

                let before = docs.len();
                docs.retain(|doc| !document_matches(doc, &filter));
                PartitionResult::Deleted {
                    deleted_count: (before - docs.len()) as u32,
                }
            }
        }
    }
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
                document: doc! { "name": "Ada", "age": 36 },
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
                document: doc! { "name": "Ada", "age": 36 },
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let rx = router
            .dispatch(PartitionCommand::Update {
                collection: "users".into(),
                filter: doc! { "name": "Ada" },
                update: doc! { "$set": { "age": 37 } },
            })
            .expect("worker reachable");
        let result = rx.await.expect("worker replied");

        assert!(
            matches!(result, PartitionResult::Modified { modified_count: 1 }),
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
                document: doc! { "task": "write tests", "done": false },
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        router
            .dispatch(PartitionCommand::Insert {
                collection: "todos".into(),
                document: doc! { "task": "ship phase 2", "done": false },
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let rx = router
            .dispatch(PartitionCommand::Delete {
                collection: "todos".into(),
                filter: doc! { "task": "write tests" },
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
            PartitionResult::Found { documents } => {
                assert_eq!(documents[0].get_i64("n").unwrap(), 1);
            }
            other => panic!("expected Found, got {:?}", other),
        }
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
                    document: doc! { "seq": i },
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
            PartitionResult::Found { documents } => {
                assert_eq!(documents[0].get_i64("n").unwrap(), 5);
            }
            other => panic!("expected Found, got {:?}", other),
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
