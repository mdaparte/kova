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
use crate::search::SearchIndex;
use crate::types::{CommandEnvelope, DecaySpec, PartitionCommand, PartitionResult};

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
    /// Field indexes are created lazily on first insert; a failure to
    /// allocate one is logged and that field is skipped, never fatal.
    index: SearchIndex,
    /// Secondary index *declarations* per collection (`createIndexes`).
    ///
    /// Metadata only: it keeps `listIndexes` honest and stable for
    /// drivers that create an index then list it, but nothing is built
    /// from it yet — lookups still scan the in-memory map. Real index
    /// construction is Phase 4.
    declared_indexes: HashMap<String, Vec<Document>>,
}

impl PartitionWorker {
    /// Creates a new worker with the given id and command receiver.
    fn new(id: usize, receiver: Receiver<CommandEnvelope>) -> Self {
        Self {
            id,
            collections: HashMap::new(),
            receiver,
            index: SearchIndex::new(),
            declared_indexes: HashMap::new(),
        }
    }

    /// Builds the search index entry for one document and adds it.
    ///
    /// Each top-level field becomes its own index entry (Phase 3.5),
    /// which is what makes per-field boosts possible. Indexing
    /// failures are logged, never propagated: a document that cannot
    /// be tokenized must still be stored and returned by normal
    /// queries.
    fn index_document(&mut self, collection: &str, document: &Document) {
        let id = document_id_string(document);
        let fields = extract_field_values(document);
        if let Err(err) = self.index.add(collection, &id, &fields) {
            LOGGER.warn(format!("Failed to index document {}: {}", id, err));
        }
    }

    /// Removes one document from the search index (best effort).
    fn unindex_document(&mut self, collection: &str, document: &Document) {
        let id = document_id_string(document);
        if let Err(err) = self.index.remove(collection, &id) {
            LOGGER.warn(format!("Failed to unindex document {}: {}", id, err));
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
                // `docs` borrow ends here (NLL); the re-index below
                // needs `&mut self` again.

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
                self.declared_indexes.remove(&collection);
                if let Err(err) = self.index.remove_collection(&collection) {
                    LOGGER.warn(format!(
                        "Failed to drop collection {} from index: {}",
                        collection, err
                    ));
                }
                PartitionResult::Dropped { collection }
            }

            PartitionCommand::DropDatabase { database } => {
                // One database's collections hash across partitions:
                // this worker drops only the namespaces it owns, plus
                // their search index entries. Bare namespaces belong
                // to the "test" database by convention.
                let prefix = format!("{}.", database);
                let doomed: Vec<String> = self
                    .collections
                    .keys()
                    .filter(|key| {
                        key.starts_with(&prefix) || (database == "test" && !key.contains('.'))
                    })
                    .cloned()
                    .collect();

                for key in &doomed {
                    self.collections.remove(key);
                    self.declared_indexes.remove(key);
                    if let Err(err) = self.index.remove_collection(key) {
                        LOGGER.warn(format!(
                            "Failed to drop collection {} from index: {}",
                            key, err
                        ));
                    }
                }
                PartitionResult::DroppedDatabase { database }
            }

            PartitionCommand::CreateCollection { collection } => {
                // MongoDB reports NamespaceExists for a duplicate
                // create; erroring (instead of a silent no-op) keeps
                // the operation idempotent for the data: an existing
                // collection is never recreated or cleared.
                if self.collections.contains_key(&collection) {
                    return PartitionResult::Error {
                        message: format!("collection already exists: {}", collection),
                    };
                }
                self.collections.insert(collection, Vec::new());
                PartitionResult::Created
            }

            PartitionCommand::ListIndexes { collection } => {
                // MongoDB answers NamespaceExists ("ns not found") for
                // a collection it does not know; matching that keeps
                // drivers from rendering a phantom empty collection.
                if !self.collections.contains_key(&collection) {
                    return PartitionResult::Error {
                        message: format!("ns not found: {}", collection),
                    };
                }

                // Every collection carries the mandatory `_id_` index,
                // followed by whatever `createIndexes` declared.
                let mut indexes = vec![default_id_index()];
                if let Some(declared) = self.declared_indexes.get(&collection) {
                    indexes.extend(declared.iter().cloned());
                }
                PartitionResult::ListedIndexes { indexes }
            }

            PartitionCommand::CreateIndexes {
                collection,
                indexes,
            } => {
                // MongoDB's `createIndexes` implicitly creates the
                // collection when it is missing. Spring Data relies on
                // this: it declares indexes at start-up, long before
                // the first insert.
                let created_collection = !self.collections.contains_key(&collection);
                if created_collection {
                    self.collections.insert(collection.clone(), Vec::new());
                }

                let declared = self.declared_indexes.entry(collection).or_default();
                let num_indexes_before = declared.len() as u32 + 1;

                for index in indexes {
                    // MongoDB keys declarations by name; re-declaring
                    // an existing name replaces it *in place* so the
                    // listing keeps a stable order.
                    let name = index
                        .get_str("name")
                        .ok()
                        .map(str::to_string)
                        .unwrap_or_else(|| format!("kova_index_{}", declared.len()));

                    let mut spec = index;
                    spec.insert("name", name.clone());
                    // Version 2 is what modern MongoDB reports for
                    // freshly created indexes; drivers check it.
                    spec.entry("v".to_string()).or_insert(bson::Bson::Int32(2));

                    match declared
                        .iter()
                        .position(|existing| existing.get_str("name") == Ok(name.as_str()))
                    {
                        Some(position) => declared[position] = spec,
                        None => declared.push(spec),
                    }
                }

                PartitionResult::IndexesCreated {
                    num_indexes_before,
                    num_indexes_after: declared.len() as u32 + 1,
                    created_collection,
                }
            }

            PartitionCommand::ListCollections { database, filter } => {
                // Collections are stored under their `db.collection`
                // namespace; a listing is scoped to the requesting
                // database and reports bare collection names, like
                // real MongoDB. An empty `database` lists everything
                // (engine-internal callers).
                let prefix = if database.is_empty() {
                    String::new()
                } else {
                    format!("{}.", database)
                };

                // One entry per collection this partition owns, shaped
                // like a real MongoDB listCollections document (the
                // server-generated `uuid` is omitted). The filter uses
                // the same equality matcher as every other command.
                let mut collections: Vec<Document> = self
                    .collections
                    .keys()
                    .filter_map(|key| {
                        let bare = key.strip_prefix(&prefix)?;
                        if bare.is_empty() || bare.contains('.') {
                            return None;
                        }
                        Some(bare.to_string())
                    })
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

            PartitionCommand::ListDatabases { filter } => {
                // One entry per database this partition touches. A
                // database is only "empty" when every collection it
                // owns here is empty; the network layer ANDs that flag
                // across partitions when it merges the replies.
                let mut emptiness: std::collections::BTreeMap<String, bool> =
                    std::collections::BTreeMap::new();
                for (key, docs) in &self.collections {
                    let name = key.split_once('.').map_or("test", |(db, _)| db);
                    let entry = emptiness.entry(name.to_string()).or_insert(true);
                    *entry &= docs.is_empty();
                }

                let databases: Vec<Document> = emptiness
                    .into_iter()
                    .map(|(name, empty)| {
                        doc! {
                            "name": name,
                            "sizeOnDisk": 0i64,
                            "empty": empty,
                        }
                    })
                    .filter(|database| document_matches(database, &filter))
                    .collect();
                PartitionResult::ListedDatabases { databases }
            }

            PartitionCommand::CollectionStats { collection } => {
                // Studio 3T and Compass render a collection's storage
                // panel from this; a missing namespace is an error so
                // the tools do not show a phantom zero-size collection.
                let Some(docs) = self.collections.get(&collection) else {
                    return PartitionResult::Error {
                        message: format!("ns not found: {}", collection),
                    };
                };

                // `size` is the real serialized footprint: every
                // document is measured, not estimated.
                let size: u64 = docs
                    .iter()
                    .map(|doc| {
                        bson::to_vec(doc)
                            .map(|bytes| bytes.len() as u64)
                            .unwrap_or(0)
                    })
                    .sum();
                let count = docs.len() as u64;
                let avg_obj_size = size.checked_div(count).unwrap_or(0);

                // Index sizes are declared metadata only (Phase 4
                // builds the real structures), so they report zero
                // rather than an invented number.
                let declared = self
                    .declared_indexes
                    .get(&collection)
                    .map_or(0, |v| v.len());

                PartitionResult::CollectionStats {
                    stats: doc! {
                        "ns": collection,
                        "count": count as i64,
                        "size": size as i64,
                        "avgObjSize": avg_obj_size as i64,
                        // In-memory storage: storage mirrors `size`.
                        "storageSize": size as i64,
                        "capped": false,
                        "nindexes": (declared + 1) as i64,
                        "totalIndexSize": 0i64,
                        "indexSizes": { "_id_": 0i64 },
                    },
                }
            }

            PartitionCommand::DatabaseStats { database } => {
                // One database's collections hash across partitions,
                // so this worker reports only its own share; the
                // network layer sums the counters.
                let prefix = format!("{}.", database);
                let mut collections = 0u64;
                let mut objects = 0u64;
                let mut data_size = 0u64;
                let mut indexes = 0u64;

                for (key, docs) in &self.collections {
                    // Bare namespaces belong to `test` by convention.
                    let owned =
                        key.starts_with(&prefix) || (database == "test" && !key.contains('.'));
                    if !owned {
                        continue;
                    }
                    collections += 1;
                    objects += docs.len() as u64;
                    data_size += docs
                        .iter()
                        .map(|doc| bson::to_vec(doc).map(|b| b.len() as u64).unwrap_or(0))
                        .sum::<u64>();
                    indexes += self
                        .declared_indexes
                        .get(key)
                        .map_or(1, |declared| declared.len() as u64 + 1);
                }

                PartitionResult::DatabaseStats {
                    collections,
                    objects,
                    data_size,
                    indexes,
                }
            }

            PartitionCommand::Search {
                collection,
                query,
                filter,
                options,
            } => {
                let hits = match self.index.search(&collection, &query, &options.boosts) {
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

                // (score, document) pairs in BM25 order, so a later
                // decay re-rank starts from the relevance ordering.
                let mut ranked: Vec<(f32, Document)> = Vec::with_capacity(hits.len());
                for hit in hits {
                    if let Some(doc) = docs.iter().find(|doc| document_id_string(doc) == hit.id) {
                        if document_matches(doc, &filter) {
                            ranked.push((hit.score, doc.clone()));
                        }
                    }
                }

                if let Some(decay) = &options.decay {
                    apply_decay(&mut ranked, decay);
                }

                PartitionResult::Found {
                    documents: ranked.into_iter().map(|(_, doc)| doc).collect(),
                }
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
                // `docs` borrow ends here (NLL); unindexing below
                // needs `&mut self` again.

                for doc in &removed_docs {
                    self.unindex_document(&collection, doc);
                }

                PartitionResult::Deleted { deleted_count }
            }
        }
    }
}

/// Builds the description of the mandatory `_id_` index.
///
/// Every MongoDB collection carries this index; drivers expect to see
/// it in `listCollections` (`idIndex`) and `listIndexes`, so both
/// reply shapes are built from this one definition.
fn default_id_index() -> Document {
    doc! {
        "v": 2i32,
        "key": { "_id": 1i32 },
        "name": "_id_",
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

/// Extracts every top-level field's string values of a document.
///
/// This is what the search index tokenizes, one entry per field, so
/// the index can weight fields independently (`$text.$boost`). Nested
/// documents contribute dotted paths (`author.name`) and arrays join
/// their string values into one blob. Non-string values are skipped —
/// BM25 over numbers adds noise, not signal. `_id` is excluded, like
/// a MongoDB text index (ids are not text content).
fn extract_field_values(doc: &Document) -> Vec<(String, String)> {
    use std::collections::BTreeMap;

    let mut collected: BTreeMap<String, Vec<String>> = BTreeMap::new();

    fn walk(path: &str, value: &bson::Bson, acc: &mut BTreeMap<String, Vec<String>>) {
        use bson::Bson;
        match value {
            Bson::String(s) => acc.entry(path.to_string()).or_default().push(s.clone()),
            // Arrays contribute every string element to the same
            // field: `tags: ["a", "b"]` is one searchable field.
            Bson::Array(values) => {
                for value in values {
                    walk(path, value, acc);
                }
            }
            Bson::Document(nested) => {
                for (key, value) in nested {
                    walk(&format!("{}.{}", path, key), value, acc);
                }
            }
            _ => {}
        }
    }

    for (key, value) in doc {
        if key == "_id" {
            continue;
        }
        walk(key, value, &mut collected);
    }

    collected
        .into_iter()
        .map(|(field, values)| (field, values.join(" ")))
        .collect()
}

/// Re-ranks (score, document) pairs by an exponential date decay.
///
/// Each document's score is multiplied by `2^(-age/half_life)`, where
/// the age is measured against `DecaySpec::field` (a BSON date).
/// Documents without a usable date keep their raw BM25 score: missing
/// metadata must not punish a document the index considered relevant.
fn apply_decay(ranked: &mut [(f32, Document)], spec: &DecaySpec) {
    use bson::Bson;

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or(0);

    for (score, doc) in ranked.iter_mut() {
        if let Some(Bson::DateTime(date)) = doc.get(&spec.field) {
            let age_secs = (now_ms - date.timestamp_millis()).max(0) as f64 / 1000.0;
            let factor = 2f64.powf(-age_secs / spec.half_life_secs);
            *score *= factor as f32;
        }
    }

    // Newest/highest score first; ties keep the BM25 order (stable).
    ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
}

/// Returns `true` when every field in `filter` is present and equal in `doc`.
///
/// This is an equality-only matcher: it covers the subset of the MongoDB
/// query language needed to route and verify Phase 2 commands. Rich
/// operators (`$gt`, `$in`, ...) arrive with the query engine in Phase 4.
/// Shared with the network layer to filter broadcast listings.
pub(crate) fn document_matches(doc: &Document, filter: &Document) -> bool {
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
    use crate::types::SearchOptions;

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
                options: SearchOptions::default(),
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
                options: SearchOptions::default(),
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
                        options: SearchOptions::default(),
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
                options: SearchOptions::default(),
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
        for rx in router.broadcast(PartitionCommand::ListCollections {
            database: String::new(),
            filter: doc! {},
        }) {
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
            database: String::new(),
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
    async fn test_create_collection_makes_it_listable_without_documents() {
        let router = router();

        let rx = router
            .dispatch(PartitionCommand::CreateCollection {
                collection: "shop.orders".into(),
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Created => {}
            other => panic!("expected Created, got {:?}", other),
        }

        // Scoped to the requesting database, reported by bare name.
        let mut names = Vec::new();
        for rx in router.broadcast(PartitionCommand::ListCollections {
            database: "shop".into(),
            filter: doc! {},
        }) {
            match rx.await.expect("worker replied") {
                PartitionResult::ListedCollections { collections } => names.extend(
                    collections
                        .into_iter()
                        .filter_map(|info| info.get_str("name").ok().map(String::from)),
                ),
                other => panic!("expected ListedCollections, got {:?}", other),
            }
        }
        assert_eq!(names, vec!["orders"]);

        // A duplicate create is an error, matching MongoDB's
        // NamespaceExists, and never clears existing data.
        router
            .dispatch(PartitionCommand::Insert {
                collection: "shop.orders".into(),
                documents: vec![doc! { "n": 1 }],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");
        let rx = router
            .dispatch(PartitionCommand::CreateCollection {
                collection: "shop.orders".into(),
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Error { message } => {
                assert!(message.contains("already exists"), "got: {}", message);
            }
            other => panic!("expected Error, got {:?}", other),
        }

        let rx = router
            .dispatch(PartitionCommand::Count {
                collection: "shop.orders".into(),
                filter: doc! {},
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Counted { count } => assert_eq!(count, 1, "data must survive"),
            other => panic!("expected Counted, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_create_indexes_then_list_indexes_round_trip() {
        // Mirrors the Spring Data start-up flow: declare indexes, then
        // list them when the collection is browsed.
        let router = router();
        router
            .dispatch(PartitionCommand::CreateCollection {
                collection: "shop.orders".into(),
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let rx = router
            .dispatch(PartitionCommand::CreateIndexes {
                collection: "shop.orders".into(),
                indexes: vec![
                    doc! { "key": { "sku": 1i32 }, "name": "sku_1" },
                    doc! { "key": { "qty": -1i32 }, "name": "qty_-1", "unique": true },
                ],
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::IndexesCreated {
                num_indexes_before,
                num_indexes_after,
                created_collection,
            } => {
                assert_eq!(num_indexes_before, 1, "only _id_ before");
                assert_eq!(num_indexes_after, 3, "_id_ plus two declared");
                assert!(
                    !created_collection,
                    "the collection was created explicitly first"
                );
            }
            other => panic!("expected IndexesCreated, got {:?}", other),
        }

        // Re-declaring the same name is a no-op, not a duplicate.
        let rx = router
            .dispatch(PartitionCommand::CreateIndexes {
                collection: "shop.orders".into(),
                indexes: vec![doc! { "key": { "sku": 1i32 }, "name": "sku_1" }],
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::IndexesCreated {
                num_indexes_after, ..
            } => assert_eq!(num_indexes_after, 3, "re-declaration must not grow"),
            other => panic!("expected IndexesCreated, got {:?}", other),
        }

        let rx = router
            .dispatch(PartitionCommand::ListIndexes {
                collection: "shop.orders".into(),
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::ListedIndexes { indexes } => {
                let names: Vec<&str> = indexes
                    .iter()
                    .filter_map(|spec| spec.get_str("name").ok())
                    .collect();
                assert_eq!(names, vec!["_id_", "sku_1", "qty_-1"]);
                // The mandatory index keeps its own definition.
                assert_eq!(indexes[0].get_i32("v").unwrap(), 2);
                assert_eq!(
                    indexes[0].get_document("key").unwrap(),
                    &doc! { "_id": 1i32 }
                );
                // `v` is filled in for declarations that omitted it.
                assert_eq!(indexes[1].get_i32("v").unwrap(), 2);
            }
            other => panic!("expected ListedIndexes, got {:?}", other),
        }

        // Dropping the collection drops its declarations too.
        router
            .dispatch(PartitionCommand::Drop {
                collection: "shop.orders".into(),
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");
        let rx = router
            .dispatch(PartitionCommand::ListIndexes {
                collection: "shop.orders".into(),
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Error { message } => {
                assert!(message.contains("ns not found"), "got: {}", message);
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_create_indexes_creates_missing_collection() {
        // Spring Data declares indexes at application start-up, before
        // any insert exists. MongoDB's `createIndexes` creates the
        // collection implicitly; without that the boot flow fails.
        let router = router();

        let rx = router
            .dispatch(PartitionCommand::CreateIndexes {
                collection: "spring.users".into(),
                indexes: vec![doc! { "key": { "email": 1i32 }, "name": "email_1" }],
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::IndexesCreated {
                num_indexes_before,
                num_indexes_after,
                created_collection,
            } => {
                assert!(created_collection, "collection must be created");
                assert_eq!(num_indexes_before, 1, "_id_ is pre-declared");
                assert_eq!(num_indexes_after, 2);
            }
            other => panic!("expected IndexesCreated, got {:?}", other),
        }

        // The implicitly created collection is now listable...
        let mut names = Vec::new();
        for rx in router.broadcast(PartitionCommand::ListCollections {
            database: "spring".into(),
            filter: doc! {},
        }) {
            match rx.await.expect("worker replied") {
                PartitionResult::ListedCollections { collections } => names.extend(
                    collections
                        .into_iter()
                        .filter_map(|info| info.get_str("name").ok().map(String::from)),
                ),
                other => panic!("expected ListedCollections, got {:?}", other),
            }
        }
        assert_eq!(names, vec!["users"]);

        // ...and its indexes list the declaration made before it existed.
        let rx = router
            .dispatch(PartitionCommand::ListIndexes {
                collection: "spring.users".into(),
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::ListedIndexes { indexes } => {
                let names: Vec<&str> = indexes
                    .iter()
                    .filter_map(|spec| spec.get_str("name").ok())
                    .collect();
                assert_eq!(names, vec!["_id_", "email_1"]);
            }
            other => panic!("expected ListedIndexes, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_list_indexes_reports_only_id_index_by_default() {
        let router = router();
        router
            .dispatch(PartitionCommand::CreateCollection {
                collection: "plain".into(),
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let rx = router
            .dispatch(PartitionCommand::ListIndexes {
                collection: "plain".into(),
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::ListedIndexes { indexes } => {
                assert_eq!(indexes.len(), 1, "only the mandatory _id_ index");
                assert_eq!(indexes[0].get_str("name").unwrap(), "_id_");
            }
            other => panic!("expected ListedIndexes, got {:?}", other),
        }

        // An unknown collection is NamespaceNotFound, not an empty list.
        let rx = router
            .dispatch(PartitionCommand::ListIndexes {
                collection: "ghost".into(),
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Error { message } => {
                assert!(message.contains("ns not found"), "got: {}", message);
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_same_collection_name_in_two_databases_is_isolated() {
        let router = router();
        for db in ["shop", "inventory"] {
            router
                .dispatch(PartitionCommand::Insert {
                    collection: format!("{}.orders", db),
                    documents: vec![doc! { "from": db }],
                })
                .expect("worker reachable")
                .await
                .expect("worker replied");
        }

        // Each database sees only its own "orders".
        for db in ["shop", "inventory"] {
            let rx = router
                .dispatch(PartitionCommand::Find {
                    collection: format!("{}.orders", db),
                    filter: doc! {},
                })
                .expect("worker reachable");
            match rx.await.expect("worker replied") {
                PartitionResult::Found { documents } => {
                    assert_eq!(documents.len(), 1, "{} must see one doc", db);
                    assert_eq!(documents[0].get_str("from").unwrap(), db);
                }
                other => panic!("expected Found, got {:?}", other),
            }
        }
    }

    #[tokio::test]
    async fn test_list_databases_merges_partitions() {
        let router = router();

        // "shop" gets a document; "empty" only gets an (empty)
        // collection. They hash to different partitions.
        router
            .dispatch(PartitionCommand::Insert {
                collection: "shop.orders".into(),
                documents: vec![doc! { "n": 1 }],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");
        router
            .dispatch(PartitionCommand::CreateCollection {
                collection: "empty.logs".into(),
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let mut merged: Vec<Document> = Vec::new();
        for rx in router.broadcast(PartitionCommand::ListDatabases { filter: doc! {} }) {
            match rx.await.expect("worker replied") {
                PartitionResult::ListedDatabases { mut databases } => {
                    merged.append(&mut databases);
                }
                other => panic!("expected ListedDatabases, got {:?}", other),
            }
        }

        let shop = merged
            .iter()
            .find(|db| db.get_str("name") == Ok("shop"))
            .expect("shop listed");
        assert!(!shop.get_bool("empty").unwrap(), "shop holds a document");

        let empty = merged
            .iter()
            .find(|db| db.get_str("name") == Ok("empty"))
            .expect("empty listed");
        assert!(empty.get_bool("empty").unwrap(), "no documents yet");
    }

    #[tokio::test]
    async fn test_collection_stats_reports_real_serialized_size() {
        let router = router();
        router
            .dispatch(PartitionCommand::Insert {
                collection: "shop.orders".into(),
                documents: vec![doc! { "sku": "a" }, doc! { "sku": "bb" }],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let rx = router
            .dispatch(PartitionCommand::CollectionStats {
                collection: "shop.orders".into(),
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::CollectionStats { stats } => {
                assert_eq!(stats.get_str("ns").unwrap(), "shop.orders");
                assert_eq!(stats.get_i64("count"), Ok(2));

                // `size` must equal the real serialized footprint, not
                // an estimate: compute it independently here.
                let expected: i64 = bson::to_vec(&doc! { "sku": "a" }).unwrap().len() as i64
                    + bson::to_vec(&doc! { "sku": "bb" }).unwrap().len() as i64;
                assert_eq!(stats.get_i64("size"), Ok(expected));
                assert_eq!(stats.get_i64("avgObjSize"), Ok(expected / 2));
                assert!(!stats.get_bool("capped").unwrap());
                // Only the mandatory _id_ index exists so far.
                assert_eq!(stats.get_i64("nindexes"), Ok(1));
            }
            other => panic!("expected CollectionStats, got {:?}", other),
        }

        // An unknown namespace is an error, not a phantom zero-size
        // collection.
        let rx = router
            .dispatch(PartitionCommand::CollectionStats {
                collection: "shop.ghost".into(),
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Error { message } => {
                assert!(message.contains("ns not found"), "got: {}", message);
            }
            other => panic!("expected Error, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_database_stats_sums_across_partitions() {
        let router = router();
        // Two collections in the same database; they hash to different
        // partitions, so only a summed broadcast sees both.
        for coll in ["shop.orders", "shop.users"] {
            router
                .dispatch(PartitionCommand::Insert {
                    collection: coll.into(),
                    documents: vec![doc! { "n": 1 }],
                })
                .expect("worker reachable")
                .await
                .expect("worker replied");
        }
        // A different database must not leak into the totals.
        router
            .dispatch(PartitionCommand::Insert {
                collection: "other.things".into(),
                documents: vec![doc! { "n": 1 }],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let mut collections = 0u64;
        let mut objects = 0u64;
        let mut data_size = 0u64;
        for rx in router.broadcast(PartitionCommand::DatabaseStats {
            database: "shop".into(),
        }) {
            match rx.await.expect("worker replied") {
                PartitionResult::DatabaseStats {
                    collections: c,
                    objects: o,
                    data_size: s,
                    indexes: _,
                } => {
                    collections += c;
                    objects += o;
                    data_size += s;
                }
                other => panic!("expected DatabaseStats, got {:?}", other),
            }
        }

        assert_eq!(collections, 2, "only shop's two collections count");
        assert_eq!(objects, 2, "one document each");
        assert!(data_size > 0, "size must be measured, not zero");
    }

    #[tokio::test]
    async fn test_drop_database_removes_only_that_database() {
        let router = router();
        for (db, word) in [("shop", "alpha"), ("inventory", "omega")] {
            router
                .dispatch(PartitionCommand::Insert {
                    collection: format!("{}.orders", db),
                    documents: vec![doc! { "text": word }],
                })
                .expect("worker reachable")
                .await
                .expect("worker replied");
        }

        // Broadcast: one database's collections hash across workers.
        for rx in router.broadcast(PartitionCommand::DropDatabase {
            database: "shop".into(),
        }) {
            match rx.await.expect("worker replied") {
                PartitionResult::DroppedDatabase { database } => {
                    assert_eq!(database, "shop");
                }
                other => panic!("expected DroppedDatabase, got {:?}", other),
            }
        }

        // shop.orders is gone (data and search index), inventory intact.
        let rx = router
            .dispatch(PartitionCommand::Count {
                collection: "shop.orders".into(),
                filter: doc! {},
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Counted { count } => assert_eq!(count, 0),
            other => panic!("expected Counted, got {:?}", other),
        }

        let rx = router
            .dispatch(PartitionCommand::Search {
                collection: "inventory.orders".into(),
                query: "omega".into(),
                filter: doc! {},
                options: SearchOptions::default(),
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 1, "other database must survive");
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

    #[test]
    fn test_extract_field_values_splits_fields_and_skips_id() {
        let doc = doc! {
            "_id": "a",
            "title": "Rust engines",
            "tags": ["db", "fast"],
            "author": { "name": "Ada" },
            "year": 2026i32,
        };
        let mut fields = extract_field_values(&doc);
        fields.sort();

        assert_eq!(
            fields,
            vec![
                ("author.name".to_string(), "Ada".to_string()),
                ("tags".to_string(), "db fast".to_string()),
                ("title".to_string(), "Rust engines".to_string()),
            ],
            "_id and non-string values must not be indexed"
        );
    }

    #[test]
    fn test_apply_decay_prefers_newer_documents() {
        use bson::DateTime;
        use std::time::{SystemTime, UNIX_EPOCH};

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        let old = DateTime::from_millis(now_ms - 10 * 86_400_000); // 10 days ago
        let new = DateTime::from_millis(now_ms - 60); // just now

        let mut ranked = vec![
            (10.0f32, doc! { "_id": "old", "when": old }),
            (10.0f32, doc! { "_id": "new", "when": new }),
        ];
        apply_decay(
            &mut ranked,
            &DecaySpec {
                field: "when".into(),
                half_life_secs: 86_400.0, // one day
            },
        );

        assert_eq!(ranked[0].1.get_str("_id").unwrap(), "new");
        assert_eq!(ranked[1].1.get_str("_id").unwrap(), "old");
        // 10 half-lives: ~2^-10 of the original score.
        assert!(ranked[1].0 < 0.01, "decay factor: {}", ranked[1].0);
    }

    #[test]
    fn test_apply_decay_keeps_documents_without_date() {
        let mut ranked = vec![
            (1.0f32, doc! { "_id": "undated" }),
            (0.5f32, doc! { "_id": "also_undated" }),
        ];
        apply_decay(
            &mut ranked,
            &DecaySpec {
                field: "when".into(),
                half_life_secs: 1.0,
            },
        );

        assert_eq!(ranked[0].1.get_str("_id").unwrap(), "undated");
        assert_eq!(ranked[0].0, 1.0, "missing date must not be penalized");
    }

    #[tokio::test]
    async fn test_search_boost_reorders_worker_reply() {
        let router = router();
        router
            .dispatch(PartitionCommand::Insert {
                collection: "articles".into(),
                documents: vec![
                    doc! { "_id": "body", "body": "rust oven seasoning" },
                    doc! { "_id": "title", "title": "Rust programming" },
                ],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let rx = router
            .dispatch(PartitionCommand::Search {
                collection: "articles".into(),
                query: "rust".into(),
                filter: doc! {},
                options: SearchOptions {
                    boosts: vec![("title".to_string(), 20.0)],
                    decay: None,
                },
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 2);
                assert_eq!(
                    documents[0].get_str("_id").unwrap(),
                    "title",
                    "boosted title must rank first"
                );
            }
            other => panic!("expected Found, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_search_decay_reorders_worker_reply() {
        use bson::DateTime;
        use std::time::{SystemTime, UNIX_EPOCH};

        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;

        let router = router();
        router
            .dispatch(PartitionCommand::Insert {
                collection: "news".into(),
                documents: vec![
                    doc! {
                        "_id": "old",
                        "text": "rust release",
                        "created_at": DateTime::from_millis(now_ms - 30 * 86_400_000),
                    },
                    doc! {
                        "_id": "new",
                        "text": "rust release",
                        "created_at": DateTime::from_millis(now_ms),
                    },
                ],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let rx = router
            .dispatch(PartitionCommand::Search {
                collection: "news".into(),
                query: "rust".into(),
                filter: doc! {},
                options: SearchOptions {
                    boosts: Vec::new(),
                    decay: Some(DecaySpec {
                        field: "created_at".into(),
                        half_life_secs: 86_400.0,
                    }),
                },
            })
            .expect("worker reachable");
        match rx.await.expect("worker replied") {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 2);
                assert_eq!(
                    documents[0].get_str("_id").unwrap(),
                    "new",
                    "decay must rank the fresh document first"
                );
            }
            other => panic!("expected Found, got {:?}", other),
        }
    }
}
