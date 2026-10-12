//! Partition workers and command routing.
//!
//! This module implements the lock-free storage side of the engine.
//! Each partition is a **single-threaded** worker (`std::thread`).
//! Documents live in pages. The worker's RAM is the buffer pool and
//! the search index, not a copy of every document. Network tasks never
//! touch storage memory directly: they send a [`CommandEnvelope`]
//! through a `crossbeam::channel` and await the reply on a `oneshot`.
//!
//! Because a partition is confined to one thread, no `Mutex` or
//! `RwLock` is required on the data structures — this is the core of
//! the thread-per-core design described in `docs/ARCHITECTURE.md`.
//!
//! Each worker owns a [`engine::StorageEngine`]. Mutations are appended
//! to the WAL before a page is dirtied. A checkpoint flushes those
//! pages and records the LSN. Startup opens the pages and replays the
//! WAL tail.

mod aggregate;
mod buffer_pool;
mod crc;
mod disk;
mod engine;
mod heap;
mod page;
mod wal;

use std::sync::Arc;
use std::thread::JoinHandle;

use bson::{doc, Document};
use crossbeam_channel::{unbounded, Receiver, Sender};
use tokio::sync::oneshot;

use crate::config::StorageConfig;
use crate::logger::LOGGER;
use crate::search::SearchIndex;
use crate::types::{CommandEnvelope, DecaySpec, PartitionCommand, PartitionResult};

use self::engine::StorageEngine;
use self::wal::WalOp;

/// A single-threaded storage worker.
///
/// The worker runs on its own OS thread and processes commands
/// sequentially from its channel. It owns every collection in its
/// partition exclusively, so no synchronization primitives are needed.
pub struct PartitionWorker {
    id: usize,
    receiver: Receiver<CommandEnvelope>,
    /// Full-text index mirroring this partition's documents.
    ///
    /// Field indexes are created lazily on first insert; a failure to
    /// allocate one is logged and that field is skipped, never fatal.
    /// The postings live in RAM. The document bodies do not.
    index: SearchIndex,
    /// WAL, document pages, and the buffer pool for this partition.
    engine: StorageEngine,
    /// How many partitions the router spawned. `$lookup` uses it to
    /// decide whether the foreign collection lives on this worker.
    partition_count: usize,
}

impl PartitionWorker {
    /// Creates a worker and restores its collections from disk.
    fn new(
        id: usize,
        partition_count: usize,
        receiver: Receiver<CommandEnvelope>,
        storage: StorageConfig,
    ) -> Self {
        let mut engine = StorageEngine::open(id, &storage).unwrap_or_else(|err| {
            abort_storage(id, err);
        });
        let recovery = engine.load().unwrap_or_else(|err| abort_storage(id, err));
        let pending = recovery.pending;
        let mut worker = Self {
            id,
            receiver,
            index: SearchIndex::new(),
            engine,
            partition_count,
        };
        // Replay before building the search index so tokenization sees
        // the pages after the WAL tail, and each document is indexed once.
        for record in pending {
            let result = worker.apply_recorded(record.op.into_command(), false);
            if let PartitionResult::Error { message } = result {
                LOGGER.warn(format!(
                    "partition {} skipped WAL lsn {} during recovery: {}",
                    id, record.lsn, message
                ));
            }
        }
        if let Err(err) = worker.reindex_all() {
            abort_storage(id, err);
        }
        LOGGER.info(format!(
            "Partition {} recovered {} collection(s), lsn {}",
            id,
            worker.engine.collection_count(),
            worker.engine.last_lsn()
        ));
        worker
    }

    /// Builds the search index entry for one document and adds it.
    ///
    /// Each top-level field becomes its own index entry (Phase 4),
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

        if self.engine.checkpoint_on_shutdown() {
            if let Err(message) = self.write_checkpoint() {
                LOGGER.error(format!(
                    "partition {} shutdown checkpoint failed: {}",
                    self.id, message
                ));
            }
        }

        LOGGER.info(format!(
            "Partition worker {} shutting down ({} collections)",
            self.id,
            self.engine.collection_count()
        ));
    }

    /// Appends a mutating command to the WAL, applies it to pages, and
    /// checkpoints when the log has grown past the configured threshold.
    fn execute(&mut self, mut command: PartitionCommand) -> PartitionResult {
        // An insert without `_id` gets one before the WAL record is
        // written. Replay then sees the same id and does not store a
        // second copy. Two identical documents from the client stay
        // distinct because each call draws a new id.
        if let PartitionCommand::Insert { documents, .. } = &mut command {
            for document in documents.iter_mut() {
                if document.get("_id").is_none() {
                    document.insert("_id", bson::Bson::ObjectId(bson::oid::ObjectId::new()));
                }
            }
        }
        let appended = if let Some(op) = WalOp::from_command(&command) {
            if let Err(message) = self.engine.append(&op) {
                LOGGER.error(format!(
                    "partition {} failed to append WAL record: {}",
                    self.id, message
                ));
                return PartitionResult::Error { message };
            }
            true
        } else {
            false
        };

        let result = self.apply_recorded(command, true);

        if appended && self.engine.should_checkpoint() {
            if let Err(message) = self.write_checkpoint() {
                LOGGER.error(format!(
                    "partition {} checkpoint failed: {}",
                    self.id, message
                ));
            }
        }
        result
    }

    /// Executes a command against this partition's pages.
    ///
    /// `touch_index` is false while replaying the WAL. Recovery builds
    /// the search index once, after every record has been applied.
    fn apply_recorded(&mut self, command: PartitionCommand, touch_index: bool) -> PartitionResult {
        match command {
            PartitionCommand::Insert {
                collection,
                documents,
            } => {
                let stored = match self.engine.insert_documents(&collection, documents) {
                    Ok(stored) => stored,
                    Err(message) => return PartitionResult::Error { message },
                };
                if touch_index {
                    for document in &stored {
                        self.index_document(&collection, document);
                    }
                }
                PartitionResult::Inserted {
                    inserted_count: stored.len() as u32,
                }
            }

            PartitionCommand::Find {
                collection,
                filter,
                projection,
                sort,
                skip,
                limit,
            } => {
                if let Err(message) = validate_filter(&filter) {
                    return PartitionResult::Error { message };
                }
                let documents = match self.engine.find_documents(&collection, &filter) {
                    Ok(documents) => documents,
                    Err(message) => return PartitionResult::Error { message },
                };
                match aggregate::apply_view(
                    documents,
                    projection.as_ref(),
                    sort.as_ref(),
                    skip,
                    limit,
                ) {
                    Ok(documents) => PartitionResult::Found { documents },
                    Err(message) => PartitionResult::Error { message },
                }
            }

            PartitionCommand::Aggregate {
                collection,
                pipeline,
                resolved,
            } => self.execute_aggregate(&collection, &pipeline, &resolved),

            PartitionCommand::Count { collection, filter } => {
                if let Err(message) = validate_filter(&filter) {
                    return PartitionResult::Error { message };
                }
                match self.engine.count_documents(&collection, &filter) {
                    Ok(count) => PartitionResult::Counted { count },
                    Err(message) => PartitionResult::Error { message },
                }
            }

            PartitionCommand::Update {
                collection,
                filter,
                update,
                multi,
            } => {
                if let Err(message) = validate_filter(&filter) {
                    return PartitionResult::Error { message };
                }
                if let Err(message) = validate_update(&update) {
                    return PartitionResult::Error { message };
                }
                let (matched_count, updated) =
                    match self
                        .engine
                        .update_documents(&collection, &filter, &update, multi)
                    {
                        Ok(updated) => updated,
                        Err(message) => return PartitionResult::Error { message },
                    };
                // `_id` is preserved, so removing and adding the same id
                // replaces the postings with the new field values.
                if touch_index {
                    for doc in &updated {
                        self.unindex_document(&collection, doc);
                        self.index_document(&collection, doc);
                    }
                }
                PartitionResult::Modified {
                    matched_count,
                    modified_count: updated.len() as u32,
                }
            }

            PartitionCommand::Drop { collection } => {
                if let Err(message) = self.engine.drop_collection(&collection) {
                    return PartitionResult::Error { message };
                }
                if touch_index {
                    if let Err(err) = self.index.remove_collection(&collection) {
                        LOGGER.warn(format!(
                            "Failed to drop collection {} from index: {}",
                            collection, err
                        ));
                    }
                }
                PartitionResult::Dropped { collection }
            }

            PartitionCommand::DropDatabase { database } => {
                // One database's collections hash across partitions:
                // this worker drops only the namespaces it owns, plus
                // their search index entries. Bare namespaces belong
                // to the "test" database by convention.
                let doomed = match self.engine.drop_database(&database) {
                    Ok(doomed) => doomed,
                    Err(message) => return PartitionResult::Error { message },
                };
                if touch_index {
                    for key in &doomed {
                        if let Err(err) = self.index.remove_collection(key) {
                            LOGGER.warn(format!(
                                "Failed to drop collection {} from index: {}",
                                key, err
                            ));
                        }
                    }
                }
                PartitionResult::DroppedDatabase { database }
            }

            PartitionCommand::CreateCollection { collection } => {
                // MongoDB reports NamespaceExists for a duplicate
                // create; erroring (instead of a silent no-op) keeps
                // the operation idempotent for the data: an existing
                // collection is never recreated or cleared.
                if !self.engine.ensure_collection(&collection) {
                    return PartitionResult::Error {
                        message: format!("collection already exists: {}", collection),
                    };
                }
                PartitionResult::Created
            }

            PartitionCommand::ListIndexes { collection } => {
                // MongoDB answers NamespaceExists ("ns not found") for
                // a collection it does not know; matching that keeps
                // drivers from rendering a phantom empty collection.
                if !self.engine.contains_collection(&collection) {
                    return PartitionResult::Error {
                        message: format!("ns not found: {}", collection),
                    };
                }

                // Every collection carries the mandatory `_id_` index,
                // followed by whatever `createIndexes` declared.
                let mut indexes = vec![default_id_index()];
                if let Some(declared) = self.engine.index_specs(&collection) {
                    indexes.extend(declared);
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
                let created_collection = self.engine.ensure_collection(&collection);
                let mut declared = self.engine.index_specs(&collection).unwrap_or_default();
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

                let num_indexes_after = declared.len() as u32 + 1;
                self.engine.set_indexes(&collection, declared);
                PartitionResult::IndexesCreated {
                    num_indexes_before,
                    num_indexes_after,
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
                    .engine
                    .names()
                    .iter()
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
                for key in self.engine.names() {
                    let name = key.split_once('.').map_or("test", |(db, _)| db);
                    let entry = emptiness.entry(name.to_string()).or_insert(true);
                    *entry &= self.engine.doc_count(&key) == 0;
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
                let Some((count, size)) = (match self.engine.bson_size(&collection) {
                    Ok(stats) => stats,
                    Err(message) => return PartitionResult::Error { message },
                }) else {
                    return PartitionResult::Error {
                        message: format!("ns not found: {}", collection),
                    };
                };

                // `size` is the serialized BSON footprint of the documents.
                let avg_obj_size = size.checked_div(count).unwrap_or(0);

                // Index sizes are declared metadata only, so they report
                // zero rather than an invented number.
                let declared = self.engine.index_count(&collection);

                PartitionResult::CollectionStats {
                    stats: doc! {
                        "ns": collection,
                        "count": count as i64,
                        "size": size as i64,
                        "avgObjSize": avg_obj_size as i64,
                        // Document bytes, measured from the BSON bodies.
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

                for key in self.engine.names() {
                    // Bare namespaces belong to `test` by convention.
                    let owned =
                        key.starts_with(&prefix) || (database == "test" && !key.contains('.'));
                    if !owned {
                        continue;
                    }
                    let (count, size) = match self.engine.bson_size(&key) {
                        Ok(Some(stats)) => stats,
                        Ok(None) => continue,
                        Err(message) => return PartitionResult::Error { message },
                    };
                    collections += 1;
                    objects += count;
                    data_size += size;
                    indexes += self.engine.index_count(&key) as u64 + 1;
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
                if let Err(message) = validate_filter(&filter) {
                    return PartitionResult::Error { message };
                }
                let hits = match self.index.search(&collection, &query, &options.boosts) {
                    Ok(hits) => hits,
                    Err(err) => return PartitionResult::Error { message: err },
                };

                // The index stores ids, not documents: look each hit up
                // on its page and apply any non-`$text` predicates the
                // client also sent.
                let mut ranked: Vec<(f32, Document)> = Vec::with_capacity(hits.len());
                for hit in hits {
                    let doc = match self.engine.find_by_id(&collection, &hit.id) {
                        Ok(doc) => doc,
                        Err(message) => return PartitionResult::Error { message },
                    };
                    if let Some(doc) = doc {
                        if document_matches(&doc, &filter) {
                            ranked.push((hit.score, doc));
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
                if let Err(message) = validate_filter(&filter) {
                    return PartitionResult::Error { message };
                }
                let removed = match self.engine.delete_documents(&collection, &filter, limit) {
                    Ok(removed) => removed,
                    Err(message) => return PartitionResult::Error { message },
                };
                if touch_index {
                    for doc in &removed {
                        self.unindex_document(&collection, doc);
                    }
                }
                PartitionResult::Deleted {
                    deleted_count: removed.len() as u32,
                }
            }
        }
    }

    /// Runs a pipeline. A leading `$match` is applied while the pages
    /// are scanned, so documents that fail it are not kept. A `$lookup`
    /// whose collection hashes here is read the same way; any other
    /// collection is reported back so the network layer can load it.
    fn execute_aggregate(
        &mut self,
        collection: &str,
        pipeline: &[Document],
        resolved: &[(String, Vec<Document>)],
    ) -> PartitionResult {
        if let Err(message) = aggregate::validate_pipeline(pipeline) {
            return PartitionResult::Error { message };
        }
        let (filter, start) = match aggregate::split_leading_matches(pipeline) {
            Ok(split) => split,
            Err(message) => return PartitionResult::Error { message },
        };
        let input = match self.engine.find_documents(collection, &filter) {
            Ok(input) => input,
            Err(message) => return PartitionResult::Error { message },
        };
        let id = self.id;
        let partitions = self.partition_count.max(1);
        let outcome = aggregate::run(input, &pipeline[start..], &mut |namespace| {
            if let Some((_, documents)) = resolved.iter().find(|(name, _)| name == namespace) {
                return Ok(aggregate::Foreign::Ready(documents.clone()));
            }
            let owner = (fnv1a(namespace.as_bytes()) % partitions as u64) as usize;
            if owner == id {
                let documents = self.engine.find_documents(namespace, &doc! {})?;
                return Ok(aggregate::Foreign::Ready(documents));
            }
            Ok(aggregate::Foreign::Remote)
        });
        match outcome {
            Ok(aggregate::Output::Docs(documents)) => PartitionResult::Found { documents },
            Ok(aggregate::Output::Remote { namespace }) => {
                PartitionResult::NeedForeign { namespace }
            }
            Ok(aggregate::Output::Out {
                namespace,
                documents,
            }) => PartitionResult::StageOut {
                namespace,
                documents,
            },
            Err(message) => PartitionResult::Error { message },
        }
    }

    /// Rebuilds the Tantivy index by scanning pages one at a time.
    ///
    /// The search index lives in RAM. Pages store the documents, not
    /// the postings, so every startup tokenizes them again.
    fn reindex_all(&mut self) -> Result<(), String> {
        let index = &mut self.index;
        self.engine.for_each_document(|collection, document| {
            let id = document_id_string(document);
            let fields = extract_field_values(document);
            if let Err(err) = index.add(collection, &id, &fields) {
                LOGGER.warn(format!("Failed to index document {}: {}", id, err));
            }
        })
    }

    /// Flushes dirty pages and truncates the WAL they cover.
    fn write_checkpoint(&mut self) -> Result<(), String> {
        self.engine.checkpoint_pages()
    }
}

/// Logs an unrecoverable storage failure and stops the process.
///
/// A bad checkpoint or a WAL checksum error cannot be applied safely.
/// Continuing with an empty partition would look like data loss.
fn abort_storage(id: usize, err: String) -> ! {
    LOGGER.error(format!("partition {} storage engine failed: {}", id, err));
    std::process::exit(1);
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
pub(super) fn document_id_string(doc: &Document) -> String {
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

/// Returns `true` when `doc` satisfies every predicate in `filter`.
///
/// Field predicates are combined with implicit AND, matching MongoDB.
/// A value that is a document of operators (`{ $gt: 2000 }`) is compared;
/// anything else must be equal to the stored value. Numbers compare across
/// Int32, Int64 and Double, so a shell literal `2000` matches a NumberInt.
/// Shared with the network layer to filter broadcast listings.
pub(crate) fn document_matches(doc: &Document, filter: &Document) -> bool {
    filter.iter().all(|(key, expected)| match key.as_str() {
        "$and" => expected.as_array().is_some_and(|items| {
            items.iter().all(|item| {
                item.as_document()
                    .is_some_and(|clause| document_matches(doc, clause))
            })
        }),
        "$or" => expected.as_array().is_some_and(|items| {
            !items.is_empty()
                && items.iter().any(|item| {
                    item.as_document()
                        .is_some_and(|clause| document_matches(doc, clause))
                })
        }),
        "$nor" => expected.as_array().is_some_and(|items| {
            items.iter().all(|item| {
                item.as_document()
                    .is_none_or(|clause| !document_matches(doc, clause))
            })
        }),
        "$not" => expected
            .as_document()
            .is_some_and(|clause| !document_matches(doc, clause)),
        other if other.starts_with('$') => false,
        field => value_matches(aggregate::get_path(doc, field), expected),
    })
}

/// Rejects a filter operator this phase does not implement.
///
/// `document_matches` treats an unknown operator as "no match" so a
/// listing filter cannot abort a worker. Commands call this first and
/// return the name of the operator to the client.
pub(crate) fn validate_filter(filter: &Document) -> Result<(), String> {
    for (key, value) in filter {
        match key.as_str() {
            "$and" | "$or" | "$nor" => {
                let items = value
                    .as_array()
                    .ok_or_else(|| format!("{key} requires an array"))?;
                for item in items {
                    let clause = item
                        .as_document()
                        .ok_or_else(|| format!("{key} entries must be documents"))?;
                    validate_filter(clause)?;
                }
            }
            "$not" => {
                let clause = value
                    .as_document()
                    .ok_or_else(|| "$not requires a query document".to_string())?;
                validate_filter(clause)?;
            }
            other if other.starts_with('$') => {
                return Err(format!("unknown operator '{other}'"));
            }
            _ => validate_predicate(value)?,
        }
    }
    Ok(())
}

/// Matches one field value against a literal or an operator document.
fn value_matches(actual: Option<&bson::Bson>, expected: &bson::Bson) -> bool {
    if let bson::Bson::RegularExpression(regex) = expected {
        return regex_match(actual, &regex.pattern, Some(regex.options.as_str()));
    }
    if let bson::Bson::Document(operators) = expected {
        if is_operator_document(operators) {
            return operators_match(actual, operators);
        }
    }
    actual.is_some_and(|value| bson_eq(value, expected))
}

fn validate_predicate(value: &bson::Bson) -> Result<(), String> {
    if let bson::Bson::RegularExpression(regex) = value {
        return compile_regex(&regex.pattern, Some(regex.options.as_str())).map(|_| ());
    }
    let bson::Bson::Document(operators) = value else {
        return Ok(());
    };
    if !is_operator_document(operators) {
        return Ok(());
    }
    let has_regex = operators.contains_key("$regex");
    for (op, operand) in operators {
        match op.as_str() {
            "$eq" | "$ne" | "$gt" | "$gte" | "$lt" | "$lte" => {}
            "$in" | "$nin" => {
                if operand.as_array().is_none() {
                    return Err(format!("{op} requires an array"));
                }
            }
            "$exists" => {
                if operand.as_bool().is_none() {
                    return Err("$exists requires a boolean".to_string());
                }
            }
            "$not" => {
                let inner = operand
                    .as_document()
                    .ok_or_else(|| "$not requires an operator document".to_string())?;
                if !is_operator_document(inner) {
                    return Err("$not requires an operator document".to_string());
                }
                validate_predicate(operand)?;
            }
            "$regex" => {
                let pattern = match operand {
                    bson::Bson::String(pattern) => pattern.as_str(),
                    bson::Bson::RegularExpression(regex) => regex.pattern.as_str(),
                    _ => return Err("$regex requires a string".to_string()),
                };
                let options = operators.get_str("$options").ok();
                compile_regex(pattern, options)?;
            }
            "$options" => {
                if !has_regex {
                    return Err("$options requires $regex".to_string());
                }
                if operand.as_str().is_none() {
                    return Err("$options requires a string".to_string());
                }
            }
            "$type" => validate_type_operand(operand)?,
            "$elemMatch" => {
                let inner = operand
                    .as_document()
                    .ok_or_else(|| "$elemMatch requires a document".to_string())?;
                if is_operator_document(inner) {
                    validate_predicate(operand)?;
                } else {
                    validate_filter(inner)?;
                }
            }
            other => return Err(format!("unknown operator '{other}'")),
        }
    }
    Ok(())
}

fn operators_match(actual: Option<&bson::Bson>, operators: &Document) -> bool {
    let options = operators.get_str("$options").ok();
    operators.iter().all(|(op, operand)| {
        if op == "$options" {
            return true;
        }
        if op == "$regex" {
            let (pattern, embedded) = match operand {
                bson::Bson::String(pattern) => (pattern.as_str(), None),
                bson::Bson::RegularExpression(regex) => {
                    (regex.pattern.as_str(), Some(regex.options.as_str()))
                }
                _ => return false,
            };
            let options = options.or(embedded);
            return regex_match(actual, pattern, options);
        }
        operator_matches(op, actual, operand)
    })
}

/// A document is an operator expression when every key starts with `$`.
///
/// An empty document is a stored value, not an operator list, so equality
/// still matches a field whose value is `{}`.
fn is_operator_document(doc: &Document) -> bool {
    !doc.is_empty() && doc.keys().all(|key| key.starts_with('$'))
}

fn operator_matches(op: &str, actual: Option<&bson::Bson>, operand: &bson::Bson) -> bool {
    match op {
        "$eq" => actual.is_some_and(|value| bson_eq(value, operand)),
        "$ne" => actual.map(|value| !bson_eq(value, operand)).unwrap_or(true),
        "$gt" => bson_cmp(actual, operand).is_some_and(|order| order.is_gt()),
        "$gte" => bson_cmp(actual, operand).is_some_and(|order| order.is_ge()),
        "$lt" => bson_cmp(actual, operand).is_some_and(|order| order.is_lt()),
        "$lte" => bson_cmp(actual, operand).is_some_and(|order| order.is_le()),
        "$in" => operand.as_array().is_some_and(|items| {
            actual.is_some_and(|value| items.iter().any(|item| bson_eq(value, item)))
        }),
        "$nin" => operand.as_array().is_some_and(|items| {
            actual
                .map(|value| items.iter().all(|item| !bson_eq(value, item)))
                .unwrap_or(true)
        }),
        "$exists" => actual.is_some() == operand.as_bool().unwrap_or(false),
        "$not" => operand
            .as_document()
            .is_some_and(|inner| !value_matches(actual, &bson::Bson::Document(inner.clone()))),
        "$type" => type_matches(actual, operand),
        "$elemMatch" => elem_match(actual, operand),
        _ => false,
    }
}

fn regex_match(actual: Option<&bson::Bson>, pattern: &str, options: Option<&str>) -> bool {
    let Some(bson::Bson::String(text)) = actual else {
        return false;
    };
    compile_regex(pattern, options)
        .ok()
        .is_some_and(|regex| regex.is_match(text))
}

fn compile_regex(pattern: &str, options: Option<&str>) -> Result<regex::Regex, String> {
    let mut builder = regex::RegexBuilder::new(pattern);
    if let Some(options) = options {
        for flag in options.chars() {
            match flag {
                'i' => {
                    builder.case_insensitive(true);
                }
                'm' => {
                    builder.multi_line(true);
                }
                's' => {
                    builder.dot_matches_new_line(true);
                }
                'x' => {
                    builder.ignore_whitespace(true);
                }
                other => return Err(format!("unknown regex option '{other}'")),
            }
        }
    }
    builder
        .build()
        .map_err(|err| format!("invalid regex: {err}"))
}

fn type_matches(actual: Option<&bson::Bson>, operand: &bson::Bson) -> bool {
    let Some(actual) = actual else {
        return false;
    };
    match operand {
        bson::Bson::Array(items) => items.iter().any(|item| type_one(actual, item)),
        other => type_one(actual, other),
    }
}

fn type_one(actual: &bson::Bson, operand: &bson::Bson) -> bool {
    let name = match operand {
        bson::Bson::String(name) => name.as_str(),
        bson::Bson::Int32(code) => return type_code(actual) == Some(*code),
        bson::Bson::Int64(code) => return type_code(actual) == Some(*code as i32),
        bson::Bson::Double(code) if code.fract() == 0.0 => {
            return type_code(actual) == Some(*code as i32);
        }
        _ => return false,
    };
    match name {
        "double" => matches!(actual, bson::Bson::Double(_)),
        "string" => matches!(actual, bson::Bson::String(_)),
        "object" => matches!(actual, bson::Bson::Document(_)),
        "array" => matches!(actual, bson::Bson::Array(_)),
        "binData" => matches!(actual, bson::Bson::Binary(_)),
        "objectId" => matches!(actual, bson::Bson::ObjectId(_)),
        "bool" => matches!(actual, bson::Bson::Boolean(_)),
        "date" => matches!(actual, bson::Bson::DateTime(_)),
        "null" => matches!(actual, bson::Bson::Null),
        "regex" => matches!(actual, bson::Bson::RegularExpression(_)),
        "int" => matches!(actual, bson::Bson::Int32(_)),
        "long" => matches!(actual, bson::Bson::Int64(_)),
        "decimal" => matches!(actual, bson::Bson::Decimal128(_)),
        "number" => as_number(actual).is_some(),
        _ => false,
    }
}

fn type_code(value: &bson::Bson) -> Option<i32> {
    Some(match value {
        bson::Bson::Double(_) => 1,
        bson::Bson::String(_) => 2,
        bson::Bson::Document(_) => 3,
        bson::Bson::Array(_) => 4,
        bson::Bson::Binary(_) => 5,
        bson::Bson::ObjectId(_) => 7,
        bson::Bson::Boolean(_) => 8,
        bson::Bson::DateTime(_) => 9,
        bson::Bson::Null => 10,
        bson::Bson::RegularExpression(_) => 11,
        bson::Bson::Int32(_) => 16,
        bson::Bson::Int64(_) => 18,
        bson::Bson::Decimal128(_) => 19,
        _ => return None,
    })
}

fn validate_type_operand(operand: &bson::Bson) -> Result<(), String> {
    let items: Vec<&bson::Bson> = match operand {
        bson::Bson::Array(items) => items.iter().collect(),
        other => vec![other],
    };
    if items.is_empty() {
        return Err("$type requires a type name or code".to_string());
    }
    for item in items {
        let known = match item {
            bson::Bson::String(name) => matches!(
                name.as_str(),
                "double"
                    | "string"
                    | "object"
                    | "array"
                    | "binData"
                    | "objectId"
                    | "bool"
                    | "date"
                    | "null"
                    | "regex"
                    | "int"
                    | "long"
                    | "decimal"
                    | "number"
            ),
            bson::Bson::Int32(_) | bson::Bson::Int64(_) => true,
            bson::Bson::Double(code) => code.fract() == 0.0,
            _ => false,
        };
        if !known {
            return Err("$type requires a type name or code".to_string());
        }
    }
    Ok(())
}

fn elem_match(actual: Option<&bson::Bson>, operand: &bson::Bson) -> bool {
    let (Some(bson::Bson::Array(items)), Some(predicate)) = (actual, operand.as_document()) else {
        return false;
    };
    items.iter().any(|element| {
        if is_operator_document(predicate) {
            value_matches(Some(element), operand)
        } else if let bson::Bson::Document(doc) = element {
            document_matches(doc, predicate)
        } else {
            false
        }
    })
}

pub(super) fn bson_eq(left: &bson::Bson, right: &bson::Bson) -> bool {
    bson_cmp(Some(left), right) == Some(std::cmp::Ordering::Equal) || left == right
}

/// Orders two BSON values when they share a comparable type.
///
/// Numbers compare across widths. Dates compare by instant. Strings compare
/// lexicographically. A missing field, or two values of different kinds,
/// is not ordered, so a range operator does not match them.
pub(super) fn bson_cmp(
    actual: Option<&bson::Bson>,
    expected: &bson::Bson,
) -> Option<std::cmp::Ordering> {
    use bson::Bson;

    let actual = actual?;
    if let (Some(left), Some(right)) = (as_number(actual), as_number(expected)) {
        return Some(left.total_cmp(&right));
    }
    match (actual, expected) {
        (Bson::DateTime(left), Bson::DateTime(right)) => {
            Some(left.timestamp_millis().cmp(&right.timestamp_millis()))
        }
        (Bson::String(left), Bson::String(right)) => Some(left.cmp(right)),
        (Bson::Boolean(left), Bson::Boolean(right)) => Some(left.cmp(right)),
        _ => None,
    }
}

fn as_number(value: &bson::Bson) -> Option<f64> {
    match value {
        bson::Bson::Int32(n) => Some(f64::from(*n)),
        bson::Bson::Int64(n) => Some(*n as f64),
        bson::Bson::Double(n) => Some(*n),
        _ => None,
    }
}

/// Applies an update document to `doc`.
///
/// A document whose keys do not start with `$` replaces `doc` and keeps
/// `_id`. Otherwise every key must be a known operator. Operators run
/// in a fixed order: `$rename`, `$unset`, `$inc`, `$set`, `$push`,
/// `$pull`. `$set` therefore overwrites a field `$inc` just changed.
pub(super) fn apply_update(doc: &mut Document, update: &Document) -> Result<(), String> {
    validate_update(update)?;
    if !update.keys().any(|key| key.starts_with('$')) {
        let id = doc.get("_id").cloned();
        *doc = update.clone();
        if let Some(id) = id {
            doc.insert("_id", id);
        }
        return Ok(());
    }

    let mut next = doc.clone();
    if let Ok(rename) = update.get_document("$rename") {
        for (from, to) in rename {
            let to = to
                .as_str()
                .ok_or_else(|| "$rename targets must be strings".to_string())?;
            if let Some(value) = aggregate::remove_path(&mut next, from) {
                aggregate::set_path(&mut next, to, value);
            }
        }
    }
    if let Ok(unset) = update.get_document("$unset") {
        for key in unset.keys() {
            aggregate::remove_path(&mut next, key);
        }
    }
    if let Ok(fields) = update.get_array("$unset") {
        for field in fields {
            let key = field
                .as_str()
                .ok_or_else(|| "$unset entries must be field names".to_string())?;
            aggregate::remove_path(&mut next, key);
        }
    }
    if let Ok(inc) = update.get_document("$inc") {
        for (key, delta) in inc {
            let current = aggregate::get_path(&next, key).cloned();
            aggregate::set_path(&mut next, key, add_numbers(current.as_ref(), delta)?);
        }
    }
    if let Ok(set) = update.get_document("$set") {
        for (key, value) in set {
            aggregate::set_path(&mut next, key, value.clone());
        }
    }
    if let Ok(push) = update.get_document("$push") {
        for (key, value) in push {
            push_value(&mut next, key, value)?;
        }
    }
    if let Ok(pull) = update.get_document("$pull") {
        for (key, value) in pull {
            pull_value(&mut next, key, value)?;
        }
    }
    *doc = next;
    Ok(())
}

pub(super) fn validate_update(update: &Document) -> Result<(), String> {
    if !update.keys().any(|key| key.starts_with('$')) {
        return Ok(());
    }
    for (key, value) in update {
        match key.as_str() {
            "$set" | "$inc" | "$push" | "$pull" | "$rename" => {
                let fields = value
                    .as_document()
                    .ok_or_else(|| format!("{key} requires a document"))?;
                for (field, operand) in fields {
                    guard_id(field)?;
                    match key.as_str() {
                        "$inc" => {
                            if as_number(operand).is_none() {
                                return Err(format!("$inc of '{field}' requires a number"));
                            }
                        }
                        "$rename" => {
                            let target = operand
                                .as_str()
                                .ok_or_else(|| "$rename targets must be strings".to_string())?;
                            guard_id(target)?;
                        }
                        _ => {}
                    }
                }
            }
            "$unset" => match value {
                bson::Bson::Document(fields) => {
                    for field in fields.keys() {
                        guard_id(field)?;
                    }
                }
                bson::Bson::Array(fields) => {
                    for field in fields {
                        let name = field
                            .as_str()
                            .ok_or_else(|| "$unset entries must be field names".to_string())?;
                        guard_id(name)?;
                    }
                }
                _ => return Err("$unset requires a document or an array".to_string()),
            },
            other => return Err(format!("unknown operator '{other}'")),
        }
    }
    Ok(())
}

fn guard_id(path: &str) -> Result<(), String> {
    if path == "_id" || path.starts_with("_id.") {
        return Err("the _id field is immutable".to_string());
    }
    Ok(())
}

fn add_numbers(current: Option<&bson::Bson>, delta: &bson::Bson) -> Result<bson::Bson, String> {
    let current = current.unwrap_or(&bson::Bson::Int32(0));
    let left = as_number(current).ok_or_else(|| "$inc requires a numeric field".to_string())?;
    let right = as_number(delta).ok_or_else(|| "$inc requires a number".to_string())?;
    let sum = left + right;
    if let (Some(left), Some(right)) = (as_i64(current), as_i64(delta)) {
        if let Some(exact) = left.checked_add(right) {
            if matches!(current, bson::Bson::Int32(_))
                && matches!(delta, bson::Bson::Int32(_))
                && (i32::MIN as i64..=i32::MAX as i64).contains(&exact)
            {
                return Ok(bson::Bson::Int32(exact as i32));
            }
            return Ok(bson::Bson::Int64(exact));
        }
    }
    Ok(bson::Bson::Double(sum))
}

fn as_i64(value: &bson::Bson) -> Option<i64> {
    match value {
        bson::Bson::Int32(n) => Some(i64::from(*n)),
        bson::Bson::Int64(n) => Some(*n),
        _ => None,
    }
}

fn push_value(doc: &mut Document, key: &str, value: &bson::Bson) -> Result<(), String> {
    let mut items = match aggregate::get_path(doc, key) {
        None => Vec::new(),
        Some(bson::Bson::Array(items)) => items.clone(),
        Some(_) => return Err(format!("$push of '{key}' requires an array")),
    };
    if let bson::Bson::Document(spec) = value {
        if let Ok(each) = spec.get_array("$each") {
            items.extend(each.iter().cloned());
            aggregate::set_path(doc, key, bson::Bson::Array(items));
            return Ok(());
        }
    }
    items.push(value.clone());
    aggregate::set_path(doc, key, bson::Bson::Array(items));
    Ok(())
}

fn pull_value(doc: &mut Document, key: &str, value: &bson::Bson) -> Result<(), String> {
    let Some(bson::Bson::Array(items)) = aggregate::get_path(doc, key).cloned() else {
        if aggregate::get_path(doc, key).is_none() {
            return Ok(());
        }
        return Err(format!("$pull of '{key}' requires an array"));
    };
    let kept = items
        .into_iter()
        .filter(|element| !pull_matches(element, value))
        .collect();
    aggregate::set_path(doc, key, bson::Bson::Array(kept));
    Ok(())
}

fn pull_matches(element: &bson::Bson, spec: &bson::Bson) -> bool {
    if let bson::Bson::Document(operators) = spec {
        if is_operator_document(operators) {
            return value_matches(Some(element), spec);
        }
    }
    bson_eq(element, spec)
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
    /// Temporary data directory removed after the workers exit.
    scratch: Option<std::path::PathBuf>,
}

impl PartitionRouter {
    /// Spawns `partition_count` workers on dedicated OS threads.
    ///
    /// The running server uses `kova.toml`. Tests get a private temp
    /// directory so parallel cases do not share a WAL.
    pub fn new(partition_count: usize) -> Self {
        Self::with_storage(partition_count, process_storage())
    }

    /// Spawns workers that persist under `storage.data_dir`.
    pub fn with_storage(partition_count: usize, storage: StorageConfig) -> Self {
        assert!(partition_count > 0, "at least one partition is required");

        let scratch = if storage.ephemeral {
            Some(std::path::PathBuf::from(&storage.data_dir))
        } else {
            None
        };

        let mut senders = Vec::with_capacity(partition_count);
        let mut handles = Vec::with_capacity(partition_count);

        for id in 0..partition_count {
            let (tx, rx) = unbounded();
            senders.push(tx);
            let storage = storage.clone();

            let handle = std::thread::Builder::new()
                .name(format!("kova-partition-{}", id))
                .spawn(move || {
                    let mut worker = PartitionWorker::new(id, partition_count, rx, storage);
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
                scratch,
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
        // Join before deleting a scratch directory: the workers still
        // fsync their shutdown checkpoint into it.
        self.senders.clear();
        if let Ok(mut handles) = self.handles.lock() {
            for handle in handles.drain(..) {
                let _ = handle.join();
            }
        }
        if let Some(path) = self.scratch.take() {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

/// Storage settings for this process.
///
/// Tests never touch the server's `data/` directory. The binary reads
/// `kova.toml` (and `KOVA_DATA_DIR`).
fn process_storage() -> StorageConfig {
    #[cfg(test)]
    {
        StorageConfig::ephemeral()
    }
    #[cfg(not(test))]
    {
        crate::config::CONFIG.storage.clone()
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
            .dispatch(PartitionCommand::find("users", doc! { "name": "Ada" }))
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
            .dispatch(PartitionCommand::find("empty", doc! { "name": "nobody" }))
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
            .dispatch(PartitionCommand::find("users", doc! { "age": 37 }))
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
                .dispatch(PartitionCommand::find(format!("{}.orders", db), doc! {}))
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
                documents: vec![
                    doc! { "_id": 1, "sku": "a" },
                    doc! { "_id": 2, "sku": "bb" },
                ],
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
                let expected: i64 = bson::to_vec(&doc! { "_id": 1, "sku": "a" }).unwrap().len()
                    as i64
                    + bson::to_vec(&doc! { "_id": 2, "sku": "bb" }).unwrap().len() as i64;
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
    fn test_document_matches_range_and_set_operators() {
        let smith = doc! { "ename": "SMITH", "job": "CLERK", "sal": 800i32, "deptno": 20i32 };
        let king = doc! { "ename": "KING", "job": "PRESIDENT", "sal": 5000i32, "deptno": 10i32 };

        assert!(document_matches(&king, &doc! { "sal": { "$gt": 2000 } }));
        assert!(!document_matches(&smith, &doc! { "sal": { "$gt": 2000 } }));
        assert!(!document_matches(
            &smith,
            &doc! { "sal": { "$gt": 2000.0 } }
        ));
        // A double literal from the shell still compares against NumberInt.
        assert!(document_matches(&king, &doc! { "sal": { "$gte": 5000.0 } }));
        assert!(document_matches(&smith, &doc! { "sal": { "$lt": 1000 } }));
        assert!(document_matches(&smith, &doc! { "sal": { "$lte": 800 } }));
        assert!(document_matches(
            &smith,
            &doc! { "job": "CLERK", "sal": { "$lt": 1000 } }
        ));
        assert!(document_matches(
            &smith,
            &doc! { "job": { "$in": ["CLERK", "MANAGER"] } }
        ));
        assert!(!document_matches(
            &king,
            &doc! { "job": { "$in": ["CLERK", "MANAGER"] } }
        ));
        assert!(document_matches(
            &king,
            &doc! { "comm": { "$exists": false } }
        ));
        assert!(document_matches(
            &doc! { "ename": "ALLEN", "comm": 300i32 },
            &doc! { "comm": { "$exists": true, "$gt": 0 } }
        ));
        assert!(document_matches(
            &smith,
            &doc! { "$or": [ { "job": "PRESIDENT" }, { "job": "CLERK" } ] }
        ));
        assert!(!document_matches(&king, &doc! { "sal": { "$lt": 1000 } }));

        let early = doc! { "hiredate": bson::DateTime::from_millis(345_859_200_000i64) };
        let late = doc! { "hiredate": bson::DateTime::from_millis(378_691_200_000i64) };
        let cutoff = bson::DateTime::from_millis(378_691_200_000i64);
        assert!(!document_matches(
            &early,
            &doc! { "hiredate": { "$gt": cutoff } }
        ));
        assert!(document_matches(
            &late,
            &doc! { "hiredate": { "$gte": cutoff } }
        ));
    }

    #[test]
    fn test_apply_update_set_operator() {
        let mut doc = doc! { "name": "Ada", "age": 36 };
        apply_update(&mut doc, &doc! { "$set": { "age": 37 } }).unwrap();

        assert_eq!(doc.get_i32("age").unwrap(), 37);
        assert_eq!(doc.get_str("name").unwrap(), "Ada");
    }

    #[test]
    fn test_apply_update_replacement_preserves_id() {
        let mut doc = doc! { "_id": 1, "name": "Ada", "age": 36 };
        apply_update(&mut doc, &doc! { "name": "Grace" }).unwrap();

        assert_eq!(doc.get_i32("_id").unwrap(), 1);
        assert_eq!(doc.get_str("name").unwrap(), "Grace");
        assert!(doc.get("age").is_none(), "replacement drops old fields");
    }

    #[test]
    fn test_document_matches_regex_type_elem_match_and_not() {
        let doc = doc! {
            "ename": "SMITH",
            "sal": 800,
            "tags": [ { "x": 1 }, { "x": 5 } ],
        };
        assert!(document_matches(
            &doc,
            &doc! { "ename": { "$regex": "^sm", "$options": "i" } }
        ));
        assert!(!document_matches(
            &doc,
            &doc! { "ename": { "$regex": "^zz" } }
        ));
        assert!(document_matches(&doc, &doc! { "sal": { "$type": "int" } }));
        assert!(document_matches(
            &doc,
            &doc! { "sal": { "$type": "number" } }
        ));
        assert!(!document_matches(
            &doc,
            &doc! { "ename": { "$type": "int" } }
        ));
        assert!(document_matches(
            &doc,
            &doc! { "tags": { "$elemMatch": { "x": { "$gt": 4 } } } }
        ));
        assert!(!document_matches(
            &doc,
            &doc! { "tags": { "$elemMatch": { "x": { "$gt": 9 } } } }
        ));
        assert!(document_matches(
            &doc,
            &doc! { "sal": { "$not": { "$gt": 1000 } } }
        ));
        assert!(document_matches(
            &doc,
            &doc! { "$not": { "ename": "KING" } }
        ));
        assert!(!document_matches(
            &doc,
            &doc! { "$not": { "ename": "SMITH" } }
        ));
        let error = validate_filter(&doc! { "sal": { "$bogus": 1 } }).unwrap_err();
        assert!(error.contains("$bogus"), "{error}");
    }

    #[test]
    fn test_apply_update_operators() {
        let mut doc = doc! {
            "_id": 1,
            "name": "Ada",
            "age": 36,
            "city": "London",
            "tags": ["a"],
        };
        apply_update(
            &mut doc,
            &doc! {
                "$rename": { "city": "town" },
                "$unset": { "name": "" },
                "$inc": { "age": 1 },
                "$push": { "tags": { "$each": ["b", "c"] } },
                "$pull": { "tags": "a" },
            },
        )
        .unwrap();
        assert!(doc.get("name").is_none());
        assert!(doc.get("city").is_none());
        assert_eq!(doc.get_str("town").unwrap(), "London");
        assert_eq!(doc.get_i32("age").unwrap(), 37);
        let tags = doc.get_array("tags").unwrap();
        assert_eq!(tags.len(), 2);
        assert_eq!(tags[0].as_str(), Some("b"));
        let error = apply_update(&mut doc, &doc! { "$foo": { "age": 1 } }).unwrap_err();
        assert!(error.contains("$foo"), "{error}");
        let error = apply_update(&mut doc, &doc! { "$unset": { "_id": "" } }).unwrap_err();
        assert!(error.contains("_id"), "{error}");
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

    fn durable_config(dir: &std::path::Path, checkpoint_on_shutdown: bool) -> StorageConfig {
        StorageConfig {
            data_dir: dir.to_string_lossy().into_owned(),
            page_size: 512,
            buffer_pool_pages: 8,
            wal_fsync: true,
            wal_segment_bytes: 1024 * 1024,
            checkpoint_every: 0,
            checkpoint_on_shutdown,
            ephemeral: false,
        }
    }

    fn scratch_dir() -> std::path::PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("kova-part-{}-{}", std::process::id(), n));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    async fn dispatch(router: &PartitionRouter, command: PartitionCommand) -> PartitionResult {
        router
            .dispatch(command)
            .expect("worker reachable")
            .await
            .expect("worker replied")
    }

    #[tokio::test]
    async fn test_crash_replays_insert_update_and_delete() {
        let dir = scratch_dir();
        let config = durable_config(&dir, false);
        {
            let router = PartitionRouter::with_storage(1, config.clone());
            dispatch(
                &router,
                PartitionCommand::Insert {
                    collection: "db.emp".into(),
                    documents: vec![
                        doc! { "name": "SMITH", "sal": 800 },
                        doc! { "name": "KING", "sal": 5000 },
                    ],
                },
            )
            .await;
            dispatch(
                &router,
                PartitionCommand::Update {
                    collection: "db.emp".into(),
                    filter: doc! { "name": "SMITH" },
                    update: doc! { "$set": { "sal": 900 } },
                    multi: false,
                },
            )
            .await;
            dispatch(
                &router,
                PartitionCommand::Delete {
                    collection: "db.emp".into(),
                    filter: doc! { "name": "KING" },
                    limit: 1,
                },
            )
            .await;
        }

        let router = PartitionRouter::with_storage(1, config);
        match dispatch(&router, PartitionCommand::find("db.emp", doc! {})).await {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 1);
                assert_eq!(documents[0].get_str("name").unwrap(), "SMITH");
                assert_eq!(documents[0].get_i32("sal").unwrap(), 900);
            }
            other => panic!("expected Found, got {:?}", other),
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn test_checkpoint_survives_without_the_wal() {
        let dir = scratch_dir();
        let config = durable_config(&dir, true);
        {
            let router = PartitionRouter::with_storage(1, config.clone());
            dispatch(
                &router,
                PartitionCommand::Insert {
                    collection: "db.emp".into(),
                    documents: vec![doc! { "name": "SCOTT", "text": "rustacean" }],
                },
            )
            .await;
        }

        let wal = dir.join("partition-0").join("wal");
        assert!(
            std::fs::read_dir(&wal).unwrap().next().is_none(),
            "shutdown checkpoint removes WAL segments"
        );

        let router = PartitionRouter::with_storage(1, config);
        match dispatch(
            &router,
            PartitionCommand::Search {
                collection: "db.emp".into(),
                query: "rustacean".into(),
                filter: doc! {},
                options: SearchOptions::default(),
            },
        )
        .await
        {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 1);
                assert_eq!(documents[0].get_str("name").unwrap(), "SCOTT");
            }
            other => panic!("expected Found, got {:?}", other),
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    fn as_number(doc: &Document, key: &str) -> f64 {
        match doc.get(key) {
            Some(bson::Bson::Int32(n)) => f64::from(*n),
            Some(bson::Bson::Int64(n)) => *n as f64,
            Some(bson::Bson::Double(n)) => *n,
            other => panic!("{key} is not a number: {other:?}"),
        }
    }

    fn batch_of(reply: &Document) -> Vec<Document> {
        reply
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap()
            .iter()
            .map(|value| value.as_document().unwrap().clone())
            .collect()
    }

    #[tokio::test]
    async fn test_aggregate_matches_sql_and_replays_from_the_wal() {
        let dir = scratch_dir();
        let config = durable_config(&dir, false);
        let employees = vec![
            doc! { "_id": 1, "ename": "SMITH", "job": "CLERK", "sal": 800, "deptno": 10 },
            doc! { "_id": 2, "ename": "ALLEN", "job": "SALESMAN", "sal": 1600, "deptno": 30 },
            doc! { "_id": 3, "ename": "KING", "job": "PRESIDENT", "sal": 5000, "deptno": 10 },
            doc! { "_id": 4, "ename": "BLAKE", "job": "MANAGER", "sal": 2850, "deptno": 30 },
        ];
        {
            let router = PartitionRouter::with_storage(1, config.clone());
            dispatch(
                &router,
                PartitionCommand::Insert {
                    collection: "db.emp".into(),
                    documents: employees.clone(),
                },
            )
            .await;
            dispatch(
                &router,
                PartitionCommand::Insert {
                    collection: "db.dept".into(),
                    documents: vec![
                        doc! { "_id": 10, "deptno": 10, "dname": "ACCOUNTING" },
                        doc! { "_id": 30, "deptno": 30, "dname": "SALES" },
                    ],
                },
            )
            .await;

            let grouped = crate::network::execute_aggregate(
                &router,
                PartitionCommand::Aggregate {
                    collection: "db.emp".into(),
                    pipeline: vec![doc! {
                        "$group": {
                            "_id": "$job",
                            "total": { "$sum": "$sal" },
                            "n": { "$count": {} },
                        }
                    }],
                    resolved: Vec::new(),
                },
            )
            .await;
            assert_eq!(grouped.get_f64("ok").unwrap(), 1.0);

            let sql = crate::query_engine::execute_sql_query(
                "SELECT job, SUM(sal) AS total, COUNT(*) AS n FROM emp WHERE sal > 1000 GROUP BY job",
                &[("emp", std::sync::Arc::new(employees.clone()))],
            )
            .await
            .expect("sql");
            let mut expected = std::collections::HashMap::new();
            for row in batch_of(&sql) {
                expected.insert(
                    row.get_str("job").unwrap().to_string(),
                    (as_number(&row, "total"), as_number(&row, "n")),
                );
            }
            let matched = crate::network::execute_aggregate(
                &router,
                PartitionCommand::Aggregate {
                    collection: "db.emp".into(),
                    pipeline: vec![
                        doc! { "$match": { "sal": { "$gt": 1000 } } },
                        doc! { "$group": { "_id": "$job", "total": { "$sum": "$sal" }, "n": { "$count": {} } } },
                    ],
                    resolved: Vec::new(),
                },
            )
            .await;
            let mut actual = std::collections::HashMap::new();
            for row in batch_of(&matched) {
                actual.insert(
                    row.get_str("_id").unwrap().to_string(),
                    (as_number(&row, "total"), as_number(&row, "n")),
                );
            }
            assert_eq!(actual, expected);

            let projected = crate::network::execute_aggregate(
                &router,
                PartitionCommand::Aggregate {
                    collection: "db.emp".into(),
                    pipeline: vec![
                        doc! { "$match": { "ename": "SMITH" } },
                        doc! { "$project": { "ename": 1, "_id": 0 } },
                    ],
                    resolved: Vec::new(),
                },
            )
            .await;
            let projected = batch_of(&projected);
            assert_eq!(projected.len(), 1);
            assert_eq!(projected[0].get_str("ename").unwrap(), "SMITH");
            assert!(projected[0].get("sal").is_none());

            dispatch(
                &router,
                PartitionCommand::Update {
                    collection: "db.emp".into(),
                    filter: doc! { "ename": "SMITH" },
                    update: doc! { "$inc": { "sal": 100 } },
                    multi: false,
                },
            )
            .await;
            let raised = crate::network::execute_aggregate(
                &router,
                PartitionCommand::Aggregate {
                    collection: "db.emp".into(),
                    pipeline: vec![doc! { "$match": { "ename": "SMITH" } }],
                    resolved: Vec::new(),
                },
            )
            .await;
            assert_eq!(as_number(&batch_of(&raised)[0], "sal"), 900.0);

            let written = crate::network::execute_aggregate(
                &router,
                PartitionCommand::Aggregate {
                    collection: "db.emp".into(),
                    pipeline: vec![
                        doc! { "$match": { "sal": { "$gt": 1000 } } },
                        doc! { "$out": "db.high" },
                    ],
                    resolved: Vec::new(),
                },
            )
            .await;
            assert_eq!(written.get_f64("ok").unwrap(), 1.0);
            assert!(batch_of(&written).is_empty());

            let joined = crate::network::execute_aggregate(
                &router,
                PartitionCommand::Aggregate {
                    collection: "db.emp".into(),
                    pipeline: vec![
                        doc! { "$match": { "ename": "KING" } },
                        doc! { "$lookup": {
                            "from": "db.dept",
                            "localField": "deptno",
                            "foreignField": "deptno",
                            "as": "dept",
                        } },
                    ],
                    resolved: Vec::new(),
                },
            )
            .await;
            let king = &batch_of(&joined)[0];
            let dept = king.get_array("dept").unwrap();
            assert_eq!(dept.len(), 1);
            assert_eq!(
                dept[0].as_document().unwrap().get_str("dname").unwrap(),
                "ACCOUNTING"
            );

            let rejected = crate::network::execute_aggregate(
                &router,
                PartitionCommand::Aggregate {
                    collection: "db.emp".into(),
                    pipeline: vec![doc! { "$bogus": 1 }],
                    resolved: Vec::new(),
                },
            )
            .await;
            assert_eq!(rejected.get_f64("ok").unwrap(), 0.0);
            assert!(rejected.get_str("errmsg").unwrap().contains("$bogus"));
        }

        let router = PartitionRouter::with_storage(1, config);
        let replayed = crate::network::execute_aggregate(
            &router,
            PartitionCommand::Aggregate {
                collection: "db.emp".into(),
                pipeline: vec![doc! { "$match": { "ename": "SMITH" } }],
                resolved: Vec::new(),
            },
        )
        .await;
        assert_eq!(as_number(&batch_of(&replayed)[0], "sal"), 900.0);
        match dispatch(&router, PartitionCommand::find("db.high", doc! {})).await {
            PartitionResult::Found { documents } => assert_eq!(documents.len(), 3),
            other => panic!("expected the $out collection, got {:?}", other),
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn test_lookup_reads_the_partition_that_owns_the_foreign_collection() {
        let router = PartitionRouter::new(4);
        dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.emp".into(),
                documents: vec![doc! { "_id": 1, "ename": "KING", "deptno": 10 }],
            },
        )
        .await;
        dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.dept".into(),
                documents: vec![doc! { "_id": 10, "deptno": 10, "dname": "ACCOUNTING" }],
            },
        )
        .await;
        let reply = crate::network::execute_aggregate(
            &router,
            PartitionCommand::Aggregate {
                collection: "db.emp".into(),
                pipeline: vec![doc! { "$lookup": {
                    "from": "db.dept",
                    "localField": "deptno",
                    "foreignField": "deptno",
                    "as": "dept",
                } }],
                resolved: Vec::new(),
            },
        )
        .await;
        assert_eq!(reply.get_f64("ok").unwrap(), 1.0);
        let rows = batch_of(&reply);
        let dept = rows[0].get_array("dept").unwrap();
        assert_eq!(
            dept[0].as_document().unwrap().get_str("dname").unwrap(),
            "ACCOUNTING"
        );
    }

    /// Page 1 of the discretionary-award report in `scripts/bonus-report.js`.
    ///
    /// Staff with no `bonus` row (SMITH) drop out at `$unwind`. Departments
    /// are ordered by the award total, then skipped and limited like a page.
    fn bonus_report(skip: i32, write: bool) -> Vec<Document> {
        let mut pipeline = vec![
            doc! { "$match": { "deptno": { "$in": [10, 20, 30] } } },
            doc! { "$lookup": {
                "from": "db.bonus",
                "localField": "empno",
                "foreignField": "empno",
                "as": "awards",
            } },
            doc! { "$unwind": "$awards" },
            doc! { "$lookup": {
                "from": "db.dept",
                "localField": "deptno",
                "foreignField": "deptno",
                "as": "dept",
            } },
            doc! { "$unwind": "$dept" },
            doc! { "$addFields": {
                "award": "$awards.comm",
                "department": "$dept.dname",
                "location": "$dept.loc",
            } },
            doc! { "$project": {
                "_id": 0,
                "ename": 1,
                "job": 1,
                "sal": 1,
                "award": 1,
                "department": 1,
                "location": 1,
            } },
            doc! { "$sort": { "sal": -1 } },
            doc! { "$group": {
                "_id": "$department",
                "location": { "$first": "$location" },
                "headcount": { "$count": {} },
                "payroll": { "$sum": "$sal" },
                "awardTotal": { "$sum": "$award" },
                "minSal": { "$min": "$sal" },
                "maxSal": { "$max": "$sal" },
                "avgSal": { "$avg": "$sal" },
                "names": { "$push": "$ename" },
                "jobs": { "$addToSet": "$job" },
                "highestPaid": { "$first": "$ename" },
                "lowestPaid": { "$last": "$ename" },
            } },
            doc! { "$sort": { "awardTotal": -1, "_id": 1 } },
            doc! { "$skip": skip },
            doc! { "$limit": 2 },
            doc! { "$replaceRoot": { "newRoot": {
                "department": "$_id",
                "location": "$location",
                "headcount": "$headcount",
                "payroll": "$payroll",
                "awards": "$awardTotal",
                "averageSalary": "$avgSal",
                "salaryBand": { "low": "$minSal", "high": "$maxSal" },
                "recipients": "$names",
                "roles": "$jobs",
                "highestPaid": "$highestPaid",
                "lowestPaid": "$lowestPaid",
            } } },
        ];
        if write {
            pipeline.push(doc! { "$out": "db.bonusReport" });
        }
        pipeline
    }

    #[tokio::test]
    async fn test_bonus_report_pages_departments_by_award() {
        let router = PartitionRouter::new(4);
        dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.dept".into(),
                documents: vec![
                    doc! { "deptno": 10, "dname": "ACCOUNTING", "loc": "NEW YORK" },
                    doc! { "deptno": 20, "dname": "RESEARCH", "loc": "DALLAS" },
                    doc! { "deptno": 30, "dname": "SALES", "loc": "CHICAGO" },
                    doc! { "deptno": 40, "dname": "OPERATIONS", "loc": "BOSTON" },
                ],
            },
        )
        .await;
        dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.emp".into(),
                documents: vec![
                    doc! { "empno": 7369, "ename": "SMITH", "job": "CLERK", "sal": 800, "deptno": 20 },
                    doc! { "empno": 7499, "ename": "ALLEN", "job": "SALESMAN", "sal": 1600, "deptno": 30 },
                    doc! { "empno": 7521, "ename": "WARD", "job": "SALESMAN", "sal": 1250, "deptno": 30 },
                    doc! { "empno": 7654, "ename": "MARTIN", "job": "SALESMAN", "sal": 1250, "deptno": 30 },
                    doc! { "empno": 7788, "ename": "SCOTT", "job": "ANALYST", "sal": 3000, "deptno": 20 },
                    doc! { "empno": 7839, "ename": "KING", "job": "PRESIDENT", "sal": 5000, "deptno": 10 },
                    doc! { "empno": 7934, "ename": "MILLER", "job": "CLERK", "sal": 1300, "deptno": 10 },
                ],
            },
        )
        .await;
        dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.bonus".into(),
                documents: vec![
                    doc! { "empno": 7499, "ename": "ALLEN", "job": "SALESMAN", "sal": 1600, "comm": 200 },
                    doc! { "empno": 7521, "ename": "WARD", "job": "SALESMAN", "sal": 1250, "comm": 150 },
                    doc! { "empno": 7654, "ename": "MARTIN", "job": "SALESMAN", "sal": 1250, "comm": 100 },
                    doc! { "empno": 7788, "ename": "SCOTT", "job": "ANALYST", "sal": 3000, "comm": 250 },
                    doc! { "empno": 7839, "ename": "KING", "job": "PRESIDENT", "sal": 5000, "comm": 500 },
                    doc! { "empno": 7934, "ename": "MILLER", "job": "CLERK", "sal": 1300, "comm": 80 },
                ],
            },
        )
        .await;

        let page = crate::network::execute_aggregate(
            &router,
            PartitionCommand::Aggregate {
                collection: "db.emp".into(),
                pipeline: bonus_report(0, false),
                resolved: Vec::new(),
            },
        )
        .await;
        assert_eq!(page.get_f64("ok").unwrap(), 1.0, "{page:?}");
        let page = batch_of(&page);
        assert_eq!(page.len(), 2, "{page:?}");
        assert_eq!(page[0].get_str("department").unwrap(), "ACCOUNTING");
        assert_eq!(page[0].get_str("location").unwrap(), "NEW YORK");
        assert_eq!(as_number(&page[0], "awards"), 580.0);
        assert_eq!(as_number(&page[0], "payroll"), 6300.0);
        assert_eq!(as_number(&page[0], "headcount"), 2.0);
        assert_eq!(as_number(&page[0], "averageSalary"), 3150.0);
        assert_eq!(page[0].get_str("highestPaid").unwrap(), "KING");
        assert_eq!(page[0].get_str("lowestPaid").unwrap(), "MILLER");
        let band = page[0].get_document("salaryBand").unwrap();
        assert_eq!(as_number(band, "low"), 1300.0);
        assert_eq!(as_number(band, "high"), 5000.0);
        assert_eq!(
            page[0]
                .get_array("recipients")
                .unwrap()
                .iter()
                .map(|value| value.as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["KING", "MILLER"]
        );
        assert_eq!(page[1].get_str("department").unwrap(), "SALES");
        assert_eq!(as_number(&page[1], "awards"), 450.0);
        assert_eq!(page[1].get_str("highestPaid").unwrap(), "ALLEN");

        let next_page = crate::network::execute_aggregate(
            &router,
            PartitionCommand::Aggregate {
                collection: "db.emp".into(),
                pipeline: bonus_report(2, false),
                resolved: Vec::new(),
            },
        )
        .await;
        let next_page = batch_of(&next_page);
        assert_eq!(next_page.len(), 1, "{next_page:?}");
        assert_eq!(next_page[0].get_str("department").unwrap(), "RESEARCH");
        assert_eq!(as_number(&next_page[0], "awards"), 250.0);
        assert_eq!(next_page[0].get_str("highestPaid").unwrap(), "SCOTT");

        let written = crate::network::execute_aggregate(
            &router,
            PartitionCommand::Aggregate {
                collection: "db.emp".into(),
                pipeline: bonus_report(0, true),
                resolved: Vec::new(),
            },
        )
        .await;
        assert_eq!(written.get_f64("ok").unwrap(), 1.0, "{written:?}");
        assert!(batch_of(&written).is_empty());
        match dispatch(&router, PartitionCommand::find("db.bonusReport", doc! {})).await {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 2);
                assert_eq!(documents[0].get_str("department").unwrap(), "ACCOUNTING");
                assert_eq!(documents[1].get_str("department").unwrap(), "SALES");
            }
            other => panic!("expected the saved page, got {other:?}"),
        }

        let counted = crate::network::execute_aggregate(
            &router,
            PartitionCommand::Aggregate {
                collection: "db.bonusReport".into(),
                pipeline: vec![doc! { "$count": "departmentsOnPage" }],
                resolved: Vec::new(),
            },
        )
        .await;
        let counted = batch_of(&counted);
        assert_eq!(as_number(&counted[0], "departmentsOnPage"), 2.0);
    }
}
