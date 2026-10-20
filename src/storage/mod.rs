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
mod crypto;
mod disk;
mod engine;
mod heap;
mod mvcc;
mod page;
mod transaction;
mod wal;

use std::sync::Arc;
use std::thread::JoinHandle;

use bson::{doc, Document};
use crossbeam_channel::{unbounded, Receiver, Sender};
use tokio::sync::oneshot;

use crate::config::StorageConfig;
use crate::index::{Access, IndexManager};
use crate::logger::LOGGER;
use crate::search::SearchIndex;
use crate::types::{CommandEnvelope, DecaySpec, PartitionCommand, PartitionResult};

use self::engine::StorageEngine;
use self::mvcc::{validate_document, VersionStore};
use self::transaction::{TransactionManager, View, Write};
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
    /// Older committed images that an open snapshot can still read.
    versions: VersionStore,
    /// Writes that have not been committed. One partition thread, so
    /// this map needs no lock.
    transactions: TransactionManager,
    /// B-tree and hash indexes. Rebuilt from document pages at startup,
    /// then updated on every committed write. The catalog remembers
    /// which indexes exist.
    indexes: IndexManager,
    /// `data/` root. View route files and the search directories live here.
    data_dir: std::path::PathBuf,
    /// Disk Tantivy indexes for views with `kova.inverted`.
    inverted: std::collections::HashMap<String, crate::search::DiskIndex>,
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
        let data_dir = std::path::PathBuf::from(&storage.data_dir);
        let mut worker = Self {
            id,
            receiver,
            index: SearchIndex::new(),
            engine,
            partition_count,
            versions: VersionStore::new(),
            transactions: TransactionManager::new(),
            indexes: IndexManager::new(),
            data_dir,
            inverted: std::collections::HashMap::new(),
        };
        worker.open_existing_indexes();
        // Replay before building the search index so tokenization sees
        // the pages after the WAL tail, and each document is indexed once.
        for record in pending {
            let result = record
                .op
                .into_commands()
                .into_iter()
                .map(|command| worker.apply_recorded(command, false))
                .find(|result| matches!(result, PartitionResult::Error { .. }))
                .unwrap_or(PartitionResult::Created);
            if let PartitionResult::Error { message } = result {
                LOGGER.warn(format!(
                    "partition {} skipped WAL lsn {} during recovery: {}",
                    id, record.lsn, message
                ));
            }
        }
        worker.build_missing_indexes();
        worker.publish_routes();
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
        if matches!(
            command,
            PartitionCommand::BeginTransaction { .. }
                | PartitionCommand::CommitTransaction { .. }
                | PartitionCommand::AbortTransaction { .. }
                | PartitionCommand::Transactional { .. }
        ) {
            return self.execute_transaction(command);
        }
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
        if let Err(message) = self.screen_schema(&command) {
            return PartitionResult::Error { message };
        }
        if let Err(message) = self.vet_account(&command) {
            return PartitionResult::Error { message };
        }
        if let Err(message) = self.screen_drop(&command) {
            return PartitionResult::Error { message };
        }
        let images = match self.committed_images(&command) {
            Ok(images) => images,
            Err(message) => return PartitionResult::Error { message },
        };
        if let Err(message) = self.guard_indexes(&command, &images) {
            return PartitionResult::Error { message };
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
        let lsn = self.engine.last_lsn();

        let result = self.apply_recorded(command, true);
        if appended && !matches!(result, PartitionResult::Error { .. }) {
            self.observe_committed(&images, lsn);
            self.sync_indexes(&images);
        }

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
                    crate::metrics::METRICS.insert();
                }
                for document in &stored {
                    self.note_view_document(&collection, document);
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
                if let Some(view) = self.engine.heap.view(&collection).cloned() {
                    return self.read_view(
                        view,
                        &filter,
                        projection,
                        sort,
                        skip,
                        limit,
                        touch_index,
                    );
                }
                let documents = match self.planned_documents(&collection, &filter) {
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
                    Ok(documents) => {
                        if touch_index {
                            crate::metrics::METRICS.query();
                        }
                        PartitionResult::Found { documents }
                    }
                    Err(message) => PartitionResult::Error { message },
                }
            }

            PartitionCommand::Aggregate {
                collection,
                pipeline,
                resolved,
            } => {
                if let Some(view) = self.engine.heap.view(&collection).cloned() {
                    let mut full = view.pipeline;
                    full.extend(pipeline);
                    let result = self.execute_aggregate(&view.source, &full, &resolved);
                    if touch_index && !matches!(result, PartitionResult::Error { .. }) {
                        crate::metrics::METRICS.query();
                    }
                    return result;
                }
                let result = self.execute_aggregate(&collection, &pipeline, &resolved);
                if touch_index && !matches!(result, PartitionResult::Error { .. }) {
                    crate::metrics::METRICS.query();
                }
                result
            }

            PartitionCommand::Count { collection, filter } => {
                if let Err(message) = validate_filter(&filter) {
                    return PartitionResult::Error { message };
                }
                if let Some(view) = self.engine.heap.view(&collection).cloned() {
                    return self.count_view(view, &filter, touch_index);
                }
                let count = match self.indexes.access(&collection, &filter) {
                    Access::Collection => {
                        crate::metrics::METRICS.collection_scan();
                        self.engine.count_documents(&collection, &filter)
                    }
                    Access::Id { .. } | Access::Index { .. } => self
                        .planned_documents(&collection, &filter)
                        .map(|documents| documents.len() as u64),
                };
                match count {
                    Ok(count) => {
                        if touch_index {
                            crate::metrics::METRICS.query();
                        }
                        PartitionResult::Counted { count }
                    }
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
                    crate::metrics::METRICS.update();
                }
                for doc in &updated {
                    self.note_view_document(&collection, doc);
                }
                PartitionResult::Modified {
                    matched_count,
                    modified_count: updated.len() as u32,
                }
            }

            PartitionCommand::Drop { collection } => {
                if self.engine.heap.view(&collection).is_some() {
                    return self.remove_view(&collection);
                }
                if let Err(message) = self.engine.drop_collection(&collection) {
                    return PartitionResult::Error { message };
                }
                if touch_index {
                    self.indexes.drop_collection(&collection);
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
                    self.indexes.drop_database(&database);
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
                let previous = self.engine.index_specs(&collection).unwrap_or_default();
                let mut declared = previous.clone();
                let num_indexes_before = declared.len() as u32 + 1;
                merge_index_specs(&mut declared, &indexes);
                let num_indexes_after = declared.len() as u32 + 1;
                self.engine.set_indexes(&collection, declared);
                if touch_index {
                    if let Err(message) = self.install_indexes(&collection) {
                        self.engine.set_indexes(&collection, previous);
                        let _ = self.install_indexes(&collection);
                        return PartitionResult::Error { message };
                    }
                }
                PartitionResult::IndexesCreated {
                    num_indexes_before,
                    num_indexes_after,
                    created_collection,
                }
            }

            PartitionCommand::DropIndexes { collection, index } => {
                if !self.engine.contains_collection(&collection) {
                    return PartitionResult::Error {
                        message: format!("ns not found: {}", collection),
                    };
                }
                if index == "_id_" {
                    return PartitionResult::Error {
                        message: "cannot drop the _id index".into(),
                    };
                }
                let mut declared = self.engine.index_specs(&collection).unwrap_or_default();
                let n_indexes_was = declared.len() as u32 + 1;
                if index == "*" {
                    declared.clear();
                } else {
                    let before = declared.len();
                    declared.retain(|spec| spec.get_str("name") != Ok(index.as_str()));
                    if declared.len() == before {
                        return PartitionResult::Error {
                            message: format!("index not found: {index}"),
                        };
                    }
                }
                self.engine.set_indexes(&collection, declared);
                if touch_index {
                    if let Err(message) = self.indexes.drop_index(&collection, &index) {
                        return PartitionResult::Error { message };
                    }
                }
                PartitionResult::IndexesDropped { n_indexes_was }
            }

            PartitionCommand::Explain { collection, filter } => {
                if let Err(message) = validate_filter(&filter) {
                    return PartitionResult::Error { message };
                }
                let (stage, index_name) = match self.indexes.access(&collection, &filter) {
                    Access::Id { .. } => ("IDHACK".to_string(), Some("_id_".to_string())),
                    Access::Index { name, .. } => ("IXSCAN".to_string(), Some(name)),
                    Access::Collection => ("COLLSCAN".to_string(), None),
                };
                PartitionResult::Plan { stage, index_name }
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
                for view in self.engine.heap.views() {
                    let bare_source = view
                        .source
                        .strip_prefix(&prefix)
                        .unwrap_or(view.source.as_str());
                    let bare_name = view
                        .name
                        .strip_prefix(&prefix)
                        .unwrap_or(view.name.as_str());
                    if bare_name.is_empty() || bare_name.contains('.') {
                        continue;
                    }
                    let mut kova = view.kova.clone();
                    if let Some(index) = self.inverted.get(&view.name) {
                        let mut fields = doc! {};
                        for (name, kind) in index.kinds() {
                            fields.insert(name, kind.as_str());
                        }
                        kova.insert("fields", fields);
                    }
                    collections.push(doc! {
                        "name": bare_name,
                        "type": "view",
                        "options": {
                            "viewOn": bare_source,
                            "pipeline": view.pipeline.clone(),
                            "kova": kova,
                        },
                        "info": { "readOnly": true },
                    });
                }
                collections.retain(|info| document_matches(info, &filter));
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
                if let Some(view) = self.engine.heap.view(&collection).cloned() {
                    return self.search_view(
                        view,
                        &query,
                        &filter,
                        &options.facets,
                        options.fuzzy,
                        touch_index,
                    );
                }
                if !options.facets.is_empty() {
                    return PartitionResult::Error {
                        message: "facets require kova.inverted".into(),
                    };
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

                if touch_index {
                    crate::metrics::METRICS.query();
                }
                PartitionResult::Found {
                    documents: ranked.into_iter().map(|(_, doc)| doc).collect(),
                }
            }

            PartitionCommand::CollMod {
                collection,
                validator,
            } => {
                if !self.engine.contains_collection(&collection) {
                    return PartitionResult::Error {
                        message: format!("ns not found: {collection}"),
                    };
                }
                self.engine.set_validator(&collection, validator);
                PartitionResult::Created
            }

            PartitionCommand::CreateUser {
                name,
                hash,
                roles,
                trusted,
            } => match self.engine.heap.create_user(&name, &hash, &roles, trusted) {
                Ok(()) => PartitionResult::Account { disabled: None },
                Err(message) => PartitionResult::Error { message },
            },
            PartitionCommand::UpdateUser { name, enabled } => {
                match self.engine.heap.update_user(&name, enabled) {
                    Ok(()) => PartitionResult::Account {
                        disabled: if enabled { None } else { Some(name) },
                    },
                    Err(message) => PartitionResult::Error { message },
                }
            }
            PartitionCommand::GrantRoles {
                name,
                roles,
                revoke,
            } => match self.engine.heap.grant_roles(&name, &roles, revoke) {
                Ok(()) => PartitionResult::Account { disabled: None },
                Err(message) => PartitionResult::Error { message },
            },
            PartitionCommand::CreateRole { name, actions } => {
                match self.engine.heap.create_role(&name, &actions) {
                    Ok(()) => PartitionResult::Account { disabled: None },
                    Err(message) => PartitionResult::Error { message },
                }
            }
            PartitionCommand::DropRole { name } => match self.engine.heap.drop_role(&name) {
                Ok(()) => PartitionResult::Account { disabled: None },
                Err(message) => PartitionResult::Error { message },
            },
            PartitionCommand::CreateView {
                name,
                source,
                pipeline,
                kova,
            } => self.create_view(name, source, pipeline, kova),
            PartitionCommand::Authenticate { name, password } => {
                match self.engine.heap.authenticate(&name, &password) {
                    Ok(roles) => {
                        let actions = match crate::security::expand_roles(
                            &roles,
                            &self.engine.heap.custom_roles(),
                        ) {
                            Ok(actions) => actions,
                            Err(message) => return PartitionResult::Error { message },
                        };
                        PartitionResult::Authenticated {
                            user: name,
                            actions: actions
                                .iter()
                                .map(|action| action.name().to_string())
                                .collect(),
                        }
                    }
                    Err(message) => PartitionResult::Error { message },
                }
            }

            PartitionCommand::BeginTransaction { .. }
            | PartitionCommand::CommitTransaction { .. }
            | PartitionCommand::AbortTransaction { .. }
            | PartitionCommand::Transactional { .. } => PartitionResult::Error {
                message: "transaction command reached the page apply path".into(),
            },

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
                    crate::metrics::METRICS.delete();
                }
                for doc in &removed {
                    self.note_view_removal(&collection, &document_id_string(doc));
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
        let input = match self.planned_documents(collection, &filter) {
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

    /// Rebuilds Tantivy and the secondary indexes by scanning pages.
    ///
    /// Both structures live in RAM. Pages store the documents, and the
    /// catalog stores which indexes exist, so every startup walks the
    /// pages once for each structure.
    fn reindex_all(&mut self) -> Result<(), String> {
        self.indexes = IndexManager::new();
        for name in self.engine.names() {
            let specs = self.engine.index_specs(&name).unwrap_or_default();
            self.indexes.declare(&name, &specs)?;
        }
        let index = &mut self.index;
        self.engine.for_each_document(|collection, document| {
            let id = document_id_string(document);
            let fields = extract_field_values(document);
            if let Err(err) = index.add(collection, &id, &fields) {
                LOGGER.warn(format!("Failed to index document {}: {}", id, err));
            }
        })?;
        let indexes = &mut self.indexes;
        self.engine.for_each_document(|collection, document| {
            let id = document_id_string(document);
            if let Err(err) = indexes.insert(collection, &id, document) {
                LOGGER.warn(format!(
                    "Failed to update secondary index for {}: {}",
                    id, err
                ));
            }
        })
    }

    /// Runs `beginTransaction`, `commit`, `abort`, or a statement that
    /// belongs to an open transaction. Nothing here is written to a
    /// page until commit.
    fn execute_transaction(&mut self, command: PartitionCommand) -> PartitionResult {
        match command {
            PartitionCommand::BeginTransaction {
                txn_id,
                isolation,
                timeout_ms,
                ..
            } => {
                let parsed = match transaction::Isolation::parse(&isolation) {
                    Ok(parsed) => parsed,
                    Err(message) => return PartitionResult::Error { message },
                };
                match self
                    .transactions
                    .begin(txn_id, parsed, timeout_ms, self.engine.last_lsn())
                {
                    Ok(txn_id) => {
                        crate::metrics::METRICS.transaction_started();
                        LOGGER.info(format!(
                            "partition {} transaction {} begin isolation {}",
                            self.id, txn_id, isolation
                        ));
                        PartitionResult::Transaction { txn_id }
                    }
                    Err(message) => PartitionResult::Error { message },
                }
            }
            PartitionCommand::CommitTransaction { txn_id, .. } => {
                let writes = match self.transactions.commit(txn_id, &self.versions) {
                    Ok(writes) => writes,
                    Err(message) => {
                        LOGGER.warn(format!("partition {} {}", self.id, message));
                        self.versions.gc(self.transactions.oldest_snapshot());
                        return PartitionResult::Error { message };
                    }
                };
                if let Err(message) = self.apply_commit(&writes) {
                    LOGGER.error(format!(
                        "partition {} transaction {} commit failed: {}",
                        self.id, txn_id, message
                    ));
                    return PartitionResult::Error { message };
                }
                LOGGER.info(format!(
                    "partition {} transaction {} commit",
                    self.id, txn_id
                ));
                crate::metrics::METRICS.transaction_committed();
                PartitionResult::Transaction { txn_id }
            }
            PartitionCommand::AbortTransaction { txn_id, .. } => {
                match self.transactions.abort(txn_id) {
                    Ok(()) => {
                        crate::metrics::METRICS.transaction_aborted();
                        LOGGER.info(format!(
                            "partition {} transaction {} abort",
                            self.id, txn_id
                        ));
                        self.versions.gc(self.transactions.oldest_snapshot());
                        PartitionResult::Transaction { txn_id }
                    }
                    Err(message) => PartitionResult::Error { message },
                }
            }
            PartitionCommand::Transactional { txn_id, inner } => {
                self.in_transaction(txn_id, *inner)
            }
            _ => PartitionResult::Error {
                message: "transaction command reached the wrong path".into(),
            },
        }
    }

    /// Appends every staged write as one WAL record, then applies it.
    fn apply_commit(&mut self, writes: &[Write]) -> Result<(), String> {
        if writes.is_empty() {
            self.versions.gc(self.transactions.oldest_snapshot());
            return Ok(());
        }
        self.rehearse_writes(writes)?;
        let ops = writes.iter().map(wal_op_for).collect::<Vec<_>>();
        self.engine.append(&WalOp::Transaction { ops })?;
        let lsn = self.engine.last_lsn();
        for write in writes {
            let result = self.apply_recorded(command_for(write), true);
            if let PartitionResult::Error { message } = result {
                return Err(message);
            }
        }
        for write in writes {
            self.versions.note(
                &write.collection,
                &write.id,
                lsn,
                write.page_previous.clone(),
                write.after.is_none(),
            );
            if let Some(previous) = &write.page_previous {
                self.indexes.remove(&write.collection, &write.id, previous);
            }
            if let Some(after) = &write.after {
                self.indexes.insert(&write.collection, &write.id, after)?;
            }
        }
        self.versions.gc(self.transactions.oldest_snapshot());
        if self.engine.should_checkpoint() {
            self.write_checkpoint()?;
        }
        Ok(())
    }

    fn in_transaction(&mut self, txn_id: u64, command: PartitionCommand) -> PartitionResult {
        match command {
            PartitionCommand::Insert {
                collection,
                mut documents,
            } => {
                for document in &mut documents {
                    if document.get("_id").is_none() {
                        document.insert("_id", bson::Bson::ObjectId(bson::oid::ObjectId::new()));
                    }
                }
                if let Err(message) = self.validate_docs(&collection, &documents) {
                    return PartitionResult::Error { message };
                }
                let mut view = match self.collection_view(txn_id, &collection) {
                    Ok(view) => view,
                    Err(message) => return PartitionResult::Error { message },
                };
                let mut inserted = 0u32;
                for document in documents {
                    let id = document_id_string(&document);
                    if view
                        .iter()
                        .any(|existing| document_id_string(existing) == id)
                    {
                        continue;
                    }
                    let staged = self.stage_write(txn_id, &collection, &id, Some(document.clone()));
                    if let Err(message) = staged {
                        return PartitionResult::Error { message };
                    }
                    view.push(document);
                    inserted += 1;
                }
                PartitionResult::Inserted {
                    inserted_count: inserted,
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
                let documents = match self.matching(txn_id, &collection, &filter) {
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
                    Ok(documents) => {
                        crate::metrics::METRICS.query();
                        crate::metrics::METRICS.collection_scan();
                        PartitionResult::Found { documents }
                    }
                    Err(message) => PartitionResult::Error { message },
                }
            }
            PartitionCommand::Count { collection, filter } => {
                if let Err(message) = validate_filter(&filter) {
                    return PartitionResult::Error { message };
                }
                match self.matching(txn_id, &collection, &filter) {
                    Ok(documents) => {
                        crate::metrics::METRICS.query();
                        crate::metrics::METRICS.collection_scan();
                        PartitionResult::Counted {
                            count: documents.len() as u64,
                        }
                    }
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
                let view = match self.collection_view(txn_id, &collection) {
                    Ok(view) => view,
                    Err(message) => return PartitionResult::Error { message },
                };
                let mut matched = 0u32;
                let mut modified = 0u32;
                for document in view {
                    if !document_matches(&document, &filter) {
                        continue;
                    }
                    matched += 1;
                    let mut updated = document.clone();
                    if let Err(message) = apply_update(&mut updated, &update) {
                        return PartitionResult::Error { message };
                    }
                    if let Err(message) = self.validate_docs(&collection, &[updated.clone()]) {
                        return PartitionResult::Error { message };
                    }
                    let id = document_id_string(&document);
                    if let Err(message) = self.stage_write(txn_id, &collection, &id, Some(updated))
                    {
                        return PartitionResult::Error { message };
                    }
                    modified += 1;
                    if !multi {
                        break;
                    }
                }
                PartitionResult::Modified {
                    matched_count: matched,
                    modified_count: modified,
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
                let view = match self.collection_view(txn_id, &collection) {
                    Ok(view) => view,
                    Err(message) => return PartitionResult::Error { message },
                };
                let mut deleted = 0u32;
                for document in view {
                    if limit > 0 && deleted >= limit {
                        break;
                    }
                    if !document_matches(&document, &filter) {
                        continue;
                    }
                    let id = document_id_string(&document);
                    if let Err(message) = self.stage_write(txn_id, &collection, &id, None) {
                        return PartitionResult::Error { message };
                    }
                    deleted += 1;
                }
                PartitionResult::Deleted {
                    deleted_count: deleted,
                }
            }
            PartitionCommand::Aggregate {
                collection,
                pipeline,
                resolved,
            } => self.aggregate_in_transaction(txn_id, &collection, &pipeline, &resolved),
            _ => PartitionResult::Error {
                message: "this command cannot run inside a transaction".into(),
            },
        }
    }

    fn aggregate_in_transaction(
        &mut self,
        txn_id: u64,
        collection: &str,
        pipeline: &[Document],
        resolved: &[(String, Vec<Document>)],
    ) -> PartitionResult {
        if pipeline.iter().any(|stage| stage.contains_key("$out")) {
            return PartitionResult::Error {
                message: "$out is not allowed inside a transaction".into(),
            };
        }
        let documents = match self.collection_view(txn_id, collection) {
            Ok(documents) => documents,
            Err(message) => return PartitionResult::Error { message },
        };
        for document in &documents {
            let id = document_id_string(document);
            let lsn = self.base_lsn(txn_id, collection, &id);
            self.transactions.note_read(txn_id, collection, &id, lsn);
        }
        let id = self.id;
        let partitions = self.partition_count.max(1);
        let outcome = aggregate::run(documents, pipeline, &mut |namespace| {
            if let Some((_, documents)) = resolved.iter().find(|(name, _)| name == namespace) {
                return Ok(aggregate::Foreign::Ready(documents.clone()));
            }
            let owner = (fnv1a(namespace.as_bytes()) % partitions as u64) as usize;
            if owner == id {
                let documents = self.collection_view(txn_id, namespace)?;
                return Ok(aggregate::Foreign::Ready(documents));
            }
            Ok(aggregate::Foreign::Remote)
        });
        match outcome {
            Ok(aggregate::Output::Docs(documents)) => PartitionResult::Found { documents },
            Ok(aggregate::Output::Remote { namespace }) => {
                PartitionResult::NeedForeign { namespace }
            }
            Ok(aggregate::Output::Out { .. }) => PartitionResult::Error {
                message: "$out is not allowed inside a transaction".into(),
            },
            Err(message) => PartitionResult::Error { message },
        }
    }

    /// Documents the statement is allowed to see, including its own writes.
    fn collection_view(&mut self, txn_id: u64, collection: &str) -> Result<Vec<Document>, String> {
        let now = self.engine.last_lsn();
        let view = self.transactions.view(txn_id, now)?;
        let pages = self.engine.find_documents(collection, &doc! {})?;
        let mut rows: Vec<(String, Document)> = Vec::new();
        match view {
            View::Dirty => {
                for document in pages {
                    rows.push((document_id_string(&document), document));
                }
                for (id, after) in self.transactions.dirty_writes(txn_id, collection) {
                    overlay(&mut rows, id, after);
                }
            }
            View::Snapshot { lsn } => {
                for document in pages {
                    let id = document_id_string(&document);
                    if let Some(visible) =
                        self.versions.visible(collection, &id, lsn, Some(&document))
                    {
                        rows.push((id, visible));
                    }
                }
                for ghost in self.versions.ghosts(collection, lsn) {
                    let id = document_id_string(&ghost);
                    if rows.iter().all(|(seen, _)| seen != &id) {
                        rows.push((id, ghost));
                    }
                }
            }
        }
        for (id, after) in self.transactions.own_writes(txn_id, collection) {
            overlay(&mut rows, id, after);
        }
        Ok(rows.into_iter().map(|(_, document)| document).collect())
    }

    /// Filter matches, recorded for serializable validation.
    fn matching(
        &mut self,
        txn_id: u64,
        collection: &str,
        filter: &Document,
    ) -> Result<Vec<Document>, String> {
        let documents = self.collection_view(txn_id, collection)?;
        let matched: Vec<Document> = documents
            .into_iter()
            .filter(|document| document_matches(document, filter))
            .collect();
        for document in &matched {
            let id = document_id_string(document);
            let lsn = self.base_lsn(txn_id, collection, &id);
            self.transactions.note_read(txn_id, collection, &id, lsn);
        }
        Ok(matched)
    }

    fn stage_write(
        &mut self,
        txn_id: u64,
        collection: &str,
        id: &str,
        after: Option<Document>,
    ) -> Result<(), String> {
        let base_lsn = self.base_lsn(txn_id, collection, id);
        self.transactions
            .note_read(txn_id, collection, id, base_lsn);
        let page_previous = self.engine.find_by_id(collection, id)?;
        self.transactions.stage(
            txn_id,
            Write {
                collection: collection.to_string(),
                id: id.to_string(),
                base_lsn,
                page_previous,
                after,
            },
        )
    }

    fn base_lsn(&mut self, txn_id: u64, collection: &str, id: &str) -> u64 {
        let now = self.engine.last_lsn();
        let snapshot = match self.transactions.view(txn_id, now) {
            Ok(View::Snapshot { lsn }) => lsn,
            _ => now,
        };
        self.versions.visible_lsn(collection, id, snapshot)
    }

    fn validate_docs(&self, collection: &str, documents: &[Document]) -> Result<(), String> {
        let Some(schema) = self.engine.validator(collection) else {
            return Ok(());
        };
        for document in documents {
            validate_document(document, &schema)?;
        }
        Ok(())
    }

    fn vet_account(&self, command: &PartitionCommand) -> Result<(), String> {
        match command {
            PartitionCommand::CreateUser {
                name,
                roles,
                trusted,
                ..
            } => self.engine.heap.can_create_user(name, roles, *trusted),
            PartitionCommand::UpdateUser { name, enabled } => {
                self.engine.heap.can_update_user(name, *enabled)
            }
            PartitionCommand::GrantRoles {
                name,
                roles,
                revoke,
            } => self.engine.heap.can_grant_roles(name, roles, *revoke),
            PartitionCommand::CreateRole { name, actions } => {
                self.engine.heap.can_create_role(name, actions)
            }
            PartitionCommand::DropRole { name } => self.engine.heap.can_drop_role(name),
            _ => Ok(()),
        }
    }

    fn screen_drop(&self, command: &PartitionCommand) -> Result<(), String> {
        let PartitionCommand::Drop { collection } = command else {
            return Ok(());
        };
        if self.engine.heap.view(collection).is_some() {
            return Ok(());
        }
        if let Some(view) = self.engine.heap.view_reading(collection) {
            return Err(format!(
                "cannot drop {collection}; view {} reads it",
                view.name
            ));
        }
        Ok(())
    }

    /// Rejects an auto-commit write that breaks the collection validator.
    /// The check runs before the WAL append.
    fn screen_schema(&mut self, command: &PartitionCommand) -> Result<(), String> {
        match command {
            PartitionCommand::Insert {
                collection,
                documents,
            } => self.validate_docs(collection, documents),
            PartitionCommand::Update {
                collection,
                filter,
                update,
                multi,
            } => {
                let Some(schema) = self.engine.validator(collection) else {
                    return Ok(());
                };
                let mut documents = self.engine.find_documents(collection, filter)?;
                if !multi {
                    documents.truncate(1);
                }
                for document in &mut documents {
                    apply_update(document, update)?;
                    validate_document(document, &schema)?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Images that an auto-commit is about to replace. Captured before
    /// the page changes so a snapshot taken earlier can still read them.
    fn committed_images(
        &mut self,
        command: &PartitionCommand,
    ) -> Result<Vec<CommittedImage>, String> {
        match command {
            PartitionCommand::Insert {
                collection,
                documents,
            } => {
                let mut images = Vec::new();
                for document in documents {
                    let id = document_id_string(document);
                    if self.engine.find_by_id(collection, &id)?.is_some() {
                        continue;
                    }
                    images.push(CommittedImage {
                        collection: collection.clone(),
                        id,
                        previous: None,
                        after: Some(document.clone()),
                        deleted: false,
                    });
                }
                Ok(images)
            }
            PartitionCommand::Update {
                collection,
                filter,
                update,
                multi,
            } => {
                let mut documents = self.engine.find_documents(collection, filter)?;
                if !multi {
                    documents.truncate(1);
                }
                let mut images = Vec::new();
                for document in documents {
                    let id = document_id_string(&document);
                    let mut after = document.clone();
                    apply_update(&mut after, update)?;
                    images.push(CommittedImage {
                        collection: collection.clone(),
                        id,
                        previous: Some(document),
                        after: Some(after),
                        deleted: false,
                    });
                }
                Ok(images)
            }
            PartitionCommand::Delete {
                collection,
                filter,
                limit,
            } => {
                let mut documents = self.engine.find_documents(collection, filter)?;
                if *limit > 0 {
                    documents.truncate(*limit as usize);
                }
                Ok(images_of(collection, documents, true))
            }
            PartitionCommand::Drop { collection } => {
                let documents = self.engine.find_documents(collection, &doc! {})?;
                Ok(images_of(collection, documents, true))
            }
            PartitionCommand::DropDatabase { database } => {
                let prefix = format!("{database}.");
                let names = self.engine.names();
                let mut images = Vec::new();
                for name in names {
                    let owned =
                        name.starts_with(&prefix) || (database == "test" && !name.contains('.'));
                    if !owned {
                        continue;
                    }
                    let documents = self.engine.find_documents(&name, &doc! {})?;
                    images.extend(images_of(&name, documents, true));
                }
                Ok(images)
            }
            _ => Ok(Vec::new()),
        }
    }

    fn observe_committed(&mut self, images: &[CommittedImage], lsn: u64) {
        for image in images {
            self.versions.note(
                &image.collection,
                &image.id,
                lsn,
                image.previous.clone(),
                image.deleted,
            );
        }
        self.versions.gc(self.transactions.oldest_snapshot());
    }

    /// Flushes dirty pages and truncates the WAL they cover.
    fn write_checkpoint(&mut self) -> Result<(), String> {
        self.engine.checkpoint_pages()
    }

    /// Rejects a unique violation before the WAL is appended.
    fn guard_indexes(
        &mut self,
        command: &PartitionCommand,
        images: &[CommittedImage],
    ) -> Result<(), String> {
        self.rehearse_images(images)?;
        self.preview_indexes(command)
    }

    fn rehearse_images(&self, images: &[CommittedImage]) -> Result<(), String> {
        let mut grouped: std::collections::HashMap<
            String,
            Vec<(String, Option<Document>, Option<Document>)>,
        > = std::collections::HashMap::new();
        for image in images {
            grouped.entry(image.collection.clone()).or_default().push((
                image.id.clone(),
                image.previous.clone(),
                image.after.clone(),
            ));
        }
        for (collection, changes) in grouped {
            self.indexes.rehearse(&collection, &changes)?;
        }
        Ok(())
    }

    fn rehearse_writes(&self, writes: &[Write]) -> Result<(), String> {
        let mut grouped: std::collections::HashMap<
            String,
            Vec<(String, Option<Document>, Option<Document>)>,
        > = std::collections::HashMap::new();
        for write in writes {
            grouped.entry(write.collection.clone()).or_default().push((
                write.id.clone(),
                write.page_previous.clone(),
                write.after.clone(),
            ));
        }
        for (collection, changes) in grouped {
            self.indexes.rehearse(&collection, &changes)?;
        }
        Ok(())
    }

    fn preview_indexes(&mut self, command: &PartitionCommand) -> Result<(), String> {
        let PartitionCommand::CreateIndexes {
            collection,
            indexes,
        } = command
        else {
            return Ok(());
        };
        let mut declared = self.engine.index_specs(collection).unwrap_or_default();
        merge_index_specs(&mut declared, indexes);
        let documents = self.engine.find_documents(collection, &doc! {})?;
        let pairs = documents
            .into_iter()
            .map(|document| (document_id_string(&document), document))
            .collect::<Vec<_>>();
        self.indexes.preview(collection, &declared, &pairs)
    }

    fn install_indexes(&mut self, collection: &str) -> Result<(), String> {
        let specs = self.engine.index_specs(collection).unwrap_or_default();
        let documents = self.engine.find_documents(collection, &doc! {})?;
        let pairs = documents
            .into_iter()
            .map(|document| (document_id_string(&document), document))
            .collect::<Vec<_>>();
        self.indexes.declare(collection, &specs)?;
        self.indexes.rebuild(collection, &pairs)
    }

    fn sync_indexes(&mut self, images: &[CommittedImage]) {
        for image in images {
            if let Some(previous) = &image.previous {
                self.indexes.remove(&image.collection, &image.id, previous);
            }
            if let Some(after) = &image.after {
                if let Err(message) = self.indexes.insert(&image.collection, &image.id, after) {
                    LOGGER.warn(format!(
                        "partition {} index update for {} failed: {}",
                        self.id, image.id, message
                    ));
                }
            }
        }
    }

    fn read_view(
        &mut self,
        view: crate::storage::heap::ViewSpec,
        filter: &Document,
        projection: Option<Document>,
        sort: Option<Document>,
        skip: u64,
        limit: Option<u64>,
        touch_index: bool,
    ) -> PartitionResult {
        let mut pipeline = view.pipeline;
        if !filter.is_empty() {
            pipeline.push(doc! { "$match": filter.clone() });
        }
        if let Some(projection) = projection {
            pipeline.push(doc! { "$project": projection });
        }
        if let Some(sort) = sort {
            pipeline.push(doc! { "$sort": sort });
        }
        if skip > 0 {
            pipeline.push(doc! { "$skip": skip as i64 });
        }
        if let Some(limit) = limit {
            pipeline.push(doc! { "$limit": limit as i64 });
        }
        let result = self.execute_aggregate(&view.source, &pipeline, &[]);
        if touch_index && !matches!(result, PartitionResult::Error { .. }) {
            crate::metrics::METRICS.query();
        }
        result
    }

    fn count_view(
        &mut self,
        view: crate::storage::heap::ViewSpec,
        filter: &Document,
        touch_index: bool,
    ) -> PartitionResult {
        let mut pipeline = view.pipeline;
        if !filter.is_empty() {
            pipeline.push(doc! { "$match": filter.clone() });
        }
        match self.execute_aggregate(&view.source, &pipeline, &[]) {
            PartitionResult::Found { documents } => {
                if touch_index {
                    crate::metrics::METRICS.query();
                }
                PartitionResult::Counted {
                    count: documents.len() as u64,
                }
            }
            PartitionResult::Error { message } => PartitionResult::Error { message },
            other => PartitionResult::Error {
                message: format!("view count failed: {other:?}"),
            },
        }
    }

    fn search_view(
        &mut self,
        view: crate::storage::heap::ViewSpec,
        query: &str,
        filter: &Document,
        facets: &Document,
        fuzzy: f64,
        touch_index: bool,
    ) -> PartitionResult {
        if !facets.is_empty() && !view.inverted() {
            return PartitionResult::Error {
                message: "facets require kova.inverted".into(),
            };
        }
        if view.inverted() && !facets.is_empty() {
            let ready = self
                .inverted
                .get(&view.name)
                .is_some_and(|index| index.fast_ready());
            if !ready {
                if let Err(message) = self.rebuild_inverted(&view) {
                    return PartitionResult::Error { message };
                }
            }
        }
        let (hits, facet_doc) = if view.inverted() {
            let Some(index) = self.inverted.get_mut(&view.name) else {
                return PartitionResult::Error {
                    message: format!("inverted index for {} is not open", view.name),
                };
            };
            let hits = match index.search_text(query, fuzzy) {
                Ok(hits) => hits,
                Err(message) => return PartitionResult::Error { message },
            };
            let facet_doc = if facets.is_empty() {
                None
            } else {
                match index.aggregate(query, facets, fuzzy) {
                    Ok(facets) => Some(facets),
                    Err(message) => return PartitionResult::Error { message },
                }
            };
            (hits, facet_doc)
        } else {
            match self.index.search(&view.source, query, &[]) {
                Ok(hits) => (
                    hits.into_iter().map(|hit| (hit.id, hit.score)).collect(),
                    None,
                ),
                Err(message) => return PartitionResult::Error { message },
            }
        };
        let mut ranked = Vec::new();
        for (id, score) in hits {
            let Some(document) = (match self.engine.find_by_id(&view.source, &id) {
                Ok(document) => document,
                Err(message) => return PartitionResult::Error { message },
            }) else {
                continue;
            };
            let passed = match self.project_view(&view.pipeline, document) {
                Ok(document) => document,
                Err(message) => return PartitionResult::Error { message },
            };
            let Some(document) = passed else {
                continue;
            };
            if document_matches(&document, filter) {
                ranked.push((score, document));
            }
        }
        ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        if touch_index {
            crate::metrics::METRICS.query();
        }
        let documents = ranked.into_iter().map(|(_, document)| document).collect();
        match facet_doc {
            Some(facets) => PartitionResult::Faceted { documents, facets },
            None => PartitionResult::Found { documents },
        }
    }

    fn create_view(
        &mut self,
        name: String,
        source: String,
        pipeline: Vec<Document>,
        kova: Document,
    ) -> PartitionResult {
        if let Err(message) = aggregate::validate_pipeline(&pipeline) {
            return PartitionResult::Error { message };
        }
        if pipeline.iter().any(|stage| stage.contains_key("$out")) {
            return PartitionResult::Error {
                message: "a view cannot use $out".into(),
            };
        }
        let inverted = match kova.get("inverted") {
            None => false,
            Some(bson::Bson::Boolean(value)) => *value,
            Some(_) => {
                return PartitionResult::Error {
                    message: "kova.inverted must be a boolean".into(),
                };
            }
        };
        if inverted {
            if let Err(message) = inverted_pipeline(&pipeline) {
                return PartitionResult::Error { message };
            }
        }
        let spec = crate::storage::heap::ViewSpec {
            name: name.clone(),
            source: source.clone(),
            pipeline,
            kova,
        };
        if let Err(message) = self.engine.heap.insert_view(spec.clone()) {
            return PartitionResult::Error { message };
        }
        if let Err(message) = write_view_route(&self.data_dir, &name, &source) {
            return PartitionResult::Error { message };
        }
        if inverted {
            if let Err(message) = self.rebuild_inverted(&spec) {
                return PartitionResult::Error { message };
            }
        }
        PartitionResult::Created
    }

    fn remove_view(&mut self, name: &str) -> PartitionResult {
        let spec = match self.engine.heap.remove_view(name) {
            Ok(spec) => spec,
            Err(message) => return PartitionResult::Error { message },
        };
        let _ = std::fs::remove_file(view_route_path(&self.data_dir, name));
        self.inverted.remove(name);
        if spec.inverted() {
            let _ = std::fs::remove_dir_all(index_path(&self.engine, name));
        }
        PartitionResult::Dropped {
            collection: name.to_string(),
        }
    }

    fn open_existing_indexes(&mut self) {
        let views: Vec<_> = self
            .engine
            .heap
            .views()
            .iter()
            .filter(|view| view.inverted())
            .cloned()
            .collect();
        for view in views {
            let path = index_path(&self.engine, &view.name);
            if !path.join("meta.json").exists() {
                continue;
            }
            match crate::search::DiskIndex::open(&path) {
                Ok(index) if index.fast_ready() => {
                    self.inverted.insert(view.name, index);
                }
                Ok(_) => LOGGER.info(format!(
                    "partition {} inverted index {} will be rebuilt for facets",
                    self.id, view.name
                )),
                Err(message) => LOGGER.warn(format!(
                    "partition {} inverted index {}: {message}",
                    self.id, view.name
                )),
            }
        }
    }

    fn build_missing_indexes(&mut self) {
        let views: Vec<_> = self
            .engine
            .heap
            .views()
            .iter()
            .filter(|view| view.inverted() && !self.inverted.contains_key(&view.name))
            .cloned()
            .collect();
        for view in views {
            if let Err(message) = self.rebuild_inverted(&view) {
                LOGGER.warn(format!(
                    "partition {} inverted index {}: {message}",
                    self.id, view.name
                ));
            }
        }
    }

    fn publish_routes(&self) {
        for view in self.engine.heap.views() {
            if let Err(message) = write_view_route(&self.data_dir, &view.name, &view.source) {
                LOGGER.warn(format!(
                    "partition {} view route {}: {message}",
                    self.id, view.name
                ));
            }
        }
    }

    fn rebuild_inverted(&mut self, view: &crate::storage::heap::ViewSpec) -> Result<(), String> {
        let docs = self.view_documents(view)?;
        let path = index_path(&self.engine, &view.name);
        let mut index = if let Some(index) = self.inverted.remove(&view.name) {
            index
        } else {
            crate::search::DiskIndex::open(&path)?
        };
        index.rebuild(&docs)?;
        self.inverted.insert(view.name.clone(), index);
        Ok(())
    }

    fn view_documents(
        &mut self,
        view: &crate::storage::heap::ViewSpec,
    ) -> Result<Vec<(String, Document)>, String> {
        let input = self.engine.find_documents(&view.source, &doc! {})?;
        let output = match aggregate::run(input, &view.pipeline, &mut |_| {
            Ok(aggregate::Foreign::Remote)
        })? {
            aggregate::Output::Docs(documents) => documents,
            aggregate::Output::Remote { namespace } => {
                return Err(format!("view pipeline needs {namespace}"));
            }
            aggregate::Output::Out { .. } => return Err("a view cannot use $out".into()),
        };
        let mut pairs = Vec::with_capacity(output.len());
        for document in output {
            if document.get("_id").is_none() {
                return Err("an inverted view must keep _id".into());
            }
            let id = document_id_string(&document);
            pairs.push((id, document));
        }
        Ok(pairs)
    }

    fn project_view(
        &mut self,
        pipeline: &[Document],
        document: Document,
    ) -> Result<Option<Document>, String> {
        match aggregate::run(vec![document], pipeline, &mut |_| {
            Ok(aggregate::Foreign::Remote)
        })? {
            aggregate::Output::Docs(mut documents) => Ok(documents.pop()),
            aggregate::Output::Remote { namespace } => {
                Err(format!("view pipeline needs {namespace}"))
            }
            aggregate::Output::Out { .. } => Err("a view cannot use $out".into()),
        }
    }

    fn note_view_document(&mut self, collection: &str, document: &Document) {
        let id = document_id_string(document);
        let views: Vec<_> = self
            .engine
            .heap
            .views()
            .iter()
            .filter(|view| view.inverted() && view.source == collection)
            .cloned()
            .collect();
        for view in views {
            let passed = match self.project_view(&view.pipeline, document.clone()) {
                Ok(passed) => passed,
                Err(message) => {
                    LOGGER.warn(format!("view {} skipped a write: {message}", view.name));
                    continue;
                }
            };
            let grew = {
                let Some(index) = self.inverted.get_mut(&view.name) else {
                    continue;
                };
                match passed {
                    Some(projected) => match index.upsert(&id, &projected) {
                        Ok(grew) => grew,
                        Err(message) => {
                            LOGGER.warn(format!("view {} index: {message}", view.name));
                            continue;
                        }
                    },
                    None => {
                        if let Err(message) = index.remove(&id) {
                            LOGGER.warn(format!("view {} index: {message}", view.name));
                        }
                        continue;
                    }
                }
            };
            if grew {
                if let Err(message) = self.rebuild_inverted(&view) {
                    LOGGER.warn(format!("view {} rebuild: {message}", view.name));
                }
            }
        }
    }

    fn note_view_removal(&mut self, collection: &str, id: &str) {
        let names: Vec<String> = self
            .engine
            .heap
            .views()
            .iter()
            .filter(|view| view.inverted() && view.source == collection)
            .map(|view| view.name.clone())
            .collect();
        for name in names {
            if let Some(index) = self.inverted.get_mut(&name) {
                if let Err(message) = index.remove(id) {
                    LOGGER.warn(format!("view {name} index: {message}"));
                }
            }
        }
    }

    /// Reads through the index when one covers `filter`.
    ///
    /// An id lookup and an index scan still read pages. The index only
    /// chooses which ids to keep. A transaction snapshot does not use
    /// this path: it reads the version chain instead.
    fn planned_documents(
        &mut self,
        collection: &str,
        filter: &Document,
    ) -> Result<Vec<Document>, String> {
        match self.indexes.access(collection, filter) {
            Access::Collection => {
                crate::metrics::METRICS.collection_scan();
                self.engine.find_documents(collection, filter)
            }
            Access::Id { id } => {
                crate::metrics::METRICS.index_lookup();
                let mut documents = Vec::new();
                if let Some(document) = self.engine.find_by_id(collection, &id)? {
                    if document_matches(&document, filter) {
                        documents.push(document);
                    }
                }
                Ok(documents)
            }
            Access::Index { ids, .. } => {
                crate::metrics::METRICS.index_lookup();
                self.engine.find_ids(collection, &ids, filter)
            }
        }
    }
}

/// A document an auto-commit is about to replace, plus the image it
/// will leave behind. Captured before the page changes.
struct CommittedImage {
    collection: String,
    id: String,
    previous: Option<Document>,
    /// The document after the write. `None` on delete.
    after: Option<Document>,
    deleted: bool,
}

fn merge_index_specs(declared: &mut Vec<Document>, incoming: &[Document]) {
    for index in incoming {
        let name = index
            .get_str("name")
            .ok()
            .map(str::to_string)
            .unwrap_or_else(|| format!("kova_index_{}", declared.len()));
        let mut spec = index.clone();
        spec.insert("name", name.clone());
        spec.entry("v".to_string()).or_insert(bson::Bson::Int32(2));
        match declared
            .iter()
            .position(|existing| existing.get_str("name") == Ok(name.as_str()))
        {
            Some(position) => declared[position] = spec,
            None => declared.push(spec),
        }
    }
}

fn images_of(collection: &str, documents: Vec<Document>, deleted: bool) -> Vec<CommittedImage> {
    documents
        .into_iter()
        .map(|document| {
            let id = document_id_string(&document);
            CommittedImage {
                collection: collection.to_string(),
                id,
                previous: Some(document),
                after: None,
                deleted,
            }
        })
        .collect()
}

fn overlay(rows: &mut Vec<(String, Document)>, id: String, after: Option<Document>) {
    if let Some(after) = after {
        if let Some(slot) = rows.iter_mut().find(|(seen, _)| seen == &id) {
            *slot = (id, after);
        } else {
            rows.push((id, after));
        }
    } else {
        rows.retain(|(seen, _)| seen != &id);
    }
}

fn id_filter(document: &Document) -> Document {
    match document.get("_id") {
        Some(id) => doc! { "_id": id.clone() },
        None => doc! {},
    }
}

fn wal_op_for(write: &Write) -> WalOp {
    match &write.after {
        Some(after) if write.page_previous.is_none() => WalOp::Insert {
            collection: write.collection.clone(),
            documents: vec![after.clone()],
        },
        Some(after) => WalOp::Update {
            collection: write.collection.clone(),
            filter: id_filter(after),
            update: after.clone(),
            multi: false,
        },
        None => WalOp::Delete {
            collection: write.collection.clone(),
            filter: id_filter(write.page_previous.as_ref().unwrap_or(&Document::new())),
            limit: 1,
        },
    }
}

fn command_for(write: &Write) -> PartitionCommand {
    match wal_op_for(write) {
        WalOp::Insert {
            collection,
            documents,
        } => PartitionCommand::Insert {
            collection,
            documents,
        },
        WalOp::Update {
            collection,
            filter,
            update,
            multi,
        } => PartitionCommand::Update {
            collection,
            filter,
            update,
            multi,
        },
        WalOp::Delete {
            collection,
            filter,
            limit,
        } => PartitionCommand::Delete {
            collection,
            filter,
            limit,
        },
        _ => PartitionCommand::Count {
            collection: write.collection.clone(),
            filter: doc! {},
        },
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
    data_dir: std::path::PathBuf,
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
                data_dir: std::path::PathBuf::from(&storage.data_dir),
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
        let named = command.collection();
        let routed = if named.is_empty() {
            String::new()
        } else {
            std::fs::read_to_string(view_route_path(&self.inner.data_dir, named))
                .ok()
                .map(|source| source.trim().to_string())
                .filter(|source| !source.is_empty())
                .unwrap_or_else(|| named.to_string())
        };
        let index = if command.is_account() {
            0
        } else {
            self.partition_index(&routed)
        };

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

fn view_route_path(data_dir: &std::path::Path, name: &str) -> std::path::PathBuf {
    let safe = name.replace(['/', '\\'], "_");
    data_dir.join("view-routes").join(safe)
}

fn write_view_route(data_dir: &std::path::Path, name: &str, source: &str) -> Result<(), String> {
    let path = view_route_path(data_dir, name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| format!("view route: {err}"))?;
    }
    std::fs::write(path, source).map_err(|err| format!("view route: {err}"))
}

fn index_path(engine: &StorageEngine, name: &str) -> std::path::PathBuf {
    let safe = name.replace(['/', '\\'], "_");
    engine.root().join("search").join(safe)
}

fn inverted_pipeline(pipeline: &[Document]) -> Result<(), String> {
    for stage in pipeline {
        let Some(name) = stage.keys().next() else {
            return Err("a view stage is empty".into());
        };
        match name.as_str() {
            "$match" => {}
            "$project" => {
                let spec = stage
                    .get_document("$project")
                    .map_err(|_| "$project requires a document".to_string())?;
                if hides_id(spec) {
                    return Err("an inverted view must keep _id".into());
                }
            }
            other => {
                return Err(format!(
                    "an inverted view only allows $match and $project, found {other}"
                ));
            }
        }
    }
    Ok(())
}

fn hides_id(spec: &Document) -> bool {
    match spec.get("_id") {
        Some(bson::Bson::Int32(0) | bson::Bson::Int64(0) | bson::Bson::Boolean(false)) => true,
        Some(bson::Bson::Double(number)) if *number == 0.0 => true,
        _ => false,
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
                    ..SearchOptions::default()
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
                    ..SearchOptions::default()
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
            encryption_key: None,
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
    async fn test_users_survive_restart_and_bad_passwords_match() {
        let dir = scratch_dir();
        let config = durable_config(&dir, true);
        let root_hash = crate::security::hash_password("secret").unwrap();
        let ada_hash = crate::security::hash_password("ada-secret").unwrap();
        {
            let router = PartitionRouter::with_storage(1, config.clone());
            match dispatch(
                &router,
                PartitionCommand::CreateUser {
                    name: "ada".into(),
                    hash: ada_hash.clone(),
                    roles: vec!["read".into()],
                    trusted: true,
                },
            )
            .await
            {
                PartitionResult::Error { message } => {
                    assert!(message.contains("root"), "{message}");
                }
                other => panic!("expected Error, got {other:?}"),
            }

            match dispatch(
                &router,
                PartitionCommand::CreateUser {
                    name: "root".into(),
                    hash: root_hash,
                    roles: vec!["root".into()],
                    trusted: false,
                },
            )
            .await
            {
                PartitionResult::Account { disabled: None } => {}
                other => panic!("expected Account, got {other:?}"),
            }

            match dispatch(
                &router,
                PartitionCommand::CreateRole {
                    name: "analyst".into(),
                    actions: vec!["dropDatabase".into()],
                },
            )
            .await
            {
                PartitionResult::Error { message } => {
                    assert!(message.contains("dropDatabase"), "{message}");
                }
                other => panic!("expected Error, got {other:?}"),
            }

            dispatch(
                &router,
                PartitionCommand::CreateRole {
                    name: "analyst".into(),
                    actions: vec!["find".into(), "viewStatus".into()],
                },
            )
            .await;
            dispatch(
                &router,
                PartitionCommand::CreateUser {
                    name: "ada".into(),
                    hash: ada_hash,
                    roles: vec!["read".into()],
                    trusted: true,
                },
            )
            .await;
            dispatch(
                &router,
                PartitionCommand::GrantRoles {
                    name: "ada".into(),
                    roles: vec!["analyst".into()],
                    revoke: false,
                },
            )
            .await;

            let bad = dispatch(
                &router,
                PartitionCommand::Authenticate {
                    name: "ada".into(),
                    password: "nope".into(),
                },
            )
            .await;
            let unknown = dispatch(
                &router,
                PartitionCommand::Authenticate {
                    name: "nobody".into(),
                    password: "nope".into(),
                },
            )
            .await;
            let (
                PartitionResult::Error { message: bad },
                PartitionResult::Error { message: unknown },
            ) = (bad, unknown)
            else {
                panic!("bad and unknown passwords must fail");
            };
            assert_eq!(bad, "authentication failed");
            assert_eq!(bad, unknown);

            match dispatch(
                &router,
                PartitionCommand::Authenticate {
                    name: "ada".into(),
                    password: "ada-secret".into(),
                },
            )
            .await
            {
                PartitionResult::Authenticated { user, actions } => {
                    assert_eq!(user, "ada");
                    assert!(actions.iter().any(|action| action == "find"));
                    assert!(actions.iter().any(|action| action == "viewStatus"));
                    assert!(!actions.iter().any(|action| action == "insert"));
                }
                other => panic!("expected Authenticated, got {other:?}"),
            }

            dispatch(
                &router,
                PartitionCommand::UpdateUser {
                    name: "ada".into(),
                    enabled: false,
                },
            )
            .await;
            match dispatch(
                &router,
                PartitionCommand::Authenticate {
                    name: "ada".into(),
                    password: "ada-secret".into(),
                },
            )
            .await
            {
                PartitionResult::Error { message } => assert_eq!(message, "authentication failed"),
                other => panic!("expected Error, got {other:?}"),
            }

            match dispatch(
                &router,
                PartitionCommand::UpdateUser {
                    name: "root".into(),
                    enabled: false,
                },
            )
            .await
            {
                PartitionResult::Error { message } => {
                    assert!(message.contains("last root"), "{message}");
                }
                other => panic!("expected Error, got {other:?}"),
            }

            match dispatch(
                &router,
                PartitionCommand::DropRole {
                    name: "read".into(),
                },
            )
            .await
            {
                PartitionResult::Error { message } => {
                    assert!(message.contains("built-in"), "{message}");
                }
                other => panic!("expected Error, got {other:?}"),
            }
        }

        let router = PartitionRouter::with_storage(1, config);
        match dispatch(
            &router,
            PartitionCommand::Authenticate {
                name: "root".into(),
                password: "secret".into(),
            },
        )
        .await
        {
            PartitionResult::Authenticated { actions, .. } => {
                assert_eq!(actions.len(), 11);
            }
            other => panic!("expected Authenticated, got {other:?}"),
        }
        match dispatch(
            &router,
            PartitionCommand::Authenticate {
                name: "ada".into(),
                password: "ada-secret".into(),
            },
        )
        .await
        {
            PartitionResult::Error { message } => assert_eq!(message, "authentication failed"),
            other => panic!("expected Error, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn test_view_filters_and_inverted_index_survives_restart() {
        let dir = scratch_dir();
        let config = durable_config(&dir, true);
        let router = PartitionRouter::with_storage(8, config.clone());
        let source = "db.articles";
        let view = (0..64)
            .map(|n| format!("db.v{n}"))
            .find(|name| router.partition_index(name) != router.partition_index(source))
            .expect("a view name on another partition");

        dispatch(
            &router,
            PartitionCommand::Insert {
                collection: source.into(),
                documents: vec![
                    doc! { "_id": "1", "title": "rust engine", "sal": 10i32, "published": true },
                    doc! { "_id": "2", "title": "other", "sal": 3i32, "published": false },
                ],
            },
        )
        .await;

        let rejected = dispatch(
            &router,
            PartitionCommand::CreateView {
                name: "db.grouped".into(),
                source: source.into(),
                pipeline: vec![doc! { "$group": { "_id": "$published", "n": { "$sum": 1 } } }],
                kova: doc! { "inverted": true },
            },
        )
        .await;
        match rejected {
            PartitionResult::Error { message } => assert!(message.contains("$group"), "{message}"),
            other => panic!("expected Error, got {other:?}"),
        }

        match dispatch(
            &router,
            PartitionCommand::CreateView {
                name: view.clone(),
                source: source.into(),
                pipeline: vec![doc! { "$match": { "published": true } }],
                kova: doc! { "inverted": true, "later": "kept" },
            },
        )
        .await
        {
            PartitionResult::Created => {}
            other => panic!("expected Created, got {other:?}"),
        }

        match dispatch(&router, PartitionCommand::find(&view, doc! {})).await {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 1);
                assert_eq!(documents[0].get_str("title").unwrap(), "rust engine");
            }
            other => panic!("expected Found, got {other:?}"),
        }

        let mut listed = Vec::new();
        for rx in router.broadcast(PartitionCommand::ListCollections {
            database: "db".into(),
            filter: doc! {},
        }) {
            if let PartitionResult::ListedCollections { collections } =
                rx.await.expect("worker replied")
            {
                listed.extend(collections);
            }
        }
        let info = listed
            .iter()
            .find(|info| info.get_str("name") == Ok(view.trim_start_matches("db.")))
            .unwrap_or_else(|| panic!("view missing from {listed:?}"));
        assert_eq!(info.get_str("type").unwrap(), "view");
        let options = info.get_document("options").unwrap();
        assert_eq!(options.get_str("viewOn").unwrap(), "articles");
        let kova = options.get_document("kova").unwrap();
        assert_eq!(kova.get_str("later").unwrap(), "kept");
        let fields = kova.get_document("fields").unwrap();
        assert_eq!(fields.get_str("title").unwrap(), "text");
        assert_eq!(fields.get_str("sal").unwrap(), "i64");
        assert_eq!(fields.get_str("published").unwrap(), "bool");

        match dispatch(
            &router,
            PartitionCommand::Search {
                collection: view.clone(),
                query: "rust".into(),
                filter: doc! {},
                options: SearchOptions {
                    facets: doc! {
                        "title": { "terms": { "field": "title" } },
                        "sal": { "range": { "field": "sal", "ranges": [ { "to": 5 }, { "from": 5 } ] } },
                    },
                    ..SearchOptions::default()
                },
            },
        )
        .await
        {
            PartitionResult::Faceted { documents, facets } => {
                assert_eq!(documents.len(), 1, "{documents:?}");
                assert_eq!(documents[0].get_str("_id").unwrap(), "1");
                let title = facets.get_document("title").expect("title facet");
                let buckets = title.get_array("buckets").expect("buckets");
                assert!(
                    buckets.iter().any(|bucket| {
                        bucket.as_document().is_some_and(|entry| {
                            entry.get_str("key") == Ok("rust engine")
                                && entry.get_i64("doc_count") == Ok(1)
                        })
                    }),
                    "{facets:?}"
                );
            }
            other => panic!("expected Faceted, got {other:?}"),
        }

        match dispatch(
            &router,
            PartitionCommand::Search {
                collection: view.clone(),
                query: "engien".into(),
                filter: doc! {},
                options: SearchOptions {
                    fuzzy: 0.3,
                    ..SearchOptions::default()
                },
            },
        )
        .await
        {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 1, "{documents:?}");
                assert_eq!(documents[0].get_str("title").unwrap(), "rust engine");
            }
            other => panic!("expected Found, got {other:?}"),
        }

        match dispatch(
            &router,
            PartitionCommand::Drop {
                collection: source.into(),
            },
        )
        .await
        {
            PartitionResult::Error { message } => {
                assert!(message.contains("reads it"), "{message}")
            }
            other => panic!("expected Error, got {other:?}"),
        }
        drop(router);

        let router = PartitionRouter::with_storage(8, config);
        match dispatch(
            &router,
            PartitionCommand::Search {
                collection: view,
                query: "rust".into(),
                filter: doc! {},
                options: SearchOptions::default(),
            },
        )
        .await
        {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 1, "{documents:?}");
                assert_eq!(documents[0].get_str("title").unwrap(), "rust engine");
            }
            other => panic!("expected Found, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(dir);
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

    #[tokio::test]
    async fn test_lookup_pipeline_joins_salary_grades_across_partitions() {
        let router = PartitionRouter::new(4);
        dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.emp".into(),
                documents: vec![
                    doc! { "_id": 1, "ename": "KING", "sal": 5000 },
                    doc! { "_id": 2, "ename": "SMITH", "sal": 800 },
                ],
            },
        )
        .await;
        dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.salgrade".into(),
                documents: vec![
                    doc! { "grade": 1, "losal": 700, "hisal": 1200 },
                    doc! { "grade": 5, "losal": 3001, "hisal": 9999 },
                ],
            },
        )
        .await;
        let reply = crate::network::execute_aggregate(
            &router,
            PartitionCommand::Aggregate {
                collection: "db.emp".into(),
                pipeline: vec![doc! { "$lookup": {
                    "from": "db.salgrade",
                    "let": { "salary": "$sal" },
                    "pipeline": [{
                        "$match": {
                            "$expr": {
                                "$and": [
                                    { "$gte": ["$$salary", "$losal"] },
                                    { "$lte": ["$$salary", "$hisal"] },
                                ]
                            }
                        }
                    }],
                    "as": "grade",
                } }],
                resolved: Vec::new(),
            },
        )
        .await;
        assert_eq!(reply.get_f64("ok").unwrap(), 1.0);
        let rows = batch_of(&reply);
        let grade_of = |name: &str| {
            let row = rows
                .iter()
                .find(|row| row.get_str("ename") == Ok(name))
                .unwrap();
            row.get_array("grade").unwrap()[0]
                .as_document()
                .unwrap()
                .get_i32("grade")
                .unwrap()
        };
        assert_eq!(grade_of("KING"), 5);
        assert_eq!(grade_of("SMITH"), 1);
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

    fn txn_id(result: PartitionResult) -> u64 {
        match result {
            PartitionResult::Transaction { txn_id } => txn_id,
            other => panic!("expected a transaction id, got {other:?}"),
        }
    }

    fn begin(isolation: &str, timeout_ms: Option<u64>) -> PartitionCommand {
        PartitionCommand::BeginTransaction {
            collection: "db.emp".into(),
            txn_id: 0,
            isolation: isolation.into(),
            timeout_ms,
        }
    }

    async fn sal_of(router: &PartitionRouter, txn: Option<u64>) -> i32 {
        let find = PartitionCommand::find("db.emp", doc! {});
        let find = match txn {
            Some(txn_id) => PartitionCommand::Transactional {
                txn_id,
                inner: Box::new(find),
            },
            None => find,
        };
        let result = dispatch(router, find).await;
        match result {
            PartitionResult::Found { documents } => documents[0].get_i32("sal").unwrap(),
            other => panic!("expected a document, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_snapshot_hides_later_writes_until_commit() {
        let router = PartitionRouter::new(1);
        dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.emp".into(),
                documents: vec![doc! { "_id": 1, "sal": 800 }],
            },
        )
        .await;
        let txn = txn_id(dispatch(&router, begin("snapshot", None)).await);
        dispatch(
            &router,
            PartitionCommand::Update {
                collection: "db.emp".into(),
                filter: doc! { "_id": 1 },
                update: doc! { "$set": { "sal": 900 } },
                multi: false,
            },
        )
        .await;
        assert_eq!(sal_of(&router, Some(txn)).await, 800);
        assert_eq!(sal_of(&router, None).await, 900);

        let inserted = dispatch(
            &router,
            PartitionCommand::Transactional {
                txn_id: txn,
                inner: Box::new(PartitionCommand::Insert {
                    collection: "db.emp".into(),
                    documents: vec![
                        doc! { "_id": 2, "name": "A" },
                        doc! { "_id": 3, "name": "B" },
                    ],
                }),
            },
        )
        .await;
        assert!(matches!(
            inserted,
            PartitionResult::Inserted { inserted_count: 2 }
        ));
        dispatch(
            &router,
            PartitionCommand::AbortTransaction {
                collection: "db.emp".into(),
                txn_id: txn,
            },
        )
        .await;
        match dispatch(&router, PartitionCommand::find("db.emp", doc! {})).await {
            PartitionResult::Found { documents } => assert_eq!(documents.len(), 1),
            other => panic!("abort leaked a document: {other:?}"),
        }

        let txn = txn_id(dispatch(&router, begin("snapshot", None)).await);
        dispatch(
            &router,
            PartitionCommand::Transactional {
                txn_id: txn,
                inner: Box::new(PartitionCommand::Insert {
                    collection: "db.emp".into(),
                    documents: vec![doc! { "_id": 4, "name": "KEPT" }],
                }),
            },
        )
        .await;
        dispatch(
            &router,
            PartitionCommand::CommitTransaction {
                collection: "db.emp".into(),
                txn_id: txn,
            },
        )
        .await;
        match dispatch(&router, PartitionCommand::find("db.emp", doc! { "_id": 4 })).await {
            PartitionResult::Found { documents } => {
                assert_eq!(documents[0].get_str("name").unwrap(), "KEPT")
            }
            other => panic!("commit did not publish the insert: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_write_conflict_aborts_the_later_transaction() {
        let router = PartitionRouter::new(1);
        dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.emp".into(),
                documents: vec![doc! { "_id": 1, "sal": 1 }],
            },
        )
        .await;
        let first = txn_id(dispatch(&router, begin("snapshot", None)).await);
        let second = txn_id(dispatch(&router, begin("snapshot", None)).await);
        let update = |txn_id, sal| PartitionCommand::Transactional {
            txn_id,
            inner: Box::new(PartitionCommand::Update {
                collection: "db.emp".into(),
                filter: doc! { "_id": 1 },
                update: doc! { "$set": { "sal": sal } },
                multi: false,
            }),
        };
        dispatch(&router, update(first, 2)).await;
        dispatch(&router, update(second, 3)).await;
        assert!(matches!(
            dispatch(
                &router,
                PartitionCommand::CommitTransaction {
                    collection: "db.emp".into(),
                    txn_id: first,
                },
            )
            .await,
            PartitionResult::Transaction { .. }
        ));
        match dispatch(
            &router,
            PartitionCommand::CommitTransaction {
                collection: "db.emp".into(),
                txn_id: second,
            },
        )
        .await
        {
            PartitionResult::Error { message } => {
                assert!(message.contains("write conflict"), "{message}")
            }
            other => panic!("expected a conflict, got {other:?}"),
        }
        assert_eq!(sal_of(&router, None).await, 2);
    }

    #[tokio::test]
    async fn test_read_committed_sees_the_next_statement() {
        let router = PartitionRouter::new(1);
        dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.emp".into(),
                documents: vec![doc! { "_id": 1, "sal": 1 }],
            },
        )
        .await;
        let txn = txn_id(dispatch(&router, begin("readCommitted", None)).await);
        assert_eq!(sal_of(&router, Some(txn)).await, 1);
        dispatch(
            &router,
            PartitionCommand::Update {
                collection: "db.emp".into(),
                filter: doc! { "_id": 1 },
                update: doc! { "$set": { "sal": 9 } },
                multi: false,
            },
        )
        .await;
        assert_eq!(sal_of(&router, Some(txn)).await, 9);
    }

    #[tokio::test]
    async fn test_serializable_aborts_when_a_read_changes() {
        let router = PartitionRouter::new(1);
        dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.emp".into(),
                documents: vec![doc! { "_id": "a", "n": 1 }, doc! { "_id": "b", "n": 1 }],
            },
        )
        .await;
        let reader = txn_id(dispatch(&router, begin("serializable", None)).await);
        dispatch(
            &router,
            PartitionCommand::Transactional {
                txn_id: reader,
                inner: Box::new(PartitionCommand::find("db.emp", doc! { "_id": "a" })),
            },
        )
        .await;
        let writer = txn_id(dispatch(&router, begin("snapshot", None)).await);
        dispatch(
            &router,
            PartitionCommand::Transactional {
                txn_id: writer,
                inner: Box::new(PartitionCommand::Update {
                    collection: "db.emp".into(),
                    filter: doc! { "_id": "a" },
                    update: doc! { "$set": { "n": 2 } },
                    multi: false,
                }),
            },
        )
        .await;
        dispatch(
            &router,
            PartitionCommand::CommitTransaction {
                collection: "db.emp".into(),
                txn_id: writer,
            },
        )
        .await;
        dispatch(
            &router,
            PartitionCommand::Transactional {
                txn_id: reader,
                inner: Box::new(PartitionCommand::Update {
                    collection: "db.emp".into(),
                    filter: doc! { "_id": "b" },
                    update: doc! { "$set": { "n": 2 } },
                    multi: false,
                }),
            },
        )
        .await;
        match dispatch(
            &router,
            PartitionCommand::CommitTransaction {
                collection: "db.emp".into(),
                txn_id: reader,
            },
        )
        .await
        {
            PartitionResult::Error { message } => {
                assert!(message.contains("serialization"), "{message}")
            }
            other => panic!("expected a serialization conflict, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_repeatable_read_stays_put_and_read_uncommitted_sees_dirty_writes() {
        let router = PartitionRouter::new(1);
        let repeatable = txn_id(dispatch(&router, begin("repeatableRead", None)).await);
        dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.emp".into(),
                documents: vec![doc! { "_id": 1, "sal": 1 }],
            },
        )
        .await;
        assert_eq!(sal_of(&router, Some(repeatable)).await, 1);
        dispatch(
            &router,
            PartitionCommand::Update {
                collection: "db.emp".into(),
                filter: doc! { "_id": 1 },
                update: doc! { "$set": { "sal": 5 } },
                multi: false,
            },
        )
        .await;
        assert_eq!(sal_of(&router, Some(repeatable)).await, 1);

        let reader = txn_id(dispatch(&router, begin("readUncommitted", None)).await);
        let writer = txn_id(dispatch(&router, begin("snapshot", None)).await);
        dispatch(
            &router,
            PartitionCommand::Transactional {
                txn_id: writer,
                inner: Box::new(PartitionCommand::Insert {
                    collection: "db.emp".into(),
                    documents: vec![doc! { "_id": 2, "sal": 7 }],
                }),
            },
        )
        .await;
        match dispatch(
            &router,
            PartitionCommand::Transactional {
                txn_id: reader,
                inner: Box::new(PartitionCommand::find("db.emp", doc! { "_id": 2 })),
            },
        )
        .await
        {
            PartitionResult::Found { documents } => assert_eq!(documents.len(), 1),
            other => panic!("read uncommitted missed the dirty insert: {other:?}"),
        }
        dispatch(
            &router,
            PartitionCommand::AbortTransaction {
                collection: "db.emp".into(),
                txn_id: writer,
            },
        )
        .await;
        match dispatch(
            &router,
            PartitionCommand::Transactional {
                txn_id: reader,
                inner: Box::new(PartitionCommand::find("db.emp", doc! { "_id": 2 })),
            },
        )
        .await
        {
            PartitionResult::Found { documents } => assert!(documents.is_empty()),
            other => panic!("aborted write stayed visible: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_timeout_and_validator_reject_the_write() {
        let router = PartitionRouter::new(1);
        let txn = txn_id(dispatch(&router, begin("snapshot", Some(0))).await);
        match dispatch(
            &router,
            PartitionCommand::Transactional {
                txn_id: txn,
                inner: Box::new(PartitionCommand::Insert {
                    collection: "db.emp".into(),
                    documents: vec![doc! { "_id": 1, "ename": "LATE" }],
                }),
            },
        )
        .await
        {
            PartitionResult::Error { message } => assert!(message.contains("timeout"), "{message}"),
            other => panic!("expected a timeout, got {other:?}"),
        }

        dispatch(
            &router,
            PartitionCommand::CreateCollection {
                collection: "db.emp".into(),
            },
        )
        .await;
        dispatch(
            &router,
            PartitionCommand::CollMod {
                collection: "db.emp".into(),
                validator: doc! { "required": ["ename"] },
            },
        )
        .await;
        match dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.emp".into(),
                documents: vec![doc! { "_id": 1, "sal": 1 }],
            },
        )
        .await
        {
            PartitionResult::Error { message } => assert!(message.contains("ename"), "{message}"),
            other => panic!("expected a schema error, got {other:?}"),
        }
        assert!(matches!(
            dispatch(
                &router,
                PartitionCommand::Insert {
                    collection: "db.emp".into(),
                    documents: vec![doc! { "_id": 1, "ename": "KING" }],
                },
            )
            .await,
            PartitionResult::Inserted { inserted_count: 1 }
        ));
    }

    #[tokio::test]
    async fn test_commit_survives_restart_and_abort_does_not() {
        let dir = scratch_dir();
        let config = durable_config(&dir, false);
        {
            let router = PartitionRouter::with_storage(1, config.clone());
            let txn = txn_id(dispatch(&router, begin("snapshot", None)).await);
            dispatch(
                &router,
                PartitionCommand::Transactional {
                    txn_id: txn,
                    inner: Box::new(PartitionCommand::Insert {
                        collection: "db.emp".into(),
                        documents: vec![doc! { "_id": 1, "name": "KEPT" }],
                    }),
                },
            )
            .await;
            dispatch(
                &router,
                PartitionCommand::CommitTransaction {
                    collection: "db.emp".into(),
                    txn_id: txn,
                },
            )
            .await;
            let txn = txn_id(dispatch(&router, begin("snapshot", None)).await);
            dispatch(
                &router,
                PartitionCommand::Transactional {
                    txn_id: txn,
                    inner: Box::new(PartitionCommand::Insert {
                        collection: "db.emp".into(),
                        documents: vec![doc! { "_id": 2, "name": "LOST" }],
                    }),
                },
            )
            .await;
        }

        let router = PartitionRouter::with_storage(1, config);
        match dispatch(&router, PartitionCommand::find("db.emp", doc! {})).await {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 1);
                assert_eq!(documents[0].get_str("name").unwrap(), "KEPT");
            }
            other => panic!("expected the committed document, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    async fn seed_indexed_emp(router: &PartitionRouter) {
        dispatch(
            router,
            PartitionCommand::Insert {
                collection: "db.emp".into(),
                documents: vec![
                    doc! { "_id": 1i32, "ename": "SMITH", "job": "CLERK", "sal": 800i32 },
                    doc! { "_id": 2i32, "ename": "ALLEN", "job": "SALESMAN", "sal": 1600i32 },
                    doc! { "_id": 3i32, "ename": "KING", "job": "PRESIDENT", "sal": 5000i32 },
                    doc! { "_id": 4i32, "job": "CLERK" },
                ],
            },
        )
        .await;
        let created = dispatch(
            router,
            PartitionCommand::CreateIndexes {
                collection: "db.emp".into(),
                indexes: vec![
                    doc! { "name": "sal_1", "key": { "sal": 1i32 } },
                    doc! { "name": "job_hashed", "key": { "job": "hashed" } },
                    doc! { "name": "ename_1", "key": { "ename": 1i32 }, "unique": true, "sparse": true },
                ],
            },
        )
        .await;
        assert!(
            matches!(created, PartitionResult::IndexesCreated { .. }),
            "indexes should build, got {created:?}"
        );
    }

    #[tokio::test]
    async fn test_index_range_hash_id_and_collection_scan() {
        let router = router();
        seed_indexed_emp(&router).await;

        match dispatch(
            &router,
            PartitionCommand::find("db.emp", doc! { "sal": { "$gt": 1000i32 } }),
        )
        .await
        {
            PartitionResult::Found { documents } => {
                let mut names: Vec<_> = documents
                    .iter()
                    .map(|document| document.get_str("ename").unwrap().to_string())
                    .collect();
                names.sort();
                assert_eq!(names, vec!["ALLEN".to_string(), "KING".to_string()]);
            }
            other => panic!("expected the salary range, got {other:?}"),
        }

        match dispatch(
            &router,
            PartitionCommand::find("db.emp", doc! { "job": "CLERK" }),
        )
        .await
        {
            PartitionResult::Found { documents } => {
                assert_eq!(documents.len(), 2, "the clerk without a name stays");
            }
            other => panic!("expected both clerks, got {other:?}"),
        }

        match dispatch(
            &router,
            PartitionCommand::Explain {
                collection: "db.emp".into(),
                filter: doc! { "sal": { "$gt": 1000i32 } },
            },
        )
        .await
        {
            PartitionResult::Plan { stage, index_name } => {
                assert_eq!(stage, "IXSCAN");
                assert_eq!(index_name.as_deref(), Some("sal_1"));
            }
            other => panic!("expected an index scan, got {other:?}"),
        }

        match dispatch(
            &router,
            PartitionCommand::Explain {
                collection: "db.emp".into(),
                filter: doc! { "_id": 3i32 },
            },
        )
        .await
        {
            PartitionResult::Plan { stage, index_name } => {
                assert_eq!(stage, "IDHACK");
                assert_eq!(index_name.as_deref(), Some("_id_"));
            }
            other => panic!("expected an id lookup, got {other:?}"),
        }

        match dispatch(
            &router,
            PartitionCommand::Explain {
                collection: "db.emp".into(),
                filter: doc! { "deptno": 10i32 },
            },
        )
        .await
        {
            PartitionResult::Plan { stage, index_name } => {
                assert_eq!(stage, "COLLSCAN");
                assert!(index_name.is_none());
            }
            other => panic!("expected a collection scan, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_unique_index_rejects_a_duplicate_before_the_write() {
        let router = router();
        seed_indexed_emp(&router).await;
        let rejected = dispatch(
            &router,
            PartitionCommand::Insert {
                collection: "db.emp".into(),
                documents: vec![doc! { "_id": 5i32, "ename": "SMITH", "sal": 1i32 }],
            },
        )
        .await;
        match rejected {
            PartitionResult::Error { message } => {
                assert!(message.contains("duplicate"), "{message}")
            }
            other => panic!("expected a duplicate key, got {other:?}"),
        }
        match dispatch(
            &router,
            PartitionCommand::find("db.emp", doc! { "ename": "SMITH" }),
        )
        .await
        {
            PartitionResult::Found { documents } => assert_eq!(documents.len(), 1),
            other => panic!("expected the original SMITH, got {other:?}"),
        }
        match dispatch(
            &router,
            PartitionCommand::find("db.emp", doc! { "_id": 5i32 }),
        )
        .await
        {
            PartitionResult::Found { documents } => assert!(documents.is_empty()),
            other => panic!("expected the rejected id to be absent, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_update_and_drop_keep_the_index_honest() {
        let router = router();
        seed_indexed_emp(&router).await;
        dispatch(
            &router,
            PartitionCommand::Update {
                collection: "db.emp".into(),
                filter: doc! { "ename": "SMITH" },
                update: doc! { "$set": { "sal": 2000i32 } },
                multi: false,
            },
        )
        .await;
        match dispatch(
            &router,
            PartitionCommand::find("db.emp", doc! { "sal": { "$gt": 1000i32 } }),
        )
        .await
        {
            PartitionResult::Found { documents } => {
                let mut names: Vec<_> = documents
                    .iter()
                    .map(|document| document.get_str("ename").unwrap().to_string())
                    .collect();
                names.sort();
                assert_eq!(
                    names,
                    vec!["ALLEN".to_string(), "KING".to_string(), "SMITH".to_string()]
                );
            }
            other => panic!("expected SMITH in the new range, got {other:?}"),
        }

        let dropped = dispatch(
            &router,
            PartitionCommand::DropIndexes {
                collection: "db.emp".into(),
                index: "sal_1".into(),
            },
        )
        .await;
        assert!(
            matches!(
                dropped,
                PartitionResult::IndexesDropped { n_indexes_was: 4 }
            ),
            "got {dropped:?}"
        );
        match dispatch(
            &router,
            PartitionCommand::Explain {
                collection: "db.emp".into(),
                filter: doc! { "sal": { "$gt": 1000i32 } },
            },
        )
        .await
        {
            PartitionResult::Plan { stage, .. } => assert_eq!(stage, "COLLSCAN"),
            other => panic!("expected a collection scan after the drop, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_index_survives_restart() {
        let dir = scratch_dir();
        let config = durable_config(&dir, true);
        {
            let router = PartitionRouter::with_storage(1, config.clone());
            seed_indexed_emp(&router).await;
        }
        let router = PartitionRouter::with_storage(1, config);
        match dispatch(
            &router,
            PartitionCommand::find("db.emp", doc! { "sal": { "$gt": 1000i32 } }),
        )
        .await
        {
            PartitionResult::Found { documents } => assert_eq!(documents.len(), 2),
            other => panic!("expected the range after restart, got {other:?}"),
        }
        match dispatch(
            &router,
            PartitionCommand::Explain {
                collection: "db.emp".into(),
                filter: doc! { "sal": { "$gt": 1000i32 } },
            },
        )
        .await
        {
            PartitionResult::Plan { stage, index_name } => {
                assert_eq!(stage, "IXSCAN");
                assert_eq!(index_name.as_deref(), Some("sal_1"));
            }
            other => panic!("expected the index after restart, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(dir);
    }
}
