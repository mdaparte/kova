//! Per-partition storage engine: WAL, page store, and buffer pool.
//!
//! Documents live in pages. The buffer pool caches the pages a query
//! is touching. A checkpoint flushes dirty pages and records the LSN;
//! it does not copy the collection into one BSON image.
//!
//! On-disk layout, one directory per partition:
//!
//! ```text
//! {data_dir}/partition-{id}/
//!   checkpoint.bin
//!   wal/0000000000000001.wal
//!   pages/0000000000000001.page
//! ```

use std::fs;
use std::path::{Path, PathBuf};

use crate::config::StorageConfig;
use crate::logger::LOGGER;

use super::buffer_pool::BufferPool;
use super::disk::{atomic_write, io_err, PageStore};
use super::heap::Heap;
use super::page::Page;
use super::wal::{Wal, WalOp, WalRecord};

const MANIFEST_MAGIC: u32 = 0x4348_4B50; // CHKP
const MANIFEST_VERSION: u32 = 1;

/// WAL records that still have to be applied to pages.
pub struct Recovery {
    /// Mutations with an LSN above the catalog watermark.
    pub pending: Vec<WalRecord>,
}

struct Manifest {
    checkpoint_lsn: u64,
    next_lsn: u64,
    next_page_id: u64,
    page_ids: Vec<u64>,
}

/// Durability for one partition thread.
pub struct StorageEngine {
    partition: usize,
    root: PathBuf,
    sync: bool,
    checkpoint_every: u64,
    checkpoint_on_shutdown: bool,
    appends_since_checkpoint: u64,
    manifest_path: PathBuf,
    wal: Wal,
    pub(super) pool: BufferPool,
    pub(super) disk: PageStore,
    pub(super) heap: Heap,
}

impl StorageEngine {
    /// Creates the partition directory and the empty log. [`Self::load`]
    /// reads an existing snapshot.
    pub fn open(partition: usize, config: &StorageConfig) -> Result<Self, String> {
        let partition_u32 = u32::try_from(partition)
            .map_err(|_| format!("partition id {} does not fit in u32", partition))?;
        let root = PathBuf::from(&config.data_dir).join(format!("partition-{partition}"));
        fs::create_dir_all(&root).map_err(|err| io_err(&root, err))?;
        let page_size = config.page_size as usize;
        Page::payload_capacity(page_size, config.encryption_key.is_some())?;
        let disk = PageStore::open(root.join("pages"), page_size, config.encryption_key)?;
        // The pool and the store share the directory. Each has its own
        // handle; pages are immutable once flushed, so that is safe on
        // this single thread.
        let pool_disk = PageStore::open(root.join("pages"), page_size, config.encryption_key)?;
        let pool = BufferPool::new(pool_disk, config.buffer_pool_pages)?;
        let wal = Wal::open(
            root.join("wal"),
            partition_u32,
            config.wal_fsync,
            config.wal_segment_bytes,
            config.encryption_key,
        )?;
        Ok(Self {
            partition,
            root: root.clone(),
            sync: config.wal_fsync,
            checkpoint_every: config.checkpoint_every,
            checkpoint_on_shutdown: config.checkpoint_on_shutdown,
            appends_since_checkpoint: 0,
            manifest_path: root.join("checkpoint.bin"),
            wal,
            pool,
            disk,
            heap: Heap::empty(page_size, config.encryption_key.is_some()),
        })
    }

    /// Partition directory (`data/partition-N`).
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Opens every page file, overlays the catalog, and returns the WAL
    /// tail. Page files that are not in the manifest are kept: a crash
    /// can flush a dirty page before the catalog records it.
    pub fn load(&mut self) -> Result<Recovery, String> {
        let manifest = self.read_manifest()?;
        self.wal.set_next_lsn(manifest.next_lsn);
        let ids = self.disk.list_ids()?;
        let warm = self
            .heap
            .try_load_catalog(&mut self.pool, ids.contains(&1))?;
        if !warm {
            self.heap.discover(&mut self.pool, &ids)?;
        }
        self.heap.ensure_catalog();
        self.heap.advance_next_page_id(manifest.next_page_id);

        let records = self.wal.replay()?;
        let applied = self.heap.applied_lsn();
        let pending = records
            .into_iter()
            .filter(|record| record.lsn > applied)
            .collect::<Vec<_>>();

        LOGGER.info(format!(
            "partition {} loaded catalog lsn {}, {} pending wal record(s)",
            self.partition,
            applied,
            pending.len()
        ));

        Ok(Recovery { pending })
    }

    /// Appends a mutation. The caller applies it to memory after this returns.
    pub fn append(&mut self, op: &WalOp) -> Result<u64, String> {
        let lsn = self.wal.append(op)?;
        crate::metrics::METRICS.wal_write();
        self.appends_since_checkpoint += 1;
        Ok(lsn)
    }

    /// Whether enough records have been appended to checkpoint.
    pub fn should_checkpoint(&self) -> bool {
        self.checkpoint_every > 0 && self.appends_since_checkpoint >= self.checkpoint_every
    }

    /// Whether a clean worker shutdown should snapshot memory.
    pub fn checkpoint_on_shutdown(&self) -> bool {
        self.checkpoint_on_shutdown
    }

    /// Highest LSN assigned so far.
    pub fn last_lsn(&self) -> u64 {
        self.wal.last_lsn()
    }

    /// Flushes dirty pages, records the LSN, and drops the WAL it covers.
    ///
    /// The document bodies are already in the page files. This does not
    /// build a second BSON image of the collections. Tests call this
    /// directly. The server checkpoints through [`Self::checkpoint_with_indexes`].
    #[cfg(test)]
    pub fn checkpoint_pages(&mut self) -> Result<(), String> {
        self.checkpoint_with_indexes(None)
    }

    /// Same checkpoint, and writes `index` as buffer-pool pages named by
    /// the catalog root. `index` is the B-tree and hash snapshot.
    pub fn checkpoint_with_indexes(&mut self, index: Option<&[u8]>) -> Result<(), String> {
        let lsn = self.wal.last_lsn();
        self.heap.set_applied_lsn(lsn);
        if let Some(bytes) = index {
            self.heap
                .store_index_bytes(&mut self.pool, &self.disk, bytes)?;
        }
        self.heap.publish(&mut self.pool, &self.disk)?;
        self.heap.write_root(&mut self.pool)?;
        self.pool.flush()?;
        let stats = self.pool.stats();
        LOGGER.debug(format!(
            "partition {} buffer pool hits {} misses {} evictions {} pinned {}",
            self.partition, stats.hits, stats.misses, stats.evictions, stats.pinned
        ));

        let page_ids = self.heap.live_page_ids();
        let manifest = Manifest {
            checkpoint_lsn: self.heap.applied_lsn(),
            next_lsn: self.wal.next_lsn(),
            next_page_id: self.heap.next_page_id(),
            page_ids: page_ids.clone(),
        };
        manifest.write(&self.manifest_path, self.sync)?;
        self.wal.discard_segments()?;
        self.disk.retain(&page_ids)?;
        self.appends_since_checkpoint = 0;

        LOGGER.info(format!(
            "partition {} checkpoint lsn {} ({} pages)",
            self.partition,
            self.heap.applied_lsn(),
            page_ids.len()
        ));
        Ok(())
    }

    #[cfg(test)]
    pub fn pool_stats(&self) -> super::buffer_pool::PoolStats {
        self.pool.stats()
    }

    #[cfg(test)]
    pub fn retained_documents(&self) -> usize {
        self.heap.retained_documents()
    }

    pub fn collection_count(&self) -> usize {
        self.heap.collection_count()
    }

    pub fn names(&self) -> Vec<String> {
        self.heap.names()
    }

    pub fn contains_collection(&self, collection: &str) -> bool {
        self.heap.contains(collection)
    }

    pub fn doc_count(&self, collection: &str) -> u64 {
        self.heap.doc_count(collection)
    }

    pub fn index_specs(&self, collection: &str) -> Option<Vec<bson::Document>> {
        self.heap.index_specs(collection)
    }

    pub fn index_count(&self, collection: &str) -> usize {
        self.heap.index_count(collection)
    }

    pub fn ensure_collection(&mut self, collection: &str) -> bool {
        self.heap.ensure_collection(collection)
    }

    pub fn set_indexes(&mut self, collection: &str, indexes: Vec<bson::Document>) {
        self.heap.set_indexes(collection, indexes);
    }

    pub fn declare_counter(&mut self, collection: &str, field: &str) {
        self.heap.declare_counter(collection, field);
    }

    pub fn fill_autoincrement(
        &self,
        collection: &str,
        documents: &mut [bson::Document],
        already: &[bson::Document],
    ) {
        self.heap.fill_autoincrement(collection, documents, already);
    }

    pub fn validator(&self, collection: &str) -> Option<bson::Document> {
        self.heap.validator(collection)
    }

    pub fn set_validator(&mut self, collection: &str, validator: bson::Document) {
        self.heap.set_validator(collection, validator);
    }

    pub fn insert_documents(
        &mut self,
        collection: &str,
        documents: Vec<bson::Document>,
    ) -> Result<Vec<bson::Document>, String> {
        self.heap.insert(&mut self.pool, collection, documents)
    }

    pub fn find_documents(
        &mut self,
        collection: &str,
        filter: &bson::Document,
    ) -> Result<Vec<bson::Document>, String> {
        self.heap.find(&mut self.pool, collection, filter)
    }

    pub fn count_documents(
        &mut self,
        collection: &str,
        filter: &bson::Document,
    ) -> Result<u64, String> {
        self.heap.count(&mut self.pool, collection, filter)
    }

    pub fn find_by_id(
        &mut self,
        collection: &str,
        id: &str,
    ) -> Result<Option<bson::Document>, String> {
        self.heap.find_by_id(&mut self.pool, collection, id)
    }

    /// Fetches the documents an index named. The page walk keeps those ids.
    pub fn find_ids(
        &mut self,
        collection: &str,
        ids: &[String],
        filter: &bson::Document,
    ) -> Result<Vec<bson::Document>, String> {
        self.heap.find_ids(&mut self.pool, collection, ids, filter)
    }

    /// Index snapshot pages, when a checkpoint has written them.
    pub fn read_index_bytes(&mut self) -> Result<Option<Vec<u8>>, String> {
        self.heap.read_index_bytes(&mut self.pool)
    }

    pub fn for_each_document(
        &mut self,
        visit: impl FnMut(&str, &bson::Document),
    ) -> Result<(), String> {
        self.heap.for_each_document(&mut self.pool, visit)
    }

    pub fn update_documents(
        &mut self,
        collection: &str,
        filter: &bson::Document,
        update: &bson::Document,
        multi: bool,
    ) -> Result<(u32, Vec<bson::Document>), String> {
        self.heap.update(
            &mut self.pool,
            &self.disk,
            collection,
            filter,
            update,
            multi,
        )
    }

    pub fn delete_documents(
        &mut self,
        collection: &str,
        filter: &bson::Document,
        limit: u32,
    ) -> Result<Vec<bson::Document>, String> {
        self.heap
            .delete(&mut self.pool, &self.disk, collection, filter, limit)
    }

    pub fn drop_collection(&mut self, collection: &str) -> Result<bool, String> {
        self.heap
            .drop_collection(&mut self.pool, &self.disk, collection)
    }

    pub fn drop_database(&mut self, database: &str) -> Result<Vec<String>, String> {
        self.heap
            .drop_database(&mut self.pool, &self.disk, database)
    }

    pub fn catalog_is_warm(&self) -> bool {
        self.heap.catalog_is_warm()
    }

    /// Pages read for user collections since this engine opened.
    /// Startup tests compare the counter across a reopen.
    #[cfg(test)]
    pub fn user_page_reads(&self) -> u64 {
        self.heap.user_page_reads()
    }

    pub fn applied_lsn(&self) -> u64 {
        self.heap.applied_lsn()
    }

    /// Field names and types recorded for `collection`. Tests check the
    /// catalog after a checkpoint.
    #[cfg(test)]
    pub fn field_types(&self, collection: &str) -> Vec<(String, String)> {
        self.heap.field_types(collection)
    }

    pub fn reconcile_fields(&mut self) -> Result<usize, String> {
        self.heap.reconcile_fields(&mut self.pool)
    }

    #[cfg(test)]
    pub fn clear_fields(&mut self, collection: &str) {
        self.heap.clear_fields(collection);
    }

    pub fn refresh_ids(&mut self, collection: &str) -> Result<(), String> {
        self.heap.refresh_ids(&mut self.pool, collection)
    }

    pub fn bson_size(&mut self, collection: &str) -> Result<Option<(u64, u64)>, String> {
        self.heap.bson_size(&mut self.pool, collection)
    }

    fn read_manifest(&self) -> Result<Manifest, String> {
        if !self.manifest_path.exists() {
            return Ok(Manifest {
                checkpoint_lsn: 0,
                next_lsn: 1,
                next_page_id: 1,
                page_ids: Vec::new(),
            });
        }
        Manifest::read(&self.manifest_path)
    }
}

impl Manifest {
    fn write(&self, path: &Path, _sync: bool) -> Result<(), String> {
        if self.page_ids.len() > u32::MAX as usize {
            return Err("checkpoint has too many pages".into());
        }
        let mut bytes = Vec::with_capacity(32 + self.page_ids.len() * 8);
        bytes.extend_from_slice(&MANIFEST_MAGIC.to_le_bytes());
        bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.checkpoint_lsn.to_le_bytes());
        bytes.extend_from_slice(&self.next_lsn.to_le_bytes());
        bytes.extend_from_slice(&self.next_page_id.to_le_bytes());
        bytes.extend_from_slice(&(self.page_ids.len() as u32).to_le_bytes());
        for id in &self.page_ids {
            bytes.extend_from_slice(&id.to_le_bytes());
        }
        atomic_write(path, &bytes)
    }

    fn read(path: &Path) -> Result<Self, String> {
        let bytes = fs::read(path).map_err(|err| io_err(path, err))?;
        // magic + version + three u64s + page count.
        if bytes.len() < 36 {
            return Err(format!(
                "{}: checkpoint manifest is truncated",
                path.display()
            ));
        }
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if magic != MANIFEST_MAGIC {
            return Err(format!(
                "{}: checkpoint magic {:08x} is not CHKP",
                path.display(),
                magic
            ));
        }
        let version = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        if version != MANIFEST_VERSION {
            return Err(format!(
                "{}: checkpoint version {} is not supported",
                path.display(),
                version
            ));
        }
        let checkpoint_lsn = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let next_lsn = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
        let next_page_id = u64::from_le_bytes(bytes[24..32].try_into().unwrap());
        let count = u32::from_le_bytes(bytes[32..36].try_into().unwrap()) as usize;
        if count > 1_000_000 {
            return Err(format!(
                "{}: checkpoint lists too many pages",
                path.display()
            ));
        }
        let needed = 36 + count * 8;
        if bytes.len() != needed {
            return Err(format!(
                "{}: checkpoint manifest is {} bytes, expected {}",
                path.display(),
                bytes.len(),
                needed
            ));
        }
        let mut page_ids = Vec::with_capacity(count);
        for index in 0..count {
            let start = 36 + index * 8;
            page_ids.push(u64::from_le_bytes(
                bytes[start..start + 8].try_into().unwrap(),
            ));
        }
        Ok(Self {
            checkpoint_lsn,
            next_lsn,
            next_page_id,
            page_ids,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn config(dir: &Path) -> StorageConfig {
        StorageConfig {
            data_dir: dir.to_string_lossy().into_owned(),
            page_size: 512,
            buffer_pool_pages: 2,
            wal_fsync: false,
            wal_segment_bytes: 1024 * 1024,
            checkpoint_every: 0,
            checkpoint_on_shutdown: false,
            ephemeral: false,
            encryption_key: None,
        }
    }

    fn scratch() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("kova-engine-{}-{}", std::process::id(), n));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn test_catalog_restart_does_not_read_user_pages() {
        let dir = scratch();
        let config = config(&dir);
        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        let documents: Vec<_> = (0..6)
            .map(|n| doc! { "_id": n, "name": format!("row-{n}"), "pad": "p".repeat(400) })
            .collect();
        engine.insert_documents("db.t", documents).unwrap();
        engine.checkpoint_pages().unwrap();
        drop(engine);

        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        assert!(engine.catalog_is_warm());
        assert_eq!(engine.user_page_reads(), 0);
        assert!(engine.names().iter().any(|name| name == "db.t"));
        assert!(engine.names().iter().any(|name| name == "admin.fields"));
        let fields = engine.field_types("db.t");
        assert!(
            fields
                .iter()
                .any(|(name, kind)| name == "name" && kind == "string"),
            "{fields:?}"
        );
        assert!(
            fields
                .iter()
                .any(|(name, kind)| name == "_id" && kind == "int"),
            "{fields:?}"
        );
        let found = engine
            .find_documents("db.t", &doc! { "name": "row-5" })
            .unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].get_str("name").unwrap(), "row-5");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_empty_field_catalog_is_rebuilt_from_the_documents() {
        let dir = scratch();
        let config = config(&dir);
        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        engine
            .insert_documents(
                "db.emp",
                vec![doc! { "_id": 1i32, "ename": "SMITH", "sal": 800i32 }],
            )
            .unwrap();
        engine.clear_fields("db.emp");
        engine.checkpoint_pages().unwrap();
        drop(engine);

        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        assert!(engine.field_types("db.emp").is_empty());
        let rebuilt = engine.reconcile_fields().unwrap();
        assert_eq!(rebuilt, 1);
        let fields = engine.field_types("db.emp");
        assert!(
            fields
                .iter()
                .any(|(name, kind)| name == "ename" && kind == "string"),
            "{fields:?}"
        );
        assert!(
            fields
                .iter()
                .any(|(name, kind)| name == "sal" && kind == "int"),
            "{fields:?}"
        );
        engine.checkpoint_pages().unwrap();
        drop(engine);

        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        let reads = engine.user_page_reads();
        assert_eq!(engine.reconcile_fields().unwrap(), 0);
        assert_eq!(engine.user_page_reads(), reads);
        assert!(engine
            .field_types("db.emp")
            .iter()
            .any(|(name, _)| name == "ename"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_wal_tail_then_checkpoint_pages_survive_reopen() {
        let dir = scratch();
        let config = config(&dir);

        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        engine
            .append(&WalOp::Insert {
                collection: "db.emp".into(),
                documents: vec![doc! { "name": "SMITH" }],
            })
            .unwrap();
        drop(engine);

        let mut engine = StorageEngine::open(0, &config).unwrap();
        let recovery = engine.load().unwrap();
        assert_eq!(recovery.pending.len(), 1);
        let WalOp::Insert {
            collection,
            documents,
        } = recovery.pending[0].op.clone()
        else {
            panic!("expected the insert record");
        };
        engine.insert_documents(&collection, documents).unwrap();
        engine.checkpoint_pages().unwrap();
        let wal_dir = dir.join("partition-0").join("wal");
        assert!(
            fs::read_dir(&wal_dir).unwrap().next().is_none(),
            "checkpoint truncates the WAL"
        );
        assert_eq!(engine.retained_documents(), 0);
        drop(engine);

        let mut engine = StorageEngine::open(0, &config).unwrap();
        let recovery = engine.load().unwrap();
        assert!(recovery.pending.is_empty());
        let found = engine.find_documents("db.emp", &doc! {}).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].get_str("name").unwrap(), "SMITH");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_find_reads_pages_evicted_from_a_small_pool() {
        let dir = scratch();
        let config = config(&dir);
        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        let documents: Vec<_> = (0..6)
            .map(|n| doc! { "_id": n, "name": format!("row-{n}"), "pad": "p".repeat(400) })
            .collect();
        engine.insert_documents("db.t", documents).unwrap();
        assert_eq!(engine.retained_documents(), 0);
        let found = engine
            .find_documents("db.t", &doc! { "name": "row-5" })
            .unwrap();
        assert_eq!(found.len(), 1);
        assert!(
            engine.pool_stats().misses > 0,
            "a pool of 2 pages must read the rest from disk"
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_update_and_delete_of_an_evicted_document() {
        let dir = scratch();
        let config = config(&dir);
        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        let documents: Vec<_> = (0..6)
            .map(|n| doc! { "_id": n, "name": format!("row-{n}"), "pad": "p".repeat(400) })
            .collect();
        engine.insert_documents("db.t", documents).unwrap();
        engine.checkpoint_pages().unwrap();
        drop(engine);

        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        let (matched, _) = engine
            .update_documents(
                "db.t",
                &doc! { "name": "row-4" },
                &doc! { "$set": { "name": "updated" } },
                false,
            )
            .unwrap();
        assert_eq!(matched, 1);
        let removed = engine
            .delete_documents("db.t", &doc! { "name": "row-1" }, 1)
            .unwrap();
        assert_eq!(removed.len(), 1);
        let found = engine.find_documents("db.t", &doc! {}).unwrap();
        assert_eq!(found.len(), 5);
        assert!(found
            .iter()
            .any(|doc| doc.get_str("name").unwrap() == "updated"));
        assert!(found
            .iter()
            .all(|doc| doc.get_str("name").unwrap() != "row-1"));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_overflow_document_round_trips() {
        let dir = scratch();
        let config = config(&dir);
        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        let wide = "w".repeat(800);
        engine
            .insert_documents("db.wide", vec![doc! { "_id": "big", "blob": &wide }])
            .unwrap();
        let found = engine.find_documents("db.wide", &doc! {}).unwrap();
        assert_eq!(found[0].get_str("blob").unwrap(), wide);
        engine.checkpoint_pages().unwrap();
        drop(engine);

        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        let found = engine.find_documents("db.wide", &doc! {}).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].get_str("blob").unwrap(), wide);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_compressed_pages_round_trip_after_restart() {
        let dir = scratch();
        let config = config(&dir);
        let blob = "a".repeat(300);
        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        engine
            .insert_documents("db.emp", vec![doc! { "_id": "1", "blob": &blob }])
            .unwrap();
        engine.checkpoint_pages().unwrap();

        let pages_dir = dir.join("partition-0").join("pages");
        let mut saw_short = false;
        for entry in fs::read_dir(&pages_dir).unwrap() {
            let entry = entry.unwrap();
            let len = entry.metadata().unwrap().len();
            assert!(
                len <= config.page_size as u64,
                "page file {} is {len} bytes",
                entry.path().display()
            );
            if len < config.page_size as u64 {
                saw_short = true;
            }
        }
        assert!(
            saw_short,
            "a compressed page file is shorter than the envelope"
        );
        drop(engine);

        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        let found = engine.find_documents("db.emp", &doc! {}).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].get_str("blob").unwrap(), blob);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_encrypted_restart_needs_the_key() {
        let dir = scratch();
        let key = [7u8; 32];
        let mut config = config(&dir);
        config.encryption_key = Some(key);
        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        engine
            .insert_documents("db.emp", vec![doc! { "_id": "1", "name": "SMITH" }])
            .unwrap();
        engine.checkpoint_pages().unwrap();
        drop(engine);

        let mut plain = config.clone();
        plain.encryption_key = None;
        let mut engine = StorageEngine::open(0, &plain).unwrap();
        let err = match engine.load() {
            Err(message) => message,
            Ok(_) => panic!("encrypted pages need the key"),
        };
        assert!(err.contains("encrypted"), "{err}");
        drop(engine);

        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        let found = engine.find_documents("db.emp", &doc! {}).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].get_str("name").unwrap(), "SMITH");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_aggregate_scan_does_not_retain_documents() {
        let dir = scratch();
        let config = config(&dir);
        let mut engine = StorageEngine::open(0, &config).unwrap();
        engine.load().unwrap();
        let documents: Vec<_> = (0..6)
            .map(|n| doc! { "_id": n, "sal": n, "pad": "p".repeat(400) })
            .collect();
        engine.insert_documents("db.emp", documents).unwrap();
        let input = engine.find_documents("db.emp", &doc! {}).unwrap();
        assert_eq!(engine.retained_documents(), 0);
        let output = super::super::aggregate::run(
            input,
            &[
                doc! { "$match": { "sal": { "$gt": 3 } } },
                doc! { "$count": "n" },
            ],
            &mut |_| Ok(super::super::aggregate::Foreign::Ready(Vec::new())),
        )
        .unwrap();
        assert_eq!(engine.retained_documents(), 0);
        let super::super::aggregate::Output::Docs(rows) = output else {
            panic!("expected a count");
        };
        assert_eq!(rows[0].get_i32("n").unwrap(), 2);
        assert!(engine.pool_stats().misses > 0);
        let _ = fs::remove_dir_all(dir);
    }
}
