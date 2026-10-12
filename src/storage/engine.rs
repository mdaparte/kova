//! Per-partition storage engine: WAL, page store, and buffer pool.
//!
//! The worker still serves queries from its in-memory map. This engine
//! makes that map durable:
//!
//! 1. A mutation is appended to the WAL before the map changes.
//! 2. A checkpoint writes the map into fixed-size pages through the
//!    buffer pool, records the LSN, and deletes the WAL segments the
//!    snapshot already covers.
//! 3. Startup loads those pages and returns the WAL tail for the worker
//!    to apply.
//!
//! On-disk layout, one directory per partition:
//!
//! ```text
//! {data_dir}/partition-{id}/
//!   checkpoint.bin
//!   wal/0000000000000001.wal
//!   pages/0000000000000001.page
//! ```
//!
//! `checkpoint.bin` is the source of truth for which pages belong to the
//! latest snapshot. A crash while writing new pages leaves the previous
//! manifest in place; orphan page files are removed on the next open.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use bson::{doc, Bson, Document};

use crate::config::StorageConfig;
use crate::logger::LOGGER;

use super::buffer_pool::BufferPool;
use super::disk::{atomic_write, io_err, PageStore};
use super::page::{Page, PAGE_HEADER_LEN};
use super::wal::{Wal, WalOp, WalRecord};

const MANIFEST_MAGIC: u32 = 0x4348_4B50; // CHKP
const MANIFEST_VERSION: u32 = 1;

/// Collections or index declarations, keyed by namespace.
type SnapshotMap = HashMap<String, Vec<Document>>;

/// State loaded from disk, plus the WAL tail the worker still has to apply.
pub struct Recovery {
    /// Collections restored from the last checkpoint.
    pub collections: SnapshotMap,
    /// Index declarations restored from the last checkpoint.
    pub indexes: SnapshotMap,
    /// Mutations with an LSN above the checkpoint.
    pub pending: Vec<WalOp>,
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
    page_size: usize,
    sync: bool,
    checkpoint_every: u64,
    checkpoint_on_shutdown: bool,
    appends_since_checkpoint: u64,
    next_page_id: u64,
    manifest_path: PathBuf,
    wal: Wal,
    pool: BufferPool,
    disk: PageStore,
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
        Page::payload_capacity(page_size)?;
        let disk = PageStore::open(root.join("pages"), page_size)?;
        // The pool and the store share the directory. Each has its own
        // handle; pages are immutable once flushed, so that is safe on
        // this single thread.
        let pool_disk = PageStore::open(root.join("pages"), page_size)?;
        let pool = BufferPool::new(pool_disk, config.buffer_pool_pages)?;
        let wal = Wal::open(
            root.join("wal"),
            partition_u32,
            config.wal_fsync,
            config.wal_segment_bytes,
        )?;
        Ok(Self {
            partition,
            page_size,
            sync: config.wal_fsync,
            checkpoint_every: config.checkpoint_every,
            checkpoint_on_shutdown: config.checkpoint_on_shutdown,
            appends_since_checkpoint: 0,
            next_page_id: 1,
            manifest_path: root.join("checkpoint.bin"),
            wal,
            pool,
            disk,
        })
    }

    /// Loads the latest checkpoint and the WAL records that follow it.
    pub fn load(&mut self) -> Result<Recovery, String> {
        let manifest = self.read_manifest()?;
        self.wal.set_next_lsn(manifest.next_lsn);
        self.next_page_id = manifest.next_page_id.max(1);

        let mut image = Vec::new();
        for page_id in &manifest.page_ids {
            let page = self.pool.fetch(*page_id)?;
            image.extend_from_slice(&page.payload);
            self.pool.unpin(*page_id)?;
        }
        let (collections, indexes) = if image.is_empty() {
            (HashMap::new(), HashMap::new())
        } else {
            decode_image(&image)?
        };

        // Drop page files that a crashed checkpoint left behind.
        self.disk.retain(&manifest.page_ids)?;

        let records = self.wal.replay()?;
        let pending = records
            .into_iter()
            .filter(|record| record.lsn > manifest.checkpoint_lsn)
            .map(|WalRecord { op, .. }| op)
            .collect::<Vec<_>>();

        LOGGER.info(format!(
            "partition {} loaded checkpoint lsn {}, {} pending wal record(s)",
            self.partition,
            manifest.checkpoint_lsn,
            pending.len()
        ));

        Ok(Recovery {
            collections,
            indexes,
            pending,
        })
    }

    /// Appends a mutation. The caller applies it to memory after this returns.
    pub fn append(&mut self, op: &WalOp) -> Result<u64, String> {
        let lsn = self.wal.append(op)?;
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

    /// Writes `image` as the new snapshot and drops the WAL it covers.
    pub fn store_checkpoint(&mut self, image: &[u8]) -> Result<(), String> {
        let lsn = self.wal.last_lsn();
        let mut page_ids = Vec::new();
        if !image.is_empty() {
            let capacity = self.page_size - PAGE_HEADER_LEN;
            for chunk in image.chunks(capacity) {
                let id = self.next_page_id;
                self.next_page_id += 1;
                self.pool.insert(Page::new(id, lsn, chunk.to_vec()))?;
                page_ids.push(id);
            }
        }
        self.pool.flush()?;
        let stats = self.pool.stats();
        LOGGER.debug(format!(
            "partition {} buffer pool hits {} misses {} evictions {} pinned {}",
            self.partition, stats.hits, stats.misses, stats.evictions, stats.pinned
        ));

        let manifest = Manifest {
            checkpoint_lsn: lsn,
            next_lsn: self.wal.next_lsn(),
            next_page_id: self.next_page_id,
            page_ids: page_ids.clone(),
        };
        manifest.write(&self.manifest_path, self.sync)?;
        self.wal.discard_segments()?;
        self.disk.retain(&page_ids)?;
        self.appends_since_checkpoint = 0;

        LOGGER.info(format!(
            "partition {} checkpoint lsn {} ({} pages)",
            self.partition,
            lsn,
            page_ids.len()
        ));
        Ok(())
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

/// Serializes the working set into the checkpoint payload.
pub fn encode_image(collections: &SnapshotMap, indexes: &SnapshotMap) -> Result<Vec<u8>, String> {
    let mut names: Vec<&String> = collections.keys().collect();
    names.sort();
    let mut cols = Document::new();
    for name in names {
        let docs = &collections[name];
        cols.insert(
            name,
            Bson::Array(docs.iter().cloned().map(Bson::Document).collect()),
        );
    }

    let mut index_names: Vec<&String> = indexes.keys().collect();
    index_names.sort();
    let mut idxs = Document::new();
    for name in index_names {
        let docs = &indexes[name];
        idxs.insert(
            name,
            Bson::Array(docs.iter().cloned().map(Bson::Document).collect()),
        );
    }

    let image = doc! {
        "collections": cols,
        "indexes": idxs,
    };
    bson::to_vec(&image).map_err(|err| format!("checkpoint encode: {}", err))
}

fn decode_image(bytes: &[u8]) -> Result<(SnapshotMap, SnapshotMap), String> {
    let image: Document =
        bson::from_slice(bytes).map_err(|err| format!("checkpoint decode: {}", err))?;
    let collections = decode_map(image.get_document("collections").ok())?;
    let indexes = decode_map(image.get_document("indexes").ok())?;
    Ok((collections, indexes))
}

fn decode_map(doc: Option<&Document>) -> Result<SnapshotMap, String> {
    let mut map = HashMap::new();
    let Some(doc) = doc else {
        return Ok(map);
    };
    for (name, value) in doc {
        let array = value
            .as_array()
            .ok_or_else(|| format!("checkpoint entry {} is not an array", name))?;
        let mut docs = Vec::with_capacity(array.len());
        for item in array {
            let document = item
                .as_document()
                .ok_or_else(|| format!("checkpoint entry {} has a non-document", name))?;
            docs.push(document.clone());
        }
        map.insert(name.clone(), docs);
    }
    Ok(map)
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
            page_size: 64,
            buffer_pool_pages: 2,
            wal_fsync: false,
            wal_segment_bytes: 1024 * 1024,
            checkpoint_every: 0,
            checkpoint_on_shutdown: false,
            ephemeral: false,
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
        assert!(recovery.collections.is_empty());
        assert_eq!(recovery.pending.len(), 1);

        let mut collections = HashMap::new();
        collections.insert(
            "db.emp".into(),
            vec![doc! { "name": "SMITH", "note": "x".repeat(80) }],
        );
        let image = encode_image(&collections, &HashMap::new()).unwrap();
        assert!(
            image.len() > 32,
            "the snapshot must span more than one page"
        );
        engine.store_checkpoint(&image).unwrap();
        let wal_dir = dir.join("partition-0").join("wal");
        assert!(
            fs::read_dir(&wal_dir).unwrap().next().is_none(),
            "checkpoint truncates the WAL"
        );
        drop(engine);

        let mut engine = StorageEngine::open(0, &config).unwrap();
        let recovery = engine.load().unwrap();
        assert!(recovery.pending.is_empty());
        assert_eq!(
            recovery.collections["db.emp"][0].get_str("name").unwrap(),
            "SMITH"
        );
        let _ = fs::remove_dir_all(dir);
    }
}
