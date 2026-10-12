//! LRU page cache with pin/unpin eviction.
//!
//! The pool is confined to one partition thread, the same way the
//! collection map is. Pinning a page keeps it resident while the caller
//! is copying the payload out. Dirty pages are written back on eviction
//! and on an explicit flush at checkpoint time.

use std::collections::HashMap;

use crate::logger::LOGGER;

use super::disk::PageStore;
use super::page::Page;

/// Counters exposed for tests and `debug` logs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolStats {
    /// Fetches served from memory.
    pub hits: u64,
    /// Fetches that read the page store.
    pub misses: u64,
    /// Pages dropped to make room.
    pub evictions: u64,
    /// Sum of pin counts across resident pages.
    pub pinned: u32,
}

struct Frame {
    page_id: u64,
    page: Page,
    pin: u32,
    dirty: bool,
    live: bool,
    prev: Option<usize>,
    next: Option<usize>,
}

impl Frame {
    fn vacant() -> Self {
        Self {
            page_id: 0,
            page: Page::new(0, 0, Vec::new()),
            pin: 0,
            dirty: false,
            live: false,
            prev: None,
            next: None,
        }
    }
}

/// Fixed-capacity cache in front of a [`PageStore`].
pub struct BufferPool {
    frames: Vec<Frame>,
    free: Vec<usize>,
    index: HashMap<u64, usize>,
    /// Most recently used.
    head: Option<usize>,
    /// Least recently used.
    tail: Option<usize>,
    capacity: usize,
    hits: u64,
    misses: u64,
    evictions: u64,
    disk: PageStore,
}

impl BufferPool {
    /// Creates an empty pool that reads and writes through `disk`.
    pub fn new(disk: PageStore, capacity: usize) -> Result<Self, String> {
        if capacity == 0 {
            return Err("buffer pool capacity must be at least 1".into());
        }
        Ok(Self {
            frames: Vec::with_capacity(capacity),
            free: Vec::new(),
            index: HashMap::new(),
            head: None,
            tail: None,
            capacity,
            hits: 0,
            misses: 0,
            evictions: 0,
            disk,
        })
    }

    /// Inserts or replaces a dirty page. The page is not pinned, so a
    /// later insert may evict it after writing it back.
    pub fn insert(&mut self, page: Page) -> Result<(), String> {
        if let Some(&slot) = self.index.get(&page.id) {
            self.frames[slot].page = page;
            self.frames[slot].dirty = true;
            self.touch(slot);
            return Ok(());
        }
        let slot = self.alloc_slot()?;
        self.install(slot, page, true, 0);
        Ok(())
    }

    /// Returns a copy of the page, pinning it until [`Self::unpin`].
    pub fn fetch(&mut self, page_id: u64) -> Result<Page, String> {
        if let Some(&slot) = self.index.get(&page_id) {
            self.frames[slot].pin = self.frames[slot].pin.saturating_add(1);
            self.touch(slot);
            self.hits += 1;
            LOGGER.debug(format!("buffer pool hit page {}", page_id));
            return Ok(self.frames[slot].page.clone());
        }

        self.misses += 1;
        LOGGER.debug(format!("buffer pool miss page {}", page_id));
        let page = self.disk.read(page_id)?;
        let slot = self.alloc_slot()?;
        self.install(slot, page.clone(), false, 1);
        Ok(page)
    }

    /// Drops one pin. The page stays cached until it is the LRU victim.
    pub fn unpin(&mut self, page_id: u64) -> Result<(), String> {
        let slot = *self
            .index
            .get(&page_id)
            .ok_or_else(|| format!("buffer pool unpin of unknown page {}", page_id))?;
        if self.frames[slot].pin == 0 {
            return Err(format!("buffer pool page {} is not pinned", page_id));
        }
        self.frames[slot].pin -= 1;
        Ok(())
    }

    /// Drops a cached page without writing it. Used when a collection is
    /// deleted and the dirty copy must not be flushed back.
    pub fn discard(&mut self, page_id: u64) {
        let Some(&slot) = self.index.get(&page_id) else {
            return;
        };
        self.unlink(slot);
        self.index.remove(&page_id);
        self.frames[slot].live = false;
        self.frames[slot].dirty = false;
        self.frames[slot].pin = 0;
        self.free.push(slot);
    }

    /// Writes every dirty page and clears the dirty bit.
    pub fn flush(&mut self) -> Result<(), String> {
        let dirty: Vec<Page> = self
            .frames
            .iter()
            .filter(|frame| frame.live && frame.dirty)
            .map(|frame| frame.page.clone())
            .collect();
        for page in dirty {
            self.disk.write(&page)?;
            if let Some(&slot) = self.index.get(&page.id) {
                self.frames[slot].dirty = false;
            }
        }
        Ok(())
    }

    /// Whether `page_id` is currently cached.
    #[cfg(test)]
    pub fn contains(&self, page_id: u64) -> bool {
        self.index.contains_key(&page_id)
    }

    /// Snapshot of the counters.
    pub fn stats(&self) -> PoolStats {
        let pinned = self
            .frames
            .iter()
            .filter(|frame| frame.live)
            .map(|frame| frame.pin)
            .sum();
        PoolStats {
            hits: self.hits,
            misses: self.misses,
            evictions: self.evictions,
            pinned,
        }
    }

    fn alloc_slot(&mut self) -> Result<usize, String> {
        if let Some(slot) = self.free.pop() {
            return Ok(slot);
        }
        if self.frames.len() < self.capacity {
            self.frames.push(Frame::vacant());
            return Ok(self.frames.len() - 1);
        }
        self.evict_one()?;
        self.free
            .pop()
            .ok_or_else(|| "buffer pool eviction did not free a slot".to_string())
    }

    fn evict_one(&mut self) -> Result<(), String> {
        let mut cursor = self.tail;
        while let Some(slot) = cursor {
            let prev = self.frames[slot].prev;
            if self.frames[slot].live && self.frames[slot].pin == 0 {
                return self.evict_slot(slot);
            }
            cursor = prev;
        }
        Err("buffer pool exhaustion: every cached page is pinned".into())
    }

    fn evict_slot(&mut self, slot: usize) -> Result<(), String> {
        let page_id = self.frames[slot].page_id;
        if self.frames[slot].dirty {
            let page = self.frames[slot].page.clone();
            self.disk.write(&page)?;
        }
        self.unlink(slot);
        self.index.remove(&page_id);
        self.frames[slot].live = false;
        self.frames[slot].pin = 0;
        self.frames[slot].dirty = false;
        self.free.push(slot);
        self.evictions += 1;
        LOGGER.warn(format!("buffer pool evicted page {}", page_id));
        Ok(())
    }

    fn install(&mut self, slot: usize, page: Page, dirty: bool, pin: u32) {
        let page_id = page.id;
        self.frames[slot].page_id = page_id;
        self.frames[slot].page = page;
        self.frames[slot].pin = pin;
        self.frames[slot].dirty = dirty;
        self.frames[slot].live = true;
        self.frames[slot].prev = None;
        self.frames[slot].next = None;
        self.index.insert(page_id, slot);
        self.push_front(slot);
    }

    fn touch(&mut self, slot: usize) {
        self.unlink(slot);
        self.push_front(slot);
    }

    fn push_front(&mut self, slot: usize) {
        self.frames[slot].prev = None;
        self.frames[slot].next = self.head;
        if let Some(head) = self.head {
            self.frames[head].prev = Some(slot);
        } else {
            self.tail = Some(slot);
        }
        self.head = Some(slot);
    }

    fn unlink(&mut self, slot: usize) {
        let prev = self.frames[slot].prev;
        let next = self.frames[slot].next;
        match prev {
            Some(index) => self.frames[index].next = next,
            None => {
                if self.head == Some(slot) {
                    self.head = next;
                }
            }
        }
        match next {
            Some(index) => self.frames[index].prev = prev,
            None => {
                if self.tail == Some(slot) {
                    self.tail = prev;
                }
            }
        }
        self.frames[slot].prev = None;
        self.frames[slot].next = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn scratch() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("kova-pool-{}-{}", std::process::id(), n));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn pool(dir: &std::path::Path, capacity: usize) -> BufferPool {
        let disk = PageStore::open(dir.join("pages"), 64).unwrap();
        BufferPool::new(disk, capacity).unwrap()
    }

    #[test]
    fn test_lru_evicts_the_cold_page_and_keeps_pins() {
        let dir = scratch();
        let mut pool = pool(&dir, 2);
        pool.insert(Page::new(1, 1, b"one".to_vec())).unwrap();
        pool.insert(Page::new(2, 1, b"two".to_vec())).unwrap();
        // Touch page 1 so page 2 becomes the LRU victim.
        let fetched = pool.fetch(1).unwrap();
        assert_eq!(fetched.payload, b"one");
        pool.unpin(1).unwrap();

        pool.insert(Page::new(3, 1, b"three".to_vec())).unwrap();
        assert!(!pool.contains(2), "page 2 was the least recently used");
        assert!(pool.contains(1));
        assert!(pool.contains(3));
        assert!(pool.stats().evictions >= 1);

        // Both resident pages pinned: the pool must refuse to evict them.
        pool.fetch(1).unwrap();
        pool.fetch(3).unwrap();
        let err = pool.insert(Page::new(4, 1, b"four".to_vec()));
        assert!(err.is_err(), "pinned pages must not be evicted");
        assert!(err.unwrap_err().contains("exhaustion"));

        pool.unpin(1).unwrap();
        pool.unpin(3).unwrap();
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_flush_then_fetch_miss_reads_disk() {
        let dir = scratch();
        {
            let mut pool = pool(&dir, 2);
            pool.insert(Page::new(9, 4, b"disk".to_vec())).unwrap();
            pool.flush().unwrap();
        }
        let mut pool = pool(&dir, 2);
        let page = pool.fetch(9).unwrap();
        assert_eq!(page.payload, b"disk");
        assert_eq!(pool.stats().misses, 1);
        let again = pool.fetch(9).unwrap();
        assert_eq!(again.payload, b"disk");
        assert_eq!(pool.stats().hits, 1);
        pool.unpin(9).unwrap();
        pool.unpin(9).unwrap();
        let _ = fs::remove_dir_all(dir);
    }
}
