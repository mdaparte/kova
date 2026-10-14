//! Page files under a partition directory.
//!
//! Each page is one file, written to a temporary name and renamed into
//! place so a crash never leaves a half-written page at the live path.
//! The buffer pool is the only caller on the worker thread.

use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use super::page::Page;

/// Reads and writes page files.
///
/// Each file is at most `page_size` bytes: a header plus the zstd frame.
pub struct PageStore {
    dir: PathBuf,
    page_size: usize,
}

impl PageStore {
    /// Opens the page directory, creating it when missing.
    pub fn open(dir: impl Into<PathBuf>, page_size: usize) -> Result<Self, String> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|err| io_err(&dir, err))?;
        Ok(Self { dir, page_size })
    }

    /// Writes `page` atomically and fsyncs it before the rename.
    pub fn write(&self, page: &Page) -> Result<(), String> {
        let bytes = page.encode(self.page_size)?;
        let path = self.path(page.id);
        atomic_write(&path, &bytes)
    }

    /// Reads one page. A truncated file or a bad checksum is an error.
    pub fn read(&self, page_id: u64) -> Result<Page, String> {
        let path = self.path(page_id);
        let bytes = fs::read(&path).map_err(|err| io_err(&path, err))?;
        Page::decode(&bytes, self.page_size)
    }

    /// Deletes one page file. Missing files are not an error.
    pub fn remove(&self, page_id: u64) -> Result<(), String> {
        let path = self.path(page_id);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(io_err(&path, err)),
        }
    }

    /// Page ids currently stored in this directory.
    pub fn list_ids(&self) -> Result<Vec<u64>, String> {
        let mut ids = Vec::new();
        for entry in fs::read_dir(&self.dir).map_err(|err| io_err(&self.dir, err))? {
            let entry = entry.map_err(|err| io_err(&self.dir, err))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(hex) = name.strip_suffix(".page") {
                if let Ok(id) = u64::from_str_radix(hex, 16) {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    /// Deletes page files whose ids are not in `keep`.
    pub fn retain(&self, keep: &[u64]) -> Result<(), String> {
        for id in self.list_ids()? {
            if !keep.contains(&id) {
                self.remove(id)?;
            }
        }
        Ok(())
    }

    fn path(&self, page_id: u64) -> PathBuf {
        self.dir.join(format!("{page_id:016x}.page"))
    }
}

/// Writes `bytes` to `path` via a temporary file in the same directory.
pub fn atomic_write(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let tmp = path.with_extension("tmp");
    {
        let mut file = File::create(&tmp).map_err(|err| io_err(&tmp, err))?;
        file.write_all(bytes).map_err(|err| io_err(&tmp, err))?;
        file.sync_all().map_err(|err| io_err(&tmp, err))?;
    }
    fs::rename(&tmp, path).map_err(|err| io_err(path, err))?;
    if let Some(parent) = path.parent() {
        // Directory fsync publishes the rename. A failure here is not
        // fatal: the file itself is already durable.
        if let Ok(dir) = File::open(parent) {
            let _ = dir.sync_all();
        }
    }
    Ok(())
}

pub fn io_err(path: &Path, err: std::io::Error) -> String {
    format!("{}: {}", path.display(), err)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch() -> PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("kova-disk-{}-{}", std::process::id(), n));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn test_page_store_round_trip_and_retain() {
        let dir = scratch();
        let store = PageStore::open(dir.join("pages"), 128).unwrap();
        store.write(&Page::new(1, 3, b"aa".to_vec())).unwrap();
        store.write(&Page::new(2, 3, b"bb".to_vec())).unwrap();
        assert_eq!(store.read(1).unwrap().payload, b"aa");
        store.retain(&[1]).unwrap();
        assert!(store.read(2).is_err());
        assert_eq!(store.list_ids().unwrap(), vec![1]);
        let _ = fs::remove_dir_all(dir);
    }
}
