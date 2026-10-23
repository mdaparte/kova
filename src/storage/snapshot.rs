//! A copy of the page files and view routes at one checkpoint.
//!
//! The bytes are what a primary ships when a follower is too far behind
//! the oplog. The WAL and the Raft log stay out: the follower opens the
//! pages and then applies only the oplog entries after this snapshot.

use std::fs;
use std::path::{Component, Path};

use super::disk::{atomic_write, io_err};

const MAGIC: &[u8; 4] = b"KSV1";

/// Packs checkpoint manifests, page files, and view routes under `root`.
pub fn pack_tree(root: &Path) -> Result<Vec<u8>, String> {
    let mut files = Vec::new();
    collect(root, root, &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(pack(&files))
}

/// Rejects a snapshot that is not a page archive.
pub fn check(bytes: &[u8]) -> Result<(), String> {
    unpack(bytes).map(|_| ())
}

/// Writes this partition's files from a snapshot. Page files that are
/// not in the snapshot are removed. The WAL is removed so startup does
/// not replay it on top of the checkpoint.
pub fn install_partition(root: &Path, partition: usize, bytes: &[u8]) -> Result<(), String> {
    let files = unpack(bytes)?;
    let prefix = format!("partition-{partition}/");
    let part = root.join(format!("partition-{partition}"));
    let _ = fs::remove_dir_all(part.join("pages"));
    let _ = fs::remove_dir_all(part.join("wal"));
    let _ = fs::remove_dir_all(part.join("search"));
    let _ = fs::remove_file(part.join("checkpoint.bin"));
    for (path, data) in files {
        if path.starts_with(&prefix) || path.starts_with("view-routes/") {
            let target = root.join(&path);
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent).map_err(|err| io_err(parent, err))?;
            }
            atomic_write(&target, &data)?;
        }
    }
    Ok(())
}

fn collect(root: &Path, dir: &Path, files: &mut Vec<(String, Vec<u8>)>) -> Result<(), String> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir).map_err(|err| io_err(dir, err))? {
        let entry = entry.map_err(|err| io_err(dir, err))?;
        let path = entry.path();
        let rel = path
            .strip_prefix(root)
            .map_err(|err| err.to_string())?
            .to_string_lossy()
            .replace('\\', "/");
        if path.is_dir() {
            if keep_dir(&rel) {
                collect(root, &path, files)?;
            }
            continue;
        }
        if keep_file(&rel) {
            let bytes = fs::read(&path).map_err(|err| io_err(&path, err))?;
            files.push((rel, bytes));
        }
    }
    Ok(())
}

fn keep_dir(rel: &str) -> bool {
    rel == "view-routes"
        || rel.starts_with("partition-") && (rel.ends_with("/pages") || !rel.contains('/'))
}

fn keep_file(rel: &str) -> bool {
    if rel.starts_with("view-routes/") {
        return !rel.contains("..");
    }
    let mut parts = rel.split('/');
    let Some(partition) = parts.next() else {
        return false;
    };
    if !partition.starts_with("partition-") {
        return false;
    }
    match (parts.next(), parts.next()) {
        (Some("checkpoint.bin"), None) => true,
        (Some("pages"), Some(name)) if !name.is_empty() && parts.next().is_none() => true,
        _ => false,
    }
}

fn pack(files: &[(String, Vec<u8>)]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(MAGIC);
    bytes.extend_from_slice(&(files.len() as u32).to_le_bytes());
    for (path, data) in files {
        let path_bytes = path.as_bytes();
        bytes.extend_from_slice(&(path_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(path_bytes);
        bytes.extend_from_slice(&(data.len() as u64).to_le_bytes());
        bytes.extend_from_slice(data);
    }
    bytes
}

fn unpack(bytes: &[u8]) -> Result<Vec<(String, Vec<u8>)>, String> {
    if bytes.len() < 8 || &bytes[0..4] != MAGIC {
        return Err("snapshot is not a Kova page snapshot".into());
    }
    let count = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    let mut offset = 8;
    let mut files = Vec::with_capacity(count);
    for _ in 0..count {
        if offset + 4 > bytes.len() {
            return Err("snapshot file header is truncated".into());
        }
        let path_len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if offset + path_len + 8 > bytes.len() {
            return Err("snapshot path is truncated".into());
        }
        let path = std::str::from_utf8(&bytes[offset..offset + path_len])
            .map_err(|_| "snapshot path is not utf-8".to_string())?
            .to_string();
        offset += path_len;
        check_path(&path)?;
        let size = u64::from_le_bytes(bytes[offset..offset + 8].try_into().unwrap()) as usize;
        offset += 8;
        if offset + size > bytes.len() {
            return Err("snapshot file is truncated".into());
        }
        files.push((path, bytes[offset..offset + size].to_vec()));
        offset += size;
    }
    if offset != bytes.len() {
        return Err("snapshot has trailing bytes".into());
    }
    Ok(files)
}

fn check_path(path: &str) -> Result<(), String> {
    if path.is_empty() || path.starts_with('/') || path.contains('\\') {
        return Err(format!("snapshot path '{path}' is not allowed"));
    }
    let climbs = Path::new(path)
        .components()
        .any(|component| !matches!(component, Component::Normal(_)));
    if climbs {
        return Err(format!("snapshot path '{path}' is not allowed"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_round_trips_page_files_and_rejects_a_parent_path() {
        let root =
            std::env::temp_dir().join(format!("kova-snap-{}-{}", std::process::id(), line!()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("partition-0/pages")).unwrap();
        fs::create_dir_all(root.join("partition-0/wal")).unwrap();
        fs::write(root.join("partition-0/checkpoint.bin"), b"catalog").unwrap();
        fs::write(root.join("partition-0/pages/0001.page"), b"page").unwrap();
        fs::write(root.join("partition-0/wal/0001.wal"), b"wal").unwrap();
        fs::create_dir_all(root.join("view-routes")).unwrap();
        fs::write(root.join("view-routes/db.emp_search"), b"db.emp").unwrap();
        let bytes = pack_tree(&root).unwrap();
        let _ = fs::remove_dir_all(&root);
        install_partition(&root, 0, &bytes).unwrap();
        assert_eq!(
            fs::read(root.join("partition-0/checkpoint.bin")).unwrap(),
            b"catalog"
        );
        assert_eq!(
            fs::read(root.join("partition-0/pages/0001.page")).unwrap(),
            b"page"
        );
        assert!(!root.join("partition-0/wal/0001.wal").exists());
        assert_eq!(
            fs::read(root.join("view-routes/db.emp_search")).unwrap(),
            b"db.emp"
        );
        let evil = super::pack(&[("../secret".into(), b"no".to_vec())]);
        assert!(super::unpack(&evil).is_err());
        let _ = fs::remove_dir_all(&root);
    }
}
