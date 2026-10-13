//! The Raft log is the oplog.
//!
//! Each entry is one client command. Its log index is the timestamp a
//! secondary uses to ask for "operations after this point". Vote and log
//! are rewritten to `oplog.json` before OpenRaft is told the append is
//! durable. Snapshot shipping is off, so this file is not purged.

use std::collections::BTreeMap;
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use openraft::storage::RaftLogStorage;
use openraft::{Entry, EntryPayload, LogId, LogState, RaftLogReader, StorageError, Vote};
use serde::{Deserialize, Serialize};

use super::{OplogCommand, TypeConfig};

#[derive(Clone)]
pub struct OplogStore {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    dir: PathBuf,
    vote: Option<Vote<u64>>,
    last_purged: Option<LogId<u64>>,
    log: BTreeMap<u64, Entry<TypeConfig>>,
}

#[derive(Serialize, Deserialize)]
struct DiskLog {
    vote: Option<Vote<u64>>,
    last_purged: Option<LogId<u64>>,
    entries: Vec<Entry<TypeConfig>>,
}

impl OplogStore {
    pub fn open(dir: &Path) -> Result<Self, String> {
        std::fs::create_dir_all(dir).map_err(|err| err.to_string())?;
        let inner = read_disk(dir).map_err(|err| format!("oplog {}: {}", dir.display(), err))?;
        Ok(Self {
            inner: Arc::new(Mutex::new(Inner {
                dir: dir.to_path_buf(),
                vote: inner.0,
                last_purged: inner.1,
                log: inner.2,
            })),
        })
    }

    /// Normal client commands with a log index greater than `after`.
    pub fn commands_after(&self, after: u64) -> Vec<(u64, u64, Vec<u8>)> {
        let guard = lock(&self.inner);
        guard
            .log
            .range((Bound::Excluded(after), Bound::Unbounded))
            .filter_map(|(_, entry)| match &entry.payload {
                EntryPayload::Normal(OplogCommand { body }) => Some((
                    entry.log_id.index,
                    entry.log_id.leader_id.term,
                    body.clone(),
                )),
                _ => None,
            })
            .collect()
    }

    #[allow(clippy::result_large_err)]
    fn persist(inner: &Inner) -> Result<(), StorageError<u64>> {
        let disk = DiskLog {
            vote: inner.vote,
            last_purged: inner.last_purged,
            entries: inner.log.values().cloned().collect(),
        };
        let bytes = serde_json::to_vec(&disk).map_err(io_failure)?;
        atomic_write(&inner.dir.join("oplog.json"), &bytes).map_err(io_failure)
    }
}

pub(super) fn atomic_write(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = path.with_extension("tmp");
    {
        let mut file = std::fs::File::create(&tmp)?;
        use std::io::Write;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(tmp, path)
}

type LoadedLog = (
    Option<Vote<u64>>,
    Option<LogId<u64>>,
    BTreeMap<u64, Entry<TypeConfig>>,
);

fn read_disk(dir: &Path) -> std::io::Result<LoadedLog> {
    let path = dir.join("oplog.json");
    if !path.exists() {
        return Ok((None, None, BTreeMap::new()));
    }
    let bytes = std::fs::read(path)?;
    if bytes.is_empty() {
        return Ok((None, None, BTreeMap::new()));
    }
    let disk: DiskLog = serde_json::from_slice(&bytes)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    let mut log = BTreeMap::new();
    for entry in disk.entries {
        log.insert(entry.log_id.index, entry);
    }
    Ok((disk.vote, disk.last_purged, log))
}

fn io_failure(err: impl std::error::Error + 'static) -> StorageError<u64> {
    openraft::StorageIOError::write_logs(openraft::AnyError::new(&err)).into()
}

fn lock(inner: &Mutex<Inner>) -> std::sync::MutexGuard<'_, Inner> {
    inner.lock().unwrap_or_else(|poison| poison.into_inner())
}

impl RaftLogReader<TypeConfig> for Arc<OplogStore> {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + openraft::OptionalSend,
    {
        let guard = lock(&self.inner);
        Ok(guard
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for Arc<OplogStore> {
    type LogReader = Arc<OplogStore>;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let guard = lock(&self.inner);
        let last = guard.log.values().next_back().map(|entry| entry.log_id);
        Ok(LogState {
            last_purged_log_id: guard.last_purged,
            last_log_id: last.or(guard.last_purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut guard = lock(&self.inner);
        guard.vote = Some(*vote);
        OplogStore::persist(&guard)
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(lock(&self.inner).vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: openraft::storage::LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut guard = lock(&self.inner);
        for entry in entries {
            guard.log.insert(entry.log_id.index, entry);
        }
        let persisted = OplogStore::persist(&guard);
        drop(guard);
        match persisted {
            Ok(()) => {
                callback.log_io_completed(Ok(()));
                Ok(())
            }
            Err(err) => {
                callback.log_io_completed(Err(std::io::Error::other(err.to_string())));
                Err(err)
            }
        }
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut guard = lock(&self.inner);
        guard.log.split_off(&log_id.index);
        OplogStore::persist(&guard)
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut guard = lock(&self.inner);
        guard.log.retain(|index, _| *index > log_id.index);
        guard.last_purged = Some(log_id);
        OplogStore::persist(&guard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oplog_file_round_trips_a_command() {
        let dir =
            std::env::temp_dir().join(format!("kova-oplog-{}-{}", std::process::id(), line!()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = OplogStore::open(&dir).expect("open");
        let log_id = LogId {
            leader_id: openraft::CommittedLeaderId::new(3, 1),
            index: 4,
        };
        let entry = Entry {
            log_id,
            payload: EntryPayload::Normal(OplogCommand {
                body: b"king".to_vec(),
            }),
        };
        {
            let mut guard = lock(&store.inner);
            guard.vote = Some(Vote::new(3, 1));
            guard.log.insert(4, entry);
            OplogStore::persist(&guard).expect("persist");
        }
        let reopened = OplogStore::open(&dir).expect("reopen");
        let found = reopened.commands_after(0);
        assert_eq!(found, vec![(4, 3, b"king".to_vec())]);
        assert!(reopened.commands_after(4).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
