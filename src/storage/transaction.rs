//! Transactions on one partition.
//!
//! A transaction's writes stay here until commit. Commit checks
//! write-write conflicts, then the worker appends one WAL record and
//! applies it to the pages. Abort throws the writes away. The page file
//! never holds an uncommitted document.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use bson::Document;

use super::mvcc::VersionStore;

/// How a transaction reads committed data.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Isolation {
    /// Sees other transactions' uncommitted writes.
    ReadUncommitted,
    /// A new snapshot at every statement.
    ReadCommitted,
    /// One snapshot, taken at the first statement.
    RepeatableRead,
    /// One snapshot, taken at begin.
    Snapshot,
    /// Snapshot plus abort when a document this transaction read was
    /// committed by someone else.
    Serializable,
}

impl Isolation {
    pub(super) fn parse(name: &str) -> Result<Self, String> {
        match name {
            "readUncommitted" => Ok(Self::ReadUncommitted),
            "readCommitted" => Ok(Self::ReadCommitted),
            "repeatableRead" => Ok(Self::RepeatableRead),
            "snapshot" => Ok(Self::Snapshot),
            "serializable" => Ok(Self::Serializable),
            other => Err(format!("unknown isolation level '{other}'")),
        }
    }

    fn pins_at_begin(self) -> bool {
        matches!(self, Self::Snapshot | Self::Serializable)
    }
}

/// What a statement should read.
pub(super) enum View {
    /// Latest committed pages, then every other transaction's writes.
    Dirty,
    /// Versions with `commit_lsn <= lsn`.
    Snapshot { lsn: u64 },
}

/// One staged write. `page_previous` is the committed page image at the
/// first time this transaction touched the id.
#[derive(Clone, Debug)]
pub(super) struct Write {
    pub collection: String,
    pub id: String,
    pub base_lsn: u64,
    pub page_previous: Option<Document>,
    pub after: Option<Document>,
}

struct Txn {
    isolation: Isolation,
    snapshot_lsn: u64,
    snapshot_fixed: bool,
    started: Instant,
    timeout: Option<Duration>,
    writes: Vec<Write>,
    /// `(namespace, id, lsn observed)` for serializable validation.
    reads: Vec<(String, String, u64)>,
}

/// Active transactions for one partition worker.
pub(super) struct TransactionManager {
    next: u64,
    txns: HashMap<u64, Txn>,
}

impl TransactionManager {
    pub(super) fn new() -> Self {
        Self {
            next: 1,
            txns: HashMap::new(),
        }
    }

    /// Opens a transaction. `txn_id == 0` allocates an id.
    pub(super) fn begin(
        &mut self,
        txn_id: u64,
        isolation: Isolation,
        timeout_ms: Option<u64>,
        now_lsn: u64,
    ) -> Result<u64, String> {
        let txn_id = if txn_id == 0 {
            let id = self.next;
            self.next += 1;
            id
        } else {
            if self.txns.contains_key(&txn_id) {
                return Err(format!("transaction {txn_id} is already active"));
            }
            self.next = self.next.max(txn_id.saturating_add(1));
            txn_id
        };
        let pins = isolation.pins_at_begin();
        self.txns.insert(
            txn_id,
            Txn {
                isolation,
                snapshot_lsn: if pins { now_lsn } else { 0 },
                snapshot_fixed: pins,
                started: Instant::now(),
                timeout: timeout_ms.map(Duration::from_millis),
                writes: Vec::new(),
                reads: Vec::new(),
            },
        );
        Ok(txn_id)
    }

    pub(super) fn abort(&mut self, txn_id: u64) -> Result<(), String> {
        if self.txns.remove(&txn_id).is_none() {
            return Err(format!("transaction {txn_id} is not active"));
        }
        Ok(())
    }

    /// The snapshot or dirty view for the next statement.
    pub(super) fn view(&mut self, txn_id: u64, now_lsn: u64) -> Result<View, String> {
        self.touch(txn_id, now_lsn)?;
        let txn = self.txn(txn_id)?;
        if txn.isolation == Isolation::ReadUncommitted {
            Ok(View::Dirty)
        } else {
            Ok(View::Snapshot {
                lsn: txn.snapshot_lsn,
            })
        }
    }

    /// Remembers a document a serializable transaction returned.
    pub(super) fn note_read(&mut self, txn_id: u64, namespace: &str, id: &str, lsn: u64) {
        let Some(txn) = self.txns.get_mut(&txn_id) else {
            return;
        };
        if txn.isolation != Isolation::Serializable {
            return;
        }
        if txn
            .reads
            .iter()
            .any(|(ns, seen, _)| ns == namespace && seen == id)
        {
            return;
        }
        txn.reads.push((namespace.to_string(), id.to_string(), lsn));
    }

    /// Stages a write. A second write to the same id keeps the original
    /// base and replaces the result. Insert-then-delete cancels.
    pub(super) fn stage(&mut self, txn_id: u64, write: Write) -> Result<(), String> {
        let txn = self
            .txns
            .get_mut(&txn_id)
            .ok_or_else(|| format!("transaction {txn_id} is not active"))?;
        if let Some(position) = txn
            .writes
            .iter()
            .position(|existing| existing.collection == write.collection && existing.id == write.id)
        {
            txn.writes[position].after = write.after;
            if txn.writes[position].after.is_none() && txn.writes[position].page_previous.is_none()
            {
                txn.writes.remove(position);
            }
            return Ok(());
        }
        if write.after.is_none() && write.page_previous.is_none() {
            return Ok(());
        }
        txn.writes.push(write);
        Ok(())
    }

    /// Uncommitted writes of every other transaction, in id order, so
    /// Read Uncommitted can overlay them. Later ids win.
    pub(super) fn dirty_writes(
        &self,
        except: u64,
        collection: &str,
    ) -> Vec<(String, Option<Document>)> {
        let mut ids: Vec<u64> = self
            .txns
            .keys()
            .copied()
            .filter(|id| *id != except)
            .collect();
        ids.sort_unstable();
        let mut writes = Vec::new();
        for id in ids {
            for write in &self.txns[&id].writes {
                if write.collection == collection {
                    writes.push((write.id.clone(), write.after.clone()));
                }
            }
        }
        writes
    }

    /// This transaction's own writes for one collection.
    pub(super) fn own_writes(
        &self,
        txn_id: u64,
        collection: &str,
    ) -> Vec<(String, Option<Document>)> {
        self.txns
            .get(&txn_id)
            .map(|txn| {
                txn.writes
                    .iter()
                    .filter(|write| write.collection == collection)
                    .map(|write| (write.id.clone(), write.after.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Checks conflicts and removes the transaction, returning its writes.
    ///
    /// The caller appends those writes to the WAL. A conflict or a
    /// timeout aborts the transaction here, so the writes never reach
    /// the log.
    pub(super) fn commit(
        &mut self,
        txn_id: u64,
        versions: &VersionStore,
    ) -> Result<Vec<Write>, String> {
        self.touch(txn_id, 0)?;
        let conflict = {
            let txn = self.txn(txn_id)?;
            txn.writes.iter().find_map(|write| {
                let current = versions.commit_lsn(&write.collection, &write.id);
                (current != write.base_lsn).then(|| {
                    format!(
                        "transaction {txn_id} aborted: write conflict on {} {}",
                        write.collection, write.id
                    )
                })
            })
        };
        if let Some(message) = conflict {
            self.txns.remove(&txn_id);
            return Err(message);
        }
        let serial = {
            let txn = self.txn(txn_id)?;
            if txn.isolation != Isolation::Serializable {
                None
            } else {
                txn.reads.iter().find_map(|(namespace, id, seen)| {
                    (versions.commit_lsn(namespace, id) != *seen).then(|| {
                        format!(
                            "transaction {txn_id} aborted: serialization conflict on {namespace} {id}"
                        )
                    })
                })
            }
        };
        if let Some(message) = serial {
            self.txns.remove(&txn_id);
            return Err(message);
        }
        Ok(self.txns.remove(&txn_id).expect("txn exists").writes)
    }

    /// Smallest snapshot still held. Read Committed does not hold one
    /// between statements. `u64::MAX` means nothing is held.
    pub(super) fn oldest_snapshot(&self) -> u64 {
        self.txns
            .values()
            .filter_map(|txn| match txn.isolation {
                Isolation::ReadUncommitted | Isolation::ReadCommitted => None,
                Isolation::RepeatableRead if !txn.snapshot_fixed => None,
                _ => Some(txn.snapshot_lsn),
            })
            .min()
            .unwrap_or(u64::MAX)
    }

    fn touch(&mut self, txn_id: u64, now_lsn: u64) -> Result<(), String> {
        let txn = self
            .txns
            .get_mut(&txn_id)
            .ok_or_else(|| format!("transaction {txn_id} is not active"))?;
        if txn
            .timeout
            .is_some_and(|limit| txn.started.elapsed() >= limit)
        {
            self.txns.remove(&txn_id);
            return Err(format!("transaction {txn_id} aborted: timeout"));
        }
        if txn.isolation == Isolation::ReadCommitted {
            txn.snapshot_lsn = now_lsn;
        } else if txn.isolation == Isolation::RepeatableRead && !txn.snapshot_fixed {
            txn.snapshot_lsn = now_lsn;
            txn.snapshot_fixed = true;
        }
        Ok(())
    }

    fn txn(&self, txn_id: u64) -> Result<&Txn, String> {
        self.txns
            .get(&txn_id)
            .ok_or_else(|| format!("transaction {txn_id} is not active"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    fn write(id: &str, base: u64, after: Option<i32>) -> Write {
        Write {
            collection: "db.emp".into(),
            id: id.into(),
            base_lsn: base,
            page_previous: Some(doc! { "_id": id, "sal": 1 }),
            after: after.map(|sal| doc! { "_id": id, "sal": sal }),
        }
    }

    #[test]
    fn the_later_writer_loses_the_conflict() {
        let mut versions = VersionStore::new();
        versions.note("db.emp", "1", 1, None, false);
        let mut txns = TransactionManager::new();
        let first = txns.begin(0, Isolation::Snapshot, None, 1).unwrap();
        let second = txns.begin(0, Isolation::Snapshot, None, 1).unwrap();
        txns.stage(first, write("1", 1, Some(2))).unwrap();
        txns.stage(second, write("1", 1, Some(3))).unwrap();
        let committed = txns.commit(first, &versions).unwrap();
        assert_eq!(committed.len(), 1);
        versions.note("db.emp", "1", 2, Some(doc! { "_id": "1", "sal": 1 }), false);
        let error = txns.commit(second, &versions).unwrap_err();
        assert!(error.contains("write conflict"), "{error}");
        assert!(txns.abort(second).is_err());
    }

    #[test]
    fn serializable_aborts_when_a_read_document_changes() {
        let mut versions = VersionStore::new();
        versions.note("db.emp", "a", 1, None, false);
        let mut txns = TransactionManager::new();
        let txn = txns.begin(0, Isolation::Serializable, None, 1).unwrap();
        txns.note_read(txn, "db.emp", "a", 1);
        versions.note("db.emp", "a", 2, Some(doc! { "_id": "a" }), false);
        let error = txns.commit(txn, &versions).unwrap_err();
        assert!(error.contains("serialization"), "{error}");
    }

    #[test]
    fn a_zero_timeout_aborts_on_the_next_call() {
        let versions = VersionStore::new();
        let mut txns = TransactionManager::new();
        let txn = txns.begin(0, Isolation::Snapshot, Some(0), 0).unwrap();
        let error = txns.commit(txn, &versions).unwrap_err();
        assert!(error.contains("timeout"), "{error}");
    }

    #[test]
    fn abort_discards_the_write_set() {
        let versions = VersionStore::new();
        let mut txns = TransactionManager::new();
        let txn = txns.begin(0, Isolation::Snapshot, None, 0).unwrap();
        txns.stage(txn, write("1", 0, Some(1))).unwrap();
        txns.abort(txn).unwrap();
        assert!(txns.own_writes(txn, "db.emp").is_empty());
        let error = txns.commit(txn, &versions).unwrap_err();
        assert!(error.contains("not active"), "{error}");
    }
}
