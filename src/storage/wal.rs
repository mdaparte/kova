//! Segmented write-ahead log.
//!
//! Every mutation is appended here before the partition dirties a page.
//! A record is `magic || lsn || crc32 || len || bson`.
//! The CRC covers the LSN and the payload, so a torn tail (crash mid-append)
//! is truncated on open and a bit flip is rejected.
//!
//! Layout under `{data_dir}/partition-{id}/wal/`:
//!
//! ```text
//! 0000000000000001.wal
//! 0000000000000002.wal
//! ```
//!
//! A segment starts with a 24-byte header. When the next record would
//! cross `wal_segment_bytes`, the log rolls to a new file. One record
//! larger than the limit still occupies a segment of its own.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use bson::{doc, Bson, Document};

use crate::logger::LOGGER;
use crate::types::PartitionCommand;

use super::crc::crc32;
use super::crypto;
use super::disk::io_err;

const WAL_SEAL_MAGIC: u32 = 0x5345_414C; // SEAL

const SEGMENT_MAGIC: u32 = 0x5741_4C31; // WAL1
const RECORD_MAGIC: u32 = 0x5245_4331; // REC1
const SEGMENT_HEADER_LEN: usize = 24;
const RECORD_HEADER_LEN: usize = 20;
const MAX_PAYLOAD: usize = 64 * 1024 * 1024;

/// One logical mutation, independent of the in-memory apply path.
#[derive(Debug, Clone, PartialEq)]
pub enum WalOp {
    /// Insert a batch into `collection`.
    Insert {
        /// `db.collection` namespace.
        collection: String,
        /// Documents in command order.
        documents: Vec<Document>,
    },
    /// Update matching documents.
    Update {
        /// `db.collection` namespace.
        collection: String,
        /// Query filter.
        filter: Document,
        /// Update document (`$set` or replacement).
        update: Document,
        /// `true` updates every match.
        multi: bool,
    },
    /// Delete matching documents.
    Delete {
        /// `db.collection` namespace.
        collection: String,
        /// Query filter.
        filter: Document,
        /// `0` deletes every match; otherwise a cap.
        limit: u32,
    },
    /// Drop one collection.
    Drop {
        /// `db.collection` namespace.
        collection: String,
    },
    /// Drop every collection of one database on this partition.
    DropDatabase {
        /// Database name.
        database: String,
    },
    /// Create an empty collection.
    CreateCollection {
        /// `db.collection` namespace.
        collection: String,
    },
    /// Record index declarations.
    CreateIndexes {
        /// `db.collection` namespace.
        collection: String,
        /// Index specification documents.
        indexes: Vec<Document>,
    },
    /// Drop one secondary index, or every secondary index when `index` is `"*"`.
    DropIndexes {
        /// `db.collection` namespace.
        collection: String,
        /// Index name, or `"*"`.
        index: String,
    },
    /// Replace a collection's document validator.
    CollMod {
        /// `db.collection` namespace.
        collection: String,
        /// Validator document (`required`, `properties`).
        validator: Document,
    },
    /// A user or role change. The document holds the hash, never the password.
    Auth {
        /// `op` plus the fields of that account command.
        spec: Document,
    },
    /// Several mutations committed as one transaction.
    ///
    /// Recovery applies every inner operation or none of them: a torn
    /// tail drops the whole record, so a crash cannot publish half of
    /// the commit.
    Transaction {
        /// Mutations in commit order.
        ops: Vec<WalOp>,
    },
}

/// A record as stored on disk.
#[derive(Debug, Clone, PartialEq)]
pub struct WalRecord {
    /// Monotonic log sequence number. Starts at 1.
    pub lsn: u64,
    /// The mutation.
    pub op: WalOp,
}

struct SegmentFile {
    file: File,
    len: u64,
}

/// Append-only log for one partition.
pub struct Wal {
    dir: PathBuf,
    partition: u32,
    fsync: bool,
    segment_bytes: u64,
    next_lsn: u64,
    next_segment: u64,
    current: Option<SegmentFile>,
    key: Option<[u8; 32]>,
}

impl WalOp {
    /// The log record for a mutating command. Reads return `None`.
    pub fn from_command(command: &PartitionCommand) -> Option<Self> {
        match command {
            PartitionCommand::Insert {
                collection,
                documents,
            } => Some(Self::Insert {
                collection: collection.clone(),
                documents: documents.clone(),
            }),
            PartitionCommand::Update {
                collection,
                filter,
                update,
                multi,
            } => Some(Self::Update {
                collection: collection.clone(),
                filter: filter.clone(),
                update: update.clone(),
                multi: *multi,
            }),
            PartitionCommand::Delete {
                collection,
                filter,
                limit,
            } => Some(Self::Delete {
                collection: collection.clone(),
                filter: filter.clone(),
                limit: *limit,
            }),
            PartitionCommand::Drop { collection } => Some(Self::Drop {
                collection: collection.clone(),
            }),
            PartitionCommand::DropDatabase { database } => Some(Self::DropDatabase {
                database: database.clone(),
            }),
            PartitionCommand::CreateCollection { collection } => Some(Self::CreateCollection {
                collection: collection.clone(),
            }),
            PartitionCommand::CreateIndexes {
                collection,
                indexes,
            } => Some(Self::CreateIndexes {
                collection: collection.clone(),
                indexes: indexes.clone(),
            }),
            PartitionCommand::DropIndexes { collection, index } => Some(Self::DropIndexes {
                collection: collection.clone(),
                index: index.clone(),
            }),
            PartitionCommand::CollMod {
                collection,
                validator,
            } => Some(Self::CollMod {
                collection: collection.clone(),
                validator: validator.clone(),
            }),
            PartitionCommand::CreateUser {
                name,
                hash,
                roles,
                trusted,
            } => Some(Self::Auth {
                spec: doc! {
                    "op": "createUser",
                    "name": name.clone(),
                    "hash": hash.clone(),
                    "roles": roles.clone(),
                    "trusted": *trusted,
                },
            }),
            PartitionCommand::UpdateUser { name, enabled } => Some(Self::Auth {
                spec: doc! {
                    "op": "updateUser",
                    "name": name.clone(),
                    "enabled": *enabled,
                },
            }),
            PartitionCommand::GrantRoles {
                name,
                roles,
                revoke,
            } => Some(Self::Auth {
                spec: doc! {
                    "op": if *revoke { "revokeRoles" } else { "grantRoles" },
                    "name": name.clone(),
                    "roles": roles.clone(),
                },
            }),
            PartitionCommand::CreateRole { name, actions } => Some(Self::Auth {
                spec: doc! {
                    "op": "createRole",
                    "name": name.clone(),
                    "actions": actions.clone(),
                },
            }),
            PartitionCommand::DropRole { name } => Some(Self::Auth {
                spec: doc! { "op": "dropRole", "name": name.clone() },
            }),
            PartitionCommand::Find { .. }
            | PartitionCommand::Aggregate { .. }
            | PartitionCommand::Count { .. }
            | PartitionCommand::ListIndexes { .. }
            | PartitionCommand::ListCollections { .. }
            | PartitionCommand::ListDatabases { .. }
            | PartitionCommand::CollectionStats { .. }
            | PartitionCommand::DatabaseStats { .. }
            | PartitionCommand::Search { .. }
            | PartitionCommand::BeginTransaction { .. }
            | PartitionCommand::CommitTransaction { .. }
            | PartitionCommand::AbortTransaction { .. }
            | PartitionCommand::Explain { .. }
            | PartitionCommand::Transactional { .. }
            | PartitionCommand::Authenticate { .. } => None,
        }
    }

    /// Rebuilds the commands a worker applies during recovery.
    ///
    /// A transaction record expands into its inner mutations, in order.
    pub fn into_commands(self) -> Vec<PartitionCommand> {
        match self {
            Self::Transaction { ops } => ops.into_iter().flat_map(Self::into_commands).collect(),
            other => vec![other.into_command()],
        }
    }

    /// Rebuilds the command a worker applies during recovery.
    fn into_command(self) -> PartitionCommand {
        match self {
            Self::Insert {
                collection,
                documents,
            } => PartitionCommand::Insert {
                collection,
                documents,
            },
            Self::Update {
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
            Self::Delete {
                collection,
                filter,
                limit,
            } => PartitionCommand::Delete {
                collection,
                filter,
                limit,
            },
            Self::Drop { collection } => PartitionCommand::Drop { collection },
            Self::DropDatabase { database } => PartitionCommand::DropDatabase { database },
            Self::CreateCollection { collection } => {
                PartitionCommand::CreateCollection { collection }
            }
            Self::CreateIndexes {
                collection,
                indexes,
            } => PartitionCommand::CreateIndexes {
                collection,
                indexes,
            },
            Self::DropIndexes { collection, index } => {
                PartitionCommand::DropIndexes { collection, index }
            }
            Self::CollMod {
                collection,
                validator,
            } => PartitionCommand::CollMod {
                collection,
                validator,
            },
            Self::Auth { spec } => auth_command(&spec),
            Self::Transaction { .. } => {
                unreachable!("transaction records expand in into_commands")
            }
        }
    }

    /// Short name used in debug logs.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Insert { .. } => "insert",
            Self::Update { .. } => "update",
            Self::Delete { .. } => "delete",
            Self::Drop { .. } => "drop",
            Self::DropDatabase { .. } => "dropDatabase",
            Self::CreateCollection { .. } => "create",
            Self::CreateIndexes { .. } => "createIndexes",
            Self::DropIndexes { .. } => "dropIndexes",
            Self::CollMod { .. } => "collMod",
            Self::Auth { spec } => match spec.get_str("op").unwrap_or("auth") {
                "createUser" => "createUser",
                "updateUser" => "updateUser",
                "grantRoles" => "grantRoles",
                "revokeRoles" => "revokeRoles",
                "createRole" => "createRole",
                "dropRole" => "dropRole",
                _ => "auth",
            },
            Self::Transaction { .. } => "transaction",
        }
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut body = doc! { "op": self.name() };
        match self {
            Self::Insert {
                collection,
                documents,
            } => {
                body.insert("ns", collection.clone());
                body.insert("docs", docs_to_bson(documents));
            }
            Self::Update {
                collection,
                filter,
                update,
                multi,
            } => {
                body.insert("ns", collection.clone());
                body.insert("filter", filter.clone());
                body.insert("update", update.clone());
                body.insert("multi", *multi);
            }
            Self::Delete {
                collection,
                filter,
                limit,
            } => {
                body.insert("ns", collection.clone());
                body.insert("filter", filter.clone());
                body.insert("limit", i64::from(*limit));
            }
            Self::Drop { collection } | Self::CreateCollection { collection } => {
                body.insert("ns", collection.clone());
            }
            Self::DropDatabase { database } => {
                body.insert("db", database.clone());
            }
            Self::CreateIndexes {
                collection,
                indexes,
            } => {
                body.insert("ns", collection.clone());
                body.insert("indexes", docs_to_bson(indexes));
            }
            Self::DropIndexes { collection, index } => {
                body.insert("ns", collection.clone());
                body.insert("index", index.clone());
            }
            Self::CollMod {
                collection,
                validator,
            } => {
                body.insert("ns", collection.clone());
                body.insert("validator", validator.clone());
            }
            Self::Auth { spec } => {
                body = spec.clone();
            }
            Self::Transaction { ops } => {
                let encoded = ops
                    .iter()
                    .map(|op| {
                        bson::from_slice::<Document>(&op.encode()?).map_err(|err| err.to_string())
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                body.insert(
                    "ops",
                    Bson::Array(encoded.into_iter().map(Bson::Document).collect()),
                );
            }
        }
        bson::to_vec(&body).map_err(|err| format!("wal encode: {}", err))
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        let body: Document =
            bson::from_slice(bytes).map_err(|err| format!("wal payload: {}", err))?;
        let op = body
            .get_str("op")
            .map_err(|_| "wal payload is missing op".to_string())?;
        match op {
            "insert" => Ok(Self::Insert {
                collection: required_str(&body, "ns")?,
                documents: required_docs(&body, "docs")?,
            }),
            "update" => Ok(Self::Update {
                collection: required_str(&body, "ns")?,
                filter: required_doc(&body, "filter")?,
                update: required_doc(&body, "update")?,
                multi: body.get_bool("multi").unwrap_or(false),
            }),
            "delete" => Ok(Self::Delete {
                collection: required_str(&body, "ns")?,
                filter: required_doc(&body, "filter")?,
                limit: body.get_i64("limit").unwrap_or(0) as u32,
            }),
            "drop" => Ok(Self::Drop {
                collection: required_str(&body, "ns")?,
            }),
            "dropDatabase" => Ok(Self::DropDatabase {
                database: required_str(&body, "db")?,
            }),
            "create" => Ok(Self::CreateCollection {
                collection: required_str(&body, "ns")?,
            }),
            "createIndexes" => Ok(Self::CreateIndexes {
                collection: required_str(&body, "ns")?,
                indexes: required_docs(&body, "indexes")?,
            }),
            "dropIndexes" => Ok(Self::DropIndexes {
                collection: required_str(&body, "ns")?,
                index: required_str(&body, "index")?,
            }),
            "collMod" => Ok(Self::CollMod {
                collection: required_str(&body, "ns")?,
                validator: required_doc(&body, "validator")?,
            }),
            "createUser" | "updateUser" | "grantRoles" | "revokeRoles" | "createRole"
            | "dropRole" => Ok(Self::Auth { spec: body }),
            "transaction" => {
                let values = body
                    .get_array("ops")
                    .map_err(|_| "wal transaction is missing ops".to_string())?;
                let mut ops = Vec::with_capacity(values.len());
                for value in values {
                    let document = value
                        .as_document()
                        .ok_or_else(|| "wal transaction op must be a document".to_string())?;
                    let bytes = bson::to_vec(document).map_err(|err| err.to_string())?;
                    ops.push(Self::decode(&bytes)?);
                }
                Ok(Self::Transaction { ops })
            }
            other => Err(format!("wal payload has unknown op {}", other)),
        }
    }
}

impl Wal {
    /// Creates the log directory. Call [`Self::replay`] before appending
    /// when the directory may already hold segments.
    pub fn open(
        dir: impl Into<PathBuf>,
        partition: u32,
        fsync: bool,
        segment_bytes: u64,
        key: Option<[u8; 32]>,
    ) -> Result<Self, String> {
        let dir = dir.into();
        fs::create_dir_all(&dir).map_err(|err| io_err(&dir, err))?;
        if segment_bytes < SEGMENT_HEADER_LEN as u64 {
            return Err(format!(
                "wal segment size {} is smaller than the header",
                segment_bytes
            ));
        }
        Ok(Self {
            dir,
            partition,
            fsync,
            segment_bytes,
            next_lsn: 1,
            next_segment: 1,
            current: None,
            key,
        })
    }

    /// Raises the next LSN. Used when a checkpoint remembered a higher one.
    pub fn set_next_lsn(&mut self, next: u64) {
        if next > self.next_lsn {
            self.next_lsn = next;
        }
    }

    /// LSN that the next append will use.
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    /// Highest assigned LSN, or 0 when nothing has been logged.
    pub fn last_lsn(&self) -> u64 {
        self.next_lsn.saturating_sub(1)
    }

    /// Reads every segment. A torn tail on the last segment is truncated.
    /// A checksum failure is an error. After this returns, appends continue
    /// at the end of the last good byte.
    pub fn replay(&mut self) -> Result<Vec<WalRecord>, String> {
        let segments = self.list_segments()?;
        let last = segments.last().copied();
        let mut records = Vec::new();
        for id in &segments {
            let allow_torn = Some(*id) == last;
            records.extend(self.read_segment(*id, allow_torn)?);
        }
        self.next_segment = last.map(|id| id + 1).unwrap_or(1);
        self.position_for_append(last)?;
        for record in &records {
            if record.lsn.saturating_add(1) > self.next_lsn {
                self.next_lsn = record.lsn + 1;
            }
        }
        Ok(records)
    }

    /// Appends one mutation and returns its LSN.
    pub fn append(&mut self, op: &WalOp) -> Result<u64, String> {
        let payload = op.encode()?;
        if payload.len() > MAX_PAYLOAD {
            return Err(format!("wal record is {} bytes", payload.len()));
        }
        let lsn = self.next_lsn;
        let encoded = encode_record(lsn, &payload, self.key.as_ref())?;
        self.ensure_room(encoded.len())?;
        let segment = self
            .current
            .as_mut()
            .ok_or_else(|| "wal has no open segment".to_string())?;
        segment
            .file
            .write_all(&encoded)
            .map_err(|err| format!("wal append: {}", err))?;
        if self.fsync {
            segment
                .file
                .sync_all()
                .map_err(|err| format!("wal fsync: {}", err))?;
        }
        segment.len += encoded.len() as u64;
        self.next_lsn += 1;
        LOGGER.debug(format!(
            "partition {} wal lsn {} op {}",
            self.partition,
            lsn,
            op.name()
        ));
        Ok(lsn)
    }

    /// Deletes every segment. The next append starts a new file.
    /// The LSN counter is left unchanged.
    pub fn discard_segments(&mut self) -> Result<(), String> {
        self.current = None;
        for id in self.list_segments()? {
            let path = self.segment_path(id);
            fs::remove_file(&path).map_err(|err| io_err(&path, err))?;
        }
        self.next_segment = 1;
        Ok(())
    }

    /// Segment ids present on disk, in order.
    #[cfg(test)]
    pub fn segment_ids(&self) -> Result<Vec<u64>, String> {
        self.list_segments()
    }

    fn ensure_room(&mut self, record_len: usize) -> Result<(), String> {
        let need_roll = match &self.current {
            None => true,
            Some(segment) => {
                let empty = segment.len <= SEGMENT_HEADER_LEN as u64;
                let fits = segment.len + record_len as u64 <= self.segment_bytes;
                !fits && !empty
            }
        };
        if need_roll {
            self.roll()?;
        }
        Ok(())
    }

    fn roll(&mut self) -> Result<(), String> {
        let replacing = self.current.is_some();
        self.current = None;
        let id = self.next_segment;
        self.next_segment += 1;
        let path = self.segment_path(id);
        let mut file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|err| io_err(&path, err))?;
        let header = segment_header(self.partition, id);
        file.write_all(&header).map_err(|err| io_err(&path, err))?;
        if self.fsync {
            file.sync_all().map_err(|err| io_err(&path, err))?;
        }
        if replacing {
            LOGGER.warn(format!(
                "partition {} WAL rolled to segment {:016x}",
                self.partition, id
            ));
        }
        self.current = Some(SegmentFile {
            file,
            len: SEGMENT_HEADER_LEN as u64,
        });
        Ok(())
    }

    fn position_for_append(&mut self, last: Option<u64>) -> Result<(), String> {
        self.current = None;
        let Some(id) = last else {
            return Ok(());
        };
        let path = self.segment_path(id);
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|err| io_err(&path, err))?;
        let len = file.metadata().map_err(|err| io_err(&path, err))?.len();
        // replay read the segment from the start. Appends must continue
        // after the last good byte, not overwrite the header.
        file.seek(SeekFrom::Start(len))
            .map_err(|err| io_err(&path, err))?;
        self.current = Some(SegmentFile { file, len });
        Ok(())
    }

    fn read_segment(&self, id: u64, allow_torn: bool) -> Result<Vec<WalRecord>, String> {
        let path = self.segment_path(id);
        let mut file = OpenOptions::new()
            .read(true)
            .write(allow_torn)
            .open(&path)
            .map_err(|err| io_err(&path, err))?;
        let header = read_exact_or_err(&mut file, SEGMENT_HEADER_LEN, &path)?;
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        if magic != SEGMENT_MAGIC {
            return Err(format!(
                "{}: segment magic {:08x} is not WAL1",
                path.display(),
                magic
            ));
        }
        let stored_id = u64::from_le_bytes(header[8..16].try_into().unwrap());
        if stored_id != id {
            return Err(format!(
                "{}: segment id {} does not match file name {}",
                path.display(),
                stored_id,
                id
            ));
        }

        let mut records = Vec::new();
        let mut offset = SEGMENT_HEADER_LEN as u64;
        loop {
            match read_record(&mut file, self.key.as_ref())? {
                ReadRecord::Eof => break,
                ReadRecord::Record { record, len } => {
                    records.push(record);
                    offset += len;
                }
                ReadRecord::Torn => {
                    if !allow_torn {
                        return Err(format!(
                            "{}: torn record before the last WAL segment",
                            path.display()
                        ));
                    }
                    file.set_len(offset).map_err(|err| {
                        format!("{}: truncate torn tail: {}", path.display(), err)
                    })?;
                    if self.fsync {
                        let _ = file.sync_all();
                    }
                    LOGGER.warn(format!(
                        "partition {} truncated torn WAL tail at offset {} in segment {:016x}",
                        self.partition, offset, id
                    ));
                    break;
                }
                ReadRecord::Corrupt(message) => {
                    return Err(format!("{}: {}", path.display(), message));
                }
            }
        }
        Ok(records)
    }

    fn list_segments(&self) -> Result<Vec<u64>, String> {
        let mut ids = Vec::new();
        if !self.dir.exists() {
            return Ok(ids);
        }
        for entry in fs::read_dir(&self.dir).map_err(|err| io_err(&self.dir, err))? {
            let entry = entry.map_err(|err| io_err(&self.dir, err))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(hex) = name.strip_suffix(".wal") {
                if let Ok(id) = u64::from_str_radix(hex, 16) {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    fn segment_path(&self, id: u64) -> PathBuf {
        self.dir.join(format!("{id:016x}.wal"))
    }
}

enum ReadRecord {
    Eof,
    Torn,
    Record { record: WalRecord, len: u64 },
    Corrupt(String),
}

fn read_record(file: &mut File, key: Option<&[u8; 32]>) -> Result<ReadRecord, String> {
    let mut header = [0u8; RECORD_HEADER_LEN];
    match read_full(file, &mut header).map_err(|err| format!("wal read: {}", err))? {
        Fill::Eof => return Ok(ReadRecord::Eof),
        Fill::Partial => return Ok(ReadRecord::Torn),
        Fill::Full => {}
    }
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    if magic != RECORD_MAGIC {
        return Ok(ReadRecord::Corrupt(format!(
            "record magic {:08x} is not REC1",
            magic
        )));
    }
    let lsn = u64::from_le_bytes(header[4..12].try_into().unwrap());
    let stored_crc = u32::from_le_bytes(header[12..16].try_into().unwrap());
    let len = u32::from_le_bytes(header[16..20].try_into().unwrap()) as usize;
    if len > MAX_PAYLOAD {
        return Ok(ReadRecord::Corrupt(format!(
            "record length {} exceeds the maximum",
            len
        )));
    }
    let mut payload = vec![0u8; len];
    match read_full(file, &mut payload).map_err(|err| format!("wal read: {}", err))? {
        Fill::Full => {}
        Fill::Eof | Fill::Partial => return Ok(ReadRecord::Torn),
    }
    let mut crc_input = Vec::with_capacity(8 + payload.len());
    crc_input.extend_from_slice(&lsn.to_le_bytes());
    crc_input.extend_from_slice(&payload);
    let actual = crc32(&crc_input);
    if actual != stored_crc {
        return Ok(ReadRecord::Corrupt(format!(
            "lsn {} checksum mismatch (stored {:08x}, computed {:08x})",
            lsn, stored_crc, actual
        )));
    }
    let payload = match unprotect(&payload, key) {
        Ok(payload) => payload,
        Err(err) => return Ok(ReadRecord::Corrupt(err)),
    };
    let op = match WalOp::decode(&payload) {
        Ok(op) => op,
        Err(err) => return Ok(ReadRecord::Corrupt(err)),
    };
    Ok(ReadRecord::Record {
        record: WalRecord { lsn, op },
        len: (RECORD_HEADER_LEN + len) as u64,
    })
}

fn encode_record(lsn: u64, payload: &[u8], key: Option<&[u8; 32]>) -> Result<Vec<u8>, String> {
    let payload = protect(payload, key)?;
    if payload.len() > MAX_PAYLOAD {
        return Err(format!("wal record is {} bytes", payload.len()));
    }
    let mut crc_input = Vec::with_capacity(8 + payload.len());
    crc_input.extend_from_slice(&lsn.to_le_bytes());
    crc_input.extend_from_slice(&payload);
    let checksum = crc32(&crc_input);
    let mut out = Vec::with_capacity(RECORD_HEADER_LEN + payload.len());
    out.extend_from_slice(&RECORD_MAGIC.to_le_bytes());
    out.extend_from_slice(&lsn.to_le_bytes());
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

fn protect(payload: &[u8], key: Option<&[u8; 32]>) -> Result<Vec<u8>, String> {
    let Some(key) = key else {
        return Ok(payload.to_vec());
    };
    let sealed = crypto::seal(key, payload)?;
    let mut out = Vec::with_capacity(4 + sealed.len());
    out.extend_from_slice(&WAL_SEAL_MAGIC.to_le_bytes());
    out.extend_from_slice(&sealed);
    Ok(out)
}

fn unprotect(payload: &[u8], key: Option<&[u8; 32]>) -> Result<Vec<u8>, String> {
    let sealed = payload.len() >= 4
        && u32::from_le_bytes(payload[..4].try_into().unwrap()) == WAL_SEAL_MAGIC;
    match (sealed, key) {
        (false, None) => Ok(payload.to_vec()),
        (false, Some(_)) => Err("wal record is not encrypted".into()),
        (true, None) => Err("wal record is encrypted".into()),
        (true, Some(key)) => crypto::open(key, &payload[4..]),
    }
}

fn auth_command(spec: &Document) -> PartitionCommand {
    let name = spec.get_str("name").unwrap_or("").to_string();
    let roles = spec
        .get_array("roles")
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default();
    match spec.get_str("op").unwrap_or("") {
        "updateUser" => PartitionCommand::UpdateUser {
            name,
            enabled: spec.get_bool("enabled").unwrap_or(true),
        },
        "grantRoles" => PartitionCommand::GrantRoles {
            name,
            roles,
            revoke: false,
        },
        "revokeRoles" => PartitionCommand::GrantRoles {
            name,
            roles,
            revoke: true,
        },
        "createRole" => PartitionCommand::CreateRole {
            name,
            actions: spec
                .get_array("actions")
                .map(|values| {
                    values
                        .iter()
                        .filter_map(|value| value.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default(),
        },
        "dropRole" => PartitionCommand::DropRole { name },
        _ => PartitionCommand::CreateUser {
            name,
            hash: spec.get_str("hash").unwrap_or("").to_string(),
            roles,
            trusted: spec.get_bool("trusted").unwrap_or(false),
        },
    }
}

fn segment_header(partition: u32, segment: u64) -> [u8; SEGMENT_HEADER_LEN] {
    let mut buf = [0u8; SEGMENT_HEADER_LEN];
    buf[0..4].copy_from_slice(&SEGMENT_MAGIC.to_le_bytes());
    buf[4..8].copy_from_slice(&1u32.to_le_bytes());
    buf[8..16].copy_from_slice(&segment.to_le_bytes());
    buf[16..20].copy_from_slice(&partition.to_le_bytes());
    buf
}

enum Fill {
    Eof,
    Partial,
    Full,
}

fn read_full(file: &mut File, buf: &mut [u8]) -> std::io::Result<Fill> {
    let mut filled = 0;
    while filled < buf.len() {
        match file.read(&mut buf[filled..])? {
            0 => break,
            n => filled += n,
        }
    }
    if filled == 0 {
        Ok(Fill::Eof)
    } else if filled < buf.len() {
        Ok(Fill::Partial)
    } else {
        Ok(Fill::Full)
    }
}

fn read_exact_or_err(file: &mut File, len: usize, path: &Path) -> Result<Vec<u8>, String> {
    let mut buf = vec![0u8; len];
    match read_full(file, &mut buf).map_err(|err| io_err(path, err))? {
        Fill::Full => Ok(buf),
        Fill::Eof | Fill::Partial => Err(format!("{}: truncated segment header", path.display())),
    }
}

fn docs_to_bson(docs: &[Document]) -> Bson {
    Bson::Array(docs.iter().cloned().map(Bson::Document).collect())
}

fn required_str(doc: &Document, key: &str) -> Result<String, String> {
    doc.get_str(key)
        .map(str::to_string)
        .map_err(|_| format!("wal payload is missing {}", key))
}

fn required_doc(doc: &Document, key: &str) -> Result<Document, String> {
    doc.get_document(key)
        .cloned()
        .map_err(|_| format!("wal payload is missing {}", key))
}

fn required_docs(doc: &Document, key: &str) -> Result<Vec<Document>, String> {
    let array = doc
        .get_array(key)
        .map_err(|_| format!("wal payload is missing {}", key))?;
    let mut docs = Vec::with_capacity(array.len());
    for value in array {
        match value {
            Bson::Document(document) => docs.push(document.clone()),
            _ => return Err(format!("wal payload {}.item is not a document", key)),
        }
    }
    Ok(docs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn scratch() -> PathBuf {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("kova-wal-{}-{}", std::process::id(), n));
        fs::create_dir_all(&path).unwrap();
        path
    }

    fn sample(name: &str) -> WalOp {
        WalOp::Insert {
            collection: "db.emp".into(),
            documents: vec![doc! { "name": name }],
        }
    }

    #[test]
    fn test_replay_preserves_order_and_rolls_segments() {
        let dir = scratch();
        let mut wal = Wal::open(&dir, 0, false, 64, None).unwrap();
        wal.replay().unwrap();
        wal.append(&sample("a")).unwrap();
        wal.append(&sample("b")).unwrap();
        wal.append(&sample("c")).unwrap();
        assert!(wal.segment_ids().unwrap().len() >= 2);

        let mut wal = Wal::open(&dir, 0, false, 64, None).unwrap();
        let records = wal.replay().unwrap();
        let names: Vec<&str> = records
            .iter()
            .map(|record| match &record.op {
                WalOp::Insert { documents, .. } => documents[0].get_str("name").unwrap(),
                _ => panic!("insert"),
            })
            .collect();
        assert_eq!(names, ["a", "b", "c"]);
        assert_eq!(records[0].lsn, 1);
        assert_eq!(wal.next_lsn(), 4);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_torn_tail_is_truncated_and_appends_continue() {
        let dir = scratch();
        let mut wal = Wal::open(&dir, 0, false, 1024 * 1024, None).unwrap();
        wal.replay().unwrap();
        wal.append(&sample("kept")).unwrap();
        drop(wal);

        let segment = fs::read_dir(&dir).unwrap().next().unwrap().unwrap().path();
        let mut file = OpenOptions::new().append(true).open(&segment).unwrap();
        file.write_all(&[0x11, 0x22, 0x33, 0x44, 0x55]).unwrap();
        drop(file);

        let mut wal = Wal::open(&dir, 0, false, 1024 * 1024, None).unwrap();
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 1);
        wal.append(&sample("after")).unwrap();

        let mut wal = Wal::open(&dir, 0, false, 1024 * 1024, None).unwrap();
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 2);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn test_bad_checksum_is_an_error() {
        let dir = scratch();
        let mut wal = Wal::open(&dir, 0, false, 1024 * 1024, None).unwrap();
        wal.replay().unwrap();
        wal.append(&sample("kept")).unwrap();
        drop(wal);

        let segment = fs::read_dir(&dir).unwrap().next().unwrap().unwrap().path();
        let bytes = fs::read(&segment).unwrap();
        // Flip a payload byte past the header and the record header.
        let mut bytes = bytes;
        let at = SEGMENT_HEADER_LEN + RECORD_HEADER_LEN + 4;
        bytes[at] ^= 0xff;
        fs::write(&segment, bytes).unwrap();

        let mut wal = Wal::open(&dir, 0, false, 1024 * 1024, None).unwrap();
        let err = wal.replay().expect_err("checksum");
        assert!(err.contains("checksum"), "{}", err);
        let _ = fs::remove_dir_all(dir);
    }
}
