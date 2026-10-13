//! MongoDB Wire Protocol message decoding and encoding (OP_MSG).
//!
//! This module turns raw bytes coming from a client into typed
//! [`PartitionCommand`]s, and builds the OP_MSG replies that go back.
//! It is pure byte/document manipulation with no I/O, so every function
//! here is unit-testable without a socket.

use bson::{doc, from_slice, to_vec, Document};

use crate::types::{DecaySpec, PartitionCommand, PartitionResult, SearchOptions};

/// OP_MSG operation code.
pub const OP_MSG: i32 = 2013;

/// OP_REPLY operation code (the response to an OP_QUERY).
///
/// Drivers open the connection with a legacy OP_QUERY handshake, and
/// the wire protocol requires the answer to carry this opcode.
pub const OP_REPLY: i32 = 1;

/// Size of the fixed 16-byte message header.
pub const HEADER_SIZE: usize = 16;

/// Size of the 4-byte `flagBits` field that follows the header in OP_MSG.
pub const FLAG_BITS_SIZE: usize = 4;

/// Largest message the server accepts from a client.
///
/// Matches `maxMessageSizeBytes` announced in the `hello` reply, so a
/// frame claiming more is a protocol violation rather than a size we
/// should allocate for.
pub const MAX_MESSAGE_SIZE: usize = 48_000_000;

/// A decoded OP_MSG payload: the command document a client sent us.
#[derive(Debug, Clone)]
pub struct OpMsg {
    pub flag_bits: u32,
    pub command: Document,
}

/// Decodes an OP_MSG body (everything after the 16-byte header).
///
/// Layout: `flagBits` (4 bytes) followed by sections. Only section kind 0
/// (a single BSON document) is read, which is what official MongoDB
/// drivers send for command requests.
pub fn decode_op_msg(body: &[u8]) -> Result<OpMsg, String> {
    if body.len() < FLAG_BITS_SIZE {
        return Err(format!(
            "OP_MSG body too short: {} bytes (need at least {})",
            body.len(),
            FLAG_BITS_SIZE
        ));
    }

    // Every integer in the wire protocol is little-endian.
    let flag_bits = u32::from_le_bytes(
        body[0..4]
            .try_into()
            .map_err(|_| "flagBits must be 4 bytes".to_string())?,
    );

    let mut sections = &body[FLAG_BITS_SIZE..];

    // Bit 0 marks a CRC-32C checksum trailing the sections; it is not
    // part of any section payload. The MongoDB wire spec assigns this
    // flag to bit 0 (value 1). The Java driver (Studio 3T, Spring
    // Data) sets it, the Node driver does not, which is why a wrong
    // bit mask only broke Java clients: their checksum lingered as
    // trailing garbage and poisoned the section walk.
    const CHECKSUM_PRESENT: u32 = 1;
    if flag_bits & CHECKSUM_PRESENT != 0 {
        if sections.len() < 4 {
            return Err("OP_MSG sets the checksum flag but carries no checksum".to_string());
        }
        sections = &sections[..sections.len() - 4];
    }

    if sections.is_empty() {
        return Err("OP_MSG has no sections".to_string());
    }

    // Section 0 holds the command document; drivers may append further
    // sections (kind 1 carries insert/update/delete payloads). Each
    // section is self-delimiting, so walk them instead of assuming the
    // command BSON runs to the end of the buffer.
    let mut command: Option<Document> = None;
    let mut offset = 0usize;

    while offset < sections.len() {
        let kind = sections[offset];
        offset += 1;

        match kind {
            0 => {
                let (document, consumed) = read_bson(&sections[offset..])?;
                command = Some(document);
                offset += consumed;
            }
            1 => {
                // Document sequence (wire spec):
                //   int32 size — bytes in this sequence, including itself
                //   cstring identifier — "documents", "updates", "deletes"
                //   zero or more BSON documents
                // Drivers send insert payloads this way. The size is not
                // part of the identifier; skipping it makes the next NUL
                // inside the size field end the name early, and the BSON
                // reader then treats "\0\0do" from "documents" as a length
                // (1868824576).
                if sections.len() - offset < 4 {
                    return Err("OP_MSG section 1 is missing its size".to_string());
                }
                let size = i32::from_le_bytes(
                    sections[offset..offset + 4]
                        .try_into()
                        .map_err(|_| "section 1 size must be 4 bytes".to_string())?,
                ) as usize;
                if size < 5 || offset + size > sections.len() {
                    return Err(format!(
                        "OP_MSG section 1 size {} out of range for {} available bytes",
                        size,
                        sections.len() - offset
                    ));
                }
                let after_size = &sections[offset + 4..offset + size];
                let nul = after_size
                    .iter()
                    .position(|byte| *byte == 0)
                    .ok_or_else(|| "section 1 identifier is not terminated".to_string())?;
                let identifier = std::str::from_utf8(&after_size[..nul])
                    .map_err(|_| "section 1 identifier is not UTF-8".to_string())?
                    .to_string();

                let mut cursor = nul + 1;
                let mut sequence = Vec::new();
                while cursor < after_size.len() {
                    let (document, consumed) = read_bson(&after_size[cursor..])?;
                    sequence.push(bson::Bson::Document(document));
                    cursor += consumed;
                }

                let command = command
                    .as_mut()
                    .ok_or_else(|| "OP_MSG section 1 precedes section 0".to_string())?;
                command.insert(identifier, bson::Bson::Array(sequence));
                offset += size;
            }
            other => {
                return Err(format!("unsupported OP_MSG section kind {}", other));
            }
        }
    }

    let command = command.ok_or_else(|| "OP_MSG has no section 0".to_string())?;
    Ok(OpMsg { flag_bits, command })
}

/// Reads one BSON document from the front of `bytes`.
///
/// Returns the document and how many bytes it occupied, leaving any
/// trailing sections for the caller to continue with.
fn read_bson(bytes: &[u8]) -> Result<(Document, usize), String> {
    if bytes.len() < 4 {
        return Err(format!(
            "truncated BSON length prefix: {} bytes",
            bytes.len()
        ));
    }
    let length = i32::from_le_bytes(
        bytes[0..4]
            .try_into()
            .map_err(|_| "length prefix must be 4 bytes".to_string())?,
    ) as usize;

    if length < 5 || length > bytes.len() {
        return Err(format!(
            "BSON length {} out of range for {} available bytes",
            length,
            bytes.len()
        ));
    }

    let document: Document =
        from_slice(&bytes[..length]).map_err(|err| format!("invalid BSON section: {}", err))?;
    Ok((document, length))
}

/// Decodes an OP_QUERY body (everything after the 16-byte header).
///
/// Layout: `flagBits` (4 bytes), `fullCollectionName` (cstring),
/// `numberToSkip` (4 bytes), `numberToReturn` (4 bytes) and the query
/// BSON document. Modern drivers use OP_QUERY only for the very first
/// handshake message (`isMaster` on `admin.$cmd`); everything after
/// the handshake uses OP_MSG.
pub fn decode_op_query(body: &[u8]) -> Result<OpMsg, String> {
    if body.len() < FLAG_BITS_SIZE {
        return Err(format!(
            "OP_QUERY body too short: {} bytes (need at least {})",
            body.len(),
            FLAG_BITS_SIZE
        ));
    }

    let flag_bits = u32::from_le_bytes(
        body[0..4]
            .try_into()
            .map_err(|_| "flagBits must be 4 bytes".to_string())?,
    );

    let rest = &body[FLAG_BITS_SIZE..];

    // fullCollectionName: NUL-terminated string (e.g. "admin.$cmd").
    let nul = rest
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| "OP_QUERY is missing fullCollectionName".to_string())?;
    let _full_collection_name = std::str::from_utf8(&rest[..nul])
        .map_err(|_| "OP_QUERY fullCollectionName is not UTF-8".to_string())?;

    let after_name = &rest[nul + 1..];
    if after_name.len() < 8 {
        return Err(format!(
            "OP_QUERY missing skip/return fields: {} bytes left",
            after_name.len()
        ));
    }

    let command: Document =
        from_slice(&after_name[8..]).map_err(|err| format!("invalid BSON in OP_QUERY: {}", err))?;

    Ok(OpMsg { flag_bits, command })
}

/// Builds a full OP_REPLY frame ready to be written to the socket.
///
/// OP_REPLY is the response to a legacy OP_QUERY: header, then
/// `responseFlags` (4), `cursorID` (8), `startingFrom` (4),
/// `numberReturned` (4) and one or more documents.
pub fn encode_op_reply(
    request_id: i32,
    response_to: i32,
    body: &Document,
) -> Result<Vec<u8>, String> {
    let document = to_vec(body).map_err(|err| format!("BSON encode failed: {}", err))?;

    // header (16) + flags (4) + cursorID (8) + startingFrom (4) +
    // numberReturned (4) + document
    let message_length = (HEADER_SIZE + 4 + 8 + 4 + 4 + document.len()) as u32;

    let mut frame = Vec::with_capacity(message_length as usize);
    frame.extend_from_slice(&message_length.to_le_bytes());
    frame.extend_from_slice(&request_id.to_le_bytes());
    frame.extend_from_slice(&response_to.to_le_bytes());
    frame.extend_from_slice(&OP_REPLY.to_le_bytes());
    frame.extend_from_slice(&0i32.to_le_bytes()); // responseFlags
    frame.extend_from_slice(&0i64.to_le_bytes()); // cursorID (dead)
    frame.extend_from_slice(&0i32.to_le_bytes()); // startingFrom
    frame.extend_from_slice(&1i32.to_le_bytes()); // numberReturned
    frame.extend_from_slice(&document);

    Ok(frame)
}

/// Translates a decoded command document into a [`PartitionCommand`].
///
/// Supports the commands Phase 2 can execute: `insert`, `find`, `update`,
/// `delete` and `count`. Anything else returns an error string that is
/// surfaced to the client as a Mongo error reply.
pub fn command_from_document(command: &Document) -> Result<PartitionCommand, String> {
    // Every client command names its database through `$db`; storage
    // namespaces collections as `db.collection` so `test.users` and
    // `shop.users` are distinct collections, like real MongoDB.
    let db = command.get_str("$db").unwrap_or("");
    let namespaced = |collection: &str| namespace(db, collection);
    let txn_id = read_txn_id(command)?;

    if command.contains_key("insert") {
        let collection = command.get_str("insert").unwrap();
        let values = command
            .get_array("documents")
            .map_err(|_| "insert requires a 'documents' array")?;

        let mut documents = Vec::with_capacity(values.len());
        for value in values {
            let document = value
                .as_document()
                .ok_or_else(|| "insert requires documents to be objects".to_string())?
                .clone();
            documents.push(document);
        }

        if documents.is_empty() {
            return Err("insert requires at least one document".to_string());
        }

        return bind_txn(
            txn_id,
            PartitionCommand::Insert {
                collection: namespaced(collection),
                documents,
            },
        );
    }

    // GUI clients create collections explicitly; without this command
    // their "create collection" action fails and the collection never
    // appears in listings.
    if let Ok(collection) = command.get_str("create") {
        // MongoDB rejects dotted collection names (3.6+): the storage
        // namespace uses `.` as the db/collection separator, so a dot
        // here would be ambiguous anyway.
        if collection.contains('.') {
            return Err("collection names cannot contain '.'".to_string());
        }
        if command.get_bool("capped").unwrap_or(false) {
            warn_non_mongo(
                "'capped' collections are not supported by Kova; creating a normal collection",
            );
        }
        return bind_txn(
            txn_id,
            PartitionCommand::CreateCollection {
                collection: namespaced(collection),
            },
        );
    }

    if command.contains_key("find") {
        let collection = command.get_str("find").unwrap();
        let filter = command
            .get_document("filter")
            .cloned()
            .unwrap_or_else(|_| doc! {});

        // `{ $text: { $search: "..." } }` is MongoDB's full-text
        // operator; route it to the search index instead of the
        // equality matcher.
        if let Ok(text) = filter.get_document("$text") {
            let search_query = text
                .get_str("$search")
                .map_err(|_| "$text requires a '$search' string")?
                .to_string();

            let options = parse_text_options(text)?;

            // Keep the client's other predicates as a post-filter.
            let mut rest = filter.clone();
            rest.remove("$text");

            return bind_txn(
                txn_id,
                PartitionCommand::Search {
                    collection: namespaced(collection),
                    query: search_query,
                    filter: rest,
                    options,
                },
            );
        }

        let projection = optional_document(command, "projection")?;
        let sort = optional_document(command, "sort")?;
        let skip = non_negative_field(command, "skip")?.unwrap_or(0);
        // A find limit of 0 means "no cap", matching MongoDB.
        let limit = non_negative_field(command, "limit")?.filter(|limit| *limit > 0);

        return bind_txn(
            txn_id,
            PartitionCommand::Find {
                collection: namespaced(collection),
                filter,
                projection,
                sort,
                skip,
                limit,
            },
        );
    }

    if let Ok(collection) = command.get_str("aggregate") {
        let values = command
            .get_array("pipeline")
            .map_err(|_| "aggregate requires a 'pipeline' array")?;
        let mut pipeline = Vec::with_capacity(values.len());
        for value in values {
            let mut stage = value
                .as_document()
                .ok_or_else(|| "aggregate pipeline stages must be documents".to_string())?
                .clone();
            namespace_stage(&mut stage, db)?;
            pipeline.push(stage);
        }
        return bind_txn(
            txn_id,
            PartitionCommand::Aggregate {
                collection: namespaced(collection),
                pipeline,
                resolved: Vec::new(),
            },
        );
    }

    if command.contains_key("update") {
        let collection = command.get_str("update").unwrap();
        let updates = command
            .get_array("updates")
            .map_err(|_| "update requires an 'updates' array")?;

        let entry = updates
            .first()
            .and_then(|value| value.as_document())
            .ok_or_else(|| "update requires at least one entry".to_string())?;

        let filter = entry.get_document("q").cloned().unwrap_or_else(|_| doc! {});
        let update = entry
            .get_document("u")
            .map_err(|_| "update entry requires a 'u' document")?
            .clone();
        // The MongoDB `update` command defaults to single-document
        // updates; `multi: true` is required for updateMany.
        let multi = entry.get_bool("multi").unwrap_or(false);

        return bind_txn(
            txn_id,
            PartitionCommand::Update {
                collection: namespaced(collection),
                filter,
                update,
                multi,
            },
        );
    }

    if command.contains_key("delete") {
        let collection = command.get_str("delete").unwrap();
        let deletes = command
            .get_array("deletes")
            .map_err(|_| "delete requires a 'deletes' array")?;

        let entry = deletes
            .first()
            .and_then(|value| value.as_document())
            .ok_or_else(|| "delete requires at least one entry".to_string())?;

        let filter = entry.get_document("q").cloned().unwrap_or_else(|_| doc! {});
        // `limit` 0 means "delete all matches" (deleteMany); deleteOne
        // sends 1.
        let limit = entry
            .get_i32("limit")
            .or_else(|_| entry.get_i64("limit").map(|v| v as i32))
            .unwrap_or(0)
            .max(0) as u32;

        return bind_txn(
            txn_id,
            PartitionCommand::Delete {
                collection: namespaced(collection),
                filter,
                limit,
            },
        );
    }

    if command.contains_key("count") {
        let collection = command.get_str("count").unwrap();
        let filter = command
            .get_document("query")
            .cloned()
            .unwrap_or_else(|_| doc! {});

        return bind_txn(
            txn_id,
            PartitionCommand::Count {
                collection: namespaced(collection),
                filter,
            },
        );
    }

    if command.contains_key("drop") {
        let collection = command.get_str("drop").unwrap();
        return bind_txn(
            txn_id,
            PartitionCommand::Drop {
                collection: namespaced(collection),
            },
        );
    }

    // Java drivers (Spring Data, the MongoDB Java driver) list a
    // collection's indexes whenever it is browsed; without this
    // command the browse view fails with "unsupported command".
    if let Ok(collection) = command.get_str("listIndexes") {
        return bind_txn(
            txn_id,
            PartitionCommand::ListIndexes {
                collection: namespaced(collection),
            },
        );
    }

    // The same drivers declare indexes at start-up. Kova records the
    // declarations (metadata until Phase 10) so `listIndexes` reports
    // a stable, non-surprising set.
    if let Ok(collection) = command.get_str("createIndexes") {
        let indexes = command
            .get_array("indexes")
            .map_err(|_| "createIndexes requires an 'indexes' array")?
            .iter()
            .filter_map(|value| value.as_document().cloned())
            .collect();

        return bind_txn(
            txn_id,
            PartitionCommand::CreateIndexes {
                collection: namespaced(collection),
                indexes,
            },
        );
    }

    if let Ok(collection) = command
        .get_str("dropIndexes")
        .or_else(|_| command.get_str("deleteIndexes"))
    {
        let index = command.get_str("index").unwrap_or("*").to_string();
        return bind_txn(
            txn_id,
            PartitionCommand::DropIndexes {
                collection: namespaced(collection),
                index,
            },
        );
    }

    // GUI clients drop whole databases from their sidebar.
    if command.contains_key("dropDatabase") {
        return bind_txn(
            txn_id,
            PartitionCommand::DropDatabase {
                database: if db.is_empty() {
                    "admin".to_string()
                } else {
                    db.to_string()
                },
            },
        );
    }

    if command.contains_key("listCollections") {
        let filter = command
            .get_document("filter")
            .cloned()
            .unwrap_or_else(|_| doc! {});

        return bind_txn(
            txn_id,
            PartitionCommand::ListCollections {
                database: db.to_string(),
                filter,
            },
        );
    }

    // GUI clients enumerate the database tree with `listDatabases`.
    if command.contains_key("listDatabases") {
        let filter = command
            .get_document("filter")
            .cloned()
            .unwrap_or_else(|_| doc! {});

        return bind_txn(txn_id, PartitionCommand::ListDatabases { filter });
    }

    // Studio 3T and Compass render a collection's storage panel from
    // `collStats`; without it the panel stays empty.
    if let Ok(collection) = command.get_str("collStats") {
        return bind_txn(
            txn_id,
            PartitionCommand::CollectionStats {
                collection: namespaced(collection),
            },
        );
    }

    // The same tools size a whole database with `dbStats`.
    if command.contains_key("dbStats") {
        return bind_txn(
            txn_id,
            PartitionCommand::DatabaseStats {
                database: if db.is_empty() {
                    "admin".to_string()
                } else {
                    db.to_string()
                },
            },
        );
    }

    if command.contains_key("beginTransaction") {
        let collection = command
            .get_str("collection")
            .map_err(|_| "beginTransaction requires a collection".to_string())?;
        let isolation = command
            .get_str("isolation")
            .unwrap_or("snapshot")
            .to_string();
        let timeout_ms = non_negative_field(command, "timeoutMs")?;
        return Ok(PartitionCommand::BeginTransaction {
            collection: namespaced(collection),
            txn_id: txn_id.unwrap_or(0),
            isolation,
            timeout_ms,
        });
    }

    if command.contains_key("commit") {
        let collection = command
            .get_str("collection")
            .map_err(|_| "commit requires a collection".to_string())?;
        let txn_id = txn_id.ok_or_else(|| "commit requires txnId".to_string())?;
        return Ok(PartitionCommand::CommitTransaction {
            collection: namespaced(collection),
            txn_id,
        });
    }

    if command.contains_key("abort") {
        let collection = command
            .get_str("collection")
            .map_err(|_| "abort requires a collection".to_string())?;
        let txn_id = txn_id.ok_or_else(|| "abort requires txnId".to_string())?;
        return Ok(PartitionCommand::AbortTransaction {
            collection: namespaced(collection),
            txn_id,
        });
    }

    if let Ok(collection) = command.get_str("collMod") {
        let validator = command
            .get_document("validator")
            .map_err(|_| "collMod requires a validator document".to_string())?
            .clone();
        return bind_txn(
            txn_id,
            PartitionCommand::CollMod {
                collection: namespaced(collection),
                validator,
            },
        );
    }

    let name = command
        .keys()
        .next()
        .map(|key| key.as_str())
        .unwrap_or("<empty>");
    Err(format!("unsupported command '{}'", name))
}

fn read_txn_id(command: &Document) -> Result<Option<u64>, String> {
    if !command.contains_key("txnId") {
        return Ok(None);
    }
    let id = if let Ok(id) = command.get_i64("txnId") {
        id
    } else if let Ok(id) = command.get_i32("txnId") {
        i64::from(id)
    } else {
        return Err("txnId must be a number".to_string());
    };
    if id < 0 {
        return Err("txnId must be non-negative".to_string());
    }
    Ok(Some(id as u64))
}

/// Attaches `txnId` to a data command. Other commands reject it so a
/// client cannot believe a `drop` ran inside the transaction.
fn bind_txn(txn_id: Option<u64>, command: PartitionCommand) -> Result<PartitionCommand, String> {
    let Some(txn_id) = txn_id else {
        return Ok(command);
    };
    match command {
        PartitionCommand::Insert { .. }
        | PartitionCommand::Find { .. }
        | PartitionCommand::Update { .. }
        | PartitionCommand::Delete { .. }
        | PartitionCommand::Count { .. }
        | PartitionCommand::Aggregate { .. } => Ok(PartitionCommand::Transactional {
            txn_id,
            inner: Box::new(command),
        }),
        _ => Err(
            "txnId is only supported on insert, find, update, delete, count, and aggregate"
                .to_string(),
        ),
    }
}

fn optional_document(command: &Document, key: &str) -> Result<Option<Document>, String> {
    if !command.contains_key(key) {
        return Ok(None);
    }
    command
        .get_document(key)
        .cloned()
        .map(Some)
        .map_err(|_| format!("{key} must be a document"))
}

fn non_negative_field(command: &Document, key: &str) -> Result<Option<u64>, String> {
    if !command.contains_key(key) {
        return Ok(None);
    }
    let number = match command.get(key) {
        Some(bson::Bson::Int32(value)) => i64::from(*value),
        Some(bson::Bson::Int64(value)) => *value,
        Some(bson::Bson::Double(value)) if value.is_finite() && value.fract() == 0.0 => {
            *value as i64
        }
        _ => return Err(format!("{key} must be an integer")),
    };
    if number < 0 {
        return Err(format!("{key} must be non-negative"));
    }
    Ok(Some(number as u64))
}

/// Rewrites `$lookup.from` and `$out` to `db.collection` so a later
/// partition lookup uses the same routing key as `find`.
fn namespace_stage(stage: &mut Document, db: &str) -> Result<(), String> {
    if let Ok(mut lookup) = stage.get_document("$lookup").cloned() {
        if let Ok(from) = lookup.get_str("from") {
            lookup.insert("from", namespace(db, from));
        }
        if let Ok(pipeline) = lookup.get_array("pipeline").cloned() {
            let mut nested = Vec::with_capacity(pipeline.len());
            for value in pipeline {
                let mut child = value
                    .as_document()
                    .cloned()
                    .ok_or_else(|| "$lookup pipeline stages must be documents".to_string())?;
                namespace_stage(&mut child, db)?;
                nested.push(bson::Bson::Document(child));
            }
            lookup.insert("pipeline", nested);
        }
        stage.insert("$lookup", lookup);
    }
    if let Ok(name) = stage.get_str("$out") {
        stage.insert("$out", namespace(db, name));
    } else if let Ok(spec) = stage.get_document("$out").cloned() {
        if let Ok(coll) = spec.get_str("coll") {
            let target_db = spec.get_str("db").unwrap_or(db);
            stage.insert("$out", namespace(target_db, coll));
        }
    }
    Ok(())
}

/// The collection and filter an `explain` should ask the worker about.
///
/// A direct call of [`explain_reply`] still reports `COLLSCAN`. The
/// network layer uses this pair to ask the worker, which knows the
/// indexes, and then overwrites the stage.
pub fn explain_request(command: &Document) -> Option<(String, Document)> {
    let explained = explain_inner(command)?;
    let db = command.get_str("$db").unwrap_or("test");
    if let Ok(name) = explained.get_str("find") {
        let filter = explained
            .get_document("filter")
            .cloned()
            .unwrap_or_else(|_| doc! {});
        return Some((namespace(db, name), filter));
    }
    if let Ok(name) = explained.get_str("count") {
        let filter = explained
            .get_document("query")
            .cloned()
            .unwrap_or_else(|_| doc! {});
        return Some((namespace(db, name), filter));
    }
    if let Ok(name) = explained.get_str("aggregate") {
        let filter = explained
            .get_array("pipeline")
            .map(|pipeline| leading_match_filter(pipeline))
            .unwrap_or_else(|_| doc! {});
        return Some((namespace(db, name), filter));
    }
    None
}

fn explain_inner(command: &Document) -> Option<&Document> {
    if command.get_bool("explain").unwrap_or(false)
        && (command.contains_key("find")
            || command.contains_key("aggregate")
            || command.contains_key("count"))
    {
        Some(command)
    } else {
        command.get_document("explain").ok()
    }
}

fn leading_match_filter(pipeline: &[bson::Bson]) -> Document {
    let mut filters = Vec::new();
    for stage in pipeline {
        let Some(doc) = stage.as_document() else {
            break;
        };
        let Ok(filter) = doc.get_document("$match") else {
            break;
        };
        if filter.contains_key("$expr") {
            break;
        }
        filters.push(filter.clone());
    }
    match filters.len() {
        0 => doc! {},
        1 => filters.pop().unwrap_or_default(),
        _ => doc! { "$and": filters },
    }
}

/// Answers `explain` without running the query.
///
/// The stage written here is `COLLSCAN`. Callers that can reach a
/// worker replace it with the stage that worker reports.
pub fn explain_reply(command: &Document) -> Option<Result<Document, String>> {
    let explained = explain_inner(command)?;
    Some(explain_plan(command, explained))
}

fn explain_plan(outer: &Document, inner: &Document) -> Result<Document, String> {
    let db = outer.get_str("$db").unwrap_or("test");
    let mut plan = doc! { "stage": "COLLSCAN" };
    let collection = if let Ok(name) = inner.get_str("find") {
        if let Ok(filter) = inner.get_document("filter") {
            plan.insert("filter", filter.clone());
        }
        if let Ok(sort) = inner.get_document("sort") {
            plan.insert("sort", sort.clone());
        }
        if let Ok(projection) = inner.get_document("projection") {
            plan.insert("projection", projection.clone());
        }
        name
    } else if let Ok(name) = inner.get_str("aggregate") {
        let pipeline = inner
            .get_array("pipeline")
            .map_err(|_| "aggregate requires a 'pipeline' array")?
            .clone();
        plan.insert("pipeline", pipeline);
        name
    } else if let Ok(name) = inner.get_str("count") {
        if let Ok(filter) = inner.get_document("query") {
            plan.insert("filter", filter.clone());
        }
        name
    } else {
        let name = inner
            .keys()
            .next()
            .map(|key| key.as_str())
            .unwrap_or("<empty>");
        return Err(format!("explain does not support '{name}'"));
    };

    Ok(doc! {
        "ok": 1.0,
        "queryPlanner": {
            "plannerVersion": 1i32,
            "namespace": namespace(db, collection),
            "indexFilterSet": false,
            "winningPlan": plan,
            "rejectedPlans": [],
        },
    })
}

/// Builds the storage-side namespace for one collection.
///
/// Storage addresses collections by `db.collection`; commands without
/// a `$db` (direct engine tests, minimal clients) keep the bare name
/// so they remain valid keys in the same flat map.
fn namespace(db: &str, collection: &str) -> String {
    if db.is_empty() {
        collection.to_string()
    } else {
        format!("{}.{}", db, collection)
    }
}

/// Operators understood inside a `$text` document.
///
/// `$search` is the only one MongoDB requires; the other three are
/// Kova extensions (or unsupported MongoDB features) and produce a
/// warning when `warn_on_non_mongo_queries` is enabled.
const TEXT_OPERATORS: [&str; 4] = ["$search", "$boost", "$analyzer", "$decay"];

/// Parses the ranking options of a `$text` filter.
///
/// `$boost` (per-field weights) and `$decay` (recency half-life) are
/// Kova extensions — real MongoDB has neither, it only reads index
/// weights declared at index creation — so honoring them is warned
/// about. Operators MongoDB defines but Kova does not implement
/// (e.g. `$language`) are ignored with the same warning instead of
/// failing the query.
fn parse_text_options(text: &Document) -> Result<SearchOptions, String> {
    let mut options = SearchOptions::default();

    if let Ok(boosts) = text.get_document("$boost") {
        warn_non_mongo(
            "'$text.$boost' is a Kova extension (MongoDB declares weights at index creation)",
        );
        for (field, value) in boosts {
            let weight = bson_number(value)
                .ok_or_else(|| format!("$text.$boost.{} must be a number", field))?;
            if !weight.is_finite() || weight < 0.0 {
                return Err(format!(
                    "$text.$boost.{} must be a non-negative number",
                    field
                ));
            }
            options.boosts.push((field.clone(), weight as f32));
        }
    }

    if text.contains_key("$analyzer") {
        // The token pipeline is fixed when the index is created
        // (`[search]` in kova.toml); changing it per query would make
        // already-indexed terms unsearchable, so it is rejected loudly
        // rather than silently half-honored.
        warn_non_mongo(
            "'$text.$analyzer' is a Kova extension and is ignored (the analyzer is fixed in kova.toml [search])",
        );
    }

    if let Ok(decay) = text.get_document("$decay") {
        warn_non_mongo("'$text.$decay' is a Kova extension (MongoDB has no score decay)");
        let field = decay
            .get_str("field")
            .map_err(|_| "$text.$decay requires a 'field' string")?
            .to_string();
        let half_life_secs = decay
            .get("halfLifeSeconds")
            .and_then(bson_number)
            .ok_or_else(|| "$text.$decay requires a numeric 'halfLifeSeconds'".to_string())?;
        if !half_life_secs.is_finite() || half_life_secs <= 0.0 {
            return Err(
                "$text.$decay halfLifeSeconds must be a number greater than zero".to_string(),
            );
        }
        options.decay = Some(DecaySpec {
            field,
            half_life_secs,
        });
    }

    for key in text.keys() {
        if !TEXT_OPERATORS.contains(&key.as_str()) {
            warn_non_mongo(format!(
                "'$text.{}' is not supported by Kova and is ignored",
                key
            ));
        }
    }

    Ok(options)
}

/// Extracts a BSON number as `f64`, accepting every numeric type.
///
/// Drivers send integers as `Int32`/`Int64`, never as `Double`, so
/// parsing only `Double` would reject every real-world `$boost`.
fn bson_number(value: &bson::Bson) -> Option<f64> {
    match value {
        bson::Bson::Double(d) => Some(*d),
        bson::Bson::Int32(n) => Some(*n as f64),
        bson::Bson::Int64(n) => Some(*n as f64),
        _ => None,
    }
}

/// Logs a warning about a non-MongoDB query feature, when enabled.
///
/// The toggle lives in `kova.toml` (`warn_on_non_mongo_queries`, env
/// `KOVA_WARN_NON_MONGO_QUERIES`) so drivers that always send the
/// same extension can run quietly.
fn warn_non_mongo(message: impl AsRef<str>) {
    if crate::config::CONFIG.warn_on_non_mongo_queries {
        crate::logger::LOGGER.warn(format!("Non-MongoDB query feature: {}", message.as_ref()));
    }
}

/// Answers commands handled directly by the protocol layer.
///
/// Official MongoDB drivers issue `hello` (formerly `isMaster`) during
/// the handshake and `ping` to check liveness. These never reach
/// storage: they are answered here so the partition workers stay
/// focused on data commands.
///
/// Returns `None` when the command must be routed to storage instead.
pub fn special_command(command: &Document) -> Option<Document> {
    if command.contains_key("ping") {
        return Some(doc! { "ok": 1.0 });
    }

    if command.contains_key("hello")
        || command.contains_key("isMaster")
        || command.contains_key("ismaster")
    {
        return Some(hello_reply());
    }

    // Metadata commands every driver/tool issues at startup. They carry
    // no data-plane meaning, so answering them here keeps the mongosh
    // startup sequence quiet without teaching storage anything.
    // Accept both "buildInfo" (MongoDB canonical) and "buildinfo" (
    // Studio 3T sends the lower-case form) so neither is rejected.
    if command.contains_key("buildInfo") || command.contains_key("buildinfo") {
        return Some(build_info_reply());
    }

    if command.contains_key("getParameter") {
        return Some(get_parameter_reply(command));
    }

    // Sessions are not implemented; acknowledging `endSessions` is the
    // correct answer because the server owns no session state to drop.
    if command.contains_key("endSessions") {
        return Some(doc! { "ok": 1.0 });
    }

    // Compass and some drivers probe the server with `connectionStatus`
    // during initial connection. Kova tracks no authentication state,
    // so the honest answer is no users and no current/available readers.
    if command.contains_key("connectionStatus") {
        return Some(doc! {
            "ok": 1.0,
            "authInfo": {
                "authenticatedUsers": Vec::<bson::Bson>::new(),
                "authenticatedUserRoles": Vec::<bson::Bson>::new(),
            },
        });
    }

    // Compass and some drivers also probe with `hostInfo` to learn
    // about the server's platform. Kova is always running on the host
    // it was started on, so we report a minimal but honest reply.
    if command.contains_key("hostInfo") {
        let platform = std::env::consts::OS;
        let arch = std::env::consts::ARCH;
        return Some(doc! {
            "ok": 1.0,
            "host": platform.to_string(),
            "zone": "UTC".to_string(),
            "system": format!("{}/{}", arch, platform),
            "cpu": arch.to_string(),
        });
    }

    // The Atlas CLI and some Compass versions probe whether the server
    // is an Atlas deployment by sending `atlasVersion`. Kova is a local
    // in-memory engine, never Atlas, so the honest reply is "not set".
    if command.contains_key("atlasVersion") {
        return Some(doc! {
            "ok": 1.0,
            "version": bson::Bson::Null,
        });
    }

    None
}

/// Builds the reply to a `buildInfo` request.
///
/// The version mirrors the wire version advertised in [`hello_reply`]
/// so a client comparing both sees a consistent server. It reports
/// MongoDB 5.0.x on purpose so the Studio 3T bundled legacy `mongo`
/// shell (v5.0.x) does not refuse the connection.
///
/// Studio 3T's bundled shell is the *legacy* `mongo` shell, not
/// `mongosh`. That legacy shell only understands wire protocol up to
/// version 17 (MongoDB 5.0). When Kova advertised 8.2 / wire version
/// 27, the bundled shell exited immediately with:
///   "The shell executable cannot be run ... Could not get status from
///    the shell process. Please check that the configured executable is
///    a valid MongoDB Shell."
///
/// Real MongoDB >= 5.0 already requires mongosh, but Studio 3T still
/// bundles the legacy shell for backwards compatibility. Matching the
/// legacy shell's expectations keeps Studio 3T usable without forcing
/// the user to install mongosh first.
fn build_info_reply() -> Document {
    doc! {
        "ok": 1.0,
        "version": "7.0.15",
        "versionArray": [7, 0, 15, 0],
        "gitVersion": "kova",
        "modules": [],
        "allocator": "system",
        "javascriptEngine": "none",
        "sysInfo": "Kova in-memory engine",
    }
}

/// Builds the reply to a `getParameter` request.
///
/// Only `featureCompatibilityVersion` is answered (drivers read it to
/// detect server capabilities); any other requested parameter is
/// reported as unknown, matching a real server's behavior.
fn get_parameter_reply(command: &Document) -> Document {
    if command.get_i32("featureCompatibilityVersion").is_ok()
        || command.get_i64("featureCompatibilityVersion").is_ok()
    {
        return doc! {
            "ok": 1.0,
            // A real server reports the fcv as `major.minor` only,
            // without the patch component.
            "featureCompatibilityVersion": {
                "version": "7.0",
                "apiVersion": "1",
            },
        };
    }

    // `getParameter: "<name>"` for a parameter we do not model.
    doc! {
        "ok": 0.0,
        "errmsg": "no such parameter",
    }
}

/// Builds the reply to a `hello`/`isMaster` handshake.
///
/// The advertised limits mirror a standalone MongoDB 7.0 server so
/// clients negotiate a compatible wire version and proceed with the
/// connection. `maxWireVersion` 25 is what a real 7.0 server reports;
/// drivers negotiate `min(client_max, server_max)`, so advertising a
/// newer wire version never breaks older clients.
///
/// We intentionally advertise MongoDB 7.0 (not 8.2) because the
/// Studio 3T bundled shell (`.3T_community/third-party/2025.1.0/shell/5.0.x/mongo`)
/// is the legacy `mongo` shell, not `mongosh`. That legacy shell only
/// speaks wire protocol up to version 17 (MongoDB 5.0). When Kova
/// advertised 8.2 / wire version 27, the bundled shell exited
/// immediately with "unable to open shell".
///
/// Real MongoDB servers >= 5.0 already require mongosh, but Studio 3T
/// still bundles the legacy mongo shell for backwards compatibility.
/// Matching that legacy shell's expectations keeps Studio 3T working
/// without requiring the user to switch to mongosh first.
fn hello_reply() -> Document {
    doc! {
        "ok": 1.0,
        // Kova is always a writable primary in Phase 2.
        "isWritablePrimary": true,
        "ismaster": true,
        "helloOk": true,
        "maxBsonObjectSize": 16 * 1024 * 1024,
        "maxMessageSizeBytes": 48_000_000,
        "maxWriteBatchSize": 100_000,
        "minWireVersion": 0,
        "maxWireVersion": 25,
        "readOnly": false,
        "logicalSessionTimeoutMinutes": 30,
        "localTime": bson::DateTime::now(),
        "connectionId": 1,
    }
}

/// Converts a [`PartitionResult`] into the BSON body of a Mongo reply.
pub fn reply_document(result: &PartitionResult) -> Document {
    match result {
        PartitionResult::Inserted { inserted_count } => doc! {
            "ok": 1.0,
            "n": *inserted_count as i64,
        },
        PartitionResult::Found { documents } => {
            let mut reply = doc! { "ok": 1.0 };
            reply.insert(
                "cursor",
                doc! {
                    "id": 0i64,
                    "ns": "",
                    "firstBatch": documents.clone(),
                },
            );
            reply
        }
        PartitionResult::Counted { count } => doc! {
            "ok": 1.0,
            "n": *count as i64,
        },
        PartitionResult::Dropped { collection } => doc! {
            "ok": 1.0,
            "dropped": collection.clone(),
        },
        PartitionResult::ListedCollections { collections } => {
            let mut reply = doc! { "ok": 1.0 };
            reply.insert(
                "cursor",
                doc! {
                    "id": 0i64,
                    // The namespace is filled in by the network layer,
                    // which knows the requesting database.
                    "ns": "",
                    "firstBatch": collections.clone(),
                },
            );
            reply
        }
        PartitionResult::Modified {
            matched_count,
            modified_count,
        } => doc! {
            "ok": 1.0,
            "n": *matched_count as i64,
            "nModified": *modified_count as i64,
        },
        PartitionResult::Deleted { deleted_count } => doc! {
            "ok": 1.0,
            "n": *deleted_count as i64,
        },
        PartitionResult::Created => doc! {
            "ok": 1.0,
        },
        PartitionResult::IndexesCreated {
            num_indexes_before,
            num_indexes_after,
            created_collection,
        } => doc! {
            "ok": 1.0,
            // MongoDB reports these counters as Int32; drivers and
            // shells compare them with strict equality, so emitting
            // Int64 here would break type checks.
            "numIndexesBefore": *num_indexes_before as i32,
            "numIndexesAfter": *num_indexes_after as i32,
            "createdCollection": *created_collection,
        },
        PartitionResult::ListedIndexes { indexes } => doc! {
            // Cursor shape, like `listCollections`: drivers expect a
            // `firstBatch`, not a bare array.
            "ok": 1.0,
            "cursor": {
                "id": 0i64,
                "ns": "",
                "firstBatch": indexes.clone(),
            },
        },
        PartitionResult::DroppedDatabase { database } => doc! {
            "ok": 1.0,
            "dropped": database.clone(),
        },
        PartitionResult::ListedDatabases { databases } => doc! {
            // sizeOnDisk is tracked as zero: Kova keeps everything in
            // RAM and does not measure per-database footprint yet.
            "databases": databases.clone(),
            "totalSize": 0i64,
            "ok": 1.0,
        },
        PartitionResult::CollectionStats { stats } => {
            let mut reply = stats.clone();
            reply.insert("ok", 1.0);
            reply
        }
        PartitionResult::DatabaseStats {
            collections,
            objects,
            data_size,
            indexes,
        } => doc! {
            // Single-partition shape; the network layer sums every
            // worker's counters before building the broadcast reply.
            "collections": *collections as i64,
            "objects": *objects as i64,
            "dataSize": *data_size as i64,
            "storageSize": *data_size as i64,
            "indexes": *indexes as i64,
            "indexSize": 0i64,
            "scaleFactor": 1i32,
            "ok": 1.0,
        },
        PartitionResult::NeedForeign { namespace } => doc! {
            "ok": 0.0,
            "errmsg": format!("lookup of '{namespace}' was not resolved"),
        },
        PartitionResult::StageOut { namespace, .. } => doc! {
            "ok": 0.0,
            "errmsg": format!("$out to '{namespace}' was not written"),
        },
        PartitionResult::Error { message } => doc! {
            "ok": 0.0,
            "errmsg": message.clone(),
        },
        PartitionResult::Transaction { txn_id } => doc! {
            "ok": 1.0,
            "txnId": *txn_id as i64,
        },
        PartitionResult::IndexesDropped { n_indexes_was } => doc! {
            "ok": 1.0,
            "nIndexesWas": *n_indexes_was as i32,
        },
        PartitionResult::Plan { stage, index_name } => {
            let mut plan = doc! { "stage": stage.clone() };
            if let Some(name) = index_name {
                plan.insert("indexName", name.clone());
            }
            doc! { "ok": 1.0, "winningPlan": plan }
        }
    }
}

/// Builds a full OP_MSG reply frame ready to be written to the socket.
///
/// The reply echoes the request's `requestId` as `responseTo` so the
/// client can correlate it with its outstanding command.
pub fn encode_reply(request_id: i32, response_to: i32, body: &Document) -> Result<Vec<u8>, String> {
    let mut payload = Vec::with_capacity(64);
    payload.push(0u8); // section kind 0
    payload.extend(to_vec(body).map_err(|err| format!("BSON encode failed: {}", err))?);

    // header (16) + flagBits (4) + sections
    let message_length = (HEADER_SIZE + FLAG_BITS_SIZE + payload.len()) as u32;

    let mut frame = Vec::with_capacity(message_length as usize);
    // The wire protocol is little-endian (BSON and all integers).
    frame.extend_from_slice(&message_length.to_le_bytes());
    frame.extend_from_slice(&request_id.to_le_bytes());
    frame.extend_from_slice(&response_to.to_le_bytes());
    frame.extend_from_slice(&OP_MSG.to_le_bytes());
    frame.extend_from_slice(&0u32.to_le_bytes()); // flagBits
    frame.extend_from_slice(&payload);

    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_op_msg_body_with_insert() {
        let command = doc! { "insert": "users", "documents": [{ "name": "Ada" }] };
        let mut body = 0u32.to_le_bytes().to_vec(); // flagBits = 0
        body.push(0u8); // section kind 0
        body.extend(to_vec(&command).expect("encode"));

        let msg = decode_op_msg(&body).expect("decode");
        assert_eq!(msg.flag_bits, 0);
        assert_eq!(msg.command.get_str("insert").unwrap(), "users");
    }

    /// Appends a kind-1 document sequence: size, identifier, documents.
    ///
    /// `size` counts every byte after the kind byte, including itself.
    fn push_document_sequence(body: &mut Vec<u8>, identifier: &str, documents: &[Document]) {
        let mut sequence = Vec::new();
        sequence.extend_from_slice(&[0, 0, 0, 0]);
        sequence.extend_from_slice(identifier.as_bytes());
        sequence.push(0);
        for document in documents {
            sequence.extend(to_vec(document).expect("encode"));
        }
        let size = (sequence.len() as i32).to_le_bytes();
        sequence[..4].copy_from_slice(&size);
        body.push(1u8);
        body.extend(sequence);
    }

    #[test]
    fn test_decode_op_msg_with_document_sequence_section() {
        // Drivers (mongosh, Studio 3T, Spring Data) split insert /
        // update / delete payloads into a kind-1 section: the command
        // document, then a sized sequence. The documents must be folded
        // back under the identifier.
        let command = doc! { "insert": "users", "ordered": true, "$db": "shop" };
        let mut body = 0u32.to_le_bytes().to_vec(); // flagBits = 0
        body.push(0u8); // section kind 0
        body.extend(to_vec(&command).expect("encode"));
        push_document_sequence(
            &mut body,
            "documents",
            &[
                doc! { "name": "Ada" },
                doc! { "_id": "u1", "name": "Grace" },
            ],
        );

        let msg = decode_op_msg(&body).expect("decode");
        assert_eq!(msg.command.get_str("insert").unwrap(), "users");
        assert_eq!(msg.command.get_str("$db").unwrap(), "shop");
        assert!(msg.command.get_bool("ordered").unwrap());

        let documents = msg.command.get_array("documents").expect("documents");
        assert_eq!(documents.len(), 2, "both sequenced docs must survive");
        assert_eq!(
            documents[0].as_document().unwrap().get_str("name").unwrap(),
            "Ada"
        );
        assert_eq!(
            documents[1].as_document().unwrap().get_str("_id").unwrap(),
            "u1"
        );
    }

    #[test]
    fn test_decode_op_msg_ignores_trailing_checksum() {
        // Bit 0 announces a CRC-32C checksum appended after the
        // sections (wire spec). The Java driver sets it; without
        // stripping it the section walk read garbage kinds and the
        // reply never came.
        const CHECKSUM_PRESENT: u32 = 1;
        let command = doc! { "find": "users", "$db": "shop" };
        let mut body = CHECKSUM_PRESENT.to_le_bytes().to_vec();
        body.push(0u8); // section kind 0
        body.extend(to_vec(&command).expect("encode"));
        body.extend_from_slice(&[0xAB, 0xCD, 0xEF, 0x01]); // checksum

        let msg = decode_op_msg(&body).expect("decode");
        assert_eq!(msg.flag_bits, CHECKSUM_PRESENT);
        assert_eq!(msg.command.get_str("find").unwrap(), "users");
    }

    #[test]
    fn test_decode_op_msg_studio3t_handshake_bytes() {
        // Byte-for-byte capture of what Studio 3T's Java driver sent
        // and Kova failed to decode ("section kind 189"): flagBits=1
        // (checksum), section 0, `whatsmyuri` BSON, 4-byte checksum.
        // The failure silenced the reply and left the client spinning
        // on "connecting" forever, so these exact bytes must decode.
        const BODY: &[u8] = &[
            0x01, 0x00, 0x00, 0x00, // flagBits = 1 (checksum present)
            0x00, // section kind 0
            0x24, 0x00, 0x00, 0x00, // BSON length = 36
            0x10, b'w', b'h', b'a', b't', b's', b'm', b'y', b'u', b'r', b'i', 0x00, 0x01, 0x00,
            0x00, 0x00, // int32 "whatsmyuri" = 1
            0x02, b'$', b'd', b'b', 0x00, 0x06, 0x00, 0x00, 0x00, b'a', b'd', b'm', b'i', b'n',
            0x00, // string "$db" = "admin"
            0x00, // BSON terminator
            0xDE, 0xAD, 0xBE, 0xEF, // checksum
        ];
        let msg = decode_op_msg(BODY).expect("Studio 3T handshake must decode");
        assert_eq!(msg.flag_bits, 1);
        assert_eq!(msg.command.get_i32("whatsmyuri").unwrap(), 1);
        assert_eq!(msg.command.get_str("$db").unwrap(), "admin");
    }

    #[test]
    fn test_hello_reply_advertises_mongodb_7_0() {
        // Studio 3T ships a legacy mongo shell (v5.0.x) that cannot
        // negotiate wire protocol with a server advertising 8.2. The
        // shell exits immediately with "unable to open shell", which is
        // the error the user sees when clicking a collection. Matching
        // the MongoDB 7.0 wire version keeps the bundled shell happy.
        let reply = hello_reply();
        assert_eq!(reply.get_f64("ok").unwrap(), 1.0);
        assert_eq!(reply.get_i32("maxWireVersion").unwrap(), 25);
        assert_eq!(reply.get_bool("isWritablePrimary").unwrap(), true);
    }

    #[test]
    fn test_decode_rejects_truncated_bson_length() {
        let mut body = 0u32.to_le_bytes().to_vec();
        body.push(0u8); // section kind 0
        body.extend_from_slice(&200i32.to_le_bytes()); // claims 200 bytes
        body.extend_from_slice(&[0u8; 8]); // but only 8 follow

        let err = decode_op_msg(&body).expect_err("must fail");
        assert!(err.contains("out of range"), "got: {}", err);
    }

    #[test]
    fn test_decode_rejects_short_body() {
        let err = decode_op_msg(&[0u8, 0u8]).expect_err("must fail");
        assert!(err.contains("too short"), "got: {}", err);
    }

    #[test]
    fn test_decode_rejects_unknown_section_kind() {
        // Kind 0 (command) and 1 (document sequence) are supported;
        // anything above that is not defined by the wire protocol.
        let mut body = vec![0u8, 0u8, 0u8, 0u8]; // flagBits = 0
        body.push(2u8);
        let err = decode_op_msg(&body).expect_err("must fail");
        assert!(err.contains("section kind 2"), "got: {}", err);
    }

    #[test]
    fn test_decode_rejects_document_sequence_without_command() {
        // A kind-1 section with no preceding kind 0 has nowhere to
        // put its documents; failing is better than silently dropping
        // the payload.
        let mut body = vec![0u8, 0u8, 0u8, 0u8]; // flagBits = 0
        push_document_sequence(&mut body, "documents", &[doc! { "a": 1i32 }]);
        let err = decode_op_msg(&body).expect_err("must fail");
        assert!(err.contains("precedes section 0"), "got: {}", err);
    }

    #[test]
    fn test_command_insert() {
        let command = doc! { "insert": "users", "documents": [{ "name": "Ada" }] };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Insert {
                collection,
                documents,
            } => {
                assert_eq!(collection, "users");
                assert_eq!(documents.len(), 1);
                assert_eq!(documents[0].get_str("name").unwrap(), "Ada");
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn test_command_insert_many_keeps_all_documents() {
        let command = doc! {
            "insert": "users",
            "documents": [{ "name": "Ada" }, { "name": "Grace" }],
        };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Insert { documents, .. } => {
                assert_eq!(documents.len(), 2, "insertMany must keep every document");
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn test_data_commands_are_namespaced_by_db() {
        // Every client command carries `$db`; storage stores the
        // `db.collection` namespace so two databases can hold a
        // collection with the same name.
        let command = doc! {
            "insert": "users",
            "documents": [{ "name": "Ada" }],
            "$db": "shop",
        };
        match command_from_document(&command).expect("parse") {
            PartitionCommand::Insert { collection, .. } => {
                assert_eq!(collection, "shop.users");
            }
            other => panic!("expected Insert, got {:?}", other),
        }

        // Without `$db` (engine tests, minimal clients) the bare name
        // is kept.
        let command = doc! { "find": "users" };
        match command_from_document(&command).expect("parse") {
            PartitionCommand::Find { collection, .. } => assert_eq!(collection, "users"),
            other => panic!("expected Find, got {:?}", other),
        }
    }

    #[test]
    fn test_command_create_becomes_create_collection() {
        let command = doc! { "create": "orders", "$db": "shop" };
        match command_from_document(&command).expect("parse") {
            PartitionCommand::CreateCollection { collection } => {
                assert_eq!(collection, "shop.orders");
            }
            other => panic!("expected CreateCollection, got {:?}", other),
        }
    }

    #[test]
    fn test_command_create_rejects_dotted_collection_names() {
        let command = doc! { "create": "a.b", "$db": "shop" };
        let err = command_from_document(&command).expect_err("must fail");
        assert!(err.contains("cannot contain '.'"), "got: {}", err);
    }

    #[test]
    fn test_command_list_indexes_and_create_indexes() {
        // Java/Spring Data drivers list indexes when browsing a
        // collection and declare them at start-up.
        let list = doc! { "listIndexes": "orders", "$db": "shop" };
        match command_from_document(&list).expect("parse") {
            PartitionCommand::ListIndexes { collection } => {
                assert_eq!(collection, "shop.orders");
            }
            other => panic!("expected ListIndexes, got {:?}", other),
        }

        let create = doc! {
            "createIndexes": "orders",
            "indexes": [
                { "key": { "sku": 1i32 }, "name": "sku_1" },
                { "key": { "qty": -1i32 }, "name": "qty_-1", "unique": true },
            ],
            "$db": "shop",
        };
        match command_from_document(&create).expect("parse") {
            PartitionCommand::CreateIndexes {
                collection,
                indexes,
            } => {
                assert_eq!(collection, "shop.orders");
                assert_eq!(indexes.len(), 2);
                assert_eq!(indexes[0].get_str("name").unwrap(), "sku_1");
                assert!(indexes[1].get_bool("unique").unwrap());
            }
            other => panic!("expected CreateIndexes, got {:?}", other),
        }

        // Missing `indexes` array is a client error, not a panic.
        let malformed = doc! { "createIndexes": "orders" };
        assert!(command_from_document(&malformed).is_err());
    }

    #[test]
    fn test_reply_for_indexes() {
        let created = crate::types::PartitionResult::IndexesCreated {
            num_indexes_before: 1,
            num_indexes_after: 2,
            created_collection: true,
        };
        let reply = reply_document(&created);
        assert_eq!(reply.get_f64("ok"), Ok(1.0));
        assert_eq!(reply.get_i32("numIndexesBefore"), Ok(1));
        assert_eq!(reply.get_i32("numIndexesAfter"), Ok(2));
        assert!(reply.get_bool("createdCollection").unwrap());

        let listed = crate::types::PartitionResult::ListedIndexes {
            indexes: vec![doc! { "v": 2i32, "key": { "_id": 1i32 }, "name": "_id_" }],
        };
        let reply = reply_document(&listed);
        let cursor = reply.get_document("cursor").expect("cursor");
        assert_eq!(cursor.get_array("firstBatch").unwrap().len(), 1);
        assert_eq!(cursor.get_i64("id"), Ok(0));
    }

    #[test]
    fn test_command_list_collections_carries_database() {
        let command = doc! { "listCollections": 1, "$db": "shop" };
        match command_from_document(&command).expect("parse") {
            PartitionCommand::ListCollections { database, filter } => {
                assert_eq!(database, "shop");
                assert!(filter.is_empty());
            }
            other => panic!("expected ListCollections, got {:?}", other),
        }
    }

    #[test]
    fn test_command_list_databases_is_broadcast() {
        let command = doc! { "listDatabases": 1, "filter": { "name": "shop" } };
        match command_from_document(&command).expect("parse") {
            PartitionCommand::ListDatabases { filter } => {
                assert_eq!(filter.get_str("name").unwrap(), "shop");
            }
            other => panic!("expected ListDatabases, got {:?}", other),
        }
    }

    #[test]
    fn test_reply_for_created_and_listed_databases() {
        let created = crate::types::PartitionResult::Created;
        assert_eq!(reply_document(&created).get_f64("ok"), Ok(1.0));

        let listed = crate::types::PartitionResult::ListedDatabases {
            databases: vec![doc! { "name": "shop", "sizeOnDisk": 0i64, "empty": false }],
        };
        let reply = reply_document(&listed);
        assert_eq!(reply.get_f64("ok"), Ok(1.0));
        assert_eq!(reply.get_array("databases").unwrap().len(), 1);
        assert_eq!(reply.get_i64("totalSize"), Ok(0));
    }

    #[test]
    fn test_command_coll_stats_is_namespaced_and_routed() {
        // Studio 3T and Compass ask for a collection's storage panel
        // with `collStats`; it must route by namespace like `find`.
        let command = doc! { "collStats": "orders", "$db": "shop" };
        match command_from_document(&command).expect("parse") {
            PartitionCommand::CollectionStats { collection } => {
                assert_eq!(collection, "shop.orders");
            }
            other => panic!("expected CollectionStats, got {:?}", other),
        }
    }

    #[test]
    fn test_command_db_stats_is_broadcast() {
        let command = doc! { "dbStats": 1i32, "$db": "shop" };
        match command_from_document(&command).expect("parse") {
            PartitionCommand::DatabaseStats { database } => {
                assert_eq!(database, "shop");
            }
            other => panic!("expected DatabaseStats, got {:?}", other),
        }
    }

    #[test]
    fn test_reply_for_collection_and_database_stats() {
        let coll = crate::types::PartitionResult::CollectionStats {
            stats: doc! {
                "ns": "shop.orders",
                "count": 2i64,
                "size": 96i64,
                "avgObjSize": 48i64,
            },
        };
        let reply = reply_document(&coll);
        assert_eq!(reply.get_f64("ok"), Ok(1.0));
        assert_eq!(reply.get_i64("count"), Ok(2));
        assert_eq!(reply.get_str("ns").unwrap(), "shop.orders");

        let db = crate::types::PartitionResult::DatabaseStats {
            collections: 3,
            objects: 10,
            data_size: 500,
            indexes: 4,
        };
        let reply = reply_document(&db);
        assert_eq!(reply.get_f64("ok"), Ok(1.0));
        assert_eq!(reply.get_i64("collections"), Ok(3));
        assert_eq!(reply.get_i64("objects"), Ok(10));
        assert_eq!(reply.get_i64("dataSize"), Ok(500));
        assert_eq!(reply.get_i64("storageSize"), Ok(500));
        assert_eq!(reply.get_i64("indexes"), Ok(4));
    }

    #[test]
    fn test_command_find_with_text_becomes_search() {
        let command = doc! {
            "find": "articles",
            "filter": { "$text": { "$search": "rust engine" } },
        };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Search {
                collection,
                query,
                filter,
                options,
            } => {
                assert_eq!(collection, "articles");
                assert_eq!(query, "rust engine");
                assert!(filter.is_empty(), "$text must be stripped: {:?}", filter);
                assert_eq!(options, SearchOptions::default());
            }
            other => panic!("expected Search, got {:?}", other),
        }
    }

    #[test]
    fn test_command_find_with_text_keeps_other_predicates() {
        let command = doc! {
            "find": "articles",
            "filter": { "author": "Ada", "$text": { "$search": "rust" } },
        };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Search { filter, query, .. } => {
                assert_eq!(query, "rust");
                assert_eq!(filter.get_str("author").unwrap(), "Ada");
                assert!(!filter.contains_key("$text"), "$text must be stripped");
            }
            other => panic!("expected Search, got {:?}", other),
        }
    }

    #[test]
    fn test_command_find_with_text_requires_search_string() {
        let command = doc! {
            "find": "articles",
            "filter": { "$text": {} },
        };
        let err = command_from_document(&command).expect_err("must fail");
        assert!(err.contains("$search"), "got: {}", err);
    }

    #[test]
    fn test_command_find_with_text_boost_parses_weights() {
        // Real drivers encode integers as Int32, not Double, so the
        // parser must accept every numeric BSON type.
        let command = doc! {
            "find": "articles",
            "filter": { "$text": {
                "$search": "rust",
                "$boost": { "title": 3i32, "body": 1.5 },
            } },
        };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Search { options, .. } => {
                assert_eq!(
                    options.boosts,
                    vec![("title".to_string(), 3.0), ("body".to_string(), 1.5)]
                );
                assert!(options.decay.is_none());
            }
            other => panic!("expected Search, got {:?}", other),
        }
    }

    #[test]
    fn test_command_find_with_text_boost_rejects_non_numbers() {
        let command = doc! {
            "find": "articles",
            "filter": { "$text": { "$search": "rust", "$boost": { "title": "high" } } },
        };
        let err = command_from_document(&command).expect_err("must fail");
        assert!(err.contains("$boost"), "got: {}", err);
    }

    #[test]
    fn test_command_find_with_text_decay_parses_spec() {
        let command = doc! {
            "find": "articles",
            "filter": { "$text": {
                "$search": "rust",
                "$decay": { "field": "created_at", "halfLifeSeconds": 86400.0 },
            } },
        };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Search { options, .. } => {
                let decay = options.decay.expect("decay");
                assert_eq!(decay.field, "created_at");
                assert_eq!(decay.half_life_secs, 86400.0);
            }
            other => panic!("expected Search, got {:?}", other),
        }
    }

    #[test]
    fn test_command_find_with_text_decay_requires_positive_half_life() {
        let missing = doc! {
            "find": "a",
            "filter": { "$text": { "$search": "x", "$decay": { "field": "d" } } },
        };
        assert!(command_from_document(&missing).is_err());

        let negative = doc! {
            "find": "a",
            "filter": { "$text": {
                "$search": "x",
                "$decay": { "field": "d", "halfLifeSeconds": 0.0 },
            } },
        };
        let err = command_from_document(&negative).expect_err("must fail");
        assert!(err.contains("halfLifeSeconds"), "got: {}", err);
    }

    #[test]
    fn test_command_find_with_text_analyzer_is_ignored_not_fatal() {
        let command = doc! {
            "find": "articles",
            "filter": { "$text": { "$search": "rust", "$analyzer": "en_stem" } },
        };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Search { query, .. } => assert_eq!(query, "rust"),
            other => panic!("expected Search, got {:?}", other),
        }
    }

    #[test]
    fn test_command_find_with_text_ignores_unknown_operators() {
        let command = doc! {
            "find": "articles",
            "filter": { "$text": { "$search": "rust", "$language": "english" } },
        };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Search { query, .. } => assert_eq!(query, "rust"),
            other => panic!("expected Search, got {:?}", other),
        }
    }

    #[test]
    fn test_command_find_defaults_to_empty_filter() {
        let command = doc! { "find": "users" };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Find {
                collection, filter, ..
            } => {
                assert_eq!(collection, "users");
                assert!(filter.is_empty());
            }
            other => panic!("expected Find, got {:?}", other),
        }
    }

    #[test]
    fn test_command_find_reads_cursor_options() {
        let command = doc! {
            "find": "emp",
            "filter": { "sal": { "$gt": 1000 } },
            "projection": { "ename": 1, "_id": 0 },
            "sort": { "sal": -1 },
            "skip": 2,
            "limit": 5,
            "$db": "shop",
        };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Find {
                collection,
                projection,
                sort,
                skip,
                limit,
                ..
            } => {
                assert_eq!(collection, "shop.emp");
                assert_eq!(projection.unwrap().get_i32("ename").unwrap(), 1);
                assert_eq!(sort.unwrap().get_i32("sal").unwrap(), -1);
                assert_eq!(skip, 2);
                assert_eq!(limit, Some(5));
            }
            other => panic!("expected Find, got {:?}", other),
        }
    }

    #[test]
    fn test_command_aggregate_namespaces_lookup_and_out() {
        let command = doc! {
            "aggregate": "emp",
            "pipeline": [
                { "$lookup": { "from": "dept", "localField": "deptno", "foreignField": "deptno", "as": "dept" } },
                { "$out": "high" },
            ],
            "cursor": {},
            "$db": "shop",
        };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Aggregate {
                collection,
                pipeline,
                resolved,
            } => {
                assert_eq!(collection, "shop.emp");
                assert!(resolved.is_empty());
                assert_eq!(
                    pipeline[0]
                        .get_document("$lookup")
                        .unwrap()
                        .get_str("from")
                        .unwrap(),
                    "shop.dept"
                );
                assert_eq!(pipeline[1].get_str("$out").unwrap(), "shop.high");
            }
            other => panic!("expected Aggregate, got {:?}", other),
        }
    }

    #[test]
    fn test_command_aggregate_namespaces_a_nested_lookup() {
        let command = doc! {
            "aggregate": "emp",
            "pipeline": [{
                "$lookup": {
                    "from": "salgrade",
                    "let": { "salary": "$sal" },
                    "pipeline": [{
                        "$lookup": {
                            "from": "dept",
                            "localField": "deptno",
                            "foreignField": "deptno",
                            "as": "dept",
                        }
                    }],
                    "as": "grade",
                }
            }],
            "cursor": {},
            "$db": "shop",
        };
        let parsed = command_from_document(&command).expect("parse");
        let PartitionCommand::Aggregate { pipeline, .. } = parsed else {
            panic!("expected Aggregate");
        };
        let nested = pipeline[0]
            .get_document("$lookup")
            .unwrap()
            .get_array("pipeline")
            .unwrap();
        let from = nested[0]
            .as_document()
            .unwrap()
            .get_document("$lookup")
            .unwrap()
            .get_str("from")
            .unwrap();
        assert_eq!(
            pipeline[0]
                .get_document("$lookup")
                .unwrap()
                .get_str("from")
                .unwrap(),
            "shop.salgrade"
        );
        assert_eq!(from, "shop.dept");
    }

    #[test]
    fn test_begin_transaction_namespaces_the_collection() {
        let command = doc! {
            "beginTransaction": 1,
            "collection": "emp",
            "isolation": "serializable",
            "timeoutMs": 0,
            "$db": "shop",
        };
        match command_from_document(&command).unwrap() {
            PartitionCommand::BeginTransaction {
                collection,
                isolation,
                timeout_ms,
                txn_id,
            } => {
                assert_eq!(collection, "shop.emp");
                assert_eq!(isolation, "serializable");
                assert_eq!(timeout_ms, Some(0));
                assert_eq!(txn_id, 0);
            }
            other => panic!("expected BeginTransaction, got {other:?}"),
        }

        let command = doc! {
            "insert": "emp",
            "documents": [{ "ename": "KING" }],
            "txnId": 7,
            "$db": "shop",
        };
        match command_from_document(&command).unwrap() {
            PartitionCommand::Transactional { txn_id, inner } => {
                assert_eq!(txn_id, 7);
                assert_eq!(inner.collection(), "shop.emp");
            }
            other => panic!("expected a transactional insert, got {other:?}"),
        }
    }

    #[test]
    fn test_explain_reports_a_collection_scan() {
        let reply = explain_reply(&doc! {
            "explain": { "aggregate": "emp", "pipeline": [ { "$match": { "sal": { "$gt": 1000 } } } ] },
            "$db": "shop",
        })
        .expect("explain")
        .expect("plan");
        assert_eq!(reply.get_f64("ok").unwrap(), 1.0);
        let planner = reply.get_document("queryPlanner").unwrap();
        assert_eq!(planner.get_str("namespace").unwrap(), "shop.emp");
        let plan = planner.get_document("winningPlan").unwrap();
        assert_eq!(plan.get_str("stage").unwrap(), "COLLSCAN");
        assert_eq!(plan.get_array("pipeline").unwrap().len(), 1);
    }

    #[test]
    fn test_command_update() {
        let command = doc! {
            "update": "users",
            "updates": [{ "q": { "name": "Ada" }, "u": { "$set": { "age": 37 } } }],
        };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Update {
                collection,
                filter,
                update,
                multi,
            } => {
                assert_eq!(collection, "users");
                assert_eq!(filter.get_str("name").unwrap(), "Ada");
                assert!(update.contains_key("$set"));
                assert!(!multi, "update must default to single-document");
            }
            other => panic!("expected Update, got {:?}", other),
        }
    }

    #[test]
    fn test_command_delete() {
        let command = doc! {
            "delete": "todos",
            "deletes": [{ "q": { "done": true }, "limit": 0 }],
        };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Delete {
                collection,
                filter,
                limit,
            } => {
                assert_eq!(collection, "todos");
                assert!(filter.get_bool("done").unwrap());
                assert_eq!(limit, 0, "limit 0 means delete all matches");
            }
            other => panic!("expected Delete, got {:?}", other),
        }
    }

    #[test]
    fn test_command_count() {
        let command = doc! { "count": "orders", "query": { "status": "paid" } };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Count { collection, filter } => {
                assert_eq!(collection, "orders");
                assert_eq!(filter.get_str("status").unwrap(), "paid");
            }
            other => panic!("expected Count, got {:?}", other),
        }
    }

    #[test]
    fn test_special_command_ping() {
        let reply = special_command(&doc! { "ping": 1.0 }).expect("ping is special");
        assert_eq!(reply.get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn test_special_command_hello() {
        let reply = special_command(&doc! { "hello": 1.0 }).expect("hello is special");

        assert_eq!(reply.get_f64("ok").unwrap(), 1.0);
        assert!(reply.get_bool("isWritablePrimary").unwrap());
        assert!(reply.get_bool("ismaster").unwrap());
        // 25 is the wire version of a real MongoDB 7.0 server.
        assert_eq!(reply.get_i32("maxWireVersion").unwrap(), 25);
        assert_eq!(
            reply.get_i32("maxBsonObjectSize").unwrap(),
            16 * 1024 * 1024
        );
        assert!(reply.contains_key("localTime"));
    }

    #[test]
    fn test_special_command_legacy_ismaster() {
        // Pre-4.2 drivers send `isMaster` / `ismaster` instead of `hello`.
        for key in ["isMaster", "ismaster"] {
            let reply = special_command(&doc! { key: 1.0 })
                .unwrap_or_else(|| panic!("{} must be special", key));
            assert!(reply.get_bool("ismaster").unwrap());
        }
    }

    #[test]
    fn test_special_command_ignores_data_commands() {
        assert!(special_command(&doc! { "insert": "users", "documents": [] }).is_none());
        assert!(special_command(&doc! { "find": "users" }).is_none());
        assert!(special_command(&doc! { "drop": "users" }).is_none());
        assert!(
            special_command(&doc! { "buildInfo": 1 }).is_some(),
            "buildInfo is metadata"
        );
        assert!(special_command(&doc! { "endSessions": [] }).is_some());
        assert!(
            special_command(&doc! { "connectionStatus": 1.0 }).is_some(),
            "connectionStatus is metadata"
        );
        assert!(
            special_command(&doc! { "hostInfo": 1.0 }).is_some(),
            "hostInfo is metadata"
        );
        assert!(
            special_command(&doc! { "atlasVersion": 1.0 }).is_some(),
            "atlasVersion is metadata"
        );
    }

    #[test]
    fn test_special_command_host_info() {
        let reply = special_command(&doc! { "hostInfo": 1.0 }).expect("hostInfo must be answered");
        assert_eq!(reply.get_f64("ok").unwrap(), 1.0);
        assert!(
            reply
                .get_str("system")
                .unwrap()
                .contains(std::env::consts::OS),
            "system field must contain platform"
        );
        assert!(
            reply.get_str("cpu").unwrap().contains("x86_64")
                || reply.get_str("cpu").unwrap().contains("aarch64"),
            "cpu field must contain architecture"
        );
    }

    #[test]
    fn test_special_command_atlas_version() {
        let reply =
            special_command(&doc! { "atlasVersion": 1.0 }).expect("atlasVersion must be answered");
        assert_eq!(reply.get_f64("ok").unwrap(), 1.0);
        assert!(
            reply.get("version").unwrap().as_null().is_some(),
            "atlasVersion must be null for non-Atlas servers"
        );
    }

    #[test]
    fn test_command_rejects_unknown() {
        // `reIndex` is a real MongoDB command Kova does not implement;
        // the error must name it so operators can tell what is missing.
        let command = doc! { "reIndex": "users" };
        let err = command_from_document(&command).expect_err("must fail");
        assert!(err.contains("reIndex"), "got: {}", err);
    }

    #[test]
    fn test_command_drop() {
        let command = doc! { "drop": "users" };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Drop { collection } => assert_eq!(collection, "users"),
            other => panic!("expected Drop, got {:?}", other),
        }
    }

    #[test]
    fn test_command_rejects_insert_without_documents() {
        let command = doc! { "insert": "users" };
        let err = command_from_document(&command).expect_err("must fail");
        assert!(err.contains("documents"), "got: {}", err);
    }

    #[test]
    fn test_reply_document_ok_and_error() {
        let ok = reply_document(&PartitionResult::Inserted { inserted_count: 1 });
        assert_eq!(ok.get_f64("ok").unwrap(), 1.0);

        let err = reply_document(&PartitionResult::Error {
            message: "boom".into(),
        });
        assert_eq!(err.get_f64("ok").unwrap(), 0.0);
        assert_eq!(err.get_str("errmsg").unwrap(), "boom");
    }

    #[test]
    fn test_reply_document_found_wraps_cursor() {
        let found = reply_document(&PartitionResult::Found {
            documents: vec![doc! { "name": "Ada" }],
        });
        let cursor = found.get_document("cursor").expect("cursor present");
        let batch = cursor.get_array("firstBatch").expect("batch present");
        assert_eq!(batch.len(), 1);
    }

    #[test]
    fn test_decode_op_query_handshake_from_real_frame() {
        // Body of the OP_QUERY a real mongosh sends on connect:
        // flagBits=0, fullCollectionName="admin.$cmd", numberToSkip=0,
        // numberToReturn=-1, then the BSON command document.
        let mut body = 0u32.to_le_bytes().to_vec(); // flagBits
        body.extend_from_slice(b"admin.$cmd\0");
        body.extend_from_slice(&0i32.to_le_bytes()); // numberToSkip
        body.extend_from_slice(&(-1i32).to_le_bytes()); // numberToReturn
        body.extend(to_vec(&doc! { "ismaster": 1i32 }).expect("bson"));

        let query = decode_op_query(&body).expect("decode OP_QUERY");
        assert_eq!(query.flag_bits, 0);
        assert_eq!(query.command.get_i32("ismaster").unwrap(), 1);

        // The decoded command must hit the handshake path.
        assert!(special_command(&query.command).is_some());
    }

    #[test]
    fn test_decode_op_query_rejects_missing_namespace() {
        let mut body = 0u32.to_le_bytes().to_vec();
        // Non-zero bytes only: no NUL terminator for fullCollectionName.
        body.extend_from_slice(b"admin.$cmd");
        let err = decode_op_query(&body).expect_err("must fail");
        assert!(err.contains("fullCollectionName"), "got: {}", err);
    }

    #[test]
    fn test_encode_op_reply_layout() {
        let body = doc! { "ok": 1.0, "ismaster": true };
        let frame = encode_op_reply(5, 9, &body).expect("encode");

        let message_length = u32::from_le_bytes(frame[0..4].try_into().unwrap());
        let request_id = i32::from_le_bytes(frame[4..8].try_into().unwrap());
        let response_to = i32::from_le_bytes(frame[8..12].try_into().unwrap());
        let op_code = i32::from_le_bytes(frame[12..16].try_into().unwrap());
        let cursor_id = i64::from_le_bytes(frame[20..28].try_into().unwrap());
        let number_returned = i32::from_le_bytes(frame[32..36].try_into().unwrap());

        assert_eq!(frame.len(), message_length as usize);
        assert_eq!(request_id, 5);
        assert_eq!(response_to, 9, "reply must echo the request id");
        assert_eq!(op_code, OP_REPLY, "OP_QUERY is answered with OP_REPLY");
        assert_eq!(cursor_id, 0, "no live cursor");
        assert_eq!(number_returned, 1);

        let decoded: Document = from_slice(&frame[36..]).expect("decode reply");
        assert!(decoded.get_bool("ismaster").unwrap());
    }

    #[test]
    fn test_encode_reply_layout() {
        let body = doc! { "ok": 1.0 };
        let frame = encode_reply(7, 42, &body).expect("encode");

        let message_length = u32::from_le_bytes(frame[0..4].try_into().unwrap());
        let request_id = i32::from_le_bytes(frame[4..8].try_into().unwrap());
        let response_to = i32::from_le_bytes(frame[8..12].try_into().unwrap());
        let op_code = i32::from_le_bytes(frame[12..16].try_into().unwrap());
        let flag_bits = u32::from_le_bytes(frame[16..20].try_into().unwrap());

        assert_eq!(frame.len(), message_length as usize);
        assert_eq!(request_id, 7);
        assert_eq!(response_to, 42);
        assert_eq!(op_code, OP_MSG);
        assert_eq!(flag_bits, 0);
        assert_eq!(frame[20], 0, "section kind must be 0");

        // The BSON body must decode back to the original document.
        let decoded: Document = from_slice(&frame[21..]).expect("decode reply");
        assert_eq!(decoded.get_f64("ok").unwrap(), 1.0);
    }

    #[test]
    fn test_encode_reply_round_trips_through_decode() {
        let reply = doc! { "ok": 1.0, "n": 3i64 };
        let frame = encode_reply(1, 1, &reply).expect("encode");

        let msg = decode_op_msg(&frame[HEADER_SIZE..]).expect("decode");
        assert_eq!(msg.command.get_i64("n").unwrap(), 3);
    }
}
