//! MongoDB Wire Protocol message decoding and encoding (OP_MSG).
//!
//! This module turns raw bytes coming from a client into typed
//! [`PartitionCommand`]s, and builds the OP_MSG replies that go back.
//! It is pure byte/document manipulation with no I/O, so every function
//! here is unit-testable without a socket.

use bson::{doc, from_slice, to_vec, Document};

use crate::types::{PartitionCommand, PartitionResult};

/// OP_MSG operation code.
pub const OP_MSG: i32 = 2013;

/// Size of the fixed 16-byte message header.
pub const HEADER_SIZE: usize = 16;

/// Size of the 4-byte `flagBits` field that follows the header in OP_MSG.
pub const FLAG_BITS_SIZE: usize = 4;

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

    let flag_bits = u32::from_be_bytes(
        body[0..4]
            .try_into()
            .map_err(|_| "flagBits must be 4 bytes".to_string())?,
    );

    let sections = &body[FLAG_BITS_SIZE..];
    if sections.is_empty() {
        return Err("OP_MSG has no sections".to_string());
    }

    let kind = sections[0];
    match kind {
        0 => {
            let command: Document = from_slice(&sections[1..])
                .map_err(|err| format!("invalid BSON in section 0: {}", err))?;
            Ok(OpMsg { flag_bits, command })
        }
        other => Err(format!(
            "unsupported OP_MSG section kind {} (only kind 0 is handled)",
            other
        )),
    }
}

/// Translates a decoded command document into a [`PartitionCommand`].
///
/// Supports the commands Phase 2 can execute: `insert`, `find`, `update`,
/// `delete` and `count`. Anything else returns an error string that is
/// surfaced to the client as a Mongo error reply.
pub fn command_from_document(command: &Document) -> Result<PartitionCommand, String> {
    if let Some(collection) = command.get_str("insert").ok() {
        let documents = command
            .get_array("documents")
            .map_err(|_| "insert requires a 'documents' array")?;

        let document = documents
            .first()
            .and_then(|value| value.as_document())
            .ok_or_else(|| "insert requires at least one document".to_string())?
            .clone();

        return Ok(PartitionCommand::Insert {
            collection: collection.to_string(),
            document,
        });
    }

    if let Some(collection) = command.get_str("find").ok() {
        let filter = command
            .get_document("filter")
            .cloned()
            .unwrap_or_else(|_| doc! {});

        return Ok(PartitionCommand::Find {
            collection: collection.to_string(),
            filter,
        });
    }

    if let Some(collection) = command.get_str("update").ok() {
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

        return Ok(PartitionCommand::Update {
            collection: collection.to_string(),
            filter,
            update,
        });
    }

    if let Some(collection) = command.get_str("delete").ok() {
        let deletes = command
            .get_array("deletes")
            .map_err(|_| "delete requires a 'deletes' array")?;

        let entry = deletes
            .first()
            .and_then(|value| value.as_document())
            .ok_or_else(|| "delete requires at least one entry".to_string())?;

        let filter = entry.get_document("q").cloned().unwrap_or_else(|_| doc! {});

        return Ok(PartitionCommand::Delete {
            collection: collection.to_string(),
            filter,
        });
    }

    if let Some(collection) = command.get_str("count").ok() {
        let filter = command
            .get_document("query")
            .cloned()
            .unwrap_or_else(|_| doc! {});

        return Ok(PartitionCommand::Count {
            collection: collection.to_string(),
            filter,
        });
    }

    let name = command
        .keys()
        .next()
        .map(|key| key.as_str())
        .unwrap_or("<empty>");
    Err(format!("unsupported command '{}'", name))
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

    None
}

/// Builds the reply to a `hello`/`isMaster` handshake.
///
/// The advertised limits mirror a standalone MongoDB 7.x server so
/// clients negotiate a compatible wire version and proceed with the
/// connection.
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
        "maxWireVersion": 17,
        "readOnly": false,
        "logicalSessionTimeoutMinutes": 30,
        "localTime": bson::DateTime::now(),
        // No replica set: clients fall back to a standalone topology.
        "me": "localhost:27017",
        "connectionId": 1,
        "metricName": "kova",
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
        PartitionResult::Modified { modified_count } => doc! {
            "ok": 1.0,
            "nModified": *modified_count as i64,
        },
        PartitionResult::Deleted { deleted_count } => doc! {
            "ok": 1.0,
            "n": *deleted_count as i64,
        },
        PartitionResult::Error { message } => doc! {
            "ok": 0.0,
            "errmsg": message.clone(),
        },
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
    frame.extend_from_slice(&message_length.to_be_bytes());
    frame.extend_from_slice(&request_id.to_be_bytes());
    frame.extend_from_slice(&response_to.to_be_bytes());
    frame.extend_from_slice(&OP_MSG.to_be_bytes());
    frame.extend_from_slice(&0u32.to_be_bytes()); // flagBits
    frame.extend_from_slice(&payload);

    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decode_op_msg_body_with_insert() {
        let command = doc! { "insert": "users", "documents": [{ "name": "Ada" }] };
        let mut body = 0u32.to_be_bytes().to_vec(); // flagBits = 0
        body.push(0u8); // section kind 0
        body.extend(to_vec(&command).expect("encode"));

        let msg = decode_op_msg(&body).expect("decode");
        assert_eq!(msg.flag_bits, 0);
        assert_eq!(msg.command.get_str("insert").unwrap(), "users");
    }

    #[test]
    fn test_decode_rejects_short_body() {
        let err = decode_op_msg(&[0u8, 0u8]).expect_err("must fail");
        assert!(err.contains("too short"), "got: {}", err);
    }

    #[test]
    fn test_decode_rejects_unknown_section_kind() {
        let body = vec![0u8, 0u8, 0u8, 0u8, 1u8]; // kind 1 = document sequence
        let err = decode_op_msg(&body).expect_err("must fail");
        assert!(err.contains("section kind 1"), "got: {}", err);
    }

    #[test]
    fn test_command_insert() {
        let command = doc! { "insert": "users", "documents": [{ "name": "Ada" }] };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Insert {
                collection,
                document,
            } => {
                assert_eq!(collection, "users");
                assert_eq!(document.get_str("name").unwrap(), "Ada");
            }
            other => panic!("expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn test_command_find_defaults_to_empty_filter() {
        let command = doc! { "find": "users" };
        let parsed = command_from_document(&command).expect("parse");
        match parsed {
            PartitionCommand::Find { collection, filter } => {
                assert_eq!(collection, "users");
                assert!(filter.is_empty());
            }
            other => panic!("expected Find, got {:?}", other),
        }
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
            } => {
                assert_eq!(collection, "users");
                assert_eq!(filter.get_str("name").unwrap(), "Ada");
                assert!(update.contains_key("$set"));
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
            PartitionCommand::Delete { collection, filter } => {
                assert_eq!(collection, "todos");
                assert_eq!(filter.get_bool("done").unwrap(), true);
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
        assert_eq!(reply.get_bool("isWritablePrimary").unwrap(), true);
        assert_eq!(reply.get_bool("ismaster").unwrap(), true);
        assert_eq!(reply.get_i32("maxWireVersion").unwrap(), 17);
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
            assert_eq!(reply.get_bool("ismaster").unwrap(), true);
        }
    }

    #[test]
    fn test_special_command_ignores_data_commands() {
        assert!(special_command(&doc! { "insert": "users", "documents": [] }).is_none());
        assert!(special_command(&doc! { "find": "users" }).is_none());
        assert!(special_command(&doc! { "drop": "users" }).is_none());
    }

    #[test]
    fn test_command_rejects_unknown() {
        let command = doc! { "drop": "users" };
        let err = command_from_document(&command).expect_err("must fail");
        assert!(err.contains("drop"), "got: {}", err);
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
    fn test_encode_reply_layout() {
        let body = doc! { "ok": 1.0 };
        let frame = encode_reply(7, 42, &body).expect("encode");

        let message_length = u32::from_be_bytes(frame[0..4].try_into().unwrap());
        let request_id = i32::from_be_bytes(frame[4..8].try_into().unwrap());
        let response_to = i32::from_be_bytes(frame[8..12].try_into().unwrap());
        let op_code = i32::from_be_bytes(frame[12..16].try_into().unwrap());
        let flag_bits = u32::from_be_bytes(frame[16..20].try_into().unwrap());

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
