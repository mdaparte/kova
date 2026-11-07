//! One OP_MSG connection. The shell and the window both call this.
//! The window does not open a socket of its own.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use bson::{doc, Bson, Document};

use crate::protocol::{self, HEADER_SIZE, MAX_MESSAGE_SIZE, OP_MSG};

pub struct Client {
    stream: TcpStream,
    next_id: i32,
}

impl Client {
    pub fn connect(
        host: &str,
        port: u16,
        user: Option<&str>,
        password: Option<&str>,
    ) -> Result<Self, String> {
        let (user, password) = pair(user, password)?;
        let address = format!("{host}:{port}");
        let socket = address
            .to_socket_addrs()
            .map_err(|err| format!("{address} is not an address: {err}"))?
            .next()
            .ok_or_else(|| format!("{address} is not an address"))?;
        let stream = TcpStream::connect_timeout(&socket, Duration::from_secs(5))
            .map_err(|err| format!("{address} is not reachable: {err}"))?;
        stream
            .set_nodelay(true)
            .map_err(|err| format!("tcp: {err}"))?;
        let mut client = Self { stream, next_id: 1 };
        if let (Some(user), Some(password)) = (user, password) {
            let reply = client.command(doc! {
                "authenticate": 1i32,
                "user": user,
                "pwd": password,
                "$db": "admin",
            })?;
            ensure_ok(&reply)?;
        }
        Ok(client)
    }

    pub fn list_databases(&mut self) -> Result<Vec<String>, String> {
        let reply = self.command(doc! { "listDatabases": 1i32, "$db": "admin" })?;
        ensure_ok(&reply)?;
        let values = reply
            .get_array("databases")
            .map_err(|_| "listDatabases has no databases".to_string())?;
        let mut names = Vec::new();
        for value in values {
            let document = value
                .as_document()
                .ok_or_else(|| "database entry is not a document".to_string())?;
            names.push(
                document
                    .get_str("name")
                    .map_err(|_| "database entry has no name".to_string())?
                    .to_string(),
            );
        }
        names.sort();
        Ok(names)
    }

    pub fn list_collections(&mut self, database: &str) -> Result<Vec<String>, String> {
        let reply = self.command(doc! { "listCollections": 1i32, "$db": database })?;
        let mut names = names_in_cursor(&reply)?;
        names.sort();
        Ok(names)
    }

    pub fn find(
        &mut self,
        database: &str,
        collection: &str,
        filter: Document,
    ) -> Result<Vec<Document>, String> {
        let reply = self.command(doc! {
            "find": collection,
            "filter": filter,
            "$db": database,
        })?;
        cursor_batch(&reply)
    }

    pub fn insert_one(
        &mut self,
        database: &str,
        collection: &str,
        document: Document,
    ) -> Result<i64, String> {
        let reply = self.command(doc! {
            "insert": collection,
            "documents": [document],
            "$db": database,
        })?;
        ensure_ok(&reply)?;
        Ok(count_of(&reply))
    }

    pub fn update_one(
        &mut self,
        database: &str,
        collection: &str,
        filter: Document,
        update: Document,
    ) -> Result<i64, String> {
        let reply = self.command(doc! {
            "update": collection,
            "updates": [{ "q": filter, "u": update, "multi": false }],
            "$db": database,
        })?;
        ensure_ok(&reply)?;
        Ok(count_of(&reply))
    }

    pub fn delete_one(
        &mut self,
        database: &str,
        collection: &str,
        filter: Document,
    ) -> Result<i64, String> {
        let reply = self.command(doc! {
            "delete": collection,
            "deletes": [{ "q": filter, "limit": 1i32 }],
            "$db": database,
        })?;
        ensure_ok(&reply)?;
        Ok(count_of(&reply))
    }

    pub fn sql(&mut self, database: &str, statement: &str) -> Result<Vec<Document>, String> {
        let reply = self.command(doc! { "sql": statement, "$db": database })?;
        cursor_batch(&reply)
    }

    /// Replaces the document with this `_id`. The stored `_id` stays.
    pub fn save_document(
        &mut self,
        database: &str,
        collection: &str,
        id: Bson,
        document: Document,
    ) -> Result<(), String> {
        let mut filter = Document::new();
        filter.insert("_id", id);
        self.update_one(database, collection, filter, document)?;
        Ok(())
    }

    fn command(&mut self, command: Document) -> Result<Document, String> {
        let request_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let frame = protocol::encode_reply(request_id, 0, &command)?;
        self.stream
            .write_all(&frame)
            .map_err(|err| format!("write failed: {err}"))?;
        let mut header = [0u8; HEADER_SIZE];
        self.stream
            .read_exact(&mut header)
            .map_err(|err| format!("read failed: {err}"))?;
        let message_length = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        let op_code = i32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        if op_code != OP_MSG {
            return Err(format!("reply opcode is {op_code}"));
        }
        if message_length < HEADER_SIZE as u32 || message_length as usize > MAX_MESSAGE_SIZE {
            return Err(format!("reply length {message_length} is out of range"));
        }
        let mut body = vec![0u8; message_length as usize - HEADER_SIZE];
        self.stream
            .read_exact(&mut body)
            .map_err(|err| format!("read failed: {err}"))?;
        Ok(protocol::decode_op_msg(&body)?.command)
    }
}

pub fn pair<'a>(
    user: Option<&'a str>,
    password: Option<&'a str>,
) -> Result<(Option<&'a str>, Option<&'a str>), String> {
    match (
        user.filter(|value| !value.is_empty()),
        password.filter(|value| !value.is_empty()),
    ) {
        (None, None) => Ok((None, None)),
        (Some(user), Some(password)) => Ok((Some(user), Some(password))),
        _ => Err("user and password are both set, or both omitted".into()),
    }
}

fn ensure_ok(reply: &Document) -> Result<(), String> {
    let ok = match reply.get("ok") {
        Some(Bson::Double(value)) => *value == 1.0,
        Some(Bson::Int32(value)) => *value == 1,
        Some(Bson::Int64(value)) => *value == 1,
        _ => false,
    };
    if ok {
        return Ok(());
    }
    Err(reply
        .get_str("errmsg")
        .unwrap_or("command failed")
        .to_string())
}

fn count_of(reply: &Document) -> i64 {
    match reply.get("n") {
        Some(Bson::Int32(value)) => i64::from(*value),
        Some(Bson::Int64(value)) => *value,
        Some(Bson::Double(value)) => *value as i64,
        _ => 0,
    }
}

fn cursor_batch(reply: &Document) -> Result<Vec<Document>, String> {
    ensure_ok(reply)?;
    let cursor = reply
        .get_document("cursor")
        .map_err(|_| "reply has no cursor".to_string())?;
    let values = cursor
        .get_array("firstBatch")
        .map_err(|_| "cursor has no firstBatch".to_string())?;
    values
        .iter()
        .map(|value| {
            value
                .as_document()
                .cloned()
                .ok_or_else(|| "cursor row is not a document".to_string())
        })
        .collect()
}

fn names_in_cursor(reply: &Document) -> Result<Vec<String>, String> {
    Ok(cursor_batch(reply)?
        .into_iter()
        .filter_map(|document| document.get_str("name").ok().map(str::to_string))
        .collect())
}
