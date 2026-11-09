//! One OP_MSG connection. The shell and the window both call this.
//! The window does not open a socket of its own.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

use bson::{doc, Bson, Document};
use serde_json::{json, Value};

use crate::protocol::{self, HEADER_SIZE, MAX_MESSAGE_SIZE, OP_MSG};

pub struct Client {
    stream: TcpStream,
    next_id: i32,
}

pub struct CollectionInfo {
    pub name: String,
    pub kind: String,
    pub inverted: bool,
    pub options: Value,
}

pub struct IndexInfo {
    pub name: String,
    pub key: String,
    pub unique: bool,
    pub sparse: bool,
    pub parts: Vec<Value>,
}

pub struct ServerStatus {
    pub version: String,
    pub host: String,
    pub system: String,
    pub uptime: i64,
    pub memory: i64,
    pub disk: i64,
    pub disk_path: String,
    pub connections_active: i64,
    pub connections_total: i64,
    pub queries: i64,
    pub inserts: i64,
    pub updates: i64,
    pub deletes: i64,
    pub bytes_in: i64,
    pub bytes_out: i64,
    pub wal_writes: i64,
    pub feed_enabled: bool,
    pub feed_linked: bool,
    pub feed_address: String,
    pub feed_stream: String,
    pub feed_topic: String,
}

pub struct TableLine {
    pub name: String,
    pub kind: String,
    pub inverted: bool,
    pub count: i64,
    pub size: i64,
    pub avg: i64,
    pub indexes: i64,
}

pub struct Dashboard {
    pub collections: i64,
    pub views: i64,
    pub documents: i64,
    pub data_size: i64,
    pub indexes: i64,
    pub tables: Vec<TableLine>,
}

pub struct Profile {
    pub documents: i64,
    pub scanned: i64,
    pub fields: Vec<FieldProfile>,
}

pub struct FieldProfile {
    pub name: String,
    pub present: i64,
    pub missing: i64,
    pub nulls: i64,
    pub fill: f64,
    pub kind: String,
    pub distinct: i64,
    pub numeric: Option<NumericProfile>,
}

pub struct NumericProfile {
    pub min: f64,
    pub q1: f64,
    pub median: f64,
    pub q3: f64,
    pub max: f64,
    pub mean: f64,
    pub sd: f64,
    pub whisker_low: f64,
    pub whisker_high: f64,
    pub outliers: Vec<f64>,
    pub bins: Vec<ProfileBin>,
}

pub struct ProfileBin {
    pub start: f64,
    pub end: f64,
    pub n: i64,
}

pub struct FeedEvent {
    pub seq: i64,
    pub op: String,
    pub ns: String,
    pub id: String,
    pub before: String,
    pub after: String,
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
        Ok(self
            .list_collection_info(database)?
            .into_iter()
            .map(|info| info.name)
            .collect())
    }

    /// Collections and views. `kind` is `collection` or `view`.
    pub fn list_collection_info(&mut self, database: &str) -> Result<Vec<CollectionInfo>, String> {
        let reply = self.command(doc! { "listCollections": 1i32, "$db": database })?;
        let mut infos = Vec::new();
        for document in cursor_batch(&reply)? {
            let Some(name) = document.get_str("name").ok() else {
                continue;
            };
            if infos.iter().any(|info: &CollectionInfo| info.name == name) {
                continue;
            }
            let kind = document.get_str("type").unwrap_or("collection").to_string();
            let kova = document
                .get_document("options")
                .ok()
                .and_then(|options| options.get_document("kova").ok());
            let inverted = kova
                .and_then(|document| document.get_bool("inverted").ok())
                .unwrap_or(false);
            let options = kova
                .and_then(|document| serde_json::to_value(document).ok())
                .unwrap_or(Value::Null);
            infos.push(CollectionInfo {
                name: name.to_string(),
                kind,
                inverted,
                options,
            });
        }
        infos.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(infos)
    }

    /// Field names present in the first batch of documents.
    pub fn field_names(&mut self, database: &str, collection: &str) -> Result<Vec<String>, String> {
        let documents = self.find_page(database, collection, Document::new(), 0, Some(200))?;
        let mut names = std::collections::BTreeSet::new();
        for document in &documents {
            for key in document.keys() {
                names.insert(key.to_string());
            }
        }
        let mut names: Vec<String> = names.into_iter().collect();
        if let Some(position) = names.iter().position(|name| name == "_id") {
            let id = names.remove(position);
            names.insert(0, id);
        }
        Ok(names)
    }

    /// Fill rates and, for numbers, a quartile summary plus a histogram.
    ///
    /// The scan stops at 8 000 documents. `documents` is the real count.
    pub fn profile(
        &mut self,
        database: &str,
        collection: &str,
        filter: Document,
        field: Option<&str>,
    ) -> Result<Profile, String> {
        let documents = self.count(database, collection, filter.clone())?;
        let scan = documents.max(0).min(PROFILE_CAP) as u64;
        let mut seen = 0u64;
        let mut skip = 0u64;
        let mut fields: std::collections::BTreeMap<String, FieldAccum> =
            std::collections::BTreeMap::new();
        if let Some(name) = field {
            fields.insert(name.to_string(), FieldAccum::default());
        }
        while seen < scan {
            let page = self.find_page(
                database,
                collection,
                filter.clone(),
                skip,
                Some(PROFILE_PAGE),
            )?;
            if page.is_empty() {
                break;
            }
            let room = (scan - seen) as usize;
            let slice = if page.len() > room { &page[..room] } else { &page[..] };
            for document in slice {
                if let Some(name) = field {
                    note(fields.entry(name.to_string()).or_default(), document.get(name));
                } else {
                    for key in document.keys() {
                        note(
                            fields.entry(key.clone()).or_default(),
                            document.get(key),
                        );
                    }
                }
            }
            let step = slice.len() as u64;
            seen += step;
            skip += page.len() as u64;
            if (page.len() as u64) < PROFILE_PAGE {
                break;
            }
        }
        let mut rows = Vec::new();
        for (name, accum) in fields {
            rows.push(accum.finish(name, seen));
        }
        if let Some(position) = rows.iter().position(|row| row.name == "_id") {
            let id = rows.remove(position);
            rows.insert(0, id);
        }
        Ok(Profile {
            documents,
            scanned: seen as i64,
            fields: rows,
        })
    }

    /// Indexes declared on a collection, including the mandatory `_id_`.
    pub fn list_indexes(
        &mut self,
        database: &str,
        collection: &str,
    ) -> Result<Vec<IndexInfo>, String> {
        let reply = self.command(doc! { "listIndexes": collection, "$db": database })?;
        let mut infos = Vec::new();
        for document in cursor_batch(&reply)? {
            let Ok(name) = document.get_str("name") else {
                continue;
            };
            let key_doc = document.get_document("key").ok();
            let key = key_doc.map(key_label).unwrap_or_default();
            let unique = matches!(document.get("unique"), Some(Bson::Boolean(true)));
            let sparse = matches!(document.get("sparse"), Some(Bson::Boolean(true)));
            infos.push(IndexInfo {
                name: name.to_string(),
                key,
                unique,
                sparse,
                parts: key_doc.map(key_parts).unwrap_or_default(),
            });
        }
        Ok(infos)
    }

    pub fn create_view(
        &mut self,
        database: &str,
        name: &str,
        source: &str,
        pipeline: Vec<Document>,
    ) -> Result<(), String> {
        let mut command = doc! {
            "create": name,
            "viewOn": source,
            "$db": database,
        };
        command.insert(
            "pipeline",
            Bson::Array(pipeline.into_iter().map(Bson::Document).collect()),
        );
        let reply = self.command(command)?;
        ensure_ok(&reply)
    }

    pub fn find(
        &mut self,
        database: &str,
        collection: &str,
        filter: Document,
    ) -> Result<Vec<Document>, String> {
        self.find_page(database, collection, filter, 0, None)
    }

    pub fn find_page(
        &mut self,
        database: &str,
        collection: &str,
        filter: Document,
        skip: u64,
        limit: Option<u64>,
    ) -> Result<Vec<Document>, String> {
        let mut command = doc! {
            "find": collection,
            "filter": filter,
            "$db": database,
        };
        if skip > 0 {
            command.insert("skip", skip as i64);
        }
        if let Some(limit) = limit {
            command.insert("limit", limit as i64);
        }
        let reply = self.command(command)?;
        cursor_batch(&reply)
    }

    pub fn count(
        &mut self,
        database: &str,
        collection: &str,
        filter: Document,
    ) -> Result<i64, String> {
        let reply = self.command(doc! {
            "count": collection,
            "query": filter,
            "$db": database,
        })?;
        ensure_ok(&reply)?;
        Ok(count_of(&reply))
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

    /// Collections, views, and how many documents each one holds.
    pub fn dashboard(&mut self, database: &str) -> Result<Dashboard, String> {
        let info = self.list_collection_info(database)?;
        let stats = self.command(doc! { "dbStats": 1i32, "$db": database })?;
        ensure_ok(&stats)?;
        let mut tables = Vec::new();
        let mut documents = 0i64;
        let mut views = 0i64;
        for item in &info {
            if item.kind == "view" {
                views += 1;
            }
            let mut line = self.table_line(database, &item.name, &item.kind)?;
            line.inverted = item.inverted;
            if line.count > 0 {
                documents += line.count;
            }
            tables.push(line);
        }
        Ok(Dashboard {
            collections: number_field(&stats, "collections"),
            views,
            documents,
            data_size: number_field(&stats, "dataSize"),
            indexes: number_field(&stats, "indexes"),
            tables,
        })
    }

    fn table_line(
        &mut self,
        database: &str,
        name: &str,
        kind: &str,
    ) -> Result<TableLine, String> {
        if kind == "view" {
            let count = self.count(database, name, Document::new()).unwrap_or(0);
            return Ok(TableLine {
                name: name.to_string(),
                kind: kind.to_string(),
                inverted: false,
                count,
                size: 0,
                avg: 0,
                indexes: 0,
            });
        }
        let reply = self.command(doc! { "collStats": name, "$db": database })?;
        if ensure_ok(&reply).is_err() {
            let count = self.count(database, name, Document::new()).unwrap_or(0);
            return Ok(TableLine {
                name: name.to_string(),
                kind: kind.to_string(),
                inverted: false,
                count,
                size: 0,
                avg: 0,
                indexes: 0,
            });
        }
        Ok(TableLine {
            name: name.to_string(),
            kind: kind.to_string(),
            inverted: false,
            count: number_field(&reply, "count"),
            size: number_field(&reply, "size"),
            avg: number_field(&reply, "avgObjSize"),
            indexes: number_field(&reply, "nindexes"),
        })
    }

    pub fn status(&mut self) -> Result<ServerStatus, String> {
        let reply = self.command(doc! { "kovaStatus": 1i32, "$db": "admin" })?;
        ensure_ok(&reply)?;
        let feed = reply.get_document("feed").ok();
        Ok(ServerStatus {
            version: reply.get_str("version").unwrap_or("").to_string(),
            host: reply.get_str("host").unwrap_or("").to_string(),
            system: reply.get_str("system").unwrap_or("").to_string(),
            uptime: number_field(&reply, "uptime"),
            memory: number_field(&reply, "memory"),
            disk: number_field(&reply, "disk"),
            disk_path: reply.get_str("diskPath").unwrap_or("").to_string(),
            connections_active: number_field(&reply, "connectionsActive"),
            connections_total: number_field(&reply, "connectionsTotal"),
            queries: number_field(&reply, "queries"),
            inserts: number_field(&reply, "inserts"),
            updates: number_field(&reply, "updates"),
            deletes: number_field(&reply, "deletes"),
            bytes_in: number_field(&reply, "bytesIn"),
            bytes_out: number_field(&reply, "bytesOut"),
            wal_writes: number_field(&reply, "walWrites"),
            feed_enabled: feed
                .and_then(|document| document.get_bool("enabled").ok())
                .unwrap_or(false),
            feed_linked: feed
                .and_then(|document| document.get_bool("linked").ok())
                .unwrap_or(false),
            feed_address: feed
                .and_then(|document| document.get_str("address").ok())
                .unwrap_or("")
                .to_string(),
            feed_stream: feed
                .and_then(|document| document.get_str("stream").ok())
                .unwrap_or("")
                .to_string(),
            feed_topic: feed
                .and_then(|document| document.get_str("topic").ok())
                .unwrap_or("")
                .to_string(),
        })
    }

    pub fn feed_tail(&mut self, after: i64) -> Result<Vec<FeedEvent>, String> {
        let reply = self.command(doc! { "feedTail": 1i32, "after": after, "$db": "admin" })?;
        ensure_ok(&reply)?;
        let values = reply
            .get_array("records")
            .map_err(|_| "feed has no records".to_string())?;
        let mut events = Vec::new();
        for value in values {
            let Some(document) = value.as_document() else {
                continue;
            };
            events.push(FeedEvent {
                seq: number_field(document, "seq"),
                op: document.get_str("op").unwrap_or("").to_string(),
                ns: document.get_str("ns").unwrap_or("").to_string(),
                id: document.get_str("id").unwrap_or("").to_string(),
                before: document.get_str("before").unwrap_or("").to_string(),
                after: document.get_str("after").unwrap_or("").to_string(),
            });
        }
        Ok(events)
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
    number_field(reply, "n")
}

fn number_field(document: &Document, key: &str) -> i64 {
    match document.get(key) {
        Some(Bson::Int32(value)) => i64::from(*value),
        Some(Bson::Int64(value)) => *value,
        Some(Bson::Double(value)) => *value as i64,
        _ => 0,
    }
}

const PROFILE_CAP: i64 = 8_000;
const PROFILE_PAGE: u64 = 400;
const DISTINCT_CAP: usize = 256;

#[derive(Default)]
struct FieldAccum {
    present: u64,
    nulls: u64,
    numbers: Vec<f64>,
    kinds: std::collections::BTreeMap<&'static str, u64>,
    distinct: std::collections::BTreeSet<String>,
    distinct_capped: bool,
}

impl FieldAccum {
    fn finish(self, name: String, scanned: u64) -> FieldProfile {
        let present = self.present as i64;
        let nulls = self.nulls as i64;
        let missing = (scanned as i64) - present - nulls;
        let fill = if scanned == 0 {
            0.0
        } else {
            self.present as f64 / scanned as f64
        };
        let kind = self
            .kinds
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(kind, _)| (*kind).to_string())
            .unwrap_or_else(|| "empty".to_string());
        let distinct = if self.distinct_capped {
            -1
        } else {
            self.distinct.len() as i64
        };
        FieldProfile {
            name,
            present,
            missing,
            nulls,
            fill,
            kind,
            distinct,
            numeric: numeric_summary(&self.numbers),
        }
    }
}

fn note(accum: &mut FieldAccum, value: Option<&Bson>) {
    let Some(value) = value else {
        return;
    };
    if matches!(value, Bson::Null) {
        accum.nulls += 1;
        return;
    }
    accum.present += 1;
    let kind = match value {
        Bson::Double(_) | Bson::Int32(_) | Bson::Int64(_) => "number",
        Bson::String(_) => "string",
        Bson::Boolean(_) => "bool",
        Bson::DateTime(_) => "date",
        Bson::Document(_) => "object",
        Bson::Array(_) => "array",
        Bson::ObjectId(_) => "oid",
        _ => "other",
    };
    *accum.kinds.entry(kind).or_insert(0) += 1;
    if let Some(number) = as_number(value) {
        accum.numbers.push(number);
    }
    if !accum.distinct_capped {
        accum.distinct.insert(format!("{value:?}"));
        if accum.distinct.len() > DISTINCT_CAP {
            accum.distinct_capped = true;
            accum.distinct.clear();
        }
    }
}

fn as_number(value: &Bson) -> Option<f64> {
    match value {
        Bson::Int32(number) => Some(f64::from(*number)),
        Bson::Int64(number) => Some(*number as f64),
        Bson::Double(number) if number.is_finite() => Some(*number),
        _ => None,
    }
}

fn numeric_summary(values: &[f64]) -> Option<NumericProfile> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    let min = sorted[0];
    let max = sorted[sorted.len() - 1];
    let q1 = quantile(&sorted, 0.25);
    let median = quantile(&sorted, 0.5);
    let q3 = quantile(&sorted, 0.75);
    let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
    let sd = if sorted.len() > 1 {
        let variance = sorted
            .iter()
            .map(|value| (value - mean).powi(2))
            .sum::<f64>()
            / (sorted.len() - 1) as f64;
        variance.sqrt()
    } else {
        0.0
    };
    let iqr = q3 - q1;
    let low_fence = q1 - 1.5 * iqr;
    let high_fence = q3 + 1.5 * iqr;
    let whisker_low = sorted
        .iter()
        .copied()
        .find(|value| *value >= low_fence)
        .unwrap_or(min);
    let whisker_high = sorted
        .iter()
        .rev()
        .copied()
        .find(|value| *value <= high_fence)
        .unwrap_or(max);
    let outliers: Vec<f64> = sorted
        .iter()
        .copied()
        .filter(|value| *value < low_fence || *value > high_fence)
        .take(24)
        .collect();
    Some(NumericProfile {
        min,
        q1,
        median,
        q3,
        max,
        mean,
        sd,
        whisker_low,
        whisker_high,
        outliers,
        bins: histogram(&sorted, min, max),
    })
}

fn quantile(sorted: &[f64], fraction: f64) -> f64 {
    let index = ((sorted.len() - 1) as f64 * fraction).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn histogram(sorted: &[f64], min: f64, max: f64) -> Vec<ProfileBin> {
    const BINS: usize = 16;
    if min == max {
        return vec![ProfileBin {
            start: min,
            end: max,
            n: sorted.len() as i64,
        }];
    }
    let width = (max - min) / BINS as f64;
    let mut counts = vec![0i64; BINS];
    for value in sorted {
        let mut slot = ((*value - min) / width).floor() as usize;
        if slot >= BINS {
            slot = BINS - 1;
        }
        counts[slot] += 1;
    }
    counts
        .into_iter()
        .enumerate()
        .map(|(index, n)| ProfileBin {
            start: min + width * index as f64,
            end: min + width * (index as f64 + 1.0),
            n,
        })
        .collect()
}

fn key_parts(key: &Document) -> Vec<Value> {
    key.iter()
        .map(|(name, value)| {
            let order = match value {
                Bson::Int32(-1) | Bson::Int64(-1) => json!(-1),
                Bson::Double(number) if *number == -1.0 => json!(-1),
                Bson::String(text) if text == "hashed" => json!("hashed"),
                _ => json!(1),
            };
            json!({ "field": name, "order": order })
        })
        .collect()
}

fn key_label(key: &Document) -> String {
    key.iter()
        .map(|(name, value)| {
            let descending = match value {
                Bson::Int32(number) => *number == -1,
                Bson::Int64(number) => *number == -1,
                Bson::Double(number) => *number == -1.0,
                _ => false,
            };
            if descending {
                format!("{name}: -1")
            } else {
                name.clone()
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
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
