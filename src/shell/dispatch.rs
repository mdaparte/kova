//! The messages the window sends. Each one calls [`crate::shell::Client`].

use std::fs;
use std::path::PathBuf;
use std::time::Instant;

use serde_json::{json, Value};

use super::client::Client;
use super::connections::{self, Connection};
use super::parse::{parse_bson, parse_cell, parse_command, parse_document, Command};
use super::pretty;

pub struct Ui {
    client: Option<Client>,
}

impl Default for Ui {
    fn default() -> Self {
        Self::new()
    }
}

impl Ui {
    pub fn new() -> Self {
        Self { client: None }
    }

    pub fn dispatch(&mut self, request: &str) -> String {
        let value: Value = match serde_json::from_str(request) {
            Ok(value) => value,
            Err(err) => return error(&err.to_string()),
        };
        let op = value.get("op").and_then(Value::as_str).unwrap_or("");
        match op {
            "connect" => self.connect(&value),
            "databases" => self.databases(),
            "collections" => self.collections(&value),
            "fields" => self.fields(&value),
            "indexes" => self.indexes(&value),
            "find" => self.find(&value),
            "mql" => self.mql(&value),
            "sql" => self.sql(&value),
            "count" => self.count(&value),
            "export" => self.export_csv(&value),
            "createView" => self.create_view(&value),
            "pickSave" => suggest_export(&value),
            "save" => self.save(&value),
            "patch" => self.patch(&value),
            "connections" => self.connections(),
            "saveConnection" => self.save_connection(&value),
            "deleteConnection" => self.delete_connection(&value),
            "status" => self.status(),
            "dashboard" => self.dashboard(&value),
            "profile" => self.profile(&value),
            "feed" => self.feed(&value),
            other => error(&format!("unknown request {other}")),
        }
    }

    fn connect(&mut self, value: &Value) -> String {
        let host = value
            .get("host")
            .and_then(Value::as_str)
            .unwrap_or("127.0.0.1");
        let port = match value.get("port") {
            Some(Value::Number(number)) => {
                match number.as_u64().and_then(|port| u16::try_from(port).ok()) {
                    Some(port) => port,
                    None => return error("port is out of range"),
                }
            }
            Some(Value::String(text)) => match text.parse::<u16>() {
                Ok(port) => port,
                Err(_) => return error("port is out of range"),
            },
            _ => 27018,
        };
        let user = value.get("user").and_then(Value::as_str);
        let password = value.get("password").and_then(Value::as_str);
        match Client::connect(host, port, user, password) {
            Ok(client) => {
                self.client = Some(client);
                if let Some(id) = value.get("id").and_then(Value::as_str) {
                    if !id.is_empty() {
                        let _ = connections::touch(id);
                    }
                }
                ok(json!({}))
            }
            Err(err) => error(&err),
        }
    }

    fn status(&mut self) -> String {
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        match client.status() {
            Ok(status) => ok(json!({
                "version": status.version,
                "host": status.host,
                "system": status.system,
                "uptime": status.uptime,
                "memory": status.memory,
                "disk": status.disk,
                "diskPath": status.disk_path,
                "connectionsActive": status.connections_active,
                "connectionsTotal": status.connections_total,
                "queries": status.queries,
                "inserts": status.inserts,
                "updates": status.updates,
                "deletes": status.deletes,
                "bytesIn": status.bytes_in,
                "bytesOut": status.bytes_out,
                "walWrites": status.wal_writes,
                "feed": {
                    "enabled": status.feed_enabled,
                    "linked": status.feed_linked,
                    "address": status.feed_address,
                    "stream": status.feed_stream,
                    "topic": status.feed_topic,
                },
            })),
            Err(err) => error(&err),
        }
    }

    fn dashboard(&mut self, value: &Value) -> String {
        let Some(database) = value.get("db").and_then(Value::as_str) else {
            return error("missing database");
        };
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        match client.dashboard(database) {
            Ok(board) => {
                let tables: Vec<Value> = board
                    .tables
                    .iter()
                    .map(|table| {
                        json!({
                            "name": table.name,
                            "type": table.kind,
                            "inverted": table.inverted,
                            "count": table.count,
                            "size": table.size,
                            "avg": table.avg,
                            "indexes": table.indexes,
                        })
                    })
                    .collect();
                ok(json!({
                    "name": database,
                    "collections": board.collections,
                    "views": board.views,
                    "documents": board.documents,
                    "dataSize": board.data_size,
                    "indexes": board.indexes,
                    "tables": tables,
                }))
            }
            Err(err) => error(&err),
        }
    }

    fn profile(&mut self, value: &Value) -> String {
        let Some(database) = value.get("db").and_then(Value::as_str) else {
            return error("missing database");
        };
        let Some(collection) = value.get("collection").and_then(Value::as_str) else {
            return error("missing collection");
        };
        let field = value
            .get("field")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty());
        let filter = value.get("filter").and_then(Value::as_str).unwrap_or("{}");
        let filter = match parse_document(filter) {
            Ok(filter) => filter,
            Err(err) => return error(&err),
        };
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        match client.profile(database, collection, filter, field) {
            Ok(profile) => {
                let fields: Vec<Value> = profile
                    .fields
                    .iter()
                    .map(|field| {
                        json!({
                            "name": field.name,
                            "present": field.present,
                            "missing": field.missing,
                            "nulls": field.nulls,
                            "fill": field.fill,
                            "kind": field.kind,
                            "distinct": field.distinct,
                            "numeric": field.numeric.as_ref().map(|numeric| json!({
                                "min": numeric.min,
                                "q1": numeric.q1,
                                "median": numeric.median,
                                "q3": numeric.q3,
                                "max": numeric.max,
                                "mean": numeric.mean,
                                "sd": numeric.sd,
                                "whiskerLow": numeric.whisker_low,
                                "whiskerHigh": numeric.whisker_high,
                                "outliers": numeric.outliers,
                                "bins": numeric.bins.iter().map(|bin| json!({
                                    "start": bin.start,
                                    "end": bin.end,
                                    "n": bin.n,
                                })).collect::<Vec<_>>(),
                            })),
                        })
                    })
                    .collect();
                ok(json!({
                    "documents": profile.documents,
                    "scanned": profile.scanned,
                    "fields": fields,
                }))
            }
            Err(err) => error(&err),
        }
    }

    fn feed(&mut self, value: &Value) -> String {
        let after = value.get("after").and_then(Value::as_i64).unwrap_or(0);
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        match client.feed_tail(after) {
            Ok(events) => {
                let events: Vec<Value> = events
                    .iter()
                    .map(|event| {
                        json!({
                            "seq": event.seq,
                            "op": event.op,
                            "ns": event.ns,
                            "id": event.id,
                            "before": event.before,
                            "after": event.after,
                        })
                    })
                    .collect();
                ok(json!({ "events": events }))
            }
            Err(err) => error(&err),
        }
    }

    fn databases(&mut self) -> String {
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        match client.list_databases() {
            Ok(names) => ok(json!({ "names": names })),
            Err(err) => error(&err),
        }
    }

    fn collections(&mut self, value: &Value) -> String {
        let Some(database) = value.get("db").and_then(Value::as_str) else {
            return error("missing database");
        };
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        match client.list_collection_info(database) {
            Ok(items) => {
                let names: Vec<&str> = items.iter().map(|item| item.name.as_str()).collect();
                let items: Vec<Value> = items
                    .iter()
                    .map(|item| {
                        json!({
                            "name": item.name,
                            "type": item.kind,
                            "inverted": item.inverted,
                            "options": item.options,
                        })
                    })
                    .collect();
                ok(json!({ "names": names, "items": items }))
            }
            Err(err) => error(&err),
        }
    }

    fn fields(&mut self, value: &Value) -> String {
        let Some(database) = value.get("db").and_then(Value::as_str) else {
            return error("missing database");
        };
        let Some(collection) = value.get("collection").and_then(Value::as_str) else {
            return error("missing collection");
        };
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        match client.field_names(database, collection) {
            Ok(fields) => ok(json!({ "fields": fields })),
            Err(err) => error(&err),
        }
    }

    fn indexes(&mut self, value: &Value) -> String {
        let Some(database) = value.get("db").and_then(Value::as_str) else {
            return error("missing database");
        };
        let Some(collection) = value.get("collection").and_then(Value::as_str) else {
            return error("missing collection");
        };
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        match client.list_indexes(database, collection) {
            Ok(indexes) => {
                let indexes: Vec<Value> = indexes
                    .iter()
                    .map(|index| {
                        json!({
                            "name": index.name,
                            "key": index.key,
                            "unique": index.unique,
                            "sparse": index.sparse,
                            "parts": index.parts,
                        })
                    })
                    .collect();
                ok(json!({ "indexes": indexes }))
            }
            Err(err) => error(&err),
        }
    }

    fn find(&mut self, value: &Value) -> String {
        let Some(database) = value.get("db").and_then(Value::as_str) else {
            return error("missing database");
        };
        let Some(collection) = value.get("collection").and_then(Value::as_str) else {
            return error("missing collection");
        };
        let filter = value.get("filter").and_then(Value::as_str).unwrap_or("{}");
        let filter = match parse_document(filter) {
            Ok(filter) => filter,
            Err(err) => return error(&err),
        };
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        let started = Instant::now();
        match client.find(database, collection, filter) {
            Ok(documents) => found(&documents, started.elapsed().as_millis(), None),
            Err(err) => error(&err),
        }
    }

    fn mql(&mut self, value: &Value) -> String {
        let Some(database) = value.get("db").and_then(Value::as_str) else {
            return error("missing database");
        };
        let Some(text) = value.get("mql").and_then(Value::as_str) else {
            return error("missing query");
        };
        let (collection, filter) = match parse_command(text) {
            Ok(Command::Find { collection, filter }) => (collection, filter),
            Ok(_) => return error("the editor runs a find"),
            Err(err) => return error(&err),
        };
        let skip = number_of(value, "skip");
        let limit = positive(value, "limit");
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        let started = Instant::now();
        match client.find_page(database, &collection, filter, skip, limit) {
            Ok(documents) => found(
                &documents,
                started.elapsed().as_millis(),
                Some(collection.as_str()),
            ),
            Err(err) => error(&err),
        }
    }

    fn sql(&mut self, value: &Value) -> String {
        let Some(database) = value.get("db").and_then(Value::as_str) else {
            return error("missing database");
        };
        let Some(statement) = value.get("sql").and_then(Value::as_str) else {
            return error("missing sql");
        };
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        let started = Instant::now();
        match client.sql(database, statement) {
            Ok(documents) => {
                let page = page_of(
                    &documents,
                    number_of(value, "skip"),
                    positive(value, "limit"),
                );
                found(&page, started.elapsed().as_millis(), None)
            }
            Err(err) => error(&err),
        }
    }

    fn count(&mut self, value: &Value) -> String {
        let Some(database) = value.get("db").and_then(Value::as_str) else {
            return error("missing database");
        };
        let (collection, filter) = if let Some(text) = value.get("mql").and_then(Value::as_str) {
            match parse_command(text) {
                Ok(Command::Find { collection, filter }) => (collection, filter),
                Ok(_) => return error("count runs on a find"),
                Err(err) => return error(&err),
            }
        } else {
            let Some(collection) = value.get("collection").and_then(Value::as_str) else {
                return error("missing collection");
            };
            let filter = value.get("filter").and_then(Value::as_str).unwrap_or("{}");
            match parse_document(filter) {
                Ok(filter) => (collection.to_string(), filter),
                Err(err) => return error(&err),
            }
        };
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        let started = Instant::now();
        match client.count(database, &collection, filter) {
            Ok(count) => ok(json!({
                "n": count,
                "ms": started.elapsed().as_millis() as u64,
            })),
            Err(err) => error(&err),
        }
    }

    fn save(&mut self, value: &Value) -> String {
        let Some(database) = value.get("db").and_then(Value::as_str) else {
            return error("missing database");
        };
        let Some(collection) = value.get("collection").and_then(Value::as_str) else {
            return error("missing collection");
        };
        let Some(id_text) = value.get("id").and_then(Value::as_str) else {
            return error("document has no _id");
        };
        let Some(text) = value.get("document").and_then(Value::as_str) else {
            return error("missing document");
        };
        let id = match parse_bson(id_text) {
            Ok(id) => id,
            Err(err) => return keep(&err),
        };
        let document = match parse_document(text) {
            Ok(document) => document,
            Err(err) => return keep(&err),
        };
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        match client.save_document(database, collection, id, document) {
            Ok(()) => ok(json!({})),
            Err(err) => keep(&err),
        }
    }

    fn patch(&mut self, value: &Value) -> String {
        let Some(database) = value.get("db").and_then(Value::as_str) else {
            return error("missing database");
        };
        let Some(collection) = value.get("collection").and_then(Value::as_str) else {
            return error("missing collection");
        };
        let Some(id_text) = value.get("id").and_then(Value::as_str) else {
            return keep("document has no _id");
        };
        let Some(field) = value.get("field").and_then(Value::as_str) else {
            return keep("missing field");
        };
        if field.is_empty() || field == "_id" || field.starts_with('$') || field.contains('.') {
            return keep("this field cannot be edited");
        }
        let text = value.get("text").and_then(Value::as_str).unwrap_or("");
        let kind = value
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or("missing");
        let id = match parse_bson(id_text) {
            Ok(id) => id,
            Err(err) => return keep(&err),
        };
        let cell = match parse_cell(text, kind) {
            Ok(cell) => cell,
            Err(err) => return keep(&err),
        };
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        let mut set = bson::Document::new();
        set.insert(field, cell);
        let mut update = bson::Document::new();
        update.insert("$set", set);
        let mut filter = bson::Document::new();
        filter.insert("_id", id);
        match client.update_one(database, collection, filter, update) {
            Ok(_) => ok(json!({})),
            Err(err) => keep(&err),
        }
    }

    fn connections(&mut self) -> String {
        match connections::load() {
            Ok(connections) => ok(json!({
                "connections": connections,
                "path": connections::path().display().to_string(),
            })),
            Err(err) => error(&err),
        }
    }

    fn save_connection(&mut self, value: &Value) -> String {
        let connection = Connection {
            id: value.get("id").and_then(Value::as_str).unwrap_or("").into(),
            name: value
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("")
                .into(),
            host: value
                .get("host")
                .and_then(Value::as_str)
                .unwrap_or("127.0.0.1")
                .into(),
            port: port_of(value),
            user: value
                .get("user")
                .and_then(Value::as_str)
                .unwrap_or("")
                .into(),
            password: value
                .get("password")
                .and_then(Value::as_str)
                .unwrap_or("")
                .into(),
            last_connected: None,
        };
        match connections::save(connection) {
            Ok(saved) => ok(json!({ "connection": saved })),
            Err(err) => error(&err),
        }
    }

    fn delete_connection(&mut self, value: &Value) -> String {
        let Some(id) = value.get("id").and_then(Value::as_str) else {
            return error("missing connection");
        };
        match connections::delete(id) {
            Ok(()) => ok(json!({})),
            Err(err) => error(&err),
        }
    }
}

fn found(documents: &[bson::Document], ms: u128, collection: Option<&str>) -> String {
    match rows(documents) {
        Ok(rows) => {
            let mut body = json!({ "rows": rows, "ms": ms as u64 });
            if let Some(collection) = collection {
                body.as_object_mut()
                    .expect("object")
                    .insert("collection".into(), Value::String(collection.to_string()));
            }
            ok(body)
        }
        Err(err) => error(&err),
    }
}

fn page_of(documents: &[bson::Document], skip: u64, limit: Option<u64>) -> Vec<bson::Document> {
    let start = (skip as usize).min(documents.len());
    let rest = &documents[start..];
    match limit {
        Some(limit) => rest.iter().take(limit as usize).cloned().collect(),
        None => rest.to_vec(),
    }
}

fn number_of(value: &Value, key: &str) -> u64 {
    match value.get(key) {
        Some(Value::Number(number)) => number.as_u64().unwrap_or(0),
        Some(Value::String(text)) => text.parse().unwrap_or(0),
        _ => 0,
    }
}

fn positive(value: &Value, key: &str) -> Option<u64> {
    let number = number_of(value, key);
    if number == 0 {
        None
    } else {
        Some(number)
    }
}

fn suggest_export(value: &Value) -> String {
    let name = value.get("name").and_then(Value::as_str).unwrap_or("kova");
    ok(json!({
        "dir": downloads_dir().display().to_string(),
        "file": csv_file_name(name),
    }))
}

impl Ui {
    fn export_csv(&mut self, value: &Value) -> String {
        if value.get("path").is_some() || value.get("dir").is_some() {
            return self.export_query(value);
        }
        let name = value.get("name").and_then(Value::as_str).unwrap_or("kova");
        let csv = value.get("csv").and_then(Value::as_str).unwrap_or("");
        match write_csv(name, csv) {
            Ok(path) => ok(json!({ "path": path.display().to_string() })),
            Err(err) => error(&err),
        }
    }

    /// Writes every row of the current find or SQL statement, not the page on screen.
    fn export_query(&mut self, value: &Value) -> String {
        let Some(database) = value.get("db").and_then(Value::as_str) else {
            return error("missing database");
        };
        let documents = if let Some(text) = value.get("mql").and_then(Value::as_str) {
            let (collection, filter) = match parse_command(text) {
                Ok(Command::Find { collection, filter }) => (collection, filter),
                Ok(_) => return error("the editor runs a find"),
                Err(err) => return error(&err),
            };
            let Some(client) = self.client.as_mut() else {
                return error("not connected");
            };
            match client.find(database, &collection, filter) {
                Ok(documents) => documents,
                Err(err) => return error(&err),
            }
        } else if let Some(statement) = value.get("sql").and_then(Value::as_str) {
            let Some(client) = self.client.as_mut() else {
                return error("not connected");
            };
            match client.sql(database, statement) {
                Ok(documents) => documents,
                Err(err) => return error(&err),
            }
        } else {
            return error("missing query");
        };
        let csv = match documents_to_csv(&documents) {
            Ok(csv) => csv,
            Err(err) => return error(&err),
        };
        let path = match export_destination(value) {
            Ok(path) => path,
            Err(err) => return error(&err),
        };
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() && !parent.is_dir() {
                return error(&format!("folder {} does not exist", parent.display()));
            }
        }
        if let Err(err) = fs::write(&path, csv) {
            return error(&format!("save {}: {err}", path.display()));
        }
        ok(json!({
            "path": path.display().to_string(),
            "n": documents.len(),
        }))
    }

    fn create_view(&mut self, value: &Value) -> String {
        let Some(database) = value.get("db").and_then(Value::as_str) else {
            return error("missing database");
        };
        let Some(name) = value.get("name").and_then(Value::as_str).map(str::trim) else {
            return error("missing view name");
        };
        if name.is_empty() || name.contains('.') || name.contains(char::is_whitespace) {
            return error("the view name cannot contain spaces or '.'");
        }
        if let Some(sql) = value
            .get("sql")
            .and_then(Value::as_str)
            .or_else(|| value.get("mql").and_then(Value::as_str).filter(|text| sql_statement(text)))
        {
            let (source, pipeline) = match sql_view(sql) {
                Ok(parsed) => parsed,
                Err(err) => return error(&err),
            };
            let Some(client) = self.client.as_mut() else {
                return error("not connected");
            };
            return match client.create_view(database, name, &source, pipeline) {
                Ok(()) => ok(json!({ "name": name, "viewOn": source })),
                Err(err) => error(&err),
            };
        }
        let Some(text) = value.get("mql").and_then(Value::as_str) else {
            return error("missing query");
        };
        let (source, filter) = match parse_command(text) {
            Ok(Command::Find { collection, filter }) => (collection, filter),
            Ok(_) => return error("a view stores a find on one collection"),
            Err(err) => return error(&err),
        };
        let pipeline = if filter.is_empty() {
            Vec::new()
        } else {
            let mut stage = bson::Document::new();
            stage.insert("$match", filter);
            vec![stage]
        };
        let Some(client) = self.client.as_mut() else {
            return error("not connected");
        };
        match client.create_view(database, name, &source, pipeline) {
            Ok(()) => ok(json!({ "name": name, "viewOn": source })),
            Err(err) => error(&err),
        }
    }
}

fn write_csv(name: &str, csv: &str) -> Result<PathBuf, String> {
    let dir = downloads_dir();
    fs::create_dir_all(&dir).map_err(|err| format!("create {}: {err}", dir.display()))?;
    let safe = safe_name(name);
    let mut path = dir.join(format!("{safe}.csv"));
    let mut suffix = 2u32;
    while path.exists() {
        path = dir.join(format!("{safe}-{suffix}.csv"));
        suffix += 1;
    }
    fs::write(&path, csv).map_err(|err| format!("save {}: {err}", path.display()))?;
    Ok(path)
}

fn safe_name(name: &str) -> String {
    let safe: String = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect();
    let safe = safe.trim_matches('-');
    if safe.is_empty() {
        "kova".to_string()
    } else {
        safe.to_string()
    }
}

fn csv_file_name(name: &str) -> String {
    let trimmed = name.trim();
    let stem = trimmed
        .strip_suffix(".csv")
        .or_else(|| trimmed.strip_suffix(".CSV"))
        .unwrap_or(trimmed);
    format!("{}.csv", safe_name(stem))
}

fn export_destination(value: &Value) -> Result<PathBuf, String> {
    let path = if let Some(path) = value.get("path").and_then(Value::as_str) {
        if path.trim().is_empty() {
            return Err("choose a file".to_string());
        }
        PathBuf::from(path)
    } else {
        let dir = value.get("dir").and_then(Value::as_str).unwrap_or("");
        let file = value.get("file").and_then(Value::as_str).unwrap_or("");
        if dir.trim().is_empty() || file.trim().is_empty() {
            return Err("choose a folder and a file name".to_string());
        }
        PathBuf::from(dir).join(file)
    };
    Ok(with_csv_extension(path))
}

fn with_csv_extension(path: PathBuf) -> PathBuf {
    match path.extension().and_then(|ext| ext.to_str()) {
        Some(ext) if ext.eq_ignore_ascii_case("csv") => path,
        _ => path.with_extension("csv"),
    }
}

fn documents_to_csv(documents: &[bson::Document]) -> Result<String, String> {
    let mut fields: Vec<String> = documents
        .iter()
        .flat_map(|document| document.keys().cloned())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    if let Some(index) = fields.iter().position(|field| field == "_id") {
        let id = fields.remove(index);
        fields.insert(0, id);
    }
    let mut csv = String::new();
    push_csv_row(&mut csv, &fields);
    for document in documents {
        let cells = fields
            .iter()
            .map(|field| match document.get(field) {
                Some(value) => cell_text(value),
                None => Ok(String::new()),
            })
            .collect::<Result<Vec<_>, _>>()?;
        push_csv_row(&mut csv, &cells);
    }
    Ok(csv)
}

fn push_csv_row(csv: &mut String, cells: &[String]) {
    for (index, cell) in cells.iter().enumerate() {
        if index > 0 {
            csv.push(',');
        }
        if cell.contains([',', '"', '\n', '\r']) {
            csv.push('"');
            csv.push_str(&cell.replace('"', "\"\""));
            csv.push('"');
        } else {
            csv.push_str(cell);
        }
    }
    csv.push('\n');
}

fn downloads_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("KOVA_DOWNLOADS") {
        return PathBuf::from(path);
    }
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from);
    if let Some(home) = home {
        let downloads = home.join("Downloads");
        if downloads.is_dir() {
            return downloads;
        }
        return home;
    }
    std::env::temp_dir()
}

fn port_of(value: &Value) -> u16 {
    match value.get("port") {
        Some(Value::Number(number)) => number
            .as_u64()
            .and_then(|port| u16::try_from(port).ok())
            .unwrap_or(27018),
        Some(Value::String(text)) => text.parse().unwrap_or(27018),
        _ => 27018,
    }
}

fn rows(documents: &[bson::Document]) -> Result<Vec<Value>, String> {
    let mut out = Vec::new();
    for document in documents {
        let id = match document.get("_id") {
            Some(id) => serde_json::to_string(&id.clone().into_relaxed_extjson())
                .map_err(|err| err.to_string())?,
            None => "null".to_string(),
        };
        let mut cells = Vec::new();
        for (field, value) in document {
            cells.push(json!({
                "field": field,
                "text": cell_text(value)?,
                "kind": cell_kind(value),
            }));
        }
        out.push(json!({
            "id": id,
            "text": pretty(document)?,
            "cells": cells,
        }));
    }
    Ok(out)
}

fn cell_text(value: &bson::Bson) -> Result<String, String> {
    match value {
        bson::Bson::String(text) => Ok(text.clone()),
        bson::Bson::Int32(number) => Ok(number.to_string()),
        bson::Bson::Int64(number) => Ok(number.to_string()),
        bson::Bson::Double(number) => Ok(number.to_string()),
        bson::Bson::Boolean(flag) => Ok(flag.to_string()),
        bson::Bson::Null => Ok("null".into()),
        bson::Bson::ObjectId(id) => Ok(id.to_hex()),
        bson::Bson::DateTime(date) => Ok(format_date(date.timestamp_millis())),
        other => serde_json::to_string(&other.clone().into_relaxed_extjson())
            .map_err(|err| err.to_string()),
    }
}

fn cell_kind(value: &bson::Bson) -> &'static str {
    match value {
        bson::Bson::String(_) => "string",
        bson::Bson::Int32(_) => "int",
        bson::Bson::Int64(_) => "long",
        bson::Bson::Double(_) => "double",
        bson::Bson::Boolean(_) => "bool",
        bson::Bson::Null => "null",
        bson::Bson::ObjectId(_) => "oid",
        bson::Bson::DateTime(_) => "date",
        bson::Bson::Document(_) => "object",
        bson::Bson::Array(_) => "array",
        _ => "other",
    }
}

fn format_date(ms: i64) -> String {
    let day = ms.div_euclid(86_400_000);
    let tod = ms.rem_euclid(86_400_000) as u32;
    let hour = tod / 3_600_000;
    let minute = (tod % 3_600_000) / 60_000;
    let second = (tod % 60_000) / 1_000;
    let milli = tod % 1_000;
    let (year, month, day) = civil_date(day);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{milli:03}Z")
}

/// Days since 1970-01-01 to a civil year, month, and day.
fn civil_date(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let year_of_era = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = year_of_era + if month <= 2 { 1 } else { 0 };
    (year as i32, month as u32, day as u32)
}

const VIEW_SQL: &str = "a view stores a select of columns from one collection, with at most one inner join on equal fields";

fn sql_statement(text: &str) -> bool {
    let word = text
        .trim()
        .split_whitespace()
        .next()
        .unwrap_or("")
        .trim_end_matches(';');
    matches!(
        word.to_ascii_lowercase().as_str(),
        "select" | "with" | "show" | "describe" | "desc"
    )
}

struct SqlWord<'a> {
    text: &'a str,
}

/// Turns a select into the pipeline a view can store.
///
/// One collection, an optional inner join on one equality, and the
/// columns to keep. Anything wider stays a query, not a view.
fn sql_view(sql: &str) -> Result<(String, Vec<bson::Document>), String> {
    let sql = sql.trim().trim_end_matches(';').trim();
    if sql.is_empty() || sql.contains(['(', ')', ';']) {
        return Err(VIEW_SQL.into());
    }
    let words = sql_words(sql);
    if words.first().is_none_or(|word| !sql_kw(word.text, "select")) {
        return Err(VIEW_SQL.into());
    }
    if words.iter().any(|word| {
        matches!(
            word.text.to_ascii_lowercase().as_str(),
            "union"
                | "group"
                | "order"
                | "limit"
                | "having"
                | "with"
                | "except"
                | "intersect"
                | "left"
                | "right"
                | "full"
                | "cross"
                | "outer"
                | "natural"
        )
    }) {
        return Err(VIEW_SQL.into());
    }
    let from_at = words
        .iter()
        .position(|word| sql_kw(word.text, "from"))
        .ok_or(VIEW_SQL)?;
    let columns = sql_select(&words[1..from_at])?;
    let (source, source_alias, mut rest) = sql_relation(&words[from_at + 1..])?;
    let mut join: Option<SqlJoin> = None;
    if rest.first().is_some_and(|word| sql_kw(word.text, "inner")) {
        rest = &rest[1..];
    }
    if rest.first().is_some_and(|word| sql_kw(word.text, "join")) {
        let (table, alias, after) = sql_relation(&rest[1..])?;
        if after.first().is_none_or(|word| !sql_kw(word.text, "on")) || after.len() < 4 {
            return Err(VIEW_SQL.into());
        }
        if after[2].text != "=" {
            return Err(VIEW_SQL.into());
        }
        join = Some(SqlJoin {
            table,
            alias,
            left: after[1].text.to_string(),
            right: after[3].text.to_string(),
        });
        rest = &after[4..];
    }
    let mut predicate = None;
    if rest.first().is_some_and(|word| sql_kw(word.text, "where")) {
        predicate = Some(sql_predicate(
            sql,
            &source,
            &source_alias,
            join.as_ref().map(|item| item.alias.as_str()),
        )?);
        rest = &[];
    }
    if !rest.is_empty() {
        return Err(VIEW_SQL.into());
    }
    let join_alias = join.as_ref().map(|join| join.alias.as_str());
    let mut pipeline = Vec::new();
    if let Some(pred) = predicate.as_ref().filter(|pred| !pred.on_join) {
        pipeline.push(bson::doc! { "$match": pred.filter.clone() });
    }
    if let Some(join) = join.as_ref() {
        let (local, foreign) = sql_join_keys(join, &source, &source_alias)?;
        pipeline.push(bson::doc! {
            "$lookup": {
                "from": &join.table,
                "localField": local,
                "foreignField": foreign,
                "as": &join.alias,
            }
        });
        pipeline.push(bson::doc! { "$unwind": format!("${}", join.alias) });
    }
    if let Some(pred) = predicate.as_ref().filter(|pred| pred.on_join) {
        pipeline.push(bson::doc! { "$match": pred.filter.clone() });
    }
    if let Some(project) = sql_project(&columns, &source, &source_alias, join_alias)? {
        pipeline.push(bson::doc! { "$project": project });
    }
    Ok((source, pipeline))
}

struct SqlJoin {
    table: String,
    alias: String,
    left: String,
    right: String,
}

struct SqlPred {
    on_join: bool,
    filter: bson::Document,
}

enum SqlColumn {
    Star,
    Field { path: String, output: String },
}

fn sql_words(sql: &str) -> Vec<SqlWord<'_>> {
    let mut words = Vec::new();
    let mut index = 0;
    while index < sql.len() {
        let ch = sql[index..].chars().next().expect("char");
        if ch.is_whitespace() {
            index += ch.len_utf8();
            continue;
        }
        if ch == ',' || ch == '=' {
            words.push(SqlWord {
                text: &sql[index..index + 1],
            });
            index += 1;
            continue;
        }
        if ch == '\'' || ch == '"' {
            let start = index;
            index += ch.len_utf8();
            while index < sql.len() {
                let next = sql[index..].chars().next().expect("char");
                index += next.len_utf8();
                if next == ch {
                    break;
                }
            }
            words.push(SqlWord {
                text: &sql[start..index],
            });
            continue;
        }
        let start = index;
        index += ch.len_utf8();
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' || ch == '*' {
            while index < sql.len() {
                let next = sql[index..].chars().next().expect("char");
                if next.is_ascii_alphanumeric() || next == '_' || next == '.' || next == '*' {
                    index += next.len_utf8();
                } else {
                    break;
                }
            }
        }
        words.push(SqlWord {
            text: &sql[start..index],
        });
    }
    words
}

fn sql_kw(word: &str, keyword: &str) -> bool {
    word.eq_ignore_ascii_case(keyword)
}

fn sql_ident(text: &str) -> bool {
    let mut chars = text.chars();
    match chars.next() {
        Some(ch) if ch.is_ascii_alphabetic() || ch == '_' => {}
        _ => return false,
    }
    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn sql_field(text: &str) -> bool {
    !text.is_empty() && text.split('.').all(sql_ident)
}

fn sql_relation<'a>(words: &'a [SqlWord<'a>]) -> Result<(String, String, &'a [SqlWord<'a>]), String> {
    let Some(table) = words.first() else {
        return Err(VIEW_SQL.into());
    };
    if !sql_ident(table.text) {
        return Err(VIEW_SQL.into());
    }
    let mut index = 1;
    if words.get(1).is_some_and(|word| sql_kw(word.text, "as")) {
        index = 2;
    }
    if let Some(alias) = words.get(index) {
        if sql_ident(alias.text) && !sql_clause(alias.text) {
            return Ok((
                table.text.to_string(),
                alias.text.to_string(),
                &words[index + 1..],
            ));
        }
    }
    Ok((table.text.to_string(), table.text.to_string(), &words[1..]))
}

fn sql_clause(word: &str) -> bool {
    matches!(
        word.to_ascii_lowercase().as_str(),
        "where" | "join" | "inner" | "on" | "select" | "from" | "as"
    )
}

fn sql_select(words: &[SqlWord<'_>]) -> Result<Vec<SqlColumn>, String> {
    if words.is_empty() {
        return Err(VIEW_SQL.into());
    }
    if words.len() == 1 && words[0].text == "*" {
        return Ok(vec![SqlColumn::Star]);
    }
    let mut columns = Vec::new();
    let mut index = 0;
    while index < words.len() {
        let expr = words[index].text;
        if !sql_field(expr) {
            return Err(VIEW_SQL.into());
        }
        index += 1;
        let output = if words.get(index).is_some_and(|word| sql_kw(word.text, "as")) {
            index += 1;
            let alias = words.get(index).ok_or(VIEW_SQL)?;
            if !sql_ident(alias.text) {
                return Err(VIEW_SQL.into());
            }
            index += 1;
            alias.text.to_string()
        } else if words
            .get(index)
            .is_some_and(|word| word.text != "," && sql_ident(word.text))
        {
            let alias = words[index].text;
            index += 1;
            alias.to_string()
        } else {
            expr.rsplit('.').next().unwrap_or(expr).to_string()
        };
        if index < words.len() {
            if words[index].text != "," {
                return Err(VIEW_SQL.into());
            }
            index += 1;
            if index == words.len() {
                return Err(VIEW_SQL.into());
            }
        }
        columns.push(SqlColumn::Field {
            path: expr.to_string(),
            output,
        });
    }
    Ok(columns)
}

fn sql_predicate(
    sql: &str,
    source: &str,
    source_alias: &str,
    join_alias: Option<&str>,
) -> Result<SqlPred, String> {
    let Some(at) = sql.to_ascii_lowercase().rfind(" where ") else {
        return Err(VIEW_SQL.into());
    };
    let pred = sql[at + 7..].trim();
    let lower = pred.to_ascii_lowercase();
    if lower.contains(" and ") || lower.contains(" or ") {
        return Err(VIEW_SQL.into());
    }
    let (raw, rest) = split_sql_field(pred).ok_or(VIEW_SQL)?;
    let (on_join, field) = if let Some((qual, name)) = raw.split_once('.') {
        if qual == source_alias || qual == source {
            (false, name.to_string())
        } else if join_alias.is_some_and(|alias| qual == alias) {
            (true, format!("{qual}.{name}"))
        } else {
            return Err(VIEW_SQL.into());
        }
    } else {
        (false, raw.to_string())
    };
    let rest = rest.trim_start();
    let (op, literal) = if let Some(rest) = rest.strip_prefix(">=") {
        ("$gte", rest)
    } else if let Some(rest) = rest.strip_prefix("<=") {
        ("$lte", rest)
    } else if rest.starts_with("<>") || rest.starts_with("!=") {
        return Err(VIEW_SQL.into());
    } else if let Some(rest) = rest.strip_prefix('>') {
        ("$gt", rest)
    } else if let Some(rest) = rest.strip_prefix('<') {
        ("$lt", rest)
    } else if let Some(rest) = rest.strip_prefix('=') {
        ("$eq", rest)
    } else {
        return Err(VIEW_SQL.into());
    };
    let value = sql_value(literal.trim()).ok_or(VIEW_SQL)?;
    let mut filter = bson::Document::new();
    if op == "$eq" {
        filter.insert(field, value);
    } else {
        filter.insert(field, bson::doc! { op: value });
    }
    Ok(SqlPred { on_join, filter })
}

fn split_sql_field(text: &str) -> Option<(&str, &str)> {
    let text = text.trim_start();
    let end = text.find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'))?;
    if end == 0 {
        return None;
    }
    Some((&text[..end], &text[end..]))
}

fn sql_value(text: &str) -> Option<bson::Bson> {
    if let Some(inner) = text
        .strip_prefix('\'')
        .and_then(|rest| rest.strip_suffix('\''))
    {
        return Some(bson::Bson::String(inner.to_string()));
    }
    if let Some(inner) = text
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    {
        return Some(bson::Bson::String(inner.to_string()));
    }
    if let Ok(number) = text.parse::<i64>() {
        return Some(if let Ok(small) = i32::try_from(number) {
            bson::Bson::Int32(small)
        } else {
            bson::Bson::Int64(number)
        });
    }
    text.parse::<f64>()
        .ok()
        .filter(|number| number.is_finite())
        .map(bson::Bson::Double)
}

fn sql_join_keys(join: &SqlJoin, source: &str, source_alias: &str) -> Result<(String, String), String> {
    let left = sql_side(&join.left, source, source_alias, &join.table, &join.alias)?;
    let right = sql_side(&join.right, source, source_alias, &join.table, &join.alias)?;
    match (left, right) {
        (SqlSide::Local(local), SqlSide::Foreign(foreign))
        | (SqlSide::Foreign(foreign), SqlSide::Local(local)) => Ok((local, foreign)),
        _ => Err(VIEW_SQL.into()),
    }
}

enum SqlSide {
    Local(String),
    Foreign(String),
}

fn sql_side(
    text: &str,
    source: &str,
    source_alias: &str,
    foreign: &str,
    foreign_alias: &str,
) -> Result<SqlSide, String> {
    let Some((qual, field)) = text.split_once('.') else {
        return Err(VIEW_SQL.into());
    };
    if !sql_field(field) {
        return Err(VIEW_SQL.into());
    }
    if qual == source_alias || qual == source {
        Ok(SqlSide::Local(field.to_string()))
    } else if qual == foreign_alias || qual == foreign {
        Ok(SqlSide::Foreign(field.to_string()))
    } else {
        Err(VIEW_SQL.into())
    }
}

fn sql_project(
    columns: &[SqlColumn],
    source: &str,
    source_alias: &str,
    join_alias: Option<&str>,
) -> Result<Option<bson::Document>, String> {
    if columns.iter().any(|column| matches!(column, SqlColumn::Star)) {
        return Ok(None);
    }
    let mut project = bson::Document::new();
    let mut saw_id = false;
    for column in columns {
        let SqlColumn::Field { path, output } = column else {
            continue;
        };
        if !sql_ident(output) {
            return Err(VIEW_SQL.into());
        }
        if project.contains_key(output) {
            return Err(VIEW_SQL.into());
        }
        let stored = sql_stored_path(path, source, source_alias, join_alias)?;
        if output == "_id" {
            saw_id = true;
        }
        project.insert(output, format!("${stored}"));
    }
    if !saw_id {
        project.insert("_id", 0_i32);
    }
    Ok(Some(project))
}

fn sql_stored_path(
    path: &str,
    source: &str,
    source_alias: &str,
    join_alias: Option<&str>,
) -> Result<String, String> {
    let Some((qual, field)) = path.split_once('.') else {
        return Ok(path.to_string());
    };
    if qual == source_alias || qual == source {
        return Ok(field.to_string());
    }
    if join_alias.is_some_and(|alias| qual == alias) {
        return Ok(path.to_string());
    }
    Err(VIEW_SQL.into())
}

fn ok(mut body: Value) -> String {
    body.as_object_mut()
        .expect("object")
        .insert("ok".into(), Value::Bool(true));
    body.to_string()
}

fn error(message: &str) -> String {
    json!({ "ok": false, "error": message }).to_string()
}

#[cfg(test)]
mod tests {
    use super::{format_date, sql_view};

    #[test]
    fn a_date_is_utc_with_milliseconds() {
        assert_eq!(format_date(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(format_date(86_400_000), "1970-01-02T00:00:00.000Z");
        assert_eq!(format_date(-1), "1969-12-31T23:59:59.999Z");
    }

    #[test]
    fn a_sql_join_becomes_a_lookup() {
        let (source, pipeline) = sql_view(
            "SELECT BONU._id, BONU.sal, EMPR.ename name FROM bonus BONU INNER JOIN emp EMPR ON BONU.empno = EMPR.empno",
        )
        .unwrap();
        assert_eq!(source, "bonus");
        assert_eq!(
            pipeline[0].get_document("$lookup").unwrap().get_str("from").unwrap(),
            "emp"
        );
        assert_eq!(pipeline[1].get_str("$unwind").unwrap(), "$EMPR");
        let project = pipeline[2].get_document("$project").unwrap();
        assert_eq!(project.get_str("name").unwrap(), "$EMPR.ename");
        assert_eq!(project.get_str("sal").unwrap(), "$sal");
    }

    #[test]
    fn a_wider_sql_statement_is_refused() {
        assert!(sql_view("SELECT sal FROM bonus UNION SELECT sal FROM emp").is_err());
    }
}

fn keep(message: &str) -> String {
    json!({ "ok": false, "keep": true, "error": message }).to_string()
}
