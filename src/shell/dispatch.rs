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
            "export" => export_csv(&value),
            "save" => self.save(&value),
            "patch" => self.patch(&value),
            "connections" => self.connections(),
            "saveConnection" => self.save_connection(&value),
            "deleteConnection" => self.delete_connection(&value),
            "status" => self.status(),
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
                    .map(|item| json!({ "name": item.name, "type": item.kind }))
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

fn export_csv(value: &Value) -> String {
    let name = value.get("name").and_then(Value::as_str).unwrap_or("kova");
    let csv = value.get("csv").and_then(Value::as_str).unwrap_or("");
    match write_csv(name, csv) {
        Ok(path) => ok(json!({ "path": path.display().to_string() })),
        Err(err) => error(&err),
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
    use super::format_date;

    #[test]
    fn a_date_is_utc_with_milliseconds() {
        assert_eq!(format_date(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(format_date(86_400_000), "1970-01-02T00:00:00.000Z");
        assert_eq!(format_date(-1), "1969-12-31T23:59:59.999Z");
    }
}

fn keep(message: &str) -> String {
    json!({ "ok": false, "keep": true, "error": message }).to_string()
}
