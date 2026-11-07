//! The messages the window sends. Each one calls [`crate::shell::Client`].

use serde_json::{json, Value};

use super::client::Client;
use super::parse::{parse_bson, parse_document};
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
            "find" => self.find(&value),
            "sql" => self.sql(&value),
            "save" => self.save(&value),
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
                ok(json!({}))
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
        match client.list_collections(database) {
            Ok(names) => ok(json!({ "names": names })),
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
        match client.find(database, collection, filter) {
            Ok(documents) => match rows(&documents) {
                Ok(rows) => ok(json!({ "rows": rows })),
                Err(err) => error(&err),
            },
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
        match client.sql(database, statement) {
            Ok(documents) => match rows(&documents) {
                Ok(rows) => ok(json!({ "rows": rows })),
                Err(err) => error(&err),
            },
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
}

fn rows(documents: &[bson::Document]) -> Result<Vec<Value>, String> {
    let mut out = Vec::new();
    for document in documents {
        let id = match document.get("_id") {
            Some(id) => serde_json::to_string(&id.clone().into_relaxed_extjson())
                .map_err(|err| err.to_string())?,
            None => "null".to_string(),
        };
        out.push(json!({
            "id": id,
            "text": pretty(document)?,
        }));
    }
    Ok(out)
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

fn keep(message: &str) -> String {
    json!({ "ok": false, "keep": true, "error": message }).to_string()
}
