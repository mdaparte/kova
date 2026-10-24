//! mongodump directory and archive.
//!
//! A directory dump is `<db>/<collection>.bson` (BSON documents
//! concatenated) plus an optional `<collection>.metadata.json`. Pointing
//! at the dump root selects the subdirectory named by the destination
//! database, or the only database subdirectory.
//!
//! An archive is format `0.1`: magic `0x8199e26d`, one prelude block
//! (header BSON, one collection-metadata BSON per collection, terminator
//! `0xffffffff`), then namespace blocks (header BSON with `db`,
//! `collection`, `EOF`, and `CRC`, the documents, and the same
//! terminator). The CRC is not checked. A `timeseries` collection is
//! refused. Names starting with `system.` are skipped.

use std::fs;
use std::path::Path;

use bson::{Bson, Document};

use super::{check_name, CollectionDump, Dump, ViewDump};

const ARCHIVE_MAGIC: u32 = 0x8199_e26d;
const TERMINATOR: u32 = 0xffff_ffff;

pub(super) fn is_archive(bytes: &[u8]) -> bool {
    bytes.len() >= 4
        && u32::from_le_bytes(bytes[..4].try_into().expect("4 magic bytes")) == ARCHIVE_MAGIC
}

pub(super) fn read_dir(path: &Path, database: &str) -> Result<Dump, String> {
    let folder = database_folder(path, database)?;
    let mut names = std::collections::BTreeSet::new();
    for entry in
        fs::read_dir(&folder).map_err(|err| format!("cannot read {}: {err}", folder.display()))?
    {
        let entry = entry.map_err(|err| format!("cannot read {}: {err}", folder.display()))?;
        let file_name = entry.file_name();
        let file_name = file_name.to_string_lossy();
        if let Some(stem) = file_name.strip_suffix(".metadata.json") {
            names.insert(percent_decode(stem));
        } else if let Some(stem) = file_name.strip_suffix(".bson") {
            names.insert(percent_decode(stem));
        }
    }
    let mut dump = Dump::default();
    for name in names {
        if name.starts_with("system.") {
            continue;
        }
        check_name(&name)?;
        let bson_path = folder.join(format!("{name}.bson"));
        let meta_path = folder.join(format!("{name}.metadata.json"));
        let documents = if bson_path.is_file() {
            let bytes = fs::read(&bson_path)
                .map_err(|err| format!("cannot read {}: {err}", bson_path.display()))?;
            read_bson_stream(&bytes)?
        } else {
            Vec::new()
        };
        let meta = if meta_path.is_file() {
            fs::read_to_string(&meta_path)
                .map_err(|err| format!("cannot read {}: {err}", meta_path.display()))?
        } else {
            String::new()
        };
        apply_metadata(&mut dump, &name, &meta, documents)?;
    }
    Ok(dump)
}

pub(super) fn read_archive(bytes: &[u8]) -> Result<Dump, String> {
    if !is_archive(bytes) {
        return Err("file is not a mongodump archive".into());
    }
    let mut cursor = Cursor { bytes, index: 4 };
    let header = cursor
        .read_document()?
        .ok_or_else(|| "mongodump archive ends before its header".to_string())?;
    match header.get_str("version") {
        Ok("0.1") => {}
        Ok(other) => return Err(format!("mongodump archive version {other} is not 0.1")),
        Err(_) => return Err("mongodump archive header has no version".into()),
    }
    let mut metas = Vec::new();
    loop {
        match cursor.read_document_or_terminator()? {
            None => break,
            Some(meta) => metas.push(meta),
        }
    }
    let mut bodies: Vec<((String, String), Vec<Document>)> = Vec::new();
    loop {
        let Some(header) = cursor.read_document()? else {
            break;
        };
        let db = header.get_str("db").unwrap_or("").to_string();
        let collection = header.get_str("collection").unwrap_or("").to_string();
        let finished = header.get_bool("EOF").unwrap_or(false);
        let mut documents = Vec::new();
        loop {
            match cursor.read_document_or_terminator()? {
                None => break,
                Some(document) => documents.push(document),
            }
        }
        if finished || collection.is_empty() {
            continue;
        }
        if let Some((_, existing)) = bodies
            .iter_mut()
            .find(|((seen_db, seen), _)| seen_db == &db && seen == &collection)
        {
            existing.extend(documents);
        } else {
            bodies.push(((db, collection), documents));
        }
    }
    let mut dump = Dump::default();
    let mut seen = std::collections::BTreeSet::new();
    for meta in &metas {
        let name = meta.get_str("collection").unwrap_or("").to_string();
        let db = meta.get_str("db").unwrap_or("").to_string();
        if name.is_empty() || name.starts_with("system.") {
            continue;
        }
        if meta.get_str("type") == Ok("timeseries") {
            return Err(format!("timeseries collection {name} is not restored"));
        }
        check_name(&name)?;
        let documents = bodies
            .iter()
            .find(|((seen_db, seen), _)| seen_db == &db && seen == &name)
            .map(|(_, documents)| documents.clone())
            .unwrap_or_default();
        let metadata = meta.get_str("metadata").unwrap_or("");
        apply_metadata(&mut dump, &name, metadata, documents)?;
        seen.insert((db, name));
    }
    for ((_, name), documents) in bodies {
        if name.starts_with("system.") || seen.iter().any(|(_, seen)| seen == &name) {
            continue;
        }
        check_name(&name)?;
        apply_metadata(&mut dump, &name, "", documents)?;
    }
    Ok(dump)
}

fn database_folder(path: &Path, database: &str) -> Result<std::path::PathBuf, String> {
    if folder_has_bson(path) {
        return Ok(path.to_path_buf());
    }
    let mut children = Vec::new();
    for entry in
        fs::read_dir(path).map_err(|err| format!("cannot read {}: {err}", path.display()))?
    {
        let entry = entry.map_err(|err| format!("cannot read {}: {err}", path.display()))?;
        let child = entry.path();
        if child.is_dir() && folder_has_bson(&child) {
            children.push(child);
        }
    }
    if let Some(matched) = children
        .iter()
        .find(|child| child.file_name().and_then(|name| name.to_str()) == Some(database))
    {
        return Ok(matched.clone());
    }
    if children.len() == 1 {
        return Ok(children.remove(0));
    }
    if children.is_empty() {
        return Err(format!("{} has no <collection>.bson files", path.display()));
    }
    let names = children
        .iter()
        .filter_map(|child| child.file_name().and_then(|name| name.to_str()))
        .collect::<Vec<_>>()
        .join(", ");
    Err(format!(
        "{database} is not a database directory in {} ({names})",
        path.display()
    ))
}

fn folder_has_bson(path: &Path) -> bool {
    fs::read_dir(path)
        .ok()
        .into_iter()
        .flatten()
        .flatten()
        .any(|entry| entry.file_name().to_string_lossy().ends_with(".bson"))
}

fn apply_metadata(
    dump: &mut Dump,
    name: &str,
    metadata: &str,
    documents: Vec<Document>,
) -> Result<(), String> {
    let trimmed = metadata.trim();
    if trimmed.is_empty() {
        dump.collections.push(CollectionDump {
            name: name.to_string(),
            documents,
            indexes: Vec::new(),
        });
        return Ok(());
    }
    let value: serde_json::Value = serde_json::from_str(trimmed)
        .map_err(|err| format!("metadata for {name} is not JSON: {err}"))?;
    let is_view = value.get("type").and_then(|kind| kind.as_str()) == Some("view")
        || value
            .pointer("/options/viewOn")
            .and_then(|view_on| view_on.as_str())
            .is_some();
    if is_view {
        let view_on = value
            .pointer("/options/viewOn")
            .and_then(|view_on| view_on.as_str())
            .ok_or_else(|| format!("view {name} has no viewOn"))?;
        let view_on = view_on.rsplit('.').next().unwrap_or(view_on).to_string();
        check_name(&view_on)?;
        let pipeline = match value.pointer("/options/pipeline") {
            None | Some(serde_json::Value::Null) => Vec::new(),
            Some(stages) => {
                json_to_docs(stages).map_err(|err| format!("view {name} pipeline: {err}"))?
            }
        };
        dump.views.push(ViewDump {
            name: name.to_string(),
            view_on,
            pipeline,
            kova: Document::new(),
        });
        return Ok(());
    }
    let indexes = match value.get("indexes") {
        None | Some(serde_json::Value::Null) => Vec::new(),
        Some(indexes) => json_to_docs(indexes)
            .map_err(|err| format!("indexes for {name}: {err}"))?
            .into_iter()
            .filter(|spec| spec.get_str("name") != Ok("_id_"))
            .collect(),
    };
    dump.collections.push(CollectionDump {
        name: name.to_string(),
        documents,
        indexes,
    });
    Ok(())
}

fn json_to_docs(value: &serde_json::Value) -> Result<Vec<Document>, String> {
    let serde_json::Value::Array(items) = value else {
        return Err("expected a JSON array".into());
    };
    items
        .iter()
        .map(|item| match json_to_bson(item)? {
            Bson::Document(document) => Ok(document),
            _ => Err("expected a JSON object".into()),
        })
        .collect()
}

fn json_to_bson(value: &serde_json::Value) -> Result<Bson, String> {
    match value {
        serde_json::Value::Null => Ok(Bson::Null),
        serde_json::Value::Bool(value) => Ok(Bson::Boolean(*value)),
        serde_json::Value::Number(number) => {
            if let Some(value) = number.as_i64() {
                if (i64::from(i32::MIN)..=i64::from(i32::MAX)).contains(&value) {
                    Ok(Bson::Int32(value as i32))
                } else {
                    Ok(Bson::Int64(value))
                }
            } else if let Some(value) = number.as_f64() {
                Ok(Bson::Double(value))
            } else {
                Err(format!("number {number} is out of range"))
            }
        }
        serde_json::Value::String(value) => Ok(Bson::String(value.clone())),
        serde_json::Value::Array(items) => Ok(Bson::Array(
            items
                .iter()
                .map(json_to_bson)
                .collect::<Result<Vec<_>, _>>()?,
        )),
        serde_json::Value::Object(fields) => {
            let mut document = Document::new();
            for (key, value) in fields {
                document.insert(key, json_to_bson(value)?);
            }
            Ok(Bson::Document(document))
        }
    }
}

fn read_bson_stream(bytes: &[u8]) -> Result<Vec<Document>, String> {
    let mut cursor = Cursor { bytes, index: 0 };
    let mut documents = Vec::new();
    while cursor.index < cursor.bytes.len() {
        let Some(document) = cursor.read_document()? else {
            break;
        };
        documents.push(document);
    }
    Ok(documents)
}

fn percent_decode(text: &str) -> String {
    if !text.contains('%') {
        return text.to_string();
    }
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            if let Ok(byte) = u8::from_str_radix(
                std::str::from_utf8(&bytes[index + 1..index + 3]).unwrap_or(""),
                16,
            ) {
                out.push(byte);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8(out).unwrap_or_else(|_| text.to_string())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    index: usize,
}

impl Cursor<'_> {
    fn read_document(&mut self) -> Result<Option<Document>, String> {
        if self.index == self.bytes.len() {
            return Ok(None);
        }
        self.read_framed(false)
    }

    fn read_document_or_terminator(&mut self) -> Result<Option<Document>, String> {
        self.read_framed(true)
    }

    fn read_framed(&mut self, terminator_ends: bool) -> Result<Option<Document>, String> {
        if self.bytes.len() - self.index < 4 {
            return Err("truncated mongodump archive".into());
        }
        let len = i32::from_le_bytes(
            self.bytes[self.index..self.index + 4]
                .try_into()
                .expect("4 length bytes"),
        );
        if terminator_ends && len as u32 == TERMINATOR {
            self.index += 4;
            return Ok(None);
        }
        let len = usize::try_from(len).map_err(|_| format!("BSON length {len} is negative"))?;
        if len < 5 || self.index + len > self.bytes.len() {
            return Err(format!("BSON document length {len} is out of range"));
        }
        let frame = &self.bytes[self.index..self.index + len];
        let document = Document::from_reader(&mut &frame[..])
            .map_err(|err| format!("mongodump BSON: {err}"))?;
        self.index += len;
        Ok(Some(document))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn directory_dump_reads_bson_and_metadata() {
        let dir = std::env::temp_dir().join(format!("kova-mongodir-{}", std::process::id()));
        let db = dir.join("shop");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&db).expect("dir");
        let document = doc! { "ename": "SMITH", "sal": 800i32 };
        fs::write(db.join("emp.bson"), bson::to_vec(&document).expect("bson")).expect("bson");
        fs::write(
            db.join("emp.metadata.json"),
            r#"{"indexes":[{"v":2,"key":{"_id":1},"name":"_id_"},{"v":2,"key":{"ename":1},"name":"ename_1","unique":true}]}"#,
        )
        .expect("meta");
        fs::write(db.join("well_paid.metadata.json"), r#"{"type":"view","options":{"viewOn":"emp","pipeline":[{"$match":{"sal":{"$gt":2000}}}]}}"#).expect("view");
        let dump = read_dir(&dir, "shop").expect("read");
        let _ = fs::remove_dir_all(&dir);
        assert_eq!(dump.collections.len(), 1);
        assert_eq!(
            dump.collections[0].documents[0].get_str("ename"),
            Ok("SMITH")
        );
        assert_eq!(dump.collections[0].indexes.len(), 1);
        assert_eq!(
            dump.collections[0].indexes[0].get_str("name"),
            Ok("ename_1")
        );
        assert_eq!(dump.views[0].name, "well_paid");
        assert_eq!(dump.views[0].view_on, "emp");
    }

    #[test]
    fn archive_reads_prelude_then_namespace_blocks() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&ARCHIVE_MAGIC.to_le_bytes());
        push(
            &mut bytes,
            &doc! { "version": "0.1", "concurrent_collections": 1i32, "server_version": "kova", "tool_version": "kova" },
        );
        push(
            &mut bytes,
            &doc! {
                "db": "shop",
                "collection": "emp",
                "metadata": r#"{"indexes":[{"v":2,"key":{"ename":1},"name":"ename_1"}]}"#,
                "size": 0i32,
                "type": "collection",
            },
        );
        bytes.extend_from_slice(&TERMINATOR.to_le_bytes());
        push(
            &mut bytes,
            &doc! { "db": "shop", "collection": "emp", "EOF": false, "CRC": 0i64 },
        );
        push(&mut bytes, &doc! { "ename": "ALLEN", "sal": 1600i32 });
        bytes.extend_from_slice(&TERMINATOR.to_le_bytes());
        push(
            &mut bytes,
            &doc! { "db": "shop", "collection": "emp", "EOF": true, "CRC": 0i64 },
        );
        bytes.extend_from_slice(&TERMINATOR.to_le_bytes());
        let dump = read_archive(&bytes).expect("archive");
        assert_eq!(dump.collections[0].name, "emp");
        assert_eq!(
            dump.collections[0].documents[0].get_str("ename"),
            Ok("ALLEN")
        );
        assert_eq!(
            dump.collections[0].indexes[0].get_str("name"),
            Ok("ename_1")
        );
    }

    fn push(bytes: &mut Vec<u8>, document: &Document) {
        bytes.extend(bson::to_vec(document).expect("bson"));
    }
}
