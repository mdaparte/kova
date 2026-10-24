//! Kova dump 1.
//!
//! A file begins with the eight bytes `KOVADMP1` and then a sequence of
//! BSON documents. Each document has a string `kind`:
//!
//! - `collection` names the collection that the following `document` and
//!   `index` records belong to.
//! - `document` carries one stored document in `doc`.
//! - `index` carries one `createIndexes` spec in `spec`. The `_id_` index
//!   is not written; the server adds it.
//! - `view` carries `name`, `viewOn`, `pipeline`, and `kova`.

use std::fs::File;
use std::io::Write;
use std::path::Path;

use bson::{doc, Bson, Document};

use super::{CollectionDump, Dump, ViewDump};

pub(super) const MAGIC: &[u8] = b"KOVADMP1";

pub(super) fn write(path: &Path, dump: &Dump) -> Result<(), String> {
    let file_name = path
        .file_name()
        .ok_or_else(|| format!("{} is not a file path", path.display()))?;
    let mut partial_name = file_name.to_os_string();
    partial_name.push(".partial");
    let partial = path.with_file_name(partial_name);
    let write_result = (|| {
        let mut file = File::create(&partial)
            .map_err(|err| format!("cannot create {}: {err}", partial.display()))?;
        file.write_all(MAGIC)
            .map_err(|err| format!("cannot write {}: {err}", partial.display()))?;
        for collection in &dump.collections {
            write_doc(
                &mut file,
                &doc! { "kind": "collection", "name": &collection.name },
            )?;
            for document in &collection.documents {
                write_doc(
                    &mut file,
                    &doc! { "kind": "document", "doc": document.clone() },
                )?;
            }
            for spec in &collection.indexes {
                write_doc(&mut file, &doc! { "kind": "index", "spec": spec.clone() })?;
            }
        }
        for view in &dump.views {
            write_doc(
                &mut file,
                &doc! {
                    "kind": "view",
                    "name": &view.name,
                    "viewOn": &view.view_on,
                    "pipeline": view.pipeline.iter().cloned().map(Bson::Document).collect::<Vec<_>>(),
                    "kova": view.kova.clone(),
                },
            )?;
        }
        file.sync_all()
            .map_err(|err| format!("cannot finish {}: {err}", partial.display()))?;
        Ok(())
    })();
    if let Err(err) = write_result {
        let _ = std::fs::remove_file(&partial);
        return Err(err);
    }
    std::fs::rename(&partial, path).map_err(|err| {
        let _ = std::fs::remove_file(&partial);
        format!("cannot replace {} with the dump: {err}", path.display())
    })
}

pub(super) fn read_bytes(bytes: &[u8]) -> Result<Dump, String> {
    if !bytes.starts_with(MAGIC) {
        return Err("file is not a Kova dump".into());
    }
    let mut rest = &bytes[MAGIC.len()..];
    let mut dump = Dump::default();
    let mut current: Option<String> = None;
    while let Some(record) = read_doc(&mut rest)? {
        let kind = record
            .get_str("kind")
            .map_err(|_| "Kova dump record has no kind".to_string())?;
        match kind {
            "collection" => {
                let name = record
                    .get_str("name")
                    .map_err(|_| "collection record has no name".to_string())?
                    .to_string();
                super::check_name(&name)?;
                if !dump
                    .collections
                    .iter()
                    .any(|collection| collection.name == name)
                {
                    dump.collections.push(CollectionDump {
                        name: name.clone(),
                        documents: Vec::new(),
                        indexes: Vec::new(),
                    });
                }
                current = Some(name);
            }
            "document" => {
                let name = current
                    .clone()
                    .ok_or_else(|| "document record before its collection".to_string())?;
                let document = record
                    .get_document("doc")
                    .map_err(|_| format!("document record for {name} has no doc"))?
                    .clone();
                collection_mut(&mut dump, &name).documents.push(document);
            }
            "index" => {
                let name = current
                    .clone()
                    .ok_or_else(|| "index record before its collection".to_string())?;
                let spec = record
                    .get_document("spec")
                    .map_err(|_| format!("index record for {name} has no spec"))?
                    .clone();
                if spec.get_str("name") != Ok("_id_") {
                    collection_mut(&mut dump, &name).indexes.push(spec);
                }
            }
            "view" => {
                let name = record
                    .get_str("name")
                    .map_err(|_| "view record has no name".to_string())?
                    .to_string();
                super::check_name(&name)?;
                let view_on = record
                    .get_str("viewOn")
                    .map_err(|_| format!("view {name} has no viewOn"))?
                    .to_string();
                super::check_name(&view_on)?;
                let pipeline = match record.get("pipeline") {
                    None => Vec::new(),
                    Some(Bson::Array(values)) => values
                        .iter()
                        .map(|value| {
                            value.as_document().cloned().ok_or_else(|| {
                                format!("view {name} pipeline stage is not a document")
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    Some(_) => return Err(format!("view {name} pipeline is not an array")),
                };
                let kova = record.get_document("kova").cloned().unwrap_or_default();
                dump.views.push(ViewDump {
                    name,
                    view_on,
                    pipeline,
                    kova,
                });
            }
            other => return Err(format!("unknown Kova dump record {other}")),
        }
    }
    if !rest.is_empty() {
        return Err("Kova dump has trailing bytes".into());
    }
    Ok(dump)
}

fn collection_mut<'a>(dump: &'a mut Dump, name: &str) -> &'a mut CollectionDump {
    let position = dump
        .collections
        .iter()
        .position(|collection| collection.name == name)
        .expect("collection header was recorded");
    &mut dump.collections[position]
}

fn write_doc(file: &mut File, document: &Document) -> Result<(), String> {
    let bytes = bson::to_vec(document).map_err(|err| format!("bson encode: {err}"))?;
    file.write_all(&bytes)
        .map_err(|err| format!("cannot write dump: {err}"))
}

fn read_doc(bytes: &mut &[u8]) -> Result<Option<Document>, String> {
    if bytes.is_empty() {
        return Ok(None);
    }
    if bytes.len() < 4 {
        return Err("truncated BSON document in Kova dump".into());
    }
    let len = i32::from_le_bytes(bytes[..4].try_into().expect("4 length bytes")) as usize;
    if len < 5 || len > bytes.len() {
        return Err(format!("BSON document length {len} is out of range"));
    }
    let (head, tail) = bytes.split_at(len);
    let document =
        Document::from_reader(&mut &head[..]).map_err(|err| format!("Kova dump BSON: {err}"))?;
    *bytes = tail;
    Ok(Some(document))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kova_dump_round_trips_documents_indexes_and_views() {
        let dump = Dump {
            collections: vec![CollectionDump {
                name: "emp".into(),
                documents: vec![doc! { "ename": "SMITH", "sal": 800i32 }],
                indexes: vec![doc! { "key": { "ename": 1i32 }, "name": "ename_1", "unique": true }],
            }],
            views: vec![ViewDump {
                name: "well_paid".into(),
                view_on: "emp".into(),
                pipeline: vec![doc! { "$match": { "sal": { "$gt": 2000i32 } } }],
                kova: doc! { "inverted": true },
            }],
        };
        let dir = std::env::temp_dir().join(format!("kova-dump-fmt-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        let path = dir.join("shop.kdmp");
        write(&path, &dump).expect("write");
        let bytes = std::fs::read(&path).expect("read");
        let loaded = read_bytes(&bytes).expect("parse");
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(loaded.collections.len(), 1);
        assert_eq!(
            loaded.collections[0].documents[0].get_str("ename"),
            Ok("SMITH")
        );
        assert_eq!(
            loaded.collections[0].indexes[0].get_str("name"),
            Ok("ename_1")
        );
        assert_eq!(loaded.views[0].name, "well_paid");
        assert_eq!(loaded.views[0].view_on, "emp");
        assert_eq!(loaded.views[0].kova.get_bool("inverted"), Ok(true));
    }
}
