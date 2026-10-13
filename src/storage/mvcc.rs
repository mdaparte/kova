//! Undo images for snapshot reads.
//!
//! The page file keeps the latest committed document. This store keeps
//! only the older images an open snapshot might still need. When the
//! oldest snapshot moves past an image, garbage collection drops it.
//! Restart throws the store away: no transaction is still open, and the
//! pages are already the committed database.

use std::collections::HashMap;

use bson::{Bson, Document};

/// One committed image that a newer commit replaced.
struct Undo {
    /// First LSN at which this image was the committed one.
    from_lsn: u64,
    /// LSN of the commit that replaced it. Visible while
    /// `from_lsn <= snapshot < until_lsn`.
    until_lsn: u64,
    /// `None` means the document did not exist in that interval.
    document: Option<Document>,
}

struct Head {
    /// LSN of the commit that wrote the page's current image.
    commit_lsn: u64,
    /// The page no longer holds the document.
    deleted: bool,
    undo: Vec<Undo>,
}

/// Per-document version chains, keyed by namespace then `_id`.
pub(super) struct VersionStore {
    chains: HashMap<String, HashMap<String, Head>>,
}

impl VersionStore {
    pub(super) fn new() -> Self {
        Self {
            chains: HashMap::new(),
        }
    }

    /// LSN of the latest committed image, or 0 when this store has not
    /// recorded the document yet (the page image is then visible to
    /// every snapshot).
    pub(super) fn commit_lsn(&self, namespace: &str, id: &str) -> u64 {
        self.head(namespace, id)
            .map(|head| head.commit_lsn)
            .unwrap_or(0)
    }

    /// LSN of the image a snapshot actually sees. Conflict checks
    /// compare this with [`Self::commit_lsn`] at commit time.
    pub(super) fn visible_lsn(&self, namespace: &str, id: &str, snapshot: u64) -> u64 {
        let Some(head) = self.head(namespace, id) else {
            return 0;
        };
        if snapshot >= head.commit_lsn {
            return head.commit_lsn;
        }
        head.undo
            .iter()
            .rev()
            .find(|undo| undo.from_lsn <= snapshot && snapshot < undo.until_lsn)
            .map(|undo| undo.from_lsn)
            .unwrap_or(0)
    }

    /// Records a commit that replaced `previous`.
    ///
    /// `previous == None` means the document did not exist. `deleted`
    /// means this commit removes it, so later snapshots see no page.
    pub(super) fn note(
        &mut self,
        namespace: &str,
        id: &str,
        lsn: u64,
        previous: Option<Document>,
        deleted: bool,
    ) {
        let from_lsn = self.commit_lsn(namespace, id);
        let head = self
            .chains
            .entry(namespace.to_string())
            .or_default()
            .entry(id.to_string())
            .or_insert(Head {
                commit_lsn: 0,
                deleted: false,
                undo: Vec::new(),
            });
        head.undo.push(Undo {
            from_lsn,
            until_lsn: lsn,
            document: previous,
        });
        head.commit_lsn = lsn;
        head.deleted = deleted;
    }

    /// The document `snapshot` is allowed to see.
    ///
    /// `page` is the body currently stored, when the scan found one.
    /// A chain that this store does not know is visible as that page:
    /// it was committed before any snapshot this process handed out.
    pub(super) fn visible(
        &self,
        namespace: &str,
        id: &str,
        snapshot: u64,
        page: Option<&Document>,
    ) -> Option<Document> {
        let Some(head) = self.head(namespace, id) else {
            return page.cloned();
        };
        if snapshot >= head.commit_lsn {
            if head.deleted {
                None
            } else {
                page.cloned()
            }
        } else {
            head.undo
                .iter()
                .rev()
                .find(|undo| undo.from_lsn <= snapshot && snapshot < undo.until_lsn)
                .and_then(|undo| undo.document.clone())
        }
    }

    /// Documents a snapshot still sees even though the page no longer
    /// holds them (they were deleted after the snapshot).
    pub(super) fn ghosts(&self, namespace: &str, snapshot: u64) -> Vec<Document> {
        let Some(chains) = self.chains.get(namespace) else {
            return Vec::new();
        };
        let mut documents = Vec::new();
        for head in chains.values() {
            if !head.deleted || snapshot >= head.commit_lsn {
                continue;
            }
            if let Some(document) = head
                .undo
                .iter()
                .rev()
                .find(|undo| undo.from_lsn <= snapshot && snapshot < undo.until_lsn)
                .and_then(|undo| undo.document.clone())
            {
                documents.push(document);
            }
        }
        documents
    }

    /// Drops images every remaining snapshot is past.
    ///
    /// `oldest` is the smallest snapshot LSN still held. `u64::MAX`
    /// means no snapshot is open, so every chain can go.
    pub(super) fn gc(&mut self, oldest: u64) {
        self.chains.retain(|_, chains| {
            chains.retain(|_, head| {
                head.undo.retain(|undo| oldest < undo.until_lsn);
                oldest < head.commit_lsn || !head.undo.is_empty()
            });
            !chains.is_empty()
        });
    }

    fn head(&self, namespace: &str, id: &str) -> Option<&Head> {
        self.chains.get(namespace)?.get(id)
    }
}

/// Checks a document against a collection validator.
///
/// The document is a small subset of MongoDB's `$jsonSchema`: `required`
/// lists field names, and `properties.<field>.bsonType` names the BSON
/// type. Anything else in the validator is ignored.
pub(super) fn validate_document(document: &Document, schema: &Document) -> Result<(), String> {
    if let Ok(required) = schema.get_array("required") {
        for item in required {
            let name = item
                .as_str()
                .ok_or_else(|| "required entries must be strings".to_string())?;
            if document.get(name).is_none() {
                return Err(format!("document is missing required field '{name}'"));
            }
        }
    }
    if let Ok(properties) = schema.get_document("properties") {
        for (name, spec) in properties {
            let Some(spec) = spec.as_document() else {
                continue;
            };
            let Some(expected) = spec.get_str("bsonType").ok() else {
                continue;
            };
            let Some(value) = document.get(name) else {
                continue;
            };
            if !type_matches(value, expected) {
                return Err(format!("field '{name}' must be {expected}"));
            }
        }
    }
    Ok(())
}

fn type_matches(value: &Bson, expected: &str) -> bool {
    match expected {
        "string" => matches!(value, Bson::String(_)),
        "int" => matches!(value, Bson::Int32(_)),
        "long" => matches!(value, Bson::Int64(_)),
        "double" => matches!(value, Bson::Double(_)),
        "number" => matches!(value, Bson::Int32(_) | Bson::Int64(_) | Bson::Double(_)),
        "bool" => matches!(value, Bson::Boolean(_)),
        "object" => matches!(value, Bson::Document(_)),
        "array" => matches!(value, Bson::Array(_)),
        "date" => matches!(value, Bson::DateTime(_)),
        "objectId" => matches!(value, Bson::ObjectId(_)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn snapshot_sees_the_image_that_was_current() {
        let mut versions = VersionStore::new();
        let first = doc! { "_id": 1, "sal": 800 };
        versions.note("db.emp", "1", 1, None, false);
        assert!(versions.visible("db.emp", "1", 0, Some(&first)).is_none());
        assert_eq!(
            versions
                .visible("db.emp", "1", 1, Some(&first))
                .unwrap()
                .get_i32("sal")
                .unwrap(),
            800
        );

        let second = doc! { "_id": 1, "sal": 900 };
        versions.note("db.emp", "1", 2, Some(first.clone()), false);
        assert_eq!(
            versions
                .visible("db.emp", "1", 1, Some(&second))
                .unwrap()
                .get_i32("sal")
                .unwrap(),
            800
        );
        assert_eq!(
            versions
                .visible("db.emp", "1", 2, Some(&second))
                .unwrap()
                .get_i32("sal")
                .unwrap(),
            900
        );
        assert_eq!(versions.visible_lsn("db.emp", "1", 1), 1);
        assert_eq!(versions.commit_lsn("db.emp", "1"), 2);
    }

    #[test]
    fn a_delete_stays_visible_to_an_older_snapshot() {
        let mut versions = VersionStore::new();
        let body = doc! { "_id": 1, "sal": 800 };
        versions.note("db.emp", "1", 1, None, false);
        versions.note("db.emp", "1", 2, Some(body), true);
        assert!(versions.visible("db.emp", "1", 2, None).is_none());
        let ghosts = versions.ghosts("db.emp", 1);
        assert_eq!(ghosts.len(), 1);
        assert_eq!(ghosts[0].get_i32("sal").unwrap(), 800);
    }

    #[test]
    fn gc_drops_images_every_snapshot_is_past() {
        let mut versions = VersionStore::new();
        versions.note("db.emp", "1", 1, None, false);
        versions.note("db.emp", "1", 2, Some(doc! { "_id": 1 }), false);
        versions.gc(2);
        assert!(versions.head("db.emp", "1").is_none());
        let page = doc! { "_id": 1, "sal": 900 };
        assert_eq!(
            versions
                .visible("db.emp", "1", 2, Some(&page))
                .unwrap()
                .get_i32("sal")
                .unwrap(),
            900
        );
    }

    #[test]
    fn validator_requires_fields_and_types() {
        let schema = doc! {
            "required": ["ename"],
            "properties": { "sal": { "bsonType": "int" } },
        };
        let missing = validate_document(&doc! { "sal": 1 }, &schema).unwrap_err();
        assert!(missing.contains("ename"), "{missing}");
        let wrong = validate_document(&doc! { "ename": "KING", "sal": "x" }, &schema).unwrap_err();
        assert!(wrong.contains("sal"), "{wrong}");
        assert!(validate_document(&doc! { "ename": "KING", "sal": 5000 }, &schema).is_ok());
    }
}
