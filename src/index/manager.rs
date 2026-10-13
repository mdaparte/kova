//! Catalog of B-tree, hash, and the mandatory `_id` index.
//!
//! `createIndexes` records the spec and builds the structure from the
//! documents already on pages. Later writes update the structure. A
//! unique index rejects a second document with the same key. A sparse
//! index skips a document that does not have every indexed field.

use std::collections::HashMap;

use bson::{Bson, Document};

use super::btree::{ordered_number, BTreeIndex, Component, End};
use super::hash_index::HashIndex;

/// How a find or count should read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Access {
    /// The filter is only an equality on `_id`.
    Id { id: String },
    /// A secondary index answered the predicate. `ids` may still need
    /// the rest of the filter applied to the fetched documents.
    Index { name: String, ids: Vec<String> },
    /// No index covers this filter.
    Collection,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    BTree,
    Hash,
}

#[derive(Clone)]
struct Spec {
    name: String,
    fields: Vec<String>,
    kind: Kind,
    unique: bool,
    sparse: bool,
    btree: BTreeIndex,
    hash: HashIndex,
}

/// Every secondary index on this partition, plus `_id` for each collection
/// that has been written or declared.
#[derive(Clone)]
pub struct IndexManager {
    by_collection: HashMap<String, Vec<Spec>>,
}

impl IndexManager {
    pub fn new() -> Self {
        Self {
            by_collection: HashMap::new(),
        }
    }

    /// Replaces the secondary indexes of one collection. The `_id` index
    /// stays. Call [`Self::rebuild`] with the current documents afterward.
    pub fn declare(&mut self, collection: &str, specs: &[Document]) -> Result<(), String> {
        let mut secondary = Vec::new();
        for spec in specs {
            secondary.push(parse_spec(spec)?);
        }
        let mut all = vec![self.take_id(collection).unwrap_or_else(id_spec)];
        all.extend(secondary);
        self.by_collection.insert(collection.to_string(), all);
        Ok(())
    }

    pub fn drop_index(&mut self, collection: &str, name: &str) -> Result<(), String> {
        if name == "_id_" {
            return Err("cannot drop the _id index".to_string());
        }
        self.ensure(collection);
        let indexes = self
            .by_collection
            .get_mut(collection)
            .expect("collection was just ensured");
        let before = indexes.len();
        if name == "*" {
            indexes.retain(|index| index.name == "_id_");
            return Ok(());
        }
        indexes.retain(|index| index.name != name);
        if indexes.len() == before {
            return Err(format!("index not found: {name}"));
        }
        Ok(())
    }

    pub fn drop_collection(&mut self, collection: &str) {
        self.by_collection.remove(collection);
    }

    /// Drops every index of collections that belong to `database`.
    pub fn drop_database(&mut self, database: &str) {
        let prefix = format!("{database}.");
        self.by_collection.retain(|name, _| {
            let owned = name.starts_with(&prefix) || (database == "test" && !name.contains('.'));
            !owned
        });
    }

    /// Tries a replacement on a copy. The live indexes stay as they are
    /// when a unique key would collide, including inside the same batch.
    pub fn rehearse(
        &self,
        collection: &str,
        changes: &[(String, Option<Document>, Option<Document>)],
    ) -> Result<(), String> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut fresh = self
            .by_collection
            .get(collection)
            .cloned()
            .unwrap_or_else(|| vec![id_spec()]);
        for (id, previous, after) in changes {
            if let Some(previous) = previous {
                remove_all(&mut fresh, id, previous);
            }
            if let Some(after) = after {
                add_all(&mut fresh, id, after)?;
            }
        }
        Ok(())
    }

    /// Builds `specs` against `documents` without publishing them.
    /// `createIndexes` calls this before the WAL so a unique violation
    /// does not leave a declaration that recovery cannot apply.
    pub fn preview(
        &self,
        collection: &str,
        specs: &[Document],
        documents: &[(String, Document)],
    ) -> Result<(), String> {
        let mut trial = self.clone();
        trial.declare(collection, specs)?;
        trial.rebuild(collection, documents)
    }

    /// Inserts every document. A unique violation leaves the previous
    /// contents of this collection's indexes in place.
    pub fn rebuild(
        &mut self,
        collection: &str,
        documents: &[(String, Document)],
    ) -> Result<(), String> {
        self.ensure(collection);
        let mut fresh = self.by_collection[collection].clone();
        for index in &mut fresh {
            index.btree = BTreeIndex::new();
            index.hash = HashIndex::new();
        }
        for (id, document) in documents {
            add_all(&mut fresh, id, document)?;
        }
        self.by_collection.insert(collection.to_string(), fresh);
        Ok(())
    }

    pub fn insert(
        &mut self,
        collection: &str,
        id: &str,
        document: &Document,
    ) -> Result<(), String> {
        self.ensure(collection);
        let indexes = self.by_collection.get_mut(collection).expect("ensured");
        add_all(indexes, id, document)
    }

    pub fn remove(&mut self, collection: &str, id: &str, document: &Document) {
        let Some(indexes) = self.by_collection.get_mut(collection) else {
            return;
        };
        remove_all(indexes, id, document);
    }

    /// Picks an access path. An empty `ids` list is a real answer: the
    /// index knows nothing matches.
    pub fn access(&self, collection: &str, filter: &Document) -> Access {
        if let Some(id) = id_equality(filter) {
            return Access::Id { id };
        }
        let Some(indexes) = self.by_collection.get(collection) else {
            return Access::Collection;
        };
        let mut best: Option<(usize, usize, Access)> = None;
        for (position, index) in indexes.iter().enumerate() {
            if index.name == "_id_" {
                continue;
            }
            let Some((score, ids)) = probe(index, filter) else {
                continue;
            };
            let candidate = Access::Index {
                name: index.name.clone(),
                ids,
            };
            let replace = match &best {
                None => true,
                Some((best_score, best_pos, _)) => {
                    score > *best_score || (score == *best_score && position < *best_pos)
                }
            };
            if replace {
                best = Some((score, position, candidate));
            }
        }
        best.map(|(_, _, access)| access)
            .unwrap_or(Access::Collection)
    }

    fn ensure(&mut self, collection: &str) {
        self.by_collection
            .entry(collection.to_string())
            .or_insert_with(|| vec![id_spec()]);
    }

    fn take_id(&mut self, collection: &str) -> Option<Spec> {
        let indexes = self.by_collection.get_mut(collection)?;
        let position = indexes.iter().position(|index| index.name == "_id_")?;
        Some(indexes.remove(position))
    }
}

fn id_spec() -> Spec {
    Spec {
        name: "_id_".to_string(),
        fields: vec!["_id".to_string()],
        kind: Kind::BTree,
        unique: true,
        sparse: false,
        btree: BTreeIndex::new(),
        hash: HashIndex::new(),
    }
}

fn parse_spec(spec: &Document) -> Result<Spec, String> {
    let name = spec
        .get_str("name")
        .map_err(|_| "index requires a name".to_string())?
        .to_string();
    let key = spec
        .get_document("key")
        .map_err(|_| format!("index '{name}' requires a key"))?;
    if key.is_empty() {
        return Err(format!("index '{name}' has an empty key"));
    }
    let mut fields = Vec::new();
    let mut hashed = false;
    for (field, direction) in key {
        if field == "_fts" || direction.as_str() == Some("text") {
            return Err("text indexes stay on the full-text index".to_string());
        }
        if direction.as_str() == Some("hashed") {
            hashed = true;
        } else if !is_direction(direction) {
            return Err(format!("index '{name}' has an unsupported key direction"));
        }
        fields.push(field.clone());
    }
    if hashed && fields.len() != 1 {
        return Err(format!("hashed index '{name}' supports one field"));
    }
    Ok(Spec {
        name,
        fields,
        kind: if hashed { Kind::Hash } else { Kind::BTree },
        unique: spec.get_bool("unique").unwrap_or(false),
        sparse: spec.get_bool("sparse").unwrap_or(false),
        btree: BTreeIndex::new(),
        hash: HashIndex::new(),
    })
}

fn remove_all(indexes: &mut [Spec], id: &str, document: &Document) {
    for index in indexes {
        let Some(key) = index_key(index, document) else {
            continue;
        };
        match index.kind {
            Kind::BTree => index.btree.remove(&key, id),
            Kind::Hash => index.hash.remove(&hash_key(&key), id),
        }
    }
}

fn add_all(indexes: &mut [Spec], id: &str, document: &Document) -> Result<(), String> {
    let mut staged = Vec::new();
    for (position, index) in indexes.iter().enumerate() {
        let Some(key) = index_key(index, document) else {
            continue;
        };
        let clash = match index.kind {
            Kind::BTree => index.unique && index.btree.has_other(&key, id),
            Kind::Hash => index.unique && index.hash.has_other(&hash_key(&key), id),
        };
        if clash {
            return Err(format!("E11000 duplicate key on index '{}'", index.name));
        }
        staged.push((position, key));
    }
    for (position, key) in staged {
        let index = &mut indexes[position];
        match index.kind {
            Kind::BTree => index.btree.insert(key, id, false)?,
            Kind::Hash => index.hash.insert(hash_key(&key), id, false)?,
        }
    }
    Ok(())
}

fn is_direction(value: &Bson) -> bool {
    match value {
        Bson::Int32(1 | -1) | Bson::Int64(1 | -1) => true,
        Bson::Double(number) => *number == 1.0 || *number == -1.0,
        _ => false,
    }
}

fn index_key(index: &Spec, document: &Document) -> Option<Vec<Component>> {
    let mut key = Vec::with_capacity(index.fields.len());
    for field in &index.fields {
        match lookup(document, field) {
            None if index.sparse => return None,
            None => key.push(Component::Null),
            Some(value) => key.push(component(value)),
        }
    }
    Some(key)
}

fn probe(index: &Spec, filter: &Document) -> Option<(usize, Vec<String>)> {
    let mut prefix = Vec::new();
    let mut low = None;
    let mut high = None;
    let mut used = 0usize;
    for field in &index.fields {
        let Some(value) = filter.get(field) else {
            break;
        };
        if let Some(bounds) = range_bounds(value) {
            if index.kind == Kind::Hash {
                return None;
            }
            low = bounds.0;
            high = bounds.1;
            used += 1;
            break;
        }
        let Some(eq) = equality_value(value) else {
            break;
        };
        prefix.push(component(eq));
        used += 1;
    }
    if used == 0 {
        return None;
    }
    if index.kind == Kind::Hash && (low.is_some() || high.is_some() || used != index.fields.len()) {
        return None;
    }
    let ids = match index.kind {
        Kind::Hash => index.hash.get(&hash_key(&prefix)),
        Kind::BTree => index.btree.scan(&prefix, low.as_ref(), high.as_ref()),
    };
    Some((used, ids))
}

fn equality_value(value: &Bson) -> Option<&Bson> {
    match value {
        Bson::Document(doc) if doc.len() == 1 && doc.get("$eq").is_some() => doc.get("$eq"),
        Bson::Document(_) => None,
        other => Some(other),
    }
}

fn range_bounds(value: &Bson) -> Option<(Option<End>, Option<End>)> {
    let Bson::Document(doc) = value else {
        return None;
    };
    if doc.is_empty()
        || doc
            .keys()
            .any(|key| !matches!(key.as_str(), "$gt" | "$gte" | "$lt" | "$lte"))
    {
        return None;
    }
    let mut low = None;
    let mut high = None;
    for (op, bound) in doc {
        let end = End {
            value: component(bound),
            inclusive: op == "$gte" || op == "$lte",
        };
        match op.as_str() {
            "$gt" | "$gte" => low = Some(end),
            "$lt" | "$lte" => high = Some(end),
            _ => return None,
        }
    }
    Some((low, high))
}

fn id_equality(filter: &Document) -> Option<String> {
    if filter.len() != 1 {
        return None;
    }
    let value = filter.get("_id")?;
    let value = equality_value(value)?;
    Some(id_of(value))
}

fn id_of(value: &Bson) -> String {
    match value {
        Bson::String(text) => text.clone(),
        Bson::ObjectId(id) => id.to_hex(),
        Bson::Int32(number) => number.to_string(),
        Bson::Int64(number) => number.to_string(),
        other => format!("b:{}", canonical_fallback(other)),
    }
}

fn component(value: &Bson) -> Component {
    match value {
        Bson::Null => Component::Null,
        Bson::Int32(number) => Component::Number(ordered_number(f64::from(*number)).unwrap_or(0)),
        Bson::Int64(number) => Component::Number(ordered_number(*number as f64).unwrap_or(0)),
        Bson::Double(number) => Component::Number(ordered_number(*number).unwrap_or(0)),
        Bson::Boolean(flag) => Component::Bool(*flag),
        Bson::String(text) => Component::String(text.clone()),
        Bson::ObjectId(id) => Component::Other(format!("o:{id}")),
        other => Component::Other(canonical_fallback(other)),
    }
}

fn canonical_fallback(value: &Bson) -> String {
    format!("{value:?}")
}

fn hash_key(parts: &[Component]) -> String {
    parts
        .iter()
        .map(|part| match part {
            Component::Null => "null".to_string(),
            Component::Number(bits) => format!("n:{bits}"),
            Component::Bool(flag) => format!("b:{flag}"),
            Component::String(text) => format!("s:{text}"),
            Component::Other(text) => format!("o:{text}"),
        })
        .collect::<Vec<_>>()
        .join("|")
}

fn lookup<'a>(document: &'a Document, path: &str) -> Option<&'a Bson> {
    let mut current = document;
    let mut parts = path.split('.');
    let last = parts.next_back()?;
    for part in parts {
        current = current.get_document(part).ok()?;
    }
    current.get(last)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn btree_range_hash_equality_and_unique() {
        let mut indexes = IndexManager::new();
        indexes
            .declare(
                "db.emp",
                &[
                    doc! { "name": "sal_1", "key": { "sal": 1 } },
                    doc! { "name": "job_hashed", "key": { "job": "hashed" } },
                    doc! { "name": "ename_1", "key": { "ename": 1 }, "unique": true, "sparse": true },
                ],
            )
            .unwrap();
        let rows = vec![
            (
                "1".into(),
                doc! { "_id": 1, "ename": "SMITH", "job": "CLERK", "sal": 800 },
            ),
            (
                "2".into(),
                doc! { "_id": 2, "ename": "ALLEN", "job": "SALESMAN", "sal": 1600 },
            ),
            (
                "3".into(),
                doc! { "_id": 3, "ename": "KING", "job": "PRESIDENT", "sal": 5000 },
            ),
            ("4".into(), doc! { "_id": 4, "job": "CLERK" }),
        ];
        indexes.rebuild("db.emp", &rows).unwrap();

        match indexes.access("db.emp", &doc! { "sal": { "$gt": 1000 } }) {
            Access::Index { name, ids } => {
                assert_eq!(name, "sal_1");
                assert_eq!(ids, vec!["2".to_string(), "3".to_string()]);
            }
            other => panic!("expected an index scan, got {other:?}"),
        }
        match indexes.access("db.emp", &doc! { "job": "CLERK" }) {
            Access::Index { name, mut ids } => {
                assert_eq!(name, "job_hashed");
                ids.sort();
                assert_eq!(ids, vec!["1".to_string(), "4".to_string()]);
            }
            other => panic!("expected a hash probe, got {other:?}"),
        }
        match indexes.access("db.emp", &doc! { "_id": 3 }) {
            Access::Id { id } => assert_eq!(id, "3"),
            other => panic!("expected an id lookup, got {other:?}"),
        }
        let duplicate = indexes.insert(
            "db.emp",
            "5",
            &doc! { "_id": 5, "ename": "SMITH", "sal": 1 },
        );
        assert!(duplicate.unwrap_err().contains("duplicate"));
    }
}
