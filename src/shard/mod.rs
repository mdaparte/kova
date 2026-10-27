//! Sharding. The router hashes `_id` onto a ring and forwards the
//! command to the Kova process that owns that arc.
//!
//! A shard is an ordinary Kova process at the configured client
//! address. That process can also run a replica set. The router speaks
//! OP_MSG and does not join the set. With `[sharding]` left out, this
//! module is not on the command path.

mod client;

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use bson::{Bson, Document};

pub use client::TcpShards;

// Set only while the router runs a SQL statement, so the statement
// can load each named collection from the shards.
tokio::task_local! {
    pub(crate) static SQL_ROUTER: Arc<ShardRouter>;
}

const VIRTUAL_NODES: u32 = 128;

/// 128 virtual nodes per shard. The point is `hash("{shard id}:{n}")`,
/// so reordering the config list does not move a document.
#[derive(Clone)]
pub struct Ring {
    nodes: Vec<(u64, u64)>,
    order: Vec<u64>,
}

impl Ring {
    pub fn new(shard_ids: &[u64]) -> Self {
        assert!(!shard_ids.is_empty(), "a ring needs a shard");
        let mut nodes = Vec::with_capacity(shard_ids.len() * VIRTUAL_NODES as usize);
        for id in shard_ids {
            for vnode in 0..VIRTUAL_NODES {
                let point = fnv64(format!("{id}:{vnode}").as_bytes());
                nodes.push((point, *id));
            }
        }
        nodes.sort_by_key(|(point, id)| (*point, *id));
        Self {
            nodes,
            order: shard_ids.to_vec(),
        }
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    /// Shard id that stores `id`. The id is stable when the config list is reordered.
    pub fn owner_id(&self, id: &Bson) -> u64 {
        let point = hash_id(id);
        let slot = self.nodes.partition_point(|(node, _)| *node < point);
        let slot = if slot == self.nodes.len() { 0 } else { slot };
        self.nodes[slot].1
    }

    /// Index into the config list of the shard that stores `id`.
    pub fn owner_index(&self, id: &Bson) -> usize {
        self.index_of(self.owner_id(id))
            .expect("ring shard is one of the configured ids")
    }

    fn index_of(&self, shard_id: u64) -> Option<usize> {
        self.order.iter().position(|candidate| *candidate == shard_id)
    }
}

pub fn hash_id(id: &Bson) -> u64 {
    fnv64(&id_bytes(id))
}

fn id_bytes(id: &Bson) -> Vec<u8> {
    match id {
        Bson::ObjectId(value) => value.bytes().to_vec(),
        Bson::String(value) => value.as_bytes().to_vec(),
        Bson::Int32(value) => value.to_le_bytes().to_vec(),
        Bson::Int64(value) => value.to_le_bytes().to_vec(),
        Bson::Double(value) => value.to_le_bytes().to_vec(),
        Bson::Boolean(value) => vec![u8::from(*value)],
        other => bson::to_vec(other).unwrap_or_default(),
    }
}

fn fnv64(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

#[async_trait]
pub trait ShardLink: Send + Sync {
    async fn call(&self, shard: usize, command: Document) -> Document;
}

#[derive(Clone)]
pub struct ShardRouter {
    ring: Ring,
    link: Arc<dyn ShardLink>,
    /// Ring from before the latest shard was added. A find by `_id`
    /// checks the new owner, then this one, and returns the document once.
    previous: Arc<std::sync::Mutex<Option<Ring>>>,
}

impl ShardRouter {
    pub fn connect(shards: &[crate::config::ShardMember]) -> Self {
        let ids: Vec<u64> = shards.iter().map(|shard| shard.id).collect();
        let addresses = shards
            .iter()
            .map(|shard| shard.client.clone())
            .collect::<Vec<_>>();
        Self::with_link(ids, Arc::new(TcpShards::new(addresses)))
    }

    pub fn with_link(shard_ids: Vec<u64>, link: Arc<dyn ShardLink>) -> Self {
        Self {
            ring: Ring::new(&shard_ids),
            link,
            previous: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Moves documents whose `_id` now falls on a new arc.
    ///
    /// `previous_ids` is the ring before the shard was added. A document
    /// that stays on its current shard is not rewritten. While the move
    /// runs, a find by `_id` returns the document once.
    pub async fn rebalance(&self, previous_ids: &[u64]) -> Result<(), String> {
        if previous_ids.is_empty() {
            return Err("rebalance needs the previous shard ids".into());
        }
        let previous = Ring::new(previous_ids);
        *self
            .previous
            .lock()
            .map_err(|_| "shard move lock was poisoned".to_string())? = Some(previous.clone());
        let result = self.move_documents(&previous).await;
        *self
            .previous
            .lock()
            .map_err(|_| "shard move lock was poisoned".to_string())? = None;
        result
    }

    /// Remembers the shard ids in `data_dir` and moves documents when the
    /// list has grown. The first start records the list and moves nothing.
    /// Removing a shard is left as it was.
    pub async fn adopt(&self, data_dir: &std::path::Path, ids: &[u64]) -> Result<(), String> {
        std::fs::create_dir_all(data_dir).map_err(|err| format!("data dir: {err}"))?;
        let path = data_dir.join("shard-ring.txt");
        let previous = read_ring(&path)?;
        match previous {
            None => write_ring(&path, ids),
            Some(old) if old == ids => Ok(()),
            Some(old) if old.len() < ids.len() && old.iter().all(|id| ids.contains(id)) => {
                self.rebalance(&old).await?;
                write_ring(&path, ids)
            }
            Some(old) => {
                crate::logger::LOGGER.warn(format!(
                    "shard list changed from {old:?} to {ids:?}; removing a shard does not move documents"
                ));
                Ok(())
            }
        }
    }

    /// Loads `namespace` (`db.collection` or a bare name) through the
    /// same routing a `find` uses.
    pub async fn find_documents(
        &self,
        namespace: &str,
        filter: &Document,
    ) -> Result<Vec<Document>, String> {
        let (db, name) = match namespace.split_once('.') {
            Some((db, name)) => (db, name),
            None => ("", namespace),
        };
        let mut command = bson::doc! { "find": name, "filter": filter.clone() };
        if !db.is_empty() {
            command.insert("$db", db);
        }
        documents_of(&self.route(&command).await)
    }

    pub async fn route(&self, command: &Document) -> Document {
        if is_transaction(command) {
            return fail("a transaction stays on one shard; the router does not open one");
        }
        if command.contains_key("getMore") {
            return fail("the router returns the batch in the first reply");
        }
        if command.contains_key("insert") {
            return self.route_insert(command).await;
        }
        if command.contains_key("find") {
            return self.route_find(command).await;
        }
        if command.contains_key("count") {
            return self.route_count(command).await;
        }
        if command.contains_key("update") {
            return self.route_update(command).await;
        }
        if command.contains_key("delete") {
            return self.route_delete(command).await;
        }
        if command.contains_key("mapReduce") {
            return self.route_cursor(command).await;
        }
        if command.contains_key("aggregate") {
            return self.route_aggregate(command).await;
        }
        if command.contains_key("explain") {
            return self.link.call(0, command.clone()).await;
        }
        if is_broadcast(command) {
            return self.route_broadcast(command).await;
        }
        fail("the router does not run this command")
    }

    async fn route_insert(&self, command: &Document) -> Document {
        let values = match command.get_array("documents") {
            Ok(values) => values,
            Err(_) => return fail("insert requires a 'documents' array"),
        };
        if values.is_empty() {
            return fail("insert requires at least one document");
        }
        let mut groups = vec![Vec::new(); self.ring.len()];
        for value in values {
            let Some(mut document) = value.as_document().cloned() else {
                return fail("insert requires documents to be objects");
            };
            if !document.contains_key("_id") {
                document.insert("_id", Bson::ObjectId(bson::oid::ObjectId::new()));
            }
            let id = document.get("_id").expect("_id was just set").clone();
            groups[self.ring.owner_index(&id)].push(document);
        }
        let mut inserted = 0i64;
        for (shard, documents) in groups.into_iter().enumerate() {
            if documents.is_empty() {
                continue;
            }
            let mut forwarded = command.clone();
            let documents = documents
                .into_iter()
                .map(Bson::Document)
                .collect::<Vec<_>>();
            forwarded.insert("documents", documents);
            let reply = self.link.call(shard, forwarded).await;
            if !is_ok(&reply) {
                let mut reply = reply;
                reply.insert("n", inserted);
                return reply;
            }
            inserted += number(&reply, "n");
        }
        bson::doc! { "ok": 1.0, "n": inserted }
    }

    async fn route_find(&self, command: &Document) -> Document {
        let filter = command.get_document("filter").cloned().unwrap_or_default();
        if let Some(id) = sole_id(&filter) {
            let owner = self.ring.owner_index(id);
            let reply = self.link.call(owner, command.clone()).await;
            if cursor_len(&reply) > 0 {
                return reply;
            }
            let previous = self.previous.lock().ok().and_then(|slot| {
                slot.as_ref()
                    .and_then(|ring| self.ring.index_of(ring.owner_id(id)))
            });
            if let Some(old) = previous {
                if old != owner {
                    return self.link.call(old, command.clone()).await;
                }
            }
            return reply;
        }
        let skip = number(command, "skip").max(0);
        let limit = match command.get("limit") {
            Some(_) => Some(number(command, "limit")).filter(|limit| *limit > 0),
            None => None,
        };
        let mut forwarded = command.clone();
        forwarded.remove("limit");
        forwarded.remove("skip");
        let replies = self.scatter(forwarded).await;
        merge_cursor(&replies, skip, limit)
    }

    async fn route_count(&self, command: &Document) -> Document {
        let filter = command.get_document("query").cloned().unwrap_or_default();
        if let Some(id) = sole_id(&filter) {
            return self
                .link
                .call(self.ring.owner_index(id), command.clone())
                .await;
        }
        sum_n(&self.scatter(command.clone()).await, &["n"])
    }

    async fn route_update(&self, command: &Document) -> Document {
        if command.get_str("update").ok() == Some("counters")
            && command.get_str("$db").unwrap_or("") == "admin"
        {
            return sum_n(&self.scatter(command.clone()).await, &["n", "nModified"]);
        }
        let Some(entry) = first_entry(command, "updates") else {
            return fail("update requires an 'updates' array");
        };
        let filter = entry.get_document("q").cloned().unwrap_or_default();
        let multi = entry.get_bool("multi").unwrap_or(false);
        self.route_write(command, &filter, multi).await
    }

    async fn route_delete(&self, command: &Document) -> Document {
        let Some(entry) = first_entry(command, "deletes") else {
            return fail("delete requires a 'deletes' array");
        };
        let filter = entry.get_document("q").cloned().unwrap_or_default();
        let limit = entry
            .get_i32("limit")
            .or_else(|_| entry.get_i64("limit").map(|value| value as i32))
            .unwrap_or(0);
        self.route_write(command, &filter, limit == 0).await
    }

    /// `every` sends the write to each shard and sums `n`. A single
    /// match walks the shards and stops at the first one that changed
    /// a document, so one filter cannot update one row per shard.
    async fn route_write(&self, command: &Document, filter: &Document, every: bool) -> Document {
        if let Some(id) = sole_id(filter) {
            return self
                .link
                .call(self.ring.owner_index(id), command.clone())
                .await;
        }
        if every {
            return sum_n(&self.scatter(command.clone()).await, &["n", "nModified"]);
        }
        let mut last = bson::doc! { "ok": 1.0, "n": 0i64, "nModified": 0i64 };
        for shard in 0..self.ring.len() {
            let reply = self.link.call(shard, command.clone()).await;
            if !is_ok(&reply) {
                return reply;
            }
            last = reply;
            if number(&last, "n") > 0 {
                return last;
            }
        }
        last
    }

    async fn route_cursor(&self, command: &Document) -> Document {
        merge_cursor(&self.scatter(command.clone()).await, 0, None)
    }

    async fn route_aggregate(&self, command: &Document) -> Document {
        let Some(stages) = command.get_array("pipeline").cloned().ok() else {
            return fail("aggregate requires a pipeline");
        };
        if let Some(index) = stage_index(&stages, "$group") {
            return self.route_group(command, &stages, index).await;
        }
        if let Some(index) = stage_index(&stages, "$lookup") {
            return self.route_lookup(command, &stages, index).await;
        }
        if let Some(message) = aggregate_refusal(command) {
            return fail(&message);
        }
        self.route_cursor(command).await
    }

    async fn route_group(&self, command: &Document, stages: &[Bson], index: usize) -> Document {
        if let Some(message) = prefix_refusal(&stages[..index]).or_else(|| tail_refusal(stages, index))
        {
            return fail(&message);
        }
        let Some(spec) = stages[index]
            .as_document()
            .and_then(|stage| stage.get_document("$group").ok())
        else {
            return fail("aggregate stage $group cannot be merged");
        };
        if let Some(message) = group_refusal(spec) {
            return fail(&message);
        }
        let (sent, averages) = rewrite_group(spec);
        let mut pipeline = stages[..index].to_vec();
        pipeline.push(Bson::Document(bson::doc! { "$group": sent }));
        let mut forwarded = command.clone();
        forwarded.insert("pipeline", pipeline);
        let replies = self.scatter(forwarded).await;
        let rows = match cursor_rows(&replies) {
            Ok(rows) => rows,
            Err(reply) => return reply,
        };
        let namespace = cursor_namespace(&replies);
        let merged = merge_groups(&rows, spec, &averages);
        cursor_of(&namespace, merged)
    }

    async fn route_lookup(&self, command: &Document, stages: &[Bson], index: usize) -> Document {
        if let Some(message) = prefix_refusal(&stages[..index]).or_else(|| tail_refusal(stages, index))
        {
            return fail(&message);
        }
        let Some(spec) = stages[index]
            .as_document()
            .and_then(|stage| stage.get_document("$lookup").ok())
        else {
            return fail("aggregate stage $lookup cannot be merged");
        };
        let (Ok(from), Ok(local_field), Ok(foreign_field)) = (
            spec.get_str("from"),
            spec.get_str("localField"),
            spec.get_str("foreignField"),
        ) else {
            return fail("aggregate stage $lookup cannot be merged");
        };
        let as_field = spec.get_str("as").unwrap_or(from);
        let db = command.get_str("$db").ok();
        let local = if index == 0 {
            let mut find = bson::doc! { "find": command.get_str("aggregate").unwrap_or(""), "filter": {} };
            if let Some(db) = db {
                find.insert("$db", db);
            }
            find
        } else {
            let mut forwarded = command.clone();
            forwarded.insert("pipeline", stages[..index].to_vec());
            forwarded
        };
        let replies = self.scatter(local).await;
        let mut rows = match cursor_rows(&replies) {
            Ok(rows) => rows,
            Err(reply) => return reply,
        };
        let mut keys = Vec::new();
        for row in &rows {
            if let Some(value) = row.get(local_field) {
                if !keys.iter().any(|kept: &Bson| kept == value) {
                    keys.push(value.clone());
                }
            }
        }
        let foreign = if keys.is_empty() {
            Vec::new()
        } else {
            match self.foreign_matches(db, from, foreign_field, &keys).await {
                Ok(documents) => documents,
                Err(reply) => return reply,
            }
        };
        for row in &mut rows {
            let key = row.get(local_field).cloned();
            let matches = foreign
                .iter()
                .filter(|document| document.get(foreign_field) == key.as_ref())
                .cloned()
                .map(Bson::Document)
                .collect::<Vec<_>>();
            row.insert(as_field, matches);
        }
        cursor_of(&cursor_namespace(&replies), rows)
    }

    async fn foreign_matches(
        &self,
        db: Option<&str>,
        collection: &str,
        field: &str,
        keys: &[Bson],
    ) -> Result<Vec<Document>, Document> {
        let filter = bson::doc! { field: { "$in": keys.to_vec() } };
        let mut command = bson::doc! { "find": collection, "filter": filter };
        if let Some(db) = db {
            command.insert("$db", db);
        }
        if field == "_id" {
            let mut groups: Vec<Vec<Bson>> = vec![Vec::new(); self.ring.len()];
            for key in keys {
                groups[self.ring.owner_index(key)].push(key.clone());
            }
            let mut found = Vec::new();
            for (shard, keys) in groups.into_iter().enumerate() {
                if keys.is_empty() {
                    continue;
                }
                command.insert("filter", bson::doc! { "_id": { "$in": keys } });
                let reply = self.link.call(shard, command.clone()).await;
                found.extend(cursor_rows(std::slice::from_ref(&reply))?);
            }
            return Ok(found);
        }
        cursor_rows(&self.scatter(command).await)
    }

    async fn move_documents(&self, previous: &Ring) -> Result<(), String> {
        let listed = self
            .route(&bson::doc! { "listDatabases": 1, "filter": {} })
            .await;
        if !is_ok(&listed) {
            return Err(listed
                .get_str("errmsg")
                .unwrap_or("listDatabases failed")
                .to_string());
        }
        let databases = listed
            .get_array("databases")
            .map(|rows| {
                rows.iter()
                    .filter_map(|row| row.as_document()?.get_str("name").ok())
                    .filter(|name| *name != "admin")
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        for database in databases {
            let listed = self
                .route(&bson::doc! {
                    "listCollections": 1,
                    "filter": {},
                    "$db": &database,
                })
                .await;
            let collections = match cursor_rows(std::slice::from_ref(&listed)) {
                Ok(rows) => rows,
                Err(reply) => {
                    return Err(reply
                        .get_str("errmsg")
                        .unwrap_or("listCollections failed")
                        .to_string());
                }
            };
            for info in collections {
                if info.get_str("type").ok() == Some("view") {
                    continue;
                }
                let Some(name) = info.get_str("name").ok() else {
                    continue;
                };
                self.move_collection(previous, &database, name).await?;
            }
        }
        Ok(())
    }

    async fn move_collection(
        &self,
        previous: &Ring,
        database: &str,
        name: &str,
    ) -> Result<(), String> {
        let found = self
            .route(&bson::doc! { "find": name, "filter": {}, "$db": database })
            .await;
        let documents = documents_of(&found)?;
        for document in documents {
            let Some(id) = document.get("_id") else {
                continue;
            };
            let new_id = self.ring.owner_id(id);
            let old_id = previous.owner_id(id);
            if new_id == old_id {
                continue;
            }
            let new_owner = self
                .ring
                .index_of(new_id)
                .ok_or_else(|| format!("shard {new_id} is not in the current list"))?;
            let old_owner = self
                .ring
                .index_of(old_id)
                .ok_or_else(|| format!("shard {old_id} is not in the current list"))?;
            let inserted = self
                .link
                .call(
                    new_owner,
                    bson::doc! {
                        "insert": name,
                        "documents": [document.clone()],
                        "$db": database,
                    },
                )
                .await;
            if !is_ok(&inserted) {
                return Err(inserted
                    .get_str("errmsg")
                    .unwrap_or("move insert failed")
                    .to_string());
            }
            let deleted = self
                .link
                .call(
                    old_owner,
                    bson::doc! {
                        "delete": name,
                        "deletes": [{ "q": { "_id": id.clone() }, "limit": 1 }],
                        "$db": database,
                    },
                )
                .await;
            if !is_ok(&deleted) {
                return Err(deleted
                    .get_str("errmsg")
                    .unwrap_or("move delete failed")
                    .to_string());
            }
        }
        Ok(())
    }

    async fn route_broadcast(&self, command: &Document) -> Document {
        let replies = self.scatter(command.clone()).await;
        if command.contains_key("listCollections") || command.contains_key("listIndexes") {
            return merge_named_batch(&replies);
        }
        if command.contains_key("listDatabases") {
            return merge_databases(&replies);
        }
        if command.contains_key("dbStats") {
            let mut reply = sum_n(
                &replies,
                &[
                    "collections",
                    "objects",
                    "dataSize",
                    "storageSize",
                    "indexes",
                ],
            );
            reply.insert("scaleFactor", 1i32);
            return reply;
        }
        if command.contains_key("collStats") {
            return merge_coll_stats(&replies);
        }
        first_ok(&replies)
    }

    async fn scatter(&self, command: Document) -> Vec<Document> {
        let mut replies = Vec::with_capacity(self.ring.len());
        for shard in 0..self.ring.len() {
            replies.push(self.link.call(shard, command.clone()).await);
        }
        replies
    }
}

fn read_ring(path: &std::path::Path) -> Result<Option<Vec<u64>>, String> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(format!("read {}: {err}", path.display())),
    };
    let mut ids = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let id = line
            .parse::<u64>()
            .map_err(|_| format!("shard ring line {line:?} is not an id"))?;
        ids.push(id);
    }
    if ids.is_empty() {
        return Ok(None);
    }
    Ok(Some(ids))
}

fn write_ring(path: &std::path::Path, ids: &[u64]) -> Result<(), String> {
    let body = ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join("\n");
    std::fs::write(path, body + "\n").map_err(|err| format!("write {}: {err}", path.display()))
}

fn is_transaction(command: &Document) -> bool {
    command.contains_key("txnId")
        || command.contains_key("beginTransaction")
        || command.contains_key("commitTransaction")
        || command.contains_key("abortTransaction")
        || command.contains_key("applyTransaction")
        || command.contains_key("commit")
        || command.contains_key("abort")
}

fn is_broadcast(command: &Document) -> bool {
    const KEYS: &[&str] = &[
        "create",
        "drop",
        "createIndexes",
        "dropIndexes",
        "deleteIndexes",
        "dropDatabase",
        "listCollections",
        "listDatabases",
        "listIndexes",
        "dbStats",
        "collStats",
        "createUser",
        "updateUser",
        "dropUser",
        "grantRolesToUser",
        "revokeRolesFromUser",
        "createRole",
        "dropRole",
    ];
    KEYS.iter().any(|key| command.contains_key(*key))
}

fn stage_index(stages: &[Bson], name: &str) -> Option<usize> {
    stages.iter().position(|stage| {
        stage
            .as_document()
            .is_some_and(|document| document.contains_key(name))
    })
}

fn stage_name(stage: &Bson) -> Option<&str> {
    stage
        .as_document()
        .and_then(|document| document.keys().next().map(String::as_str))
}

const MERGE_SAFE: &[&str] = &[
    "$match",
    "$project",
    "$addFields",
    "$set",
    "$unset",
    "$unwind",
    "$replaceRoot",
    "$replaceWith",
];

fn prefix_refusal(stages: &[Bson]) -> Option<String> {
    for stage in stages {
        let name = stage_name(stage)?;
        if !MERGE_SAFE.contains(&name) {
            return Some(format!("aggregate stage {name} cannot be merged"));
        }
    }
    None
}

fn tail_refusal(stages: &[Bson], index: usize) -> Option<String> {
    stages
        .get(index + 1)
        .and_then(stage_name)
        .map(|name| format!("aggregate stage {name} cannot be merged"))
}

fn group_refusal(spec: &Document) -> Option<String> {
    for (name, value) in spec {
        if name == "_id" {
            continue;
        }
        let Some(op) = value
            .as_document()
            .and_then(|document| document.keys().next().cloned())
        else {
            return Some(format!("aggregate accumulator {name} cannot be merged"));
        };
        if !matches!(
            op.as_str(),
            "$sum" | "$min" | "$max" | "$avg" | "$count"
        ) {
            return Some(format!("aggregate accumulator {op} cannot be merged"));
        }
    }
    None
}

struct Average {
    name: String,
    sum: String,
    count: String,
}

fn rewrite_group(spec: &Document) -> (Document, Vec<Average>) {
    let mut sent = Document::new();
    let mut averages = Vec::new();
    for (name, value) in spec {
        if name == "_id" {
            sent.insert(name, value.clone());
            continue;
        }
        let op = value
            .as_document()
            .and_then(|document| document.keys().next().cloned());
        if op.as_deref() == Some("$avg") {
            let operand = value
                .as_document()
                .and_then(|document| document.get("$avg"))
                .cloned()
                .unwrap_or(Bson::Null);
            let sum = format!("__kova_sum_{name}");
            let count = format!("__kova_n_{name}");
            sent.insert(sum.clone(), bson::doc! { "$sum": operand });
            sent.insert(count.clone(), bson::doc! { "$count": {} });
            averages.push(Average {
                name: name.clone(),
                sum,
                count,
            });
            continue;
        }
        sent.insert(name, value.clone());
    }
    (sent, averages)
}

fn merge_groups(rows: &[Document], spec: &Document, averages: &[Average]) -> Vec<Document> {
    let mut groups: Vec<Document> = Vec::new();
    for row in rows {
        let id = row.get("_id").cloned().unwrap_or(Bson::Null);
        let key = id_key(&id);
        if !groups
            .iter()
            .any(|group| id_key(group.get("_id").unwrap_or(&Bson::Null)) == key)
        {
            let mut fresh = bson::doc! { "_id": id };
            for (name, _) in spec {
                if name == "_id" || averages.iter().any(|average| average.name == *name) {
                    continue;
                }
                fresh.insert(name, Bson::Null);
            }
            for average in averages {
                fresh.insert(average.sum.clone(), 0i32);
                fresh.insert(average.count.clone(), 0i32);
            }
            groups.push(fresh);
        }
        let group = groups
            .iter_mut()
            .find(|group| id_key(group.get("_id").unwrap_or(&Bson::Null)) == key)
            .expect("group was just inserted");
        for (name, value) in spec {
            if name == "_id" {
                continue;
            }
            if let Some(average) = averages.iter().find(|average| average.name == *name) {
                let sum = bson_f64(group.get(&average.sum).unwrap_or(&Bson::Null))
                    + bson_f64(row.get(&average.sum).unwrap_or(&Bson::Null));
                let count = number(group, &average.count) + number(row, &average.count);
                group.insert(average.sum.clone(), finish_number(sum));
                group.insert(average.count.clone(), count);
                continue;
            }
            let op = value
                .as_document()
                .and_then(|document| document.keys().next().map(String::as_str))
                .unwrap_or("");
            let current = group.get(name).cloned().unwrap_or(Bson::Null);
            let incoming = row.get(name).cloned().unwrap_or(Bson::Null);
            let combined = match op {
                "$sum" | "$count" => finish_number(bson_f64(&current) + bson_f64(&incoming)),
                "$min" => tighter(&current, &incoming, true),
                "$max" => tighter(&current, &incoming, false),
                _ => incoming,
            };
            group.insert(name, combined);
        }
    }
    for group in &mut groups {
        for average in averages {
            let sum = bson_f64(group.get(&average.sum).unwrap_or(&Bson::Null));
            let count = number(group, &average.count);
            let value = if count == 0 {
                Bson::Null
            } else {
                Bson::Double(sum / count as f64)
            };
            group.insert(average.name.clone(), value);
            group.remove(&average.sum);
            group.remove(&average.count);
        }
    }
    groups
}

fn id_key(value: &Bson) -> Vec<u8> {
    bson::to_vec(&bson::doc! { "id": value.clone() }).unwrap_or_default()
}

fn bson_f64(value: &Bson) -> f64 {
    match value {
        Bson::Double(number) => *number,
        Bson::Int32(number) => f64::from(*number),
        Bson::Int64(number) => *number as f64,
        _ => 0.0,
    }
}

fn finish_number(number: f64) -> Bson {
    if number.is_finite() && number.fract() == 0.0 && number.abs() < i32::MAX as f64 {
        Bson::Int32(number as i32)
    } else if number.is_finite() && number.fract() == 0.0 {
        Bson::Int64(number as i64)
    } else {
        Bson::Double(number)
    }
}

fn tighter(current: &Bson, incoming: &Bson, low: bool) -> Bson {
    match (bson_f64_opt(current), bson_f64_opt(incoming)) {
        (None, Some(_)) => incoming.clone(),
        (Some(_), None) => current.clone(),
        (Some(left), Some(right)) => {
            let keep_current = if low { left <= right } else { left >= right };
            if keep_current {
                current.clone()
            } else {
                incoming.clone()
            }
        }
        (None, None) => incoming.clone(),
    }
}

fn bson_f64_opt(value: &Bson) -> Option<f64> {
    match value {
        Bson::Double(number) => Some(*number),
        Bson::Int32(number) => Some(f64::from(*number)),
        Bson::Int64(number) => Some(*number as f64),
        Bson::Null => None,
        _ => None,
    }
}

fn cursor_len(reply: &Document) -> usize {
    reply
        .get_document("cursor")
        .ok()
        .and_then(|cursor| cursor.get_array("firstBatch").ok())
        .map(|batch| batch.len())
        .unwrap_or(0)
}

fn cursor_namespace(replies: &[Document]) -> String {
    replies
        .iter()
        .find_map(|reply| {
            reply
                .get_document("cursor")
                .ok()
                .and_then(|cursor| cursor.get_str("ns").ok())
                .map(str::to_string)
        })
        .unwrap_or_default()
}

fn cursor_rows(replies: &[Document]) -> Result<Vec<Document>, Document> {
    let mut rows = Vec::new();
    for reply in replies {
        if !is_ok(reply) {
            return Err(reply.clone());
        }
        let Ok(cursor) = reply.get_document("cursor") else {
            return Err(fail("shard reply has no cursor"));
        };
        let Ok(batch) = cursor.get_array("firstBatch") else {
            continue;
        };
        for value in batch {
            let Some(document) = value.as_document().cloned() else {
                return Err(fail("cursor row is not a document"));
            };
            rows.push(document);
        }
    }
    Ok(rows)
}

fn cursor_of(namespace: &str, rows: Vec<Document>) -> Document {
    let rows = rows.into_iter().map(Bson::Document).collect::<Vec<_>>();
    bson::doc! {
        "ok": 1.0,
        "cursor": { "id": 0i64, "ns": namespace, "firstBatch": rows },
    }
}

/// Stages the router can concatenate. A reducing stage runs on each
/// shard; the router merges the pieces.
fn aggregate_refusal(command: &Document) -> Option<String> {
    let stages = command.get_array("pipeline").ok()?;
    const SAFE: &[&str] = &[
        "$match",
        "$project",
        "$addFields",
        "$set",
        "$unset",
        "$unwind",
        "$replaceRoot",
        "$replaceWith",
    ];
    for stage in stages {
        let doc = stage.as_document()?;
        if let Some(name) = doc.keys().find(|key| !SAFE.contains(&key.as_str())) {
            return Some(format!(
                "aggregate stage {name} stays on one shard; the router merges match and project only"
            ));
        }
    }
    None
}

fn first_entry<'a>(command: &'a Document, key: &str) -> Option<&'a Document> {
    command.get_array(key).ok()?.first()?.as_document()
}

fn sole_id(filter: &Document) -> Option<&Bson> {
    if filter.len() != 1 {
        return None;
    }
    match filter.get("_id")? {
        Bson::Document(ops) if ops.len() == 1 => ops.get("$eq"),
        Bson::Document(_) => None,
        other => Some(other),
    }
}

fn merge_cursor(replies: &[Document], skip: i64, limit: Option<i64>) -> Document {
    let mut rows = Vec::new();
    let mut namespace = String::new();
    for reply in replies {
        if !is_ok(reply) {
            return reply.clone();
        }
        let Ok(cursor) = reply.get_document("cursor") else {
            return fail("shard reply has no cursor");
        };
        if namespace.is_empty() {
            namespace = cursor.get_str("ns").unwrap_or("").to_string();
        }
        if let Ok(batch) = cursor.get_array("firstBatch") {
            rows.extend(batch.iter().cloned());
        }
    }
    let skip = usize::try_from(skip).unwrap_or(0).min(rows.len());
    rows.drain(..skip);
    if let Some(limit) = limit {
        let limit = usize::try_from(limit).unwrap_or(rows.len());
        rows.truncate(limit);
    }
    bson::doc! {
        "ok": 1.0,
        "cursor": {
            "id": 0i64,
            "ns": namespace,
            "firstBatch": rows,
        },
    }
}

fn merge_named_batch(replies: &[Document]) -> Document {
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    let mut namespace = String::new();
    for reply in replies {
        if !is_ok(reply) {
            return reply.clone();
        }
        let Ok(cursor) = reply.get_document("cursor") else {
            continue;
        };
        if namespace.is_empty() {
            namespace = cursor.get_str("ns").unwrap_or("").to_string();
        }
        let Ok(batch) = cursor.get_array("firstBatch") else {
            continue;
        };
        for value in batch {
            let name = value
                .as_document()
                .and_then(|document| document.get_str("name").ok())
                .unwrap_or("");
            if name.is_empty() || seen.insert(name.to_string()) {
                rows.push(value.clone());
            }
        }
    }
    bson::doc! {
        "ok": 1.0,
        "cursor": { "id": 0i64, "ns": namespace, "firstBatch": rows },
    }
}

fn merge_databases(replies: &[Document]) -> Document {
    let mut rows = Vec::new();
    let mut seen = BTreeSet::new();
    for reply in replies {
        if !is_ok(reply) {
            return reply.clone();
        }
        let Ok(list) = reply.get_array("databases") else {
            continue;
        };
        for value in list {
            let name = value
                .as_document()
                .and_then(|document| document.get_str("name").ok())
                .unwrap_or("");
            if seen.insert(name.to_string()) {
                rows.push(value.clone());
            }
        }
    }
    bson::doc! { "ok": 1.0, "databases": rows, "totalSize": 0i64 }
}

fn merge_coll_stats(replies: &[Document]) -> Document {
    let mut count = 0i64;
    let mut size = 0i64;
    let mut storage = 0i64;
    let mut merged = None;
    for reply in replies {
        if !is_ok(reply) {
            return reply.clone();
        }
        if merged.is_none() {
            merged = Some(reply.clone());
        }
        count += number(reply, "count");
        size += number(reply, "size");
        storage += number(reply, "storageSize");
    }
    let mut reply = merged.unwrap_or_else(|| bson::doc! { "ok": 1.0 });
    reply.insert("count", count);
    reply.insert("size", size);
    reply.insert("storageSize", storage);
    reply.insert("ok", 1.0);
    reply
}

fn sum_n(replies: &[Document], fields: &[&str]) -> Document {
    let mut totals = vec![0i64; fields.len()];
    for reply in replies {
        if !is_ok(reply) {
            return reply.clone();
        }
        for (index, field) in fields.iter().enumerate() {
            totals[index] += number(reply, field);
        }
    }
    let mut reply = bson::doc! { "ok": 1.0 };
    for (field, total) in fields.iter().zip(totals) {
        reply.insert(*field, total);
    }
    reply
}

fn first_ok(replies: &[Document]) -> Document {
    for reply in replies {
        if !is_ok(reply) {
            return reply.clone();
        }
    }
    replies
        .first()
        .cloned()
        .unwrap_or_else(|| bson::doc! { "ok": 1.0 })
}

fn documents_of(reply: &Document) -> Result<Vec<Document>, String> {
    if !is_ok(reply) {
        return Err(reply.get_str("errmsg").unwrap_or("find failed").to_string());
    }
    let cursor = reply
        .get_document("cursor")
        .map_err(|_| "shard reply has no cursor".to_string())?;
    let batch = cursor
        .get_array("firstBatch")
        .map_err(|_| "cursor has no firstBatch".to_string())?;
    batch
        .iter()
        .map(|value| {
            value
                .as_document()
                .cloned()
                .ok_or_else(|| "cursor row is not a document".to_string())
        })
        .collect()
}

fn is_ok(document: &Document) -> bool {
    match document.get("ok") {
        Some(Bson::Double(value)) => *value == 1.0,
        Some(Bson::Int32(value)) => *value == 1,
        Some(Bson::Int64(value)) => *value == 1,
        _ => false,
    }
}

fn number(document: &Document, key: &str) -> i64 {
    match document.get(key) {
        Some(Bson::Int32(value)) => i64::from(*value),
        Some(Bson::Int64(value)) => *value,
        Some(Bson::Double(value)) => *value as i64,
        _ => 0,
    }
}

fn fail(message: &str) -> Document {
    bson::doc! { "ok": 0.0, "errmsg": message }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{self, reply_document};
    use crate::storage::PartitionRouter;
    use crate::types::PartitionResult;

    struct MemoryLink {
        routers: Vec<PartitionRouter>,
    }

    #[async_trait]
    impl ShardLink for MemoryLink {
        async fn call(&self, shard: usize, command: Document) -> Document {
            let parsed = match protocol::command_from_document(&command) {
                Ok(parsed) => parsed,
                Err(message) => return fail(&message),
            };
            let Some(receiver) = self.routers[shard].dispatch(parsed) else {
                return fail("storage partition unavailable");
            };
            match receiver.await {
                Ok(result) => reply_document(&result),
                Err(_) => fail("partition worker dropped the request"),
            }
        }
    }

    #[test]
    fn the_ring_uses_every_shard_and_keeps_an_id() {
        let ring = Ring::new(&[1, 2]);
        let mut seen = [false; 2];
        for value in 0..200 {
            seen[ring.owner_index(&Bson::Int32(value))] = true;
        }
        assert!(seen[0] && seen[1], "both arcs should receive an id");
        let id = Bson::Int32(7);
        assert_eq!(ring.owner_index(&id), ring.owner_index(&id));
    }

    #[tokio::test]
    async fn two_shards_hold_different_documents_and_the_router_returns_both() {
        let ring = Ring::new(&[1, 2]);
        let mut on_left = None;
        let mut on_right = None;
        for value in 0..1000 {
            match ring.owner_index(&Bson::Int32(value)) {
                0 if on_left.is_none() => on_left = Some(value),
                1 if on_right.is_none() => on_right = Some(value),
                _ => {}
            }
            if on_left.is_some() && on_right.is_some() {
                break;
            }
        }
        let left_id = on_left.expect("an id on shard 0");
        let right_id = on_right.expect("an id on shard 1");
        let left = PartitionRouter::new(1);
        let right = PartitionRouter::new(1);
        let router = ShardRouter::with_link(
            vec![1, 2],
            Arc::new(MemoryLink {
                routers: vec![left.clone(), right.clone()],
            }),
        );

        let inserted = router
            .route(&bson::doc! {
                "insert": "emp",
                "documents": [
                    { "_id": left_id, "ename": "ADA" },
                    { "_id": right_id, "ename": "BOB" },
                ],
                "$db": "db",
            })
            .await;
        assert_eq!(inserted.get_f64("ok"), Ok(1.0), "{inserted}");
        assert_eq!(inserted.get_i64("n"), Ok(2), "{inserted}");

        let found = router
            .route(&bson::doc! { "find": "emp", "filter": {}, "$db": "db" })
            .await;
        let rows = documents_of(&found).expect("rows");
        let ids: BTreeSet<i32> = rows
            .iter()
            .map(|row| row.get_i32("_id").expect("id"))
            .collect();
        assert_eq!(ids, BTreeSet::from([left_id, right_id]));

        let only = router
            .route(&bson::doc! {
                "find": "emp",
                "filter": { "_id": left_id },
                "$db": "db",
            })
            .await;
        let only_rows = documents_of(&only).expect("one row");
        assert_eq!(only_rows.len(), 1);
        assert_eq!(only_rows[0].get_str("ename"), Ok("ADA"));

        assert_eq!(count_ids(&left).await, vec![left_id]);
        assert_eq!(count_ids(&right).await, vec![right_id]);
    }

    async fn count_ids(router: &PartitionRouter) -> Vec<i32> {
        let command = protocol::command_from_document(&bson::doc! {
            "find": "emp",
            "filter": {},
            "$db": "db",
        })
        .expect("find");
        let receiver = router.dispatch(command).expect("dispatch");
        match receiver.await.expect("reply") {
            PartitionResult::Found { documents } => documents
                .iter()
                .map(|document| document.get_i32("_id").expect("id"))
                .collect(),
            other => panic!("expected documents, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn router_group_matches_one_process_and_names_a_push() {
        let left = PartitionRouter::new(1);
        let right = PartitionRouter::new(1);
        let whole = PartitionRouter::new(1);
        let router = ShardRouter::with_link(
            vec![1, 2],
            Arc::new(MemoryLink {
                routers: vec![left, right],
            }),
        );
        let documents: Vec<Document> = (0..20)
            .map(|id| {
                bson::doc! {
                    "_id": id,
                    "job": if id % 2 == 0 { "CLERK" } else { "MANAGER" },
                    "sal": 100,
                }
            })
            .collect();
        let inserted = router
            .route(&bson::doc! {
                "insert": "emp",
                "documents": documents.clone(),
                "$db": "db",
            })
            .await;
        assert_eq!(inserted.get_f64("ok"), Ok(1.0), "{inserted}");
        let parsed = protocol::command_from_document(&bson::doc! {
            "insert": "emp",
            "documents": documents,
            "$db": "db",
        })
        .expect("insert");
        whole.dispatch(parsed).expect("dispatch").await.expect("reply");

        let group = bson::doc! {
            "aggregate": "emp",
            "pipeline": [{
                "$group": {
                    "_id": "$job",
                    "total": { "$sum": "$sal" },
                    "n": { "$count": {} },
                }
            }],
            "$db": "db",
        };
        let routed = documents_of(&router.route(&group).await).expect("router groups");
        let local = protocol::command_from_document(&group).expect("group");
        let local = match whole.dispatch(local).expect("dispatch").await.expect("reply") {
            PartitionResult::Found { documents } => documents,
            other => panic!("expected groups, got {other:?}"),
        };
        assert_eq!(totals(&routed), totals(&local));

        let refused = router
            .route(&bson::doc! {
                "aggregate": "emp",
                "pipeline": [{
                    "$group": { "_id": "$job", "names": { "$push": "$ename" } }
                }],
                "$db": "db",
            })
            .await;
        let message = refused.get_str("errmsg").unwrap_or("");
        assert!(message.contains("$push"), "{refused}");
    }

    #[tokio::test]
    async fn router_lookup_joins_on_equality() {
        let router = ShardRouter::with_link(
            vec![1, 2],
            Arc::new(MemoryLink {
                routers: vec![PartitionRouter::new(1), PartitionRouter::new(1)],
            }),
        );
        let inserted = router
            .route(&bson::doc! {
                "insert": "emp",
                "documents": [
                    { "_id": 1, "ename": "ADA", "deptno": 10 },
                    { "_id": 2, "ename": "BOB", "deptno": 20 },
                ],
                "$db": "db",
            })
            .await;
        assert_eq!(inserted.get_f64("ok"), Ok(1.0), "{inserted}");
        let inserted = router
            .route(&bson::doc! {
                "insert": "dept",
                "documents": [
                    { "_id": 10, "dname": "ACCOUNTING" },
                    { "_id": 20, "dname": "RESEARCH" },
                ],
                "$db": "db",
            })
            .await;
        assert_eq!(inserted.get_f64("ok"), Ok(1.0), "{inserted}");
        let joined = documents_of(
            &router
                .route(&bson::doc! {
                    "aggregate": "emp",
                    "pipeline": [{
                        "$lookup": {
                            "from": "dept",
                            "localField": "deptno",
                            "foreignField": "_id",
                            "as": "dept",
                        }
                    }],
                    "$db": "db",
                })
                .await,
        )
        .expect("joined rows");
        assert_eq!(joined.len(), 2, "{joined:?}");
        let ada = joined
            .iter()
            .find(|row| row.get_str("ename").ok() == Some("ADA"))
            .expect("ada");
        let dept = ada.get_array("dept").expect("dept");
        assert_eq!(dept.len(), 1);
        assert_eq!(
            dept[0].as_document().unwrap().get_str("dname"),
            Ok("ACCOUNTING")
        );
    }

    #[tokio::test]
    async fn adding_a_shard_moves_a_document_and_a_find_returns_it_once() {
        let grown = Ring::new(&[1, 2]);
        let id = (0..500)
            .find(|value| grown.owner_index(&Bson::Int32(*value)) == 1)
            .expect("an id that changes arc");
        let stays = (0..500)
            .find(|value| grown.owner_index(&Bson::Int32(*value)) == 0)
            .expect("an id that stays");
        let first = PartitionRouter::new(1);
        let second = PartitionRouter::new(1);
        let starter = ShardRouter::with_link(
            vec![1],
            Arc::new(MemoryLink {
                routers: vec![first.clone()],
            }),
        );
        let inserted = starter
            .route(&bson::doc! {
                "insert": "emp",
                "documents": [
                    { "_id": id, "ename": "MOVED" },
                    { "_id": stays, "ename": "STAYS" },
                ],
                "$db": "db",
            })
            .await;
        assert_eq!(inserted.get_f64("ok"), Ok(1.0), "{inserted}");

        let router = ShardRouter::with_link(
            vec![1, 2],
            Arc::new(MemoryLink {
                routers: vec![first.clone(), second.clone()],
            }),
        );
        *router.previous.lock().expect("lock") = Some(Ring::new(&[1]));
        let during = documents_of(
            &router
                .route(&bson::doc! { "find": "emp", "filter": { "_id": id }, "$db": "db" })
                .await,
        )
        .expect("during");
        assert_eq!(during.len(), 1);
        assert_eq!(during[0].get_str("ename"), Ok("MOVED"));
        *router.previous.lock().expect("lock") = None;

        router.rebalance(&[1]).await.expect("rebalance");
        let after = documents_of(
            &router
                .route(&bson::doc! { "find": "emp", "filter": { "_id": id }, "$db": "db" })
                .await,
        )
        .expect("after");
        assert_eq!(after.len(), 1);
        assert_eq!(after[0].get_str("ename"), Ok("MOVED"));
        assert_eq!(count_ids(&second).await, vec![id]);
        assert_eq!(count_ids(&first).await, vec![stays]);
    }

    fn totals(rows: &[Document]) -> BTreeSet<(String, i32, i32)> {
        rows.iter()
            .map(|row| {
                (
                    row.get_str("_id").unwrap_or("").to_string(),
                    row.get_i32("total").unwrap_or(0),
                    row.get_i32("n").unwrap_or(0),
                )
            })
            .collect()
    }
}
