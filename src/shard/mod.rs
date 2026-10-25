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

    /// Index into the config list of the shard that stores `id`.
    pub fn owner_index(&self, id: &Bson) -> usize {
        let point = hash_id(id);
        let slot = self.nodes.partition_point(|(node, _)| *node < point);
        let slot = if slot == self.nodes.len() { 0 } else { slot };
        let shard_id = self.nodes[slot].1;
        self.order
            .iter()
            .position(|candidate| *candidate == shard_id)
            .expect("ring shard is one of the configured ids")
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
            if let Some(message) = aggregate_refusal(command) {
                return fail(&message);
            }
            return self.route_cursor(command).await;
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
            return self
                .link
                .call(self.ring.owner_index(id), command.clone())
                .await;
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

/// Stages the router can concatenate. A reducing stage would group
/// inside each shard and report those groups as the answer.
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
}
