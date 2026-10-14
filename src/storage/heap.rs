//! Documents stored in pages, with the buffer pool as the only cache.
//!
//! A collection is a list of page ids, not a `Vec` of documents. Reading
//! a page pins it, copies the slots the caller asked for, and unpins
//! before the next page. A document bigger than one page is a chain of
//! overflow pages. The catalog page records the applied LSN, the next
//! page id, and index declarations. It does not hold document bodies.

use std::collections::{HashMap, HashSet};

use bson::{doc, Bson, Document};

use super::buffer_pool::BufferPool;
use super::disk::PageStore;
use super::page::Page;
use super::{apply_update, document_id_string, document_matches};

const DOCS_MAGIC: u32 = 0x444F_4353; // DOCS
const CATA_MAGIC: u32 = 0x4341_5441; // CATA
const CATALOG_PAGE: u64 = 1;
const FLAG_OVERFLOW: u16 = 1;
const FLAG_CONTINUATION: u16 = 2;

struct CollectionState {
    /// Normal pages and overflow heads. Continuation pages are not here.
    pages: Vec<u64>,
    overflow_heads: HashSet<u64>,
    continuations: Vec<u64>,
    indexes: Vec<Document>,
    /// Optional document validator. Absent on catalogs written before
    /// transactions existed.
    validator: Option<Document>,
    doc_count: u64,
}

struct UserRecord {
    name: String,
    hash: String,
    roles: Vec<String>,
    enabled: bool,
}

struct RoleRecord {
    name: String,
    actions: Vec<String>,
}

impl CollectionState {
    fn empty() -> Self {
        Self {
            pages: Vec::new(),
            overflow_heads: HashSet::new(),
            continuations: Vec::new(),
            indexes: Vec::new(),
            validator: None,
            doc_count: 0,
        }
    }
}

/// Page-resident collections for one partition.
pub struct Heap {
    /// Decompressed bytes that fit in one page after zstd's worst case.
    payload_capacity: usize,
    applied_lsn: u64,
    next_page_id: u64,
    collections: HashMap<String, CollectionState>,
    live: HashSet<u64>,
    users: Vec<UserRecord>,
    roles: Vec<RoleRecord>,
}

impl Heap {
    pub fn empty(page_size: usize, encrypted: bool) -> Self {
        let payload_capacity = Page::payload_capacity(page_size, encrypted)
            .expect("page size checked when the engine opens");
        Self {
            payload_capacity,
            applied_lsn: 0,
            next_page_id: 2,
            collections: HashMap::new(),
            live: HashSet::new(),
            users: Vec::new(),
            roles: Vec::new(),
        }
    }

    pub fn applied_lsn(&self) -> u64 {
        self.applied_lsn
    }

    pub fn set_applied_lsn(&mut self, lsn: u64) {
        if lsn > self.applied_lsn {
            self.applied_lsn = lsn;
        }
    }

    pub fn next_page_id(&self) -> u64 {
        self.next_page_id
    }

    /// Raises the allocator so a page id from an older manifest is not reused.
    pub fn advance_next_page_id(&mut self, next: u64) {
        self.next_page_id = self.next_page_id.max(next).max(2);
    }

    #[cfg(test)]
    pub fn retained_documents(&self) -> usize {
        0
    }

    pub fn collection_count(&self) -> usize {
        self.collections.len()
    }

    pub fn contains(&self, collection: &str) -> bool {
        self.collections.contains_key(collection)
    }

    pub fn doc_count(&self, collection: &str) -> u64 {
        self.collections
            .get(collection)
            .map(|state| state.doc_count)
            .unwrap_or(0)
    }

    pub fn names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.collections.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn index_specs(&self, collection: &str) -> Option<Vec<Document>> {
        self.collections
            .get(collection)
            .map(|state| state.indexes.clone())
    }

    pub fn index_count(&self, collection: &str) -> usize {
        self.collections
            .get(collection)
            .map(|state| state.indexes.len())
            .unwrap_or(0)
    }

    pub fn live_page_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self.live.iter().copied().collect();
        ids.sort_unstable();
        ids
    }

    fn capacity(&self) -> usize {
        self.payload_capacity
    }

    /// Rebuilds the page lists from files already on disk, then overlays
    /// the catalog. Document bodies are unpinned before the next page.
    pub fn discover(&mut self, pool: &mut BufferPool, page_ids: &[u64]) -> Result<(), String> {
        let mut max_id = 1u64;
        for id in page_ids {
            max_id = max_id.max(*id);
            if *id == CATALOG_PAGE {
                continue;
            }
            let page = pool.fetch(*id)?;
            let payload = page.payload.clone();
            pool.unpin(*id)?;
            self.note_page(*id, &payload)?;
        }
        if page_ids.contains(&CATALOG_PAGE) {
            let page = pool.fetch(CATALOG_PAGE)?;
            let payload = page.payload.clone();
            pool.unpin(CATALOG_PAGE)?;
            self.overlay_catalog(&payload)?;
            self.live.insert(CATALOG_PAGE);
        }
        self.next_page_id = self.next_page_id.max(max_id.saturating_add(1)).max(2);
        Ok(())
    }

    fn note_page(&mut self, id: u64, payload: &[u8]) -> Result<(), String> {
        if payload.len() < 4 {
            return Err(format!("page {id} has no magic"));
        }
        let magic = u32::from_le_bytes(payload[0..4].try_into().unwrap());
        if magic != DOCS_MAGIC {
            return Err(format!(
                "page {id} magic {magic:08x} is not a document page"
            ));
        }
        let flags = u16::from_le_bytes(payload[6..8].try_into().unwrap());
        let name = collection_name(payload)?;
        self.live.insert(id);
        if flags & FLAG_CONTINUATION != 0 {
            self.collections
                .entry(name)
                .or_insert_with(CollectionState::empty)
                .continuations
                .push(id);
            return Ok(());
        }
        let state = self
            .collections
            .entry(name)
            .or_insert_with(CollectionState::empty);
        state.pages.push(id);
        if flags & FLAG_OVERFLOW != 0 {
            state.overflow_heads.insert(id);
            state.doc_count += 1;
        } else {
            state.doc_count += count_slots(payload)?;
        }
        Ok(())
    }

    fn overlay_catalog(&mut self, payload: &[u8]) -> Result<(), String> {
        let body = catalog_body(payload)?;
        if let Ok(lsn) = body.get_i64("applied_lsn") {
            self.applied_lsn = self.applied_lsn.max(lsn as u64);
        }
        if let Ok(next) = body.get_i64("next_page_id") {
            self.next_page_id = self.next_page_id.max(next as u64);
        }
        let Some(cols) = body.get_document("collections").ok() else {
            return Ok(());
        };
        for (name, value) in cols {
            let state = self
                .collections
                .entry(name.clone())
                .or_insert_with(CollectionState::empty);
            if let Some(spec) = value.as_document() {
                if let Ok(indexes) = spec.get_array("indexes") {
                    state.indexes = indexes
                        .iter()
                        .filter_map(|item| item.as_document().cloned())
                        .collect();
                }
                if let Ok(validator) = spec.get_document("validator") {
                    state.validator = Some(validator.clone());
                }
            }
        }
        self.users = body
            .get_array("users")
            .ok()
            .map(|values| values.iter().filter_map(user_from_bson).collect())
            .unwrap_or_default();
        self.roles = body
            .get_array("roles")
            .ok()
            .map(|values| values.iter().filter_map(role_from_bson).collect())
            .unwrap_or_default();
        Ok(())
    }

    pub fn ensure_collection(&mut self, collection: &str) -> bool {
        if self.collections.contains_key(collection) {
            return false;
        }
        self.collections
            .insert(collection.to_string(), CollectionState::empty());
        true
    }

    pub fn validator(&self, collection: &str) -> Option<Document> {
        self.collections
            .get(collection)
            .and_then(|state| state.validator.clone())
    }

    pub fn set_validator(&mut self, collection: &str, validator: Document) {
        self.ensure_collection(collection);
        if let Some(state) = self.collections.get_mut(collection) {
            state.validator = Some(validator);
        }
    }

    pub fn set_indexes(&mut self, collection: &str, indexes: Vec<Document>) {
        self.ensure_collection(collection);
        if let Some(state) = self.collections.get_mut(collection) {
            state.indexes = indexes;
        }
    }

    pub fn can_create_user(
        &self,
        name: &str,
        roles: &[String],
        trusted: bool,
    ) -> Result<(), String> {
        if name.is_empty() {
            return Err("createUser requires a user name".into());
        }
        if roles.is_empty() {
            return Err("createUser requires at least one role".into());
        }
        self.known_roles(roles)?;
        if self.users.iter().any(|user| user.name == name) {
            return Err(format!("user {name} already exists"));
        }
        if self.users.is_empty() && !roles.iter().any(|role| role == "root") {
            return Err("the first user must have the root role".into());
        }
        if !self.users.is_empty() && !trusted {
            return Err("authentication required".into());
        }
        Ok(())
    }

    pub fn create_user(
        &mut self,
        name: &str,
        hash: &str,
        roles: &[String],
        trusted: bool,
    ) -> Result<(), String> {
        if name.is_empty() {
            return Err("createUser requires a user name".into());
        }
        if roles.is_empty() {
            return Err("createUser requires at least one role".into());
        }
        self.can_create_user(name, roles, trusted)?;
        self.users.push(UserRecord {
            name: name.to_string(),
            hash: hash.to_string(),
            roles: roles.to_vec(),
            enabled: true,
        });
        Ok(())
    }

    pub fn can_update_user(&self, name: &str, enabled: bool) -> Result<(), String> {
        let user = self
            .users
            .iter()
            .find(|user| user.name == name)
            .ok_or_else(|| format!("user {name} does not exist"))?;
        if !enabled
            && user.enabled
            && user.roles.iter().any(|role| role == "root")
            && self.enabled_roots() == 1
        {
            return Err("cannot disable the last root".into());
        }
        Ok(())
    }

    pub fn update_user(&mut self, name: &str, enabled: bool) -> Result<(), String> {
        self.can_update_user(name, enabled)?;
        let user = self
            .users
            .iter_mut()
            .find(|user| user.name == name)
            .expect("user checked above");
        user.enabled = enabled;
        Ok(())
    }

    pub fn can_grant_roles(
        &self,
        name: &str,
        roles: &[String],
        revoke: bool,
    ) -> Result<(), String> {
        self.known_roles(roles)?;
        let user = self
            .users
            .iter()
            .find(|user| user.name == name)
            .ok_or_else(|| format!("user {name} does not exist"))?;
        if !revoke {
            return Ok(());
        }
        let would_drop_root =
            user.roles.iter().any(|role| role == "root") && roles.iter().any(|role| role == "root");
        if would_drop_root && user.enabled && self.enabled_roots() == 1 {
            return Err("cannot revoke root from the last root".into());
        }
        let remaining = user
            .roles
            .iter()
            .filter(|role| !roles.iter().any(|drop| drop == *role))
            .count();
        if remaining == 0 {
            return Err("a user must keep at least one role".into());
        }
        Ok(())
    }

    pub fn grant_roles(
        &mut self,
        name: &str,
        roles: &[String],
        revoke: bool,
    ) -> Result<(), String> {
        self.can_grant_roles(name, roles, revoke)?;
        let user = self
            .users
            .iter_mut()
            .find(|user| user.name == name)
            .expect("user checked above");
        if revoke {
            user.roles
                .retain(|role| !roles.iter().any(|drop| drop == role));
        } else {
            for role in roles {
                if !user.roles.iter().any(|existing| existing == role) {
                    user.roles.push(role.clone());
                }
            }
        }
        Ok(())
    }

    pub fn can_create_role(&self, name: &str, actions: &[String]) -> Result<(), String> {
        if name.is_empty() {
            return Err("createRole requires a role name".into());
        }
        if crate::security::is_builtin(name) {
            return Err(format!("built-in role {name} cannot be created"));
        }
        if self.roles.iter().any(|role| role.name == name) {
            return Err(format!("role {name} already exists"));
        }
        if actions.is_empty() {
            return Err("createRole requires at least one action".into());
        }
        for action in actions {
            crate::security::Action::parse(action)?;
        }
        Ok(())
    }

    pub fn create_role(&mut self, name: &str, actions: &[String]) -> Result<(), String> {
        self.can_create_role(name, actions)?;
        self.roles.push(RoleRecord {
            name: name.to_string(),
            actions: actions.to_vec(),
        });
        Ok(())
    }

    pub fn can_drop_role(&self, name: &str) -> Result<(), String> {
        if crate::security::is_builtin(name) {
            return Err("built-in role cannot be dropped".into());
        }
        if !self.roles.iter().any(|role| role.name == name) {
            return Err(format!("role {name} does not exist"));
        }
        if self
            .users
            .iter()
            .any(|user| user.roles.iter().any(|role| role == name))
        {
            return Err(format!("role {name} is still granted"));
        }
        Ok(())
    }

    pub fn drop_role(&mut self, name: &str) -> Result<(), String> {
        self.can_drop_role(name)?;
        self.roles.retain(|role| role.name != name);
        Ok(())
    }

    /// Checks the password. Unknown, disabled, and wrong passwords share
    /// one error string.
    pub fn authenticate(&self, name: &str, password: &str) -> Result<Vec<String>, String> {
        let failed = || Err("authentication failed".to_string());
        let Some(user) = self.users.iter().find(|user| user.name == name) else {
            let _ = crate::security::passwords_match(password, crate::security::dummy_hash());
            return failed();
        };
        let matches = crate::security::passwords_match(password, &user.hash);
        if !user.enabled || !matches {
            return failed();
        }
        Ok(user.roles.clone())
    }

    pub fn custom_roles(&self) -> Vec<(String, Vec<String>)> {
        self.roles
            .iter()
            .map(|role| (role.name.clone(), role.actions.clone()))
            .collect()
    }

    fn known_roles(&self, roles: &[String]) -> Result<(), String> {
        for role in roles {
            if crate::security::is_builtin(role) {
                continue;
            }
            if !self.roles.iter().any(|custom| custom.name == *role) {
                return Err(format!("unknown role {role}"));
            }
        }
        Ok(())
    }

    fn enabled_roots(&self) -> usize {
        self.users
            .iter()
            .filter(|user| user.enabled && user.roles.iter().any(|role| role == "root"))
            .count()
    }

    pub fn persist_catalog(&mut self, pool: &mut BufferPool) -> Result<(), String> {
        let payload = self.encode_catalog()?;
        if payload.len() > self.capacity() {
            return Err(format!(
                "catalog is {} bytes and does not fit in a {} byte page",
                payload.len(),
                self.capacity()
            ));
        }
        pool.insert(Page::new(CATALOG_PAGE, self.applied_lsn, payload))?;
        self.live.insert(CATALOG_PAGE);
        Ok(())
    }

    fn encode_catalog(&self) -> Result<Vec<u8>, String> {
        let mut names: Vec<&String> = self.collections.keys().collect();
        names.sort();
        let mut cols = Document::new();
        for name in names {
            let state = &self.collections[name];
            let mut spec = doc! {
                "indexes": Bson::Array(state.indexes.iter().cloned().map(Bson::Document).collect()),
            };
            if let Some(validator) = &state.validator {
                spec.insert("validator", validator.clone());
            }
            cols.insert(name, spec);
        }
        let body = doc! {
            "applied_lsn": self.applied_lsn as i64,
            "next_page_id": self.next_page_id as i64,
            "collections": cols,
            "users": Bson::Array(self.users.iter().map(user_to_bson).collect()),
            "roles": Bson::Array(self.roles.iter().map(role_to_bson).collect()),
        };
        let bson = bson::to_vec(&body).map_err(|err| format!("catalog encode: {err}"))?;
        let mut payload = Vec::with_capacity(4 + bson.len());
        payload.extend_from_slice(&CATA_MAGIC.to_le_bytes());
        payload.extend_from_slice(&bson);
        Ok(payload)
    }

    pub fn insert(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        documents: Vec<Document>,
    ) -> Result<Vec<Document>, String> {
        self.ensure_collection(collection);
        let mut stored = Vec::new();
        for document in documents {
            let id = document_id_string(&document);
            if self.contains_id(pool, collection, &id)? {
                continue;
            }
            self.place(pool, collection, &document)?;
            stored.push(document);
        }
        Ok(stored)
    }

    pub fn find(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        filter: &Document,
    ) -> Result<Vec<Document>, String> {
        let mut matches = Vec::new();
        self.for_each_in(pool, collection, &mut |doc| {
            if document_matches(doc, filter) {
                matches.push(doc.clone());
            }
        })?;
        Ok(matches)
    }

    pub fn count(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        filter: &Document,
    ) -> Result<u64, String> {
        let mut count = 0u64;
        self.for_each_in(pool, collection, &mut |doc| {
            if document_matches(doc, filter) {
                count += 1;
            }
        })?;
        Ok(count)
    }

    /// Keeps documents whose id is in `ids` and that still match `filter`.
    ///
    /// The index names the ids. This walk still reads pages through the
    /// buffer pool; it does not jump to a page by pointer, because an
    /// update can move a document. An empty `ids` list is an empty answer
    /// and does not read any page.
    pub fn find_ids(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        ids: &[String],
        filter: &Document,
    ) -> Result<Vec<Document>, String> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let wanted: HashSet<&str> = ids.iter().map(String::as_str).collect();
        let mut matches = Vec::new();
        self.for_each_in(pool, collection, &mut |doc| {
            if wanted.contains(document_id_string(doc).as_str()) && document_matches(doc, filter) {
                matches.push(doc.clone());
            }
        })?;
        Ok(matches)
    }

    pub fn find_by_id(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        id: &str,
    ) -> Result<Option<Document>, String> {
        let mut found = None;
        self.for_each_in(pool, collection, &mut |doc| {
            if found.is_none() && document_id_string(doc) == id {
                found = Some(doc.clone());
            }
        })?;
        Ok(found)
    }

    pub fn for_each_document(
        &mut self,
        pool: &mut BufferPool,
        mut visit: impl FnMut(&str, &Document),
    ) -> Result<(), String> {
        let names = self.names();
        for name in names {
            let pages = self
                .collections
                .get(&name)
                .map(|state| state.pages.clone())
                .unwrap_or_default();
            let heads = self
                .collections
                .get(&name)
                .map(|state| state.overflow_heads.clone())
                .unwrap_or_default();
            for page_id in pages {
                if heads.contains(&page_id) {
                    let doc = self.read_overflow(pool, page_id)?;
                    visit(&name, &doc);
                } else {
                    let docs = self.read_docs(pool, page_id)?;
                    for doc in &docs {
                        visit(&name, doc);
                    }
                }
            }
        }
        Ok(())
    }

    /// Walks one page at a time. `visit` must not retain the document
    /// beyond the call if the caller wants the body to stay off the heap
    /// store; the page itself is unpinned before the next one is read.
    pub fn for_each_in(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        visit: &mut dyn FnMut(&Document),
    ) -> Result<(), String> {
        let pages = self
            .collections
            .get(collection)
            .map(|state| state.pages.clone())
            .unwrap_or_default();
        let heads = self
            .collections
            .get(collection)
            .map(|state| state.overflow_heads.clone())
            .unwrap_or_default();
        for page_id in pages {
            if heads.contains(&page_id) {
                let doc = self.read_overflow(pool, page_id)?;
                visit(&doc);
                continue;
            }
            let docs = self.read_docs(pool, page_id)?;
            for doc in &docs {
                visit(doc);
            }
        }
        Ok(())
    }

    pub fn update(
        &mut self,
        pool: &mut BufferPool,
        disk: &PageStore,
        collection: &str,
        filter: &Document,
        update: &Document,
        multi: bool,
    ) -> Result<(u32, Vec<Document>), String> {
        if !self.contains(collection) {
            return Ok((0, Vec::new()));
        }
        let pages = self.collections[collection].pages.clone();
        let heads = self.collections[collection].overflow_heads.clone();
        let mut matched = 0u32;
        let mut updated = Vec::new();
        for page_id in pages {
            if !multi && matched > 0 {
                break;
            }
            if heads.contains(&page_id) {
                let mut doc = self.read_overflow(pool, page_id)?;
                if document_matches(&doc, filter) {
                    matched += 1;
                    apply_update(&mut doc, update)?;
                    self.rewrite_overflow(pool, disk, collection, page_id, &doc)?;
                    updated.push(doc);
                }
                continue;
            }
            let mut docs = self.read_docs(pool, page_id)?;
            let mut changed = false;
            for doc in &mut docs {
                if document_matches(doc, filter) && (multi || matched == 0) {
                    matched += 1;
                    apply_update(doc, update)?;
                    updated.push(doc.clone());
                    changed = true;
                    if !multi {
                        break;
                    }
                }
            }
            if changed {
                self.write_docs(pool, collection, page_id, docs)?;
            }
        }
        Ok((matched, updated))
    }

    pub fn delete(
        &mut self,
        pool: &mut BufferPool,
        disk: &PageStore,
        collection: &str,
        filter: &Document,
        limit: u32,
    ) -> Result<Vec<Document>, String> {
        if !self.contains(collection) {
            return Ok(Vec::new());
        }
        let pages = self.collections[collection].pages.clone();
        let heads = self.collections[collection].overflow_heads.clone();
        let mut removed = Vec::new();
        for page_id in pages {
            if limit > 0 && removed.len() as u32 >= limit {
                break;
            }
            if heads.contains(&page_id) {
                let doc = self.read_overflow(pool, page_id)?;
                let take = document_matches(&doc, filter)
                    && (limit == 0 || (removed.len() as u32) < limit);
                if take {
                    removed.push(doc);
                    self.remove_overflow_head(pool, disk, collection, page_id)?;
                }
                continue;
            }
            let docs = self.read_docs(pool, page_id)?;
            let before = removed.len();
            let mut kept = Vec::new();
            let mut changed = false;
            for doc in docs {
                let take = document_matches(&doc, filter)
                    && (limit == 0 || (removed.len() as u32) < limit);
                if take {
                    removed.push(doc);
                    changed = true;
                } else {
                    kept.push(doc);
                }
            }
            if changed {
                let removed_here = removed.len() - before;
                self.write_docs(pool, collection, page_id, kept)?;
                if let Some(state) = self.collections.get_mut(collection) {
                    state.doc_count = state.doc_count.saturating_sub(removed_here as u64);
                }
            }
        }
        Ok(removed)
    }

    pub fn drop_collection(
        &mut self,
        pool: &mut BufferPool,
        disk: &PageStore,
        collection: &str,
    ) -> Result<bool, String> {
        let Some(state) = self.collections.remove(collection) else {
            return Ok(false);
        };
        for id in state
            .pages
            .into_iter()
            .chain(state.continuations.into_iter())
        {
            pool.discard(id);
            disk.remove(id)?;
            self.live.remove(&id);
        }
        Ok(true)
    }

    pub fn drop_database(
        &mut self,
        pool: &mut BufferPool,
        disk: &PageStore,
        database: &str,
    ) -> Result<Vec<String>, String> {
        let prefix = format!("{database}.");
        let doomed: Vec<String> = self
            .collections
            .keys()
            .filter(|key| key.starts_with(&prefix) || (database == "test" && !key.contains('.')))
            .cloned()
            .collect();
        for name in &doomed {
            self.drop_collection(pool, disk, name)?;
        }
        Ok(doomed)
    }

    pub fn bson_size(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
    ) -> Result<Option<(u64, u64)>, String> {
        if !self.contains(collection) {
            return Ok(None);
        }
        let mut count = 0u64;
        let mut size = 0u64;
        self.for_each_in(pool, collection, &mut |doc| {
            count += 1;
            size += bson::to_vec(doc)
                .map(|bytes| bytes.len() as u64)
                .unwrap_or(0);
        })?;
        if let Some(state) = self.collections.get_mut(collection) {
            state.doc_count = count;
        }
        Ok(Some((count, size)))
    }

    fn contains_id(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        id: &str,
    ) -> Result<bool, String> {
        let mut found = false;
        self.for_each_in(pool, collection, &mut |doc| {
            if document_id_string(doc) == id {
                found = true;
            }
        })?;
        Ok(found)
    }

    fn place(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        document: &Document,
    ) -> Result<(), String> {
        let pages = self.collections[collection].pages.clone();
        let heads = self.collections[collection].overflow_heads.clone();
        for page_id in pages {
            if heads.contains(&page_id) {
                continue;
            }
            let mut docs = self.read_docs(pool, page_id)?;
            docs.push(document.clone());
            if encode_docs(collection, &docs, self.capacity()).is_ok() {
                self.write_normal(pool, collection, page_id, &docs)?;
                self.collections.get_mut(collection).unwrap().doc_count += 1;
                return Ok(());
            }
        }
        if encode_docs(collection, std::slice::from_ref(document), self.capacity()).is_ok() {
            let payload = encode_docs(collection, std::slice::from_ref(document), self.capacity())?;
            let id = self.alloc(pool, payload)?;
            let state = self.collections.get_mut(collection).unwrap();
            state.pages.push(id);
            state.doc_count += 1;
            return Ok(());
        }
        self.attach_overflow(pool, collection, document)?;
        self.collections.get_mut(collection).unwrap().doc_count += 1;
        Ok(())
    }

    /// Writes `docs` back to `page_id`. Documents that no longer fit spill
    /// onto overflow pages. `doc_count` is left unchanged: these documents
    /// were already counted.
    fn write_docs(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        page_id: u64,
        docs: Vec<Document>,
    ) -> Result<(), String> {
        if encode_docs(collection, &docs, self.capacity()).is_ok() {
            return self.write_normal(pool, collection, page_id, &docs);
        }
        let mut kept = Vec::new();
        let mut spill = Vec::new();
        for doc in docs {
            kept.push(doc.clone());
            if encode_docs(collection, &kept, self.capacity()).is_err() {
                kept.pop();
                spill.push(doc);
            }
        }
        self.write_normal(pool, collection, page_id, &kept)?;
        for doc in spill {
            self.attach_overflow(pool, collection, &doc)?;
        }
        Ok(())
    }

    fn write_normal(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        page_id: u64,
        docs: &[Document],
    ) -> Result<(), String> {
        let payload = encode_docs(collection, docs, self.capacity())?;
        pool.insert(Page::new(page_id, self.applied_lsn, payload))?;
        self.live.insert(page_id);
        Ok(())
    }

    fn attach_overflow(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        document: &Document,
    ) -> Result<(), String> {
        let bytes = bson::to_vec(document).map_err(|err| format!("document encode: {err}"))?;
        let ids = self.alloc_overflow_chain(pool, collection, &bytes)?;
        let head = ids[0];
        let state = self.collections.get_mut(collection).unwrap();
        state.pages.push(head);
        state.overflow_heads.insert(head);
        for id in ids.into_iter().skip(1) {
            state.continuations.push(id);
        }
        Ok(())
    }

    /// Rewrites an overflow document without changing its head page id.
    /// Extra chunks reuse the old continuation pages. A shorter document
    /// deletes the tail. A longer one allocates new continuation pages.
    fn rewrite_overflow(
        &mut self,
        pool: &mut BufferPool,
        disk: &PageStore,
        collection: &str,
        head: u64,
        document: &Document,
    ) -> Result<(), String> {
        let bytes = bson::to_vec(document).map_err(|err| format!("document encode: {err}"))?;
        let old = self.chain_ids(pool, head)?;
        let overhead = overflow_overhead(collection)?;
        let chunk_cap = self
            .capacity()
            .checked_sub(overhead)
            .filter(|n| *n > 0)
            .ok_or_else(|| {
                format!(
                    "page payload of {} bytes cannot hold an overflow header",
                    self.capacity()
                )
            })?;
        let chunks: Vec<&[u8]> = if bytes.is_empty() {
            vec![&[]]
        } else {
            bytes.chunks(chunk_cap).collect()
        };
        let mut ids = Vec::with_capacity(chunks.len());
        ids.push(head);
        for index in 1..chunks.len() {
            if index < old.len() {
                ids.push(old[index]);
            } else {
                let id = self.next_page_id;
                self.next_page_id += 1;
                ids.push(id);
            }
        }
        for (index, chunk) in chunks.iter().enumerate() {
            let next = if index + 1 < ids.len() {
                ids[index + 1]
            } else {
                0
            };
            let flags = if index == 0 {
                FLAG_OVERFLOW
            } else {
                FLAG_OVERFLOW | FLAG_CONTINUATION
            };
            let payload = encode_overflow(collection, flags, next, chunk)?;
            pool.insert(Page::new(ids[index], self.applied_lsn, payload))?;
            self.live.insert(ids[index]);
        }
        let dropped: Vec<u64> = old.iter().skip(chunks.len()).copied().collect();
        for id in &dropped {
            pool.discard(*id);
            disk.remove(*id)?;
            self.live.remove(id);
        }
        if let Some(state) = self.collections.get_mut(collection) {
            let dropped_set: HashSet<u64> = dropped.iter().copied().collect();
            state.continuations.retain(|id| !dropped_set.contains(id));
            for id in ids.iter().skip(1) {
                if !state.continuations.contains(id) {
                    state.continuations.push(*id);
                }
            }
        }
        Ok(())
    }

    fn remove_overflow_head(
        &mut self,
        pool: &mut BufferPool,
        disk: &PageStore,
        collection: &str,
        head: u64,
    ) -> Result<(), String> {
        let chain = self.chain_ids(pool, head)?;
        if let Some(state) = self.collections.get_mut(collection) {
            state.pages.retain(|id| *id != head);
            state.overflow_heads.remove(&head);
            state.continuations.retain(|id| !chain.contains(id));
            state.doc_count = state.doc_count.saturating_sub(1);
        }
        for id in chain {
            pool.discard(id);
            disk.remove(id)?;
            self.live.remove(&id);
        }
        Ok(())
    }

    fn read_docs(&mut self, pool: &mut BufferPool, page_id: u64) -> Result<Vec<Document>, String> {
        let payload = self.read_payload(pool, page_id)?;
        decode_docs(&payload)
    }

    fn read_overflow(&mut self, pool: &mut BufferPool, head: u64) -> Result<Document, String> {
        let mut id = head;
        let mut bytes = Vec::new();
        let mut steps = 0u32;
        while id != 0 {
            steps += 1;
            if steps > 100_000 {
                return Err(format!("overflow chain from page {head} does not end"));
            }
            let payload = self.read_payload(pool, id)?;
            let (next, chunk) = decode_overflow_chunk(&payload)?;
            bytes.extend_from_slice(&chunk);
            id = next;
        }
        bson::from_slice(&bytes).map_err(|err| format!("overflow document on page {head}: {err}"))
    }

    fn chain_ids(&mut self, pool: &mut BufferPool, head: u64) -> Result<Vec<u64>, String> {
        let mut ids = Vec::new();
        let mut id = head;
        let mut steps = 0u32;
        while id != 0 {
            steps += 1;
            if steps > 100_000 {
                return Err("overflow chain does not end".into());
            }
            ids.push(id);
            let payload = self.read_payload(pool, id)?;
            let (next, _) = decode_overflow_chunk(&payload)?;
            id = next;
        }
        Ok(ids)
    }

    fn read_payload(&mut self, pool: &mut BufferPool, page_id: u64) -> Result<Vec<u8>, String> {
        let page = pool.fetch(page_id)?;
        let payload = page.payload.clone();
        pool.unpin(page_id)?;
        Ok(payload)
    }

    fn alloc(&mut self, pool: &mut BufferPool, payload: Vec<u8>) -> Result<u64, String> {
        let id = self.next_page_id;
        self.next_page_id += 1;
        pool.insert(Page::new(id, self.applied_lsn, payload))?;
        self.live.insert(id);
        Ok(id)
    }

    fn alloc_overflow_chain(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        bytes: &[u8],
    ) -> Result<Vec<u64>, String> {
        let overhead = overflow_overhead(collection)?;
        let chunk_cap = self
            .capacity()
            .checked_sub(overhead)
            .filter(|n| *n > 0)
            .ok_or_else(|| {
                format!(
                    "page payload of {} bytes cannot hold an overflow header",
                    self.capacity()
                )
            })?;
        let chunks: Vec<&[u8]> = if bytes.is_empty() {
            vec![&[]]
        } else {
            bytes.chunks(chunk_cap).collect()
        };
        let mut ids = Vec::with_capacity(chunks.len());
        for _ in &chunks {
            ids.push(self.next_page_id);
            self.next_page_id += 1;
        }
        for (index, chunk) in chunks.iter().enumerate() {
            let next = if index + 1 < ids.len() {
                ids[index + 1]
            } else {
                0
            };
            let flags = if index == 0 {
                FLAG_OVERFLOW
            } else {
                FLAG_OVERFLOW | FLAG_CONTINUATION
            };
            let payload = encode_overflow(collection, flags, next, chunk)?;
            pool.insert(Page::new(ids[index], self.applied_lsn, payload))?;
            self.live.insert(ids[index]);
        }
        Ok(ids)
    }
}

fn collection_name(payload: &[u8]) -> Result<String, String> {
    if payload.len() < 10 {
        return Err("document page header is truncated".into());
    }
    let name_len = u16::from_le_bytes(payload[8..10].try_into().unwrap()) as usize;
    let start = 10;
    let end = start + name_len;
    if payload.len() < end {
        return Err("document page name is truncated".into());
    }
    let name = std::str::from_utf8(&payload[start..end])
        .map_err(|_| "document page name is not utf-8".to_string())?;
    Ok(name.to_string())
}

fn count_slots(payload: &[u8]) -> Result<u64, String> {
    let name_len = u16::from_le_bytes(payload[8..10].try_into().unwrap()) as usize;
    let at = 10 + name_len;
    if payload.len() < at + 2 {
        return Err("document page slot count is truncated".into());
    }
    let count = u16::from_le_bytes(payload[at..at + 2].try_into().unwrap());
    let mut live = 0u64;
    let mut slot_at = at + 2;
    for _ in 0..count {
        if payload.len() < slot_at + 8 {
            return Err("document page slot directory is truncated".into());
        }
        let len = u32::from_le_bytes(payload[slot_at + 4..slot_at + 8].try_into().unwrap());
        if len > 0 {
            live += 1;
        }
        slot_at += 8;
    }
    Ok(live)
}

fn encode_docs(collection: &str, docs: &[Document], capacity: usize) -> Result<Vec<u8>, String> {
    let mut bodies = Vec::with_capacity(docs.len());
    for doc in docs {
        bodies.push(bson::to_vec(doc).map_err(|err| format!("document encode: {err}"))?);
    }
    let name = collection.as_bytes();
    if name.len() > u16::MAX as usize {
        return Err("collection name is too long for a page header".into());
    }
    if bodies.len() > u16::MAX as usize {
        return Err("too many documents for one page".into());
    }
    let dir = 10 + name.len() + 2 + 8 * bodies.len();
    let mut size = dir;
    for body in &bodies {
        size += body.len();
    }
    if size > capacity {
        return Err("documents do not fit in one page".into());
    }
    let mut buf = vec![0u8; size];
    buf[0..4].copy_from_slice(&DOCS_MAGIC.to_le_bytes());
    buf[4..6].copy_from_slice(&1u16.to_le_bytes());
    buf[6..8].copy_from_slice(&0u16.to_le_bytes());
    buf[8..10].copy_from_slice(&(name.len() as u16).to_le_bytes());
    buf[10..10 + name.len()].copy_from_slice(name);
    let mut at = 10 + name.len();
    buf[at..at + 2].copy_from_slice(&(bodies.len() as u16).to_le_bytes());
    at += 2;
    let slots_at = at;
    at += 8 * bodies.len();
    for (index, body) in bodies.iter().enumerate() {
        let slot = slots_at + index * 8;
        buf[slot..slot + 4].copy_from_slice(&(at as u32).to_le_bytes());
        buf[slot + 4..slot + 8].copy_from_slice(&(body.len() as u32).to_le_bytes());
        buf[at..at + body.len()].copy_from_slice(body);
        at += body.len();
    }
    Ok(buf)
}

fn decode_docs(payload: &[u8]) -> Result<Vec<Document>, String> {
    if payload.len() < 10 {
        return Err("document page is truncated".into());
    }
    let magic = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    if magic != DOCS_MAGIC {
        return Err(format!("document page magic {magic:08x} is not DOCS"));
    }
    let flags = u16::from_le_bytes(payload[6..8].try_into().unwrap());
    if flags & FLAG_OVERFLOW != 0 {
        return Err("overflow page has no slot directory".into());
    }
    let name_len = u16::from_le_bytes(payload[8..10].try_into().unwrap()) as usize;
    let mut at = 10 + name_len;
    if payload.len() < at + 2 {
        return Err("document page slot count is truncated".into());
    }
    let count = u16::from_le_bytes(payload[at..at + 2].try_into().unwrap()) as usize;
    at += 2;
    let mut slots = Vec::with_capacity(count);
    for _ in 0..count {
        if payload.len() < at + 8 {
            return Err("document page slot directory is truncated".into());
        }
        let offset = u32::from_le_bytes(payload[at..at + 4].try_into().unwrap()) as usize;
        let len = u32::from_le_bytes(payload[at + 4..at + 8].try_into().unwrap()) as usize;
        slots.push((offset, len));
        at += 8;
    }
    let mut docs = Vec::new();
    for (offset, len) in slots {
        if len == 0 {
            continue;
        }
        if offset + len > payload.len() {
            return Err("document slot points outside the page".into());
        }
        let doc = bson::from_slice(&payload[offset..offset + len])
            .map_err(|err| format!("document slot: {err}"))?;
        docs.push(doc);
    }
    Ok(docs)
}

fn overflow_overhead(collection: &str) -> Result<usize, String> {
    let name_len = collection.len();
    if name_len > u16::MAX as usize {
        return Err("collection name is too long for a page header".into());
    }
    Ok(10 + name_len + 8 + 4)
}

fn encode_overflow(
    collection: &str,
    flags: u16,
    next: u64,
    chunk: &[u8],
) -> Result<Vec<u8>, String> {
    let name = collection.as_bytes();
    let mut buf = Vec::with_capacity(overflow_overhead(collection)? + chunk.len());
    buf.extend_from_slice(&DOCS_MAGIC.to_le_bytes());
    buf.extend_from_slice(&1u16.to_le_bytes());
    buf.extend_from_slice(&flags.to_le_bytes());
    buf.extend_from_slice(&(name.len() as u16).to_le_bytes());
    buf.extend_from_slice(name);
    buf.extend_from_slice(&next.to_le_bytes());
    buf.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
    buf.extend_from_slice(chunk);
    Ok(buf)
}

fn decode_overflow_chunk(payload: &[u8]) -> Result<(u64, Vec<u8>), String> {
    if payload.len() < 10 {
        return Err("overflow page is truncated".into());
    }
    let name_len = u16::from_le_bytes(payload[8..10].try_into().unwrap()) as usize;
    let at = 10 + name_len;
    if payload.len() < at + 12 {
        return Err("overflow page chunk header is truncated".into());
    }
    let next = u64::from_le_bytes(payload[at..at + 8].try_into().unwrap());
    let len = u32::from_le_bytes(payload[at + 8..at + 12].try_into().unwrap()) as usize;
    let start = at + 12;
    if payload.len() < start + len {
        return Err("overflow chunk is truncated".into());
    }
    Ok((next, payload[start..start + len].to_vec()))
}

fn user_to_bson(user: &UserRecord) -> Bson {
    Bson::Document(doc! {
        "name": user.name.clone(),
        "hash": user.hash.clone(),
        "roles": user.roles.clone(),
        "enabled": user.enabled,
    })
}

fn role_to_bson(role: &RoleRecord) -> Bson {
    Bson::Document(doc! {
        "name": role.name.clone(),
        "actions": role.actions.clone(),
    })
}

fn user_from_bson(value: &Bson) -> Option<UserRecord> {
    let doc = value.as_document()?;
    Some(UserRecord {
        name: doc.get_str("name").ok()?.to_string(),
        hash: doc.get_str("hash").ok()?.to_string(),
        roles: string_list(doc, "roles"),
        enabled: doc.get_bool("enabled").unwrap_or(true),
    })
}

fn role_from_bson(value: &Bson) -> Option<RoleRecord> {
    let doc = value.as_document()?;
    Some(RoleRecord {
        name: doc.get_str("name").ok()?.to_string(),
        actions: string_list(doc, "actions"),
    })
}

fn string_list(doc: &Document, key: &str) -> Vec<String> {
    doc.get_array(key)
        .ok()
        .map(|values| {
            values
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn catalog_body(payload: &[u8]) -> Result<Document, String> {
    if payload.len() < 4 {
        return Err("catalog page is empty".into());
    }
    let magic = u32::from_le_bytes(payload[0..4].try_into().unwrap());
    if magic != CATA_MAGIC {
        return Err(format!("catalog magic {magic:08x} is not CATA"));
    }
    bson::from_slice(&payload[4..]).map_err(|err| format!("catalog decode: {err}"))
}
