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
const INDX_MAGIC: u32 = 0x494E_4458; // INDX
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
    /// `_id` values already stored. Insert checks this before writing,
    /// so a duplicate is refused without rereading every page.
    ids: HashSet<String>,
    /// Page that holds each id. An index hit reads this page. A missing
    /// id reads none. A document that moves to another page updates the
    /// entry here.
    locations: HashMap<String, u64>,
    /// Top-level field name and BSON type. A later value of another
    /// type becomes `"mixed"`.
    fields: Vec<(String, String)>,
    /// Autoincrement field and the last number handed out. The next
    /// insert that omits the field stores `last + 1`.
    counters: Vec<(String, i64)>,
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

/// A MongoDB view: a saved pipeline over `source`, not a copy of the documents.
/// `kova` holds engine options. `inverted` is the one this phase reads.
#[derive(Clone)]
pub(super) struct ViewSpec {
    pub name: String,
    pub source: String,
    pub pipeline: Vec<Document>,
    pub kova: Document,
}

impl ViewSpec {
    pub fn inverted(&self) -> bool {
        self.kova.get_bool("inverted").unwrap_or(false)
    }
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
            ids: HashSet::new(),
            locations: HashMap::new(),
            fields: Vec::new(),
            counters: Vec::new(),
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
    views: Vec<ViewSpec>,
    /// Pages that hold the B-tree and hash snapshot, in order.
    index_pages: Vec<u64>,
    /// Set when startup read a version-2 catalog instead of scanning
    /// document pages.
    catalog_warm: bool,
    /// User-document pages read while discovering or walking a collection.
    user_page_reads: u64,
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
            views: Vec::new(),
            index_pages: Vec::new(),
            catalog_warm: false,
            user_page_reads: 0,
        }
    }

    pub fn catalog_is_warm(&self) -> bool {
        self.catalog_warm
    }

    #[cfg(test)]
    pub fn user_page_reads(&self) -> u64 {
        self.user_page_reads
    }

    #[cfg(test)]
    pub fn field_types(&self, collection: &str) -> Vec<(String, String)> {
        self.collections
            .get(collection)
            .map(|state| state.fields.clone())
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub fn clear_fields(&mut self, collection: &str) {
        if let Some(state) = self.collections.get_mut(collection) {
            state.fields.clear();
        }
    }

    /// Makes the `admin` collections exist in memory so
    /// `listDatabases` can return `admin` before any user write.
    pub fn ensure_catalog(&mut self) {
        for name in crate::storage::catalog::NAMES {
            self.collections
                .entry(name.to_string())
                .or_insert_with(CollectionState::empty);
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
        self.collect_ids(pool)?;
        Ok(())
    }

    /// One pass over the pages so later inserts can see an existing `_id`
    /// without walking the collection again. Each id records its page.
    fn collect_ids(&mut self, pool: &mut BufferPool) -> Result<(), String> {
        let names: Vec<String> = self.collections.keys().cloned().collect();
        for name in names {
            self.fill_locations(pool, &name)?;
        }
        Ok(())
    }

    fn note_page(&mut self, id: u64, payload: &[u8]) -> Result<(), String> {
        if payload.len() < 4 {
            return Err(format!("page {id} has no magic"));
        }
        let magic = u32::from_le_bytes(payload[0..4].try_into().unwrap());
        if magic == INDX_MAGIC {
            self.live.insert(id);
            if !self.index_pages.contains(&id) {
                self.index_pages.push(id);
            }
            return Ok(());
        }
        if magic != DOCS_MAGIC {
            return Err(format!(
                "page {id} magic {magic:08x} is not a document page"
            ));
        }
        let flags = u16::from_le_bytes(payload[6..8].try_into().unwrap());
        let name = collection_name(payload)?;
        if !crate::storage::catalog::is_catalog(&name) {
            self.user_page_reads += 1;
        }
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
        self.views = body
            .get_array("views")
            .ok()
            .map(|values| values.iter().filter_map(view_from_bson).collect())
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

    /// Records an autoincrement field. A field that already has a counter
    /// keeps its `last` value.
    pub fn declare_counter(&mut self, collection: &str, field: &str) {
        self.ensure_collection(collection);
        let Some(state) = self.collections.get_mut(collection) else {
            return;
        };
        if !state.counters.iter().any(|(name, _)| name == field) {
            state.counters.push((field.to_string(), 0));
        }
        if !state.fields.iter().any(|(name, _)| name == field) {
            state.fields.push((field.to_string(), "int".to_string()));
        }
    }

    pub fn set_counter_last(&mut self, collection: &str, field: &str, last: i64) -> bool {
        let Some(state) = self.collections.get_mut(collection) else {
            return false;
        };
        let Some((_, slot)) = state.counters.iter_mut().find(|(name, _)| name == field) else {
            return false;
        };
        *slot = last;
        true
    }

    /// Writes the next number into fields the document left out.
    ///
    /// `already` holds documents staged ahead of this batch, so a second
    /// insert in the same transaction does not draw the same number.
    /// The stored `last` changes when the document is applied.
    pub fn fill_autoincrement(
        &self,
        collection: &str,
        documents: &mut [Document],
        already: &[Document],
    ) {
        let Some(state) = self.collections.get(collection) else {
            return;
        };
        if state.counters.is_empty() {
            return;
        }
        let mut running: HashMap<String, i64> = state.counters.iter().cloned().collect();
        for document in already {
            raise_counters(&mut running, document);
        }
        for document in documents {
            for (field, last) in &mut running {
                match document.get(field).and_then(bson_i64) {
                    Some(value) => {
                        if value > *last {
                            *last = value;
                        }
                    }
                    None if document.get(field).is_none() => {
                        let next = last.saturating_add(1);
                        *last = next;
                        document.insert(field.clone(), fit_int(next));
                    }
                    None => {}
                }
            }
        }
    }

    fn observe_counters(&mut self, collection: &str, document: &Document) {
        let Some(state) = self.collections.get_mut(collection) else {
            return;
        };
        for (field, last) in &mut state.counters {
            if let Some(value) = document.get(field).and_then(bson_i64) {
                if value > *last {
                    *last = value;
                }
            }
        }
    }

    /// Applies an update to `admin.counters` rows kept beside each collection.
    pub fn update_counters(
        &mut self,
        filter: &Document,
        update: &Document,
        multi: bool,
    ) -> Result<(u32, u32), String> {
        let rows = self.counter_docs();
        let mut matched = 0u32;
        let mut modified = 0u32;
        for mut row in rows {
            if !document_matches(&row, filter) {
                continue;
            }
            if !multi && matched > 0 {
                break;
            }
            matched += 1;
            let collection = row.get_str("collection").unwrap_or("").to_string();
            let field = row.get_str("field").unwrap_or("").to_string();
            let previous = row.get("last").and_then(bson_i64);
            super::apply_update(&mut row, update)?;
            let Some(last) = row.get("last").and_then(bson_i64) else {
                return Err("admin.counters.last must be an integer".into());
            };
            if self.set_counter_last(&collection, &field, last) && previous != Some(last) {
                modified += 1;
            }
        }
        Ok((matched, modified))
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

    pub fn view(&self, name: &str) -> Option<&ViewSpec> {
        self.views.iter().find(|view| view.name == name)
    }

    pub fn views(&self) -> &[ViewSpec] {
        &self.views
    }

    /// The first view whose documents come from `source`.
    pub fn view_reading(&self, source: &str) -> Option<&ViewSpec> {
        self.views.iter().find(|view| view.source == source)
    }

    pub fn insert_view(&mut self, spec: ViewSpec) -> Result<(), String> {
        if spec.name.is_empty() {
            return Err("create requires a view name".into());
        }
        if self.views.iter().any(|view| view.name == spec.name) {
            return Err(format!("view {} already exists", spec.name));
        }
        if self.collections.contains_key(&spec.name) {
            return Err(format!("collection {} already exists", spec.name));
        }
        if self.views.iter().any(|view| view.name == spec.source) {
            return Err("a view cannot read another view".into());
        }
        self.views.push(spec);
        Ok(())
    }

    pub fn remove_view(&mut self, name: &str) -> Result<ViewSpec, String> {
        let position = self
            .views
            .iter()
            .position(|view| view.name == name)
            .ok_or_else(|| format!("ns not found: {name}"))?;
        Ok(self.views.remove(position))
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

    /// Rewrites the `admin` collections from the in-memory catalog.
    ///
    /// User page lists are copied before the rewrite, so replacing the
    /// catalog pages does not change where user documents live.
    pub fn publish(&mut self, pool: &mut BufferPool, disk: &PageStore) -> Result<(), String> {
        self.ensure_catalog();
        let databases = self.database_docs();
        let collections = self.collection_docs();
        let fields = self.field_docs();
        let indexes = self.index_docs();
        let users = self.user_docs();
        let roles = self.role_docs();
        let views = self.view_docs();
        self.rewrite(pool, disk, crate::storage::catalog::DATABASES, databases)?;
        self.rewrite(
            pool,
            disk,
            crate::storage::catalog::COLLECTIONS,
            collections,
        )?;
        self.rewrite(pool, disk, crate::storage::catalog::FIELDS, fields)?;
        self.rewrite(pool, disk, crate::storage::catalog::INDEXES, indexes)?;
        let counters = self.counter_docs();
        self.rewrite(pool, disk, crate::storage::catalog::COUNTERS, counters)?;
        self.rewrite(pool, disk, crate::storage::catalog::USERS, users)?;
        self.rewrite(pool, disk, crate::storage::catalog::ROLES, roles)?;
        self.rewrite(pool, disk, crate::storage::catalog::VIEWS, views)?;
        Ok(())
    }

    /// Writes the version-2 root. It lists the catalog pages only.
    pub fn write_root(&mut self, pool: &mut BufferPool) -> Result<(), String> {
        let mut catalog_pages = Document::new();
        for name in crate::storage::catalog::NAMES {
            let Some(state) = self.collections.get(name) else {
                continue;
            };
            let mut ids = state.pages.clone();
            ids.extend(&state.continuations);
            catalog_pages.insert(name, crate::storage::catalog::u64s(&ids));
        }
        let body = doc! {
            "version": 2i32,
            "applied_lsn": self.applied_lsn as i64,
            "next_page_id": self.next_page_id as i64,
            "catalog_pages": catalog_pages,
            "index_pages": crate::storage::catalog::u64s(&self.index_pages),
        };
        let bson = bson::to_vec(&body).map_err(|err| format!("catalog encode: {err}"))?;
        let mut payload = Vec::with_capacity(4 + bson.len());
        payload.extend_from_slice(&CATA_MAGIC.to_le_bytes());
        payload.extend_from_slice(&bson);
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

    /// Loads a version-2 catalog. `Ok(false)` means the root is an older
    /// catalog and the caller should scan pages.
    pub fn try_load_catalog(
        &mut self,
        pool: &mut BufferPool,
        page_one: bool,
    ) -> Result<bool, String> {
        if !page_one {
            return Ok(false);
        }
        let page = pool.fetch(CATALOG_PAGE)?;
        let payload = page.payload.clone();
        pool.unpin(CATALOG_PAGE)?;
        let body = catalog_body(&payload)?;
        if body.get_i32("version").unwrap_or(1) < 2 {
            return Ok(false);
        }
        if let Ok(lsn) = body.get_i64("applied_lsn") {
            self.applied_lsn = self.applied_lsn.max(lsn as u64);
        }
        if let Ok(next) = body.get_i64("next_page_id") {
            self.next_page_id = self.next_page_id.max(next as u64);
        }
        self.index_pages = crate::storage::catalog::read_u64s(&body, "index_pages");
        for id in &self.index_pages {
            self.live.insert(*id);
        }
        self.live.insert(CATALOG_PAGE);
        let empty_pages = Document::new();
        let pages = body.get_document("catalog_pages").unwrap_or(&empty_pages);
        for name in crate::storage::catalog::NAMES {
            let ids = pages
                .get_array(name)
                .ok()
                .map(|values| {
                    values
                        .iter()
                        .filter_map(|value| value.as_i64().map(|id| id as u64))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            for id in ids {
                let page = pool.fetch(id)?;
                let payload = page.payload.clone();
                pool.unpin(id)?;
                self.note_page(id, &payload)?;
                self.next_page_id = self.next_page_id.max(id.saturating_add(1));
            }
        }
        let collection_docs = self.read_catalog(pool, crate::storage::catalog::COLLECTIONS)?;
        for document in &collection_docs {
            self.install_user(document);
        }
        let field_docs = self.read_catalog(pool, crate::storage::catalog::FIELDS)?;
        for document in &field_docs {
            let Some(namespace) = document.get_str("ns").ok() else {
                continue;
            };
            let Some(field) = document.get_str("field").ok() else {
                continue;
            };
            let Some(type_name) = document.get_str("type").ok() else {
                continue;
            };
            if let Some(state) = self.collections.get_mut(namespace) {
                state
                    .fields
                    .push((field.to_string(), type_name.to_string()));
            }
        }
        let index_docs = self.read_catalog(pool, crate::storage::catalog::INDEXES)?;
        for document in &index_docs {
            let Some(namespace) = document.get_str("ns").ok() else {
                continue;
            };
            let Some(spec) = document.get_document("spec").ok() else {
                continue;
            };
            if let Some(state) = self.collections.get_mut(namespace) {
                state.indexes.push(spec.clone());
            }
        }
        self.users = self
            .read_catalog(pool, crate::storage::catalog::USERS)?
            .iter()
            .filter_map(|document| user_from_bson(&Bson::Document(document.clone())))
            .collect();
        self.roles = self
            .read_catalog(pool, crate::storage::catalog::ROLES)?
            .iter()
            .filter_map(|document| role_from_bson(&Bson::Document(document.clone())))
            .collect();
        self.views = self
            .read_catalog(pool, crate::storage::catalog::VIEWS)?
            .iter()
            .filter_map(|document| view_from_bson(&Bson::Document(document.clone())))
            .collect();
        let counter_docs = self.read_catalog(pool, crate::storage::catalog::COUNTERS)?;
        for document in &counter_docs {
            let Ok(collection) = document.get_str("collection") else {
                continue;
            };
            let Ok(field) = document.get_str("field") else {
                continue;
            };
            let last = document.get("last").and_then(bson_i64).unwrap_or(0);
            self.declare_counter(collection, field);
            self.set_counter_last(collection, field, last);
        }
        self.catalog_warm = true;
        Ok(true)
    }

    /// Fills field rows for collections that have documents and an empty
    /// field list. A catalog written before fields existed, or a checkpoint
    /// that stored none, is completed from the pages. Collections that
    /// already have a field list are left as they are.
    pub fn reconcile_fields(&mut self, pool: &mut BufferPool) -> Result<usize, String> {
        let names = self.names();
        let mut rebuilt = 0usize;
        for name in names {
            if crate::storage::catalog::is_catalog(&name) {
                continue;
            }
            let stale = self.collections.get(&name).is_some_and(|state| {
                state.fields.is_empty() && (state.doc_count > 0 || !state.pages.is_empty())
            });
            if !stale {
                continue;
            }
            let mut folded: HashMap<String, String> = HashMap::new();
            self.for_each_in(pool, &name, &mut |document| {
                for (key, value) in document {
                    let type_name = crate::storage::catalog::type_name(value);
                    let update = match folded.get(key.as_str()) {
                        Some(existing) if existing != type_name && existing != "mixed" => {
                            Some("mixed".to_string())
                        }
                        None => Some(type_name.to_string()),
                        _ => None,
                    };
                    if let Some(type_name) = update {
                        folded.insert(key.clone(), type_name);
                    }
                }
            })?;
            if let Some(state) = self.collections.get_mut(&name) {
                let mut fields: Vec<_> = folded.into_iter().collect();
                fields.sort();
                state.fields = fields;
            }
            rebuilt += 1;
        }
        Ok(rebuilt)
    }

    /// Fills the id set from the pages the catalog already names.
    pub fn refresh_ids(&mut self, pool: &mut BufferPool, collection: &str) -> Result<(), String> {
        if !self.contains(collection) || crate::storage::catalog::is_catalog(collection) {
            return Ok(());
        }
        self.fill_locations(pool, collection)
    }

    fn read_catalog(&mut self, pool: &mut BufferPool, name: &str) -> Result<Vec<Document>, String> {
        let mut documents = Vec::new();
        self.for_each_in(pool, name, &mut |document| {
            documents.push(document.clone());
        })?;
        Ok(documents)
    }

    fn install_user(&mut self, document: &Document) {
        let Some(namespace) = document.get_str("_id").ok() else {
            return;
        };
        if crate::storage::catalog::is_catalog(namespace) {
            return;
        }
        let pages = crate::storage::catalog::read_u64s(document, "pages");
        let continuations = crate::storage::catalog::read_u64s(document, "continuations");
        let overflow = crate::storage::catalog::read_u64s(document, "overflow");
        for id in pages.iter().chain(continuations.iter()) {
            self.live.insert(*id);
            self.next_page_id = self.next_page_id.max(id.saturating_add(1));
        }
        let mut state = CollectionState::empty();
        state.pages = pages;
        state.continuations = continuations;
        state.overflow_heads = overflow.into_iter().collect();
        state.doc_count = document.get_i64("doc_count").unwrap_or(0) as u64;
        let stored_ids = crate::storage::catalog::read_strings(document, "ids");
        let stored_pages = crate::storage::catalog::read_u64s(document, "id_pages");
        state.ids = stored_ids.iter().cloned().collect();
        if stored_pages.len() == stored_ids.len() {
            state.locations = stored_ids.into_iter().zip(stored_pages).collect();
        }
        if let Ok(validator) = document.get_document("validator") {
            state.validator = Some(validator.clone());
        }
        self.collections.insert(namespace.to_string(), state);
    }

    fn rewrite(
        &mut self,
        pool: &mut BufferPool,
        disk: &PageStore,
        name: &str,
        documents: Vec<Document>,
    ) -> Result<(), String> {
        if let Some(state) = self.collections.remove(name) {
            for id in state.pages.into_iter().chain(state.continuations) {
                pool.discard(id);
                disk.remove(id)?;
                self.live.remove(&id);
            }
        }
        self.collections
            .insert(name.to_string(), CollectionState::empty());
        for document in documents {
            self.place(pool, name, &document)?;
        }
        Ok(())
    }

    fn catalog_documents(&self, name: &str) -> Vec<Document> {
        match name {
            crate::storage::catalog::DATABASES => self.database_docs(),
            crate::storage::catalog::COLLECTIONS => self.collection_docs(),
            crate::storage::catalog::FIELDS => self.field_docs(),
            crate::storage::catalog::INDEXES => self.index_docs(),
            crate::storage::catalog::COUNTERS => self.counter_docs(),
            crate::storage::catalog::USERS => self.user_docs(),
            crate::storage::catalog::ROLES => self.role_docs(),
            crate::storage::catalog::VIEWS => self.view_docs(),
            _ => Vec::new(),
        }
    }

    fn database_docs(&self) -> Vec<Document> {
        let mut names = std::collections::BTreeSet::new();
        names.insert("admin".to_string());
        for key in self.collections.keys() {
            if crate::storage::catalog::is_catalog(key) {
                continue;
            }
            names.insert(crate::storage::catalog::database_of(key).to_string());
        }
        for view in &self.views {
            names.insert(crate::storage::catalog::database_of(&view.name).to_string());
        }
        names
            .into_iter()
            .map(|name| doc! { "_id": name.clone(), "name": name })
            .collect()
    }

    fn collection_docs(&self) -> Vec<Document> {
        let mut names: Vec<&String> = self
            .collections
            .keys()
            .filter(|name| !crate::storage::catalog::is_catalog(name))
            .collect();
        names.sort();
        names
            .into_iter()
            .map(|namespace| {
                let state = &self.collections[namespace];
                let database = crate::storage::catalog::database_of(namespace);
                let name = crate::storage::catalog::bare_name(namespace);
                let mut ids: Vec<&String> = state.ids.iter().collect();
                ids.sort();
                let id_pages: Vec<u64> = ids
                    .iter()
                    .filter_map(|id| state.locations.get(id.as_str()).copied())
                    .collect();
                let mut document = doc! {
                    "_id": namespace.clone(),
                    "database": database,
                    "name": name,
                    "doc_count": state.doc_count as i64,
                    "pages": crate::storage::catalog::u64s(&state.pages),
                    "overflow": crate::storage::catalog::u64s(
                        &state.overflow_heads.iter().copied().collect::<Vec<_>>(),
                    ),
                    "continuations": crate::storage::catalog::u64s(&state.continuations),
                    "ids": ids.into_iter().cloned().collect::<Vec<_>>(),
                };
                if id_pages.len() == state.ids.len() {
                    document.insert("id_pages", crate::storage::catalog::u64s(&id_pages));
                }
                if let Some(validator) = &state.validator {
                    document.insert("validator", validator.clone());
                }
                document
            })
            .collect()
    }

    fn field_docs(&self) -> Vec<Document> {
        let mut names: Vec<&String> = self
            .collections
            .keys()
            .filter(|name| !crate::storage::catalog::is_catalog(name))
            .collect();
        names.sort();
        let mut documents = Vec::new();
        for namespace in names {
            let state = &self.collections[namespace];
            let mut fields = state.fields.clone();
            fields.sort();
            for (field, type_name) in fields {
                documents.push(doc! {
                    "_id": format!("{namespace}.{field}"),
                    "ns": namespace.clone(),
                    "database": crate::storage::catalog::database_of(namespace),
                    "collection": crate::storage::catalog::bare_name(namespace),
                    "field": field,
                    "type": type_name,
                });
            }
        }
        documents
    }

    fn index_docs(&self) -> Vec<Document> {
        let mut names: Vec<&String> = self
            .collections
            .keys()
            .filter(|name| !crate::storage::catalog::is_catalog(name))
            .collect();
        names.sort();
        let mut documents = Vec::new();
        for namespace in names {
            for spec in &self.collections[namespace].indexes {
                let name = spec.get_str("name").unwrap_or("index");
                documents.push(doc! {
                    "_id": format!("{namespace}.{name}"),
                    "ns": namespace.clone(),
                    "database": crate::storage::catalog::database_of(namespace),
                    "collection": crate::storage::catalog::bare_name(namespace),
                    "name": name,
                    "spec": spec.clone(),
                });
            }
        }
        documents
    }

    fn counter_docs(&self) -> Vec<Document> {
        let mut names: Vec<&String> = self
            .collections
            .keys()
            .filter(|name| !crate::storage::catalog::is_catalog(name))
            .collect();
        names.sort();
        let mut documents = Vec::new();
        for namespace in names {
            let mut counters = self.collections[namespace].counters.clone();
            counters.sort();
            for (field, last) in counters {
                documents.push(doc! {
                    "_id": format!("{namespace}.{field}"),
                    "collection": namespace.clone(),
                    "field": field,
                    "last": last,
                });
            }
        }
        documents
    }

    fn user_docs(&self) -> Vec<Document> {
        self.users
            .iter()
            .map(|user| {
                let mut document = doc! { "_id": user.name.clone() };
                if let Bson::Document(body) = user_to_bson(user) {
                    for (key, value) in body {
                        document.insert(key, value);
                    }
                }
                document
            })
            .collect()
    }

    fn role_docs(&self) -> Vec<Document> {
        self.roles
            .iter()
            .map(|role| {
                let mut document = doc! { "_id": role.name.clone() };
                if let Bson::Document(body) = role_to_bson(role) {
                    for (key, value) in body {
                        document.insert(key, value);
                    }
                }
                document
            })
            .collect()
    }

    fn view_docs(&self) -> Vec<Document> {
        self.views
            .iter()
            .map(|view| {
                let mut document = doc! { "_id": view.name.clone() };
                if let Bson::Document(body) = view_to_bson(view) {
                    for (key, value) in body {
                        document.insert(key, value);
                    }
                }
                document
            })
            .collect()
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
            self.observe_counters(collection, &document);
            let id = document_id_string(&document);
            if self.contains_id(collection, &id) {
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
        if crate::storage::catalog::is_catalog(collection) {
            return Ok(self
                .catalog_documents(collection)
                .into_iter()
                .filter(|document| document_matches(document, filter))
                .collect());
        }
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
        if crate::storage::catalog::is_catalog(collection) {
            return Ok(self
                .catalog_documents(collection)
                .iter()
                .filter(|document| document_matches(document, filter))
                .count() as u64);
        }
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
    /// Each id names the page recorded when the document was stored. An
    /// empty list, and an id that is not stored, read no page. A page is
    /// read once even when several hits share it.
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
        if crate::storage::catalog::is_catalog(collection) {
            let wanted: HashSet<&str> = ids.iter().map(String::as_str).collect();
            return Ok(self
                .catalog_documents(collection)
                .into_iter()
                .filter(|document| {
                    wanted.contains(document_id_string(document).as_str())
                        && document_matches(document, filter)
                })
                .collect());
        }
        let mut pages: Vec<u64> = Vec::new();
        for id in ids {
            if !self.contains_id(collection, id) {
                continue;
            }
            let Some(page) = self.location(collection, id) else {
                return self.find_ids_scan(pool, collection, ids, filter);
            };
            if !pages.contains(&page) {
                pages.push(page);
            }
        }
        if pages.is_empty() {
            return Ok(Vec::new());
        }
        let wanted: HashSet<&str> = ids.iter().map(String::as_str).collect();
        let mut matches = Vec::new();
        for page_id in pages {
            for document in self.read_page_documents(pool, collection, page_id)? {
                let id = document_id_string(&document);
                if wanted.contains(id.as_str()) && document_matches(&document, filter) {
                    matches.push(document);
                }
            }
        }
        Ok(matches)
    }

    /// A missing id reads no page. A hit reads the page recorded for it.
    pub fn find_by_id(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        id: &str,
    ) -> Result<Option<Document>, String> {
        if crate::storage::catalog::is_catalog(collection) {
            return Ok(self
                .catalog_documents(collection)
                .into_iter()
                .find(|document| document_id_string(document) == id));
        }
        if !self.contains_id(collection, id) {
            return Ok(None);
        }
        let Some(page_id) = self.location(collection, id) else {
            let mut found = None;
            self.for_each_in(pool, collection, &mut |doc| {
                if found.is_none() && document_id_string(doc) == id {
                    found = Some(doc.clone());
                }
            })?;
            return Ok(found);
        };
        for document in self.read_page_documents(pool, collection, page_id)? {
            if document_id_string(&document) == id {
                return Ok(Some(document));
            }
        }
        Ok(None)
    }

    fn find_ids_scan(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        ids: &[String],
        filter: &Document,
    ) -> Result<Vec<Document>, String> {
        let wanted: HashSet<&str> = ids.iter().map(String::as_str).collect();
        let mut matches = Vec::new();
        self.for_each_in(pool, collection, &mut |doc| {
            if wanted.contains(document_id_string(doc).as_str()) && document_matches(doc, filter) {
                matches.push(doc.clone());
            }
        })?;
        Ok(matches)
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
                if !crate::storage::catalog::is_catalog(&name) {
                    self.user_page_reads += 1;
                }
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
    ///
    /// Several normal pages are decoded on more than one core. The
    /// calling worker still owns the log, so a write cannot fork it.
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
        let parallel = pages.len() > 1 && !crate::storage::catalog::is_catalog(collection);
        let mut index = 0;
        while index < pages.len() {
            let page_id = pages[index];
            if heads.contains(&page_id) {
                self.note_user_read(collection);
                let doc = self.read_overflow(pool, page_id)?;
                visit(&doc);
                index += 1;
                continue;
            }
            let start = index;
            while index < pages.len() && !heads.contains(&pages[index]) {
                index += 1;
            }
            let run = &pages[start..index];
            if parallel && run.len() > 1 {
                let mut payloads = Vec::with_capacity(run.len());
                for page_id in run {
                    self.note_user_read(collection);
                    payloads.push(self.read_payload(pool, *page_id)?);
                }
                let decoded = decode_payloads(&payloads)?;
                for docs in decoded {
                    for doc in &docs {
                        visit(doc);
                    }
                }
            } else {
                for page_id in run {
                    self.note_user_read(collection);
                    let docs = self.read_docs(pool, *page_id)?;
                    for doc in &docs {
                        visit(doc);
                    }
                }
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
        if let Some(state) = self.collections.get_mut(collection) {
            for document in &updated {
                record_fields(state, document);
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
        if let Some(state) = self.collections.get_mut(collection) {
            for doc in &removed {
                let id = document_id_string(doc);
                state.ids.remove(&id);
                state.locations.remove(&id);
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

    fn contains_id(&self, collection: &str, id: &str) -> bool {
        self.collections
            .get(collection)
            .is_some_and(|state| state.ids.contains(id))
    }

    fn note_stored(&mut self, collection: &str, document: &Document, page_id: u64) {
        let state = self.collections.get_mut(collection).unwrap();
        state.doc_count += 1;
        let id = document_id_string(document);
        state.ids.insert(id.clone());
        state.locations.insert(id, page_id);
        record_fields(state, document);
    }

    fn location(&self, collection: &str, id: &str) -> Option<u64> {
        self.collections
            .get(collection)
            .and_then(|state| state.locations.get(id).copied())
            .filter(|page| *page > 0)
    }

    fn remember(&mut self, collection: &str, document: &Document, page_id: u64) {
        let id = document_id_string(document);
        if let Some(state) = self.collections.get_mut(collection) {
            state.locations.insert(id, page_id);
        }
    }

    fn note_user_read(&mut self, collection: &str) {
        if !crate::storage::catalog::is_catalog(collection) {
            self.user_page_reads += 1;
        }
    }

    /// Reads the documents on one page and counts that page.
    fn read_page_documents(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        page_id: u64,
    ) -> Result<Vec<Document>, String> {
        self.note_user_read(collection);
        let heads = self
            .collections
            .get(collection)
            .map(|state| state.overflow_heads.clone())
            .unwrap_or_default();
        if heads.contains(&page_id) {
            return Ok(vec![self.read_overflow(pool, page_id)?]);
        }
        self.read_docs(pool, page_id)
    }

    /// Records each document's page. Used when a catalog has ids and no
    /// page list yet, and when a cold start walks the files.
    fn fill_locations(&mut self, pool: &mut BufferPool, collection: &str) -> Result<(), String> {
        if !self.contains(collection) {
            return Ok(());
        }
        let pages = self.collections[collection].pages.clone();
        let heads = self.collections[collection].overflow_heads.clone();
        let mut ids = HashSet::new();
        let mut locations = HashMap::new();
        for page_id in pages {
            let documents = if heads.contains(&page_id) {
                vec![self.read_overflow(pool, page_id)?]
            } else {
                self.read_docs(pool, page_id)?
            };
            for document in documents {
                let id = document_id_string(&document);
                ids.insert(id.clone());
                locations.insert(id, page_id);
            }
        }
        if let Some(state) = self.collections.get_mut(collection) {
            state.ids = ids;
            state.locations = locations;
        }
        Ok(())
    }

    /// Appends to the last normal page. When that page is full, the
    /// document starts a new page. Earlier pages stay as they are.
    fn place(
        &mut self,
        pool: &mut BufferPool,
        collection: &str,
        document: &Document,
    ) -> Result<(), String> {
        let tail = self.collections[collection]
            .pages
            .iter()
            .rev()
            .copied()
            .find(|page_id| {
                !self.collections[collection]
                    .overflow_heads
                    .contains(page_id)
            });
        if let Some(page_id) = tail {
            let mut docs = self.read_docs(pool, page_id)?;
            docs.push(document.clone());
            if encode_docs(collection, &docs, self.capacity()).is_ok() {
                self.write_normal(pool, collection, page_id, &docs)?;
                self.note_stored(collection, document, page_id);
                return Ok(());
            }
        }
        if encode_docs(collection, std::slice::from_ref(document), self.capacity()).is_ok() {
            let payload = encode_docs(collection, std::slice::from_ref(document), self.capacity())?;
            let id = self.alloc(pool, payload)?;
            self.collections.get_mut(collection).unwrap().pages.push(id);
            self.note_stored(collection, document, id);
            return Ok(());
        }
        let head = self.attach_overflow(pool, collection, document)?;
        self.note_stored(collection, document, head);
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
            self.write_normal(pool, collection, page_id, &docs)?;
            for doc in &docs {
                self.remember(collection, doc, page_id);
            }
            return Ok(());
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
        for doc in &kept {
            self.remember(collection, doc, page_id);
        }
        for doc in spill {
            let head = self.attach_overflow(pool, collection, &doc)?;
            self.remember(collection, &doc, head);
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
    ) -> Result<u64, String> {
        let bytes = bson::to_vec(document).map_err(|err| format!("document encode: {err}"))?;
        let ids = self.alloc_overflow_chain(pool, collection, &bytes)?;
        let head = ids[0];
        let state = self.collections.get_mut(collection).unwrap();
        state.pages.push(head);
        state.overflow_heads.insert(head);
        for id in ids.into_iter().skip(1) {
            state.continuations.push(id);
        }
        Ok(head)
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

    /// Bytes of the index snapshot, or `None` when this directory still
    /// keeps them in `indexes.bin`.
    pub fn read_index_bytes(&mut self, pool: &mut BufferPool) -> Result<Option<Vec<u8>>, String> {
        if self.index_pages.is_empty() {
            return Ok(None);
        }
        let mut bytes = Vec::new();
        for id in self.index_pages.clone() {
            let payload = self.read_payload(pool, id)?;
            if payload.len() < 12
                || u32::from_le_bytes(payload[0..4].try_into().unwrap()) != INDX_MAGIC
            {
                return Err(format!("index page {id} is not an index snapshot"));
            }
            bytes.extend_from_slice(&payload[12..]);
        }
        Ok(Some(bytes))
    }

    /// Replaces the index snapshot pages. The next catalog root names them.
    pub fn store_index_bytes(
        &mut self,
        pool: &mut BufferPool,
        disk: &PageStore,
        bytes: &[u8],
    ) -> Result<(), String> {
        let old = std::mem::take(&mut self.index_pages);
        for id in old {
            pool.discard(id);
            disk.remove(id)?;
            self.live.remove(&id);
        }
        let cap = self.capacity().checked_sub(12).filter(|n| *n > 0).ok_or_else(|| {
            format!(
                "page payload of {} bytes cannot hold an index header",
                self.capacity()
            )
        })?;
        let pieces: Vec<&[u8]> = if bytes.is_empty() {
            vec![&[]]
        } else {
            bytes.chunks(cap).collect()
        };
        let mut ids = Vec::with_capacity(pieces.len());
        for _ in &pieces {
            ids.push(self.next_page_id);
            self.next_page_id += 1;
        }
        for (index, piece) in pieces.iter().enumerate() {
            let next = if index + 1 < ids.len() {
                ids[index + 1]
            } else {
                0
            };
            let mut payload = Vec::with_capacity(12 + piece.len());
            payload.extend_from_slice(&INDX_MAGIC.to_le_bytes());
            payload.extend_from_slice(&next.to_le_bytes());
            payload.extend_from_slice(piece);
            pool.insert(Page::new(ids[index], self.applied_lsn, payload))?;
            self.live.insert(ids[index]);
        }
        self.index_pages = ids;
        Ok(())
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

/// Decodes page bodies on more than one core, keeping page order.
fn decode_payloads(payloads: &[Vec<u8>]) -> Result<Vec<Vec<Document>>, String> {
    let workers = std::thread::available_parallelism()
        .map(|count| count.get())
        .unwrap_or(1)
        .clamp(1, 8);
    if payloads.len() < 2 || workers < 2 {
        return payloads.iter().map(|payload| decode_docs(payload)).collect();
    }
    let width = workers.min(payloads.len());
    let size = payloads.len().div_ceil(width);
    std::thread::scope(|scope| {
        let mut joins = Vec::new();
        for chunk in payloads.chunks(size) {
            joins.push(scope.spawn(move || {
                chunk
                    .iter()
                    .map(|payload| decode_docs(payload))
                    .collect::<Result<Vec<_>, _>>()
            }));
        }
        let mut decoded = Vec::with_capacity(payloads.len());
        for join in joins {
            let part = join
                .join()
                .map_err(|_| "page decode thread failed".to_string())?;
            decoded.extend(part?);
        }
        Ok(decoded)
    })
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

fn view_to_bson(view: &ViewSpec) -> Bson {
    Bson::Document(doc! {
        "name": view.name.clone(),
        "source": view.source.clone(),
        "pipeline": view.pipeline.clone(),
        "kova": view.kova.clone(),
    })
}

fn view_from_bson(value: &Bson) -> Option<ViewSpec> {
    let doc = value.as_document()?;
    let pipeline = doc
        .get_array("pipeline")
        .ok()?
        .iter()
        .filter_map(|value| value.as_document().cloned())
        .collect();
    Some(ViewSpec {
        name: doc.get_str("name").ok()?.to_string(),
        source: doc.get_str("source").ok()?.to_string(),
        pipeline,
        kova: doc.get_document("kova").ok().cloned().unwrap_or_default(),
    })
}

fn raise_counters(running: &mut HashMap<String, i64>, document: &Document) {
    for (field, last) in running.iter_mut() {
        if let Some(value) = document.get(field).and_then(bson_i64) {
            if value > *last {
                *last = value;
            }
        }
    }
}

fn bson_i64(value: &Bson) -> Option<i64> {
    match value {
        Bson::Int32(number) => Some(i64::from(*number)),
        Bson::Int64(number) => Some(*number),
        Bson::Double(number) if number.is_finite() && number.fract() == 0.0 => Some(*number as i64),
        _ => None,
    }
}

fn fit_int(number: i64) -> Bson {
    match i32::try_from(number) {
        Ok(small) => Bson::Int32(small),
        Err(_) => Bson::Int64(number),
    }
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

fn record_fields(state: &mut CollectionState, document: &Document) {
    for (key, value) in document {
        let type_name = crate::storage::catalog::type_name(value);
        if let Some((_, existing)) = state.fields.iter_mut().find(|(name, _)| name == key) {
            if existing != type_name && existing != "mixed" {
                *existing = "mixed".to_string();
            }
        } else {
            state.fields.push((key.clone(), type_name.to_string()));
        }
    }
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
