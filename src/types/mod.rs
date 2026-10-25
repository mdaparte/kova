//! Shared data types for the Kova engine.
//!
//! This module defines the command vocabulary exchanged between the
//! network layer and the storage workers. It contains no I/O: it is
//! pure data so it can be unit-tested in isolation.

use bson::Document;
use tokio::sync::oneshot;

/// The result of executing a partition command.
///
/// Workers always respond, even on failure, so the network layer can
/// always map the outcome back to a MongoDB reply.
#[derive(Debug, Clone)]
pub enum PartitionResult {
    /// The document was inserted successfully.
    Inserted { inserted_count: u32 },
    /// One or more documents matched the filter.
    Found { documents: Vec<Document> },
    /// The number of documents matching a count filter.
    Counted { count: u64 },
    /// The collection was removed.
    Dropped { collection: String },
    /// The database was removed with all of its collections.
    DroppedDatabase { database: String },
    /// Collection descriptions held by one partition.
    ListedCollections { collections: Vec<Document> },
    /// Documents were modified.
    ///
    /// `matched_count` is how many documents matched the filter;
    /// `modified_count` is how many were actually rewritten. MongoDB
    /// reports them as `n` and `nModified`, and clients read
    /// `matchedCount` from the former.
    Modified {
        matched_count: u32,
        modified_count: u32,
    },
    /// Documents were removed.
    Deleted { deleted_count: u32 },
    /// The collection was created empty (`create`).
    ///
    /// No payload: MongoDB's reply is just `{ok: 1}`, and the target
    /// namespace is already known to the caller.
    Created,
    /// Index declarations were accepted (`createIndexes`).
    ///
    /// Carries the before/after counts MongoDB reports so drivers can
    /// detect a no-op re-declaration. `created_collection` is true
    /// when the declaration brought a new collection into existence,
    /// which is how Spring Data bootstraps indexes before any insert.
    IndexesCreated {
        num_indexes_before: u32,
        num_indexes_after: u32,
        created_collection: bool,
    },
    /// A collection's indexes (`listIndexes`).
    ListedIndexes { indexes: Vec<Document> },
    /// Storage statistics for one collection (`collStats`).
    ///
    /// `size`/`avgObjSize` come from actually serializing the stored
    /// documents, so the numbers are real rather than a guess.
    CollectionStats { stats: Document },
    /// Database descriptions merged from every partition
    /// (`listDatabases`).
    ListedDatabases { databases: Vec<Document> },
    /// Raw per-partition statistics for one database (`dbStats`).
    ///
    /// Carries counters, not a document, because the network layer
    /// sums them across partitions before shaping MongoDB's reply.
    DatabaseStats {
        collections: u64,
        objects: u64,
        data_size: u64,
        indexes: u64,
    },
    /// `aggregate` needs a foreign collection this partition does not own.
    ///
    /// The network layer loads that collection from its owner and
    /// retries the pipeline with the documents attached.
    NeedForeign { namespace: String },
    /// `$out` produced documents. The network layer writes them with
    /// a normal insert so the WAL records the mutation.
    StageOut {
        namespace: String,
        documents: Vec<Document>,
    },
    /// The command could not be executed.
    Error { message: String },
    /// `beginTransaction`, `commit`, or `abort` completed.
    Transaction { txn_id: u64 },
    /// Staged writes of a commit that has not been logged yet.
    ///
    /// `ops` are WAL operation documents. Empty means the commit has
    /// nothing to replicate.
    CommitPreview { txn_id: u64, ops: Vec<Document> },
    /// `dropIndexes` removed one index, or every secondary index.
    IndexesDropped { n_indexes_was: u32 },
    /// `explain` asked the worker which access path it would use.
    Plan {
        stage: String,
        index_name: Option<String>,
    },
    /// A `$text` result that also counted facet buckets.
    Faceted {
        documents: Vec<Document>,
        facets: Document,
    },
    /// `authenticate` succeeded. `actions` are the expanded privilege names.
    Authenticated { user: String, actions: Vec<String> },
    /// A user or role change. `disabled` names a user whose connections must close.
    Account { disabled: Option<String> },
}

/// A command sent from the network layer to a storage partition.
#[derive(Debug, Clone)]
pub enum PartitionCommand {
    /// Insert one or more documents into a collection.
    Insert {
        collection: String,
        documents: Vec<Document>,
    },
    /// Find documents matching a filter.
    ///
    /// `projection`, `sort`, `skip`, and `limit` shape the cursor after
    /// the filter. `limit == None` means the command did not ask for a
    /// cap (a find `limit` of 0 is the same thing).
    Find {
        collection: String,
        filter: Document,
        projection: Option<Document>,
        sort: Option<Document>,
        skip: u64,
        limit: Option<u64>,
    },
    /// Run an aggregation pipeline over one collection.
    ///
    /// `resolved` holds foreign collections the network layer already
    /// loaded for `$lookup`. A lookup whose collection hashes to this
    /// partition is scanned here, from its pages.
    Aggregate {
        collection: String,
        pipeline: Vec<Document>,
        resolved: Vec<(String, Vec<Document>)>,
    },
    /// Update documents matching a filter.
    ///
    /// `multi == false` limits the update to the first match, which is
    /// the default for the MongoDB `update` command (used by
    /// `updateOne`).
    Update {
        collection: String,
        filter: Document,
        update: Document,
        multi: bool,
    },
    /// Delete documents matching a filter.
    ///
    /// `limit == 0` deletes every match (deleteMany); any other value
    /// caps how many documents may be removed (deleteOne sends 1).
    Delete {
        collection: String,
        filter: Document,
        limit: u32,
    },
    /// Count documents matching a filter (used by the `count` command).
    Count {
        collection: String,
        filter: Document,
    },
    /// Remove a collection and all of its documents (`drop`).
    Drop { collection: String },
    /// Remove every collection of the requesting database
    /// (`dropDatabase`). Broadcast: one database's collections hash
    /// across partitions, so every worker drops its own share.
    DropDatabase { database: String },
    /// Create an empty collection without inserting documents
    /// (`create`). Clients such as Compass issue this command from
    /// their "create collection" dialog; MongoDB itself creates
    /// collections implicitly on first insert, but GUI tools expect
    /// the explicit command to exist.
    CreateCollection {
        collection: String,
        /// Fields whose missing values are filled from `admin.counters`.
        autoincrement: Vec<String>,
    },
    /// Declare secondary indexes on a collection (`createIndexes`).
    ///
    /// The catalog keeps the spec. The worker also builds the B-tree
    /// or hash structure and rejects the command when a unique index
    /// would be violated by documents already stored.
    CreateIndexes {
        collection: String,
        indexes: Vec<Document>,
    },
    /// Drops one index, or every secondary index when `index` is `"*"`.
    ///
    /// `_id_` cannot be dropped. The catalog and the in-memory structure
    /// both lose the index.
    DropIndexes { collection: String, index: String },
    /// Asks which access path a filter would use (`explain`).
    ///
    /// The worker answers `IDHACK`, `IXSCAN`, or `COLLSCAN`. The network
    /// layer writes that stage into the explain document.
    Explain {
        collection: String,
        filter: Document,
    },
    /// List a collection's indexes (`listIndexes`).
    ///
    /// Routed by collection name, like [`PartitionCommand::Find`]:
    /// one partition owns the namespace and answers alone.
    ListIndexes { collection: String },
    /// Storage statistics for one collection (`collStats`).
    ///
    /// Routed by collection name; one partition owns the namespace.
    CollectionStats { collection: String },
    /// Storage statistics for one database (`dbStats`).
    ///
    /// Broadcast: one database's collections hash across partitions,
    /// so the network layer sums every worker's counters.
    DatabaseStats { database: String },
    /// List the collections known to every partition (`listCollections`).
    ///
    /// The routing key is empty on purpose: collection names are hashed
    /// across partitions, so the network layer broadcasts this command
    /// to all workers and merges their replies. `database` restricts
    /// the listing to one database's namespace (empty lists all).
    ListCollections { database: String, filter: Document },
    /// List the databases that own collections on any partition
    /// (`listDatabases`). Broadcast like [`PartitionCommand::ListCollections`];
    /// GUI clients need it to render their database tree.
    ListDatabases { filter: Document },
    /// Opens a transaction on the partition that owns `collection`.
    ///
    /// Later commands carry `txn_id` and must name a collection that
    /// hashes to the same partition. `txn_id == 0` asks the worker to
    /// allocate one.
    BeginTransaction {
        collection: String,
        txn_id: u64,
        isolation: String,
        timeout_ms: Option<u64>,
    },
    /// Makes the transaction's writes durable in one WAL record.
    CommitTransaction { collection: String, txn_id: u64 },
    /// Copies the staged writes and leaves the transaction open.
    ///
    /// Replication uses this before proposing the commit. A conflict
    /// still aborts the transaction.
    CommitPreview { collection: String, txn_id: u64 },
    /// Applies a replicated commit. Followers do not open a transaction.
    ///
    /// `txn_id` drops that transaction on the primary, where the writes
    /// were staged. Followers ignore an id they do not have. `ops` are
    /// WAL operation documents, appended as one record.
    ApplyTransaction {
        collection: String,
        txn_id: u64,
        ops: Vec<Document>,
    },
    /// Flushes dirty pages so a snapshot can copy them.
    Checkpoint,
    /// Replaces this partition's pages with a shipped snapshot.
    InstallSnapshot { bytes: Vec<u8> },
    /// Discards the transaction's writes.
    AbortTransaction { collection: String, txn_id: u64 },
    /// Sets or replaces a collection's document validator.
    CollMod {
        collection: String,
        validator: Document,
    },
    /// A saved pipeline over `source` (`create` + `viewOn`).
    ///
    /// `kova` keeps engine options. `inverted: true` builds a Tantivy
    /// index on disk for this view. Other keys are stored for later.
    CreateView {
        name: String,
        source: String,
        pipeline: Vec<Document>,
        kova: Document,
    },
    /// A data command that belongs to an open transaction.
    ///
    /// The write stays in the transaction until commit. The page and
    /// the WAL are not touched yet.
    Transactional {
        txn_id: u64,
        inner: Box<PartitionCommand>,
    },
    /// Full-text search within one collection (`$text` query operator).
    ///
    /// The filter carries the remaining (non-`$text`) predicates so a
    /// combined query like `{a: 1, $text: {...}}` still works: the
    /// index narrows by relevance and the worker applies the equality
    /// filter afterwards.
    Search {
        collection: String,
        query: String,
        filter: Document,
        options: SearchOptions,
    },
    /// Creates a user. `hash` is Argon2id. `trusted` is set when the
    /// caller already passed the role check. The first user must be root.
    CreateUser {
        name: String,
        hash: String,
        roles: Vec<String>,
        trusted: bool,
    },
    /// Enables or disables an account.
    UpdateUser { name: String, enabled: bool },
    /// Adds roles, or removes them when `revoke` is set.
    GrantRoles {
        name: String,
        roles: Vec<String>,
        revoke: bool,
    },
    /// Defines a custom role as a list of action names.
    CreateRole { name: String, actions: Vec<String> },
    /// Removes a custom role. Built-in roles cannot be dropped.
    DropRole { name: String },
    /// Checks a password. The plaintext never reaches the WAL.
    Authenticate { name: String, password: String },
}

/// Phase 4 ranking options carried by a [`PartitionCommand::Search`].
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SearchOptions {
    /// Per-field BM25 multipliers (`$text.$boost`), mirroring MongoDB
    /// text index weights. Fields absent from the list weigh 1.0.
    pub boosts: Vec<(String, f32)>,
    /// Optional recency decay (`$text.$decay`), applied by the worker
    /// as a re-rank over the BM25 ordering.
    pub decay: Option<DecaySpec>,
    /// Term and range buckets for an inverted view. Empty means the
    /// search returns documents only.
    pub facets: Document,
    /// `$text.$fuzzy`, a fraction of each word's length from 0 to 1.
    /// The edit distance is that fraction, rounded up, and never more
    /// than 2. Zero keeps the query exact.
    pub fuzzy: f64,
}

/// Exponential half-life decay over a date field.
///
/// A document whose date is one `half_life_secs` in the past keeps
/// half of its BM25 score, two half-lives keep a quarter, and so on.
/// Documents without a usable date are not penalized.
#[derive(Debug, Clone, PartialEq)]
pub struct DecaySpec {
    /// Document field carrying the date (`Bson::DateTime`).
    pub field: String,
    /// Half-life duration in seconds; must be greater than zero.
    pub half_life_secs: f64,
}

impl PartitionCommand {
    /// A find with no projection, sort, skip, or limit.
    pub fn find(collection: impl Into<String>, filter: Document) -> Self {
        Self::Find {
            collection: collection.into(),
            filter,
            projection: None,
            sort: None,
            skip: 0,
            limit: None,
        }
    }

    /// Account commands always run on partition 0, where the user catalog lives.
    pub fn is_account(&self) -> bool {
        matches!(
            self,
            PartitionCommand::CreateUser { .. }
                | PartitionCommand::UpdateUser { .. }
                | PartitionCommand::GrantRoles { .. }
                | PartitionCommand::CreateRole { .. }
                | PartitionCommand::DropRole { .. }
                | PartitionCommand::Authenticate { .. }
        )
    }

    /// Returns the collection name this command targets.
    ///
    /// The collection is the partition routing key: all commands for the
    /// same collection always land on the same worker.
    pub fn collection(&self) -> &str {
        match self {
            PartitionCommand::Insert { collection, .. }
            | PartitionCommand::Find { collection, .. }
            | PartitionCommand::Update { collection, .. }
            | PartitionCommand::Delete { collection, .. }
            | PartitionCommand::Count { collection, .. }
            | PartitionCommand::Drop { collection }
            | PartitionCommand::CreateCollection { collection, .. }
            | PartitionCommand::CreateIndexes { collection, .. }
            | PartitionCommand::DropIndexes { collection, .. }
            | PartitionCommand::Explain { collection, .. }
            | PartitionCommand::ListIndexes { collection }
            | PartitionCommand::CollectionStats { collection }
            | PartitionCommand::Search { collection, .. }
            | PartitionCommand::Aggregate { collection, .. }
            | PartitionCommand::BeginTransaction { collection, .. }
            | PartitionCommand::CommitTransaction { collection, .. }
            | PartitionCommand::CommitPreview { collection, .. }
            | PartitionCommand::ApplyTransaction { collection, .. }
            | PartitionCommand::AbortTransaction { collection, .. }
            | PartitionCommand::CollMod { collection, .. } => collection,
            PartitionCommand::CreateView { source, .. } => source,
            PartitionCommand::Transactional { inner, .. } => inner.collection(),
            PartitionCommand::Checkpoint
            | PartitionCommand::InstallSnapshot { .. }
            | PartitionCommand::CreateUser { .. }
            | PartitionCommand::UpdateUser { .. }
            | PartitionCommand::GrantRoles { .. }
            | PartitionCommand::CreateRole { .. }
            | PartitionCommand::DropRole { .. }
            | PartitionCommand::Authenticate { .. }
            | PartitionCommand::ListCollections { .. }
            | PartitionCommand::ListDatabases { .. }
            | PartitionCommand::DatabaseStats { .. }
            | PartitionCommand::DropDatabase { .. } => "",
        }
    }
}

/// A command paired with the channel used to return its result.
///
/// The network task creates the oneshot pair, sends the envelope through
/// the `crossbeam` channel to the owning partition worker, then awaits
/// the receiver. This keeps the worker thread fully synchronous while
/// the network task stays async.
pub struct CommandEnvelope {
    pub command: PartitionCommand,
    pub respond_to: oneshot::Sender<PartitionResult>,
}

/// One full-text search hit: the storage-side document id and score.
///
/// Hits arrive ordered by combined BM25 relevance (boosts applied), so
/// the lookup order in the worker reproduces the ranking; the score
/// lets the worker apply a `$decay` re-rank. The search module returns
/// ids; the worker looks up the actual documents and replies with
/// them, so raw index internals never cross the module boundary.
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub id: String,
    pub score: f32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_command_collection_routing_key() {
        let cmd = PartitionCommand::Insert {
            collection: "users".to_string(),
            documents: vec![doc! { "name": "Ada" }],
        };
        assert_eq!(cmd.collection(), "users");
    }

    #[test]
    fn test_all_variants_expose_collection() {
        let cmds = vec![
            PartitionCommand::Insert {
                collection: "a".into(),
                documents: vec![doc! {}],
            },
            PartitionCommand::find("b", doc! {}),
            PartitionCommand::Update {
                collection: "c".into(),
                filter: doc! {},
                update: doc! {},
                multi: true,
            },
            PartitionCommand::Delete {
                collection: "d".into(),
                filter: doc! {},
                limit: 0,
            },
            PartitionCommand::Count {
                collection: "e".into(),
                filter: doc! {},
            },
        ];

        let expected = ["a", "b", "c", "d", "e"];
        for (cmd, want) in cmds.into_iter().zip(expected) {
            assert_eq!(cmd.collection(), want);
        }
    }

    #[test]
    fn test_partition_result_variants_are_debuggable() {
        let results = vec![
            PartitionResult::Inserted { inserted_count: 1 },
            PartitionResult::Found {
                documents: vec![doc! { "_id": 1 }],
            },
            PartitionResult::Modified {
                matched_count: 3,
                modified_count: 2,
            },
            PartitionResult::Deleted { deleted_count: 3 },
            PartitionResult::Error {
                message: "boom".into(),
            },
        ];

        for result in results {
            assert!(!format!("{:?}", result).is_empty());
        }
    }
}
