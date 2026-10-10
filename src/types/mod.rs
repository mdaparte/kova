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
    /// The command could not be executed.
    Error { message: String },
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
    Find {
        collection: String,
        filter: Document,
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
    /// List the collections known to every partition (`listCollections`).
    ///
    /// The routing key is empty on purpose: collection names are hashed
    /// across partitions, so the network layer broadcasts this command
    /// to all workers and merges their replies.
    ListCollections { filter: Document },
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
    },
}

impl PartitionCommand {
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
            | PartitionCommand::Search { collection, .. } => collection,
            // Broadcast command: no single partition owns it.
            PartitionCommand::ListCollections { .. } => "",
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

/// One full-text search hit: the storage-side document id.
///
/// Hits arrive already ordered by BM25 relevance, so the lookup order
/// in the worker reproduces the ranking. The search module returns
/// ids; the worker looks up the actual documents and replies with
/// them, so raw index internals never cross the module boundary.
#[derive(Debug, Clone)]
pub struct SearchHit {
    pub id: String,
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
            PartitionCommand::Find {
                collection: "b".into(),
                filter: doc! {},
            },
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
