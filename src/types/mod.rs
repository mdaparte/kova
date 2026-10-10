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
    /// Documents were modified.
    Modified { modified_count: u32 },
    /// Documents were removed.
    Deleted { deleted_count: u32 },
    /// The command could not be executed.
    Error { message: String },
}

/// A command sent from the network layer to a storage partition.
#[derive(Debug, Clone)]
pub enum PartitionCommand {
    /// Insert a single document into a collection.
    Insert {
        collection: String,
        document: Document,
    },
    /// Find documents matching a filter.
    Find {
        collection: String,
        filter: Document,
    },
    /// Update documents matching a filter.
    Update {
        collection: String,
        filter: Document,
        update: Document,
    },
    /// Delete documents matching a filter.
    Delete {
        collection: String,
        filter: Document,
    },
    /// Count documents matching a filter (used by the `count` command).
    Count {
        collection: String,
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
            | PartitionCommand::Count { collection, .. } => collection,
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

#[cfg(test)]
mod tests {
    use super::*;
    use bson::doc;

    #[test]
    fn test_command_collection_routing_key() {
        let cmd = PartitionCommand::Insert {
            collection: "users".to_string(),
            document: doc! { "name": "Ada" },
        };
        assert_eq!(cmd.collection(), "users");
    }

    #[test]
    fn test_all_variants_expose_collection() {
        let cmds = vec![
            PartitionCommand::Insert {
                collection: "a".into(),
                document: doc! {},
            },
            PartitionCommand::Find {
                collection: "b".into(),
                filter: doc! {},
            },
            PartitionCommand::Update {
                collection: "c".into(),
                filter: doc! {},
                update: doc! {},
            },
            PartitionCommand::Delete {
                collection: "d".into(),
                filter: doc! {},
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
            PartitionResult::Modified { modified_count: 2 },
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
