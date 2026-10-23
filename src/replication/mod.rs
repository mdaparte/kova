//! Replica set inside the Kova process.
//!
//! Election, heartbeats, and the oplog are [OpenRaft](https://docs.rs/openraft).
//! Kova does not run a second election protocol and does not embed
//! ZooKeeper. With no `[replication]` section the process stays a single
//! writable node, which is what `make run-debug` does.

mod oplog;
mod replica;

use std::io::Cursor;

use serde::{Deserialize, Serialize};

pub use replica::{boot, not_primary, Replica};

/// One committed client command. `body` is the BSON document the client sent.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OplogCommand {
    pub body: Vec<u8>,
}

/// BSON reply produced when that command was applied.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OplogResponse {
    pub body: Vec<u8>,
}

/// A member as Raft stores it. `addr` is the Raft TCP endpoint.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Member {
    pub addr: String,
    pub client: String,
    pub arbiter: bool,
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = OplogCommand,
        R = OplogResponse,
        Node = Member,
);

/// Applies a committed oplog entry to this process.
///
/// The replica calls this from the Raft state machine, on the leader and
/// on every follower. It must not propose the command again.
#[async_trait::async_trait]
pub trait CommandApplier: Send + Sync + 'static {
    async fn apply(&self, body: &[u8]) -> Vec<u8>;

    /// Page files at the last applied oplog index.
    async fn snapshot(&self) -> Result<Vec<u8>, String>;

    /// Replaces local pages with a snapshot from the primary.
    async fn install(&self, bytes: &[u8]) -> Result<(), String>;
}
