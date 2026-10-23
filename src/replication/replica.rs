//! One Raft member: the TCP transport, the state machine, and the role
//! reported by `hello`.
//!
//! A write is proposed from the network task. The state machine applies
//! the committed entry through [`super::CommandApplier`], which dispatches
//! to the partition workers. The worker does not call back into Raft.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bson::{doc, Document};
use openraft::error::{ClientWriteError, InitializeError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::RaftStateMachine;
use openraft::{
    EntryPayload, LogId, Raft, RaftSnapshotBuilder, ServerState, Snapshot, SnapshotMeta,
    StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Mutex};

use crate::logger::LOGGER;

use super::oplog::{self, OplogStore};
use super::{CommandApplier, Member, OplogCommand, OplogResponse, TypeConfig};

/// Election and heartbeat intervals. The election window must sit strictly
/// above the heartbeat, which is the Raft requirement OpenRaft checks.
#[derive(Clone, Copy)]
pub struct Timing {
    pub heartbeat_ms: u64,
    pub election_min_ms: u64,
    pub election_max_ms: u64,
}

impl Timing {
    pub fn production() -> Self {
        Self {
            heartbeat_ms: 50,
            election_min_ms: 150,
            election_max_ms: 300,
        }
    }

    /// Short enough for tests, still above the heartbeat.
    pub fn test() -> Self {
        Self {
            heartbeat_ms: 40,
            election_min_ms: 100,
            election_max_ms: 200,
        }
    }
}

pub struct NodeSpec {
    pub id: u64,
    pub set_name: String,
    pub members: BTreeMap<u64, Member>,
    pub data_dir: PathBuf,
    pub timing: Timing,
    pub applier: Arc<dyn CommandApplier>,
}

pub struct Replica {
    pub id: u64,
    raft: Raft<TypeConfig>,
    oplog: Arc<OplogStore>,
    members: BTreeMap<u64, Member>,
    me: Member,
    set_name: String,
    stop: watch::Sender<bool>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

/// Starts this process as a replica-set member.
///
/// `bootstrap` commits the voter set and starts an election. Other
/// processes leave it false and wait for that log. An arbiter is enrolled
/// afterwards as a learner: it receives the oplog and does not vote, so it
/// cannot become primary. OpenRaft will elect any voter, and a data-less
/// primary cannot serve the database.
pub async fn boot(
    config: &crate::config::Config,
    applier: Arc<dyn CommandApplier>,
) -> Result<Replica, String> {
    let replication = &config.replication;
    let me = replication
        .members
        .iter()
        .find(|member| member.id == replication.id)
        .ok_or("replication.id is not listed in members")?;
    let listener = TcpListener::bind(&me.raft)
        .await
        .map_err(|err| format!("raft listen {}: {}", me.raft, err))?;
    let members = replication
        .members
        .iter()
        .map(|member| {
            (
                member.id,
                Member {
                    addr: member.raft.clone(),
                    client: member.client.clone(),
                    arbiter: member.arbiter,
                },
            )
        })
        .collect();
    let spec = NodeSpec {
        id: replication.id,
        set_name: replication.set_name.clone(),
        members,
        data_dir: PathBuf::from(&config.storage.data_dir).join(format!("raft-{}", replication.id)),
        timing: Timing::production(),
        applier,
    };
    let replica = Replica::start(spec, listener).await?;
    if replication.bootstrap {
        replica.form().await?;
    } else {
        LOGGER.info(format!(
            "replica {} is waiting for the bootstrap node",
            replica.id
        ));
    }
    LOGGER.info(format!(
        "replica set {} node {} raft {} client {}",
        replica.set_name, replica.id, replica.me.addr, replica.me.client
    ));
    Ok(replica)
}

pub fn not_primary() -> Document {
    doc! {
        "ok": 0.0,
        "errmsg": "not primary",
        "code": 10107i32,
        "codeName": "NotWritablePrimary",
    }
}

impl Replica {
    pub async fn start(spec: NodeSpec, listener: TcpListener) -> Result<Self, String> {
        let me = spec
            .members
            .get(&spec.id)
            .cloned()
            .ok_or("this node is missing from the member list")?;
        std::fs::create_dir_all(&spec.data_dir).map_err(|err| err.to_string())?;
        let oplog = Arc::new(OplogStore::open(&spec.data_dir)?);
        let machine = StateMachine::open(&spec.data_dir, me.arbiter, spec.applier)?;
        let config = raft_config(&spec.set_name, spec.timing)?;
        let raft = Raft::new(spec.id, config, Factory, oplog.clone(), machine)
            .await
            .map_err(|err| err.to_string())?;

        let (stop, _) = watch::channel(false);
        let mut tasks = Vec::new();
        let accept_raft = raft.clone();
        let mut accept_stop = stop.subscribe();
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    changed = accept_stop.changed() => {
                        if changed.is_err() || *accept_stop.borrow() {
                            break;
                        }
                    }
                    accepted = listener.accept() => {
                        let Ok((socket, _)) = accepted else { break };
                        let raft = accept_raft.clone();
                        tokio::spawn(async move {
                            let _ = serve_rpc(socket, raft).await;
                        });
                    }
                }
            }
        }));

        let arbiters: Vec<(u64, Member)> = spec
            .members
            .iter()
            .filter(|(_, member)| member.arbiter)
            .map(|(id, member)| (*id, member.clone()))
            .collect();
        if !arbiters.is_empty() && !me.arbiter {
            let enroll_raft = raft.clone();
            let mut enroll_stop = stop.subscribe();
            tasks.push(tokio::spawn(async move {
                enroll(enroll_raft, arbiters, &mut enroll_stop).await;
            }));
        }

        Ok(Self {
            id: spec.id,
            raft,
            oplog,
            members: spec.members,
            me,
            set_name: spec.set_name,
            stop,
            tasks: Mutex::new(tasks),
        })
    }

    /// Commits the voter membership and campaigns. Safe to call again after
    /// a restart: an already-formed log answers `NotAllowed`.
    pub async fn form(&self) -> Result<(), String> {
        if self.me.arbiter {
            return Err("an arbiter cannot bootstrap the set".into());
        }
        let voters: BTreeMap<u64, Member> = self
            .members
            .iter()
            .filter(|(_, member)| !member.arbiter)
            .map(|(id, member)| (*id, member.clone()))
            .collect();
        if !voters.contains_key(&self.id) {
            return Err("this node is not a voting member".into());
        }
        match self.raft.initialize(voters).await {
            Ok(()) => Ok(()),
            Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            Err(err) => Err(err.to_string()),
        }
    }

    pub async fn shutdown(&self) {
        let _ = self.stop.send(true);
        let mut tasks = self.tasks.lock().await;
        for task in tasks.drain(..) {
            task.abort();
        }
        let _ = self.raft.shutdown().await;
    }

    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().state == ServerState::Leader
    }

    pub fn watch(&self) -> watch::Receiver<openraft::RaftMetrics<u64, Member>> {
        self.raft.metrics()
    }

    pub async fn wait_applied(&self, index: u64) {
        let mut metrics = self.raft.metrics();
        loop {
            let applied = metrics
                .borrow()
                .last_applied
                .map(|id| id.index >= index)
                .unwrap_or(false);
            if applied {
                return;
            }
            if metrics.changed().await.is_err() {
                return;
            }
        }
    }

    /// Fills the replica-set fields drivers read from `hello`.
    ///
    /// `hosts`, `primary`, and `me` are the client addresses. A driver that
    /// wants the nearest member measures those itself; this process does
    /// not proxy the read.
    pub fn overlay_hello(&self, mut hello: Document) -> Document {
        let metrics = self.raft.metrics().borrow().clone();
        let writable = metrics.state == ServerState::Leader && !self.me.arbiter;
        hello.insert("isWritablePrimary", writable);
        hello.insert("ismaster", writable);
        hello.insert("secondary", !writable && !self.me.arbiter);
        hello.insert("arbiterOnly", self.me.arbiter);
        hello.insert("setName", self.set_name.clone());
        hello.insert("setVersion", 1i32);
        hello.insert("me", self.me.client.clone());
        let hosts: Vec<bson::Bson> = self
            .members
            .values()
            .filter(|member| !member.arbiter)
            .map(|member| bson::Bson::String(member.client.clone()))
            .collect();
        hello.insert("hosts", hosts);
        let arbiters: Vec<bson::Bson> = self
            .members
            .values()
            .filter(|member| member.arbiter)
            .map(|member| bson::Bson::String(member.client.clone()))
            .collect();
        if !arbiters.is_empty() {
            hello.insert("arbiters", arbiters);
        }
        if let Some(leader) = metrics.current_leader {
            if let Some(member) = self.members.get(&leader) {
                if !member.arbiter {
                    hello.insert("primary", member.client.clone());
                }
            }
        }
        hello
    }

    pub fn status(&self) -> Document {
        let metrics = self.raft.metrics().borrow().clone();
        let my_state = if self.me.arbiter {
            7
        } else if metrics.state == ServerState::Leader {
            1
        } else {
            2
        };
        let mut members = bson::Array::new();
        for (id, member) in &self.members {
            let state = if member.arbiter {
                7
            } else if metrics.current_leader == Some(*id) {
                1
            } else {
                2
            };
            members.push(bson::Bson::Document(doc! {
                "_id": *id as i64,
                "name": member.client.clone(),
                "state": state,
                "arbiterOnly": member.arbiter,
            }));
        }
        doc! {
            "ok": 1.0,
            "set": self.set_name.clone(),
            "myState": my_state,
            "term": metrics.current_term as i64,
            "members": members,
        }
    }

    /// `local.oplog.rs` cursor. `ts` is the Raft log index.
    pub fn oplog_cursor(&self, command: &Document) -> Document {
        let mut batch = bson::Array::new();
        for (index, term, body) in self.oplog.commands_after(oplog_bound(command)) {
            if batch.len() == 256 {
                break;
            }
            let operation = Document::from_reader(&mut Cursor::new(body)).unwrap_or_default();
            batch.push(bson::Bson::Document(doc! {
                "ts": index as i64,
                "t": term as i64,
                "op": "c",
                "o": operation,
            }));
        }
        doc! {
            "ok": 1.0,
            "cursor": {
                "id": 0i64,
                "ns": "local.oplog.rs",
                "firstBatch": batch,
            }
        }
    }

    /// Proposes one auto-commit command. Inserts without `_id` receive one
    /// here, so every replica stores the same id.
    pub async fn propose(&self, mut command: Document) -> (Document, u64) {
        stamp_insert_ids(&mut command);
        let body = match bson::to_vec(&command) {
            Ok(body) => body,
            Err(err) => return (doc! { "ok": 0.0, "errmsg": err.to_string() }, 0),
        };
        match self.raft.client_write(OplogCommand { body }).await {
            Ok(response) => (decode_reply(&response.data.body), response.log_id.index),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(_))) => (not_primary(), 0),
            Err(err) => (doc! { "ok": 0.0, "errmsg": err.to_string() }, 0),
        }
    }
}

impl Drop for Replica {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Ok(mut tasks) = self.tasks.try_lock() {
            for task in tasks.drain(..) {
                task.abort();
            }
        }
    }
}

fn raft_config(set_name: &str, timing: Timing) -> Result<Arc<openraft::Config>, String> {
    let config = openraft::Config {
        cluster_name: set_name.to_string(),
        heartbeat_interval: timing.heartbeat_ms,
        election_timeout_min: timing.election_min_ms,
        election_timeout_max: timing.election_max_ms,
        snapshot_policy: openraft::SnapshotPolicy::Never,
        ..openraft::Config::default()
    };
    config
        .validate()
        .map(Arc::new)
        .map_err(|err| err.to_string())
}

fn stamp_insert_ids(command: &mut Document) {
    if !command.contains_key("insert") {
        return;
    }
    let Some(bson::Bson::Array(values)) = command.get_mut("documents") else {
        return;
    };
    for value in values {
        let Some(document) = value.as_document_mut() else {
            continue;
        };
        if !document.contains_key("_id") {
            document.insert("_id", bson::Bson::ObjectId(bson::oid::ObjectId::new()));
        }
    }
}

fn decode_reply(bytes: &[u8]) -> Document {
    Document::from_reader(&mut Cursor::new(bytes))
        .unwrap_or_else(|err| doc! { "ok": 0.0, "errmsg": err.to_string() })
}

fn oplog_bound(command: &Document) -> u64 {
    let Ok(filter) = command.get_document("filter") else {
        return 0;
    };
    let Ok(ts) = filter.get_document("ts") else {
        return 0;
    };
    if let Ok(gt) = ts.get_i64("$gt") {
        return gt.max(0) as u64;
    }
    if let Ok(gte) = ts.get_i64("$gte") {
        return gte.max(0).saturating_sub(1) as u64;
    }
    0
}

async fn enroll(
    raft: Raft<TypeConfig>,
    arbiters: Vec<(u64, Member)>,
    stop: &mut watch::Receiver<bool>,
) {
    let mut metrics = raft.metrics();
    loop {
        if *stop.borrow() {
            return;
        }
        if metrics.borrow().state == ServerState::Leader {
            let mut failed = false;
            for (id, node) in &arbiters {
                if *stop.borrow() {
                    return;
                }
                if let Err(err) = raft.add_learner(*id, node.clone(), true).await {
                    LOGGER.warn(format!("arbiter {} was not enrolled: {}", id, err));
                    failed = true;
                }
            }
            if !failed {
                return;
            }
        }
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    return;
                }
            }
            changed = metrics.changed() => {
                if changed.is_err() {
                    return;
                }
            }
        }
    }
}

fn raft_config_error(err: std::io::Error) -> Unreachable {
    Unreachable::new(&err)
}

struct Factory;

struct Conn {
    addr: String,
}

impl RaftNetworkFactory<TypeConfig> for Factory {
    type Network = Conn;

    async fn new_client(&mut self, _target: u64, node: &Member) -> Self::Network {
        Conn {
            addr: node.addr.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for Conn {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, Member, RaftError<u64>>> {
        match roundtrip(&self.addr, &RpcIn::Append(rpc)).await {
            Ok(RpcOut::Append(response)) => Ok(response),
            Ok(_) => Err(RPCError::Unreachable(raft_config_error(
                std::io::Error::new(std::io::ErrorKind::InvalidData, "unexpected raft reply"),
            ))),
            Err(err) => Err(RPCError::Unreachable(raft_config_error(err))),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, Member, RaftError<u64>>> {
        match roundtrip(&self.addr, &RpcIn::Vote(rpc)).await {
            Ok(RpcOut::Vote(response)) => Ok(response),
            Ok(_) => Err(RPCError::Unreachable(raft_config_error(
                std::io::Error::new(std::io::ErrorKind::InvalidData, "unexpected raft reply"),
            ))),
            Err(err) => Err(RPCError::Unreachable(raft_config_error(err))),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, Member, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        match roundtrip(&self.addr, &RpcIn::Snapshot(rpc)).await {
            Ok(RpcOut::Snapshot(response)) => Ok(response),
            Ok(_) => Err(RPCError::Unreachable(raft_config_error(
                std::io::Error::new(std::io::ErrorKind::InvalidData, "unexpected raft reply"),
            ))),
            Err(err) => Err(RPCError::Unreachable(raft_config_error(err))),
        }
    }
}

#[derive(Serialize, Deserialize)]
enum RpcIn {
    Append(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<u64>),
    Snapshot(InstallSnapshotRequest<TypeConfig>),
}

#[derive(Serialize, Deserialize)]
enum RpcOut {
    Append(AppendEntriesResponse<u64>),
    Vote(VoteResponse<u64>),
    Snapshot(InstallSnapshotResponse<u64>),
}

const RPC_LIMIT: usize = 32 * 1024 * 1024;

async fn roundtrip(addr: &str, request: &RpcIn) -> std::io::Result<RpcOut> {
    let mut stream = tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(addr))
        .await
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::TimedOut, "raft connect"))??;
    let _ = stream.set_nodelay(true);
    let bytes = serde_json::to_vec(request)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    write_frame(&mut stream, &bytes).await?;
    let body = read_frame(&mut stream).await?;
    serde_json::from_slice(&body)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))
}

async fn serve_rpc(mut socket: TcpStream, raft: Raft<TypeConfig>) -> std::io::Result<()> {
    let _ = socket.set_nodelay(true);
    let body = read_frame(&mut socket).await?;
    let request: RpcIn = serde_json::from_slice(&body)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    let response = match request {
        RpcIn::Append(rpc) => RpcOut::Append(raft.append_entries(rpc).await.map_err(io_from_raft)?),
        RpcIn::Vote(rpc) => RpcOut::Vote(raft.vote(rpc).await.map_err(io_from_raft)?),
        RpcIn::Snapshot(rpc) => {
            RpcOut::Snapshot(raft.install_snapshot(rpc).await.map_err(io_from_raft)?)
        }
    };
    let bytes = serde_json::to_vec(&response)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    write_frame(&mut socket, &bytes).await
}

fn io_from_raft(err: impl std::fmt::Display) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::ConnectionAborted, err.to_string())
}

async fn write_frame(socket: &mut TcpStream, bytes: &[u8]) -> std::io::Result<()> {
    if bytes.len() > RPC_LIMIT {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "raft frame too large",
        ));
    }
    socket
        .write_all(&(bytes.len() as u32).to_le_bytes())
        .await?;
    socket.write_all(bytes).await?;
    socket.flush().await
}

async fn read_frame(socket: &mut TcpStream) -> std::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    socket.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len > RPC_LIMIT {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "raft frame too large",
        ));
    }
    let mut body = vec![0u8; len];
    socket.read_exact(&mut body).await?;
    Ok(body)
}

struct StateMachine {
    dir: PathBuf,
    arbiter: bool,
    applier: Arc<dyn CommandApplier>,
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, Member>,
    snapshot_meta: Option<SnapshotMeta<u64, Member>>,
    snapshot: Vec<u8>,
}

#[derive(Serialize, Deserialize)]
struct DiskState {
    last_applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, Member>,
    snapshot_meta: Option<SnapshotMeta<u64, Member>>,
    snapshot: Vec<u8>,
}

impl StateMachine {
    fn open(
        dir: &std::path::Path,
        arbiter: bool,
        applier: Arc<dyn CommandApplier>,
    ) -> Result<Self, String> {
        let path = dir.join("state.json");
        let (last_applied, membership, snapshot_meta, snapshot) = if path.exists() {
            let bytes = std::fs::read(&path).map_err(|err| err.to_string())?;
            let disk: DiskState = serde_json::from_slice(&bytes).map_err(|err| err.to_string())?;
            (
                disk.last_applied,
                disk.membership,
                disk.snapshot_meta,
                disk.snapshot,
            )
        } else {
            (None, StoredMembership::default(), None, Vec::new())
        };
        Ok(Self {
            dir: dir.to_path_buf(),
            arbiter,
            applier,
            last_applied,
            membership,
            snapshot_meta,
            snapshot,
        })
    }

    #[allow(clippy::result_large_err)]
    fn persist(&self) -> Result<(), StorageError<u64>> {
        let disk = DiskState {
            last_applied: self.last_applied,
            membership: self.membership.clone(),
            snapshot_meta: self.snapshot_meta.clone(),
            snapshot: self.snapshot.clone(),
        };
        let bytes = serde_json::to_vec(&disk).map_err(state_failure)?;
        oplog::atomic_write(&self.dir.join("state.json"), &bytes).map_err(state_failure)
    }
}

struct BuiltSnapshot {
    meta: SnapshotMeta<u64, Member>,
    bytes: Vec<u8>,
}

impl RaftSnapshotBuilder<TypeConfig> for BuiltSnapshot {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        Ok(Snapshot {
            meta: self.meta.clone(),
            snapshot: Box::new(Cursor::new(self.bytes.clone())),
        })
    }
}

impl RaftStateMachine<TypeConfig> for StateMachine {
    type SnapshotBuilder = BuiltSnapshot;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, Member>), StorageError<u64>> {
        Ok((self.last_applied, self.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<OplogResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + openraft::OptionalSend,
        I::IntoIter: openraft::OptionalSend,
    {
        let mut responses = Vec::new();
        for entry in entries {
            let log_id = entry.log_id;
            let response = match entry.payload {
                EntryPayload::Normal(command) if !self.arbiter => {
                    let body = self.applier.apply(&command.body).await;
                    OplogResponse { body }
                }
                EntryPayload::Membership(membership) => {
                    self.membership = StoredMembership::new(Some(log_id), membership);
                    empty_response()
                }
                _ => empty_response(),
            };
            self.last_applied = Some(log_id);
            responses.push(response);
        }
        self.persist()?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        let meta = self.snapshot_meta.clone().unwrap_or(SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.membership.clone(),
            snapshot_id: self
                .last_applied
                .map(|id| id.index.to_string())
                .unwrap_or_else(|| "0".into()),
        });
        BuiltSnapshot {
            meta,
            bytes: self.snapshot.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, Member>,
        mut snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let mut bytes = Vec::new();
        std::io::Read::read_to_end(&mut snapshot, &mut bytes).map_err(|err| {
            StorageError::from(openraft::StorageIOError::read_snapshot(
                None,
                openraft::AnyError::new(&err),
            ))
        })?;
        self.snapshot_meta = Some(meta.clone());
        self.snapshot = bytes;
        self.last_applied = meta.last_log_id;
        self.membership = meta.last_membership.clone();
        self.persist()
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let Some(meta) = self.snapshot_meta.clone() else {
            return Ok(None);
        };
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(self.snapshot.clone())),
        }))
    }
}

fn state_failure(err: impl std::error::Error + 'static) -> StorageError<u64> {
    openraft::StorageIOError::write_state_machine(openraft::AnyError::new(&err)).into()
}

fn empty_response() -> OplogResponse {
    let body = bson::to_vec(&doc! { "ok": 1.0 }).unwrap_or_default();
    OplogResponse { body }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use bson::doc;

    use crate::config::StorageConfig;
    use crate::protocol;
    use crate::storage::PartitionRouter;
    use crate::types::{PartitionCommand, PartitionResult};

    use super::*;

    struct Apply {
        router: PartitionRouter,
        calls: Arc<AtomicU64>,
    }

    #[async_trait::async_trait]
    impl CommandApplier for Apply {
        async fn apply(&self, body: &[u8]) -> Vec<u8> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let command = Document::from_reader(&mut Cursor::new(body)).expect("command bson");
            let parsed = protocol::command_from_document(&command).expect("command");
            let result = self
                .router
                .dispatch(parsed)
                .expect("worker")
                .await
                .expect("reply");
            bson::to_vec(&protocol::reply_document(&result)).unwrap_or_default()
        }
    }

    struct Node {
        replica: Replica,
        router: PartitionRouter,
        calls: Arc<AtomicU64>,
    }

    async fn bind_members(flags: &[(u64, bool)]) -> (Vec<TcpListener>, BTreeMap<u64, Member>) {
        let mut listeners = Vec::new();
        let mut members = BTreeMap::new();
        for (id, arbiter) in flags {
            let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
            let addr = listener.local_addr().expect("addr").to_string();
            members.insert(
                *id,
                Member {
                    addr,
                    client: format!("127.0.0.1:{}", 20000 + id),
                    arbiter: *arbiter,
                },
            );
            listeners.push(listener);
        }
        (listeners, members)
    }

    async fn start_set(flags: &[(u64, bool)]) -> Vec<Node> {
        let (listeners, members) = bind_members(flags).await;
        let mut nodes = Vec::new();
        for ((id, _), listener) in flags.iter().zip(listeners) {
            let storage = StorageConfig::ephemeral();
            let router = PartitionRouter::with_storage(1, storage.clone());
            let calls = Arc::new(AtomicU64::new(0));
            let spec = NodeSpec {
                id: *id,
                set_name: "kova".into(),
                members: members.clone(),
                data_dir: std::path::PathBuf::from(&storage.data_dir).join(format!("raft-{id}")),
                timing: Timing::test(),
                applier: Arc::new(Apply {
                    router: router.clone(),
                    calls: calls.clone(),
                }),
            };
            let replica = Replica::start(spec, listener).await.expect("start");
            nodes.push(Node {
                replica,
                router,
                calls,
            });
        }
        nodes[0].replica.form().await.expect("form");
        nodes
    }

    async fn leader_id(nodes: &[Node]) -> u64 {
        let mut watch = nodes[0].replica.watch();
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(id) = watch.borrow().current_leader {
                    if nodes
                        .iter()
                        .any(|node| node.replica.id == id && node.replica.is_leader())
                    {
                        return id;
                    }
                }
                if watch.changed().await.is_err() {
                    panic!("metrics closed");
                }
            }
        })
        .await
        .expect("leader")
    }

    async fn names(router: &PartitionRouter) -> Vec<String> {
        let command = protocol::command_from_document(&doc! {
            "find": "emp",
            "$db": "db",
            "filter": {},
        })
        .expect("find");
        let result = router
            .dispatch(command)
            .expect("worker")
            .await
            .expect("reply");
        match result {
            PartitionResult::Found { documents } => {
                let mut names: Vec<String> = documents
                    .iter()
                    .filter_map(|document| document.get_str("ename").ok().map(str::to_string))
                    .collect();
                names.sort();
                names
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    async fn insert(node: &Node, ename: &str) -> u64 {
        let (reply, index) = node
            .replica
            .propose(doc! {
                "insert": "emp",
                "$db": "db",
                "documents": [ { "ename": ename, "sal": 5000i32 } ],
            })
            .await;
        assert_eq!(reply.get_f64("ok").expect("ok"), 1.0, "{reply:?}");
        index
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn write_replicates_and_a_new_leader_accepts_the_next_write() {
        let mut nodes = start_set(&[(1, false), (2, false), (3, false)]).await;
        let first = leader_id(&nodes).await;
        let primary = nodes
            .iter()
            .find(|node| node.replica.id == first)
            .expect("primary");
        let index = insert(primary, "KING").await;
        for node in &nodes {
            tokio::time::timeout(
                std::time::Duration::from_secs(8),
                node.replica.wait_applied(index),
            )
            .await
            .expect("applied");
            assert_eq!(names(&node.router).await, vec!["KING".to_string()]);
        }
        let hello = primary
            .replica
            .overlay_hello(doc! { "ok": 1.0, "isWritablePrimary": false });
        assert!(hello.get_bool("isWritablePrimary").expect("writable"));
        assert_eq!(hello.get_str("setName").expect("set"), "kova");
        let oplog = primary
            .replica
            .oplog_cursor(&doc! { "find": "oplog.rs", "filter": { "ts": { "$gt": 0i64 } } });
        let batch = oplog
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("batch");
        assert!(!batch.is_empty());
        assert!(primary
            .replica
            .oplog
            .commands_after(0)
            .iter()
            .any(
                |(_, _, body)| Document::from_reader(&mut Cursor::new(body.as_slice()))
                    .ok()
                    .and_then(|command| command
                        .get_array("documents")
                        .ok()
                        .map(|docs| !docs.is_empty()))
                    .unwrap_or(false)
            ));

        let primary = nodes
            .iter_mut()
            .find(|node| node.replica.id == first)
            .expect("primary");
        primary.replica.shutdown().await;
        let rest: Vec<&Node> = nodes
            .iter()
            .filter(|node| node.replica.id != first)
            .collect();
        let mut watch = rest[0].replica.watch();
        let second = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Some(id) = watch.borrow().current_leader {
                    if id != first
                        && rest
                            .iter()
                            .any(|node| node.replica.id == id && node.replica.is_leader())
                    {
                        return id;
                    }
                }
                if watch.changed().await.is_err() {
                    panic!("metrics closed");
                }
            }
        })
        .await
        .expect("second leader");
        let next = nodes
            .iter()
            .find(|node| node.replica.id == second)
            .expect("next");
        let index = insert(next, "SCOTT").await;
        for node in nodes.iter().filter(|node| node.replica.id != first) {
            tokio::time::timeout(
                std::time::Duration::from_secs(8),
                node.replica.wait_applied(index),
            )
            .await
            .expect("applied");
            assert_eq!(
                names(&node.router).await,
                vec!["KING".to_string(), "SCOTT".to_string()]
            );
        }
        let follower = nodes
            .iter()
            .find(|node| node.replica.id != first && node.replica.id != second);
        let follower = follower.expect("follower");
        let hello = follower.replica.overlay_hello(doc! { "ok": 1.0 });
        assert!(hello.get_bool("secondary").expect("secondary"));
        assert!(!hello.get_bool("isWritablePrimary").expect("writable"));

        for node in &nodes {
            node.replica.shutdown().await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_arbiter_keeps_the_oplog_and_stores_no_documents() {
        let nodes = start_set(&[(1, false), (2, false), (3, true)]).await;
        let leader = leader_id(&nodes).await;
        let primary = nodes
            .iter()
            .find(|node| node.replica.id == leader)
            .expect("primary");
        let index = insert(primary, "KING").await;
        for node in &nodes {
            tokio::time::timeout(
                std::time::Duration::from_secs(8),
                node.replica.wait_applied(index),
            )
            .await
            .expect("applied");
        }
        let arbiter = nodes
            .iter()
            .find(|node| node.replica.id == 3)
            .expect("arbiter");
        assert_eq!(arbiter.calls.load(Ordering::Relaxed), 0);
        assert!(names(&arbiter.router).await.is_empty());
        assert!(!arbiter
            .replica
            .oplog_cursor(&doc! {})
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap()
            .is_empty());
        let hello = arbiter.replica.overlay_hello(doc! { "ok": 1.0 });
        assert!(hello.get_bool("arbiterOnly").expect("arbiter"));
        assert!(!hello.get_bool("isWritablePrimary").expect("writable"));
        assert_ne!(arbiter.replica.id, leader);
        for node in &nodes {
            node.replica.shutdown().await;
        }
    }

    async fn local(router: &PartitionRouter, command: PartitionCommand) -> PartitionResult {
        router
            .dispatch(command)
            .expect("worker")
            .await
            .expect("reply")
    }

    fn oplog_bodies(node: &Node) -> Vec<Document> {
        node.replica
            .oplog
            .commands_after(0)
            .iter()
            .filter_map(|(_, _, body)| {
                Document::from_reader(&mut Cursor::new(body.as_slice())).ok()
            })
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_commit_replicates_and_an_abort_stays_on_the_primary() {
        let nodes = start_set(&[(1, false), (2, false), (3, false)]).await;
        let leader = leader_id(&nodes).await;
        let primary = nodes
            .iter()
            .find(|node| node.replica.id == leader)
            .expect("primary");

        let kept = match local(
            &primary.router,
            PartitionCommand::BeginTransaction {
                collection: "db.emp".into(),
                txn_id: 0,
                isolation: "snapshot".into(),
                timeout_ms: None,
            },
        )
        .await
        {
            PartitionResult::Transaction { txn_id } => txn_id,
            other => panic!("expected a transaction, got {other:?}"),
        };
        local(
            &primary.router,
            PartitionCommand::Transactional {
                txn_id: kept,
                inner: Box::new(PartitionCommand::Insert {
                    collection: "db.emp".into(),
                    documents: vec![doc! { "_id": "king", "ename": "KING" }],
                }),
            },
        )
        .await;

        let dropped = match local(
            &primary.router,
            PartitionCommand::BeginTransaction {
                collection: "db.emp".into(),
                txn_id: 0,
                isolation: "snapshot".into(),
                timeout_ms: None,
            },
        )
        .await
        {
            PartitionResult::Transaction { txn_id } => txn_id,
            other => panic!("expected a transaction, got {other:?}"),
        };
        local(
            &primary.router,
            PartitionCommand::Transactional {
                txn_id: dropped,
                inner: Box::new(PartitionCommand::Insert {
                    collection: "db.emp".into(),
                    documents: vec![doc! { "_id": "clark", "ename": "CLARK" }],
                }),
            },
        )
        .await;
        local(
            &primary.router,
            PartitionCommand::AbortTransaction {
                collection: "db.emp".into(),
                txn_id: dropped,
            },
        )
        .await;
        assert!(
            oplog_bodies(primary).iter().all(|command| {
                !command.contains_key("beginTransaction")
                    && !command.contains_key("abort")
                    && !command.contains_key("txnId")
            }),
            "begin, abort, and staged statements stay off the oplog"
        );

        let (reply, index) = crate::network::propose_commit(
            &doc! {
                "commit": 1i32,
                "collection": "emp",
                "txnId": kept as i64,
                "$db": "db",
            },
            &primary.router,
            &primary.replica,
        )
        .await;
        assert_eq!(reply.get_f64("ok").expect("ok"), 1.0, "{reply:?}");
        for node in &nodes {
            tokio::time::timeout(
                std::time::Duration::from_secs(8),
                node.replica.wait_applied(index),
            )
            .await
            .expect("applied");
            assert_eq!(names(&node.router).await, vec!["KING".to_string()]);
        }
        let bodies = oplog_bodies(primary);
        assert!(bodies
            .iter()
            .any(|command| command.contains_key("applyTransaction")));
        assert!(bodies.iter().all(|command| {
            !command.contains_key("beginTransaction")
                && !command.contains_key("abort")
                && !command.contains_key("txnId")
                && command
                    .get_array("ops")
                    .map(|ops| !format!("{ops:?}").contains("CLARK"))
                    .unwrap_or(true)
        }));
        for node in &nodes {
            node.replica.shutdown().await;
        }
    }
}
