//! Asynchronous TCP server speaking the MongoDB Wire Protocol.
//!
//! This module owns the connection lifecycle: accept a client, spawn a
//! Tokio task, read OP_MSG frames, hand them to the storage router and
//! write the reply back. Raw protocol parsing lives in
//! [`crate::protocol`]; this file handles I/O and command routing.
//!
//! Phase 5 (DataFusion) is wired in through [`crate::query_engine`];
//! SQL text arriving as an OP_MSG command is parsed, planned and executed
//! there against in-memory collections.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::spawn;
use tokio::sync::watch;

use crate::logger::LOGGER;
use crate::protocol;
use crate::query_engine;
use std::sync::Arc;

use crate::storage::PartitionRouter;
use crate::types::{PartitionCommand, PartitionResult, SearchOptions};

pub mod wire {
    //! Fixed offsets of the 16-byte MongoDB Wire Protocol header.

    pub const HEADER_SIZE: usize = 16;

    /// Total message length, including the header itself (int32 big-endian).
    pub const MESSAGE_LENGTH_OFFSET: usize = 0;
    /// The requestId of the current message (int32 big-endian).
    pub const REQUEST_ID_OFFSET: usize = 4;
    /// The requestId this message responds to (int32 big-endian).
    pub const RESPONSE_TO_OFFSET: usize = 8;
    /// The operation code to execute (int32 big-endian).
    pub const OP_CODE_OFFSET: usize = 12;

    /// OP_MSG operation code of the MongoDB Wire Protocol.
    pub const OP_MSG: i32 = 2013;

    /// OP_QUERY operation code (legacy handshake).
    pub const OP_QUERY: i32 = 2004;

    #[derive(Debug, Clone, Copy)]
    pub struct MessageHeader {
        /// Total header length in bytes (includes the header).
        pub message_length: u32,
        /// The requestId of this message.
        pub request_id: i32,
        /// The requestId this message responds to.
        #[cfg_attr(not(test), allow(dead_code))]
        pub response_to: i32,
        /// The operation code.
        pub op_code: i32,
    }

    impl MessageHeader {
        /// Parses a 16-byte header read from the TCP stream.
        ///
        /// Every integer in the MongoDB Wire Protocol is little-endian.
        pub fn parse(bytes: &[u8; HEADER_SIZE]) -> Self {
            Self {
                message_length: u32::from_le_bytes(
                    bytes[MESSAGE_LENGTH_OFFSET..MESSAGE_LENGTH_OFFSET + 4]
                        .try_into()
                        .expect("4 bytes at MESSAGE_LENGTH_OFFSET"),
                ),
                request_id: i32::from_le_bytes(
                    bytes[REQUEST_ID_OFFSET..REQUEST_ID_OFFSET + 4]
                        .try_into()
                        .expect("4 bytes at REQUEST_ID_OFFSET"),
                ),
                response_to: i32::from_le_bytes(
                    bytes[RESPONSE_TO_OFFSET..RESPONSE_TO_OFFSET + 4]
                        .try_into()
                        .expect("4 bytes at RESPONSE_TO_OFFSET"),
                ),
                op_code: i32::from_le_bytes(
                    bytes[OP_CODE_OFFSET..OP_CODE_OFFSET + 4]
                        .try_into()
                        .expect("4 bytes at OP_CODE_OFFSET"),
                ),
            }
        }
    }
}

/// Size of the standard MongoDB Wire Protocol header (4 fields of 4 bytes each).
pub const HEADER_SIZE: usize = wire::HEADER_SIZE;

/// Resolves the listening port from the global configuration.
///
/// The port comes from `kova.toml`, overridable with `KOVA_PORT`; an
/// invalid environment value was already warned about and discarded
/// during configuration load, so this never fails.
fn resolve_port() -> u16 {
    crate::config::CONFIG.port
}

/// Monotonic counter used for the `requestId` of our own replies.
static REPLY_REQUEST_ID: AtomicI32 = AtomicI32::new(1);

struct LiveConn {
    peer: std::net::SocketAddr,
    started: std::time::Instant,
    /// `None` when idle close is disabled.
    idle_deadline: Mutex<Option<std::time::Instant>>,
    user: Mutex<Option<String>>,
    close: watch::Sender<bool>,
}

struct ConnTable {
    next: AtomicU64,
    max_connections: usize,
    /// `None` disables the idle close (`max_connection_idle_ms = 0`).
    idle: Option<std::time::Duration>,
    slots: Mutex<HashMap<u64, LiveConn>>,
    stop: watch::Sender<bool>,
}

fn connections() -> Arc<ConnTable> {
    static TABLE: OnceLock<Arc<ConnTable>> = OnceLock::new();
    TABLE
        .get_or_init(|| Arc::new(ConnTable::from_config()))
        .clone()
}

impl ConnTable {
    fn from_config() -> Self {
        let config = &crate::config::CONFIG;
        Self::new(config.max_connections, config.max_connection_idle_ms)
    }

    /// A table with its own cap. `idle_ms` of 0 disables idle close.
    #[cfg_attr(not(test), allow(dead_code))]
    fn limited(max_connections: usize, idle_ms: u64) -> Arc<Self> {
        Arc::new(Self::new(max_connections, idle_ms))
    }

    fn new(max_connections: usize, idle_ms: u64) -> Self {
        let (stop, _) = watch::channel(false);
        Self {
            next: AtomicU64::new(1),
            max_connections,
            idle: (idle_ms > 0).then(|| std::time::Duration::from_millis(idle_ms)),
            slots: Mutex::new(HashMap::new()),
            stop,
        }
    }

    fn subscribe_stop(&self) -> watch::Receiver<bool> {
        self.stop.subscribe()
    }

    /// Reserves a slot. `None` means the cap is already full; the caller
    /// answers with an error and does not count the socket.
    fn try_open(&self, peer: std::net::SocketAddr) -> Option<(u64, watch::Receiver<bool>)> {
        let mut slots = self.slots.lock().expect("connections");
        if slots.len() >= self.max_connections {
            return None;
        }
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (close, watch_rx) = watch::channel(false);
        let now = std::time::Instant::now();
        slots.insert(
            id,
            LiveConn {
                peer,
                started: now,
                idle_deadline: Mutex::new(self.idle.map(|idle| now + idle)),
                user: Mutex::new(None),
                close,
            },
        );
        drop(slots);
        crate::metrics::METRICS.connection_opened();
        Some((id, watch_rx))
    }

    fn note_command(&self, id: u64) {
        let Some(idle) = self.idle else {
            return;
        };
        let slots = self.slots.lock().expect("connections");
        if let Some(slot) = slots.get(&id) {
            *slot.idle_deadline.lock().expect("idle") = Some(std::time::Instant::now() + idle);
        }
    }

    /// Time left until this connection should close for silence.
    /// `None` means idle close is off, or the connection is already gone.
    fn idle_remaining(&self, id: u64) -> Option<std::time::Duration> {
        self.idle?;
        let slots = self.slots.lock().expect("connections");
        let slot = slots.get(&id)?;
        let deadline = (*slot.idle_deadline.lock().expect("idle"))?;
        Some(deadline.saturating_duration_since(std::time::Instant::now()))
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn len(&self) -> usize {
        self.slots.lock().expect("connections").len()
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn only_peer(&self) -> Option<std::net::SocketAddr> {
        let slots = self.slots.lock().expect("connections");
        if slots.len() != 1 {
            return None;
        }
        slots.values().next().map(|slot| slot.peer)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn only_id(&self) -> Option<u64> {
        let slots = self.slots.lock().expect("connections");
        if slots.len() != 1 {
            return None;
        }
        slots.keys().next().copied()
    }

    fn set_user(&self, id: u64, user: String) {
        if let Some(slot) = self.slots.lock().expect("connections").get(&id) {
            *slot.user.lock().expect("connection user") = Some(user);
        }
    }

    fn close_user(&self, user: &str) {
        for slot in self.slots.lock().expect("connections").values() {
            let current = slot.user.lock().expect("connection user");
            if current.as_deref() == Some(user) {
                let _ = slot.close.send(true);
            }
        }
    }

    fn close(&self, id: u64) {
        let removed = self.slots.lock().expect("connections").remove(&id);
        if let Some(slot) = removed {
            LOGGER.debug(format!(
                "Connection {} from {} closed after {} ms",
                id,
                slot.peer,
                slot.started.elapsed().as_millis()
            ));
            crate::metrics::METRICS.connection_closed();
        }
    }

    /// Closes every live connection and stops the accept loop.
    fn shutdown(&self) {
        let _ = self.stop.send(true);
        self.close_all();
    }

    fn close_all(&self) {
        for slot in self.slots.lock().expect("connections").values() {
            let _ = slot.close.send(true);
        }
    }
}

async fn wait_idle(table: &ConnTable, id: u64) {
    match table.idle_remaining(id) {
        Some(duration) => tokio::time::sleep(duration).await,
        None => std::future::pending::<()>().await,
    }
}

async fn refuse_connection(socket: &mut TcpStream) -> std::io::Result<()> {
    let body = bson::doc! {
        "ok": 0.0,
        "errmsg": "too many connections",
    };
    let frame = protocol::encode_reply(0, 0, &body)
        .map_err(|err| std::io::Error::new(std::io::ErrorKind::InvalidData, err))?;
    socket.write_all(&frame).await?;
    socket.shutdown().await?;
    Ok(())
}

async fn run_listener(
    listener: TcpListener,
    router: PartitionRouter,
    replica: Option<Arc<crate::replication::Replica>>,
    shards: Option<Arc<crate::shard::ShardRouter>>,
    table: Arc<ConnTable>,
) -> std::io::Result<()> {
    let mut stop = table.subscribe_stop();
    loop {
        if *stop.borrow() {
            table.close_all();
            return Ok(());
        }
        tokio::select! {
            changed = stop.changed() => {
                if changed.is_err() || *stop.borrow() {
                    table.close_all();
                    return Ok(());
                }
            }
            accepted = listener.accept() => {
                let (mut socket, addr) = accepted?;
                LOGGER.info(format!("Connection received from {}", addr));
                let client_router = router.clone();
                let client_replica = replica.clone();
                let client_shards = shards.clone();
                let table = Arc::clone(&table);
                spawn(async move {
                    if let Err(err) = serve_client(
                        &mut socket,
                        &client_router,
                        client_replica.as_deref(),
                        client_shards.as_deref(),
                        &table,
                    )
                    .await
                    {
                        match err.kind() {
                            std::io::ErrorKind::UnexpectedEof => {
                                LOGGER.info(format!("Client {} disconnected", addr));
                            }
                            _ => LOGGER.error(format!("Error processing client {}: {}", addr, err)),
                        }
                    }
                });
            }
        }
    }
}

struct Session {
    actions: Vec<String>,
}

/// Starts the Kova TCP network server.
///
/// Connections are accepted asynchronously; each client runs on its own
/// Tokio task. Commands are forwarded to `router`, whose partition
/// workers execute them on dedicated OS threads.
///
/// ## Port
///
/// The default port is `27018`, so a local mongod can keep `27017`.
/// It can be changed in `kova.toml` or overridden with the `KOVA_PORT`
/// environment variable (environment wins over file).
/// Accepts clients on a listener the caller already bound.
///
/// `kovadump` tests use this so they can talk to a server without
/// taking the port in `kova.toml`.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn serve_bound(
    listener: TcpListener,
    router: PartitionRouter,
) -> std::io::Result<()> {
    let table = ConnTable::limited(128, 0);
    run_listener(listener, router, None, None, table).await
}

pub async fn start_server(
    router: PartitionRouter,
    replica: Option<Arc<crate::replication::Replica>>,
    shards: Option<Arc<crate::shard::ShardRouter>>,
) -> std::io::Result<()> {
    let port = resolve_port();
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    let table = connections();
    LOGGER.info(format!(
        "Network server started on port {} (max_connections {}, idle_ms {})",
        port,
        table.max_connections,
        table.idle.map(|idle| idle.as_millis()).unwrap_or(0),
    ));
    let shutdown = Arc::clone(&table);
    spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            LOGGER.info("Shutdown requested; closing connections".to_string());
            shutdown.shutdown();
        }
    });
    run_listener(listener, router, replica, shards, table).await
}

/// Reads OP_MSG frames from one client until it disconnects.
///
/// Each iteration reads the 16-byte header, then the remaining
/// `messageLength - 16` bytes, decodes the OP_MSG body, executes the
/// command through the router and writes the reply.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) async fn handle_client(
    socket: &mut TcpStream,
    router: &PartitionRouter,
    replica: Option<&crate::replication::Replica>,
) -> std::io::Result<()> {
    serve_client(socket, router, replica, None, connections().as_ref()).await
}

async fn serve_client(
    socket: &mut TcpStream,
    router: &PartitionRouter,
    replica: Option<&crate::replication::Replica>,
    shards: Option<&crate::shard::ShardRouter>,
    table: &ConnTable,
) -> std::io::Result<()> {
    let peer = socket
        .peer_addr()
        .unwrap_or_else(|_| std::net::SocketAddr::from(([0, 0, 0, 0], 0)));
    let Some((conn_id, mut close)) = table.try_open(peer) else {
        LOGGER.warn(format!("Refusing {}: too many connections", peer));
        return refuse_connection(socket).await;
    };
    let mut session = None;
    let outcome = handle_frames(
        socket,
        router,
        replica,
        shards,
        table,
        conn_id,
        &mut close,
        &mut session,
    )
    .await;
    table.close(conn_id);
    outcome
}

async fn handle_frames(
    socket: &mut TcpStream,
    router: &PartitionRouter,
    replica: Option<&crate::replication::Replica>,
    shards: Option<&crate::shard::ShardRouter>,
    table: &ConnTable,
    conn_id: u64,
    close: &mut watch::Receiver<bool>,
    session: &mut Option<Session>,
) -> std::io::Result<()> {
    loop {
        if *close.borrow() {
            return Ok(());
        }
        // ---- header ------------------------------------------------
        // `with_capacity` only reserves memory; the length must be set
        // explicitly so `read_exact` actually fills the 16 bytes.
        let mut header_buf = BytesMut::with_capacity(HEADER_SIZE);
        header_buf.resize(HEADER_SIZE, 0);

        let read = tokio::select! {
            _ = close.changed() => None,
            _ = wait_idle(table, conn_id) => None,
            result = socket.read_exact(&mut header_buf) => Some(result),
        };
        let Some(result) = read else {
            return Ok(());
        };
        match result {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "socket unexpectedly closed by client",
                ));
            }
            Err(err) => return Err(err),
        }

        let header = wire::MessageHeader::parse(
            header_buf
                .as_ref()
                .try_into()
                .expect("exactly 16 bytes were read"),
        );
        crate::metrics::METRICS.bytes_in(HEADER_SIZE as u64);

        // ---- sanity check -------------------------------------------
        // `messageLength` must cover at least the header itself and can
        // never exceed the 48 MB limit announced in the `hello` reply.
        // Rejecting a bogus length here keeps the loop aligned instead
        // of allocating a multi-gigabyte buffer for a garbage value.
        if header.message_length < HEADER_SIZE as u32
            || header.message_length > protocol::MAX_MESSAGE_SIZE as u32
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "invalid messageLength {} (raw header {:02x?})",
                    header.message_length,
                    &header_buf[..]
                ),
            ));
        }

        table.note_command(conn_id);

        // ---- body --------------------------------------------------
        let body_len = header.message_length.saturating_sub(HEADER_SIZE as u32) as usize;

        if body_len > 0 {
            let mut body_buf = BytesMut::with_capacity(body_len);
            body_buf.resize(body_len, 0);
            socket.read_exact(&mut body_buf).await?;
            crate::metrics::METRICS.bytes_in(body_len as u64);

            if header.op_code == wire::OP_MSG {
                LOGGER.debug(format!(
                    "OP_MSG received (requestId={}, {} bytes)",
                    header.request_id, header.message_length
                ));

                if let Err(err) = process_op_msg(
                    &header, &body_buf, router, replica, shards, table, socket, conn_id, session,
                )
                .await
                {
                    // A bounded hex prefix turns "section kind 189"
                    // into an actionable diagnosis of what the client
                    // actually put on the wire.
                    let preview: Vec<String> = body_buf
                        .iter()
                        .take(32)
                        .map(|byte| format!("{:02x}", byte))
                        .collect();
                    LOGGER.warn(format!(
                        "Failed to process OP_MSG ({} bytes, prefix {}): {}",
                        body_buf.len(),
                        preview.join(" "),
                        err
                    ));
                }
            } else if header.op_code == wire::OP_QUERY {
                // Pre-4.2 handshake path: drivers open with a legacy
                // `isMaster` OP_QUERY and expect an OP_REPLY back.
                LOGGER.debug(format!(
                    "OP_QUERY received (requestId={}, {} bytes)",
                    header.request_id, header.message_length
                ));

                if let Err(err) = process_op_query(
                    &header, &body_buf, router, replica, shards, table, socket, conn_id, session,
                )
                .await
                {
                    LOGGER.warn(format!("Failed to process OP_QUERY: {}", err));
                }
            } else {
                LOGGER.warn(format!(
                    "Unsupported opCode {} (OP_MSG and OP_QUERY are handled)",
                    header.op_code
                ));
            }
        }
    }
}

/// Decodes, executes and replies to a single OP_MSG.
async fn process_op_msg(
    header: &wire::MessageHeader,
    body: &[u8],
    router: &PartitionRouter,
    replica: Option<&crate::replication::Replica>,
    shards: Option<&crate::shard::ShardRouter>,
    table: &ConnTable,
    socket: &mut TcpStream,
    conn_id: u64,
    session: &mut Option<Session>,
) -> Result<(), String> {
    let msg = protocol::decode_op_msg(body)?;
    let local_addr = socket
        .local_addr()
        .map_err(|err| format!("failed to read local address: {}", err))?;
    let reply_body = execute_replicated(
        &msg.command,
        router,
        local_addr,
        replica,
        shards,
        table,
        conn_id,
        session,
    )
    .await;

    let request_id = REPLY_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let frame = protocol::encode_reply(request_id, header.request_id, &reply_body)?;
    crate::metrics::METRICS.bytes_out(frame.len() as u64);

    socket
        .write_all(&frame)
        .await
        .map_err(|err| format!("failed to write reply: {}", err))?;

    LOGGER.info(format!(
        "Replied to requestId {} (reply requestId {})",
        header.request_id, request_id
    ));

    Ok(())
}

/// Decodes, executes and replies to a single legacy OP_QUERY.
///
/// Drivers open the connection with an `isMaster`/`hello` OP_QUERY and
/// expect an OP_REPLY frame back, so this path mirrors
/// [`process_op_msg`] but speaks the legacy reply opcode.
async fn process_op_query(
    header: &wire::MessageHeader,
    body: &[u8],
    router: &PartitionRouter,
    replica: Option<&crate::replication::Replica>,
    shards: Option<&crate::shard::ShardRouter>,
    table: &ConnTable,
    socket: &mut TcpStream,
    conn_id: u64,
    session: &mut Option<Session>,
) -> Result<(), String> {
    let query = protocol::decode_op_query(body)?;
    let local_addr = socket
        .local_addr()
        .map_err(|err| format!("failed to read local address: {}", err))?;
    let reply_body = execute_replicated(
        &query.command,
        router,
        local_addr,
        replica,
        shards,
        table,
        conn_id,
        session,
    )
    .await;

    let request_id = REPLY_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
    let frame = protocol::encode_op_reply(request_id, header.request_id, &reply_body)?;
    crate::metrics::METRICS.bytes_out(frame.len() as u64);

    socket
        .write_all(&frame)
        .await
        .map_err(|err| format!("failed to write reply: {}", err))?;

    LOGGER.info(format!(
        "Replied to OP_QUERY requestId {} (reply requestId {})",
        header.request_id, request_id
    ));

    Ok(())
}

/// Applies a committed oplog command on this process. Raft calls this
/// from the state machine; it must not propose the command again.
pub fn storage_applier(router: PartitionRouter) -> Arc<dyn crate::replication::CommandApplier> {
    Arc::new(StorageApplier { router })
}

struct StorageApplier {
    router: PartitionRouter,
}

#[async_trait::async_trait]
impl crate::replication::CommandApplier for StorageApplier {
    async fn apply(&self, body: &[u8]) -> Vec<u8> {
        let command = match bson::Document::from_reader(&mut std::io::Cursor::new(body)) {
            Ok(command) => command,
            Err(err) => {
                return bson::to_vec(&bson::doc! { "ok": 0.0, "errmsg": err.to_string() })
                    .unwrap_or_default();
            }
        };
        let local = "0.0.0.0:0".parse().expect("address");
        let reply = execute_command(&command, &self.router, local).await;
        bson::to_vec(&reply).unwrap_or_default()
    }

    async fn snapshot(&self) -> Result<Vec<u8>, String> {
        self.router.snapshot_pages().await
    }

    async fn install(&self, bytes: &[u8]) -> Result<(), String> {
        self.router.install_pages(bytes).await
    }
}

/// Auto-commit mutations travel through Raft. Reads stay on this node.
/// `begin`, staged statements, and `abort` stay on the primary. A commit
/// is proposed separately as one `applyTransaction` entry.
fn is_replicated_write(command: &bson::Document) -> bool {
    if command.contains_key("applyTransaction") {
        return true;
    }
    if command.contains_key("txnId") {
        return false;
    }
    const KEYS: &[&str] = &[
        "insert",
        "update",
        "delete",
        "findAndModify",
        "findandmodify",
        "drop",
        "dropDatabase",
        "create",
        "createIndexes",
        "dropIndexes",
        "deleteIndexes",
        "collMod",
        "createUser",
        "updateUser",
        "grantRolesToUser",
        "revokeRolesFromUser",
        "createRole",
        "dropRole",
    ];
    if KEYS.iter().any(|key| command.contains_key(*key)) {
        return true;
    }
    if !command.contains_key("aggregate") {
        return false;
    }
    command.get_array("pipeline").ok().is_some_and(|stages| {
        stages.iter().any(|stage| {
            stage
                .as_document()
                .is_some_and(|doc| doc.contains_key("$out"))
        })
    })
}

fn is_leader_local(command: &bson::Document) -> bool {
    if command.contains_key("applyTransaction") {
        return false;
    }
    command.contains_key("txnId")
        || command.contains_key("beginTransaction")
        || command.contains_key("commit")
        || command.contains_key("commitTransaction")
        || command.contains_key("abort")
        || command.contains_key("abortTransaction")
}

async fn execute_replicated(
    command: &bson::Document,
    router: &PartitionRouter,
    local_addr: std::net::SocketAddr,
    replica: Option<&crate::replication::Replica>,
    shards: Option<&crate::shard::ShardRouter>,
    table: &ConnTable,
    conn_id: u64,
    session: &mut Option<Session>,
) -> bson::Document {
    let mut command = command.clone();
    if let Err(message) = prepare_account(&mut command).await {
        return bson::doc! { "ok": 0.0, "errmsg": message };
    }
    if let Err(message) = authorize(&mut command, session) {
        return bson::doc! { "ok": 0.0, "errmsg": message };
    }
    let mut reply = dispatch_command(&command, router, local_addr, replica, shards).await;
    bind_session(&command, &mut reply, table, conn_id, session);
    reply
}

/// Replaces a createUser password with its Argon2id hash before the
/// command is logged or applied.
async fn prepare_account(command: &mut bson::Document) -> Result<(), String> {
    command.remove("$trusted");
    // `authenticate` checks the password on this connection. It is not
    // replicated, so the plaintext stays on the command until the worker
    // reads it. Every other command drops `pwd` so a password is not logged.
    if command.contains_key("authenticate") {
        return Ok(());
    }
    if !command.contains_key("createUser") || command.contains_key("hash") {
        command.remove("pwd");
        return Ok(());
    }
    let pwd = command
        .get_str("pwd")
        .map_err(|_| "createUser requires a 'pwd' string".to_string())?
        .to_string();
    let hash = tokio::task::spawn_blocking(move || crate::security::hash_password(&pwd))
        .await
        .map_err(|err| err.to_string())??;
    command.remove("pwd");
    command.insert("hash", hash);
    Ok(())
}

fn authorize(command: &mut bson::Document, session: &Option<Session>) -> Result<(), String> {
    let enabled = crate::config::CONFIG.security.enabled;
    if !enabled {
        if command.contains_key("createUser") {
            command.insert("$trusted", true);
        }
        return Ok(());
    }
    let Some(action) = crate::security::required_action(command) else {
        return Ok(());
    };
    let Some(session) = session else {
        if action == crate::security::Action::CreateUser && command.contains_key("createUser") {
            return Ok(());
        }
        return Err("authentication required".into());
    };
    if !session.actions.iter().any(|held| held == action.name()) {
        return Err(format!("unauthorized: {}", action.name()));
    }
    if command.contains_key("createUser") {
        command.insert("$trusted", true);
    }
    Ok(())
}

fn bind_session(
    command: &bson::Document,
    reply: &mut bson::Document,
    table: &ConnTable,
    conn_id: u64,
    session: &mut Option<Session>,
) {
    if command.contains_key("authenticate") && reply.get_f64("ok") == Ok(1.0) {
        if let (Ok(user), Ok(actions)) = (reply.get_str("user"), reply.get_array("actions")) {
            let user = user.to_string();
            let actions = actions
                .iter()
                .filter_map(|value| value.as_str().map(str::to_string))
                .collect();
            table.set_user(conn_id, user);
            *session = Some(Session { actions });
        }
    }
    if let Ok(user) = reply.get_str("kovaDisabledUser").map(str::to_string) {
        reply.remove("kovaDisabledUser");
        table.close_user(&user);
    }
}

async fn dispatch_command(
    command: &bson::Document,
    router: &PartitionRouter,
    local_addr: std::net::SocketAddr,
    replica: Option<&crate::replication::Replica>,
    shards: Option<&crate::shard::ShardRouter>,
) -> bson::Document {
    if let Some(shards) = shards {
        if command.contains_key("sql") {
            return crate::shard::SQL_ROUTER
                .scope(
                    std::sync::Arc::new(shards.clone()),
                    execute_command(command, router, local_addr),
                )
                .await;
        }
        if stays_on_router(command) {
            return execute_command(command, router, local_addr).await;
        }
        return shards.route(command).await;
    }
    let Some(replica) = replica else {
        return execute_command(command, router, local_addr).await;
    };
    if command.contains_key("hello")
        || command.contains_key("isMaster")
        || command.contains_key("ismaster")
    {
        if let Some(reply) = protocol::special_command(command) {
            return replica.overlay_hello(reply);
        }
    }
    if command.contains_key("replSetGetStatus") {
        return replica.status();
    }
    if command.get_str("find").ok() == Some("oplog.rs")
        && command.get_str("$db").ok() == Some("local")
    {
        return replica.oplog_cursor(command);
    }
    if (is_replicated_write(command) || is_leader_local(command)) && !replica.is_leader() {
        return crate::replication::not_primary();
    }
    if command.contains_key("commit") || command.contains_key("commitTransaction") {
        return propose_commit(command, router, replica).await.0;
    }
    if is_replicated_write(command) {
        return replica.propose(command.clone()).await.0;
    }
    execute_command(command, router, local_addr).await
}

/// Proposes one oplog entry for a commit. The entry carries the staged
/// writes. Followers apply it as one WAL record and do not open a
/// transaction. An empty commit stays on this node.
pub async fn propose_commit(
    command: &bson::Document,
    router: &PartitionRouter,
    replica: &crate::replication::Replica,
) -> (bson::Document, u64) {
    let (collection, txn_id) = match protocol::command_from_document(command) {
        Ok(PartitionCommand::CommitTransaction { collection, txn_id }) => (collection, txn_id),
        Ok(_) => {
            return (
                bson::doc! { "ok": 0.0, "errmsg": "commit requires a collection and txnId" },
                0,
            )
        }
        Err(message) => return (bson::doc! { "ok": 0.0, "errmsg": message }, 0),
    };
    let preview = match router.dispatch(PartitionCommand::CommitPreview {
        collection: collection.clone(),
        txn_id,
    }) {
        Some(receiver) => receiver.await,
        None => {
            return (
                bson::doc! { "ok": 0.0, "errmsg": "storage partition unavailable" },
                0,
            )
        }
    };
    let ops = match preview {
        Ok(PartitionResult::CommitPreview { ops, .. }) => ops,
        Ok(PartitionResult::Error { message }) => {
            return (bson::doc! { "ok": 0.0, "errmsg": message }, 0)
        }
        Ok(other) => {
            return (
                bson::doc! { "ok": 0.0, "errmsg": format!("unexpected commit preview {other:?}") },
                0,
            )
        }
        Err(_) => {
            return (
                bson::doc! { "ok": 0.0, "errmsg": "partition worker dropped the request" },
                0,
            )
        }
    };
    if ops.is_empty() {
        let committed =
            match router.dispatch(PartitionCommand::CommitTransaction { collection, txn_id }) {
                Some(receiver) => receiver.await,
                None => {
                    return (
                        bson::doc! { "ok": 0.0, "errmsg": "storage partition unavailable" },
                        0,
                    )
                }
            };
        let result = committed.unwrap_or(PartitionResult::Error {
            message: "partition worker dropped the request".into(),
        });
        return (protocol::reply_document(&result), 0);
    }
    let (db, name) = split_namespace(&collection);
    let mut entry = bson::doc! {
        "applyTransaction": name,
        "commitTxn": txn_id as i64,
        "ops": ops,
    };
    if !db.is_empty() {
        entry.insert("$db", db);
    }
    replica.propose(entry).await
}

/// Handshake and authentication stay on the process the client opened.
/// A data command is forwarded.
fn stays_on_router(command: &bson::Document) -> bool {
    protocol::special_command(command).is_some()
        || command.contains_key("whatsmyuri")
        || command.contains_key("authenticate")
}

fn split_namespace(namespace: &str) -> (String, String) {
    match namespace.split_once('.') {
        Some((db, collection)) => (db.to_string(), collection.to_string()),
        None => (String::new(), namespace.to_string()),
    }
}

/// Runs one decoded command and returns the reply document.
///
/// Handshake commands (`ping`, `hello`, `isMaster`, `whatsmyuri`) are
/// answered here without touching storage; everything else is routed
/// to its partition worker.
///
/// `local_addr` is the socket's own address, which `whatsmyuri`
/// reports back so a client can learn where it is connected.
async fn execute_command(
    command: &bson::Document,
    router: &PartitionRouter,
    local_addr: std::net::SocketAddr,
) -> bson::Document {
    if let Some(direct) = protocol::special_command(command) {
        LOGGER.debug(format!(
            "Handshake command answered directly: {:?}",
            command.keys().collect::<Vec<_>>()
        ));
        return direct;
    }

    // The Java driver (Studio 3T, Spring Data) asks `whatsmyuri` on
    // every new connection to learn the server address it reached.
    // A real mongod answers with the socket's local address.
    if command.contains_key("whatsmyuri") {
        return bson::doc! { "ok": 1.0, "me": local_addr.to_string() };
    }

    // ---- Phase 5: SQL via db.runCommand({ sql: "..." }) ------------
    // SQL text arrives as an OP_MSG command with the key "sql".
    // We extract the referenced collection name from the SQL's FROM clause,
    // use the command's `$db` to resolve the storage namespace, load the
    // in-memory documents for that collection, and execute the query with
    // DataFusion, returning the result set as a BSON document (or an error).
    if let Ok(sql_text) = command.get_str("sql") {
        if is_show_tables(sql_text) {
            let db = command.get_str("$db").unwrap_or("");
            let listed =
                list_collections(&bson::doc! { "listCollections": 1, "$db": db }, router).await;
            return tables_as_sql(listed);
        }
        if is_show_databases(sql_text) {
            let listed = list_databases(&bson::doc! { "listDatabases": 1 }, router).await;
            return databases_as_sql(listed);
        }
        if let Some(table_name) = catalog_table_name(sql_text) {
            if let Some(reply) =
                describe_from_catalog(router, command.get_str("$db").unwrap_or(""), &table_name)
                    .await
            {
                return reply;
            }
        }
        if let Some(kind) = information_schema_kind(sql_text) {
            return information_schema_from_catalog(
                router,
                command.get_str("$db").unwrap_or(""),
                kind,
                sql_text,
            )
            .await;
        }
        if let Some(table_name) = show_index_table(sql_text) {
            let db = command.get_str("$db").unwrap_or("");
            let collection = if db.is_empty() {
                table_name.clone()
            } else {
                format!("{}.{}", db, table_name)
            };
            let listed = list_indexes(router, &collection).await;
            return indexes_as_sql(&table_name, listed);
        }
        if let Some(parsed) = sql_text_search(sql_text) {
            let db = command.get_str("$db").unwrap_or("");
            return match parsed {
                Ok(search) => sql_text_query(router, db, search).await,
                Err(message) => bson::doc! { "ok": 0.0, "errmsg": message },
            };
        }
        // A query with no FROM (`SELECT 1 AS n`) has no collection to load.
        // FROM and JOIN each name a collection; each snapshot is registered
        // under the spelling used in the SQL text. A single comparison in
        // WHERE is sent as a find only when the statement reads one
        // collection, so a secondary index can narrow those pages before
        // DataFusion applies the same predicate again.
        let names = sql_table_names(sql_text);
        let pushed = if names.len() == 1 {
            sql_simple_filter(sql_text)
        } else {
            None
        };
        let mut prepared = Vec::with_capacity(names.len());
        for table_name in names {
            let db = command.get_str("$db").unwrap_or("");
            let collection = if db.is_empty() {
                table_name.clone()
            } else {
                format!("{}.{}", db, table_name)
            };
            let filter = pushed.clone().unwrap_or_default();
            match load_collection_documents(router, &collection, &filter).await {
                Ok(documents) => {
                    // An empty index answer has no columns. DataFusion
                    // would then reject the field name, so the full
                    // collection is loaded and the SQL predicate still
                    // runs. The planner stage stays the one the index chose.
                    let documents = if documents.is_empty() && pushed.is_some() {
                        match load_collection_documents(router, &collection, &bson::doc! {}).await {
                            Ok(documents) => documents,
                            Err(message) => {
                                return bson::doc! { "ok": 0.0, "errmsg": message };
                            }
                        }
                    } else {
                        documents
                    };
                    prepared.push((table_name, collection, documents));
                }
                Err(message) => {
                    return bson::doc! { "ok": 0.0, "errmsg": message };
                }
            }
        }
        let tables: Vec<(&str, std::sync::Arc<Vec<bson::Document>>)> = prepared
            .iter()
            .map(|(name, _, documents)| (name.as_str(), std::sync::Arc::clone(documents)))
            .collect();
        let mut result =
            match query_engine::execute_sql_query(&sql_for_engine(sql_text), &tables).await {
                Ok(result) => result,
                Err(err) => return bson::doc! { "ok": 0.0, "errmsg": err.to_string() },
            };
        if prepared.is_empty() {
            crate::metrics::METRICS.query();
        }
        if let Some(filter) = pushed {
            if let [(_, collection, _)] = prepared.as_slice() {
                if let Some((stage, index_name)) = index_plan(router, collection, filter).await {
                    let mut plan = bson::doc! { "stage": stage };
                    if let Some(name) = index_name {
                        plan.insert("indexName", name);
                    }
                    result.insert(
                        "queryPlanner",
                        bson::doc! {
                            "plannerVersion": 1i32,
                            "namespace": collection.clone(),
                            "winningPlan": plan,
                        },
                    );
                }
            }
        }
        return result;
    }

    if command.contains_key("mapReduce") {
        return map_reduce(command, router).await;
    }

    if let Some(explained) = protocol::explain_reply(command) {
        let mut reply = match explained {
            Ok(reply) => reply,
            Err(message) => return bson::doc! { "ok": 0.0, "errmsg": message },
        };
        if let Some((collection, filter)) = protocol::explain_request(command) {
            if let Some((stage, index_name)) = index_plan(router, &collection, filter).await {
                patch_winning_plan(&mut reply, &stage, index_name);
            }
        }
        return reply;
    }

    match protocol::command_from_document(command) {
        // Collection names hash across partitions, so these commands
        // go to every worker and the replies are merged here.
        Ok(PartitionCommand::ListCollections { .. }) => list_collections(command, router).await,
        Ok(PartitionCommand::ListDatabases { .. }) => list_databases(command, router).await,
        Ok(PartitionCommand::DropDatabase { .. }) => drop_database(command, router).await,
        Ok(PartitionCommand::DatabaseStats { .. }) => database_stats(command, router).await,
        Ok(aggregate)
            if aggregate_command(&aggregate)
                && aggregate_namespace(&aggregate)
                    .is_some_and(|name| crate::storage::is_catalog(&name)) =>
        {
            execute_catalog_aggregate(router, aggregate).await
        }
        Ok(aggregate) if aggregate_command(&aggregate) => {
            execute_aggregate(router, aggregate).await
        }
        Ok(partition_command)
            if crate::storage::is_catalog(partition_command.collection())
                && matches!(
                    partition_command,
                    PartitionCommand::Find { .. } | PartitionCommand::Count { .. }
                ) =>
        {
            match broadcast_catalog(router, partition_command).await {
                Ok(result) => cursor_ns(command, protocol::reply_document(&result)),
                Err(message) => bson::doc! { "ok": 0.0, "errmsg": message },
            }
        }
        Ok(partition_command)
            if matches!(
                &partition_command,
                PartitionCommand::Update { collection, .. }
                    if collection == crate::storage::COUNTERS
            ) =>
        {
            match broadcast_counter_update(router, partition_command).await {
                Ok(result) => protocol::reply_document(&result),
                Err(message) => bson::doc! { "ok": 0.0, "errmsg": message },
            }
        }
        Ok(partition_command) => match router.dispatch(partition_command) {
            Some(rx) => match rx.await {
                Ok(result) => cursor_ns(command, protocol::reply_document(&result)),
                Err(_) => protocol::reply_document(&PartitionResult::Error {
                    message: "partition worker dropped the request".into(),
                }),
            },
            None => protocol::reply_document(&PartitionResult::Error {
                message: "storage partition unavailable".into(),
            }),
        },
        Err(message) => {
            LOGGER.warn(format!(
                "Command rejected: {} (raw: {:?})",
                message, command
            ));
            bson::doc! { "ok": 0.0, "errmsg": message }
        }
    }
}

/// The first collection a statement reads.
///
/// `FROM`, `JOIN`, `DESCRIBE`, and `SHOW COLUMNS` all contribute. See
/// [`sql_table_names`] when the statement names more than one.
#[cfg(test)]
fn sql_table_name(sql: &str) -> Option<String> {
    sql_table_names(sql).into_iter().next()
}

/// Every stored collection a statement reads, in SQL spelling.
///
/// `FROM` and `JOIN` each name a collection, including comma lists
/// (`FROM emp, dept`) and joins written inside a subquery. `DESCRIBE`
/// and `SHOW COLUMNS` name one. `information_schema` is DataFusion's
/// catalog and is not stored. `EXTRACT(x FROM y)` is not a table
/// reference. The keyword match is case-insensitive; the identifier
/// keeps its original spelling.
fn sql_table_names(sql: &str) -> Vec<String> {
    if let Some(name) = catalog_table_name(sql) {
        return vec![name];
    }
    relation_names(sql)
}

fn is_information_schema(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name == "information_schema" || name.starts_with("information_schema.")
}

fn catalog_table_name(sql: &str) -> Option<String> {
    let text = sql.trim().trim_end_matches(';').trim();
    let mut words = text.split_whitespace();
    let first = words.next()?;
    if first.eq_ignore_ascii_case("describe") || first.eq_ignore_ascii_case("desc") {
        return bare_table(words.next()?);
    }
    if first.eq_ignore_ascii_case("show") {
        let second = words.next()?;
        if second.eq_ignore_ascii_case("columns") || second.eq_ignore_ascii_case("fields") {
            let keyword = words.next()?;
            if keyword.eq_ignore_ascii_case("from") || keyword.eq_ignore_ascii_case("in") {
                return bare_table(words.next()?);
            }
        }
    }
    None
}

fn bare_table(token: &str) -> Option<String> {
    let name = token
        .trim_end_matches(';')
        .trim_matches(|c| c == '"' || c == '`' || c == '\'');
    if name.is_empty() || is_information_schema(name) {
        return None;
    }
    let bare = name.rsplit('.').next().unwrap_or(name);
    let bare = bare.trim_matches(|c| c == '"' || c == '`' || c == '\'');
    if bare.is_empty() {
        None
    } else {
        Some(bare.to_string())
    }
}

/// Table names that follow `FROM` or `JOIN` outside a function call.
fn relation_names(sql: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut index = 0;
    // Depth 0 is the statement. A parenthesis starts a new frame that
    // becomes a table source only after its own SELECT, so
    // `EXTRACT(YEAR FROM hiredate)` does not load `hiredate`.
    let mut select_at_depth = vec![true];
    while index < sql.len() {
        let ch = sql[index..]
            .chars()
            .next()
            .expect("index on a char boundary");
        if ch == '\'' || ch == '"' || ch == '`' {
            index = skip_quoted(sql, index);
            continue;
        }
        if ch == '(' {
            select_at_depth.push(false);
            index += 1;
            continue;
        }
        if ch == ')' {
            if select_at_depth.len() > 1 {
                select_at_depth.pop();
            }
            index += 1;
            continue;
        }
        if let Some(word) = word_at(sql, index) {
            let word_len = word.len();
            if word.eq_ignore_ascii_case("select") {
                if let Some(flag) = select_at_depth.last_mut() {
                    *flag = true;
                }
            }
            let source = (word.eq_ignore_ascii_case("from") || word.eq_ignore_ascii_case("join"))
                && select_at_depth.last().copied().unwrap_or(false);
            if source {
                index = take_relation_list(sql, index + word_len, &mut names);
                continue;
            }
            index += word_len;
            continue;
        }
        index += ch.len_utf8();
    }
    names
}

fn take_relation_list(sql: &str, mut index: usize, names: &mut Vec<String>) -> usize {
    loop {
        index = skip_ws(sql, index);
        if index >= sql.len() || sql.as_bytes()[index] == b'(' {
            return index;
        }
        let Some((name, next)) = sql_relation_token(sql, index) else {
            return index;
        };
        if let Some(bare) = bare_table(&name) {
            if !names.iter().any(|existing| existing == &bare) {
                names.push(bare);
            }
        }
        index = skip_relation_tail(sql, next);
        index = skip_ws(sql, index);
        if index < sql.len() && sql.as_bytes()[index] == b',' {
            index += 1;
            continue;
        }
        return index;
    }
}

fn skip_relation_tail(sql: &str, index: usize) -> usize {
    let index = skip_ws(sql, index);
    let Some((word, next)) = sql_relation_token(sql, index) else {
        return index;
    };
    if is_sql_clause_word(&word) {
        return index;
    }
    if word.eq_ignore_ascii_case("as") {
        let after = skip_ws(sql, next);
        return sql_relation_token(sql, after)
            .map(|(_, alias_end)| alias_end)
            .unwrap_or(after);
    }
    next
}

fn is_sql_clause_word(word: &str) -> bool {
    matches!(
        word.to_ascii_lowercase().as_str(),
        "where"
            | "group"
            | "order"
            | "limit"
            | "having"
            | "union"
            | "except"
            | "intersect"
            | "window"
            | "offset"
            | "fetch"
            | "join"
            | "inner"
            | "left"
            | "right"
            | "full"
            | "cross"
            | "natural"
            | "outer"
            | "on"
            | "using"
            | "from"
            | "select"
            | "with"
            | "qualify"
    )
}

fn sql_relation_token(sql: &str, index: usize) -> Option<(String, usize)> {
    let rest = sql.get(index..)?;
    let mut chars = rest.chars();
    let first = chars.next()?;
    if first == '"' || first == '`' || first == '\'' {
        let mut name = String::new();
        let mut consumed = first.len_utf8();
        for ch in chars {
            consumed += ch.len_utf8();
            if ch == first {
                break;
            }
            name.push(ch);
        }
        if name.is_empty() {
            return None;
        }
        return Some((name, index + consumed));
    }
    if !(first.is_ascii_alphanumeric() || first == '_') {
        return None;
    }
    let mut end = index + first.len_utf8();
    while let Some(ch) = sql[end..].chars().next() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.' {
            end += ch.len_utf8();
        } else {
            break;
        }
    }
    Some((sql[index..end].to_string(), end))
}

fn word_at(sql: &str, index: usize) -> Option<&str> {
    let bytes = sql.as_bytes();
    if index > 0 {
        let prev = bytes[index - 1];
        if prev.is_ascii_alphanumeric() || prev == b'_' {
            return None;
        }
    }
    let rest = sql.get(index..)?;
    let len = rest
        .bytes()
        .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
        .count();
    if len == 0 {
        None
    } else {
        Some(&rest[..len])
    }
}

fn skip_ws(sql: &str, index: usize) -> usize {
    sql.get(index..)
        .map(|rest| index + rest.len() - rest.trim_start().len())
        .unwrap_or(index)
}

fn skip_quoted(sql: &str, index: usize) -> usize {
    let bytes = sql.as_bytes();
    if index >= bytes.len() {
        return index;
    }
    let quote = bytes[index];
    let mut index = index + 1;
    while index < bytes.len() {
        if bytes[index] == quote {
            if index + 1 < bytes.len() && bytes[index + 1] == quote {
                index += 2;
                continue;
            }
            return index + 1;
        }
        index += 1;
    }
    index
}

fn is_show_tables(sql: &str) -> bool {
    sql.trim()
        .trim_end_matches(';')
        .trim()
        .eq_ignore_ascii_case("show tables")
}

fn is_show_databases(sql: &str) -> bool {
    sql.trim()
        .trim_end_matches(';')
        .trim()
        .eq_ignore_ascii_case("show databases")
}

/// `SHOW INDEX FROM emp`, `SHOW INDEXES IN emp`, and `SHOW KEYS FROM emp`.
///
/// The collection name keeps the spelling of the last identifier, so
/// `shop.emp` is the collection `emp` in the command's database.
fn show_index_table(sql: &str) -> Option<String> {
    let text = sql.trim().trim_end_matches(';').trim();
    let mut words = text.split_whitespace();
    let first = words.next()?;
    if !first.eq_ignore_ascii_case("show") {
        return None;
    }
    let second = words.next()?;
    if !(second.eq_ignore_ascii_case("index")
        || second.eq_ignore_ascii_case("indexes")
        || second.eq_ignore_ascii_case("keys"))
    {
        return None;
    }
    let keyword = words.next()?;
    if !(keyword.eq_ignore_ascii_case("from") || keyword.eq_ignore_ascii_case("in")) {
        return None;
    }
    bare_table(words.next()?)
}

/// `SELECT ... FROM view WHERE TEXT 'clerk' [FUZZY 0.3] [FACETS ...]`.
///
/// DataFusion reads `TEXT 'clerk'` as a string, so this form is handled
/// before the planner. The search is the same `$text` path a find uses.
/// `FACETS` turns the Tantivy buckets into rows (`facet`, `bucket`,
/// `doc_count`) because the SQL cursor is the only result a command-line client prints.
struct SqlTextSearch {
    columns: Option<Vec<String>>,
    table: String,
    query: String,
    fuzzy: f64,
    facets: Option<bson::Document>,
}

fn sql_text_search(sql: &str) -> Option<Result<SqlTextSearch, String>> {
    let text = sql.trim().trim_end_matches(';').trim();
    if !has_where_text_literal(text) {
        return None;
    }
    Some(parse_text_search(text))
}

fn has_where_text_literal(sql: &str) -> bool {
    let mut scan = SqlScan::new(sql);
    while !scan.done() {
        scan.skip_ws();
        if scan.done() {
            break;
        }
        if scan.starts_quote() {
            if scan.string().is_none() {
                return false;
            }
            continue;
        }
        if scan.eat_word("where") {
            if scan.eat_word("text") {
                scan.skip_ws();
                return scan.starts_quote();
            }
            continue;
        }
        if scan.word().is_none() {
            scan.bump();
        }
    }
    false
}

fn parse_text_search(sql: &str) -> Result<SqlTextSearch, String> {
    let mut scan = SqlScan::new(sql);
    if !scan.eat_word("select") {
        return Err("TEXT search starts with SELECT".into());
    }
    let columns = parse_select_list(&mut scan)?;
    if !scan.eat_word("from") {
        return Err("TEXT search requires FROM".into());
    }
    let table = scan
        .ident()
        .and_then(|name| bare_table(&name))
        .ok_or_else(|| "TEXT search requires a table name".to_string())?;
    if scan.eat_word("as") {
        scan.ident()
            .ok_or_else(|| "TEXT search alias is missing".to_string())?;
    } else if scan
        .peek_word()
        .is_some_and(|word| !word.eq_ignore_ascii_case("where"))
    {
        scan.ident()
            .ok_or_else(|| "TEXT search alias is missing".to_string())?;
    }
    if !scan.eat_word("where") || !scan.eat_word("text") {
        return Err("TEXT search requires WHERE TEXT '...'".into());
    }
    let query = scan
        .string()
        .ok_or_else(|| "WHERE TEXT expects a quoted string".to_string())?;
    let mut fuzzy = 0.0;
    if scan.eat_word("fuzzy") {
        fuzzy = scan
            .number()
            .as_ref()
            .and_then(bson_as_f64)
            .ok_or_else(|| "FUZZY expects a number from 0 to 1".to_string())?;
        if !(0.0..=1.0).contains(&fuzzy) {
            return Err("FUZZY expects a number from 0 to 1".into());
        }
    }
    let facets = if scan.eat_word("facets") {
        Some(parse_facets(&mut scan)?)
    } else {
        None
    };
    if !scan.done() {
        return Err("TEXT search has extra tokens after the search clause".into());
    }
    Ok(SqlTextSearch {
        columns,
        table,
        query,
        fuzzy,
        facets,
    })
}

fn parse_select_list(scan: &mut SqlScan<'_>) -> Result<Option<Vec<String>>, String> {
    scan.skip_ws();
    if scan.eat_char('*') {
        return Ok(None);
    }
    let mut columns = Vec::new();
    loop {
        if scan
            .peek_word()
            .is_some_and(|word| word.eq_ignore_ascii_case("from"))
        {
            break;
        }
        let Some(name) = scan.ident().and_then(|name| bare_table(&name)) else {
            return Err("SELECT list is missing a column".into());
        };
        columns.push(name);
        if !scan.eat_char(',') {
            break;
        }
    }
    if columns.is_empty() {
        return Err("SELECT list is missing a column".into());
    }
    Ok(Some(columns))
}

fn parse_facets(scan: &mut SqlScan<'_>) -> Result<bson::Document, String> {
    let mut facets = bson::Document::new();
    loop {
        let Some(name) = scan.ident() else {
            return Err("FACETS expects a field name".into());
        };
        if scan.eat_word("terms") {
            facets.insert(name.clone(), bson::doc! { "terms": { "field": name } });
        } else if scan.eat_word("ranges") {
            let mut ranges = Vec::new();
            loop {
                ranges.push(parse_range(scan, ranges.is_empty())?);
                if scan.peek_after_comma() == Some('(') {
                    scan.eat_char(',');
                    continue;
                }
                break;
            }
            facets.insert(
                name.clone(),
                bson::doc! { "range": { "field": name, "ranges": ranges } },
            );
        } else {
            return Err(format!("FACETS {name} expects TERMS or RANGES"));
        }
        match scan.peek_after_comma() {
            Some(ch) if ch != '(' => {
                scan.eat_char(',');
            }
            _ => break,
        }
    }
    if facets.is_empty() {
        return Err("FACETS expects at least one field".into());
    }
    Ok(facets)
}

/// `(1500)` on the first range is an open start (`to`). A later `(3000)`
/// is an open end (`from`). `(1500, 3000)` sets both bounds.
fn parse_range(scan: &mut SqlScan<'_>, first: bool) -> Result<bson::Document, String> {
    if !scan.eat_char('(') {
        return Err("RANGES expects (bound) or (from, to)".into());
    }
    let start = scan
        .number()
        .ok_or_else(|| "RANGES expects a number".to_string())?;
    let range = if scan.eat_char(',') {
        let end = scan
            .number()
            .ok_or_else(|| "RANGES expects a second number".to_string())?;
        bson::doc! { "from": start, "to": end }
    } else if first {
        bson::doc! { "to": start }
    } else {
        bson::doc! { "from": start }
    };
    if !scan.eat_char(')') {
        return Err("RANGES range is missing ')'".into());
    }
    Ok(range)
}

struct SqlScan<'a> {
    sql: &'a str,
    index: usize,
}

impl<'a> SqlScan<'a> {
    fn new(sql: &'a str) -> Self {
        Self { sql, index: 0 }
    }

    fn skip_ws(&mut self) {
        self.index = skip_ws(self.sql, self.index);
    }

    fn done(&mut self) -> bool {
        self.skip_ws();
        self.index >= self.sql.len()
    }

    fn bump(&mut self) {
        if let Some(ch) = self.sql[self.index..].chars().next() {
            self.index += ch.len_utf8();
        }
    }

    fn starts_quote(&mut self) -> bool {
        self.skip_ws();
        matches!(
            self.sql.as_bytes().get(self.index),
            Some(b'\'') | Some(b'"')
        )
    }

    fn peek_word(&mut self) -> Option<&'a str> {
        self.skip_ws();
        word_at(self.sql, self.index)
    }

    fn eat_word(&mut self, expected: &str) -> bool {
        let Some(word) = self.peek_word() else {
            return false;
        };
        if word.eq_ignore_ascii_case(expected) {
            self.index += word.len();
            true
        } else {
            false
        }
    }

    fn word(&mut self) -> Option<&'a str> {
        let word = self.peek_word()?;
        self.index += word.len();
        Some(word)
    }

    fn ident(&mut self) -> Option<String> {
        self.skip_ws();
        let (name, next) = sql_relation_token(self.sql, self.index)?;
        self.index = next;
        Some(name)
    }

    fn eat_char(&mut self, expected: char) -> bool {
        self.skip_ws();
        if self.sql[self.index..].starts_with(expected) {
            self.index += expected.len_utf8();
            true
        } else {
            false
        }
    }

    fn peek_after_comma(&mut self) -> Option<char> {
        let saved = self.index;
        self.skip_ws();
        if !self.sql[self.index..].starts_with(',') {
            self.index = saved;
            return None;
        }
        let after = skip_ws(self.sql, self.index + 1);
        let ch = self.sql[after..].chars().next();
        self.index = saved;
        ch
    }

    fn string(&mut self) -> Option<String> {
        self.skip_ws();
        let bytes = self.sql.as_bytes();
        let quote = *bytes.get(self.index)?;
        if quote != b'\'' && quote != b'"' {
            return None;
        }
        let mut out = String::new();
        let mut index = self.index + 1;
        while index < bytes.len() {
            if bytes[index] == quote {
                if index + 1 < bytes.len() && bytes[index + 1] == quote {
                    out.push(quote as char);
                    index += 2;
                    continue;
                }
                self.index = index + 1;
                return Some(out);
            }
            let ch = self.sql[index..].chars().next()?;
            out.push(ch);
            index += ch.len_utf8();
        }
        None
    }

    fn number(&mut self) -> Option<bson::Bson> {
        self.skip_ws();
        let rest = self.sql.get(self.index..)?;
        let bytes = rest.as_bytes();
        let mut len = 0;
        if bytes.first() == Some(&b'-') {
            len = 1;
        }
        let mut dot = false;
        while len < bytes.len() {
            if bytes[len].is_ascii_digit() {
                len += 1;
            } else if bytes[len] == b'.' && !dot {
                dot = true;
                len += 1;
            } else {
                break;
            }
        }
        if len == 0 || bytes[..len] == [b'-'] {
            return None;
        }
        let value = sql_literal(&rest[..len])?;
        self.index += len;
        Some(value)
    }
}

fn bson_as_f64(value: &bson::Bson) -> Option<f64> {
    match value {
        bson::Bson::Int32(number) => Some(f64::from(*number)),
        bson::Bson::Int64(number) => Some(*number as f64),
        bson::Bson::Double(number) if number.is_finite() => Some(*number),
        _ => None,
    }
}

async fn sql_text_query(
    router: &PartitionRouter,
    db: &str,
    search: SqlTextSearch,
) -> bson::Document {
    let collection = if db.is_empty() {
        search.table.clone()
    } else {
        format!("{}.{}", db, search.table)
    };
    let command = PartitionCommand::Search {
        collection,
        query: search.query,
        filter: bson::doc! {},
        options: SearchOptions {
            fuzzy: search.fuzzy,
            facets: search.facets.clone().unwrap_or_default(),
            ..SearchOptions::default()
        },
    };
    let reply = match router.dispatch(command) {
        Some(rx) => match rx.await {
            Ok(result) => protocol::reply_document(&result),
            Err(_) => {
                return bson::doc! {
                    "ok": 0.0,
                    "errmsg": "partition worker dropped the request",
                };
            }
        },
        None => {
            return bson::doc! {
                "ok": 0.0,
                "errmsg": "storage partition unavailable",
            };
        }
    };
    if !reply_ok(&reply) {
        return reply;
    }
    if search.facets.is_some() {
        let facets = reply.get_document("facets").cloned().unwrap_or_default();
        return facets_as_sql(&facets);
    }
    let documents = reply
        .get_document("cursor")
        .and_then(|cursor| cursor.get_array("firstBatch"))
        .ok()
        .map(|batch| batch.clone())
        .unwrap_or_default();
    project_sql_rows(search.columns.as_deref(), &documents)
}

fn project_sql_rows(columns: Option<&[String]>, documents: &[bson::Bson]) -> bson::Document {
    let rows = documents
        .iter()
        .filter_map(|item| item.as_document())
        .map(|document| match columns {
            None => document.clone(),
            Some(columns) => {
                let mut row = bson::Document::new();
                for name in columns {
                    let value = document.get(name).cloned().unwrap_or(bson::Bson::Null);
                    row.insert(name, value);
                }
                row
            }
        })
        .collect();
    sql_cursor(rows)
}

fn facets_as_sql(facets: &bson::Document) -> bson::Document {
    let mut rows = Vec::new();
    for (name, value) in facets {
        let Some(facet) = value.as_document() else {
            continue;
        };
        let Ok(buckets) = facet.get_array("buckets") else {
            continue;
        };
        for bucket in buckets {
            let Some(bucket) = bucket.as_document() else {
                continue;
            };
            rows.push(bson::doc! {
                "facet": name.clone(),
                "bucket": bucket_label(bucket),
                "doc_count": bucket_count(bucket),
            });
        }
    }
    sql_cursor(rows)
}

fn bucket_label(bucket: &bson::Document) -> String {
    let from = bucket.get("from");
    let to = bucket.get("to");
    if from.is_some() || to.is_some() {
        let from = from.map(bson_number_text).unwrap_or_default();
        let to = to.map(bson_number_text).unwrap_or_default();
        return format!("{from}..{to}");
    }
    match bucket.get("key") {
        Some(bson::Bson::String(text)) => text.clone(),
        Some(value) => bson_number_text(value),
        None => String::new(),
    }
}

fn bucket_count(bucket: &bson::Document) -> i64 {
    match bucket.get("doc_count") {
        Some(bson::Bson::Int32(number)) => i64::from(*number),
        Some(bson::Bson::Int64(number)) => *number,
        Some(bson::Bson::Double(number)) if number.is_finite() => *number as i64,
        _ => 0,
    }
}

fn bson_number_text(value: &bson::Bson) -> String {
    match value {
        bson::Bson::Int32(number) => number.to_string(),
        bson::Bson::Int64(number) => number.to_string(),
        bson::Bson::Double(number) if number.is_finite() && number.fract() == 0.0 => {
            format!("{}", *number as i64)
        }
        bson::Bson::Double(number) => number.to_string(),
        bson::Bson::String(text) => text.clone(),
        other => format!("{other:?}"),
    }
}

fn sql_cursor(rows: Vec<bson::Document>) -> bson::Document {
    bson::doc! {
        "ok": 1.0,
        "cursor": {
            "id": 0i64,
            "ns": "sql.query",
            "firstBatch": rows,
        },
    }
}

fn reply_ok(doc: &bson::Document) -> bool {
    match doc.get("ok") {
        Some(bson::Bson::Double(value)) => *value == 1.0,
        Some(bson::Bson::Int32(value)) => *value == 1,
        Some(bson::Bson::Int64(value)) => *value == 1,
        _ => false,
    }
}

/// DataFusion accepts `DESCRIBE`, not the `DESC` shorthand.
fn sql_for_engine(sql: &str) -> String {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let Some(first) = trimmed.split_whitespace().next() else {
        return trimmed.to_string();
    };
    if first.eq_ignore_ascii_case("desc") {
        let rest = trimmed[first.len()..].trim();
        format!("DESCRIBE {rest}")
    } else {
        trimmed.to_string()
    }
}

fn databases_as_sql(listed: bson::Document) -> bson::Document {
    let ok = match listed.get("ok") {
        Some(bson::Bson::Double(value)) => *value == 1.0,
        Some(bson::Bson::Int32(value)) => *value == 1,
        Some(bson::Bson::Int64(value)) => *value == 1,
        _ => false,
    };
    if !ok {
        return listed;
    }
    let rows = listed
        .get_array("databases")
        .map(|databases| {
            databases
                .iter()
                .filter_map(|item| item.as_document())
                .filter_map(|info| info.get_str("name").ok())
                .map(|name| bson::doc! { "name": name })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    bson::doc! {
        "ok": 1.0,
        "cursor": {
            "id": 0i64,
            "ns": "sql.query",
            "firstBatch": rows,
        },
    }
}

fn indexes_as_sql(table: &str, listed: bson::Document) -> bson::Document {
    let ok = match listed.get("ok") {
        Some(bson::Bson::Double(value)) => *value == 1.0,
        Some(bson::Bson::Int32(value)) => *value == 1,
        Some(bson::Bson::Int64(value)) => *value == 1,
        _ => false,
    };
    if !ok {
        return listed;
    }
    let rows = listed
        .get_document("cursor")
        .and_then(|cursor| cursor.get_array("firstBatch"))
        .map(|batch| {
            batch
                .iter()
                .filter_map(|item| item.as_document())
                .flat_map(|spec| index_rows(table, spec))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    bson::doc! {
        "ok": 1.0,
        "cursor": {
            "id": 0i64,
            "ns": "sql.query",
            "firstBatch": rows,
        },
    }
}

/// One SQL row per indexed column. A composite key occupies several rows
/// with the same `name` and an increasing `seq`.
fn index_rows(table: &str, spec: &bson::Document) -> Vec<bson::Document> {
    let name = spec.get_str("name").unwrap_or("");
    let unique = spec_flag(spec, "unique") || name == "_id_";
    let sparse = spec_flag(spec, "sparse");
    let Some(key) = spec.get_document("key").ok().filter(|key| !key.is_empty()) else {
        return vec![index_row(table, name, 1, "", "asc", unique, sparse)];
    };
    key.iter()
        .enumerate()
        .map(|(index, (column, value))| {
            index_row(
                table,
                name,
                (index + 1) as i32,
                column,
                index_order(value),
                unique,
                sparse,
            )
        })
        .collect()
}

fn index_row(
    table: &str,
    name: &str,
    seq: i32,
    column: &str,
    order: &str,
    unique: bool,
    sparse: bool,
) -> bson::Document {
    bson::doc! {
        "table": table,
        "name": name,
        "seq": seq,
        "column": column,
        "order": order,
        "unique": unique,
        "sparse": sparse,
    }
}

fn spec_flag(spec: &bson::Document, key: &str) -> bool {
    matches!(
        spec.get(key),
        Some(bson::Bson::Boolean(true) | bson::Bson::Int32(1) | bson::Bson::Int64(1))
    )
}

fn index_order(value: &bson::Bson) -> &'static str {
    match value {
        bson::Bson::Int32(1) | bson::Bson::Int64(1) => "asc",
        bson::Bson::Int32(-1) | bson::Bson::Int64(-1) => "desc",
        bson::Bson::Double(number) if *number == 1.0 => "asc",
        bson::Bson::Double(number) if *number == -1.0 => "desc",
        bson::Bson::String(text) if text.eq_ignore_ascii_case("hashed") => "hashed",
        _ => "asc",
    }
}

fn tables_as_sql(listed: bson::Document) -> bson::Document {
    let ok = match listed.get("ok") {
        Some(bson::Bson::Double(value)) => *value == 1.0,
        Some(bson::Bson::Int32(value)) => *value == 1,
        Some(bson::Bson::Int64(value)) => *value == 1,
        _ => false,
    };
    if !ok {
        return listed;
    }
    let rows = listed
        .get_document("cursor")
        .and_then(|cursor| cursor.get_array("firstBatch"))
        .map(|batch| {
            batch
                .iter()
                .filter_map(|item| item.as_document())
                .filter_map(|info| {
                    let name = info.get_str("name").ok()?;
                    let kind = match info.get_str("type").unwrap_or("collection") {
                        "view" => "VIEW",
                        _ => "TABLE",
                    };
                    Some(bson::doc! { "name": name, "type": kind })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    bson::doc! {
        "ok": 1.0,
        "cursor": {
            "id": 0i64,
            "ns": "sql.query",
            "firstBatch": rows,
        },
    }
}

/// `DESCRIBE` and `SHOW COLUMNS` read `admin.fields` for the collection.
///
/// `None` means the catalog has no field rows yet, so the caller infers
/// the columns from the documents.
async fn describe_from_catalog(
    router: &PartitionRouter,
    db: &str,
    table: &str,
) -> Option<bson::Document> {
    let mut filter = bson::doc! { "collection": table };
    if !db.is_empty() {
        filter.insert("database", db);
    }
    let documents = catalog_rows(router, "admin.fields", filter).await.ok()?;
    if documents.is_empty() {
        return None;
    }
    let rows = documents
        .into_iter()
        .filter_map(|document| {
            let column = document.get_str("field").ok()?.to_string();
            let data_type = document.get_str("type").unwrap_or("string").to_string();
            Some(bson::doc! {
                "column_name": column,
                "data_type": data_type,
                "is_nullable": "YES",
            })
        })
        .collect();
    Some(sql_rows(rows))
}

fn information_schema_kind(sql: &str) -> Option<&'static str> {
    if !sql_table_names(sql).is_empty() {
        return None;
    }
    let lower = sql.to_ascii_lowercase();
    if lower.contains("information_schema.columns") {
        Some("columns")
    } else if lower.contains("information_schema.tables") {
        Some("tables")
    } else if lower.contains("information_schema.schemata") {
        Some("schemata")
    } else {
        None
    }
}

/// `information_schema` is answered from the `admin` catalog, not from
/// the tables registered for this one statement.
async fn information_schema_from_catalog(
    router: &PartitionRouter,
    db: &str,
    kind: &str,
    sql: &str,
) -> bson::Document {
    let rows = match kind {
        "schemata" => match catalog_rows(router, "admin.databases", bson::doc! {}).await {
            Ok(documents) => documents
                .into_iter()
                .filter_map(|document| {
                    let name = document.get_str("name").ok()?.to_string();
                    Some(bson::doc! { "schema_name": name })
                })
                .collect(),
            Err(message) => return bson::doc! { "ok": 0.0, "errmsg": message },
        },
        "tables" => {
            let mut filter = bson::doc! {};
            if !db.is_empty() {
                filter.insert("database", db);
            }
            let mut rows = match catalog_rows(router, "admin.collections", filter.clone()).await {
                Ok(documents) => documents
                    .into_iter()
                    .filter_map(|document| {
                        Some(bson::doc! {
                            "table_schema": document.get_str("database").ok()?,
                            "table_name": document.get_str("name").ok()?,
                            "table_type": "BASE TABLE",
                        })
                    })
                    .collect::<Vec<_>>(),
                Err(message) => return bson::doc! { "ok": 0.0, "errmsg": message },
            };
            match catalog_rows(router, "admin.views", bson::doc! {}).await {
                Ok(documents) => {
                    for document in documents {
                        let Some(ns) = document.get_str("_id").ok() else {
                            continue;
                        };
                        let (schema, name) = ns.split_once('.').unwrap_or(("test", ns));
                        if !db.is_empty() && schema != db {
                            continue;
                        }
                        rows.push(bson::doc! {
                            "table_schema": schema,
                            "table_name": name,
                            "table_type": "VIEW",
                        });
                    }
                }
                Err(message) => return bson::doc! { "ok": 0.0, "errmsg": message },
            }
            rows
        }
        _ => {
            let mut filter = bson::doc! {};
            if !db.is_empty() {
                filter.insert("database", db);
            }
            match catalog_rows(router, "admin.fields", filter).await {
                Ok(documents) => documents
                    .into_iter()
                    .filter_map(|document| {
                        Some(bson::doc! {
                            "table_schema": document.get_str("database").ok()?,
                            "table_name": document.get_str("collection").ok()?,
                            "column_name": document.get_str("field").ok()?,
                            "data_type": document.get_str("type").unwrap_or("string"),
                        })
                    })
                    .collect(),
                Err(message) => return bson::doc! { "ok": 0.0, "errmsg": message },
            }
        }
    };
    let (needle, alias) = match kind {
        "tables" => ("information_schema.tables", "catalog_tables"),
        "columns" => ("information_schema.columns", "catalog_columns"),
        _ => ("information_schema.schemata", "catalog_schemata"),
    };
    let sql = replace_ci(sql, needle, alias);
    match query_engine::execute_sql_query(&sql, &[(alias, std::sync::Arc::new(rows))]).await {
        Ok(result) => result,
        Err(err) => bson::doc! { "ok": 0.0, "errmsg": err.to_string() },
    }
}

fn replace_ci(sql: &str, needle: &str, replacement: &str) -> String {
    let lower = sql.to_ascii_lowercase();
    let needle = needle.to_ascii_lowercase();
    match lower.find(&needle) {
        Some(at) => {
            let mut rewritten = String::new();
            rewritten.push_str(&sql[..at]);
            rewritten.push_str(replacement);
            rewritten.push_str(&sql[at + needle.len()..]);
            rewritten
        }
        None => sql.to_string(),
    }
}

async fn catalog_rows(
    router: &PartitionRouter,
    collection: &str,
    filter: bson::Document,
) -> Result<Vec<bson::Document>, String> {
    match broadcast_catalog(router, PartitionCommand::find(collection, filter)).await {
        Ok(PartitionResult::Found { documents }) => Ok(documents),
        Ok(PartitionResult::Error { message }) => Err(message),
        Ok(_) => Err("unexpected partition result".into()),
        Err(message) => Err(message),
    }
}

fn sql_rows(rows: Vec<bson::Document>) -> bson::Document {
    bson::doc! {
        "ok": 1.0,
        "cursor": {
            "id": 0i64,
            "ns": "sql.query",
            "firstBatch": rows,
        },
    }
}

/// Loads all documents from an in-memory collection by dispatching a find
/// to its partition.
///
/// Returns the documents as an `Arc<Vec<Document>>`, or an error if the
/// collection does not exist or is empty.
async fn load_collection_documents(
    router: &PartitionRouter,
    table_name: &str,
    filter: &bson::Document,
) -> Result<Arc<Vec<bson::Document>>, String> {
    use crate::types::PartitionCommand;

    let find_cmd = PartitionCommand::find(table_name, filter.clone());
    // `admin.fields` and the other catalog collections are one row set
    // spread over every partition. A SQL read has to merge them, the
    // same way a find does.
    if !crate::storage::is_catalog(table_name) {
        if let Ok(shards) =
            crate::shard::SQL_ROUTER.try_with(|router| std::sync::Arc::clone(router))
        {
            return match shards.find_documents(table_name, filter).await {
                Ok(documents) => Ok(Arc::new(documents)),
                Err(message) => Err(message),
            };
        }
    }
    if crate::storage::is_catalog(table_name) {
        return match broadcast_catalog(router, find_cmd).await {
            Ok(PartitionResult::Found { documents }) => Ok(Arc::new(documents)),
            Ok(PartitionResult::Error { message }) => Err(message),
            Ok(_) => Err("unexpected partition result".into()),
            Err(message) => Err(message),
        };
    }

    match router.dispatch(find_cmd) {
        Some(rx) => match rx.await {
            Ok(PartitionResult::Found { documents }) => Ok(Arc::new(documents)),
            Ok(PartitionResult::Error { message }) => Err(message),
            Ok(_) => Err("unexpected partition result".into()),
            Err(_) => Err("partition worker dropped the request".into()),
        },
        None => Err("storage partition unavailable".into()),
    }
}

/// Asks the worker which access path `filter` would use.
async fn index_plan(
    router: &PartitionRouter,
    collection: &str,
    filter: bson::Document,
) -> Option<(String, Option<String>)> {
    let command = PartitionCommand::Explain {
        collection: collection.to_string(),
        filter,
    };
    match router.dispatch(command) {
        Some(rx) => match rx.await {
            Ok(PartitionResult::Plan { stage, index_name }) => Some((stage, index_name)),
            _ => None,
        },
        None => None,
    }
}

fn patch_winning_plan(reply: &mut bson::Document, stage: &str, index_name: Option<String>) {
    let Some(bson::Bson::Document(planner)) = reply.get_mut("queryPlanner") else {
        return;
    };
    let Some(bson::Bson::Document(plan)) = planner.get_mut("winningPlan") else {
        return;
    };
    plan.insert("stage", stage);
    if let Some(name) = index_name {
        plan.insert("indexName", name);
    }
}

/// A WHERE clause that is one comparison: `field > 10`, `field = 'CLERK'`.
///
/// Anything wider (AND, OR, functions) returns `None` and the SQL path
/// loads the whole collection. DataFusion still applies the predicate,
/// so this extract only drops rows the comparison itself would drop.
fn sql_simple_filter(sql: &str) -> Option<bson::Document> {
    let lower = sql.to_ascii_lowercase();
    let where_at = lower.rfind(" where ")?;
    let tail = sql[where_at + 7..].trim().trim_end_matches(';').trim();
    let tail_lower = tail.to_ascii_lowercase();
    if tail_lower.contains(" and ") || tail_lower.contains(" or ") || tail_lower.contains('(') {
        return None;
    }
    let (field, rest) = split_sql_ident(tail)?;
    let rest = rest.trim_start();
    let (op, literal) = if let Some(rest) = rest.strip_prefix(">=") {
        ("$gte", rest)
    } else if let Some(rest) = rest.strip_prefix("<=") {
        ("$lte", rest)
    } else if rest.starts_with("<>") || rest.starts_with("!=") {
        return None;
    } else if let Some(rest) = rest.strip_prefix('>') {
        ("$gt", rest)
    } else if let Some(rest) = rest.strip_prefix('<') {
        ("$lt", rest)
    } else if let Some(rest) = rest.strip_prefix('=') {
        ("$eq", rest)
    } else {
        return None;
    };
    let value = sql_literal(literal.trim())?;
    if op == "$eq" {
        let mut filter = bson::Document::new();
        filter.insert(field, value);
        Some(filter)
    } else {
        let mut bound = bson::Document::new();
        bound.insert(op, value);
        let mut filter = bson::Document::new();
        filter.insert(field, bound);
        Some(filter)
    }
}

fn split_sql_ident(text: &str) -> Option<(&str, &str)> {
    let text = text.trim_start();
    let end = text.find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'))?;
    if end == 0 {
        return None;
    }
    Some((&text[..end], &text[end..]))
}

fn sql_literal(text: &str) -> Option<bson::Bson> {
    if let Some(inner) = text
        .strip_prefix('\'')
        .and_then(|rest| rest.strip_suffix('\''))
    {
        return Some(bson::Bson::String(inner.to_string()));
    }
    if let Some(inner) = text
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    {
        return Some(bson::Bson::String(inner.to_string()));
    }
    if let Ok(number) = text.parse::<i64>() {
        return Some(if let Ok(small) = i32::try_from(number) {
            bson::Bson::Int32(small)
        } else {
            bson::Bson::Int64(number)
        });
    }
    text.parse::<f64>()
        .ok()
        .filter(|number| number.is_finite())
        .map(bson::Bson::Double)
}

/// Runs an aggregation pipeline, resolving `$lookup` across partitions
/// and writing `$out` through insert so the WAL records it.
fn aggregate_command(command: &PartitionCommand) -> bool {
    match command {
        PartitionCommand::Aggregate { .. } => true,
        PartitionCommand::Transactional { inner, .. } => {
            matches!(inner.as_ref(), PartitionCommand::Aggregate { .. })
        }
        _ => false,
    }
}

fn aggregate_namespace(command: &PartitionCommand) -> Option<String> {
    match command {
        PartitionCommand::Aggregate { collection, .. } => Some(collection.clone()),
        PartitionCommand::Transactional { inner, .. } => match inner.as_ref() {
            PartitionCommand::Aggregate { collection, .. } => Some(collection.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn aggregate_resolved_mut(
    command: &mut PartitionCommand,
) -> Option<&mut Vec<(String, Vec<bson::Document>)>> {
    match command {
        PartitionCommand::Aggregate { resolved, .. } => Some(resolved),
        PartitionCommand::Transactional { inner, .. } => match inner.as_mut() {
            PartitionCommand::Aggregate { resolved, .. } => Some(resolved),
            _ => None,
        },
        _ => None,
    }
}

pub(crate) async fn execute_aggregate(
    router: &PartitionRouter,
    mut command: PartitionCommand,
) -> bson::Document {
    let Some(namespace) = aggregate_namespace(&command) else {
        return bson::doc! {
            "ok": 0.0,
            "errmsg": "expected an aggregate command",
        };
    };

    for _ in 0..8 {
        let reply = match router.dispatch(command.clone()) {
            Some(rx) => rx.await,
            None => {
                return protocol::reply_document(&PartitionResult::Error {
                    message: "storage partition unavailable".into(),
                });
            }
        };
        match reply {
            Ok(PartitionResult::Found { documents }) => {
                return cursor_batch(&namespace, documents);
            }
            Ok(PartitionResult::NeedForeign { namespace: foreign }) => {
                let already = aggregate_resolved_mut(&mut command)
                    .map(|resolved| resolved.iter().any(|(name, _)| name == &foreign))
                    .unwrap_or(true);
                if already {
                    return protocol::reply_document(&PartitionResult::Error {
                        message: format!("lookup of '{foreign}' was already loaded"),
                    });
                }
                let documents =
                    match load_collection_documents(router, &foreign, &bson::doc! {}).await {
                        Ok(documents) => (*documents).clone(),
                        Err(message) => {
                            return protocol::reply_document(&PartitionResult::Error { message });
                        }
                    };
                if let Some(resolved) = aggregate_resolved_mut(&mut command) {
                    resolved.push((foreign, documents));
                }
            }
            Ok(PartitionResult::StageOut {
                namespace: target,
                documents,
            }) => {
                if let Err(message) = write_out(router, &target, documents).await {
                    return protocol::reply_document(&PartitionResult::Error { message });
                }
                return cursor_batch(&namespace, Vec::new());
            }
            Ok(PartitionResult::Error { message }) => {
                return protocol::reply_document(&PartitionResult::Error { message });
            }
            Ok(_) => {
                return protocol::reply_document(&PartitionResult::Error {
                    message: "unexpected aggregate result".into(),
                });
            }
            Err(_) => {
                return protocol::reply_document(&PartitionResult::Error {
                    message: "partition worker dropped the request".into(),
                });
            }
        }
    }

    protocol::reply_document(&PartitionResult::Error {
        message: "aggregate exceeded the lookup limit".into(),
    })
}

async fn write_out(
    router: &PartitionRouter,
    namespace: &str,
    documents: Vec<bson::Document>,
) -> Result<(), String> {
    let dropped = dispatch_one(
        router,
        PartitionCommand::Drop {
            collection: namespace.to_string(),
        },
    )
    .await?;
    if let PartitionResult::Error { message } = dropped {
        return Err(message);
    }
    if documents.is_empty() {
        let created = dispatch_one(
            router,
            PartitionCommand::CreateCollection {
                collection: namespace.to_string(),
                autoincrement: Vec::new(),
            },
        )
        .await?;
        if let PartitionResult::Error { message } = created {
            return Err(message);
        }
        return Ok(());
    }
    let inserted = dispatch_one(
        router,
        PartitionCommand::Insert {
            collection: namespace.to_string(),
            documents,
        },
    )
    .await?;
    if let PartitionResult::Error { message } = inserted {
        return Err(message);
    }
    Ok(())
}

async fn dispatch_one(
    router: &PartitionRouter,
    command: PartitionCommand,
) -> Result<PartitionResult, String> {
    match router.dispatch(command) {
        Some(rx) => rx
            .await
            .map_err(|_| "partition worker dropped the request".to_string()),
        None => Err("storage partition unavailable".to_string()),
    }
}

/// Reads one collection and returns `{ key, value }` for each match.
///
/// `map` is a projection. A `$field` string reads that field; any other
/// value is copied as a literal. `reduce` and `out` are refused: the
/// reply is the projection, and nothing is written.
async fn map_reduce(command: &bson::Document, router: &PartitionRouter) -> bson::Document {
    if command.contains_key("txnId") {
        return bson::doc! { "ok": 0.0, "errmsg": "mapReduce cannot run inside a transaction" };
    }
    if command
        .get("map")
        .is_some_and(|value| value.as_str().is_some())
    {
        return bson::doc! { "ok": 0.0, "errmsg": "mapReduce refused JavaScript map" };
    }
    if command.contains_key("reduce") {
        return bson::doc! { "ok": 0.0, "errmsg": "mapReduce refused reduce" };
    }
    if command.contains_key("out") {
        return bson::doc! { "ok": 0.0, "errmsg": "mapReduce refused out" };
    }
    let Ok(collection) = command.get_str("mapReduce") else {
        return bson::doc! { "ok": 0.0, "errmsg": "mapReduce requires a collection name" };
    };
    if collection.is_empty() {
        return bson::doc! { "ok": 0.0, "errmsg": "mapReduce requires a collection name" };
    }
    let Ok(map) = command.get_document("map") else {
        return bson::doc! { "ok": 0.0, "errmsg": "map must be a document" };
    };
    if !map.contains_key("key") || !map.contains_key("value") {
        return bson::doc! { "ok": 0.0, "errmsg": "map requires 'key' and 'value'" };
    }
    let query = match command.get("query") {
        None => bson::Document::new(),
        Some(bson::Bson::Document(query)) => query.clone(),
        Some(_) => return bson::doc! { "ok": 0.0, "errmsg": "query must be a document" },
    };
    let db = command.get_str("$db").unwrap_or("");
    let namespace = if db.is_empty() {
        collection.to_string()
    } else {
        format!("{db}.{collection}")
    };
    let documents = match load_collection_documents(router, &namespace, &query).await {
        Ok(documents) => documents,
        Err(message) => return bson::doc! { "ok": 0.0, "errmsg": message },
    };
    let pairs = documents
        .iter()
        .map(|document| {
            bson::doc! {
                "key": project_operand(document, map.get("key").expect("key")),
                "value": project_operand(document, map.get("value").expect("value")),
            }
        })
        .collect();
    cursor_batch(&namespace, pairs)
}

fn project_operand(document: &bson::Document, operand: &bson::Bson) -> bson::Bson {
    let bson::Bson::String(path) = operand else {
        return operand.clone();
    };
    let Some(field) = path.strip_prefix('$') else {
        return operand.clone();
    };
    if field.is_empty() || field.starts_with('$') {
        return operand.clone();
    }
    lookup_path(document, field)
        .cloned()
        .unwrap_or(bson::Bson::Null)
}

fn lookup_path<'a>(document: &'a bson::Document, path: &str) -> Option<&'a bson::Bson> {
    let (head, tail) = match path.split_once('.') {
        Some((head, tail)) => (head, Some(tail)),
        None => (path, None),
    };
    let value = document.get(head)?;
    match tail {
        None => Some(value),
        Some(rest) => lookup_path(value.as_document()?, rest),
    }
}

fn cursor_batch(namespace: &str, documents: Vec<bson::Document>) -> bson::Document {
    bson::doc! {
        "ok": 1.0,
        "cursor": {
            "id": 0i64,
            "ns": namespace,
            "firstBatch": documents,
        },
    }
}

/// Reads one collection's index specs from the partition that owns it.
async fn list_indexes(router: &PartitionRouter, collection: &str) -> bson::Document {
    match router.dispatch(PartitionCommand::ListIndexes {
        collection: collection.to_string(),
    }) {
        Some(rx) => match rx.await {
            Ok(result) => protocol::reply_document(&result),
            Err(_) => bson::doc! {
                "ok": 0.0,
                "errmsg": "partition worker dropped the request",
            },
        },
        None => bson::doc! {
            "ok": 0.0,
            "errmsg": "storage partition unavailable",
        },
    }
}

/// Reads a catalog collection from every partition and merges the rows.
///
/// Each partition reports the databases and collections it stores, so
/// the same database row arrives more than once. The merged find keeps
/// one document per `_id`.
async fn broadcast_catalog(
    router: &PartitionRouter,
    command: PartitionCommand,
) -> Result<PartitionResult, String> {
    let (command, projection, sort, skip, limit, counting) = match command {
        PartitionCommand::Find {
            collection,
            filter,
            projection,
            sort,
            skip,
            limit,
        } => (
            PartitionCommand::Find {
                collection,
                filter,
                projection: None,
                sort: None,
                skip: 0,
                limit: None,
            },
            projection,
            sort,
            skip,
            limit,
            false,
        ),
        PartitionCommand::Count { collection, filter } => (
            PartitionCommand::find(collection, filter),
            None,
            None,
            0,
            None,
            true,
        ),
        other => (other, None, None, 0, None, false),
    };
    let mut documents = Vec::new();
    for rx in router.broadcast(command) {
        match rx.await {
            Ok(PartitionResult::Found {
                documents: mut found,
            }) => documents.append(&mut found),
            Ok(PartitionResult::Error { message }) => return Err(message),
            Ok(_) | Err(_) => return Err("partition worker returned no catalog rows".into()),
        }
    }
    documents.sort_by(|left, right| catalog_row_id(left).cmp(&catalog_row_id(right)));
    documents.dedup_by(|left, right| catalog_row_id(left) == catalog_row_id(right));
    if counting {
        return Ok(PartitionResult::Counted {
            count: documents.len() as u64,
        });
    }
    let documents =
        crate::storage::shape_cursor(documents, projection.as_ref(), sort.as_ref(), skip, limit)?;
    Ok(PartitionResult::Found { documents })
}

/// Runs a catalog aggregation on the merged rows.
///
/// `countDocuments` is an aggregate. Sending it to the one partition
/// that owns `admin.fields` counts only the fields stored there.
async fn execute_catalog_aggregate(
    router: &PartitionRouter,
    command: PartitionCommand,
) -> bson::Document {
    let Some(namespace) = aggregate_namespace(&command) else {
        return bson::doc! {
            "ok": 0.0,
            "errmsg": "expected an aggregate command",
        };
    };
    let pipeline = match &command {
        PartitionCommand::Aggregate { pipeline, .. } => pipeline.clone(),
        PartitionCommand::Transactional { inner, .. } => match inner.as_ref() {
            PartitionCommand::Aggregate { pipeline, .. } => pipeline.clone(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    };
    let documents =
        match broadcast_catalog(router, PartitionCommand::find(&namespace, bson::doc! {})).await {
            Ok(PartitionResult::Found { documents }) => documents,
            Ok(other) => {
                return protocol::reply_document(&PartitionResult::Error {
                    message: format!("catalog aggregate expected documents, got {other:?}"),
                });
            }
            Err(message) => return bson::doc! { "ok": 0.0, "errmsg": message },
        };
    let result = crate::storage::run_pipeline(documents, &pipeline);
    cursor_batch(
        &namespace,
        match result {
            PartitionResult::Found { documents } => documents,
            other => return protocol::reply_document(&other),
        },
    )
}

/// Sends an `admin.counters` update to every partition and sums the counts.
///
/// The counter lives with the collection, so the partition that owns
/// `admin.counters` by name is not the one that stores the row.
async fn broadcast_counter_update(
    router: &PartitionRouter,
    command: PartitionCommand,
) -> Result<PartitionResult, String> {
    let mut matched = 0u32;
    let mut modified = 0u32;
    for rx in router.broadcast(command) {
        match rx.await {
            Ok(PartitionResult::Modified {
                matched_count,
                modified_count,
            }) => {
                matched += matched_count;
                modified += modified_count;
            }
            Ok(PartitionResult::Error { message }) => return Err(message),
            Ok(_) | Err(_) => return Err("partition worker returned no counter update".into()),
        }
    }
    Ok(PartitionResult::Modified {
        matched_count: matched,
        modified_count: modified,
    })
}

fn catalog_row_id(document: &bson::Document) -> String {
    match document.get("_id") {
        Some(bson::Bson::String(value)) => value.clone(),
        Some(other) => other.to_string(),
        None => String::new(),
    }
}

/// Broadcasts `listCollections` to every partition and merges the
/// collection descriptions into a single cursor reply.
async fn list_collections(command: &bson::Document, router: &PartitionRouter) -> bson::Document {
    let partition_command = match protocol::command_from_document(command) {
        Ok(partition_command) => partition_command,
        Err(message) => return bson::doc! { "ok": 0.0, "errmsg": message },
    };

    let mut collections: Vec<bson::Document> = Vec::new();
    for rx in router.broadcast(partition_command) {
        match rx.await {
            Ok(PartitionResult::ListedCollections {
                collections: mut listed,
            }) => collections.append(&mut listed),
            Ok(PartitionResult::Error { message }) => {
                return protocol::reply_document(&PartitionResult::Error { message });
            }
            Ok(_) | Err(_) => {
                return protocol::reply_document(&PartitionResult::Error {
                    message: "partition worker returned no collections".into(),
                });
            }
        }
    }

    collections.sort_by(|a, b| {
        a.get_str("name")
            .unwrap_or("")
            .cmp(b.get_str("name").unwrap_or(""))
    });
    collections.dedup_by(|left, right| left.get_str("name").ok() == right.get_str("name").ok());

    let db = command.get_str("$db").unwrap_or("admin");
    bson::doc! {
        "ok": 1.0,
        "cursor": {
            "id": 0i64,
            "ns": format!("{}.$cmd.listCollections", db),
            "firstBatch": collections,
        },
    }
}

/// Broadcasts `dropDatabase` to every partition.
///
/// Every worker drops the share of the database it owns; the reply
/// names the database like MongoDB's own `{dropped: "<db>"}`.
async fn drop_database(command: &bson::Document, router: &PartitionRouter) -> bson::Document {
    let partition_command = match protocol::command_from_document(command) {
        Ok(partition_command) => partition_command,
        Err(message) => return bson::doc! { "ok": 0.0, "errmsg": message },
    };
    let database = match &partition_command {
        PartitionCommand::DropDatabase { database } => database.clone(),
        _ => {
            return bson::doc! {
                "ok": 0.0,
                "errmsg": "expected a dropDatabase command",
            }
        }
    };

    for rx in router.broadcast(partition_command) {
        match rx.await {
            Ok(PartitionResult::DroppedDatabase { .. }) => {}
            Ok(PartitionResult::Error { message }) => {
                return protocol::reply_document(&PartitionResult::Error { message });
            }
            Ok(_) | Err(_) => {
                return protocol::reply_document(&PartitionResult::Error {
                    message: "partition worker failed to drop the database".into(),
                });
            }
        }
    }

    bson::doc! { "ok": 1.0, "dropped": database }
}

/// Broadcasts `dbStats` to every partition and sums the counters.
///
/// One database's collections hash across partitions, so no single
/// worker knows the whole picture; the totals are summed here.
async fn database_stats(command: &bson::Document, router: &PartitionRouter) -> bson::Document {
    let partition_command = match protocol::command_from_document(command) {
        Ok(partition_command) => partition_command,
        Err(message) => return bson::doc! { "ok": 0.0, "errmsg": message },
    };

    let database = match &partition_command {
        PartitionCommand::DatabaseStats { database } => database.clone(),
        _ => {
            return bson::doc! {
                "ok": 0.0,
                "errmsg": "expected a dbStats command",
            }
        }
    };

    let mut collections = 0u64;
    let mut objects = 0u64;
    let mut data_size = 0u64;
    let mut indexes = 0u64;

    for rx in router.broadcast(partition_command) {
        match rx.await {
            Ok(PartitionResult::DatabaseStats {
                collections: c,
                objects: o,
                data_size: s,
                indexes: i,
            }) => {
                collections += c;
                objects += o;
                data_size += s;
                indexes += i;
            }
            Ok(PartitionResult::Error { message }) => {
                return protocol::reply_document(&PartitionResult::Error { message });
            }
            Ok(_) | Err(_) => {
                return protocol::reply_document(&PartitionResult::Error {
                    message: "partition worker returned no statistics".into(),
                });
            }
        }
    }

    let mut reply = protocol::reply_document(&PartitionResult::DatabaseStats {
        collections,
        objects,
        data_size,
        indexes,
    });
    reply.insert("db", database);
    reply
}

/// Broadcasts `listDatabases` to every partition and merges the
/// database descriptions into one reply.
///
/// A database may live on several partitions (its collections hash
/// across workers); the merged entry is `empty` only when every
/// partition reported it empty.
async fn list_databases(command: &bson::Document, router: &PartitionRouter) -> bson::Document {
    let partition_command = match protocol::command_from_document(command) {
        Ok(partition_command) => partition_command,
        Err(message) => return bson::doc! { "ok": 0.0, "errmsg": message },
    };

    // name -> empty; a database is non-empty once any partition says
    // it holds documents.
    let mut emptiness: std::collections::BTreeMap<String, bool> = std::collections::BTreeMap::new();
    for rx in router.broadcast(partition_command) {
        match rx.await {
            Ok(PartitionResult::ListedDatabases { databases }) => {
                for database in databases {
                    let Some(name) = database.get_str("name").ok().map(str::to_string) else {
                        continue;
                    };
                    let empty = database.get_bool("empty").unwrap_or(false);
                    let entry = emptiness.entry(name).or_insert(true);
                    *entry &= empty;
                }
            }
            Ok(PartitionResult::Error { message }) => {
                return protocol::reply_document(&PartitionResult::Error { message });
            }
            Ok(_) | Err(_) => {
                return protocol::reply_document(&PartitionResult::Error {
                    message: "partition worker returned no databases".into(),
                });
            }
        }
    }

    let mut databases: Vec<bson::Document> = emptiness
        .into_iter()
        .map(|(name, empty)| {
            bson::doc! {
                "name": name,
                "sizeOnDisk": 0i64,
                "empty": empty,
            }
        })
        .collect();

    // The optional filter (e.g. `{name: "shop"}`) is applied once on
    // the merged list so a database split across partitions cannot
    // half-match.
    if let Ok(filter) = command.get_document("filter") {
        databases.retain(|database| crate::storage::document_matches(database, filter));
    }

    bson::doc! {
        "databases": databases,
        "totalSize": 0i64,
        "ok": 1.0,
    }
}

/// Fills `cursor.ns` with the `db.collection` namespace.
///
/// Drivers (mongosh included) refuse a cursor whose namespace is empty,
/// so the reply must name the database from `$db` and the collection
/// from the command's first field (`find`, `count`, ...).
fn cursor_ns(command: &bson::Document, mut reply: bson::Document) -> bson::Document {
    let db = command.get_str("$db").unwrap_or("admin");

    // `listIndexes` uses MongoDB's own command namespace
    // (`<db>.$cmd.listIndexes`), not the collection namespace.
    let ns = if command.contains_key("listIndexes") {
        format!("{}.$cmd.listIndexes", db)
    } else {
        let collection = command
            .get_str("find")
            .or_else(|_| command.get_str("aggregate"))
            .or_else(|_| command.get_str("count"))
            .unwrap_or("");
        if collection.is_empty() {
            return reply;
        }
        format!("{}.{}", db, collection)
    };

    if let Some(bson::Bson::Document(cursor)) = reply.get_mut("cursor") {
        cursor.insert("ns", ns);
    }

    reply
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::network::wire::{MessageHeader, HEADER_SIZE, OP_MSG};

    #[test]
    fn replicated_writes_are_auto_commit_mutations() {
        assert!(is_replicated_write(
            &bson::doc! { "insert": "emp", "documents": [] }
        ));
        assert!(is_replicated_write(&bson::doc! {
            "aggregate": "emp",
            "pipeline": [ { "$out": "bonus" } ],
        }));
        assert!(!is_replicated_write(&bson::doc! { "find": "emp" }));
        assert!(is_show_tables("show tables"));
        assert!(is_show_tables("  SHOW TABLES;  "));
        assert!(!is_show_tables("select * from emp"));
        assert!(is_show_databases("show databases"));
        assert!(is_show_databases("SHOW DATABASES;"));
        assert!(!is_show_databases("show databaases"));
        assert_eq!(sql_for_engine("desc emp;"), "DESCRIBE emp");
        assert_eq!(sql_for_engine("DESC emp"), "DESCRIBE emp");
        assert_eq!(
            sql_for_engine("select ename from emp"),
            "select ename from emp"
        );
        assert_eq!(sql_table_name("describe emp;").as_deref(), Some("emp"));
        assert_eq!(sql_table_name("DESC `emp`").as_deref(), Some("emp"));
        assert_eq!(
            sql_table_name("show columns from emp").as_deref(),
            Some("emp")
        );
        assert_eq!(
            sql_table_name("SHOW FIELDS IN shop.emp").as_deref(),
            Some("emp")
        );
        assert_eq!(
            sql_table_name("select ename from emp where sal > 2000").as_deref(),
            Some("emp")
        );
        assert_eq!(
            sql_table_name("select * from information_schema.tables").as_deref(),
            None
        );
        assert_eq!(
            sql_table_names("select a.*, b.* from emp a INNER JOIN dept b ON a.deptno = b.deptno"),
            vec!["emp".to_string(), "dept".to_string()]
        );
        assert_eq!(
            sql_table_names("select * from emp e, dept d"),
            vec!["emp".to_string(), "dept".to_string()]
        );
        assert_eq!(
            sql_table_names("select extract(year from hiredate) from emp"),
            vec!["emp".to_string()]
        );
        assert_eq!(
            sql_table_names("select * from (select * from emp) a join dept b"),
            vec!["emp".to_string(), "dept".to_string()]
        );
        assert!(!is_replicated_write(
            &bson::doc! { "insert": "emp", "txnId": 1i64 }
        ));
        assert!(!is_replicated_write(
            &bson::doc! { "aggregate": "emp", "pipeline": [ { "$match": {} } ] }
        ));
        assert!(is_replicated_write(
            &bson::doc! { "applyTransaction": "emp", "ops": [], "commitTxn": 1i64 }
        ));
        assert!(!is_replicated_write(
            &bson::doc! { "commit": 1i32, "collection": "emp", "txnId": 1i64 }
        ));
        assert!(is_leader_local(&bson::doc! { "beginTransaction": "emp" }));
        assert!(is_leader_local(
            &bson::doc! { "commit": 1i32, "collection": "emp", "txnId": 1i64 }
        ));
        assert!(is_leader_local(
            &bson::doc! { "abort": 1i32, "collection": "emp", "txnId": 1i64 }
        ));
        assert!(!is_leader_local(
            &bson::doc! { "applyTransaction": "emp", "ops": [], "commitTxn": 1i64 }
        ));
    }

    /// Builds a 16-byte little-endian header with the given fields.
    fn build_header(
        message_length: u32,
        request_id: i32,
        response_to: i32,
        op_code: i32,
    ) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];
        bytes[0..4].copy_from_slice(&message_length.to_le_bytes());
        bytes[4..8].copy_from_slice(&request_id.to_le_bytes());
        bytes[8..12].copy_from_slice(&response_to.to_le_bytes());
        bytes[12..16].copy_from_slice(&op_code.to_le_bytes());
        bytes
    }

    #[test]
    fn test_sql_simple_filter_is_one_comparison() {
        let filter = sql_simple_filter("select * from emp where sal > 1000").unwrap();
        assert_eq!(filter, bson::doc! { "sal": { "$gt": 1000 } });
        assert!(sql_simple_filter("select * from emp where sal > 1000 and deptno = 10").is_none());
    }

    #[test]
    fn test_sql_table_name_keeps_collection_case() {
        // mongosh sends lowercase SQL. Uppercasing the identifier used to
        // look up `USERS` and return an empty snapshot.
        assert_eq!(
            sql_table_name("select * from users where age > 25").as_deref(),
            Some("users")
        );
        assert_eq!(
            sql_table_name("SELECT * FROM users").as_deref(),
            Some("users")
        );
        assert_eq!(
            sql_table_name("select * from \"users\"").as_deref(),
            Some("users")
        );
    }

    #[tokio::test]
    async fn test_server_runs_sql_against_stored_documents() {
        let router = PartitionRouter::new(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test port");
        let addr = listener.local_addr().expect("local addr");

        let server_router = router.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let client_router = server_router.clone();
                tokio::spawn(async move {
                    let _ = handle_client(&mut socket, &client_router, None).await;
                });
            }
        });

        let insert = bson::doc! {
            "insert": "users",
            "$db": "shop",
            "documents": [
                { "name": "Ada", "age": 30i32 },
                { "name": "Bob", "age": 25i32 },
            ],
        };
        let frame = protocol::encode_reply(1, 0, &insert).expect("encode");
        let (inserted, _) = send_and_read_reply(addr, frame).await;
        assert_eq!(inserted.command.get_f64("ok").expect("ok"), 1.0);

        let query = bson::doc! {
            "sql": "select * from users where age > 25",
            "$db": "shop",
        };
        let frame = protocol::encode_reply(2, 0, &query).expect("encode");
        let (reply, _) = send_and_read_reply(addr, frame).await;
        assert_eq!(
            reply.command.get_f64("ok").expect("ok"),
            1.0,
            "sql must run against shop.users: {:?}",
            reply.command
        );

        let cursor = reply.command.get_document("cursor").expect("cursor");
        let batch = cursor.get_array("firstBatch").expect("firstBatch");
        assert_eq!(batch.len(), 1, "only Ada is older than 25: {:?}", batch);
        let row = batch[0].as_document().expect("row");
        assert_eq!(row.get_str("name").expect("name"), "Ada");
    }

    #[tokio::test]
    async fn test_sql_literal_count_and_group_by() {
        let router = PartitionRouter::new(1);
        let local = "127.0.0.1:1".parse().expect("addr");

        let inserted = execute_command(
            &bson::doc! {
                "insert": "emp",
                "$db": "db",
                "documents": [
                    { "ename": "SMITH", "job": "CLERK", "sal": 800i32 },
                    { "ename": "ADAMS", "job": "CLERK", "sal": 1100i32 },
                    { "ename": "KING", "job": "PRESIDENT", "sal": 5000i32 },
                ],
            },
            &router,
            local,
        )
        .await;
        assert_eq!(inserted.get_f64("ok").expect("ok"), 1.0);

        let literal = execute_command(
            &bson::doc! { "sql": "SELECT 1 AS n", "$db": "db" },
            &router,
            local,
        )
        .await;
        assert_eq!(literal.get_f64("ok").expect("ok"), 1.0, "{literal:?}");
        let batch = literal
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("firstBatch");
        assert_eq!(batch.len(), 1);

        let count = execute_command(
            &bson::doc! {
                "sql": "select count(*) as n from emp where sal > 1000",
                "$db": "db",
            },
            &router,
            local,
        )
        .await;
        assert_eq!(count.get_f64("ok").expect("ok"), 1.0, "{count:?}");
        let row = count
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap()[0]
            .as_document()
            .unwrap()
            .clone();
        let n = row
            .get_i64("n")
            .or_else(|_| row.get_i32("n").map(|v| i64::from(v)));
        assert_eq!(n, Ok(2), "ADAMS and KING: {row:?}");

        let grouped = execute_command(
            &bson::doc! {
                "sql": "select job, count(*) as n from emp group by job",
                "$db": "db",
            },
            &router,
            local,
        )
        .await;
        assert_eq!(grouped.get_f64("ok").expect("ok"), 1.0, "{grouped:?}");
        let rows = grouped
            .get_document("cursor")
            .unwrap()
            .get_array("firstBatch")
            .unwrap();
        let clerks = rows
            .iter()
            .find(|row| row.as_document().and_then(|doc| doc.get_str("job").ok()) == Some("CLERK"));
        let clerks = clerks.expect("CLERK group").as_document().unwrap();
        let n = clerks
            .get_i64("n")
            .or_else(|_| clerks.get_i32("n").map(|v| i64::from(v)));
        assert_eq!(n, Ok(2), "{clerks:?}");
    }

    #[tokio::test]
    async fn test_sql_join_loads_every_collection() {
        let router = PartitionRouter::new(1);
        let local = "127.0.0.1:1".parse().expect("addr");

        let inserted = execute_command(
            &bson::doc! {
                "insert": "emp",
                "$db": "db",
                "documents": [
                    { "ename": "KING", "deptno": 10i32 },
                    { "ename": "SMITH", "deptno": 20i32 },
                ],
            },
            &router,
            local,
        )
        .await;
        assert_eq!(inserted.get_f64("ok").expect("ok"), 1.0);
        let inserted = execute_command(
            &bson::doc! {
                "insert": "dept",
                "$db": "db",
                "documents": [
                    { "deptno": 10i32, "dname": "ACCOUNTING" },
                    { "deptno": 30i32, "dname": "SALES" },
                ],
            },
            &router,
            local,
        )
        .await;
        assert_eq!(inserted.get_f64("ok").expect("ok"), 1.0);

        let joined = execute_command(
            &bson::doc! {
                "sql": "select a.ename, b.dname from emp a inner join dept b on a.deptno = b.deptno",
                "$db": "db",
            },
            &router,
            local,
        )
        .await;
        assert_eq!(joined.get_f64("ok").expect("ok"), 1.0, "{joined:?}");
        let rows = joined
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("firstBatch");
        assert_eq!(rows.len(), 1, "{rows:?}");
        let row = rows[0].as_document().expect("row");
        assert_eq!(row.get_str("ename").expect("ename"), "KING");
        assert_eq!(row.get_str("dname").expect("dname"), "ACCOUNTING");

        let starred = execute_command(
            &bson::doc! {
                "sql": "select a.*, b.* from emp a INNER JOIN dept b ON a.deptno = b.deptno",
                "$db": "db",
            },
            &router,
            local,
        )
        .await;
        assert_eq!(starred.get_f64("ok").expect("ok"), 1.0, "{starred:?}");
        let rows = starred
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("firstBatch");
        assert_eq!(rows.len(), 1, "{rows:?}");
        let row = rows[0].as_document().expect("row");
        assert_eq!(row.get_str("ename").expect("ename"), "KING");
        assert_eq!(row.get_str("dname").expect("dname"), "ACCOUNTING");
    }

    #[test]
    fn show_index_table_reads_the_collection_name() {
        assert_eq!(
            show_index_table("show indexes from emp").as_deref(),
            Some("emp")
        );
        assert_eq!(
            show_index_table("SHOW KEYS IN `emp`;").as_deref(),
            Some("emp")
        );
        assert_eq!(
            show_index_table("show index from shop.emp").as_deref(),
            Some("emp")
        );
        assert_eq!(show_index_table("show tables"), None);
        assert_eq!(show_index_table("show index emp"), None);
    }

    #[tokio::test]
    async fn test_show_indexes_lists_the_collection_indexes() {
        let router = PartitionRouter::new(1);
        let local = "127.0.0.1:1".parse().expect("addr");

        let inserted = execute_command(
            &bson::doc! {
                "insert": "emp",
                "$db": "db",
                "documents": [ { "ename": "KING", "sal": 5000i32, "deptno": 10i32 } ],
            },
            &router,
            local,
        )
        .await;
        assert_eq!(inserted.get_f64("ok").expect("ok"), 1.0);

        let created = execute_command(
            &bson::doc! {
                "createIndexes": "emp",
                "$db": "db",
                "indexes": [
                    { "key": { "sal": 1i32 }, "name": "sal_1" },
                    { "key": { "deptno": 1i32, "sal": -1i32 }, "name": "deptno_1_sal_1" },
                    { "key": { "ename": 1i32 }, "name": "ename_1", "unique": true },
                    { "key": { "comm": 1i32 }, "name": "comm_1", "sparse": true },
                ],
            },
            &router,
            local,
        )
        .await;
        assert_eq!(created.get_f64("ok").expect("ok"), 1.0, "{created:?}");

        let listed = execute_command(
            &bson::doc! {
                "sql": "show indexes from emp",
                "$db": "db",
            },
            &router,
            local,
        )
        .await;
        assert_eq!(listed.get_f64("ok").expect("ok"), 1.0, "{listed:?}");
        let rows = listed
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("firstBatch");
        let names: Vec<&str> = rows
            .iter()
            .filter_map(|row| row.as_document()?.get_str("name").ok())
            .collect();
        assert_eq!(names.iter().filter(|name| **name == "_id_").count(), 1);
        assert!(names.contains(&"sal_1"));
        assert_eq!(
            names
                .iter()
                .filter(|name| **name == "deptno_1_sal_1")
                .count(),
            2
        );

        let composite: Vec<&bson::Document> = rows
            .iter()
            .filter_map(|row| {
                let doc = row.as_document()?;
                (doc.get_str("name").ok() == Some("deptno_1_sal_1")).then_some(doc)
            })
            .collect();
        assert_eq!(composite[0].get_i32("seq").expect("seq"), 1);
        assert_eq!(composite[0].get_str("column").expect("column"), "deptno");
        assert_eq!(composite[0].get_str("order").expect("order"), "asc");
        assert_eq!(composite[1].get_i32("seq").expect("seq"), 2);
        assert_eq!(composite[1].get_str("column").expect("column"), "sal");
        assert_eq!(composite[1].get_str("order").expect("order"), "desc");

        let ename = rows
            .iter()
            .find(|row| {
                row.as_document().and_then(|doc| doc.get_str("name").ok()) == Some("ename_1")
            })
            .expect("ename_1")
            .as_document()
            .expect("row");
        assert!(ename.get_bool("unique").expect("unique"));
        let comm = rows
            .iter()
            .find(|row| {
                row.as_document().and_then(|doc| doc.get_str("name").ok()) == Some("comm_1")
            })
            .expect("comm_1")
            .as_document()
            .expect("row");
        assert!(comm.get_bool("sparse").expect("sparse"));

        let missing = execute_command(
            &bson::doc! {
                "sql": "show keys from missing_table",
                "$db": "db",
            },
            &router,
            local,
        )
        .await;
        assert_eq!(missing.get_f64("ok").expect("ok"), 0.0, "{missing:?}");
    }

    #[test]
    fn sql_text_search_parses_the_view_query() {
        let search =
            sql_text_search("SELECT ename, job, sal\nFROM emp_search\nWHERE TEXT 'clerk';")
                .expect("text clause")
                .expect("parse");
        assert_eq!(search.table, "emp_search");
        assert_eq!(search.query, "clerk");
        assert_eq!(search.fuzzy, 0.0);
        assert!(search.facets.is_none());
        assert_eq!(
            search.columns.expect("columns"),
            vec!["ename".to_string(), "job".to_string(), "sal".to_string()]
        );

        let fuzzy =
            sql_text_search("SELECT ename, job, sal FROM emp_search WHERE TEXT 'clrek' FUZZY 0.3;")
                .expect("fuzzy clause")
                .expect("parse");
        assert!((fuzzy.fuzzy - 0.3).abs() < 1e-9);

        let faceted = sql_text_search(
            "SELECT facet, bucket, doc_count\nFROM emp_search\nWHERE TEXT 'clerk'\nFACETS\n  job TERMS,\n  sal RANGES (1500), (1500, 3000), (3000);",
        )
        .expect("facets clause")
        .expect("parse");
        let facets = faceted.facets.expect("facets");
        assert!(facets.get_document("job").unwrap().contains_key("terms"));
        let ranges = facets
            .get_document("sal")
            .unwrap()
            .get_document("range")
            .unwrap()
            .get_array("ranges")
            .unwrap();
        assert_eq!(ranges.len(), 3);
        assert!(ranges[0].as_document().unwrap().get("to").is_some());
        assert!(ranges[1].as_document().unwrap().get("from").is_some());
        assert!(ranges[1].as_document().unwrap().get("to").is_some());
        assert!(ranges[2].as_document().unwrap().get("from").is_some());

        assert!(sql_text_search("select ename from emp where sal > 1").is_none());
        assert!(sql_text_search("select ename from emp where text = 'clerk'").is_none());
    }

    #[tokio::test]
    async fn test_sql_text_search_reads_the_inverted_view() {
        let router = PartitionRouter::new(1);
        let local = "127.0.0.1:1".parse().expect("addr");

        let inserted = execute_command(
            &bson::doc! {
                "insert": "emp",
                "$db": "db",
                "documents": [
                    { "ename": "SMITH", "job": "CLERK", "sal": 800i32 },
                    { "ename": "ADAMS", "job": "CLERK", "sal": 1100i32 },
                    { "ename": "KING", "job": "PRESIDENT", "sal": 5000i32 },
                ],
            },
            &router,
            local,
        )
        .await;
        assert_eq!(inserted.get_f64("ok").expect("ok"), 1.0, "{inserted:?}");

        let created = execute_command(
            &bson::doc! {
                "create": "emp_search",
                "viewOn": "emp",
                "$db": "db",
                "kova": { "inverted": true },
            },
            &router,
            local,
        )
        .await;
        assert_eq!(created.get_f64("ok").expect("ok"), 1.0, "{created:?}");

        let searched = execute_command(
            &bson::doc! {
                "sql": "SELECT ename, job, sal FROM emp_search WHERE TEXT 'clerk'",
                "$db": "db",
            },
            &router,
            local,
        )
        .await;
        assert_eq!(searched.get_f64("ok").expect("ok"), 1.0, "{searched:?}");
        let mut names = sql_rows(&searched)
            .iter()
            .map(|row| row.get_str("ename").expect("ename").to_string())
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, ["ADAMS", "SMITH"]);

        let fuzzy = execute_command(
            &bson::doc! {
                "sql": "SELECT ename FROM emp_search WHERE TEXT 'clrek' FUZZY 0.3",
                "$db": "db",
            },
            &router,
            local,
        )
        .await;
        assert_eq!(fuzzy.get_f64("ok").expect("ok"), 1.0, "{fuzzy:?}");
        let mut names = sql_rows(&fuzzy)
            .iter()
            .map(|row| row.get_str("ename").expect("ename").to_string())
            .collect::<Vec<_>>();
        names.sort();
        assert_eq!(names, ["ADAMS", "SMITH"]);

        let faceted = execute_command(
            &bson::doc! {
                "sql": "SELECT facet, bucket, doc_count FROM emp_search WHERE TEXT 'clerk' FACETS job TERMS, sal RANGES (1500), (1500, 3000), (3000)",
                "$db": "db",
            },
            &router,
            local,
        )
        .await;
        assert_eq!(faceted.get_f64("ok").expect("ok"), 1.0, "{faceted:?}");
        let rows = sql_rows(&faceted);
        assert_eq!(facet_count(&rows, "job", "CLERK"), 2, "{rows:?}");
        assert_eq!(facet_count(&rows, "sal", "..1500"), 2, "{rows:?}");
    }

    fn sql_rows(reply: &bson::Document) -> Vec<bson::Document> {
        reply
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("firstBatch")
            .iter()
            .filter_map(|row| row.as_document().cloned())
            .collect()
    }

    fn facet_count(rows: &[bson::Document], facet: &str, bucket: &str) -> i64 {
        rows.iter()
            .find(|row| {
                row.get_str("facet").ok() == Some(facet)
                    && row.get_str("bucket").ok() == Some(bucket)
            })
            .map(|row| {
                row.get_i64("doc_count")
                    .or_else(|_| row.get_i32("doc_count").map(i64::from))
                    .unwrap_or(0)
            })
            .unwrap_or(0)
    }

    #[test]
    fn test_parse_op_msg_header() {
        let bytes = build_header(32, 1, 0, OP_MSG);
        let header = MessageHeader::parse(&bytes);

        assert_eq!(header.message_length, 32);
        assert_eq!(header.request_id, 1);
        assert_eq!(header.response_to, 0);
        assert_eq!(header.op_code, OP_MSG);
    }

    #[test]
    fn test_parse_unknown_opcode() {
        let bytes = build_header(16, 42, 7, 2012); // OP_COMPRESSED (unsupported)
        let header = MessageHeader::parse(&bytes);

        assert_eq!(header.op_code, 2012);
        assert_ne!(header.op_code, OP_MSG);
        assert_eq!(header.request_id, 42);
        assert_eq!(header.response_to, 7);
    }

    #[test]
    fn test_parse_preserves_little_endian_layout() {
        let bytes = build_header(256, 0, 0, OP_MSG);
        assert_eq!(&bytes[0..4], &[0x00, 0x01, 0x00, 0x00]);

        let header = MessageHeader::parse(&bytes);
        assert_eq!(header.message_length, 256);
    }

    #[test]
    fn test_parse_real_mongosh_handshake_header() {
        // First 16 bytes captured from a real `mongosh` connection:
        // messageLength=406, requestId=1, responseTo=0, opCode=2004.
        let captured: [u8; HEADER_SIZE] = [
            0x96, 0x01, 0x00, 0x00, // 406, little-endian
            0x01, 0x00, 0x00, 0x00, // requestId = 1
            0x00, 0x00, 0x00, 0x00, // responseTo = 0
            0xd4, 0x07, 0x00, 0x00, // opCode = 2004 (OP_QUERY)
        ];

        let header = MessageHeader::parse(&captured);
        assert_eq!(header.message_length, 406);
        assert_eq!(header.request_id, 1);
        assert_eq!(header.response_to, 0);
        assert_eq!(header.op_code, wire::OP_QUERY);
    }

    #[test]
    fn test_header_size_is_16_bytes() {
        assert_eq!(HEADER_SIZE, 16);
        assert_eq!(build_header(0, 0, 0, 0).len(), HEADER_SIZE);
    }

    /// Connects to `addr`, writes one frame and reads the reply.
    ///
    /// Returns the decoded OP_MSG reply together with its header.
    async fn send_and_read_reply(
        addr: std::net::SocketAddr,
        frame: Vec<u8>,
    ) -> (protocol::OpMsg, MessageHeader) {
        let mut client = TcpStream::connect(addr).await.expect("connect");
        client.write_all(&frame).await.expect("write request");

        let mut header_buf = [0u8; HEADER_SIZE];
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read_exact(&mut header_buf),
        )
        .await
        .expect("reply timed out")
        .expect("reply header read");

        let header = MessageHeader::parse(&header_buf);
        assert_eq!(header.op_code, OP_MSG, "reply must be OP_MSG");
        assert!(header.message_length > HEADER_SIZE as u32);

        let body_len = header.message_length as usize - HEADER_SIZE;
        let mut body = vec![0u8; body_len];
        client.read_exact(&mut body).await.expect("reply body");

        let reply = protocol::decode_op_msg(&body).expect("decode reply");
        (reply, header)
    }

    #[tokio::test]
    async fn test_server_accepts_insert_and_replies() {
        let router = PartitionRouter::new(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test port");
        let addr = listener.local_addr().expect("local addr");

        let server_router = router.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let _ = handle_client(&mut socket, &server_router, None).await;
        });

        // Build an OP_MSG frame carrying an insert command.
        let command = bson::doc! { "insert": "users", "documents": [{ "name": "Ada" }] };
        let frame = protocol::encode_reply(1, 0, &command).expect("encode");

        let mut client = TcpStream::connect(addr).await.expect("connect");
        client.write_all(&frame).await.expect("write request");

        // Read the 16-byte reply header.
        let mut header_buf = [0u8; HEADER_SIZE];
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read_exact(&mut header_buf),
        )
        .await
        .expect("reply timed out")
        .expect("reply header read");

        let reply_header = MessageHeader::parse(&header_buf);
        assert_eq!(reply_header.op_code, OP_MSG);
        assert_eq!(
            reply_header.response_to, 1,
            "reply must echo the request id"
        );
        assert!(reply_header.message_length > HEADER_SIZE as u32);

        // Read and decode the reply body.
        let body_len = reply_header.message_length as usize - HEADER_SIZE;
        let mut body = vec![0u8; body_len];
        client.read_exact(&mut body).await.expect("reply body");

        let msg = protocol::decode_op_msg(&body).expect("decode reply");
        assert_eq!(
            msg.command.get_f64("ok").expect("ok field"),
            1.0,
            "insert must succeed: {:?}",
            msg.command
        );
        assert_eq!(msg.command.get_i64("n").expect("n field"), 1);
    }

    #[tokio::test]
    async fn test_server_replies_to_ping_handshake() {
        let router = PartitionRouter::new(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test port");
        let addr = listener.local_addr().expect("local addr");

        let server_router = router.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let _ = handle_client(&mut socket, &server_router, None).await;
        });

        let frame = protocol::encode_reply(1, 0, &bson::doc! { "ping": 1.0 }).expect("encode");
        let (reply, _) = send_and_read_reply(addr, frame).await;

        assert_eq!(reply.command.get_f64("ok").expect("ok"), 1.0);
    }

    #[tokio::test]
    async fn test_server_replies_to_hello_handshake() {
        let router = PartitionRouter::new(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test port");
        let addr = listener.local_addr().expect("local addr");

        let server_router = router.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let _ = handle_client(&mut socket, &server_router, None).await;
        });

        let frame = protocol::encode_reply(1, 0, &bson::doc! { "hello": 1.0 }).expect("encode");
        let (reply, header) = send_and_read_reply(addr, frame).await;

        assert_eq!(header.response_to, 1, "reply must echo requestId");
        assert_eq!(reply.command.get_f64("ok").expect("ok"), 1.0);
        assert_eq!(
            reply
                .command
                .get_bool("isWritablePrimary")
                .expect("primary"),
            true
        );
        assert!(reply.command.get_bool("helloOk").expect("helloOk"));
    }

    #[tokio::test]
    async fn test_server_answers_legacy_op_query_handshake() {
        let router = PartitionRouter::new(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test port");
        let addr = listener.local_addr().expect("local addr");

        let server_router = router.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let _ = handle_client(&mut socket, &server_router, None).await;
        });

        // Legacy OP_QUERY body, exactly as a real driver sends it.
        let mut body = 0u32.to_le_bytes().to_vec(); // flagBits
        body.extend_from_slice(b"admin.$cmd\0");
        body.extend_from_slice(&0i32.to_le_bytes()); // numberToSkip
        body.extend_from_slice(&(-1i32).to_le_bytes()); // numberToReturn
        body.extend(bson::to_vec(&bson::doc! { "isMaster": 1i32 }).expect("bson"));

        // Build the full frame: little-endian 16-byte header + body.
        let message_length = (HEADER_SIZE + body.len()) as u32;
        let mut frame = Vec::from(message_length.to_le_bytes());
        frame.extend_from_slice(&1i32.to_le_bytes()); // requestId
        frame.extend_from_slice(&0i32.to_le_bytes()); // responseTo
        frame.extend_from_slice(&wire::OP_QUERY.to_le_bytes());
        frame.extend_from_slice(&body);

        let mut client = TcpStream::connect(addr).await.expect("connect");
        client.write_all(&frame).await.expect("write request");

        let mut header_buf = [0u8; HEADER_SIZE];
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read_exact(&mut header_buf),
        )
        .await
        .expect("reply timed out")
        .expect("reply header read");

        let reply_header = MessageHeader::parse(&header_buf);
        assert_eq!(
            reply_header.op_code,
            protocol::OP_REPLY,
            "OP_QUERY must be answered with OP_REPLY"
        );
        assert_eq!(reply_header.response_to, 1, "reply must echo requestId");
        assert!(reply_header.message_length > HEADER_SIZE as u32);

        // OP_REPLY body: flags(4) + cursorId(8) + startingFrom(4) +
        // numberReturned(4) + documents.
        let body_len = reply_header.message_length as usize - HEADER_SIZE;
        let mut body_buf = vec![0u8; body_len];
        client.read_exact(&mut body_buf).await.expect("reply body");

        let number_returned = i32::from_le_bytes(body_buf[16..20].try_into().unwrap());
        assert_eq!(number_returned, 1);

        let doc: bson::Document = bson::from_slice(&body_buf[20..]).expect("decode reply doc");
        assert_eq!(doc.get_f64("ok").expect("ok"), 1.0);
        assert!(doc.get_bool("ismaster").expect("ismaster"));
    }

    #[tokio::test]
    async fn test_server_searches_with_text_operator() {
        let router = PartitionRouter::new(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test port");
        let addr = listener.local_addr().expect("local addr");

        // This test opens one connection per request, so the server
        // must keep accepting (the single-accept pattern used by the
        // other tests would reset the second connection).
        let server_router = router.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let client_router = server_router.clone();
                tokio::spawn(async move {
                    let _ = handle_client(&mut socket, &client_router, None).await;
                });
            }
        });

        // Seed two documents over the wire.
        let command = bson::doc! {
            "insert": "articles",
            "documents": [
                { "_id": "a", "title": "Rust database internals" },
                { "_id": "b", "title": "Sourdough baking basics" },
            ],
        };
        let frame = protocol::encode_reply(1, 0, &command).expect("encode");
        let _ = send_and_read_reply(addr, frame).await;

        // $text must route to the search index and hit only "a".
        let search = bson::doc! {
            "find": "articles",
            "filter": { "$text": { "$search": "database" } },
        };
        let frame = protocol::encode_reply(2, 0, &search).expect("encode");
        let (reply, _) = send_and_read_reply(addr, frame).await;

        let cursor = reply.command.get_document("cursor").expect("cursor");
        let batch = cursor.get_array("firstBatch").expect("firstBatch");
        assert_eq!(
            batch.len(),
            1,
            "only the rust doc must match: {:?}",
            reply.command
        );
        let hit = batch[0].as_document().expect("document");
        assert_eq!(hit.get_str("_id").unwrap(), "a");
    }

    #[tokio::test]
    async fn test_server_reports_unknown_command() {
        let router = PartitionRouter::new(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind test port");
        let addr = listener.local_addr().expect("local addr");

        let server_router = router.clone();
        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let _ = handle_client(&mut socket, &server_router, None).await;
        });

        let command = bson::doc! { "reIndex": "users" };
        let frame = protocol::encode_reply(1, 0, &command).expect("encode");

        let mut client = TcpStream::connect(addr).await.expect("connect");
        client.write_all(&frame).await.expect("write request");

        let mut header_buf = [0u8; HEADER_SIZE];
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            client.read_exact(&mut header_buf),
        )
        .await
        .expect("reply timed out")
        .expect("reply header read");

        let reply_header = MessageHeader::parse(&header_buf);
        let body_len = reply_header.message_length as usize - HEADER_SIZE;
        let mut body = vec![0u8; body_len];
        client.read_exact(&mut body).await.expect("reply body");

        let msg = protocol::decode_op_msg(&body).expect("decode reply");
        assert_eq!(msg.command.get_f64("ok").expect("ok field"), 0.0);
        assert!(
            msg.command
                .get_str("errmsg")
                .expect("errmsg")
                .contains("reIndex"),
            "error must name the command: {:?}",
            msg.command
        );
    }

    #[tokio::test]
    async fn test_sql_reads_the_catalog_and_drop_removes_it() {
        let router = PartitionRouter::new(4);
        let local = "127.0.0.1:1".parse().expect("addr");
        for (db, collection) in [("t", "c"), ("t", "d"), ("novo_banco", "item")] {
            let inserted = execute_command(
                &bson::doc! {
                    "insert": collection,
                    "$db": db,
                    "documents": [ { "p": 1i32 } ],
                },
                &router,
                local,
            )
            .await;
            assert_eq!(inserted.get_f64("ok"), Ok(1.0), "{inserted:?}");
        }

        let described = execute_command(
            &bson::doc! { "sql": "DESCRIBE c", "$db": "t" },
            &router,
            local,
        )
        .await;
        let columns = sql_batch(&described);
        assert!(
            columns
                .iter()
                .any(|row| row.get_str("column_name").ok() == Some("p")),
            "{columns:?}"
        );

        let listed = execute_command(
            &bson::doc! { "sql": "SELECT table_name FROM information_schema.tables", "$db": "t" },
            &router,
            local,
        )
        .await;
        let tables = sql_batch(&listed);
        let table_names: Vec<_> = tables
            .iter()
            .filter_map(|row| row.get_str("table_name").ok())
            .collect();
        assert!(
            table_names.contains(&"c") && table_names.contains(&"d"),
            "{table_names:?}"
        );
        assert!(!table_names.contains(&"item"), "{table_names:?}");

        let fields = execute_command(
            &bson::doc! { "sql": "SELECT database, collection, field FROM fields", "$db": "admin" },
            &router,
            local,
        )
        .await;
        let field_rows = sql_batch(&fields);
        assert!(
            field_rows
                .iter()
                .any(|row| row.get_str("collection").ok() == Some("c")),
            "{field_rows:?}"
        );
        assert!(
            field_rows
                .iter()
                .any(|row| row.get_str("collection").ok() == Some("d")),
            "{field_rows:?}"
        );

        let dropped = execute_command(
            &bson::doc! { "dropDatabase": "novo_banco", "$db": "admin" },
            &router,
            local,
        )
        .await;
        assert_eq!(dropped.get_f64("ok"), Ok(1.0), "{dropped:?}");
        assert_eq!(dropped.get_str("dropped"), Ok("novo_banco"));

        let databases = execute_command(
            &bson::doc! { "find": "databases", "filter": {}, "$db": "admin" },
            &router,
            local,
        )
        .await;
        let database_rows = sql_batch(&databases);
        let names: Vec<_> = database_rows
            .iter()
            .filter_map(|row| row.get_str("name").ok())
            .collect();
        assert!(!names.contains(&"novo_banco"), "{names:?}");
        assert!(names.contains(&"t"), "{names:?}");
    }

    fn sql_batch(reply: &bson::Document) -> Vec<bson::Document> {
        reply
            .get_document("cursor")
            .and_then(|cursor| cursor.get_array("firstBatch"))
            .map(|batch| {
                batch
                    .iter()
                    .filter_map(|value| value.as_document().cloned())
                    .collect()
            })
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn test_catalog_page_is_a_slice_of_the_merged_fields() {
        let router = PartitionRouter::new(4);
        let local = "127.0.0.1:1".parse().expect("addr");
        // `t.c` and `t.d` hash to different partitions. Each document
        // contributes three field rows (`_id`, `p`, `q`).
        for collection in ["c", "d"] {
            let inserted = execute_command(
                &bson::doc! {
                    "insert": collection,
                    "$db": "t",
                    "documents": [ { "p": 1i32, "q": 2i32 } ],
                },
                &router,
                local,
            )
            .await;
            assert_eq!(inserted.get_f64("ok"), Ok(1.0), "{inserted:?}");
        }

        let page = execute_command(
            &bson::doc! {
                "find": "fields",
                "$db": "admin",
                "skip": 2i32,
                "limit": 2i32,
            },
            &router,
            local,
        )
        .await;
        let batch = page
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("batch");
        let ids: Vec<_> = batch
            .iter()
            .filter_map(|value| value.as_document()?.get_str("_id").ok())
            .collect();
        assert_eq!(ids, vec!["t.c.q", "t.d._id"], "{ids:?}");

        let counted = execute_command(
            &bson::doc! {
                "aggregate": "fields",
                "$db": "admin",
                "pipeline": [ { "$count": "n" } ],
                "cursor": {},
            },
            &router,
            local,
        )
        .await;
        let n = counted
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("batch")[0]
            .as_document()
            .expect("row")
            .get_i32("n")
            .expect("n");
        assert_eq!(n, 6, "{counted:?}");
    }

    #[tokio::test]
    async fn test_list_databases_merges_partitions_and_filters() {
        let router = PartitionRouter::new(4);

        // "shop" gets a document and "empty" an empty collection;
        // the two namespaces hash to different partitions.
        router
            .dispatch(PartitionCommand::Insert {
                collection: "shop.orders".into(),
                documents: vec![bson::doc! { "n": 1 }],
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");
        router
            .dispatch(PartitionCommand::CreateCollection {
                collection: "empty.logs".into(),
                autoincrement: Vec::new(),
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

        let listed = broadcast_catalog(
            &router,
            PartitionCommand::find("admin.databases", bson::doc! {}),
        )
        .await
        .expect("catalog");
        let PartitionResult::Found { documents } = listed else {
            panic!("expected databases, got {listed:?}");
        };
        let names: Vec<_> = documents
            .iter()
            .filter_map(|document| document.get_str("name").ok().map(str::to_string))
            .collect();
        let unique = {
            let mut copy = names.clone();
            copy.sort();
            copy.dedup();
            copy
        };
        assert_eq!(names.len(), unique.len(), "duplicated databases: {names:?}");
        assert!(names.iter().any(|name| name == "admin"), "{names:?}");
        assert!(names.iter().any(|name| name == "shop"), "{names:?}");
        assert!(names.iter().any(|name| name == "empty"), "{names:?}");

        let command = bson::doc! { "listDatabases": 1 };
        let reply = list_databases(&command, &router).await;
        assert_eq!(reply.get_f64("ok"), Ok(1.0));

        let databases = reply.get_array("databases").expect("databases");
        let entry = |name: &str| {
            databases
                .iter()
                .filter_map(|value| value.as_document())
                .find(|db| db.get_str("name") == Ok(name))
                .unwrap_or_else(|| panic!("{} must be listed", name))
        };
        assert!(!entry("shop").get_bool("empty").unwrap());
        assert!(entry("empty").get_bool("empty").unwrap());

        // The optional filter narrows the merged list.
        let filtered = list_databases(
            &bson::doc! { "listDatabases": 1, "filter": { "name": "shop" } },
            &router,
        )
        .await;
        let databases = filtered.get_array("databases").expect("databases");
        assert_eq!(databases.len(), 1);
        assert_eq!(
            databases[0].as_document().unwrap().get_str("name"),
            Ok("shop")
        );
    }

    #[tokio::test]
    async fn test_server_status_counts_an_insert_an_index_find_and_a_scan() {
        let router = PartitionRouter::new(1);
        let local = "127.0.0.1:9".parse().unwrap();
        let before = execute_command(&bson::doc! { "serverStatus": 1 }, &router, local).await;
        let read = |doc: &bson::Document, key: &str| {
            doc.get_i64(key)
                .unwrap_or_else(|_| panic!("{key} missing from serverStatus"))
        };

        let inserted = execute_command(
            &bson::doc! {
                "insert": "emp",
                "$db": "db",
                "documents": [
                    { "_id": "1", "name": "SMITH", "sal": 800 },
                    { "_id": "2", "name": "KING", "sal": 5000 },
                ],
            },
            &router,
            local,
        )
        .await;
        assert_eq!(inserted.get_f64("ok"), Ok(1.0), "{inserted}");

        let created = execute_command(
            &bson::doc! {
                "createIndexes": "emp",
                "$db": "db",
                "indexes": [ { "name": "name_1", "key": { "name": 1 } } ],
            },
            &router,
            local,
        )
        .await;
        assert_eq!(created.get_f64("ok"), Ok(1.0), "{created}");

        let by_name = execute_command(
            &bson::doc! {
                "find": "emp",
                "$db": "db",
                "filter": { "name": "SMITH" },
            },
            &router,
            local,
        )
        .await;
        let indexed = by_name
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("batch");
        assert_eq!(indexed.len(), 1);

        let scanned = execute_command(
            &bson::doc! {
                "find": "emp",
                "$db": "db",
                "filter": { "sal": { "$gt": 0 } },
            },
            &router,
            local,
        )
        .await;
        let batch = scanned
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("batch");
        assert_eq!(batch.len(), 2);

        let status = execute_command(&bson::doc! { "serverStatus": 1 }, &router, local).await;
        assert_eq!(status.get_f64("ok"), Ok(1.0), "{status}");
        assert!(status.get_i64("uptime").is_ok());
        for key in [
            "queries",
            "inserts",
            "updates",
            "deletes",
            "connectionsActive",
            "connectionsTotal",
            "bytesIn",
            "bytesOut",
            "walWrites",
            "bufferPoolHits",
            "bufferPoolMisses",
            "bufferPoolEvictions",
            "indexLookups",
            "collectionScans",
            "transactionsStarted",
            "transactionsCommitted",
            "transactionsAborted",
        ] {
            assert!(status.get_i64(key).is_ok(), "{key}");
        }
        assert!(read(&status, "inserts") >= read(&before, "inserts") + 1);
        assert!(read(&status, "queries") >= read(&before, "queries") + 2);
        assert!(read(&status, "indexLookups") >= read(&before, "indexLookups") + 1);
        assert!(read(&status, "collectionScans") >= read(&before, "collectionScans") + 1);
        assert!(read(&status, "walWrites") >= read(&before, "walWrites") + 1);
    }

    /// Reads one OP_MSG reply that is already on `socket`.
    async fn read_op_msg(socket: &mut TcpStream) -> bson::Document {
        let mut header_buf = [0u8; HEADER_SIZE];
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            socket.read_exact(&mut header_buf),
        )
        .await
        .expect("reply timed out")
        .expect("reply header");
        let header = MessageHeader::parse(&header_buf);
        assert_eq!(header.op_code, OP_MSG, "reply must be OP_MSG");
        let body_len = header.message_length as usize - HEADER_SIZE;
        let mut body = vec![0u8; body_len];
        socket.read_exact(&mut body).await.expect("reply body");
        protocol::decode_op_msg(&body)
            .expect("decode reply")
            .command
    }

    async fn wait_until(mut ready: impl FnMut() -> bool) {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !ready() {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("timed out waiting");
    }

    #[tokio::test]
    async fn a_connection_past_the_cap_is_refused_and_closing_one_frees_the_slot() {
        // Fails if the listener admits the extra socket, counts it, or
        // keeps the slot after the held client disconnects.
        let table = ConnTable::limited(1, 0);
        let router = PartitionRouter::new(1);
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server_table = Arc::clone(&table);
        tokio::spawn(async move {
            let _ = run_listener(listener, router, None, None, server_table).await;
        });

        let mut held = TcpStream::connect(addr).await.expect("held connect");
        let ping = protocol::encode_reply(1, 0, &bson::doc! { "ping": 1 }).expect("ping");
        held.write_all(&ping).await.expect("ping write");
        let admitted = read_op_msg(&mut held).await;
        assert_eq!(admitted.get_f64("ok"), Ok(1.0), "{admitted}");
        assert_eq!(table.len(), 1);
        assert_eq!(
            table.only_peer(),
            Some(held.local_addr().expect("held addr"))
        );

        let mut extra = TcpStream::connect(addr).await.expect("extra connect");
        let refused = read_op_msg(&mut extra).await;
        assert_eq!(refused.get_f64("ok"), Ok(0.0), "{refused}");
        assert!(
            refused
                .get_str("errmsg")
                .unwrap_or("")
                .contains("too many connections"),
            "{refused}"
        );
        assert_eq!(
            table.len(),
            1,
            "a refused socket is not an active connection"
        );
        let mut tail = [0u8; 1];
        let closed = tokio::time::timeout(std::time::Duration::from_secs(2), extra.read(&mut tail))
            .await
            .expect("refused socket should close")
            .expect("read");
        assert_eq!(closed, 0);

        drop(held);
        wait_until(|| table.len() == 0).await;

        let mut again = TcpStream::connect(addr).await.expect("again");
        let ping = protocol::encode_reply(2, 0, &bson::doc! { "ping": 1 }).expect("ping");
        again.write_all(&ping).await.expect("ping write");
        let admitted = read_op_msg(&mut again).await;
        assert_eq!(admitted.get_f64("ok"), Ok(1.0), "{admitted}");
        assert_eq!(table.len(), 1);
    }

    #[tokio::test]
    async fn an_idle_connection_is_closed_and_frees_its_slot() {
        // Fails if a client that never sends a command stays open.
        let table = ConnTable::limited(4, 80);
        let addr = spawn_listener(table.clone()).await;
        let mut client = TcpStream::connect(addr).await.expect("connect");
        let mut buf = [0u8; 1];
        let closed =
            tokio::time::timeout(std::time::Duration::from_secs(2), client.read(&mut buf)).await;
        assert!(
            matches!(closed, Ok(Ok(0))),
            "idle connection should be closed, got {closed:?}"
        );
        wait_until(|| table.len() == 0).await;

        let mut again = TcpStream::connect(addr).await.expect("again");
        let ping = protocol::encode_reply(1, 0, &bson::doc! { "ping": 1 }).expect("ping");
        again.write_all(&ping).await.expect("write");
        let admitted = read_op_msg(&mut again).await;
        assert_eq!(admitted.get_f64("ok"), Ok(1.0), "{admitted}");
    }

    #[tokio::test]
    async fn a_command_postpones_idle_close_then_frees_the_slot() {
        // Fails if the idle clock starts at accept and ignores later commands.
        let table = ConnTable::limited(4, 300);
        let addr = spawn_listener(table.clone()).await;
        let mut client = TcpStream::connect(addr).await.expect("connect");
        for request_id in 1..=3 {
            let frame =
                protocol::encode_reply(request_id, 0, &bson::doc! { "ping": 1 }).expect("ping");
            client.write_all(&frame).await.expect("write");
            let reply = read_op_msg(&mut client).await;
            assert_eq!(reply.get_f64("ok"), Ok(1.0), "{reply}");
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        }
        let mut buf = [0u8; 1];
        let closed =
            tokio::time::timeout(std::time::Duration::from_secs(2), client.read(&mut buf)).await;
        assert!(
            matches!(closed, Ok(Ok(0))),
            "connection should close after commands stop, got {closed:?}"
        );
    }

    #[tokio::test]
    async fn shutdown_closes_live_connections_and_frees_their_slots() {
        let table = ConnTable::limited(4, 0);
        let addr = spawn_listener(table.clone()).await;
        let mut client = TcpStream::connect(addr).await.expect("connect");
        let ping = protocol::encode_reply(1, 0, &bson::doc! { "ping": 1 }).expect("ping");
        client.write_all(&ping).await.expect("write");
        let reply = read_op_msg(&mut client).await;
        assert_eq!(reply.get_f64("ok"), Ok(1.0), "{reply}");
        wait_until(|| table.len() == 1).await;

        table.shutdown();
        let mut buf = [0u8; 1];
        let closed =
            tokio::time::timeout(std::time::Duration::from_secs(2), client.read(&mut buf)).await;
        assert!(
            matches!(closed, Ok(Ok(0))),
            "shutdown should close the socket, got {closed:?}"
        );
        wait_until(|| table.len() == 0).await;
    }

    #[tokio::test]
    async fn closing_a_disabled_user_frees_that_connection() {
        let table = ConnTable::limited(4, 0);
        let addr = spawn_listener(table.clone()).await;
        let mut client = TcpStream::connect(addr).await.expect("connect");
        let ping = protocol::encode_reply(1, 0, &bson::doc! { "ping": 1 }).expect("ping");
        client.write_all(&ping).await.expect("write");
        let reply = read_op_msg(&mut client).await;
        assert_eq!(reply.get_f64("ok"), Ok(1.0), "{reply}");
        wait_until(|| table.len() == 1).await;

        let id = table.only_id().expect("the live connection");
        table.set_user(id, "ada".to_string());
        table.close_user("ada");

        let mut buf = [0u8; 1];
        let closed =
            tokio::time::timeout(std::time::Duration::from_secs(2), client.read(&mut buf)).await;
        assert!(
            matches!(closed, Ok(Ok(0))),
            "disabling the user should close the socket, got {closed:?}"
        );
        wait_until(|| table.len() == 0).await;
    }

    #[tokio::test]
    async fn map_reduce_returns_the_projection_and_writes_no_collection() {
        // Fails if the command drops a match, keeps a non-match, or creates a collection.
        let router = PartitionRouter::new(1);
        let local = "127.0.0.1:1".parse().unwrap();
        let inserted = execute_command(
            &bson::doc! {
                "insert": "emp",
                "$db": "db",
                "documents": [
                    { "_id": "1", "job": "CLERK", "sal": 800i32 },
                    { "_id": "2", "job": "CLERK", "sal": 1100i32 },
                    { "_id": "3", "job": "PRESIDENT", "sal": 5000i32 },
                ],
            },
            &router,
            local,
        )
        .await;
        assert_eq!(inserted.get_f64("ok"), Ok(1.0), "{inserted}");

        let reply = execute_command(
            &bson::doc! {
                "mapReduce": "emp",
                "$db": "db",
                "query": { "sal": { "$gt": 1000i32 } },
                "map": { "key": "$job", "value": "$sal" },
            },
            &router,
            local,
        )
        .await;
        assert_eq!(reply.get_f64("ok"), Ok(1.0), "{reply}");
        let batch = reply
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("batch");
        let pairs: Vec<(String, i32)> = batch
            .iter()
            .map(|row| {
                let doc = row.as_document().expect("pair");
                let key = doc.get_str("key").expect("key").to_string();
                let value = doc.get_i32("value").expect("value");
                (key, value)
            })
            .collect();
        assert_eq!(
            pairs,
            vec![("CLERK".to_string(), 1100), ("PRESIDENT".to_string(), 5000)]
        );

        let listed = execute_command(
            &bson::doc! { "listCollections": 1i32, "$db": "db" },
            &router,
            local,
        )
        .await;
        let names: Vec<String> = listed
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("batch")
            .iter()
            .filter_map(|row| row.as_document()?.get_str("name").ok().map(str::to_string))
            .collect();
        assert_eq!(names, vec!["emp".to_string()], "{listed}");
    }

    #[tokio::test]
    async fn map_reduce_names_what_it_refuses() {
        let router = PartitionRouter::new(1);
        let local = "127.0.0.1:1".parse().unwrap();
        let cases = [
            (
                bson::doc! {
                    "mapReduce": "emp",
                    "$db": "db",
                    "map": "function() { emit(this.job, this.sal); }",
                },
                "JavaScript",
            ),
            (
                bson::doc! {
                    "mapReduce": "emp",
                    "$db": "db",
                    "map": { "key": "$job", "value": "$sal" },
                    "reduce": { "_id": "$key", "n": { "$sum": 1i32 } },
                },
                "reduce",
            ),
            (
                bson::doc! {
                    "mapReduce": "emp",
                    "$db": "db",
                    "map": { "key": "$job", "value": "$sal" },
                    "out": "totals",
                },
                "out",
            ),
            (
                bson::doc! {
                    "mapReduce": "emp",
                    "$db": "db",
                    "map": { "key": "$job", "value": "$sal" },
                    "txnId": 1i32,
                },
                "transaction",
            ),
        ];
        for (command, named) in cases {
            let reply = execute_command(&command, &router, local).await;
            assert_eq!(reply.get_f64("ok"), Ok(0.0), "{reply}");
            let message = reply.get_str("errmsg").unwrap_or("");
            assert!(
                message.contains(named),
                "refusal should name {named}: {message}"
            );
        }
    }

    async fn spawn_listener(table: Arc<ConnTable>) -> std::net::SocketAddr {
        let router = PartitionRouter::new(1);
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            let _ = run_listener(listener, router, None, None, table).await;
        });
        addr
    }
}
