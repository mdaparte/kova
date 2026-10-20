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
use crate::types::{PartitionCommand, PartitionResult};

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
    user: Mutex<Option<String>>,
    close: watch::Sender<bool>,
}

struct ConnTable {
    next: AtomicU64,
    slots: Mutex<HashMap<u64, LiveConn>>,
}

fn connections() -> &'static ConnTable {
    static TABLE: OnceLock<ConnTable> = OnceLock::new();
    TABLE.get_or_init(|| ConnTable {
        next: AtomicU64::new(1),
        slots: Mutex::new(HashMap::new()),
    })
}

impl ConnTable {
    fn open(&self) -> (u64, watch::Receiver<bool>) {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (close, watch_rx) = watch::channel(false);
        self.slots.lock().expect("connections").insert(
            id,
            LiveConn {
                user: Mutex::new(None),
                close,
            },
        );
        crate::metrics::METRICS.connection_opened();
        (id, watch_rx)
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
        if self
            .slots
            .lock()
            .expect("connections")
            .remove(&id)
            .is_some()
        {
            crate::metrics::METRICS.connection_closed();
        }
    }
}

struct Session {
    user: String,
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
pub async fn start_server(
    router: PartitionRouter,
    replica: Option<Arc<crate::replication::Replica>>,
) -> std::io::Result<()> {
    let port = resolve_port();
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    LOGGER.info(format!("Network server started on port {}", port));

    loop {
        let (mut socket, addr) = listener.accept().await?;
        LOGGER.info(format!("Connection received from {}", addr));

        // Each client gets its own task; `Arc` lets every task share
        // the same router without cloning worker channels individually.
        let client_router = router.clone();
        let client_replica = replica.clone();
        spawn(async move {
            if let Err(err) =
                handle_client(&mut socket, &client_router, client_replica.as_deref()).await
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

/// Reads OP_MSG frames from one client until it disconnects.
///
/// Each iteration reads the 16-byte header, then the remaining
/// `messageLength - 16` bytes, decodes the OP_MSG body, executes the
/// command through the router and writes the reply.
async fn handle_client(
    socket: &mut TcpStream,
    router: &PartitionRouter,
    replica: Option<&crate::replication::Replica>,
) -> std::io::Result<()> {
    let (conn_id, mut close) = connections().open();
    let mut session = None;
    let outcome = handle_frames(socket, router, replica, conn_id, &mut close, &mut session).await;
    connections().close(conn_id);
    outcome
}

async fn handle_frames(
    socket: &mut TcpStream,
    router: &PartitionRouter,
    replica: Option<&crate::replication::Replica>,
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
                    &header, &body_buf, router, replica, socket, conn_id, session,
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
                    &header, &body_buf, router, replica, socket, conn_id, session,
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
    socket: &mut TcpStream,
    conn_id: u64,
    session: &mut Option<Session>,
) -> Result<(), String> {
    let msg = protocol::decode_op_msg(body)?;
    let local_addr = socket
        .local_addr()
        .map_err(|err| format!("failed to read local address: {}", err))?;
    let reply_body =
        execute_replicated(&msg.command, router, local_addr, replica, conn_id, session).await;

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
}

/// Auto-commit mutations travel through Raft. Reads stay on this node.
/// Transactions stay on the primary: a commit replays writes that exist
/// only in that worker, so the command itself is not an oplog entry.
fn is_replicated_write(command: &bson::Document) -> bool {
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
    command.contains_key("txnId")
        || command.contains_key("beginTransaction")
        || command.contains_key("commitTransaction")
        || command.contains_key("abortTransaction")
}

async fn execute_replicated(
    command: &bson::Document,
    router: &PartitionRouter,
    local_addr: std::net::SocketAddr,
    replica: Option<&crate::replication::Replica>,
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
    let mut reply = dispatch_command(&command, router, local_addr, replica).await;
    bind_session(&command, &mut reply, conn_id, session);
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
            connections().set_user(conn_id, user.clone());
            *session = Some(Session { user, actions });
        }
    }
    if let Ok(user) = reply.get_str("kovaDisabledUser").map(str::to_string) {
        reply.remove("kovaDisabledUser");
        connections().close_user(&user);
    }
}

async fn dispatch_command(
    command: &bson::Document,
    router: &PartitionRouter,
    local_addr: std::net::SocketAddr,
    replica: Option<&crate::replication::Replica>,
) -> bson::Document {
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
    if is_replicated_write(command) {
        return replica.propose(command.clone()).await.0;
    }
    execute_command(command, router, local_addr).await
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
        // A query with no FROM (`SELECT 1 AS n`) has no collection to load.
        // A query that names one collection loads that snapshot and registers
        // it under the spelling used in the SQL text. A single comparison in
        // WHERE is sent as a find, so a secondary index can narrow the pages
        // before DataFusion applies the same predicate again.
        let pushed = sql_simple_filter(sql_text);
        let prepared = match sql_table_name(sql_text) {
            Some(table_name) => {
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
                            match load_collection_documents(router, &collection, &bson::doc! {})
                                .await
                            {
                                Ok(documents) => documents,
                                Err(message) => {
                                    return bson::doc! { "ok": 0.0, "errmsg": message };
                                }
                            }
                        } else {
                            documents
                        };
                        Some((table_name, collection, documents))
                    }
                    Err(message) => {
                        return bson::doc! { "ok": 0.0, "errmsg": message };
                    }
                }
            }
            None => None,
        };
        let tables: Vec<(&str, std::sync::Arc<Vec<bson::Document>>)> = match &prepared {
            Some((name, _, documents)) => vec![(name.as_str(), std::sync::Arc::clone(documents))],
            None => Vec::new(),
        };
        let mut result = match query_engine::execute_sql_query(sql_text, &tables).await {
            Ok(result) => result,
            Err(err) => return bson::doc! { "ok": 0.0, "errmsg": err.to_string() },
        };
        if prepared.is_none() {
            crate::metrics::METRICS.query();
        }
        if let (Some(filter), Some((_, collection, _))) = (pushed, prepared) {
            if let Some((stage, index_name)) = index_plan(router, &collection, filter).await {
                let mut plan = bson::doc! { "stage": stage };
                if let Some(name) = index_name {
                    plan.insert("indexName", name);
                }
                result.insert(
                    "queryPlanner",
                    bson::doc! {
                        "plannerVersion": 1i32,
                        "namespace": collection,
                        "winningPlan": plan,
                    },
                );
            }
        }
        return result;
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
        Ok(aggregate) if aggregate_command(&aggregate) => {
            execute_aggregate(router, aggregate).await
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

/// Extracts a table/collection name from a simple SQL FROM clause.
///
/// The keyword match is case-insensitive, but the identifier is returned
/// with its original spelling. MongoDB collection names are case-sensitive,
/// so uppercasing `users` into `USERS` loads an empty collection: `SELECT *`
/// comes back with no rows, and `WHERE age > 25` fails with "No field named
/// age" because the empty snapshot has no columns.
///
/// This is intentionally minimal for Phase 5. It does not handle joins,
/// subqueries, or schema-qualified names.
fn sql_table_name(sql: &str) -> Option<String> {
    let bytes = sql.as_bytes();
    let from_pos = (0..bytes.len().saturating_sub(3)).find(|&index| {
        let word = &bytes[index..index + 4];
        word.eq_ignore_ascii_case(b"from")
            && (index == 0 || !bytes[index - 1].is_ascii_alphanumeric())
            && (index + 4 >= bytes.len() || !bytes[index + 4].is_ascii_alphanumeric())
    })?;

    let after_from = sql.get(from_pos + 4..)?;
    let trimmed = after_from.trim_start();
    let end = trimmed
        .find(|c: char| c.is_whitespace() || c == '(' || c == ',' || c == ';')
        .unwrap_or(trimmed.len());
    let name = trimmed[..end]
        .trim()
        .trim_matches(|c| c == '"' || c == '`' || c == '\'');
    if name.is_empty() {
        None
    } else {
        Some(name.to_string())
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
    } else if let Some(rest) = rest.strip_prefix("<>") {
        return None;
    } else if let Some(rest) = rest.strip_prefix("!=") {
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

/// Broadcasts `listCollections` to every partition and merges the
/// collection descriptions into a single cursor reply.
///
/// The reply namespace follows MongoDB's own format,
/// `<db>.$cmd.listCollections`.
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
        assert!(!is_replicated_write(
            &bson::doc! { "insert": "emp", "txnId": 1i64 }
        ));
        assert!(!is_replicated_write(
            &bson::doc! { "aggregate": "emp", "pipeline": [ { "$match": {} } ] }
        ));
        assert!(is_leader_local(&bson::doc! { "beginTransaction": "emp" }));
        assert!(is_leader_local(
            &bson::doc! { "commitTransaction": 1i32, "txnId": 1i64 }
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
            })
            .expect("worker reachable")
            .await
            .expect("worker replied");

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
}
