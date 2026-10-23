//! unixODBC driver for Kova SQL.
//!
//! `cargo build` writes `target/debug/libkova.so`. unixODBC loads that
//! file. The connection string is
//! `DRIVER=Kova;SERVER=host;PORT=27018;DATABASE=name`. `SERVER` is the
//! host, `PORT` defaults to 27018, and `DATABASE` is the `$db` of the
//! command. `SQLConnect` also accepts `host`, `host:port`, and
//! `host:port/database`.
//!
//! `SQLExecDirect` sends one `{ sql, $db }` command on the MongoDB port
//! and reads the cursor batch back as rows. The SQL runs through the
//! same DataFusion path as `db.runCommand({ sql })`. Column names are
//! the fields of those rows. An empty batch has no columns.
//!
//! `UID` and `PWD` (or the user and password of `SQLConnect`) send
//! `{ authenticate, user, pwd }` on that socket before any SQL. The
//! server binds the session the same way a MongoDB client does. A
//! connection with neither value stays open, which is the server with
//! security left off. A connection with only one of the two fails.
//!
//! This is the Linux driver (LP64, `SQLLEN` is 64 bits). The Windows
//! driver manager is a later platform.

#![allow(non_snake_case, clippy::missing_safety_doc, clippy::too_many_arguments)]

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Mutex;
use std::time::Duration;

use bson::Document;

const SQL_SUCCESS: i16 = 0;
const SQL_SUCCESS_WITH_INFO: i16 = 1;
const SQL_ERROR: i16 = -1;
const SQL_INVALID_HANDLE: i16 = -2;
const SQL_NO_DATA: i16 = 100;

const SQL_NULL_DATA: i64 = -1;
const SQL_NTS: i32 = -3;

const SQL_HANDLE_ENV: i16 = 1;
const SQL_HANDLE_DBC: i16 = 2;
const SQL_HANDLE_STMT: i16 = 3;

const SQL_ATTR_ODBC_VERSION: i32 = 200;
const SQL_ATTR_AUTOCOMMIT: i32 = 102;

const SQL_CLOSE: i16 = 0;
const SQL_DROP: i16 = 1;
const SQL_UNBIND: i16 = 2;
const SQL_RESET_PARAMS: i16 = 3;

const SQL_INTEGER: i16 = 4;
const SQL_VARCHAR: i16 = 12;
const SQL_BIGINT: i16 = -5;
const SQL_DOUBLE: i16 = 8;
const SQL_BIT: i16 = -7;

const SQL_C_CHAR: i16 = 1;
const SQL_C_LONG: i16 = 4;
const SQL_C_SBIGINT: i16 = -5;
const SQL_C_DOUBLE: i16 = 8;
const SQL_C_BIT: i16 = -7;

const SQL_NULLABLE: i16 = 1;

const SQL_DRIVER_NAME: u16 = 6;
const SQL_DRIVER_VER: u16 = 7;
const SQL_ODBC_VER: u16 = 10;
const SQL_DBMS_NAME: u16 = 17;
const SQL_DBMS_VER: u16 = 18;
const SQL_IDENTIFIER_QUOTE_CHAR: u16 = 29;
const SQL_DRIVER_ODBC_VER: u16 = 77;

const IO_TIMEOUT: Duration = Duration::from_secs(10);

struct Diag {
    state: &'static str,
    message: String,
}

impl Diag {
    fn clear() -> Self {
        Self {
            state: "00000",
            message: String::new(),
        }
    }

    fn new(state: &'static str, message: impl Into<String>) -> Self {
        Self {
            state,
            message: message.into(),
        }
    }
}

struct Env {
    diag: Diag,
}

struct Conn {
    env: u64,
    database: String,
    socket: Option<TcpStream>,
    next_request: i32,
    diag: Diag,
}

struct Stmt {
    conn: u64,
    columns: Vec<Column>,
    rows: Vec<Vec<Cell>>,
    next: usize,
    current: Option<usize>,
    executed: bool,
    closed: bool,
    prepared: Option<String>,
    bindings: Vec<Option<Binding>>,
    diag: Diag,
}

struct Column {
    name: String,
    kind: i16,
    size: u64,
}

#[derive(Clone)]
enum Cell {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
}

struct Binding {
    target_type: i16,
    target: usize,
    buffer_len: i64,
    indicator: usize,
}

enum Written {
    Ok,
    Truncated,
}

enum Fail {
    Server(String),
    Reply(String),
}

#[derive(Clone)]
struct Target {
    host: String,
    port: u16,
    database: String,
    user: Option<String>,
    password: Option<String>,
}

struct Registry {
    envs: BTreeMap<u64, Env>,
    conns: BTreeMap<u64, Conn>,
    stmts: BTreeMap<u64, Stmt>,
    next: u64,
}

impl Registry {
    fn alloc(&mut self) -> u64 {
        let id = self.next;
        self.next = self.next.wrapping_add(1).max(1);
        id
    }
}

fn registry() -> std::sync::MutexGuard<'static, Registry> {
    REGISTRY.lock().unwrap_or_else(|err| err.into_inner())
}

static REGISTRY: Mutex<Registry> = Mutex::new(Registry {
    envs: BTreeMap::new(),
    conns: BTreeMap::new(),
    stmts: BTreeMap::new(),
    next: 1,
});

fn connection_target(text: &str) -> Result<Target, String> {
    let mut server = None;
    let mut port = None;
    let mut database = None;
    let mut user = None;
    let mut password = None;
    for part in split_attributes(text) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let Some((key, value)) = part.split_once('=') else {
            return Err(format!("connection attribute `{part}` has no value"));
        };
        let value = unbrace(value.trim());
        match key.trim().to_ascii_uppercase().as_str() {
            "SERVER" | "HOST" | "SERVERNAME" => server = Some(value.to_string()),
            "PORT" => {
                port = Some(
                    value
                        .parse::<u16>()
                        .map_err(|_| format!("PORT `{value}` is not a number"))?,
                );
            }
            "DATABASE" | "DB" => database = Some(value.to_string()),
            "UID" | "USER" | "USERNAME" => user = Some(value.to_string()),
            "PWD" | "PASSWORD" => password = Some(value.to_string()),
            _ => {}
        }
    }
    let Some(server) = server else {
        return Err("SERVER is missing from the connection string".to_string());
    };
    let (host, port) = host_and_port(&server, port)?;
    let target = Target {
        host,
        port,
        database: database.unwrap_or_default(),
        user: blank_to_none(user),
        password: blank_to_none(password),
    };
    Ok(target)
}

fn server_name_target(server: &str) -> Result<Target, String> {
    if server.contains('=') {
        return connection_target(server);
    }
    let (hostport, database) = match server.split_once('/') {
        Some((hostport, database)) => (hostport, database.to_string()),
        None => (server, String::new()),
    };
    let (host, port) = host_and_port(hostport, None)?;
    Ok(Target {
        host,
        port,
        database,
        user: None,
        password: None,
    })
}

fn blank_to_none(value: Option<String>) -> Option<String> {
    value.filter(|text| !text.is_empty())
}

/// Both values log in. Neither leaves the socket anonymous. One of the
/// two is an incomplete login.
fn paired(target: &Target) -> Result<Option<(String, String)>, String> {
    match (&target.user, &target.password) {
        (None, None) => Ok(None),
        (Some(user), Some(password)) => Ok(Some((user.clone(), password.clone()))),
        (Some(_), None) => Err("PWD is missing".to_string()),
        (None, Some(_)) => Err("UID is missing".to_string()),
    }
}

fn with_login(
    mut target: Target,
    user: Option<String>,
    password: Option<String>,
) -> Result<Target, String> {
    if user.is_some() {
        target.user = user;
    }
    if password.is_some() {
        target.password = password;
    }
    paired(&target)?;
    Ok(target)
}

fn host_and_port(server: &str, port: Option<u16>) -> Result<(String, u16), String> {
    if server.is_empty() {
        return Err("SERVER is empty".to_string());
    }
    if let Some(port) = port {
        return Ok((server.to_string(), port));
    }
    if let Some((host, port_text)) = server.rsplit_once(':') {
        if !host.is_empty() {
            if let Ok(port) = port_text.parse::<u16>() {
                return Ok((host.to_string(), port));
            }
        }
    }
    Ok((server.to_string(), 27018))
}

fn split_attributes(text: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth: i32 = 0;
    for (index, ch) in text.char_indices() {
        match ch {
            '{' => depth += 1,
            '}' => depth = depth.saturating_sub(1),
            ';' if depth == 0 => {
                parts.push(&text[start..index]);
                start = index + ch.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&text[start..]);
    parts
}

fn unbrace(value: &str) -> &str {
    value
        .strip_prefix('{')
        .and_then(|text| text.strip_suffix('}'))
        .unwrap_or(value)
        .trim()
}

fn dial(target: &Target) -> Result<TcpStream, String> {
    let addr = format!("{}:{}", target.host, target.port);
    let mut last = format!("no address for {addr}");
    let addrs = addr
        .to_socket_addrs()
        .map_err(|err| format!("cannot resolve {addr}: {err}"))?;
    for sockaddr in addrs {
        match TcpStream::connect_timeout(&sockaddr, IO_TIMEOUT) {
            Ok(socket) => {
                let _ = socket.set_nodelay(true);
                let _ = socket.set_read_timeout(Some(IO_TIMEOUT));
                let _ = socket.set_write_timeout(Some(IO_TIMEOUT));
                return Ok(socket);
            }
            Err(err) => last = err.to_string(),
        }
    }
    Err(format!("cannot connect to {addr}: {last}"))
}

fn round_trip(
    socket: &mut TcpStream,
    request_id: i32,
    sql: &str,
    database: &str,
) -> Result<Document, String> {
    exchange(
        socket,
        request_id,
        bson::doc! { "sql": sql, "$db": database },
    )
}

fn exchange(
    socket: &mut TcpStream,
    request_id: i32,
    command: Document,
) -> Result<Document, String> {
    let frame = crate::protocol::encode_reply(request_id, 0, &command)?;
    socket.write_all(&frame).map_err(|err| err.to_string())?;
    let mut header = [0u8; crate::protocol::HEADER_SIZE];
    socket
        .read_exact(&mut header)
        .map_err(|err| err.to_string())?;
    let message_length = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    let op_code = i32::from_le_bytes([header[12], header[13], header[14], header[15]]);
    if op_code != crate::protocol::OP_MSG {
        return Err(format!("reply opcode is {op_code}"));
    }
    if message_length < crate::protocol::HEADER_SIZE as u32
        || message_length as usize > crate::protocol::MAX_MESSAGE_SIZE
    {
        return Err(format!("reply length {message_length} is out of range"));
    }
    let mut body = vec![0u8; message_length as usize - crate::protocol::HEADER_SIZE];
    socket
        .read_exact(&mut body)
        .map_err(|err| err.to_string())?;
    Ok(crate::protocol::decode_op_msg(&body)?.command)
}

fn login(socket: &mut TcpStream, user: &str, password: &str, database: &str) -> Result<(), String> {
    let reply = exchange(
        socket,
        1,
        bson::doc! {
            "authenticate": 1i32,
            "user": user,
            "pwd": password,
            "$db": database,
        },
    )?;
    if command_ok(&reply) {
        return Ok(());
    }
    Err(match reply.get("errmsg") {
        Some(bson::Bson::String(text)) => text.clone(),
        _ => "authentication failed".to_string(),
    })
}

fn command_ok(doc: &Document) -> bool {
    match doc.get("ok") {
        Some(bson::Bson::Double(value)) => *value == 1.0,
        Some(bson::Bson::Int32(value)) => *value == 1,
        Some(bson::Bson::Int64(value)) => *value == 1,
        _ => false,
    }
}

fn cell_from(value: &bson::Bson) -> Cell {
    match value {
        bson::Bson::Null | bson::Bson::Undefined => Cell::Null,
        bson::Bson::Boolean(value) => Cell::Bool(*value),
        bson::Bson::Int32(value) => Cell::Int(i64::from(*value)),
        bson::Bson::Int64(value) => Cell::Int(*value),
        bson::Bson::Double(value) => Cell::Float(*value),
        bson::Bson::String(value) => Cell::Text(value.clone()),
        bson::Bson::ObjectId(value) => Cell::Text(value.to_hex()),
        other => Cell::Text(format!("{other:?}")),
    }
}

fn cell_text(cell: &Cell) -> String {
    match cell {
        Cell::Null => String::new(),
        Cell::Bool(true) => "1".to_string(),
        Cell::Bool(false) => "0".to_string(),
        Cell::Int(value) => value.to_string(),
        Cell::Float(value) => format!("{value}"),
        Cell::Text(value) => value.clone(),
    }
}

fn column_type(rows: &[Vec<Cell>], index: usize) -> (i16, u64) {
    let mut text = false;
    let mut float = false;
    let mut int = false;
    let mut boolean = false;
    let mut wide = false;
    let mut chars = 1u64;
    for row in rows {
        match row.get(index) {
            Some(Cell::Text(value)) => {
                text = true;
                chars = chars.max(value.len() as u64);
            }
            Some(Cell::Float(_)) => float = true,
            Some(Cell::Int(value)) => {
                int = true;
                if *value > i64::from(i32::MAX) || *value < i64::from(i32::MIN) {
                    wide = true;
                }
            }
            Some(Cell::Bool(_)) => boolean = true,
            _ => {}
        }
    }
    if text {
        (SQL_VARCHAR, chars.max(1))
    } else if float {
        (SQL_DOUBLE, 24)
    } else if int {
        (if wide { SQL_BIGINT } else { SQL_INTEGER }, 20)
    } else if boolean {
        (SQL_BIT, 1)
    } else {
        (SQL_VARCHAR, 1)
    }
}

fn rows_of(doc: &Document) -> Result<(Vec<Column>, Vec<Vec<Cell>>), Fail> {
    if !command_ok(doc) {
        let message = match doc.get("errmsg") {
            Some(bson::Bson::String(text)) => text.clone(),
            _ => "sql failed".to_string(),
        };
        return Err(Fail::Server(message));
    }
    let cursor = doc
        .get_document("cursor")
        .map_err(|_| Fail::Reply("sql reply has no cursor".to_string()))?;
    let batch = cursor
        .get_array("firstBatch")
        .map_err(|_| Fail::Reply("sql reply has no rows".to_string()))?;
    let mut names = Vec::new();
    let mut docs = Vec::new();
    for item in batch {
        let row = item
            .as_document()
            .ok_or_else(|| Fail::Reply("sql row is not a document".to_string()))?;
        for key in row.keys() {
            if !names.iter().any(|name| name == key) {
                names.push(key.clone());
            }
        }
        docs.push(row);
    }
    let mut table = Vec::with_capacity(docs.len());
    for row in &docs {
        table.push(
            names
                .iter()
                .map(|name| match row.get(name) {
                    Some(value) => cell_from(value),
                    None => Cell::Null,
                })
                .collect(),
        );
    }
    let columns = names
        .into_iter()
        .enumerate()
        .map(|(index, name)| {
            let (kind, size) = column_type(&table, index);
            Column { name, kind, size }
        })
        .collect();
    Ok((columns, table))
}

fn connect_id(id: u64, target: Target) -> i16 {
    let login_as = match paired(&target) {
        Ok(pair) => pair,
        Err(message) => return conn_error(id, "28000", &message),
    };
    {
        let mut reg = registry();
        let Some(conn) = reg.conns.get_mut(&id) else {
            return SQL_INVALID_HANDLE;
        };
        if conn.socket.is_some() {
            conn.diag = Diag::new("08002", "connection is already open");
            return SQL_ERROR;
        }
    }
    let mut socket = match dial(&target) {
        Ok(socket) => socket,
        Err(message) => return conn_error(id, "08001", &message),
    };
    let mut next_request = 1;
    if let Some((user, password)) = login_as {
        if let Err(message) = login(&mut socket, &user, &password, &target.database) {
            return conn_error(id, "28000", &message);
        }
        next_request = 2;
    }
    let mut reg = registry();
    let Some(conn) = reg.conns.get_mut(&id) else {
        return SQL_INVALID_HANDLE;
    };
    conn.database = target.database;
    conn.next_request = next_request;
    conn.socket = Some(socket);
    conn.diag = Diag::clear();
    SQL_SUCCESS
}

fn conn_error(id: u64, state: &'static str, message: &str) -> i16 {
    let mut reg = registry();
    if let Some(conn) = reg.conns.get_mut(&id) {
        conn.diag = Diag::new(state, message);
        SQL_ERROR
    } else {
        SQL_INVALID_HANDLE
    }
}

fn execute_id(stmt_id: u64, sql: &str) -> i16 {
    if sql.is_empty() {
        return stmt_error(stmt_id, "HY090", "statement text is empty");
    }
    let (conn_id, database, request, mut socket) = {
        let mut reg = registry();
        let Some(stmt) = reg.stmts.get(&stmt_id) else {
            return SQL_INVALID_HANDLE;
        };
        let conn_id = stmt.conn;
        let Some(conn) = reg.conns.get_mut(&conn_id) else {
            return stmt_error(stmt_id, "08003", "connection is not open");
        };
        let Some(socket) = conn.socket.take() else {
            return stmt_error(stmt_id, "08003", "connection is not open");
        };
        let request = conn.next_request;
        conn.next_request = conn.next_request.wrapping_add(1).max(1);
        (conn_id, conn.database.clone(), request, socket)
    };
    let exchanged = round_trip(&mut socket, request, sql, &database);
    let mut reg = registry();
    match &exchanged {
        Ok(_) => {
            if let Some(conn) = reg.conns.get_mut(&conn_id) {
                conn.socket = Some(socket);
            }
        }
        Err(_) => drop(socket),
    }
    let Some(stmt) = reg.stmts.get_mut(&stmt_id) else {
        return SQL_INVALID_HANDLE;
    };
    stmt.rows.clear();
    stmt.columns.clear();
    stmt.next = 0;
    stmt.current = None;
    stmt.closed = false;
    match exchanged {
        Ok(doc) => match rows_of(&doc) {
            Ok((columns, rows)) => {
                stmt.columns = columns;
                stmt.rows = rows;
                stmt.executed = true;
                stmt.diag = Diag::clear();
                SQL_SUCCESS
            }
            Err(Fail::Server(message)) => {
                stmt.executed = true;
                stmt.diag = Diag::new("42000", message);
                SQL_ERROR
            }
            Err(Fail::Reply(message)) => {
                stmt.executed = true;
                stmt.diag = Diag::new("HY000", message);
                SQL_ERROR
            }
        },
        Err(message) => {
            stmt.executed = true;
            stmt.diag = Diag::new("08S01", message);
            SQL_ERROR
        }
    }
}

fn stmt_error(stmt_id: u64, state: &'static str, message: &str) -> i16 {
    let mut reg = registry();
    if let Some(stmt) = reg.stmts.get_mut(&stmt_id) {
        stmt.diag = Diag::new(state, message);
        SQL_ERROR
    } else {
        SQL_INVALID_HANDLE
    }
}

fn fetch_id(stmt_id: u64) -> i16 {
    let mut reg = registry();
    let Some(stmt) = reg.stmts.get_mut(&stmt_id) else {
        return SQL_INVALID_HANDLE;
    };
    if !stmt.executed {
        stmt.diag = Diag::new("HY010", "statement has no result");
        return SQL_ERROR;
    }
    if stmt.closed {
        stmt.diag = Diag::new("24000", "cursor is closed");
        return SQL_ERROR;
    }
    if stmt.next >= stmt.rows.len() {
        stmt.current = None;
        stmt.diag = Diag::clear();
        return SQL_NO_DATA;
    }
    let row = stmt.rows[stmt.next].clone();
    stmt.current = Some(stmt.next);
    stmt.next += 1;
    let mut truncated = false;
    for (index, binding) in stmt.bindings.iter().enumerate() {
        let Some(binding) = binding else {
            continue;
        };
        let Some(cell) = row.get(index) else {
            continue;
        };
        match unsafe { write_bound(cell, binding) } {
            Ok(Written::Ok) => {}
            Ok(Written::Truncated) => truncated = true,
            Err((state, message)) => {
                stmt.diag = Diag::new(state, message);
                return SQL_ERROR;
            }
        }
    }
    if truncated {
        stmt.diag = Diag::new("01004", "string data was truncated");
        SQL_SUCCESS_WITH_INFO
    } else {
        stmt.diag = Diag::clear();
        SQL_SUCCESS
    }
}

fn get_data_id(
    stmt_id: u64,
    column: u16,
    target_type: i16,
    target: *mut std::ffi::c_void,
    buffer_len: i64,
    indicator: *mut i64,
) -> i16 {
    let mut reg = registry();
    let Some(stmt) = reg.stmts.get_mut(&stmt_id) else {
        return SQL_INVALID_HANDLE;
    };
    let Some(row_index) = stmt.current else {
        stmt.diag = Diag::new("24000", "no current row");
        return SQL_ERROR;
    };
    let index = column as usize;
    if index == 0 || index > stmt.columns.len() {
        stmt.diag = Diag::new("07009", "column number is out of range");
        return SQL_ERROR;
    }
    let cell = stmt.rows[row_index][index - 1].clone();
    let binding = Binding {
        target_type,
        target: target as usize,
        buffer_len,
        indicator: indicator as usize,
    };
    match unsafe { write_bound(&cell, &binding) } {
        Ok(Written::Ok) => {
            stmt.diag = Diag::clear();
            SQL_SUCCESS
        }
        Ok(Written::Truncated) => {
            stmt.diag = Diag::new("01004", "string data was truncated");
            SQL_SUCCESS_WITH_INFO
        }
        Err((state, message)) => {
            stmt.diag = Diag::new(state, message);
            SQL_ERROR
        }
    }
}

unsafe fn write_bound(cell: &Cell, binding: &Binding) -> Result<Written, (&'static str, String)> {
    let indicator = binding.indicator as *mut i64;
    if matches!(cell, Cell::Null) {
        if indicator.is_null() {
            return Err(("22002", "a null column needs an indicator".to_string()));
        }
        *indicator = SQL_NULL_DATA;
        return Ok(Written::Ok);
    }
    let target = binding.target as *mut std::ffi::c_void;
    if target.is_null() {
        return Err(("HY009", "output buffer is null".to_string()));
    }
    match binding.target_type {
        SQL_C_CHAR => {
            let text = cell_text(cell);
            if !indicator.is_null() {
                *indicator = text.len() as i64;
            }
            if binding.buffer_len <= 0 {
                return Err(("HY090", "buffer length is empty".to_string()));
            }
            let buf = target as *mut u8;
            let room = (binding.buffer_len as usize) - 1;
            let bytes = text.as_bytes();
            let copied = bytes.len().min(room);
            if copied > 0 {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, copied);
            }
            *buf.add(copied) = 0;
            if copied < bytes.len() {
                Ok(Written::Truncated)
            } else {
                Ok(Written::Ok)
            }
        }
        SQL_C_LONG => {
            let value = cell_i64(cell)?;
            let value = i32::try_from(value).map_err(|_| {
                (
                    "22003",
                    "numeric value is out of range for a 32-bit integer".to_string(),
                )
            })?;
            (target as *mut i32).write(value);
            if !indicator.is_null() {
                *indicator = 4;
            }
            Ok(Written::Ok)
        }
        SQL_C_SBIGINT => {
            let value = cell_i64(cell)?;
            (target as *mut i64).write(value);
            if !indicator.is_null() {
                *indicator = 8;
            }
            Ok(Written::Ok)
        }
        SQL_C_DOUBLE => {
            let value = cell_f64(cell)?;
            (target as *mut f64).write(value);
            if !indicator.is_null() {
                *indicator = 8;
            }
            Ok(Written::Ok)
        }
        SQL_C_BIT => {
            let value = cell_bit(cell)?;
            (target as *mut u8).write(u8::from(value));
            if !indicator.is_null() {
                *indicator = 1;
            }
            Ok(Written::Ok)
        }
        _ => Err((
            "HY003",
            format!("buffer type {} is not supported", binding.target_type),
        )),
    }
}

fn cell_i64(cell: &Cell) -> Result<i64, (&'static str, String)> {
    match cell {
        Cell::Null => Err(("22002", "column is null".to_string())),
        Cell::Bool(value) => Ok(i64::from(*value)),
        Cell::Int(value) => Ok(*value),
        Cell::Float(value) => {
            if value.is_finite() && value.fract() == 0.0 {
                Ok(*value as i64)
            } else {
                Err(("22018", "column is not an integer".to_string()))
            }
        }
        Cell::Text(value) => value
            .parse()
            .map_err(|_| ("22018", "column is not an integer".to_string())),
    }
}

fn cell_f64(cell: &Cell) -> Result<f64, (&'static str, String)> {
    match cell {
        Cell::Null => Err(("22002", "column is null".to_string())),
        Cell::Bool(value) => Ok(if *value { 1.0 } else { 0.0 }),
        Cell::Int(value) => Ok(*value as f64),
        Cell::Float(value) => Ok(*value),
        Cell::Text(value) => value
            .parse()
            .map_err(|_| ("22018", "column is not a number".to_string())),
    }
}

fn cell_bit(cell: &Cell) -> Result<bool, (&'static str, String)> {
    match cell {
        Cell::Null => Err(("22002", "column is null".to_string())),
        Cell::Bool(value) => Ok(*value),
        Cell::Int(value) => Ok(*value != 0),
        Cell::Float(value) => Ok(*value != 0.0),
        Cell::Text(value) => match value.as_str() {
            "0" | "false" | "FALSE" => Ok(false),
            "1" | "true" | "TRUE" => Ok(true),
            _ => Err(("22018", "column is not a bit".to_string())),
        },
    }
}

fn info_text(info_type: u16) -> Option<&'static str> {
    match info_type {
        SQL_DRIVER_NAME => Some("libkova.so"),
        SQL_DRIVER_VER | SQL_DBMS_VER => Some("00.01.0000"),
        SQL_ODBC_VER | SQL_DRIVER_ODBC_VER => Some("03.00"),
        SQL_DBMS_NAME => Some("Kova"),
        SQL_IDENTIFIER_QUOTE_CHAR => Some("\""),
        _ => None,
    }
}

unsafe fn read_text(ptr: *const u8, len: i32) -> Result<String, String> {
    if ptr.is_null() {
        return Err("text pointer is null".to_string());
    }
    let bytes = if len == SQL_NTS {
        let mut count = 0usize;
        while count < 1_000_000 && *ptr.add(count) != 0 {
            count += 1;
        }
        if count == 1_000_000 {
            return Err("text is too long".to_string());
        }
        std::slice::from_raw_parts(ptr, count)
    } else if len < 0 {
        return Err("text length is negative".to_string());
    } else if len as usize > 1_000_000 {
        return Err("text is too long".to_string());
    } else {
        std::slice::from_raw_parts(ptr, len as usize)
    };
    String::from_utf8(bytes.to_vec()).map_err(|_| "text is not utf-8".to_string())
}

unsafe fn read_wide(ptr: *const u16, len: i32) -> Result<String, String> {
    if ptr.is_null() {
        return Err("text pointer is null".to_string());
    }
    let mut units = Vec::new();
    if len == SQL_NTS {
        let mut index = 0usize;
        while index < 1_000_000 {
            let unit = *ptr.add(index);
            if unit == 0 {
                break;
            }
            units.push(unit);
            index += 1;
        }
        if units.len() == 1_000_000 {
            return Err("text is too long".to_string());
        }
    } else if len < 0 {
        return Err("text length is negative".to_string());
    } else if len as usize > 1_000_000 {
        return Err("text is too long".to_string());
    } else {
        for index in 0..len as usize {
            units.push(*ptr.add(index));
        }
    }
    String::from_utf16(&units).map_err(|_| "text is not utf-16".to_string())
}

unsafe fn optional_text(ptr: *const u8, len: i32) -> Result<Option<String>, String> {
    if ptr.is_null() || len == 0 {
        return Ok(None);
    }
    Ok(blank_to_none(Some(read_text(ptr, len)?)))
}

unsafe fn optional_wide(ptr: *const u16, len: i32) -> Result<Option<String>, String> {
    if ptr.is_null() || len == 0 {
        return Ok(None);
    }
    Ok(blank_to_none(Some(read_wide(ptr, len)?)))
}

unsafe fn write_text(buf: *mut u8, cap: i16, text: &str, len_out: *mut i16) -> i16 {
    let bytes = text.as_bytes();
    if !len_out.is_null() {
        *len_out = i16::try_from(bytes.len()).unwrap_or(i16::MAX);
    }
    if buf.is_null() || cap <= 0 {
        return if bytes.is_empty() {
            SQL_SUCCESS
        } else if buf.is_null() && cap == 0 {
            SQL_SUCCESS
        } else {
            SQL_ERROR
        };
    }
    let room = (cap as usize) - 1;
    let copied = bytes.len().min(room);
    if copied > 0 {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, copied);
    }
    *buf.add(copied) = 0;
    if copied < bytes.len() {
        SQL_SUCCESS_WITH_INFO
    } else {
        SQL_SUCCESS
    }
}

unsafe fn write_wide(buf: *mut u16, cap: i16, text: &str, len_out: *mut i16) -> i16 {
    let units: Vec<u16> = text.encode_utf16().collect();
    if !len_out.is_null() {
        *len_out = i16::try_from(units.len()).unwrap_or(i16::MAX);
    }
    if buf.is_null() || cap <= 0 {
        return if units.is_empty() || (buf.is_null() && cap == 0) {
            SQL_SUCCESS
        } else {
            SQL_ERROR
        };
    }
    let room = (cap as usize) - 1;
    let copied = units.len().min(room);
    if copied > 0 {
        std::ptr::copy_nonoverlapping(units.as_ptr(), buf, copied);
    }
    *buf.add(copied) = 0;
    if copied < units.len() {
        SQL_SUCCESS_WITH_INFO
    } else {
        SQL_SUCCESS
    }
}

unsafe fn write_state(buf: *mut u8, state: &str) {
    if buf.is_null() {
        return;
    }
    let bytes = state.as_bytes();
    let copied = bytes.len().min(5);
    if copied > 0 {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, copied);
    }
    for index in copied..5 {
        *buf.add(index) = b' ';
    }
    *buf.add(5) = 0;
}

fn id_of(handle: *mut std::ffi::c_void) -> Option<u64> {
    let id = handle as u64;
    if id == 0 {
        None
    } else {
        Some(id)
    }
}

fn alloc_handle(
    handle_type: i16,
    input: *mut std::ffi::c_void,
    output: *mut *mut std::ffi::c_void,
) -> i16 {
    if output.is_null() {
        return SQL_ERROR;
    }
    let mut reg = registry();
    let id = match handle_type {
        SQL_HANDLE_ENV => {
            let id = reg.alloc();
            reg.envs.insert(
                id,
                Env {
                    diag: Diag::clear(),
                },
            );
            id
        }
        SQL_HANDLE_DBC => {
            let Some(env) = id_of(input) else {
                return SQL_INVALID_HANDLE;
            };
            if !reg.envs.contains_key(&env) {
                return SQL_INVALID_HANDLE;
            }
            let id = reg.alloc();
            reg.conns.insert(
                id,
                Conn {
                    env,
                    database: String::new(),
                    socket: None,
                    next_request: 1,
                    diag: Diag::clear(),
                },
            );
            id
        }
        SQL_HANDLE_STMT => {
            let Some(conn) = id_of(input) else {
                return SQL_INVALID_HANDLE;
            };
            if !reg.conns.contains_key(&conn) {
                return SQL_INVALID_HANDLE;
            }
            let id = reg.alloc();
            reg.stmts.insert(
                id,
                Stmt {
                    conn,
                    columns: Vec::new(),
                    rows: Vec::new(),
                    next: 0,
                    current: None,
                    executed: false,
                    closed: false,
                    prepared: None,
                    bindings: Vec::new(),
                    diag: Diag::clear(),
                },
            );
            id
        }
        _ => return SQL_ERROR,
    };
    unsafe {
        *output = id as *mut std::ffi::c_void;
    }
    SQL_SUCCESS
}

fn free_handle(handle_type: i16, handle: *mut std::ffi::c_void) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let mut reg = registry();
    match handle_type {
        SQL_HANDLE_ENV => {
            if reg.conns.values().any(|conn| conn.env == id) {
                if let Some(env) = reg.envs.get_mut(&id) {
                    env.diag = Diag::new("HY010", "environment still has a connection");
                }
                return SQL_ERROR;
            }
            if reg.envs.remove(&id).is_some() {
                SQL_SUCCESS
            } else {
                SQL_INVALID_HANDLE
            }
        }
        SQL_HANDLE_DBC => {
            if reg.stmts.values().any(|stmt| stmt.conn == id) {
                if let Some(conn) = reg.conns.get_mut(&id) {
                    conn.diag = Diag::new("HY010", "connection still has a statement");
                }
                return SQL_ERROR;
            }
            if reg.conns.remove(&id).is_some() {
                SQL_SUCCESS
            } else {
                SQL_INVALID_HANDLE
            }
        }
        SQL_HANDLE_STMT => {
            if reg.stmts.remove(&id).is_some() {
                SQL_SUCCESS
            } else {
                SQL_INVALID_HANDLE
            }
        }
        _ => SQL_ERROR,
    }
}

fn normalized(target: &Target) -> String {
    let mut text = format!(
        "DRIVER=Kova;SERVER={};PORT={};DATABASE={}",
        target.host, target.port, target.database
    );
    if let Some(user) = &target.user {
        text.push_str(";UID=");
        text.push_str(user);
    }
    text
}

fn finish_connect(id: u64, target: &Target, out: *mut u8, out_max: i16, out_len: *mut i16) -> i16 {
    let rc = connect_id(id, target.clone());
    if rc != SQL_SUCCESS {
        return rc;
    }
    let text = normalized(target);
    let written = unsafe { write_text(out, out_max, &text, out_len) };
    if written == SQL_SUCCESS_WITH_INFO {
        let mut reg = registry();
        if let Some(conn) = reg.conns.get_mut(&id) {
            conn.diag = Diag::new("01004", "connection string was truncated");
        }
    }
    written
}

fn diag_of(handle_type: i16, id: u64) -> Option<Diag> {
    let reg = registry();
    let diag = match handle_type {
        SQL_HANDLE_ENV => reg.envs.get(&id).map(|env| &env.diag),
        SQL_HANDLE_DBC => reg.conns.get(&id).map(|conn| &conn.diag),
        SQL_HANDLE_STMT => reg.stmts.get(&id).map(|stmt| &stmt.diag),
        _ => None,
    }?;
    if diag.message.is_empty() {
        None
    } else {
        Some(Diag {
            state: diag.state,
            message: diag.message.clone(),
        })
    }
}

/// Allocates an environment, connection, or statement handle.
#[no_mangle]
pub unsafe extern "C" fn SQLAllocHandle(
    handle_type: i16,
    input: *mut std::ffi::c_void,
    output: *mut *mut std::ffi::c_void,
) -> i16 {
    alloc_handle(handle_type, input, output)
}

/// Frees a handle. A handle that still owns a child returns `HY010`.
#[no_mangle]
pub unsafe extern "C" fn SQLFreeHandle(handle_type: i16, handle: *mut std::ffi::c_void) -> i16 {
    free_handle(handle_type, handle)
}

/// Records `SQL_ATTR_ODBC_VERSION`. Other attributes are rejected.
#[no_mangle]
pub unsafe extern "C" fn SQLSetEnvAttr(
    handle: *mut std::ffi::c_void,
    attribute: i32,
    _value: *mut std::ffi::c_void,
    _length: i32,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let mut reg = registry();
    let Some(env) = reg.envs.get_mut(&id) else {
        return SQL_INVALID_HANDLE;
    };
    if attribute == SQL_ATTR_ODBC_VERSION {
        env.diag = Diag::clear();
        SQL_SUCCESS
    } else {
        env.diag = Diag::new("HY092", "environment attribute is not supported");
        SQL_ERROR
    }
}

/// Accepts autocommit. Each statement is already one command.
#[no_mangle]
pub unsafe extern "C" fn SQLSetConnectAttr(
    handle: *mut std::ffi::c_void,
    attribute: i32,
    _value: *mut std::ffi::c_void,
    _length: i32,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let mut reg = registry();
    let Some(conn) = reg.conns.get_mut(&id) else {
        return SQL_INVALID_HANDLE;
    };
    if attribute == SQL_ATTR_AUTOCOMMIT {
        conn.diag = Diag::clear();
        SQL_SUCCESS
    } else {
        conn.diag = Diag::new("HY092", "connection attribute is not supported");
        SQL_ERROR
    }
}

/// Opens `SERVER`, `PORT`, and `DATABASE` from the connection string.
#[no_mangle]
pub unsafe extern "C" fn SQLDriverConnect(
    handle: *mut std::ffi::c_void,
    _window: *mut std::ffi::c_void,
    input: *mut u8,
    input_len: i16,
    out: *mut u8,
    out_max: i16,
    out_len: *mut i16,
    _completion: u16,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let text = match read_text(input, i32::from(input_len)) {
        Ok(text) => text,
        Err(message) => {
            let mut reg = registry();
            if let Some(conn) = reg.conns.get_mut(&id) {
                conn.diag = Diag::new("HY090", message);
                return SQL_ERROR;
            }
            return SQL_INVALID_HANDLE;
        }
    };
    let target = match connection_target(&text) {
        Ok(target) => target,
        Err(message) => {
            let mut reg = registry();
            if let Some(conn) = reg.conns.get_mut(&id) {
                conn.diag = Diag::new("HY000", message);
                return SQL_ERROR;
            }
            return SQL_INVALID_HANDLE;
        }
    };
    finish_connect(id, &target, out, out_max, out_len)
}

/// Wide-character [`SQLDriverConnect`].
#[no_mangle]
pub unsafe extern "C" fn SQLDriverConnectW(
    handle: *mut std::ffi::c_void,
    _window: *mut std::ffi::c_void,
    input: *mut u16,
    input_len: i16,
    out: *mut u16,
    out_max: i16,
    out_len: *mut i16,
    _completion: u16,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let text = match read_wide(input, i32::from(input_len)) {
        Ok(text) => text,
        Err(message) => return conn_error(id, "HY090", &message),
    };
    let target = match connection_target(&text) {
        Ok(target) => target,
        Err(message) => return conn_error(id, "HY000", &message),
    };
    let rc = finish_connect(id, &target, std::ptr::null_mut(), 0, std::ptr::null_mut());
    if rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO {
        return rc;
    }
    let written = write_wide(out, out_max, &normalized(&target), out_len);
    if written == SQL_SUCCESS_WITH_INFO {
        let mut reg = registry();
        if let Some(conn) = reg.conns.get_mut(&id) {
            conn.diag = Diag::new("01004", "connection string was truncated");
        }
    }
    written
}

/// Opens `host`, `host:port`, or `host:port/database`. User and password
/// log in when both are present.
#[no_mangle]
pub unsafe extern "C" fn SQLConnect(
    handle: *mut std::ffi::c_void,
    server: *mut u8,
    server_len: i16,
    user: *mut u8,
    user_len: i16,
    auth: *mut u8,
    auth_len: i16,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let text = match read_text(server, i32::from(server_len)) {
        Ok(text) => text,
        Err(message) => return conn_error(id, "HY090", &message),
    };
    let user = match optional_text(user, i32::from(user_len)) {
        Ok(user) => user,
        Err(message) => return conn_error(id, "HY090", &message),
    };
    let password = match optional_text(auth, i32::from(auth_len)) {
        Ok(password) => password,
        Err(message) => return conn_error(id, "HY090", &message),
    };
    let target = match server_name_target(&text) {
        Ok(target) => target,
        Err(message) => return conn_error(id, "HY000", &message),
    };
    let target = match with_login(target, user, password) {
        Ok(target) => target,
        Err(message) => return conn_error(id, "28000", &message),
    };
    connect_id(id, target)
}

/// Wide-character [`SQLConnect`].
#[no_mangle]
pub unsafe extern "C" fn SQLConnectW(
    handle: *mut std::ffi::c_void,
    server: *mut u16,
    server_len: i16,
    user: *mut u16,
    user_len: i16,
    auth: *mut u16,
    auth_len: i16,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let text = match read_wide(server, i32::from(server_len)) {
        Ok(text) => text,
        Err(message) => return conn_error(id, "HY090", &message),
    };
    let user = match optional_wide(user, i32::from(user_len)) {
        Ok(user) => user,
        Err(message) => return conn_error(id, "HY090", &message),
    };
    let password = match optional_wide(auth, i32::from(auth_len)) {
        Ok(password) => password,
        Err(message) => return conn_error(id, "HY090", &message),
    };
    let target = match server_name_target(&text) {
        Ok(target) => target,
        Err(message) => return conn_error(id, "HY000", &message),
    };
    let target = match with_login(target, user, password) {
        Ok(target) => target,
        Err(message) => return conn_error(id, "28000", &message),
    };
    connect_id(id, target)
}

/// Closes the MongoDB socket. The handle stays allocated.
#[no_mangle]
pub unsafe extern "C" fn SQLDisconnect(handle: *mut std::ffi::c_void) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let mut reg = registry();
    let Some(conn) = reg.conns.get_mut(&id) else {
        return SQL_INVALID_HANDLE;
    };
    if conn.socket.take().is_none() {
        conn.diag = Diag::new("08003", "connection is not open");
        return SQL_ERROR;
    }
    conn.diag = Diag::clear();
    SQL_SUCCESS
}

/// Runs the SQL text and keeps the cursor batch on the statement.
#[no_mangle]
pub unsafe extern "C" fn SQLExecDirect(
    handle: *mut std::ffi::c_void,
    text: *mut u8,
    length: i32,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    match read_text(text, length) {
        Ok(sql) => execute_id(id, &sql),
        Err(message) => stmt_error(id, "HY090", &message),
    }
}

/// Wide-character [`SQLExecDirect`].
#[no_mangle]
pub unsafe extern "C" fn SQLExecDirectW(
    handle: *mut std::ffi::c_void,
    text: *mut u16,
    length: i32,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    match read_wide(text, length) {
        Ok(sql) => execute_id(id, &sql),
        Err(message) => stmt_error(id, "HY090", &message),
    }
}

/// Stores SQL text for [`SQLExecute`].
#[no_mangle]
pub unsafe extern "C" fn SQLPrepare(
    handle: *mut std::ffi::c_void,
    text: *mut u8,
    length: i32,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let sql = match read_text(text, length) {
        Ok(sql) => sql,
        Err(message) => return stmt_error(id, "HY090", &message),
    };
    if sql.is_empty() {
        return stmt_error(id, "HY090", "statement text is empty");
    }
    let mut reg = registry();
    let Some(stmt) = reg.stmts.get_mut(&id) else {
        return SQL_INVALID_HANDLE;
    };
    stmt.prepared = Some(sql);
    stmt.diag = Diag::clear();
    SQL_SUCCESS
}

/// Wide-character [`SQLPrepare`].
#[no_mangle]
pub unsafe extern "C" fn SQLPrepareW(
    handle: *mut std::ffi::c_void,
    text: *mut u16,
    length: i32,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let sql = match read_wide(text, length) {
        Ok(sql) => sql,
        Err(message) => return stmt_error(id, "HY090", &message),
    };
    let owned = match std::ffi::CString::new(sql) {
        Ok(owned) => owned,
        Err(_) => return stmt_error(id, "HY090", "statement text is empty"),
    };
    SQLPrepare(handle, owned.as_ptr() as *mut u8, SQL_NTS)
}

/// Runs the statement stored by [`SQLPrepare`].
#[no_mangle]
pub unsafe extern "C" fn SQLExecute(handle: *mut std::ffi::c_void) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let sql = {
        let reg = registry();
        let Some(stmt) = reg.stmts.get(&id) else {
            return SQL_INVALID_HANDLE;
        };
        match &stmt.prepared {
            Some(sql) => sql.clone(),
            None => return stmt_error(id, "HY010", "statement is not prepared"),
        }
    };
    execute_id(id, &sql)
}

/// Writes the number of columns in the current result.
#[no_mangle]
pub unsafe extern "C" fn SQLNumResultCols(handle: *mut std::ffi::c_void, count: *mut i16) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let mut reg = registry();
    let Some(stmt) = reg.stmts.get_mut(&id) else {
        return SQL_INVALID_HANDLE;
    };
    if count.is_null() {
        stmt.diag = Diag::new("HY009", "column count pointer is null");
        return SQL_ERROR;
    }
    if !stmt.executed {
        stmt.diag = Diag::new("HY010", "statement has no result");
        return SQL_ERROR;
    }
    *count = i16::try_from(stmt.columns.len()).unwrap_or(i16::MAX);
    stmt.diag = Diag::clear();
    SQL_SUCCESS
}

/// Describes one column. Column numbers start at 1.
#[no_mangle]
pub unsafe extern "C" fn SQLDescribeCol(
    handle: *mut std::ffi::c_void,
    column: u16,
    name: *mut u8,
    name_max: i16,
    name_len: *mut i16,
    data_type: *mut i16,
    column_size: *mut u64,
    digits: *mut i16,
    nullable: *mut i16,
) -> i16 {
    describe_col(
        handle,
        column,
        name,
        name_max,
        name_len,
        data_type,
        column_size,
        digits,
        nullable,
        false,
    )
}

/// Wide-character [`SQLDescribeCol`].
#[no_mangle]
pub unsafe extern "C" fn SQLDescribeColW(
    handle: *mut std::ffi::c_void,
    column: u16,
    name: *mut u16,
    name_max: i16,
    name_len: *mut i16,
    data_type: *mut i16,
    column_size: *mut u64,
    digits: *mut i16,
    nullable: *mut i16,
) -> i16 {
    describe_col(
        handle,
        column,
        name as *mut u8,
        name_max,
        name_len,
        data_type,
        column_size,
        digits,
        nullable,
        true,
    )
}

unsafe fn describe_col(
    handle: *mut std::ffi::c_void,
    column: u16,
    name: *mut u8,
    name_max: i16,
    name_len: *mut i16,
    data_type: *mut i16,
    column_size: *mut u64,
    digits: *mut i16,
    nullable: *mut i16,
    wide: bool,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let mut reg = registry();
    let Some(stmt) = reg.stmts.get_mut(&id) else {
        return SQL_INVALID_HANDLE;
    };
    if !stmt.executed {
        stmt.diag = Diag::new("HY010", "statement has no result");
        return SQL_ERROR;
    }
    let index = column as usize;
    if index == 0 || index > stmt.columns.len() {
        stmt.diag = Diag::new("07009", "column number is out of range");
        return SQL_ERROR;
    }
    let described = &stmt.columns[index - 1];
    let label = described.name.clone();
    let kind = described.kind;
    let size = described.size;
    stmt.diag = Diag::clear();
    drop(reg);
    if !data_type.is_null() {
        *data_type = kind;
    }
    if !column_size.is_null() {
        *column_size = size;
    }
    if !digits.is_null() {
        *digits = 0;
    }
    if !nullable.is_null() {
        *nullable = SQL_NULLABLE;
    }
    if wide {
        write_wide(name as *mut u16, name_max, &label, name_len)
    } else {
        write_text(name, name_max, &label, name_len)
    }
}

/// Copies the current row into buffers from [`SQLBindCol`] and steps forward.
#[no_mangle]
pub unsafe extern "C" fn SQLFetch(handle: *mut std::ffi::c_void) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    fetch_id(id)
}

/// Reads one column of the current row.
#[no_mangle]
pub unsafe extern "C" fn SQLGetData(
    handle: *mut std::ffi::c_void,
    column: u16,
    target_type: i16,
    target: *mut std::ffi::c_void,
    buffer_len: i64,
    indicator: *mut i64,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    get_data_id(id, column, target_type, target, buffer_len, indicator)
}

/// Binds a column buffer filled by [`SQLFetch`]. Column numbers start at 1.
#[no_mangle]
pub unsafe extern "C" fn SQLBindCol(
    handle: *mut std::ffi::c_void,
    column: u16,
    target_type: i16,
    target: *mut std::ffi::c_void,
    buffer_len: i64,
    indicator: *mut i64,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    if column == 0 {
        return stmt_error(id, "07009", "column number is out of range");
    }
    let mut reg = registry();
    let Some(stmt) = reg.stmts.get_mut(&id) else {
        return SQL_INVALID_HANDLE;
    };
    let index = column as usize - 1;
    if stmt.bindings.len() <= index {
        stmt.bindings.resize_with(index + 1, || None);
    }
    if target.is_null() {
        stmt.bindings[index] = None;
    } else {
        stmt.bindings[index] = Some(Binding {
            target_type,
            target: target as usize,
            buffer_len,
            indicator: indicator as usize,
        });
    }
    stmt.diag = Diag::clear();
    SQL_SUCCESS
}

/// Closes the cursor, unbinds columns, or frees the statement.
#[no_mangle]
pub unsafe extern "C" fn SQLFreeStmt(handle: *mut std::ffi::c_void, option: i16) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    if option == SQL_DROP {
        return free_handle(SQL_HANDLE_STMT, handle);
    }
    let mut reg = registry();
    let Some(stmt) = reg.stmts.get_mut(&id) else {
        return SQL_INVALID_HANDLE;
    };
    match option {
        SQL_CLOSE => {
            stmt.rows.clear();
            stmt.next = 0;
            stmt.current = None;
            stmt.closed = true;
            stmt.diag = Diag::clear();
            SQL_SUCCESS
        }
        SQL_UNBIND => {
            stmt.bindings.clear();
            stmt.diag = Diag::clear();
            SQL_SUCCESS
        }
        SQL_RESET_PARAMS => {
            stmt.diag = Diag::clear();
            SQL_SUCCESS
        }
        _ => {
            stmt.diag = Diag::new("HY092", "statement option is not supported");
            SQL_ERROR
        }
    }
}

/// Writes the number of rows in the batch. `-1` before a statement runs.
#[no_mangle]
pub unsafe extern "C" fn SQLRowCount(handle: *mut std::ffi::c_void, count: *mut i64) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let mut reg = registry();
    let Some(stmt) = reg.stmts.get_mut(&id) else {
        return SQL_INVALID_HANDLE;
    };
    if count.is_null() {
        stmt.diag = Diag::new("HY009", "row count pointer is null");
        return SQL_ERROR;
    }
    *count = if stmt.executed {
        stmt.rows.len() as i64
    } else {
        -1
    };
    stmt.diag = Diag::clear();
    SQL_SUCCESS
}

/// A statement has one result.
#[no_mangle]
pub unsafe extern "C" fn SQLMoreResults(handle: *mut std::ffi::c_void) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let reg = registry();
    if reg.stmts.contains_key(&id) {
        SQL_NO_DATA
    } else {
        SQL_INVALID_HANDLE
    }
}

/// Returns the diagnostic on a handle. Record numbers other than 1 are empty.
#[no_mangle]
pub unsafe extern "C" fn SQLGetDiagRec(
    handle_type: i16,
    handle: *mut std::ffi::c_void,
    record: i16,
    state: *mut u8,
    native: *mut i32,
    message: *mut u8,
    message_max: i16,
    message_len: *mut i16,
) -> i16 {
    if record < 1 {
        return SQL_ERROR;
    }
    if record > 1 {
        return SQL_NO_DATA;
    }
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let Some(diag) = diag_of(handle_type, id) else {
        return SQL_NO_DATA;
    };
    write_state(state, diag.state);
    if !native.is_null() {
        *native = 0;
    }
    write_text(message, message_max, &diag.message, message_len)
}

/// Wide-character [`SQLGetDiagRec`].
#[no_mangle]
pub unsafe extern "C" fn SQLGetDiagRecW(
    handle_type: i16,
    handle: *mut std::ffi::c_void,
    record: i16,
    state: *mut u16,
    native: *mut i32,
    message: *mut u16,
    message_max: i16,
    message_len: *mut i16,
) -> i16 {
    if record < 1 {
        return SQL_ERROR;
    }
    if record > 1 {
        return SQL_NO_DATA;
    }
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let Some(diag) = diag_of(handle_type, id) else {
        return SQL_NO_DATA;
    };
    if !state.is_null() {
        let units: Vec<u16> = diag.state.encode_utf16().collect();
        for index in 0..5 {
            *state.add(index) = units.get(index).copied().unwrap_or(u16::from(b' '));
        }
        *state.add(5) = 0;
    }
    if !native.is_null() {
        *native = 0;
    }
    write_wide(message, message_max, &diag.message, message_len)
}

/// Reports the driver and DBMS names used by a driver manager.
#[no_mangle]
pub unsafe extern "C" fn SQLGetInfo(
    handle: *mut std::ffi::c_void,
    info_type: u16,
    value: *mut std::ffi::c_void,
    buffer_max: i16,
    length: *mut i16,
) -> i16 {
    info(handle, info_type, value, buffer_max, length, false)
}

/// Wide-character [`SQLGetInfo`].
#[no_mangle]
pub unsafe extern "C" fn SQLGetInfoW(
    handle: *mut std::ffi::c_void,
    info_type: u16,
    value: *mut std::ffi::c_void,
    buffer_max: i16,
    length: *mut i16,
) -> i16 {
    info(handle, info_type, value, buffer_max, length, true)
}

unsafe fn info(
    handle: *mut std::ffi::c_void,
    info_type: u16,
    value: *mut std::ffi::c_void,
    buffer_max: i16,
    length: *mut i16,
    wide: bool,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let mut reg = registry();
    let Some(conn) = reg.conns.get_mut(&id) else {
        return SQL_INVALID_HANDLE;
    };
    let Some(text) = info_text(info_type) else {
        conn.diag = Diag::new("HYC00", format!("info type {info_type} is not supported"));
        return SQL_ERROR;
    };
    conn.diag = Diag::clear();
    drop(reg);
    if wide {
        write_wide(value as *mut u16, buffer_max, text, length)
    } else {
        write_text(value as *mut u8, buffer_max, text, length)
    }
}

/// There is nothing to commit: each statement already ran on its own.
#[no_mangle]
pub unsafe extern "C" fn SQLEndTran(
    handle_type: i16,
    handle: *mut std::ffi::c_void,
    _completion: i16,
) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let reg = registry();
    let known = match handle_type {
        SQL_HANDLE_ENV => reg.envs.contains_key(&id),
        SQL_HANDLE_DBC => reg.conns.contains_key(&id),
        _ => false,
    };
    if known {
        SQL_SUCCESS
    } else {
        SQL_INVALID_HANDLE
    }
}

/// A statement runs to completion inside [`SQLExecDirect`].
#[no_mangle]
pub unsafe extern "C" fn SQLCancel(handle: *mut std::ffi::c_void) -> i16 {
    let Some(id) = id_of(handle) else {
        return SQL_INVALID_HANDLE;
    };
    let reg = registry();
    if reg.stmts.contains_key(&id) {
        SQL_SUCCESS
    } else {
        SQL_INVALID_HANDLE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_string_reads_server_port_and_database() {
        let target = connection_target("DRIVER={Kova};SERVER=127.0.0.1;PORT=27019;DATABASE=shop")
            .expect("parse");
        assert_eq!(target.host, "127.0.0.1");
        assert_eq!(target.port, 27019);
        assert_eq!(target.database, "shop");

        let named = server_name_target("127.0.0.1:27019/shop").expect("server name");
        assert_eq!(named.host, "127.0.0.1");
        assert_eq!(named.port, 27019);
        assert_eq!(named.database, "shop");

        assert!(connection_target("DRIVER=Kova").is_err());

        let login = connection_target("SERVER=127.0.0.1;UID=root;PWD=secret").expect("login");
        assert_eq!(login.user.as_deref(), Some("root"));
        assert_eq!(login.password.as_deref(), Some("secret"));
        let shown = normalized(&login);
        assert!(shown.contains("UID=root"));
        assert!(!shown.contains("secret"));
        assert!(paired(&connection_target("SERVER=127.0.0.1;UID=root").expect("half")).is_err());
    }

    struct Session {
        env: *mut std::ffi::c_void,
        dbc: *mut std::ffi::c_void,
        stmt: *mut std::ffi::c_void,
    }

    impl Drop for Session {
        fn drop(&mut self) {
            unsafe {
                if !self.stmt.is_null() {
                    SQLFreeHandle(SQL_HANDLE_STMT, self.stmt);
                }
                if !self.dbc.is_null() {
                    SQLDisconnect(self.dbc);
                    SQLFreeHandle(SQL_HANDLE_DBC, self.dbc);
                }
                if !self.env.is_null() {
                    SQLFreeHandle(SQL_HANDLE_ENV, self.env);
                }
            }
        }
    }

    fn diagnostic(handle_type: i16, handle: *mut std::ffi::c_void) -> String {
        let mut state = [0u8; 6];
        let mut message = [0u8; 512];
        let mut native = 0i32;
        let mut length = 0i16;
        let rc = unsafe {
            SQLGetDiagRec(
                handle_type,
                handle,
                1,
                state.as_mut_ptr(),
                &mut native,
                message.as_mut_ptr(),
                512,
                &mut length,
            )
        };
        if rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO {
            return format!("no diagnostic ({rc})");
        }
        let text = std::str::from_utf8(&message)
            .unwrap_or("")
            .trim_end_matches('\0');
        let code = std::str::from_utf8(&state)
            .unwrap_or("")
            .trim_end_matches('\0');
        format!("{code} {text}")
    }

    fn run_select(connection: &str, sql: &str) -> Result<Vec<Vec<String>>, String> {
        unsafe {
            let mut session = Session {
                env: std::ptr::null_mut(),
                dbc: std::ptr::null_mut(),
                stmt: std::ptr::null_mut(),
            };
            let rc = SQLAllocHandle(SQL_HANDLE_ENV, std::ptr::null_mut(), &mut session.env);
            if rc != SQL_SUCCESS {
                return Err(format!("alloc env {rc}"));
            }
            let rc = SQLSetEnvAttr(
                session.env,
                SQL_ATTR_ODBC_VERSION,
                3 as *mut std::ffi::c_void,
                0,
            );
            if rc != SQL_SUCCESS {
                return Err(diagnostic(SQL_HANDLE_ENV, session.env));
            }
            let rc = SQLAllocHandle(SQL_HANDLE_DBC, session.env, &mut session.dbc);
            if rc != SQL_SUCCESS {
                return Err(diagnostic(SQL_HANDLE_ENV, session.env));
            }
            let mut conn = connection.as_bytes().to_vec();
            conn.push(0);
            let rc = SQLDriverConnect(
                session.dbc,
                std::ptr::null_mut(),
                conn.as_mut_ptr(),
                SQL_NTS as i16,
                std::ptr::null_mut(),
                0,
                std::ptr::null_mut(),
                0,
            );
            if rc != SQL_SUCCESS {
                return Err(diagnostic(SQL_HANDLE_DBC, session.dbc));
            }
            let rc = SQLAllocHandle(SQL_HANDLE_STMT, session.dbc, &mut session.stmt);
            if rc != SQL_SUCCESS {
                return Err(diagnostic(SQL_HANDLE_DBC, session.dbc));
            }
            let mut query = sql.as_bytes().to_vec();
            query.push(0);
            let rc = SQLExecDirect(session.stmt, query.as_mut_ptr(), SQL_NTS);
            if rc != SQL_SUCCESS {
                return Err(diagnostic(SQL_HANDLE_STMT, session.stmt));
            }
            let mut cols = 0i16;
            let rc = SQLNumResultCols(session.stmt, &mut cols);
            if rc != SQL_SUCCESS {
                return Err(diagnostic(SQL_HANDLE_STMT, session.stmt));
            }
            let mut rows = Vec::new();
            loop {
                let rc = SQLFetch(session.stmt);
                if rc == SQL_NO_DATA {
                    break;
                }
                if rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO {
                    return Err(diagnostic(SQL_HANDLE_STMT, session.stmt));
                }
                let mut row = Vec::new();
                for column in 1..=cols {
                    let mut buf = vec![0u8; 256];
                    let mut indicator = 0i64;
                    let rc = SQLGetData(
                        session.stmt,
                        column as u16,
                        SQL_C_CHAR,
                        buf.as_mut_ptr() as *mut std::ffi::c_void,
                        buf.len() as i64,
                        &mut indicator,
                    );
                    if rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO {
                        return Err(diagnostic(SQL_HANDLE_STMT, session.stmt));
                    }
                    let text = if indicator == SQL_NULL_DATA {
                        String::new()
                    } else {
                        std::str::from_utf8(&buf)
                            .unwrap_or("")
                            .trim_end_matches('\0')
                            .to_string()
                    };
                    row.push(text);
                }
                rows.push(row);
            }
            Ok(rows)
        }
    }

    async fn mongo_command(addr: std::net::SocketAddr, command: bson::Document) -> bson::Document {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let frame = crate::protocol::encode_reply(1, 0, &command).expect("encode");
        let mut client = tokio::net::TcpStream::connect(addr).await.expect("connect");
        client.write_all(&frame).await.expect("write");
        let mut header = [0u8; crate::protocol::HEADER_SIZE];
        tokio::time::timeout(Duration::from_secs(10), client.read_exact(&mut header))
            .await
            .expect("timed out")
            .expect("header");
        let message_length = u32::from_le_bytes(header[0..4].try_into().expect("4 bytes"));
        let mut body = vec![0u8; message_length as usize - crate::protocol::HEADER_SIZE];
        client.read_exact(&mut body).await.expect("body");
        crate::protocol::decode_op_msg(&body)
            .expect("decode")
            .command
    }

    #[tokio::test]
    async fn an_odbc_select_matches_the_sql_command() {
        let router = crate::storage::PartitionRouter::new(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let server_router = router.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let client_router = server_router.clone();
                tokio::spawn(async move {
                    let _ = crate::network::handle_client(&mut socket, &client_router, None).await;
                });
            }
        });

        let inserted = mongo_command(
            addr,
            bson::doc! {
                "insert": "users",
                "$db": "shop",
                "documents": [
                    { "name": "Ada", "age": 30i32 },
                    { "name": "Bob", "age": 25i32 },
                ],
            },
        )
        .await;
        assert_eq!(inserted.get_f64("ok").expect("ok"), 1.0);

        let sql = "select name, age from users where age > 25";
        let connection = format!(
            "DRIVER=Kova;SERVER={};PORT={};DATABASE=shop",
            addr.ip(),
            addr.port()
        );
        let odbc_rows = tokio::task::spawn_blocking(move || run_select(&connection, sql))
            .await
            .expect("odbc thread")
            .expect("odbc select");

        let reply = mongo_command(addr, bson::doc! { "sql": sql, "$db": "shop" }).await;
        assert_eq!(reply.get_f64("ok").expect("ok"), 1.0, "{reply:?}");
        let batch = reply
            .get_document("cursor")
            .expect("cursor")
            .get_array("firstBatch")
            .expect("batch");
        let mongo_rows: Vec<Vec<String>> = batch
            .iter()
            .map(|row| {
                row.as_document()
                    .expect("row")
                    .iter()
                    .map(|(_, value)| cell_text(&cell_from(value)))
                    .collect()
            })
            .collect();
        assert_eq!(odbc_rows, mongo_rows);
        assert_eq!(odbc_rows, vec![vec!["Ada".to_string(), "30".to_string()]]);

        let bad = tokio::task::spawn_blocking(move || {
            run_select(
                &format!(
                    "DRIVER=Kova;SERVER={};PORT={};DATABASE=shop",
                    addr.ip(),
                    addr.port()
                ),
                "select from",
            )
        })
        .await
        .expect("odbc thread");
        assert!(bad.is_err(), "a broken statement returns a diagnostic");
    }

    async fn serve() -> std::net::SocketAddr {
        let router = crate::storage::PartitionRouter::new(1);
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    break;
                };
                let client_router = router.clone();
                tokio::spawn(async move {
                    let _ = crate::network::handle_client(&mut socket, &client_router, None).await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn an_odbc_login_sends_authenticate() {
        let addr = serve().await;
        let created = mongo_command(
            addr,
            bson::doc! {
                "createUser": "root",
                "pwd": "secret",
                "roles": ["root"],
                "$db": "admin",
            },
        )
        .await;
        assert_eq!(created.get_f64("ok").expect("ok"), 1.0, "{created:?}");

        let inserted = mongo_command(
            addr,
            bson::doc! {
                "insert": "users",
                "$db": "shop",
                "documents": [{ "name": "Ada", "age": 30i32 }],
            },
        )
        .await;
        assert_eq!(inserted.get_f64("ok").expect("ok"), 1.0, "{inserted:?}");

        let wrong = format!(
            "DRIVER=Kova;SERVER={};PORT={};DATABASE=shop;UID=root;PWD=wrong",
            addr.ip(),
            addr.port()
        );
        let rejected =
            tokio::task::spawn_blocking(move || run_select(&wrong, "select name from users"))
                .await
                .expect("odbc thread");
        let message = rejected.expect_err("a wrong password is refused");
        assert!(message.contains("authentication failed"), "{message}");

        let right = format!(
            "DRIVER=Kova;SERVER={};PORT={};DATABASE=shop;UID=root;PWD=secret",
            addr.ip(),
            addr.port()
        );
        let rows =
            tokio::task::spawn_blocking(move || run_select(&right, "select name from users"))
                .await
                .expect("odbc thread")
                .expect("logged in select");
        assert_eq!(rows, vec![vec!["Ada".to_string()]]);
    }
}
