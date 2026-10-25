//! Backup and restore over the MongoDB port.
//!
//! `kovadump backup` writes a Kova dump of one database. `kovadump restore`
//! reads that file, a `pg_dump` plain-SQL file, a `mysqldump` file, or a
//! `mongodump` directory or archive, and writes the rows with `insert`.
//! The WAL, indexes, and replication therefore see ordinary writes.
//!
//! The dialects are pinned in the submodules: [`kova`], [`sql`], and [`mongo`].

mod kova;
mod mongo;
mod sql;

use std::collections::BTreeSet;
use std::io::{IsTerminal, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::Path;
use std::time::Duration;

use bson::{doc, Bson, Document};

const PAGE: i64 = 200;
const INSERT_BATCH: usize = 128;
const IO_TIMEOUT: Duration = Duration::from_secs(120);

/// Where `kovadump` connects, and which database it reads or writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Target {
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: Option<String>,
    pub password: Option<String>,
}

/// How many collections, documents, and views a backup or restore moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    pub collections: usize,
    pub documents: usize,
    pub views: usize,
}

impl std::fmt::Display for Report {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "{} collections, {} documents, {} views",
            self.collections, self.documents, self.views
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
struct Dump {
    collections: Vec<CollectionDump>,
    views: Vec<ViewDump>,
}

#[derive(Debug, Clone, PartialEq)]
struct CollectionDump {
    name: String,
    documents: Vec<Document>,
    indexes: Vec<Document>,
    /// Fields filled from `admin.counters` when an insert omits them.
    autoincrement: Vec<String>,
    /// Explicit `last` values. Empty means the inserted numbers decide it.
    last: Vec<(String, i64)>,
}

#[derive(Debug, Clone, PartialEq)]
struct ViewDump {
    name: String,
    view_on: String,
    pipeline: Vec<Document>,
    kova: Document,
}

impl Dump {
    fn report(&self) -> Report {
        Report {
            collections: self.collections.len(),
            documents: self
                .collections
                .iter()
                .map(|collection| collection.documents.len())
                .sum(),
            views: self.views.len(),
        }
    }
}

/// Writes a Kova dump of `target`'s database to `path`.
pub fn backup(target: &Target, path: &Path) -> Result<Report, String> {
    target.validate()?;
    let mut client = Client::connect(target)?;
    let dump = client.read_database(&target.database)?;
    kova::write(path, &dump)?;
    Ok(dump.report())
}

/// Which collections a restore writes, and whether an existing one is replaced.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RestoreOptions {
    /// Empty restores every collection and view in the dump.
    pub tables: Vec<String>,
    /// Replace existing collections without asking.
    pub yes: bool,
}

/// Restores `path` into `target`'s database through `insert`.
///
/// Collections named in [`RestoreOptions::tables`] are the only ones
/// written. When one of them is already in the database, restore asks
/// on a terminal before dropping it. [`RestoreOptions::yes`] skips the
/// question. A non-interactive run has to pass that flag to replace
/// anything.
pub fn restore(target: &Target, path: &Path, options: &RestoreOptions) -> Result<Report, String> {
    target.validate()?;
    if !path.exists() {
        return Err(format!("{} does not exist", path.display()));
    }
    let dump = select_dump(load(path, &target.database)?, &options.tables)?;
    let mut client = Client::connect(target)?;
    let listed = cursor_batch(&client.command(doc! {
        "listCollections": 1i32,
        "$db": &target.database,
    })?)?;
    let (replacing, blocking) = collisions(&dump, &listed);
    confirm_overwrite(&target.database, &replacing, &blocking, options.yes)?;
    let mut progress = Progress::new(dump.report().documents as u64);
    client.write_database(&target.database, &dump, &mut progress)?;
    Ok(dump.report())
}

impl Target {
    fn validate(&self) -> Result<(), String> {
        if self.host.is_empty() {
            return Err("host is empty".into());
        }
        if self.database.is_empty() {
            return Err("database is empty".into());
        }
        match (&self.user, &self.password) {
            (Some(_), Some(_)) | (None, None) => Ok(()),
            _ => Err("user and password are both required".into()),
        }
    }
}

fn load(path: &Path, database: &str) -> Result<Dump, String> {
    if path.is_dir() {
        return mongo::read_dir(path, database);
    }
    let bytes =
        std::fs::read(path).map_err(|err| format!("cannot read {}: {err}", path.display()))?;
    if bytes.starts_with(kova::MAGIC) {
        return kova::read_bytes(&bytes);
    }
    if mongo::is_archive(&bytes) {
        return mongo::read_archive(&bytes);
    }
    let text = String::from_utf8(bytes).map_err(|_| {
        format!(
            "{} is not a Kova dump, a mongodump archive, or UTF-8 SQL",
            path.display()
        )
    })?;
    let dialect = sql::detect(&text)?;
    sql::parse(&text, dialect)
}

fn select_dump(mut dump: Dump, tables: &[String]) -> Result<Dump, String> {
    if tables.is_empty() {
        return Ok(dump);
    }
    let mut wanted = BTreeSet::new();
    for name in tables {
        let name = name.trim();
        if name.is_empty() {
            return Err("empty table name".into());
        }
        check_name(name)?;
        wanted.insert(name.to_string());
    }
    dump.collections
        .retain(|collection| wanted.contains(&collection.name));
    dump.views.retain(|view| wanted.contains(&view.name));
    for collection in &dump.collections {
        wanted.remove(&collection.name);
    }
    for view in &dump.views {
        wanted.remove(&view.name);
    }
    if !wanted.is_empty() {
        return Err(format!(
            "not in the dump: {}",
            wanted.into_iter().collect::<Vec<_>>().join(", ")
        ));
    }
    Ok(dump)
}

/// Names from the dump that already exist, and views on the server that
/// read one of those collections.
fn collisions(dump: &Dump, listed: &[Document]) -> (Vec<String>, Vec<String>) {
    let existing: BTreeSet<&str> = listed
        .iter()
        .filter_map(|info| info.get_str("name").ok())
        .collect();
    let mut replacing = Vec::new();
    for collection in &dump.collections {
        if existing.contains(collection.name.as_str()) {
            replacing.push(collection.name.clone());
        }
    }
    for view in &dump.views {
        if existing.contains(view.name.as_str()) {
            replacing.push(view.name.clone());
        }
    }
    let blocking = views_blocking_restore(listed, dump)
        .into_iter()
        .filter(|name| !replacing.iter().any(|kept| kept == name))
        .collect();
    (replacing, blocking)
}

fn confirm_overwrite(
    database: &str,
    replacing: &[String],
    blocking: &[String],
    yes: bool,
) -> Result<(), String> {
    if replacing.is_empty() && blocking.is_empty() {
        return Ok(());
    }
    let message = overwrite_message(database, replacing, blocking);
    if yes {
        eprintln!("{message}");
        return Ok(());
    }
    if !std::io::stdin().is_terminal() {
        return Err(refusal_message(database, replacing, blocking));
    }
    eprint!("{message}\nOverwrite? [y/N] ");
    let _ = std::io::stderr().flush();
    let mut line = String::new();
    std::io::stdin()
        .read_line(&mut line)
        .map_err(|err| format!("could not read the answer: {err}"))?;
    if accepts_overwrite(&line) {
        return Ok(());
    }
    Err("restore cancelled".into())
}

fn overwrite_message(database: &str, replacing: &[String], blocking: &[String]) -> String {
    let mut message = format!("database {database} already has:\n");
    for name in replacing {
        message.push_str(&format!("  {name}\n"));
    }
    if !blocking.is_empty() {
        message.push_str("views that read a restored collection will be dropped:\n");
        for name in blocking {
            message.push_str(&format!("  {name}\n"));
        }
    }
    message.trim_end().to_string()
}

fn refusal_message(database: &str, replacing: &[String], blocking: &[String]) -> String {
    format!(
        "{}\npass --yes to overwrite",
        overwrite_message(database, replacing, blocking)
    )
}

fn accepts_overwrite(line: &str) -> bool {
    matches!(line.trim().to_ascii_lowercase().as_str(), "y" | "yes")
}

/// One line rewritten while collections are inserted.
struct Progress {
    total: u64,
    done: u64,
    label: String,
    interactive: bool,
}

impl Progress {
    fn new(total: u64) -> Self {
        Self {
            total,
            done: 0,
            label: String::new(),
            interactive: std::io::stderr().is_terminal(),
        }
    }

    fn set_label(&mut self, label: &str) {
        self.label = label.to_string();
        self.show(true);
    }

    fn tick(&mut self, count: u64) {
        self.done = self.done.saturating_add(count);
        self.show(false);
    }

    fn show(&mut self, force: bool) {
        if self.total == 0 && self.label.is_empty() {
            return;
        }
        if self.interactive {
            eprint!("\r\x1b[2K{}", self.line());
            let _ = std::io::stderr().flush();
        } else if force {
            eprintln!("{}", self.line());
        }
    }

    fn line(&self) -> String {
        progress_line(self.done, self.total, &self.label)
    }
}

impl Drop for Progress {
    fn drop(&mut self) {
        if self.interactive && (!self.label.is_empty() || self.done > 0) {
            eprint!("\r\x1b[2K");
            let _ = std::io::stderr().flush();
        }
    }
}

fn progress_line(done: u64, total: u64, label: &str) -> String {
    const WIDTH: usize = 28;
    let percent = if total == 0 {
        100
    } else {
        (done.saturating_mul(100) / total).min(100)
    };
    let filled = if total == 0 {
        WIDTH
    } else {
        ((done.saturating_mul(WIDTH as u64) / total) as usize).min(WIDTH)
    };
    let mut bar = "=".repeat(filled);
    bar.push_str(&"-".repeat(WIDTH - filled));
    let shown: String = label.chars().take(40).collect();
    format!("[{bar}] {percent:>3}%  {done}/{total}  {shown}")
}

fn check_name(name: &str) -> Result<(), String> {
    if name.is_empty() || name.contains('.') {
        return Err(format!(
            "collection name {name:?} cannot be empty or contain '.'"
        ));
    }
    Ok(())
}

struct Client {
    socket: TcpStream,
    next_id: i32,
}

impl Client {
    fn connect(target: &Target) -> Result<Self, String> {
        let socket = dial(target)?;
        let mut client = Self { socket, next_id: 1 };
        if let (Some(user), Some(password)) = (&target.user, &target.password) {
            let reply = client.command(doc! {
                "authenticate": 1i32,
                "user": user,
                "pwd": password,
                "$db": &target.database,
            })?;
            ensure_ok(&reply)?;
        }
        Ok(client)
    }

    fn read_database(&mut self, database: &str) -> Result<Dump, String> {
        let listed = self.command(doc! { "listCollections": 1i32, "$db": database })?;
        let mut dump = Dump::default();
        for info in cursor_batch(&listed)? {
            let name = info
                .get_str("name")
                .map_err(|_| "listCollections entry has no name".to_string())?
                .to_string();
            if name.starts_with("system.") {
                continue;
            }
            check_name(&name)?;
            if info.get_str("type") == Ok("view") {
                let options = info
                    .get_document("options")
                    .map_err(|_| format!("view {name} has no options"))?;
                let view_on = options
                    .get_str("viewOn")
                    .map_err(|_| format!("view {name} has no viewOn"))?
                    .to_string();
                check_name(&view_on)?;
                let pipeline = match options.get("pipeline") {
                    None => Vec::new(),
                    Some(Bson::Array(values)) => values
                        .iter()
                        .map(|value| {
                            value.as_document().cloned().ok_or_else(|| {
                                format!("view {name} pipeline stage is not a document")
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    Some(_) => return Err(format!("view {name} pipeline is not an array")),
                };
                let kova = options.get_document("kova").cloned().unwrap_or_default();
                dump.views.push(ViewDump {
                    name,
                    view_on,
                    pipeline,
                    kova,
                });
                continue;
            }
            let mut documents = Vec::new();
            let mut skip = 0i64;
            loop {
                let found = self.command(doc! {
                    "find": &name,
                    "filter": {},
                    "skip": skip,
                    "limit": PAGE,
                    "$db": database,
                })?;
                let batch = cursor_batch(&found)?;
                let count = batch.len() as i64;
                documents.extend(batch);
                if count < PAGE {
                    break;
                }
                skip += count;
            }
            let listed_indexes = self.command(doc! {
                "listIndexes": &name,
                "$db": database,
            })?;
            let indexes = cursor_batch(&listed_indexes)?
                .into_iter()
                .filter(|spec| spec.get_str("name") != Ok("_id_"))
                .collect();
            let namespace = format!("{database}.{name}");
            let found_counters = self.command(doc! {
                "find": "counters",
                "filter": { "collection": &namespace },
                "$db": "admin",
            })?;
            let mut autoincrement = Vec::new();
            let mut last = Vec::new();
            for row in cursor_batch(&found_counters)? {
                let Ok(field) = row.get_str("field") else {
                    continue;
                };
                autoincrement.push(field.to_string());
                if let Some(value) = bson_i64(row.get("last")) {
                    last.push((field.to_string(), value));
                }
            }
            dump.collections.push(CollectionDump {
                name,
                documents,
                indexes,
                autoincrement,
                last,
            });
        }
        Ok(dump)
    }

    fn write_database(
        &mut self,
        database: &str,
        dump: &Dump,
        progress: &mut Progress,
    ) -> Result<(), String> {
        let listed = self.command(doc! { "listCollections": 1i32, "$db": database })?;
        for name in views_blocking_restore(&cursor_batch(&listed)?, dump) {
            let reply = self.command(doc! { "drop": name, "$db": database })?;
            ensure_ok(&reply)?;
        }
        for collection in &dump.collections {
            self.replace_collection(database, collection, progress)?;
        }
        for view in &dump.views {
            progress.set_label(&view.name);
            let reply = self.command(doc! { "drop": &view.name, "$db": database })?;
            ensure_ok(&reply)?;
            let mut command = doc! {
                "create": &view.name,
                "viewOn": &view.view_on,
                "pipeline": view.pipeline.iter().cloned().map(Bson::Document).collect::<Vec<_>>(),
                "$db": database,
            };
            if !view.kova.is_empty() {
                command.insert("kova", view.kova.clone());
            }
            ensure_ok(&self.command(command)?)?;
        }
        Ok(())
    }

    fn replace_collection(
        &mut self,
        database: &str,
        collection: &CollectionDump,
        progress: &mut Progress,
    ) -> Result<(), String> {
        progress.set_label(&collection.name);
        ensure_ok(&self.command(doc! { "drop": &collection.name, "$db": database })?)?;
        let mut create = doc! { "create": &collection.name, "$db": database };
        if !collection.autoincrement.is_empty() {
            create.insert(
                "autoincrement",
                collection
                    .autoincrement
                    .iter()
                    .cloned()
                    .map(Bson::String)
                    .collect::<Vec<_>>(),
            );
        }
        ensure_ok(&self.command(create)?)?;
        if !collection.indexes.is_empty() {
            let indexes = collection
                .indexes
                .iter()
                .cloned()
                .map(Bson::Document)
                .collect::<Vec<_>>();
            ensure_ok(&self.command(doc! {
                "createIndexes": &collection.name,
                "indexes": indexes,
                "$db": database,
            })?)?;
        }
        for batch in collection.documents.chunks(INSERT_BATCH) {
            let documents = batch
                .iter()
                .map(|document| {
                    let mut document = document.clone();
                    if !document.contains_key("_id") {
                        document.insert("_id", Bson::ObjectId(bson::oid::ObjectId::new()));
                    }
                    Bson::Document(document)
                })
                .collect::<Vec<_>>();
            if documents.is_empty() {
                continue;
            }
            let count = documents.len() as u64;
            ensure_ok(&self.command(doc! {
                "insert": &collection.name,
                "documents": documents,
                "$db": database,
            })?)?;
            progress.tick(count);
        }
        for (field, last) in &collection.last {
            ensure_ok(&self.command(doc! {
                "update": "counters",
                "updates": [{
                    "q": { "_id": format!("{database}.{}.{}", collection.name, field) },
                    "u": { "$set": { "last": *last } },
                    "multi": false,
                }],
                "$db": "admin",
            })?)?;
        }
        Ok(())
    }

    fn command(&mut self, command: Document) -> Result<Document, String> {
        let request_id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        let frame = crate::protocol::encode_reply(request_id, 0, &command)?;
        self.socket
            .write_all(&frame)
            .map_err(|err| format!("write to server failed: {}", io_message(&err)))?;
        let mut header = [0u8; crate::protocol::HEADER_SIZE];
        self.socket
            .read_exact(&mut header)
            .map_err(|err| format!("read from server failed: {}", io_message(&err)))?;
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
        self.socket
            .read_exact(&mut body)
            .map_err(|err| format!("read from server failed: {}", io_message(&err)))?;
        Ok(crate::protocol::decode_op_msg(&body)?.command)
    }
}

fn views_blocking_restore(listed: &[Document], dump: &Dump) -> Vec<String> {
    let mut replacing = std::collections::BTreeSet::new();
    for collection in &dump.collections {
        replacing.insert(collection.name.as_str());
    }
    for view in &dump.views {
        replacing.insert(view.name.as_str());
    }
    listed
        .iter()
        .filter(|info| info.get_str("type") == Ok("view"))
        .filter_map(|info| {
            let name = info.get_str("name").ok()?;
            let view_on = info
                .get_document("options")
                .ok()
                .and_then(|options| options.get_str("viewOn").ok())
                .unwrap_or("");
            if replacing.contains(name) || replacing.contains(view_on) {
                Some(name.to_string())
            } else {
                None
            }
        })
        .collect()
}

fn cursor_batch(reply: &Document) -> Result<Vec<Document>, String> {
    ensure_ok(reply)?;
    let cursor = reply
        .get_document("cursor")
        .map_err(|_| format!("reply has no cursor: {reply}"))?;
    let values = cursor
        .get_array("firstBatch")
        .map_err(|_| "cursor has no firstBatch".to_string())?;
    values
        .iter()
        .map(|value| {
            value
                .as_document()
                .cloned()
                .ok_or_else(|| "cursor row is not a document".to_string())
        })
        .collect()
}

pub(super) fn bson_i64(value: Option<&Bson>) -> Option<i64> {
    match value? {
        Bson::Int32(number) => Some(i64::from(*number)),
        Bson::Int64(number) => Some(*number),
        Bson::Double(number) if number.is_finite() && number.fract() == 0.0 => Some(*number as i64),
        _ => None,
    }
}

fn ensure_ok(reply: &Document) -> Result<(), String> {
    let ok = match reply.get("ok") {
        Some(Bson::Double(value)) => *value == 1.0,
        Some(Bson::Int32(value)) => *value == 1,
        Some(Bson::Int64(value)) => *value == 1,
        _ => false,
    };
    if ok {
        return Ok(());
    }
    Err(reply
        .get_str("errmsg")
        .unwrap_or("command failed")
        .to_string())
}

fn io_message(err: &std::io::Error) -> String {
    if err.kind() == std::io::ErrorKind::WouldBlock
        || err.kind() == std::io::ErrorKind::TimedOut
        || err.raw_os_error() == Some(11)
    {
        return format!("the server did not answer within {}s", IO_TIMEOUT.as_secs());
    }
    err.to_string()
}

fn dial(target: &Target) -> Result<TcpStream, String> {
    let addr = format!("{}:{}", target.host, target.port);
    let mut last = format!("no address for {addr}");
    for _ in 0..50 {
        let mut connected = None;
        let addrs = addr
            .to_socket_addrs()
            .map_err(|err| format!("cannot resolve {addr}: {err}"))?;
        for sockaddr in addrs {
            match TcpStream::connect_timeout(&sockaddr, Duration::from_millis(200)) {
                Ok(socket) => {
                    connected = Some(socket);
                    break;
                }
                Err(err) => last = err.to_string(),
            }
        }
        if let Some(socket) = connected {
            let _ = socket.set_nodelay(true);
            let _ = socket.set_read_timeout(Some(IO_TIMEOUT));
            let _ = socket.set_write_timeout(Some(IO_TIMEOUT));
            return Ok(socket);
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    Err(format!("cannot connect to {addr}: {last}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    static TEMP_SEQ: AtomicU64 = AtomicU64::new(1);

    struct Running {
        addr: std::net::SocketAddr,
        task: tokio::task::JoinHandle<()>,
    }

    impl Drop for Running {
        fn drop(&mut self) {
            self.task.abort();
        }
    }

    struct TempDir(std::path::PathBuf);

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn server() -> Running {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("addr");
        let router = crate::storage::PartitionRouter::new(crate::PARTITION_COUNT);
        let task = tokio::spawn(async move {
            let _ = crate::network::serve_bound(listener, router).await;
        });
        Running { addr, task }
    }

    fn target(port: u16) -> Target {
        Target {
            host: "127.0.0.1".into(),
            port,
            database: "shop".into(),
            user: None,
            password: None,
        }
    }

    fn temp_dir() -> TempDir {
        let dir = std::env::temp_dir().join(format!(
            "kova-dump-{}-{}",
            std::process::id(),
            TEMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir");
        TempDir(dir)
    }

    fn names(client: &mut Client, database: &str, collection: &str) -> Result<Vec<String>, String> {
        let reply = client.command(doc! {
            "find": collection,
            "filter": {},
            "$db": database,
        })?;
        let mut names = cursor_batch(&reply)?
            .iter()
            .map(|document| document.get_str("ename").unwrap_or("").to_string())
            .collect::<Vec<_>>();
        names.sort();
        Ok(names)
    }

    #[test]
    fn a_user_without_a_password_is_refused() {
        let target = Target {
            host: "127.0.0.1".into(),
            port: 1,
            database: "shop".into(),
            user: Some("root".into()),
            password: None,
        };
        let err = target.validate().expect_err("half a login");
        assert!(err.contains("password"), "{err}");
    }

    #[tokio::test]
    async fn each_dump_restores_and_find_returns_the_documents() {
        let running = server().await;
        let port = running.addr.port();
        let result = tokio::task::spawn_blocking(move || exercise(port))
            .await
            .expect("join");
        result.expect("restore");
    }

    fn exercise(port: u16) -> Result<(), String> {
        let target = target(port);
        let dir = temp_dir();
        let mut client = Client::connect(&target)?;
        ensure_ok(&client.command(doc! {
            "insert": "emp",
            "documents": [
                { "ename": "SMITH", "sal": 800i32 },
                { "ename": "ALLEN", "sal": 1600i32 },
            ],
            "$db": "shop",
        })?)?;
        ensure_ok(&client.command(doc! {
            "createIndexes": "emp",
            "indexes": [{ "key": { "ename": 1i32 }, "name": "ename_1", "unique": true }],
            "$db": "shop",
        })?)?;
        ensure_ok(&client.command(doc! {
            "create": "well_paid",
            "viewOn": "emp",
            "pipeline": [{ "$match": { "sal": { "$gt": 1000i32 } } }],
            "$db": "shop",
        })?)?;
        let kova_path = dir.0.join("shop.kdmp");
        let report = backup(&target, &kova_path)?;
        assert_eq!(report.collections, 1, "kova backup collections");
        assert_eq!(report.documents, 2, "kova backup documents");
        assert_eq!(report.views, 1, "kova backup views");
        let restored = restore(&target, &kova_path, &yes_restore())?;
        assert_eq!(restored.documents, 2, "kova restore");
        let mut client = Client::connect(&target)?;
        assert_eq!(
            names(&mut client, "shop", "emp")?,
            vec!["ALLEN".to_string(), "SMITH".to_string()],
            "kova find"
        );
        assert_eq!(
            names(&mut client, "shop", "well_paid")?,
            vec!["ALLEN".to_string()],
            "kova view"
        );
        let indexes = cursor_batch(&client.command(doc! {
            "listIndexes": "emp",
            "$db": "shop",
        })?)?;
        assert!(
            indexes
                .iter()
                .any(|spec| spec.get_str("name") == Ok("ename_1")),
            "kova index: {indexes:?}"
        );

        let postgres = dir.0.join("pg.sql");
        std::fs::write(
            &postgres,
            r#"
--
-- PostgreSQL database dump
--
CREATE TABLE public.emp (
    empno integer NOT NULL,
    ename text
);
COPY public.emp (empno, ename) FROM stdin;
7369	SMITH
7499	ALLEN
\.
"#,
        )
        .map_err(|err| err.to_string())?;
        restore(&target, &postgres, &yes_restore())?;
        let mut client = Client::connect(&target)?;
        assert_eq!(
            names(&mut client, "shop", "emp")?,
            vec!["ALLEN".to_string(), "SMITH".to_string()],
            "postgres find"
        );
        assert_eq!(
            cursor_batch(
                &client.command(
                    doc! { "find": "emp", "filter": { "empno": 7369i32 }, "$db": "shop" }
                )?
            )?
            .len(),
            1,
            "postgres empno"
        );
        ensure_ok(&client.command(doc! {
            "insert": "dept",
            "documents": [{ "ename": "KEEP" }],
            "$db": "shop",
        })?)?;
        let partial = dir.0.join("partial.sql");
        std::fs::write(
            &partial,
            r#"
--
-- PostgreSQL database dump
--
CREATE TABLE public.emp (
    empno integer NOT NULL,
    ename text
);
INSERT INTO public.emp (empno, ename) VALUES (7839, 'KING');
CREATE TABLE public.dept (
    deptno integer,
    dname text
);
INSERT INTO public.dept (deptno, dname) VALUES (10, 'ACCOUNTING');
"#,
        )
        .map_err(|err| err.to_string())?;
        restore(
            &target,
            &partial,
            &RestoreOptions {
                tables: vec!["emp".into()],
                yes: true,
            },
        )?;
        let mut client = Client::connect(&target)?;
        assert_eq!(
            names(&mut client, "shop", "emp")?,
            vec!["KING".to_string()],
            "whitelist replace"
        );
        assert_eq!(
            names(&mut client, "shop", "dept")?,
            vec!["KEEP".to_string()],
            "whitelist leaves other collections"
        );

        let mysql = dir.0.join("my.sql");
        std::fs::write(
            &mysql,
            r#"
-- MySQL dump 10.13
CREATE TABLE `bonus` (
  `ename` varchar(10),
  `sal` int
);
INSERT INTO `bonus` VALUES ('SMITH',800),('ALLEN',1600);
"#,
        )
        .map_err(|err| err.to_string())?;
        restore(&target, &mysql, &yes_restore())?;
        let mut client = Client::connect(&target)?;
        assert_eq!(
            names(&mut client, "shop", "bonus")?,
            vec!["ALLEN".to_string(), "SMITH".to_string()],
            "mysql find"
        );

        let mongo_db = dir.0.join("dump").join("shop");
        std::fs::create_dir_all(&mongo_db).map_err(|err| err.to_string())?;
        let document = doc! { "ename": "KING", "sal": 5000i32 };
        std::fs::write(
            mongo_db.join("court.bson"),
            bson::to_vec(&document).map_err(|err| err.to_string())?,
        )
        .map_err(|err| err.to_string())?;
        std::fs::write(
            mongo_db.join("court.metadata.json"),
            r#"{"indexes":[{"v":2,"key":{"_id":1},"name":"_id_"},{"v":2,"key":{"ename":1},"name":"ename_1"}]}"#,
        )
        .map_err(|err| err.to_string())?;
        restore(&target, &dir.0.join("dump"), &yes_restore())?;
        let mut client = Client::connect(&target)?;
        assert_eq!(
            names(&mut client, "shop", "court")?,
            vec!["KING".to_string()],
            "mongodump directory find"
        );

        let archive = dir.0.join("shop.archive");
        std::fs::write(&archive, sample_archive()).map_err(|err| err.to_string())?;
        restore(&target, &archive, &yes_restore())?;
        let mut client = Client::connect(&target)?;
        assert_eq!(
            names(&mut client, "shop", "peer")?,
            vec!["SCOTT".to_string()],
            "mongodump archive find"
        );
        Ok(())
    }

    fn yes_restore() -> RestoreOptions {
        RestoreOptions {
            tables: Vec::new(),
            yes: true,
        }
    }

    #[test]
    fn a_table_list_keeps_only_the_named_collections() {
        let dump = Dump {
            collections: vec![
                CollectionDump {
                    name: "emp".into(),
                    documents: vec![doc! { "ename": "SMITH" }],
                    indexes: Vec::new(),
                    autoincrement: Vec::new(),
                    last: Vec::new(),
                },
                CollectionDump {
                    name: "dept".into(),
                    documents: vec![doc! { "dname": "SALES" }],
                    indexes: Vec::new(),
                    autoincrement: Vec::new(),
                    last: Vec::new(),
                },
            ],
            views: vec![ViewDump {
                name: "well_paid".into(),
                view_on: "emp".into(),
                pipeline: Vec::new(),
                kova: Document::new(),
            }],
        };
        let selected = select_dump(dump.clone(), &["emp".into(), "well_paid".into()]).unwrap();
        assert_eq!(selected.collections.len(), 1);
        assert_eq!(selected.collections[0].name, "emp");
        assert_eq!(selected.views.len(), 1);
        let err = select_dump(dump, &["missing".into()]).unwrap_err();
        assert!(err.contains("missing"), "{err}");
    }

    #[test]
    fn an_existing_collection_is_listed_before_overwrite() {
        let dump = Dump {
            collections: vec![CollectionDump {
                name: "emp".into(),
                documents: Vec::new(),
                indexes: Vec::new(),
                autoincrement: Vec::new(),
                last: Vec::new(),
            }],
            views: Vec::new(),
        };
        let listed = vec![
            doc! { "name": "emp", "type": "collection" },
            doc! { "name": "well_paid", "type": "view", "options": { "viewOn": "emp" } },
            doc! { "name": "dept", "type": "collection" },
        ];
        let (replacing, blocking) = collisions(&dump, &listed);
        assert_eq!(replacing, vec!["emp".to_string()]);
        assert_eq!(blocking, vec!["well_paid".to_string()]);
        let message = overwrite_message("shop", &replacing, &blocking);
        assert!(message.contains("database shop already has"), "{message}");
        assert!(message.contains("emp"), "{message}");
        assert!(message.contains("well_paid"), "{message}");
        assert!(!message.contains("dept"), "{message}");
        assert!(accepts_overwrite("y"));
        assert!(accepts_overwrite(" YES "));
        assert!(!accepts_overwrite("n"));
        let err = refusal_message("shop", &replacing, &blocking);
        assert!(err.contains("--yes"), "{err}");
    }

    #[test]
    fn the_progress_line_fills_with_the_documents_written() {
        let line = progress_line(2, 6, "proposta_venda");
        assert_eq!(
            line,
            "[=========-------------------]  33%  2/6  proposta_venda"
        );
        assert_eq!(
            progress_line(6, 6, "proposta_venda"),
            "[============================] 100%  6/6  proposta_venda"
        );
    }

    #[tokio::test]
    async fn sql_setval_continues_on_the_next_insert() {
        let running = server().await;
        let port = running.addr.port();
        let result = tokio::task::spawn_blocking(move || setval_round_trip(port))
            .await
            .expect("join");
        result.expect("setval");
    }

    fn setval_round_trip(port: u16) -> Result<(), String> {
        let target = target(port);
        let dir = temp_dir();
        let sql = dir.0.join("banco.sql");
        std::fs::write(
            &sql,
            r#"
-- PostgreSQL database dump
CREATE TABLE public.banco (
    id integer DEFAULT nextval('public.banco_id_seq'::regclass) NOT NULL,
    codigo text
);
INSERT INTO public.banco VALUES (115, '001');
ALTER TABLE ONLY public.banco
    ADD CONSTRAINT banco_pkey PRIMARY KEY (id);
SELECT pg_catalog.setval('public.banco_id_seq', 115, true);
"#,
        )
        .map_err(|err| err.to_string())?;
        restore(&target, &sql, &yes_restore())?;
        let mut client = Client::connect(&target)?;
        assert_eq!(counter_of(&mut client, "shop.banco.id")?, 115);
        insert_codigo(&mut client, "002")?;
        assert_eq!(counter_of(&mut client, "shop.banco.id")?, 116);
        assert!(banco_ids(&mut client)?.contains(&116));
        insert_codigo(&mut client, "003")?;
        assert!(banco_ids(&mut client)?.contains(&117));

        let path = dir.0.join("shop.kdmp");
        backup(&target, &path)?;
        restore(&target, &path, &yes_restore())?;
        let mut client = Client::connect(&target)?;
        assert_eq!(counter_of(&mut client, "shop.banco.id")?, 117);
        insert_codigo(&mut client, "004")?;
        assert!(banco_ids(&mut client)?.contains(&118));
        Ok(())
    }

    fn counter_of(client: &mut Client, id: &str) -> Result<i64, String> {
        let reply = client.command(doc! {
            "find": "counters",
            "filter": { "_id": id },
            "$db": "admin",
        })?;
        let rows = cursor_batch(&reply)?;
        let row = rows.first().ok_or_else(|| format!("no counter {id}"))?;
        bson_i64(row.get("last")).ok_or_else(|| format!("counter {id} has no last"))
    }

    fn insert_codigo(client: &mut Client, codigo: &str) -> Result<(), String> {
        ensure_ok(&client.command(doc! {
            "insert": "banco",
            "documents": [{ "codigo": codigo }],
            "$db": "shop",
        })?)
    }

    fn banco_ids(client: &mut Client) -> Result<Vec<i32>, String> {
        let reply = client.command(doc! {
            "find": "banco",
            "filter": {},
            "$db": "shop",
        })?;
        Ok(cursor_batch(&reply)?
            .iter()
            .filter_map(|document| document.get_i32("id").ok())
            .collect())
    }

    fn sample_archive() -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0x8199_e26du32.to_le_bytes());
        extend(
            &mut bytes,
            &doc! { "version": "0.1", "concurrent_collections": 1i32 },
        );
        extend(
            &mut bytes,
            &doc! { "db": "shop", "collection": "peer", "metadata": "{}", "size": 0i32, "type": "collection" },
        );
        bytes.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        extend(
            &mut bytes,
            &doc! { "db": "shop", "collection": "peer", "EOF": false, "CRC": 0i64 },
        );
        extend(&mut bytes, &doc! { "ename": "SCOTT", "sal": 3000i32 });
        bytes.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        extend(
            &mut bytes,
            &doc! { "db": "shop", "collection": "peer", "EOF": true, "CRC": 0i64 },
        );
        bytes.extend_from_slice(&0xffff_ffffu32.to_le_bytes());
        bytes
    }

    fn extend(bytes: &mut Vec<u8>, document: &Document) {
        bytes.extend(bson::to_vec(document).expect("bson"));
    }
}
